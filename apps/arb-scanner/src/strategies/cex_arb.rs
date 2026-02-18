use async_trait::async_trait;
use chrono::Utc;
use futures::{SinkExt, StreamExt};
use log::{error, info, warn};
use serde_json::Value;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{sleep, Duration};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use url::Url;
use uuid::Uuid;

use crate::engine::{PolymarketClient, WS_URL};
use crate::strategies::control::{
    build_scan_payload,
    compute_strategy_bet_size,
    is_strategy_enabled,
    publish_heartbeat,
    publish_event,
    read_risk_config,
    read_risk_guard_cooldown,
    read_sim_available_cash,
    read_simulation_reset_ts,
    strategy_variant,
    reserve_sim_notional_for_strategy,
    release_sim_notional_for_strategy,
    settle_sim_position_for_strategy,
    read_trading_mode,
    TradingMode,
};
use crate::strategies::market_data::{update_book_from_market_ws, BinaryBook};
use crate::strategies::simulation::{realized_pnl, SimCostModel};
use crate::strategies::vol_regime::{
    detect_regime, regime_edge_adjustment_bps, regime_exit_multipliers, regime_sizing_multiplier,
};
use crate::strategies::Strategy;

const COINBASE_ADVANCED_WS_URL: &str = "wss://advanced-trade-ws.coinbase.com";
const MOMENTUM_LOOKBACK_MS: i64 = 800;
const MOMENTUM_ENTRY_THRESHOLD: f64 = 0.0008; // 8 bps in <1s
const MOMENTUM_STD_MULTIPLIER: f64 = 1.5;
const MOMENTUM_MAX_THRESHOLD: f64 = 0.0040; // 40 bps ceiling
const COINBASE_STALE_MS: i64 = 2_000;
const BOOK_STALE_MS: i64 = 1_500;
const TAKE_PROFIT_PCT: f64 = 0.03;
const STOP_LOSS_PCT: f64 = -0.02;
const MAX_HOLD_MS: i64 = 15_000;
const MAX_CONCURRENT_POSITIONS: usize = 3;
const COINBASE_PRODUCT_ID: &str = "BTC-USD";
const ENTRY_EXPIRY_CUTOFF_MS: i64 = 30_000;
const MAX_ENTRY_SPREAD: f64 = 0.08;
const LIVE_PREVIEW_COOLDOWN_MS: i64 = 2_000;

fn coinbase_ws_url() -> String {
    std::env::var("COINBASE_WS_URL").unwrap_or_else(|_| COINBASE_ADVANCED_WS_URL.to_string())
}

fn coinbase_ticker_subscriptions(ws_url: &str) -> Vec<Value> {
    if ws_url.contains("ws-feed.exchange.coinbase.com") {
        vec![serde_json::json!({
            "type": "subscribe",
            "product_ids": [COINBASE_PRODUCT_ID],
            "channels": ["ticker", "heartbeat"]
        })]
    } else {
        vec![
            serde_json::json!({
                "type": "subscribe",
                "channel": "ticker",
                "product_ids": [COINBASE_PRODUCT_ID]
            }),
            serde_json::json!({
                "type": "subscribe",
                "channel": "heartbeats",
                "product_ids": [COINBASE_PRODUCT_ID]
            }),
        ]
    }
}

fn parse_number(value: Option<&Value>) -> Option<f64> {
    match value {
        Some(v) => v
            .as_str()
            .and_then(|s| s.parse::<f64>().ok())
            .or_else(|| v.as_f64()),
        None => None,
    }
}

fn parse_sequence(value: Option<&Value>) -> Option<u64> {
    value.and_then(|v| {
        v.as_u64()
            .or_else(|| v.as_i64().and_then(|s| if s >= 0 { Some(s as u64) } else { None }))
            .or_else(|| v.as_str().and_then(|s| s.parse::<u64>().ok()))
    })
}

fn parse_coinbase_ticker_price(payload: &str) -> Option<(f64, Option<u64>)> {
    let parsed = serde_json::from_str::<Value>(payload).ok()?;
    let root_sequence = parse_sequence(parsed.get("sequence_num"))
        .or_else(|| parse_sequence(parsed.get("sequence")));

    // Legacy Coinbase Exchange feed shape.
    if parsed.get("type").and_then(|v| v.as_str()) == Some("ticker")
        && parsed.get("product_id").and_then(|v| v.as_str()) == Some(COINBASE_PRODUCT_ID)
    {
        return parse_number(parsed.get("price")).map(|price| {
            let seq = parse_sequence(parsed.get("sequence")).or(root_sequence);
            (price, seq)
        });
    }

    // Coinbase Advanced Trade shape.
    if parsed.get("channel").and_then(|v| v.as_str()) != Some("ticker") {
        return None;
    }

    let events = parsed.get("events")?.as_array()?;
    for event in events {
        let Some(tickers) = event.get("tickers").and_then(|v| v.as_array()) else {
            continue;
        };
        for ticker in tickers {
            if ticker.get("product_id").and_then(|v| v.as_str()) != Some(COINBASE_PRODUCT_ID) {
                continue;
            }

            if let Some(price) = parse_number(ticker.get("price")) {
                return Some((price, root_sequence));
            }

            let bid = parse_number(ticker.get("best_bid"));
            let ask = parse_number(ticker.get("best_ask"));
            if let (Some(best_bid), Some(best_ask)) = (bid, ask) {
                return Some(((best_bid + best_ask) / 2.0, root_sequence));
            }
        }
    }

    None
}

fn parse_coinbase_message_sequence(payload: &str) -> Option<u64> {
    let parsed = serde_json::from_str::<Value>(payload).ok()?;
    parse_sequence(parsed.get("sequence_num")).or_else(|| parse_sequence(parsed.get("sequence")))
}

fn rolling_std(values: &VecDeque<f64>) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let var = values
        .iter()
        .map(|v| {
            let d = *v - mean;
            d * d
        })
        .sum::<f64>()
        / (values.len() as f64 - 1.0);
    var.max(0.0).sqrt()
}

#[derive(Debug, Clone, Copy)]
enum Side {
    Yes,
    No,
}

#[derive(Debug, Clone)]
struct Position {
    id: String,
    side: Side,
    entry_poly: f64,
    entry_cb: f64,
    size: f64,
    timestamp_ms: i64,
}

pub struct CexArbStrategy {
    client: PolymarketClient,
    open_positions: Arc<RwLock<Vec<Position>>>,
}

impl CexArbStrategy {
    pub fn new() -> Self {
        Self {
            client: PolymarketClient::new(),
            open_positions: Arc::new(RwLock::new(Vec::new())),
        }
    }
}

#[async_trait]
impl Strategy for CexArbStrategy {
    async fn run(&self, redis_client: redis::Client) {
        info!("Starting CEX Latency Sniper [Coinbase Momentum vs Polymarket Quotes]...");

        let mut conn = match redis_client.get_multiplexed_async_connection().await {
            Ok(c) => c,
            Err(e) => {
                error!("Redis Connect Fail: {}", e);
                return;
            }
        };
        let cost_model = SimCostModel::from_env();
        let variant = strategy_variant();
        let mut last_seen_reset_ts = 0_i64;

        let latest_cb_price = Arc::new(RwLock::new((0.0, 0_i64)));
        let cb_writer = latest_cb_price.clone();
        let coinbase_ws_url = coinbase_ws_url();
        let coinbase_subscriptions = coinbase_ticker_subscriptions(&coinbase_ws_url);

        // Coinbase ticker feed
        tokio::spawn(async move {
            loop {
                match connect_async(coinbase_ws_url.as_str()).await {
                    Ok((mut ws_stream, _)) => {
                        info!("Connected to Coinbase ticker feed: {}", coinbase_ws_url);
                        let mut last_sequence: Option<u64> = None;
                        for sub_msg in &coinbase_subscriptions {
                            if let Err(e) = ws_stream.send(Message::Text(sub_msg.to_string())).await {
                                error!("Coinbase subscribe failed: {}", e);
                            }
                        }

                        while let Some(msg) = ws_stream.next().await {
                            match msg {
                                Ok(Message::Text(text)) => {
                                    if let Some(seq) = parse_coinbase_message_sequence(&text) {
                                        if let Some(prev) = last_sequence {
                                            if seq <= prev {
                                                continue;
                                            }
                                            let expected = prev.saturating_add(1);
                                            if seq > expected {
                                                warn!(
                                                    "Coinbase ticker sequence gap detected (expected {}, got {}); reconnecting",
                                                    expected,
                                                    seq
                                                );
                                                break;
                                            }
                                        }
                                        last_sequence = Some(seq);
                                    }

                                    if let Some((price, _sequence)) = parse_coinbase_ticker_price(&text) {
                                        let mut w = cb_writer.write().await;
                                        *w = (price, Utc::now().timestamp_millis());
                                    }
                                }
                                Ok(Message::Ping(payload)) => {
                                    let _ = ws_stream.send(Message::Pong(payload)).await;
                                }
                                Ok(Message::Close(_)) => break,
                                Ok(Message::Binary(_)) | Ok(Message::Pong(_)) => continue,
                                Ok(_) => continue,
                                Err(e) => {
                                    error!("Coinbase ticker message error: {}", e);
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("Coinbase WS reconnecting after error: {}", e);
                        sleep(Duration::from_secs(3)).await;
                    }
                }
            }
        });

        loop {
            let target_market = loop {
                if let Some(m) = self.client.fetch_current_market("BTC").await {
                    break m;
                }
                sleep(Duration::from_secs(5)).await;
            };

            info!("Sniper locked on market: {}", target_market.slug);

            let poly_url = match Url::parse(WS_URL) {
                Ok(url) => url,
                Err(e) => {
                    error!("Invalid polymarket WS URL: {}", e);
                    sleep(Duration::from_secs(3)).await;
                    continue;
                }
            };
            use tokio_tungstenite::tungstenite::client::IntoClientRequest;
            let mut request = match poly_url.into_client_request() {
                Ok(req) => req,
                Err(e) => {
                    error!("Failed to create polymarket WS request: {}", e);
                    sleep(Duration::from_secs(3)).await;
                    continue;
                }
            };
            if let Ok(ua) = "Mozilla/5.0".parse() {
                request.headers_mut().insert("User-Agent", ua);
            }

            let mut poly_ws = match connect_async(request).await {
                Ok((ws, _)) => ws,
                Err(e) => {
                    error!("Polymarket WS connect failed: {}", e);
                    sleep(Duration::from_secs(3)).await;
                    continue;
                }
            };

            let sub_msg = serde_json::json!({
                "assets_ids": [target_market.yes_token, target_market.no_token],
                "type": "market"
            });
            let _ = poly_ws.send(Message::Text(sub_msg.to_string())).await;

            let mut interval = tokio::time::interval(Duration::from_millis(100));
            let market_expiry = target_market
                .slug
                .split("-15m-")
                .nth(1)
                .and_then(|s| s.parse::<i64>().ok())
                .map(|s| s + 900)
                .unwrap_or(Utc::now().timestamp() + 900);

            let mut book = BinaryBook::default();
            let mut cb_history: VecDeque<(i64, f64)> = VecDeque::new();
            let mut momentum_history: VecDeque<f64> = VecDeque::new();
            let positions_link = self.open_positions.clone();
            let mut last_live_preview_ms = 0_i64;

            loop {
                tokio::select! {
                    Some(msg) = poly_ws.next() => {
                        match msg {
                            Ok(Message::Text(text)) => {
                                update_book_from_market_ws(
                                    &text,
                                    &target_market.yes_token,
                                    &target_market.no_token,
                                    &mut book,
                                );
                            }
                            Ok(Message::Ping(payload)) => {
                                let _ = poly_ws.send(Message::Pong(payload)).await;
                            }
                            Ok(Message::Close(_)) => break,
                            Ok(Message::Binary(_)) | Ok(Message::Pong(_)) => {}
                            Ok(_) => {}
                            Err(e) => {
                                error!("CEX sniper Polymarket WS error: {}", e);
                                break;
                            }
                        }
                    }
                    _ = interval.tick() => {
                        let now_ms = Utc::now().timestamp_millis();
                        let enabled = is_strategy_enabled(&mut conn, "CEX_SNIPER").await;

                        if !enabled {
                            let released_notional = {
                                let mut positions = positions_link.write().await;
                                let release = positions.iter().map(|pos| pos.size).sum::<f64>();
                                if release > 0.0 {
                                    positions.clear();
                                }
                                release
                            };
                            if released_notional > 0.0 {
                                let _ = release_sim_notional_for_strategy(&mut conn, "CEX_SNIPER", released_notional).await;
                            }
                            publish_heartbeat(&mut conn, "cex_arb").await;
                            continue;
                        }

                        // Risk guard cooldown — skip entry if backend set a post-loss cooldown.
                        let cooldown_until = read_risk_guard_cooldown(&mut conn, "CEX_SNIPER").await;
                        if cooldown_until > 0 && now_ms < cooldown_until {
                            publish_heartbeat(&mut conn, "cex_arb").await;
                            continue;
                        }

                        let (cb_price, cb_ts_ms) = *latest_cb_price.read().await;
                        if cb_price <= 0.0
                            || now_ms - cb_ts_ms > COINBASE_STALE_MS
                            || !book.yes.is_valid()
                            || !book.no.is_valid()
                            || now_ms - book.last_update_ms > BOOK_STALE_MS
                        {
                            publish_heartbeat(&mut conn, "cex_arb").await;
                            continue;
                        }

                        cb_history.push_back((now_ms, cb_price));
                        while let Some((ts, _)) = cb_history.front() {
                            if now_ms - ts > 2_000 {
                                cb_history.pop_front();
                            } else {
                                break;
                            }
                        }

                        if cb_history.len() < 4 {
                            publish_heartbeat(&mut conn, "cex_arb").await;
                            continue;
                        }

                        let reference_price = cb_history
                            .iter()
                            .find(|(ts, _)| now_ms - *ts >= MOMENTUM_LOOKBACK_MS)
                            .map(|(_, p)| *p);

                        let Some(reference_price) = reference_price else {
                            publish_heartbeat(&mut conn, "cex_arb").await;
                            continue;
                        };

                        let momentum = if reference_price > 0.0 {
                            (cb_price - reference_price) / reference_price
                        } else {
                            0.0
                        };
                        momentum_history.push_back(momentum);
                        while momentum_history.len() > 256 {
                            momentum_history.pop_front();
                        }

                        let momentum_sigma = rolling_std(&momentum_history);
                        let cb_price_vec: Vec<(i64, f64)> = cb_history.iter().copied().collect();
                        let regime_snapshot = detect_regime(&cb_price_vec);
                        let regime_label = regime_snapshot
                            .as_ref()
                            .map(|regime| match regime.regime {
                                crate::strategies::vol_regime::Regime::Trending => "TRENDING",
                                crate::strategies::vol_regime::Regime::MeanReverting => "MEAN_REVERTING",
                                crate::strategies::vol_regime::Regime::Choppy => "CHOPPY",
                            })
                            .unwrap_or("UNKNOWN");
                        let regime_hurst = regime_snapshot
                            .as_ref()
                            .map(|regime| regime.hurst_exponent)
                            .unwrap_or(0.5);
                        let regime_parkinson_vol = regime_snapshot
                            .as_ref()
                            .map(|regime| regime.parkinson_vol)
                            .unwrap_or(0.0);
                        let regime_edge_adjustment = regime_snapshot
                            .as_ref()
                            .map(regime_edge_adjustment_bps)
                            .unwrap_or(0.0);
                        let regime_size_multiplier = regime_snapshot
                            .as_ref()
                            .map(regime_sizing_multiplier)
                            .unwrap_or(1.0);
                        let (tp_mult, sl_mult) = regime_snapshot
                            .as_ref()
                            .map(regime_exit_multipliers)
                            .unwrap_or((1.0, 1.0));
                        let adaptive_threshold = (MOMENTUM_ENTRY_THRESHOLD
                            .max(momentum_sigma * MOMENTUM_STD_MULTIPLIER))
                            .min(MOMENTUM_MAX_THRESHOLD);
                        let required_momentum = (adaptive_threshold
                            + cost_model.round_trip_cost_rate()
                            + regime_edge_adjustment)
                            .max(cost_model.round_trip_cost_rate() * 0.5);
                        let passes_threshold = momentum.abs() >= required_momentum;
                        let direction = if momentum >= 0.0 { "YES" } else { "NO" };
                        let reason = if passes_threshold {
                            format!(
                                "Momentum {:.2}% exceeds adaptive+cost+regime {:.2}% threshold ({}) [{}]",
                                momentum * 100.0,
                                required_momentum * 100.0,
                                direction,
                                regime_label
                            )
                        } else {
                            format!(
                                "Momentum {:.2}% below adaptive+cost+regime {:.2}% threshold [{}]",
                                momentum * 100.0,
                                required_momentum * 100.0,
                                regime_label
                            )
                        };

                        let scan_msg = build_scan_payload(
                            &target_market.market_id,
                            "SNIPER-FEED",
                            "CEX_SNIPER",
                            "MOMENTUM",
                            "RATIO",
                            [cb_price, book.yes.mid()],
                            book.yes.mid() + book.no.mid(),
                            "YES_NO_MID_SUM",
                            momentum,
                            required_momentum,
                            passes_threshold,
                            reason,
                            now_ms,
                            serde_json::json!({
                                    "reference_price": reference_price,
                                    "direction": direction,
                                    "yes_mid": book.yes.mid(),
                                    "no_mid": book.no.mid(),
                                    "momentum_sigma": momentum_sigma,
                                    "regime": regime_label,
                                    "regime_hurst": regime_hurst,
                                    "regime_parkinson_vol": regime_parkinson_vol,
                                    "regime_edge_adjustment_bps": regime_edge_adjustment * 10_000.0,
                                    "regime_size_multiplier": regime_size_multiplier,
                                    "tp_multiplier": tp_mult,
                                    "sl_multiplier": sl_mult,
                                    "required_momentum": required_momentum,
                                    "round_trip_cost_rate": cost_model.round_trip_cost_rate(),
                                }),
                        );
                        publish_event(&mut conn, "arbitrage:scan", scan_msg.to_string()).await;

                        let trading_mode = read_trading_mode(&mut conn).await;
                        if trading_mode == TradingMode::Live {
                            let time_to_expiry_ms = market_expiry.saturating_mul(1000) - now_ms;
                            let yes_spread = (book.yes.best_ask - book.yes.best_bid).max(0.0);
                            let no_spread = (book.no.best_ask - book.no.best_bid).max(0.0);
                            let eligible_long = momentum >= required_momentum
                                && book.yes.best_ask >= 0.03
                                && book.yes.best_ask <= 0.97
                                && yes_spread <= MAX_ENTRY_SPREAD;
                            let eligible_short = momentum <= -required_momentum
                                && book.no.best_ask >= 0.03
                                && book.no.best_ask <= 0.97
                                && no_spread <= MAX_ENTRY_SPREAD;

                            if (eligible_long || eligible_short)
                                && time_to_expiry_ms >= ENTRY_EXPIRY_CUTOFF_MS
                                && now_ms - last_live_preview_ms >= LIVE_PREVIEW_COOLDOWN_MS
                            {
                                let risk_cfg = read_risk_config(&mut conn).await;
                                let available_cash = read_sim_available_cash(&mut conn).await;
                                let raw_preview_size = compute_strategy_bet_size(
                                    &mut conn,
                                    "CEX_SNIPER",
                                    available_cash,
                                    &risk_cfg,
                                    10.0,
                                    0.20,
                                ).await;
                                let preview_size = (raw_preview_size * regime_size_multiplier)
                                    .min(available_cash)
                                    .max(0.0);
                                if preview_size > 0.0 {
                                    let preview_price = if eligible_long { book.yes.best_ask } else { book.no.best_ask };
                                    if preview_price > 0.0 {
                                        let execution_id = Uuid::new_v4().to_string();
                                        let preview_msg = serde_json::json!({
                                            "execution_id": execution_id,
                                            "market": "CEX Sniper",
                                            "side": "LIVE_DRY_RUN",
                                            "price": preview_price,
                                            "size": preview_size,
                                            "timestamp": now_ms,
                                            "mode": "LIVE_DRY_RUN",
                                            "details": {
                                                "direction": direction,
                                                "momentum": momentum,
                                                "adaptive_threshold": adaptive_threshold,
                                                "required_momentum": required_momentum,
                                                "regime": regime_label,
                                                "regime_hurst": regime_hurst,
                                                "regime_parkinson_vol": regime_parkinson_vol,
                                                "regime_size_multiplier": regime_size_multiplier,
                                                "coinbase_price": cb_price,
                                                "time_to_expiry_ms": time_to_expiry_ms,
                                                "yes_spread": yes_spread,
                                                "no_spread": no_spread,
                                                "preflight": {
                                                    "venue": "POLYMARKET",
                                                    "strategy": "CEX_SNIPER",
                                                    "orders": [
                                                        {
                                                            "token_id": if eligible_long { target_market.yes_token.clone() } else { target_market.no_token.clone() },
                                                            "condition_id": target_market.market_id,
                                                            "side": "BUY",
                                                            "price": preview_price,
                                                            "size": preview_size,
                                                            "size_unit": "USD_NOTIONAL",
                                                        }
                                                    ]
                                                }
                                            }
                                        });
                                        publish_event(&mut conn, "arbitrage:execution", preview_msg.to_string()).await;
                                        last_live_preview_ms = now_ms;
                                    }
                                }
                            }
                            let (cleared, released_notional) = {
                                let mut positions = positions_link.write().await;
                                let count = positions.len();
                                let release = positions.iter().map(|pos| pos.size).sum::<f64>();
                                if count > 0 {
                                    positions.clear();
                                }
                                (count, release)
                            };
                            if released_notional > 0.0 {
                                let _ = release_sim_notional_for_strategy(&mut conn, "CEX_SNIPER", released_notional).await;
                            }
                            if cleared > 0 {
                                info!("CEX Sniper: clearing {} paper position(s) in LIVE mode", cleared);
                            }
                            publish_heartbeat(&mut conn, "cex_arb").await;
                            continue;
                        }

                        let reset_ts = read_simulation_reset_ts(&mut conn).await;
                        if reset_ts > last_seen_reset_ts {
                            last_seen_reset_ts = reset_ts;
                            cb_history.clear();
                            momentum_history.clear();
                            let mut positions = positions_link.write().await;
                            positions.clear();
                            publish_heartbeat(&mut conn, "cex_arb").await;
                            continue;
                        }

                        let mut closed: Vec<(Position, f64, f64, i64, &'static str)> = Vec::new();
                        let mut new_entry: Option<Position> = None;

                        {
                            let mut positions = positions_link.write().await;
                            let mut done: Vec<usize> = Vec::new();

                            for (idx, pos) in positions.iter().enumerate() {
                                let exit_price = match pos.side {
                                    Side::Yes => book.yes.best_bid,
                                    Side::No => book.no.best_bid,
                                };

                                if exit_price <= 0.0 {
                                    continue;
                                }

                                let gross_return = (exit_price - pos.entry_poly) / pos.entry_poly;
                                let hold_ms = now_ms - pos.timestamp_ms;

                                // Adaptive exit multipliers from the current regime snapshot.
                                let adjusted_tp = TAKE_PROFIT_PCT * tp_mult;
                                let adjusted_sl = STOP_LOSS_PCT * sl_mult;

                                if gross_return >= adjusted_tp || gross_return <= adjusted_sl || hold_ms >= MAX_HOLD_MS {
                                    let reason = if gross_return >= adjusted_tp {
                                        "TAKE_PROFIT"
                                    } else if gross_return <= adjusted_sl {
                                        "STOP_LOSS"
                                    } else {
                                        "TIME_EXIT"
                                    };
                                    closed.push((pos.clone(), exit_price, gross_return, hold_ms, reason));
                                    done.push(idx);
                                }
                            }

                            for idx in done.iter().rev() {
                                positions.remove(*idx);
                            }

                            if positions.len() < MAX_CONCURRENT_POSITIONS {
                                let time_to_expiry_ms = market_expiry.saturating_mul(1000) - now_ms;
                                let yes_spread = (book.yes.best_ask - book.yes.best_bid).max(0.0);
                                let no_spread = (book.no.best_ask - book.no.best_bid).max(0.0);
                                // Skip if already positioned on the same side (duplicate market guard)
                                let has_yes = positions.iter().any(|p| matches!(p.side, Side::Yes));
                                let has_no = positions.iter().any(|p| matches!(p.side, Side::No));
                                if time_to_expiry_ms < ENTRY_EXPIRY_CUTOFF_MS {
                                    // Skip new entries close to settlement to avoid execution and resolution risk.
                                } else if momentum >= required_momentum
                                    && book.yes.best_ask >= 0.03
                                    && book.yes.best_ask <= 0.97
                                    && yes_spread <= MAX_ENTRY_SPREAD
                                    && !has_yes
                                {
                                    let pos = Position {
                                        id: Uuid::new_v4().to_string(),
                                        side: Side::Yes,
                                        entry_poly: book.yes.best_ask,
                                        entry_cb: cb_price,
                                        size: 0.0,
                                        timestamp_ms: now_ms,
                                    };
                                    new_entry = Some(pos);
                                } else if momentum <= -required_momentum
                                    && book.no.best_ask >= 0.03
                                    && book.no.best_ask <= 0.97
                                    && no_spread <= MAX_ENTRY_SPREAD
                                    && !has_no
                                {
                                    let pos = Position {
                                        id: Uuid::new_v4().to_string(),
                                        side: Side::No,
                                        entry_poly: book.no.best_ask,
                                        entry_cb: cb_price,
                                        size: 0.0,
                                        timestamp_ms: now_ms,
                                    };
                                    new_entry = Some(pos);
                                }
                            }
                        }

                        for (pos, exit_price, gross_return, hold_ms, reason) in closed {
                            let pnl = realized_pnl(pos.size, gross_return, cost_model);
                            let new_bankroll = settle_sim_position_for_strategy(&mut conn, "CEX_SNIPER", pos.size, pnl).await;
                            let execution_id = pos.id.clone();

                            let pnl_msg = serde_json::json!({
                                "execution_id": execution_id.clone(),
                                "strategy": "CEX_SNIPER",
                                "variant": variant.as_str(),
                                "pnl": pnl,
                                "notional": pos.size,
                                "timestamp": now_ms,
                                "bankroll": new_bankroll,
                                "mode": "PAPER",
                                    "details": {
                                        "action": "CLOSE_SNIPE",
                                        "reason": reason,
                                        "regime": regime_label,
                                        "side": match pos.side { Side::Yes => "YES", Side::No => "NO" },
                                        "entry": pos.entry_poly,
                                        "exit": exit_price,
                                    "entry_cb": pos.entry_cb,
                                    "exit_cb": cb_price,
                                    "hold_ms": hold_ms,
                                    "gross_return": gross_return,
                                    "net_return": if pos.size > 0.0 { pnl / pos.size } else { 0.0 },
                                    "roi": format!("{:.2}%", if pos.size > 0.0 { (pnl / pos.size) * 100.0 } else { 0.0 }),
                                    "round_trip_cost_rate": cost_model.round_trip_cost_rate(),
                                }
                            });
                            publish_event(&mut conn, "strategy:pnl", pnl_msg.to_string()).await;

                            let exec_msg = serde_json::json!({
                                "execution_id": execution_id,
                                "market": "CEX Sniper",
                                "side": "CLOSE",
                                "price": exit_price,
                                "size": pos.size,
                                "timestamp": now_ms,
                                "mode": "PAPER",
                                "details": {
                                    "reason": reason,
                                    "gross_return": gross_return,
                                    "net_return": if pos.size > 0.0 { pnl / pos.size } else { 0.0 },
                                    "hold_ms": hold_ms,
                                }
                            });
                            publish_event(&mut conn, "arbitrage:execution", exec_msg.to_string()).await;
                        }

                        if let Some(mut pos) = new_entry {
                            let risk_cfg = read_risk_config(&mut conn).await;
                            let available_cash = read_sim_available_cash(&mut conn).await;
                            let base_size = compute_strategy_bet_size(
                                &mut conn,
                                "CEX_SNIPER",
                                available_cash,
                                &risk_cfg,
                                10.0,
                                0.20,
                            ).await;
                            pos.size = (base_size * regime_size_multiplier)
                                .min(available_cash)
                                .max(0.0);
                            if pos.size <= 0.0 {
                                publish_heartbeat(&mut conn, "cex_arb").await;
                                continue;
                            }

                            if !reserve_sim_notional_for_strategy(&mut conn, "CEX_SNIPER", pos.size).await {
                                publish_heartbeat(&mut conn, "cex_arb").await;
                                continue;
                            }

                            {
                                let mut positions = positions_link.write().await;
                                if positions.len() < MAX_CONCURRENT_POSITIONS {
                                    positions.push(pos.clone());
                                } else {
                                    let _ = release_sim_notional_for_strategy(&mut conn, "CEX_SNIPER", pos.size).await;
                                    publish_heartbeat(&mut conn, "cex_arb").await;
                                    continue;
                                }
                            }

                            let direction = match pos.side { Side::Yes => "YES", Side::No => "NO" };
                            let execution_id = pos.id.clone();
                            let exec_msg = serde_json::json!({
                                "execution_id": execution_id,
                                "market": "CEX Sniper",
                                "side": "ENTRY",
                                "price": pos.entry_poly,
                                "size": pos.size,
                                "timestamp": now_ms,
                                "mode": "PAPER",
                                "details": {
                                    "direction": direction,
                                    "momentum": momentum,
                                    "adaptive_threshold": adaptive_threshold,
                                    "required_momentum": required_momentum,
                                    "regime": regime_label,
                                    "regime_size_multiplier": regime_size_multiplier,
                                    "position_id": pos.id,
                                }
                            });
                            publish_event(&mut conn, "arbitrage:execution", exec_msg.to_string()).await;
                        }

                        publish_heartbeat(&mut conn, "cex_arb").await;
                    }
                }

                if Utc::now().timestamp() > market_expiry {
                    break;
                }
            }
        }
    }
}
