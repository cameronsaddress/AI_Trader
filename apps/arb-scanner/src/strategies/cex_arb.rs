use async_trait::async_trait;
use chrono::Utc;
use futures::{SinkExt, StreamExt};
use log::{error, info};
use redis::AsyncCommands;
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
    apply_sim_pnl,
    build_scan_payload,
    compute_bet_size,
    is_strategy_enabled,
    publish_heartbeat,
    read_risk_config,
    read_sim_bankroll,
    read_simulation_reset_ts,
    strategy_variant,
    read_trading_mode,
    TradingMode,
};
use crate::strategies::market_data::{update_book_from_market_ws, BinaryBook};
use crate::strategies::simulation::{realized_pnl, SimCostModel};
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

fn parse_coinbase_ticker_price(payload: &str) -> Option<f64> {
    let parsed = serde_json::from_str::<Value>(payload).ok()?;

    // Legacy Coinbase Exchange feed shape.
    if parsed.get("type").and_then(|v| v.as_str()) == Some("ticker")
        && parsed.get("product_id").and_then(|v| v.as_str()) == Some(COINBASE_PRODUCT_ID)
    {
        return parse_number(parsed.get("price"));
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
                return Some(price);
            }

            let bid = parse_number(ticker.get("best_bid"));
            let ask = parse_number(ticker.get("best_ask"));
            if let (Some(best_bid), Some(best_ask)) = (bid, ask) {
                return Some((best_bid + best_ask) / 2.0);
            }
        }
    }

    None
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

        let mut conn = match redis_client.get_async_connection().await {
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
                        for sub_msg in &coinbase_subscriptions {
                            if let Err(e) = ws_stream.send(Message::Text(sub_msg.to_string())).await {
                                error!("Coinbase subscribe failed: {}", e);
                            }
                        }

                        while let Some(msg) = ws_stream.next().await {
                            match msg {
                                Ok(Message::Text(text)) => {
                                    if let Some(price) = parse_coinbase_ticker_price(&text) {
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
                        let adaptive_threshold = (MOMENTUM_ENTRY_THRESHOLD
                            .max(momentum_sigma * MOMENTUM_STD_MULTIPLIER))
                            .min(MOMENTUM_MAX_THRESHOLD);
                        let required_momentum = adaptive_threshold + cost_model.round_trip_cost_rate();
                        let passes_threshold = momentum.abs() >= required_momentum;
                        let direction = if momentum >= 0.0 { "YES" } else { "NO" };
                        let reason = if passes_threshold {
                            format!(
                                "Momentum {:.2}% exceeds adaptive+cost {:.2}% threshold ({})",
                                momentum * 100.0,
                                required_momentum * 100.0,
                                direction
                            )
                        } else {
                            format!(
                                "Momentum {:.2}% below adaptive+cost {:.2}% threshold",
                                momentum * 100.0,
                                required_momentum * 100.0
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
                                    "required_momentum": required_momentum,
                                    "round_trip_cost_rate": cost_model.round_trip_cost_rate(),
                                }),
                        );
                        let _: () = conn.publish("arbitrage:scan", scan_msg.to_string()).await.unwrap_or_default();

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
                                let bankroll = read_sim_bankroll(&mut conn).await;
                                let preview_size = compute_bet_size(bankroll, &risk_cfg, 10.0, 0.20);
                                if preview_size > 0.0 {
                                    let preview_price = if eligible_long { book.yes.best_ask } else { book.no.best_ask };
                                    if preview_price > 0.0 {
                                        let preview_msg = serde_json::json!({
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
                                        let _: () = conn.publish("arbitrage:execution", preview_msg.to_string()).await.unwrap_or_default();
                                        last_live_preview_ms = now_ms;
                                    }
                                }
                            }
                            let cleared = {
                                let mut positions = positions_link.write().await;
                                let count = positions.len();
                                if count > 0 {
                                    positions.clear();
                                }
                                count
                            };
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

                                if gross_return >= TAKE_PROFIT_PCT || gross_return <= STOP_LOSS_PCT || hold_ms >= MAX_HOLD_MS {
                                    let reason = if gross_return >= TAKE_PROFIT_PCT {
                                        "TAKE_PROFIT"
                                    } else if gross_return <= STOP_LOSS_PCT {
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
                                if time_to_expiry_ms < ENTRY_EXPIRY_CUTOFF_MS {
                                    // Skip new entries close to settlement to avoid execution and resolution risk.
                                } else if momentum >= required_momentum
                                    && book.yes.best_ask >= 0.03
                                    && book.yes.best_ask <= 0.97
                                    && yes_spread <= MAX_ENTRY_SPREAD
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
                            let new_bankroll = apply_sim_pnl(&mut conn, pnl).await;

                            let pnl_msg = serde_json::json!({
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
                            let _: () = conn.publish("strategy:pnl", pnl_msg.to_string()).await.unwrap_or_default();
                        }

                        if let Some(mut pos) = new_entry {
                            let risk_cfg = read_risk_config(&mut conn).await;
                            let bankroll = read_sim_bankroll(&mut conn).await;
                            pos.size = compute_bet_size(bankroll, &risk_cfg, 10.0, 0.20);
                            if pos.size <= 0.0 {
                                publish_heartbeat(&mut conn, "cex_arb").await;
                                continue;
                            }

                            {
                                let mut positions = positions_link.write().await;
                                if positions.len() < MAX_CONCURRENT_POSITIONS {
                                    positions.push(pos.clone());
                                } else {
                                    publish_heartbeat(&mut conn, "cex_arb").await;
                                    continue;
                                }
                            }

                            let direction = match pos.side { Side::Yes => "YES", Side::No => "NO" };
                            let exec_msg = serde_json::json!({
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
                                    "position_id": pos.id,
                                }
                            });
                            let _: () = conn.publish("arbitrage:execution", exec_msg.to_string()).await.unwrap_or_default();
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
