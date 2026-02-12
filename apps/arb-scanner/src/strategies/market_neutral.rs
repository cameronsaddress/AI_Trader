use async_trait::async_trait;
use chrono::{Timelike, Utc};
use futures::{SinkExt, StreamExt};
use log::{error, info};
use redis::AsyncCommands;
use serde_json::Value;
use statrs::distribution::{ContinuousCDF, Normal};
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{sleep, Duration};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use url::Url;

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
const RISK_FREE_RATE: f64 = 0.045;
const ASSUMED_VOLATILITY: f64 = 0.65;
const SPOT_HISTORY_WINDOW_MS: i64 = 20 * 60 * 1000;
const VOL_WINDOW_MS: i64 = 5 * 60 * 1000;
const VOL_MIN_SAMPLES: usize = 8;
const SPOT_STALE_MS: i64 = 2_500;
const BOOK_STALE_MS: i64 = 1_500;
const ENTRY_EDGE_THRESHOLD: f64 = 0.04;
const EXIT_EDGE_THRESHOLD: f64 = 0.01;
const MAX_ENTRY_EDGE_THRESHOLD: f64 = 0.10;
const TAKE_PROFIT_PCT: f64 = 0.08;
const STOP_LOSS_PCT: f64 = -0.05;
const ENTRY_EXPIRY_CUTOFF_SECS: i64 = 45;
const MAX_ENTRY_SPREAD: f64 = 0.08;
const LIVE_PREVIEW_COOLDOWN_MS: i64 = 2_000;
const MIN_ENTRY_PRICE: f64 = 0.08;
const MAX_ENTRY_PRICE: f64 = 0.92;
const MAX_PARITY_DEVIATION: f64 = 0.02;
const MIN_EDGE_TO_SPREAD_RATIO: f64 = 1.25;
const ENTRY_COOLDOWN_MS: i64 = 8_000;
const MIN_HOLD_MS: i64 = 1_500;
const MAX_TRADES_PER_WINDOW: usize = 3;
const MAX_CONSECUTIVE_LOSSES: u32 = 3;

fn coinbase_ws_url() -> String {
    std::env::var("COINBASE_WS_URL").unwrap_or_else(|_| COINBASE_ADVANCED_WS_URL.to_string())
}

fn coinbase_ticker_subscriptions(ws_url: &str, product_id: &str) -> Vec<Value> {
    if ws_url.contains("ws-feed.exchange.coinbase.com") {
        vec![serde_json::json!({
            "type": "subscribe",
            "product_ids": [product_id],
            "channels": ["ticker", "heartbeat"]
        })]
    } else {
        vec![
            serde_json::json!({
                "type": "subscribe",
                "channel": "ticker",
                "product_ids": [product_id]
            }),
            serde_json::json!({
                "type": "subscribe",
                "channel": "heartbeats",
                "product_ids": [product_id]
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

fn parse_coinbase_ticker_price(payload: &str, product_id: &str) -> Option<f64> {
    let parsed = serde_json::from_str::<Value>(payload).ok()?;

    // Legacy Coinbase Exchange feed shape.
    if parsed.get("type").and_then(|v| v.as_str()) == Some("ticker")
        && parsed.get("product_id").and_then(|v| v.as_str()) == Some(product_id)
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
            if ticker.get("product_id").and_then(|v| v.as_str()) != Some(product_id) {
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

#[derive(Debug, Clone)]
struct Position {
    entry_price: f64,
    size: f64,
    timestamp_ms: i64,
}

pub struct MarketNeutralStrategy {
    client: PolymarketClient,
    asset: String,
    latest_spot_price: Arc<RwLock<(f64, i64)>>,
    spot_history: Arc<RwLock<VecDeque<(i64, f64)>>>,
    open_position: Arc<RwLock<Option<Position>>>,
}

impl MarketNeutralStrategy {
    pub fn new(asset: String) -> Self {
        Self {
            client: PolymarketClient::new(),
            asset,
            latest_spot_price: Arc::new(RwLock::new((0.0, 0))),
            spot_history: Arc::new(RwLock::new(VecDeque::new())),
            open_position: Arc::new(RwLock::new(None)),
        }
    }

    fn strategy_id(&self) -> String {
        format!("{}_15M", self.asset)
    }

    fn heartbeat_id(&self) -> String {
        format!("{}_15m", self.asset.to_lowercase())
    }

    fn calculate_fair_value(&self, spot: f64, strike: f64, time_to_expiry_years: f64, sigma: f64) -> f64 {
        if time_to_expiry_years <= 0.0 {
            return if spot > strike { 1.0 } else { 0.0 };
        }

        let r = RISK_FREE_RATE;

        let d2 = ((spot / strike).ln() + (r - 0.5 * sigma.powi(2)) * time_to_expiry_years)
            / (sigma * time_to_expiry_years.sqrt());

        match Normal::new(0.0, 1.0) {
            Ok(normal) => (-r * time_to_expiry_years).exp() * normal.cdf(d2),
            Err(_) => 0.5,
        }
    }

    fn parse_window_start_ts(slug: &str) -> Option<i64> {
        slug
            .split("-15m-")
            .nth(1)
            .and_then(|s| s.parse::<i64>().ok())
    }

    fn current_window_slug(&self) -> String {
        let now = Utc::now();
        let minute = now.minute();
        let window_start_minute = minute - (minute % 15);
        let window_start = match now
            .with_minute(window_start_minute)
            .and_then(|x| x.with_second(0))
            .and_then(|x| x.with_nanosecond(0))
        {
            Some(ts) => ts,
            None => now,
        };

        format!("{}-updown-15m-{}", self.asset.to_lowercase(), window_start.timestamp())
    }

    fn spot_at_window_start(history: &VecDeque<(i64, f64)>, window_start_ms: i64) -> Option<f64> {
        if history.is_empty() {
            return None;
        }

        // Prefer a reading at/before window start to avoid look-ahead bias.
        if let Some((_, px)) = history
            .iter()
            .rev()
            .find(|(ts, px)| *ts <= window_start_ms && *px > 0.0)
        {
            return Some(*px);
        }

        // If strategy starts after window open, fall back to first observed price after start.
        history
            .iter()
            .find(|(ts, px)| *ts >= window_start_ms && *px > 0.0)
            .map(|(_, px)| *px)
    }

    fn estimate_annualized_vol(history: &VecDeque<(i64, f64)>) -> Option<f64> {
        if history.len() < VOL_MIN_SAMPLES {
            return None;
        }

        let latest_ts = history.back().map(|(ts, _)| *ts)?;
        let cutoff = latest_ts - VOL_WINDOW_MS;
        let points: Vec<(i64, f64)> = history
            .iter()
            .copied()
            .filter(|(ts, _)| *ts >= cutoff)
            .collect();
        let mut returns: Vec<f64> = Vec::with_capacity(history.len().saturating_sub(1));
        let mut dt_ms: Vec<f64> = Vec::with_capacity(history.len().saturating_sub(1));

        for window in points.windows(2) {
            let (t0, p0) = window[0];
            let (t1, p1) = window[1];
            if p0 <= 0.0 || p1 <= 0.0 || t1 <= t0 {
                continue;
            }
            returns.push((p1 / p0).ln());
            dt_ms.push((t1 - t0) as f64);
        }

        if returns.len() < VOL_MIN_SAMPLES || dt_ms.is_empty() {
            return None;
        }

        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let var = returns
            .iter()
            .map(|r| {
                let d = *r - mean;
                d * d
            })
            .sum::<f64>()
            / (returns.len() as f64 - 1.0);

        let std_step = var.max(0.0).sqrt();
        let avg_dt_ms = dt_ms.iter().sum::<f64>() / dt_ms.len() as f64;
        if avg_dt_ms <= 0.0 {
            return None;
        }

        let steps_per_year = 31_536_000_000.0 / avg_dt_ms;
        Some((std_step * steps_per_year.sqrt()).clamp(0.10, 2.00))
    }
}

#[async_trait]
impl Strategy for MarketNeutralStrategy {
    async fn run(&self, redis_client: redis::Client) {
        info!("Starting fair-value strategy for {}", self.asset);

        let mut conn = match redis_client.get_async_connection().await {
            Ok(c) => c,
            Err(e) => {
                error!("Redis connect failed: {}", e);
                return;
            }
        };
        let cost_model = SimCostModel::from_env();
        let variant = strategy_variant();
        let mut last_seen_reset_ts = 0_i64;

        let spot_writer = self.latest_spot_price.clone();
        let spot_history_writer = self.spot_history.clone();
        let asset_pair = format!("{}-USD", self.asset);
        let coinbase_ws_url = coinbase_ws_url();
        let coinbase_subscriptions = coinbase_ticker_subscriptions(&coinbase_ws_url, &asset_pair);

        tokio::spawn(async move {
            loop {
                match connect_async(coinbase_ws_url.as_str()).await {
                    Ok((mut ws_stream, _)) => {
                        info!(
                            "Coinbase WS connected for {} pricing via {}",
                            asset_pair,
                            coinbase_ws_url
                        );
                        for subscribe_msg in &coinbase_subscriptions {
                            if let Err(e) = ws_stream.send(Message::Text(subscribe_msg.to_string())).await {
                                error!("Coinbase subscribe failed: {}", e);
                            }
                        }

                        while let Some(msg) = ws_stream.next().await {
                            match msg {
                                Ok(Message::Text(text)) => {
                                    if let Some(price) = parse_coinbase_ticker_price(&text, &asset_pair) {
                                        let mut w = spot_writer.write().await;
                                        *w = (price, Utc::now().timestamp_millis());
                                        let now_ms = Utc::now().timestamp_millis();
                                        let mut history = spot_history_writer.write().await;
                                        history.push_back((now_ms, price));
                                        while let Some((ts, _)) = history.front() {
                                            if now_ms - *ts > SPOT_HISTORY_WINDOW_MS {
                                                history.pop_front();
                                            } else {
                                                break;
                                            }
                                        }
                                    }
                                }
                                Ok(Message::Ping(payload)) => {
                                    let _ = ws_stream.send(Message::Pong(payload)).await;
                                }
                                Ok(Message::Close(_)) => break,
                                Ok(Message::Binary(_)) | Ok(Message::Pong(_)) => continue,
                                Ok(_) => continue,
                                Err(e) => {
                                    error!("{} Coinbase ticker message error: {}", asset_pair, e);
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
                if let Some(m) = self.client.fetch_current_market(&self.asset).await {
                    break m;
                }
                sleep(Duration::from_secs(5)).await;
            };

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

            let mut interval = tokio::time::interval(Duration::from_millis(250));
            let window_start_ts = Self::parse_window_start_ts(&target_market.slug).unwrap_or_else(|| {
                let now_ts = Utc::now().timestamp();
                now_ts - now_ts.rem_euclid(900)
            });
            let expiry_ts = window_start_ts + 900;

            let mut book = BinaryBook::default();
            let mut last_live_preview_ms = 0_i64;
            let mut last_entry_ms = 0_i64;
            let mut trades_this_window = 0_usize;
            let mut consecutive_losses = 0_u32;

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
                                error!("{} fair-value Polymarket WS error: {}", self.asset, e);
                                break;
                            }
                        }
                    }
                    _ = interval.tick() => {
                        let strategy_id = self.strategy_id();
                        let heartbeat_id = self.heartbeat_id();

                        if !is_strategy_enabled(&mut conn, &strategy_id).await {
                            publish_heartbeat(&mut conn, &heartbeat_id).await;
                            continue;
                        }

                        if !book.yes.is_valid() || !book.no.is_valid() {
                            publish_heartbeat(&mut conn, &heartbeat_id).await;
                            continue;
                        }

                        let now_ms = Utc::now().timestamp_millis();
                        if now_ms - book.last_update_ms > BOOK_STALE_MS {
                            publish_heartbeat(&mut conn, &heartbeat_id).await;
                            continue;
                        }

                        let (spot, spot_ts_ms) = *self.latest_spot_price.read().await;
                        if spot <= 0.0 || now_ms - spot_ts_ms > SPOT_STALE_MS {
                            publish_heartbeat(&mut conn, &heartbeat_id).await;
                            continue;
                        }

                        let now_ts = Utc::now().timestamp();
                        let remaining_seconds = expiry_ts - now_ts;
                        if remaining_seconds <= 0 {
                            break;
                        }

                        let tte_years = remaining_seconds as f64 / 31_536_000.0;
                        let sigma = {
                            let history = self.spot_history.read().await;
                            Self::estimate_annualized_vol(&history).unwrap_or(ASSUMED_VOLATILITY)
                        };
                        let window_start_ms = window_start_ts * 1000;
                        let window_start_spot = {
                            let history = self.spot_history.read().await;
                            Self::spot_at_window_start(&history, window_start_ms).unwrap_or(spot)
                        };
                        let fair_yes = self.calculate_fair_value(spot, window_start_spot, tte_years, sigma);
                        let yes_mid = book.yes.mid();
                        let yes_no_mid_sum = book.yes.mid() + book.no.mid();
                        let yes_spread = (book.yes.best_ask - book.yes.best_bid).max(0.0);
                        let edge = fair_yes - book.yes.best_ask;
                        let adaptive_entry_edge = (ENTRY_EDGE_THRESHOLD
                            + ((sigma - ASSUMED_VOLATILITY).max(0.0) * 0.02))
                            .clamp(ENTRY_EDGE_THRESHOLD, MAX_ENTRY_EDGE_THRESHOLD);
                        let required_edge = adaptive_entry_edge + cost_model.round_trip_cost_rate();
                        let parity_deviation = (yes_no_mid_sum - 1.0).abs();
                        let edge_to_spread_ratio = if yes_spread > 0.0 {
                            edge / yes_spread
                        } else {
                            f64::INFINITY
                        };
                        let price_ok = book.yes.best_ask >= MIN_ENTRY_PRICE && book.yes.best_ask <= MAX_ENTRY_PRICE;
                        let parity_ok = parity_deviation <= MAX_PARITY_DEVIATION;
                        let spread_ok = yes_spread <= MAX_ENTRY_SPREAD;
                        let spread_efficiency_ok = yes_spread <= 0.0 || edge_to_spread_ratio >= MIN_EDGE_TO_SPREAD_RATIO;
                        let risk_budget_ok = trades_this_window < MAX_TRADES_PER_WINDOW
                            && consecutive_losses < MAX_CONSECUTIVE_LOSSES;
                        let structural_filters_ok = price_ok && parity_ok && spread_ok && spread_efficiency_ok && risk_budget_ok;
                        let passes_threshold = edge >= required_edge && structural_filters_ok;
                        let reason = if edge < required_edge {
                            format!(
                                "Fair value edge {:.2}c below entry+cost threshold {:.2}c",
                                edge * 100.0,
                                required_edge * 100.0
                            )
                        } else if !price_ok {
                            format!(
                                "YES ask {:.2}c outside entry band [{:.2}c, {:.2}c]",
                                book.yes.best_ask * 100.0,
                                MIN_ENTRY_PRICE * 100.0,
                                MAX_ENTRY_PRICE * 100.0
                            )
                        } else if !parity_ok {
                            format!(
                                "YES+NO mid sum {:.4} deviates from parity by {:.2}c (> {:.2}c)",
                                yes_no_mid_sum,
                                parity_deviation * 100.0,
                                MAX_PARITY_DEVIATION * 100.0
                            )
                        } else if !spread_ok {
                            format!(
                                "YES spread {:.2}c exceeds max {:.2}c",
                                yes_spread * 100.0,
                                MAX_ENTRY_SPREAD * 100.0
                            )
                        } else if !spread_efficiency_ok {
                            format!(
                                "Edge/spread ratio {:.2} below required {:.2}",
                                edge_to_spread_ratio,
                                MIN_EDGE_TO_SPREAD_RATIO
                            )
                        } else if !risk_budget_ok {
                            format!(
                                "Risk budget gate active (trades {}, consecutive losses {})",
                                trades_this_window,
                                consecutive_losses
                            )
                        } else {
                            format!(
                                "Fair value edge {:.2}c cleared all entry filters (threshold {:.2}c)",
                                edge * 100.0,
                                required_edge * 100.0
                            )
                        };

                        let scan_msg = build_scan_payload(
                            &target_market.market_id,
                            &format!("{} FV", self.asset),
                            &strategy_id,
                            "FAIR_VALUE_EDGE",
                            "PRICE",
                            [yes_mid, fair_yes],
                            yes_no_mid_sum,
                            "YES_NO_MID_SUM",
                            edge,
                            required_edge,
                            passes_threshold,
                            reason,
                            now_ms,
                            serde_json::json!({
                                "spot": spot,
                                "window_start_spot": window_start_spot,
                                "tte_years": tte_years,
                                "sigma_annualized": sigma,
                                "best_ask_yes": book.yes.best_ask,
                                "fair_yes": fair_yes,
                                "required_edge": required_edge,
                                "yes_spread": yes_spread,
                                "parity_deviation": parity_deviation,
                                "edge_to_spread_ratio": edge_to_spread_ratio,
                                "trades_this_window": trades_this_window,
                                "consecutive_losses": consecutive_losses,
                                "price_ok": price_ok,
                                "parity_ok": parity_ok,
                                "spread_ok": spread_ok,
                                "spread_efficiency_ok": spread_efficiency_ok,
                                "risk_budget_ok": risk_budget_ok,
                                "round_trip_cost_rate": cost_model.round_trip_cost_rate(),
                            }),
                        );
                        let _: () = conn.publish("arbitrage:scan", scan_msg.to_string()).await.unwrap_or_default();

                        let trading_mode = read_trading_mode(&mut conn).await;
                        if trading_mode == TradingMode::Live {
                            if passes_threshold
                                && book.yes.best_ask > 0.0
                                && yes_spread <= MAX_ENTRY_SPREAD
                                && remaining_seconds > ENTRY_EXPIRY_CUTOFF_SECS
                                && now_ms - last_entry_ms >= ENTRY_COOLDOWN_MS
                                && now_ms - last_live_preview_ms >= LIVE_PREVIEW_COOLDOWN_MS
                            {
                                let bankroll = read_sim_bankroll(&mut conn).await;
                                let risk_cfg = read_risk_config(&mut conn).await;
                                let size = compute_bet_size(bankroll, &risk_cfg, 10.0, 0.20);
                                if size > 0.0 {
                                    let preview_msg = serde_json::json!({
                                        "market": format!("Long {}", self.asset),
                                        "side": "LIVE_DRY_RUN",
                                        "price": book.yes.best_ask,
                                        "size": size,
                                        "timestamp": now_ms,
                                        "mode": "LIVE_DRY_RUN",
                                        "details": {
                                            "fair_value": fair_yes,
                                            "edge": edge,
                                            "adaptive_entry_edge": adaptive_entry_edge,
                                            "required_edge": required_edge,
                                            "spot": spot,
                                            "sigma_annualized": sigma,
                                            "yes_spread": yes_spread,
                                            "parity_deviation": parity_deviation,
                                            "edge_to_spread_ratio": edge_to_spread_ratio,
                                            "seconds_to_expiry": remaining_seconds,
                                            "preflight": {
                                                "venue": "POLYMARKET",
                                                "strategy": strategy_id,
                                                "orders": [
                                                    {
                                                        "token_id": target_market.yes_token,
                                                        "condition_id": target_market.market_id,
                                                        "side": "BUY",
                                                        "price": book.yes.best_ask,
                                                        "size": size,
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
                            let had_position = {
                                let mut pos_lock = self.open_position.write().await;
                                let existed = pos_lock.is_some();
                                if existed {
                                    *pos_lock = None;
                                }
                                existed
                            };
                            if had_position {
                                info!("{} strategy: clearing paper position in LIVE mode", strategy_id);
                            }
                            publish_heartbeat(&mut conn, &heartbeat_id).await;
                            continue;
                        }

                        let reset_ts = read_simulation_reset_ts(&mut conn).await;
                        if reset_ts > last_seen_reset_ts {
                            last_seen_reset_ts = reset_ts;
                            let mut pos_lock = self.open_position.write().await;
                            *pos_lock = None;
                            last_entry_ms = 0;
                            trades_this_window = 0;
                            consecutive_losses = 0;
                            publish_heartbeat(&mut conn, &heartbeat_id).await;
                            continue;
                        }

                        let mut close_event: Option<(Position, f64, f64, f64, i64, &'static str)> = None;
                        let mut entry_signal_price: Option<f64> = None;

                        {
                            let mut pos_lock = self.open_position.write().await;

                            if let Some(pos) = pos_lock.as_ref() {
                                let mark_price = book.yes.mid();
                                let executable_exit_price = book.yes.best_bid;
                                if mark_price > 0.0 && executable_exit_price > 0.0 {
                                    let mark_return = (mark_price - pos.entry_price) / pos.entry_price;
                                    let executable_return = (executable_exit_price - pos.entry_price) / pos.entry_price;
                                    let hold_ms = now_ms - pos.timestamp_ms;
                                    let close_reason = if mark_return >= TAKE_PROFIT_PCT {
                                        Some("TAKE_PROFIT")
                                    } else if hold_ms >= MIN_HOLD_MS && executable_return <= STOP_LOSS_PCT {
                                        Some("STOP_LOSS")
                                    } else if hold_ms >= MIN_HOLD_MS && edge < EXIT_EDGE_THRESHOLD {
                                        Some("EDGE_DECAY")
                                    } else if remaining_seconds <= 30 {
                                        Some("EXPIRY_EXIT")
                                    } else {
                                        None
                                    };

                                    if let Some(reason) = close_reason {
                                        close_event = Some((pos.clone(), executable_exit_price, mark_return, executable_return, hold_ms, reason));
                                        *pos_lock = None;
                                    }
                                }
                            }

                            if pos_lock.is_none()
                                && passes_threshold
                                && remaining_seconds > ENTRY_EXPIRY_CUTOFF_SECS
                                && now_ms - last_entry_ms >= ENTRY_COOLDOWN_MS
                            {
                                entry_signal_price = Some(book.yes.best_ask);
                            }
                        }

                        if let Some((pos, exit_price, mark_return, executable_return, hold_ms, reason)) = close_event {
                            let pnl = realized_pnl(pos.size, executable_return, cost_model);
                            let new_bankroll = apply_sim_pnl(&mut conn, pnl).await;
                            if pnl < 0.0 {
                                consecutive_losses = consecutive_losses.saturating_add(1);
                            } else {
                                consecutive_losses = 0;
                            }

                            let net_return = if pos.size > 0.0 { pnl / pos.size } else { 0.0 };
                            let pnl_msg = serde_json::json!({
                                "strategy": strategy_id,
                                "variant": variant.as_str(),
                                "pnl": pnl,
                                "notional": pos.size,
                                "timestamp": now_ms,
                                "bankroll": new_bankroll,
                                "mode": "PAPER",
                                "details": {
                                    "action": "CLOSE_FV",
                                    "reason": reason,
                                    "entry": pos.entry_price,
                                    "exit": exit_price,
                                    "hold_ms": hold_ms,
                                    "edge": edge,
                                    "mark_return": mark_return,
                                    "gross_return": executable_return,
                                    "net_return": net_return,
                                    "roi": format!("{:.2}%", net_return * 100.0),
                                    "consecutive_losses": consecutive_losses,
                                    "round_trip_cost_rate": cost_model.round_trip_cost_rate(),
                                }
                            });
                            let _: () = conn.publish("strategy:pnl", pnl_msg.to_string()).await.unwrap_or_default();
                        }

                        if let Some(entry_price) = entry_signal_price {
                            let bankroll = read_sim_bankroll(&mut conn).await;
                            let risk_cfg = read_risk_config(&mut conn).await;
                            let size = compute_bet_size(bankroll, &risk_cfg, 10.0, 0.20);
                            if size <= 0.0 {
                                publish_heartbeat(&mut conn, &heartbeat_id).await;
                                continue;
                            }
                            let mut accepted = false;
                            {
                                let mut pos_lock = self.open_position.write().await;
                                if pos_lock.is_none() {
                                    *pos_lock = Some(Position {
                                        entry_price,
                                        size,
                                        timestamp_ms: now_ms,
                                    });
                                    accepted = true;
                                }
                            }

                            if accepted {
                                last_entry_ms = now_ms;
                                trades_this_window = trades_this_window.saturating_add(1);
                                let exec_msg = serde_json::json!({
                                    "market": format!("Long {}", self.asset),
                                    "side": "ENTRY",
                                    "price": entry_price,
                                    "size": size,
                                    "timestamp": now_ms,
                                    "mode": "PAPER",
                                    "details": {
                                        "fair_value": fair_yes,
                                        "edge": edge,
                                        "adaptive_entry_edge": adaptive_entry_edge,
                                        "spot": spot,
                                        "sigma_annualized": sigma,
                                        "parity_deviation": parity_deviation,
                                        "edge_to_spread_ratio": edge_to_spread_ratio,
                                        "trades_this_window": trades_this_window,
                                        "slug": self.current_window_slug(),
                                    }
                                });
                                let _: () = conn.publish("arbitrage:execution", exec_msg.to_string()).await.unwrap_or_default();
                            }
                        }

                        publish_heartbeat(&mut conn, &heartbeat_id).await;
                    }
                }
            }
        }
    }
}
