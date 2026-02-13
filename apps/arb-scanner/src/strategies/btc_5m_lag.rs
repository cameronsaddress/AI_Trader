use async_trait::async_trait;
use chrono::Utc;
use futures::{SinkExt, StreamExt};
use log::{error, info, warn};
use redis::AsyncCommands;
use serde_json::Value;
use statrs::distribution::{ContinuousCDF, Normal};
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{sleep, Duration};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use url::Url;

use crate::engine::{MarketTarget, PolymarketClient, WS_URL};
use crate::strategies::control::{
    build_scan_payload,
    compute_strategy_bet_size,
    is_strategy_enabled,
    publish_heartbeat,
    read_risk_config,
    read_sim_available_cash,
    read_simulation_reset_ts,
    read_trading_mode,
    reserve_sim_notional_for_strategy,
    release_sim_notional_for_strategy,
    settle_sim_position_for_strategy,
    strategy_variant,
    TradingMode,
};
use crate::strategies::market_data::{update_book_from_market_ws, BinaryBook};
use crate::strategies::simulation::SimCostModel;
use crate::strategies::Strategy;

const COINBASE_ADVANCED_WS_URL: &str = "wss://advanced-trade-ws.coinbase.com";
const COINBASE_PRODUCT_ID: &str = "BTC-USD";
const WINDOW_SECONDS: i64 = 300;
const SPOT_HISTORY_WINDOW_MS: i64 = 30 * 60 * 1000;
const VOL_WINDOW_MS: i64 = 5 * 60 * 1000;
const VOL_MIN_SAMPLES: usize = 12;
const SPOT_STALE_MS: i64 = 2_500;
const BOOK_STALE_MS: i64 = 1_500;
const ENTRY_EXPIRY_CUTOFF_SECS: i64 = 12;
const LIVE_PREVIEW_COOLDOWN_MS: i64 = 2_000;

// Avoid extreme prices that can explode ROI at settlement.
const MIN_ENTRY_PRICE: f64 = 0.15;
const MAX_ENTRY_PRICE: f64 = 0.85;
const MAX_PARITY_DEVIATION: f64 = 0.02;
const DEFAULT_MIN_EXPECTED_NET_RETURN: f64 = 0.006; // 60 bps expected net return
const DEFAULT_MAX_ENTRY_SPREAD: f64 = 0.08; // 8c wide markets are ignored
const DEFAULT_MAX_POSITION_FRACTION: f64 = 0.25;

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

fn parse_coinbase_message_sequence(payload: &str) -> Option<u64> {
    let parsed = serde_json::from_str::<Value>(payload).ok()?;
    parse_sequence(parsed.get("sequence_num")).or_else(|| parse_sequence(parsed.get("sequence")))
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

fn parse_window_start_ts(slug: &str) -> Option<i64> {
    slug.split("-5m-").nth(1).and_then(|s| s.parse::<i64>().ok())
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

    if points.len() < VOL_MIN_SAMPLES {
        return None;
    }

    let mut returns: Vec<f64> = Vec::with_capacity(points.len().saturating_sub(1));
    let mut dt_ms: Vec<f64> = Vec::with_capacity(points.len().saturating_sub(1));

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

    // 365 days * 24h * 60m * 60s * 1000ms
    let steps_per_year = 31_536_000_000.0 / avg_dt_ms;
    Some((std_step * steps_per_year.sqrt()).clamp(0.10, 2.00))
}

fn calculate_fair_yes(spot: f64, strike: f64, time_to_expiry_years: f64, sigma: f64) -> f64 {
    if time_to_expiry_years <= 0.0 {
        return if spot > strike { 1.0 } else { 0.0 };
    }

    let r = 0.045;
    let denom = sigma * time_to_expiry_years.sqrt();
    if denom <= 0.0 {
        return 0.5;
    }

    let d2 = ((spot / strike).ln() + (r - 0.5 * sigma.powi(2)) * time_to_expiry_years) / denom;
    match Normal::new(0.0, 1.0) {
        Ok(normal) => (-r * time_to_expiry_years).exp() * normal.cdf(d2),
        Err(_) => 0.5,
    }
}

#[derive(Debug, Clone, Copy)]
enum Side {
    Up,
    Down,
}

#[derive(Debug, Clone)]
struct Position {
    side: Side,
    entry_price: f64,
    notional_usd: f64,
    entry_ts_ms: i64,
    window_start_spot: f64,
    window_start_ts: i64,
    expiry_ts: i64,
    token_id: String,
    condition_id: String,
    fair_yes: f64,
    sigma: f64,
}

fn expected_roi(prob: f64, price: f64) -> f64 {
    if !(prob.is_finite() && price.is_finite()) || price <= 0.0 {
        return -1.0;
    }
    (prob / price) - 1.0
}

fn kelly_fraction(prob: f64, price: f64) -> f64 {
    if !(prob.is_finite() && price.is_finite()) || price <= 0.0 || price >= 1.0 {
        return 0.0;
    }
    ((prob - price) / (1.0 - price)).clamp(0.0, 1.0)
}

fn resolve_position_return(side: Side, entry_price: f64, window_start_spot: f64, end_spot: f64) -> (bool, f64) {
    let up = end_spot > window_start_spot;
    let won = match side {
        Side::Up => up,
        Side::Down => !up,
    };
    let gross_return = if won {
        ((1.0 / entry_price) - 1.0).min(10.0)
    } else {
        -1.0
    };
    (won, gross_return)
}

pub struct Btc5mLagStrategy {
    client: PolymarketClient,
    latest_spot_price: Arc<RwLock<(f64, i64)>>,
    spot_history: Arc<RwLock<VecDeque<(i64, f64)>>>,
}

impl Btc5mLagStrategy {
    pub fn new() -> Self {
        Self {
            client: PolymarketClient::new(),
            latest_spot_price: Arc::new(RwLock::new((0.0, 0))),
            spot_history: Arc::new(RwLock::new(VecDeque::new())),
        }
    }

    fn strategy_id(&self) -> &'static str {
        "BTC_5M"
    }

    fn heartbeat_id(&self) -> &'static str {
        "btc_5m"
    }

    async fn fetch_target_market(&self) -> MarketTarget {
        loop {
            if let Some(market) = self.client.fetch_current_market_window("BTC", WINDOW_SECONDS).await {
                return market;
            }
            sleep(Duration::from_millis(800)).await;
        }
    }

    fn expected_net_return_threshold() -> f64 {
        std::env::var("BTC_5M_MIN_EXPECTED_NET_RETURN")
            .ok()
            .and_then(|value| value.trim().parse::<f64>().ok())
            .filter(|value| value.is_finite() && *value >= 0.0 && *value <= 1.0)
            .unwrap_or(DEFAULT_MIN_EXPECTED_NET_RETURN)
    }

    fn max_entry_spread() -> f64 {
        std::env::var("BTC_5M_MAX_ENTRY_SPREAD")
            .ok()
            .and_then(|value| value.trim().parse::<f64>().ok())
            .filter(|value| value.is_finite() && *value > 0.0 && *value <= 0.30)
            .unwrap_or(DEFAULT_MAX_ENTRY_SPREAD)
    }

    fn max_position_fraction() -> f64 {
        std::env::var("BTC_5M_MAX_POSITION_FRACTION")
            .ok()
            .and_then(|value| value.trim().parse::<f64>().ok())
            .filter(|value| value.is_finite() && *value > 0.0 && *value <= 1.0)
            .unwrap_or(DEFAULT_MAX_POSITION_FRACTION)
    }
}

#[async_trait]
impl Strategy for Btc5mLagStrategy {
    async fn run(&self, redis_client: redis::Client) {
        info!("Starting BTC 5m lag engine [Coinbase vs Polymarket 5m odds]...");

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

        // Coinbase ticker feed
        let spot_writer = self.latest_spot_price.clone();
        let spot_history_writer = self.spot_history.clone();
        let coinbase_ws_url = coinbase_ws_url();
        let coinbase_subscriptions = coinbase_ticker_subscriptions(&coinbase_ws_url);

        tokio::spawn(async move {
            loop {
                match connect_async(coinbase_ws_url.as_str()).await {
                    Ok((mut ws_stream, _)) => {
                        info!("Coinbase WS connected for BTC pricing via {}", coinbase_ws_url);
                        let mut last_sequence: Option<u64> = None;
                        for subscribe_msg in &coinbase_subscriptions {
                            if let Err(e) = ws_stream.send(Message::Text(subscribe_msg.to_string())).await {
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

                                    if let Some(price) = parse_coinbase_ticker_price(&text) {
                                        let now_ms = Utc::now().timestamp_millis();
                                        {
                                            let mut writer = spot_writer.write().await;
                                            *writer = (price, now_ms);
                                        }
                                        {
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
                    Err(e) => error!("Coinbase WS connection failed: {}", e),
                }
                sleep(Duration::from_secs(1)).await;
            }
        });

        let mut open_position: Option<Position> = None;
        let mut last_live_preview_ms = 0_i64;

        loop {
            let target_market = self.fetch_target_market().await;
            let Some(window_start_ts) = parse_window_start_ts(&target_market.slug) else {
                warn!("BTC_5M: could not parse window start ts from slug {}", target_market.slug);
                sleep(Duration::from_millis(800)).await;
                continue;
            };
            let expiry_ts = window_start_ts + WINDOW_SECONDS;

            info!("BTC_5M scanning market: {}", target_market.slug);

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

            let mut interval = tokio::time::interval(Duration::from_millis(90));
            let mut book = BinaryBook::default();

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
                                error!("BTC_5M Polymarket WS error: {}", e);
                                break;
                            }
                        }
                    }
                    _ = interval.tick() => {
                        let now_ms = Utc::now().timestamp_millis();
                        let now_ts = Utc::now().timestamp();
                        let mode = read_trading_mode(&mut conn).await;

                        if now_ts >= expiry_ts {
                            if let Some(pos) = open_position.take() {
                                if mode == TradingMode::Live {
                                    let _ = release_sim_notional_for_strategy(&mut conn, self.strategy_id(), pos.notional_usd).await;
                                    publish_heartbeat(&mut conn, self.heartbeat_id()).await;
                                    break;
                                }

                                let (spot, spot_ts_ms) = *self.latest_spot_price.read().await;
                                let end_spot = if spot > 0.0 && now_ms - spot_ts_ms <= SPOT_STALE_MS {
                                    spot
                                } else {
                                    // Fall back to the latest history point after expiry (best effort).
                                    let history = self.spot_history.read().await;
                                    history
                                        .iter()
                                        .rev()
                                        .find(|(ts, px)| *ts >= pos.expiry_ts * 1000 && *px > 0.0)
                                        .map(|(_, px)| *px)
                                        .unwrap_or(spot)
                                };

                                if pos.window_start_spot > 0.0 && end_spot > 0.0 {
                                    let (won, gross_return) = resolve_position_return(pos.side, pos.entry_price, pos.window_start_spot, end_spot);
                                    let net_return = gross_return - cost_model.per_side_cost_rate();
                                    let pnl = pos.notional_usd * net_return;
                                    let new_bankroll = settle_sim_position_for_strategy(&mut conn, self.strategy_id(), pos.notional_usd, pnl).await;
                                    let ts_ms = Utc::now().timestamp_millis();
                                    let hold_ms = ts_ms - pos.entry_ts_ms;
                                    let side_label = match pos.side { Side::Up => "UP", Side::Down => "DOWN" };

                                    let pnl_msg = serde_json::json!({
                                        "strategy": self.strategy_id(),
                                        "variant": variant.as_str(),
                                        "pnl": pnl,
                                        "notional": pos.notional_usd,
                                        "timestamp": ts_ms,
                                        "bankroll": new_bankroll,
                                        "mode": "PAPER",
                                        "details": {
                                            "action": "RESOLVE_WINDOW",
                                            "side": side_label,
                                            "won": won,
                                            "entry_price": pos.entry_price,
                                            "window_start_spot": pos.window_start_spot,
                                            "end_spot": end_spot,
                                            "window_start_ts": pos.window_start_ts,
                                            "expiry_ts": pos.expiry_ts,
                                            "fair_yes": pos.fair_yes,
                                            "sigma_annualized": pos.sigma,
                                            "hold_ms": hold_ms,
                                            "gross_return": gross_return,
                                            "net_return": net_return,
                                            "roi": format!("{:.2}%", net_return * 100.0),
                                            "per_side_cost_rate": cost_model.per_side_cost_rate(),
                                            "token_id": pos.token_id,
                                            "condition_id": pos.condition_id,
                                            "slug": target_market.slug,
                                        }
                                    });
                                    let _: () = conn.publish("strategy:pnl", pnl_msg.to_string()).await.unwrap_or_default();

                                    let settle_msg = serde_json::json!({
                                        "market": "BTC 5m Engine",
                                        "side": if won { "WIN" } else { "LOSS" },
                                        "price": pos.entry_price,
                                        "size": pos.notional_usd,
                                        "timestamp": ts_ms,
                                        "mode": "PAPER",
                                        "details": {
                                            "strategy": self.strategy_id(),
                                            "position_side": side_label,
                                            "pnl": pnl,
                                            "net_return": net_return,
                                            "hold_ms": hold_ms,
                                            "window_start_spot": pos.window_start_spot,
                                            "end_spot": end_spot,
                                            "slug": target_market.slug,
                                        }
                                    });
                                    let _: () = conn.publish("arbitrage:execution", settle_msg.to_string()).await.unwrap_or_default();
                                }
                            }
                            break;
                        }

                        if !is_strategy_enabled(&mut conn, self.strategy_id()).await {
                            if let Some(pos) = open_position.take() {
                                let _ = release_sim_notional_for_strategy(&mut conn, self.strategy_id(), pos.notional_usd).await;
                            }
                            publish_heartbeat(&mut conn, self.heartbeat_id()).await;
                            continue;
                        }

                        if !book.yes.is_valid() || !book.no.is_valid() || now_ms - book.last_update_ms > BOOK_STALE_MS {
                            publish_heartbeat(&mut conn, self.heartbeat_id()).await;
                            continue;
                        }

                        let reset_ts = read_simulation_reset_ts(&mut conn).await;
                        if reset_ts > last_seen_reset_ts {
                            last_seen_reset_ts = reset_ts;
                            open_position = None;
                            last_live_preview_ms = 0;
                            publish_heartbeat(&mut conn, self.heartbeat_id()).await;
                            continue;
                        }

                        let (spot, spot_ts_ms) = *self.latest_spot_price.read().await;
                        if spot <= 0.0 || now_ms - spot_ts_ms > SPOT_STALE_MS {
                            publish_heartbeat(&mut conn, self.heartbeat_id()).await;
                            continue;
                        }

                        let remaining_seconds = expiry_ts - now_ts;
                        if remaining_seconds <= ENTRY_EXPIRY_CUTOFF_SECS {
                            publish_heartbeat(&mut conn, self.heartbeat_id()).await;
                            continue;
                        }

                        let window_start_ms = window_start_ts * 1000;
                        let window_start_spot = {
                            let history = self.spot_history.read().await;
                            spot_at_window_start(&history, window_start_ms).unwrap_or(spot)
                        };
                        if window_start_spot <= 0.0 {
                            publish_heartbeat(&mut conn, self.heartbeat_id()).await;
                            continue;
                        }

                        let tte_years = remaining_seconds as f64 / 31_536_000.0;
                        let sigma = {
                            let history = self.spot_history.read().await;
                            estimate_annualized_vol(&history).unwrap_or(0.65)
                        };
                        let fair_yes = calculate_fair_yes(spot, window_start_spot, tte_years, sigma).clamp(0.0, 1.0);
                        let fair_no = (1.0 - fair_yes).clamp(0.0, 1.0);

                        let yes_mid = book.yes.mid();
                        let no_mid = book.no.mid();
                        let yes_no_mid_sum = yes_mid + no_mid;
                        let parity_deviation = (yes_no_mid_sum - 1.0).abs();
                        let parity_ok = parity_deviation <= MAX_PARITY_DEVIATION;

                        let yes_spread = (book.yes.best_ask - book.yes.best_bid).max(0.0);
                        let no_spread = (book.no.best_ask - book.no.best_bid).max(0.0);
                        let max_spread = Self::max_entry_spread();

                        let up_price = book.yes.best_ask;
                        let down_price = book.no.best_ask;
                        let up_price_ok = (MIN_ENTRY_PRICE..=MAX_ENTRY_PRICE).contains(&up_price) && yes_spread <= max_spread;
                        let down_price_ok = (MIN_ENTRY_PRICE..=MAX_ENTRY_PRICE).contains(&down_price) && no_spread <= max_spread;

                        let up_expected = expected_roi(fair_yes, up_price);
                        let down_expected = expected_roi(fair_no, down_price);
                        let up_net_expected = up_expected - cost_model.per_side_cost_rate();
                        let down_net_expected = down_expected - cost_model.per_side_cost_rate();
                        let min_net_expected = Self::expected_net_return_threshold();

                        let (best_side, best_price, best_prob, best_expected, best_net_expected, best_token, best_spread) = if up_net_expected >= down_net_expected {
                            (Side::Up, up_price, fair_yes, up_expected, up_net_expected, target_market.yes_token.clone(), yes_spread)
                        } else {
                            (Side::Down, down_price, fair_no, down_expected, down_net_expected, target_market.no_token.clone(), no_spread)
                        };

                        let best_price_ok = match best_side {
                            Side::Up => up_price_ok,
                            Side::Down => down_price_ok,
                        };

                        let passes_threshold = parity_ok
                            && best_price_ok
                            && best_net_expected >= min_net_expected;

                        let best_side_label = match best_side { Side::Up => "UP", Side::Down => "DOWN" };
                        let reason = if !parity_ok {
                            format!(
                                "YES+NO mid sum {:.4} deviates from parity by {:.2}c (> {:.2}c)",
                                yes_no_mid_sum,
                                parity_deviation * 100.0,
                                MAX_PARITY_DEVIATION * 100.0,
                            )
                        } else if !best_price_ok {
                            format!(
                                "{} entry {:.2}c outside band [{:.2}c,{:.2}c] or spread {:.2}c too wide",
                                best_side_label,
                                best_price * 100.0,
                                MIN_ENTRY_PRICE * 100.0,
                                MAX_ENTRY_PRICE * 100.0,
                                best_spread * 100.0,
                            )
                        } else if best_net_expected < min_net_expected {
                            format!(
                                "{} expected net {:.1}bps below threshold {:.1}bps",
                                best_side_label,
                                best_net_expected * 10_000.0,
                                min_net_expected * 10_000.0,
                            )
                        } else {
                            format!(
                                "{} expected net {:.1}bps cleared threshold {:.1}bps",
                                best_side_label,
                                best_net_expected * 10_000.0,
                                min_net_expected * 10_000.0,
                            )
                        };

                        // Encode direction in the scan score sign so peer-consensus logic can reason about UP vs DOWN.
                        //
                        // IMPORTANT: `best_net_expected` can be negative (no edge). We still want the sign to represent
                        // the *direction we'd trade* (UP vs DOWN), so we sign the magnitude.
                        let direction_sign = match best_side {
                            Side::Up => 1.0,
                            Side::Down => -1.0,
                        };
                        let scan_score = direction_sign * best_net_expected.abs();
                        let scan_msg = build_scan_payload(
                            &target_market.market_id,
                            "BTC 5m Engine",
                            self.strategy_id(),
                            "WINDOW_EXPECTED_RETURN",
                            "RATIO",
                            [spot, best_price],
                            best_prob,
                            "FAIR_PROB",
                            scan_score,
                            min_net_expected,
                            passes_threshold,
                            reason,
                            now_ms,
                            serde_json::json!({
                                "spot": spot,
                                "window_start_spot": window_start_spot,
                                "fair_yes": fair_yes,
                                "fair_no": fair_no,
                                "sigma_annualized": sigma,
                                "tte_years": tte_years,
                                "yes_bid": book.yes.best_bid,
                                "yes_ask": book.yes.best_ask,
                                "no_bid": book.no.best_bid,
                                "no_ask": book.no.best_ask,
                                "parity_deviation": parity_deviation,
                                "yes_mid": yes_mid,
                                "no_mid": no_mid,
                                "yes_no_mid_sum": yes_no_mid_sum,
                                "best_side": best_side_label,
                                "expected_roi_up": up_expected,
                                "expected_roi_down": down_expected,
                                "expected_net_up": up_net_expected,
                                "expected_net_down": down_net_expected,
                                "best_price": best_price,
                                "best_prob": best_prob,
                                "best_expected_roi": best_expected,
                                "best_net_expected_roi": best_net_expected,
                                "kelly_fraction": kelly_fraction(best_prob, best_price),
                                "per_side_cost_rate": cost_model.per_side_cost_rate(),
                                "min_expected_net_return": min_net_expected,
                                "slug": target_market.slug,
                                "window_start_ts": window_start_ts,
                                "expiry_ts": expiry_ts,
                            }),
                        );
                        let _: () = conn.publish("arbitrage:scan", scan_msg.to_string()).await.unwrap_or_default();

                        if mode == TradingMode::Live {
                            if let Some(pos) = open_position.take() {
                                let _ = release_sim_notional_for_strategy(&mut conn, self.strategy_id(), pos.notional_usd).await;
                            }
                            if passes_threshold && now_ms - last_live_preview_ms >= LIVE_PREVIEW_COOLDOWN_MS {
                                let available_cash = read_sim_available_cash(&mut conn).await;
                                let risk_cfg = read_risk_config(&mut conn).await;
                                let size = compute_strategy_bet_size(
                                    &mut conn,
                                    self.strategy_id(),
                                    available_cash,
                                    &risk_cfg,
                                    10.0,
                                    Self::max_position_fraction(),
                                ).await;
                                if size > 0.0 {
                                    let side_label = match best_side { Side::Up => "UP", Side::Down => "DOWN" };
                                    let preview_msg = serde_json::json!({
                                        "market": "BTC 5m Engine",
                                        "side": format!("LIVE_DRY_RUN_{}", side_label),
                                        "price": best_price,
                                        "size": size,
                                        "timestamp": now_ms,
                                        "mode": "LIVE_DRY_RUN",
                                        "details": {
                                            "strategy": self.strategy_id(),
                                            "side": side_label,
                                            "spot": spot,
                                            "window_start_spot": window_start_spot,
                                            "fair_yes": fair_yes,
                                            "expected_net_return": best_net_expected,
                                            "kelly_fraction": kelly_fraction(best_prob, best_price),
                                            "sigma_annualized": sigma,
                                            "seconds_to_expiry": remaining_seconds,
                                            "preflight": {
                                                "venue": "POLYMARKET",
                                                "strategy": self.strategy_id(),
                                                "orders": [
                                                    {
                                                        "token_id": best_token,
                                                        "condition_id": target_market.market_id.clone(),
                                                        "side": "BUY",
                                                        "price": best_price,
                                                        "size": size,
                                                        "size_unit": "USD_NOTIONAL"
                                                    }
                                                ]
                                            }
                                        }
                                    });
                                    let _: () = conn.publish("arbitrage:execution", preview_msg.to_string()).await.unwrap_or_default();
                                    last_live_preview_ms = now_ms;
                                }
                            }
                            publish_heartbeat(&mut conn, self.heartbeat_id()).await;
                            continue;
                        }

                        // PAPER mode: enter at most one position per 5m window, hold to resolution.
                        if open_position.is_none() && passes_threshold {
                            let available_cash = read_sim_available_cash(&mut conn).await;
                            let risk_cfg = read_risk_config(&mut conn).await;
                            let size = compute_strategy_bet_size(
                                &mut conn,
                                self.strategy_id(),
                                available_cash,
                                &risk_cfg,
                                10.0,
                                Self::max_position_fraction(),
                            ).await;
                            if size > 0.0 && reserve_sim_notional_for_strategy(&mut conn, self.strategy_id(), size).await {
                                let side_label = match best_side { Side::Up => "UP", Side::Down => "DOWN" };
                                open_position = Some(Position {
                                    side: best_side,
                                    entry_price: best_price,
                                    notional_usd: size,
                                    entry_ts_ms: now_ms,
                                    window_start_spot,
                                    window_start_ts,
                                    expiry_ts,
                                    token_id: best_token.clone(),
                                    condition_id: target_market.market_id.clone(),
                                    fair_yes,
                                    sigma,
                                });

                                let exec_msg = serde_json::json!({
                                    "market": "BTC 5m Engine",
                                    "side": "ENTRY",
                                    "price": best_price,
                                    "size": size,
                                    "timestamp": now_ms,
                                    "mode": "PAPER",
                                    "details": {
                                        "strategy": self.strategy_id(),
                                        "side": side_label,
                                        "spot": spot,
                                        "window_start_spot": window_start_spot,
                                        "fair_yes": fair_yes,
                                        "expected_net_return": best_net_expected,
                                        "kelly_fraction": kelly_fraction(best_prob, best_price),
                                        "sigma_annualized": sigma,
                                        "seconds_to_expiry": remaining_seconds,
                                        "slug": target_market.slug,
                                    }
                                });
                                let _: () = conn.publish("arbitrage:execution", exec_msg.to_string()).await.unwrap_or_default();
                            }
                        }

                        publish_heartbeat(&mut conn, self.heartbeat_id()).await;
                    }
                }
            }
        }
    }
}
