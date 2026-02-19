use async_trait::async_trait;
use chrono::Utc;
use futures::{SinkExt, StreamExt};
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use statrs::distribution::{ContinuousCDF, Normal};
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
    clear_strategy_open_positions,
    compute_strategy_bet_size,
    entered_live_mode,
    is_strategy_enabled,
    persist_strategy_open_positions,
    publish_heartbeat,
    publish_event,
    publish_execution_event,
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
    restore_strategy_open_positions,
};
use crate::strategies::implied_vol::{blend_sigma, spawn_deribit_iv_feed, ImpliedVolSnapshot};
use crate::strategies::market_data::{update_book_from_market_ws, BinaryBook};
use crate::strategies::simulation::{realized_pnl, SimCostModel};
use crate::strategies::vol_regime::{detect_regime, regime_exit_multipliers};
use crate::strategies::Strategy;

const COINBASE_ADVANCED_WS_URL: &str = "wss://advanced-trade-ws.coinbase.com";
const RISK_FREE_RATE: f64 = 0.045;
const ASSUMED_VOLATILITY: f64 = 0.65;
const SPOT_HISTORY_WINDOW_MS: i64 = 20 * 60 * 1000;
const VOL_WINDOW_MS: i64 = 5 * 60 * 1000;
const VOL_MIN_SAMPLES: usize = 8;
const SPOT_STALE_MS: i64 = 2_500;
const BOOK_STALE_MS: i64 = 1_500;
const DEFAULT_ENTRY_EDGE_THRESHOLD: f64 = 0.04;
const DEFAULT_EXIT_EDGE_THRESHOLD: f64 = 0.01;
const DEFAULT_MAX_ENTRY_EDGE_THRESHOLD: f64 = 0.10;
const DEFAULT_TAKE_PROFIT_PCT: f64 = 0.08;
const DEFAULT_STOP_LOSS_PCT: f64 = -0.05;
const ENTRY_EXPIRY_CUTOFF_SECS: i64 = 45;
const DEFAULT_MAX_ENTRY_SPREAD: f64 = 0.08;
const LIVE_PREVIEW_COOLDOWN_MS: i64 = 2_000;
const MIN_ENTRY_PRICE: f64 = 0.08;
const MAX_ENTRY_PRICE: f64 = 0.92;
const MAX_PARITY_DEVIATION: f64 = 0.02;
const DEFAULT_MIN_EDGE_TO_SPREAD_RATIO: f64 = 1.25;
const ENTRY_COOLDOWN_MS: i64 = 8_000;
const MIN_HOLD_MS: i64 = 1_500;
const MAX_HOLD_MS: i64 = 300_000;
const DEFAULT_MAX_TRADES_PER_WINDOW: usize = 3;
const DEFAULT_MAX_CONSECUTIVE_LOSSES: u32 = 3;
const DEFAULT_MAX_POSITION_FRACTION: f64 = 0.20;
const RESOLUTION_GRACE_SECS: i64 = 180;
const RESOLUTION_CHECK_COOLDOWN_MS: i64 = 2_000;

fn env_i64(name: &str, fallback: i64, min: i64, max: i64) -> i64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .unwrap_or(fallback)
        .clamp(min, max)
}

fn env_f64(name: &str, fallback: f64, min: f64, max: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<f64>().ok())
        .unwrap_or(fallback)
        .clamp(min, max)
}

#[derive(Debug, Clone, Copy)]
struct StrategyParams {
    entry_edge_threshold: f64,
    max_entry_edge_threshold: f64,
    exit_edge_threshold: f64,
    take_profit_pct: f64,
    stop_loss_pct: f64,
    max_entry_spread: f64,
    min_edge_to_spread_ratio: f64,
    max_trades_per_window: usize,
    max_consecutive_losses: u32,
    max_position_fraction: f64,
}

impl StrategyParams {
    fn for_asset(asset: &str) -> Self {
        match asset {
            // BTC: tighter filters to reduce low-edge churn (was 130 trades/day for ~$0 PnL).
            "BTC" => Self {
                entry_edge_threshold: 0.06,
                max_entry_edge_threshold: 0.095,
                exit_edge_threshold: 0.015,
                take_profit_pct: 0.08,
                stop_loss_pct: -0.04,
                max_entry_spread: 0.06,
                min_edge_to_spread_ratio: 1.60,
                max_trades_per_window: 1,
                max_consecutive_losses: 1,
                max_position_fraction: 0.16,
            },
            // SOL showed negative expectancy overnight; keep sizing and turnover conservative.
            "SOL" => Self {
                entry_edge_threshold: 0.055,
                max_entry_edge_threshold: 0.10,
                exit_edge_threshold: 0.012,
                take_profit_pct: 0.070,
                stop_loss_pct: -0.030,
                max_entry_spread: 0.06,
                min_edge_to_spread_ratio: 1.60,
                max_trades_per_window: 1,
                max_consecutive_losses: 1,
                max_position_fraction: 0.10,
            },
            _ => Self {
                entry_edge_threshold: DEFAULT_ENTRY_EDGE_THRESHOLD,
                max_entry_edge_threshold: DEFAULT_MAX_ENTRY_EDGE_THRESHOLD,
                exit_edge_threshold: DEFAULT_EXIT_EDGE_THRESHOLD,
                take_profit_pct: DEFAULT_TAKE_PROFIT_PCT,
                stop_loss_pct: DEFAULT_STOP_LOSS_PCT,
                max_entry_spread: DEFAULT_MAX_ENTRY_SPREAD,
                min_edge_to_spread_ratio: DEFAULT_MIN_EDGE_TO_SPREAD_RATIO,
                max_trades_per_window: DEFAULT_MAX_TRADES_PER_WINDOW,
                max_consecutive_losses: DEFAULT_MAX_CONSECUTIVE_LOSSES,
                max_position_fraction: DEFAULT_MAX_POSITION_FRACTION,
            },
        }
    }
}

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

fn parse_sequence(value: Option<&Value>) -> Option<u64> {
    value.and_then(|v| {
        v.as_u64()
            .or_else(|| v.as_i64().and_then(|s| if s >= 0 { Some(s as u64) } else { None }))
            .or_else(|| v.as_str().and_then(|s| s.parse::<u64>().ok()))
    })
}

fn parse_coinbase_ticker_price(payload: &str, product_id: &str) -> Option<(f64, Option<u64>)> {
    let parsed = serde_json::from_str::<Value>(payload).ok()?;
    let root_sequence = parse_sequence(parsed.get("sequence_num"))
        .or_else(|| parse_sequence(parsed.get("sequence")));

    // Legacy Coinbase Exchange feed shape.
    if parsed.get("type").and_then(|v| v.as_str()) == Some("ticker")
        && parsed.get("product_id").and_then(|v| v.as_str()) == Some(product_id)
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
            if ticker.get("product_id").and_then(|v| v.as_str()) != Some(product_id) {
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

fn is_transient_ws_error_message(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("connection reset")
        || lower.contains("without closing handshake")
        || lower.contains("broken pipe")
        || lower.contains("connection closed")
        || lower.contains("io error")
        || lower.contains("timed out")
}

fn is_transient_ws_error(error: &tokio_tungstenite::tungstenite::Error) -> bool {
    if matches!(
        error,
        tokio_tungstenite::tungstenite::Error::ConnectionClosed
            | tokio_tungstenite::tungstenite::Error::AlreadyClosed
    ) {
        return true;
    }
    is_transient_ws_error_message(&error.to_string())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Position {
    execution_id: String,
    market_id: String,
    yes_token: String,
    entry_price: f64,
    size: f64,
    timestamp_ms: i64,
}

pub struct MarketNeutralStrategy {
    client: PolymarketClient,
    asset: String,
    horizon_seconds: i64,
    latest_spot_price: Arc<RwLock<(f64, i64)>>,
    spot_history: Arc<RwLock<VecDeque<(i64, f64)>>>,
    implied_vol: Arc<RwLock<Option<ImpliedVolSnapshot>>>,
    open_position: Arc<RwLock<Option<Position>>>,
}

impl MarketNeutralStrategy {
    pub fn new(asset: String) -> Self {
        Self::new_with_window(asset, 900)
    }

    pub fn new_with_window(asset: String, horizon_seconds: i64) -> Self {
        let window = horizon_seconds.clamp(60, 24 * 60 * 60);
        Self {
            client: PolymarketClient::new(),
            asset,
            horizon_seconds: window,
            latest_spot_price: Arc::new(RwLock::new((0.0, 0))),
            spot_history: Arc::new(RwLock::new(VecDeque::new())),
            implied_vol: Arc::new(RwLock::new(None)),
            open_position: Arc::new(RwLock::new(None)),
        }
    }

    fn window_seconds(&self) -> i64 {
        self.horizon_seconds.clamp(60, 24 * 60 * 60)
    }

    fn window_minutes(&self) -> i64 {
        (self.window_seconds() / 60).max(1)
    }

    fn strategy_id(&self) -> String {
        format!("{}_{}M", self.asset, self.window_minutes())
    }

    fn heartbeat_id(&self) -> String {
        format!("{}_{}m", self.asset.to_lowercase(), self.window_minutes())
    }

    fn entry_expiry_cutoff_secs(&self) -> i64 {
        let default_cutoff = if self.window_seconds() <= 300 { 20 } else { ENTRY_EXPIRY_CUTOFF_SECS };
        let env_key = format!("{}_{}M_ENTRY_EXPIRY_CUTOFF_SECS", self.asset, self.window_minutes());
        std::env::var(&env_key)
            .ok()
            .and_then(|raw| raw.trim().parse::<i64>().ok())
            .unwrap_or(default_cutoff)
            .clamp(5, self.window_seconds().saturating_sub(5))
    }

    fn env_prefix(&self) -> String {
        format!("{}_{}M", self.asset.to_uppercase(), self.window_minutes())
    }

    fn live_preview_cooldown_ms(&self) -> i64 {
        let key = format!("{}_LIVE_PREVIEW_COOLDOWN_MS", self.env_prefix());
        env_i64(&key, LIVE_PREVIEW_COOLDOWN_MS, 200, 30_000)
    }

    fn max_spot_age_ms(&self) -> i64 {
        let key = format!("{}_MAX_SPOT_AGE_MS", self.env_prefix());
        let fallback = if self.window_seconds() <= 300 { 1_500 } else { SPOT_STALE_MS };
        env_i64(&key, fallback, 300, 30_000)
    }

    fn max_book_age_ms(&self) -> i64 {
        let key = format!("{}_MAX_BOOK_AGE_MS", self.env_prefix());
        let fallback = if self.window_seconds() <= 300 { 1_000 } else { BOOK_STALE_MS };
        env_i64(&key, fallback, 300, 30_000)
    }

    fn iv_blend_weight(&self) -> f64 {
        let key = format!("{}_IV_BLEND_WEIGHT", self.env_prefix());
        env_f64(&key, 0.60, 0.0, 1.0)
    }

    fn iv_max_stale_ms(&self) -> i64 {
        let key = format!("{}_IV_MAX_STALE_MS", self.env_prefix());
        env_i64(&key, 120_000, 1_000, 10 * 60_000)
    }

    fn decision_interval_ms(&self) -> u64 {
        let key = format!("{}_DECISION_INTERVAL_MS", self.env_prefix());
        let fallback = if self.window_seconds() <= 300 { 150 } else { 250 };
        env_i64(&key, fallback, 50, 2_000) as u64
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
        slug.rsplit('-').next().and_then(|s| s.parse::<i64>().ok())
    }

    fn current_window_slug(&self) -> String {
        let window = self.window_seconds();
        let now_ts = Utc::now().timestamp();
        let window_start = now_ts - now_ts.rem_euclid(window);
        format!("{}-updown-{}m-{}", self.asset.to_lowercase(), self.window_minutes(), window_start)
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

    fn valid_persisted_position(position: &Position) -> bool {
        !position.execution_id.trim().is_empty()
            && !position.market_id.trim().is_empty()
            && !position.yes_token.trim().is_empty()
            && position.entry_price.is_finite()
            && position.entry_price > 0.0
            && position.entry_price < 1.0
            && position.size.is_finite()
            && position.size > 0.0
            && position.timestamp_ms > 0
    }
}

#[async_trait]
impl Strategy for MarketNeutralStrategy {
    async fn run(&self, redis_client: redis::Client) {
        info!("Starting fair-value strategy for {}", self.asset);

        let mut conn = match redis_client.get_multiplexed_async_connection().await {
            Ok(c) => c,
            Err(e) => {
                error!("Redis connect failed: {}", e);
                return;
            }
        };
        let cost_model = SimCostModel::from_env();
        let strategy_id = self.strategy_id();
        let heartbeat_id = self.heartbeat_id();
        let variant = strategy_variant();
        let params = StrategyParams::for_asset(&self.asset);
        let mut last_seen_reset_ts = 0_i64;
        let spot_max_age_ms = self.max_spot_age_ms();
        let book_max_age_ms = self.max_book_age_ms();
        let live_preview_cooldown_ms = self.live_preview_cooldown_ms();
        let sigma_iv_weight = self.iv_blend_weight();
        let sigma_iv_max_stale_ms = self.iv_max_stale_ms();
        let decision_interval_ms = self.decision_interval_ms();

        if let Some(restored) = restore_strategy_open_positions::<Position>(&mut conn, &strategy_id).await {
            if Self::valid_persisted_position(&restored) {
                {
                    let mut pos_lock = self.open_position.write().await;
                    *pos_lock = Some(restored.clone());
                }
                info!(
                    "{} restored persisted position {} (entry {:.4}, notional {:.2})",
                    strategy_id,
                    restored.execution_id,
                    restored.entry_price,
                    restored.size
                );
            } else {
                clear_strategy_open_positions(&mut conn, &strategy_id).await;
                warn!(
                    "{} dropped invalid persisted open position snapshot",
                    strategy_id
                );
            }
        }

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
                                                    "Coinbase ticker sequence gap detected for {} (expected {}, got {}); reconnecting",
                                                    asset_pair,
                                                    expected,
                                                    seq
                                                );
                                                break;
                                            }
                                        }
                                        last_sequence = Some(seq);
                                    }

                                    if let Some((price, _sequence)) = parse_coinbase_ticker_price(&text, &asset_pair) {
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

        match self.asset.to_uppercase().as_str() {
            "BTC" => {
                spawn_deribit_iv_feed("BTC", self.latest_spot_price.clone(), self.implied_vol.clone());
            }
            "ETH" => {
                spawn_deribit_iv_feed("ETH", self.latest_spot_price.clone(), self.implied_vol.clone());
            }
            _ => {}
        }

        loop {
            let target_market = loop {
                if let Some(m) = self.client.fetch_current_market_window(&self.asset, self.window_seconds()).await {
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
                    if is_transient_ws_error(&e) {
                        warn!("{} fair-value Polymarket WS connect transient failure: {}", self.asset, e);
                    } else {
                        error!("Polymarket WS connect failed: {}", e);
                    }
                    sleep(Duration::from_secs(3)).await;
                    continue;
                }
            };

            let sub_msg = serde_json::json!({
                "assets_ids": [target_market.yes_token, target_market.no_token],
                "type": "market"
            });
            let _ = poly_ws.send(Message::Text(sub_msg.to_string())).await;

            let mut interval = tokio::time::interval(Duration::from_millis(decision_interval_ms));
            let window_start_ts = Self::parse_window_start_ts(&target_market.slug).unwrap_or_else(|| {
                let now_ts = Utc::now().timestamp();
                now_ts - now_ts.rem_euclid(self.window_seconds())
            });
            let expiry_ts = window_start_ts + self.window_seconds();

            let mut book = BinaryBook::default();
            let mut last_live_preview_ms = 0_i64;
            let mut last_entry_ms = 0_i64;
            let mut last_resolution_lookup_ms = 0_i64;
            let mut trades_this_window = 0_usize;
            let mut consecutive_losses = 0_u32;
            let mut ws_disconnected = false;
            let reconnect_backoff_secs = 1_u64;
            let mut disconnect_reason = String::new();

            loop {
                tokio::select! {
                    msg = poly_ws.next() => {
                        match msg {
                            Some(Ok(Message::Text(text))) => {
                                update_book_from_market_ws(
                                    &text,
                                    &target_market.yes_token,
                                    &target_market.no_token,
                                    &mut book,
                                );
                            }
                            Some(Ok(Message::Ping(payload))) => {
                                let _ = poly_ws.send(Message::Pong(payload)).await;
                            }
                            Some(Ok(Message::Close(frame))) => {
                                ws_disconnected = true;
                                disconnect_reason = frame
                                    .as_ref()
                                    .map(|close| {
                                        let reason = close.reason.trim();
                                        if reason.is_empty() {
                                            format!("code={:?}", close.code)
                                        } else {
                                            format!("code={:?}, reason={}", close.code, reason)
                                        }
                                    })
                                    .unwrap_or_else(|| "no close frame".to_string());
                                warn!(
                                    "{} fair-value Polymarket WS closed; reconnecting ({})",
                                    self.asset, disconnect_reason
                                );
                                break;
                            }
                            Some(Ok(Message::Binary(_))) | Some(Ok(Message::Pong(_))) => {}
                            Some(Ok(_)) => {}
                            Some(Err(e)) => {
                                ws_disconnected = true;
                                disconnect_reason = e.to_string();
                                if is_transient_ws_error(&e) {
                                    info!(
                                        "{} fair-value Polymarket WS transient error; reconnecting ({})",
                                        self.asset, disconnect_reason
                                    );
                                } else {
                                    error!("{} fair-value Polymarket WS error: {}", self.asset, e);
                                }
                                break;
                            }
                            None => {
                                ws_disconnected = true;
                                disconnect_reason = "stream ended".to_string();
                                warn!(
                                    "{} fair-value Polymarket WS stream ended; reconnecting",
                                    self.asset
                                );
                                break;
                            }
                        }
                    }
                    _ = interval.tick() => {
                        let entry_expiry_cutoff_secs = self.entry_expiry_cutoff_secs();

                        if !is_strategy_enabled(&mut conn, &strategy_id).await {
                            let released_notional = {
                                let mut pos_lock = self.open_position.write().await;
                                pos_lock.take().map(|pos| pos.size).unwrap_or(0.0)
                            };
                            clear_strategy_open_positions(&mut conn, &strategy_id).await;
                            if released_notional > 0.0 {
                                let _ = release_sim_notional_for_strategy(&mut conn, &strategy_id, released_notional).await;
                            }
                            publish_heartbeat(&mut conn, &heartbeat_id).await;
                            continue;
                        }

                        if !book.yes.is_valid() || !book.no.is_valid() {
                            publish_heartbeat(&mut conn, &heartbeat_id).await;
                            continue;
                        }

                        let now_ms = Utc::now().timestamp_millis();

                        // Force-close if book stale beyond MAX_HOLD to free capital
                        if now_ms - book.last_update_ms > book_max_age_ms {
                            let mut pos_lock = self.open_position.write().await;
                            if let Some(pos) = pos_lock.as_ref() {
                                let hold_ms = now_ms - pos.timestamp_ms;
                                if hold_ms >= MAX_HOLD_MS {
                                    let pos_owned = pos.clone();
                                    *pos_lock = None;
                                    drop(pos_lock);
                                    clear_strategy_open_positions(&mut conn, &strategy_id).await;
                                    let _ = settle_sim_position_for_strategy(&mut conn, &strategy_id, pos_owned.size, 0.0).await;
                                    let stale_msg = serde_json::json!({
                                        "execution_id": pos_owned.execution_id,
                                        "strategy": strategy_id,
                                        "pnl": 0.0,
                                        "notional": pos_owned.size,
                                        "timestamp": now_ms,
                                        "mode": "PAPER",
                                        "details": {
                                            "action": "STALE_FORCE_CLOSE",
                                            "reason": "STALE_FORCE_CLOSE",
                                            "entry": pos_owned.entry_price,
                                            "hold_ms": hold_ms,
                                        }
                                    });
                                    publish_event(&mut conn, "strategy:pnl", stale_msg.to_string()).await;
                                    info!("{} STALE_FORCE_CLOSE after {}ms", strategy_id, hold_ms);
                                }
                            }
                            publish_heartbeat(&mut conn, &heartbeat_id).await;
                            continue;
                        }

                        // Risk guard cooldown — skip entry if backend set a post-loss cooldown.
                        let cooldown_until = read_risk_guard_cooldown(&mut conn, &strategy_id).await;
                        if cooldown_until > 0 && now_ms < cooldown_until {
                            publish_heartbeat(&mut conn, &heartbeat_id).await;
                            continue;
                        }

                        let (spot, spot_ts_ms) = *self.latest_spot_price.read().await;
                        if spot <= 0.0 || now_ms - spot_ts_ms > spot_max_age_ms {
                            publish_heartbeat(&mut conn, &heartbeat_id).await;
                            continue;
                        }
                        let spot_age_ms = now_ms.saturating_sub(spot_ts_ms);
                        let book_age_ms = now_ms.saturating_sub(book.last_update_ms);

                        // Safety guard: never carry a position across market rollovers.
                        let rollover_position = {
                            let mut pos_lock = self.open_position.write().await;
                            if let Some(pos) = pos_lock.as_ref() {
                                if pos.market_id != target_market.market_id {
                                    let owned = pos.clone();
                                    *pos_lock = None;
                                    Some(owned)
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        };
                        if let Some(pos) = rollover_position {
                            clear_strategy_open_positions(&mut conn, &strategy_id).await;
                            let new_bankroll = settle_sim_position_for_strategy(&mut conn, &strategy_id, pos.size, 0.0).await;
                            let pnl_msg = serde_json::json!({
                                "execution_id": pos.execution_id,
                                "strategy": strategy_id.clone(),
                                "variant": variant.as_str(),
                                "pnl": 0.0,
                                "notional": pos.size,
                                "timestamp": now_ms,
                                "bankroll": new_bankroll,
                                "mode": "PAPER",
                                "details": {
                                    "action": "MARKET_ROLLOVER_CLOSE",
                                    "reason": "MARKET_ROLLOVER",
                                    "expected_market_id": pos.market_id,
                                    "active_market_id": target_market.market_id.clone(),
                                }
                            });
                            publish_event(&mut conn, "strategy:pnl", pnl_msg.to_string()).await;
                            warn!("{} MARKET_ROLLOVER close emitted to prevent cross-market state leakage", strategy_id);
                        }

                        let now_ts = Utc::now().timestamp();
                        let remaining_seconds = expiry_ts - now_ts;
                        if remaining_seconds <= 0 {
                            let maybe_pos = {
                                let mut pos_lock = self.open_position.write().await;
                                pos_lock.take()
                            };
                            if let Some(pos) = maybe_pos {
                                let resolved_price = if now_ms - last_resolution_lookup_ms >= RESOLUTION_CHECK_COOLDOWN_MS {
                                    last_resolution_lookup_ms = now_ms;
                                    self
                                        .client
                                        .fetch_resolved_outcome_price(&pos.market_id, &pos.yes_token, true)
                                        .await
                                } else {
                                    None
                                };
                                let (exit_price, exit_source) = if let Some(price) = resolved_price {
                                    (price.clamp(0.0, 1.0), "RESOLVED_OUTCOME")
                                } else if now_ts <= expiry_ts + RESOLUTION_GRACE_SECS {
                                    {
                                        let mut pos_lock = self.open_position.write().await;
                                        *pos_lock = Some(pos.clone());
                                    }
                                    persist_strategy_open_positions(&mut conn, &strategy_id, &pos).await;
                                    publish_heartbeat(&mut conn, &heartbeat_id).await;
                                    continue;
                                } else {
                                    let best_bid = book.yes.best_bid;
                                    let mid = book.yes.mid();
                                    if best_bid.is_finite() && best_bid >= 0.0 {
                                        (best_bid.clamp(0.0, 1.0), "BOOK_BID_AFTER_GRACE")
                                    } else if mid.is_finite() && mid >= 0.0 {
                                        (mid.clamp(0.0, 1.0), "BOOK_MID_AFTER_GRACE")
                                    } else {
                                        (pos.entry_price.clamp(0.0, 1.0), "ENTRY_FALLBACK_AFTER_GRACE")
                                    }
                                };

                                clear_strategy_open_positions(&mut conn, &strategy_id).await;
                                let gross_return = ((exit_price - pos.entry_price) / pos.entry_price).max(-0.999);
                                let pnl = realized_pnl(pos.size, gross_return, cost_model);
                                let new_bankroll = settle_sim_position_for_strategy(&mut conn, &strategy_id, pos.size, pnl).await;
                                if pnl < 0.0 {
                                    consecutive_losses = consecutive_losses.saturating_add(1);
                                } else {
                                    consecutive_losses = 0;
                                }
                                let net_return = if pos.size > 0.0 { pnl / pos.size } else { 0.0 };
                                let execution_id = pos.execution_id.clone();

                                let pnl_msg = serde_json::json!({
                                    "execution_id": execution_id.clone(),
                                    "strategy": strategy_id.clone(),
                                    "variant": variant.as_str(),
                                    "pnl": pnl,
                                    "notional": pos.size,
                                    "timestamp": now_ms,
                                    "bankroll": new_bankroll,
                                    "mode": "PAPER",
                                    "details": {
                                        "action": "FORCE_CLOSE_EXPIRY",
                                        "reason": "TIME_EXPIRY",
                                        "entry": pos.entry_price,
                                        "exit": exit_price,
                                        "exit_source": exit_source,
                                        "hold_ms": now_ms - pos.timestamp_ms,
                                        "gross_return": gross_return,
                                        "net_return": net_return,
                                        "roi": format!("{:.2}%", net_return * 100.0),
                                        "consecutive_losses": consecutive_losses,
                                        "round_trip_cost_rate": cost_model.round_trip_cost_rate(),
                                    }
                                });
                                publish_event(&mut conn, "strategy:pnl", pnl_msg.to_string()).await;

                                let settle_msg = serde_json::json!({
                                    "execution_id": execution_id,
                                    "market": format!("Long {}", self.asset),
                                    "side": "TIME_EXPIRY",
                                    "price": exit_price,
                                    "size": pos.size,
                                    "timestamp": now_ms,
                                    "mode": "PAPER",
                                    "details": {
                                        "strategy": strategy_id.clone(),
                                        "pnl": pnl,
                                        "net_return": net_return,
                                        "hold_ms": now_ms - pos.timestamp_ms,
                                        "exit_source": exit_source,
                                        "reason": "TIME_EXPIRY",
                                    }
                                });
                                publish_execution_event(&mut conn, settle_msg).await;
                                info!("{} TIME_EXPIRY close pnl=${:.2}", strategy_id, pnl);
                            }
                            break;
                        }

                        let tte_years = remaining_seconds as f64 / 31_536_000.0;
                        let realized_sigma = {
                            let history = self.spot_history.read().await;
                            Self::estimate_annualized_vol(&history)
                        };
                        let iv_snapshot = self.implied_vol.read().await.clone();
                        let iv_age_ms = iv_snapshot
                            .as_ref()
                            .map(|iv| now_ms.saturating_sub(iv.timestamp_ms))
                            .unwrap_or(-1);
                        let iv_is_fresh = iv_snapshot
                            .as_ref()
                            .map(|iv| now_ms.saturating_sub(iv.timestamp_ms) <= sigma_iv_max_stale_ms)
                            .unwrap_or(false);
                        let implied_sigma = if iv_is_fresh {
                            iv_snapshot.as_ref().map(|iv| iv.annualized_iv)
                        } else {
                            None
                        };
                        let sigma = blend_sigma(realized_sigma, implied_sigma, sigma_iv_weight, ASSUMED_VOLATILITY);
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
                        let adaptive_entry_edge = (params.entry_edge_threshold
                            + ((sigma - ASSUMED_VOLATILITY).max(0.0) * 0.02))
                            .clamp(params.entry_edge_threshold, params.max_entry_edge_threshold);
                        let divergence_uncertainty_bps = env_f64(
                            &format!("{}_DIVERGENCE_UNCERTAINTY_BPS", self.env_prefix()),
                            12.0,
                            0.0,
                            300.0,
                        );
                        let uncertainty_buffer = divergence_uncertainty_bps / 10_000.0;
                        let required_edge = adaptive_entry_edge + cost_model.round_trip_cost_rate() + uncertainty_buffer;
                        let divergence_floor = env_f64(
                            &format!("{}_DIVERGENCE_MIN_FLOOR", self.env_prefix()),
                            0.02,
                            0.0,
                            0.20,
                        );
                        let divergence_spread_mult = env_f64(
                            &format!("{}_DIVERGENCE_SPREAD_MULT", self.env_prefix()),
                            0.35,
                            0.0,
                            2.0,
                        );
                        let divergence_vol_mult = env_f64(
                            &format!("{}_DIVERGENCE_VOL_MULT", self.env_prefix()),
                            0.25,
                            0.0,
                            2.0,
                        );
                        let parity_deviation = (yes_no_mid_sum - 1.0).abs();
                        let edge_to_spread_ratio = if yes_spread > 0.0 {
                            edge / yes_spread
                        } else {
                            f64::INFINITY
                        };
                        let sigma_term = (sigma * tte_years.sqrt()).clamp(0.0, 1.0);
                        let required_divergence = (
                            divergence_floor
                                + uncertainty_buffer
                                + (divergence_spread_mult * yes_spread)
                                + (divergence_vol_mult * sigma_term)
                        )
                            .clamp(0.0, 0.25);
                        let divergence_ok = edge >= required_divergence;
                        let price_ok = book.yes.best_ask >= MIN_ENTRY_PRICE && book.yes.best_ask <= MAX_ENTRY_PRICE;
                        let parity_ok = parity_deviation <= MAX_PARITY_DEVIATION;
                        let spread_ok = yes_spread <= params.max_entry_spread;
                        let spread_efficiency_ok = yes_spread <= 0.0 || edge_to_spread_ratio >= params.min_edge_to_spread_ratio;
                        let risk_budget_ok = trades_this_window < params.max_trades_per_window
                            && consecutive_losses < params.max_consecutive_losses;
                        let freshness_ok = spot_age_ms <= spot_max_age_ms && book_age_ms <= book_max_age_ms;
                        let structural_filters_ok = freshness_ok && price_ok && parity_ok && spread_ok && spread_efficiency_ok && risk_budget_ok;
                        let passes_threshold = edge >= required_edge && divergence_ok && structural_filters_ok;
                        let reason = if !freshness_ok {
                            format!(
                                "Freshness gate blocked entry (spot {}ms>{}ms or book {}ms>{}ms)",
                                spot_age_ms, spot_max_age_ms, book_age_ms, book_max_age_ms
                            )
                        } else if edge < required_edge {
                            format!(
                                "Fair value edge {:.2}c below entry+cost threshold {:.2}c",
                                edge * 100.0,
                                required_edge * 100.0
                            )
                        } else if !divergence_ok {
                            format!(
                                "Fair/market divergence {:.1}bps below required {:.1}bps",
                                edge * 10_000.0,
                                required_divergence * 10_000.0
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
                                params.max_entry_spread * 100.0
                            )
                        } else if !spread_efficiency_ok {
                            format!(
                                "Edge/spread ratio {:.2} below required {:.2}",
                                edge_to_spread_ratio,
                                params.min_edge_to_spread_ratio
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
                                "spot_age_ms": spot_age_ms,
                                "book_age_ms": book_age_ms,
                                "window_start_spot": window_start_spot,
                                "tte_years": tte_years,
                                "sigma_annualized": sigma,
                                "sigma_realized_annualized": realized_sigma.unwrap_or(-1.0),
                                "sigma_implied_annualized": implied_sigma.unwrap_or(-1.0),
                                "sigma_iv_blend_weight": sigma_iv_weight,
                                "sigma_iv_fresh": iv_is_fresh,
                                "sigma_iv_age_ms": iv_age_ms,
                                "sigma_iv_sample_count": iv_snapshot.as_ref().map(|iv| iv.sample_count).unwrap_or(0),
                                "sigma_iv_source": iv_snapshot.as_ref().map(|iv| iv.source.clone()).unwrap_or_else(|| "NONE".to_string()),
                                "best_ask_yes": book.yes.best_ask,
                                "fair_yes": fair_yes,
                                "required_edge": required_edge,
                                "required_divergence": required_divergence,
                                "divergence_ok": divergence_ok,
                                "divergence_floor": divergence_floor,
                                "divergence_uncertainty_bps": divergence_uncertainty_bps,
                                "divergence_spread_component": divergence_spread_mult * yes_spread,
                                "divergence_vol_component": divergence_vol_mult * sigma_term,
                                "yes_spread": yes_spread,
                                "parity_deviation": parity_deviation,
                                "edge_to_spread_ratio": edge_to_spread_ratio,
                                "trades_this_window": trades_this_window,
                                "consecutive_losses": consecutive_losses,
                                "freshness_ok": freshness_ok,
                                "spot_age_max_ms": spot_max_age_ms,
                                "book_age_max_ms": book_max_age_ms,
                                "price_ok": price_ok,
                                "parity_ok": parity_ok,
                                "spread_ok": spread_ok,
                                "spread_efficiency_ok": spread_efficiency_ok,
                                "risk_budget_ok": risk_budget_ok,
                                "round_trip_cost_rate": cost_model.round_trip_cost_rate(),
                            }),
                        );
                        publish_event(&mut conn, "arbitrage:scan", scan_msg.to_string()).await;

                        let trading_mode = read_trading_mode(&mut conn).await;
                        let just_entered_live = entered_live_mode(&mut conn, &strategy_id, trading_mode).await;
                        if trading_mode == TradingMode::Live {
                            if passes_threshold
                                && book.yes.best_ask > 0.0
                                && yes_spread <= params.max_entry_spread
                                && remaining_seconds > entry_expiry_cutoff_secs
                                && now_ms - last_entry_ms >= ENTRY_COOLDOWN_MS
                                && now_ms - last_live_preview_ms >= live_preview_cooldown_ms
                            {
                                let available_cash = read_sim_available_cash(&mut conn).await;
                                let risk_cfg = read_risk_config(&mut conn).await;
                                let size = compute_strategy_bet_size(
                                    &mut conn,
                                    &strategy_id,
                                    available_cash,
                                    &risk_cfg,
                                    10.0,
                                    params.max_position_fraction,
                                ).await;
                                if size > 0.0 {
                                    let execution_id = Uuid::new_v4().to_string();
                                    let preview_msg = serde_json::json!({
                                        "execution_id": execution_id,
                                        "strategy": strategy_id,
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
                                            "required_divergence": required_divergence,
                                            "divergence_ok": divergence_ok,
                                            "spot": spot,
                                            "spot_age_ms": spot_age_ms,
                                            "book_age_ms": book_age_ms,
                                            "freshness_ok": freshness_ok,
                                            "sigma_annualized": sigma,
                                            "sigma_realized_annualized": realized_sigma.unwrap_or(-1.0),
                                            "sigma_implied_annualized": implied_sigma.unwrap_or(-1.0),
                                            "sigma_iv_fresh": iv_is_fresh,
                                            "sigma_iv_age_ms": iv_age_ms,
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
                                    publish_execution_event(&mut conn, preview_msg).await;
                                    last_live_preview_ms = now_ms;
                                    last_entry_ms = now_ms;
                                }
                            }
                            if just_entered_live {
                                let released_notional = {
                                    let mut pos_lock = self.open_position.write().await;
                                    pos_lock.take().map(|pos| pos.size).unwrap_or(0.0)
                                };
                                clear_strategy_open_positions(&mut conn, &strategy_id).await;
                                if released_notional > 0.0 {
                                    let _ = release_sim_notional_for_strategy(&mut conn, &strategy_id, released_notional).await;
                                    info!("{} strategy: cleared paper position on LIVE transition", strategy_id);
                                }
                            }
                            publish_heartbeat(&mut conn, &heartbeat_id).await;
                            continue;
                        }

                        let reset_ts = read_simulation_reset_ts(&mut conn).await;
                        if reset_ts > last_seen_reset_ts {
                            last_seen_reset_ts = reset_ts;
                            {
                                let mut pos_lock = self.open_position.write().await;
                                *pos_lock = None;
                            }
                            clear_strategy_open_positions(&mut conn, &strategy_id).await;
                            last_entry_ms = 0;
                            trades_this_window = 0;
                            consecutive_losses = 0;
                            publish_heartbeat(&mut conn, &heartbeat_id).await;
                            continue;
                        }

                        // Adaptive exit thresholds based on volatility regime.
                        let (tp_mult, sl_mult) = {
                            let history = self.spot_history.read().await;
                            let price_vec: Vec<(i64, f64)> = history.iter().copied().collect();
                            if let Some(regime) = detect_regime(&price_vec) {
                                regime_exit_multipliers(&regime)
                            } else {
                                (1.0, 1.0)
                            }
                        };
                        let adjusted_tp = params.take_profit_pct * tp_mult;
                        let adjusted_sl = params.stop_loss_pct * sl_mult;

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
                                    let close_reason = if mark_return >= adjusted_tp {
                                        Some("TAKE_PROFIT")
                                    } else if hold_ms >= MIN_HOLD_MS && executable_return <= adjusted_sl {
                                        Some("STOP_LOSS")
                                    } else if hold_ms >= MIN_HOLD_MS && edge < params.exit_edge_threshold {
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

                            // Skip if already positioned in this market (duplicate market guard)
                            if pos_lock.is_none()
                                && passes_threshold
                                && remaining_seconds > entry_expiry_cutoff_secs
                                && now_ms - last_entry_ms >= ENTRY_COOLDOWN_MS
                            {
                                entry_signal_price = Some(book.yes.best_ask);
                            }
                        }

                        if let Some((pos, exit_price, mark_return, executable_return, hold_ms, reason)) = close_event {
                            clear_strategy_open_positions(&mut conn, &strategy_id).await;
                            let pnl = realized_pnl(pos.size, executable_return, cost_model);
                            let new_bankroll = settle_sim_position_for_strategy(&mut conn, &strategy_id, pos.size, pnl).await;
                            if pnl < 0.0 {
                                consecutive_losses = consecutive_losses.saturating_add(1);
                            } else {
                                consecutive_losses = 0;
                            }

                            let net_return = if pos.size > 0.0 { pnl / pos.size } else { 0.0 };
                            let execution_id = pos.execution_id.clone();
                            let pnl_msg = serde_json::json!({
                                "execution_id": execution_id,
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
                            publish_event(&mut conn, "strategy:pnl", pnl_msg.to_string()).await;
                        }

                        if let Some(entry_price) = entry_signal_price {
                            let available_cash = read_sim_available_cash(&mut conn).await;
                            let risk_cfg = read_risk_config(&mut conn).await;
                            let size = compute_strategy_bet_size(
                                &mut conn,
                                &strategy_id,
                                available_cash,
                                &risk_cfg,
                                10.0,
                                params.max_position_fraction,
                            ).await;
                            if size <= 0.0 {
                                publish_heartbeat(&mut conn, &heartbeat_id).await;
                                continue;
                            }

                            if !reserve_sim_notional_for_strategy(&mut conn, &strategy_id, size).await {
                                publish_heartbeat(&mut conn, &heartbeat_id).await;
                                continue;
                            }
                            let mut accepted = false;
                            let mut execution_id: Option<String> = None;
                            let mut persisted_position: Option<Position> = None;
                            {
                                let mut pos_lock = self.open_position.write().await;
                                if pos_lock.is_none() {
                                    let id = Uuid::new_v4().to_string();
                                    let position = Position {
                                        execution_id: id.clone(),
                                        market_id: target_market.market_id.clone(),
                                        yes_token: target_market.yes_token.clone(),
                                        entry_price,
                                        size,
                                        timestamp_ms: now_ms,
                                    };
                                    *pos_lock = Some(position.clone());
                                    accepted = true;
                                    execution_id = Some(id);
                                    persisted_position = Some(position);
                                }
                            }
                            if let Some(position) = persisted_position.as_ref() {
                                persist_strategy_open_positions(&mut conn, &strategy_id, position).await;
                            }

                            if accepted {
                                last_entry_ms = now_ms;
                                trades_this_window = trades_this_window.saturating_add(1);
                                let exec_id = execution_id.unwrap_or_else(|| Uuid::new_v4().to_string());
                                let exec_msg = serde_json::json!({
                                    "execution_id": exec_id,
                                    "strategy": strategy_id.clone(),
                                    "market": format!("Long {}", self.asset),
                                    "side": "ENTRY",
                                    "price": entry_price,
                                    "size": size,
                                    "timestamp": now_ms,
                                    "mode": "PAPER",
                                    "details": {
                                        "strategy": strategy_id.clone(),
                                        "condition_id": target_market.market_id.clone(),
                                        "fair_value": fair_yes,
                                        "edge": edge,
                                        "adaptive_entry_edge": adaptive_entry_edge,
                                        "required_edge": required_edge,
                                        "required_divergence": required_divergence,
                                        "divergence_ok": divergence_ok,
                                        "spot": spot,
                                        "spot_age_ms": spot_age_ms,
                                        "book_age_ms": book_age_ms,
                                        "freshness_ok": freshness_ok,
                                        "sigma_annualized": sigma,
                                        "sigma_realized_annualized": realized_sigma.unwrap_or(-1.0),
                                        "sigma_implied_annualized": implied_sigma.unwrap_or(-1.0),
                                        "sigma_iv_fresh": iv_is_fresh,
                                        "sigma_iv_age_ms": iv_age_ms,
                                        "parity_deviation": parity_deviation,
                                        "edge_to_spread_ratio": edge_to_spread_ratio,
                                        "trades_this_window": trades_this_window,
                                        "slug": self.current_window_slug(),
                                    }
                                });
                                publish_execution_event(&mut conn, exec_msg).await;
                            } else {
                                let _ = release_sim_notional_for_strategy(&mut conn, &strategy_id, size).await;
                            }
                        }

                        publish_heartbeat(&mut conn, &heartbeat_id).await;
                    }
                }
            }

            if ws_disconnected {
                sleep(Duration::from_secs(reconnect_backoff_secs)).await;
                info!(
                    "{} fair-value Polymarket WS reconnect cycle complete (last disconnect: {})",
                    self.asset, disconnect_reason
                );
            }
        }
    }
}
