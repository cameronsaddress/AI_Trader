use async_trait::async_trait;
use chrono::{Datelike, Timelike, Utc};
use futures::{SinkExt, Stream, StreamExt};
use log::{error, info, warn};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use statrs::distribution::{ContinuousCDF, Normal};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{sleep, timeout, Duration};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use url::Url;
use uuid::Uuid;

use crate::engine::{MarketTarget, PolymarketClient, WS_URL};
use crate::strategies::control::{
    await_execution_decision,
    build_scan_payload,
    check_drawdown_breached,
    compute_strategy_bet_size,
    entered_live_mode,
    is_strategy_enabled,
    publish_heartbeat,
    publish_event,
    publish_execution_event,
    read_risk_config,
    read_risk_guard_cooldown,
    read_sim_available_cash,
    read_simulation_reset_ts,
    read_trading_mode,
    reserve_sim_notional_for_strategy,
    release_sim_notional_for_strategy,
    settle_sim_position_for_strategy,
    strategy_variant,
    ExecutionDecision,
    TradingMode,
};
use crate::strategies::implied_vol::{blend_sigma, spawn_deribit_iv_feed, ImpliedVolSnapshot};
use crate::strategies::market_data::{update_book_from_market_ws, BinaryBook};
use crate::strategies::simulation::{simulate_maker_entry_fill, SimCostModel, polymarket_taker_fee};
use crate::strategies::Strategy;

const COINBASE_ADVANCED_WS_URL: &str = "wss://advanced-trade-ws.coinbase.com";
const COINBASE_PRODUCT_ID: &str = "BTC-USD";
// Binance global WS can return HTTP 451 from some regions; default to Binance.US for US-hosted systems.
const BINANCE_US_BOOK_WS_URL: &str = "wss://stream.binance.us:9443/ws/btcusdt@bookTicker";
const OKX_PUBLIC_WS_URL: &str = "wss://ws.okx.com:8443/ws/v5/public";
const BYBIT_SPOT_WS_URL: &str = "wss://stream.bybit.com/v5/public/spot";
const KRAKEN_PUBLIC_WS_URL: &str = "wss://ws.kraken.com";
const BITFINEX_PUBLIC_WS_URL: &str = "wss://api-pub.bitfinex.com/ws/2";
const WINDOW_SECONDS: i64 = 300;
const SPOT_HISTORY_WINDOW_MS: i64 = 30 * 60 * 1000;
const SPOT_HISTORY_MAX_POINTS: usize = 250_000;
const VOL_WINDOW_MS: i64 = 5 * 60 * 1000;
const VOL_MIN_SAMPLES: usize = 12;
const SPOT_STALE_MS: i64 = 2_500;
const BOOK_STALE_MS: i64 = 1_500;
const ML_FEATURE_MAX_STALE_MS: i64 = 90_000;

fn entry_expiry_cutoff_secs() -> i64 {
    std::env::var("BTC_5M_ENTRY_EXPIRY_CUTOFF_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v| *v >= 5 && *v <= 30)
        .unwrap_or(8) // Reduced from 12 to 8
}

const LIVE_PREVIEW_COOLDOWN_MS: i64 = 2_000;
const SETTLEMENT_SPOT_TIMEOUT_MS: i64 = 12_000;

fn live_preview_cooldown_ms() -> i64 {
    std::env::var("BTC_5M_LIVE_PREVIEW_COOLDOWN_MS")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .filter(|v| *v >= 200 && *v <= 30_000)
        .unwrap_or(LIVE_PREVIEW_COOLDOWN_MS)
}

fn max_spot_age_ms() -> i64 {
    std::env::var("BTC_5M_MAX_SPOT_AGE_MS")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .filter(|v| *v >= 300 && *v <= 30_000)
        .unwrap_or(SPOT_STALE_MS)
}

fn max_book_age_ms() -> i64 {
    std::env::var("BTC_5M_MAX_BOOK_AGE_MS")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .filter(|v| *v >= 300 && *v <= 30_000)
        .unwrap_or(BOOK_STALE_MS)
}

fn iv_max_stale_ms() -> i64 {
    std::env::var("BTC_5M_IV_MAX_STALE_MS")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .filter(|v| *v >= 1_000 && *v <= 10 * 60_000)
        .unwrap_or(90_000)
}

fn iv_blend_weight() -> f64 {
    std::env::var("BTC_5M_IV_BLEND_WEIGHT")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|v| v.is_finite() && *v >= 0.0 && *v <= 1.0)
        .unwrap_or(0.65)
}

const OPEN_POSITION_REDIS_PREFIX: &str = "strategy:open_position:";
const OPEN_POSITION_TTL_SECS: i64 = 60 * 60; // 1h
const WINDOW_START_SPOT_REDIS_PREFIX: &str = "btc_5m:window_start_spot:";
const WINDOW_START_SPOT_TTL_SECS: i64 = 6 * 60 * 60; // 6h
const WINDOW_START_SPOT_MAX_SKEW_MS: i64 = 5_000;

// Avoid extreme prices that can explode ROI at settlement.
const MIN_ENTRY_PRICE: f64 = 0.15;
const MAX_ENTRY_PRICE: f64 = 0.85;
const MAX_PARITY_DEVIATION: f64 = 0.02;
const DEFAULT_MIN_EXPECTED_NET_RETURN: f64 = 0.004; // 40 bps expected net return
const DEFAULT_MAX_ENTRY_SPREAD: f64 = 0.08; // 8c wide markets are ignored
const DEFAULT_MAX_POSITION_FRACTION: f64 = 0.35;
const DEFAULT_MAX_DRAWDOWN_PCT: f64 = -5.0;
const DEFAULT_TAKE_PROFIT_MIN_RETURN: f64 = 0.35;
const DEFAULT_TAKE_PROFIT_MIN_BID: f64 = 0.82;
const DEFAULT_TAKE_PROFIT_MIN_HOLD_MS: i64 = 30_000;
const DEFAULT_THETA_SNIPER_WINDOW_SECS: i64 = 60;
const DEFAULT_THETA_SNIPER_MIN_DIVERGENCE_BPS: f64 = 1200.0;
const DEFAULT_THETA_SNIPER_MAX_BOOK_AGE_MS: i64 = 500;
const DEFAULT_THETA_SNIPER_SIZE_MULT: f64 = 0.50;
const DEFAULT_THETA_SNIPER_EXTRA_EDGE_BPS: f64 = 40.0;
const DEFAULT_MIN_TOP5_ASK_DEPTH_USD: f64 = 60.0;
const DEFAULT_MAKER_ENTRY_MAX_EDGE_BPS: f64 = 80.0;
const DEFAULT_MAKER_ENTRY_MIN_TIME_REMAINING_SECS: i64 = 120;
const DEFAULT_MAKER_ENTRY_MIN_FILL_PROB: f64 = 0.18;
// Risk-free rate for Black-Scholes fair-value pricing.
// Negligible over a 5-minute window but kept explicit for correctness.
const RISK_FREE_RATE: f64 = 0.045;

fn coinbase_ws_url() -> String {
    std::env::var("COINBASE_WS_URL").unwrap_or_else(|_| COINBASE_ADVANCED_WS_URL.to_string())
}

fn ws_url_from_env(var: &str, default: &str) -> String {
    std::env::var(var).unwrap_or_else(|_| default.to_string())
}

fn binance_ws_url() -> String {
    ws_url_from_env("BINANCE_WS_URL", BINANCE_US_BOOK_WS_URL)
}

fn okx_ws_url() -> String {
    ws_url_from_env("OKX_WS_URL", OKX_PUBLIC_WS_URL)
}

fn bybit_ws_url() -> String {
    ws_url_from_env("BYBIT_WS_URL", BYBIT_SPOT_WS_URL)
}

fn kraken_ws_url() -> String {
    ws_url_from_env("KRAKEN_WS_URL", KRAKEN_PUBLIC_WS_URL)
}

fn bitfinex_ws_url() -> String {
    ws_url_from_env("BITFINEX_WS_URL", BITFINEX_PUBLIC_WS_URL)
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

#[derive(Debug, Clone, Copy)]
struct CexVenueQuote {
    price: f64,
    pressure: f64,
    activity: f64,
    timestamp_ms: i64,
}

#[derive(Debug, Clone, Copy)]
struct CexFeedSample {
    mid: f64,
    pressure: f64,
    activity: f64,
}

impl CexFeedSample {
    fn new(mid: f64, pressure: f64, activity: f64) -> Option<Self> {
        if !mid.is_finite() || mid <= 0.0 {
            return None;
        }
        let safe_pressure = pressure.clamp(-1.0, 1.0);
        let safe_activity = if activity.is_finite() {
            activity.max(0.0)
        } else {
            0.0
        };
        Some(Self {
            mid,
            pressure: safe_pressure,
            activity: safe_activity,
        })
    }
}

fn size_pressure(bid_size: Option<f64>, ask_size: Option<f64>) -> f64 {
    let b = bid_size.unwrap_or(0.0).max(0.0);
    let a = ask_size.unwrap_or(0.0).max(0.0);
    let denom = b + a;
    if denom <= 0.0 {
        0.0
    } else {
        ((b - a) / denom).clamp(-1.0, 1.0)
    }
}

fn composite_activity(primary: Option<f64>, fallback: Option<f64>) -> f64 {
    let p = primary.unwrap_or(0.0).max(0.0);
    let f = fallback.unwrap_or(0.0).max(0.0);
    if p > 0.0 {
        p
    } else {
        f
    }
}

fn parse_coinbase_ticker_sample(payload: &str) -> Option<CexFeedSample> {
    let parsed = serde_json::from_str::<Value>(payload).ok()?;

    // Legacy Coinbase Exchange feed shape.
    if parsed.get("type").and_then(|v| v.as_str()) == Some("ticker")
        && parsed.get("product_id").and_then(|v| v.as_str()) == Some(COINBASE_PRODUCT_ID)
    {
        let mid = parse_number(parsed.get("price"))
            .or_else(|| {
                let bid = parse_number(parsed.get("best_bid"));
                let ask = parse_number(parsed.get("best_ask"));
                match (bid, ask) {
                    (Some(b), Some(a)) if b > 0.0 && a > 0.0 => Some((a + b) / 2.0),
                    _ => None,
                }
            })?;
        let bid_size = parse_number(parsed.get("best_bid_size"));
        let ask_size = parse_number(parsed.get("best_ask_size"));
        let pressure = size_pressure(bid_size, ask_size);
        let activity = composite_activity(
            parse_number(parsed.get("last_size")),
            bid_size.zip(ask_size).map(|(b, a)| b + a),
        );
        return CexFeedSample::new(mid, pressure, activity);
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

            let bid = parse_number(ticker.get("best_bid"));
            let ask = parse_number(ticker.get("best_ask"));
            let mid = if let Some(price) = parse_number(ticker.get("price")) {
                Some(price)
            } else {
                match (bid, ask) {
                    (Some(best_bid), Some(best_ask)) if best_bid > 0.0 && best_ask > 0.0 => {
                        Some((best_bid + best_ask) / 2.0)
                    }
                    _ => None,
                }
            };
            let bid_size = parse_number(ticker.get("best_bid_size"))
                .or_else(|| parse_number(ticker.get("best_bid_quantity")));
            let ask_size = parse_number(ticker.get("best_ask_size"))
                .or_else(|| parse_number(ticker.get("best_ask_quantity")));
            let pressure = size_pressure(bid_size, ask_size);
            let activity = composite_activity(
                parse_number(ticker.get("last_size")),
                bid_size.zip(ask_size).map(|(b, a)| b + a),
            );
            if let (Some(best_bid), Some(best_ask)) = (bid, ask) {
                if best_bid > 0.0 && best_ask > 0.0 {
                    return CexFeedSample::new((best_bid + best_ask) / 2.0, pressure, activity);
                }
            }
            if let Some(m) = mid {
                return CexFeedSample::new(m, pressure, activity);
            }
        }
    }

    None
}

fn parse_binance_book_sample(payload: &str) -> Option<CexFeedSample> {
    let parsed = serde_json::from_str::<Value>(payload).ok()?;
    let bid = parse_number(parsed.get("b"));
    let ask = parse_number(parsed.get("a"));
    let bid_size = parse_number(parsed.get("B"));
    let ask_size = parse_number(parsed.get("A"));
    match (bid, ask) {
        (Some(b), Some(a)) if b > 0.0 && a > 0.0 => CexFeedSample::new(
            (a + b) / 2.0,
            size_pressure(bid_size, ask_size),
            composite_activity(None, bid_size.zip(ask_size).map(|(x, y)| x + y)),
        ),
        _ => None,
    }
}

fn parse_okx_ticker_sample(payload: &str) -> Option<CexFeedSample> {
    let parsed = serde_json::from_str::<Value>(payload).ok()?;
    if parsed.get("event").and_then(|v| v.as_str()).is_some() {
        return None;
    }
    let arg = parsed.get("arg")?;
    if arg.get("channel").and_then(|v| v.as_str()) != Some("tickers") {
        return None;
    }
    // Prefer BTC-USDT (most liquid). Treat USDT as USD for UI/edge telemetry.
    if arg.get("instId").and_then(|v| v.as_str()) != Some("BTC-USDT") {
        return None;
    }
    let data = parsed.get("data")?.as_array()?;
    let row = data.first()?;
    let bid = parse_number(row.get("bidPx"));
    let ask = parse_number(row.get("askPx"));
    let bid_size = parse_number(row.get("bidSz"));
    let ask_size = parse_number(row.get("askSz"));
    let activity = composite_activity(
        parse_number(row.get("lastSz")),
        parse_number(row.get("vol24h")).or_else(|| parse_number(row.get("volCcy24h"))),
    );
    if let (Some(b), Some(a)) = (bid, ask) {
        if b > 0.0 && a > 0.0 {
            return CexFeedSample::new((a + b) / 2.0, size_pressure(bid_size, ask_size), activity);
        }
    }
    parse_number(row.get("last"))
        .and_then(|mid| CexFeedSample::new(mid, size_pressure(bid_size, ask_size), activity))
}

fn parse_bybit_ticker_sample(payload: &str) -> Option<CexFeedSample> {
    let parsed = serde_json::from_str::<Value>(payload).ok()?;
    let topic = parsed.get("topic")?.as_str()?;
    if topic != "tickers.BTCUSDT" {
        return None;
    }
    let data = parsed.get("data")?;
    let bid = parse_number(data.get("bid1Price"));
    let ask = parse_number(data.get("ask1Price"));
    let bid_size = parse_number(data.get("bid1Size"));
    let ask_size = parse_number(data.get("ask1Size"));
    let activity = composite_activity(
        parse_number(data.get("volume24h")),
        parse_number(data.get("turnover24h")),
    );
    if let (Some(b), Some(a)) = (bid, ask) {
        if b > 0.0 && a > 0.0 {
            return CexFeedSample::new((a + b) / 2.0, size_pressure(bid_size, ask_size), activity);
        }
    }
    parse_number(data.get("lastPrice"))
        .and_then(|mid| CexFeedSample::new(mid, size_pressure(bid_size, ask_size), activity))
}

fn parse_kraken_ticker_sample(payload: &str) -> Option<CexFeedSample> {
    let parsed = serde_json::from_str::<Value>(payload).ok()?;
    if parsed.is_object() {
        return None;
    }
    let arr = parsed.as_array()?;
    if arr.len() < 4 {
        return None;
    }
    if arr.get(2)?.as_str() != Some("ticker") {
        return None;
    }
    let pair = arr.get(3)?.as_str().unwrap_or("");
    if pair != "XBT/USD" && pair != "XBT/USDT" {
        return None;
    }
    let data = arr.get(1)?;
    let ask = data
        .get("a")
        .and_then(|v| v.as_array())
        .and_then(|v| v.first())
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<f64>().ok());
    let ask_size = data
        .get("a")
        .and_then(|v| v.as_array())
        .and_then(|v| v.get(1))
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<f64>().ok());
    let bid = data
        .get("b")
        .and_then(|v| v.as_array())
        .and_then(|v| v.first())
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<f64>().ok());
    let bid_size = data
        .get("b")
        .and_then(|v| v.as_array())
        .and_then(|v| v.get(1))
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<f64>().ok());
    match (bid, ask) {
        (Some(b), Some(a)) if b > 0.0 && a > 0.0 => CexFeedSample::new(
            (a + b) / 2.0,
            size_pressure(bid_size, ask_size),
            bid_size.zip(ask_size).map(|(x, y)| x + y).unwrap_or(0.0),
        ),
        _ => None,
    }
}

fn parse_bitfinex_ticker_sample(data: &[Value]) -> Option<CexFeedSample> {
    let bid = data.first().and_then(|v| v.as_f64());
    let bid_size = data.get(1).and_then(|v| v.as_f64());
    let ask = data.get(2).and_then(|v| v.as_f64());
    let ask_size = data.get(3).and_then(|v| v.as_f64());
    let volume_24h = data.get(7).and_then(|v| v.as_f64()).map(|v| v.abs());
    match (bid, ask) {
        (Some(b), Some(a)) if b > 0.0 && a > 0.0 => CexFeedSample::new(
            (a + b) / 2.0,
            size_pressure(bid_size, ask_size),
            composite_activity(volume_24h, bid_size.zip(ask_size).map(|(x, y)| x.abs() + y.abs())),
        ),
        _ => None,
    }
}

fn median_price(mut values: Vec<f64>) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.retain(|v| v.is_finite() && *v > 0.0);
    if values.is_empty() {
        return None;
    }
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mid = values.len() / 2;
    if values.len() % 2 == 1 {
        values.get(mid).copied()
    } else {
        let a = values.get(mid.saturating_sub(1)).copied().unwrap_or(values[mid]);
        let b = values.get(mid).copied().unwrap_or(a);
        Some((a + b) / 2.0)
    }
}

/// Per-venue base reliability weight. Higher = more trusted / lower latency.
fn venue_base_weight(venue: &str) -> f64 {
    match venue {
        "coinbase" => 1.2,
        "binance" => 1.1,
        "okx" => 1.0,
        "bybit" => 0.9,
        "kraken" => 0.9,
        "bitfinex" => 0.8,
        _ => 0.5,
    }
}

/// Weighted median using per-venue reliability scores.
/// Weight = base_weight * freshness_decay * outlier_penalty.
/// - Freshness decays linearly from 1.0 to 0.1 over `stale_ms`.
/// - Outlier penalty halves weight for prices deviating >30bps from simple median.
fn reliability_weighted_median(
    venue_prices: &[(&str, f64, i64)],
    now_ms: i64,
    stale_ms: i64,
) -> Option<f64> {
    let valid: Vec<(&str, f64, i64)> = venue_prices
        .iter()
        .filter(|(_, px, ts)| *px > 0.0 && px.is_finite() && now_ms - *ts <= stale_ms)
        .copied()
        .collect();

    if valid.is_empty() {
        return None;
    }

    // Simple median for outlier detection
    let simple_median = {
        let mut prices: Vec<f64> = valid.iter().map(|(_, px, _)| *px).collect();
        prices.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let mid = prices.len() / 2;
        if prices.len() % 2 == 1 {
            prices[mid]
        } else {
            (prices[mid.saturating_sub(1)] + prices[mid]) / 2.0
        }
    };

    // Compute per-venue weights
    let mut weighted: Vec<(f64, f64)> = Vec::with_capacity(valid.len());
    for (venue, px, ts) in &valid {
        let base = venue_base_weight(venue);
        let age_ms = (now_ms - ts).max(0) as f64;
        let freshness = (1.0 - age_ms / stale_ms as f64).max(0.1);
        let deviation = ((px - simple_median) / simple_median).abs();
        let outlier = if deviation > 0.003 { 0.3 } else { 1.0 };
        weighted.push((*px, base * freshness * outlier));
    }

    // Sort by price, find weighted median
    weighted.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    let total_weight: f64 = weighted.iter().map(|(_, w)| w).sum();
    if total_weight <= 0.0 {
        return None;
    }

    let half = total_weight / 2.0;
    let mut cumulative = 0.0;
    for (px, w) in &weighted {
        cumulative += w;
        if cumulative >= half {
            return Some(*px);
        }
    }
    weighted.last().map(|(px, _)| *px)
}

#[derive(Debug, Clone, Copy, Default)]
struct CexVolumeSignal {
    pressure: f64,
    activity_ratio: f64,
    venues: usize,
}

fn compute_cex_volume_signal(
    quotes: &HashMap<&'static str, CexVenueQuote>,
    now_ms: i64,
    stale_ms: i64,
) -> Option<CexVolumeSignal> {
    let mut valid: Vec<(&str, CexVenueQuote)> = quotes
        .iter()
        .filter_map(|(venue, quote)| {
            if quote.price <= 0.0 || !quote.price.is_finite() {
                return None;
            }
            if now_ms.saturating_sub(quote.timestamp_ms) > stale_ms {
                return None;
            }
            Some((*venue, *quote))
        })
        .collect();

    if valid.len() < 2 {
        return None;
    }

    let activity_values: Vec<f64> = valid.iter().map(|(_, quote)| quote.activity.max(0.0)).collect();
    let median_activity = median_price(activity_values.clone()).unwrap_or(0.0);
    let avg_activity = activity_values.iter().sum::<f64>() / (activity_values.len() as f64).max(1.0);
    let activity_ratio = if median_activity > 0.0 {
        (avg_activity / median_activity).clamp(0.0, 8.0)
    } else {
        1.0
    };

    let venue_count = valid.len();
    let mut weighted_pressure = 0.0;
    let mut weight_sum = 0.0;
    for (venue, quote) in valid.drain(..) {
        let base = venue_base_weight(venue);
        let age_ms = now_ms.saturating_sub(quote.timestamp_ms) as f64;
        let freshness = (1.0 - age_ms / stale_ms as f64).clamp(0.1, 1.0);
        let act_mult = if median_activity > 0.0 {
            (quote.activity / median_activity).clamp(0.25, 4.0)
        } else {
            1.0
        };
        let weight = base * freshness * act_mult;
        weighted_pressure += weight * quote.pressure;
        weight_sum += weight;
    }

    if weight_sum <= 0.0 {
        return None;
    }

    Some(CexVolumeSignal {
        pressure: (weighted_pressure / weight_sum).clamp(-1.0, 1.0),
        activity_ratio,
        venues: venue_count,
    })
}

fn open_position_redis_key(strategy_id: &str) -> String {
    format!("{}{}", OPEN_POSITION_REDIS_PREFIX, strategy_id.trim().to_uppercase())
}

fn window_start_spot_redis_key(window_start_ts: i64) -> String {
    format!("{}{}", WINDOW_START_SPOT_REDIS_PREFIX, window_start_ts)
}

fn side_label(side: Side) -> &'static str {
    match side {
        Side::Up => "UP",
        Side::Down => "DOWN",
    }
}

fn parse_side_label(label: &str) -> Option<Side> {
    match label.trim().to_uppercase().as_str() {
        "UP" => Some(Side::Up),
        "DOWN" => Some(Side::Down),
        _ => None,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedPosition {
    execution_id: String,
    side: String,
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
    slug: String,
}

impl PersistedPosition {
    fn from_position(pos: &Position) -> Self {
        Self {
            execution_id: pos.execution_id.clone(),
            side: side_label(pos.side).to_string(),
            entry_price: pos.entry_price,
            notional_usd: pos.notional_usd,
            entry_ts_ms: pos.entry_ts_ms,
            window_start_spot: pos.window_start_spot,
            window_start_ts: pos.window_start_ts,
            expiry_ts: pos.expiry_ts,
            token_id: pos.token_id.clone(),
            condition_id: pos.condition_id.clone(),
            fair_yes: pos.fair_yes,
            sigma: pos.sigma,
            slug: pos.slug.clone(),
        }
    }

    fn into_position(self) -> Option<Position> {
        let side = parse_side_label(&self.side)?;
        if !self.entry_price.is_finite() || self.entry_price <= 0.0 || self.entry_price >= 1.0 {
            return None;
        }
        if !self.notional_usd.is_finite() || self.notional_usd <= 0.0 {
            return None;
        }
        Some(Position {
            execution_id: self.execution_id,
            side,
            entry_price: self.entry_price,
            notional_usd: self.notional_usd,
            entry_ts_ms: self.entry_ts_ms,
            window_start_spot: self.window_start_spot,
            window_start_ts: self.window_start_ts,
            expiry_ts: self.expiry_ts,
            token_id: self.token_id,
            condition_id: self.condition_id,
            fair_yes: self.fair_yes,
            sigma: self.sigma,
            slug: self.slug,
        })
    }
}

async fn load_open_position(
    conn: &mut redis::aio::MultiplexedConnection,
    strategy_id: &str,
) -> Option<Position> {
    let key = open_position_redis_key(strategy_id);
    let raw: Option<String> = conn.get(&key).await.unwrap_or(None);
    let raw = raw?;
    let parsed = serde_json::from_str::<PersistedPosition>(&raw).ok()?;
    parsed.into_position()
}

async fn persist_open_position(
    conn: &mut redis::aio::MultiplexedConnection,
    strategy_id: &str,
    pos: &Position,
) {
    let key = open_position_redis_key(strategy_id);
    let payload = match serde_json::to_string(&PersistedPosition::from_position(pos)) {
        Ok(encoded) => encoded,
        Err(error) => {
            warn!("[{}] failed to encode persisted open position: {}", strategy_id, error);
            return;
        }
    };
    if let Err(error) = conn.set::<_, _, ()>(&key, payload).await {
        warn!("[{}] failed to persist open position {}: {}", strategy_id, key, error);
        return;
    }
    if let Err(error) = conn.expire::<_, ()>(&key, OPEN_POSITION_TTL_SECS).await {
        warn!("[{}] failed to set TTL for {}: {}", strategy_id, key, error);
    }
}

async fn clear_open_position(conn: &mut redis::aio::MultiplexedConnection, strategy_id: &str) {
    let key = open_position_redis_key(strategy_id);
    if let Err(error) = conn.del::<_, ()>(&key).await {
        warn!("[{}] failed to clear open position {}: {}", strategy_id, key, error);
    }
}

async fn read_cached_window_start_spot(
    conn: &mut redis::aio::MultiplexedConnection,
    window_start_ts: i64,
) -> Option<f64> {
    let key = window_start_spot_redis_key(window_start_ts);
    let raw: Option<f64> = conn.get(&key).await.unwrap_or(None);
    raw.filter(|v| v.is_finite() && *v > 0.0)
}

async fn cache_window_start_spot(
    conn: &mut redis::aio::MultiplexedConnection,
    window_start_ts: i64,
    spot: f64,
) {
    if !spot.is_finite() || spot <= 0.0 {
        return;
    }
    let key = window_start_spot_redis_key(window_start_ts);
    let ok: bool = conn.set_nx(&key, spot).await.unwrap_or(false);
    if ok {
        if let Err(error) = conn.expire::<_, ()>(&key, WINDOW_START_SPOT_TTL_SECS).await {
            warn!("[BTC_5M] failed to set window spot TTL {}: {}", key, error);
        }
    }
}

/// Compute the next reconnect delay with exponential backoff (1s → 2s → 4s → ... → 30s cap).
/// Resets to 1s on a successful connection that received at least one message.
fn next_backoff_secs(current: u64) -> u64 {
    (current * 2).clamp(1, 30)
}

fn ws_read_timeout_ms() -> u64 {
    std::env::var("SCANNER_WS_READ_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|v| *v >= 5_000 && *v <= 180_000)
        .unwrap_or(45_000)
}

async fn next_ws_message_or_reconnect<S>(
    ws_stream: &mut S,
) -> Option<Result<Message, tokio_tungstenite::tungstenite::Error>>
where
    S: Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    let timeout_ms = ws_read_timeout_ms();
    match timeout(Duration::from_millis(timeout_ms), ws_stream.next()).await {
        Ok(next) => next,
        Err(_) => {
            warn!("WS feed read timed out after {}ms; reconnecting", timeout_ms);
            None
        }
    }
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

fn spawn_supervised_feed_task<F>(name: &'static str, mut spawn_once: F)
where
    F: FnMut() -> tokio::task::JoinHandle<()> + Send + 'static,
{
    tokio::spawn(async move {
        let mut restart_backoff_secs = 1_u64;
        loop {
            let handle = spawn_once();
            match handle.await {
                Ok(()) => warn!("{} feed task exited; restarting", name),
                Err(error) => error!("{} feed task crashed: {}; restarting", name, error),
            }
            sleep(Duration::from_secs(restart_backoff_secs)).await;
            restart_backoff_secs = next_backoff_secs(restart_backoff_secs);
        }
    });
}

fn spawn_binance_feed(
    quotes: Arc<RwLock<HashMap<&'static str, CexVenueQuote>>>,
) -> tokio::task::JoinHandle<()> {
    let ws_url = binance_ws_url();
    tokio::spawn(async move {
        let mut backoff_secs = 1_u64;
        loop {
            match connect_async(ws_url.as_str()).await {
                Ok((mut ws_stream, _)) => {
                    info!("Binance WS connected for BTC pricing via {}", ws_url);
                    backoff_secs = 1;
                    while let Some(msg) = next_ws_message_or_reconnect(&mut ws_stream).await {
                        match msg {
                            Ok(Message::Text(text)) => {
                                if let Some(sample) = parse_binance_book_sample(&text) {
                                    let now_ms = Utc::now().timestamp_millis();
                                    let mut writer = quotes.write().await;
                                    writer.insert("binance", CexVenueQuote {
                                        price: sample.mid,
                                        pressure: sample.pressure,
                                        activity: sample.activity,
                                        timestamp_ms: now_ms,
                                    });
                                }
                            }
                            Ok(Message::Ping(payload)) => {
                                let _ = ws_stream.send(Message::Pong(payload)).await;
                            }
                            Ok(Message::Close(_)) => break,
                            Ok(Message::Binary(_)) | Ok(Message::Pong(_)) => continue,
                            Ok(_) => continue,
                            Err(e) => {
                                error!("Binance WS message error: {}", e);
                                break;
                            }
                        }
                    }
                }
                Err(e) => error!("Binance WS connection failed: {}", e),
            }
            sleep(Duration::from_secs(backoff_secs)).await;
            backoff_secs = next_backoff_secs(backoff_secs);
        }
    })
}

fn spawn_okx_feed(
    quotes: Arc<RwLock<HashMap<&'static str, CexVenueQuote>>>,
) -> tokio::task::JoinHandle<()> {
    let ws_url = okx_ws_url();
    tokio::spawn(async move {
        let subscribe_msg = serde_json::json!({
            "op": "subscribe",
            "args": [{"channel": "tickers", "instId": "BTC-USDT"}]
        })
        .to_string();
        let mut backoff_secs = 1_u64;
        loop {
            match connect_async(ws_url.as_str()).await {
                Ok((mut ws_stream, _)) => {
                    info!("OKX WS connected for BTC pricing via {}", ws_url);
                    backoff_secs = 1;
                    if let Err(e) = ws_stream.send(Message::Text(subscribe_msg.clone())).await {
                        error!("OKX subscribe failed: {}", e);
                    }
                    while let Some(msg) = next_ws_message_or_reconnect(&mut ws_stream).await {
                        match msg {
                            Ok(Message::Text(text)) => {
                                let trimmed = text.trim();
                                if trimmed == "ping" {
                                    let _ = ws_stream.send(Message::Text("pong".to_string())).await;
                                    continue;
                                }
                                if let Some(sample) = parse_okx_ticker_sample(trimmed) {
                                    let now_ms = Utc::now().timestamp_millis();
                                    let mut writer = quotes.write().await;
                                    writer.insert("okx", CexVenueQuote {
                                        price: sample.mid,
                                        pressure: sample.pressure,
                                        activity: sample.activity,
                                        timestamp_ms: now_ms,
                                    });
                                }
                            }
                            Ok(Message::Ping(payload)) => {
                                let _ = ws_stream.send(Message::Pong(payload)).await;
                            }
                            Ok(Message::Close(_)) => break,
                            Ok(Message::Binary(_)) | Ok(Message::Pong(_)) => continue,
                            Ok(_) => continue,
                            Err(e) => {
                                error!("OKX WS message error: {}", e);
                                break;
                            }
                        }
                    }
                }
                Err(e) => error!("OKX WS connection failed: {}", e),
            }
            sleep(Duration::from_secs(backoff_secs)).await;
            backoff_secs = next_backoff_secs(backoff_secs);
        }
    })
}

fn spawn_bybit_feed(
    quotes: Arc<RwLock<HashMap<&'static str, CexVenueQuote>>>,
) -> tokio::task::JoinHandle<()> {
    let ws_url = bybit_ws_url();
    tokio::spawn(async move {
        let subscribe_msg = serde_json::json!({
            "op": "subscribe",
            "args": ["tickers.BTCUSDT"]
        })
        .to_string();
        let mut backoff_secs = 1_u64;
        loop {
            match connect_async(ws_url.as_str()).await {
                Ok((mut ws_stream, _)) => {
                    info!("Bybit WS connected for BTC pricing via {}", ws_url);
                    backoff_secs = 1;
                    if let Err(e) = ws_stream.send(Message::Text(subscribe_msg.clone())).await {
                        error!("Bybit subscribe failed: {}", e);
                    }
                    while let Some(msg) = next_ws_message_or_reconnect(&mut ws_stream).await {
                        match msg {
                            Ok(Message::Text(text)) => {
                                let trimmed = text.trim();
                                // Respond to bybit application-level pings if they occur.
                                if trimmed.contains("\"op\":\"ping\"") {
                                    let _ = ws_stream.send(Message::Text("{\"op\":\"pong\"}".to_string())).await;
                                    continue;
                                }
                                if let Some(sample) = parse_bybit_ticker_sample(trimmed) {
                                    let now_ms = Utc::now().timestamp_millis();
                                    let mut writer = quotes.write().await;
                                    writer.insert("bybit", CexVenueQuote {
                                        price: sample.mid,
                                        pressure: sample.pressure,
                                        activity: sample.activity,
                                        timestamp_ms: now_ms,
                                    });
                                }
                            }
                            Ok(Message::Ping(payload)) => {
                                let _ = ws_stream.send(Message::Pong(payload)).await;
                            }
                            Ok(Message::Close(_)) => break,
                            Ok(Message::Binary(_)) | Ok(Message::Pong(_)) => continue,
                            Ok(_) => continue,
                            Err(e) => {
                                error!("Bybit WS message error: {}", e);
                                break;
                            }
                        }
                    }
                }
                Err(e) => error!("Bybit WS connection failed: {}", e),
            }
            sleep(Duration::from_secs(backoff_secs)).await;
            backoff_secs = next_backoff_secs(backoff_secs);
        }
    })
}

fn spawn_kraken_feed(
    quotes: Arc<RwLock<HashMap<&'static str, CexVenueQuote>>>,
) -> tokio::task::JoinHandle<()> {
    let ws_url = kraken_ws_url();
    tokio::spawn(async move {
        let subscribe_msg = serde_json::json!({
            "event": "subscribe",
            "pair": ["XBT/USD"],
            "subscription": {"name": "ticker"}
        })
        .to_string();
        let mut backoff_secs = 1_u64;
        loop {
            match connect_async(ws_url.as_str()).await {
                Ok((mut ws_stream, _)) => {
                    info!("Kraken WS connected for BTC pricing via {}", ws_url);
                    backoff_secs = 1;
                    if let Err(e) = ws_stream.send(Message::Text(subscribe_msg.clone())).await {
                        error!("Kraken subscribe failed: {}", e);
                    }
                    while let Some(msg) = next_ws_message_or_reconnect(&mut ws_stream).await {
                        match msg {
                            Ok(Message::Text(text)) => {
                                let trimmed = text.trim();
                                if let Ok(v) = serde_json::from_str::<Value>(trimmed) {
                                    if let Some(event) = v.get("event").and_then(|e| e.as_str()) {
                                        if event == "ping" {
                                            let reqid = v.get("reqid").and_then(|r| r.as_i64());
                                            let pong = if let Some(id) = reqid {
                                                serde_json::json!({"event":"pong","reqid": id}).to_string()
                                            } else {
                                                serde_json::json!({"event":"pong"}).to_string()
                                            };
                                            let _ = ws_stream.send(Message::Text(pong)).await;
                                            continue;
                                        }
                                    }
                                }

                                if let Some(sample) = parse_kraken_ticker_sample(trimmed) {
                                    let now_ms = Utc::now().timestamp_millis();
                                    let mut writer = quotes.write().await;
                                    writer.insert("kraken", CexVenueQuote {
                                        price: sample.mid,
                                        pressure: sample.pressure,
                                        activity: sample.activity,
                                        timestamp_ms: now_ms,
                                    });
                                }
                            }
                            Ok(Message::Ping(payload)) => {
                                let _ = ws_stream.send(Message::Pong(payload)).await;
                            }
                            Ok(Message::Close(_)) => break,
                            Ok(Message::Binary(_)) | Ok(Message::Pong(_)) => continue,
                            Ok(_) => continue,
                            Err(e) => {
                                error!("Kraken WS message error: {}", e);
                                break;
                            }
                        }
                    }
                }
                Err(e) => error!("Kraken WS connection failed: {}", e),
            }
            sleep(Duration::from_secs(backoff_secs)).await;
            backoff_secs = next_backoff_secs(backoff_secs);
        }
    })
}

fn spawn_bitfinex_feed(
    quotes: Arc<RwLock<HashMap<&'static str, CexVenueQuote>>>,
) -> tokio::task::JoinHandle<()> {
    let ws_url = bitfinex_ws_url();
    tokio::spawn(async move {
        let subscribe_msg = serde_json::json!({
            "event": "subscribe",
            "channel": "ticker",
            "symbol": "tBTCUSD"
        })
        .to_string();
        let mut backoff_secs = 1_u64;
        loop {
            match connect_async(ws_url.as_str()).await {
                Ok((mut ws_stream, _)) => {
                    info!("Bitfinex WS connected for BTC pricing via {}", ws_url);
                    backoff_secs = 1;
                    let mut chan_id: Option<i64> = None;
                    if let Err(e) = ws_stream.send(Message::Text(subscribe_msg.clone())).await {
                        error!("Bitfinex subscribe failed: {}", e);
                    }
                    while let Some(msg) = next_ws_message_or_reconnect(&mut ws_stream).await {
                        match msg {
                            Ok(Message::Text(text)) => {
                                let trimmed = text.trim();
                                let Ok(v) = serde_json::from_str::<Value>(trimmed) else {
                                    continue;
                                };

                                if v.is_object() {
                                    if let Some(event) = v.get("event").and_then(|e| e.as_str()) {
                                        if event == "subscribed"
                                            && v.get("channel").and_then(|c| c.as_str()) == Some("ticker")
                                        {
                                            chan_id = v.get("chanId").and_then(|c| c.as_i64());
                                        } else if event == "ping" {
                                            let cid = v.get("cid").and_then(|c| c.as_i64());
                                            let pong = if let Some(cid) = cid {
                                                serde_json::json!({"event":"pong","cid": cid}).to_string()
                                            } else {
                                                serde_json::json!({"event":"pong"}).to_string()
                                            };
                                            let _ = ws_stream.send(Message::Text(pong)).await;
                                        }
                                    }
                                    continue;
                                }

                                let Some(arr) = v.as_array() else {
                                    continue;
                                };
                                if arr.len() < 2 {
                                    continue;
                                }
                                let Some(id) = arr.first().and_then(|c| c.as_i64()) else {
                                    continue;
                                };
                                if chan_id.is_some() && chan_id != Some(id) {
                                    continue;
                                }
                                if arr.get(1).and_then(|hb| hb.as_str()) == Some("hb") {
                                    continue;
                                }
                                let Some(data) = arr.get(1).and_then(|d| d.as_array()) else {
                                    continue;
                                };
                                if let Some(sample) = parse_bitfinex_ticker_sample(data) {
                                    let now_ms = Utc::now().timestamp_millis();
                                    let mut writer = quotes.write().await;
                                    writer.insert("bitfinex", CexVenueQuote {
                                        price: sample.mid,
                                        pressure: sample.pressure,
                                        activity: sample.activity,
                                        timestamp_ms: now_ms,
                                    });
                                }
                            }
                            Ok(Message::Ping(payload)) => {
                                let _ = ws_stream.send(Message::Pong(payload)).await;
                            }
                            Ok(Message::Close(_)) => break,
                            Ok(Message::Binary(_)) | Ok(Message::Pong(_)) => continue,
                            Ok(_) => continue,
                            Err(e) => {
                                error!("Bitfinex WS message error: {}", e);
                                break;
                            }
                        }
                    }
                }
                Err(e) => error!("Bitfinex WS connection failed: {}", e),
            }
            sleep(Duration::from_secs(backoff_secs)).await;
            backoff_secs = next_backoff_secs(backoff_secs);
        }
    })
}

fn spawn_coinbase_feed(
    spot_writer: Arc<RwLock<(f64, i64)>>,
    spot_history_writer: Arc<RwLock<VecDeque<(i64, f64)>>>,
    cex_coinbase_writer: Arc<RwLock<HashMap<&'static str, CexVenueQuote>>>,
    coinbase_ws_url: String,
    coinbase_subscriptions: Vec<Value>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut backoff_secs = 1_u64;
        loop {
            match connect_async(coinbase_ws_url.as_str()).await {
                Ok((mut ws_stream, _)) => {
                    info!("Coinbase WS connected for BTC pricing via {}", coinbase_ws_url);
                    backoff_secs = 1;
                    let mut last_sequence: Option<u64> = None;
                    for subscribe_msg in &coinbase_subscriptions {
                        if let Err(e) = ws_stream.send(Message::Text(subscribe_msg.to_string())).await {
                            error!("Coinbase subscribe failed: {}", e);
                        }
                    }

                    while let Some(msg) = next_ws_message_or_reconnect(&mut ws_stream).await {
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

                                if let Some(sample) = parse_coinbase_ticker_sample(&text) {
                                    let now_ms = Utc::now().timestamp_millis();
                                    {
                                        let mut writer = spot_writer.write().await;
                                        *writer = (sample.mid, now_ms);
                                    }
                                    {
                                        let mut writer = cex_coinbase_writer.write().await;
                                        writer.insert("coinbase", CexVenueQuote {
                                            price: sample.mid,
                                            pressure: sample.pressure,
                                            activity: sample.activity,
                                            timestamp_ms: now_ms,
                                        });
                                    }
                                    {
                                        let mut history = spot_history_writer.write().await;
                                        history.push_back((now_ms, sample.mid));
                                        while let Some((ts, _)) = history.front() {
                                            if now_ms - *ts > SPOT_HISTORY_WINDOW_MS {
                                                history.pop_front();
                                            } else {
                                                break;
                                            }
                                        }
                                        while history.len() > SPOT_HISTORY_MAX_POINTS {
                                            history.pop_front();
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
            sleep(Duration::from_secs(backoff_secs)).await;
            backoff_secs = next_backoff_secs(backoff_secs);
        }
    })
}

fn parse_window_start_ts(slug: &str) -> Option<i64> {
    slug.split("-5m-").nth(1).and_then(|s| s.parse::<i64>().ok())
}

fn spot_near_window_start(
    history: &VecDeque<(i64, f64)>,
    window_start_ms: i64,
    max_skew_ms: i64,
) -> Option<(i64, f64)> {
    if history.is_empty() {
        return None;
    }

    // Prefer a reading at/before window start to avoid look-ahead bias.
    if let Some((ts, px)) = history
        .iter()
        .rev()
        .find(|(ts, px)| *ts <= window_start_ms && *px > 0.0)
    {
        if window_start_ms - *ts <= max_skew_ms {
            return Some((*ts, *px));
        }
    }

    // Accept the first tick *after* window start only if it's very close to the boundary.
    history
        .iter()
        .find(|(ts, px)| *ts >= window_start_ms && *px > 0.0)
        .and_then(|(ts, px)| {
            if *ts - window_start_ms <= max_skew_ms {
                Some((*ts, *px))
            } else {
                None
            }
        })
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
    if !spot.is_finite() || !strike.is_finite() || spot <= 0.0 || strike <= 0.0 {
        return 0.5;
    }
    if time_to_expiry_years <= 0.0 {
        return if spot > strike { 1.0 } else { 0.0 };
    }

    let denom = sigma * time_to_expiry_years.sqrt();
    if denom <= 0.0 {
        return 0.5;
    }

    let d2 = ((spot / strike).ln() + (RISK_FREE_RATE - 0.5 * sigma.powi(2)) * time_to_expiry_years) / denom;
    match Normal::new(0.0, 1.0) {
        Ok(normal) => (-RISK_FREE_RATE * time_to_expiry_years).exp() * normal.cdf(d2),
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
    execution_id: String,
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
    slug: String,
}

fn expected_roi(prob: f64, price: f64) -> f64 {
    if !(prob.is_finite() && price.is_finite()) || price <= 0.0 {
        return -1.0;
    }
    (prob / price) - 1.0
}

fn kelly_fraction_fee_adjusted(prob: f64, price: f64, entry_cost_rate: f64) -> f64 {
    if !(prob.is_finite() && price.is_finite() && entry_cost_rate.is_finite())
        || price <= 0.0
        || price >= 1.0
        || entry_cost_rate < 0.0
    {
        return 0.0;
    }

    let p = prob.clamp(0.0, 1.0);
    let q = 1.0 - p;
    let win_return = ((1.0 / price) - 1.0) - entry_cost_rate;
    let loss_return = -1.0 - entry_cost_rate;
    if win_return <= 0.0 || loss_return >= 0.0 {
        return 0.0;
    }

    let gain = win_return;
    let loss = -loss_return;
    let numerator = (p * gain) - (q * loss);
    if numerator <= 0.0 {
        return 0.0;
    }
    let denominator = gain * loss;
    if denominator <= 0.0 {
        return 0.0;
    }
    (numerator / denominator).clamp(0.0, 1.0)
}

/// Intraday seasonality multiplier based on historical trade outcome patterns.
/// Returns a multiplier (0.5-1.5) that scales the edge threshold.
/// Higher multiplier = more favorable trading window = lower threshold.
fn seasonality_multiplier(hour_utc: u32, day_of_week: u32) -> f64 {
    // Weekend (Sat=6, Sun=0 in chrono): reduced activity
    let weekend_discount: f64 = if day_of_week == 6 || day_of_week == 0 { 0.7 } else { 1.0 };

    let hour_mult: f64 = match hour_utc {
        0..=3 => 0.8,    // Late Asian: thin liquidity
        4..=7 => 0.9,    // Pre-London
        8..=10 => 1.3,   // London open: high volatility, more edge
        11..=13 => 1.0,  // Mid-day overlap
        14..=16 => 1.3,  // US open: deep liquidity, most opportunity
        17..=19 => 1.0,  // US afternoon
        20..=23 => 0.85, // Post-US: thinning
        _ => 1.0,
    };

    (hour_mult * weekend_discount).clamp(0.5, 1.5)
}

fn resolve_position_return(side: Side, entry_price: f64, window_start_spot: f64, end_spot: f64) -> (bool, f64) {
    if !entry_price.is_finite() || entry_price <= 0.0 || !end_spot.is_finite() || end_spot <= 0.0 {
        return (false, -1.0);
    }
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

/// ML features from backend (persisted to Redis from ml:features pub/sub).
#[derive(Debug, Clone)]
struct MlFeatures {
    rsi_14: f64,
    sma_20: f64,
    price: f64,
    timestamp: i64,
    ml_prob_up: Option<f64>,
    ml_signal_confidence: Option<f64>,
    ml_signal_edge: Option<f64>,
    ml_model: Option<String>,
    ml_model_ready: bool,
    ml_model_samples: Option<f64>,
    ml_model_eval_auc: Option<f64>,
    ml_model_eval_logloss: Option<f64>,
    ml_model_eval_brier: Option<f64>,
}

async fn read_ml_features(conn: &mut redis::aio::MultiplexedConnection, symbol: &str) -> Option<MlFeatures> {
    let key = format!("ml:features:latest:{}", symbol);
    let raw: Option<String> = conn.get(&key).await.ok()?;
    let raw = raw?;
    let parsed: serde_json::Value = serde_json::from_str(&raw).ok()?;
    Some(MlFeatures {
        rsi_14: parsed.get("rsi_14").and_then(|v| v.as_f64()).unwrap_or(50.0),
        sma_20: parsed.get("sma_20").and_then(|v| v.as_f64()).unwrap_or(0.0),
        price: parsed.get("price").and_then(|v| v.as_f64()).unwrap_or(0.0),
        timestamp: parsed.get("timestamp").and_then(|v| v.as_i64()).unwrap_or(0),
        ml_prob_up: parsed.get("ml_prob_up").and_then(|v| v.as_f64()),
        ml_signal_confidence: parsed.get("ml_signal_confidence").and_then(|v| v.as_f64()),
        ml_signal_edge: parsed.get("ml_signal_edge").and_then(|v| v.as_f64()),
        ml_model: parsed.get("ml_model").and_then(|v| v.as_str()).map(|s| s.to_string()),
        ml_model_ready: parsed
            .get("ml_model_ready")
            .and_then(|v| v.as_bool())
            .unwrap_or(false),
        ml_model_samples: parsed.get("ml_model_samples").and_then(|v| v.as_f64()),
        ml_model_eval_auc: parsed.get("ml_model_eval_auc").and_then(|v| v.as_f64()),
        ml_model_eval_logloss: parsed.get("ml_model_eval_logloss").and_then(|v| v.as_f64()),
        ml_model_eval_brier: parsed.get("ml_model_eval_brier").and_then(|v| v.as_f64()),
    })
}

/// Market regime from backend meta-controller (persisted to Redis).
#[derive(Debug, Clone, PartialEq)]
enum MarketRegime { Trend, MeanRevert, LowLiquidity, Chop, Unknown }

async fn read_market_regime(conn: &mut redis::aio::MultiplexedConnection) -> (MarketRegime, f64) {
    let raw: Option<String> = conn.get("meta_controller:regime").await.unwrap_or(None);
    let raw = match raw { Some(r) => r, None => return (MarketRegime::Unknown, 0.0) };
    let parsed: serde_json::Value = match serde_json::from_str(&raw) { Ok(v) => v, Err(_) => return (MarketRegime::Unknown, 0.0) };
    let regime_str = parsed.get("regime").and_then(|v| v.as_str()).unwrap_or("UNKNOWN");
    let confidence = parsed.get("confidence").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let regime = match regime_str {
        "TREND" => MarketRegime::Trend,
        "MEAN_REVERT" => MarketRegime::MeanRevert,
        "LOW_LIQUIDITY" => MarketRegime::LowLiquidity,
        "CHOP" => MarketRegime::Chop,
        _ => MarketRegime::Unknown,
    };
    (regime, confidence)
}

/// Compute simple momentum from spot history: returns price change over last N minutes.
fn compute_spot_momentum(history: &VecDeque<(i64, f64)>, lookback_ms: i64) -> Option<f64> {
    if history.len() < 5 { return None; }
    let now_ms = history.back()?.0;
    let cutoff = now_ms - lookback_ms;
    // Find the earliest sample within the lookback window.
    let old = history.iter().find(|(ts, _)| *ts >= cutoff)?;
    let latest = history.back()?;
    if old.1 <= 0.0 { return None; }
    Some((latest.1 - old.1) / old.1)
}

pub struct Btc5mLagStrategy {
    client: PolymarketClient,
    latest_spot_price: Arc<RwLock<(f64, i64)>>,
    spot_history: Arc<RwLock<VecDeque<(i64, f64)>>>,
    implied_vol: Arc<RwLock<Option<ImpliedVolSnapshot>>>,
}

impl Btc5mLagStrategy {
    pub fn new() -> Self {
        Self {
            client: PolymarketClient::new(),
            latest_spot_price: Arc::new(RwLock::new((0.0, 0))),
            spot_history: Arc::new(RwLock::new(VecDeque::new())),
            implied_vol: Arc::new(RwLock::new(None)),
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

    fn max_drawdown_pct() -> f64 {
        std::env::var("BTC_5M_MAX_DRAWDOWN_PCT")
            .ok()
            .and_then(|value| value.trim().parse::<f64>().ok())
            .filter(|value| value.is_finite() && *value <= 0.0 && *value >= -100.0)
            .unwrap_or(DEFAULT_MAX_DRAWDOWN_PCT)
    }

    fn take_profit_min_return() -> f64 {
        std::env::var("BTC_5M_TAKE_PROFIT_MIN_RETURN")
            .ok()
            .and_then(|value| value.trim().parse::<f64>().ok())
            .filter(|value| value.is_finite() && *value >= 0.01 && *value <= 5.0)
            .unwrap_or(DEFAULT_TAKE_PROFIT_MIN_RETURN)
    }

    fn take_profit_min_bid() -> f64 {
        std::env::var("BTC_5M_TAKE_PROFIT_MIN_BID")
            .ok()
            .and_then(|value| value.trim().parse::<f64>().ok())
            .filter(|value| value.is_finite() && *value >= 0.50 && *value <= 0.99)
            .unwrap_or(DEFAULT_TAKE_PROFIT_MIN_BID)
    }

    fn take_profit_min_hold_ms() -> i64 {
        std::env::var("BTC_5M_TAKE_PROFIT_MIN_HOLD_MS")
            .ok()
            .and_then(|value| value.trim().parse::<i64>().ok())
            .filter(|value| *value >= 0 && *value <= 300_000)
            .unwrap_or(DEFAULT_TAKE_PROFIT_MIN_HOLD_MS)
    }

    fn live_order_type() -> String {
        let raw = std::env::var("BTC_5M_LIVE_ORDER_TYPE").unwrap_or_else(|_| "FAK".to_string());
        let normalized = raw.trim().to_ascii_uppercase();
        match normalized.as_str() {
            "FOK" | "FAK" | "GTC" | "GTD" => normalized,
            _ => "FAK".to_string(),
        }
    }

    fn theta_sniper_enabled() -> bool {
        std::env::var("BTC_5M_THETA_SNIPER_ENABLED")
            .ok()
            .map(|value| value != "0" && value.to_lowercase() != "false")
            .unwrap_or(true)
    }

    fn theta_sniper_window_secs() -> i64 {
        std::env::var("BTC_5M_THETA_SNIPER_WINDOW_SECS")
            .ok()
            .and_then(|value| value.trim().parse::<i64>().ok())
            .filter(|value| *value >= 10 && *value <= 180)
            .unwrap_or(DEFAULT_THETA_SNIPER_WINDOW_SECS)
    }

    fn theta_sniper_min_divergence() -> f64 {
        std::env::var("BTC_5M_THETA_SNIPER_MIN_DIVERGENCE_BPS")
            .ok()
            .and_then(|value| value.trim().parse::<f64>().ok())
            .filter(|value| value.is_finite() && *value >= 100.0 && *value <= 5_000.0)
            .map(|value| value / 10_000.0)
            .unwrap_or(DEFAULT_THETA_SNIPER_MIN_DIVERGENCE_BPS / 10_000.0)
    }

    fn theta_sniper_max_book_age_ms() -> i64 {
        std::env::var("BTC_5M_THETA_SNIPER_MAX_BOOK_AGE_MS")
            .ok()
            .and_then(|value| value.trim().parse::<i64>().ok())
            .filter(|value| *value >= 100 && *value <= 3_000)
            .unwrap_or(DEFAULT_THETA_SNIPER_MAX_BOOK_AGE_MS)
    }

    fn theta_sniper_size_mult() -> f64 {
        std::env::var("BTC_5M_THETA_SNIPER_SIZE_MULT")
            .ok()
            .and_then(|value| value.trim().parse::<f64>().ok())
            .filter(|value| value.is_finite() && *value > 0.05 && *value <= 1.0)
            .unwrap_or(DEFAULT_THETA_SNIPER_SIZE_MULT)
    }

    fn theta_sniper_extra_edge() -> f64 {
        std::env::var("BTC_5M_THETA_SNIPER_EXTRA_EDGE_BPS")
            .ok()
            .and_then(|value| value.trim().parse::<f64>().ok())
            .filter(|value| value.is_finite() && *value >= 0.0 && *value <= 500.0)
            .map(|value| value / 10_000.0)
            .unwrap_or(DEFAULT_THETA_SNIPER_EXTRA_EDGE_BPS / 10_000.0)
    }

    fn min_top5_ask_depth_usd() -> f64 {
        std::env::var("BTC_5M_MIN_TOP5_ASK_DEPTH_USD")
            .ok()
            .and_then(|value| value.trim().parse::<f64>().ok())
            .filter(|value| value.is_finite() && *value >= 10.0 && *value <= 10_000.0)
            .unwrap_or(DEFAULT_MIN_TOP5_ASK_DEPTH_USD)
    }

    fn maker_entry_enabled() -> bool {
        std::env::var("BTC_5M_MAKER_ENTRY_ENABLED")
            .ok()
            .map(|value| value != "0" && value.to_lowercase() != "false")
            .unwrap_or(true)
    }

    fn maker_entry_max_edge() -> f64 {
        std::env::var("BTC_5M_MAKER_ENTRY_MAX_EDGE_BPS")
            .ok()
            .and_then(|value| value.trim().parse::<f64>().ok())
            .filter(|value| value.is_finite() && *value >= 10.0 && *value <= 500.0)
            .map(|value| value / 10_000.0)
            .unwrap_or(DEFAULT_MAKER_ENTRY_MAX_EDGE_BPS / 10_000.0)
    }

    fn maker_entry_min_time_remaining_secs() -> i64 {
        std::env::var("BTC_5M_MAKER_ENTRY_MIN_TIME_REMAINING_SECS")
            .ok()
            .and_then(|value| value.trim().parse::<i64>().ok())
            .filter(|value| *value >= 30 && *value <= 240)
            .unwrap_or(DEFAULT_MAKER_ENTRY_MIN_TIME_REMAINING_SECS)
    }

    fn maker_entry_min_fill_prob() -> f64 {
        std::env::var("BTC_5M_MAKER_ENTRY_MIN_FILL_PROB")
            .ok()
            .and_then(|value| value.trim().parse::<f64>().ok())
            .filter(|value| value.is_finite() && *value >= 0.05 && *value <= 0.95)
            .unwrap_or(DEFAULT_MAKER_ENTRY_MIN_FILL_PROB)
    }
}

#[async_trait]
impl Strategy for Btc5mLagStrategy {
    async fn run(&self, redis_client: redis::Client) {
        info!("Starting BTC 5m lag engine [Coinbase vs Polymarket 5m odds]...");

        let mut conn = match redis_client.get_multiplexed_async_connection().await {
            Ok(c) => c,
            Err(e) => {
                error!("Redis connect failed: {}", e);
                return;
            }
        };
        let cost_model = SimCostModel::from_env();
        let variant = strategy_variant();
        let mut last_seen_reset_ts = 0_i64;
        let spot_max_age_ms = max_spot_age_ms();
        let book_max_age_ms = max_book_age_ms();
        let live_preview_cooldown_ms = live_preview_cooldown_ms();
        let sigma_iv_weight = iv_blend_weight();
        let sigma_iv_max_stale_ms = iv_max_stale_ms();
        let take_profit_min_hold_ms = Self::take_profit_min_hold_ms();
        let take_profit_min_return = Self::take_profit_min_return();
        let take_profit_min_bid = Self::take_profit_min_bid();
        let live_order_type = Self::live_order_type();
        let theta_sniper_enabled = Self::theta_sniper_enabled();
        let theta_sniper_window_secs = Self::theta_sniper_window_secs();
        let theta_sniper_min_divergence = Self::theta_sniper_min_divergence();
        let theta_sniper_max_book_age_ms = Self::theta_sniper_max_book_age_ms();
        let theta_sniper_size_mult = Self::theta_sniper_size_mult();
        let theta_sniper_extra_edge = Self::theta_sniper_extra_edge();
        let min_top5_ask_depth_usd = Self::min_top5_ask_depth_usd();
        let maker_entry_enabled = Self::maker_entry_enabled();
        let maker_entry_max_edge = Self::maker_entry_max_edge();
        let maker_entry_min_time_remaining_secs = Self::maker_entry_min_time_remaining_secs();
        let maker_entry_min_fill_prob = Self::maker_entry_min_fill_prob();

        // 3E: Consecutive loss circuit breaker (local)
        let mut consecutive_losses: u32 = 0;
        const MAX_CONSECUTIVE_LOSSES: u32 = 3;

        // Cross-exchange price + micro-liquidity feeds (Coinbase + public CEX tickers).
        // Used for UI telemetry + "cross-exchange signal flow" and can be used to harden the spot signal.
        let cex_quotes: Arc<RwLock<HashMap<&'static str, CexVenueQuote>>> = Arc::new(RwLock::new(HashMap::new()));

        // Coinbase ticker feed (primary + also mirrored into cex_quotes).
        let spot_writer = self.latest_spot_price.clone();
        let spot_history_writer = self.spot_history.clone();
        let cex_coinbase_writer = cex_quotes.clone();
        let coinbase_ws_url = coinbase_ws_url();
        let coinbase_subscriptions = coinbase_ticker_subscriptions(&coinbase_ws_url);

        spawn_supervised_feed_task("BTC_5M_COINBASE", move || {
            spawn_coinbase_feed(
                spot_writer.clone(),
                spot_history_writer.clone(),
                cex_coinbase_writer.clone(),
                coinbase_ws_url.clone(),
                coinbase_subscriptions.clone(),
            )
        });

        // Additional public CEX feeds (no auth).
        let binance_prices = cex_quotes.clone();
        spawn_supervised_feed_task("BTC_5M_BINANCE", move || {
            spawn_binance_feed(binance_prices.clone())
        });
        let okx_prices = cex_quotes.clone();
        spawn_supervised_feed_task("BTC_5M_OKX", move || {
            spawn_okx_feed(okx_prices.clone())
        });
        let bybit_prices = cex_quotes.clone();
        spawn_supervised_feed_task("BTC_5M_BYBIT", move || {
            spawn_bybit_feed(bybit_prices.clone())
        });
        let kraken_prices = cex_quotes.clone();
        spawn_supervised_feed_task("BTC_5M_KRAKEN", move || {
            spawn_kraken_feed(kraken_prices.clone())
        });
        let bitfinex_prices = cex_quotes.clone();
        spawn_supervised_feed_task("BTC_5M_BITFINEX", move || {
            spawn_bitfinex_feed(bitfinex_prices.clone())
        });
        let iv_spot = self.latest_spot_price.clone();
        let iv_sink = self.implied_vol.clone();
        spawn_supervised_feed_task("BTC_5M_DERIBIT_IV", move || {
            spawn_deribit_iv_feed("BTC", iv_spot.clone(), iv_sink.clone())
        });

        // Resume any open PAPER-mode position after restarts so reserved notional
        // is released/settled and the UI can hydrate open positions correctly.
        let mut open_position: Option<Position> = load_open_position(&mut conn, self.strategy_id()).await;
        if let Some(pos) = open_position.as_ref() {
            info!(
                "BTC_5M restored open position {} (side={}, notional=${:.2})",
                pos.execution_id,
                side_label(pos.side),
                pos.notional_usd
            );
        }
        let mut last_live_preview_ms = 0_i64;
        let mut last_hold_scan_ms = 0_i64;

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
                    if is_transient_ws_error(&e) {
                        warn!("BTC_5M Polymarket WS connect transient failure: {}", e);
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

            let mut interval = tokio::time::interval(Duration::from_millis(90));
            let mut book = BinaryBook::default();
            // When resuming after a restart, spot feeds may take a moment to hydrate.
            // If a position expires before we have a usable boundary price, wait briefly
            // before force-settling to avoid leaving reserved notional stuck.
            let mut settlement_wait_start_ms: Option<i64> = None;

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
                            Some(Ok(Message::Close(_))) => break,
                            Some(Ok(Message::Binary(_))) | Some(Ok(Message::Pong(_))) => {}
                            Some(Ok(_)) => {}
                            Some(Err(e)) => {
                                if is_transient_ws_error(&e) {
                                    warn!("BTC_5M Polymarket WS transient error; reconnecting ({})", e);
                                } else {
                                    error!("BTC_5M Polymarket WS error: {}", e);
                                }
                                break;
                            }
                            None => {
                                warn!("BTC_5M Polymarket WS stream ended");
                                break;
                            }
                        }
                    }
                    _ = interval.tick() => {
                        let now_ms = Utc::now().timestamp_millis();
                        let now_ts = Utc::now().timestamp();
                        let mode = read_trading_mode(&mut conn).await;
                        let reset_ts = read_simulation_reset_ts(&mut conn).await;
                        if reset_ts > last_seen_reset_ts {
                            last_seen_reset_ts = reset_ts;
                            open_position = None;
                            settlement_wait_start_ms = None;
                            last_live_preview_ms = 0;
                            last_hold_scan_ms = 0;
                            consecutive_losses = 0;
                            clear_open_position(&mut conn, self.strategy_id()).await;
                            publish_heartbeat(&mut conn, self.heartbeat_id()).await;
                            continue;
                        }

                        if now_ts >= expiry_ts {
                            if mode == TradingMode::Live {
                                if let Some(pos) = open_position.take() {
                                    let _ = release_sim_notional_for_strategy(&mut conn, self.strategy_id(), pos.notional_usd).await;
                                }
                                clear_open_position(&mut conn, self.strategy_id()).await;
                                publish_heartbeat(&mut conn, self.heartbeat_id()).await;
                                break;
                            }

                            let Some(pos) = open_position.as_ref() else {
                                break;
                            };

                            if settlement_wait_start_ms.is_none() {
                                settlement_wait_start_ms = Some(now_ms);
                            }

                            // Resolve against a price close to the 5m boundary to avoid drifting
                            // the outcome if the strategy loop stalls.
                            let expiry_ms = pos.expiry_ts * 1000;
                            let (spot, spot_ts_ms) = *self.latest_spot_price.read().await;
                            let history = self.spot_history.read().await;
                            let boundary_spot = spot_near_window_start(&history, expiry_ms, WINDOW_START_SPOT_MAX_SKEW_MS)
                                .map(|(_, px)| px);
                            drop(history);

                            let near_boundary = (now_ms - expiry_ms).abs() <= WINDOW_START_SPOT_MAX_SKEW_MS;
                            let fresh_spot = spot > 0.0 && now_ms - spot_ts_ms <= spot_max_age_ms;
                            let mut end_spot = boundary_spot
                                .or(if near_boundary && fresh_spot { Some(spot) } else { None })
                                .unwrap_or(0.0);

                            if end_spot <= 0.0 {
                                // Fall back to reliability-weighted median if Coinbase is unavailable.
                                let cex_snapshot = { cex_quotes.read().await.clone() };
                                let venue_entries: Vec<(&str, f64, i64)> = cex_snapshot
                                    .iter()
                                    .map(|(venue, quote)| (*venue, quote.price, quote.timestamp_ms))
                                    .collect();
                                end_spot = reliability_weighted_median(&venue_entries, now_ms, spot_max_age_ms)
                                    .unwrap_or(0.0);
                            }

                            if end_spot <= 0.0 {
                                let waited_ms = now_ms - settlement_wait_start_ms.unwrap_or(now_ms);
                                if waited_ms < SETTLEMENT_SPOT_TIMEOUT_MS {
                                    publish_heartbeat(&mut conn, self.heartbeat_id()).await;
                                    continue;
                                }
                                // Capture position data before dropping the borrow.
                                let execution_id = pos.execution_id.clone();
                                let notional_usd = pos.notional_usd;
                                let entry_price = pos.entry_price;
                                let window_start_spot = pos.window_start_spot;
                                let window_start_ts = pos.window_start_ts;
                                let expiry_ts_pos = pos.expiry_ts;
                                let fair_yes = pos.fair_yes;
                                let sigma = pos.sigma;
                                let side = pos.side;
                                let token_id = pos.token_id.clone();
                                let condition_id = pos.condition_id.clone();
                                let slug = pos.slug.clone();
                                let entry_ts_ms = pos.entry_ts_ms;

                                // 3C: Use last known spot instead of settling at $0 P&L.
                                let fallback_spot = {
                                    let (coinbase_px, _) = *self.latest_spot_price.read().await;
                                    if coinbase_px > 0.0 {
                                        coinbase_px
                                    } else {
                                        // Try cross-exchange median as a last resort.
                                        let cex_snap = { cex_quotes.read().await.clone() };
                                        let cex_vals: Vec<f64> = cex_snap.values()
                                            .filter(|quote| quote.price > 0.0)
                                            .map(|quote| quote.price)
                                            .collect();
                                        median_price(cex_vals).unwrap_or(0.0)
                                    }
                                };

                                let (timeout_pnl, timeout_won, timeout_gross, timeout_net, timeout_end_spot_json) = if fallback_spot > 0.0 {
                                    let (won, gross_return) = resolve_position_return(side, entry_price, window_start_spot, fallback_spot);
                                    let net_return = gross_return - cost_model.round_trip_cost_rate();
                                    let pnl = notional_usd * net_return;
                                    warn!(
                                        "BTC_5M settlement timeout: using fallback spot {:.2} for {} (pnl={:.2})",
                                        fallback_spot, execution_id, pnl
                                    );
                                    (pnl, won, gross_return, net_return, serde_json::json!(fallback_spot))
                                } else {
                                    warn!(
                                        "BTC_5M settlement missing spot after {}ms; recording timeout for {} with $0 PnL",
                                        waited_ms, execution_id
                                    );
                                    (0.0, false, 0.0, 0.0, serde_json::Value::Null)
                                };

                                clear_open_position(&mut conn, self.strategy_id()).await;
                                open_position = None;

                                // 3E: Update consecutive loss counter on timeout settlement
                                if timeout_won { consecutive_losses = 0; } else { consecutive_losses += 1; }
                                let new_bankroll = settle_sim_position_for_strategy(&mut conn, self.strategy_id(), notional_usd, timeout_pnl).await;
                                let ts_ms = Utc::now().timestamp_millis();
                                let hold_ms = ts_ms - entry_ts_ms;
                                let side_label = match side { Side::Up => "UP", Side::Down => "DOWN" };

                                let pnl_msg = serde_json::json!({
                                    "execution_id": execution_id.clone(),
                                    "strategy": self.strategy_id(),
                                    "variant": variant.as_str(),
                                    "pnl": timeout_pnl,
                                    "notional": notional_usd,
                                    "timestamp": ts_ms,
                                    "bankroll": new_bankroll,
                                    "mode": "PAPER",
                                    "details": {
                                        "action": "SETTLEMENT_TIMEOUT",
                                        "side": side_label,
                                        "won": timeout_won,
                                        "entry_price": entry_price,
                                        "window_start_spot": window_start_spot,
                                        "end_spot": timeout_end_spot_json.clone(),
                                        "window_start_ts": window_start_ts,
                                        "expiry_ts": expiry_ts_pos,
                                        "fair_yes": fair_yes,
                                        "sigma_annualized": sigma,
                                        "hold_ms": hold_ms,
                                        "gross_return": timeout_gross,
                                        "net_return": timeout_net,
                                        "roi": format!("{:.2}%", timeout_net * 100.0),
                                        "per_side_cost_rate": cost_model.per_side_cost_rate(),
                                        "token_id": token_id,
                                        "condition_id": condition_id,
                                        "slug": slug.clone(),
                                    }
                                });
                                publish_event(&mut conn, "strategy:pnl", pnl_msg.to_string()).await;

                                let settle_msg = serde_json::json!({
                                    "execution_id": execution_id,
                                    "market": "BTC 5m Engine",
                                    "side": if timeout_won { "TIMEOUT_WIN" } else { "TIMEOUT" },
                                    "price": entry_price,
                                    "size": notional_usd,
                                    "timestamp": ts_ms,
                                    "mode": "PAPER",
                                    "details": {
                                        "strategy": self.strategy_id(),
                                        "position_side": side_label,
                                        "pnl": timeout_pnl,
                                        "net_return": timeout_net,
                                        "hold_ms": hold_ms,
                                        "window_start_spot": window_start_spot,
                                        "end_spot": timeout_end_spot_json,
                                        "slug": slug,
                                    }
                                });
                                publish_execution_event(&mut conn, settle_msg).await;

                                publish_heartbeat(&mut conn, self.heartbeat_id()).await;
                                break;
                            }

                            // We have an end price, settle the position.
                            let pos = match open_position.take() {
                                Some(p) => p,
                                None => break,
                            };
                            // Clear persisted position BEFORE settlement so a crash during
                            // settle_sim_position cannot cause double-settlement on restart.
                            clear_open_position(&mut conn, self.strategy_id()).await;

                            let (won, gross_return) = resolve_position_return(pos.side, pos.entry_price, pos.window_start_spot, end_spot);
                            let net_return = gross_return - cost_model.round_trip_cost_rate();
                            let pnl = pos.notional_usd * net_return;
                            // 3E: Update consecutive loss counter
                            if won { consecutive_losses = 0; } else { consecutive_losses += 1; }
                            let new_bankroll = settle_sim_position_for_strategy(&mut conn, self.strategy_id(), pos.notional_usd, pnl).await;
                            let ts_ms = Utc::now().timestamp_millis();
                            let hold_ms = ts_ms - pos.entry_ts_ms;
                            let side_label = match pos.side { Side::Up => "UP", Side::Down => "DOWN" };
                            let execution_id = pos.execution_id.clone();

                            let pnl_msg = serde_json::json!({
                                "execution_id": execution_id.clone(),
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
                            publish_event(&mut conn, "strategy:pnl", pnl_msg.to_string()).await;

                            let settle_msg = serde_json::json!({
                                "execution_id": execution_id,
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
                            publish_execution_event(&mut conn, settle_msg).await;
                            break;
                        }

                        if !is_strategy_enabled(&mut conn, self.strategy_id()).await {
                            if let Some(pos) = open_position.take() {
                                let _ = release_sim_notional_for_strategy(&mut conn, self.strategy_id(), pos.notional_usd).await;
                            }
                            clear_open_position(&mut conn, self.strategy_id()).await;
                            publish_heartbeat(&mut conn, self.heartbeat_id()).await;
                            continue;
                        }

                        // Risk guard: post-loss cooldown — skip if backend set a cooldown.
                        if open_position.is_none() {
                            let cooldown_until = read_risk_guard_cooldown(&mut conn, self.strategy_id()).await;
                            if cooldown_until > 0 && now_ms < cooldown_until {
                                if now_ms - last_hold_scan_ms >= 1000 {
                                    let spot_hint = self.latest_spot_price.read().await.0;
                                    let scan_msg = build_scan_payload(
                                        &target_market.market_id,
                                        "BTC 5m Engine",
                                        self.strategy_id(),
                                        "WINDOW_EXPECTED_RETURN",
                                        "RATIO",
                                        [spot_hint.max(0.0), 0.5],
                                        0.5,
                                        "FAIR_PROB",
                                        0.0,
                                        Self::expected_net_return_threshold(),
                                        false,
                                        format!("Cooldown hold: risk guard active until {}", cooldown_until),
                                        now_ms,
                                        serde_json::json!({
                                            "warmup_reason": "RISK_GUARD_COOLDOWN",
                                            "cooldown_until": cooldown_until,
                                            "spot": spot_hint,
                                            "slug": target_market.slug,
                                            "window_start_ts": window_start_ts,
                                            "expiry_ts": expiry_ts,
                                        }),
                                    );
                                    publish_event(&mut conn, "arbitrage:scan", scan_msg.to_string()).await;
                                    last_hold_scan_ms = now_ms;
                                }
                                publish_heartbeat(&mut conn, self.heartbeat_id()).await;
                                continue;
                            }

                            // Max drawdown enforcement — skip entry if equity drawdown exceeds limit.
                            let max_dd = Self::max_drawdown_pct();
                            if check_drawdown_breached(&mut conn, max_dd).await {
                                warn!("[BTC_5M] Max drawdown ({:.1}%) breached — skipping entry", max_dd);
                                if now_ms - last_hold_scan_ms >= 1000 {
                                    let spot_hint = self.latest_spot_price.read().await.0;
                                    let scan_msg = build_scan_payload(
                                        &target_market.market_id,
                                        "BTC 5m Engine",
                                        self.strategy_id(),
                                        "WINDOW_EXPECTED_RETURN",
                                        "RATIO",
                                        [spot_hint.max(0.0), 0.5],
                                        0.5,
                                        "FAIR_PROB",
                                        0.0,
                                        Self::expected_net_return_threshold(),
                                        false,
                                        format!("Risk hold: max drawdown {:.1}% breached", max_dd),
                                        now_ms,
                                        serde_json::json!({
                                            "warmup_reason": "MAX_DRAWDOWN_BREACH",
                                            "max_drawdown_pct": max_dd,
                                            "spot": spot_hint,
                                            "slug": target_market.slug,
                                            "window_start_ts": window_start_ts,
                                            "expiry_ts": expiry_ts,
                                        }),
                                    );
                                    publish_event(&mut conn, "arbitrage:scan", scan_msg.to_string()).await;
                                    last_hold_scan_ms = now_ms;
                                }
                                publish_heartbeat(&mut conn, self.heartbeat_id()).await;
                                continue;
                            }
                        }

                        if !book.yes.is_valid() || !book.no.is_valid()
                            || now_ms - book.yes_update_ms > book_max_age_ms
                            || now_ms - book.no_update_ms > book_max_age_ms
                        {
                            if now_ms - last_hold_scan_ms >= 1000 {
                                let yes_age_ms = now_ms.saturating_sub(book.yes_update_ms);
                                let no_age_ms = now_ms.saturating_sub(book.no_update_ms);
                                let spot_hint = self.latest_spot_price.read().await.0;
                                let scan_msg = build_scan_payload(
                                    &target_market.market_id,
                                    "BTC 5m Engine",
                                    self.strategy_id(),
                                    "WINDOW_EXPECTED_RETURN",
                                    "RATIO",
                                    [spot_hint.max(0.0), 0.5],
                                    0.5,
                                    "FAIR_PROB",
                                    0.0,
                                    Self::expected_net_return_threshold(),
                                    false,
                                    "Hold: book invalid or stale".to_string(),
                                    now_ms,
                                    serde_json::json!({
                                        "warmup_reason": "BOOK_INVALID_OR_STALE",
                                        "spot": spot_hint,
                                        "yes_valid": book.yes.is_valid(),
                                        "no_valid": book.no.is_valid(),
                                        "yes_age_ms": yes_age_ms,
                                        "no_age_ms": no_age_ms,
                                        "slug": target_market.slug,
                                        "window_start_ts": window_start_ts,
                                        "expiry_ts": expiry_ts,
                                    }),
                                );
                                publish_event(&mut conn, "arbitrage:scan", scan_msg.to_string()).await;
                                last_hold_scan_ms = now_ms;
                            }
                            publish_heartbeat(&mut conn, self.heartbeat_id()).await;
                            continue;
                        }

                        let (raw_spot, spot_ts_ms) = *self.latest_spot_price.read().await;
                        let mut spot = raw_spot;
                        let mut spot_age_for_gate_ms = now_ms.saturating_sub(spot_ts_ms);
                        if spot <= 0.0 || now_ms - spot_ts_ms > spot_max_age_ms {
                            // If Coinbase stalls, continue with the reliability-weighted CEX median
                            // instead of freezing BTC_5M scans and starving downstream consumers.
                            let cex_snapshot = { cex_quotes.read().await.clone() };
                            let venue_entries: Vec<(&str, f64, i64)> = cex_snapshot
                                .iter()
                                .map(|(venue, quote)| (*venue, quote.price, quote.timestamp_ms))
                                .collect();
                            if let Some(fallback_spot) = reliability_weighted_median(&venue_entries, now_ms, spot_max_age_ms) {
                                spot = fallback_spot;
                                if let Some(best_age) = venue_entries
                                    .iter()
                                    .filter(|(_, px, ts_ms)| *px > 0.0 && now_ms.saturating_sub(*ts_ms) <= spot_max_age_ms)
                                    .map(|(_, _, ts_ms)| now_ms.saturating_sub(*ts_ms))
                                    .min()
                                {
                                    spot_age_for_gate_ms = best_age;
                                }
                            } else {
                                publish_heartbeat(&mut conn, self.heartbeat_id()).await;
                                continue;
                            }
                        }

                        // 3D: Early exits for risk control and profit capture.
                        if let Some(ref pos) = open_position {
                            let hold_ms = now_ms - pos.entry_ts_ms;
                            let side_label = match pos.side { Side::Up => "UP", Side::Down => "DOWN" };
                            let execution_id = pos.execution_id.clone();
                            let best_bid = match pos.side {
                                Side::Up => book.yes.best_bid,
                                Side::Down => book.no.best_bid,
                            };
                            let mark_mid = match pos.side {
                                Side::Up => book.yes.mid(),
                                Side::Down => book.no.mid(),
                            };
                            let mut exit_price = if best_bid.is_finite() && best_bid > 0.0 {
                                best_bid
                            } else if mark_mid.is_finite() && mark_mid > 0.0 {
                                mark_mid
                            } else {
                                pos.entry_price
                            };
                            exit_price = exit_price.clamp(0.001, 0.999);
                            let gross_return = ((exit_price - pos.entry_price) / pos.entry_price).max(-0.999);
                            let net_return = (gross_return - cost_model.round_trip_cost_rate()).max(-0.999);
                            let spot_move = if spot > 0.0 && pos.window_start_spot > 0.0 {
                                (spot - pos.window_start_spot) / pos.window_start_spot
                            } else { 0.0 };

                            let adverse_threshold = std::env::var("BTC_5M_EARLY_EXIT_ADVERSE_BPS")
                                .ok().and_then(|v| v.parse::<f64>().ok())
                                .unwrap_or(30.0) / 10_000.0;
                            let adverse = hold_ms > 30_000 && match pos.side {
                                Side::Up => spot_move < -adverse_threshold,
                                Side::Down => spot_move > adverse_threshold,
                            };
                            let take_profit = hold_ms >= take_profit_min_hold_ms
                                && best_bid.is_finite()
                                && best_bid >= take_profit_min_bid
                                && net_return >= take_profit_min_return;

                            if take_profit || adverse {
                                let close_reason = if take_profit { "TAKE_PROFIT" } else { "SIGNAL_SL" };
                                let close_action = if take_profit { "TAKE_PROFIT_EARLY" } else { "EARLY_EXIT" };
                                let settle_side = if take_profit { "TAKE_PROFIT" } else { "EARLY_EXIT" };
                                let pnl = pos.notional_usd * net_return;
                                let new_bankroll = settle_sim_position_for_strategy(&mut conn, self.strategy_id(), pos.notional_usd, pnl).await;
                                if pnl < 0.0 { consecutive_losses = consecutive_losses.saturating_add(1); } else { consecutive_losses = 0; }

                                let pnl_msg = serde_json::json!({
                                    "execution_id": execution_id.clone(),
                                    "strategy": self.strategy_id(),
                                    "variant": variant.as_str(),
                                    "pnl": pnl,
                                    "notional": pos.notional_usd,
                                    "timestamp": now_ms,
                                    "bankroll": new_bankroll,
                                    "mode": "PAPER",
                                    "details": {
                                        "action": close_action,
                                        "reason": close_reason,
                                        "side": side_label,
                                        "entry_price": pos.entry_price,
                                        "exit_price": exit_price,
                                        "spot": spot,
                                        "spot_move": spot_move,
                                        "adverse_threshold": adverse_threshold,
                                        "take_profit_min_bid": take_profit_min_bid,
                                        "take_profit_min_return": take_profit_min_return,
                                        "take_profit_min_hold_ms": take_profit_min_hold_ms,
                                        "window_start_spot": pos.window_start_spot,
                                        "gross_return": gross_return,
                                        "net_return": net_return,
                                        "hold_ms": hold_ms,
                                        "round_trip_cost_rate": cost_model.round_trip_cost_rate(),
                                        "token_id": pos.token_id.clone(),
                                        "condition_id": pos.condition_id.clone(),
                                        "slug": pos.slug.clone(),
                                    }
                                });
                                publish_event(&mut conn, "strategy:pnl", pnl_msg.to_string()).await;

                                let settle_msg = serde_json::json!({
                                    "execution_id": execution_id,
                                    "market": "BTC 5m Engine",
                                    "side": settle_side,
                                    "price": exit_price,
                                    "size": pos.notional_usd,
                                    "timestamp": now_ms,
                                    "mode": "PAPER",
                                    "details": {
                                        "strategy": self.strategy_id(),
                                        "position_side": side_label,
                                        "reason": close_reason,
                                        "pnl": pnl,
                                        "net_return": net_return,
                                        "spot_move": spot_move,
                                        "adverse_threshold": adverse_threshold,
                                        "take_profit_min_bid": take_profit_min_bid,
                                        "take_profit_min_return": take_profit_min_return,
                                        "take_profit_min_hold_ms": take_profit_min_hold_ms,
                                        "hold_ms": hold_ms,
                                        "window_start_spot": pos.window_start_spot,
                                        "end_spot": spot,
                                        "slug": pos.slug.clone(),
                                    }
                                });
                                publish_execution_event(&mut conn, settle_msg).await;
                                clear_open_position(&mut conn, self.strategy_id()).await;
                                open_position = None;

                                if take_profit {
                                    info!(
                                        "[BTC_5M] Early take-profit exit: bid={:.3} pnl={:.2} net_return={:.4} hold_ms={}",
                                        best_bid,
                                        pnl,
                                        net_return,
                                        hold_ms,
                                    );
                                } else {
                                    warn!(
                                        "[BTC_5M] Early exit on adverse signal: spot_move={:.4} pnl={:.2} return={:.4}",
                                        spot_move,
                                        pnl,
                                        net_return,
                                    );
                                }
                                publish_heartbeat(&mut conn, self.heartbeat_id()).await;
                                continue;
                            }
                        }

                        // Reliability-weighted cross-exchange median for fair value.
                        let cex_snapshot = { cex_quotes.read().await.clone() };
                        let mut cex_prices_json = serde_json::Map::new();
                        let mut venue_entries: Vec<(&str, f64, i64)> = Vec::new();
                        for (venue, quote) in cex_snapshot.iter() {
                            if quote.price > 0.0 {
                                cex_prices_json.insert((*venue).to_string(), serde_json::json!(quote.price));
                            }
                            venue_entries.push((*venue, quote.price, quote.timestamp_ms));
                        }
                        let cex_mid = reliability_weighted_median(&venue_entries, now_ms, spot_max_age_ms)
                            .unwrap_or(spot);
                        let cex_volume_signal = compute_cex_volume_signal(&cex_snapshot, now_ms, spot_max_age_ms)
                            .unwrap_or_default();

                        let remaining_seconds = expiry_ts - now_ts;
                        let theta_sniper_active = theta_sniper_enabled
                            && remaining_seconds > 0
                            && remaining_seconds <= theta_sniper_window_secs;
                        if remaining_seconds <= entry_expiry_cutoff_secs() && !theta_sniper_active {
                            publish_heartbeat(&mut conn, self.heartbeat_id()).await;
                            continue;
                        }

                        let window_start_ms = window_start_ts * 1000;
                        let window_start_spot = if let Some(cached) = read_cached_window_start_spot(&mut conn, window_start_ts).await {
                            cached
                        } else {
                            let history = self.spot_history.read().await;
                            match spot_near_window_start(&history, window_start_ms, WINDOW_START_SPOT_MAX_SKEW_MS) {
                                Some((_ts, px)) => {
                                    cache_window_start_spot(&mut conn, window_start_ts, px).await;
                                    px
                                }
                                None => 0.0,
                            }
                        };
                        if window_start_spot <= 0.0 {
                            let scan_msg = build_scan_payload(
                                &target_market.market_id,
                                "BTC 5m Engine",
                                self.strategy_id(),
                                "WINDOW_EXPECTED_RETURN",
                                "RATIO",
                                [spot, 0.5],
                                0.5,
                                "FAIR_PROB",
                                0.0,
                                Self::expected_net_return_threshold(),
                                false,
                                format!(
                                    "Warmup hold: missing window_start_spot for {} (waiting for boundary sample)",
                                    target_market.slug
                                ),
                                now_ms,
                                serde_json::json!({
                                    "spot": spot,
                                    "cex_mid": cex_mid,
                                    "cex_prices": Value::Object(cex_prices_json.clone()),
                                    "window_start_spot": 0.0,
                                    "window_start_spot_ready": false,
                                    "warmup_reason": "WINDOW_START_SPOT_MISSING",
                                    "slug": target_market.slug,
                                    "window_start_ts": window_start_ts,
                                    "expiry_ts": expiry_ts,
                                }),
                            );
                            publish_event(&mut conn, "arbitrage:scan", scan_msg.to_string()).await;
                            publish_heartbeat(&mut conn, self.heartbeat_id()).await;
                            continue;
                        }

                        let tte_years = remaining_seconds as f64 / 31_536_000.0;
                        let realized_sigma = {
                            let history = self.spot_history.read().await;
                            estimate_annualized_vol(&history)
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
                        let sigma = blend_sigma(realized_sigma, implied_sigma, sigma_iv_weight, 0.65);
                        let vol_estimate: Option<f64> = if sigma > 0.0 { Some(sigma) } else { None };

                        // 3F: Volatility circuit breaker — skip entry when volatility is extreme.
                        if open_position.is_none() {
                            if let Some(vol) = vol_estimate {
                                if vol > 2.20 { // extreme volatility fails safe even with blended IV
                                    warn!("[BTC_5M] Volatility circuit breaker: vol={:.2} too high", vol);
                                    publish_heartbeat(&mut conn, self.heartbeat_id()).await;
                                    continue;
                                }
                            }
                        }

                        // 3A: Dynamic edge threshold based on volatility
                        let vol_adjustment = if let Some(vol) = vol_estimate {
                            if vol > 0.80 { -0.001 }     // High vol: lower threshold by 10 bps
                            else if vol < 0.30 { 0.001 } // Low vol: raise threshold by 10 bps
                            else { 0.0 }
                        } else { 0.0 };

                        let fair_yes = calculate_fair_yes(cex_mid, window_start_spot, tte_years, sigma).clamp(0.0, 1.0);
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

                        // Fee-curve-aware cost: Polymarket dynamic taker fee = p*(1-p)*base_rate
                        // plus slippage. Near 0.50 → ~50 bps fee; near extremes → ~0 bps.
                        let fee_curve_rate: f64 = std::env::var("BTC_5M_FEE_CURVE_BASE_RATE")
                            .ok().and_then(|v| v.parse().ok()).unwrap_or(0.02);
                        let slippage_rate = cost_model.slippage_bps_per_side / 10_000.0;
                        let up_taker_fee = polymarket_taker_fee(up_price, fee_curve_rate);
                        let down_taker_fee = polymarket_taker_fee(down_price, fee_curve_rate);
                        let up_net_expected = up_expected - up_taker_fee - slippage_rate;
                        let down_net_expected = down_expected - down_taker_fee - slippage_rate;
                        let min_net_expected = Self::expected_net_return_threshold();

                        let (best_side, best_price, best_prob, best_expected, best_net_expected, best_token, best_spread) = if up_net_expected >= down_net_expected {
                            (Side::Up, up_price, fair_yes, up_expected, up_net_expected, target_market.yes_token.clone(), yes_spread)
                        } else {
                            (Side::Down, down_price, fair_no, down_expected, down_net_expected, target_market.no_token.clone(), no_spread)
                        };
                        let best_side_label = match best_side { Side::Up => "UP", Side::Down => "DOWN" };

                        let best_price_ok = match best_side {
                            Side::Up => up_price_ok,
                            Side::Down => down_price_ok,
                        };
                        let (
                            side_bid_depth_notional,
                            side_ask_depth_notional,
                            side_bid_depth_size,
                            side_ask_depth_size,
                            side_bid_depth_levels,
                            side_ask_depth_levels,
                        ) = match best_side {
                            Side::Up => (
                                book.yes_depth.bid_notional_top5,
                                book.yes_depth.ask_notional_top5,
                                book.yes_depth.bid_size_top5,
                                book.yes_depth.ask_size_top5,
                                book.yes_depth.bid_levels,
                                book.yes_depth.ask_levels,
                            ),
                            Side::Down => (
                                book.no_depth.bid_notional_top5,
                                book.no_depth.ask_notional_top5,
                                book.no_depth.bid_size_top5,
                                book.no_depth.ask_size_top5,
                                book.no_depth.bid_levels,
                                book.no_depth.ask_levels,
                            ),
                        };
                        let side_depth_notional_sum = side_bid_depth_notional + side_ask_depth_notional;
                        let side_depth_imbalance = if side_depth_notional_sum > 0.0 {
                            ((side_bid_depth_notional - side_ask_depth_notional) / side_depth_notional_sum).clamp(-1.0, 1.0)
                        } else {
                            0.0
                        };
                        let side_depth_mid = if (side_bid_depth_size + side_ask_depth_size) > 0.0 {
                            (side_bid_depth_notional + side_ask_depth_notional)
                                / (side_bid_depth_size + side_ask_depth_size)
                        } else {
                            best_price
                        };
                        let depth_ok = side_ask_depth_notional >= min_top5_ask_depth_usd;
                        let yes_book_age_ms = now_ms.saturating_sub(book.yes_update_ms);
                        let no_book_age_ms = now_ms.saturating_sub(book.no_update_ms);

                        let best_taker_entry_cost_rate = match best_side {
                            Side::Up => up_taker_fee + slippage_rate,
                            Side::Down => down_taker_fee + slippage_rate,
                        };
                        let maker_mode_candidate = maker_entry_enabled
                            && remaining_seconds >= maker_entry_min_time_remaining_secs
                            && best_net_expected > 0.0
                            && best_net_expected <= maker_entry_max_edge;
                        let maker_entry_price = (best_price - (best_spread * 0.50)).clamp(0.001, best_price);
                        let side_book_age_ms = match best_side {
                            Side::Up => yes_book_age_ms,
                            Side::Down => no_book_age_ms,
                        };
                        let maker_sample = if maker_mode_candidate {
                            let uniform_sample = ((now_ms.rem_euclid(10_000)) as f64) / 10_000.0;
                            simulate_maker_entry_fill(
                                maker_entry_price,
                                best_spread,
                                side_book_age_ms,
                                best_net_expected,
                                cost_model,
                                uniform_sample,
                            )
                        } else {
                            simulate_maker_entry_fill(
                                maker_entry_price,
                                best_spread,
                                side_book_age_ms,
                                best_net_expected,
                                cost_model,
                                1.0,
                            )
                        };
                        let maker_active = maker_mode_candidate && maker_sample.fill_probability >= maker_entry_min_fill_prob;
                        let best_entry_cost_rate = if maker_active {
                            cost_model.maker_side_cost_rate()
                        } else {
                            best_taker_entry_cost_rate
                        };
                        let effective_best_net_expected = best_expected - best_entry_cost_rate;
                        let freshness_ok_base = spot_age_for_gate_ms <= spot_max_age_ms
                            && yes_book_age_ms <= book_max_age_ms
                            && no_book_age_ms <= book_max_age_ms;
                        let freshness_ok = if theta_sniper_active {
                            freshness_ok_base
                                && spot_age_for_gate_ms <= theta_sniper_max_book_age_ms
                                && yes_book_age_ms <= theta_sniper_max_book_age_ms
                                && no_book_age_ms <= theta_sniper_max_book_age_ms
                        } else {
                            freshness_ok_base
                        };

                        // Divergence-first execution gate. Net expectancy already includes fees/slippage;
                        // this enforces a separate fair-vs-market gap buffer for model uncertainty.
                        let divergence_min_floor = std::env::var("BTC_5M_DIVERGENCE_MIN_FLOOR")
                            .ok()
                            .and_then(|v| v.parse::<f64>().ok())
                            .unwrap_or(0.03)
                            .clamp(0.0, 0.20);
                        let divergence_spread_mult = std::env::var("BTC_5M_DIVERGENCE_SPREAD_MULT")
                            .ok()
                            .and_then(|v| v.parse::<f64>().ok())
                            .unwrap_or(0.40)
                            .clamp(0.0, 2.0);
                        let divergence_vol_mult = std::env::var("BTC_5M_DIVERGENCE_VOL_MULT")
                            .ok()
                            .and_then(|v| v.parse::<f64>().ok())
                            .unwrap_or(0.30)
                            .clamp(0.0, 2.0);
                        let divergence_uncertainty_bps = std::env::var("BTC_5M_DIVERGENCE_UNCERTAINTY_BPS")
                            .ok()
                            .and_then(|v| v.parse::<f64>().ok())
                            .unwrap_or(12.0)
                            .clamp(0.0, 300.0);
                        let sigma_term = (sigma * tte_years.sqrt()).clamp(0.0, 1.0);
                        let required_divergence = (
                            divergence_min_floor
                                + (divergence_uncertainty_bps / 10_000.0)
                                + (divergence_spread_mult * best_spread)
                                + (divergence_vol_mult * sigma_term)
                        )
                            .clamp(0.0, 0.25);
                        let required_divergence = if theta_sniper_active {
                            required_divergence.max(theta_sniper_min_divergence)
                        } else {
                            required_divergence
                        };
                        let fair_market_divergence = (best_prob - best_price).clamp(-1.0, 1.0);
                        let divergence_ok = fair_market_divergence >= required_divergence;

                        // Hard coinflip exclusion zone: entries within this band of 0.50
                        // are NEVER taken. At 0.50, the market correctly prices a coinflip
                        // and our model has no structural edge (proven 36% WR at 0.50).
                        // This is NOT a penalty — it's a hard block.
                        let coinflip_block_band: f64 = std::env::var("BTC_5M_COINFLIP_BLOCK_BAND")
                            .ok().and_then(|v| v.parse().ok()).unwrap_or(0.03);
                        let coinflip_blocked = (best_price - 0.50_f64).abs() < coinflip_block_band;

                        // Graduated edge penalty for prices near (but outside) the hard block zone.
                        // At exactly 0.50: +100 bps extra required; scales to +0 at 0.53+.
                        let fair_price_extra_bps: f64 = std::env::var("BTC_5M_FAIR_PRICE_EXTRA_BPS")
                            .ok().and_then(|v| v.parse().ok()).unwrap_or(100.0);
                        let fair_price_band: f64 = std::env::var("BTC_5M_FAIR_PRICE_BAND")
                            .ok().and_then(|v| v.parse().ok()).unwrap_or(0.03);
                        let fair_price_penalty = {
                            let dist = (best_price - 0.50_f64).abs();
                            if dist < fair_price_band {
                                (fair_price_extra_bps / 10_000.0) * (1.0 - dist / fair_price_band)
                            } else {
                                0.0
                            }
                        };
                        let base_edge_with_vol = (min_net_expected + vol_adjustment).max(0.0);
                        let theta_edge_penalty = if theta_sniper_active {
                            theta_sniper_extra_edge
                        } else {
                            0.0
                        };
                        let effective_min_edge_base = base_edge_with_vol + fair_price_penalty + theta_edge_penalty;

                        // ── Momentum / Trend Alignment Filter ──
                        // Read RSI + SMA from Redis (published by ML service → backend).
                        // If betting against strong momentum, require extra edge.
                        let ml = read_ml_features(&mut conn, "BTC-USD").await;
                        let ml_max_stale_ms: i64 = std::env::var("BTC_5M_ML_FEATURE_MAX_STALE_MS")
                            .ok()
                            .and_then(|v| v.parse().ok())
                            .filter(|v| *v >= 5_000 && *v <= 10 * 60_000)
                            .unwrap_or(ML_FEATURE_MAX_STALE_MS);
                        let ml_age_ms = ml.as_ref().and_then(|f| {
                            if f.timestamp > 0 {
                                Some(now_ms.saturating_sub(f.timestamp))
                            } else {
                                None
                            }
                        });
                        let ml_is_fresh = ml_age_ms
                            .map(|age| age >= 0 && age <= ml_max_stale_ms)
                            .unwrap_or(false);
                        let momentum_enabled: bool = std::env::var("BTC_5M_MOMENTUM_FILTER_ENABLED")
                            .ok().map(|v| v != "0" && v.to_lowercase() != "false").unwrap_or(true);
                        let momentum_rsi_high: f64 = std::env::var("BTC_5M_MOMENTUM_RSI_HIGH")
                            .ok().and_then(|v| v.parse().ok()).unwrap_or(72.0);
                        let momentum_rsi_low: f64 = std::env::var("BTC_5M_MOMENTUM_RSI_LOW")
                            .ok().and_then(|v| v.parse().ok()).unwrap_or(28.0);
                        let momentum_penalty_bps: f64 = std::env::var("BTC_5M_MOMENTUM_PENALTY_BPS")
                            .ok().and_then(|v| v.parse().ok()).unwrap_or(80.0);

                        let mut momentum_penalty: f64 = 0.0;
                        let mut momentum_label = "NONE";
                        if momentum_enabled {
                            if ml_is_fresh {
                                if let Some(features) = ml.as_ref() {
                                let rsi = features.rsi_14;
                                let sma = features.sma_20;
                                let price = features.price;

                                // Rule 1: RSI extremes — if overbought and betting UP, penalty.
                                //                       if oversold and betting DOWN, penalty.
                                let rsi_penalty = match best_side {
                                    Side::Up if rsi > momentum_rsi_high => {
                                        // Overbought: betting UP is contrarian. Scale penalty by how extreme.
                                        let excess = (rsi - momentum_rsi_high) / (100.0 - momentum_rsi_high);
                                        momentum_label = "RSI_OVERBOUGHT";
                                        (momentum_penalty_bps / 10_000.0) * excess.clamp(0.0, 1.0)
                                    }
                                    Side::Down if rsi < momentum_rsi_low => {
                                        // Oversold: betting DOWN is contrarian.
                                        let excess = (momentum_rsi_low - rsi) / momentum_rsi_low;
                                        momentum_label = "RSI_OVERSOLD";
                                        (momentum_penalty_bps / 10_000.0) * excess.clamp(0.0, 1.0)
                                    }
                                    _ => 0.0,
                                };

                                // Rule 2: SMA trend alignment — if price is well below SMA and
                                // betting UP, that's against the trend. Extra penalty.
                                let sma_penalty = if sma > 0.0 && price > 0.0 {
                                    let deviation = (price - sma) / sma; // positive = above SMA
                                    match best_side {
                                        Side::Up if deviation < -0.002 => {
                                            // Price below SMA, betting UP = against trend
                                            if momentum_label == "NONE" { momentum_label = "BELOW_SMA"; }
                                            (momentum_penalty_bps / 2.0 / 10_000.0) * (deviation.abs() / 0.01).clamp(0.0, 1.0)
                                        }
                                        Side::Down if deviation > 0.002 => {
                                            // Price above SMA, betting DOWN = against trend
                                            if momentum_label == "NONE" { momentum_label = "ABOVE_SMA"; }
                                            (momentum_penalty_bps / 2.0 / 10_000.0) * (deviation.abs() / 0.01).clamp(0.0, 1.0)
                                        }
                                        _ => 0.0,
                                    }
                                } else { 0.0 };

                                momentum_penalty = rsi_penalty + sma_penalty;
                                }
                            } else {
                                momentum_label = "ML_STALE_OR_MISSING";
                            }

                            // Rule 3: Raw spot momentum from our own price history.
                            // If BTC moved >0.15% in one direction over the last 10 minutes
                            // and we're betting the opposite way, add penalty.
                            let history = self.spot_history.read().await;
                            let lookback_ms: i64 = 10 * 60 * 1000; // 10 minutes
                            if let Some(mom) = compute_spot_momentum(&history, lookback_ms) {
                                let threshold = 0.0015; // 0.15% in 10 minutes = meaningful move
                                let against_trend = match best_side {
                                    Side::Up => mom < -threshold,   // BTC falling, betting UP
                                    Side::Down => mom > threshold,  // BTC rising, betting DOWN
                                };
                                if against_trend {
                                    let strength = (mom.abs() / 0.005).clamp(0.0, 1.0); // full penalty at 0.5%
                                    let spot_penalty = (momentum_penalty_bps / 10_000.0) * strength;
                                    momentum_penalty += spot_penalty;
                                    if momentum_label == "NONE" { momentum_label = "AGAINST_SPOT_TREND"; }
                                }
                            }
                        }

                        // ── Volatility Regime Sizing Adjustment ──
                        // Read the meta-controller regime. In CHOP or LOW_LIQUIDITY, require extra edge
                        // (effectively reducing position count). BTC_5M is FAIR_VALUE family.
                        let regime_filter_enabled: bool = std::env::var("BTC_5M_REGIME_FILTER_ENABLED")
                            .ok().map(|v| v != "0" && v.to_lowercase() != "false").unwrap_or(true);
                        let regime_chop_penalty_bps: f64 = std::env::var("BTC_5M_REGIME_CHOP_PENALTY_BPS")
                            .ok().and_then(|v| v.parse().ok()).unwrap_or(50.0);

                        let mut regime_penalty: f64 = 0.0;
                        let mut regime_label = "NONE";
                        if regime_filter_enabled {
                            let (regime, confidence) = read_market_regime(&mut conn).await;
                            if confidence >= 0.3 {
                                match regime {
                                    MarketRegime::Chop => {
                                        regime_penalty = regime_chop_penalty_bps / 10_000.0;
                                        regime_label = "CHOP";
                                    }
                                    MarketRegime::LowLiquidity => {
                                        regime_penalty = (regime_chop_penalty_bps * 1.5) / 10_000.0;
                                        regime_label = "LOW_LIQUIDITY";
                                    }
                                    MarketRegime::MeanRevert => {
                                        // BTC_5M is FAIR_VALUE (directional), slightly disfavored
                                        regime_penalty = (regime_chop_penalty_bps * 0.5) / 10_000.0;
                                        regime_label = "MEAN_REVERT";
                                    }
                                    MarketRegime::Trend => {
                                        // FAIR_VALUE thrives in trends — slight bonus (negative penalty)
                                        regime_penalty = -(regime_chop_penalty_bps * 0.3) / 10_000.0;
                                        regime_label = "TREND";
                                    }
                                    MarketRegime::Unknown => {}
                                }
                            }
                        }

                        // ── Spread Quality Gate ──
                        // Tighter spreads = better entry quality. Scale edge requirement by spread.
                        // Full size at spread ≤ 3c. Linear penalty up to max spread.
                        let spread_quality_bps: f64 = std::env::var("BTC_5M_SPREAD_QUALITY_PENALTY_BPS")
                            .ok().and_then(|v| v.parse().ok()).unwrap_or(60.0);
                        let spread_quality_floor: f64 = 0.03; // 3 cents — below this, no penalty
                        let spread_penalty = if best_spread > spread_quality_floor {
                            let excess = (best_spread - spread_quality_floor) / (max_spread - spread_quality_floor).max(0.01);
                            (spread_quality_bps / 10_000.0) * excess.clamp(0.0, 1.0)
                        } else {
                            0.0
                        };

                        // ── Cross-Exchange Volume Divergence Overlay ──
                        // Uses per-venue top-of-book size pressure + activity as a short-horizon momentum prior.
                        let cex_volume_signal_enabled: bool = std::env::var("BTC_5M_CEX_VOLUME_SIGNAL_ENABLED")
                            .ok()
                            .map(|v| v != "0" && v.to_lowercase() != "false")
                            .unwrap_or(true);
                        let cex_volume_min_venues: usize = std::env::var("BTC_5M_CEX_VOLUME_MIN_VENUES")
                            .ok()
                            .and_then(|v| v.parse::<usize>().ok())
                            .filter(|v| *v >= 2 && *v <= 6)
                            .unwrap_or(3);
                        let cex_volume_min_pressure: f64 = std::env::var("BTC_5M_CEX_VOLUME_MIN_PRESSURE")
                            .ok()
                            .and_then(|v| v.parse::<f64>().ok())
                            .unwrap_or(0.10)
                            .clamp(0.02, 0.95);
                        let cex_volume_min_activity_ratio: f64 = std::env::var("BTC_5M_CEX_VOLUME_MIN_ACTIVITY_RATIO")
                            .ok()
                            .and_then(|v| v.parse::<f64>().ok())
                            .unwrap_or(0.90)
                            .clamp(0.50, 5.0);
                        let cex_volume_bonus_bps: f64 = std::env::var("BTC_5M_CEX_VOLUME_BONUS_BPS")
                            .ok()
                            .and_then(|v| v.parse::<f64>().ok())
                            .unwrap_or(35.0)
                            .clamp(0.0, 300.0);
                        let cex_volume_penalty_bps: f64 = std::env::var("BTC_5M_CEX_VOLUME_PENALTY_BPS")
                            .ok()
                            .and_then(|v| v.parse::<f64>().ok())
                            .unwrap_or(45.0)
                            .clamp(0.0, 400.0);
                        let mut cex_volume_signal_penalty = 0.0;
                        let mut cex_volume_signal_bonus = 0.0;
                        let mut cex_volume_signal_label = "NONE";
                        if cex_volume_signal_enabled {
                            let pressure = cex_volume_signal.pressure;
                            let activity_ratio = cex_volume_signal.activity_ratio;
                            let strong = cex_volume_signal.venues >= cex_volume_min_venues
                                && pressure.abs() >= cex_volume_min_pressure
                                && activity_ratio >= cex_volume_min_activity_ratio;
                            if strong {
                                let aligned = match best_side {
                                    Side::Up => pressure > 0.0,
                                    Side::Down => pressure < 0.0,
                                };
                                let pressure_scale = (pressure.abs() / 0.60).clamp(0.0, 1.0);
                                let activity_scale = (activity_ratio / 2.0).clamp(0.5, 1.5);
                                if aligned {
                                    cex_volume_signal_bonus = (cex_volume_bonus_bps / 10_000.0)
                                        * pressure_scale
                                        * activity_scale;
                                    cex_volume_signal_label = "CEX_VOLUME_ALIGNED";
                                } else {
                                    cex_volume_signal_penalty = (cex_volume_penalty_bps / 10_000.0)
                                        * pressure_scale
                                        * activity_scale;
                                    cex_volume_signal_label = "CEX_VOLUME_DIVERGENT";
                                }
                            } else {
                                cex_volume_signal_label = "CEX_VOLUME_WEAK";
                            }
                        }

                        // ── Polymarket L2 Depth Signal ──
                        // Reward when near-side book has stronger bid support; penalize when ask overhang dominates.
                        let depth_signal_enabled: bool = std::env::var("BTC_5M_DEPTH_SIGNAL_ENABLED")
                            .ok()
                            .map(|v| v != "0" && v.to_lowercase() != "false")
                            .unwrap_or(true);
                        let depth_signal_bonus_bps: f64 = std::env::var("BTC_5M_DEPTH_SIGNAL_BONUS_BPS")
                            .ok()
                            .and_then(|v| v.parse::<f64>().ok())
                            .unwrap_or(30.0)
                            .clamp(0.0, 300.0);
                        let depth_signal_penalty_bps: f64 = std::env::var("BTC_5M_DEPTH_SIGNAL_PENALTY_BPS")
                            .ok()
                            .and_then(|v| v.parse::<f64>().ok())
                            .unwrap_or(35.0)
                            .clamp(0.0, 400.0);
                        let mut depth_signal_penalty = 0.0;
                        let mut depth_signal_bonus = 0.0;
                        let mut depth_signal_label = "NONE";
                        if depth_signal_enabled {
                            let strength = side_depth_imbalance.abs().clamp(0.0, 1.0);
                            if side_depth_imbalance > 0.0 {
                                depth_signal_bonus = (depth_signal_bonus_bps / 10_000.0) * strength;
                                depth_signal_label = "DEPTH_ALIGNED";
                            } else if side_depth_imbalance < -0.05 {
                                depth_signal_penalty = (depth_signal_penalty_bps / 10_000.0) * strength;
                                depth_signal_label = "DEPTH_AGAINST";
                            } else {
                                depth_signal_label = "DEPTH_NEUTRAL";
                            }
                        }

                        // ── ML Model Signal Adjustment ──
                        // Use supervised probability signal as an additive filter:
                        // - disagreement raises threshold (penalty)
                        // - strong alignment can grant a bounded threshold bonus
                        let model_signal_enabled: bool = std::env::var("BTC_5M_MODEL_SIGNAL_FILTER_ENABLED")
                            .ok()
                            .map(|v| v != "0" && v.to_lowercase() != "false")
                            .unwrap_or(true);
                        let model_signal_conf_min: f64 = std::env::var("BTC_5M_MODEL_SIGNAL_CONFIDENCE_MIN")
                            .ok()
                            .and_then(|v| v.parse::<f64>().ok())
                            .unwrap_or(0.22)
                            .clamp(0.05_f64, 0.95_f64);
                        let model_signal_penalty_bps: f64 = std::env::var("BTC_5M_MODEL_SIGNAL_PENALTY_BPS")
                            .ok()
                            .and_then(|v| v.parse::<f64>().ok())
                            .unwrap_or(90.0)
                            .clamp(0.0_f64, 500.0_f64);
                        let model_signal_bonus_bps: f64 = std::env::var("BTC_5M_MODEL_SIGNAL_BONUS_BPS")
                            .ok()
                            .and_then(|v| v.parse::<f64>().ok())
                            .unwrap_or(35.0)
                            .clamp(0.0_f64, 300.0_f64);
                        let model_signal_min_samples: f64 = std::env::var("BTC_5M_MODEL_SIGNAL_MIN_SAMPLES")
                            .ok()
                            .and_then(|v| v.parse::<f64>().ok())
                            .unwrap_or(240.0)
                            .clamp(40.0_f64, 20_000.0_f64);
                        let model_signal_min_auc: f64 = std::env::var("BTC_5M_MODEL_SIGNAL_MIN_AUC")
                            .ok()
                            .and_then(|v| v.parse::<f64>().ok())
                            .unwrap_or(0.52)
                            .clamp(0.50_f64, 0.95_f64);
                        let model_signal_max_logloss: f64 = std::env::var("BTC_5M_MODEL_SIGNAL_MAX_LOGLOSS")
                            .ok()
                            .and_then(|v| v.parse::<f64>().ok())
                            .unwrap_or(0.72)
                            .clamp(0.40_f64, 1.50_f64);
                        let model_signal_max_brier: f64 = std::env::var("BTC_5M_MODEL_SIGNAL_MAX_BRIER")
                            .ok()
                            .and_then(|v| v.parse::<f64>().ok())
                            .unwrap_or(0.28)
                            .clamp(0.10_f64, 0.50_f64);

                        let mut model_signal_penalty: f64 = 0.0;
                        let mut model_signal_bonus: f64 = 0.0;
                        let mut model_signal_label = "NONE";
                        let mut model_signal_side_prob: f64 = 0.5;
                        let mut model_signal_confidence: f64 = 0.0;
                        let mut model_signal_edge: f64 = 0.0;
                        let mut model_signal_ready = false;
                        let mut model_signal_name = "NONE".to_string();
                        let mut model_signal_quality_ok = false;
                        let mut model_signal_model_auc = 0.0;
                        let mut model_signal_model_logloss = 0.0;
                        let mut model_signal_model_brier = 0.0;
                        let mut model_signal_model_samples = 0.0;

                        if model_signal_enabled {
                            if ml_is_fresh {
                                if let Some(features) = ml.as_ref() {
                                    if let Some(name) = features.ml_model.as_ref() {
                                        model_signal_name = name.clone();
                                    }
                                    model_signal_edge = features.ml_signal_edge.unwrap_or(0.0).clamp(-1.0, 1.0);
                                    model_signal_ready = features.ml_model_ready;
                                    model_signal_model_auc = features.ml_model_eval_auc.unwrap_or(0.0);
                                    model_signal_model_logloss = features.ml_model_eval_logloss.unwrap_or(0.0);
                                    model_signal_model_brier = features.ml_model_eval_brier.unwrap_or(0.0);
                                    model_signal_model_samples = features.ml_model_samples.unwrap_or(0.0);

                                    if features.ml_model_ready {
                                        let samples_ok = features
                                            .ml_model_samples
                                            .map(|value| value >= model_signal_min_samples)
                                            .unwrap_or(false);
                                        let auc_ok = features
                                            .ml_model_eval_auc
                                            .map(|value| value >= model_signal_min_auc)
                                            .unwrap_or(false);
                                        let logloss_ok = features
                                            .ml_model_eval_logloss
                                            .map(|value| value <= model_signal_max_logloss)
                                            .unwrap_or(false);
                                        let brier_ok = features
                                            .ml_model_eval_brier
                                            .map(|value| value <= model_signal_max_brier)
                                            .unwrap_or(false);
                                        model_signal_quality_ok = samples_ok && auc_ok && logloss_ok && brier_ok;

                                        if model_signal_quality_ok {
                                            if let Some(prob_up_raw) = features.ml_prob_up {
                                                let prob_up = prob_up_raw.clamp(0.0, 1.0);
                                                model_signal_side_prob = match best_side {
                                                    Side::Up => prob_up,
                                                    Side::Down => 1.0 - prob_up,
                                                };
                                                let inferred_conf = (model_signal_side_prob - 0.5).abs() * 2.0;
                                                model_signal_confidence = features
                                                    .ml_signal_confidence
                                                    .unwrap_or(inferred_conf)
                                                    .clamp(0.0, 1.0);
                                                if model_signal_confidence >= model_signal_conf_min {
                                                    let conf_scale = ((model_signal_confidence - model_signal_conf_min)
                                                        / (1.0_f64 - model_signal_conf_min).max(0.05_f64))
                                                        .clamp(0.0_f64, 1.0_f64);
                                                    let side_edge = ((model_signal_side_prob - 0.5) * 2.0).clamp(-1.0, 1.0);
                                                    if side_edge >= 0.0 {
                                                        model_signal_bonus =
                                                            (model_signal_bonus_bps / 10_000.0) * side_edge * conf_scale;
                                                        model_signal_label = "MODEL_ALIGNED";
                                                    } else {
                                                        model_signal_penalty =
                                                            (model_signal_penalty_bps / 10_000.0) * (-side_edge) * conf_scale;
                                                        model_signal_label = "MODEL_DISAGREE";
                                                    }
                                                } else {
                                                    model_signal_label = "MODEL_LOW_CONF";
                                                }
                                            } else {
                                                model_signal_label = "MODEL_NO_PROB";
                                            }
                                        } else {
                                            model_signal_label = "MODEL_QUALITY_LOW";
                                        }
                                    } else {
                                        model_signal_label = "MODEL_NOT_READY";
                                    }
                                } else {
                                    model_signal_label = "MODEL_MISSING";
                                }
                            } else {
                                model_signal_label = "MODEL_STALE";
                            }
                        }

                        let total_signal_penalty =
                            momentum_penalty
                                + regime_penalty
                                + spread_penalty
                                + model_signal_penalty
                                + cex_volume_signal_penalty
                                + depth_signal_penalty;
                        let total_signal_bonus =
                            model_signal_bonus + cex_volume_signal_bonus + depth_signal_bonus;
                        let non_negative_signal_penalty = total_signal_penalty.max(0.0);
                        let now_utc = Utc::now();
                        let hour_utc = now_utc.hour();
                        let day_of_week = now_utc.weekday().num_days_from_sunday();
                        let seasonality = seasonality_multiplier(hour_utc, day_of_week);
                        let threshold_before_seasonality =
                            (effective_min_edge_base + non_negative_signal_penalty - total_signal_bonus).max(0.0);
                        // Seasonality > 1.0 means favorable window: lower threshold.
                        let effective_min_edge = (threshold_before_seasonality / seasonality).max(0.0);

                        let passes_threshold = freshness_ok
                            && parity_ok
                            && best_price_ok
                            && divergence_ok
                            && depth_ok
                            && !coinflip_blocked
                            && effective_best_net_expected >= effective_min_edge;

                        // Adaptive sizing: scale down weak/fragile edges and scale toward full
                        // risk-budget when edge quality and model confidence are both strong.
                        let kelly = kelly_fraction_fee_adjusted(best_prob, best_price, best_entry_cost_rate);
                        let kelly_floor: f64 = std::env::var("BTC_5M_KELLY_SCALE_FLOOR")
                            .ok()
                            .and_then(|v| v.parse::<f64>().ok())
                            .unwrap_or(0.20)
                            .clamp(0.05, 0.75);
                        let edge_scale_target_ratio: f64 =
                            std::env::var("BTC_5M_EDGE_SCALE_TARGET_RATIO")
                                .ok()
                                .and_then(|v| v.parse::<f64>().ok())
                                .unwrap_or(1.25)
                                .clamp(1.0, 3.0);
                        let edge_scale_floor: f64 = std::env::var("BTC_5M_EDGE_SCALE_FLOOR")
                            .ok()
                            .and_then(|v| v.parse::<f64>().ok())
                            .unwrap_or(0.40)
                            .clamp(0.10, 1.0);
                        let edge_strength_ratio = if effective_min_edge > 0.0 {
                            (effective_best_net_expected / effective_min_edge).max(0.0)
                        } else if effective_best_net_expected > 0.0 {
                            2.0
                        } else {
                            0.0
                        };
                        let kelly_scale =
                            (kelly_floor + (1.0 - kelly_floor) * kelly).clamp(kelly_floor, 1.0);
                        let edge_scale =
                            (edge_strength_ratio / edge_scale_target_ratio).clamp(edge_scale_floor, 1.0);
                        let mut dynamic_size_scale = (kelly_scale * edge_scale).clamp(0.10, 1.0);
                        if theta_sniper_active {
                            dynamic_size_scale = (dynamic_size_scale * theta_sniper_size_mult).clamp(0.05, 1.0);
                        }

                        let reason = if !freshness_ok {
                            format!(
                                "Freshness gate blocked entry (spot={}ms>{}ms or yes/no age {}ms/{}ms>{}ms)",
                                spot_age_for_gate_ms,
                                spot_max_age_ms,
                                yes_book_age_ms,
                                no_book_age_ms,
                                book_max_age_ms,
                            )
                        } else if !parity_ok {
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
                        } else if !divergence_ok {
                            format!(
                                "{} fair/market divergence {:.1}bps below required {:.1}bps (floor {:.1} + uncertainty {:.1} + spread {:.1} + vol {:.1})",
                                best_side_label,
                                fair_market_divergence * 10_000.0,
                                required_divergence * 10_000.0,
                                divergence_min_floor * 10_000.0,
                                divergence_uncertainty_bps,
                                (divergence_spread_mult * best_spread) * 10_000.0,
                                (divergence_vol_mult * sigma_term) * 10_000.0,
                            )
                        } else if !depth_ok {
                            format!(
                                "{} top-5 ask depth ${:.2} below minimum ${:.2}",
                                best_side_label,
                                side_ask_depth_notional,
                                min_top5_ask_depth_usd,
                            )
                        } else if coinflip_blocked {
                            format!(
                                "{} COINFLIP BLOCKED: price {:.2}c within {:.1}c of 0.50 (no structural edge)",
                                best_side_label,
                                best_price * 100.0,
                                coinflip_block_band * 100.0,
                            )
                        } else if effective_best_net_expected < effective_min_edge {
                            format!(
                                "{} expected net {:.1}bps below adaptive threshold {:.1}bps (base+vol {:.1} + fair {:.1} + signal {:.1} - bonus {:.1}, seasonality {:.2}x)",
                                best_side_label,
                                effective_best_net_expected * 10_000.0,
                                effective_min_edge * 10_000.0,
                                base_edge_with_vol * 10_000.0,
                                fair_price_penalty * 10_000.0,
                                non_negative_signal_penalty * 10_000.0,
                                total_signal_bonus * 10_000.0,
                                seasonality,
                            )
                        } else {
                            format!(
                                "{} expected net {:.1}bps cleared adaptive threshold {:.1}bps (edge ratio {:.2}x)",
                                best_side_label,
                                effective_best_net_expected * 10_000.0,
                                effective_min_edge * 10_000.0,
                                edge_strength_ratio,
                            )
                        };

                        // Encode direction in the scan score sign so peer-consensus logic can reason about UP vs DOWN.
                        //
                        // IMPORTANT: `effective_best_net_expected` can be negative (no edge). We still want the sign to represent
                        // the *direction we'd trade* (UP vs DOWN), so we sign the magnitude.
                        let direction_sign = match best_side {
                            Side::Up => 1.0,
                            Side::Down => -1.0,
                        };
                        let scan_score = direction_sign * effective_best_net_expected.abs();

                        // Expose any open position in the scan telemetry so the UI can hydrate the POSITIONS rail
                        // even if it missed the ENTRY execution log (e.g. page refresh mid-window).
                        let open_position_meta = open_position.as_ref().map(|pos| {
                            let side_label = match pos.side { Side::Up => "UP", Side::Down => "DOWN" };
                            serde_json::json!({
                                "execution_id": pos.execution_id,
                                "side": side_label,
                                "entry_price": pos.entry_price,
                                "notional": pos.notional_usd,
                                "entry_ts_ms": pos.entry_ts_ms,
                                "window_start_ts": pos.window_start_ts,
                                "expiry_ts": pos.expiry_ts,
                            })
                        });

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
                            effective_min_edge,
                            passes_threshold,
                            reason,
                            now_ms,
                            serde_json::json!({
                                "spot": spot,
                                "cex_mid": cex_mid,
                                "cex_prices": Value::Object(cex_prices_json),
                                "open_position": open_position_meta,
                                "window_start_spot": window_start_spot,
                                "fair_yes": fair_yes,
                                "fair_no": fair_no,
                                "sigma_annualized": sigma,
                                "sigma_realized_annualized": realized_sigma.unwrap_or(-1.0),
                                "sigma_implied_annualized": implied_sigma.unwrap_or(-1.0),
                                "sigma_iv_blend_weight": sigma_iv_weight,
                                "sigma_iv_source": iv_snapshot.as_ref().map(|iv| iv.source.clone()).unwrap_or_else(|| "NONE".to_string()),
                                "sigma_iv_age_ms": iv_age_ms,
                                "sigma_iv_fresh": iv_is_fresh,
                                "sigma_iv_sample_count": iv_snapshot.as_ref().map(|iv| iv.sample_count).unwrap_or(0),
                                "sigma_iv_median_tte_years": iv_snapshot.as_ref().map(|iv| iv.median_tte_years).unwrap_or(0.0),
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
                                "best_depth_mid": side_depth_mid,
                                "best_prob": best_prob,
                                "best_expected_roi": best_expected,
                                "best_net_expected_roi": effective_best_net_expected,
                                "best_net_expected_taker_roi": best_net_expected,
                                "entry_style": if maker_active { "MAKER" } else { "TAKER" },
                                "edge_to_spread_ratio": if best_spread > 0.0 {
                                    (effective_best_net_expected / best_spread).max(-20.0).min(20.0)
                                } else {
                                    0.0
                                },
                                "obi": side_depth_imbalance,
                                "buy_pressure": ((cex_volume_signal.pressure + 1.0) / 2.0).clamp(0.0, 1.0),
                                "uptick_ratio": ((cex_volume_signal.pressure + 1.0) / 2.0).clamp(0.0, 1.0),
                                "cluster_confidence": cex_volume_signal.pressure.abs().clamp(0.0, 1.0),
                                "flow_acceleration": (cex_volume_signal.activity_ratio - 1.0).clamp(-2.0, 4.0),
                                "dynamic_cost_rate": best_entry_cost_rate,
                                "momentum_sigma": sigma_term,
                                "fair_market_divergence_bps": fair_market_divergence * 10_000.0,
                                "required_divergence_bps": required_divergence * 10_000.0,
                                "divergence_ok": divergence_ok,
                                "divergence_floor_bps": divergence_min_floor * 10_000.0,
                                "divergence_uncertainty_bps": divergence_uncertainty_bps,
                                "divergence_spread_component_bps": (divergence_spread_mult * best_spread) * 10_000.0,
                                "divergence_vol_component_bps": (divergence_vol_mult * sigma_term) * 10_000.0,
                                "kelly_fraction": kelly,
                                "kelly_entry_cost_bps": best_entry_cost_rate * 10_000.0,
                                "kelly_scale": kelly_scale,
                                "edge_scale": edge_scale,
                                "dynamic_size_scale": dynamic_size_scale,
                                "edge_strength_ratio": edge_strength_ratio,
                                "per_side_cost_rate": cost_model.per_side_cost_rate(),
                                "fee_curve_base_rate": fee_curve_rate,
                                "up_taker_fee_bps": up_taker_fee * 10_000.0,
                                "down_taker_fee_bps": down_taker_fee * 10_000.0,
                                "slippage_rate_bps": slippage_rate * 10_000.0,
                                "base_expected_net_return_bps": min_net_expected * 10_000.0,
                                "base_with_vol_threshold_bps": base_edge_with_vol * 10_000.0,
                                "threshold_before_seasonality_bps": threshold_before_seasonality * 10_000.0,
                                "seasonality_multiplier": seasonality,
                                "seasonality_hour_utc": hour_utc,
                                "seasonality_day_of_week": day_of_week,
                                "min_expected_net_return": effective_min_edge,
                                "coinflip_blocked": coinflip_blocked,
                                "coinflip_block_band": coinflip_block_band,
                                "freshness_ok": freshness_ok,
                                "theta_sniper_active": theta_sniper_active,
                                "theta_sniper_window_secs": theta_sniper_window_secs,
                                "theta_sniper_min_divergence_bps": theta_sniper_min_divergence * 10_000.0,
                                "spot_age_ms": spot_age_for_gate_ms,
                                "spot_age_max_ms": spot_max_age_ms,
                                "yes_book_age_ms": yes_book_age_ms,
                                "no_book_age_ms": no_book_age_ms,
                                "book_age_max_ms": book_max_age_ms,
                                "depth_ok": depth_ok,
                                "depth_min_top5_ask_usd": min_top5_ask_depth_usd,
                                "depth_side_bid_notional_top5": side_bid_depth_notional,
                                "depth_side_ask_notional_top5": side_ask_depth_notional,
                                "depth_side_bid_levels": side_bid_depth_levels,
                                "depth_side_ask_levels": side_ask_depth_levels,
                                "depth_side_imbalance": side_depth_imbalance,
                                "fair_price_penalty_bps": fair_price_penalty * 10_000.0,
                                "momentum_penalty_bps": momentum_penalty * 10_000.0,
                                "momentum_label": momentum_label,
                                "ml_features_fresh": ml_is_fresh,
                                "ml_features_age_ms": ml_age_ms.unwrap_or(-1),
                                "regime_penalty_bps": regime_penalty * 10_000.0,
                                "regime_label": regime_label,
                                "spread_penalty_bps": spread_penalty * 10_000.0,
                                "cex_volume_signal_pressure": cex_volume_signal.pressure,
                                "cex_volume_signal_activity_ratio": cex_volume_signal.activity_ratio,
                                "cex_volume_signal_venues": cex_volume_signal.venues,
                                "cex_volume_signal_label": cex_volume_signal_label,
                                "cex_volume_signal_penalty_bps": cex_volume_signal_penalty * 10_000.0,
                                "cex_volume_signal_bonus_bps": cex_volume_signal_bonus * 10_000.0,
                                "depth_signal_label": depth_signal_label,
                                "depth_signal_penalty_bps": depth_signal_penalty * 10_000.0,
                                "depth_signal_bonus_bps": depth_signal_bonus * 10_000.0,
                                "model_signal_penalty_bps": model_signal_penalty * 10_000.0,
                                "model_signal_bonus_bps": model_signal_bonus * 10_000.0,
                                "model_signal_label": model_signal_label,
                                "model_signal_model": model_signal_name,
                                "model_signal_ready": model_signal_ready,
                                "model_signal_quality_ok": model_signal_quality_ok,
                                "model_signal_model_auc": model_signal_model_auc,
                                "model_signal_model_logloss": model_signal_model_logloss,
                                "model_signal_model_brier": model_signal_model_brier,
                                "model_signal_model_samples": model_signal_model_samples,
                                "model_signal_side_prob": model_signal_side_prob,
                                "model_signal_confidence": model_signal_confidence,
                                "model_signal_edge": model_signal_edge,
                                "total_signal_penalty_bps": total_signal_penalty * 10_000.0,
                                "total_signal_penalty_non_negative_bps": non_negative_signal_penalty * 10_000.0,
                                "total_signal_bonus_bps": total_signal_bonus * 10_000.0,
                                "maker_entry_candidate": maker_mode_candidate,
                                "maker_entry_fill_prob": maker_sample.fill_probability,
                                "maker_entry_min_fill_prob": maker_entry_min_fill_prob,
                                "maker_entry_price": maker_entry_price,
                                "rsi_14": ml.as_ref().map(|f| f.rsi_14).unwrap_or(-1.0),
                                "sma_20": ml.as_ref().map(|f| f.sma_20).unwrap_or(-1.0),
                                "ml_prob_up": ml.as_ref().and_then(|f| f.ml_prob_up).unwrap_or(0.5),
                                "ml_signal_confidence_raw": ml.as_ref().and_then(|f| f.ml_signal_confidence).unwrap_or(0.0),
                                "ml_signal_edge_raw": ml.as_ref().and_then(|f| f.ml_signal_edge).unwrap_or(0.0),
                                "ml_feature_timestamp": ml.as_ref().map(|f| f.timestamp).unwrap_or(0),
                                "slug": target_market.slug,
                                "window_start_ts": window_start_ts,
                                "expiry_ts": expiry_ts,
                                "max_drawdown_pct": Self::max_drawdown_pct(),
                            }),
                        );
                        publish_event(&mut conn, "arbitrage:scan", scan_msg.to_string()).await;

                        let effective_live_order_type = if maker_active {
                            "GTC".to_string()
                        } else {
                            live_order_type.clone()
                        };
                        let intended_entry_price = if maker_active {
                            maker_entry_price
                        } else {
                            best_price
                        };

                        let just_entered_live = entered_live_mode(&mut conn, self.strategy_id(), mode).await;
                        if mode == TradingMode::Live {
                            if just_entered_live {
                                if let Some(pos) = open_position.take() {
                                    let _ = release_sim_notional_for_strategy(&mut conn, self.strategy_id(), pos.notional_usd).await;
                                    clear_open_position(&mut conn, self.strategy_id()).await;
                                }
                            }
                            if passes_threshold && now_ms - last_live_preview_ms >= live_preview_cooldown_ms {
                                let available_cash = read_sim_available_cash(&mut conn).await;
                                let risk_cfg = read_risk_config(&mut conn).await;
                                let raw_size = compute_strategy_bet_size(
                                    &mut conn,
                                    self.strategy_id(),
                                    available_cash,
                                    &risk_cfg,
                                    10.0,
                                    Self::max_position_fraction(),
                                ).await;
                                let size = raw_size * dynamic_size_scale;
                                if size >= 10.0 {
                                    let execution_id = Uuid::new_v4().to_string();
                                    let side_label = match best_side { Side::Up => "UP", Side::Down => "DOWN" };
                                    let preview_msg = serde_json::json!({
                                        "execution_id": execution_id,
                                        "market": "BTC 5m Engine",
                                        "side": format!("LIVE_DRY_RUN_{}", side_label),
                                        "price": intended_entry_price,
                                        "size": size,
                                        "order_type": effective_live_order_type.clone(),
                                        "timestamp": now_ms,
                                        "mode": "LIVE_DRY_RUN",
                                        "details": {
                                            "strategy": self.strategy_id(),
                                            "side": side_label,
                                            "order_type": effective_live_order_type.clone(),
                                            "maker_intent": maker_active,
                                            "spot": spot,
                                            "window_start_spot": window_start_spot,
                                            "fair_yes": fair_yes,
                                            "expected_net_return": effective_best_net_expected,
                                            "kelly_fraction": kelly,
                                            "kelly_entry_cost_bps": best_entry_cost_rate * 10_000.0,
                                            "kelly_scale": kelly_scale,
                                            "edge_scale": edge_scale,
                                            "edge_strength_ratio": edge_strength_ratio,
                                            "dynamic_size_scale": dynamic_size_scale,
                                            "raw_size": raw_size,
                                            "effective_threshold": effective_min_edge,
                                            "spot_age_ms": spot_age_for_gate_ms,
                                            "yes_book_age_ms": yes_book_age_ms,
                                            "no_book_age_ms": no_book_age_ms,
                                            "freshness_ok": freshness_ok,
                                            "fair_market_divergence_bps": fair_market_divergence * 10_000.0,
                                            "required_divergence_bps": required_divergence * 10_000.0,
                                            "divergence_ok": divergence_ok,
                                            "sigma_realized_annualized": realized_sigma.unwrap_or(-1.0),
                                            "sigma_implied_annualized": implied_sigma.unwrap_or(-1.0),
                                            "sigma_iv_fresh": iv_is_fresh,
                                            "sigma_iv_age_ms": iv_age_ms,
                                            "ml_features_age_ms": ml_age_ms.unwrap_or(-1),
                                            "model_signal_label": model_signal_label,
                                            "model_signal_confidence": model_signal_confidence,
                                            "model_signal_bonus_bps": model_signal_bonus * 10_000.0,
                                            "model_signal_penalty_bps": model_signal_penalty * 10_000.0,
                                            "seasonality_multiplier": seasonality,
                                            "sigma_annualized": sigma,
                                            "seconds_to_expiry": remaining_seconds,
                                            "preflight": {
                                                "venue": "POLYMARKET",
                                                "strategy": self.strategy_id(),
                                                "order_type": effective_live_order_type.clone(),
                                                "maker_intent": maker_active,
                                                "orders": [
                                                    {
                                                        "token_id": best_token,
                                                        "condition_id": target_market.market_id.clone(),
                                                        "side": "BUY",
                                                        "price": intended_entry_price,
                                                        "size": size,
                                                        "size_unit": "USD_NOTIONAL"
                                                    }
                                                ]
                                            }
                                        }
                                    });
                                    publish_execution_event(&mut conn, preview_msg).await;
                                    last_live_preview_ms = now_ms;
                                }
                            }
                            publish_heartbeat(&mut conn, self.heartbeat_id()).await;
                            continue;
                        }

                        // 3E: Consecutive loss circuit breaker — skip entry after too many losses.
                        if open_position.is_none() && passes_threshold && consecutive_losses >= MAX_CONSECUTIVE_LOSSES {
                            warn!("[BTC_5M] Circuit breaker: {} consecutive losses, skipping entry", consecutive_losses);
                            consecutive_losses -= 1; // Allow retry next window
                            publish_heartbeat(&mut conn, self.heartbeat_id()).await;
                            continue;
                        }

                        // PAPER mode: enter at most one position per 5m window, hold to resolution.
                        if open_position.is_none() && passes_threshold {
                            let available_cash = read_sim_available_cash(&mut conn).await;
                            let risk_cfg = read_risk_config(&mut conn).await;
                            let raw_size = compute_strategy_bet_size(
                                &mut conn,
                                self.strategy_id(),
                                available_cash,
                                &risk_cfg,
                                10.0,
                                Self::max_position_fraction(),
                            ).await;
                            let size = raw_size * dynamic_size_scale;
                            if size >= 10.0 {
                                let side_label = match best_side { Side::Up => "UP", Side::Down => "DOWN" };
                                let execution_id = Uuid::new_v4().to_string();
                                if maker_active && !maker_sample.filled {
                                    let miss_msg = serde_json::json!({
                                        "execution_id": execution_id.clone(),
                                        "market": "BTC 5m Engine",
                                        "market_id": target_market.market_id.clone(),
                                        "side": "ENTRY_MAKER_MISS",
                                        "price": maker_entry_price,
                                        "size": size,
                                        "timestamp": now_ms,
                                        "mode": "PAPER",
                                        "details": {
                                            "strategy": self.strategy_id(),
                                            "side": side_label,
                                            "entry_style": "MAKER",
                                            "fill_probability": maker_sample.fill_probability,
                                            "min_fill_probability": maker_entry_min_fill_prob,
                                            "expected_net_return": effective_best_net_expected,
                                            "reason": "MAKER_NOT_FILLED",
                                        }
                                    });
                                    publish_execution_event(&mut conn, miss_msg).await;
                                    publish_heartbeat(&mut conn, self.heartbeat_id()).await;
                                    continue;
                                }
                                let executed_entry_price = if maker_active {
                                    maker_sample.execution_price.clamp(0.001, 0.999)
                                } else {
                                    intended_entry_price
                                };
                                let exec_msg = serde_json::json!({
                                    "execution_id": execution_id.clone(),
                                    "market": "BTC 5m Engine",
                                    "market_id": target_market.market_id.clone(),
                                    "side": "ENTRY",
                                    "price": executed_entry_price,
                                    "size": size,
                                    "timestamp": now_ms,
                                    "mode": "PAPER",
                                    "details": {
                                        "strategy": self.strategy_id(),
                                        "side": side_label,
                                        "entry_style": if maker_active { "MAKER" } else { "TAKER" },
                                        "order_type": if maker_active { "GTC" } else { "FOK" },
                                        "token_id": best_token.clone(),
                                        "condition_id": target_market.market_id.clone(),
                                        "spot": spot,
                                        "window_start_spot": window_start_spot,
                                        "fair_yes": fair_yes,
                                        "expected_net_return": effective_best_net_expected,
                                        "kelly_fraction": kelly,
                                        "kelly_entry_cost_bps": best_entry_cost_rate * 10_000.0,
                                        "kelly_scale": kelly_scale,
                                        "edge_scale": edge_scale,
                                        "edge_strength_ratio": edge_strength_ratio,
                                        "dynamic_size_scale": dynamic_size_scale,
                                        "raw_size": raw_size,
                                        "effective_threshold": effective_min_edge,
                                        "spot_age_ms": spot_age_for_gate_ms,
                                        "yes_book_age_ms": yes_book_age_ms,
                                        "no_book_age_ms": no_book_age_ms,
                                        "freshness_ok": freshness_ok,
                                        "fair_market_divergence_bps": fair_market_divergence * 10_000.0,
                                        "required_divergence_bps": required_divergence * 10_000.0,
                                        "divergence_ok": divergence_ok,
                                        "sigma_realized_annualized": realized_sigma.unwrap_or(-1.0),
                                        "sigma_implied_annualized": implied_sigma.unwrap_or(-1.0),
                                        "sigma_iv_fresh": iv_is_fresh,
                                        "sigma_iv_age_ms": iv_age_ms,
                                        "ml_features_age_ms": ml_age_ms.unwrap_or(-1),
                                        "maker_fill_probability": maker_sample.fill_probability,
                                        "maker_adverse_bps": maker_sample.adverse_bps,
                                        "seasonality_multiplier": seasonality,
                                        "sigma_annualized": sigma,
                                        "seconds_to_expiry": remaining_seconds,
                                        "slug": target_market.slug.clone(),
                                    }
                                });
                                publish_execution_event(&mut conn, exec_msg).await;
                                let decision = await_execution_decision(&mut conn, &execution_id).await;
                                if decision != ExecutionDecision::Accepted {
                                    warn!(
                                        "[BTC_5M] Entry blocked by backend decision {:?} execution_id={}",
                                        decision, execution_id
                                    );
                                    publish_heartbeat(&mut conn, self.heartbeat_id()).await;
                                    continue;
                                }

                                if !reserve_sim_notional_for_strategy(&mut conn, self.strategy_id(), size).await {
                                    publish_heartbeat(&mut conn, self.heartbeat_id()).await;
                                    continue;
                                }

                                let pos = Position {
                                    execution_id: execution_id.clone(),
                                    side: best_side,
                                    entry_price: executed_entry_price,
                                    notional_usd: size,
                                    entry_ts_ms: now_ms,
                                    window_start_spot,
                                    window_start_ts,
                                    expiry_ts,
                                    token_id: best_token.clone(),
                                    condition_id: target_market.market_id.clone(),
                                    fair_yes,
                                    sigma,
                                    slug: target_market.slug.clone(),
                                };
                                persist_open_position(&mut conn, self.strategy_id(), &pos).await;
                                open_position = Some(pos);
                            }
                        }

                        publish_heartbeat(&mut conn, self.heartbeat_id()).await;
                    }
                }
            }
        }
    }
}
