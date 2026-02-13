use async_trait::async_trait;
use chrono::Utc;
use futures::{SinkExt, StreamExt};
use log::{error, info, warn};
use redis::AsyncCommands;
use serde_json::Value;
use std::collections::{BTreeMap, VecDeque};
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
    read_risk_config,
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
use crate::strategies::Strategy;

const COINBASE_ADVANCED_WS_URL: &str = "wss://advanced-trade-ws.coinbase.com";
const OBI_SIGNAL_THRESHOLD: f64 = 0.45;
const OBI_STD_MULTIPLIER: f64 = 2.0;
const OBI_MAX_THRESHOLD: f64 = 0.80;
const OBI_ENTRY_BUFFER: f64 = 0.08;
const OBI_TREND_WINDOW: usize = 8;
const MIN_OBI_TREND_DELTA: f64 = 0.015;
const MIN_OBI_PASS_STREAK: u32 = 3;
const SIGNAL_COOLDOWN_MS: i64 = 10_000;
const OBI_DEPTH_LEVELS: usize = 15;
const BOOK_STALE_MS: i64 = 1_500;
const MAX_HOLD_MS: i64 = 20_000;
const TAKE_PROFIT_PCT: f64 = 0.03;
const STOP_LOSS_PCT: f64 = -0.02;
const COINBASE_PRODUCT_ID: &str = "BTC-USD";
const MAX_ENTRY_SPREAD_BPS: f64 = 5.0;
const MIN_ENTRY_PRICE: f64 = 0.15;
const MAX_ENTRY_PRICE: f64 = 0.85;
const MAX_ABS_SETTLEMENT_RETURN: f64 = 0.20;
const MAX_POSITION_FRACTION: f64 = 0.05;
const ENTRY_EXPIRY_CUTOFF_MS: i64 = 30_000;

fn coinbase_ws_url() -> String {
    std::env::var("COINBASE_WS_URL").unwrap_or_else(|_| COINBASE_ADVANCED_WS_URL.to_string())
}

fn coinbase_level2_subscriptions(ws_url: &str) -> Vec<Value> {
    if ws_url.contains("ws-feed.exchange.coinbase.com") {
        vec![serde_json::json!({
            "type": "subscribe",
            "product_ids": [COINBASE_PRODUCT_ID],
            "channels": ["level2_batch", "heartbeat"]
        })]
    } else {
        vec![
            serde_json::json!({
                "type": "subscribe",
                "channel": "level2",
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

#[derive(Debug, Clone, Copy)]
enum PositionSide {
    Long,
    Short,
}

#[derive(Debug, Clone)]
struct Position {
    execution_id: String,
    side: PositionSide,
    entry_mid: f64,
    size: f64,
    timestamp_ms: i64,
    entry_obi: f64,
}

#[derive(Debug, Clone, Default)]
struct LocalOrderBook {
    bids: BTreeMap<i64, f64>,
    asks: BTreeMap<i64, f64>,
    last_update_ms: i64,
}

impl LocalOrderBook {
    fn parse_price_tick(price: f64) -> Option<i64> {
        if !price.is_finite() || price <= 0.0 {
            return None;
        }
        Some((price * 10_000.0).round() as i64)
    }

    fn update(&mut self, side: &str, price: f64, size: f64) {
        let Some(key) = Self::parse_price_tick(price) else {
            return;
        };
        self.last_update_ms = Utc::now().timestamp_millis();

        let normalized_side = side.trim().to_ascii_lowercase();
        // Coinbase Advanced Trade can emit "offer" for asks; support both feeds.
        let is_bid = matches!(normalized_side.as_str(), "buy" | "bid");
        let is_ask = matches!(normalized_side.as_str(), "sell" | "ask" | "offer");
        if !is_bid && !is_ask {
            return;
        }

        if size <= 0.0 {
            if is_bid {
                self.bids.remove(&key);
            } else if is_ask {
                self.asks.remove(&key);
            }
            return;
        }

        if is_bid {
            self.bids.insert(key, size);
        } else if is_ask {
            self.asks.insert(key, size);
        }
    }

    fn calculate_obi(&self, depth_levels: usize) -> f64 {
        let bid_vol: f64 = self.bids.iter().rev().take(depth_levels).map(|(_, v)| *v).sum();
        let ask_vol: f64 = self.asks.iter().take(depth_levels).map(|(_, v)| *v).sum();

        if bid_vol + ask_vol == 0.0 {
            return 0.0;
        }

        (bid_vol - ask_vol) / (bid_vol + ask_vol)
    }

    fn best_bid(&self) -> Option<f64> {
        self.bids.iter().next_back().map(|(k, _)| *k as f64 / 10_000.0)
    }

    fn best_ask(&self) -> Option<f64> {
        self.asks.iter().next().map(|(k, _)| *k as f64 / 10_000.0)
    }

    fn mid(&self) -> Option<f64> {
        let bid = self.best_bid()?;
        let ask = self.best_ask()?;
        if bid > 0.0 && ask > 0.0 {
            Some((bid + ask) / 2.0)
        } else {
            None
        }
    }
}

fn rolling_std(values: &std::collections::VecDeque<f64>) -> f64 {
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

fn apply_coinbase_book_message(
    book: &mut LocalOrderBook,
    payload: &str,
    last_sequence: &mut Option<u64>,
) -> Result<bool, (u64, u64)> {
    let Ok(val) = serde_json::from_str::<Value>(payload) else {
        return Ok(false);
    };
    let sequence = parse_sequence(val.get("sequence_num"))
        .or_else(|| parse_sequence(val.get("sequence")));

    if let Some(seq) = sequence {
        if let Some(prev) = *last_sequence {
            if seq <= prev {
                return Ok(false);
            }
            let expected = prev.saturating_add(1);
            if seq > expected {
                return Err((expected, seq));
            }
        }
        *last_sequence = Some(seq);
    }

    // Legacy Coinbase Exchange level2 feed.
    if val.get("product_id").and_then(|v| v.as_str()) == Some(COINBASE_PRODUCT_ID) {
        let msg_type = val.get("type").and_then(|s| s.as_str()).unwrap_or("");
        if msg_type == "snapshot" {
            book.bids.clear();
            book.asks.clear();

            if let Some(bids) = val.get("bids").and_then(|a| a.as_array()) {
                for item in bids {
                    if let (Some(price), Some(size)) = (parse_number(item.get(0)), parse_number(item.get(1))) {
                        book.update("buy", price, size);
                    }
                }
            }

            if let Some(asks) = val.get("asks").and_then(|a| a.as_array()) {
                for item in asks {
                    if let (Some(price), Some(size)) = (parse_number(item.get(0)), parse_number(item.get(1))) {
                        book.update("sell", price, size);
                    }
                }
            }
            return Ok(true);
        }

        if msg_type == "l2update" {
            if let Some(changes) = val.get("changes").and_then(|a| a.as_array()) {
                for change in changes {
                    if let (Some(side), Some(price), Some(size)) = (
                        change.get(0).and_then(|x| x.as_str()),
                        parse_number(change.get(1)),
                        parse_number(change.get(2)),
                    ) {
                        book.update(side, price, size);
                    }
                }
            }
            return Ok(true);
        }
    }

    // Coinbase Advanced Trade level2 feed.
    if val.get("channel").and_then(|v| v.as_str()) != Some("l2_data") {
        return Ok(false);
    }

    let Some(events) = val.get("events").and_then(|v| v.as_array()) else {
        return Ok(false);
    };

    let mut updated = false;
    for event in events {
        if event.get("product_id").and_then(|v| v.as_str()) != Some(COINBASE_PRODUCT_ID) {
            continue;
        }

        if event.get("type").and_then(|v| v.as_str()) == Some("snapshot") {
            book.bids.clear();
            book.asks.clear();
        }

        if let Some(updates) = event.get("updates").and_then(|v| v.as_array()) {
            for update in updates {
                let side = update.get("side").and_then(|v| v.as_str()).unwrap_or_default();
                let price = parse_number(update.get("price_level"));
                let size = parse_number(update.get("new_quantity"));
                if let (Some(price), Some(size)) = (price, size) {
                    book.update(side, price, size);
                    updated = true;
                }
            }
        }
    }

    Ok(updated)
}

pub struct ObiScalperStrategy {
    client: PolymarketClient,
    book: Arc<RwLock<LocalOrderBook>>,
}

impl ObiScalperStrategy {
    pub fn new() -> Self {
        Self {
            client: PolymarketClient::new(),
            book: Arc::new(RwLock::new(LocalOrderBook::default())),
        }
    }
}

#[async_trait]
impl Strategy for ObiScalperStrategy {
    async fn run(&self, redis_client: redis::Client) {
        info!("Starting OBI Scalper [Coinbase level2 order book]");

        let mut conn = match redis_client.get_async_connection().await {
            Ok(c) => c,
            Err(e) => {
                error!("Redis Connect Fail: {}", e);
                return;
            }
        };
        let cost_model = SimCostModel::from_env();
        let variant = strategy_variant();
        let coinbase_ws_url = coinbase_ws_url();
        let coinbase_subscriptions = coinbase_level2_subscriptions(&coinbase_ws_url);

        let book_writer = self.book.clone();
        tokio::spawn(async move {
            loop {
                match connect_async(coinbase_ws_url.as_str()).await {
                    Ok((mut ws, _)) => {
                        info!("OBI connected to Coinbase order book feed: {}", coinbase_ws_url);
                        let mut last_sequence: Option<u64> = None;
                        for sub_msg in &coinbase_subscriptions {
                            if let Err(e) = ws.send(Message::Text(sub_msg.to_string())).await {
                                error!("Coinbase level2 subscribe failed: {}", e);
                            }
                        }

                        while let Some(msg) = ws.next().await {
                            match msg {
                                Ok(Message::Text(text)) => {
                                    let mut b = book_writer.write().await;
                                    match apply_coinbase_book_message(&mut b, &text, &mut last_sequence) {
                                        Ok(_) => {}
                                        Err((expected, got)) => {
                                            warn!(
                                                "OBI Coinbase sequence gap detected (expected {}, got {}); reconnecting local order book",
                                                expected,
                                                got
                                            );
                                            break;
                                        }
                                    }
                                }
                                Ok(Message::Ping(payload)) => {
                                    // Keep connection healthy when server sends pings.
                                    let _ = ws.send(Message::Pong(payload)).await;
                                }
                                Ok(Message::Close(_)) => break,
                                Ok(Message::Binary(_)) | Ok(Message::Pong(_)) => continue,
                                Ok(_) => continue,
                                Err(e) => {
                                    error!("OBI Coinbase message error: {}", e);
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("OBI Coinbase reconnecting after error: {}", e);
                        sleep(Duration::from_secs(3)).await;
                    }
                }
            }
        });

        let mut last_signal_ms = 0_i64;
        let mut obi_history: VecDeque<f64> = VecDeque::new();
        let mut open_position: Option<Position> = None;
        let mut pass_streak = 0_u32;
        let mut last_seen_reset_ts = 0_i64;

        loop {
            let target_market = loop {
                if let Some(m) = self.client.fetch_current_market("BTC").await {
                    break m;
                }
                sleep(Duration::from_secs(5)).await;
            };

            info!("OBI Scalper locked on market: {}", target_market.slug);

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

            let market_expiry = target_market
                .slug
                .split("-15m-")
                .nth(1)
                .and_then(|s| s.parse::<i64>().ok())
                .map(|s| s + 900)
                .unwrap_or(Utc::now().timestamp() + 900);

            let mut interval = tokio::time::interval(Duration::from_millis(100));
            let mut poly_book = BinaryBook::default();

            loop {
                tokio::select! {
                    Some(msg) = poly_ws.next() => {
                        match msg {
                            Ok(Message::Text(text)) => {
                                update_book_from_market_ws(
                                    &text,
                                    &target_market.yes_token,
                                    &target_market.no_token,
                                    &mut poly_book,
                                );
                            }
                            Ok(Message::Ping(payload)) => {
                                let _ = poly_ws.send(Message::Pong(payload)).await;
                            }
                            Ok(Message::Close(_)) => break,
                            Ok(Message::Binary(_)) | Ok(Message::Pong(_)) => {}
                            Ok(_) => {}
                            Err(e) => {
                                error!("OBI Polymarket WS error: {}", e);
                                break;
                            }
                        }
                    }
                    _ = interval.tick() => {
                        if !is_strategy_enabled(&mut conn, "OBI_SCALPER").await {
                            if let Some(pos) = open_position.take() {
                                let _ = release_sim_notional_for_strategy(&mut conn, "OBI_SCALPER", pos.size).await;
                            }
                            pass_streak = 0;
                            publish_heartbeat(&mut conn, "obi_scalper").await;
                            continue;
                        }

                        let now_ms = Utc::now().timestamp_millis();
                        let remaining_ms = market_expiry.saturating_mul(1000) - now_ms;
                        if remaining_ms <= 0 {
                            break;
                        }

                        let (obi, book_last_update_ms, best_bid, best_ask, mid) = {
                            let b = self.book.read().await;
                            (
                                b.calculate_obi(OBI_DEPTH_LEVELS),
                                b.last_update_ms,
                                b.best_bid(),
                                b.best_ask(),
                                b.mid(),
                            )
                        };

                        if now_ms - book_last_update_ms > BOOK_STALE_MS {
                            pass_streak = 0;
                            publish_heartbeat(&mut conn, "obi_scalper").await;
                            continue;
                        }

                        let Some(mid_price) = mid else {
                            pass_streak = 0;
                            publish_heartbeat(&mut conn, "obi_scalper").await;
                            continue;
                        };
                        let spread_bps = match (best_bid, best_ask) {
                            (Some(bid), Some(ask)) if ask > 0.0 && bid > 0.0 && mid_price > 0.0 => {
                                ((ask - bid) / mid_price) * 10_000.0
                            }
                            _ => f64::INFINITY,
                        };

                        if now_ms - poly_book.last_update_ms > BOOK_STALE_MS
                            || !poly_book.yes.is_valid()
                            || !poly_book.no.is_valid()
                        {
                            pass_streak = 0;
                            publish_heartbeat(&mut conn, "obi_scalper").await;
                            continue;
                        }

                        let yes_bid = poly_book.yes.best_bid;
                        let yes_ask = poly_book.yes.best_ask;
                        let no_bid = poly_book.no.best_bid;
                        let no_ask = poly_book.no.best_ask;
                        let yes_mid = poly_book.yes.mid();
                        let no_mid = poly_book.no.mid();

                        let poly_entry_spread_bps = if obi >= 0.0 {
                            if yes_mid > 0.0 {
                                ((yes_ask - yes_bid) / yes_mid) * 10_000.0
                            } else {
                                f64::INFINITY
                            }
                        } else if no_mid > 0.0 {
                            ((no_ask - no_bid) / no_mid) * 10_000.0
                        } else {
                            f64::INFINITY
                        };

                        obi_history.push_back(obi);
                        while obi_history.len() > 300 {
                            obi_history.pop_front();
                        }
                        let sigma = rolling_std(&obi_history);
                        let adaptive_threshold = (OBI_SIGNAL_THRESHOLD.max(sigma * OBI_STD_MULTIPLIER)).min(OBI_MAX_THRESHOLD);
                        let effective_threshold = (adaptive_threshold + OBI_ENTRY_BUFFER).min(0.95);
                        let trend_delta = if obi_history.len() >= OBI_TREND_WINDOW {
                            let start_idx = obi_history.len() - OBI_TREND_WINDOW;
                            let mut window: Vec<f64> = obi_history.iter().skip(start_idx).copied().collect();
                            if window.len() < OBI_TREND_WINDOW {
                                0.0
                            } else {
                                let half = OBI_TREND_WINDOW / 2;
                                let early_avg = window.drain(..half).sum::<f64>() / half as f64;
                                let late_len = OBI_TREND_WINDOW - half;
                                let late_avg = window.iter().sum::<f64>() / late_len as f64;
                                late_avg - early_avg
                            }
                        } else {
                            0.0
                        };
                        let trend_ok = if obi >= 0.0 {
                            trend_delta >= MIN_OBI_TREND_DELTA
                        } else {
                            trend_delta <= -MIN_OBI_TREND_DELTA
                        };
                        let passes_threshold = obi.abs() >= effective_threshold && trend_ok;
                        if passes_threshold {
                            pass_streak = pass_streak.saturating_add(1);
                        } else {
                            pass_streak = 0;
                        }
                        let reason = if obi.abs() < effective_threshold {
                            format!(
                                "|OBI| {:.3} below entry threshold {:.3}",
                                obi.abs(),
                                effective_threshold
                            )
                        } else if !trend_ok {
                            format!(
                                "OBI trend delta {:.3} failed directional filter {:.3}",
                                trend_delta,
                                MIN_OBI_TREND_DELTA
                            )
                        } else {
                            format!(
                                "|OBI| {:.3} exceeded entry threshold {:.3} with trend {:.3} (streak {}/{})",
                                obi.abs(),
                                effective_threshold,
                                trend_delta,
                                pass_streak,
                                MIN_OBI_PASS_STREAK
                            )
                        };

                        let scan_msg = build_scan_payload(
                            &target_market.market_id,
                            "OBI-SCALPER",
                            "OBI_SCALPER",
                            "ORDER_BOOK_IMBALANCE",
                            "RATIO",
                            [best_bid.unwrap_or(mid_price), best_ask.unwrap_or(mid_price)],
                            mid_price,
                            "TOP_OF_BOOK_MID",
                            obi,
                            effective_threshold,
                            passes_threshold,
                            reason,
                            now_ms,
                            serde_json::json!({
                                "obi_sigma": sigma,
                                "depth_levels": OBI_DEPTH_LEVELS,
                                "adaptive_threshold": adaptive_threshold,
                                "effective_threshold": effective_threshold,
                                "trend_delta": trend_delta,
                                "trend_ok": trend_ok,
                                "pass_streak": pass_streak,
                                "min_pass_streak": MIN_OBI_PASS_STREAK,
                                "coinbase_spread_bps": spread_bps,
                                "poly_yes_bid": yes_bid,
                                "poly_yes_ask": yes_ask,
                                "poly_no_bid": no_bid,
                                "poly_no_ask": no_ask,
                                "poly_entry_spread_bps": poly_entry_spread_bps,
                                "max_entry_spread_bps": MAX_ENTRY_SPREAD_BPS,
                            }),
                        );
                        let _: () = conn.publish("arbitrage:scan", scan_msg.to_string()).await.unwrap_or_default();

                        if obi.abs() >= 0.25 {
                            let alpha_msg = serde_json::json!({
                                "market": "BTC-USD",
                                "signal": "OBI",
                                "value": obi,
                                "timestamp": now_ms
                            });
                            let _: () = conn.publish("alpha:obi", alpha_msg.to_string()).await.unwrap_or_default();
                        }

                        let trading_mode = read_trading_mode(&mut conn).await;
                        let side = if obi >= 0.0 { PositionSide::Long } else { PositionSide::Short };
                        let (entry_price, exit_mark, token_id, token_side) = match side {
                            PositionSide::Long => (yes_ask, yes_bid, target_market.yes_token.clone(), "YES"),
                            PositionSide::Short => (no_ask, no_bid, target_market.no_token.clone(), "NO"),
                        };

                        if trading_mode == TradingMode::Live {
                            if passes_threshold
                                && pass_streak >= MIN_OBI_PASS_STREAK
                                && now_ms - last_signal_ms >= SIGNAL_COOLDOWN_MS
                                && spread_bps <= MAX_ENTRY_SPREAD_BPS
                                && poly_entry_spread_bps <= MAX_ENTRY_SPREAD_BPS
                                && remaining_ms >= ENTRY_EXPIRY_CUTOFF_MS
                                && (MIN_ENTRY_PRICE..=MAX_ENTRY_PRICE).contains(&entry_price)
                            {
                                let available_cash = read_sim_available_cash(&mut conn).await;
                                let risk_cfg = read_risk_config(&mut conn).await;
                                let size = compute_strategy_bet_size(
                                    &mut conn,
                                    "OBI_SCALPER",
                                    available_cash,
                                    &risk_cfg,
                                    10.0,
                                    MAX_POSITION_FRACTION,
                                ).await;
                                if size > 0.0 {
                                    let execution_id = Uuid::new_v4().to_string();
                                    let dry_run_msg = serde_json::json!({
                                        "execution_id": execution_id,
                                        "market": "OBI Scalper",
                                        "side": if matches!(side, PositionSide::Long) { "LIVE_DRY_RUN_LONG" } else { "LIVE_DRY_RUN_SHORT" },
                                        "price": entry_price,
                                        "size": size,
                                        "timestamp": now_ms,
                                        "mode": "LIVE_DRY_RUN",
                                        "details": {
                                            "obi": obi,
                                            "adaptive_threshold": adaptive_threshold,
                                            "pass_streak": pass_streak,
                                            "coinbase_mid": mid_price,
                                            "token_side": token_side,
                                            "entry_price": entry_price,
                                            "preflight": {
                                                "venue": "POLYMARKET",
                                                "strategy": "OBI_SCALPER",
                                                "orders": [
                                                    {
                                                        "token_id": token_id,
                                                        "condition_id": target_market.market_id.clone(),
                                                        "side": "BUY",
                                                        "price": entry_price,
                                                        "size": size,
                                                        "size_unit": "USD_NOTIONAL"
                                                    }
                                                ]
                                            }
                                        }
                                    });
                                    let _: () = conn.publish("arbitrage:execution", dry_run_msg.to_string()).await.unwrap_or_default();
                                    last_signal_ms = now_ms;
                                }
                            }

                            if let Some(pos) = open_position.take() {
                                let _ = release_sim_notional_for_strategy(&mut conn, "OBI_SCALPER", pos.size).await;
                                info!("OBI Scalper: clearing paper position in LIVE mode");
                            }
                            publish_heartbeat(&mut conn, "obi_scalper").await;
                            continue;
                        }

                        let reset_ts = read_simulation_reset_ts(&mut conn).await;
                        if reset_ts > last_seen_reset_ts {
                            last_seen_reset_ts = reset_ts;
                            open_position = None;
                            obi_history.clear();
                            pass_streak = 0;
                            last_signal_ms = 0;
                            publish_heartbeat(&mut conn, "obi_scalper").await;
                            continue;
                        }

                        if let Some(pos) = open_position.clone() {
                            let mark_price = if matches!(pos.side, PositionSide::Long) { yes_bid } else { no_bid };
                            if mark_price > 0.0 {
                                let gross_return = match pos.side {
                                    PositionSide::Long => (mark_price - pos.entry_mid) / pos.entry_mid,
                                    PositionSide::Short => (pos.entry_mid - mark_price) / pos.entry_mid,
                                };
                                let hold_ms = now_ms - pos.timestamp_ms;
                                let close_reason = if gross_return >= TAKE_PROFIT_PCT {
                                    Some("TAKE_PROFIT")
                                } else if gross_return <= STOP_LOSS_PCT {
                                    Some("STOP_LOSS")
                                } else if hold_ms >= MAX_HOLD_MS {
                                    Some("TIME_EXIT")
                                } else if matches!(pos.side, PositionSide::Long) && obi < 0.05 {
                                    Some("SIGNAL_DECAY")
                                } else if matches!(pos.side, PositionSide::Short) && obi > -0.05 {
                                    Some("SIGNAL_DECAY")
                                } else {
                                    None
                                };

                                if let Some(reason) = close_reason {
                                    let capped_gross_return = gross_return
                                        .max(-MAX_ABS_SETTLEMENT_RETURN)
                                        .min(MAX_ABS_SETTLEMENT_RETURN);
                                    let return_capped = (capped_gross_return - gross_return).abs() > f64::EPSILON;
                                    let pnl = realized_pnl(pos.size, capped_gross_return, cost_model);
                                    let new_bankroll = settle_sim_position_for_strategy(&mut conn, "OBI_SCALPER", pos.size, pnl).await;

                                    let net_return = if pos.size > 0.0 { pnl / pos.size } else { 0.0 };
                                    let execution_id = pos.execution_id.clone();
                                    let pnl_msg = serde_json::json!({
                                        "execution_id": execution_id.clone(),
                                        "strategy": "OBI_SCALPER",
                                        "variant": variant.as_str(),
                                        "pnl": pnl,
                                        "notional": pos.size,
                                        "timestamp": now_ms,
                                        "bankroll": new_bankroll,
                                        "mode": "PAPER",
                                        "details": {
                                            "action": "CLOSE_OBI",
                                            "reason": reason,
                                            "entry_mid": pos.entry_mid,
                                            "exit_mid": mark_price,
                                            "entry_obi": pos.entry_obi,
                                            "exit_obi": obi,
                                            "hold_ms": hold_ms,
                                            "gross_return": gross_return,
                                            "capped_gross_return": capped_gross_return,
                                            "settlement_cap_abs_return": MAX_ABS_SETTLEMENT_RETURN,
                                            "return_capped": return_capped,
                                            "net_return": net_return,
                                            "roi": format!("{:.2}%", net_return * 100.0),
                                            "round_trip_cost_rate": cost_model.round_trip_cost_rate(),
                                        }
                                    });
                                    let _: () = conn.publish("strategy:pnl", pnl_msg.to_string()).await.unwrap_or_default();

                                    let exec_msg = serde_json::json!({
                                        "execution_id": execution_id,
                                        "market": "OBI Scalper",
                                        "side": "CLOSE",
                                        "price": mark_price,
                                        "size": pos.size,
                                        "timestamp": now_ms,
                                        "mode": "PAPER",
                                        "details": {
                                            "reason": reason,
                                            "gross_return": gross_return,
                                            "capped_gross_return": capped_gross_return,
                                            "return_capped": return_capped,
                                            "net_return": net_return,
                                        }
                                    });
                                    let _: () = conn.publish("arbitrage:execution", exec_msg.to_string()).await.unwrap_or_default();
                                    open_position = None;
                                }
                            }
                        }

                        if open_position.is_none()
                            && passes_threshold
                            && pass_streak >= MIN_OBI_PASS_STREAK
                            && now_ms - last_signal_ms >= SIGNAL_COOLDOWN_MS
                            && spread_bps <= MAX_ENTRY_SPREAD_BPS
                            && poly_entry_spread_bps <= MAX_ENTRY_SPREAD_BPS
                            && remaining_ms >= ENTRY_EXPIRY_CUTOFF_MS
                            && (MIN_ENTRY_PRICE..=MAX_ENTRY_PRICE).contains(&entry_price)
                            && exit_mark > 0.0
                        {
                            let available_cash = read_sim_available_cash(&mut conn).await;
                            let risk_cfg = read_risk_config(&mut conn).await;
                            let size = compute_strategy_bet_size(
                                &mut conn,
                                "OBI_SCALPER",
                                available_cash,
                                &risk_cfg,
                                10.0,
                                MAX_POSITION_FRACTION,
                            ).await;
                            if size <= 0.0 {
                                publish_heartbeat(&mut conn, "obi_scalper").await;
                                continue;
                            }

                            if !reserve_sim_notional_for_strategy(&mut conn, "OBI_SCALPER", size).await {
                                publish_heartbeat(&mut conn, "obi_scalper").await;
                                continue;
                            }

                            let execution_id = Uuid::new_v4().to_string();
                            open_position = Some(Position {
                                execution_id: execution_id.clone(),
                                side,
                                entry_mid: entry_price,
                                size,
                                timestamp_ms: now_ms,
                                entry_obi: obi,
                            });

                            let exec_msg = serde_json::json!({
                                "execution_id": execution_id,
                                "market": "OBI Scalper",
                                "side": if matches!(side, PositionSide::Long) { "ENTRY_LONG" } else { "ENTRY_SHORT" },
                                "price": entry_price,
                                "size": size,
                                "timestamp": now_ms,
                                "mode": "PAPER",
                                "details": {
                                    "obi": obi,
                                    "adaptive_threshold": adaptive_threshold,
                                    "pass_streak": pass_streak,
                                    "token_side": token_side,
                                    "type": "ORDER_BOOK_IMBALANCE"
                                }
                            });
                            let _: () = conn.publish("arbitrage:execution", exec_msg.to_string()).await.unwrap_or_default();
                            last_signal_ms = now_ms;
                        }

                        publish_heartbeat(&mut conn, "obi_scalper").await;
                    }
                }

                if Utc::now().timestamp() > market_expiry {
                    break;
                }
            }
        }
    }
}
