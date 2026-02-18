use async_trait::async_trait;
use chrono::Utc;
use futures::{SinkExt, StreamExt};
use log::{error, info, warn};
use std::collections::HashMap;
use tokio::time::{sleep, Duration};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use url::Url;
use uuid::Uuid;

use crate::engine::{MarketTarget, PolymarketClient, WS_URL};
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
    read_trading_mode,
    release_sim_notional_for_strategy,
    reserve_sim_notional_for_strategy,
    settle_sim_position_for_strategy,
    strategy_variant,
    TradingMode,
};
use crate::strategies::market_data::{update_books_from_market_ws, BinaryBook, TokenBinding};
use crate::strategies::simulation::{polymarket_taker_fee, SimCostModel};
use crate::strategies::Strategy;

const BOOK_STALE_MS: i64 = 3_000;
const REFRESH_UNIVERSE_MS: i64 = 60_000;
const ENTRY_COOLDOWN_MS: i64 = 10_000;
const MIN_TIME_TO_EXPIRY_SECS: i64 = 300;
const MAX_TIME_TO_EXPIRY_SECS: i64 = 24 * 60 * 60;
const MAX_UNIVERSE_MARKETS: usize = 40;
const MAX_OPEN_POSITIONS: usize = 6;
const MAX_PARITY_DEVIATION: f64 = 0.04;
const MAX_HOLD_MS: i64 = 300_000;
const RESOLUTION_GRACE_MS: i64 = 180_000;
const STRATEGY_ID: &str = "LONGSHOT_BIAS";

fn longshot_price_ceiling() -> f64 {
    std::env::var("LONGSHOT_PRICE_CEILING").ok().and_then(|v| v.parse().ok()).unwrap_or(0.15)
}

/// Empirical longshot bias mispricing curve from 72.1M Kalshi trades research.
fn longshot_bias_edge(price: f64) -> f64 {
    if price <= 0.01 || price >= 0.30 { return 0.0; }
    let anchors: [(f64, f64); 6] = [
        (0.02, 0.20), (0.05, 0.164), (0.10, 0.090),
        (0.15, 0.080), (0.20, 0.075), (0.25, 0.072),
    ];
    if price <= anchors[0].0 { return anchors[0].1; }
    if price >= anchors[5].0 { return anchors[5].1; }
    for w in anchors.windows(2) {
        if price >= w[0].0 && price <= w[1].0 {
            let t = (price - w[0].0) / (w[1].0 - w[0].0);
            return w[0].1 + t * (w[1].1 - w[0].1);
        }
    }
    0.0
}

fn min_net_edge() -> f64 {
    std::env::var("LONGSHOT_MIN_NET_EDGE").ok().and_then(|v| v.parse().ok()).unwrap_or(0.04)
}
fn fee_curve_rate() -> f64 {
    std::env::var("LONGSHOT_FEE_CURVE_BASE_RATE").ok().and_then(|v| v.parse().ok()).unwrap_or(0.02)
}

#[derive(Debug, Clone)]
struct Position {
    execution_id: String,
    market_id: String,
    token_id: String,
    question: String,
    fade_side: &'static str,
    entry_side: &'static str,
    entry_price: f64,
    longshot_price: f64,
    size: f64,
    timestamp_ms: i64,
    expiry_ts: i64,
}

pub struct LongshotBiasStrategy {
    client: PolymarketClient,
}

impl LongshotBiasStrategy {
    pub fn new() -> Self { Self { client: PolymarketClient::new() } }

    fn build_universe(markets: Vec<MarketTarget>) -> (HashMap<String, MarketTarget>, HashMap<String, TokenBinding>, Vec<String>) {
        let mut mbi = HashMap::new();
        let mut tb = HashMap::new();
        let mut ids = Vec::new();
        for m in markets {
            if m.market_id.is_empty() || m.yes_token.is_empty() || m.no_token.is_empty() || m.yes_token == m.no_token { continue; }
            let mid = m.market_id.clone();
            tb.insert(m.yes_token.clone(), TokenBinding { market_key: mid.clone(), is_yes: true });
            tb.insert(m.no_token.clone(), TokenBinding { market_key: mid.clone(), is_yes: false });
            ids.push(m.yes_token.clone());
            ids.push(m.no_token.clone());
            mbi.insert(mid, m);
        }
        (mbi, tb, ids)
    }
}

#[async_trait]
impl Strategy for LongshotBiasStrategy {
    async fn run(&self, redis_client: redis::Client) {
        info!("Starting Longshot Bias Scanner strategy...");
        let mut conn = match redis_client.get_multiplexed_async_connection().await {
            Ok(c) => c,
            Err(e) => { error!("Redis connect failed: {}", e); return; }
        };
        let cost_model = SimCostModel::from_env();
        let variant = strategy_variant();
        let mut positions: Vec<Position> = Vec::new();
        let mut books: HashMap<String, BinaryBook> = HashMap::new();
        let mut last_entry_ts = 0_i64;
        let mut last_seen_reset_ts = 0_i64;

        loop {
            publish_heartbeat(&mut conn, STRATEGY_ID).await;
            if !is_strategy_enabled(&mut conn, STRATEGY_ID).await {
                sleep(Duration::from_secs(5)).await;
                continue;
            }

            let now_secs = Utc::now().timestamp();
            let all = self.client.fetch_active_binary_markets(MAX_UNIVERSE_MARKETS).await;
            let filtered: Vec<MarketTarget> = all.into_iter().filter(|m| {
                let exp = m.expiry_ts.unwrap_or(i64::MAX);
                exp > now_secs + MIN_TIME_TO_EXPIRY_SECS && exp <= now_secs + MAX_TIME_TO_EXPIRY_SECS
            }).collect();

            if filtered.is_empty() {
                warn!("[LONGSHOT] No eligible markets");
                sleep(Duration::from_secs(4)).await;
                continue;
            }

            let (market_by_id, token_bindings, asset_ids) = Self::build_universe(filtered);
            if market_by_id.is_empty() || asset_ids.is_empty() {
                sleep(Duration::from_secs(4)).await;
                continue;
            }
            info!("[LONGSHOT] Tracking {} markets / {} tokens", market_by_id.len(), asset_ids.len());

            let poly_url = match Url::parse(WS_URL) {
                Ok(u) => u,
                Err(e) => { error!("[LONGSHOT] Bad WS URL: {}", e); sleep(Duration::from_secs(2)).await; continue; }
            };
            use tokio_tungstenite::tungstenite::client::IntoClientRequest;
            let mut request = match poly_url.into_client_request() {
                Ok(r) => r,
                Err(e) => { error!("[LONGSHOT] WS request: {}", e); sleep(Duration::from_secs(2)).await; continue; }
            };
            if let Ok(ua) = "Mozilla/5.0".parse() { request.headers_mut().insert("User-Agent", ua); }

            let mut poly_ws = match connect_async(request).await {
                Ok((ws, _)) => ws,
                Err(e) => { error!("[LONGSHOT] WS connect: {}", e); sleep(Duration::from_secs(2)).await; continue; }
            };
            let sub = serde_json::json!({ "assets_ids": asset_ids, "type": "market" });
            if let Err(e) = poly_ws.send(Message::Text(sub.to_string())).await {
                error!("[LONGSHOT] WS sub: {}", e); sleep(Duration::from_secs(2)).await; continue;
            }

            let refresh_deadline = Utc::now().timestamp_millis() + REFRESH_UNIVERSE_MS;
            let mut interval = tokio::time::interval(Duration::from_millis(2000));

            loop {
                tokio::select! {
                    Some(msg) = poly_ws.next() => {
                        match msg {
                            Ok(Message::Text(text)) => { let _ = update_books_from_market_ws(&text, &token_bindings, &mut books); }
                            Ok(Message::Ping(p)) => { let _ = poly_ws.send(Message::Pong(p)).await; }
                            Ok(Message::Close(_)) => break,
                            Ok(_) => {}
                            Err(e) => { error!("[LONGSHOT] WS error: {}", e); break; }
                        }
                    }
                    _ = interval.tick() => {
                        let now_ms = Utc::now().timestamp_millis();
                        if now_ms >= refresh_deadline { break; }

                        publish_heartbeat(&mut conn, STRATEGY_ID).await;

                        let reset_ts = read_simulation_reset_ts(&mut conn).await;
                        if reset_ts > last_seen_reset_ts {
                            last_seen_reset_ts = reset_ts;
                            let rel = positions.iter().map(|p| p.size).sum::<f64>();
                            positions.clear(); books.clear();
                            if rel > 0.0 { let _ = release_sim_notional_for_strategy(&mut conn, STRATEGY_ID, rel).await; }
                            continue;
                        }

                        if !is_strategy_enabled(&mut conn, STRATEGY_ID).await { continue; }

                        // Risk guard cooldown — skip entry if backend set a post-loss cooldown.
                        let cooldown_until = read_risk_guard_cooldown(&mut conn, STRATEGY_ID).await;
                        if cooldown_until > 0 && now_ms < cooldown_until {
                            publish_heartbeat(&mut conn, STRATEGY_ID).await;
                            continue;
                        }

                        let mode = read_trading_mode(&mut conn).await;
                        if mode == TradingMode::Live {
                            // Release paper positions to avoid ghost capital lockup
                            for pos in positions.drain(..) {
                                let _ = release_sim_notional_for_strategy(&mut conn, STRATEGY_ID, pos.size).await;
                            }
                        }

                        // ── Settle expired / stale positions ──
                        if mode != TradingMode::Live {
                            let mut keep: Vec<Position> = Vec::new();
                            for pos in positions.drain(..) {
                                // Force-close if book stale beyond MAX_HOLD to free capital
                                let hold_ms = now_ms - pos.timestamp_ms;
                                let book_fresh = books.get(&pos.market_id)
                                    .map(|b| now_ms - b.last_update_ms <= BOOK_STALE_MS)
                                    .unwrap_or(false);
                                if !book_fresh && hold_ms >= MAX_HOLD_MS {
                                    let _ = settle_sim_position_for_strategy(&mut conn, STRATEGY_ID, pos.size, 0.0).await;
                                    let bankroll = read_sim_available_cash(&mut conn).await;
                                    let pnl_msg = serde_json::json!({
                                        "strategy": STRATEGY_ID,
                                        "variant": variant,
                                        "pnl": 0.0,
                                        "notional": pos.size,
                                        "timestamp": now_ms,
                                        "bankroll": bankroll,
                                        "mode": format!("{:?}", mode),
                                        "details": {
                                            "action": "CLOSE_POSITION",
                                            "reason": "STALE_FORCE_CLOSE",
                                            "question": pos.question,
                                            "fade_side": pos.fade_side,
                                            "entry_side": pos.entry_side,
                                            "entry_price": pos.entry_price,
                                            "hold_ms": hold_ms,
                                        }
                                    });
                                    publish_event(&mut conn, "strategy:pnl", pnl_msg.to_string()).await;
                                    let exec_msg = serde_json::json!({
                                        "execution_id": pos.execution_id,
                                        "market": "Longshot Bias",
                                        "side": "EXIT",
                                        "price": pos.entry_price,
                                        "size": pos.size,
                                        "timestamp": now_ms,
                                        "mode": format!("{:?}", mode),
                                        "details": {
                                            "action": "CLOSE_POSITION",
                                            "reason": "STALE_FORCE_CLOSE",
                                            "condition_id": pos.market_id,
                                            "question": pos.question,
                                            "pnl": 0.0,
                                        }
                                    });
                                    publish_event(&mut conn, "arbitrage:execution", exec_msg.to_string()).await;
                                    info!("[LONGSHOT] STALE_FORCE_CLOSE {}", pos.question);
                                    continue;
                                }

                                let secs_left = pos.expiry_ts - now_ms / 1000;
                                if secs_left <= 0 {
                                    let (exit_price, exit_source) = if let Some(resolved_price) = self
                                        .client
                                        .fetch_resolved_outcome_price(&pos.market_id, &pos.token_id, pos.entry_side == "YES")
                                        .await
                                    {
                                        (resolved_price.clamp(0.0, 1.0), "RESOLVED_OUTCOME")
                                    } else if now_ms <= (pos.expiry_ts * 1000) + RESOLUTION_GRACE_MS {
                                        keep.push(pos);
                                        continue;
                                    } else {
                                        match books.get(&pos.market_id) {
                                            Some(book) => {
                                                let bid = if pos.entry_side == "YES" {
                                                    book.yes.best_bid
                                                } else {
                                                    book.no.best_bid
                                                };
                                                if bid.is_finite() && (0.0..=1.0).contains(&bid) {
                                                    (bid, "BOOK_BID_AFTER_GRACE")
                                                } else {
                                                    (0.0, "NO_LIQUID_BID_AFTER_GRACE")
                                                }
                                            }
                                            None => (0.0, "BOOK_MISSING_AFTER_GRACE"),
                                        }
                                    };
                                    let gross = if pos.entry_price > 0.0 && exit_price >= 0.0 {
                                        ((exit_price / pos.entry_price) - 1.0).max(-0.999)
                                    } else {
                                        -0.999
                                    };
                                    let net = (gross - cost_model.round_trip_cost_rate()).max(-0.999);
                                    let pnl = pos.size * net;
                                    let _ = settle_sim_position_for_strategy(&mut conn, STRATEGY_ID, pos.size, pnl).await;
                                    let bankroll = read_sim_available_cash(&mut conn).await;
                                    let hold_ms = now_ms - pos.timestamp_ms;
                                    let pnl_msg = serde_json::json!({
                                        "strategy": STRATEGY_ID,
                                        "variant": variant,
                                        "pnl": pnl,
                                        "notional": pos.size,
                                        "timestamp": now_ms,
                                        "bankroll": bankroll,
                                        "mode": format!("{:?}", mode),
                                        "details": {
                                            "action": "CLOSE_POSITION",
                                            "reason": "EXPIRY",
                                            "question": pos.question,
                                            "fade_side": pos.fade_side,
                                            "entry_side": pos.entry_side,
                                            "entry": pos.entry_price,
                                            "exit": exit_price,
                                            "exit_source": exit_source,
                                            "longshot_price": pos.longshot_price,
                                            "gross_return": gross,
                                            "net_return": net,
                                            "round_trip_cost_rate": cost_model.round_trip_cost_rate(),
                                            "hold_ms": hold_ms,
                                        }
                                    });
                                    publish_event(&mut conn, "strategy:pnl", pnl_msg.to_string()).await;
                                    let exec_msg = serde_json::json!({
                                        "execution_id": pos.execution_id,
                                        "market": "Longshot Bias",
                                        "side": "EXIT",
                                        "price": exit_price,
                                        "size": pos.size,
                                        "timestamp": now_ms,
                                        "mode": format!("{:?}", mode),
                                        "details": {
                                            "action": "CLOSE_POSITION",
                                            "reason": "EXPIRY",
                                            "condition_id": pos.market_id,
                                            "question": pos.question,
                                            "entry_price": pos.entry_price,
                                            "exit_price": exit_price,
                                            "exit_source": exit_source,
                                            "pnl": pnl,
                                            "gross_return": gross,
                                            "net_return": net,
                                        }
                                    });
                                    publish_event(&mut conn, "arbitrage:execution", exec_msg.to_string()).await;
                                    info!("[LONGSHOT] SETTLED {} pnl=${:.2}", pos.question, pnl);
                                } else {
                                    keep.push(pos);
                                }
                            }
                            positions = keep;
                        }

                        // ── Scan for longshot bias opportunities ──
                        let can_enter = positions.len() < MAX_OPEN_POSITIONS && (now_ms - last_entry_ts) > ENTRY_COOLDOWN_MS;
                        let ceiling = longshot_price_ceiling();
                        let threshold = min_net_edge();
                        let fee_rate = fee_curve_rate();

                        struct Opp { mid: String, fade: &'static str, entry: &'static str, ep: f64, lp: f64, bias: f64, ne: f64, exp: i64 }
                        let mut best: Option<Opp> = None;

                        for (mid, market) in &market_by_id {
                            if positions.iter().any(|p| p.market_id == *mid) { continue; }
                            let book = match books.get(mid) { Some(b) => b, None => continue };
                            if now_ms - book.last_update_ms > BOOK_STALE_MS { continue; }
                            let ym = book.yes.mid(); let nm = book.no.mid();
                            if ym <= 0.0 || nm <= 0.0 { continue; }
                            if (ym + nm - 1.0).abs() > MAX_PARITY_DEVIATION { continue; }
                            let exp = match market.expiry_ts { Some(t) => t, None => continue };

                            // YES is longshot → buy NO to fade
                            if ym <= ceiling && ym >= 0.02 {
                                let bias = longshot_bias_edge(ym);
                                let opp_ask = book.no.best_ask;
                                if opp_ask > 0.0 && opp_ask <= 0.98 {
                                    let ne = bias - polymarket_taker_fee(opp_ask, fee_rate) - cost_model.slippage_bps_per_side / 10_000.0;
                                    if ne >= threshold && best.as_ref().is_none_or(|current| ne > current.ne) {
                                        best = Some(Opp { mid: mid.clone(), fade: "YES", entry: "NO", ep: opp_ask, lp: ym, bias, ne, exp });
                                    }
                                }
                            }
                            // NO is longshot → buy YES to fade
                            if nm <= ceiling && nm >= 0.02 {
                                let bias = longshot_bias_edge(nm);
                                let opp_ask = book.yes.best_ask;
                                if opp_ask > 0.0 && opp_ask <= 0.98 {
                                    let ne = bias - polymarket_taker_fee(opp_ask, fee_rate) - cost_model.slippage_bps_per_side / 10_000.0;
                                    if ne >= threshold && best.as_ref().is_none_or(|current| ne > current.ne) {
                                        best = Some(Opp { mid: mid.clone(), fade: "NO", entry: "YES", ep: opp_ask, lp: nm, bias, ne, exp });
                                    }
                                }
                            }

                            // Scan telemetry
                            let yb = if ym <= ceiling { longshot_bias_edge(ym) } else { 0.0 };
                            let nb = if nm <= ceiling { longshot_bias_edge(nm) } else { 0.0 };
                            let bb = yb.max(nb);
                            let sp = bb > threshold;
                            let reason = if !sp { format!("Bias {:.1}% < {:.1}%", bb*100.0, threshold*100.0) }
                                else { format!("Bias {:.1}% > {:.1}%", bb*100.0, threshold*100.0) };
                            let scan = build_scan_payload(mid, &market.question, STRATEGY_ID,
                                "LONGSHOT_BIAS_FADE", "PCT", [ym, nm], bb, "BIAS_EDGE",
                                bb*1e4, threshold*1e4, sp, reason, now_ms,
                                serde_json::json!({
                                    "yes_mid": ym, "no_mid": nm, "yes_bias_pct": yb*100.0,
                                    "no_bias_pct": nb*100.0, "ceiling": ceiling,
                                    "fee_curve_rate": fee_rate, "question": market.question,
                                    "slug": market.slug, "positions_count": positions.len(),
                                }),
                            );
                            publish_event(&mut conn, "arbitrage:scan", scan.to_string()).await;
                        }

                        // ── Execute best entry ──
                        if can_enter {
                            if let Some(opp) = best {
                                // Skip if already positioned in this market
                                let already_in = positions.iter().any(|p| p.market_id == opp.mid);
                                if already_in { continue; }

                                let market = match market_by_id.get(&opp.mid) { Some(m) => m, None => continue };
                                let cash = read_sim_available_cash(&mut conn).await;
                                let cfg = read_risk_config(&mut conn).await;
                                let sz = compute_strategy_bet_size(&mut conn, STRATEGY_ID, cash, &cfg, 1.0, 0.10).await;
                                if sz >= 1.0 && mode == TradingMode::Live {
                                    let eid = Uuid::new_v4().to_string();
                                    let token_id = if opp.entry == "YES" { market.yes_token.clone() } else { market.no_token.clone() };
                                    let preview_msg = serde_json::json!({
                                        "execution_id": eid,
                                        "strategy": STRATEGY_ID,
                                        "market": "Longshot Bias",
                                        "side": "LIVE_DRY_RUN",
                                        "price": opp.ep,
                                        "size": sz,
                                        "timestamp": now_ms,
                                        "mode": "LIVE_DRY_RUN",
                                        "details": {
                                            "strategy": STRATEGY_ID,
                                            "condition_id": opp.mid,
                                            "question": market.question,
                                            "fade_side": opp.fade,
                                            "entry_side": opp.entry,
                                            "longshot_price": opp.lp,
                                            "bias_edge_pct": opp.bias*100.0,
                                            "net_edge_pct": opp.ne*100.0,
                                            "variant": variant,
                                            "preflight": {
                                                "venue": "POLYMARKET",
                                                "strategy": STRATEGY_ID,
                                                "orders": [
                                                    {
                                                        "token_id": token_id,
                                                        "condition_id": opp.mid,
                                                        "side": "BUY",
                                                        "price": opp.ep,
                                                        "size": sz,
                                                        "size_unit": "USD_NOTIONAL"
                                                    }
                                                ]
                                            }
                                        }
                                    });
                                    publish_event(&mut conn, "arbitrage:execution", preview_msg.to_string()).await;
                                    last_entry_ts = now_ms;
                                } else if sz >= 1.0 && reserve_sim_notional_for_strategy(&mut conn, STRATEGY_ID, sz).await {
                                    let eid = Uuid::new_v4().to_string();
                                    let token_id = if opp.entry == "YES" { market.yes_token.clone() } else { market.no_token.clone() };
                                    positions.push(Position {
                                        execution_id: eid.clone(), market_id: opp.mid.clone(),
                                        token_id,
                                        question: market.question.clone(), fade_side: opp.fade,
                                        entry_side: opp.entry, entry_price: opp.ep,
                                        longshot_price: opp.lp, size: sz,
                                        timestamp_ms: now_ms, expiry_ts: opp.exp,
                                    });
                                    last_entry_ts = now_ms;
                                    info!("[LONGSHOT] ENTRY fade {} buy {} @ {:.2}c (longshot={:.2}c bias={:.1}%)",
                                        opp.fade, opp.entry, opp.ep*100.0, opp.lp*100.0, opp.bias*100.0);
                                    let exec_msg = serde_json::json!({
                                        "execution_id": eid,
                                        "market": "Longshot Bias",
                                        "side": "ENTRY",
                                        "price": opp.ep,
                                        "size": sz,
                                        "timestamp": now_ms,
                                        "mode": format!("{:?}", mode),
                                        "details": {
                                            "condition_id": opp.mid,
                                            "question": market.question,
                                            "fade_side": opp.fade,
                                            "entry_side": opp.entry,
                                            "longshot_price": opp.lp,
                                            "bias_edge_pct": opp.bias*100.0,
                                            "net_edge_pct": opp.ne*100.0,
                                            "variant": variant,
                                        }
                                    });
                                    publish_event(&mut conn, "arbitrage:execution", exec_msg.to_string()).await;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
