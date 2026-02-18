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

const BOOK_STALE_MS: i64 = 2_000;
const REFRESH_UNIVERSE_MS: i64 = 35_000;
const ENTRY_COOLDOWN_MS: i64 = 5_000;
const MIN_TIME_TO_EXPIRY_SECS: i64 = 180;
const MAX_TIME_TO_EXPIRY_SECS: i64 = 8 * 60 * 60;
const MAX_UNIVERSE_MARKETS: usize = 20;
const MAX_OPEN_POSITIONS: usize = 4;
const MIN_ENTRY_PRICE: f64 = 0.10;
const MAX_ENTRY_PRICE: f64 = 0.90;
const MAX_SPREAD: f64 = 0.12;
const MAX_PARITY_DEVIATION: f64 = 0.03;
const TAKE_PROFIT_PCT: f64 = 0.012;
const STOP_LOSS_PCT: f64 = -0.020;
const MAX_HOLD_MS: i64 = 120_000;
const STRATEGY_ID: &str = "AS_MARKET_MAKER";

fn gamma() -> f64 {
    std::env::var("AS_MM_GAMMA").ok().and_then(|v| v.parse().ok()).unwrap_or(0.3)
}
fn kappa() -> f64 {
    std::env::var("AS_MM_KAPPA").ok().and_then(|v| v.parse().ok()).unwrap_or(1.5)
}
fn min_half_spread() -> f64 {
    std::env::var("AS_MM_MIN_HALF_SPREAD").ok().and_then(|v| v.parse().ok()).unwrap_or(0.015)
}
fn min_edge_bps() -> f64 {
    std::env::var("AS_MM_MIN_EDGE_BPS").ok().and_then(|v| v.parse().ok()).unwrap_or(15.0)
}
fn fee_curve_rate() -> f64 {
    std::env::var("AS_MM_FEE_CURVE_BASE_RATE").ok().and_then(|v| v.parse().ok()).unwrap_or(0.02)
}

#[derive(Debug, Clone, Copy)]
enum Side { Yes, No }

#[derive(Debug, Clone)]
struct Position {
    execution_id: String,
    market_id: String,
    question: String,
    side: Side,
    entry_price: f64,
    size: f64,
    timestamp_ms: i64,
}

fn estimate_sigma(mid_history: &[(i64, f64)]) -> f64 {
    if mid_history.len() < 5 { return 0.30; }
    let returns: Vec<f64> = mid_history.windows(2)
        .filter_map(|w| {
            if w[1].0 <= w[0].0 || w[0].1 <= 0.0 { return None; }
            Some((w[1].1 - w[0].1) / w[0].1)
        })
        .collect();
    if returns.len() < 3 { return 0.30; }
    let mean: f64 = returns.iter().sum::<f64>() / returns.len() as f64;
    let var: f64 = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / returns.len() as f64;
    let std: f64 = var.sqrt();
    let samples_per_year: f64 = 365.25 * 24.0 * 3600.0 * 2.0; // ~500ms tick rate
    (std * samples_per_year.sqrt()).clamp(0.05, 3.0)
}

fn reservation_price(mid: f64, inventory: f64, g: f64, sigma: f64, tte: f64) -> f64 {
    (mid - inventory * g * sigma * sigma * tte).clamp(0.01, 0.99)
}

fn optimal_half_spread(g: f64, sigma: f64, k: f64, tte: f64) -> f64 {
    let tc = g * sigma * sigma * tte / 2.0;
    let ic = if g > 0.0 { (1.0 / g) * (1.0 + g / k).ln() } else { 0.01 };
    (tc + ic).max(min_half_spread())
}

pub struct AsMarketMakerStrategy {
    client: PolymarketClient,
}

impl AsMarketMakerStrategy {
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
impl Strategy for AsMarketMakerStrategy {
    async fn run(&self, redis_client: redis::Client) {
        info!("Starting Avellaneda-Stoikov Market Maker strategy...");
        let mut conn = match redis_client.get_multiplexed_async_connection().await {
            Ok(c) => c,
            Err(e) => { error!("Redis connect failed: {}", e); return; }
        };
        let cost_model = SimCostModel::from_env();
        let variant = strategy_variant();
        let mut positions: Vec<Position> = Vec::new();
        let mut books: HashMap<String, BinaryBook> = HashMap::new();
        let mut mid_history: HashMap<String, Vec<(i64, f64)>> = HashMap::new();
        let mut last_entry_ts = 0_i64;
        let mut last_seen_reset_ts = 0_i64;

        // Outer loop: fetch universe, connect WS
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
                warn!("[AS_MM] No eligible markets");
                sleep(Duration::from_secs(4)).await;
                continue;
            }

            let (market_by_id, token_bindings, asset_ids) = Self::build_universe(filtered);
            if market_by_id.is_empty() || asset_ids.is_empty() {
                sleep(Duration::from_secs(4)).await;
                continue;
            }
            info!("[AS_MM] Tracking {} markets / {} tokens", market_by_id.len(), asset_ids.len());

            // Connect WS
            let poly_url = match Url::parse(WS_URL) {
                Ok(u) => u,
                Err(e) => { error!("[AS_MM] Bad WS URL: {}", e); sleep(Duration::from_secs(2)).await; continue; }
            };
            use tokio_tungstenite::tungstenite::client::IntoClientRequest;
            let mut request = match poly_url.into_client_request() {
                Ok(r) => r,
                Err(e) => { error!("[AS_MM] WS request error: {}", e); sleep(Duration::from_secs(2)).await; continue; }
            };
            if let Ok(ua) = "Mozilla/5.0".parse() { request.headers_mut().insert("User-Agent", ua); }

            let mut poly_ws = match connect_async(request).await {
                Ok((ws, _)) => ws,
                Err(e) => { error!("[AS_MM] WS connect failed: {}", e); sleep(Duration::from_secs(2)).await; continue; }
            };
            let sub = serde_json::json!({ "assets_ids": asset_ids, "type": "market" });
            if let Err(e) = poly_ws.send(Message::Text(sub.to_string())).await {
                error!("[AS_MM] WS sub failed: {}", e);
                sleep(Duration::from_secs(2)).await;
                continue;
            }

            let refresh_deadline = Utc::now().timestamp_millis() + REFRESH_UNIVERSE_MS;
            let mut interval = tokio::time::interval(Duration::from_millis(500));

            // Inner loop: process WS messages + scan on tick
            loop {
                tokio::select! {
                    Some(msg) = poly_ws.next() => {
                        match msg {
                            Ok(Message::Text(text)) => { let _ = update_books_from_market_ws(&text, &token_bindings, &mut books); }
                            Ok(Message::Ping(p)) => { let _ = poly_ws.send(Message::Pong(p)).await; }
                            Ok(Message::Close(_)) => break,
                            Ok(_) => {}
                            Err(e) => { error!("[AS_MM] WS error: {}", e); break; }
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
                            positions.clear();
                            books.clear();
                            if rel > 0.0 { let _ = release_sim_notional_for_strategy(&mut conn, STRATEGY_ID, rel).await; }
                            continue;
                        }

                        if !is_strategy_enabled(&mut conn, STRATEGY_ID).await {
                            continue;
                        }

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
                        let gamma_val = gamma();
                        let kappa_val = kappa();
                        let min_edge = min_edge_bps() / 10_000.0;
                        let fee_rate = fee_curve_rate();

                        // ── Exit management ──
                        if mode != TradingMode::Live {
                            let mut keep: Vec<Position> = Vec::new();
                            for pos in positions.drain(..) {
                                let book = match books.get(&pos.market_id) {
                                    Some(b) => b,
                                    None => { keep.push(pos); continue; }
                                };
                                if now_ms - book.last_update_ms > BOOK_STALE_MS { keep.push(pos); continue; }
                                let mid = match pos.side { Side::Yes => book.yes.mid(), Side::No => book.no.mid() };
                                if mid <= 0.0 { keep.push(pos); continue; }
                                let age = now_ms - pos.timestamp_ms;
                                let gross = (mid / pos.entry_price) - 1.0;
                                let reason = if gross >= TAKE_PROFIT_PCT { Some("TP") }
                                    else if gross <= STOP_LOSS_PCT { Some("SL") }
                                    else if age >= MAX_HOLD_MS { Some("TIMEOUT") }
                                    else { None };
                                if let Some(r) = reason {
                                    let cost = cost_model.round_trip_cost_rate();
                                    let net = gross - cost;
                                    let pnl = pos.size * net;
                                    let _ = settle_sim_position_for_strategy(&mut conn, STRATEGY_ID, pos.size, pnl).await;
                                    let bankroll = read_sim_available_cash(&mut conn).await;
                                    // PnL channel — for trade recording + bankroll tracking
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
                                            "reason": r,
                                            "market_id": pos.market_id,
                                            "question": pos.question,
                                            "side": match pos.side { Side::Yes => "YES", Side::No => "NO" },
                                            "entry": pos.entry_price,
                                            "exit": mid,
                                            "gross_return": gross,
                                            "net_return": net,
                                            "round_trip_cost_rate": cost,
                                            "hold_ms": age,
                                        }
                                    });
                                    publish_event(&mut conn, "strategy:pnl", pnl_msg.to_string()).await;
                                    // Execution channel — for execution trace visibility
                                    let exec_msg = serde_json::json!({
                                        "execution_id": pos.execution_id,
                                        "market": "AS Market Maker",
                                        "side": "EXIT",
                                        "price": mid,
                                        "size": pos.size,
                                        "timestamp": now_ms,
                                        "mode": format!("{:?}", mode),
                                        "details": {
                                            "action": "CLOSE_POSITION",
                                            "reason": r,
                                            "condition_id": pos.market_id,
                                            "question": pos.question,
                                            "token_side": match pos.side { Side::Yes => "YES", Side::No => "NO" },
                                            "entry_price": pos.entry_price,
                                            "exit_price": mid,
                                            "pnl": pnl,
                                            "gross_return": gross,
                                            "net_return": net,
                                        }
                                    });
                                    publish_event(&mut conn, "arbitrage:execution", exec_msg.to_string()).await;
                                    info!("[AS_MM] EXIT {} pnl=${:.2} ({})", pos.question, pnl, r);
                                } else {
                                    keep.push(pos);
                                }
                            }
                            positions = keep;
                        }

                        // ── Scan all markets ──
                        let can_enter = positions.len() < MAX_OPEN_POSITIONS && (now_ms - last_entry_ts) > ENTRY_COOLDOWN_MS;
                        let mut best: Option<(String, Side, f64, f64, f64, f64, f64, f64, i64)> = None;

                        for (mid, market) in &market_by_id {
                            let book = match books.get(mid) { Some(b) => b, None => continue };
                            if now_ms - book.last_update_ms > BOOK_STALE_MS { continue; }
                            let ym = book.yes.mid(); let nm = book.no.mid();
                            if ym <= 0.0 || nm <= 0.0 { continue; }
                            let parity = (ym + nm - 1.0).abs();
                            if parity > MAX_PARITY_DEVIATION { continue; }
                            let exp = match market.expiry_ts { Some(t) => t, None => continue };
                            let ste = exp - now_ms / 1000;
                            if ste < MIN_TIME_TO_EXPIRY_SECS { continue; }
                            let tte = ste as f64 / (365.25 * 24.0 * 3600.0);

                            let hist = mid_history.entry(mid.clone()).or_default();
                            hist.push((now_ms, ym));
                            if hist.len() > 500 { hist.drain(..hist.len() - 500); }
                            let sigma = estimate_sigma(hist);

                            let inv: f64 = positions.iter().filter(|p| p.market_id == *mid)
                                .map(|p| match p.side { Side::Yes => 1.0, Side::No => -1.0 }).sum();

                            let r_yes = reservation_price(ym, inv, gamma_val, sigma, tte);
                            let hs = optimal_half_spread(gamma_val, sigma, kappa_val, tte);
                            let theo_ask_yes = r_yes + hs;

                            let r_no = reservation_price(nm, -inv, gamma_val, sigma, tte);
                            let theo_ask_no = r_no + hs;

                            let ya = book.yes.best_ask; let na = book.no.best_ask;
                            let ys = (book.yes.best_ask - book.yes.best_bid).max(0.0);
                            let ns = (book.no.best_ask - book.no.best_bid).max(0.0);
                            let book_age_ms = now_ms.saturating_sub(book.last_update_ms);

                            // YES opportunity
                            if (MIN_ENTRY_PRICE..=MAX_ENTRY_PRICE).contains(&ya) && ys <= MAX_SPREAD {
                                let e = theo_ask_yes - ya;
                                let maker_entry_cost = cost_model.maker_side_cost_rate();
                                let taker_exit_cost = polymarket_taker_fee(
                                    theo_ask_yes.clamp(0.001, 0.999),
                                    fee_rate,
                                ) + (cost_model.slippage_bps_per_side / 10_000.0);
                                let ne = e - maker_entry_cost - taker_exit_cost;
                                if ne > min_edge && best.as_ref().is_none_or(|current| ne > current.2) {
                                    best = Some((mid.clone(), Side::Yes, ne, ya, r_yes, hs, sigma, tte, book_age_ms));
                                }
                            }
                            // NO opportunity
                            if (MIN_ENTRY_PRICE..=MAX_ENTRY_PRICE).contains(&na) && ns <= MAX_SPREAD {
                                let e = theo_ask_no - na;
                                let maker_entry_cost = cost_model.maker_side_cost_rate();
                                let taker_exit_cost = polymarket_taker_fee(
                                    theo_ask_no.clamp(0.001, 0.999),
                                    fee_rate,
                                ) + (cost_model.slippage_bps_per_side / 10_000.0);
                                let ne = e - maker_entry_cost - taker_exit_cost;
                                if ne > min_edge && best.as_ref().is_none_or(|current| ne > current.2) {
                                    best = Some((mid.clone(), Side::No, ne, na, r_no, hs, sigma, tte, book_age_ms));
                                }
                            }

                            let be = (theo_ask_yes - ya).max(theo_ask_no - na);
                            let sp = be > min_edge;
                            let reason = if !sp { format!("Edge {:.1}bp < {:.1}bp", be*1e4, min_edge*1e4) }
                                else { format!("Edge {:.1}bp > {:.1}bp", be*1e4, min_edge*1e4) };
                            let scan = build_scan_payload(mid, &market.question, STRATEGY_ID,
                                "AS_SPREAD_CAPTURE", "SPREAD_BPS", [ym, nm], hs*1e4,
                                "HALF_SPREAD_BPS", be*1e4, min_edge*1e4, sp, reason, now_ms,
                                serde_json::json!({
                                    "reservation_yes": r_yes, "reservation_no": r_no,
                                    "half_spread": hs, "sigma": sigma, "tte_years": tte,
                                    "gamma": gamma_val, "kappa": kappa_val, "inventory": inv,
                                    "fee_curve_rate": fee_rate, "question": market.question,
                                    "slug": market.slug, "positions_count": positions.len(),
                                }),
                            );
                            publish_event(&mut conn, "arbitrage:scan", scan.to_string()).await;
                        }

                        // ── Execute best entry ──
                        if can_enter {
                            if let Some((mid, side, ne, ep, res, hs, sigma, tte, book_age_ms)) = best {
                                // Skip if already positioned in this market
                                let already_in = positions.iter().any(|p| p.market_id == mid);
                                if already_in { continue; }

                                let market = match market_by_id.get(&mid) { Some(m) => m, None => continue };
                                let cash = read_sim_available_cash(&mut conn).await;
                                let cfg = read_risk_config(&mut conn).await;
                                let sz = compute_strategy_bet_size(&mut conn, STRATEGY_ID, cash, &cfg, 1.0, 0.15).await;
                                if sz >= 1.0 && mode == TradingMode::Live {
                                    let eid = Uuid::new_v4().to_string();
                                    let sl = match side { Side::Yes => "YES", Side::No => "NO" };
                                    let token_id = match side { Side::Yes => market.yes_token.clone(), Side::No => market.no_token.clone() };
                                    let preview_msg = serde_json::json!({
                                        "execution_id": eid,
                                        "strategy": STRATEGY_ID,
                                        "market": "AS Market Maker",
                                        "side": "LIVE_DRY_RUN",
                                        "price": ep,
                                        "size": sz,
                                        "timestamp": now_ms,
                                        "mode": "LIVE_DRY_RUN",
                                        "details": {
                                            "strategy": STRATEGY_ID,
                                            "condition_id": mid,
                                            "question": market.question,
                                            "token_side": sl,
                                            "net_edge_bps": ne*1e4,
                                            "reservation_price": res,
                                            "half_spread": hs,
                                            "sigma": sigma,
                                            "tte_years": tte,
                                            "book_age_ms": book_age_ms,
                                            "variant": variant,
                                            "preflight": {
                                                "venue": "POLYMARKET",
                                                "strategy": STRATEGY_ID,
                                                "orders": [
                                                    {
                                                        "token_id": token_id,
                                                        "condition_id": mid,
                                                        "side": "BUY",
                                                        "price": ep,
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
                                    let sl = match side { Side::Yes => "YES", Side::No => "NO" };
                                    positions.push(Position {
                                        execution_id: eid.clone(), market_id: mid.clone(),
                                        question: market.question.clone(), side, entry_price: ep,
                                        size: sz, timestamp_ms: now_ms,
                                    });
                                    last_entry_ts = now_ms;
                                    info!("[AS_MM] ENTRY {} {} @ {:.2}c edge={:.1}bp", market.question, sl, ep*100.0, ne*1e4);
                                    let exec_msg = serde_json::json!({
                                        "execution_id": eid,
                                        "market": "AS Market Maker",
                                        "side": "ENTRY",
                                        "price": ep,
                                        "size": sz,
                                        "timestamp": now_ms,
                                        "mode": format!("{:?}", mode),
                                        "details": {
                                            "condition_id": mid,
                                            "question": market.question,
                                            "token_side": sl,
                                            "net_edge_bps": ne*1e4,
                                            "reservation_price": res,
                                            "half_spread": hs,
                                            "sigma": sigma,
                                            "tte_years": tte,
                                            "book_age_ms": book_age_ms,
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
