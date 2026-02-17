use async_trait::async_trait;
use chrono::Utc;
use futures::{SinkExt, StreamExt};
use log::{error, info};
use redis::AsyncCommands;
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
use crate::strategies::simulation::{pair_net_edge_after_cost, polymarket_taker_fee, SimCostModel};
use crate::strategies::Strategy;

fn min_net_edge() -> f64 {
    std::env::var("ATOMIC_ARB_MIN_NET_EDGE")
        .ok().and_then(|v| v.parse().ok()).unwrap_or(0.0010) // 10 bps default (was 35)
}
fn min_buffer_floor() -> f64 {
    std::env::var("ATOMIC_ARB_MIN_BUFFER_FLOOR")
        .ok().and_then(|v| v.parse().ok()).unwrap_or(0.0020) // 20 bps default (was 60)
}
fn fee_curve_rate() -> f64 {
    std::env::var("POLYMARKET_FEE_CURVE_RATE")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|v| v.is_finite() && *v > 0.0 && *v <= 0.10)
        .unwrap_or(0.022)
}
const FIRE_COOLDOWN_MS: i64 = 2_000;
const BOOK_STALE_MS: i64 = 1_500;
const ENTRY_EXPIRY_CUTOFF_SECS: i64 = 20;
const MIN_LEG_PRICE: f64 = 0.01;
const MAX_LEG_PRICE: f64 = 0.99;
const MAX_HOLD_MS: i64 = 120_000;

#[derive(Debug, Clone)]
struct PendingPaperTrade {
    execution_id: String,
    market_id: String,
    entry_ts_ms: i64,
    notional_usd: f64,
    ask_sum: f64,
    net_edge: f64,
}

pub struct AtomicArbStrategy {
    client: PolymarketClient,
}

impl AtomicArbStrategy {
    pub fn new() -> Self {
        Self {
            client: PolymarketClient::new(),
        }
    }

    fn gross_pair_edge(ask_sum: f64) -> f64 {
        if ask_sum <= 0.0 {
            return -1.0;
        }
        (1.0 / ask_sum) - 1.0
    }
}

#[async_trait]
impl Strategy for AtomicArbStrategy {
    async fn run(&self, redis_client: redis::Client) {
        info!("Starting Atomic Arb [YES ask + NO ask < 1.0]...");

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

        loop {
            let target_market = loop {
                if let Some(m) = self.client.fetch_current_market("BTC").await {
                    break m;
                }
                sleep(Duration::from_secs(5)).await;
            };

            info!("Atomic arb scanning market: {}", target_market.slug);

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

            let mut interval = tokio::time::interval(Duration::from_millis(75));
            let market_expiry = target_market.expiry_ts.unwrap_or_else(|| {
                // Fallback: parse expiry from slug pattern "...-15m-{timestamp}"
                target_market.slug.split("-15m-")
                    .nth(1)
                    .and_then(|s| s.parse::<i64>().ok())
                    .map(|s| s + 900)
                    .unwrap_or(Utc::now().timestamp() + 900)
            });

            let mut book = BinaryBook::default();
            let mut last_fire_ms = 0_i64;
            let mut pending_settlements: Vec<PendingPaperTrade> = Vec::new();

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
                                error!("Atomic arb Polymarket WS error: {}", e);
                                break;
                            }
                        }
                    }
                    _ = interval.tick() => {
                        let now_ms = Utc::now().timestamp_millis();

                        if !is_strategy_enabled(&mut conn, "ATOMIC_ARB").await {
                            let release_notional = pending_settlements
                                .iter()
                                .map(|pending| pending.notional_usd)
                                .sum::<f64>();
                            if release_notional > 0.0 {
                                pending_settlements.clear();
                                let _ = release_sim_notional_for_strategy(&mut conn, "ATOMIC_ARB", release_notional).await;
                            }
                            publish_heartbeat(&mut conn, "atomic_arb").await;
                            continue;
                        }

                        // Risk guard cooldown — skip entry if backend set a post-loss cooldown.
                        let cooldown_until = read_risk_guard_cooldown(&mut conn, "ATOMIC_ARB").await;
                        if cooldown_until > 0 && now_ms < cooldown_until {
                            publish_heartbeat(&mut conn, "atomic_arb").await;
                            continue;
                        }

                        if !book.yes.is_valid() || !book.no.is_valid() || now_ms - book.last_update_ms > BOOK_STALE_MS {
                            publish_heartbeat(&mut conn, "atomic_arb").await;
                            continue;
                        }

                        let reset_ts = read_simulation_reset_ts(&mut conn).await;
                        if reset_ts > last_seen_reset_ts {
                            last_seen_reset_ts = reset_ts;
                            last_fire_ms = 0;
                            let release_notional = pending_settlements.iter().map(|p| p.notional_usd).sum::<f64>();
                            pending_settlements.clear();
                            if release_notional > 0.0 {
                                let _ = release_sim_notional_for_strategy(&mut conn, "ATOMIC_ARB", release_notional).await;
                            }
                        }

                        // Force-close stale pending trades to free capital
                        {
                            let mut keep: Vec<PendingPaperTrade> = Vec::new();
                            for pending in pending_settlements.drain(..) {
                                let hold_ms = now_ms - pending.entry_ts_ms;
                                if hold_ms >= MAX_HOLD_MS {
                                    let _ = settle_sim_position_for_strategy(&mut conn, "ATOMIC_ARB", pending.notional_usd, 0.0).await;
                                } else {
                                    keep.push(pending);
                                }
                            }
                            pending_settlements = keep;
                        }

                        if book.yes.best_ask < MIN_LEG_PRICE
                            || book.yes.best_ask > MAX_LEG_PRICE
                            || book.no.best_ask < MIN_LEG_PRICE
                            || book.no.best_ask > MAX_LEG_PRICE
                        {
                            publish_heartbeat(&mut conn, "atomic_arb").await;
                            continue;
                        }

                        let ask_sum = book.yes.best_ask + book.no.best_ask;
                        let gross_edge = Self::gross_pair_edge(ask_sum);
                        let static_net_edge = pair_net_edge_after_cost(gross_edge, cost_model);
                        let yes_fee = polymarket_taker_fee(book.yes.best_ask, fee_curve_rate());
                        let no_fee = polymarket_taker_fee(book.no.best_ask, fee_curve_rate());
                        let slippage_rate = cost_model.slippage_bps_per_side / 10_000.0;
                        let total_dynamic_cost = yes_fee + no_fee + 2.0 * slippage_rate;
                        let modeled_edge = gross_edge - total_dynamic_cost;
                        let net_edge = modeled_edge
                            .min(static_net_edge)
                            .min(gross_edge - min_buffer_floor());
                        let passes_threshold = net_edge >= min_net_edge();
                        let seconds_to_expiry = (market_expiry - Utc::now().timestamp()).max(0);
                        let reason = if passes_threshold {
                            format!(
                                "Net edge {:.2}% cleared entry threshold {:.2}% after {:.2}% modeled+floor cost buffer",
                                net_edge * 100.0,
                                min_net_edge() * 100.0,
                                (gross_edge - net_edge) * 100.0
                            )
                        } else {
                            format!(
                                "Net edge {:.2}% below required {:.2}% after {:.2}% modeled+floor cost buffer",
                                net_edge * 100.0,
                                min_net_edge() * 100.0,
                                (gross_edge - net_edge) * 100.0
                            )
                        };

                        let scan_msg = build_scan_payload(
                            &target_market.market_id,
                            "ATOMIC-ARB",
                            "ATOMIC_ARB",
                            "NET_EDGE",
                            "RATIO",
                            [book.yes.best_ask, book.no.best_ask],
                            ask_sum,
                            "YES_NO_ASK_SUM",
                            net_edge,
                            min_net_edge(),
                            passes_threshold,
                            reason,
                            now_ms,
                            serde_json::json!({
                                "yes_ask": book.yes.best_ask,
                                "no_ask": book.no.best_ask,
                                "gross_edge": gross_edge,
                                "static_net_edge": static_net_edge,
                                "modeled_edge": modeled_edge,
                                "buffer_used": gross_edge - net_edge,
                                "dynamic_cost_rate": total_dynamic_cost,
                                "seconds_to_expiry": seconds_to_expiry,
                            }),
                        );
                        let _: () = conn.publish("arbitrage:scan", scan_msg.to_string()).await.unwrap_or_default();

                        if read_trading_mode(&mut conn).await == TradingMode::Live {
                            if !pending_settlements.is_empty() {
                                let release_notional = pending_settlements
                                    .iter()
                                    .map(|pending| pending.notional_usd)
                                    .sum::<f64>();
                                info!(
                                    "Atomic arb: clearing {} pending paper settlement(s) after LIVE mode switch",
                                    pending_settlements.len()
                                );
                                pending_settlements.clear();
                                if release_notional > 0.0 {
                                    let _ = release_sim_notional_for_strategy(&mut conn, "ATOMIC_ARB", release_notional).await;
                                }
                            }
                            if passes_threshold
                                && (now_ms - last_fire_ms) >= FIRE_COOLDOWN_MS
                                && seconds_to_expiry > ENTRY_EXPIRY_CUTOFF_SECS
                            {
                                let available_cash = read_sim_available_cash(&mut conn).await;
                                let risk_cfg = read_risk_config(&mut conn).await;
                                let investment_usd = compute_strategy_bet_size(
                                    &mut conn,
                                    "ATOMIC_ARB",
                                    available_cash,
                                    &risk_cfg,
                                    10.0,
                                    0.20,
                                ).await;
                                if investment_usd > 0.0 {
                                    let shares = investment_usd / ask_sum;
                                    let yes_notional = shares * book.yes.best_ask;
                                    let no_notional = shares * book.no.best_ask;
                                    let expected_pnl = investment_usd * net_edge;
                                    let execution_id = Uuid::new_v4().to_string();
                                    let dry_run_msg = serde_json::json!({
                                        "execution_id": execution_id,
                                        "market": "Atomic Arb",
                                        "side": "LIVE_DRY_RUN",
                                        "price": ask_sum,
                                        "size": investment_usd,
                                        "timestamp": now_ms,
                                        "mode": "LIVE_DRY_RUN",
                                        "details": {
                                            "yes_ask": book.yes.best_ask,
                                            "no_ask": book.no.best_ask,
                                            "gross_edge": gross_edge,
                                            "net_edge": net_edge,
                                            "threshold": min_net_edge(),
                                            "shares": shares,
                                            "yes_notional": yes_notional,
                                            "no_notional": no_notional,
                                            "expected_pnl": expected_pnl,
                                            "preflight": {
                                                "venue": "POLYMARKET",
                                                "strategy": "ATOMIC_ARB",
                                                "orders": [
                                                    {
                                                        "token_id": target_market.yes_token,
                                                        "condition_id": target_market.market_id,
                                                        "side": "BUY",
                                                        "price": book.yes.best_ask,
                                                        "size": shares,
                                                        "size_unit": "SHARES",
                                                    },
                                                    {
                                                        "token_id": target_market.no_token,
                                                        "condition_id": target_market.market_id,
                                                        "side": "BUY",
                                                        "price": book.no.best_ask,
                                                        "size": shares,
                                                        "size_unit": "SHARES",
                                                    }
                                                ]
                                            }
                                        }
                                    });
                                    let _: () = conn.publish("arbitrage:execution", dry_run_msg.to_string()).await.unwrap_or_default();
                                    last_fire_ms = now_ms;
                                }
                            }
                            publish_heartbeat(&mut conn, "atomic_arb").await;
                            continue;
                        }

                        // Skip if already positioned in this market
                        let already_in = pending_settlements.iter().any(|p| p.market_id == target_market.market_id);
                        if net_edge >= min_net_edge()
                            && (now_ms - last_fire_ms) >= FIRE_COOLDOWN_MS
                            && seconds_to_expiry > ENTRY_EXPIRY_CUTOFF_SECS
                            && !already_in
                        {
                            let available_cash = read_sim_available_cash(&mut conn).await;
                            let risk_cfg = read_risk_config(&mut conn).await;
                            let investment_usd = compute_strategy_bet_size(
                                &mut conn,
                                "ATOMIC_ARB",
                                available_cash,
                                &risk_cfg,
                                10.0,
                                0.20,
                            ).await;
                            if investment_usd <= 0.0 {
                                publish_heartbeat(&mut conn, "atomic_arb").await;
                                continue;
                            }

                            if !reserve_sim_notional_for_strategy(&mut conn, "ATOMIC_ARB", investment_usd).await {
                                publish_heartbeat(&mut conn, "atomic_arb").await;
                                continue;
                            }

                            let shares = investment_usd / ask_sum;
                            let expected_profit = investment_usd * net_edge;
                            let execution_id = Uuid::new_v4().to_string();

                            pending_settlements.push(PendingPaperTrade {
                                execution_id: execution_id.clone(),
                                market_id: target_market.market_id.clone(),
                                entry_ts_ms: now_ms,
                                notional_usd: investment_usd,
                                ask_sum,
                                net_edge,
                            });

                            let exec_msg = serde_json::json!({
                                "execution_id": execution_id,
                                "market": "Atomic Arb",
                                "side": "ENTRY",
                                "price": ask_sum,
                                "size": investment_usd,
                                "timestamp": now_ms,
                                "mode": "PAPER",
                                "details": {
                                    "yes_ask": book.yes.best_ask,
                                    "no_ask": book.no.best_ask,
                                    "shares": shares,
                                    "gross_edge": gross_edge,
                                    "net_edge": net_edge,
                                    "expected_profit": expected_profit,
                                    "settlement_status": "PENDING_EXPIRY",
                                }
                            });
                            let _: () = conn.publish("arbitrage:execution", exec_msg.to_string()).await.unwrap_or_default();

                            last_fire_ms = now_ms;
                        }

                        publish_heartbeat(&mut conn, "atomic_arb").await;
                    }
                }

                if Utc::now().timestamp() > market_expiry {
                    if !pending_settlements.is_empty() {
                        let settle_ts_ms = Utc::now().timestamp_millis();
                        let mut settled_count = 0_usize;
                        for pending in pending_settlements.drain(..) {
                            let settled_pnl = pending.notional_usd * pending.net_edge;
                            let new_bankroll = settle_sim_position_for_strategy(&mut conn, "ATOMIC_ARB", pending.notional_usd, settled_pnl).await;
                            settled_count = settled_count.saturating_add(1);
                            let execution_id = pending.execution_id.clone();

                            let pnl_msg = serde_json::json!({
                                "execution_id": execution_id.clone(),
                                "strategy": "ATOMIC_ARB",
                                "variant": variant.as_str(),
                                "pnl": settled_pnl,
                                "notional": pending.notional_usd,
                                "timestamp": settle_ts_ms,
                                "bankroll": new_bankroll,
                                "mode": "PAPER",
                                "details": {
                                    "action": "SETTLE_ATOMIC",
                                    "sum": pending.ask_sum,
                                    "net_edge": pending.net_edge,
                                    "hold_ms": settle_ts_ms - pending.entry_ts_ms,
                                    "roi": format!("{:.2}%", pending.net_edge * 100.0),
                                }
                            });
                            let _: () = conn.publish("strategy:pnl", pnl_msg.to_string()).await.unwrap_or_default();

                            let settle_msg = serde_json::json!({
                                "execution_id": execution_id,
                                "market": "Atomic Arb",
                                "side": "SETTLEMENT",
                                "price": pending.ask_sum,
                                "size": pending.notional_usd,
                                "timestamp": settle_ts_ms,
                                "mode": "PAPER",
                                "details": {
                                    "pnl": settled_pnl,
                                    "net_edge": pending.net_edge,
                                    "hold_ms": settle_ts_ms - pending.entry_ts_ms,
                                }
                            });
                            let _: () = conn.publish("arbitrage:execution", settle_msg.to_string()).await.unwrap_or_default();
                        }

                        info!(
                            "Atomic arb settled {} pending paper position(s) for market {}",
                            settled_count,
                            target_market.slug
                        );
                    }
                    break;
                }
            }
        }
    }
}
