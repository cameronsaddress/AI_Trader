use async_trait::async_trait;
use chrono::Utc;
use futures::{SinkExt, StreamExt};
use log::{error, info};
use redis::AsyncCommands;
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
use crate::strategies::simulation::{pair_net_edge_after_cost, SimCostModel};
use crate::strategies::Strategy;

const MIN_NET_EDGE: f64 = 0.0035; // 35 bps after fees/slippage
const MIN_BUFFER_FLOOR: f64 = 0.0060; // conservative lower bound buffer (60 bps)
const FIRE_COOLDOWN_MS: i64 = 2_000;
const BOOK_STALE_MS: i64 = 1_500;
const ENTRY_EXPIRY_CUTOFF_SECS: i64 = 20;
const MIN_LEG_PRICE: f64 = 0.01;
const MAX_LEG_PRICE: f64 = 0.99;

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
            let market_expiry = target_market
                .slug
                .split("-15m-")
                .nth(1)
                .and_then(|s| s.parse::<i64>().ok())
                .map(|s| s + 900)
                .unwrap_or(Utc::now().timestamp() + 900);

            let mut book = BinaryBook::default();
            let mut last_fire_ms = 0_i64;

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
                        let modeled_edge = pair_net_edge_after_cost(gross_edge, cost_model);
                        let net_edge = modeled_edge.min(gross_edge - MIN_BUFFER_FLOOR);
                        let passes_threshold = net_edge >= MIN_NET_EDGE;
                        let seconds_to_expiry = (market_expiry - Utc::now().timestamp()).max(0);
                        let reason = if passes_threshold {
                            format!(
                                "Net edge {:.2}% cleared entry threshold {:.2}% after {:.2}% modeled+floor cost buffer",
                                net_edge * 100.0,
                                MIN_NET_EDGE * 100.0,
                                (gross_edge - net_edge) * 100.0
                            )
                        } else {
                            format!(
                                "Net edge {:.2}% below required {:.2}% after {:.2}% modeled+floor cost buffer",
                                net_edge * 100.0,
                                MIN_NET_EDGE * 100.0,
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
                            MIN_NET_EDGE,
                            passes_threshold,
                            reason,
                            now_ms,
                            serde_json::json!({
                                "yes_ask": book.yes.best_ask,
                                "no_ask": book.no.best_ask,
                                "gross_edge": gross_edge,
                                "modeled_edge": modeled_edge,
                                "buffer_used": gross_edge - net_edge,
                                "per_side_cost_rate": cost_model.per_side_cost_rate(),
                                "seconds_to_expiry": seconds_to_expiry,
                            }),
                        );
                        let _: () = conn.publish("arbitrage:scan", scan_msg.to_string()).await.unwrap_or_default();

                        if read_trading_mode(&mut conn).await == TradingMode::Live {
                            if passes_threshold
                                && (now_ms - last_fire_ms) >= FIRE_COOLDOWN_MS
                                && seconds_to_expiry > ENTRY_EXPIRY_CUTOFF_SECS
                            {
                                let bankroll = read_sim_bankroll(&mut conn).await;
                                let risk_cfg = read_risk_config(&mut conn).await;
                                let investment_usd = compute_bet_size(bankroll, &risk_cfg, 10.0, 0.20);
                                if investment_usd > 0.0 {
                                    let shares = investment_usd / ask_sum;
                                    let yes_notional = shares * book.yes.best_ask;
                                    let no_notional = shares * book.no.best_ask;
                                    let expected_pnl = investment_usd * net_edge;
                                    let dry_run_msg = serde_json::json!({
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
                                            "threshold": MIN_NET_EDGE,
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

                        if net_edge > MIN_NET_EDGE
                            && (now_ms - last_fire_ms) >= FIRE_COOLDOWN_MS
                            && seconds_to_expiry > ENTRY_EXPIRY_CUTOFF_SECS
                        {
                            let bankroll = read_sim_bankroll(&mut conn).await;
                            let risk_cfg = read_risk_config(&mut conn).await;
                            let investment_usd = compute_bet_size(bankroll, &risk_cfg, 10.0, 0.20);
                            if investment_usd <= 0.0 {
                                publish_heartbeat(&mut conn, "atomic_arb").await;
                                continue;
                            }

                            let shares = investment_usd / ask_sum;
                            let expected_profit = investment_usd * net_edge;
                            let new_bankroll = apply_sim_pnl(&mut conn, expected_profit).await;

                            let exec_msg = serde_json::json!({
                                "market": "Atomic Arb",
                                "side": "ATOMIC_ARB",
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
                                }
                            });
                            let _: () = conn.publish("arbitrage:execution", exec_msg.to_string()).await.unwrap_or_default();

                            let pnl_msg = serde_json::json!({
                                "strategy": "ATOMIC_ARB",
                                "variant": variant.as_str(),
                                "pnl": expected_profit,
                                "notional": investment_usd,
                                "timestamp": now_ms,
                                "bankroll": new_bankroll,
                                "mode": "PAPER",
                                "details": {
                                    "action": "ARB_WIN",
                                    "sum": ask_sum,
                                    "net_edge": net_edge,
                                    "roi": format!("{:.2}%", net_edge * 100.0),
                                }
                            });
                            let _: () = conn.publish("strategy:pnl", pnl_msg.to_string()).await.unwrap_or_default();

                            last_fire_ms = now_ms;
                        }

                        publish_heartbeat(&mut conn, "atomic_arb").await;
                    }
                }

                if Utc::now().timestamp() > market_expiry {
                    break;
                }
            }
        }
    }
}
