use async_trait::async_trait;
use chrono::Utc;
use futures::{SinkExt, StreamExt};
use log::{error, info, warn};
use redis::AsyncCommands;
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
    read_risk_config,
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
use crate::strategies::simulation::{pair_net_edge_after_cost, SimCostModel};
use crate::strategies::Strategy;

const MIN_NET_EDGE: f64 = 0.0045; // 45 bps minimum expected lock edge after costs.
const MIN_BUFFER_FLOOR: f64 = 0.0060; // Additional conservative floor for stale quotes.
const BOOK_STALE_MS: i64 = 2_000;
const ENTRY_COOLDOWN_MS: i64 = 2_000;
const REFRESH_UNIVERSE_MS: i64 = 45_000;
const ENTRY_EXPIRY_CUTOFF_SECS: i64 = 90;
const MIN_LEG_PRICE: f64 = 0.01;
const MAX_LEG_PRICE: f64 = 0.99;
const MAX_UNIVERSE_MARKETS: usize = 28;
const DEFAULT_MAX_HORIZON_SECS: i64 = 12 * 60 * 60;
const LIVE_PREVIEW_COOLDOWN_MS: i64 = 2_000;

#[derive(Debug, Clone)]
struct PendingPaperTrade {
    execution_id: String,
    market_id: String,
    question: String,
    entry_ts_ms: i64,
    expiry_ts: i64,
    notional_usd: f64,
    ask_sum: f64,
    net_edge: f64,
}

pub struct GraphArbStrategy {
    client: PolymarketClient,
}

impl GraphArbStrategy {
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

    fn max_horizon_secs() -> i64 {
        let parsed = std::env::var("GRAPH_ARB_MAX_HORIZON_SECS")
            .ok()
            .and_then(|raw| raw.trim().parse::<i64>().ok())
            .unwrap_or(DEFAULT_MAX_HORIZON_SECS);
        parsed.max(ENTRY_EXPIRY_CUTOFF_SECS + 60)
    }

    fn build_universe(markets: Vec<MarketTarget>) -> (HashMap<String, MarketTarget>, HashMap<String, TokenBinding>, Vec<String>) {
        let mut market_by_id: HashMap<String, MarketTarget> = HashMap::new();
        let mut token_bindings: HashMap<String, TokenBinding> = HashMap::new();
        let mut asset_ids: Vec<String> = Vec::new();

        for market in markets {
            if market.market_id.is_empty() {
                continue;
            }
            let market_id = market.market_id.clone();
            if market.yes_token.is_empty() || market.no_token.is_empty() || market.yes_token == market.no_token {
                continue;
            }

            token_bindings.insert(
                market.yes_token.clone(),
                TokenBinding {
                    market_key: market_id.clone(),
                    is_yes: true,
                },
            );
            token_bindings.insert(
                market.no_token.clone(),
                TokenBinding {
                    market_key: market_id.clone(),
                    is_yes: false,
                },
            );
            asset_ids.push(market.yes_token.clone());
            asset_ids.push(market.no_token.clone());
            market_by_id.insert(market_id, market);
        }

        (market_by_id, token_bindings, asset_ids)
    }
}

#[async_trait]
impl Strategy for GraphArbStrategy {
    async fn run(&self, redis_client: redis::Client) {
        info!("Starting Graph Arb [multi-market no-arb scanner]");

        let mut conn = match redis_client.get_async_connection().await {
            Ok(c) => c,
            Err(e) => {
                error!("Redis Connect Fail: {}", e);
                return;
            }
        };

        let variant = strategy_variant();
        let cost_model = SimCostModel::from_env();
        let mut books: HashMap<String, BinaryBook> = HashMap::new();
        let mut pending_settlements: Vec<PendingPaperTrade> = Vec::new();
        let mut last_seen_reset_ts = 0_i64;
        let mut last_fire_ms = 0_i64;
        let mut last_live_preview_ms = 0_i64;
        let max_horizon_secs = Self::max_horizon_secs();

        loop {
            let now_ts = Utc::now().timestamp();
            let universe = self
                .client
                .fetch_active_binary_markets(MAX_UNIVERSE_MARKETS)
                .await
                .into_iter()
                .filter(|market| {
                    let expiry = market.expiry_ts.unwrap_or(i64::MAX);
                    expiry > now_ts + ENTRY_EXPIRY_CUTOFF_SECS && expiry <= now_ts + max_horizon_secs
                })
                .collect::<Vec<_>>();

            if universe.is_empty() {
                warn!("Graph arb: no eligible markets found in horizon window");
                publish_heartbeat(&mut conn, "graph_arb").await;
                sleep(Duration::from_secs(5)).await;
                continue;
            }

            let (market_by_id, token_bindings, asset_ids) = Self::build_universe(universe);
            if market_by_id.is_empty() || asset_ids.is_empty() {
                publish_heartbeat(&mut conn, "graph_arb").await;
                sleep(Duration::from_secs(5)).await;
                continue;
            }

            info!("Graph arb tracking {} markets / {} token legs", market_by_id.len(), asset_ids.len());

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
                "assets_ids": asset_ids,
                "type": "market",
            });
            if let Err(e) = poly_ws.send(Message::Text(sub_msg.to_string())).await {
                error!("Graph arb subscription failed: {}", e);
                sleep(Duration::from_secs(2)).await;
                continue;
            }

            let refresh_deadline_ms = Utc::now().timestamp_millis() + REFRESH_UNIVERSE_MS;
            let mut interval = tokio::time::interval(Duration::from_millis(90));

            loop {
                tokio::select! {
                    Some(msg) = poly_ws.next() => {
                        match msg {
                            Ok(Message::Text(text)) => {
                                let _ = update_books_from_market_ws(&text, &token_bindings, &mut books);
                            }
                            Ok(Message::Ping(payload)) => {
                                let _ = poly_ws.send(Message::Pong(payload)).await;
                            }
                            Ok(Message::Close(_)) => break,
                            Ok(Message::Binary(_)) | Ok(Message::Pong(_)) => {}
                            Ok(_) => {}
                            Err(e) => {
                                error!("Graph arb Polymarket WS error: {}", e);
                                break;
                            }
                        }
                    }
                    _ = interval.tick() => {
                        let now_ms = Utc::now().timestamp_millis();
                        let now_secs = Utc::now().timestamp();

                        if now_ms >= refresh_deadline_ms {
                            break;
                        }

                        let reset_ts = read_simulation_reset_ts(&mut conn).await;
                        if reset_ts > last_seen_reset_ts {
                            last_seen_reset_ts = reset_ts;
                            let release_notional = pending_settlements.iter().map(|trade| trade.notional_usd).sum::<f64>();
                            pending_settlements.clear();
                            books.clear();
                            if release_notional > 0.0 {
                                let _ = release_sim_notional_for_strategy(&mut conn, "GRAPH_ARB", release_notional).await;
                            }
                            publish_heartbeat(&mut conn, "graph_arb").await;
                            continue;
                        }

                        let mut carry_forward: Vec<PendingPaperTrade> = Vec::new();
                        for pending in pending_settlements.drain(..) {
                            if now_secs < pending.expiry_ts {
                                carry_forward.push(pending);
                                continue;
                            }

                            let settled_pnl = pending.notional_usd * pending.net_edge;
                            let new_bankroll = settle_sim_position_for_strategy(&mut conn, "GRAPH_ARB", pending.notional_usd, settled_pnl).await;
                            let execution_id = pending.execution_id.clone();
                            let settle_msg = serde_json::json!({
                                "execution_id": execution_id.clone(),
                                "strategy": "GRAPH_ARB",
                                "variant": variant.as_str(),
                                "pnl": settled_pnl,
                                "notional": pending.notional_usd,
                                "timestamp": now_ms,
                                "bankroll": new_bankroll,
                                "mode": "PAPER",
                                "details": {
                                    "action": "SETTLE_GRAPH_ARB",
                                    "market_id": pending.market_id,
                                    "question": pending.question,
                                    "sum": pending.ask_sum,
                                    "net_edge": pending.net_edge,
                                    "hold_ms": now_ms - pending.entry_ts_ms,
                                    "roi": format!("{:.2}%", pending.net_edge * 100.0),
                                }
                            });
                            let _: () = conn.publish("strategy:pnl", settle_msg.to_string()).await.unwrap_or_default();

                            let exec_msg = serde_json::json!({
                                "execution_id": execution_id,
                                "market": "Graph Arb",
                                "side": "SETTLEMENT",
                                "price": pending.ask_sum,
                                "size": pending.notional_usd,
                                "timestamp": now_ms,
                                "mode": "PAPER",
                                "details": {
                                    "market_id": pending.market_id,
                                    "question": pending.question,
                                    "pnl": settled_pnl,
                                    "net_edge": pending.net_edge,
                                }
                            });
                            let _: () = conn.publish("arbitrage:execution", exec_msg.to_string()).await.unwrap_or_default();
                        }
                        pending_settlements = carry_forward;

                        if !is_strategy_enabled(&mut conn, "GRAPH_ARB").await {
                            publish_heartbeat(&mut conn, "graph_arb").await;
                            continue;
                        }

                        let mut best_candidate: Option<(&MarketTarget, f64, f64, f64, f64, i64, f64, f64)> = None;
                        for market in market_by_id.values() {
                            let Some(book) = books.get(&market.market_id) else {
                                continue;
                            };
                            if !book.yes.is_valid() || !book.no.is_valid() || now_ms - book.last_update_ms > BOOK_STALE_MS {
                                continue;
                            }
                            if book.yes.best_ask < MIN_LEG_PRICE
                                || book.yes.best_ask > MAX_LEG_PRICE
                                || book.no.best_ask < MIN_LEG_PRICE
                                || book.no.best_ask > MAX_LEG_PRICE
                            {
                                continue;
                            }

                            let expiry_ts = market.expiry_ts.unwrap_or(i64::MAX);
                            let seconds_to_expiry = expiry_ts.saturating_sub(now_secs);
                            if seconds_to_expiry <= ENTRY_EXPIRY_CUTOFF_SECS || seconds_to_expiry > max_horizon_secs {
                                continue;
                            }

                            let ask_sum = book.yes.best_ask + book.no.best_ask;
                            let gross_edge = Self::gross_pair_edge(ask_sum);
                            let modeled_edge = pair_net_edge_after_cost(gross_edge, cost_model);
                            let net_edge = modeled_edge.min(gross_edge - MIN_BUFFER_FLOOR);
                            let horizon_penalty = ((seconds_to_expiry as f64) / (max_horizon_secs as f64)) * 0.0025;
                            let required_edge = MIN_NET_EDGE + horizon_penalty;
                            if !net_edge.is_finite() || !required_edge.is_finite() {
                                continue;
                            }

                            match best_candidate {
                                Some((_, current_edge, _, _, _, _, _, _)) if net_edge <= current_edge => {}
                                _ => {
                                    best_candidate = Some((
                                        market,
                                        net_edge,
                                        ask_sum,
                                        gross_edge,
                                        required_edge,
                                        seconds_to_expiry,
                                        book.yes.best_ask,
                                        book.no.best_ask,
                                    ));
                                }
                            }
                        }

                        let Some((market, net_edge, ask_sum, gross_edge, required_edge, seconds_to_expiry, yes_ask, no_ask)) = best_candidate else {
                            publish_heartbeat(&mut conn, "graph_arb").await;
                            continue;
                        };

                        let passes_threshold = net_edge >= required_edge;
                        let reason = if passes_threshold {
                            format!(
                                "Graph edge {:.2}% cleared required {:.2}% for horizon {}s",
                                net_edge * 100.0,
                                required_edge * 100.0,
                                seconds_to_expiry
                            )
                        } else {
                            format!(
                                "Graph edge {:.2}% below required {:.2}% for horizon {}s",
                                net_edge * 100.0,
                                required_edge * 100.0,
                                seconds_to_expiry
                            )
                        };

                        let scan_msg = build_scan_payload(
                            &market.market_id,
                            "GRAPH-ARB",
                            "GRAPH_ARB",
                            "CONSTRAINT_EDGE",
                            "RATIO",
                            [ask_sum, gross_edge],
                            ask_sum,
                            "YES_NO_ASK_SUM",
                            net_edge,
                            required_edge,
                            passes_threshold,
                            reason,
                            now_ms,
                            serde_json::json!({
                                "question": market.question.clone(),
                                "market_slug": market.slug.clone(),
                                "gross_edge": gross_edge,
                                "net_edge": net_edge,
                                "required_edge": required_edge,
                                "seconds_to_expiry": seconds_to_expiry,
                                "round_trip_cost_rate": cost_model.round_trip_cost_rate(),
                                "active_pending_settlements": pending_settlements.len(),
                            }),
                        );
                        let _: () = conn.publish("arbitrage:scan", scan_msg.to_string()).await.unwrap_or_default();

                        let trading_mode = read_trading_mode(&mut conn).await;
                        if trading_mode == TradingMode::Live {
                            if !pending_settlements.is_empty() {
                                let release_notional = pending_settlements.iter().map(|trade| trade.notional_usd).sum::<f64>();
                                pending_settlements.clear();
                                if release_notional > 0.0 {
                                    let _ = release_sim_notional_for_strategy(&mut conn, "GRAPH_ARB", release_notional).await;
                                }
                            }

                            if passes_threshold
                                && now_ms - last_live_preview_ms >= LIVE_PREVIEW_COOLDOWN_MS
                            {
                                let available_cash = read_sim_available_cash(&mut conn).await;
                                let risk_cfg = read_risk_config(&mut conn).await;
                                let investment_usd = compute_strategy_bet_size(
                                    &mut conn,
                                    "GRAPH_ARB",
                                    available_cash,
                                    &risk_cfg,
                                    10.0,
                                    0.20,
                                ).await;

                                if investment_usd > 0.0 {
                                    let shares = investment_usd / ask_sum;
                                    let execution_id = Uuid::new_v4().to_string();
                                    let preview_msg = serde_json::json!({
                                        "execution_id": execution_id,
                                        "market": "Graph Arb",
                                        "side": "LIVE_DRY_RUN",
                                        "price": ask_sum,
                                        "size": investment_usd,
                                        "timestamp": now_ms,
                                        "mode": "LIVE_DRY_RUN",
                                        "details": {
                                            "strategy": "GRAPH_ARB",
                                            "condition_id": market.market_id.clone(),
                                            "question": market.question.clone(),
                                            "net_edge": net_edge,
                                            "required_edge": required_edge,
                                            "seconds_to_expiry": seconds_to_expiry,
                                            "preflight": {
                                                "venue": "POLYMARKET",
                                                "strategy": "GRAPH_ARB",
                                                "orders": [
                                                    {
                                                        "token_id": market.yes_token.clone(),
                                                        "condition_id": market.market_id.clone(),
                                                        "side": "BUY",
                                                        "price": yes_ask,
                                                        "size": shares,
                                                        "size_unit": "SHARES",
                                                    },
                                                    {
                                                        "token_id": market.no_token.clone(),
                                                        "condition_id": market.market_id.clone(),
                                                        "side": "BUY",
                                                        "price": no_ask,
                                                        "size": shares,
                                                        "size_unit": "SHARES",
                                                    }
                                                ]
                                            }
                                        }
                                    });
                                    let _: () = conn.publish("arbitrage:execution", preview_msg.to_string()).await.unwrap_or_default();
                                    last_live_preview_ms = now_ms;
                                }
                            }

                            publish_heartbeat(&mut conn, "graph_arb").await;
                            continue;
                        }

                        if passes_threshold && (now_ms - last_fire_ms) >= ENTRY_COOLDOWN_MS {
                            let available_cash = read_sim_available_cash(&mut conn).await;
                            let risk_cfg = read_risk_config(&mut conn).await;
                            let investment_usd = compute_strategy_bet_size(
                                &mut conn,
                                "GRAPH_ARB",
                                available_cash,
                                &risk_cfg,
                                10.0,
                                0.20,
                            ).await;

                            if investment_usd > 0.0 && reserve_sim_notional_for_strategy(&mut conn, "GRAPH_ARB", investment_usd).await {
                                let execution_id = Uuid::new_v4().to_string();
                                pending_settlements.push(PendingPaperTrade {
                                    execution_id: execution_id.clone(),
                                    market_id: market.market_id.clone(),
                                    question: market.question.clone(),
                                    entry_ts_ms: now_ms,
                                    expiry_ts: now_secs + seconds_to_expiry,
                                    notional_usd: investment_usd,
                                    ask_sum,
                                    net_edge,
                                });
                                let exec_msg = serde_json::json!({
                                    "execution_id": execution_id,
                                    "market": "Graph Arb",
                                    "side": "ENTRY",
                                    "price": ask_sum,
                                    "size": investment_usd,
                                    "timestamp": now_ms,
                                    "mode": "PAPER",
                                    "details": {
                                        "condition_id": market.market_id.clone(),
                                        "question": market.question.clone(),
                                        "net_edge": net_edge,
                                        "required_edge": required_edge,
                                        "seconds_to_expiry": seconds_to_expiry,
                                    }
                                });
                                let _: () = conn.publish("arbitrage:execution", exec_msg.to_string()).await.unwrap_or_default();
                                last_fire_ms = now_ms;
                            }
                        }

                        publish_heartbeat(&mut conn, "graph_arb").await;
                    }
                }
            }
        }
    }
}
