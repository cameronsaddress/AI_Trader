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
use crate::strategies::simulation::{realized_pnl, SimCostModel};
use crate::strategies::Strategy;

const BOOK_STALE_MS: i64 = 2_000;
const ENTRY_COOLDOWN_MS: i64 = 3_000;
const REFRESH_UNIVERSE_MS: i64 = 35_000;
const LIVE_PREVIEW_COOLDOWN_MS: i64 = 2_000;
const MAX_UNIVERSE_MARKETS: usize = 32;
const MIN_TIME_TO_EXPIRY_SECS: i64 = 90;
const MAX_TIME_TO_EXPIRY_SECS: i64 = 6 * 60 * 60;
const MIN_PARITY_EDGE: f64 = 0.020; // 2.0c
const EXIT_PARITY_EDGE: f64 = 0.004; // 0.4c
const MAX_ENTRY_SPREAD: f64 = 0.08; // 8c
const MIN_ENTRY_PRICE: f64 = 0.06;
const MAX_ENTRY_PRICE: f64 = 0.94;
const TAKE_PROFIT_PCT: f64 = 0.07;
const STOP_LOSS_PCT: f64 = -0.04;
const MAX_HOLD_MS: i64 = 120_000;
const MAX_OPEN_POSITIONS: usize = 4;

#[derive(Debug, Clone, Copy)]
enum Side {
    Yes,
    No,
}

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

pub struct ConvergenceCarryStrategy {
    client: PolymarketClient,
}

impl ConvergenceCarryStrategy {
    pub fn new() -> Self {
        Self {
            client: PolymarketClient::new(),
        }
    }

    fn build_universe(markets: Vec<MarketTarget>) -> (HashMap<String, MarketTarget>, HashMap<String, TokenBinding>, Vec<String>) {
        let mut market_by_id: HashMap<String, MarketTarget> = HashMap::new();
        let mut token_bindings: HashMap<String, TokenBinding> = HashMap::new();
        let mut asset_ids: Vec<String> = Vec::new();

        for market in markets {
            if market.market_id.is_empty()
                || market.yes_token.is_empty()
                || market.no_token.is_empty()
                || market.yes_token == market.no_token
            {
                continue;
            }

            let market_id = market.market_id.clone();
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

    fn parity_edges(book: &BinaryBook) -> (f64, f64) {
        let yes_mid = book.yes.mid();
        let no_mid = book.no.mid();
        let fair_yes = (1.0 - no_mid).clamp(0.01, 0.99);
        let fair_no = (1.0 - yes_mid).clamp(0.01, 0.99);
        let yes_edge = fair_yes - book.yes.best_ask;
        let no_edge = fair_no - book.no.best_ask;
        (yes_edge, no_edge)
    }

    fn entry_spread(book: &BinaryBook, side: Side) -> f64 {
        match side {
            Side::Yes => (book.yes.best_ask - book.yes.best_bid).max(0.0),
            Side::No => (book.no.best_ask - book.no.best_bid).max(0.0),
        }
    }

    fn entry_price(book: &BinaryBook, side: Side) -> f64 {
        match side {
            Side::Yes => book.yes.best_ask,
            Side::No => book.no.best_ask,
        }
    }

    fn mark_price(book: &BinaryBook, side: Side) -> f64 {
        match side {
            Side::Yes => book.yes.best_bid,
            Side::No => book.no.best_bid,
        }
    }

    fn side_label(side: Side) -> &'static str {
        match side {
            Side::Yes => "YES",
            Side::No => "NO",
        }
    }
}

#[async_trait]
impl Strategy for ConvergenceCarryStrategy {
    async fn run(&self, redis_client: redis::Client) {
        info!("Starting Convergence Carry [multi-market parity reversion]");

        let mut conn = match redis_client.get_async_connection().await {
            Ok(c) => c,
            Err(e) => {
                error!("Redis connect failed: {}", e);
                return;
            }
        };

        let variant = strategy_variant();
        let cost_model = SimCostModel::from_env();
        let mut books: HashMap<String, BinaryBook> = HashMap::new();
        let mut open_positions: Vec<Position> = Vec::new();
        let mut last_seen_reset_ts = 0_i64;
        let mut last_entry_ms = 0_i64;
        let mut last_live_preview_ms = 0_i64;

        loop {
            let now_secs = Utc::now().timestamp();
            let universe = self
                .client
                .fetch_active_binary_markets(MAX_UNIVERSE_MARKETS)
                .await
                .into_iter()
                .filter(|market| {
                    let expiry = market.expiry_ts.unwrap_or(i64::MAX);
                    expiry > now_secs + MIN_TIME_TO_EXPIRY_SECS && expiry <= now_secs + MAX_TIME_TO_EXPIRY_SECS
                })
                .collect::<Vec<_>>();

            if universe.is_empty() {
                warn!("Convergence carry: no eligible markets in configured horizon");
                publish_heartbeat(&mut conn, "convergence_carry").await;
                sleep(Duration::from_secs(4)).await;
                continue;
            }

            let (market_by_id, token_bindings, asset_ids) = Self::build_universe(universe);
            if market_by_id.is_empty() || asset_ids.is_empty() {
                publish_heartbeat(&mut conn, "convergence_carry").await;
                sleep(Duration::from_secs(4)).await;
                continue;
            }

            info!(
                "Convergence carry tracking {} markets / {} token legs",
                market_by_id.len(),
                asset_ids.len()
            );

            let poly_url = match Url::parse(WS_URL) {
                Ok(url) => url,
                Err(e) => {
                    error!("Invalid polymarket WS URL: {}", e);
                    sleep(Duration::from_secs(2)).await;
                    continue;
                }
            };
            use tokio_tungstenite::tungstenite::client::IntoClientRequest;
            let mut request = match poly_url.into_client_request() {
                Ok(req) => req,
                Err(e) => {
                    error!("Failed to create polymarket WS request: {}", e);
                    sleep(Duration::from_secs(2)).await;
                    continue;
                }
            };
            if let Ok(ua) = "Mozilla/5.0".parse() {
                request.headers_mut().insert("User-Agent", ua);
            }

            let mut poly_ws = match connect_async(request).await {
                Ok((ws, _)) => ws,
                Err(e) => {
                    error!("Convergence carry WS connect failed: {}", e);
                    sleep(Duration::from_secs(2)).await;
                    continue;
                }
            };

            let sub_msg = serde_json::json!({
                "assets_ids": asset_ids,
                "type": "market",
            });
            if let Err(e) = poly_ws.send(Message::Text(sub_msg.to_string())).await {
                error!("Convergence carry subscription failed: {}", e);
                sleep(Duration::from_secs(2)).await;
                continue;
            }

            let refresh_deadline_ms = Utc::now().timestamp_millis() + REFRESH_UNIVERSE_MS;
            let mut interval = tokio::time::interval(Duration::from_millis(110));

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
                                error!("Convergence carry WS error: {}", e);
                                break;
                            }
                        }
                    }
                    _ = interval.tick() => {
                        let now_ms = Utc::now().timestamp_millis();
                        let now_ts = Utc::now().timestamp();
                        if now_ms >= refresh_deadline_ms {
                            break;
                        }

                        let reset_ts = read_simulation_reset_ts(&mut conn).await;
                        if reset_ts > last_seen_reset_ts {
                            last_seen_reset_ts = reset_ts;
                            let release_notional = open_positions.iter().map(|pos| pos.size).sum::<f64>();
                            open_positions.clear();
                            books.clear();
                            last_entry_ms = 0;
                            if release_notional > 0.0 {
                                let _ = release_sim_notional_for_strategy(&mut conn, "CONVERGENCE_CARRY", release_notional).await;
                            }
                            publish_heartbeat(&mut conn, "convergence_carry").await;
                            continue;
                        }

                        let enabled = is_strategy_enabled(&mut conn, "CONVERGENCE_CARRY").await;
                        if !enabled {
                            if !open_positions.is_empty() {
                                let mut released = 0.0;
                                for pos in open_positions.drain(..) {
                                    let mark = books
                                        .get(&pos.market_id)
                                        .map(|book| Self::mark_price(book, pos.side))
                                        .unwrap_or(0.0);
                                    if mark > 0.0 && pos.entry_price > 0.0 {
                                        let gross_return = (mark - pos.entry_price) / pos.entry_price;
                                        let pnl = realized_pnl(pos.size, gross_return, cost_model);
                                        let _ = settle_sim_position_for_strategy(&mut conn, "CONVERGENCE_CARRY", pos.size, pnl).await;
                                    } else {
                                        released += pos.size;
                                    }
                                }
                                if released > 0.0 {
                                    let _ = release_sim_notional_for_strategy(&mut conn, "CONVERGENCE_CARRY", released).await;
                                }
                            }
                            publish_heartbeat(&mut conn, "convergence_carry").await;
                            continue;
                        }

                        // Exit management first.
                        let mut keep_positions: Vec<Position> = Vec::new();
                        for pos in open_positions.drain(..) {
                            let Some(book) = books.get(&pos.market_id) else {
                                keep_positions.push(pos);
                                continue;
                            };
                            if now_ms - book.last_update_ms > BOOK_STALE_MS || !book.yes.is_valid() || !book.no.is_valid() {
                                keep_positions.push(pos);
                                continue;
                            }
                            let mark = Self::mark_price(book, pos.side);
                            if mark <= 0.0 || pos.entry_price <= 0.0 {
                                keep_positions.push(pos);
                                continue;
                            }

                            let hold_ms = now_ms - pos.timestamp_ms;
                            let gross_return = (mark - pos.entry_price) / pos.entry_price;
                            let (yes_edge, no_edge) = Self::parity_edges(book);
                            let active_edge = match pos.side {
                                Side::Yes => yes_edge,
                                Side::No => no_edge,
                            };

                            let close_reason = if gross_return >= TAKE_PROFIT_PCT {
                                Some("TAKE_PROFIT")
                            } else if gross_return <= STOP_LOSS_PCT {
                                Some("STOP_LOSS")
                            } else if hold_ms >= MAX_HOLD_MS {
                                Some("TIME_EXIT")
                            } else if active_edge <= EXIT_PARITY_EDGE {
                                Some("EDGE_DECAY")
                            } else {
                                None
                            };

                            if let Some(reason) = close_reason {
                                let pnl = realized_pnl(pos.size, gross_return, cost_model);
                                let bankroll = settle_sim_position_for_strategy(&mut conn, "CONVERGENCE_CARRY", pos.size, pnl).await;
                                let pnl_msg = serde_json::json!({
                                    "execution_id": pos.execution_id,
                                    "strategy": "CONVERGENCE_CARRY",
                                    "variant": variant.as_str(),
                                    "pnl": pnl,
                                    "notional": pos.size,
                                    "timestamp": now_ms,
                                    "bankroll": bankroll,
                                    "mode": "PAPER",
                                    "details": {
                                        "action": "CLOSE_POSITION",
                                        "reason": reason,
                                        "market_id": pos.market_id,
                                        "question": pos.question,
                                        "side": Self::side_label(pos.side),
                                        "entry": pos.entry_price,
                                        "exit": mark,
                                        "gross_return": gross_return,
                                        "hold_ms": hold_ms,
                                        "parity_edge": active_edge,
                                    }
                                });
                                let _: () = conn.publish("strategy:pnl", pnl_msg.to_string()).await.unwrap_or_default();
                            } else {
                                keep_positions.push(pos);
                            }
                        }
                        open_positions = keep_positions;

                        let mut best_candidate: Option<(&MarketTarget, Side, f64, f64, f64, f64, i64)> = None;
                        for market in market_by_id.values() {
                            let Some(book) = books.get(&market.market_id) else {
                                continue;
                            };
                            if !book.yes.is_valid() || !book.no.is_valid() || now_ms - book.last_update_ms > BOOK_STALE_MS {
                                continue;
                            }
                            let expiry_ts = market.expiry_ts.unwrap_or(i64::MAX);
                            let time_to_expiry = expiry_ts.saturating_sub(now_ts);
                            if time_to_expiry <= MIN_TIME_TO_EXPIRY_SECS || time_to_expiry > MAX_TIME_TO_EXPIRY_SECS {
                                continue;
                            }

                            let (yes_edge, no_edge) = Self::parity_edges(book);
                            let (side, edge) = if yes_edge >= no_edge {
                                (Side::Yes, yes_edge)
                            } else {
                                (Side::No, no_edge)
                            };

                            let entry_price = Self::entry_price(book, side);
                            let spread = Self::entry_spread(book, side);
                            if entry_price < MIN_ENTRY_PRICE
                                || entry_price > MAX_ENTRY_PRICE
                                || spread > MAX_ENTRY_SPREAD
                            {
                                continue;
                            }

                            let threshold = MIN_PARITY_EDGE + cost_model.round_trip_cost_rate();
                            if !edge.is_finite() {
                                continue;
                            }

                            match best_candidate {
                                Some((_, _, current_edge, _, _, _, _)) if edge <= current_edge => {}
                                _ => {
                                    best_candidate = Some((
                                        market,
                                        side,
                                        edge,
                                        threshold,
                                        entry_price,
                                        spread,
                                        time_to_expiry,
                                    ));
                                }
                            }
                        }

                        let Some((market, side, edge, threshold, entry_price, spread, time_to_expiry)) = best_candidate else {
                            publish_heartbeat(&mut conn, "convergence_carry").await;
                            continue;
                        };

                        let passes_threshold = edge >= threshold;
                        let reason = if passes_threshold {
                            format!(
                                "Parity edge {:.2}c cleared threshold {:.2}c on {}",
                                edge * 100.0,
                                threshold * 100.0,
                                Self::side_label(side),
                            )
                        } else {
                            format!(
                                "Parity edge {:.2}c below threshold {:.2}c on {}",
                                edge * 100.0,
                                threshold * 100.0,
                                Self::side_label(side),
                            )
                        };

                        let scan_msg = build_scan_payload(
                            &market.market_id,
                            "CONVERGENCE-CARRY",
                            "CONVERGENCE_CARRY",
                            "PARITY_EDGE",
                            "PRICE",
                            [entry_price, spread],
                            market.best_bid.unwrap_or(0.0) + market.best_ask.unwrap_or(0.0),
                            "MARKET_TOP_SUM",
                            edge,
                            threshold,
                            passes_threshold,
                            reason,
                            now_ms,
                            serde_json::json!({
                                "market_slug": market.slug.clone(),
                                "question": market.question.clone(),
                                "side": Self::side_label(side),
                                "entry_price": entry_price,
                                "spread": spread,
                                "time_to_expiry_secs": time_to_expiry,
                                "open_positions": open_positions.len(),
                            }),
                        );
                        let _: () = conn.publish("arbitrage:scan", scan_msg.to_string()).await.unwrap_or_default();

                        if open_positions.len() >= MAX_OPEN_POSITIONS
                            || now_ms - last_entry_ms < ENTRY_COOLDOWN_MS
                            || !passes_threshold
                        {
                            publish_heartbeat(&mut conn, "convergence_carry").await;
                            continue;
                        }

                        let trading_mode = read_trading_mode(&mut conn).await;
                        if trading_mode == TradingMode::Live {
                            if now_ms - last_live_preview_ms >= LIVE_PREVIEW_COOLDOWN_MS {
                                let available_cash = read_sim_available_cash(&mut conn).await;
                                let risk_cfg = read_risk_config(&mut conn).await;
                                let size = compute_strategy_bet_size(
                                    &mut conn,
                                    "CONVERGENCE_CARRY",
                                    available_cash,
                                    &risk_cfg,
                                    10.0,
                                    0.12,
                                ).await;
                                if size > 0.0 {
                                    let token_id = if matches!(side, Side::Yes) {
                                        market.yes_token.clone()
                                    } else {
                                        market.no_token.clone()
                                    };
                                    let execution_id = Uuid::new_v4().to_string();
                                    let preview_msg = serde_json::json!({
                                        "execution_id": execution_id,
                                        "market": "Convergence Carry",
                                        "side": "LIVE_DRY_RUN",
                                        "price": entry_price,
                                        "size": size,
                                        "timestamp": now_ms,
                                        "mode": "LIVE_DRY_RUN",
                                        "details": {
                                            "strategy": "CONVERGENCE_CARRY",
                                            "condition_id": market.market_id.clone(),
                                            "question": market.question.clone(),
                                            "token_side": Self::side_label(side),
                                            "parity_edge": edge,
                                            "threshold": threshold,
                                            "preflight": {
                                                "venue": "POLYMARKET",
                                                "strategy": "CONVERGENCE_CARRY",
                                                "orders": [
                                                    {
                                                        "token_id": token_id,
                                                        "condition_id": market.market_id.clone(),
                                                        "side": "BUY",
                                                        "price": entry_price,
                                                        "size": size,
                                                        "size_unit": "USD_NOTIONAL",
                                                    }
                                                ]
                                            }
                                        }
                                    });
                                    let _: () = conn.publish("arbitrage:execution", preview_msg.to_string()).await.unwrap_or_default();
                                    last_live_preview_ms = now_ms;
                                    last_entry_ms = now_ms;
                                }
                            }
                            publish_heartbeat(&mut conn, "convergence_carry").await;
                            continue;
                        }

                        let available_cash = read_sim_available_cash(&mut conn).await;
                        let risk_cfg = read_risk_config(&mut conn).await;
                        let size = compute_strategy_bet_size(
                            &mut conn,
                            "CONVERGENCE_CARRY",
                            available_cash,
                            &risk_cfg,
                            10.0,
                            0.12,
                        ).await;
                        if size > 0.0 && reserve_sim_notional_for_strategy(&mut conn, "CONVERGENCE_CARRY", size).await {
                            let execution_id = Uuid::new_v4().to_string();
                            open_positions.push(Position {
                                execution_id: execution_id.clone(),
                                market_id: market.market_id.clone(),
                                question: market.question.clone(),
                                side,
                                entry_price,
                                size,
                                timestamp_ms: now_ms,
                            });
                            last_entry_ms = now_ms;
                            let exec_msg = serde_json::json!({
                                "execution_id": execution_id,
                                "market": "Convergence Carry",
                                "side": "ENTRY",
                                "price": entry_price,
                                "size": size,
                                "timestamp": now_ms,
                                "mode": "PAPER",
                                "details": {
                                    "condition_id": market.market_id.clone(),
                                    "question": market.question.clone(),
                                    "token_side": Self::side_label(side),
                                    "parity_edge": edge,
                                    "threshold": threshold,
                                    "spread": spread,
                                }
                            });
                            let _: () = conn.publish("arbitrage:execution", exec_msg.to_string()).await.unwrap_or_default();
                        }

                        publish_heartbeat(&mut conn, "convergence_carry").await;
                    }
                }
            }
        }
    }
}
