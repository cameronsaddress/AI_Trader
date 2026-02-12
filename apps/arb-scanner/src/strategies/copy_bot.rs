use async_trait::async_trait;
use chrono::Utc;
use ethers::prelude::*;
use log::{error, info, warn};
use redis::AsyncCommands;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{sleep, Duration};

use crate::engine::PolymarketClient;
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
use crate::strategies::simulation::{realized_pnl, SimCostModel};
use crate::strategies::Strategy;

const POLYGON_WS: &str = "wss://polygon-bor-rpc.publicnode.com";
const CTF_EXCHANGE: &str = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E";
const CLUSTER_WINDOW_SECS: i64 = 5 * 60;
const ENTRY_COOLDOWN_SECS: i64 = 20;
const MIN_HOLD_SECS: i64 = 10;
const ENTRY_WALLETS_THRESHOLD: usize = 3;
const ENTRY_VOLUME_THRESHOLD_USDC: f64 = 10_000.0;
const ENTRY_BUY_RATIO_THRESHOLD: f64 = 0.65;
const MAX_SINGLE_WALLET_SHARE: f64 = 0.55;
const ENTRY_UPTICK_RATIO_THRESHOLD: f64 = 0.55;
const MIN_PRICE_DRIFT_PCT: f64 = 0.01;
const TAKE_PROFIT_PCT: f64 = 0.12;
const STOP_LOSS_PCT: f64 = -0.08;
const LIVE_PREVIEW_COOLDOWN_SECS: i64 = 20;

#[derive(Debug, Clone)]
struct TrackedTrade {
    timestamp: i64,
    maker: Address,
    taker: Address,
    amount_usdc: f64,
    price: f64,
    side: u8, // 0 buy (USDC -> token), 1 sell (token -> USDC)
}

#[derive(Debug, Clone)]
struct Position {
    entry_price: f64,
    size: f64,
    timestamp: i64,
}

pub struct SyndicateStrategy {
    _client: PolymarketClient,
    trade_window: Arc<RwLock<HashMap<U256, Vec<TrackedTrade>>>>,
    open_positions: Arc<RwLock<HashMap<U256, Position>>>,
    last_entry_ts: Arc<RwLock<HashMap<U256, i64>>>,
}

impl SyndicateStrategy {
    pub fn new() -> Self {
        Self {
            _client: PolymarketClient::new(),
            trade_window: Arc::new(RwLock::new(HashMap::new())),
            open_positions: Arc::new(RwLock::new(HashMap::new())),
            last_entry_ts: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl Strategy for SyndicateStrategy {
    async fn run(&self, redis_client: redis::Client) {
        info!("Starting Syndicate strategy [on-chain cluster follower]");

        let mut conn = match redis_client.get_async_connection().await {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to connect to Redis: {}", e);
                return;
            }
        };
        let cost_model = SimCostModel::from_env();
        let event_variant = strategy_variant();

        let trade_window_writer = self.trade_window.clone();
        let positions_writer = self.open_positions.clone();
        let last_entry_writer = self.last_entry_ts.clone();
        let redis_client_clone = redis_client.clone();
        let event_cost_model = cost_model;

        tokio::spawn(async move {
            let mut event_conn = match redis_client_clone.get_async_connection().await {
                Ok(c) => c,
                Err(e) => {
                    error!("Syndicate event Redis connection failed: {}", e);
                    return;
                }
            };
            let mut event_last_reset_ts = 0_i64;

            abigen!(
                CTFExchange,
                r#"[
                    event OrderFilled(bytes32 indexed orderHash, address indexed maker, address indexed taker, uint256 makerAssetId, uint256 takerAssetId, uint256 makerAmount, uint256 takerAmount, uint256 feeAmount)
                ]"#
            );

            loop {
                info!("Connecting to Polygon WS for OrderFilled feed...");
                let provider = match Provider::<Ws>::connect(POLYGON_WS).await {
                    Ok(p) => p,
                    Err(e) => {
                        error!("Polygon WS connect error: {}", e);
                        sleep(Duration::from_secs(5)).await;
                        continue;
                    }
                };

                let exchange_address = match CTF_EXCHANGE.parse::<Address>() {
                    Ok(addr) => addr,
                    Err(e) => {
                        error!("Invalid CTF exchange address: {}", e);
                        sleep(Duration::from_secs(5)).await;
                        continue;
                    }
                };

                let filter = Filter::new()
                    .address(exchange_address)
                    .event("OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)");

                let mut stream = match provider.subscribe_logs(&filter).await {
                    Ok(s) => s,
                    Err(e) => {
                        error!("Polygon subscribe error: {}", e);
                        sleep(Duration::from_secs(5)).await;
                        continue;
                    }
                };

                while let Some(log) = stream.next().await {
                    let reset_ts = read_simulation_reset_ts(&mut event_conn).await;
                    if reset_ts > event_last_reset_ts {
                        event_last_reset_ts = reset_ts;
                        {
                            let mut positions = positions_writer.write().await;
                            positions.clear();
                        }
                        {
                            let mut window = trade_window_writer.write().await;
                            window.clear();
                        }
                        {
                            let mut last_entry = last_entry_writer.write().await;
                            last_entry.clear();
                        }
                    }

                    match ethers::contract::parse_log::<OrderFilledFilter>(log) {
                        Ok(event) => {
                            let maker_asset = event.maker_asset_id;
                            let taker_asset = event.taker_asset_id;
                            let maker_amount = event.maker_amount.as_u128() as f64;
                            let taker_amount = event.taker_amount.as_u128() as f64;

                            if maker_amount <= 0.0 || taker_amount <= 0.0 {
                                continue;
                            }

                            // Polymarket CTF collateral leg uses token id 0.
                            let (token_id, amount_usdc, price, side) = if maker_asset.is_zero() && !taker_asset.is_zero() {
                                // Buy token using USDC
                                (taker_asset, maker_amount / 1_000_000.0, maker_amount / taker_amount, 0)
                            } else if taker_asset.is_zero() && !maker_asset.is_zero() {
                                // Sell token for USDC
                                (maker_asset, taker_amount / 1_000_000.0, taker_amount / maker_amount, 1)
                            } else {
                                continue;
                            };

                            if token_id.is_zero() || price <= 0.01 || price >= 0.99 {
                                continue;
                            }

                            if event.maker == event.taker {
                                continue;
                            }

                            let trade = TrackedTrade {
                                timestamp: Utc::now().timestamp(),
                                maker: event.maker,
                                taker: event.taker,
                                amount_usdc,
                                price,
                                side,
                            };

                            {
                                let mut window = trade_window_writer.write().await;
                                window.entry(token_id).or_default().push(trade.clone());

                                let now = Utc::now().timestamp();
                                for list in window.values_mut() {
                                    list.retain(|t| t.timestamp > now - CLUSTER_WINDOW_SECS);
                                }
                            }

                            // Exit logic on incoming trade updates.
                            let trading_mode = read_trading_mode(&mut event_conn).await;
                            let mut cleared_live = false;
                            let mut close_candidate: Option<(Position, f64, &'static str)> = None;

                            {
                                let mut positions = positions_writer.write().await;
                                if trading_mode == TradingMode::Live {
                                    cleared_live = positions.remove(&token_id).is_some();
                                } else if let Some(pos) = positions.get(&token_id).cloned() {
                                    if Utc::now().timestamp() - pos.timestamp >= MIN_HOLD_SECS {
                                        let pnl_pct = (price - pos.entry_price) / pos.entry_price;
                                        if pnl_pct >= TAKE_PROFIT_PCT || pnl_pct <= STOP_LOSS_PCT {
                                            let reason = if pnl_pct >= TAKE_PROFIT_PCT { "TAKE_PROFIT" } else { "STOP_LOSS" };
                                            close_candidate = Some((pos, pnl_pct, reason));
                                            positions.remove(&token_id);
                                        }
                                    }
                                }
                            }

                            if trading_mode == TradingMode::Live {
                                if cleared_live {
                                    info!("Syndicate: clearing paper position for token {} in LIVE mode", token_id);
                                }
                                continue;
                            }

                            if let Some((pos, pnl_pct, reason)) = close_candidate {
                                let realized = realized_pnl(pos.size, pnl_pct, event_cost_model);
                                let new_bankroll = apply_sim_pnl(&mut event_conn, realized).await;

                                let pnl_msg = serde_json::json!({
                                    "strategy": "SYNDICATE",
                                    "variant": event_variant.as_str(),
                                    "pnl": realized,
                                    "notional": pos.size,
                                    "timestamp": Utc::now().timestamp_millis(),
                                    "bankroll": new_bankroll,
                                    "mode": "PAPER",
                                    "details": {
                                        "action": "CLOSE_POSITION",
                                        "reason": reason,
                                        "token_id": token_id.to_string(),
                                        "entry": pos.entry_price,
                                        "exit": price,
                                        "gross_return": pnl_pct,
                                        "net_return": if pos.size > 0.0 { realized / pos.size } else { 0.0 },
                                        "roi": format!("{:.2}%", if pos.size > 0.0 { (realized / pos.size) * 100.0 } else { 0.0 }),
                                        "round_trip_cost_rate": event_cost_model.round_trip_cost_rate(),
                                    }
                                });
                                let _: () = event_conn.publish("strategy:pnl", pnl_msg.to_string()).await.unwrap_or_default();
                            }
                        }
                        Err(e) => {
                            warn!("OrderFilled parse error: {:?}", e);
                        }
                    }
                }
            }
        });

        let mut last_seen_reset_ts = 0_i64;
        loop {
            sleep(Duration::from_secs(5)).await;

            if !is_strategy_enabled(&mut conn, "SYNDICATE").await {
                publish_heartbeat(&mut conn, "copy_bot").await;
                continue;
            }

            let trading_mode = read_trading_mode(&mut conn).await;
            if trading_mode == TradingMode::Live {
                let mut positions = self.open_positions.write().await;
                if !positions.is_empty() {
                    info!("Syndicate: clearing {} paper position(s) in LIVE mode", positions.len());
                    positions.clear();
                }
            }

            let reset_ts = read_simulation_reset_ts(&mut conn).await;
            if reset_ts > last_seen_reset_ts {
                last_seen_reset_ts = reset_ts;
                {
                    let mut positions = self.open_positions.write().await;
                    positions.clear();
                }
                {
                    let mut window = self.trade_window.write().await;
                    window.clear();
                }
                {
                    let mut last_entry = self.last_entry_ts.write().await;
                    last_entry.clear();
                }
                publish_heartbeat(&mut conn, "copy_bot").await;
                continue;
            }

            let now = Utc::now().timestamp();
            let window_snapshot: Vec<(U256, Vec<TrackedTrade>)> = {
                let window = self.trade_window.read().await;
                window.iter().map(|(token_id, trades)| (*token_id, trades.clone())).collect()
            };
            let adaptive_volume_threshold = {
                let mut vols: Vec<f64> = window_snapshot
                    .iter()
                    .map(|(_, trades)| trades.iter().map(|t| t.amount_usdc).sum::<f64>())
                    .filter(|v| v.is_finite() && *v > 0.0)
                    .collect();
                if vols.is_empty() {
                    ENTRY_VOLUME_THRESHOLD_USDC
                } else {
                    vols.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                    let median = vols[vols.len() / 2];
                    (ENTRY_VOLUME_THRESHOLD_USDC.max(median * 0.70)).min(100_000.0)
                }
            };

            for (token_id, trades) in window_snapshot.iter() {
                if trades.len() < 3 {
                    continue;
                }

                let mut buyer_wallets = HashSet::new();
                let mut wallet_volume: HashMap<Address, f64> = HashMap::new();
                let mut buy_volume_usdc = 0.0_f64;
                let mut sell_volume_usdc = 0.0_f64;
                let mut upticks: usize = 0;
                let mut comparisons: usize = 0;
                let mut first_price = 0.0_f64;
                let mut last_price = 0.0_f64;

                for (idx, t) in trades.iter().enumerate() {
                    let buyer = if t.side == 0 { t.maker } else { t.taker };
                    buyer_wallets.insert(buyer);
                    *wallet_volume.entry(buyer).or_insert(0.0) += t.amount_usdc;

                    if t.side == 0 {
                        buy_volume_usdc += t.amount_usdc;
                    } else {
                        sell_volume_usdc += t.amount_usdc;
                    }

                    if idx == 0 {
                        first_price = t.price;
                    }
                    last_price = t.price;

                    if idx > 0 {
                        let prev = trades[idx - 1].price;
                        if t.price >= prev {
                            upticks += 1;
                        }
                        comparisons += 1;
                    }
                }

                let unique_wallets = buyer_wallets.len();
                let total_vol = buy_volume_usdc + sell_volume_usdc;
                let buy_ratio = if total_vol > 0.0 {
                    buy_volume_usdc / total_vol
                } else {
                    0.5
                };
                let uptick_ratio = if comparisons > 0 {
                    upticks as f64 / comparisons as f64
                } else {
                    0.5
                };
                let price_drift = if first_price > 0.0 {
                    (last_price - first_price) / first_price
                } else {
                    0.0
                };

                let max_wallet_share = if buy_volume_usdc > 0.0 {
                    wallet_volume
                        .values()
                        .fold(0.0_f64, |acc, v| acc.max(*v / buy_volume_usdc))
                } else {
                    1.0
                };

                if unique_wallets < ENTRY_WALLETS_THRESHOLD
                    || total_vol < adaptive_volume_threshold
                    || buy_ratio < ENTRY_BUY_RATIO_THRESHOLD
                    || uptick_ratio < ENTRY_UPTICK_RATIO_THRESHOLD
                    || price_drift < MIN_PRICE_DRIFT_PCT
                    || max_wallet_share > MAX_SINGLE_WALLET_SHARE
                {
                    continue;
                }

                if trading_mode == TradingMode::Paper {
                    let positions = self.open_positions.read().await;
                    if positions.contains_key(token_id) {
                        continue;
                    }
                }

                {
                    let last_entry_ts = self.last_entry_ts.read().await;
                    if let Some(last) = last_entry_ts.get(token_id) {
                        let cooldown = if trading_mode == TradingMode::Live {
                            LIVE_PREVIEW_COOLDOWN_SECS
                        } else {
                            ENTRY_COOLDOWN_SECS
                        };
                        if now - *last < cooldown {
                            continue;
                        }
                    }
                }

                let mut recent_buys: Vec<f64> = trades
                    .iter()
                    .rev()
                    .filter(|t| t.side == 0)
                    .take(5)
                    .map(|t| t.price)
                    .collect();

                if recent_buys.is_empty() {
                    continue;
                }

                recent_buys.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let avg_price: f64 = recent_buys.iter().sum::<f64>() / recent_buys.len() as f64;

                let bankroll = read_sim_bankroll(&mut conn).await;
                let risk_cfg = read_risk_config(&mut conn).await;
                let bet_size = compute_bet_size(bankroll, &risk_cfg, 10.0, 0.20);
                if bet_size <= 0.0 {
                    continue;
                }

                {
                    let mut last_entry_ts = self.last_entry_ts.write().await;
                    last_entry_ts.insert(*token_id, now);
                }

                let ts_ms = Utc::now().timestamp_millis();
                let scan_score = buy_ratio - 0.5;
                let scan_threshold = ENTRY_BUY_RATIO_THRESHOLD - 0.5;
                let scan_msg = build_scan_payload(
                    &token_id.to_string(),
                    "SYNDICATE-ALERT",
                    "SYNDICATE",
                    "BUY_PRESSURE",
                    "RATIO",
                    [total_vol, unique_wallets as f64],
                    avg_price,
                    "AVERAGE_BUY_ENTRY",
                    scan_score,
                    scan_threshold,
                    true,
                    format!(
                        "Cluster passed filters: buyers={} (>= {}), volume=${:.2} (>= ${:.2}), buy_pressure={:.2}% (>= {:.2}%), uptick_ratio={:.2}% (>= {:.2}%), price_drift={:.2}% (>= {:.2}%), max_wallet_share={:.2}% (<= {:.2}%)",
                        unique_wallets,
                        ENTRY_WALLETS_THRESHOLD,
                        total_vol,
                        adaptive_volume_threshold,
                        buy_ratio * 100.0,
                        ENTRY_BUY_RATIO_THRESHOLD * 100.0,
                        uptick_ratio * 100.0,
                        ENTRY_UPTICK_RATIO_THRESHOLD * 100.0,
                        price_drift * 100.0,
                        MIN_PRICE_DRIFT_PCT * 100.0,
                        max_wallet_share * 100.0,
                        MAX_SINGLE_WALLET_SHARE * 100.0
                    ),
                    ts_ms,
                    serde_json::json!({
                        "unique_buyers": unique_wallets,
                        "volume_usdc": total_vol,
                        "adaptive_volume_threshold": adaptive_volume_threshold,
                        "buy_pressure": buy_ratio,
                        "uptick_ratio": uptick_ratio,
                        "price_drift": price_drift,
                        "max_wallet_share": max_wallet_share,
                    }),
                );
                let _: () = conn.publish("arbitrage:scan", scan_msg.to_string()).await.unwrap_or_default();

                if trading_mode == TradingMode::Live {
                    let preview_msg = serde_json::json!({
                        "market": "Syndicate",
                        "side": "LIVE_DRY_RUN",
                        "price": avg_price,
                        "size": bet_size,
                        "timestamp": ts_ms,
                        "mode": "LIVE_DRY_RUN",
                        "details": {
                            "token_id": token_id.to_string(),
                            "wallets": unique_wallets,
                            "volume_usdc": total_vol,
                            "buy_pressure": buy_ratio,
                            "uptick_ratio": uptick_ratio,
                            "price_drift": price_drift,
                            "max_wallet_share": max_wallet_share,
                            "preflight": {
                                "venue": "POLYMARKET",
                                "strategy": "SYNDICATE",
                                "orders": [
                                    {
                                        "token_id": token_id.to_string(),
                                        "side": "BUY",
                                        "price": avg_price,
                                        "size": bet_size,
                                        "size_unit": "USD_NOTIONAL"
                                    }
                                ]
                            }
                        }
                    });
                    let _: () = conn.publish("arbitrage:execution", preview_msg.to_string()).await.unwrap_or_default();
                    continue;
                }

                {
                    let mut positions = self.open_positions.write().await;
                    positions.insert(*token_id, Position {
                        entry_price: avg_price,
                        size: bet_size,
                        timestamp: now,
                    });
                }

                let exec_msg = serde_json::json!({
                    "market": "Syndicate",
                    "side": "ENTRY",
                    "price": avg_price,
                    "size": bet_size,
                    "timestamp": ts_ms,
                    "mode": "PAPER",
                    "details": {
                        "token_id": token_id.to_string(),
                        "wallets": unique_wallets,
                        "volume_usdc": total_vol,
                        "buy_pressure": buy_ratio,
                        "uptick_ratio": uptick_ratio,
                        "price_drift": price_drift,
                        "max_wallet_share": max_wallet_share,
                    }
                });
                let _: () = conn.publish("arbitrage:execution", exec_msg.to_string()).await.unwrap_or_default();

                info!(
                    "SYNDICATE ENTRY token={} wallets={} vol={:.2} price={:.3}",
                    token_id,
                    unique_wallets,
                    total_vol,
                    avg_price
                );
            }

            publish_heartbeat(&mut conn, "copy_bot").await;
        }
    }
}
