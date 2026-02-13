use chrono::Utc;
use redis::AsyncCommands;
use serde::Deserialize;

pub const DEFAULT_SIM_BANKROLL: f64 = 1000.0;
const SIM_BANKROLL_KEY: &str = "sim_bankroll";
const SIM_LEDGER_CASH_KEY: &str = "sim_ledger:cash";
const SIM_LEDGER_RESERVED_KEY: &str = "sim_ledger:reserved";
const SIM_LEDGER_REALIZED_PNL_KEY: &str = "sim_ledger:realized_pnl";
const SIM_LEDGER_RESERVED_BY_STRATEGY_PREFIX: &str = "sim_ledger:reserved:strategy:";
const SIM_LEDGER_RESERVED_BY_FAMILY_PREFIX: &str = "sim_ledger:reserved:family:";
const STRATEGY_RISK_MULTIPLIER_PREFIX: &str = "strategy:risk_multiplier:";
const DEFAULT_STRATEGY_CONCENTRATION_CAP_PCT: f64 = 0.35;
const DEFAULT_FAMILY_CONCENTRATION_CAP_PCT: f64 = 0.60;
const DEFAULT_GLOBAL_UTILIZATION_CAP_PCT: f64 = 0.90;
const DEFAULT_NUMERIC_EPSILON: f64 = 1e-9;

#[derive(Debug, Clone, Deserialize)]
pub struct RiskConfig {
    pub model: String,
    pub value: f64,
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            model: "FIXED".to_string(),
            value: 50.0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradingMode {
    Paper,
    Live,
}

pub async fn read_risk_config(conn: &mut redis::aio::Connection) -> RiskConfig {
    let raw: String = conn.get("system:risk_config").await.unwrap_or_else(|_| "{}".to_string());
    serde_json::from_str::<RiskConfig>(&raw).unwrap_or_default()
}

async fn ensure_sim_ledger(conn: &mut redis::aio::Connection) {
    let fallback_equity = conn
        .get::<_, f64>(SIM_BANKROLL_KEY)
        .await
        .unwrap_or(DEFAULT_SIM_BANKROLL);
    let _: bool = conn.set_nx(SIM_BANKROLL_KEY, fallback_equity).await.unwrap_or(false);
    let _: bool = conn.set_nx(SIM_LEDGER_CASH_KEY, fallback_equity).await.unwrap_or(false);
    let _: bool = conn.set_nx(SIM_LEDGER_RESERVED_KEY, 0.0_f64).await.unwrap_or(false);
    let _: bool = conn.set_nx(SIM_LEDGER_REALIZED_PNL_KEY, 0.0_f64).await.unwrap_or(false);
}

fn strategy_reserved_key(strategy_id: &str) -> String {
    format!("{}{}", SIM_LEDGER_RESERVED_BY_STRATEGY_PREFIX, strategy_id.trim().to_uppercase())
}

fn family_reserved_key(family_id: &str) -> String {
    format!("{}{}", SIM_LEDGER_RESERVED_BY_FAMILY_PREFIX, family_id.trim().to_uppercase())
}

fn read_env_cap_pct(name: &str, fallback: f64, min: f64, max: f64) -> f64 {
    let parsed = std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<f64>().ok())
        .filter(|value| value.is_finite())
        .unwrap_or(fallback);
    parsed.clamp(min, max)
}

fn strategy_concentration_cap_pct() -> f64 {
    read_env_cap_pct(
        "SIM_STRATEGY_CONCENTRATION_CAP_PCT",
        DEFAULT_STRATEGY_CONCENTRATION_CAP_PCT,
        0.05,
        1.0,
    )
}

fn family_concentration_cap_pct() -> f64 {
    read_env_cap_pct(
        "SIM_FAMILY_CONCENTRATION_CAP_PCT",
        DEFAULT_FAMILY_CONCENTRATION_CAP_PCT,
        0.10,
        1.0,
    )
}

fn global_utilization_cap_pct() -> f64 {
    read_env_cap_pct(
        "SIM_GLOBAL_UTILIZATION_CAP_PCT",
        DEFAULT_GLOBAL_UTILIZATION_CAP_PCT,
        0.10,
        1.0,
    )
}

pub fn strategy_family(strategy_id: &str) -> &'static str {
    match strategy_id.trim().to_uppercase().as_str() {
        "BTC_5M" | "BTC_15M" | "ETH_15M" | "SOL_15M" => "FAIR_VALUE",
        "ATOMIC_ARB" | "GRAPH_ARB" => "ARBITRAGE",
        "CEX_SNIPER" => "CEX_MICROSTRUCTURE",
        "OBI_SCALPER" => "ORDER_FLOW",
        "SYNDICATE" => "FLOW_PRESSURE",
        "CONVERGENCE_CARRY" => "CARRY_PARITY",
        "MAKER_MM" => "MARKET_MAKING",
        _ => "GENERIC",
    }
}

pub async fn read_strategy_reserved_notional(conn: &mut redis::aio::Connection, strategy_id: &str) -> f64 {
    ensure_sim_ledger(conn).await;
    let key = strategy_reserved_key(strategy_id);
    conn.get::<_, f64>(key).await.unwrap_or(0.0)
}

pub async fn read_family_reserved_notional(conn: &mut redis::aio::Connection, family_id: &str) -> f64 {
    ensure_sim_ledger(conn).await;
    let key = family_reserved_key(family_id);
    conn.get::<_, f64>(key).await.unwrap_or(0.0)
}

pub async fn read_sim_available_cash(conn: &mut redis::aio::Connection) -> f64 {
    ensure_sim_ledger(conn).await;
    conn.get::<_, f64>(SIM_LEDGER_CASH_KEY)
        .await
        .unwrap_or(DEFAULT_SIM_BANKROLL)
}

pub async fn read_sim_reserved_notional(conn: &mut redis::aio::Connection) -> f64 {
    ensure_sim_ledger(conn).await;
    conn.get::<_, f64>(SIM_LEDGER_RESERVED_KEY)
        .await
        .unwrap_or(0.0)
}

pub async fn read_sim_bankroll(conn: &mut redis::aio::Connection) -> f64 {
    ensure_sim_ledger(conn).await;
    let cash = read_sim_available_cash(conn).await;
    let reserved = read_sim_reserved_notional(conn).await;
    let equity = cash + reserved;
    let _: () = conn.set(SIM_BANKROLL_KEY, equity).await.unwrap_or_default();
    equity
}

pub async fn reserve_sim_notional_for_strategy(
    conn: &mut redis::aio::Connection,
    strategy_id: &str,
    notional: f64,
) -> bool {
    if !notional.is_finite() || notional <= 0.0 {
        return false;
    }

    ensure_sim_ledger(conn).await;
    let family_id = strategy_family(strategy_id);
    let strategy_key = strategy_reserved_key(strategy_id);
    let family_key = family_reserved_key(family_id);
    let strategy_cap = strategy_concentration_cap_pct();
    let family_cap = family_concentration_cap_pct();
    let utilization_cap = global_utilization_cap_pct();

    let reserve_script = r#"
        local cash = tonumber(redis.call("GET", KEYS[1]) or "0")
        local reserved = tonumber(redis.call("GET", KEYS[2]) or "0")
        local amount = tonumber(ARGV[1]) or 0
        local strategy_reserved = tonumber(redis.call("GET", KEYS[4]) or "0")
        local family_reserved = tonumber(redis.call("GET", KEYS[5]) or "0")
        local strategy_cap = tonumber(ARGV[2]) or 1
        local family_cap = tonumber(ARGV[3]) or 1
        local utilization_cap = tonumber(ARGV[4]) or 1
        local epsilon = tonumber(ARGV[5]) or 1e-9
        local equity = cash + reserved
        if amount <= 0 then
            redis.call("SET", KEYS[3], equity)
            return {0, cash, reserved, equity, 0}
        end
        if equity <= 0 then
            redis.call("SET", KEYS[3], equity)
            return {0, cash, reserved, equity, 5}
        end
        if cash + epsilon < amount then
            redis.call("SET", KEYS[3], equity)
            return {0, cash, reserved, equity, 1}
        end
        if strategy_cap > 0 and (strategy_reserved + amount) > ((equity * strategy_cap) + epsilon) then
            redis.call("SET", KEYS[3], equity)
            return {0, cash, reserved, equity, 2}
        end
        if family_cap > 0 and (family_reserved + amount) > ((equity * family_cap) + epsilon) then
            redis.call("SET", KEYS[3], equity)
            return {0, cash, reserved, equity, 3}
        end
        if utilization_cap > 0 and (reserved + amount) > ((equity * utilization_cap) + epsilon) then
            redis.call("SET", KEYS[3], equity)
            return {0, cash, reserved, equity, 4}
        end
        cash = cash - amount
        reserved = reserved + amount
        strategy_reserved = strategy_reserved + amount
        family_reserved = family_reserved + amount
        equity = cash + reserved
        redis.call("SET", KEYS[1], cash)
        redis.call("SET", KEYS[2], reserved)
        redis.call("SET", KEYS[3], equity)
        redis.call("SET", KEYS[4], strategy_reserved)
        redis.call("SET", KEYS[5], family_reserved)
        return {1, cash, reserved, equity, 0}
    "#;

    let eval_result = redis::cmd("EVAL")
        .arg(reserve_script)
        .arg(5)
        .arg(SIM_LEDGER_CASH_KEY)
        .arg(SIM_LEDGER_RESERVED_KEY)
        .arg(SIM_BANKROLL_KEY)
        .arg(&strategy_key)
        .arg(&family_key)
        .arg(notional)
        .arg(strategy_cap)
        .arg(family_cap)
        .arg(utilization_cap)
        .arg(DEFAULT_NUMERIC_EPSILON)
        .query_async::<_, (i32, f64, f64, f64, i32)>(conn)
        .await;

    match eval_result {
        Ok((ok, _cash, _reserved, _equity, _reason_code)) => ok == 1,
        Err(_) => {
            let cash = read_sim_available_cash(conn).await;
            if cash + 1e-9 < notional {
                return false;
            }
            let equity = read_sim_bankroll(conn).await;
            if equity <= 0.0 {
                return false;
            }
            let current_reserved = read_sim_reserved_notional(conn).await;
            let strategy_reserved = read_strategy_reserved_notional(conn, strategy_id).await;
            let family_reserved = read_family_reserved_notional(conn, family_id).await;
            if strategy_reserved + notional > (equity * strategy_cap + DEFAULT_NUMERIC_EPSILON) {
                return false;
            }
            if family_reserved + notional > (equity * family_cap + DEFAULT_NUMERIC_EPSILON) {
                return false;
            }
            if current_reserved + notional > (equity * utilization_cap + DEFAULT_NUMERIC_EPSILON) {
                return false;
            }
            let _: f64 = redis::cmd("INCRBYFLOAT")
                .arg(SIM_LEDGER_CASH_KEY)
                .arg(-notional)
                .query_async(conn)
                .await
                .unwrap_or(cash - notional);
            let reserved_before = read_sim_reserved_notional(conn).await;
            let _: f64 = redis::cmd("INCRBYFLOAT")
                .arg(SIM_LEDGER_RESERVED_KEY)
                .arg(notional)
                .query_async(conn)
                .await
                .unwrap_or(reserved_before + notional);
            let _: f64 = redis::cmd("INCRBYFLOAT")
                .arg(&strategy_key)
                .arg(notional)
                .query_async(conn)
                .await
                .unwrap_or(strategy_reserved + notional);
            let _: f64 = redis::cmd("INCRBYFLOAT")
                .arg(&family_key)
                .arg(notional)
                .query_async(conn)
                .await
                .unwrap_or(family_reserved + notional);
            let equity = read_sim_bankroll(conn).await;
            let _: () = conn.set(SIM_BANKROLL_KEY, equity).await.unwrap_or_default();
            true
        }
    }
}

pub async fn settle_sim_position_for_strategy(
    conn: &mut redis::aio::Connection,
    strategy_id: &str,
    reserved_notional: f64,
    pnl: f64,
) -> f64 {
    ensure_sim_ledger(conn).await;
    let family_id = strategy_family(strategy_id);
    let strategy_key = strategy_reserved_key(strategy_id);
    let family_key = family_reserved_key(family_id);
    let release_notional = if reserved_notional.is_finite() {
        reserved_notional.max(0.0)
    } else {
        0.0
    };
    let pnl_value = if pnl.is_finite() { pnl } else { 0.0 };

    let settle_script = r#"
        local cash = tonumber(redis.call("GET", KEYS[1]) or "0")
        local reserved = tonumber(redis.call("GET", KEYS[2]) or "0")
        local strategy_reserved = tonumber(redis.call("GET", KEYS[5]) or "0")
        local family_reserved = tonumber(redis.call("GET", KEYS[6]) or "0")
        local release_notional = tonumber(ARGV[1]) or 0
        local pnl = tonumber(ARGV[2]) or 0
        if release_notional < 0 then
            release_notional = 0
        end
        local release = math.min(release_notional, reserved)
        reserved = reserved - release
        strategy_reserved = math.max(0, strategy_reserved - release)
        family_reserved = math.max(0, family_reserved - release)
        cash = cash + release + pnl
        local equity = cash + reserved
        redis.call("SET", KEYS[1], cash)
        redis.call("SET", KEYS[2], reserved)
        redis.call("SET", KEYS[3], equity)
        redis.call("INCRBYFLOAT", KEYS[4], pnl)
        redis.call("SET", KEYS[5], strategy_reserved)
        redis.call("SET", KEYS[6], family_reserved)
        return {cash, reserved, equity}
    "#;

    let eval_result = redis::cmd("EVAL")
        .arg(settle_script)
        .arg(6)
        .arg(SIM_LEDGER_CASH_KEY)
        .arg(SIM_LEDGER_RESERVED_KEY)
        .arg(SIM_BANKROLL_KEY)
        .arg(SIM_LEDGER_REALIZED_PNL_KEY)
        .arg(&strategy_key)
        .arg(&family_key)
        .arg(release_notional)
        .arg(pnl_value)
        .query_async::<_, (f64, f64, f64)>(conn)
        .await;

    match eval_result {
        Ok((_cash, _reserved, equity)) => equity,
        Err(_) => {
            let reserved_before = read_sim_reserved_notional(conn).await;
            let release = release_notional.min(reserved_before).max(0.0);
            let _: f64 = redis::cmd("INCRBYFLOAT")
                .arg(SIM_LEDGER_RESERVED_KEY)
                .arg(-release)
                .query_async(conn)
                .await
                .unwrap_or((reserved_before - release).max(0.0));
            let strategy_reserved_before = read_strategy_reserved_notional(conn, strategy_id).await;
            let strategy_release = release.min(strategy_reserved_before);
            let _: f64 = redis::cmd("INCRBYFLOAT")
                .arg(&strategy_key)
                .arg(-strategy_release)
                .query_async(conn)
                .await
                .unwrap_or((strategy_reserved_before - strategy_release).max(0.0));
            let family_reserved_before = read_family_reserved_notional(conn, family_id).await;
            let family_release = release.min(family_reserved_before);
            let _: f64 = redis::cmd("INCRBYFLOAT")
                .arg(&family_key)
                .arg(-family_release)
                .query_async(conn)
                .await
                .unwrap_or((family_reserved_before - family_release).max(0.0));
            let cash_before = read_sim_available_cash(conn).await;
            let _: f64 = redis::cmd("INCRBYFLOAT")
                .arg(SIM_LEDGER_CASH_KEY)
                .arg(release + pnl_value)
                .query_async(conn)
                .await
                .unwrap_or(cash_before + release + pnl_value);
            let _: f64 = redis::cmd("INCRBYFLOAT")
                .arg(SIM_LEDGER_REALIZED_PNL_KEY)
                .arg(pnl_value)
                .query_async(conn)
                .await
                .unwrap_or(pnl_value);
            read_sim_bankroll(conn).await
        }
    }
}

pub async fn release_sim_notional_for_strategy(
    conn: &mut redis::aio::Connection,
    strategy_id: &str,
    reserved_notional: f64,
) -> f64 {
    settle_sim_position_for_strategy(conn, strategy_id, reserved_notional, 0.0).await
}

pub async fn read_simulation_reset_ts(conn: &mut redis::aio::Connection) -> i64 {
    conn.get::<_, i64>("system:simulation_reset_ts")
        .await
        .unwrap_or(0)
}

pub async fn read_trading_mode(conn: &mut redis::aio::Connection) -> TradingMode {
    let raw: String = conn.get("system:trading_mode").await.unwrap_or_else(|_| "PAPER".to_string());
    if raw.trim().eq_ignore_ascii_case("LIVE") {
        TradingMode::Live
    } else {
        TradingMode::Paper
    }
}

pub fn strategy_variant() -> String {
    let raw = std::env::var("STRATEGY_VARIANT").unwrap_or_else(|_| "baseline".to_string());
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        "baseline".to_string()
    } else {
        trimmed.to_string()
    }
}

pub fn compute_bet_size(bankroll: f64, cfg: &RiskConfig, min_size: f64, max_fraction: f64) -> f64 {
    if bankroll <= 0.0 || max_fraction <= 0.0 {
        return 0.0;
    }

    let mut size = if cfg.model == "PERCENT" {
        bankroll * (cfg.value / 100.0)
    } else {
        cfg.value
    };

    if !size.is_finite() {
        size = min_size;
    }

    let max_size = bankroll * max_fraction;
    if max_size <= 0.0 || !max_size.is_finite() {
        return 0.0;
    }

    if size > max_size {
        size = max_size;
    }

    if size < min_size {
        return 0.0;
    }

    size
}

pub async fn read_strategy_risk_multiplier(conn: &mut redis::aio::Connection, strategy_id: &str) -> f64 {
    if strategy_id.trim().is_empty() {
        return 1.0;
    }

    let key = format!("{}{}", STRATEGY_RISK_MULTIPLIER_PREFIX, strategy_id.trim());
    let parsed = conn.get::<_, f64>(&key).await.ok();
    if let Some(value) = parsed {
        if value.is_finite() {
            return value.clamp(0.0, 2.0);
        }
    }

    let _: bool = conn.set_nx(&key, 1.0_f64).await.unwrap_or(false);
    1.0
}

pub async fn compute_strategy_bet_size(
    conn: &mut redis::aio::Connection,
    strategy_id: &str,
    bankroll: f64,
    cfg: &RiskConfig,
    min_size: f64,
    max_fraction: f64,
) -> f64 {
    let base = compute_bet_size(bankroll, cfg, min_size, max_fraction);
    if base <= 0.0 || !base.is_finite() {
        return 0.0;
    }

    let multiplier = read_strategy_risk_multiplier(conn, strategy_id).await;
    if multiplier <= 0.0 {
        return 0.0;
    }
    let mut sized = base * multiplier;
    if !sized.is_finite() {
        return 0.0;
    }

    let hard_max = bankroll * max_fraction;
    if hard_max <= 0.0 || !hard_max.is_finite() {
        return 0.0;
    }
    if sized > hard_max {
        sized = hard_max;
    }

    let equity = read_sim_bankroll(conn).await;
    if equity <= 0.0 || !equity.is_finite() {
        return 0.0;
    }
    let family_id = strategy_family(strategy_id);
    let reserved_total = read_sim_reserved_notional(conn).await;
    let reserved_strategy = read_strategy_reserved_notional(conn, strategy_id).await;
    let reserved_family = read_family_reserved_notional(conn, family_id).await;
    let remaining_strategy = (equity * strategy_concentration_cap_pct()) - reserved_strategy;
    let remaining_family = (equity * family_concentration_cap_pct()) - reserved_family;
    let remaining_utilization = (equity * global_utilization_cap_pct()) - reserved_total;
    let cap_remaining = remaining_strategy.min(remaining_family).min(remaining_utilization);
    if !cap_remaining.is_finite() || cap_remaining <= DEFAULT_NUMERIC_EPSILON {
        return 0.0;
    }
    if sized > cap_remaining {
        sized = cap_remaining;
    }
    if sized < min_size {
        return 0.0;
    }

    sized
}

#[cfg(test)]
mod tests {
    use super::strategy_family;

    #[test]
    fn strategy_family_maps_known_ids() {
        assert_eq!(strategy_family("BTC_5M"), "FAIR_VALUE");
        assert_eq!(strategy_family("BTC_15M"), "FAIR_VALUE");
        assert_eq!(strategy_family("ATOMIC_ARB"), "ARBITRAGE");
        assert_eq!(strategy_family("OBI_SCALPER"), "ORDER_FLOW");
        assert_eq!(strategy_family("MAKER_MM"), "MARKET_MAKING");
        assert_eq!(strategy_family("unknown"), "GENERIC");
    }
}

pub async fn is_strategy_enabled(conn: &mut redis::aio::Connection, strategy_id: &str) -> bool {
    let key = format!("strategy:enabled:{}", strategy_id);
    let raw: String = conn.get(key).await.unwrap_or_else(|_| "1".to_string());
    raw != "0"
}

pub async fn publish_heartbeat(conn: &mut redis::aio::Connection, id: &str) {
    let heartbeat = serde_json::json!({
        "id": id,
        "timestamp": Utc::now().timestamp_millis(),
    });

    let _: () = conn
        .publish("system:heartbeat", heartbeat.to_string())
        .await
        .unwrap_or_default();
}

#[allow(clippy::too_many_arguments)]
pub fn build_scan_payload(
    market_id: &str,
    symbol: &str,
    strategy: &str,
    signal_type: &str,
    unit: &str,
    prices: [f64; 2],
    metric_value: f64,
    metric_label: &str,
    score: f64,
    threshold: f64,
    passes_threshold: bool,
    reason: String,
    timestamp: i64,
    meta: serde_json::Value,
) -> serde_json::Value {
    serde_json::json!({
        // Backward-compatible fields consumed by existing UI.
        "market_id": market_id,
        "symbol": symbol,
        "prices": prices,
        "sum": metric_value,
        "gap": score,
        "timestamp": timestamp,
        // Normalized schema fields.
        "strategy": strategy,
        "signal_type": signal_type,
        "unit": unit,
        "threshold": threshold,
        "score": score,
        "passes_threshold": passes_threshold,
        "reason": reason,
        "metric_label": metric_label,
        "meta": meta,
    })
}
