use chrono::Utc;
use hmac::{Hmac, Mac};
use log::warn;
use redis::AsyncCommands;
use serde::Deserialize;
use sha2::Sha256;

pub const DEFAULT_SIM_BANKROLL: f64 = 1000.0;
const SIM_BANKROLL_KEY: &str = "sim_bankroll";
const SIM_LEDGER_CASH_KEY: &str = "sim_ledger:cash";
const SIM_LEDGER_RESERVED_KEY: &str = "sim_ledger:reserved";
const SIM_LEDGER_REALIZED_PNL_KEY: &str = "sim_ledger:realized_pnl";
const SIM_LEDGER_RESERVED_BY_STRATEGY_PREFIX: &str = "sim_ledger:reserved:strategy:";
const SIM_LEDGER_RESERVED_BY_FAMILY_PREFIX: &str = "sim_ledger:reserved:family:";
const STRATEGY_RISK_MULTIPLIER_PREFIX: &str = "strategy:risk_multiplier:";
const STRATEGY_RISK_OVERLAY_PREFIX: &str = "strategy:risk_overlay:cross_horizon:";
const STRATEGY_LAST_MODE_PREFIX: &str = "strategy:last_mode:";
const DEFAULT_STRATEGY_CONCENTRATION_CAP_PCT: f64 = 0.35;
const DEFAULT_FAMILY_CONCENTRATION_CAP_PCT: f64 = 0.60;
const DEFAULT_UNDERLYING_CONCENTRATION_CAP_PCT: f64 = 0.70;
const DEFAULT_GLOBAL_UTILIZATION_CAP_PCT: f64 = 0.90;
const DEFAULT_GLOBAL_MAX_DRAWDOWN_PCT: f64 = -5.0;
const DEFAULT_NUMERIC_EPSILON: f64 = 1e-9;
const SIM_LEDGER_RESERVED_BY_UNDERLYING_PREFIX: &str = "sim_ledger:reserved:underlying:";

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PortfolioRegime {
    Trend,
    MeanRevert,
    LowLiquidity,
    Chop,
    Unknown,
}

fn regime_sizing_enabled() -> bool {
    match std::env::var("META_REGIME_SIZING_ENABLED") {
        Ok(raw) => {
            let normalized = raw.trim().to_ascii_lowercase();
            !(normalized == "0" || normalized == "false" || normalized == "no")
        }
        Err(_) => true,
    }
}

fn global_max_drawdown_pct() -> f64 {
    std::env::var("SIM_GLOBAL_MAX_DRAWDOWN_PCT")
        .ok()
        .and_then(|v| v.trim().parse::<f64>().ok())
        .filter(|v| v.is_finite())
        .unwrap_or(DEFAULT_GLOBAL_MAX_DRAWDOWN_PCT)
        .clamp(-95.0, -0.1)
}

fn regime_min_confidence() -> f64 {
    std::env::var("META_REGIME_MIN_CONFIDENCE")
        .ok()
        .and_then(|v| v.trim().parse::<f64>().ok())
        .filter(|v| v.is_finite())
        .unwrap_or(0.30)
        .clamp(0.0, 1.0)
}

fn parse_portfolio_regime(raw: &str) -> PortfolioRegime {
    match raw.trim().to_ascii_uppercase().as_str() {
        "TREND" => PortfolioRegime::Trend,
        "MEAN_REVERT" => PortfolioRegime::MeanRevert,
        "LOW_LIQUIDITY" => PortfolioRegime::LowLiquidity,
        "CHOP" => PortfolioRegime::Chop,
        _ => PortfolioRegime::Unknown,
    }
}

fn regime_family_size_multiplier(regime: PortfolioRegime, family: &str, confidence: f64) -> f64 {
    if confidence < regime_min_confidence() {
        return 1.0;
    }

    match regime {
        PortfolioRegime::Trend => match family {
            "FAIR_VALUE" | "CEX_MICROSTRUCTURE" | "ORDER_FLOW" => 1.10,
            "ARBITRAGE" => 1.05,
            "MARKET_MAKING" => 0.95,
            "FLOW_PRESSURE" => 0.95,
            "CARRY_PARITY" => 0.95,
            "BIAS_EXPLOITATION" => 1.00,
            _ => 1.00,
        },
        PortfolioRegime::MeanRevert => match family {
            "MARKET_MAKING" | "CARRY_PARITY" | "BIAS_EXPLOITATION" => 1.10,
            "ARBITRAGE" => 1.05,
            "FAIR_VALUE" => 0.90,
            "CEX_MICROSTRUCTURE" => 0.95,
            "ORDER_FLOW" => 0.95,
            "FLOW_PRESSURE" => 0.95,
            _ => 1.00,
        },
        PortfolioRegime::LowLiquidity => match family {
            "ARBITRAGE" => 0.90,
            "MARKET_MAKING" => 0.80,
            "CARRY_PARITY" => 0.80,
            "FAIR_VALUE" => 0.75,
            "CEX_MICROSTRUCTURE" => 0.70,
            "ORDER_FLOW" => 0.70,
            "FLOW_PRESSURE" => 0.70,
            "BIAS_EXPLOITATION" => 0.80,
            _ => 0.85,
        },
        PortfolioRegime::Chop => match family {
            "MARKET_MAKING" | "CARRY_PARITY" | "BIAS_EXPLOITATION" => 1.05,
            "ARBITRAGE" => 1.00,
            "FAIR_VALUE" => 0.85,
            "CEX_MICROSTRUCTURE" => 0.90,
            "ORDER_FLOW" => 0.90,
            "FLOW_PRESSURE" => 0.90,
            _ => 1.00,
        },
        PortfolioRegime::Unknown => 1.00,
    }
}

async fn read_portfolio_regime(
    conn: &mut redis::aio::MultiplexedConnection,
) -> (PortfolioRegime, f64) {
    let raw: Option<String> = match conn.get("meta_controller:regime").await {
        Ok(value) => value,
        Err(error) => {
            warn!("failed to read meta_controller:regime from redis: {}", error);
            None
        }
    };
    let Some(payload) = raw else {
        return (PortfolioRegime::Unknown, 0.0);
    };
    let parsed: serde_json::Value = match serde_json::from_str(&payload) {
        Ok(value) => value,
        Err(error) => {
            warn!("failed to parse meta_controller:regime payload: {}", error);
            return (PortfolioRegime::Unknown, 0.0);
        }
    };
    let regime = parsed
        .get("regime")
        .and_then(|v| v.as_str())
        .map(parse_portfolio_regime)
        .unwrap_or(PortfolioRegime::Unknown);
    let confidence = parsed
        .get("confidence")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0)
        .clamp(0.0, 1.0);
    (regime, confidence)
}

async fn read_f64_or(
    conn: &mut redis::aio::MultiplexedConnection,
    key: &str,
    fallback: f64,
) -> f64 {
    match conn.get::<_, f64>(key).await {
        Ok(value) => value,
        Err(error) => {
            warn!("failed to read {} from redis; using fallback {}: {}", key, fallback, error);
            fallback
        }
    }
}

pub async fn read_risk_config(conn: &mut redis::aio::MultiplexedConnection) -> RiskConfig {
    let raw: String = match conn.get("system:risk_config").await {
        Ok(payload) => payload,
        Err(error) => {
            warn!("failed to read system:risk_config from redis: {}", error);
            return RiskConfig::default();
        }
    };
    match serde_json::from_str::<RiskConfig>(&raw) {
        Ok(config) => config,
        Err(error) => {
            warn!("invalid system:risk_config payload; using default: {}", error);
            RiskConfig::default()
        }
    }
}

async fn ensure_sim_ledger(conn: &mut redis::aio::MultiplexedConnection) {
    let fallback_equity = read_f64_or(conn, SIM_BANKROLL_KEY, DEFAULT_SIM_BANKROLL).await;
    if let Err(error) = conn.set_nx::<_, _, bool>(SIM_BANKROLL_KEY, fallback_equity).await {
        warn!("failed to initialize {}: {}", SIM_BANKROLL_KEY, error);
    }
    if let Err(error) = conn.set_nx::<_, _, bool>(SIM_LEDGER_CASH_KEY, fallback_equity).await {
        warn!("failed to initialize {}: {}", SIM_LEDGER_CASH_KEY, error);
    }
    if let Err(error) = conn.set_nx::<_, _, bool>(SIM_LEDGER_RESERVED_KEY, 0.0_f64).await {
        warn!("failed to initialize {}: {}", SIM_LEDGER_RESERVED_KEY, error);
    }
    if let Err(error) = conn
        .set_nx::<_, _, bool>(SIM_LEDGER_REALIZED_PNL_KEY, 0.0_f64)
        .await
    {
        warn!("failed to initialize {}: {}", SIM_LEDGER_REALIZED_PNL_KEY, error);
    }
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
        "BTC_5M" | "BTC_15M" | "ETH_5M" | "ETH_15M" | "SOL_5M" | "SOL_15M" => "FAIR_VALUE",
        "ATOMIC_ARB" | "GRAPH_ARB" => "ARBITRAGE",
        "CEX_SNIPER" => "CEX_MICROSTRUCTURE",
        "OBI_SCALPER" => "ORDER_FLOW",
        "SYNDICATE" => "FLOW_PRESSURE",
        "CONVERGENCE_CARRY" => "CARRY_PARITY",
        "MAKER_MM" => "MARKET_MAKING",
        "AS_MARKET_MAKER" => "MARKET_MAKING",
        "LONGSHOT_BIAS" => "BIAS_EXPLOITATION",
        _ => "GENERIC",
    }
}

pub fn strategy_underlying(strategy_id: &str) -> &'static str {
    match strategy_id.trim().to_uppercase().as_str() {
        "BTC_5M" | "BTC_15M" => "BTC",
        "ETH_5M" | "ETH_15M" => "ETH",
        "SOL_5M" | "SOL_15M" => "SOL",
        "CEX_SNIPER" | "OBI_SCALPER" | "SYNDICATE" => "BTC",
        "ATOMIC_ARB" | "GRAPH_ARB" | "CONVERGENCE_CARRY" | "MAKER_MM"
            | "AS_MARKET_MAKER" | "LONGSHOT_BIAS" => "POLY_EVENT",
        _ => "UNKNOWN",
    }
}

fn underlying_reserved_key(underlying: &str) -> String {
    format!("{}{}", SIM_LEDGER_RESERVED_BY_UNDERLYING_PREFIX, underlying.trim().to_uppercase())
}

fn underlying_concentration_cap_pct() -> f64 {
    read_env_cap_pct(
        "SIM_UNDERLYING_CONCENTRATION_CAP_PCT",
        DEFAULT_UNDERLYING_CONCENTRATION_CAP_PCT,
        0.10,
        1.0,
    )
}

pub async fn read_underlying_reserved_notional(conn: &mut redis::aio::MultiplexedConnection, underlying: &str) -> f64 {
    ensure_sim_ledger(conn).await;
    let key = underlying_reserved_key(underlying);
    read_f64_or(conn, &key, 0.0).await
}

pub async fn read_strategy_reserved_notional(conn: &mut redis::aio::MultiplexedConnection, strategy_id: &str) -> f64 {
    ensure_sim_ledger(conn).await;
    let key = strategy_reserved_key(strategy_id);
    read_f64_or(conn, &key, 0.0).await
}

pub async fn read_family_reserved_notional(conn: &mut redis::aio::MultiplexedConnection, family_id: &str) -> f64 {
    ensure_sim_ledger(conn).await;
    let key = family_reserved_key(family_id);
    read_f64_or(conn, &key, 0.0).await
}

pub async fn read_sim_available_cash(conn: &mut redis::aio::MultiplexedConnection) -> f64 {
    ensure_sim_ledger(conn).await;
    read_f64_or(conn, SIM_LEDGER_CASH_KEY, DEFAULT_SIM_BANKROLL).await
}

pub async fn read_sim_reserved_notional(conn: &mut redis::aio::MultiplexedConnection) -> f64 {
    ensure_sim_ledger(conn).await;
    read_f64_or(conn, SIM_LEDGER_RESERVED_KEY, 0.0).await
}

pub async fn read_sim_bankroll(conn: &mut redis::aio::MultiplexedConnection) -> f64 {
    ensure_sim_ledger(conn).await;
    let cash = read_sim_available_cash(conn).await;
    let reserved = read_sim_reserved_notional(conn).await;
    let equity = cash + reserved;
    if let Err(error) = conn.set::<_, _, ()>(SIM_BANKROLL_KEY, equity).await {
        warn!("[Ledger] failed to persist {}: {}", SIM_BANKROLL_KEY, error);
    }
    equity
}

/// Validates that the ledger invariant holds: cash + reserved ≈ bankroll.
/// Returns true if invariant holds, false if drift detected.
pub async fn validate_ledger_invariant(conn: &mut redis::aio::MultiplexedConnection) -> bool {
    let cash = read_sim_available_cash(conn).await;
    let reserved = read_sim_reserved_notional(conn).await;
    let bankroll = read_f64_or(conn, SIM_BANKROLL_KEY, 0.0).await;
    let computed = cash + reserved;
    let diff = (computed - bankroll).abs();
    if diff > 1.0 {
        log::error!(
            "LEDGER_INVARIANT_VIOLATION: cash={:.2} + reserved={:.2} = {:.2} != bankroll={:.2}, diff={:.4}",
            cash, reserved, computed, bankroll, diff
        );
        if let Err(error) = redis::cmd("PUBLISH")
            .arg("system:alert")
            .arg(format!(r#"{{"level":"CRITICAL","msg":"LEDGER_DRIFT","diff":{:.4}}}"#, diff))
            .query_async::<()>(&mut *conn)
            .await
        {
            warn!("[Ledger] failed to publish drift alert: {}", error);
        }
        return false;
    }
    true
}

const SIM_LEDGER_PEAK_EQUITY_KEY: &str = "sim_ledger:peak_equity";

pub async fn read_sim_realized_pnl(conn: &mut redis::aio::MultiplexedConnection) -> f64 {
    ensure_sim_ledger(conn).await;
    read_f64_or(conn, SIM_LEDGER_REALIZED_PNL_KEY, 0.0).await
}

/// Returns true if the portfolio drawdown from peak equity exceeds the limit.
/// `max_dd_pct` should be negative (e.g., -5.0 means -5% drawdown triggers).
pub async fn check_drawdown_breached(conn: &mut redis::aio::MultiplexedConnection, max_dd_pct: f64) -> bool {
    if max_dd_pct >= 0.0 {
        return false; // No drawdown limit configured
    }
    let equity = read_sim_bankroll(conn).await;
    if equity <= 0.0 {
        return true; // Zero equity is a breach
    }
    let peak = read_f64_or(conn, SIM_LEDGER_PEAK_EQUITY_KEY, equity).await;
    let peak = if equity > peak || peak <= 0.0 {
        if let Err(error) = conn.set::<_, _, ()>(SIM_LEDGER_PEAK_EQUITY_KEY, equity).await {
            warn!("[Ledger] failed to persist {}: {}", SIM_LEDGER_PEAK_EQUITY_KEY, error);
        }
        equity
    } else {
        peak
    };
    let dd_pct = ((equity - peak) / peak) * 100.0;
    dd_pct <= max_dd_pct
}

pub async fn reserve_sim_notional_for_strategy(
    conn: &mut redis::aio::MultiplexedConnection,
    strategy_id: &str,
    notional: f64,
) -> bool {
    if !notional.is_finite() || notional <= 0.0 {
        return false;
    }

    ensure_sim_ledger(conn).await;
    let family_id = strategy_family(strategy_id);
    let underlying_id = strategy_underlying(strategy_id);
    let strategy_key = strategy_reserved_key(strategy_id);
    let family_key = family_reserved_key(family_id);
    let underlying_key = underlying_reserved_key(underlying_id);
    let strategy_cap = strategy_concentration_cap_pct();
    let family_cap = family_concentration_cap_pct();
    let underlying_cap = underlying_concentration_cap_pct();
    let utilization_cap = global_utilization_cap_pct();

    let reserve_script = r#"
        local cash = tonumber(redis.call("GET", KEYS[1]) or "0")
        local reserved = tonumber(redis.call("GET", KEYS[2]) or "0")
        local amount = tonumber(ARGV[1]) or 0
        local strategy_reserved = tonumber(redis.call("GET", KEYS[4]) or "0")
        local family_reserved = tonumber(redis.call("GET", KEYS[5]) or "0")
        local underlying_reserved = tonumber(redis.call("GET", KEYS[6]) or "0")
        local strategy_cap = tonumber(ARGV[2]) or 1
        local family_cap = tonumber(ARGV[3]) or 1
        local utilization_cap = tonumber(ARGV[4]) or 1
        local epsilon = tonumber(ARGV[5]) or 1e-9
        local underlying_cap = tonumber(ARGV[6]) or 1
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
        if underlying_cap > 0 and (underlying_reserved + amount) > ((equity * underlying_cap) + epsilon) then
            redis.call("SET", KEYS[3], equity)
            return {0, cash, reserved, equity, 6}
        end
        cash = cash - amount
        reserved = reserved + amount
        strategy_reserved = strategy_reserved + amount
        family_reserved = family_reserved + amount
        underlying_reserved = underlying_reserved + amount
        equity = cash + reserved
        redis.call("SET", KEYS[1], cash)
        redis.call("SET", KEYS[2], reserved)
        redis.call("SET", KEYS[3], equity)
        redis.call("SET", KEYS[4], strategy_reserved)
        redis.call("SET", KEYS[5], family_reserved)
        redis.call("SET", KEYS[6], underlying_reserved)
        return {1, cash, reserved, equity, 0}
    "#;

    let eval_result = redis::cmd("EVAL")
        .arg(reserve_script)
        .arg(6)
        .arg(SIM_LEDGER_CASH_KEY)
        .arg(SIM_LEDGER_RESERVED_KEY)
        .arg(SIM_BANKROLL_KEY)
        .arg(&strategy_key)
        .arg(&family_key)
        .arg(&underlying_key)
        .arg(notional)
        .arg(strategy_cap)
        .arg(family_cap)
        .arg(utilization_cap)
        .arg(DEFAULT_NUMERIC_EPSILON)
        .arg(underlying_cap)
        .query_async::<(i32, f64, f64, f64, i32)>(conn)
        .await;

    match eval_result {
        Ok((ok, _cash, _reserved, _equity, _reason_code)) => {
            let reserved_ok = ok == 1;
            if reserved_ok {
                let _ = validate_ledger_invariant(conn).await;
            }
            reserved_ok
        }
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
            let und_reserved = read_underlying_reserved_notional(conn, underlying_id).await;
            if strategy_reserved + notional > (equity * strategy_cap + DEFAULT_NUMERIC_EPSILON) {
                return false;
            }
            if family_reserved + notional > (equity * family_cap + DEFAULT_NUMERIC_EPSILON) {
                return false;
            }
            if und_reserved + notional > (equity * underlying_cap + DEFAULT_NUMERIC_EPSILON) {
                return false;
            }
            if current_reserved + notional > (equity * utilization_cap + DEFAULT_NUMERIC_EPSILON) {
                return false;
            }
            let write_result = redis::pipe()
                .atomic()
                .cmd("INCRBYFLOAT")
                .arg(SIM_LEDGER_CASH_KEY)
                .arg(-notional)
                .ignore()
                .cmd("INCRBYFLOAT")
                .arg(SIM_LEDGER_RESERVED_KEY)
                .arg(notional)
                .ignore()
                .cmd("INCRBYFLOAT")
                .arg(&strategy_key)
                .arg(notional)
                .ignore()
                .cmd("INCRBYFLOAT")
                .arg(&family_key)
                .arg(notional)
                .ignore()
                .cmd("INCRBYFLOAT")
                .arg(&underlying_key)
                .arg(notional)
                .ignore()
                .cmd("SET")
                .arg(SIM_BANKROLL_KEY)
                .arg(equity)
                .ignore()
                .query_async::<()>(conn)
                .await;

            if let Err(e) = write_result {
                log::error!("REDIS_WRITE_FAIL reserve fallback MULTI/EXEC: {}", e);
                return false;
            }
            let _ = validate_ledger_invariant(conn).await;
            true
        }
    }
}

pub async fn settle_sim_position_for_strategy(
    conn: &mut redis::aio::MultiplexedConnection,
    strategy_id: &str,
    reserved_notional: f64,
    pnl: f64,
) -> f64 {
    ensure_sim_ledger(conn).await;
    let family_id = strategy_family(strategy_id);
    let underlying_id = strategy_underlying(strategy_id);
    let strategy_key = strategy_reserved_key(strategy_id);
    let family_key = family_reserved_key(family_id);
    let underlying_key = underlying_reserved_key(underlying_id);
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
        local underlying_reserved = tonumber(redis.call("GET", KEYS[7]) or "0")
        local release_notional = tonumber(ARGV[1]) or 0
        local pnl = tonumber(ARGV[2]) or 0
        if release_notional < 0 then
            release_notional = 0
        end
        local release = math.min(release_notional, reserved)
        reserved = reserved - release
        strategy_reserved = math.max(0, strategy_reserved - release)
        family_reserved = math.max(0, family_reserved - release)
        underlying_reserved = math.max(0, underlying_reserved - release)
        cash = cash + release + pnl
        if cash < 0 then
            cash = 0
        end
        local equity = cash + reserved
        redis.call("SET", KEYS[1], cash)
        redis.call("SET", KEYS[2], reserved)
        redis.call("SET", KEYS[3], equity)
        redis.call("INCRBYFLOAT", KEYS[4], pnl)
        redis.call("SET", KEYS[5], strategy_reserved)
        redis.call("SET", KEYS[6], family_reserved)
        redis.call("SET", KEYS[7], underlying_reserved)
        return {cash, reserved, equity}
    "#;

    let eval_result = redis::cmd("EVAL")
        .arg(settle_script)
        .arg(7)
        .arg(SIM_LEDGER_CASH_KEY)
        .arg(SIM_LEDGER_RESERVED_KEY)
        .arg(SIM_BANKROLL_KEY)
        .arg(SIM_LEDGER_REALIZED_PNL_KEY)
        .arg(&strategy_key)
        .arg(&family_key)
        .arg(&underlying_key)
        .arg(release_notional)
        .arg(pnl_value)
        .query_async::<(f64, f64, f64)>(conn)
        .await;

    match eval_result {
        Ok((_cash, _reserved, equity)) => {
            let _ = validate_ledger_invariant(conn).await;
            equity
        }
        Err(_) => {
            let reserved_before = read_sim_reserved_notional(conn).await;
            let release = release_notional.min(reserved_before).max(0.0);
            let strategy_reserved_before = read_strategy_reserved_notional(conn, strategy_id).await;
            let strategy_release = release.min(strategy_reserved_before);
            let family_reserved_before = read_family_reserved_notional(conn, family_id).await;
            let family_release = release.min(family_reserved_before);
            let und_reserved_before = read_underlying_reserved_notional(conn, underlying_id).await;
            let und_release = release.min(und_reserved_before);
            let write_result = redis::pipe()
                .atomic()
                .cmd("INCRBYFLOAT")
                .arg(SIM_LEDGER_RESERVED_KEY)
                .arg(-release)
                .ignore()
                .cmd("INCRBYFLOAT")
                .arg(&strategy_key)
                .arg(-strategy_release)
                .ignore()
                .cmd("INCRBYFLOAT")
                .arg(&family_key)
                .arg(-family_release)
                .ignore()
                .cmd("INCRBYFLOAT")
                .arg(&underlying_key)
                .arg(-und_release)
                .ignore()
                .cmd("INCRBYFLOAT")
                .arg(SIM_LEDGER_CASH_KEY)
                .arg(release + pnl_value)
                .ignore()
                .cmd("INCRBYFLOAT")
                .arg(SIM_LEDGER_REALIZED_PNL_KEY)
                .arg(pnl_value)
                .ignore()
                .query_async::<()>(conn)
                .await;

            if let Err(e) = write_result {
                log::error!("REDIS_WRITE_FAIL settle fallback MULTI/EXEC: {}", e);
                return 0.0;
            }
            let equity = read_sim_bankroll(conn).await;
            let _ = validate_ledger_invariant(conn).await;
            equity
        }
    }
}

pub async fn release_sim_notional_for_strategy(
    conn: &mut redis::aio::MultiplexedConnection,
    strategy_id: &str,
    reserved_notional: f64,
) -> f64 {
    settle_sim_position_for_strategy(conn, strategy_id, reserved_notional, 0.0).await
}

pub async fn read_simulation_reset_ts(conn: &mut redis::aio::MultiplexedConnection) -> i64 {
    conn.get::<_, i64>("system:simulation_reset_ts")
        .await
        .unwrap_or(0)
}

pub async fn read_trading_mode(conn: &mut redis::aio::MultiplexedConnection) -> TradingMode {
    let raw: String = conn.get("system:trading_mode").await.unwrap_or_else(|_| "PAPER".to_string());
    if raw.trim().eq_ignore_ascii_case("LIVE") {
        TradingMode::Live
    } else {
        TradingMode::Paper
    }
}

/// Persist strategy mode marker and return true only when this process
/// observes a PAPER -> LIVE transition for the strategy.
pub async fn entered_live_mode(
    conn: &mut redis::aio::MultiplexedConnection,
    strategy_id: &str,
    current_mode: TradingMode,
) -> bool {
    let strategy_key = strategy_id.trim().to_uppercase();
    if strategy_key.is_empty() {
        return false;
    }
    let key = format!("{}{}", STRATEGY_LAST_MODE_PREFIX, strategy_key);
    let current = match current_mode {
        TradingMode::Live => "LIVE",
        TradingMode::Paper => "PAPER",
    };
    let previous = conn.get::<_, Option<String>>(&key).await.unwrap_or(None);
    let just_entered_live = matches!(current_mode, TradingMode::Live)
        && !previous
            .as_deref()
            .is_some_and(|value| value.trim().eq_ignore_ascii_case("LIVE"));
    if let Err(error) = conn.set::<_, _, ()>(&key, current).await {
        warn!("failed to persist strategy mode marker {}: {}", key, error);
    }
    just_entered_live
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

pub async fn read_strategy_risk_multiplier(conn: &mut redis::aio::MultiplexedConnection, strategy_id: &str) -> f64 {
    if strategy_id.trim().is_empty() {
        return 0.25;
    }

    let key = format!("{}{}", STRATEGY_RISK_MULTIPLIER_PREFIX, strategy_id.trim());
    match conn.get::<_, f64>(&key).await {
        Ok(value) if value.is_finite() => return value.clamp(0.0, 3.0),
        Ok(_) => {}
        Err(error) => warn!("failed to read strategy risk multiplier {}: {}", key, error),
    }

    if let Err(error) = conn.set_nx::<_, _, bool>(&key, 0.25_f64).await {
        warn!("failed to initialize strategy risk multiplier {}: {}", key, error);
    }
    0.25
}

pub async fn read_strategy_risk_overlay(conn: &mut redis::aio::MultiplexedConnection, strategy_id: &str) -> f64 {
    if strategy_id.trim().is_empty() {
        return 1.0;
    }
    let key = format!("{}{}", STRATEGY_RISK_OVERLAY_PREFIX, strategy_id.trim());
    match conn.get::<_, Option<f64>>(&key).await {
        Ok(Some(value)) if value.is_finite() => value.clamp(0.0, 2.5),
        Ok(Some(_)) => 1.0,
        Ok(None) => 1.0,
        Err(error) => {
            warn!("failed to read strategy risk overlay {}: {}", key, error);
            1.0
        }
    }
}

/// Read anti-martingale size factor from Redis (set by backend risk guard).
pub async fn read_risk_guard_size_factor(conn: &mut redis::aio::MultiplexedConnection, strategy_id: &str) -> f64 {
    let key = format!("risk_guard:size_factor:{}", strategy_id);
    match conn.get::<_, Option<f64>>(&key).await {
        Ok(Some(value)) => value.clamp(0.1, 1.0),
        Ok(None) => 1.0,
        Err(error) => {
            warn!("failed to read risk guard size factor {}: {}", key, error);
            1.0
        }
    }
}

/// Read post-loss cooldown deadline from Redis (epoch ms). Returns 0 if not set.
pub async fn read_risk_guard_cooldown(conn: &mut redis::aio::MultiplexedConnection, strategy_id: &str) -> i64 {
    let key = format!("risk_guard:cooldown:{}", strategy_id);
    match conn.get::<_, Option<i64>>(&key).await {
        Ok(Some(value)) => value,
        Ok(None) => 0,
        Err(error) => {
            warn!("failed to read risk guard cooldown {}: {}", key, error);
            0
        }
    }
}

pub async fn compute_strategy_bet_size(
    conn: &mut redis::aio::MultiplexedConnection,
    strategy_id: &str,
    bankroll: f64,
    cfg: &RiskConfig,
    min_size: f64,
    max_fraction: f64,
) -> f64 {
    let portfolio_dd_limit = global_max_drawdown_pct();
    if check_drawdown_breached(conn, portfolio_dd_limit).await {
        return 0.0;
    }

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

    let overlay = read_strategy_risk_overlay(conn, strategy_id).await;
    if overlay <= 0.0 {
        return 0.0;
    }
    sized *= overlay;

    // Apply anti-martingale size reduction (from backend risk guard).
    let size_factor = read_risk_guard_size_factor(conn, strategy_id).await;
    if size_factor < 1.0 {
        sized *= size_factor;
    }

    if regime_sizing_enabled() {
        let (regime, confidence) = read_portfolio_regime(conn).await;
        let family = strategy_family(strategy_id);
        let regime_multiplier = regime_family_size_multiplier(regime, family, confidence);
        if regime_multiplier <= DEFAULT_NUMERIC_EPSILON {
            return 0.0;
        }
        sized *= regime_multiplier;
    }

    // Per-strategy hard notional cap (env: {STRATEGY_ID}_NOTIONAL_CAP).
    let cap_key = format!("{}_NOTIONAL_CAP", strategy_id);
    let notional_cap: f64 = std::env::var(&cap_key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(f64::MAX);
    if notional_cap > 0.0 && sized > notional_cap {
        sized = notional_cap;
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
    let underlying_id = strategy_underlying(strategy_id);
    let reserved_total = read_sim_reserved_notional(conn).await;
    let reserved_strategy = read_strategy_reserved_notional(conn, strategy_id).await;
    let reserved_family = read_family_reserved_notional(conn, family_id).await;
    let reserved_underlying = read_underlying_reserved_notional(conn, underlying_id).await;
    let remaining_strategy = (equity * strategy_concentration_cap_pct()) - reserved_strategy;
    let remaining_family = (equity * family_concentration_cap_pct()) - reserved_family;
    let remaining_underlying = (equity * underlying_concentration_cap_pct()) - reserved_underlying;
    let remaining_utilization = (equity * global_utilization_cap_pct()) - reserved_total;
    let cap_remaining = remaining_strategy.min(remaining_family).min(remaining_underlying).min(remaining_utilization);
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

pub async fn is_strategy_enabled(conn: &mut redis::aio::MultiplexedConnection, strategy_id: &str) -> bool {
    let key = format!("strategy:enabled:{}", strategy_id);
    match conn.get::<_, Option<String>>(key).await {
        Ok(Some(raw)) => raw != "0",
        Ok(None) => {
            warn!("strategy enable key missing for {}; fail-closed", strategy_id);
            false
        }
        Err(error) => {
            warn!("failed to read strategy enable key for {}; fail-closed: {}", strategy_id, error);
            false
        }
    }
}

pub async fn publish_heartbeat(conn: &mut redis::aio::MultiplexedConnection, id: &str) {
    let realized_pnl = read_sim_realized_pnl(conn).await;
    let heartbeat = serde_json::json!({
        "id": id,
        "timestamp": Utc::now().timestamp_millis(),
        "sim_realized_pnl": realized_pnl,
    });

    publish_event(conn, "system:heartbeat", heartbeat.to_string()).await;
}

pub async fn publish_event(
    conn: &mut redis::aio::MultiplexedConnection,
    channel: &str,
    payload: impl Into<String>,
) {
    let encoded = payload.into();
    if let Err(error) = conn.publish::<_, _, ()>(channel, encoded).await {
        warn!("[RedisPublish] channel={} error={}", channel, error);
    }
}

fn execution_event_hmac_secret() -> Option<String> {
    std::env::var("EXECUTION_EVENT_HMAC_SECRET")
        .ok()
        .map(|raw| raw.trim().to_string())
        .filter(|raw| !raw.is_empty())
}

fn execution_event_hmac(payload: &serde_json::Value, secret: &str) -> Option<String> {
    let serialized = serde_json::to_string(payload).ok()?;
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).ok()?;
    mac.update(serialized.as_bytes());
    Some(hex::encode(mac.finalize().into_bytes()))
}

pub async fn publish_execution_event(
    conn: &mut redis::aio::MultiplexedConnection,
    payload: serde_json::Value,
) {
    let encoded = if let Some(secret) = execution_event_hmac_secret() {
        if let Some(signature) = execution_event_hmac(&payload, &secret) {
            serde_json::json!({
                "payload": payload,
                "_sig_algo": "HMAC_SHA256",
                "_sig": signature,
            })
            .to_string()
        } else {
            payload.to_string()
        }
    } else {
        payload.to_string()
    };
    publish_event(conn, "arbitrage:execution", encoded).await;
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

#[cfg(test)]
mod tests {
    use super::{strategy_family, strategy_underlying};

    #[test]
    fn strategy_family_maps_known_ids() {
        assert_eq!(strategy_family("BTC_5M"), "FAIR_VALUE");
        assert_eq!(strategy_family("BTC_15M"), "FAIR_VALUE");
        assert_eq!(strategy_family("ATOMIC_ARB"), "ARBITRAGE");
        assert_eq!(strategy_family("OBI_SCALPER"), "ORDER_FLOW");
        assert_eq!(strategy_family("MAKER_MM"), "MARKET_MAKING");
        assert_eq!(strategy_family("unknown"), "GENERIC");
    }

    #[test]
    fn strategy_underlying_maps_known_ids() {
        assert_eq!(strategy_underlying("BTC_5M"), "BTC");
        assert_eq!(strategy_underlying("BTC_15M"), "BTC");
        assert_eq!(strategy_underlying("ETH_15M"), "ETH");
        assert_eq!(strategy_underlying("SOL_15M"), "SOL");
        assert_eq!(strategy_underlying("CEX_SNIPER"), "BTC");
        assert_eq!(strategy_underlying("MAKER_MM"), "POLY_EVENT");
        assert_eq!(strategy_underlying("AS_MARKET_MAKER"), "POLY_EVENT");
        assert_eq!(strategy_underlying("LONGSHOT_BIAS"), "POLY_EVENT");
        assert_eq!(strategy_underlying("unknown"), "UNKNOWN");
    }
}
