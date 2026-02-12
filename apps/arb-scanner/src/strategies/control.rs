use chrono::Utc;
use redis::AsyncCommands;
use serde::Deserialize;

pub const DEFAULT_SIM_BANKROLL: f64 = 1000.0;

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

pub async fn read_sim_bankroll(conn: &mut redis::aio::Connection) -> f64 {
    conn.get::<_, f64>("sim_bankroll")
        .await
        .unwrap_or(DEFAULT_SIM_BANKROLL)
}

pub async fn apply_sim_pnl(conn: &mut redis::aio::Connection, pnl: f64) -> f64 {
    let bankroll_before = read_sim_bankroll(conn).await;
    redis::cmd("INCRBYFLOAT")
        .arg("sim_bankroll")
        .arg(pnl)
        .query_async::<_, f64>(conn)
        .await
        .unwrap_or(bankroll_before + pnl)
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
