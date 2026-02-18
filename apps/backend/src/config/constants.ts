/**
 * Shared configuration constants for the AI Trader backend.
 * All values are initialized from process.env at module load time.
 * Extracted from index.ts to reduce monolith size.
 */
import path from 'path';
import type { RuntimePhase, RuntimeModuleState } from '../types/backend-types';

// ── Strategy identifiers ────────────────────────────────────────────
export const STRATEGY_IDS = [
    'BTC_5M',
    'BTC_15M',
    'ETH_15M',
    'SOL_15M',
    'CEX_SNIPER',
    'SYNDICATE',
    'ATOMIC_ARB',
    'OBI_SCALPER',
    'GRAPH_ARB',
    'CONVERGENCE_CARRY',
    'MAKER_MM',
    'AS_MARKET_MAKER',
    'LONGSHOT_BIAS',
];

export const SCANNER_HEARTBEAT_IDS = [
    'scanner_swarms',
    'btc_5m',
    'btc_15m',
    'eth_15m',
    'sol_15m',
    'cex_arb',
    'SYNDICATE',
    'copy_bot',
    'atomic_arb',
    'obi_scalper',
    'graph_arb',
    'convergence_carry',
    'maker_mm',
    'AS_MARKET_MAKER',
    'LONGSHOT_BIAS',
];

// ── Execution trace ─────────────────────────────────────────────────
export const EXECUTION_TRACE_TTL_MS = Math.max(60_000, Number(process.env.EXECUTION_TRACE_TTL_MS || String(6 * 60 * 60 * 1000)));
export const EXECUTION_TRACE_MAX_EVENTS = Math.max(10, Number(process.env.EXECUTION_TRACE_MAX_EVENTS || '80'));
export const EXECUTION_EVENT_HMAC_SECRET = (process.env.EXECUTION_EVENT_HMAC_SECRET || '').trim();
export const EXECUTION_EVENT_REQUIRE_SIGNATURE = (
    process.env.EXECUTION_EVENT_REQUIRE_SIGNATURE === undefined
        ? EXECUTION_EVENT_HMAC_SECRET.length > 0
        : process.env.EXECUTION_EVENT_REQUIRE_SIGNATURE === 'true'
);

// ── Sim ledger ──────────────────────────────────────────────────────
export const DEFAULT_SIM_BANKROLL = 1000;
export const STRATEGY_METRICS_KEY = 'system:strategy_metrics:paper:v1';
export const SIM_BANKROLL_KEY = 'sim_bankroll';
export const SIM_LEDGER_CASH_KEY = 'sim_ledger:cash';
export const SIM_LEDGER_RESERVED_KEY = 'sim_ledger:reserved';
export const SIM_LEDGER_REALIZED_PNL_KEY = 'sim_ledger:realized_pnl';
export const SIM_LEDGER_RESERVED_BY_STRATEGY_PREFIX = 'sim_ledger:reserved:strategy:';
export const SIM_LEDGER_RESERVED_BY_FAMILY_PREFIX = 'sim_ledger:reserved:family:';
export const SIM_LEDGER_RESERVED_BY_UNDERLYING_PREFIX = 'sim_ledger:reserved:underlying:';
export const SIM_STRATEGY_CONCENTRATION_CAP_PCT = Math.min(
    1,
    Math.max(0.05, Number(process.env.SIM_STRATEGY_CONCENTRATION_CAP_PCT || '0.35')),
);
export const SIM_FAMILY_CONCENTRATION_CAP_PCT = Math.min(
    1,
    Math.max(0.10, Number(process.env.SIM_FAMILY_CONCENTRATION_CAP_PCT || '0.60')),
);
export const SIM_UNDERLYING_CONCENTRATION_CAP_PCT = Math.min(
    1,
    Math.max(0.10, Number(process.env.SIM_UNDERLYING_CONCENTRATION_CAP_PCT || '0.70')),
);
export const SIM_GLOBAL_UTILIZATION_CAP_PCT = Math.min(
    1,
    Math.max(0.10, Number(process.env.SIM_GLOBAL_UTILIZATION_CAP_PCT || '0.90')),
);

// ── Risk allocator ──────────────────────────────────────────────────
export const STRATEGY_RISK_MULTIPLIER_PREFIX = 'strategy:risk_multiplier:';
export const STRATEGY_WEIGHT_FLOOR = 0.25;
export const STRATEGY_WEIGHT_CAP = Math.min(3.0, Math.max(1.0, Number(process.env.STRATEGY_WEIGHT_CAP || '2.0')));
export const STRATEGY_ALLOCATOR_EPSILON = 1e-6;
export const STRATEGY_ALLOCATOR_MIN_SAMPLES = Math.max(4, Number(process.env.STRATEGY_ALLOCATOR_MIN_SAMPLES || '30'));
export const STRATEGY_ALLOCATOR_TARGET_SHARPE = Number(process.env.STRATEGY_ALLOCATOR_TARGET_SHARPE || '0.55');
export const DEFAULT_DISABLED_STRATEGIES = new Set(
    (process.env.DEFAULT_DISABLED_STRATEGIES || 'SOL_15M,SYNDICATE')
        .split(',')
        .map((value) => value.trim().toUpperCase())
        .filter((value) => value.length > 0 && STRATEGY_IDS.includes(value)),
);

// ── Profit Vault ────────────────────────────────────────────────────
export const VAULT_ENABLED = process.env.VAULT_ENABLED !== 'false';
export const VAULT_BANKROLL_CEILING = Math.max(0, Number(process.env.VAULT_BANKROLL_CEILING || '1100'));
export const VAULT_REDIS_KEY = 'sim_ledger:vault';

// ── Risk Guard ──────────────────────────────────────────────────────
export const RISK_GUARD_TRAILING_STOP = Math.max(0, Number(process.env.RISK_GUARD_TRAILING_STOP || '100'));
export const RISK_GUARD_CONSEC_LOSS_LIMIT = Math.max(2, Number(process.env.RISK_GUARD_CONSEC_LOSS_LIMIT || '4'));
export const RISK_GUARD_CONSEC_LOSS_COOLDOWN_MS = Math.max(60_000, Number(process.env.RISK_GUARD_CONSEC_LOSS_COOLDOWN_MS || '1800000'));
export const RISK_GUARD_POST_LOSS_COOLDOWN_MS = Math.max(0, Number(process.env.RISK_GUARD_POST_LOSS_COOLDOWN_MS || '300000'));
export const RISK_GUARD_ANTI_MARTINGALE_AFTER = Math.max(1, Number(process.env.RISK_GUARD_ANTI_MARTINGALE_AFTER || '2'));
export const RISK_GUARD_ANTI_MARTINGALE_FACTOR = Math.min(1, Math.max(0.1, Number(process.env.RISK_GUARD_ANTI_MARTINGALE_FACTOR || '0.5')));
export const RISK_GUARD_DIRECTION_LIMIT = Math.max(2, Number(process.env.RISK_GUARD_DIRECTION_LIMIT || '3'));
export const RISK_GUARD_PROFIT_TAPER_START = Math.max(0, Number(process.env.RISK_GUARD_PROFIT_TAPER_START || '200'));
export const RISK_GUARD_PROFIT_TAPER_FLOOR = Math.min(1, Math.max(0.1, Number(process.env.RISK_GUARD_PROFIT_TAPER_FLOOR || '0.5')));
export const RISK_GUARD_DAILY_TARGET = Math.max(0, Number(process.env.RISK_GUARD_DAILY_TARGET || '300'));
export const RISK_GUARD_DAY_RESET_HOUR_UTC = Math.min(23, Math.max(0, Math.floor(Number(process.env.RISK_GUARD_DAY_RESET_HOUR_UTC || '8'))));
export const RISK_GUARD_STRATEGIES = new Set(
    (process.env.RISK_GUARD_STRATEGIES || STRATEGY_IDS.join(','))
        .split(',').map((s) => s.trim().toUpperCase()).filter((s) => s.length > 0),
);

// ── Signal & Intelligence Gate ──────────────────────────────────────
export const SIGNAL_WINDOW_MS = 30_000;
export const SIM_RESET_ON_BOOT = process.env.SIM_RESET_ON_BOOT === 'true';
export const RESET_VALIDATION_TRADES_ON_SIM_RESET = process.env.RESET_VALIDATION_TRADES_ON_SIM_RESET !== 'false';
export const CONTROL_PLANE_TOKEN = (process.env.CONTROL_PLANE_TOKEN || '').trim();
export const INTELLIGENCE_GATE_ENABLED = process.env.INTELLIGENCE_GATE_ENABLED !== 'false';
export const INTELLIGENCE_GATE_MAX_STALENESS_MS = Math.max(500, Number(process.env.INTELLIGENCE_GATE_MAX_STALENESS_MS || '3000'));
export const INTELLIGENCE_GATE_MIN_MARGIN = Number(process.env.INTELLIGENCE_GATE_MIN_MARGIN || '0');
export const INTELLIGENCE_GATE_CONFIRMATION_WINDOW_MS = Math.max(
    INTELLIGENCE_GATE_MAX_STALENESS_MS,
    Number(process.env.INTELLIGENCE_GATE_CONFIRMATION_WINDOW_MS || '5000'),
);
export const INTELLIGENCE_GATE_REQUIRE_PEER_CONFIRMATION = process.env.INTELLIGENCE_GATE_REQUIRE_PEER_CONFIRMATION !== 'false';
export const INTELLIGENCE_GATE_STRONG_MARGIN = Math.max(0, Number(process.env.INTELLIGENCE_GATE_STRONG_MARGIN || '0.01'));
export const INTELLIGENCE_GATE_CONFIRMATION_STRATEGIES = new Set([
    'BTC_5M',
    'BTC_15M',
    'ETH_15M',
    'SOL_15M',
    'CEX_SNIPER',
    'OBI_SCALPER',
    'CONVERGENCE_CARRY',
    'MAKER_MM',
]);
export const INTELLIGENCE_SCAN_RETENTION_MS = Math.max(60_000, INTELLIGENCE_GATE_CONFIRMATION_WINDOW_MS * 20);

// ── Data Integrity ──────────────────────────────────────────────────
export const DATA_INTEGRITY_ALERT_COOLDOWN_MS = Math.max(10_000, Number(process.env.DATA_INTEGRITY_ALERT_COOLDOWN_MS || '60000'));
export const DATA_INTEGRITY_ALERT_MIN_CONSECUTIVE_WARN = Math.max(2, Number(process.env.DATA_INTEGRITY_ALERT_MIN_CONSECUTIVE_WARN || '4'));
export const DATA_INTEGRITY_ALERT_MIN_CONSECUTIVE_CRITICAL = Math.max(
    DATA_INTEGRITY_ALERT_MIN_CONSECUTIVE_WARN + 1,
    Number(process.env.DATA_INTEGRITY_ALERT_MIN_CONSECUTIVE_CRITICAL || '8'),
);
export const DATA_INTEGRITY_ALERT_RING_LIMIT = Math.max(10, Number(process.env.DATA_INTEGRITY_ALERT_RING_LIMIT || '120'));
export const STRATEGY_SAMPLE_RETENTION = Math.max(100, Number(process.env.STRATEGY_SAMPLE_RETENTION || '2000'));

// ── Strategy Governance ─────────────────────────────────────────────
export const STRATEGY_GOVERNANCE_ENABLED = process.env.STRATEGY_GOVERNANCE_ENABLED !== 'false';
export const STRATEGY_GOVERNANCE_AUTOPILOT = process.env.STRATEGY_GOVERNANCE_AUTOPILOT === 'true';
export const STRATEGY_GOVERNANCE_INTERVAL_MS = Math.max(10_000, Number(process.env.STRATEGY_GOVERNANCE_INTERVAL_MS || '60000'));
export const STRATEGY_GOVERNANCE_MIN_TRADES = Math.max(10, Number(process.env.STRATEGY_GOVERNANCE_MIN_TRADES || '24'));
export const STRATEGY_GOVERNANCE_MIN_TRADES_DEMOTE = Math.max(
    STRATEGY_GOVERNANCE_MIN_TRADES,
    Number(process.env.STRATEGY_GOVERNANCE_MIN_TRADES_DEMOTE || '36'),
);
export const STRATEGY_GOVERNANCE_ACTION_COOLDOWN_MS = Math.max(
    STRATEGY_GOVERNANCE_INTERVAL_MS,
    Number(process.env.STRATEGY_GOVERNANCE_ACTION_COOLDOWN_MS || '180000'),
);
export const STRATEGY_GOVERNANCE_AUDIT_LIMIT = Math.max(30, Number(process.env.STRATEGY_GOVERNANCE_AUDIT_LIMIT || '250'));
export const STRATEGY_GOVERNANCE_WALK_FORWARD_SPLITS = Math.max(
    2,
    Number(process.env.STRATEGY_GOVERNANCE_WALK_FORWARD_SPLITS || '3'),
);
export const STRATEGY_GOVERNANCE_MIN_TEST_TRADES = Math.max(
    6,
    Number(process.env.STRATEGY_GOVERNANCE_MIN_TEST_TRADES || '8'),
);

// ── Feature Registry ────────────────────────────────────────────────
export const FEATURE_REGISTRY_MAX_ROWS = Math.max(1000, Number(process.env.FEATURE_REGISTRY_MAX_ROWS || '12000'));
export const FEATURE_REGISTRY_LABEL_LOOKBACK_MS = Math.max(
    60_000,
    Number(process.env.FEATURE_REGISTRY_LABEL_LOOKBACK_MS || '1800000'),
);

// ── Meta Controller ─────────────────────────────────────────────────
export const META_CONTROLLER_ENABLED = process.env.META_CONTROLLER_ENABLED !== 'false';
export const META_CONTROLLER_ADVISORY_ONLY = process.env.META_CONTROLLER_ADVISORY_ONLY !== 'false';
export const META_CONTROLLER_REFRESH_DEBOUNCE_MS = Math.max(
    100,
    Number(process.env.META_CONTROLLER_REFRESH_DEBOUNCE_MS || '400'),
);
export const CROSS_HORIZON_ROUTER_ENABLED = process.env.CROSS_HORIZON_ROUTER_ENABLED !== 'false';
export const CROSS_HORIZON_ROUTER_INTERVAL_MS = Math.max(
    1_000,
    Number(process.env.CROSS_HORIZON_ROUTER_INTERVAL_MS || '5000'),
);
export const CROSS_HORIZON_ROUTER_MIN_MARGIN = Math.max(
    0,
    Number(process.env.CROSS_HORIZON_ROUTER_MIN_MARGIN || '0.012'),
);
export const CROSS_HORIZON_ROUTER_STRONG_MARGIN = Math.max(
    CROSS_HORIZON_ROUTER_MIN_MARGIN,
    Number(process.env.CROSS_HORIZON_ROUTER_STRONG_MARGIN || '0.030'),
);
export const CROSS_HORIZON_ROUTER_CONFLICT_MULT = Math.min(
    1.0,
    Math.max(0.10, Number(process.env.CROSS_HORIZON_ROUTER_CONFLICT_MULT || '0.70')),
);
export const CROSS_HORIZON_ROUTER_CONFIRM_MULT = Math.max(
    1.0,
    Number(process.env.CROSS_HORIZON_ROUTER_CONFIRM_MULT || '1.15'),
);
export const CROSS_HORIZON_ROUTER_MAX_BOOST = Math.max(
    CROSS_HORIZON_ROUTER_CONFIRM_MULT,
    Number(process.env.CROSS_HORIZON_ROUTER_MAX_BOOST || '1.30'),
);
export const CROSS_HORIZON_OVERLAY_KEY_PREFIX = (
    process.env.CROSS_HORIZON_OVERLAY_KEY_PREFIX
    || 'strategy:risk_overlay:cross_horizon:'
).trim() || 'strategy:risk_overlay:cross_horizon:';

// ── Model Probability Gate ──────────────────────────────────────────
export const MODEL_PROBABILITY_GATE_ENABLED = process.env.MODEL_PROBABILITY_GATE_ENABLED !== 'false';
export const MODEL_PROBABILITY_GATE_ENFORCE_PAPER = process.env.MODEL_PROBABILITY_GATE_ENFORCE_PAPER !== 'false';
export const MODEL_PROBABILITY_GATE_ENFORCE_LIVE = process.env.MODEL_PROBABILITY_GATE_ENFORCE_LIVE !== 'false';
export const LEGACY_BRAIN_LOOP_ENABLED = process.env.LEGACY_BRAIN_LOOP_ENABLED === 'true';
export const LEGACY_BRAIN_LOOP_UNSAFE_OK = process.env.LEGACY_BRAIN_LOOP_UNSAFE_OK === 'true';
export const MODEL_PROBABILITY_GATE_MIN_PROB = Math.min(
    0.95,
    Math.max(0.50, Number(process.env.MODEL_PROBABILITY_GATE_MIN_PROB || '0.55')),
);
export const MODEL_PROBABILITY_GATE_MAX_STALENESS_MS = Math.max(
    500,
    Number(process.env.MODEL_PROBABILITY_GATE_MAX_STALENESS_MS || '5000'),
);
export const MODEL_PROBABILITY_GATE_REQUIRE_MODEL = process.env.MODEL_PROBABILITY_GATE_REQUIRE_MODEL === 'true';
export const MODEL_PROBABILITY_GATE_DISABLE_ON_DRIFT = process.env.MODEL_PROBABILITY_GATE_DISABLE_ON_DRIFT !== 'false';
export const MODEL_PROBABILITY_GATE_DRIFT_DISABLE_MS = Math.max(
    60_000,
    Number(process.env.MODEL_PROBABILITY_GATE_DRIFT_DISABLE_MS || '900000'),
);
export const MODEL_PROBABILITY_GATE_TUNER_ENABLED = process.env.MODEL_PROBABILITY_GATE_TUNER_ENABLED !== 'false';
export const MODEL_PROBABILITY_GATE_TUNER_ADVISORY_ONLY = process.env.MODEL_PROBABILITY_GATE_TUNER_ADVISORY_ONLY !== 'false';
export const MODEL_PROBABILITY_GATE_TUNER_INTERVAL_MS = Math.max(
    30_000,
    Number(process.env.MODEL_PROBABILITY_GATE_TUNER_INTERVAL_MS || '120000'),
);
export const MODEL_PROBABILITY_GATE_TUNER_LOOKBACK_MS = Math.max(
    300_000,
    Number(process.env.MODEL_PROBABILITY_GATE_TUNER_LOOKBACK_MS || '21600000'),
);
export const MODEL_PROBABILITY_GATE_COUNTERFACTUAL_WINDOW_MS = Math.max(
    30_000,
    Number(process.env.MODEL_PROBABILITY_GATE_COUNTERFACTUAL_WINDOW_MS || '600000'),
);
export const MODEL_PROBABILITY_GATE_TUNER_MIN_SAMPLES = Math.max(
    20,
    Number(process.env.MODEL_PROBABILITY_GATE_TUNER_MIN_SAMPLES || '60'),
);
export const MODEL_PROBABILITY_GATE_TUNER_STEP = Math.min(
    0.05,
    Math.max(0.001, Number(process.env.MODEL_PROBABILITY_GATE_TUNER_STEP || '0.01')),
);
export const MODEL_PROBABILITY_GATE_TUNER_MAX_DRIFT = Math.min(
    0.25,
    Math.max(0.01, Number(process.env.MODEL_PROBABILITY_GATE_TUNER_MAX_DRIFT || '0.08')),
);

// ── ML Trainer ──────────────────────────────────────────────────────
export const MODEL_TRAINER_ENABLED = process.env.MODEL_TRAINER_ENABLED !== 'false';
export const MODEL_TRAINER_INTERVAL_MS = Math.max(30_000, Number(process.env.MODEL_TRAINER_INTERVAL_MS || '120000'));
export const MODEL_TRAINER_MIN_LABELED_ROWS = Math.max(40, Number(process.env.MODEL_TRAINER_MIN_LABELED_ROWS || '80'));
export const MODEL_TRAINER_MIN_NEW_LABELS = Math.max(1, Number(process.env.MODEL_TRAINER_MIN_NEW_LABELS || '8'));
export const MODEL_TRAINER_MAX_ROWS = Math.max(MODEL_TRAINER_MIN_LABELED_ROWS, Number(process.env.MODEL_TRAINER_MAX_ROWS || '3000'));
export const MODEL_TRAINER_SPLITS = Math.max(3, Number(process.env.MODEL_TRAINER_SPLITS || '5'));
export const MODEL_TRAINER_PURGE_ROWS = Math.max(8, Number(process.env.MODEL_TRAINER_PURGE_ROWS || '24'));
export const MODEL_TRAINER_EMBARGO_ROWS = Math.max(8, Number(process.env.MODEL_TRAINER_EMBARGO_ROWS || '24'));
export const MODEL_PREDICTION_TRACE_MAX = Math.max(5_000, Number(process.env.MODEL_PREDICTION_TRACE_MAX || '100000'));
export const MODEL_INFERENCE_MAX_TRACKED_ROWS = Math.max(
    1_000,
    Number(process.env.MODEL_INFERENCE_MAX_TRACKED_ROWS || '6000'),
);

// ── File paths (depend on __dirname of the original index.ts) ───────
// These are re-evaluated when imported — the BACKEND_APP_ROOT is
// resolved relative to config/ (one level up from config/ = src/,
// one more = apps/backend/).
export const BACKEND_APP_ROOT = path.resolve(__dirname, '../..');
export const FEATURE_REGISTRY_EVENT_LOG_PATH = path.resolve(
    BACKEND_APP_ROOT,
    process.env.FEATURE_REGISTRY_EVENT_LOG_PATH || 'logs/feature_registry_events.jsonl',
);
export const FEATURE_DATASET_MANIFEST_PATH = path.resolve(
    BACKEND_APP_ROOT,
    process.env.FEATURE_DATASET_MANIFEST_PATH || '../../reports/feature_dataset_manifest.json',
);
export const SIGNAL_MODEL_REPORT_PATH = path.resolve(
    BACKEND_APP_ROOT,
    process.env.SIGNAL_MODEL_REPORT_PATH || '../../reports/models/signal_model_report.json',
);
export const SIGNAL_MODEL_ARTIFACT_PATH = path.resolve(
    BACKEND_APP_ROOT,
    process.env.SIGNAL_MODEL_ARTIFACT_PATH || '../../reports/models/signal_model_latest.json',
);

// ── Runtime module catalog ──────────────────────────────────────────
export const BUILD_ACTIVE_PHASE: RuntimePhase = 'PHASE_1';
export const RUNTIME_OFFLINE_MULTIPLIER = 3;
export const RUNTIME_MODULE_CATALOG: Array<Omit<RuntimeModuleState, 'heartbeat_ms' | 'events' | 'last_detail'>> = [
    {
        id: 'MARKET_DATA_PIPELINE',
        label: 'Market Data Pipeline',
        phase: 'PHASE_1',
        health: 'STANDBY',
        expected_interval_ms: 5_000,
    },
    {
        id: 'SCAN_INGEST',
        label: 'Scanner Ingest',
        phase: 'PHASE_1',
        health: 'STANDBY',
        expected_interval_ms: 5_000,
    },
    {
        id: 'INTELLIGENCE_GATE',
        label: 'Intelligence Gate',
        phase: 'PHASE_1',
        health: 'STANDBY',
        expected_interval_ms: 10_000,
    },
    {
        id: 'RISK_ALLOCATOR',
        label: 'Risk Allocator',
        phase: 'PHASE_1',
        health: 'STANDBY',
        expected_interval_ms: 0,
    },
    {
        id: 'PNL_LEDGER',
        label: 'PnL Ledger',
        phase: 'PHASE_1',
        health: 'STANDBY',
        expected_interval_ms: 0,
    },
    {
        id: 'SETTLEMENT_ENGINE',
        label: 'Settlement Engine',
        phase: 'PHASE_1',
        health: 'STANDBY',
        expected_interval_ms: 60_000,
    },
    {
        id: 'EXECUTION_PREFLIGHT',
        label: 'Execution Preflight',
        phase: 'PHASE_1',
        health: 'STANDBY',
        expected_interval_ms: 10_000,
    },
    {
        id: 'TRADING_MODE_GUARD',
        label: 'Trading Mode Guard',
        phase: 'PHASE_1',
        health: 'ONLINE',
        expected_interval_ms: 0,
    },
    {
        id: 'FEATURE_REGISTRY',
        label: 'Feature Registry',
        phase: 'PHASE_2',
        health: 'STANDBY',
        expected_interval_ms: 0,
    },
    {
        id: 'ML_TRAINER',
        label: 'ML Trainer',
        phase: 'PHASE_2',
        health: 'STANDBY',
        expected_interval_ms: 0,
    },
    {
        id: 'MODEL_INFERENCE',
        label: 'Model Inference',
        phase: 'PHASE_2',
        health: 'STANDBY',
        expected_interval_ms: 0,
    },
    {
        id: 'DRIFT_MONITOR',
        label: 'Drift Monitor',
        phase: 'PHASE_2',
        health: 'STANDBY',
        expected_interval_ms: 0,
    },
    {
        id: 'ENSEMBLE_ROUTER',
        label: 'Ensemble Router',
        phase: 'PHASE_3',
        health: 'STANDBY',
        expected_interval_ms: 0,
    },
    {
        id: 'CROSS_HORIZON_ROUTER',
        label: 'Cross-Horizon Router',
        phase: 'PHASE_3',
        health: 'STANDBY',
        expected_interval_ms: CROSS_HORIZON_ROUTER_INTERVAL_MS,
    },
    {
        id: 'REGIME_ENGINE',
        label: 'Regime Engine',
        phase: 'PHASE_3',
        health: 'STANDBY',
        expected_interval_ms: 0,
    },
    {
        id: 'UNCERTAINTY_GATE',
        label: 'Uncertainty Gate',
        phase: 'PHASE_3',
        health: 'STANDBY',
        expected_interval_ms: 0,
    },
];
