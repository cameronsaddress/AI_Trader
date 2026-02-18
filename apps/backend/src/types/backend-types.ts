/**
 * Shared type definitions for the AI Trader backend.
 * Extracted from index.ts to reduce monolith size and enable reuse.
 */
import { TradingMode } from '../services/HyperliquidExecutor';

// ── Metric taxonomy ─────────────────────────────────────────────────
export type MetricFamily =
    | 'ARBITRAGE_EDGE'
    | 'MOMENTUM'
    | 'FAIR_VALUE'
    | 'MARKET_MAKING'
    | 'ORDER_FLOW'
    | 'FLOW_PRESSURE'
    | 'UNKNOWN';

export type MetricDirection = 'ABSOLUTE' | 'SIGNED';

// ── Risk & control payloads ─────────────────────────────────────────
export type RiskModel = 'FIXED' | 'PERCENT';

export type RiskConfig = {
    model: RiskModel;
    value: number;
    timestamp: number;
};

export type StrategyTogglePayload = {
    id: string;
    active: boolean;
    timestamp?: number;
};

export type TradingModePayload = {
    mode: TradingMode;
    confirmation?: string;
    timestamp?: number;
    live_order_posting_enabled?: boolean;
};

export type SimulationResetPayload = {
    bankroll?: number;
    confirmation?: string;
    timestamp?: number;
    force_defaults?: boolean;
};

// ── Strategy metrics ────────────────────────────────────────────────
export type StrategyMetric = {
    pnl: number;
    daily_trades: number;
    updated_at: number;
};

export type StrategyPerformance = {
    sample_count: number;
    win_count: number;
    loss_count: number;
    ema_return: number;
    downside_ema: number;
    return_var_ema: number;
    base_multiplier: number;
    meta_overlay: number;
    multiplier: number;
    updated_at: number;
};

// ── Runtime health ──────────────────────────────────────────────────
export type RuntimePhase = 'PHASE_1' | 'PHASE_2' | 'PHASE_3';
export type RuntimeHealth = 'ONLINE' | 'DEGRADED' | 'OFFLINE' | 'STANDBY';

export type RuntimeModuleState = {
    id: string;
    label: string;
    phase: RuntimePhase;
    health: RuntimeHealth;
    expected_interval_ms: number;
    heartbeat_ms: number;
    events: number;
    last_detail: string;
};

// ── Scanner / signal types ──────────────────────────────────────────
export type StrategyScanState = {
    strategy: string;
    symbol: string;
    market_key: string;
    timestamp: number;
    passes_threshold: boolean;
    score: number;
    threshold: number;
    reason: string;
    signal_type: string;
    unit: string;
    metric_family: MetricFamily;
    directionality: MetricDirection;
    comparable_group: string;
    meta_features?: Record<string, number>;
};

export type StrategySignalSummary = {
    strategy: string;
    symbol: string;
    market_key: string;
    timestamp: number;
    age_ms: number;
    passes_threshold: boolean;
    score: number;
    threshold: number;
    margin: number;
    normalized_margin: number;
    signal_type: string;
    unit: string;
    metric_family: MetricFamily;
    directionality: MetricDirection;
    comparable_group: string;
    reason: string;
};

export type ComparableGroupSummary = {
    comparable_group: string;
    metric_family: MetricFamily;
    unit: string;
    sample_size: number;
    pass_rate_pct: number;
    mean_margin: number;
    mean_normalized_margin: number;
    strategies: string[];
};

// ── Data integrity ──────────────────────────────────────────────────
export type StrategyQuality = {
    total_scans: number;
    pass_scans: number;
    hold_scans: number;
    threshold_blocks: number;
    spread_blocks: number;
    parity_blocks: number;
    stale_blocks: number;
    risk_blocks: number;
    other_blocks: number;
    last_reason: string;
    updated_at: number;
};

export type DataIntegrityCategory = 'threshold' | 'spread' | 'parity' | 'stale' | 'risk' | 'other';
export type DataIntegrityAlertSeverity = 'WARN' | 'CRITICAL';

export type DataIntegrityCounters = {
    threshold: number;
    spread: number;
    parity: number;
    stale: number;
    risk: number;
    other: number;
};

export type StrategyDataIntegrity = DataIntegrityCounters & {
    total_scans: number;
    hold_scans: number;
    consecutive_holds: number;
    last_category: DataIntegrityCategory | null;
    last_reason: string;
    updated_at: number;
    last_alert_at: number;
};

export type DataIntegrityAlert = {
    id: string;
    strategy: string;
    market_key: string;
    category: DataIntegrityCategory;
    severity: DataIntegrityAlertSeverity;
    reason: string;
    consecutive_holds: number;
    timestamp: number;
};

export type DataIntegrityState = {
    totals: DataIntegrityCounters & {
        total_scans: number;
        hold_scans: number;
    };
    strategies: Record<string, StrategyDataIntegrity>;
    recent_alerts: DataIntegrityAlert[];
    updated_at: number;
};

// ── Cost diagnostics ────────────────────────────────────────────────
export type StrategyCostDiagnostics = {
    trades: number;
    notional_sum: number;
    net_pnl_sum: number;
    gross_return_sum: number;
    gross_return_samples: number;
    net_return_sum: number;
    cost_drag_sum: number;
    cost_drag_samples: number;
    estimated_cost_usd_sum: number;
    missing_cost_fields: number;
    avg_cost_drag_bps: number;
    avg_net_return_bps: number;
    avg_gross_return_bps: number;
    updated_at: number;
};

export type StrategyTradeSample = {
    strategy: string;
    timestamp: number;
    pnl: number;
    notional: number;
    net_return: number;
    gross_return: number | null;
    cost_drag: number | null;
    reason: string;
    label_eligible?: boolean;
    execution_id?: string;
    market_key?: string | null;
};

// ── Walk-forward / governance ───────────────────────────────────────
export type WalkForwardSplit = {
    split: number;
    train_trades: number;
    test_trades: number;
    oos_mean_return: number;
    oos_sharpe: number;
    oos_total_pnl: number;
    oos_max_drawdown: number;
    oos_win_rate: number;
};

export type WalkForwardSummary = {
    eligible: boolean;
    splits: WalkForwardSplit[];
    oos_splits: number;
    pass_splits: number;
    pass_rate_pct: number;
    avg_oos_return: number;
    avg_oos_sharpe: number;
    worst_oos_drawdown: number;
};

export type StrategyGovernanceDecision = {
    strategy: string;
    action: 'PROMOTE' | 'HOLD' | 'DEMOTE_DISABLE';
    reason: string;
    trades: number;
    net_pnl: number;
    avg_return: number;
    sharpe: number;
    win_rate_pct: number;
    cost_drag_bps: number;
    walk_forward: WalkForwardSummary;
    confidence: number;
    score: number;
    timestamp: number;
    autopilot_applied: boolean;
    multiplier_before: number;
    multiplier_after: number;
    enabled_before: boolean;
    enabled_after: boolean;
};

export type GovernanceState = {
    autopilot_enabled: boolean;
    autopilot_effective: boolean;
    trading_mode: TradingMode;
    interval_ms: number;
    updated_at: number;
    decisions: Record<string, StrategyGovernanceDecision>;
    audit: StrategyGovernanceDecision[];
};

// ── Feature registry / ML ───────────────────────────────────────────
export type FeatureSnapshot = {
    id: string;
    strategy: string;
    market_key: string;
    timestamp: number;
    signal_type: string;
    metric_family: MetricFamily;
    features: Record<string, number>;
    label_net_return: number | null;
    label_pnl: number | null;
    label_timestamp: number | null;
    label_source: string | null;
};

export type FeatureRegistrySummary = {
    rows: number;
    labeled_rows: number;
    unlabeled_rows: number;
    leakage_violations: number;
    by_strategy: Record<string, {
        rows: number;
        labeled_rows: number;
        unlabeled_rows: number;
    }>;
    latest_rows: FeatureSnapshot[];
    updated_at: number;
};

export type RegimeLabel = 'TREND' | 'MEAN_REVERT' | 'LOW_LIQUIDITY' | 'CHOP' | 'UNKNOWN';

export type MetaControllerState = {
    enabled: boolean;
    advisory_only: boolean;
    regime: RegimeLabel;
    confidence: number;
    signal_count: number;
    pass_rate_pct: number;
    mean_abs_normalized_margin: number;
    family_scores: Record<string, number>;
    allowed_families: string[];
    strategy_overrides: Record<string, {
        family: string;
        recommended_multiplier: number;
        rationale: string;
    }>;
    updated_at: number;
};

export type MlPipelineStatus = {
    feature_event_log_path: string;
    feature_event_log_rows: number;
    dataset_manifest_path: string;
    dataset_rows: number;
    dataset_labeled_rows: number;
    dataset_feature_count: number;
    dataset_leakage_violations: number;
    model_report_path: string;
    model_eligible: boolean;
    model_rows: number;
    model_feature_count: number;
    model_cv_folds: number;
    model_reason: string | null;
    updated_at: number;
};

export type SignalModelArtifact = {
    eligible: boolean;
    generated_at: string | null;
    feature_columns: string[];
    weights: number[];
    bias: number;
    normalization: {
        means: number[];
        stds: number[];
    };
};

export type ModelInferenceEntry = {
    strategy: string;
    market_key: string;
    timestamp: number;
    probability_positive: number | null;
    probability_gate: number;
    pass_probability_gate: boolean;
    model_loaded: boolean;
    reason: string;
    feature_count: number;
    model_generated_at: string | null;
};

export type ModelInferenceState = {
    model_path: string;
    model_loaded: boolean;
    model_generated_at: string | null;
    tracked_rows: number;
    latest: Record<string, ModelInferenceEntry>;
    updated_at: number;
};

export type ModelDriftPerStrategy = {
    sample_count: number;
    brier_ema: number;
    logloss_ema: number;
    accuracy_pct: number;
    updated_at: number;
};

export type ModelDriftStatus = 'HEALTHY' | 'WARN' | 'CRITICAL';

export type ModelDriftState = {
    status: ModelDriftStatus;
    sample_count: number;
    brier_ema: number;
    logloss_ema: number;
    calibration_error_ema: number;
    accuracy_pct: number;
    gate_enabled: boolean;
    gate_enforcing: boolean;
    gate_disabled_until: number;
    issues: string[];
    by_strategy: Record<string, ModelDriftPerStrategy>;
    updated_at: number;
};

export type ModelPredictionTrace = {
    row_id: string;
    strategy: string;
    market_key: string;
    timestamp: number;
    probability_positive: number | null;
    probability_gate: number;
    model_loaded: boolean;
};

// ── Execution trace ─────────────────────────────────────────────────
export type ExecutionTraceEventType =
    | 'EXECUTION_EVENT'
    | 'INTELLIGENCE_GATE'
    | 'MODEL_GATE'
    | 'PREFLIGHT'
    | 'LIVE_EXECUTION'
    | 'PNL';

export type ExecutionTraceEvent = {
    type: ExecutionTraceEventType;
    timestamp: number;
    payload: unknown;
};

export type ExecutionTrace = {
    execution_id: string;
    strategy: string | null;
    market_key: string | null;
    created_at: number;
    updated_at: number;
    scan_snapshot: StrategyScanState | null;
    events: ExecutionTraceEvent[];
};
