import React from 'react';
import { AlertTriangle, RotateCcw } from 'lucide-react';
import { DashboardLayout } from '../components/layout/DashboardLayout';
import { ArbitrageScannerWidget } from '../components/widgets/ArbitrageScannerWidget';
import { StrategyControlWidget } from '../components/widgets/StrategyControlWidget';
import { useSocket } from '../context/SocketContext';

type TradingMode = 'PAPER' | 'LIVE';

type StatsState = {
    latency: string;
    signals: number;
    markets: number;
    scannerAlive: boolean;
    bankroll: number;
    availableCash: number;
    reservedCapital: number;
    capitalUtilizationPct: number;
    realizedPnl: number;
};

type LedgerHealthStatus = 'HEALTHY' | 'WARN' | 'CRITICAL';

type LedgerConcentrationEntry = {
    id: string;
    reserved: number;
    share_pct: number;
};

type LedgerHealthState = {
    status: LedgerHealthStatus;
    issues: string[];
    checked_at: number;
    ledger: {
        cash: number;
        reserved: number;
        realized_pnl: number;
        equity: number;
        utilization_pct: number;
    };
    caps: {
        strategy_pct: number;
        family_pct: number;
        utilization_pct: number;
    };
    strategy_reserved_total: number;
    family_reserved_total: number;
    reserved_gap_vs_strategy: number;
    reserved_gap_vs_family: number;
    top_strategy_exposure: LedgerConcentrationEntry[];
    top_family_exposure: LedgerConcentrationEntry[];
};

type RuntimePhase = 'PHASE_1' | 'PHASE_2' | 'PHASE_3';
type RuntimeHealth = 'ONLINE' | 'DEGRADED' | 'OFFLINE' | 'STANDBY';

type RuntimeModuleState = {
    id: string;
    label: string;
    phase: RuntimePhase;
    health: RuntimeHealth;
    expected_interval_ms: number;
    heartbeat_ms: number;
    events: number;
    last_detail: string;
};

type RuntimeStatusState = {
    phase: RuntimePhase;
    modules: RuntimeModuleState[];
    timestamp: number;
};

type StrategySignalState = {
    strategy: string;
    symbol: string;
    market_key: string;
    timestamp: number;
    age_ms: number;
    passes_threshold: boolean;
    score: number;
    threshold: number;
    unit: string;
    signal_type: string;
    metric_family: string;
    reason: string;
};

type ComparableGroupState = {
    comparable_group: string;
    metric_family: string;
    unit: string;
    sample_size: number;
    pass_rate_pct: number;
    mean_margin: number;
    mean_normalized_margin: number;
    strategies: string[];
};

type StrategyAllocatorEntry = {
    multiplier: number;
    sample_count: number;
    ema_return: number;
    updated_at: number;
};

type StrategyQualityEntry = {
    total_scans: number;
    pass_scans: number;
    hold_scans: number;
    threshold_blocks: number;
    spread_blocks: number;
    parity_blocks: number;
    stale_blocks: number;
    risk_blocks: number;
    other_blocks: number;
    pass_rate_pct: number;
    last_reason: string;
    updated_at: number;
};

type DataIntegrityCategory = 'threshold' | 'spread' | 'parity' | 'stale' | 'risk' | 'other';
type DataIntegritySeverity = 'WARN' | 'CRITICAL';

type StrategyDataIntegrityEntry = {
    total_scans: number;
    hold_scans: number;
    threshold: number;
    spread: number;
    parity: number;
    stale: number;
    risk: number;
    other: number;
    consecutive_holds: number;
    last_category: DataIntegrityCategory | null;
    last_reason: string;
    updated_at: number;
    last_alert_at: number;
};

type DataIntegrityAlert = {
    id: string;
    strategy: string;
    market_key: string;
    category: DataIntegrityCategory;
    severity: DataIntegritySeverity;
    reason: string;
    consecutive_holds: number;
    timestamp: number;
};

type DataIntegrityState = {
    totals: {
        total_scans: number;
        hold_scans: number;
        threshold: number;
        spread: number;
        parity: number;
        stale: number;
        risk: number;
        other: number;
    };
    strategies: Record<string, StrategyDataIntegrityEntry>;
    recent_alerts: DataIntegrityAlert[];
    updated_at: number;
};

type StrategyCostDiagnosticsEntry = {
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

type WalkForwardSummary = {
    eligible: boolean;
    oos_splits: number;
    pass_splits: number;
    pass_rate_pct: number;
    avg_oos_return: number;
    avg_oos_sharpe: number;
    worst_oos_drawdown: number;
};

type StrategyGovernanceDecision = {
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

type GovernanceState = {
    autopilot_enabled: boolean;
    autopilot_effective: boolean;
    trading_mode: TradingMode;
    interval_ms: number;
    updated_at: number;
    decisions: Record<string, StrategyGovernanceDecision>;
    audit: StrategyGovernanceDecision[];
};

type FeatureRegistrySummary = {
    rows: number;
    labeled_rows: number;
    unlabeled_rows: number;
    leakage_violations: number;
    by_strategy: Record<string, {
        rows: number;
        labeled_rows: number;
        unlabeled_rows: number;
    }>;
    latest_rows: Array<{
        id: string;
        strategy: string;
        market_key: string;
        timestamp: number;
        signal_type: string;
        metric_family: string;
        label_net_return: number | null;
        label_pnl: number | null;
        label_timestamp: number | null;
    }>;
    updated_at: number;
};

type RegimeLabel = 'TREND' | 'MEAN_REVERT' | 'LOW_LIQUIDITY' | 'CHOP' | 'UNKNOWN';

type MetaControllerState = {
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

type MlPipelineState = {
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

type ModelInferenceEntry = {
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

type ModelInferenceState = {
    model_path: string;
    model_loaded: boolean;
    model_generated_at: string | null;
    tracked_rows: number;
    latest: Record<string, ModelInferenceEntry>;
    updated_at: number;
};

type ModelDriftPerStrategy = {
    sample_count: number;
    brier_ema: number;
    logloss_ema: number;
    accuracy_pct: number;
    updated_at: number;
};

type ModelDriftState = {
    status: 'HEALTHY' | 'WARN' | 'CRITICAL';
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

type IntelligenceBlockEvent = {
    strategy: string;
    market_key: string;
    reason: string;
    timestamp: number;
};

type SystemHealthEntry = {
    name: string;
    status: string;
    load?: string;
};

type ExecutionLog = {
    timestamp: number;
    side: string;
    market: string;
    price: string;
    size: string;
    execution_id?: string;
};

type ExecutionTraceEvent = {
    type: string;
    timestamp: number;
    payload: unknown;
};

type ExecutionTrace = {
    execution_id: string;
    strategy: string | null;
    market_key: string | null;
    created_at: number;
    updated_at: number;
    scan_snapshot: unknown | null;
    events: ExecutionTraceEvent[];
};

type StrategyPnlPayload = {
    strategy?: string;
    pnl?: number;
    timestamp?: number;
    bankroll?: number;
    details?: {
        action?: string;
        exit?: number;
    };
};

type TradingModePayload = {
    mode?: TradingMode;
    live_order_posting_enabled?: boolean;
};

type SimulationResetEvent = {
    bankroll?: number;
};

type StrategyLiveExecutionEvent = {
    strategy?: string;
    total?: number;
    posted?: number;
    failed?: number;
    dryRun?: boolean;
    ok?: boolean;
    reason?: string;
    timestamp?: number;
};

type IntelligenceAssetScope = 'ALL' | 'BTC' | 'ETH' | 'SOL' | 'OTHER';

const DEFAULT_SIM_BANKROLL = 1000;
const STRATEGY_NAME_MAP: Record<string, string> = {
    BTC_5M: 'BTC 5m Engine',
    BTC_15M: 'BTC Fair Value',
    ETH_15M: 'ETH Fair Value',
    SOL_15M: 'SOL Fair Value',
    CEX_SNIPER: 'CEX Latency Arb',
    SYNDICATE: 'Whale Syndicate',
    ATOMIC_ARB: 'Atomic Pair Arb',
    OBI_SCALPER: 'OBI Scalper',
    GRAPH_ARB: 'Graph Constraint Arb',
    CONVERGENCE_CARRY: 'Convergence Carry',
    MAKER_MM: 'Maker Micro-MM',
};

function inferAssetScope(strategy: string): Exclude<IntelligenceAssetScope, 'ALL'> {
    const upper = (strategy || '').trim().toUpperCase();
    if (upper.startsWith('BTC_') || upper === 'CEX_SNIPER' || upper === 'OBI_SCALPER' || upper === 'ATOMIC_ARB') {
        return 'BTC';
    }
    if (upper.startsWith('ETH_')) {
        return 'ETH';
    }
    if (upper.startsWith('SOL_')) {
        return 'SOL';
    }
    return 'OTHER';
}

function formatUsd(value: number): string {
    return `$${value.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function parseNumber(value: unknown): number | null {
    if (value === null || value === undefined || value === '') {
        return null;
    }
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
}

function parseText(value: unknown): string | null {
    if (typeof value !== 'string') {
        return null;
    }
    const trimmed = value.trim();
    return trimmed.length > 0 ? trimmed : null;
}

function normalizeUnit(value: unknown): string {
    return typeof value === 'string' && value.trim().length > 0
        ? value.trim().toUpperCase()
        : 'RAW';
}

function formatSignalValue(value: number, unit: string): string {
    if (!Number.isFinite(value)) {
        return '--';
    }
    const normalized = normalizeUnit(unit);
    if (normalized === 'RATIO') {
        const sign = value > 0 ? '+' : '';
        return `${sign}${(value * 100).toFixed(2)}%`;
    }
    if (normalized === 'PRICE') {
        const sign = value > 0 ? '+' : '';
        return `${sign}${(value * 100).toFixed(2)}c`;
    }
    return value.toFixed(4);
}

function formatPercent(value: number): string {
    if (!Number.isFinite(value)) {
        return '--';
    }
    return `${value.toFixed(1)}%`;
}

function parseStrategySignal(input: unknown): StrategySignalState | null {
    if (!input || typeof input !== 'object') {
        return null;
    }

    const payload = input as Partial<StrategySignalState>;
    if (typeof payload.strategy !== 'string' || payload.strategy.trim().length === 0) {
        return null;
    }

    return {
        strategy: payload.strategy,
        symbol: typeof payload.symbol === 'string' ? payload.symbol : payload.strategy,
        market_key: typeof payload.market_key === 'string' ? payload.market_key : payload.strategy.toLowerCase(),
        timestamp: parseNumber(payload.timestamp) ?? Date.now(),
        age_ms: parseNumber(payload.age_ms) ?? 0,
        passes_threshold: payload.passes_threshold === true,
        score: parseNumber(payload.score) ?? 0,
        threshold: parseNumber(payload.threshold) ?? 0,
        unit: normalizeUnit(payload.unit),
        signal_type: typeof payload.signal_type === 'string' ? payload.signal_type : 'UNKNOWN',
        metric_family: typeof payload.metric_family === 'string' ? payload.metric_family : 'UNKNOWN',
        reason: typeof payload.reason === 'string' ? payload.reason : '',
    };
}

function parseComparableGroup(input: unknown): ComparableGroupState | null {
    if (!input || typeof input !== 'object') {
        return null;
    }

    const payload = input as Partial<ComparableGroupState>;
    if (typeof payload.comparable_group !== 'string' || payload.comparable_group.trim().length === 0) {
        return null;
    }

    return {
        comparable_group: payload.comparable_group,
        metric_family: typeof payload.metric_family === 'string' ? payload.metric_family : 'UNKNOWN',
        unit: normalizeUnit(payload.unit),
        sample_size: parseNumber(payload.sample_size) ?? 0,
        pass_rate_pct: parseNumber(payload.pass_rate_pct) ?? 0,
        mean_margin: parseNumber(payload.mean_margin) ?? 0,
        mean_normalized_margin: parseNumber(payload.mean_normalized_margin) ?? 0,
        strategies: Array.isArray(payload.strategies)
            ? payload.strategies.filter((entry): entry is string => typeof entry === 'string')
            : [],
    };
}

function parseAllocatorMap(input: unknown): Record<string, StrategyAllocatorEntry> {
    if (!input || typeof input !== 'object') {
        return {};
    }

    const parsed: Record<string, StrategyAllocatorEntry> = {};
    for (const [strategy, value] of Object.entries(input as Record<string, unknown>)) {
        if (!value || typeof value !== 'object') {
            continue;
        }
        const raw = value as Partial<StrategyAllocatorEntry>;
        parsed[strategy] = {
            multiplier: parseNumber(raw.multiplier) ?? 1,
            sample_count: parseNumber(raw.sample_count) ?? 0,
            ema_return: parseNumber(raw.ema_return) ?? 0,
            updated_at: parseNumber(raw.updated_at) ?? Date.now(),
        };
    }
    return parsed;
}

function parseRuntimeModule(input: unknown): RuntimeModuleState | null {
    if (!input || typeof input !== 'object') {
        return null;
    }
    const payload = input as Partial<RuntimeModuleState>;
    const phase = payload.phase;
    const health = payload.health;
    if (
        typeof payload.id !== 'string'
        || typeof payload.label !== 'string'
        || (phase !== 'PHASE_1' && phase !== 'PHASE_2' && phase !== 'PHASE_3')
        || (health !== 'ONLINE' && health !== 'DEGRADED' && health !== 'OFFLINE' && health !== 'STANDBY')
    ) {
        return null;
    }

    return {
        id: payload.id,
        label: payload.label,
        phase,
        health,
        expected_interval_ms: parseNumber(payload.expected_interval_ms) ?? 0,
        heartbeat_ms: parseNumber(payload.heartbeat_ms) ?? 0,
        events: parseNumber(payload.events) ?? 0,
        last_detail: typeof payload.last_detail === 'string' ? payload.last_detail : '',
    };
}

function parseRuntimeStatus(input: unknown): RuntimeStatusState | null {
    if (!input || typeof input !== 'object') {
        return null;
    }
    const payload = input as Partial<RuntimeStatusState>;
    const phase = payload.phase;
    if (phase !== 'PHASE_1' && phase !== 'PHASE_2' && phase !== 'PHASE_3') {
        return null;
    }

    const modules = Array.isArray(payload.modules)
        ? payload.modules
            .map((module) => parseRuntimeModule(module))
            .filter((module): module is RuntimeModuleState => module !== null)
        : [];

    return {
        phase,
        modules,
        timestamp: parseNumber(payload.timestamp) ?? Date.now(),
    };
}

function parseQualityMap(input: unknown): Record<string, StrategyQualityEntry> {
    if (!input || typeof input !== 'object') {
        return {};
    }

    const parsed: Record<string, StrategyQualityEntry> = {};
    for (const [strategy, value] of Object.entries(input as Record<string, unknown>)) {
        if (!value || typeof value !== 'object') {
            continue;
        }
        const raw = value as Partial<StrategyQualityEntry>;
        parsed[strategy] = {
            total_scans: parseNumber(raw.total_scans) ?? 0,
            pass_scans: parseNumber(raw.pass_scans) ?? 0,
            hold_scans: parseNumber(raw.hold_scans) ?? 0,
            threshold_blocks: parseNumber(raw.threshold_blocks) ?? 0,
            spread_blocks: parseNumber(raw.spread_blocks) ?? 0,
            parity_blocks: parseNumber(raw.parity_blocks) ?? 0,
            stale_blocks: parseNumber(raw.stale_blocks) ?? 0,
            risk_blocks: parseNumber(raw.risk_blocks) ?? 0,
            other_blocks: parseNumber(raw.other_blocks) ?? 0,
            pass_rate_pct: parseNumber(raw.pass_rate_pct) ?? 0,
            last_reason: typeof raw.last_reason === 'string' ? raw.last_reason : '',
            updated_at: parseNumber(raw.updated_at) ?? Date.now(),
        };
    }
    return parsed;
}

function parseDataIntegrityCategory(value: unknown): DataIntegrityCategory | null {
    if (typeof value !== 'string') {
        return null;
    }
    const normalized = value.toLowerCase();
    if (
        normalized === 'threshold'
        || normalized === 'spread'
        || normalized === 'parity'
        || normalized === 'stale'
        || normalized === 'risk'
        || normalized === 'other'
    ) {
        return normalized;
    }
    return null;
}

function parseDataIntegritySeverity(value: unknown): DataIntegritySeverity | null {
    if (value === 'WARN' || value === 'CRITICAL') {
        return value;
    }
    return null;
}

function parseDataIntegrityStrategyEntry(input: unknown): StrategyDataIntegrityEntry | null {
    if (!input || typeof input !== 'object') {
        return null;
    }
    const payload = input as Partial<StrategyDataIntegrityEntry>;
    return {
        total_scans: parseNumber(payload.total_scans) ?? 0,
        hold_scans: parseNumber(payload.hold_scans) ?? 0,
        threshold: parseNumber(payload.threshold) ?? 0,
        spread: parseNumber(payload.spread) ?? 0,
        parity: parseNumber(payload.parity) ?? 0,
        stale: parseNumber(payload.stale) ?? 0,
        risk: parseNumber(payload.risk) ?? 0,
        other: parseNumber(payload.other) ?? 0,
        consecutive_holds: parseNumber(payload.consecutive_holds) ?? 0,
        last_category: parseDataIntegrityCategory(payload.last_category),
        last_reason: typeof payload.last_reason === 'string' ? payload.last_reason : '',
        updated_at: parseNumber(payload.updated_at) ?? Date.now(),
        last_alert_at: parseNumber(payload.last_alert_at) ?? 0,
    };
}

function parseDataIntegrityAlert(input: unknown): DataIntegrityAlert | null {
    if (!input || typeof input !== 'object') {
        return null;
    }
    const payload = input as Partial<DataIntegrityAlert>;
    const strategy = typeof payload.strategy === 'string' ? payload.strategy : null;
    const category = parseDataIntegrityCategory(payload.category);
    const severity = parseDataIntegritySeverity(payload.severity);
    if (!strategy || !category || !severity) {
        return null;
    }
    return {
        id: typeof payload.id === 'string' ? payload.id : `${strategy}:${Date.now()}`,
        strategy,
        market_key: typeof payload.market_key === 'string' ? payload.market_key : 'unknown',
        category,
        severity,
        reason: typeof payload.reason === 'string' ? payload.reason : '',
        consecutive_holds: parseNumber(payload.consecutive_holds) ?? 0,
        timestamp: parseNumber(payload.timestamp) ?? Date.now(),
    };
}

function parseDataIntegrityState(input: unknown): DataIntegrityState {
    if (!input || typeof input !== 'object') {
        return {
            totals: {
                total_scans: 0,
                hold_scans: 0,
                threshold: 0,
                spread: 0,
                parity: 0,
                stale: 0,
                risk: 0,
                other: 0,
            },
            strategies: {},
            recent_alerts: [],
            updated_at: 0,
        };
    }
    const payload = input as Partial<DataIntegrityState>;
    const totals = payload.totals && typeof payload.totals === 'object'
        ? payload.totals
        : {};
    const strategiesRaw = payload.strategies && typeof payload.strategies === 'object'
        ? payload.strategies
        : {};
    const strategies: Record<string, StrategyDataIntegrityEntry> = {};
    for (const [strategy, value] of Object.entries(strategiesRaw as Record<string, unknown>)) {
        const parsed = parseDataIntegrityStrategyEntry(value);
        if (parsed) {
            strategies[strategy] = parsed;
        }
    }

    const alerts = Array.isArray(payload.recent_alerts)
        ? payload.recent_alerts
            .map((entry) => parseDataIntegrityAlert(entry))
            .filter((entry): entry is DataIntegrityAlert => entry !== null)
        : [];

    return {
        totals: {
            total_scans: parseNumber((totals as Record<string, unknown>).total_scans) ?? 0,
            hold_scans: parseNumber((totals as Record<string, unknown>).hold_scans) ?? 0,
            threshold: parseNumber((totals as Record<string, unknown>).threshold) ?? 0,
            spread: parseNumber((totals as Record<string, unknown>).spread) ?? 0,
            parity: parseNumber((totals as Record<string, unknown>).parity) ?? 0,
            stale: parseNumber((totals as Record<string, unknown>).stale) ?? 0,
            risk: parseNumber((totals as Record<string, unknown>).risk) ?? 0,
            other: parseNumber((totals as Record<string, unknown>).other) ?? 0,
        },
        strategies,
        recent_alerts: alerts,
        updated_at: parseNumber(payload.updated_at) ?? 0,
    };
}

function parseCostDiagnosticsMap(input: unknown): Record<string, StrategyCostDiagnosticsEntry> {
    if (!input || typeof input !== 'object') {
        return {};
    }
    const parsed: Record<string, StrategyCostDiagnosticsEntry> = {};
    for (const [strategy, value] of Object.entries(input as Record<string, unknown>)) {
        if (!value || typeof value !== 'object') {
            continue;
        }
        const raw = value as Partial<StrategyCostDiagnosticsEntry>;
        parsed[strategy] = {
            trades: parseNumber(raw.trades) ?? 0,
            notional_sum: parseNumber(raw.notional_sum) ?? 0,
            net_pnl_sum: parseNumber(raw.net_pnl_sum) ?? 0,
            gross_return_sum: parseNumber(raw.gross_return_sum) ?? 0,
            gross_return_samples: parseNumber(raw.gross_return_samples) ?? 0,
            net_return_sum: parseNumber(raw.net_return_sum) ?? 0,
            cost_drag_sum: parseNumber(raw.cost_drag_sum) ?? 0,
            cost_drag_samples: parseNumber(raw.cost_drag_samples) ?? 0,
            estimated_cost_usd_sum: parseNumber(raw.estimated_cost_usd_sum) ?? 0,
            missing_cost_fields: parseNumber(raw.missing_cost_fields) ?? 0,
            avg_cost_drag_bps: parseNumber(raw.avg_cost_drag_bps) ?? 0,
            avg_net_return_bps: parseNumber(raw.avg_net_return_bps) ?? 0,
            avg_gross_return_bps: parseNumber(raw.avg_gross_return_bps) ?? 0,
            updated_at: parseNumber(raw.updated_at) ?? 0,
        };
    }
    return parsed;
}

function parseWalkForwardSummary(input: unknown): WalkForwardSummary {
    if (!input || typeof input !== 'object') {
        return {
            eligible: false,
            oos_splits: 0,
            pass_splits: 0,
            pass_rate_pct: 0,
            avg_oos_return: 0,
            avg_oos_sharpe: 0,
            worst_oos_drawdown: 0,
        };
    }
    const payload = input as Partial<WalkForwardSummary>;
    return {
        eligible: payload.eligible === true,
        oos_splits: parseNumber(payload.oos_splits) ?? 0,
        pass_splits: parseNumber(payload.pass_splits) ?? 0,
        pass_rate_pct: parseNumber(payload.pass_rate_pct) ?? 0,
        avg_oos_return: parseNumber(payload.avg_oos_return) ?? 0,
        avg_oos_sharpe: parseNumber(payload.avg_oos_sharpe) ?? 0,
        worst_oos_drawdown: parseNumber(payload.worst_oos_drawdown) ?? 0,
    };
}

function parseGovernanceDecision(input: unknown): StrategyGovernanceDecision | null {
    if (!input || typeof input !== 'object') {
        return null;
    }
    const payload = input as Partial<StrategyGovernanceDecision>;
    if (typeof payload.strategy !== 'string') {
        return null;
    }
    const action = payload.action;
    if (action !== 'PROMOTE' && action !== 'HOLD' && action !== 'DEMOTE_DISABLE') {
        return null;
    }
    return {
        strategy: payload.strategy,
        action,
        reason: typeof payload.reason === 'string' ? payload.reason : '',
        trades: parseNumber(payload.trades) ?? 0,
        net_pnl: parseNumber(payload.net_pnl) ?? 0,
        avg_return: parseNumber(payload.avg_return) ?? 0,
        sharpe: parseNumber(payload.sharpe) ?? 0,
        win_rate_pct: parseNumber(payload.win_rate_pct) ?? 0,
        cost_drag_bps: parseNumber(payload.cost_drag_bps) ?? 0,
        walk_forward: parseWalkForwardSummary(payload.walk_forward),
        confidence: parseNumber(payload.confidence) ?? 0,
        score: parseNumber(payload.score) ?? 0,
        timestamp: parseNumber(payload.timestamp) ?? Date.now(),
        autopilot_applied: payload.autopilot_applied === true,
        multiplier_before: parseNumber(payload.multiplier_before) ?? 1,
        multiplier_after: parseNumber(payload.multiplier_after) ?? 1,
        enabled_before: payload.enabled_before === true,
        enabled_after: payload.enabled_after === true,
    };
}

function parseGovernanceState(input: unknown): GovernanceState {
    if (!input || typeof input !== 'object') {
        return {
            autopilot_enabled: false,
            autopilot_effective: false,
            trading_mode: 'PAPER',
            interval_ms: 0,
            updated_at: 0,
            decisions: {},
            audit: [],
        };
    }

    const payload = input as Partial<GovernanceState>;
    const decisionsRaw = payload.decisions && typeof payload.decisions === 'object'
        ? payload.decisions
        : {};
    const decisions: Record<string, StrategyGovernanceDecision> = {};
    for (const [strategy, value] of Object.entries(decisionsRaw as Record<string, unknown>)) {
        const parsed = parseGovernanceDecision(value);
        if (parsed) {
            decisions[strategy] = parsed;
        }
    }
    const audit = Array.isArray(payload.audit)
        ? payload.audit
            .map((entry) => parseGovernanceDecision(entry))
            .filter((entry): entry is StrategyGovernanceDecision => entry !== null)
        : [];

    return {
        autopilot_enabled: payload.autopilot_enabled === true,
        autopilot_effective: payload.autopilot_effective === true,
        trading_mode: payload.trading_mode === 'LIVE' ? 'LIVE' : 'PAPER',
        interval_ms: parseNumber(payload.interval_ms) ?? 0,
        updated_at: parseNumber(payload.updated_at) ?? 0,
        decisions,
        audit,
    };
}

function parseLedgerConcentrationEntries(input: unknown): LedgerConcentrationEntry[] {
    if (!Array.isArray(input)) {
        return [];
    }
    return input
        .map((entry) => {
            if (!entry || typeof entry !== 'object') {
                return null;
            }
            const payload = entry as Partial<LedgerConcentrationEntry>;
            if (typeof payload.id !== 'string') {
                return null;
            }
            return {
                id: payload.id,
                reserved: parseNumber(payload.reserved) ?? 0,
                share_pct: parseNumber(payload.share_pct) ?? 0,
            };
        })
        .filter((entry): entry is LedgerConcentrationEntry => entry !== null);
}

function parseLedgerHealthState(input: unknown): LedgerHealthState {
    if (!input || typeof input !== 'object') {
        return {
            status: 'HEALTHY',
            issues: [],
            checked_at: 0,
            ledger: {
                cash: DEFAULT_SIM_BANKROLL,
                reserved: 0,
                realized_pnl: 0,
                equity: DEFAULT_SIM_BANKROLL,
                utilization_pct: 0,
            },
            caps: {
                strategy_pct: 35,
                family_pct: 60,
                utilization_pct: 90,
            },
            strategy_reserved_total: 0,
            family_reserved_total: 0,
            reserved_gap_vs_strategy: 0,
            reserved_gap_vs_family: 0,
            top_strategy_exposure: [],
            top_family_exposure: [],
        };
    }
    const payload = input as Partial<LedgerHealthState>;
    const status = payload.status;
    return {
        status: status === 'WARN' || status === 'CRITICAL' ? status : 'HEALTHY',
        issues: Array.isArray(payload.issues)
            ? payload.issues.filter((value): value is string => typeof value === 'string')
            : [],
        checked_at: parseNumber(payload.checked_at) ?? 0,
        ledger: {
            cash: parseNumber(payload.ledger?.cash) ?? DEFAULT_SIM_BANKROLL,
            reserved: parseNumber(payload.ledger?.reserved) ?? 0,
            realized_pnl: parseNumber(payload.ledger?.realized_pnl) ?? 0,
            equity: parseNumber(payload.ledger?.equity) ?? DEFAULT_SIM_BANKROLL,
            utilization_pct: parseNumber(payload.ledger?.utilization_pct) ?? 0,
        },
        caps: {
            strategy_pct: parseNumber(payload.caps?.strategy_pct) ?? 35,
            family_pct: parseNumber(payload.caps?.family_pct) ?? 60,
            utilization_pct: parseNumber(payload.caps?.utilization_pct) ?? 90,
        },
        strategy_reserved_total: parseNumber(payload.strategy_reserved_total) ?? 0,
        family_reserved_total: parseNumber(payload.family_reserved_total) ?? 0,
        reserved_gap_vs_strategy: parseNumber(payload.reserved_gap_vs_strategy) ?? 0,
        reserved_gap_vs_family: parseNumber(payload.reserved_gap_vs_family) ?? 0,
        top_strategy_exposure: parseLedgerConcentrationEntries(payload.top_strategy_exposure),
        top_family_exposure: parseLedgerConcentrationEntries(payload.top_family_exposure),
    };
}

function parseFeatureRegistrySummary(input: unknown): FeatureRegistrySummary {
    if (!input || typeof input !== 'object') {
        return {
            rows: 0,
            labeled_rows: 0,
            unlabeled_rows: 0,
            leakage_violations: 0,
            by_strategy: {},
            latest_rows: [],
            updated_at: 0,
        };
    }
    const payload = input as Partial<FeatureRegistrySummary>;
    const strategyMap: FeatureRegistrySummary['by_strategy'] = {};
    if (payload.by_strategy && typeof payload.by_strategy === 'object') {
        for (const [strategy, value] of Object.entries(payload.by_strategy as Record<string, unknown>)) {
            if (!value || typeof value !== 'object') {
                continue;
            }
            const row = value as { rows?: unknown; labeled_rows?: unknown; unlabeled_rows?: unknown };
            strategyMap[strategy] = {
                rows: parseNumber(row.rows) ?? 0,
                labeled_rows: parseNumber(row.labeled_rows) ?? 0,
                unlabeled_rows: parseNumber(row.unlabeled_rows) ?? 0,
            };
        }
    }
    const latestRows = Array.isArray(payload.latest_rows)
        ? payload.latest_rows
            .map((entry) => {
                if (!entry || typeof entry !== 'object') {
                    return null;
                }
                const row = entry as Record<string, unknown>;
                const strategy = typeof row.strategy === 'string' ? row.strategy : null;
                const id = typeof row.id === 'string' ? row.id : null;
                if (!strategy || !id) {
                    return null;
                }
                return {
                    id,
                    strategy,
                    market_key: typeof row.market_key === 'string' ? row.market_key : '',
                    timestamp: parseNumber(row.timestamp) ?? 0,
                    signal_type: typeof row.signal_type === 'string' ? row.signal_type : 'UNKNOWN',
                    metric_family: typeof row.metric_family === 'string' ? row.metric_family : 'UNKNOWN',
                    label_net_return: parseNumber(row.label_net_return),
                    label_pnl: parseNumber(row.label_pnl),
                    label_timestamp: parseNumber(row.label_timestamp),
                };
            })
            .filter((entry): entry is FeatureRegistrySummary['latest_rows'][number] => entry !== null)
        : [];
    return {
        rows: parseNumber(payload.rows) ?? 0,
        labeled_rows: parseNumber(payload.labeled_rows) ?? 0,
        unlabeled_rows: parseNumber(payload.unlabeled_rows) ?? 0,
        leakage_violations: parseNumber(payload.leakage_violations) ?? 0,
        by_strategy: strategyMap,
        latest_rows: latestRows,
        updated_at: parseNumber(payload.updated_at) ?? 0,
    };
}

function parseMetaControllerState(input: unknown): MetaControllerState {
    if (!input || typeof input !== 'object') {
        return {
            enabled: false,
            advisory_only: true,
            regime: 'UNKNOWN',
            confidence: 0,
            signal_count: 0,
            pass_rate_pct: 0,
            mean_abs_normalized_margin: 0,
            family_scores: {},
            allowed_families: [],
            strategy_overrides: {},
            updated_at: 0,
        };
    }
    const payload = input as Partial<MetaControllerState>;
    const regime = payload.regime;
    const familyScoresRaw = payload.family_scores && typeof payload.family_scores === 'object'
        ? payload.family_scores
        : {};
    const familyScores: Record<string, number> = {};
    for (const [family, score] of Object.entries(familyScoresRaw as Record<string, unknown>)) {
        const parsed = parseNumber(score);
        if (parsed !== null) {
            familyScores[family] = parsed;
        }
    }
    const overridesRaw = payload.strategy_overrides && typeof payload.strategy_overrides === 'object'
        ? payload.strategy_overrides
        : {};
    const strategyOverrides: MetaControllerState['strategy_overrides'] = {};
    for (const [strategy, value] of Object.entries(overridesRaw as Record<string, unknown>)) {
        if (!value || typeof value !== 'object') {
            continue;
        }
        const row = value as {
            family?: unknown;
            recommended_multiplier?: unknown;
            rationale?: unknown;
        };
        strategyOverrides[strategy] = {
            family: typeof row.family === 'string' ? row.family : 'GENERIC',
            recommended_multiplier: parseNumber(row.recommended_multiplier) ?? 1,
            rationale: typeof row.rationale === 'string' ? row.rationale : '',
        };
    }
    return {
        enabled: payload.enabled === true,
        advisory_only: payload.advisory_only !== false,
        regime: regime === 'TREND'
            || regime === 'MEAN_REVERT'
            || regime === 'LOW_LIQUIDITY'
            || regime === 'CHOP'
            ? regime
            : 'UNKNOWN',
        confidence: parseNumber(payload.confidence) ?? 0,
        signal_count: parseNumber(payload.signal_count) ?? 0,
        pass_rate_pct: parseNumber(payload.pass_rate_pct) ?? 0,
        mean_abs_normalized_margin: parseNumber(payload.mean_abs_normalized_margin) ?? 0,
        family_scores: familyScores,
        allowed_families: Array.isArray(payload.allowed_families)
            ? payload.allowed_families.filter((value): value is string => typeof value === 'string')
            : [],
        strategy_overrides: strategyOverrides,
        updated_at: parseNumber(payload.updated_at) ?? 0,
    };
}

function parseMlPipelineState(input: unknown): MlPipelineState {
    if (!input || typeof input !== 'object') {
        return {
            feature_event_log_path: '',
            feature_event_log_rows: 0,
            dataset_manifest_path: '',
            dataset_rows: 0,
            dataset_labeled_rows: 0,
            dataset_feature_count: 0,
            dataset_leakage_violations: 0,
            model_report_path: '',
            model_eligible: false,
            model_rows: 0,
            model_feature_count: 0,
            model_cv_folds: 0,
            model_reason: null,
            updated_at: 0,
        };
    }
    const payload = input as Partial<MlPipelineState>;
    return {
        feature_event_log_path: typeof payload.feature_event_log_path === 'string' ? payload.feature_event_log_path : '',
        feature_event_log_rows: parseNumber(payload.feature_event_log_rows) ?? 0,
        dataset_manifest_path: typeof payload.dataset_manifest_path === 'string' ? payload.dataset_manifest_path : '',
        dataset_rows: parseNumber(payload.dataset_rows) ?? 0,
        dataset_labeled_rows: parseNumber(payload.dataset_labeled_rows) ?? 0,
        dataset_feature_count: parseNumber(payload.dataset_feature_count) ?? 0,
        dataset_leakage_violations: parseNumber(payload.dataset_leakage_violations) ?? 0,
        model_report_path: typeof payload.model_report_path === 'string' ? payload.model_report_path : '',
        model_eligible: payload.model_eligible === true,
        model_rows: parseNumber(payload.model_rows) ?? 0,
        model_feature_count: parseNumber(payload.model_feature_count) ?? 0,
        model_cv_folds: parseNumber(payload.model_cv_folds) ?? 0,
        model_reason: typeof payload.model_reason === 'string' ? payload.model_reason : null,
        updated_at: parseNumber(payload.updated_at) ?? 0,
    };
}

function parseModelInferenceEntry(input: unknown): ModelInferenceEntry | null {
    if (!input || typeof input !== 'object') {
        return null;
    }
    const payload = input as Partial<ModelInferenceEntry>;
    if (typeof payload.strategy !== 'string' || typeof payload.market_key !== 'string') {
        return null;
    }
    return {
        strategy: payload.strategy,
        market_key: payload.market_key,
        timestamp: parseNumber(payload.timestamp) ?? 0,
        probability_positive: parseNumber(payload.probability_positive),
        probability_gate: parseNumber(payload.probability_gate) ?? 0.55,
        pass_probability_gate: payload.pass_probability_gate === true,
        model_loaded: payload.model_loaded === true,
        reason: typeof payload.reason === 'string' ? payload.reason : '',
        feature_count: parseNumber(payload.feature_count) ?? 0,
        model_generated_at: typeof payload.model_generated_at === 'string' ? payload.model_generated_at : null,
    };
}

function parseModelInferenceState(input: unknown): ModelInferenceState {
    if (!input || typeof input !== 'object') {
        return {
            model_path: '',
            model_loaded: false,
            model_generated_at: null,
            tracked_rows: 0,
            latest: {},
            updated_at: 0,
        };
    }
    const payload = input as Partial<ModelInferenceState>;
    const latestRaw = payload.latest && typeof payload.latest === 'object'
        ? payload.latest
        : {};
    const latest: Record<string, ModelInferenceEntry> = {};
    for (const [key, value] of Object.entries(latestRaw as Record<string, unknown>)) {
        const parsed = parseModelInferenceEntry(value);
        if (parsed) {
            latest[key] = parsed;
        }
    }
    return {
        model_path: typeof payload.model_path === 'string' ? payload.model_path : '',
        model_loaded: payload.model_loaded === true,
        model_generated_at: typeof payload.model_generated_at === 'string' ? payload.model_generated_at : null,
        tracked_rows: parseNumber(payload.tracked_rows) ?? Object.keys(latest).length,
        latest,
        updated_at: parseNumber(payload.updated_at) ?? 0,
    };
}

function parseModelDriftState(input: unknown): ModelDriftState {
    if (!input || typeof input !== 'object') {
        return {
            status: 'HEALTHY',
            sample_count: 0,
            brier_ema: 0,
            logloss_ema: 0,
            calibration_error_ema: 0,
            accuracy_pct: 0,
            gate_enabled: false,
            gate_enforcing: false,
            gate_disabled_until: 0,
            issues: [],
            by_strategy: {},
            updated_at: 0,
        };
    }
    const payload = input as Partial<ModelDriftState>;
    const status = payload.status;
    const byStrategyRaw = payload.by_strategy && typeof payload.by_strategy === 'object'
        ? payload.by_strategy
        : {};
    const byStrategy: Record<string, ModelDriftPerStrategy> = {};
    for (const [strategy, value] of Object.entries(byStrategyRaw as Record<string, unknown>)) {
        if (!value || typeof value !== 'object') {
            continue;
        }
        const row = value as Partial<ModelDriftPerStrategy>;
        byStrategy[strategy] = {
            sample_count: parseNumber(row.sample_count) ?? 0,
            brier_ema: parseNumber(row.brier_ema) ?? 0,
            logloss_ema: parseNumber(row.logloss_ema) ?? 0,
            accuracy_pct: parseNumber(row.accuracy_pct) ?? 0,
            updated_at: parseNumber(row.updated_at) ?? 0,
        };
    }
    return {
        status: status === 'WARN' || status === 'CRITICAL' ? status : 'HEALTHY',
        sample_count: parseNumber(payload.sample_count) ?? 0,
        brier_ema: parseNumber(payload.brier_ema) ?? 0,
        logloss_ema: parseNumber(payload.logloss_ema) ?? 0,
        calibration_error_ema: parseNumber(payload.calibration_error_ema) ?? 0,
        accuracy_pct: parseNumber(payload.accuracy_pct) ?? 0,
        gate_enabled: payload.gate_enabled === true,
        gate_enforcing: payload.gate_enforcing === true,
        gate_disabled_until: parseNumber(payload.gate_disabled_until) ?? 0,
        issues: Array.isArray(payload.issues)
            ? payload.issues.filter((value): value is string => typeof value === 'string')
            : [],
        by_strategy: byStrategy,
        updated_at: parseNumber(payload.updated_at) ?? 0,
    };
}

function dominantBlockLabel(entry: StrategyQualityEntry): string {
    const buckets: Array<{ label: string; value: number }> = [
        { label: 'threshold', value: entry.threshold_blocks },
        { label: 'spread', value: entry.spread_blocks },
        { label: 'parity', value: entry.parity_blocks },
        { label: 'stale', value: entry.stale_blocks },
        { label: 'risk', value: entry.risk_blocks },
        { label: 'other', value: entry.other_blocks },
    ];
    const top = buckets.sort((a, b) => b.value - a.value)[0];
    if (!top || top.value <= 0) {
        return 'none';
    }
    return top.label;
}

function runtimeHealthClass(health: RuntimeHealth): string {
    if (health === 'ONLINE') {
        return 'text-emerald-300';
    }
    if (health === 'DEGRADED') {
        return 'text-amber-300';
    }
    if (health === 'OFFLINE') {
        return 'text-rose-300';
    }
    return 'text-gray-400';
}

function formatAgeMs(timestamp: number, now: number): string {
    if (!Number.isFinite(timestamp) || timestamp <= 0) {
        return '--';
    }
    return `${Math.max(0, now - timestamp)}ms`;
}

function toExecutionLog(input: unknown): ExecutionLog {
    if (!input || typeof input !== 'object') {
        return {
            timestamp: Date.now(),
            side: 'INFO',
            market: 'SYSTEM',
            price: 'UNKNOWN',
            size: '--',
            execution_id: undefined,
        };
    }

    const payload = input as Record<string, unknown>;
    const ts = parseNumber(payload.timestamp) ?? Date.now();
    const details = (payload.details && typeof payload.details === 'object' && !Array.isArray(payload.details))
        ? payload.details as Record<string, unknown>
        : null;
    const executionId = parseText(payload.execution_id)
        || parseText(payload.executionId)
        || parseText(details?.execution_id)
        || parseText(details?.executionId)
        || undefined;

    return {
        timestamp: ts,
        side: typeof payload.side === 'string' ? payload.side : 'INFO',
        market: typeof payload.market === 'string' ? payload.market : 'SYSTEM',
        price: typeof payload.price === 'string' ? payload.price : String(payload.price ?? '---'),
        size: typeof payload.size === 'string' ? payload.size : String(payload.size ?? '--'),
        execution_id: executionId,
    };
}

export const PolymarketPage: React.FC = () => {
    const { socket } = useSocket();
    const [stats, setStats] = React.useState<StatsState>({
        latency: '--',
        signals: 0,
        markets: 0,
        scannerAlive: false,
        bankroll: DEFAULT_SIM_BANKROLL,
        availableCash: DEFAULT_SIM_BANKROLL,
        reservedCapital: 0,
        capitalUtilizationPct: 0,
        realizedPnl: 0,
    });
    const [execLogs, setExecLogs] = React.useState<ExecutionLog[]>([]);
    const [traceExecutionId, setTraceExecutionId] = React.useState<string | null>(null);
    const [executionTrace, setExecutionTrace] = React.useState<ExecutionTrace | null>(null);
    const [executionTraceError, setExecutionTraceError] = React.useState<string | null>(null);
    const [executionTraceLoading, setExecutionTraceLoading] = React.useState(false);
    const [strategySignals, setStrategySignals] = React.useState<StrategySignalState[]>([]);
    const [comparableGroups, setComparableGroups] = React.useState<ComparableGroupState[]>([]);
    const [strategyAllocator, setStrategyAllocator] = React.useState<Record<string, StrategyAllocatorEntry>>({});
    const [strategyQuality, setStrategyQuality] = React.useState<Record<string, StrategyQualityEntry>>({});
    const [strategyCostDiagnostics, setStrategyCostDiagnostics] = React.useState<Record<string, StrategyCostDiagnosticsEntry>>({});
    const [dataIntegrity, setDataIntegrity] = React.useState<DataIntegrityState>({
        totals: {
            total_scans: 0,
            hold_scans: 0,
            threshold: 0,
            spread: 0,
            parity: 0,
            stale: 0,
            risk: 0,
            other: 0,
        },
        strategies: {},
        recent_alerts: [],
        updated_at: 0,
    });
    const [governance, setGovernance] = React.useState<GovernanceState>({
        autopilot_enabled: false,
        autopilot_effective: false,
        trading_mode: 'PAPER',
        interval_ms: 0,
        updated_at: 0,
        decisions: {},
        audit: [],
    });
    const [ledgerHealth, setLedgerHealth] = React.useState<LedgerHealthState>({
        status: 'HEALTHY',
        issues: [],
        checked_at: 0,
        ledger: {
            cash: DEFAULT_SIM_BANKROLL,
            reserved: 0,
            realized_pnl: 0,
            equity: DEFAULT_SIM_BANKROLL,
            utilization_pct: 0,
        },
        caps: {
            strategy_pct: 35,
            family_pct: 60,
            utilization_pct: 90,
        },
        strategy_reserved_total: 0,
        family_reserved_total: 0,
        reserved_gap_vs_strategy: 0,
        reserved_gap_vs_family: 0,
        top_strategy_exposure: [],
        top_family_exposure: [],
    });
    const [featureRegistry, setFeatureRegistry] = React.useState<FeatureRegistrySummary>({
        rows: 0,
        labeled_rows: 0,
        unlabeled_rows: 0,
        leakage_violations: 0,
        by_strategy: {},
        latest_rows: [],
        updated_at: 0,
    });
    const [metaController, setMetaController] = React.useState<MetaControllerState>({
        enabled: false,
        advisory_only: true,
        regime: 'UNKNOWN',
        confidence: 0,
        signal_count: 0,
        pass_rate_pct: 0,
        mean_abs_normalized_margin: 0,
        family_scores: {},
        allowed_families: [],
        strategy_overrides: {},
        updated_at: 0,
    });
    const [mlPipeline, setMlPipeline] = React.useState<MlPipelineState>({
        feature_event_log_path: '',
        feature_event_log_rows: 0,
        dataset_manifest_path: '',
        dataset_rows: 0,
        dataset_labeled_rows: 0,
        dataset_feature_count: 0,
        dataset_leakage_violations: 0,
        model_report_path: '',
        model_eligible: false,
        model_rows: 0,
        model_feature_count: 0,
        model_cv_folds: 0,
        model_reason: null,
        updated_at: 0,
    });
    const [modelInference, setModelInference] = React.useState<ModelInferenceState>({
        model_path: '',
        model_loaded: false,
        model_generated_at: null,
        tracked_rows: 0,
        latest: {},
        updated_at: 0,
    });
    const [modelDrift, setModelDrift] = React.useState<ModelDriftState>({
        status: 'HEALTHY',
        sample_count: 0,
        brier_ema: 0,
        logloss_ema: 0,
        calibration_error_ema: 0,
        accuracy_pct: 0,
        gate_enabled: false,
        gate_enforcing: false,
        gate_disabled_until: 0,
        issues: [],
        by_strategy: {},
        updated_at: 0,
    });
    const [intelligenceBlocks, setIntelligenceBlocks] = React.useState<IntelligenceBlockEvent[]>([]);
    const [tradingMode, setTradingMode] = React.useState<TradingMode>('PAPER');
    const [liveOrderPostingEnabled, setLiveOrderPostingEnabled] = React.useState(false);
    const [lastLiveExecution, setLastLiveExecution] = React.useState<StrategyLiveExecutionEvent | null>(null);
    const [runtimeStatus, setRuntimeStatus] = React.useState<RuntimeStatusState>({
        phase: 'PHASE_1',
        modules: [],
        timestamp: 0,
    });
    const [resetError, setResetError] = React.useState<string | null>(null);
    const [showResetConfirm, setShowResetConfirm] = React.useState(false);
    const [resetText, setResetText] = React.useState('');
    const [resetForceDefaults, setResetForceDefaults] = React.useState(false);

    const isPaperMode = tradingMode === 'PAPER';
    const canConfirmReset = resetText.trim().toUpperCase() === 'RESET';
    const [intelligenceAssetScope, setIntelligenceAssetScope] = React.useState<IntelligenceAssetScope>('ALL');
    const [intelligenceFamilyScope, setIntelligenceFamilyScope] = React.useState<string>('ALL');

    const intelligenceFamilyOptions = React.useMemo(() => {
        const families = new Set<string>();
        for (const signal of strategySignals) {
            families.add(signal.metric_family);
        }
        return ['ALL', ...Array.from(families).sort()];
    }, [strategySignals]);

    const scopedStrategySignals = React.useMemo(() => {
        return strategySignals.filter((signal) => {
            if (intelligenceAssetScope !== 'ALL' && inferAssetScope(signal.strategy) !== intelligenceAssetScope) {
                return false;
            }
            if (intelligenceFamilyScope !== 'ALL' && signal.metric_family !== intelligenceFamilyScope) {
                return false;
            }
            return true;
        });
    }, [intelligenceAssetScope, intelligenceFamilyScope, strategySignals]);

    const scopedPassFractionPct = scopedStrategySignals.length > 0
        ? (scopedStrategySignals.filter((signal) => signal.passes_threshold).length / scopedStrategySignals.length) * 100
        : 0;
    const intelligenceScopeLabel = React.useMemo(() => {
        const parts: string[] = [];
        if (intelligenceAssetScope !== 'ALL') {
            parts.push(intelligenceAssetScope);
        }
        if (intelligenceFamilyScope !== 'ALL') {
            parts.push(intelligenceFamilyScope);
        }
        return parts.length > 0 ? parts.join(' \u00b7 ') : 'ALL';
    }, [intelligenceAssetScope, intelligenceFamilyScope]);
    const medianSignalAgeMs = React.useMemo(() => {
        if (scopedStrategySignals.length === 0) {
            return 0;
        }
        const sorted = [...scopedStrategySignals].map((signal) => signal.age_ms).sort((a, b) => a - b);
        return sorted[Math.floor(sorted.length / 2)] ?? 0;
    }, [scopedStrategySignals]);
    const metricFamilyCoverage = React.useMemo(
        () => new Set(scopedStrategySignals.map((signal) => signal.metric_family)).size,
        [scopedStrategySignals],
    );
    const topComparableGroups = React.useMemo(
        () => [...comparableGroups]
            .filter((group) => group.sample_size > 0)
            .sort((a, b) => Math.abs(b.mean_normalized_margin) - Math.abs(a.mean_normalized_margin))
            .slice(0, 4),
        [comparableGroups],
    );
    const allocatorRows = React.useMemo(
        () => Object.entries(strategyAllocator)
            .map(([strategy, entry]) => ({ strategy, ...entry }))
            .sort((a, b) => Math.abs(b.multiplier - 1) - Math.abs(a.multiplier - 1))
            .slice(0, 8),
        [strategyAllocator],
    );
    const recentBlocks = React.useMemo(
        () => intelligenceBlocks.slice(0, 3),
        [intelligenceBlocks],
    );
    const runtimeCounts = React.useMemo(() => {
        let online = 0;
        let degraded = 0;
        let offline = 0;
        let standby = 0;
        for (const module of runtimeStatus.modules) {
            if (module.health === 'ONLINE') online += 1;
            if (module.health === 'DEGRADED') degraded += 1;
            if (module.health === 'OFFLINE') offline += 1;
            if (module.health === 'STANDBY') standby += 1;
        }
        return { online, degraded, offline, standby };
    }, [runtimeStatus.modules]);
    const runtimeModulesByPhase = React.useMemo(() => {
        const phases: RuntimePhase[] = ['PHASE_1', 'PHASE_2', 'PHASE_3'];
        return phases.map((phase) => ({
            phase,
            modules: runtimeStatus.modules.filter((module) => module.phase === phase),
        }));
    }, [runtimeStatus.modules]);
    const lowestPassRateRows = React.useMemo(
        () => Object.entries(strategyQuality)
            .filter(([, entry]) => entry.total_scans > 0)
            .map(([strategy, entry]) => ({ strategy, ...entry }))
            .sort((a, b) => a.pass_rate_pct - b.pass_rate_pct)
            .slice(0, 6),
        [strategyQuality],
    );
    const integrityAlertRows = React.useMemo(
        () => dataIntegrity.recent_alerts.slice(0, 5),
        [dataIntegrity.recent_alerts],
    );
    const holdRatePct = React.useMemo(() => {
        const total = dataIntegrity.totals.total_scans;
        if (total <= 0) {
            return 0;
        }
        return (dataIntegrity.totals.hold_scans / total) * 100;
    }, [dataIntegrity.totals]);
    const costDragRows = React.useMemo(
        () => Object.entries(strategyCostDiagnostics)
            .map(([strategy, entry]) => ({ strategy, ...entry }))
            .filter((entry) => entry.trades > 0)
            .sort((a, b) => b.avg_cost_drag_bps - a.avg_cost_drag_bps)
            .slice(0, 6),
        [strategyCostDiagnostics],
    );
    const governanceDecisionRows = React.useMemo(
        () => Object.values(governance.decisions)
            .sort((a, b) => {
                const rank = (action: StrategyGovernanceDecision['action']) => (
                    action === 'DEMOTE_DISABLE' ? 0 : action === 'PROMOTE' ? 1 : 2
                );
                const actionDelta = rank(a.action) - rank(b.action);
                if (actionDelta !== 0) {
                    return actionDelta;
                }
                return b.score - a.score;
            }),
        [governance.decisions],
    );
    const governanceAuditRows = React.useMemo(
        () => governance.audit.slice(0, 6),
        [governance.audit],
    );
    const featureLabelCoveragePct = React.useMemo(() => {
        if (featureRegistry.rows <= 0) {
            return 0;
        }
        return (featureRegistry.labeled_rows / featureRegistry.rows) * 100;
    }, [featureRegistry.rows, featureRegistry.labeled_rows]);
    const topMetaOverrides = React.useMemo(
        () => Object.entries(metaController.strategy_overrides)
            .map(([strategy, override]) => ({ strategy, ...override }))
            .sort(
                (a, b) => Math.abs(b.recommended_multiplier - 1) - Math.abs(a.recommended_multiplier - 1),
            )
            .slice(0, 5),
        [metaController.strategy_overrides],
    );
    const topModelInferenceRows = React.useMemo(
        () => Object.values(modelInference.latest)
            .sort((a, b) => {
                const aProb = a.probability_positive ?? 0;
                const bProb = b.probability_positive ?? 0;
                return Math.abs(bProb - b.probability_gate) - Math.abs(aProb - a.probability_gate);
            })
            .slice(0, 5),
        [modelInference.latest],
    );
    const topModelDriftRows = React.useMemo(
        () => Object.entries(modelDrift.by_strategy)
            .map(([strategy, entry]) => ({ strategy, ...entry }))
            .sort((a, b) => b.brier_ema - a.brier_ema)
            .slice(0, 4),
        [modelDrift.by_strategy],
    );

    React.useEffect(() => {
        let mounted = true;

        const fetchStats = async () => {
            try {
                const response = await fetch('/api/arb/stats');
                if (!response.ok || !mounted) {
                    return;
                }

                const data = await response.json() as {
                    bankroll?: number;
                    active_signals?: number;
                    active_markets?: number;
                    scanner_alive?: boolean;
                    simulation_ledger?: {
                        cash?: number;
                        reserved?: number;
                        realized_pnl?: number;
                        utilization_pct?: number;
                    };
                    intelligence?: {
                        strategies?: unknown[];
                        comparable_groups?: unknown[];
                    };
                    strategy_allocator?: unknown;
                    strategy_quality?: unknown;
                    strategy_cost_diagnostics?: unknown;
                    data_integrity?: unknown;
                    strategy_governance?: unknown;
                    ledger_health?: unknown;
                    feature_registry?: unknown;
                    meta_controller?: unknown;
                    ml_pipeline?: unknown;
                    model_inference?: unknown;
                    model_drift?: unknown;
                    trading_mode?: TradingMode;
                    live_order_posting_enabled?: boolean;
                    build_runtime?: unknown;
                };

                const bankroll = parseNumber(data.bankroll);
                const signals = parseNumber(data.active_signals);
                const markets = parseNumber(data.active_markets);
                const availableCash = parseNumber(data.simulation_ledger?.cash);
                const reservedCapital = parseNumber(data.simulation_ledger?.reserved);
                const realizedPnl = parseNumber(data.simulation_ledger?.realized_pnl);
                const utilizationPct = parseNumber(data.simulation_ledger?.utilization_pct);
                const strategySignalRows = Array.isArray(data.intelligence?.strategies)
                    ? data.intelligence!.strategies
                        .map((entry) => parseStrategySignal(entry))
                        .filter((entry): entry is StrategySignalState => entry !== null)
                    : [];
                const comparableGroupRows = Array.isArray(data.intelligence?.comparable_groups)
                    ? data.intelligence!.comparable_groups
                        .map((entry) => parseComparableGroup(entry))
                        .filter((entry): entry is ComparableGroupState => entry !== null)
                    : [];
                const allocator = parseAllocatorMap(data.strategy_allocator);
                const quality = parseQualityMap(data.strategy_quality);
                const costDiagnostics = parseCostDiagnosticsMap(data.strategy_cost_diagnostics);
                const integrity = parseDataIntegrityState(data.data_integrity);
                const governanceState = parseGovernanceState(data.strategy_governance);
                const parsedLedgerHealth = parseLedgerHealthState(data.ledger_health);
                const parsedFeatureRegistry = parseFeatureRegistrySummary(data.feature_registry);
                const parsedMetaController = parseMetaControllerState(data.meta_controller);
                const parsedMlPipeline = parseMlPipelineState(data.ml_pipeline);
                const parsedModelInference = parseModelInferenceState(data.model_inference);
                const parsedModelDrift = parseModelDriftState(data.model_drift);
                const runtime = parseRuntimeStatus(data.build_runtime);

                setStats((prev) => ({
                    ...prev,
                    bankroll: bankroll ?? prev.bankroll,
                    signals: signals ?? prev.signals,
                    markets: markets ?? prev.markets,
                    scannerAlive: typeof data.scanner_alive === 'boolean' ? data.scanner_alive : prev.scannerAlive,
                    availableCash: availableCash ?? prev.availableCash,
                    reservedCapital: reservedCapital ?? prev.reservedCapital,
                    capitalUtilizationPct: utilizationPct ?? prev.capitalUtilizationPct,
                    realizedPnl: realizedPnl ?? prev.realizedPnl,
                }));
                setStrategySignals(strategySignalRows);
                setComparableGroups(comparableGroupRows);
                setStrategyAllocator(allocator);
                setStrategyQuality(quality);
                setStrategyCostDiagnostics(costDiagnostics);
                setDataIntegrity(integrity);
                setGovernance(governanceState);
                setLedgerHealth(parsedLedgerHealth);
                setFeatureRegistry(parsedFeatureRegistry);
                setMetaController(parsedMetaController);
                setMlPipeline(parsedMlPipeline);
                setModelInference(parsedModelInference);
                setModelDrift(parsedModelDrift);
                if (runtime) {
                    setRuntimeStatus(runtime);
                }

                if (data.trading_mode === 'PAPER' || data.trading_mode === 'LIVE') {
                    setTradingMode(data.trading_mode);
                }
                if (typeof data.live_order_posting_enabled === 'boolean') {
                    setLiveOrderPostingEnabled(data.live_order_posting_enabled);
                }
            } catch {
                // keep last known stats on transient network errors
            }
        };

        void fetchStats();
        const intervalId = window.setInterval(() => {
            void fetchStats();
        }, 2000);

        return () => {
            mounted = false;
            window.clearInterval(intervalId);
        };
    }, []);

    React.useEffect(() => {
        if (!socket) return;

        const handleSystemHealth = (data: SystemHealthEntry[]) => {
            const latency = data.find((s) => s.name === 'Redis Queue')?.load || '--';

            setStats((prev) => ({
                ...prev,
                latency,
            }));
        };

        const handleExecutionLog = (log: unknown) => {
            setExecLogs((prev) => [toExecutionLog(log), ...prev].slice(0, 50));
        };

        const handleStrategyPnl = (data: StrategyPnlPayload) => {
            const bankroll = parseNumber(data.bankroll);
            if (bankroll !== null) {
                setStats((prev) => ({
                    ...prev,
                    bankroll,
                }));
            }

            const pnl = parseNumber(data.pnl) ?? 0;
            const exit = parseNumber(data.details?.exit);
            const rawPnl = data as unknown as Record<string, unknown>;
            const rawPnlDetails = (rawPnl.details && typeof rawPnl.details === 'object' && !Array.isArray(rawPnl.details))
                ? rawPnl.details as Record<string, unknown>
                : null;
            const executionId = parseText(rawPnl.execution_id)
                || parseText(rawPnl.executionId)
                || parseText(rawPnlDetails?.execution_id)
                || parseText(rawPnlDetails?.executionId)
                || undefined;
            const logEntry: ExecutionLog = {
                timestamp: parseNumber(data.timestamp) ?? Date.now(),
                side: pnl >= 0 ? 'WIN' : 'LOSS',
                market: typeof data.strategy === 'string' ? data.strategy : 'UNKNOWN',
                price: `${data.details?.action || 'CLOSE'} @ ${exit !== null ? exit.toFixed(2) : '---'}`,
                size: `$${Math.abs(pnl).toFixed(2)}`,
                execution_id: executionId,
            };
            setExecLogs((prev) => [logEntry, ...prev].slice(0, 50));
        };

        const handleTradingMode = (payload: TradingModePayload) => {
            if (payload.mode === 'PAPER' || payload.mode === 'LIVE') {
                setTradingMode(payload.mode);
            }
            if (typeof payload.live_order_posting_enabled === 'boolean') {
                setLiveOrderPostingEnabled(payload.live_order_posting_enabled);
            }
        };

        const handleSimulationReset = (payload: SimulationResetEvent) => {
            const bankroll = parseNumber(payload.bankroll) ?? DEFAULT_SIM_BANKROLL;
            setStats((prev) => ({
                ...prev,
                bankroll,
                availableCash: bankroll,
                reservedCapital: 0,
                capitalUtilizationPct: 0,
                realizedPnl: 0,
                signals: 0,
                markets: 0,
            }));
            setExecLogs([]);
            setStrategySignals([]);
            setComparableGroups([]);
            setStrategyQuality({});
            setStrategyCostDiagnostics({});
            setDataIntegrity({
                totals: {
                    total_scans: 0,
                    hold_scans: 0,
                    threshold: 0,
                    spread: 0,
                    parity: 0,
                    stale: 0,
                    risk: 0,
                    other: 0,
                },
                strategies: {},
                recent_alerts: [],
                updated_at: Date.now(),
            });
            setGovernance((prev) => ({
                ...prev,
                decisions: {},
                audit: [],
                updated_at: Date.now(),
            }));
            setLedgerHealth((prev) => ({
                ...prev,
                status: 'HEALTHY',
                issues: [],
                checked_at: Date.now(),
                ledger: {
                    ...prev.ledger,
                    cash: bankroll,
                    reserved: 0,
                    realized_pnl: 0,
                    equity: bankroll,
                    utilization_pct: 0,
                },
                strategy_reserved_total: 0,
                family_reserved_total: 0,
                reserved_gap_vs_strategy: 0,
                reserved_gap_vs_family: 0,
                top_strategy_exposure: [],
                top_family_exposure: [],
            }));
            setFeatureRegistry({
                rows: 0,
                labeled_rows: 0,
                unlabeled_rows: 0,
                leakage_violations: 0,
                by_strategy: {},
                latest_rows: [],
                updated_at: Date.now(),
            });
            setIntelligenceBlocks([]);
            setResetError(null);
            setShowResetConfirm(false);
            setResetText('');
        };

        const handleSimulationResetError = (payload: { message?: string }) => {
            setResetError(payload.message || 'Failed to reset simulation');
        };

        const handleStrategyLiveExecution = (payload: StrategyLiveExecutionEvent) => {
            setLastLiveExecution(payload);
        };

        const handleIntelligenceBlock = (payload: unknown) => {
            if (!payload || typeof payload !== 'object') {
                return;
            }
            const raw = payload as Partial<IntelligenceBlockEvent>;
            const strategy = typeof raw.strategy === 'string' ? raw.strategy : 'UNKNOWN';
            const marketKey = typeof raw.market_key === 'string' ? raw.market_key : 'UNKNOWN';
            const reason = typeof raw.reason === 'string' ? raw.reason : 'Execution blocked by intelligence gate';
            const timestamp = parseNumber(raw.timestamp) ?? Date.now();
            setIntelligenceBlocks((prev) => [
                { strategy, market_key: marketKey, reason, timestamp },
                ...prev,
            ].slice(0, 20));
        };

        const handleRiskMultiplierUpdate = (payload: unknown) => {
            if (!payload || typeof payload !== 'object') {
                return;
            }
            const raw = payload as {
                strategy?: string;
                multiplier?: number;
                sample_count?: number;
                ema_return?: number;
                timestamp?: number;
            };
            if (typeof raw.strategy !== 'string' || raw.strategy.trim().length === 0) {
                return;
            }
            const strategyId = raw.strategy;
            setStrategyAllocator((prev) => ({
                ...prev,
                [strategyId]: {
                    multiplier: parseNumber(raw.multiplier) ?? 1,
                    sample_count: parseNumber(raw.sample_count) ?? 0,
                    ema_return: parseNumber(raw.ema_return) ?? 0,
                    updated_at: parseNumber(raw.timestamp) ?? Date.now(),
                },
            }));
        };

        const handleRiskMultiplierSnapshot = (payload: unknown) => {
            setStrategyAllocator(parseAllocatorMap(payload));
        };
        const handleStrategyQualityUpdate = (payload: unknown) => {
            if (!payload || typeof payload !== 'object') {
                return;
            }
            const raw = payload as { strategy?: string };
            if (typeof raw.strategy !== 'string' || raw.strategy.trim().length === 0) {
                return;
            }
            const strategy = raw.strategy;
            const parsed = parseQualityMap({ [strategy]: payload });
            const entry = parsed[strategy];
            if (!entry) {
                return;
            }
            setStrategyQuality((prev) => ({
                ...prev,
                [strategy]: entry,
            }));
        };
        const handleStrategyQualitySnapshot = (payload: unknown) => {
            setStrategyQuality(parseQualityMap(payload));
        };
        const handleRuntimeStatus = (payload: unknown) => {
            const parsed = parseRuntimeStatus(payload);
            if (!parsed) {
                return;
            }
            setRuntimeStatus(parsed);
        };
        const handleDataIntegrityUpdate = (payload: unknown) => {
            if (!payload || typeof payload !== 'object') {
                return;
            }
            const raw = payload as { strategy?: string };
            if (typeof raw.strategy !== 'string' || raw.strategy.trim().length === 0) {
                return;
            }
            const strategy = raw.strategy;
            const parsedEntry = parseDataIntegrityStrategyEntry(payload);
            if (!parsedEntry) {
                return;
            }
            setDataIntegrity((prev) => ({
                ...prev,
                strategies: {
                    ...prev.strategies,
                    [strategy]: parsedEntry,
                },
                updated_at: Date.now(),
            }));
        };
        const handleDataIntegrityAlert = (payload: unknown) => {
            const parsed = parseDataIntegrityAlert(payload);
            if (!parsed) {
                return;
            }
            setDataIntegrity((prev) => ({
                ...prev,
                recent_alerts: [
                    parsed,
                    ...prev.recent_alerts.filter((entry) => entry.id !== parsed.id),
                ].slice(0, 20),
                updated_at: parsed.timestamp,
            }));
        };
        const handleDataIntegritySnapshot = (payload: unknown) => {
            setDataIntegrity(parseDataIntegrityState(payload));
        };
        const handleStrategyCostDiagnosticsUpdate = (payload: unknown) => {
            if (!payload || typeof payload !== 'object') {
                return;
            }
            const raw = payload as { strategy?: string };
            if (typeof raw.strategy !== 'string' || raw.strategy.trim().length === 0) {
                return;
            }
            const strategy = raw.strategy;
            const parsed = parseCostDiagnosticsMap({ [strategy]: payload });
            if (!parsed[strategy]) {
                return;
            }
            setStrategyCostDiagnostics((prev) => ({
                ...prev,
                [strategy]: parsed[strategy],
            }));
        };
        const handleStrategyCostDiagnosticsSnapshot = (payload: unknown) => {
            setStrategyCostDiagnostics(parseCostDiagnosticsMap(payload));
        };
        const handleGovernanceUpdate = (payload: unknown) => {
            const parsed = parseGovernanceDecision(payload);
            if (!parsed) {
                return;
            }
            setGovernance((prev) => ({
                ...prev,
                decisions: {
                    ...prev.decisions,
                    [parsed.strategy]: parsed,
                },
                updated_at: parsed.timestamp,
            }));
        };
        const handleGovernanceAudit = (payload: unknown) => {
            const parsed = parseGovernanceDecision(payload);
            if (!parsed) {
                return;
            }
            setGovernance((prev) => ({
                ...prev,
                audit: [
                    parsed,
                    ...prev.audit.filter((entry) => !(entry.strategy === parsed.strategy && entry.timestamp === parsed.timestamp)),
                ].slice(0, 30),
                updated_at: parsed.timestamp,
            }));
        };
        const handleGovernanceSnapshot = (payload: unknown) => {
            setGovernance(parseGovernanceState(payload));
        };
        const handleLedgerHealthUpdate = (payload: unknown) => {
            setLedgerHealth(parseLedgerHealthState(payload));
        };
        const handleFeatureRegistryUpdate = (payload: unknown) => {
            setFeatureRegistry(parseFeatureRegistrySummary(payload));
        };
        const handleFeatureRegistrySnapshot = (payload: unknown) => {
            setFeatureRegistry(parseFeatureRegistrySummary(payload));
        };
        const handleMetaControllerUpdate = (payload: unknown) => {
            setMetaController(parseMetaControllerState(payload));
        };
        const handleMlPipelineStatusUpdate = (payload: unknown) => {
            setMlPipeline(parseMlPipelineState(payload));
        };
        const handleModelInferenceSnapshot = (payload: unknown) => {
            setModelInference(parseModelInferenceState(payload));
        };
        const handleModelInferenceUpdate = (payload: unknown) => {
            const parsed = parseModelInferenceEntry(payload);
            if (!parsed) {
                return;
            }
            const key = `${parsed.strategy}:${parsed.market_key}`;
            setModelInference((prev) => ({
                ...prev,
                tracked_rows: Math.max(prev.tracked_rows, Object.keys(prev.latest).length + (prev.latest[key] ? 0 : 1)),
                latest: {
                    ...prev.latest,
                    [key]: parsed,
                },
                updated_at: parsed.timestamp || Date.now(),
            }));
        };
        const handleModelDriftUpdate = (payload: unknown) => {
            setModelDrift(parseModelDriftState(payload));
        };

        socket.on('system_health_update', handleSystemHealth);
        socket.on('execution_log', handleExecutionLog);
        socket.on('strategy_pnl', handleStrategyPnl);
        socket.on('strategy_live_execution', handleStrategyLiveExecution);
        socket.on('strategy_intelligence_block', handleIntelligenceBlock);
        socket.on('strategy_model_block', handleIntelligenceBlock);
        socket.on('strategy_risk_multiplier_update', handleRiskMultiplierUpdate);
        socket.on('strategy_risk_multiplier_snapshot', handleRiskMultiplierSnapshot);
        socket.on('strategy_quality_update', handleStrategyQualityUpdate);
        socket.on('strategy_quality_snapshot', handleStrategyQualitySnapshot);
        socket.on('strategy_cost_diagnostics_update', handleStrategyCostDiagnosticsUpdate);
        socket.on('strategy_cost_diagnostics_snapshot', handleStrategyCostDiagnosticsSnapshot);
        socket.on('data_integrity_update', handleDataIntegrityUpdate);
        socket.on('data_integrity_alert', handleDataIntegrityAlert);
        socket.on('data_integrity_snapshot', handleDataIntegritySnapshot);
        socket.on('strategy_governance_update', handleGovernanceUpdate);
        socket.on('strategy_governance_audit', handleGovernanceAudit);
        socket.on('strategy_governance_snapshot', handleGovernanceSnapshot);
        socket.on('ledger_health_update', handleLedgerHealthUpdate);
        socket.on('feature_registry_update', handleFeatureRegistryUpdate);
        socket.on('feature_registry_snapshot', handleFeatureRegistrySnapshot);
        socket.on('meta_controller_update', handleMetaControllerUpdate);
        socket.on('ml_pipeline_status_update', handleMlPipelineStatusUpdate);
        socket.on('model_inference_snapshot', handleModelInferenceSnapshot);
        socket.on('model_inference_update', handleModelInferenceUpdate);
        socket.on('model_drift_update', handleModelDriftUpdate);
        socket.on('trading_mode_update', handleTradingMode);
        socket.on('simulation_reset', handleSimulationReset);
        socket.on('simulation_reset_error', handleSimulationResetError);
        socket.on('runtime_status_update', handleRuntimeStatus);
        socket.emit('request_trading_mode');

        return () => {
            socket.off('system_health_update', handleSystemHealth);
            socket.off('execution_log', handleExecutionLog);
            socket.off('strategy_pnl', handleStrategyPnl);
            socket.off('strategy_live_execution', handleStrategyLiveExecution);
            socket.off('strategy_intelligence_block', handleIntelligenceBlock);
            socket.off('strategy_model_block', handleIntelligenceBlock);
            socket.off('strategy_risk_multiplier_update', handleRiskMultiplierUpdate);
            socket.off('strategy_risk_multiplier_snapshot', handleRiskMultiplierSnapshot);
            socket.off('strategy_quality_update', handleStrategyQualityUpdate);
            socket.off('strategy_quality_snapshot', handleStrategyQualitySnapshot);
            socket.off('strategy_cost_diagnostics_update', handleStrategyCostDiagnosticsUpdate);
            socket.off('strategy_cost_diagnostics_snapshot', handleStrategyCostDiagnosticsSnapshot);
            socket.off('data_integrity_update', handleDataIntegrityUpdate);
            socket.off('data_integrity_alert', handleDataIntegrityAlert);
            socket.off('data_integrity_snapshot', handleDataIntegritySnapshot);
            socket.off('strategy_governance_update', handleGovernanceUpdate);
            socket.off('strategy_governance_audit', handleGovernanceAudit);
            socket.off('strategy_governance_snapshot', handleGovernanceSnapshot);
            socket.off('ledger_health_update', handleLedgerHealthUpdate);
            socket.off('feature_registry_update', handleFeatureRegistryUpdate);
            socket.off('feature_registry_snapshot', handleFeatureRegistrySnapshot);
            socket.off('meta_controller_update', handleMetaControllerUpdate);
            socket.off('ml_pipeline_status_update', handleMlPipelineStatusUpdate);
            socket.off('model_inference_snapshot', handleModelInferenceSnapshot);
            socket.off('model_inference_update', handleModelInferenceUpdate);
            socket.off('model_drift_update', handleModelDriftUpdate);
            socket.off('trading_mode_update', handleTradingMode);
            socket.off('simulation_reset', handleSimulationReset);
            socket.off('simulation_reset_error', handleSimulationResetError);
            socket.off('runtime_status_update', handleRuntimeStatus);
        };
    }, [socket]);

    const resetSimulation = () => {
        if (!socket || !canConfirmReset) return;
        socket.emit('reset_simulation', {
            bankroll: DEFAULT_SIM_BANKROLL,
            confirmation: 'RESET',
            force_defaults: resetForceDefaults,
            timestamp: Date.now(),
        });
    };

    const closeExecutionTrace = React.useCallback(() => {
        setTraceExecutionId(null);
        setExecutionTrace(null);
        setExecutionTraceError(null);
        setExecutionTraceLoading(false);
    }, []);

    const openExecutionTrace = React.useCallback(async (executionId: string) => {
        if (!executionId) {
            return;
        }
        setTraceExecutionId(executionId);
        setExecutionTrace(null);
        setExecutionTraceError(null);
        setExecutionTraceLoading(true);
        try {
            const response = await fetch(`/api/arb/execution-trace/${encodeURIComponent(executionId)}`);
            if (!response.ok) {
                const payload = await response.json().catch(() => null) as { error?: string } | null;
                setExecutionTraceError(payload?.error || `Trace lookup failed (${response.status})`);
                return;
            }
            const data = await response.json() as ExecutionTrace;
            setExecutionTrace(data);
        } catch (error) {
            setExecutionTraceError(error instanceof Error ? error.message : String(error));
        } finally {
            setExecutionTraceLoading(false);
        }
    }, []);

    return (
        <DashboardLayout>
            <div className="p-6 space-y-6">
            <header className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between mb-8">
                <div>
                    <h1 className="text-2xl font-bold bg-gradient-to-r from-blue-400 to-emerald-400 bg-clip-text text-transparent">
                        Polymarket Execution Engine
                    </h1>
                    <p className="text-gray-400 text-sm mt-1">
                        High-frequency arbitrage scanner & execution monitor
                    </p>
                </div>
                <div className="flex flex-wrap items-center gap-3">
                    <div className="flex items-center gap-2">
                        <span className="flex h-3 w-3 relative">
                            <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
                            <span className="relative inline-flex rounded-full h-3 w-3 bg-emerald-500"></span>
                        </span>
                        <span className="text-xs text-emerald-400 font-mono">ENGINE ACTIVE</span>
                    </div>

                    <div className={`px-3 py-1 rounded-md text-[10px] font-mono border ${isPaperMode ? 'text-blue-300 border-blue-500/30 bg-blue-500/10' : 'text-red-300 border-red-500/40 bg-red-500/10'}`}>
                        {isPaperMode ? 'PAPER TRADING' : liveOrderPostingEnabled ? 'LIVE ARMED' : 'LIVE DRY-RUN'}
                    </div>

                    <button
                        type="button"
                        onClick={() => {
                            setResetError(null);
                            setResetText('');
                            setResetForceDefaults(false);
                            setShowResetConfirm(true);
                        }}
                        disabled={!isPaperMode}
                        className={`px-3 py-1.5 rounded-md text-xs font-mono border transition-colors ${
                            isPaperMode
                                ? 'border-amber-500/40 text-amber-300 bg-amber-500/10 hover:bg-amber-500/20'
                                : 'border-gray-600 text-gray-500 bg-gray-800/40 cursor-not-allowed'
                        }`}
                    >
                        <span className="inline-flex items-center gap-1">
                            <RotateCcw size={13} />
                            Reset Sim
                        </span>
                    </button>
                </div>
            </header>
            <div className="text-xs font-mono text-gray-500">
                Trading mode is controlled from the top header toggle.
            </div>
            {!isPaperMode && !liveOrderPostingEnabled && (
                <div className="text-xs font-mono text-amber-300">
                    LIVE is in dry-run mode. Real order posting is blocked until <span className="text-white">LIVE_ORDER_POSTING_ENABLED=true</span>.
                </div>
            )}
            {resetError && (
                <div className="text-xs font-mono text-red-400">{resetError}</div>
            )}
            {lastLiveExecution && (
                <div className={`text-xs font-mono ${
                    lastLiveExecution.dryRun
                        ? 'text-amber-300'
                        : lastLiveExecution.ok
                        ? 'text-emerald-300'
                        : 'text-red-300'
                }`}>
                    LIVE EXEC {lastLiveExecution.dryRun ? 'DRY-RUN' : 'POST'}: {parseNumber(lastLiveExecution.posted) ?? 0}/{parseNumber(lastLiveExecution.total) ?? 0} {lastLiveExecution.strategy || 'UNKNOWN'}
                    {lastLiveExecution.reason ? ` | ${lastLiveExecution.reason}` : ''}
                </div>
            )}

            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                <div className="lg:col-span-2 space-y-6">
                    <section>
                        <h2 className="text-sm uppercase tracking-wider text-gray-500 mb-4 font-mono">Live Opportunities</h2>
                        <ArbitrageScannerWidget />
                    </section>

                    <section>
                        <StrategyControlWidget />
                    </section>

                    <section className="bg-black/40 backdrop-blur-sm border border-white/5 rounded-xl p-4 h-[300px] font-mono text-xs overflow-y-auto">
                        <h2 className="text-xs uppercase tracking-wider text-gray-500 mb-2 sticky top-0 bg-black/40 backdrop-blur-md py-2">Execution Stream</h2>
                        <div className="space-y-1 text-gray-400">
                            {execLogs.length === 0 && (
                                <div className="flex gap-2">
                                    <span className="text-gray-600">[SAFE_MODE]</span>
                                    <span className="text-blue-400">Scanner</span>
                                    <span>Scanning active markets...</span>
                                </div>
                            )}
                            {execLogs.map((log, i) => (
                                <div
                                    key={`${log.timestamp}-${i}`}
                                    className={`flex gap-2 border-b border-white/5 pb-1 mb-1 ${log.execution_id ? 'cursor-pointer hover:bg-white/5' : ''}`}
                                    role={log.execution_id ? 'button' : undefined}
                                    onClick={() => {
                                        if (log.execution_id) {
                                            void openExecutionTrace(log.execution_id);
                                        }
                                    }}
                                    title={log.execution_id ? `Open trace: ${log.execution_id}` : undefined}
                                >
                                    <span className="text-gray-600">[{new Date(log.timestamp).toLocaleTimeString()}]</span>
                                    <span className={`font-bold ${(log.side === 'BUY' || log.side === 'WIN') ? 'text-green-400' : 'text-red-400'}`}>
                                        {log.side}
                                    </span>
                                    <span className="text-white">{log.market}</span>
                                    <span className="text-gray-500">
                                        {log.price} | Size: {log.size}
                                    </span>
                                    {log.execution_id && (
                                        <span className="text-gray-700 font-mono ml-auto">
                                            #{log.execution_id.slice(0, 8)}
                                        </span>
                                    )}
                                </div>
                            ))}
                        </div>
                    </section>
                </div>

                <div className="space-y-6">
                    <div className="bg-white/5 border border-white/10 rounded-xl p-6">
                        <h3 className="text-sm font-medium text-gray-300 mb-4">Engine Stats</h3>
                        <div className="space-y-4">
                            <div className="flex justify-between items-center">
                                <span className="text-gray-500 text-sm">Latency</span>
                                <span className="text-emerald-400 font-mono">{stats.latency}</span>
                            </div>
                            <div className="flex justify-between items-center">
                                <span className="text-gray-500 text-sm">Active Signals</span>
                                <span className="text-white font-mono">{stats.signals}</span>
                            </div>
                            <div className="flex justify-between items-center">
                                <span className="text-gray-500 text-sm">Active Markets</span>
                                <span className="text-white font-mono">{stats.markets}</span>
                            </div>
                            <div className="flex justify-between items-center">
                                <span className="text-gray-500 text-sm">Scanner</span>
                                <span className={`font-mono ${stats.scannerAlive ? 'text-emerald-400' : 'text-rose-400'}`}>
                                    {stats.scannerAlive ? 'ONLINE' : 'OFFLINE'}
                                </span>
                            </div>
                            <div className="flex justify-between items-center">
                                <span className="text-gray-500 text-sm">Ledger Health</span>
                                <span className={`font-mono ${
                                    ledgerHealth.status === 'HEALTHY'
                                        ? 'text-emerald-300'
                                        : ledgerHealth.status === 'WARN'
                                            ? 'text-amber-300'
                                            : 'text-rose-300'
                                }`}>
                                    {ledgerHealth.status}
                                </span>
                            </div>
                            <div className="flex justify-between items-center">
                                <span className="text-gray-500 text-sm">Mode</span>
                                <span className={`font-mono ${isPaperMode ? 'text-blue-300' : 'text-red-300'}`}>
                                    {isPaperMode ? 'PAPER' : liveOrderPostingEnabled ? 'LIVE_ARMED' : 'LIVE_DRY_RUN'}
                                </span>
                            </div>
                            <div className="flex justify-between items-center">
                                <span className="text-gray-500 text-sm">Equity</span>
                                <span className="text-blue-400 font-mono">{formatUsd(stats.bankroll)}</span>
                            </div>
                            <div className="flex justify-between items-center">
                                <span className="text-gray-500 text-sm">Available Cash</span>
                                <span className="text-white font-mono">{formatUsd(stats.availableCash)}</span>
                            </div>
                            <div className="flex justify-between items-center">
                                <span className="text-gray-500 text-sm">Reserved</span>
                                <span className={`font-mono ${stats.reservedCapital > 0 ? 'text-amber-300' : 'text-gray-300'}`}>
                                    {formatUsd(stats.reservedCapital)}
                                </span>
                            </div>
                            <div className="flex justify-between items-center">
                                <span className="text-gray-500 text-sm">Realized PnL</span>
                                <span className={`font-mono ${stats.realizedPnl >= 0 ? 'text-emerald-300' : 'text-rose-300'}`}>
                                    {stats.realizedPnl >= 0 ? '+' : '-'}{formatUsd(Math.abs(stats.realizedPnl))}
                                </span>
                            </div>
                            <div className="flex justify-between items-center">
                                <span className="text-gray-500 text-sm">Utilization</span>
                                <span className="text-gray-300 font-mono">{stats.capitalUtilizationPct.toFixed(1)}%</span>
                            </div>
                            <div className="flex justify-between items-center">
                                <span className="text-gray-500 text-sm">Reserved Gap</span>
                                <span className={`font-mono ${
                                    Math.max(ledgerHealth.reserved_gap_vs_strategy, ledgerHealth.reserved_gap_vs_family) > 5
                                        ? 'text-rose-300'
                                        : Math.max(ledgerHealth.reserved_gap_vs_strategy, ledgerHealth.reserved_gap_vs_family) > 1
                                            ? 'text-amber-300'
                                            : 'text-emerald-300'
                                }`}>
                                    ${Math.max(ledgerHealth.reserved_gap_vs_strategy, ledgerHealth.reserved_gap_vs_family).toFixed(2)}
                                </span>
                            </div>
                        </div>
                    </div>

                    <div className="bg-white/5 border border-white/10 rounded-xl p-6">
                        <div className="flex items-center justify-between gap-3 mb-4 flex-wrap">
                            <h3 className="text-sm font-medium text-gray-300">Intelligence Control Plane</h3>
                            <div className="flex items-center gap-2 text-[10px] font-mono">
                                <select
                                    value={intelligenceAssetScope}
                                    onChange={(e) => setIntelligenceAssetScope(e.target.value as IntelligenceAssetScope)}
                                    className="bg-black/40 border border-white/10 rounded px-2 py-1 text-gray-200"
                                    title="Scope these summary stats by asset. Some strategies are not asset-specific."
                                >
                                    <option value="ALL">All Assets</option>
                                    <option value="BTC">BTC</option>
                                    <option value="ETH">ETH</option>
                                    <option value="SOL">SOL</option>
                                    <option value="OTHER">Other</option>
                                </select>
                                <select
                                    value={intelligenceFamilyScope}
                                    onChange={(e) => setIntelligenceFamilyScope(e.target.value)}
                                    className="bg-black/40 border border-white/10 rounded px-2 py-1 text-gray-200"
                                    title="Scope these summary stats by signal family."
                                >
                                    {intelligenceFamilyOptions.map((family) => (
                                        <option key={family} value={family}>
                                            {family === 'ALL' ? 'All Families' : family}
                                        </option>
                                    ))}
                                </select>
                            </div>
                        </div>
                        <div className="space-y-4">
                            <div className="grid grid-cols-3 gap-2 text-[11px] font-mono">
                                <div className="rounded border border-white/10 bg-black/30 p-2">
                                    <div className="text-gray-500">Passing (Snapshot)</div>
                                    <div className="text-emerald-300">{formatPercent(scopedPassFractionPct)}</div>
                                    <div className="text-[10px] text-gray-600 mt-1">
                                        {intelligenceScopeLabel} | n={scopedStrategySignals.length}
                                    </div>
                                </div>
                                <div className="rounded border border-white/10 bg-black/30 p-2">
                                    <div className="text-gray-500">Median Age</div>
                                    <div className="text-cyan-300">{Math.round(medianSignalAgeMs)}ms</div>
                                </div>
                                <div className="rounded border border-white/10 bg-black/30 p-2">
                                    <div className="text-gray-500">Families</div>
                                    <div className="text-white">{metricFamilyCoverage}</div>
                                </div>
                            </div>
                            <div className="text-[10px] font-mono text-gray-500">
                                Hold Rate: <span className={`${holdRatePct > 70 ? 'text-rose-300' : holdRatePct > 45 ? 'text-amber-300' : 'text-emerald-300'}`}>{holdRatePct.toFixed(1)}%</span> ({dataIntegrity.totals.hold_scans}/{dataIntegrity.totals.total_scans} scans)
                            </div>

                            <div>
                                <div className="text-[10px] uppercase tracking-wider text-gray-500 mb-2">Recent Gate Blocks</div>
                                <div className="space-y-1">
                                    {recentBlocks.length === 0 && (
                                        <div className="text-xs text-gray-500 font-mono">No recent intelligence blocks</div>
                                    )}
                                    {recentBlocks.map((block, index) => (
                                        <div key={`${block.timestamp}-${index}`} className="border border-white/10 rounded p-2 bg-black/30">
                                            <div className="flex justify-between items-center text-[11px]">
                                                <span className="text-rose-300 font-mono">{STRATEGY_NAME_MAP[block.strategy] || block.strategy}</span>
                                                <span className="text-gray-500 font-mono">{new Date(block.timestamp).toLocaleTimeString()}</span>
                                            </div>
                                            <div className="text-[10px] text-gray-400 truncate" title={block.reason}>
                                                {block.reason}
                                            </div>
                                        </div>
                                    ))}
                                </div>
                            </div>

                            <div>
                                <div className="text-[10px] uppercase tracking-wider text-gray-500 mb-2">Risk Allocator</div>
                                <div className="space-y-1">
                                    {allocatorRows.length === 0 && (
                                        <div className="text-xs text-gray-500 font-mono">Allocator not available</div>
                                    )}
                                    {allocatorRows.map((entry) => (
                                        <div key={entry.strategy} className="flex justify-between items-center text-[11px] font-mono border-b border-white/10 pb-1">
                                            <span className="text-gray-200 truncate pr-2">{STRATEGY_NAME_MAP[entry.strategy] || entry.strategy}</span>
                                            <span className={`${entry.multiplier > 1 ? 'text-emerald-300' : entry.multiplier < 1 ? 'text-rose-300' : 'text-gray-300'}`}>
                                                {entry.multiplier.toFixed(2)}x
                                            </span>
                                        </div>
                                    ))}
                                </div>
                            </div>

                            <div>
                                <div className="text-[10px] uppercase tracking-wider text-gray-500 mb-2">Data Integrity Alerts</div>
                                <div className="space-y-1">
                                    {integrityAlertRows.length === 0 && (
                                        <div className="text-xs text-gray-500 font-mono">No data-integrity alerts</div>
                                    )}
                                    {integrityAlertRows.map((alert) => (
                                        <div key={alert.id} className="border border-white/10 rounded p-2 bg-black/30">
                                            <div className="flex justify-between items-center text-[11px] font-mono">
                                                <span className="text-gray-200 truncate pr-2">{STRATEGY_NAME_MAP[alert.strategy] || alert.strategy}</span>
                                                <span className={`${alert.severity === 'CRITICAL' ? 'text-rose-300' : 'text-amber-300'}`}>
                                                    {alert.category.toUpperCase()} {alert.severity}
                                                </span>
                                            </div>
                                            <div className="flex justify-between items-center text-[10px] text-gray-500 font-mono mt-1">
                                                <span>x{alert.consecutive_holds}</span>
                                                <span>{new Date(alert.timestamp).toLocaleTimeString()}</span>
                                            </div>
                                            <div className="text-[10px] text-gray-400 truncate" title={alert.reason}>
                                                {alert.reason}
                                            </div>
                                        </div>
                                    ))}
                                </div>
                            </div>

                            <div>
                                <div className="text-[10px] uppercase tracking-wider text-gray-500 mb-2">Cost Drag (Bps)</div>
                                <div className="space-y-1">
                                    {costDragRows.length === 0 && (
                                        <div className="text-xs text-gray-500 font-mono">No trade-cost sample yet</div>
                                    )}
                                    {costDragRows.map((entry) => (
                                        <div key={entry.strategy} className="flex justify-between items-center text-[11px] font-mono border-b border-white/10 pb-1">
                                            <span className="text-gray-200 truncate pr-2">{STRATEGY_NAME_MAP[entry.strategy] || entry.strategy}</span>
                                            <span className={`${entry.avg_cost_drag_bps > 25 ? 'text-rose-300' : entry.avg_cost_drag_bps > 10 ? 'text-amber-300' : 'text-emerald-300'}`}>
                                                {entry.avg_cost_drag_bps.toFixed(1)}bps
                                            </span>
                                        </div>
                                    ))}
                                </div>
                            </div>

                            <div>
                                <div className="text-[10px] uppercase tracking-wider text-gray-500 mb-2">Concentration Guard</div>
                                <div className="space-y-1">
                                    <div className="text-[10px] text-gray-500 font-mono">
                                        Caps: strat {ledgerHealth.caps.strategy_pct.toFixed(1)}% | family {ledgerHealth.caps.family_pct.toFixed(1)}% | util {ledgerHealth.caps.utilization_pct.toFixed(1)}%
                                    </div>
                                    {ledgerHealth.top_strategy_exposure.length === 0 && (
                                        <div className="text-xs text-gray-500 font-mono">No active reserved exposure</div>
                                    )}
                                    {ledgerHealth.top_strategy_exposure.slice(0, 3).map((entry) => (
                                        <div key={entry.id} className="flex justify-between items-center text-[11px] font-mono border-b border-white/10 pb-1">
                                            <span className="text-gray-200 truncate pr-2">{STRATEGY_NAME_MAP[entry.id] || entry.id}</span>
                                            <span className={`${entry.share_pct > ledgerHealth.caps.strategy_pct ? 'text-rose-300' : 'text-gray-300'}`}>
                                                {entry.share_pct.toFixed(1)}%
                                            </span>
                                        </div>
                                    ))}
                                    {ledgerHealth.issues.length > 0 && (
                                        <div className="text-[10px] text-amber-300 font-mono truncate" title={ledgerHealth.issues[0]}>
                                            {ledgerHealth.issues[0]}
                                        </div>
                                    )}
                                </div>
                            </div>

                            <div>
                                <div className="text-[10px] uppercase tracking-wider text-gray-500 mb-2">Meta Controller</div>
                                <div className="space-y-1">
                                    <div className="text-[10px] text-gray-500 font-mono">
                                        {metaController.enabled ? 'ENABLED' : 'DISABLED'} | {metaController.advisory_only ? 'ADVISORY' : 'ACTIVE'}
                                    </div>
                                    <div className="grid grid-cols-3 gap-2 text-[10px] font-mono">
                                        <div className="rounded border border-white/10 bg-black/20 p-2">
                                            <div className="text-gray-500">Regime</div>
                                            <div className="text-cyan-300">{metaController.regime}</div>
                                        </div>
                                        <div className="rounded border border-white/10 bg-black/20 p-2">
                                            <div className="text-gray-500">Confidence</div>
                                            <div className={`${metaController.confidence >= 0.5 ? 'text-emerald-300' : metaController.confidence >= 0.25 ? 'text-amber-300' : 'text-rose-300'}`}>
                                                {(metaController.confidence * 100).toFixed(1)}%
                                            </div>
                                        </div>
                                        <div className="rounded border border-white/10 bg-black/20 p-2">
                                            <div className="text-gray-500">Signals</div>
                                            <div className="text-white">{metaController.signal_count}</div>
                                        </div>
                                    </div>
                                    <div className="text-[10px] text-gray-500 font-mono">
                                        pass {metaController.pass_rate_pct.toFixed(1)}% | |margin| {metaController.mean_abs_normalized_margin.toFixed(3)}
                                    </div>
                                    <div className="text-[10px] text-gray-500 font-mono truncate">
                                        families: {metaController.allowed_families.length > 0 ? metaController.allowed_families.join(', ') : '--'}
                                    </div>
                                    {topMetaOverrides.length === 0 && (
                                        <div className="text-xs text-gray-500 font-mono">No strategy overrides yet</div>
                                    )}
                                    {topMetaOverrides.map((row) => (
                                        <div key={row.strategy} className="flex justify-between items-center text-[11px] font-mono border-b border-white/10 pb-1">
                                            <span className="text-gray-200 truncate pr-2">{STRATEGY_NAME_MAP[row.strategy] || row.strategy}</span>
                                            <span className={`${row.recommended_multiplier > 1 ? 'text-emerald-300' : row.recommended_multiplier < 1 ? 'text-rose-300' : 'text-gray-300'}`}>
                                                {row.recommended_multiplier.toFixed(2)}x
                                            </span>
                                        </div>
                                    ))}
                                </div>
                            </div>

                            <div>
                                <div className="text-[10px] uppercase tracking-wider text-gray-500 mb-2">Model Inference (Advisory)</div>
                                <div className="space-y-1">
                                    <div className="text-[10px] text-gray-500 font-mono">
                                        {modelInference.model_loaded ? 'MODEL_LOADED' : 'MODEL_PENDING'} | rows {modelInference.tracked_rows}
                                    </div>
                                    {topModelInferenceRows.length === 0 && (
                                        <div className="text-xs text-gray-500 font-mono">No model inference rows yet</div>
                                    )}
                                    {topModelInferenceRows.map((row) => {
                                        const probability = row.probability_positive;
                                        return (
                                            <div key={`${row.strategy}:${row.market_key}`} className="border border-white/10 rounded p-2 bg-black/20 text-[10px] font-mono">
                                                <div className="flex justify-between items-center">
                                                    <span className="text-gray-200 truncate pr-2">{STRATEGY_NAME_MAP[row.strategy] || row.strategy}</span>
                                                    <span className={`${
                                                        probability !== null && probability >= row.probability_gate
                                                            ? 'text-emerald-300'
                                                            : 'text-gray-400'
                                                    }`}>
                                                        {probability !== null ? `${(probability * 100).toFixed(1)}%` : '--'}
                                                    </span>
                                                </div>
                                                <div className="text-gray-500 truncate">
                                                    gate {(row.probability_gate * 100).toFixed(0)}% | {row.reason}
                                                </div>
                                            </div>
                                        );
                                    })}
                                </div>
                            </div>

                            <div>
                                <div className="text-[10px] uppercase tracking-wider text-gray-500 mb-2">Comparable Regimes</div>
                                <div className="space-y-1">
                                    {topComparableGroups.length === 0 && (
                                        <div className="text-xs text-gray-500 font-mono">No comparable-group sample yet</div>
                                    )}
                                    {topComparableGroups.map((group) => (
                                        <div key={group.comparable_group} className="border-b border-white/10 pb-1">
                                            <div className="flex justify-between items-center text-[11px]">
                                                <span className="text-gray-200 font-mono truncate pr-2">{group.comparable_group}</span>
                                                <span className={`font-mono ${group.mean_normalized_margin >= 0 ? 'text-emerald-300' : 'text-rose-300'}`}>
                                                    {formatSignalValue(group.mean_margin, group.unit)}
                                                </span>
                                            </div>
                                            <div className="flex justify-between items-center text-[10px] text-gray-500 font-mono">
                                                <span>{group.sample_size} samples</span>
                                                <span>{formatPercent(group.pass_rate_pct)} pass</span>
                                            </div>
                                        </div>
                                    ))}
                                </div>
                            </div>
                        </div>
                    </div>

                    <div className="bg-white/5 border border-white/10 rounded-xl p-6">
                        <h3 className="text-sm font-medium text-gray-300 mb-4">Build & Runtime Control Plane</h3>
                        <div className="space-y-4">
                            <div className="grid grid-cols-2 gap-2 text-[11px] font-mono">
                                <div className="rounded border border-white/10 bg-black/30 p-2">
                                    <div className="text-gray-500">Active Phase</div>
                                    <div className="text-cyan-300">{runtimeStatus.phase.replace('_', ' ')}</div>
                                </div>
                                <div className="rounded border border-white/10 bg-black/30 p-2">
                                    <div className="text-gray-500">Modules</div>
                                    <div className="text-white">{runtimeStatus.modules.length}</div>
                                </div>
                            </div>
                            <div className="grid grid-cols-4 gap-2 text-[10px] font-mono">
                                <div className="rounded border border-emerald-500/20 bg-emerald-500/10 p-2 text-emerald-300">ON {runtimeCounts.online}</div>
                                <div className="rounded border border-amber-500/20 bg-amber-500/10 p-2 text-amber-300">DEG {runtimeCounts.degraded}</div>
                                <div className="rounded border border-rose-500/20 bg-rose-500/10 p-2 text-rose-300">OFF {runtimeCounts.offline}</div>
                                <div className="rounded border border-white/10 bg-black/30 p-2 text-gray-300">STBY {runtimeCounts.standby}</div>
                            </div>
                            <div className="max-h-[340px] overflow-y-auto space-y-3 pr-1">
                                {runtimeModulesByPhase.map((phaseGroup) => (
                                    <div key={phaseGroup.phase}>
                                        <div className="text-[10px] uppercase tracking-wider text-gray-500 mb-1">
                                            {phaseGroup.phase.replace('_', ' ')}
                                        </div>
                                        {phaseGroup.modules.length === 0 && (
                                            <div className="text-[11px] font-mono text-gray-500 border border-white/10 rounded p-2 bg-black/20">
                                                No modules registered
                                            </div>
                                        )}
                                        {phaseGroup.modules.map((module) => (
                                            <div key={module.id} className="border border-white/10 rounded p-2 bg-black/30 mb-1">
                                                <div className="flex items-center justify-between text-[11px] font-mono">
                                                    <span className="text-gray-200 truncate pr-2">{module.label}</span>
                                                    <span className={runtimeHealthClass(module.health)}>{module.health}</span>
                                                </div>
                                                <div className="flex items-center justify-between text-[10px] font-mono text-gray-500 mt-1">
                                                    <span>events {module.events}</span>
                                                    <span>last {formatAgeMs(module.heartbeat_ms, runtimeStatus.timestamp || Date.now())}</span>
                                                </div>
                                                <div className="text-[10px] text-gray-400 mt-1 truncate" title={module.last_detail}>
                                                    {module.last_detail || 'no details'}
                                                </div>
                                            </div>
                                        ))}
                                    </div>
                                ))}
                            </div>
                            <div>
                                <div className="text-[10px] uppercase tracking-wider text-gray-500 mb-2">
                                    Signal Quality (Lowest Pass Rate)
                                </div>
                                <div className="space-y-1 max-h-[180px] overflow-y-auto pr-1">
                                    {lowestPassRateRows.length === 0 && (
                                        <div className="text-[11px] text-gray-500 font-mono border border-white/10 rounded p-2 bg-black/20">
                                            Collecting scan samples...
                                        </div>
                                    )}
                                    {lowestPassRateRows.map((entry) => (
                                        <div key={entry.strategy} className="border border-white/10 rounded p-2 bg-black/20">
                                            <div className="flex items-center justify-between text-[11px] font-mono">
                                                <span className="text-gray-200 truncate pr-2">{STRATEGY_NAME_MAP[entry.strategy] || entry.strategy}</span>
                                                <span className={`${entry.pass_rate_pct >= 50 ? 'text-emerald-300' : entry.pass_rate_pct >= 20 ? 'text-amber-300' : 'text-rose-300'}`}>
                                                    {entry.pass_rate_pct.toFixed(1)}%
                                                </span>
                                            </div>
                                            <div className="flex items-center justify-between text-[10px] font-mono text-gray-500 mt-1">
                                                <span>n={entry.total_scans}</span>
                                                <span>block={dominantBlockLabel(entry)}</span>
                                            </div>
                                        </div>
                                    ))}
                                </div>
                            </div>
                            <div>
                                <div className="text-[10px] uppercase tracking-wider text-gray-500 mb-2">
                                    ML Feature Registry
                                </div>
                                <div className="grid grid-cols-2 gap-2 text-[10px] font-mono">
                                    <div className="rounded border border-white/10 bg-black/20 p-2">
                                        <div className="text-gray-500">Rows</div>
                                        <div className="text-white">{featureRegistry.rows}</div>
                                    </div>
                                    <div className="rounded border border-white/10 bg-black/20 p-2">
                                        <div className="text-gray-500">Labeled</div>
                                        <div className={`${featureLabelCoveragePct >= 40 ? 'text-emerald-300' : featureLabelCoveragePct >= 15 ? 'text-amber-300' : 'text-rose-300'}`}>
                                            {featureLabelCoveragePct.toFixed(1)}%
                                        </div>
                                    </div>
                                    <div className="rounded border border-white/10 bg-black/20 p-2">
                                        <div className="text-gray-500">Leakage Violations</div>
                                        <div className={`${featureRegistry.leakage_violations > 0 ? 'text-rose-300' : 'text-emerald-300'}`}>
                                            {featureRegistry.leakage_violations}
                                        </div>
                                    </div>
                                    <div className="rounded border border-white/10 bg-black/20 p-2">
                                        <div className="text-gray-500">Updated</div>
                                        <div className="text-cyan-300">{formatAgeMs(featureRegistry.updated_at, Date.now())}</div>
                                    </div>
                                </div>
                                <div className="mt-2 grid grid-cols-2 gap-2 text-[10px] font-mono">
                                    <div className="rounded border border-white/10 bg-black/20 p-2">
                                        <div className="text-gray-500">Event Log Rows</div>
                                        <div className="text-white">{mlPipeline.feature_event_log_rows}</div>
                                    </div>
                                    <div className="rounded border border-white/10 bg-black/20 p-2">
                                        <div className="text-gray-500">Dataset Rows</div>
                                        <div className="text-cyan-300">{mlPipeline.dataset_rows}</div>
                                    </div>
                                    <div className="rounded border border-white/10 bg-black/20 p-2">
                                        <div className="text-gray-500">Labeled Rows</div>
                                        <div className={`${mlPipeline.dataset_labeled_rows >= 40 ? 'text-emerald-300' : mlPipeline.dataset_labeled_rows >= 20 ? 'text-amber-300' : 'text-rose-300'}`}>
                                            {mlPipeline.dataset_labeled_rows}
                                        </div>
                                    </div>
                                    <div className="rounded border border-white/10 bg-black/20 p-2">
                                        <div className="text-gray-500">Model</div>
                                        <div className={`${mlPipeline.model_eligible ? 'text-emerald-300' : 'text-amber-300'}`}>
                                            {mlPipeline.model_eligible ? 'ELIGIBLE' : 'NOT_READY'}
                                        </div>
                                    </div>
                                    <div className="rounded border border-white/10 bg-black/20 p-2">
                                        <div className="text-gray-500">Model Rows</div>
                                        <div className="text-gray-300">{mlPipeline.model_rows}</div>
                                    </div>
                                    <div className="rounded border border-white/10 bg-black/20 p-2">
                                        <div className="text-gray-500">CV Folds</div>
                                        <div className="text-gray-300">{mlPipeline.model_cv_folds}</div>
                                    </div>
                                </div>
                                {!mlPipeline.model_eligible && mlPipeline.model_reason && (
                                    <div className="mt-2 text-[10px] text-amber-300 font-mono truncate" title={mlPipeline.model_reason}>
                                        {mlPipeline.model_reason}
                                    </div>
                                )}
                                <div className="mt-2 grid grid-cols-2 gap-2 text-[10px] font-mono">
                                    <div className="rounded border border-white/10 bg-black/20 p-2">
                                        <div className="text-gray-500">Drift Status</div>
                                        <div className={`${
                                            modelDrift.status === 'HEALTHY'
                                                ? 'text-emerald-300'
                                                : modelDrift.status === 'WARN'
                                                    ? 'text-amber-300'
                                                    : 'text-rose-300'
                                        }`}>
                                            {modelDrift.status}
                                        </div>
                                    </div>
                                    <div className="rounded border border-white/10 bg-black/20 p-2">
                                        <div className="text-gray-500">Gate</div>
                                        <div className={`${modelDrift.gate_enforcing ? 'text-emerald-300' : 'text-amber-300'}`}>
                                            {modelDrift.gate_enforcing ? 'ENFORCING' : 'BYPASS'}
                                        </div>
                                    </div>
                                    <div className="rounded border border-white/10 bg-black/20 p-2">
                                        <div className="text-gray-500">Brier EMA</div>
                                        <div className={`${modelDrift.brier_ema < 0.20 ? 'text-emerald-300' : modelDrift.brier_ema < 0.28 ? 'text-amber-300' : 'text-rose-300'}`}>
                                            {modelDrift.brier_ema.toFixed(3)}
                                        </div>
                                    </div>
                                    <div className="rounded border border-white/10 bg-black/20 p-2">
                                        <div className="text-gray-500">Accuracy</div>
                                        <div className="text-cyan-300">{modelDrift.accuracy_pct.toFixed(1)}%</div>
                                    </div>
                                </div>
                                {modelDrift.issues.length > 0 && (
                                    <div className="mt-2 text-[10px] text-amber-300 font-mono truncate" title={modelDrift.issues[0]}>
                                        {modelDrift.issues[0]}
                                    </div>
                                )}
                                <div className="mt-2 space-y-1 max-h-[90px] overflow-y-auto pr-1">
                                    {topModelDriftRows.map((row) => (
                                        <div key={row.strategy} className="border border-white/10 rounded p-2 bg-black/20 text-[10px] font-mono">
                                            <div className="flex items-center justify-between">
                                                <span className="text-gray-200 truncate pr-2">{STRATEGY_NAME_MAP[row.strategy] || row.strategy}</span>
                                                <span className={`${row.brier_ema < 0.20 ? 'text-emerald-300' : row.brier_ema < 0.28 ? 'text-amber-300' : 'text-rose-300'}`}>
                                                    {row.brier_ema.toFixed(3)}
                                                </span>
                                            </div>
                                            <div className="text-gray-500">
                                                n={row.sample_count} | acc {row.accuracy_pct.toFixed(1)}%
                                            </div>
                                        </div>
                                    ))}
                                    {topModelDriftRows.length === 0 && (
                                        <div className="text-[10px] text-gray-500 font-mono border border-white/10 rounded p-2 bg-black/20">
                                            No drift sample yet (waiting for labeled model predictions)
                                        </div>
                                    )}
                                </div>
                                <div className="mt-2 space-y-1 max-h-[110px] overflow-y-auto pr-1">
                                    {featureRegistry.latest_rows.slice(0, 4).map((row) => (
                                        <div key={row.id} className="border border-white/10 rounded p-2 bg-black/20 text-[10px] font-mono">
                                            <div className="flex items-center justify-between">
                                                <span className="text-gray-200 truncate pr-2">{STRATEGY_NAME_MAP[row.strategy] || row.strategy}</span>
                                                <span className={`${row.label_net_return !== null ? 'text-emerald-300' : 'text-gray-500'}`}>
                                                    {row.label_net_return !== null ? 'LABELED' : 'PENDING'}
                                                </span>
                                            </div>
                                            <div className="text-gray-500 truncate">{row.metric_family} | {row.signal_type}</div>
                                        </div>
                                    ))}
                                    {featureRegistry.latest_rows.length === 0 && (
                                        <div className="text-[10px] text-gray-500 font-mono border border-white/10 rounded p-2 bg-black/20">
                                            Collecting feature rows...
                                        </div>
                                    )}
                                </div>
                            </div>
                        </div>
                    </div>

                    <div className="bg-white/5 border border-white/10 rounded-xl p-6">
                        <h3 className="text-sm font-medium text-gray-300 mb-4">Strategy Governance</h3>
                        <div className="space-y-4">
                            <div className="grid grid-cols-3 gap-2 text-[11px] font-mono">
                                <div className="rounded border border-white/10 bg-black/30 p-2">
                                    <div className="text-gray-500">Autopilot</div>
                                    <div className={`${governance.autopilot_enabled ? 'text-emerald-300' : 'text-gray-300'}`}>
                                        {governance.autopilot_enabled ? 'ENABLED' : 'DISABLED'}
                                    </div>
                                </div>
                                <div className="rounded border border-white/10 bg-black/30 p-2">
                                    <div className="text-gray-500">Effective</div>
                                    <div className={`${governance.autopilot_effective ? 'text-emerald-300' : 'text-amber-300'}`}>
                                        {governance.autopilot_effective ? 'YES' : 'NO'}
                                    </div>
                                </div>
                                <div className="rounded border border-white/10 bg-black/30 p-2">
                                    <div className="text-gray-500">Mode</div>
                                    <div className={`${governance.trading_mode === 'PAPER' ? 'text-blue-300' : 'text-red-300'}`}>
                                        {governance.trading_mode}
                                    </div>
                                </div>
                            </div>

                            <div>
                                <div className="text-[10px] uppercase tracking-wider text-gray-500 mb-2">Decisions</div>
                                <div className="space-y-1 max-h-[180px] overflow-y-auto pr-1">
                                    {governanceDecisionRows.length === 0 && (
                                        <div className="text-xs text-gray-500 font-mono">No governance decisions yet</div>
                                    )}
                                    {governanceDecisionRows.map((decision) => (
                                        <div key={decision.strategy} className="border border-white/10 rounded p-2 bg-black/30">
                                            <div className="flex items-center justify-between text-[11px] font-mono">
                                                <span className="text-gray-200 truncate pr-2">{STRATEGY_NAME_MAP[decision.strategy] || decision.strategy}</span>
                                                <span className={`${
                                                    decision.action === 'PROMOTE'
                                                        ? 'text-emerald-300'
                                                        : decision.action === 'DEMOTE_DISABLE'
                                                            ? 'text-rose-300'
                                                            : 'text-gray-300'
                                                }`}>
                                                    {decision.action}
                                                </span>
                                            </div>
                                            <div className="flex items-center justify-between text-[10px] font-mono text-gray-500 mt-1">
                                                <span>n={decision.trades} | win {decision.win_rate_pct.toFixed(1)}%</span>
                                                <span>wf {decision.walk_forward.pass_rate_pct.toFixed(1)}%</span>
                                            </div>
                                            <div className="text-[10px] text-gray-400 truncate" title={decision.reason}>
                                                {decision.reason}
                                            </div>
                                        </div>
                                    ))}
                                </div>
                            </div>

                            <div>
                                <div className="text-[10px] uppercase tracking-wider text-gray-500 mb-2">Recent Governance Audit</div>
                                <div className="space-y-1 max-h-[150px] overflow-y-auto pr-1">
                                    {governanceAuditRows.length === 0 && (
                                        <div className="text-xs text-gray-500 font-mono">No governance actions yet</div>
                                    )}
                                    {governanceAuditRows.map((entry, index) => (
                                        <div key={`${entry.strategy}-${entry.timestamp}-${index}`} className="border border-white/10 rounded p-2 bg-black/20">
                                            <div className="flex justify-between items-center text-[11px] font-mono">
                                                <span className="text-gray-200 truncate pr-2">{STRATEGY_NAME_MAP[entry.strategy] || entry.strategy}</span>
                                                <span className="text-gray-500">{new Date(entry.timestamp).toLocaleTimeString()}</span>
                                            </div>
                                            <div className="text-[10px] font-mono mt-1">
                                                <span className={`${
                                                    entry.action === 'PROMOTE'
                                                        ? 'text-emerald-300'
                                                        : entry.action === 'DEMOTE_DISABLE'
                                                            ? 'text-rose-300'
                                                            : 'text-gray-300'
                                                }`}>
                                                    {entry.action}
                                                </span>
                                                <span className="text-gray-400">
                                                    {' '}score {entry.score.toFixed(1)} | drag {entry.cost_drag_bps.toFixed(1)}bps
                                                </span>
                                            </div>
                                        </div>
                                    ))}
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            {showResetConfirm && (
                <div className="fixed inset-0 z-[100] bg-black/80 backdrop-blur-sm flex items-center justify-center p-4">
                    <div className="w-full max-w-md bg-[#111] border border-amber-500/30 rounded-xl p-6 shadow-2xl">
                        <div className="flex items-center gap-2 text-amber-300 mb-3">
                            <AlertTriangle size={18} />
                            <h3 className="font-bold">Reset Simulation</h3>
                        </div>
                        <p className="text-sm text-gray-300 mb-4">
                            This clears paper bankroll back to <span className="font-mono text-white">{formatUsd(DEFAULT_SIM_BANKROLL)}</span> and resets paper PnL/logs/telemetry. Type <span className="font-mono text-white">RESET</span> to confirm.
                        </p>
                        <input
                            value={resetText}
                            onChange={(e) => setResetText(e.target.value)}
                            placeholder="Type RESET"
                            className="w-full bg-black/50 border border-white/20 rounded px-3 py-2 text-sm text-white mb-4 focus:outline-none focus:border-amber-400"
                        />
                        <label className="flex items-start gap-2 text-xs font-mono text-gray-300 mb-4 select-none">
                            <input
                                type="checkbox"
                                checked={resetForceDefaults}
                                onChange={(e) => setResetForceDefaults(e.target.checked)}
                                className="mt-0.5"
                            />
                            <span>
                                Force default strategy toggles (re-enable defaults, disable default-disabled). Leave unchecked to preserve your current bot on/off settings.
                            </span>
                        </label>
                        <div className="flex justify-end gap-2">
                            <button
                                onClick={() => {
                                    setShowResetConfirm(false);
                                    setResetText('');
                                    setResetForceDefaults(false);
                                }}
                                className="px-3 py-2 text-sm rounded border border-white/20 text-gray-300 hover:bg-white/5"
                            >
                                Cancel
                            </button>
                            <button
                                onClick={resetSimulation}
                                disabled={!canConfirmReset}
                                className={`px-3 py-2 text-sm rounded font-bold ${canConfirmReset ? 'bg-amber-500 text-black hover:bg-amber-400' : 'bg-gray-700 text-gray-400 cursor-not-allowed'}`}
                            >
                                Confirm Reset
                            </button>
                        </div>
                    </div>
                </div>
            )}

            {traceExecutionId && (
                <div className="fixed inset-0 z-[110] bg-black/80 backdrop-blur-sm flex items-center justify-center p-4">
                    <div className="w-full max-w-4xl bg-[#0b0b0b] border border-white/10 rounded-xl p-6 shadow-2xl">
                        <div className="flex items-center justify-between gap-3 mb-4">
                            <div>
                                <h3 className="text-sm font-medium text-gray-200 font-mono">Execution Trace</h3>
                                <div className="text-[10px] font-mono text-gray-500 mt-1">
                                    execution_id: <span className="text-gray-200">{traceExecutionId}</span>
                                </div>
                            </div>
                            <button
                                type="button"
                                onClick={closeExecutionTrace}
                                className="px-3 py-2 text-xs rounded border border-white/20 text-gray-300 hover:bg-white/5 font-mono"
                            >
                                Close
                            </button>
                        </div>

                        {executionTraceLoading && (
                            <div className="text-xs font-mono text-cyan-300">Loading trace...</div>
                        )}
                        {executionTraceError && (
                            <div className="text-xs font-mono text-rose-300">{executionTraceError}</div>
                        )}
                        {executionTrace && (
                            <pre className="mt-3 text-[11px] font-mono text-gray-200 bg-black/40 border border-white/10 rounded p-3 overflow-auto max-h-[70vh]">
{JSON.stringify(executionTrace, null, 2)}
                            </pre>
                        )}
                    </div>
                </div>
            )}
            </div>
        </DashboardLayout>
    );
};
