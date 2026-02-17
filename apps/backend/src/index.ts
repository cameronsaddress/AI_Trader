import express from 'express';
import type { NextFunction, Request, Response } from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';
import cors from 'cors';
import helmet from 'helmet';
import dotenv from 'dotenv';
import fs from 'fs/promises';
import path from 'path';
import { randomUUID } from 'crypto';
import { Side } from '@polymarket/clob-client';

import { MarketDataService } from './services/MarketDataService';
import { ContextBuilder } from './services/ContextBuilder';
import { AgentManager } from './agents/AgentManager';
import { DecisionGate } from './agents/DecisionGate';
import { RiskGuard } from './services/RiskGuard';
import { HyperliquidExecutor, TradingMode } from './services/HyperliquidExecutor';
import { PolymarketPreflightService } from './services/PolymarketPreflightService';
import type { PolymarketLiveExecutionResult } from './services/PolymarketPreflightService';
import { PolymarketSettlementService, settlementEventToExecutionLog } from './services/PolymarketSettlementService';
import { StrategyTradeRecorder } from './services/StrategyTradeRecorder';
import { FeatureRegistryRecorder } from './services/FeatureRegistryRecorder';
import { extractBearerToken } from './middleware/auth';
import { connectRedis, redisClient, subscriber as redisSubscriber } from './config/redis';

dotenv.config();

const app = express();
const httpServer = createServer(app);

const io = new Server(httpServer, {
    cors: {
        origin: (requestOrigin, callback) => {
            const allowed = [
                'http://localhost:3112',
                'http://localhost:5114',
                process.env.FRONTEND_URL,
            ];

            if (!requestOrigin || allowed.includes(requestOrigin) || requestOrigin.match(/^http:\/\/\d+\.\d+\.\d+\.\d+:3112$/)) {
                callback(null, true);
            } else {
                console.warn(`[CORS] Blocked origin: ${requestOrigin}`);
                callback(new Error('Not allowed by CORS'));
            }
        },
        methods: ['GET', 'POST'],
        credentials: true,
    },
});

app.use(helmet());
app.use(cors({
    origin: (requestOrigin, callback) => {
        const allowed = [
            'http://localhost:3112',
            'http://localhost:5114',
            process.env.FRONTEND_URL,
        ];

        if (!requestOrigin || allowed.includes(requestOrigin) || requestOrigin.match(/^http:\/\/\d+\.\d+\.\d+\.\d+:3112$/)) {
            callback(null, true);
        } else {
            callback(new Error('Not allowed by CORS'));
        }
    },
    methods: ['GET', 'POST'],
    credentials: true,
}));
app.use(express.json());

app.get('/health', (_req, res) => {
    res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// ── Types ────────────────────────────────────────────────────────────
// All shared type definitions live in types/backend-types.ts
import type {
    RiskModel, RiskConfig, StrategyTogglePayload, TradingModePayload,
    SimulationResetPayload, StrategyMetric, StrategyPerformance,
    RuntimePhase, RuntimeHealth, RuntimeModuleState,
    StrategyScanState, StrategyQuality,
    DataIntegrityCategory, DataIntegrityAlertSeverity, DataIntegrityCounters,
    StrategyDataIntegrity, DataIntegrityAlert, DataIntegrityState,
    StrategyCostDiagnostics, StrategyTradeSample,
    WalkForwardSplit, WalkForwardSummary,
    StrategyGovernanceDecision, GovernanceState,
    FeatureSnapshot, FeatureRegistrySummary,
    RegimeLabel, MetaControllerState,
    MlPipelineStatus, SignalModelArtifact,
    ModelInferenceEntry, ModelInferenceState,
    ModelDriftPerStrategy, ModelDriftStatus, ModelDriftState,
    ModelPredictionTrace,
    ExecutionTraceEventType, ExecutionTraceEvent, ExecutionTrace,
    MetricFamily, MetricDirection,
    StrategySignalSummary, ComparableGroupSummary,
} from './types/backend-types';

// ── Constants ────────────────────────────────────────────────────────
// All configuration constants live in config/constants.ts
import {
    STRATEGY_IDS, SCANNER_HEARTBEAT_IDS,
    EXECUTION_TRACE_TTL_MS, EXECUTION_TRACE_MAX_EVENTS,
    DEFAULT_SIM_BANKROLL, STRATEGY_METRICS_KEY,
    SIM_BANKROLL_KEY, SIM_LEDGER_CASH_KEY, SIM_LEDGER_RESERVED_KEY,
    SIM_LEDGER_REALIZED_PNL_KEY, SIM_LEDGER_RESERVED_BY_STRATEGY_PREFIX,
    SIM_LEDGER_RESERVED_BY_FAMILY_PREFIX,
    SIM_STRATEGY_CONCENTRATION_CAP_PCT, SIM_FAMILY_CONCENTRATION_CAP_PCT,
    SIM_GLOBAL_UTILIZATION_CAP_PCT,
    STRATEGY_RISK_MULTIPLIER_PREFIX, STRATEGY_WEIGHT_FLOOR, STRATEGY_WEIGHT_CAP,
    STRATEGY_ALLOCATOR_EPSILON, STRATEGY_ALLOCATOR_MIN_SAMPLES,
    STRATEGY_ALLOCATOR_TARGET_SHARPE, DEFAULT_DISABLED_STRATEGIES,
    VAULT_ENABLED, VAULT_BANKROLL_CEILING, VAULT_REDIS_KEY,
    RISK_GUARD_TRAILING_STOP, RISK_GUARD_CONSEC_LOSS_LIMIT,
    RISK_GUARD_CONSEC_LOSS_COOLDOWN_MS, RISK_GUARD_POST_LOSS_COOLDOWN_MS,
    RISK_GUARD_ANTI_MARTINGALE_AFTER, RISK_GUARD_ANTI_MARTINGALE_FACTOR,
    RISK_GUARD_DIRECTION_LIMIT, RISK_GUARD_PROFIT_TAPER_START,
    RISK_GUARD_PROFIT_TAPER_FLOOR, RISK_GUARD_DAILY_TARGET,
    RISK_GUARD_DAY_RESET_HOUR_UTC, RISK_GUARD_STRATEGIES,
    SIGNAL_WINDOW_MS, SIM_RESET_ON_BOOT, RESET_VALIDATION_TRADES_ON_SIM_RESET,
    CONTROL_PLANE_TOKEN,
    INTELLIGENCE_GATE_ENABLED, INTELLIGENCE_GATE_MAX_STALENESS_MS,
    INTELLIGENCE_GATE_MIN_MARGIN, INTELLIGENCE_GATE_CONFIRMATION_WINDOW_MS,
    INTELLIGENCE_GATE_REQUIRE_PEER_CONFIRMATION, INTELLIGENCE_GATE_STRONG_MARGIN,
    INTELLIGENCE_GATE_CONFIRMATION_STRATEGIES, INTELLIGENCE_SCAN_RETENTION_MS,
    DATA_INTEGRITY_ALERT_COOLDOWN_MS, DATA_INTEGRITY_ALERT_MIN_CONSECUTIVE_WARN,
    DATA_INTEGRITY_ALERT_MIN_CONSECUTIVE_CRITICAL, DATA_INTEGRITY_ALERT_RING_LIMIT,
    STRATEGY_SAMPLE_RETENTION,
    STRATEGY_GOVERNANCE_ENABLED, STRATEGY_GOVERNANCE_AUTOPILOT,
    STRATEGY_GOVERNANCE_INTERVAL_MS, STRATEGY_GOVERNANCE_MIN_TRADES,
    STRATEGY_GOVERNANCE_MIN_TRADES_DEMOTE, STRATEGY_GOVERNANCE_ACTION_COOLDOWN_MS,
    STRATEGY_GOVERNANCE_AUDIT_LIMIT, STRATEGY_GOVERNANCE_WALK_FORWARD_SPLITS,
    STRATEGY_GOVERNANCE_MIN_TEST_TRADES,
    FEATURE_REGISTRY_MAX_ROWS, FEATURE_REGISTRY_LABEL_LOOKBACK_MS,
    META_CONTROLLER_ENABLED, META_CONTROLLER_ADVISORY_ONLY, META_CONTROLLER_REFRESH_DEBOUNCE_MS,
    MODEL_PROBABILITY_GATE_ENABLED, MODEL_PROBABILITY_GATE_ENFORCE_PAPER,
    MODEL_PROBABILITY_GATE_ENFORCE_LIVE, LEGACY_BRAIN_LOOP_ENABLED,
    MODEL_PROBABILITY_GATE_MIN_PROB, MODEL_PROBABILITY_GATE_MAX_STALENESS_MS,
    MODEL_PROBABILITY_GATE_REQUIRE_MODEL, MODEL_PROBABILITY_GATE_DISABLE_ON_DRIFT,
    MODEL_PROBABILITY_GATE_DRIFT_DISABLE_MS,
    MODEL_TRAINER_ENABLED, MODEL_TRAINER_INTERVAL_MS,
    MODEL_TRAINER_MIN_LABELED_ROWS, MODEL_TRAINER_MIN_NEW_LABELS, MODEL_TRAINER_MAX_ROWS,
    MODEL_TRAINER_SPLITS, MODEL_TRAINER_PURGE_ROWS, MODEL_TRAINER_EMBARGO_ROWS,
    MODEL_PREDICTION_TRACE_MAX, MODEL_INFERENCE_MAX_TRACKED_ROWS,
    BACKEND_APP_ROOT, FEATURE_REGISTRY_EVENT_LOG_PATH,
    FEATURE_DATASET_MANIFEST_PATH, SIGNAL_MODEL_REPORT_PATH,
    SIGNAL_MODEL_ARTIFACT_PATH,
    BUILD_ACTIVE_PHASE, RUNTIME_OFFLINE_MULTIPLIER, RUNTIME_MODULE_CATALOG,
} from './config/constants';

// Runtime state (not extractable — mutable per-process)
const executionTraces = new Map<string, ExecutionTrace>();

const strategyStatus: Record<string, boolean> = Object.fromEntries(STRATEGY_IDS.map((id) => [id, true]));

// ── Risk Guard state ─────────────────────────────────────────────────
interface RiskGuardState {
    cumPnl: number;
    peakPnl: number;
    consecutiveLosses: number;
    lastLossAt: number;
    lastDirection: string | null;
    consecutiveSameDirLosses: number;
    pausedUntil: number;
    dayStartMs: number;
}
const riskGuardState: Record<string, RiskGuardState> = {};

function computeDayStartMs(): number {
    // Trading day boundary: configurable hour in UTC (default 8 = midnight PST).
    // E.g. RISK_GUARD_DAY_RESET_HOUR_UTC=8 means the "trading day" runs 08:00 UTC → 08:00 UTC.
    const now = new Date();
    const boundary = new Date(now);
    boundary.setUTCHours(RISK_GUARD_DAY_RESET_HOUR_UTC, 0, 0, 0);
    // If we haven't passed today's boundary yet, the day started at yesterday's boundary.
    if (now.getTime() < boundary.getTime()) {
        boundary.setUTCDate(boundary.getUTCDate() - 1);
    }
    return boundary.getTime();
}

function nextDayBoundaryMs(): number {
    const dayStart = computeDayStartMs();
    return dayStart + 86_400_000;
}

function ensureRiskGuardState(strategyId: string): RiskGuardState {
    const dayStart = computeDayStartMs();
    if (!riskGuardState[strategyId] || riskGuardState[strategyId].dayStartMs < dayStart) {
        riskGuardState[strategyId] = {
            cumPnl: 0, peakPnl: 0,
            consecutiveLosses: 0, lastLossAt: 0,
            lastDirection: null, consecutiveSameDirLosses: 0,
            pausedUntil: 0, dayStartMs: dayStart,
        };
    }
    return riskGuardState[strategyId];
}

async function processRiskGuard(strategyId: string, pnl: number, parsed: Record<string, unknown>): Promise<void> {
    if (!RISK_GUARD_STRATEGIES.has(strategyId)) return;

    const state = ensureRiskGuardState(strategyId);
    const now = Date.now();
    const direction = typeof parsed?.side === 'string' ? parsed.side : null;
    const isLoss = pnl < 0;

    // ── Update tracking ──
    state.cumPnl += pnl;
    if (state.cumPnl > state.peakPnl) state.peakPnl = state.cumPnl;

    if (isLoss) {
        state.consecutiveLosses += 1;
        state.lastLossAt = now;
        if (direction && direction === state.lastDirection) {
            state.consecutiveSameDirLosses += 1;
        } else {
            state.consecutiveSameDirLosses = 1;
        }
    } else {
        state.consecutiveLosses = 0;
        state.consecutiveSameDirLosses = 0;
    }
    state.lastDirection = direction;

    // ── Guard 5: Size factor (anti-martingale + profit taper) ──
    let sizeFactor = 1.0;

    // Anti-martingale: halve after consecutive losses
    if (state.consecutiveLosses >= RISK_GUARD_ANTI_MARTINGALE_AFTER) {
        const reductions = state.consecutiveLosses - RISK_GUARD_ANTI_MARTINGALE_AFTER + 1;
        sizeFactor = Math.min(sizeFactor, Math.max(0.1, Math.pow(RISK_GUARD_ANTI_MARTINGALE_FACTOR, reductions)));
    }

    // Profit taper: reduce sizing after big profit run to prevent give-back
    if (RISK_GUARD_PROFIT_TAPER_START > 0 && state.cumPnl > RISK_GUARD_PROFIT_TAPER_START) {
        const excess = state.cumPnl - RISK_GUARD_PROFIT_TAPER_START;
        const taperFactor = Math.max(RISK_GUARD_PROFIT_TAPER_FLOOR, 1.0 - (excess / (RISK_GUARD_PROFIT_TAPER_START * 2)));
        sizeFactor = Math.min(sizeFactor, taperFactor);
    }

    if (sizeFactor < 1.0) {
        await redisClient.set(`risk_guard:size_factor:${strategyId}`, String(sizeFactor));
        await redisClient.expire(`risk_guard:size_factor:${strategyId}`, 7200);
    } else {
        await redisClient.del(`risk_guard:size_factor:${strategyId}`);
    }

    // ── Guard 4: Post-loss cooldown (set Redis key for scanner) ──
    if (isLoss && RISK_GUARD_POST_LOSS_COOLDOWN_MS > 0) {
        const cooldownUntil = now + RISK_GUARD_POST_LOSS_COOLDOWN_MS;
        await redisClient.set(`risk_guard:cooldown:${strategyId}`, String(cooldownUntil));
        await redisClient.expire(`risk_guard:cooldown:${strategyId}`, Math.ceil(RISK_GUARD_POST_LOSS_COOLDOWN_MS / 1000) + 60);
    }

    // ── Check pause triggers (most severe first) ──
    let shouldPause = false;
    let pauseReason = '';
    let pauseDurationMs = 0;

    // Guard 1: Daily trailing stop
    if (RISK_GUARD_TRAILING_STOP > 0) {
        const drawdown = state.peakPnl - state.cumPnl;
        if (drawdown >= RISK_GUARD_TRAILING_STOP) {
            shouldPause = true;
            pauseReason = `trailing_stop: drawdown $${drawdown.toFixed(2)} >= $${RISK_GUARD_TRAILING_STOP}`;
            pauseDurationMs = nextDayBoundaryMs() - now;
        }
    }

    // Guard 8: Daily profit target — once you've made enough, stop trading.
    // Prevents giving back a great day by over-trading into a regime change.
    if (!shouldPause && RISK_GUARD_DAILY_TARGET > 0 && state.cumPnl >= RISK_GUARD_DAILY_TARGET) {
        shouldPause = true;
        pauseReason = `daily_target: cumPnl $${state.cumPnl.toFixed(2)} >= target $${RISK_GUARD_DAILY_TARGET}`;
        pauseDurationMs = nextDayBoundaryMs() - now;
    }

    // Guard 2: Consecutive loss circuit breaker
    if (!shouldPause && state.consecutiveLosses >= RISK_GUARD_CONSEC_LOSS_LIMIT) {
        shouldPause = true;
        pauseReason = `consecutive_losses: ${state.consecutiveLosses} >= ${RISK_GUARD_CONSEC_LOSS_LIMIT}`;
        pauseDurationMs = RISK_GUARD_CONSEC_LOSS_COOLDOWN_MS;
    }

    // Guard 7: Directional diversity
    if (!shouldPause && state.consecutiveSameDirLosses >= RISK_GUARD_DIRECTION_LIMIT) {
        shouldPause = true;
        pauseReason = `direction_lock: ${state.consecutiveSameDirLosses} same-dir losses (${direction}) >= ${RISK_GUARD_DIRECTION_LIMIT}`;
        pauseDurationMs = RISK_GUARD_POST_LOSS_COOLDOWN_MS;
    }

    if (shouldPause && now < state.pausedUntil) {
        // Already paused from a prior trigger, skip re-setting
        return;
    }

    if (shouldPause) {
        state.pausedUntil = now + pauseDurationMs;
        await redisClient.set(`strategy:enabled:${strategyId}`, '0');
        await redisClient.expire(`strategy:enabled:${strategyId}`, 86400); // 24 hours
        strategyStatus[strategyId] = false;
        io.emit('strategy_status_update', strategyStatus);
        console.info(`[RiskGuard] PAUSED ${strategyId}: ${pauseReason} | resume at ${new Date(state.pausedUntil).toISOString()}`);

        // Schedule auto-resume (if not trailing stop to EOD)
        if (pauseDurationMs > 0 && pauseDurationMs < 86_400_000) {
            setTimeout(async () => {
                try {
                    const s = ensureRiskGuardState(strategyId);
                    if (s.pausedUntil <= Date.now()) {
                        await redisClient.set(`strategy:enabled:${strategyId}`, '1');
                        await redisClient.expire(`strategy:enabled:${strategyId}`, 86400); // 24 hours
                        strategyStatus[strategyId] = true;
                        io.emit('strategy_status_update', strategyStatus);
                        console.info(`[RiskGuard] RESUMED ${strategyId} after cooldown`);
                    }
                } catch (err) {
                    console.error(`[RiskGuard] Error resuming ${strategyId}:`, err);
                }
            }, pauseDurationMs);
        }
    }

    io.emit('risk_guard_update', { strategy: strategyId, ...state });

    // Persist risk guard state to Redis so it survives restarts.
    await redisClient.set(`risk_guard:state:${strategyId}`, JSON.stringify(state));
    await redisClient.expire(`risk_guard:state:${strategyId}`, 72 * 60 * 60); // 72 hours
}

// ── Profit Vault: sweep excess cash into protected vault ─────────────
async function sweepProfitsToVault(): Promise<void> {
    if (!VAULT_ENABLED || VAULT_BANKROLL_CEILING <= 0) return;
    try {
        const cash = parseFloat(await redisClient.get('sim_ledger:cash') || '0');
        if (cash <= VAULT_BANKROLL_CEILING) return;
        const excess = Math.round((cash - VAULT_BANKROLL_CEILING) * 100) / 100;
        if (excess <= 0.01) return;

        await redisClient.incrByFloat('sim_ledger:cash', -excess);
        await redisClient.incrByFloat('sim_bankroll', -excess);
        await redisClient.incrByFloat(VAULT_REDIS_KEY, excess);

        const vault = parseFloat(await redisClient.get(VAULT_REDIS_KEY) || '0');
        console.info(`[Vault] Swept $${excess.toFixed(2)} to vault (total locked: $${vault.toFixed(2)})`);
        io.emit('vault_update', { swept: excess, vault: Math.round(vault * 100) / 100, ceiling: VAULT_BANKROLL_CEILING });
    } catch (err) {
        console.error('[Vault] sweep error:', err);
    }
}

// Load risk guard state from Redis on boot.
async function loadRiskGuardStates(): Promise<void> {
    for (const id of RISK_GUARD_STRATEGIES) {
        try {
            const raw = await redisClient.get(`risk_guard:state:${id}`);
            if (!raw) continue;
            const saved = JSON.parse(raw) as RiskGuardState;
            const dayStart = computeDayStartMs();
            if (saved.dayStartMs >= dayStart) {
                riskGuardState[id] = saved;
                console.info(`[RiskGuard] Restored state for ${id}: cumPnl=$${saved.cumPnl.toFixed(2)}, peak=$${saved.peakPnl.toFixed(2)}, consec=${saved.consecutiveLosses}`);
            }
        } catch { /* ignore corrupt state */ }
    }
}

const strategyMetrics: Record<string, StrategyMetric> = Object.fromEntries(
    STRATEGY_IDS.map((id) => [id, { pnl: 0, daily_trades: 0, updated_at: 0 }]),
);
const strategyPerformance: Record<string, StrategyPerformance> = Object.fromEntries(
    STRATEGY_IDS.map((id) => [id, {
        sample_count: 0,
        win_count: 0,
        loss_count: 0,
        ema_return: 0,
        downside_ema: 0,
        return_var_ema: 0,
        multiplier: DEFAULT_DISABLED_STRATEGIES.has(id) ? 0 : 1,
        updated_at: 0,
    }]),
);
const strategyQuality: Record<string, StrategyQuality> = Object.fromEntries(
    STRATEGY_IDS.map((id) => [id, {
        total_scans: 0,
        pass_scans: 0,
        hold_scans: 0,
        threshold_blocks: 0,
        spread_blocks: 0,
        parity_blocks: 0,
        stale_blocks: 0,
        risk_blocks: 0,
        other_blocks: 0,
        last_reason: '',
        updated_at: 0,
    }]),
);
const strategyDataIntegrity: Record<string, StrategyDataIntegrity> = Object.fromEntries(
    STRATEGY_IDS.map((id) => [id, {
        total_scans: 0,
        hold_scans: 0,
        threshold: 0,
        spread: 0,
        parity: 0,
        stale: 0,
        risk: 0,
        other: 0,
        consecutive_holds: 0,
        last_category: null,
        last_reason: '',
        updated_at: 0,
        last_alert_at: 0,
    }]),
);
const strategyCostDiagnostics: Record<string, StrategyCostDiagnostics> = Object.fromEntries(
    STRATEGY_IDS.map((id) => [id, {
        trades: 0,
        notional_sum: 0,
        net_pnl_sum: 0,
        gross_return_sum: 0,
        gross_return_samples: 0,
        net_return_sum: 0,
        cost_drag_sum: 0,
        cost_drag_samples: 0,
        estimated_cost_usd_sum: 0,
        missing_cost_fields: 0,
        avg_cost_drag_bps: 0,
        avg_net_return_bps: 0,
        avg_gross_return_bps: 0,
        updated_at: 0,
    }]),
);
const strategyTradeSamples: Record<string, StrategyTradeSample[]> = Object.fromEntries(
    STRATEGY_IDS.map((id) => [id, []]),
);
const strategyGovernanceLastActionMs: Record<string, number> = Object.fromEntries(
    STRATEGY_IDS.map((id) => [id, 0]),
);
const strategyGovernanceState: GovernanceState = {
    autopilot_enabled: STRATEGY_GOVERNANCE_AUTOPILOT,
    autopilot_effective: false,
    trading_mode: 'PAPER',
    interval_ms: STRATEGY_GOVERNANCE_INTERVAL_MS,
    updated_at: 0,
    decisions: {},
    audit: [],
};
const dataIntegrityState: DataIntegrityState = {
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
    strategies: strategyDataIntegrity,
    recent_alerts: [],
    updated_at: 0,
};
let ledgerHealthState: LedgerHealthState = {
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
        strategy_pct: SIM_STRATEGY_CONCENTRATION_CAP_PCT * 100,
        family_pct: SIM_FAMILY_CONCENTRATION_CAP_PCT * 100,
        utilization_pct: SIM_GLOBAL_UTILIZATION_CAP_PCT * 100,
    },
    strategy_reserved_total: 0,
    family_reserved_total: 0,
    reserved_gap_vs_strategy: 0,
    reserved_gap_vs_family: 0,
    top_strategy_exposure: [],
    top_family_exposure: [],
};
const featureRegistryRows: FeatureSnapshot[] = [];
const featureRegistryIndexById = new Map<string, number>();
const featureRegistryCountsByStrategy: Record<string, { rows: number; labeled_rows: number; unlabeled_rows: number }> = {};
let featureRegistryLabeledRows = 0;
let featureRegistryLeakageViolations = 0;
let featureRegistryUpdatedAt = 0;
const FEATURE_REGISTRY_TRIM_BATCH_ROWS = Math.max(200, Math.floor(FEATURE_REGISTRY_MAX_ROWS * 0.10));
const FEATURE_REGISTRY_TRIM_TRIGGER_ROWS = FEATURE_REGISTRY_MAX_ROWS + FEATURE_REGISTRY_TRIM_BATCH_ROWS;
const FEATURE_REGISTRY_TRIM_TARGET_ROWS = Math.max(1_000, FEATURE_REGISTRY_MAX_ROWS - FEATURE_REGISTRY_TRIM_BATCH_ROWS);
let metaControllerState: MetaControllerState = {
    enabled: META_CONTROLLER_ENABLED,
    advisory_only: META_CONTROLLER_ADVISORY_ONLY,
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
let mlPipelineStatus: MlPipelineStatus = {
    feature_event_log_path: FEATURE_REGISTRY_EVENT_LOG_PATH,
    feature_event_log_rows: 0,
    dataset_manifest_path: FEATURE_DATASET_MANIFEST_PATH,
    dataset_rows: 0,
    dataset_labeled_rows: 0,
    dataset_feature_count: 0,
    dataset_leakage_violations: 0,
    model_report_path: SIGNAL_MODEL_REPORT_PATH,
    model_eligible: false,
    model_rows: 0,
    model_feature_count: 0,
    model_cv_folds: 0,
    model_reason: 'no training report',
    updated_at: 0,
};
let signalModelArtifact: SignalModelArtifact | null = null;
let modelInferenceState: ModelInferenceState = {
    model_path: SIGNAL_MODEL_ARTIFACT_PATH,
    model_loaded: false,
    model_generated_at: null,
    tracked_rows: 0,
    latest: {},
    updated_at: 0,
};
const modelInferenceTimestampByKey = new Map<string, number>();
const MODEL_INFERENCE_TRIM_BATCH_ROWS = Math.max(
    100,
    Math.floor(MODEL_INFERENCE_MAX_TRACKED_ROWS * 0.10),
);
const MODEL_INFERENCE_TRIM_TRIGGER_ROWS = MODEL_INFERENCE_MAX_TRACKED_ROWS + MODEL_INFERENCE_TRIM_BATCH_ROWS;
let modelDriftState: ModelDriftState = {
    status: 'HEALTHY',
    sample_count: 0,
    brier_ema: 0,
    logloss_ema: 0,
    calibration_error_ema: 0,
    accuracy_pct: 0,
    gate_enabled: MODEL_PROBABILITY_GATE_ENABLED,
    gate_enforcing: MODEL_PROBABILITY_GATE_ENABLED && MODEL_PROBABILITY_GATE_ENFORCE_PAPER,
    gate_disabled_until: 0,
    issues: [],
    by_strategy: {},
    updated_at: 0,
};
const modelPredictionByRowId = new Map<string, ModelPredictionTrace>();
let modelTrainerInFlight = false;
let modelTrainerLastLabeledRows = 0;
let governanceCycleInFlight = false;
let lastScannerParseErrorLogMs = 0;
let metaControllerRefreshTimer: NodeJS.Timeout | null = null;
let metaControllerRefreshScheduled = false;
let metaControllerRefreshInFlight = false;
const heartbeats: Record<string, number> = {};
const latestStrategyScans = new Map<string, StrategyScanState>();
const latestStrategyScansByMarket = new Map<string, StrategyScanState>();
const runtimeModules: Record<string, RuntimeModuleState> = Object.fromEntries(
    RUNTIME_MODULE_CATALOG.map((module) => [module.id, {
        ...module,
        heartbeat_ms: 0,
        events: 0,
        last_detail: 'not started',
    }]),
);

const marketDataService = new MarketDataService();
const contextBuilder = new ContextBuilder(marketDataService);
const agentManager = new AgentManager(contextBuilder);
const decisionGate = new DecisionGate();
const riskGuard = new RiskGuard();
const executor = new HyperliquidExecutor();
const polymarketPreflight = new PolymarketPreflightService();
const settlementService = new PolymarketSettlementService(redisClient);
const strategyTradeRecorder = new StrategyTradeRecorder();
const featureRegistryRecorder = new FeatureRegistryRecorder();

function isControlPlaneTokenConfigured(): boolean {
    return CONTROL_PLANE_TOKEN.length > 0;
}

function isControlPlaneAuthorized(token: string | null): boolean {
    if (!isControlPlaneTokenConfigured()) {
        return false;
    }
    return token === CONTROL_PLANE_TOKEN;
}

function getControlTokenFromRequest(req: Request): string | null {
    const authHeader = req.header('authorization');
    const directHeader = req.header('x-control-plane-token');
    return extractBearerToken(authHeader) || extractBearerToken(directHeader);
}

function getControlTokenFromSocket(socket: {
    handshake: {
        auth?: Record<string, unknown>;
        headers?: Record<string, unknown>;
    };
}): string | null {
    const authToken = extractBearerToken(socket.handshake.auth?.token);
    const authHeader = extractBearerToken(socket.handshake.headers?.authorization);
    const directHeader = extractBearerToken(socket.handshake.headers?.['x-control-plane-token']);
    return authToken || authHeader || directHeader;
}

function requireControlPlaneAuth(req: Request, res: Response, next: NextFunction): void {
    if (!isControlPlaneTokenConfigured()) {
        res.status(503).json({ error: 'Control plane token is not configured' });
        return;
    }

    if (!isControlPlaneAuthorized(getControlTokenFromRequest(req))) {
        res.status(401).json({ error: 'Unauthorized control plane request' });
        return;
    }

    next();
}

function enforceSocketControlAuth(socket: { emit: (event: string, payload: Record<string, unknown>) => boolean }, action: string, authorized: boolean): boolean {
    if (authorized) {
        return true;
    }
    socket.emit('auth_error', {
        action,
        message: isControlPlaneTokenConfigured()
            ? 'Unauthorized control plane operation'
            : 'Control plane token is not configured',
    });
    return false;
}

type SimulationLedgerSnapshot = {
    cash: number;
    reserved: number;
    realized_pnl: number;
    equity: number;
    utilization_pct: number;
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
    ledger: SimulationLedgerSnapshot;
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

function getRecentStrategyScans(now = Date.now()): StrategyScanState[] {
    pruneIntelligenceState(now);
    return [...latestStrategyScans.values()].filter((scan) => now - scan.timestamp <= SIGNAL_WINDOW_MS);
}

function getActiveSignalCount(now = Date.now()): number {
    return getRecentStrategyScans(now).length;
}

function getActiveMarketCount(now = Date.now()): number {
    pruneIntelligenceState(now);
    const activeMarkets = new Set<string>();
    for (const scan of latestStrategyScansByMarket.values()) {
        if (now - scan.timestamp <= SIGNAL_WINDOW_MS) {
            activeMarkets.add(scan.market_key);
        }
    }
    return activeMarkets.size;
}

function normalizeMetricUnit(raw: string | null): string {
    if (!raw) {
        return 'RAW';
    }
    const normalized = raw.trim().toUpperCase();
    return normalized.length > 0 ? normalized : 'RAW';
}

function parseMetricFamily(input: unknown): MetricFamily | null {
    const raw = asString(input);
    if (!raw) {
        return null;
    }

    const normalized = raw.trim().toUpperCase();
    const allowed: MetricFamily[] = [
        'ARBITRAGE_EDGE',
        'MOMENTUM',
        'FAIR_VALUE',
        'MARKET_MAKING',
        'ORDER_FLOW',
        'FLOW_PRESSURE',
        'UNKNOWN',
    ];
    return allowed.includes(normalized as MetricFamily) ? normalized as MetricFamily : null;
}

function parseMetricDirection(input: unknown): MetricDirection | null {
    const raw = asString(input);
    if (!raw) {
        return null;
    }
    const normalized = raw.trim().toUpperCase();
    if (normalized === 'ABSOLUTE' || normalized === 'SIGNED') {
        return normalized as MetricDirection;
    }
    return null;
}

function strategyFamily(strategyRaw: string): string {
    const strategy = strategyRaw.trim().toUpperCase();
    if (strategy === 'BTC_5M' || strategy === 'BTC_15M' || strategy === 'ETH_15M' || strategy === 'SOL_15M') {
        return 'FAIR_VALUE';
    }
    if (strategy === 'ATOMIC_ARB' || strategy === 'GRAPH_ARB') {
        return 'ARBITRAGE';
    }
    if (strategy === 'CEX_SNIPER' || strategy === 'CEX_ARB') {
        return 'CEX_MICROSTRUCTURE';
    }
    if (strategy === 'OBI_SCALPER') {
        return 'ORDER_FLOW';
    }
    if (strategy === 'SYNDICATE' || strategy === 'COPY_BOT') {
        return 'FLOW_PRESSURE';
    }
    if (strategy === 'CONVERGENCE_CARRY') {
        return 'CARRY_PARITY';
    }
    if (strategy === 'MAKER_MM' || strategy === 'AS_MARKET_MAKER') {
        return 'MARKET_MAKING';
    }
    if (strategy === 'LONGSHOT_BIAS') {
        return 'BIAS_EXPLOITATION';
    }
    return 'GENERIC';
}

function inferScanDescriptor(strategyRaw: string, signalTypeRaw: string | null, unitRaw: string | null): {
    signal_type: string;
    unit: string;
    metric_family: MetricFamily;
    directionality: MetricDirection;
    comparable_group: string;
} {
    const strategy = strategyRaw.trim().toUpperCase();
    const signalType = (signalTypeRaw || 'UNKNOWN').trim().toUpperCase() || 'UNKNOWN';
    const unit = normalizeMetricUnit(unitRaw);

    if (strategy === 'ATOMIC_ARB') {
        return {
            signal_type: signalType,
            unit: unit === 'RAW' ? 'RATIO' : unit,
            metric_family: 'ARBITRAGE_EDGE',
            directionality: 'ABSOLUTE',
            comparable_group: 'ATOMIC_ARB_NET_EDGE',
        };
    }

    if (strategy === 'GRAPH_ARB') {
        return {
            signal_type: signalType,
            unit: unit === 'RAW' ? 'RATIO' : unit,
            metric_family: 'ARBITRAGE_EDGE',
            directionality: 'ABSOLUTE',
            comparable_group: 'GRAPH_ARB_CONSTRAINT_EDGE',
        };
    }

    if (strategy === 'CEX_SNIPER' || strategy === 'CEX_ARB') {
        return {
            signal_type: signalType,
            unit: unit === 'RAW' ? 'RATIO' : unit,
            metric_family: 'MOMENTUM',
            directionality: 'ABSOLUTE',
            comparable_group: 'CEX_SNIPER_MOMENTUM',
        };
    }

    if (strategy === 'OBI_SCALPER') {
        return {
            signal_type: signalType,
            unit: unit === 'RAW' ? 'RATIO' : unit,
            metric_family: 'ORDER_FLOW',
            directionality: 'ABSOLUTE',
            comparable_group: 'OBI_SCALPER_IMBALANCE',
        };
    }

    if (strategy === 'MAKER_MM') {
        return {
            signal_type: signalType,
            unit: unit === 'RAW' ? 'RATIO' : unit,
            metric_family: 'MARKET_MAKING',
            directionality: 'ABSOLUTE',
            comparable_group: 'MAKER_MM_EXPECTANCY',
        };
    }

    if (strategy === 'AS_MARKET_MAKER') {
        return {
            signal_type: signalType,
            unit: unit === 'RAW' ? 'RATIO' : unit,
            metric_family: 'MARKET_MAKING',
            directionality: 'ABSOLUTE',
            comparable_group: 'AS_MARKET_MAKER_EDGE',
        };
    }

    if (strategy === 'SYNDICATE' || strategy === 'COPY_BOT') {
        return {
            signal_type: signalType,
            unit: unit === 'RAW' ? 'RATIO' : unit,
            metric_family: 'FLOW_PRESSURE',
            directionality: 'ABSOLUTE',
            comparable_group: 'SYNDICATE_FLOW_PRESSURE',
        };
    }

    if (strategy.endsWith('_15M') || strategy.endsWith('_5M')) {
        return {
            signal_type: signalType,
            unit: unit === 'RAW' ? 'PRICE' : unit,
            metric_family: 'FAIR_VALUE',
            directionality: 'SIGNED',
            comparable_group: `${strategy}_FAIR_VALUE`,
        };
    }

    if (strategy === 'CONVERGENCE_CARRY') {
        return {
            signal_type: signalType,
            unit: unit === 'RAW' ? 'PRICE' : unit,
            metric_family: 'FAIR_VALUE',
            directionality: 'SIGNED',
            comparable_group: 'CONVERGENCE_CARRY_PARITY',
        };
    }

    if (strategy === 'LONGSHOT_BIAS') {
        return {
            signal_type: signalType,
            unit: unit === 'RAW' ? 'RATIO' : unit,
            metric_family: 'ARBITRAGE_EDGE',
            directionality: 'ABSOLUTE',
            comparable_group: 'LONGSHOT_BIAS_EDGE',
        };
    }

    if (signalType.includes('MOMENTUM')) {
        return {
            signal_type: signalType,
            unit: unit === 'RAW' ? 'RATIO' : unit,
            metric_family: 'MOMENTUM',
            directionality: 'ABSOLUTE',
            comparable_group: 'MOMENTUM_GENERIC',
        };
    }

    return {
        signal_type: signalType,
        unit,
        metric_family: 'UNKNOWN',
        directionality: 'ABSOLUTE',
        comparable_group: `${strategy || 'UNKNOWN'}_GENERIC`,
    };
}

function getStrategySignalSummaries(now = Date.now()): StrategySignalSummary[] {
    return getRecentStrategyScans(now)
        .map((scan) => ({
            strategy: scan.strategy,
            symbol: scan.symbol,
            market_key: scan.market_key,
            timestamp: scan.timestamp,
            age_ms: now - scan.timestamp,
            passes_threshold: scan.passes_threshold,
            score: scan.score,
            threshold: scan.threshold,
            margin: computeScanMargin(scan),
            normalized_margin: computeNormalizedMargin(scan),
            signal_type: scan.signal_type,
            unit: scan.unit,
            metric_family: scan.metric_family,
            directionality: scan.directionality,
            comparable_group: scan.comparable_group,
            reason: scan.reason,
        }))
        .sort((a, b) => {
            const passDelta = Number(b.passes_threshold) - Number(a.passes_threshold);
            if (passDelta !== 0) {
                return passDelta;
            }
            return a.strategy.localeCompare(b.strategy);
        });
}

function getComparableGroupSummaries(now = Date.now()): ComparableGroupSummary[] {
    const scans = getRecentStrategyScans(now);
    const buckets = new Map<string, {
        comparable_group: string;
        metric_family: MetricFamily;
        unit: string;
        sample_size: number;
        pass_count: number;
        margin_sum: number;
        normalized_margin_sum: number;
        strategies: Set<string>;
    }>();

    for (const scan of scans) {
        const key = `${scan.comparable_group}::${scan.unit}`;
        const existing = buckets.get(key) || {
            comparable_group: scan.comparable_group,
            metric_family: scan.metric_family,
            unit: scan.unit,
            sample_size: 0,
            pass_count: 0,
            margin_sum: 0,
            normalized_margin_sum: 0,
            strategies: new Set<string>(),
        };

        existing.sample_size += 1;
        existing.pass_count += scan.passes_threshold ? 1 : 0;
        existing.margin_sum += computeScanMargin(scan);
        existing.normalized_margin_sum += computeNormalizedMargin(scan);
        existing.strategies.add(scan.strategy);
        buckets.set(key, existing);
    }

    return [...buckets.values()]
        .map((bucket) => ({
            comparable_group: bucket.comparable_group,
            metric_family: bucket.metric_family,
            unit: bucket.unit,
            sample_size: bucket.sample_size,
            pass_rate_pct: bucket.sample_size > 0 ? (bucket.pass_count / bucket.sample_size) * 100 : 0,
            mean_margin: bucket.sample_size > 0 ? bucket.margin_sum / bucket.sample_size : 0,
            mean_normalized_margin: bucket.sample_size > 0
                ? bucket.normalized_margin_sum / bucket.sample_size
                : 0,
            strategies: [...bucket.strategies.values()].sort((a, b) => a.localeCompare(b)),
        }))
        .sort((a, b) => a.comparable_group.localeCompare(b.comparable_group));
}

function asRecord(input: unknown): Record<string, unknown> | null {
    if (!input || typeof input !== 'object' || Array.isArray(input)) {
        return null;
    }
    return input as Record<string, unknown>;
}

function asString(input: unknown): string | null {
    return typeof input === 'string' && input.trim().length > 0 ? input.trim() : null;
}

function asNumber(input: unknown): number | null {
    if (input === null || input === undefined || input === '') {
        return null;
    }
    const parsed = Number(input);
    return Number.isFinite(parsed) ? parsed : null;
}

function normalizeTimestampMs(input: unknown): number {
    const numeric = asNumber(input);
    if (numeric !== null) {
        return Math.trunc(numeric);
    }
    const raw = asString(input);
    if (!raw) {
        return Date.now();
    }
    const parsed = Date.parse(raw);
    return Number.isFinite(parsed) ? parsed : Date.now();
}

const SCAN_META_FEATURE_LIMIT = 32;
const SCAN_META_FEATURE_MAX_DEPTH = 2;

function normalizeFeatureColumn(input: string, prefix = 'meta'): string | null {
    const normalized = `${prefix}_${input}`
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '_')
        .replace(/^_+|_+$/g, '');
    if (!normalized) {
        return null;
    }
    return normalized.slice(0, 48);
}

function collectNumericMetaFeatures(
    input: unknown,
    prefix: string,
    out: Record<string, number>,
    depth = 0,
): void {
    if (Object.keys(out).length >= SCAN_META_FEATURE_LIMIT || depth > SCAN_META_FEATURE_MAX_DEPTH) {
        return;
    }

    if (Array.isArray(input)) {
        return;
    }

    const numeric = asNumber(input);
    if (numeric !== null && Number.isFinite(numeric)) {
        const key = normalizeFeatureColumn(prefix, 'meta');
        if (key && !Object.prototype.hasOwnProperty.call(out, key)) {
            out[key] = numeric;
        }
        return;
    }

    const record = asRecord(input);
    if (!record) {
        return;
    }

    for (const [rawKey, value] of Object.entries(record)) {
        if (Object.keys(out).length >= SCAN_META_FEATURE_LIMIT) {
            break;
        }
        const key = rawKey.trim();
        if (!key) {
            continue;
        }
        const childPrefix = depth === 0 ? key : `${prefix}_${key}`;
        collectNumericMetaFeatures(value, childPrefix, out, depth + 1);
    }
}

function extractScanMetaFeatures(meta: unknown): Record<string, number> {
    const out: Record<string, number> = {};
    collectNumericMetaFeatures(meta, 'meta', out, 0);
    return out;
}

function normalizeMarketKey(input: unknown): string | null {
    const raw = asString(input);
    if (!raw) {
        return null;
    }
    return raw.toLowerCase();
}

function normalizeConditionId(input: unknown): string | null {
    const raw = asString(input);
    if (!raw) {
        return null;
    }
    if (!/^0x[a-fA-F0-9]{64}$/.test(raw)) {
        return null;
    }
    return raw.toLowerCase();
}

function clampProbability(input: unknown, fallback: number): number {
    const parsed = asNumber(input);
    if (parsed === null) {
        return fallback;
    }
    return Math.min(0.99, Math.max(0.01, parsed));
}

function buildStrategyMarketKey(strategy: string, marketKey: string): string {
    return `${strategy}::${marketKey}`;
}

function pruneIntelligenceState(now = Date.now()): void {
    for (const [strategy, scan] of latestStrategyScans.entries()) {
        if (now - scan.timestamp > INTELLIGENCE_SCAN_RETENTION_MS) {
            latestStrategyScans.delete(strategy);
        }
    }

    for (const [key, scan] of latestStrategyScansByMarket.entries()) {
        if (now - scan.timestamp > INTELLIGENCE_SCAN_RETENTION_MS) {
            latestStrategyScansByMarket.delete(key);
        }
    }
}

function extractScanMarketKey(payload: unknown): string | null {
    const record = asRecord(payload);
    if (!record) {
        return null;
    }
    const meta = asRecord(record.meta);
    return normalizeMarketKey(
        asString(record.market_id)
        || asString(meta?.condition_id)
        || asString(meta?.token_id)
        || asString(record.symbol),
    );
}

function inferExecutionStrategyFromMarketLabel(record: Record<string, unknown>): string | null {
    const market = asString(record.market)?.toLowerCase() || '';
    if (!market) {
        return null;
    }
    if (market.includes('cex sniper')) return 'CEX_SNIPER';
    if (market.includes('syndicate')) return 'SYNDICATE';
    if (market.includes('graph arb')) return 'GRAPH_ARB';
    if (market.includes('atomic arb')) return 'ATOMIC_ARB';
    if (market.includes('convergence carry')) return 'CONVERGENCE_CARRY';
    if (market.includes('maker micro-mm') || market.includes('maker mm')) return 'MAKER_MM';
    if (market.includes('as market maker')) return 'AS_MARKET_MAKER';
    if (market.includes('longshot bias')) return 'LONGSHOT_BIAS';
    return null;
}

function extractExecutionStrategy(payload: unknown): string | null {
    const record = asRecord(payload);
    if (!record) {
        return null;
    }

    const details = asRecord(record.details);
    const preflight = details ? asRecord(details.preflight) : null;
    const strategyRaw = asString(preflight?.strategy)
        || asString(details?.strategy)
        || asString(details?.strategy_id)
        || asString(record.strategy)
        || asString(record.strategy_id)
        || inferExecutionStrategyFromMarketLabel(record);

    if (!strategyRaw) {
        return null;
    }

    const strategy = strategyRaw.toUpperCase();
    if (STRATEGY_IDS.includes(strategy)) {
        return strategy;
    }

    if (strategy === 'CEX_ARB') {
        return 'CEX_SNIPER';
    }

    return strategy;
}

function extractExecutionMarketKey(payload: unknown): string | null {
    const record = asRecord(payload);
    if (!record) {
        return null;
    }

    const details = asRecord(record.details);
    const preflight = details ? asRecord(details.preflight) : null;
    const orders = Array.isArray(preflight?.orders) ? preflight.orders : [];

    for (const order of orders) {
        const entry = asRecord(order);
        if (!entry) {
            continue;
        }
        const conditionKey = normalizeMarketKey(entry.condition_id || entry.conditionId);
        if (conditionKey) {
            return conditionKey;
        }
        const tokenKey = normalizeMarketKey(entry.token_id || entry.tokenId);
        if (tokenKey) {
            return tokenKey;
        }
    }

    return normalizeMarketKey(
        asString(record.market_id)
        || asString(preflight?.condition_id)
        || asString(preflight?.conditionId)
        || asString(preflight?.token_id)
        || asString(preflight?.tokenId)
        || asString(details?.condition_id)
        || asString(details?.conditionId)
        || asString(details?.token_id)
        || asString(details?.tokenId)
        || asString(record.symbol)
        || asString(details?.symbol)
        || asString(record.market),
    );
}

function extractExecutionId(payload: unknown): string | null {
    const record = asRecord(payload);
    if (!record) {
        return null;
    }
    const direct = asString(record.execution_id) || asString(record.executionId);
    if (direct) {
        return direct;
    }
    const details = asRecord(record.details);
    return asString(details?.execution_id) || asString(details?.executionId);
}

function ensureExecutionId(payload: unknown): string {
    const existing = extractExecutionId(payload);
    if (existing) {
        return existing;
    }
    const id = randomUUID();
    if (payload && typeof payload === 'object') {
        const record = payload as Record<string, unknown>;
        record.execution_id = id;
        const details = asRecord(record.details);
        if (details) {
            details.execution_id = id;
        }
    }
    return id;
}

function pruneExecutionTraces(now = Date.now()): void {
    for (const [id, trace] of executionTraces.entries()) {
        if (now - trace.updated_at > EXECUTION_TRACE_TTL_MS) {
            executionTraces.delete(id);
        }
    }
}

function snapshotScanForExecution(strategy: string | null, marketKey: string | null): StrategyScanState | null {
    if (!strategy) {
        return null;
    }
    if (marketKey) {
        return latestStrategyScansByMarket.get(buildStrategyMarketKey(strategy, marketKey)) || null;
    }
    return latestStrategyScans.get(strategy) || null;
}

function upsertExecutionTrace(executionId: string, meta: {
    strategy: string | null;
    market_key: string | null;
    scan_snapshot: StrategyScanState | null;
}): ExecutionTrace {
    const now = Date.now();
    const existing = executionTraces.get(executionId);
    if (existing) {
        existing.strategy = existing.strategy || meta.strategy;
        existing.market_key = existing.market_key || meta.market_key;
        existing.scan_snapshot = existing.scan_snapshot || meta.scan_snapshot;
        existing.updated_at = now;
        return existing;
    }

    const created: ExecutionTrace = {
        execution_id: executionId,
        strategy: meta.strategy,
        market_key: meta.market_key,
        created_at: now,
        updated_at: now,
        scan_snapshot: meta.scan_snapshot,
        events: [],
    };
    executionTraces.set(executionId, created);
    pruneExecutionTraces(now);
    return created;
}

function pushExecutionTraceEvent(
    executionId: string,
    type: ExecutionTraceEventType,
    payload: unknown,
    meta: { strategy: string | null; market_key: string | null } = { strategy: null, market_key: null },
): void {
    const trace = upsertExecutionTrace(executionId, {
        strategy: meta.strategy,
        market_key: meta.market_key,
        scan_snapshot: snapshotScanForExecution(meta.strategy, meta.market_key),
    });
    const now = Date.now();
    trace.events.push({ type, timestamp: now, payload });
    if (trace.events.length > EXECUTION_TRACE_MAX_EVENTS) {
        trace.events = trace.events.slice(-EXECUTION_TRACE_MAX_EVENTS);
    }
    trace.updated_at = now;
    pruneExecutionTraces(now);
}

function computeScanMargin(scan: StrategyScanState): number {
    const raw = Math.abs(scan.score) - Math.abs(scan.threshold);
    return scan.passes_threshold ? Math.abs(raw) : -Math.abs(raw);
}

function computeNormalizedMargin(scan: StrategyScanState): number {
    const denominator = Math.max(Math.abs(scan.threshold), 1e-9);
    const raw = (Math.abs(scan.score) - Math.abs(scan.threshold)) / denominator;
    return scan.passes_threshold ? Math.abs(raw) : -Math.abs(raw);
}

function collectPeerSignals(strategy: string, marketKey: string | null, now: number): {
    count: number;
    peers: string[];
    consensus: number;
} {
    if (!marketKey) {
        return { count: 0, peers: [], consensus: 0 };
    }

    const peers: string[] = [];
    let consensus = 0;

    for (const scan of latestStrategyScansByMarket.values()) {
        if (scan.market_key !== marketKey || scan.strategy === strategy) {
            continue;
        }
        if (now - scan.timestamp > INTELLIGENCE_GATE_CONFIRMATION_WINDOW_MS) {
            continue;
        }
        if (!scan.passes_threshold) {
            continue;
        }

        peers.push(scan.strategy);
        const directionalSign = Math.sign(scan.score);
        const signedDirection = directionalSign === 0 ? 1 : directionalSign;
        consensus += signedDirection * Math.max(0, computeNormalizedMargin(scan));
    }

    return { count: peers.length, peers: Array.from(new Set(peers)), consensus };
}

function evaluateIntelligenceGate(payload: unknown, tradingMode: TradingMode): {
    ok: boolean;
    reason?: string;
    strategy?: string;
    marketKey?: string | null;
    scan?: StrategyScanState;
    margin?: number;
    normalizedMargin?: number;
    ageMs?: number;
    peerSignals?: number;
    peerStrategies?: string[];
    peerConsensus?: number;
} {
    if (!INTELLIGENCE_GATE_ENABLED || tradingMode !== 'LIVE') {
        return { ok: true };
    }

    const strategy = extractExecutionStrategy(payload);
    if (!strategy) {
        return { ok: false, reason: 'Execution payload missing strategy identity' };
    }

    const marketKey = extractExecutionMarketKey(payload);
    const scan = (marketKey
        ? latestStrategyScansByMarket.get(buildStrategyMarketKey(strategy, marketKey))
        : null) || latestStrategyScans.get(strategy);
    if (!scan) {
        return {
            ok: false,
            strategy,
            marketKey,
            reason: marketKey
                ? `No scan intelligence found for ${strategy} on ${marketKey}`
                : `No scan intelligence found for ${strategy}`,
        };
    }

    const now = Date.now();
    const ageMs = now - scan.timestamp;
    if (ageMs > INTELLIGENCE_GATE_MAX_STALENESS_MS) {
        return {
            ok: false,
            strategy,
            marketKey,
            scan,
            ageMs,
            reason: `Scan stale for ${strategy} (${ageMs}ms > ${INTELLIGENCE_GATE_MAX_STALENESS_MS}ms)`,
        };
    }

    if (!scan.passes_threshold) {
        return {
            ok: false,
            strategy,
            marketKey,
            scan,
            ageMs,
            reason: `Latest scan for ${strategy} does not pass threshold`,
        };
    }

    const margin = computeScanMargin(scan);
    const normalizedMargin = computeNormalizedMargin(scan);
    if (margin < INTELLIGENCE_GATE_MIN_MARGIN) {
        return {
            ok: false,
            strategy,
            marketKey,
            scan,
            ageMs,
            margin,
            normalizedMargin,
            reason: `Signal margin ${margin.toFixed(6)} below minimum ${INTELLIGENCE_GATE_MIN_MARGIN.toFixed(6)}`,
        };
    }

    const peerSignals = collectPeerSignals(strategy, marketKey, now);
    const needsPeerConfirmation = INTELLIGENCE_GATE_REQUIRE_PEER_CONFIRMATION
        && INTELLIGENCE_GATE_CONFIRMATION_STRATEGIES.has(strategy);
    if (needsPeerConfirmation && peerSignals.count === 0 && normalizedMargin < INTELLIGENCE_GATE_STRONG_MARGIN) {
        return {
            ok: false,
            strategy,
            marketKey,
            scan,
            margin,
            normalizedMargin,
            ageMs,
            peerSignals: peerSignals.count,
            peerStrategies: peerSignals.peers,
            peerConsensus: peerSignals.consensus,
            reason: `No corroborating peer signal for ${strategy} on ${scan.market_key} (margin ${normalizedMargin.toFixed(3)} < strong ${INTELLIGENCE_GATE_STRONG_MARGIN.toFixed(3)})`,
        };
    }

    return {
        ok: true,
        strategy,
        marketKey,
        scan,
        margin,
        normalizedMargin,
        ageMs,
        peerSignals: peerSignals.count,
        peerStrategies: peerSignals.peers,
        peerConsensus: peerSignals.consensus,
    };
}

async function emitSettlementEvents(events: Awaited<ReturnType<typeof settlementService.runCycle>>): Promise<void> {
    if (!events.length) {
        return;
    }
    touchRuntimeModule('SETTLEMENT_ENGINE', 'ONLINE', `processed ${events.length} settlement event(s)`);
    for (const event of events) {
        io.emit('settlement_event', event);
        io.emit('execution_log', settlementEventToExecutionLog(event));
    }
    io.emit('settlement_snapshot', settlementService.getSnapshot());
}

function normalizeRiskConfig(input: Partial<RiskConfig>, preserveTimestamp = false): RiskConfig | null {
    const model = input.model === 'PERCENT' ? 'PERCENT' : input.model === 'FIXED' ? 'FIXED' : null;
    const rawValue = Number(input.value);

    if (!model || !Number.isFinite(rawValue)) {
        return null;
    }

    const value = model === 'PERCENT'
        ? Math.max(0.1, Math.min(5.0, rawValue))
        : Math.max(10, Math.min(5000, rawValue));

    return {
        model,
        value,
        timestamp: preserveTimestamp && Number.isFinite(Number(input.timestamp))
            ? Number(input.timestamp)
            : Date.now(),
    };
}

async function getRiskConfig(): Promise<RiskConfig> {
    const raw = await redisClient.get('system:risk_config');
    if (raw) {
        try {
            const parsed = JSON.parse(raw) as RiskConfig;
            const normalized = normalizeRiskConfig(parsed, true);
            if (normalized) {
                return normalized;
            }
        } catch {
            // ignore malformed payload and fall back to default
        }
    }

    const fallback: RiskConfig = { model: 'FIXED', value: 50, timestamp: Date.now() };
    await redisClient.set('system:risk_config', JSON.stringify(fallback));
    return fallback;
}

function touchRuntimeModule(id: string, health: RuntimeHealth, detail: string): void {
    const module = runtimeModules[id];
    if (!module) {
        return;
    }

    module.health = health;
    module.heartbeat_ms = Date.now();
    module.events += 1;
    module.last_detail = detail;
}

function runtimeModuleSnapshot(now = Date.now()): RuntimeModuleState[] {
    return Object.values(runtimeModules)
        .map((module) => {
            if (module.expected_interval_ms <= 0 || module.health === 'STANDBY') {
                return { ...module };
            }
            const staleMs = now - module.heartbeat_ms;
            if (module.heartbeat_ms > 0 && staleMs > module.expected_interval_ms * RUNTIME_OFFLINE_MULTIPLIER) {
                return {
                    ...module,
                    health: 'OFFLINE' as RuntimeHealth,
                    last_detail: `stale for ${staleMs}ms`,
                };
            }
            return { ...module };
        })
        .sort((a, b) => {
            if (a.phase !== b.phase) {
                return a.phase.localeCompare(b.phase);
            }
            return a.label.localeCompare(b.label);
        });
}

function runtimeStatusPayload(): { phase: RuntimePhase; modules: RuntimeModuleState[]; timestamp: number } {
    const now = Date.now();
    return {
        phase: BUILD_ACTIVE_PHASE,
        modules: runtimeModuleSnapshot(now),
        timestamp: now,
    };
}

function normalizeTradingMode(mode: unknown): TradingMode | null {
    if (typeof mode !== 'string') {
        return null;
    }

    const normalized = mode.trim().toUpperCase();
    if (normalized === 'PAPER' || normalized === 'LIVE') {
        return normalized as TradingMode;
    }
    return null;
}

async function getTradingMode(): Promise<TradingMode> {
    const raw = await redisClient.get('system:trading_mode');
    const normalized = normalizeTradingMode(raw);
    if (normalized) {
        return normalized;
    }

    const fallback: TradingMode = 'PAPER';
    await redisClient.set('system:trading_mode', fallback);
    return fallback;
}

async function setTradingMode(mode: TradingMode): Promise<void> {
    const payload = {
        mode,
        timestamp: Date.now(),
        live_order_posting_enabled: isLiveOrderPostingEnabled(),
    };

    await redisClient.set('system:trading_mode', mode);
    await redisClient.publish('system:trading_mode', JSON.stringify(payload));
    touchRuntimeModule('TRADING_MODE_GUARD', 'ONLINE', `mode set to ${mode}`);
    strategyGovernanceState.trading_mode = mode;
    strategyGovernanceState.autopilot_effective = strategyGovernanceState.autopilot_enabled && mode === 'PAPER';
    strategyGovernanceState.updated_at = Date.now();
    io.emit('strategy_governance_snapshot', governancePayload());
}

function isLiveOrderPostingEnabled(): boolean {
    return process.env.LIVE_ORDER_POSTING_ENABLED === 'true';
}

interface LiveReadinessResult {
    ready: boolean;
    failures: string[];
}

async function isLiveReady(): Promise<LiveReadinessResult> {
    const failures: string[] = [];

    if (!isLiveOrderPostingEnabled()) {
        failures.push('LIVE_ORDER_POSTING_ENABLED is not set');
    }

    const now = Date.now();
    const scannerLastBeat = Math.max(...SCANNER_HEARTBEAT_IDS.map((id) => heartbeats[id] || 0));
    if (now - scannerLastBeat > 15_000) {
        failures.push('Scanner heartbeat stale (no heartbeat in 15s)');
    }

    try {
        await redisClient.ping();
    } catch {
        failures.push('Redis connection failed');
    }

    const preflightReadiness = await polymarketPreflight.getReadinessSnapshot();
    for (const issue of preflightReadiness.failures) {
        failures.push(`Polymarket preflight: ${issue}`);
    }
    if (isLiveOrderPostingEnabled() && !preflightReadiness.clientInitialized) {
        failures.push('Polymarket preflight client is not initialized for live posting');
    }

    if (!MODEL_PROBABILITY_GATE_ENFORCE_LIVE) {
        failures.push('MODEL_PROBABILITY_GATE_ENFORCE_LIVE is disabled');
    }

    return { ready: failures.length === 0, failures };
}

function normalizeResetBankroll(value: unknown): number | null {
    if (value === undefined || value === null) {
        return DEFAULT_SIM_BANKROLL;
    }

    const numeric = Number(value);
    if (!Number.isFinite(numeric) || numeric < 0 || numeric > 1_000_000_000) {
        return null;
    }

    return Math.round(numeric * 100) / 100;
}

async function getSimulationLedgerSnapshot(): Promise<SimulationLedgerSnapshot> {
    const fallbackBankroll = asNumber(await redisClient.get(SIM_BANKROLL_KEY)) ?? DEFAULT_SIM_BANKROLL;
    const cashRaw = asNumber(await redisClient.get(SIM_LEDGER_CASH_KEY));
    const reservedRaw = asNumber(await redisClient.get(SIM_LEDGER_RESERVED_KEY));
    const realizedRaw = asNumber(await redisClient.get(SIM_LEDGER_REALIZED_PNL_KEY));

    const cash = cashRaw ?? fallbackBankroll;
    const reserved = reservedRaw ?? 0;
    const realizedPnl = realizedRaw ?? 0;
    const equity = cash + reserved;
    const utilizationPct = equity > 0 ? (reserved / equity) * 100 : 0;

    if (cashRaw === null) {
        await redisClient.set(SIM_LEDGER_CASH_KEY, cash.toFixed(8));
    }
    if (reservedRaw === null) {
        await redisClient.set(SIM_LEDGER_RESERVED_KEY, reserved.toFixed(8));
    }
    if (realizedRaw === null) {
        await redisClient.set(SIM_LEDGER_REALIZED_PNL_KEY, realizedPnl.toFixed(8));
    }
    await redisClient.set(SIM_BANKROLL_KEY, equity.toFixed(8));

    return {
        cash,
        reserved,
        realized_pnl: realizedPnl,
        equity,
        utilization_pct: utilizationPct,
    };
}

async function reconcileSimulationLedgerWithStrategyMetrics(): Promise<void> {
    const ledger = await getSimulationLedgerSnapshot();
    const cumulativePnl = Object.values(strategyMetrics)
        .reduce((sum, metric) => sum + (asNumber(metric?.pnl) ?? 0), 0);

    const ledgerLooksFresh = Math.abs(ledger.realized_pnl) < 1e-6
        && Math.abs(ledger.reserved) < 1e-6
        && Math.abs(ledger.cash - DEFAULT_SIM_BANKROLL) < 1e-6;
    if (!ledgerLooksFresh || Math.abs(cumulativePnl) < 0.01) {
        return;
    }

    const reconciledCash = Math.max(0, DEFAULT_SIM_BANKROLL + cumulativePnl);
    const reconciledEquity = reconciledCash + ledger.reserved;
    await redisClient.set(SIM_LEDGER_CASH_KEY, reconciledCash.toFixed(8));
    await redisClient.set(SIM_LEDGER_REALIZED_PNL_KEY, cumulativePnl.toFixed(8));
    await redisClient.set(SIM_BANKROLL_KEY, reconciledEquity.toFixed(8));
}

async function scanReservedMap(prefix: string): Promise<Record<string, number>> {
    const entries: Record<string, number> = {};
    const pattern = `${prefix}*`;
    try {
        for await (const key of redisClient.scanIterator({ MATCH: pattern, COUNT: 200 })) {
            if (typeof key !== 'string' || !key.startsWith(prefix)) {
                continue;
            }
            const id = key.slice(prefix.length).trim().toUpperCase();
            if (!id) {
                continue;
            }
            const value = asNumber(await redisClient.get(key)) ?? 0;
            if (!Number.isFinite(value) || Math.abs(value) < 1e-9) {
                continue;
            }
            entries[id] = Math.max(0, value);
        }
    } catch {
        // best-effort snapshot
    }
    return entries;
}

async function clearReservedMaps(): Promise<void> {
    const patterns = [
        `${SIM_LEDGER_RESERVED_BY_STRATEGY_PREFIX}*`,
        `${SIM_LEDGER_RESERVED_BY_FAMILY_PREFIX}*`,
    ];
    for (const pattern of patterns) {
        const deleteBatch: string[] = [];
        try {
            for await (const key of redisClient.scanIterator({ MATCH: pattern, COUNT: 200 })) {
                if (typeof key === 'string') {
                    deleteBatch.push(key);
                }
                if (deleteBatch.length >= 100) {
                    await redisClient.del(deleteBatch);
                    deleteBatch.length = 0;
                }
            }
            if (deleteBatch.length > 0) {
                await redisClient.del(deleteBatch);
            }
        } catch {
            // best-effort cleanup
        }
    }
}

function rankConcentration(entries: Record<string, number>, equity: number, limit = 8): LedgerConcentrationEntry[] {
    const denominator = equity > 0 ? equity : 1;
    return Object.entries(entries)
        .map(([id, reserved]) => ({
            id,
            reserved,
            share_pct: (reserved / denominator) * 100,
        }))
        .sort((a, b) => b.reserved - a.reserved)
        .slice(0, limit)
        .map((entry) => ({
            ...entry,
            reserved: Math.round(entry.reserved * 100) / 100,
            share_pct: Math.round(entry.share_pct * 10) / 10,
        }));
}

async function evaluateLedgerHealth(): Promise<LedgerHealthState> {
    const ledger = await getSimulationLedgerSnapshot();
    const strategyReserved = await scanReservedMap(SIM_LEDGER_RESERVED_BY_STRATEGY_PREFIX);
    const familyReserved = await scanReservedMap(SIM_LEDGER_RESERVED_BY_FAMILY_PREFIX);
    const strategyTotal = Object.values(strategyReserved).reduce((sum, value) => sum + value, 0);
    const familyTotal = Object.values(familyReserved).reduce((sum, value) => sum + value, 0);
    const gapVsStrategy = Math.abs(ledger.reserved - strategyTotal);
    const gapVsFamily = Math.abs(ledger.reserved - familyTotal);
    const issues: string[] = [];
    const equity = Math.max(0, ledger.equity);

    if (ledger.cash < -0.01) {
        issues.push(`cash is negative (${ledger.cash.toFixed(2)})`);
    }
    if (ledger.reserved < -0.01) {
        issues.push(`reserved is negative (${ledger.reserved.toFixed(2)})`);
    }
    if (equity < 0.01 && Math.abs(ledger.realized_pnl) > 0.01) {
        issues.push('equity nearly zero with non-trivial realized pnl');
    }

    const warnGapAbs = Math.max(1, equity * 0.005);
    const criticalGapAbs = Math.max(5, equity * 0.02);
    if (gapVsStrategy > criticalGapAbs) {
        issues.push(`strategy reserved mismatch ${gapVsStrategy.toFixed(2)} exceeds critical ${criticalGapAbs.toFixed(2)}`);
    } else if (gapVsStrategy > warnGapAbs) {
        issues.push(`strategy reserved mismatch ${gapVsStrategy.toFixed(2)} exceeds warn ${warnGapAbs.toFixed(2)}`);
    }
    if (gapVsFamily > criticalGapAbs) {
        issues.push(`family reserved mismatch ${gapVsFamily.toFixed(2)} exceeds critical ${criticalGapAbs.toFixed(2)}`);
    } else if (gapVsFamily > warnGapAbs) {
        issues.push(`family reserved mismatch ${gapVsFamily.toFixed(2)} exceeds warn ${warnGapAbs.toFixed(2)}`);
    }

    const strategyCapPct = SIM_STRATEGY_CONCENTRATION_CAP_PCT * 100;
    const familyCapPct = SIM_FAMILY_CONCENTRATION_CAP_PCT * 100;
    const utilizationCapPct = SIM_GLOBAL_UTILIZATION_CAP_PCT * 100;
    const topStrategies = rankConcentration(strategyReserved, equity);
    const topFamilies = rankConcentration(familyReserved, equity);
    const maxStrategyShare = topStrategies[0]?.share_pct || 0;
    const maxFamilyShare = topFamilies[0]?.share_pct || 0;
    if (maxStrategyShare > strategyCapPct + 0.5) {
        issues.push(`strategy concentration ${maxStrategyShare.toFixed(1)}% above cap ${strategyCapPct.toFixed(1)}%`);
    }
    if (maxFamilyShare > familyCapPct + 0.5) {
        issues.push(`family concentration ${maxFamilyShare.toFixed(1)}% above cap ${familyCapPct.toFixed(1)}%`);
    }
    if (ledger.utilization_pct > utilizationCapPct + 0.5) {
        issues.push(`utilization ${ledger.utilization_pct.toFixed(1)}% above cap ${utilizationCapPct.toFixed(1)}%`);
    }

    const hasCritical = issues.some((issue) => issue.includes('critical') || issue.includes('negative'));
    const status: LedgerHealthStatus = hasCritical
        ? 'CRITICAL'
        : issues.length > 0
            ? 'WARN'
            : 'HEALTHY';

    return {
        status,
        issues: issues.slice(0, 12),
        checked_at: Date.now(),
        ledger: {
            cash: Math.round(ledger.cash * 100) / 100,
            reserved: Math.round(ledger.reserved * 100) / 100,
            realized_pnl: Math.round(ledger.realized_pnl * 100) / 100,
            equity: Math.round(ledger.equity * 100) / 100,
            utilization_pct: Math.round(ledger.utilization_pct * 10) / 10,
        },
        caps: {
            strategy_pct: Math.round(strategyCapPct * 10) / 10,
            family_pct: Math.round(familyCapPct * 10) / 10,
            utilization_pct: Math.round(utilizationCapPct * 10) / 10,
        },
        strategy_reserved_total: Math.round(strategyTotal * 100) / 100,
        family_reserved_total: Math.round(familyTotal * 100) / 100,
        reserved_gap_vs_strategy: Math.round(gapVsStrategy * 100) / 100,
        reserved_gap_vs_family: Math.round(gapVsFamily * 100) / 100,
        top_strategy_exposure: topStrategies,
        top_family_exposure: topFamilies,
    };
}

async function refreshLedgerHealth(): Promise<void> {
    ledgerHealthState = await evaluateLedgerHealth();
    io.emit('ledger_health_update', ledgerHealthState);
    touchRuntimeModule(
        'PNL_LEDGER',
        ledgerHealthState.status === 'CRITICAL'
            ? 'DEGRADED'
            : 'ONLINE',
        ledgerHealthState.status === 'HEALTHY'
            ? 'ledger healthy'
            : `ledger ${ledgerHealthState.status.toLowerCase()}: ${ledgerHealthState.issues[0] || 'issue detected'}`,
    );
}

function clearStrategyMetrics(): void {
    for (const id of STRATEGY_IDS) {
        strategyMetrics[id] = { pnl: 0, daily_trades: 0, updated_at: Date.now() };
    }
}

function clearStrategyQuality(): void {
    for (const id of STRATEGY_IDS) {
        strategyQuality[id] = {
            total_scans: 0,
            pass_scans: 0,
            hold_scans: 0,
            threshold_blocks: 0,
            spread_blocks: 0,
            parity_blocks: 0,
            stale_blocks: 0,
            risk_blocks: 0,
            other_blocks: 0,
            last_reason: '',
            updated_at: Date.now(),
        };
    }
}

function clearStrategyPerformance(): void {
    for (const id of STRATEGY_IDS) {
        strategyPerformance[id] = {
            sample_count: 0,
            win_count: 0,
            loss_count: 0,
            ema_return: 0,
            downside_ema: 0,
            return_var_ema: 0,
            multiplier: DEFAULT_DISABLED_STRATEGIES.has(id) ? 0 : 1,
            updated_at: Date.now(),
        };
    }
}

function clearStrategyCostDiagnostics(): void {
    for (const id of STRATEGY_IDS) {
        strategyCostDiagnostics[id] = {
            trades: 0,
            notional_sum: 0,
            net_pnl_sum: 0,
            gross_return_sum: 0,
            gross_return_samples: 0,
            net_return_sum: 0,
            cost_drag_sum: 0,
            cost_drag_samples: 0,
            estimated_cost_usd_sum: 0,
            missing_cost_fields: 0,
            avg_cost_drag_bps: 0,
            avg_net_return_bps: 0,
            avg_gross_return_bps: 0,
            updated_at: Date.now(),
        };
    }
}

function clearStrategyTradeSamples(): void {
    for (const id of STRATEGY_IDS) {
        strategyTradeSamples[id] = [];
    }
}

function clearDataIntegrityState(): void {
    dataIntegrityState.totals = {
        total_scans: 0,
        hold_scans: 0,
        threshold: 0,
        spread: 0,
        parity: 0,
        stale: 0,
        risk: 0,
        other: 0,
    };
    dataIntegrityState.recent_alerts = [];
    dataIntegrityState.updated_at = Date.now();
    for (const id of STRATEGY_IDS) {
        strategyDataIntegrity[id] = {
            total_scans: 0,
            hold_scans: 0,
            threshold: 0,
            spread: 0,
            parity: 0,
            stale: 0,
            risk: 0,
            other: 0,
            consecutive_holds: 0,
            last_category: null,
            last_reason: '',
            updated_at: Date.now(),
            last_alert_at: 0,
        };
    }
}

function clearGovernanceState(): void {
    strategyGovernanceState.updated_at = Date.now();
    strategyGovernanceState.autopilot_effective = false;
    strategyGovernanceState.trading_mode = 'PAPER';
    strategyGovernanceState.audit = [];
    strategyGovernanceState.decisions = {};
    for (const id of STRATEGY_IDS) {
        strategyGovernanceLastActionMs[id] = 0;
    }
}

function strategyRiskMultiplierKey(strategyId: string): string {
    return `${STRATEGY_RISK_MULTIPLIER_PREFIX}${strategyId}`;
}

function ensureStrategyPerformanceEntry(strategyId: string): StrategyPerformance {
    if (!strategyPerformance[strategyId]) {
        strategyPerformance[strategyId] = {
            sample_count: 0,
            win_count: 0,
            loss_count: 0,
            ema_return: 0,
            downside_ema: 0,
            return_var_ema: 0,
            multiplier: DEFAULT_DISABLED_STRATEGIES.has(strategyId) ? 0 : 1,
            updated_at: 0,
        };
    }
    return strategyPerformance[strategyId];
}

function ensureStrategyQualityEntry(strategyId: string): StrategyQuality {
    if (!strategyQuality[strategyId]) {
        strategyQuality[strategyId] = {
            total_scans: 0,
            pass_scans: 0,
            hold_scans: 0,
            threshold_blocks: 0,
            spread_blocks: 0,
            parity_blocks: 0,
            stale_blocks: 0,
            risk_blocks: 0,
            other_blocks: 0,
            last_reason: '',
            updated_at: 0,
        };
    }
    return strategyQuality[strategyId];
}

function ensureStrategyDataIntegrityEntry(strategyId: string): StrategyDataIntegrity {
    if (!strategyDataIntegrity[strategyId]) {
        strategyDataIntegrity[strategyId] = {
            total_scans: 0,
            hold_scans: 0,
            threshold: 0,
            spread: 0,
            parity: 0,
            stale: 0,
            risk: 0,
            other: 0,
            consecutive_holds: 0,
            last_category: null,
            last_reason: '',
            updated_at: 0,
            last_alert_at: 0,
        };
    }
    return strategyDataIntegrity[strategyId];
}

function ensureStrategyCostDiagnosticsEntry(strategyId: string): StrategyCostDiagnostics {
    if (!strategyCostDiagnostics[strategyId]) {
        strategyCostDiagnostics[strategyId] = {
            trades: 0,
            notional_sum: 0,
            net_pnl_sum: 0,
            gross_return_sum: 0,
            gross_return_samples: 0,
            net_return_sum: 0,
            cost_drag_sum: 0,
            cost_drag_samples: 0,
            estimated_cost_usd_sum: 0,
            missing_cost_fields: 0,
            avg_cost_drag_bps: 0,
            avg_net_return_bps: 0,
            avg_gross_return_bps: 0,
            updated_at: 0,
        };
    }
    return strategyCostDiagnostics[strategyId];
}

function classifyBlockReason(reason: string): DataIntegrityCategory {
    const normalized = reason.toLowerCase();
    if (normalized.includes('stale') || normalized.includes('sequence gap')) {
        return 'stale';
    }
    if (normalized.includes('spread')) {
        return 'spread';
    }
    if (normalized.includes('parity')) {
        return 'parity';
    }
    if (
        normalized.includes('risk budget')
        || normalized.includes('consecutive loss')
        || normalized.includes('trades ')
        || normalized.includes('cooldown')
    ) {
        return 'risk';
    }
    if (normalized.includes('threshold') || normalized.includes('below')) {
        return 'threshold';
    }
    return 'other';
}

function shouldEmitDataIntegrityAlert(
    strategyState: StrategyDataIntegrity,
    category: DataIntegrityCategory,
    now: number,
): DataIntegrityAlertSeverity | null {
    if (now - strategyState.last_alert_at < DATA_INTEGRITY_ALERT_COOLDOWN_MS) {
        return null;
    }
    const consecutive = strategyState.consecutive_holds;
    if (consecutive >= DATA_INTEGRITY_ALERT_MIN_CONSECUTIVE_CRITICAL) {
        return 'CRITICAL';
    }
    if (consecutive >= DATA_INTEGRITY_ALERT_MIN_CONSECUTIVE_WARN && category !== 'threshold') {
        return 'WARN';
    }
    return null;
}

function updateDataIntegrityFromScan(scan: StrategyScanState): DataIntegrityAlert | null {
    const strategyState = ensureStrategyDataIntegrityEntry(scan.strategy);
    const now = Date.now();
    strategyState.total_scans += 1;
    strategyState.updated_at = now;
    strategyState.last_reason = scan.reason;

    dataIntegrityState.totals.total_scans += 1;
    dataIntegrityState.updated_at = now;

    if (scan.passes_threshold) {
        strategyState.consecutive_holds = 0;
        strategyState.last_category = null;
        return null;
    }

    const category = classifyBlockReason(scan.reason);
    strategyState.hold_scans += 1;
    strategyState[category] += 1;
    strategyState.consecutive_holds += 1;
    strategyState.last_category = category;

    dataIntegrityState.totals.hold_scans += 1;
    dataIntegrityState.totals[category] += 1;

    const severity = shouldEmitDataIntegrityAlert(strategyState, category, now);
    if (!severity) {
        return null;
    }

    strategyState.last_alert_at = now;
    const alert: DataIntegrityAlert = {
        id: `${scan.strategy}:${scan.market_key}:${now}`,
        strategy: scan.strategy,
        market_key: scan.market_key,
        category,
        severity,
        reason: scan.reason,
        consecutive_holds: strategyState.consecutive_holds,
        timestamp: now,
    };

    dataIntegrityState.recent_alerts = [
        alert,
        ...dataIntegrityState.recent_alerts,
    ].slice(0, DATA_INTEGRITY_ALERT_RING_LIMIT);
    return alert;
}

function updateStrategyQuality(scan: StrategyScanState): void {
    const state = ensureStrategyQualityEntry(scan.strategy);
    state.total_scans += 1;
    state.last_reason = scan.reason;
    state.updated_at = Date.now();

    if (scan.passes_threshold) {
        state.pass_scans += 1;
    } else {
        state.hold_scans += 1;
        const category = classifyBlockReason(scan.reason);
        if (category === 'threshold') state.threshold_blocks += 1;
        if (category === 'spread') state.spread_blocks += 1;
        if (category === 'parity') state.parity_blocks += 1;
        if (category === 'stale') state.stale_blocks += 1;
        if (category === 'risk') state.risk_blocks += 1;
        if (category === 'other') state.other_blocks += 1;
    }
}

function parseTradeSample(payload: unknown): StrategyTradeSample | null {
    const record = asRecord(payload);
    if (!record) {
        return null;
    }

    const strategy = asString(record.strategy);
    const pnl = asNumber(record.pnl);
    if (!strategy || pnl === null) {
        return null;
    }

    const details = asRecord(record.details);
    const timestamp = asNumber(record.timestamp) ?? Date.now();
    const executionId = asString(record.execution_id)
        || asString(record.executionId)
        || asString(details?.execution_id)
        || asString(details?.executionId);
    const marketKey = extractExecutionMarketKey(record)
        || (executionId ? executionTraces.get(executionId)?.market_key || null : null);
    const rawNotional = asNumber(record.notional)
        ?? asNumber(details?.notional)
        ?? Math.abs(pnl);
    const notional = Math.max(1e-9, rawNotional > 0 ? rawNotional : 1);
    const fallbackNetReturn = pnl / notional;
    const netReturn = asNumber(details?.net_return) ?? fallbackNetReturn;
    const grossReturn = asNumber(details?.gross_return) ?? asNumber(details?.mark_return);
    const roundTripCostRate = asNumber(details?.round_trip_cost_rate);

    let costDrag: number | null = null;
    if (grossReturn !== null) {
        costDrag = grossReturn - netReturn;
    } else if (roundTripCostRate !== null) {
        costDrag = roundTripCostRate;
    }

    const reason = asString(record.reason) || asString(details?.reason) || '';
    const labelEligible = isLabelEligibleReason(reason);

    return {
        strategy,
        timestamp,
        pnl,
        notional,
        net_return: netReturn,
        gross_return: grossReturn,
        cost_drag: costDrag,
        reason,
        label_eligible: labelEligible,
        execution_id: executionId || undefined,
        market_key: marketKey,
    };
}

function isLabelEligibleReason(reason: string): boolean {
    if (!reason) return false;
    const upper = reason.trim().toUpperCase();
    // Signal-driven and time-based exits are usable for ML labelling
    const eligible = ['TP', 'SL', 'TAKE_PROFIT', 'STOP_LOSS', 'SIGNAL_TP', 'SIGNAL_SL',
        'EDGE_DECAY', 'EXPIRY', 'EXPIRY_EXIT', 'TIMEOUT', 'TIME_EXPIRY', 'TIME_EXIT', 'MAX_HOLD'];
    return eligible.includes(upper);
}

function updateStrategyCostDiagnostics(sample: StrategyTradeSample): StrategyCostDiagnostics {
    const entry = ensureStrategyCostDiagnosticsEntry(sample.strategy);
    entry.trades += 1;
    entry.notional_sum += sample.notional;
    entry.net_pnl_sum += sample.pnl;
    entry.net_return_sum += sample.net_return;
    entry.updated_at = Date.now();

    if (sample.gross_return !== null) {
        entry.gross_return_sum += sample.gross_return;
        entry.gross_return_samples += 1;
    }

    if (sample.cost_drag !== null) {
        entry.cost_drag_sum += sample.cost_drag;
        entry.cost_drag_samples += 1;
        entry.estimated_cost_usd_sum += sample.notional * sample.cost_drag;
    } else {
        entry.missing_cost_fields += 1;
    }

    const trades = Math.max(1, entry.trades);
    const grossSamples = Math.max(1, entry.gross_return_samples);
    const costSamples = Math.max(1, entry.cost_drag_samples);
    entry.avg_net_return_bps = (entry.net_return_sum / trades) * 10_000;
    entry.avg_gross_return_bps = (entry.gross_return_sum / grossSamples) * 10_000;
    entry.avg_cost_drag_bps = (entry.cost_drag_sum / costSamples) * 10_000;
    return entry;
}

function pushTradeSample(sample: StrategyTradeSample): void {
    if (!strategyTradeSamples[sample.strategy]) {
        strategyTradeSamples[sample.strategy] = [];
    }
    const bucket = strategyTradeSamples[sample.strategy];
    bucket.push(sample);
    if (bucket.length > STRATEGY_SAMPLE_RETENTION) {
        bucket.splice(0, bucket.length - STRATEGY_SAMPLE_RETENTION);
    }
}

function sampleMean(values: number[]): number {
    if (values.length === 0) {
        return 0;
    }
    return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function sampleStd(values: number[]): number {
    if (values.length < 2) {
        return 0;
    }
    const mean = sampleMean(values);
    const variance = values.reduce((sum, value) => {
        const delta = value - mean;
        return sum + (delta * delta);
    }, 0) / (values.length - 1);
    return Math.sqrt(Math.max(0, variance));
}

function computeMaxDrawdownFromPnls(pnls: number[]): number {
    let equity = 0;
    let peak = 0;
    let worst = 0;
    for (const pnl of pnls) {
        equity += pnl;
        peak = Math.max(peak, equity);
        const drawdown = equity - peak;
        worst = Math.min(worst, drawdown);
    }
    return worst;
}

function computeWalkForwardSummary(samples: StrategyTradeSample[]): WalkForwardSummary {
    if (samples.length < Math.max(2 * STRATEGY_GOVERNANCE_MIN_TEST_TRADES, 20)) {
        return {
            eligible: false,
            splits: [],
            oos_splits: 0,
            pass_splits: 0,
            pass_rate_pct: 0,
            avg_oos_return: 0,
            avg_oos_sharpe: 0,
            worst_oos_drawdown: 0,
        };
    }

    const ordered = [...samples].sort((a, b) => a.timestamp - b.timestamp);
    const n = ordered.length;
    const testSize = Math.max(
        STRATEGY_GOVERNANCE_MIN_TEST_TRADES,
        Math.floor(n / (STRATEGY_GOVERNANCE_WALK_FORWARD_SPLITS + 1)),
    );

    const splits: WalkForwardSplit[] = [];
    for (let index = 0; index < STRATEGY_GOVERNANCE_WALK_FORWARD_SPLITS; index += 1) {
        const trainEnd = testSize * (index + 1);
        const testEnd = Math.min(n, trainEnd + testSize);
        if (trainEnd >= n || (testEnd - trainEnd) < STRATEGY_GOVERNANCE_MIN_TEST_TRADES) {
            break;
        }

        const testRows = ordered.slice(trainEnd, testEnd);
        const returns = testRows.map((row) => row.net_return);
        const pnls = testRows.map((row) => row.pnl);
        const meanReturn = sampleMean(returns);
        const stdReturn = sampleStd(returns);
        const sharpe = stdReturn > 1e-12 ? meanReturn / stdReturn : 0;
        const winRate = testRows.length > 0
            ? (testRows.filter((row) => row.pnl > 0).length / testRows.length)
            : 0;

        splits.push({
            split: index + 1,
            train_trades: trainEnd,
            test_trades: testRows.length,
            oos_mean_return: meanReturn,
            oos_sharpe: sharpe,
            oos_total_pnl: pnls.reduce((sum, pnl) => sum + pnl, 0),
            oos_max_drawdown: computeMaxDrawdownFromPnls(pnls),
            oos_win_rate: winRate,
        });
    }

    if (splits.length === 0) {
        return {
            eligible: false,
            splits: [],
            oos_splits: 0,
            pass_splits: 0,
            pass_rate_pct: 0,
            avg_oos_return: 0,
            avg_oos_sharpe: 0,
            worst_oos_drawdown: 0,
        };
    }

    const passSplits = splits.filter((split) => split.oos_mean_return > 0 && split.oos_sharpe > 0).length;
    return {
        eligible: true,
        splits,
        oos_splits: splits.length,
        pass_splits: passSplits,
        pass_rate_pct: (passSplits / splits.length) * 100,
        avg_oos_return: sampleMean(splits.map((split) => split.oos_mean_return)),
        avg_oos_sharpe: sampleMean(splits.map((split) => split.oos_sharpe)),
        worst_oos_drawdown: Math.min(...splits.map((split) => split.oos_max_drawdown)),
    };
}

function computeGovernanceConfidence(trades: number, walkForward: WalkForwardSummary): number {
    const sampleConfidence = Math.min(1, trades / Math.max(STRATEGY_GOVERNANCE_MIN_TRADES_DEMOTE, 60));
    const wfConfidence = walkForward.eligible ? Math.min(1, walkForward.oos_splits / STRATEGY_GOVERNANCE_WALK_FORWARD_SPLITS) : 0;
    return Math.min(1, (sampleConfidence * 0.75) + (wfConfidence * 0.25));
}

function evaluateStrategyGovernance(strategyId: string): StrategyGovernanceDecision {
    const samples = strategyTradeSamples[strategyId] || [];
    const trades = samples.length;
    const netPnl = samples.reduce((sum, sample) => sum + sample.pnl, 0);
    const returns = samples.map((sample) => sample.net_return);
    const avgReturn = sampleMean(returns);
    const stdReturn = sampleStd(returns);
    const sharpe = stdReturn > 1e-12 ? avgReturn / stdReturn : 0;
    const winRatePct = trades > 0
        ? (samples.filter((sample) => sample.pnl > 0).length / trades) * 100
        : 0;
    const walkForward = computeWalkForwardSummary(samples);
    const diagnostics = ensureStrategyCostDiagnosticsEntry(strategyId);
    const costDragBps = diagnostics.avg_cost_drag_bps;
    const confidence = computeGovernanceConfidence(trades, walkForward);
    const score = (avgReturn * 10_000 * 0.45) + (sharpe * 35) + ((winRatePct - 50) * 0.8) + ((walkForward.pass_rate_pct - 50) * 0.5);

    let action: StrategyGovernanceDecision['action'] = 'HOLD';
    let reason = `insufficient sample (${trades}/${STRATEGY_GOVERNANCE_MIN_TRADES})`;

    if (trades >= STRATEGY_GOVERNANCE_MIN_TRADES) {
        const healthyWalkForward = !walkForward.eligible || walkForward.pass_rate_pct >= 50;
        if (netPnl > 0 && avgReturn > 0 && sharpe > 0.12 && winRatePct >= 51 && healthyWalkForward) {
            action = 'PROMOTE';
            reason = `positive expectancy (pnl ${netPnl.toFixed(2)}, sharpe ${sharpe.toFixed(2)}, win ${winRatePct.toFixed(1)}%)`;
        } else {
            reason = `holding: edge weak (pnl ${netPnl.toFixed(2)}, sharpe ${sharpe.toFixed(2)}, win ${winRatePct.toFixed(1)}%)`;
        }
    }

    if (
        trades >= STRATEGY_GOVERNANCE_MIN_TRADES_DEMOTE
        && (netPnl <= 0 || avgReturn <= 0 || sharpe < 0 || winRatePct < 45 || (walkForward.eligible && walkForward.pass_rate_pct < 35))
    ) {
        action = 'DEMOTE_DISABLE';
        reason = `negative expectancy (pnl ${netPnl.toFixed(2)}, sharpe ${sharpe.toFixed(2)}, win ${winRatePct.toFixed(1)}%)`;
    }

    const perf = ensureStrategyPerformanceEntry(strategyId);
    const multiplierBefore = perf.multiplier;
    const enabledBefore = Boolean(strategyStatus[strategyId]);
    return {
        strategy: strategyId,
        action,
        reason,
        trades,
        net_pnl: netPnl,
        avg_return: avgReturn,
        sharpe,
        win_rate_pct: winRatePct,
        cost_drag_bps: costDragBps,
        walk_forward: walkForward,
        confidence,
        score,
        timestamp: Date.now(),
        autopilot_applied: false,
        multiplier_before: multiplierBefore,
        multiplier_after: multiplierBefore,
        enabled_before: enabledBefore,
        enabled_after: enabledBefore,
    };
}

async function maybeApplyGovernanceDecision(
    decision: StrategyGovernanceDecision,
    autopilotAllowed: boolean,
): Promise<StrategyGovernanceDecision> {
    if (!autopilotAllowed) {
        return decision;
    }
    const now = Date.now();
    const lastActionAt = strategyGovernanceLastActionMs[decision.strategy] || 0;
    if (now - lastActionAt < STRATEGY_GOVERNANCE_ACTION_COOLDOWN_MS) {
        return decision;
    }

    const perf = ensureStrategyPerformanceEntry(decision.strategy);
    if (decision.action === 'PROMOTE') {
        const promotedMultiplier = Math.min(STRATEGY_WEIGHT_CAP, Math.max(perf.multiplier, 1.1 + (decision.confidence * 0.2)));
        perf.multiplier = promotedMultiplier;
        perf.updated_at = now;
        strategyStatus[decision.strategy] = true;
        await publishStrategyMultiplier(decision.strategy, promotedMultiplier);
        await redisClient.set(`strategy:enabled:${decision.strategy}`, '1');
        await redisClient.expire(`strategy:enabled:${decision.strategy}`, 86400); // 24 hours
        await redisClient.publish('strategy:control', JSON.stringify({
            id: decision.strategy,
            active: true,
            timestamp: now,
            source: 'governance_autopilot',
        }));
        io.emit('strategy_status_update', strategyStatus);
        io.emit('strategy_risk_multiplier_update', {
            strategy: decision.strategy,
            multiplier: promotedMultiplier,
            sample_count: perf.sample_count,
            ema_return: perf.ema_return,
            downside_ema: perf.downside_ema,
            return_var_ema: perf.return_var_ema,
            timestamp: now,
            source: 'governance_autopilot',
        });
        strategyGovernanceLastActionMs[decision.strategy] = now;
        return {
            ...decision,
            autopilot_applied: true,
            multiplier_after: promotedMultiplier,
            enabled_after: true,
        };
    }

    if (decision.action === 'DEMOTE_DISABLE') {
        perf.multiplier = 0;
        perf.updated_at = now;
        strategyStatus[decision.strategy] = false;
        await publishStrategyMultiplier(decision.strategy, 0);
        await redisClient.set(`strategy:enabled:${decision.strategy}`, '0');
        await redisClient.expire(`strategy:enabled:${decision.strategy}`, 86400); // 24 hours
        await redisClient.publish('strategy:control', JSON.stringify({
            id: decision.strategy,
            active: false,
            timestamp: now,
            source: 'governance_autopilot',
        }));
        io.emit('strategy_status_update', strategyStatus);
        io.emit('strategy_risk_multiplier_update', {
            strategy: decision.strategy,
            multiplier: 0,
            sample_count: perf.sample_count,
            ema_return: perf.ema_return,
            downside_ema: perf.downside_ema,
            return_var_ema: perf.return_var_ema,
            timestamp: now,
            source: 'governance_autopilot',
        });
        strategyGovernanceLastActionMs[decision.strategy] = now;
        return {
            ...decision,
            autopilot_applied: true,
            multiplier_after: 0,
            enabled_after: false,
        };
    }

    return decision;
}

function governancePayload(): GovernanceState {
    return {
        autopilot_enabled: strategyGovernanceState.autopilot_enabled,
        autopilot_effective: strategyGovernanceState.autopilot_effective,
        trading_mode: strategyGovernanceState.trading_mode,
        interval_ms: strategyGovernanceState.interval_ms,
        updated_at: strategyGovernanceState.updated_at,
        decisions: Object.fromEntries(
            Object.entries(strategyGovernanceState.decisions).map(([strategy, decision]) => [strategy, {
                ...decision,
                net_pnl: Math.round(decision.net_pnl * 100) / 100,
                avg_return: Math.round(decision.avg_return * 100000) / 100000,
                sharpe: Math.round(decision.sharpe * 1000) / 1000,
                win_rate_pct: Math.round(decision.win_rate_pct * 10) / 10,
                cost_drag_bps: Math.round(decision.cost_drag_bps * 10) / 10,
                confidence: Math.round(decision.confidence * 1000) / 1000,
                score: Math.round(decision.score * 100) / 100,
                walk_forward: {
                    ...decision.walk_forward,
                    pass_rate_pct: Math.round(decision.walk_forward.pass_rate_pct * 10) / 10,
                    avg_oos_return: Math.round(decision.walk_forward.avg_oos_return * 100000) / 100000,
                    avg_oos_sharpe: Math.round(decision.walk_forward.avg_oos_sharpe * 1000) / 1000,
                    worst_oos_drawdown: Math.round(decision.walk_forward.worst_oos_drawdown * 100) / 100,
                },
            }]),
        ),
        audit: strategyGovernanceState.audit.slice(0, STRATEGY_GOVERNANCE_AUDIT_LIMIT).map((decision) => ({
            ...decision,
            net_pnl: Math.round(decision.net_pnl * 100) / 100,
            avg_return: Math.round(decision.avg_return * 100000) / 100000,
            sharpe: Math.round(decision.sharpe * 1000) / 1000,
            win_rate_pct: Math.round(decision.win_rate_pct * 10) / 10,
            cost_drag_bps: Math.round(decision.cost_drag_bps * 10) / 10,
            confidence: Math.round(decision.confidence * 1000) / 1000,
            score: Math.round(decision.score * 100) / 100,
            walk_forward: {
                ...decision.walk_forward,
                pass_rate_pct: Math.round(decision.walk_forward.pass_rate_pct * 10) / 10,
                avg_oos_return: Math.round(decision.walk_forward.avg_oos_return * 100000) / 100000,
                avg_oos_sharpe: Math.round(decision.walk_forward.avg_oos_sharpe * 1000) / 1000,
                worst_oos_drawdown: Math.round(decision.walk_forward.worst_oos_drawdown * 100) / 100,
            },
        })),
    };
}

function dataIntegrityPayload(): DataIntegrityState {
    return {
        totals: {
            ...dataIntegrityState.totals,
        },
        strategies: Object.fromEntries(
            STRATEGY_IDS.map((strategyId) => [strategyId, ensureStrategyDataIntegrityEntry(strategyId)]),
        ),
        recent_alerts: dataIntegrityState.recent_alerts.slice(0, DATA_INTEGRITY_ALERT_RING_LIMIT),
        updated_at: dataIntegrityState.updated_at,
    };
}

function strategyCostDiagnosticsPayload(): Record<string, StrategyCostDiagnostics> {
    return Object.fromEntries(
        STRATEGY_IDS.map((strategyId) => {
            const entry = ensureStrategyCostDiagnosticsEntry(strategyId);
            return [strategyId, {
                ...entry,
                net_pnl_sum: Math.round(entry.net_pnl_sum * 100) / 100,
                notional_sum: Math.round(entry.notional_sum * 100) / 100,
                estimated_cost_usd_sum: Math.round(entry.estimated_cost_usd_sum * 100) / 100,
                avg_cost_drag_bps: Math.round(entry.avg_cost_drag_bps * 10) / 10,
                avg_net_return_bps: Math.round(entry.avg_net_return_bps * 10) / 10,
                avg_gross_return_bps: Math.round(entry.avg_gross_return_bps * 10) / 10,
            }];
        }),
    );
}

function ensureFeatureRegistryCounter(strategy: string): { rows: number; labeled_rows: number; unlabeled_rows: number } {
    if (!featureRegistryCountsByStrategy[strategy]) {
        featureRegistryCountsByStrategy[strategy] = {
            rows: 0,
            labeled_rows: 0,
            unlabeled_rows: 0,
        };
    }
    return featureRegistryCountsByStrategy[strategy];
}

function addFeatureRegistryRowStats(row: FeatureSnapshot): void {
    const counter = ensureFeatureRegistryCounter(row.strategy);
    const labeled = row.label_net_return !== null || row.label_pnl !== null;
    counter.rows += 1;
    if (labeled) {
        counter.labeled_rows += 1;
        featureRegistryLabeledRows += 1;
    } else {
        counter.unlabeled_rows += 1;
    }
}

function removeFeatureRegistryRowStats(row: FeatureSnapshot): void {
    const counter = featureRegistryCountsByStrategy[row.strategy];
    if (!counter) {
        return;
    }

    const labeled = row.label_net_return !== null || row.label_pnl !== null;
    counter.rows = Math.max(0, counter.rows - 1);
    if (labeled) {
        counter.labeled_rows = Math.max(0, counter.labeled_rows - 1);
        featureRegistryLabeledRows = Math.max(0, featureRegistryLabeledRows - 1);
    } else {
        counter.unlabeled_rows = Math.max(0, counter.unlabeled_rows - 1);
    }

    if (counter.rows === 0) {
        delete featureRegistryCountsByStrategy[row.strategy];
    }
}

function markFeatureRegistryRowLabeled(row: FeatureSnapshot): void {
    const counter = ensureFeatureRegistryCounter(row.strategy);
    counter.labeled_rows += 1;
    counter.unlabeled_rows = Math.max(0, counter.unlabeled_rows - 1);
    featureRegistryLabeledRows += 1;
}

function trimFeatureRegistryRowsIfNeeded(): void {
    if (featureRegistryRows.length <= FEATURE_REGISTRY_TRIM_TRIGGER_ROWS) {
        return;
    }

    const removeCount = Math.max(1, featureRegistryRows.length - FEATURE_REGISTRY_TRIM_TARGET_ROWS);
    const removed = featureRegistryRows.splice(0, removeCount);
    for (const row of removed) {
        featureRegistryIndexById.delete(row.id);
        modelPredictionByRowId.delete(row.id);
        removeFeatureRegistryRowStats(row);
    }

    featureRegistryIndexById.clear();
    for (let index = 0; index < featureRegistryRows.length; index += 1) {
        featureRegistryIndexById.set(featureRegistryRows[index].id, index);
    }
}

function featureRegistrySummary(): FeatureRegistrySummary {
    const byStrategy: FeatureRegistrySummary['by_strategy'] = {};
    for (const [strategy, counter] of Object.entries(featureRegistryCountsByStrategy)) {
        if (counter.rows <= 0) {
            continue;
        }
        byStrategy[strategy] = {
            rows: counter.rows,
            labeled_rows: counter.labeled_rows,
            unlabeled_rows: counter.unlabeled_rows,
        };
    }

    const rows = featureRegistryRows.length;
    const labeledRows = Math.min(rows, featureRegistryLabeledRows);
    return {
        rows,
        labeled_rows: labeledRows,
        unlabeled_rows: Math.max(0, rows - labeledRows),
        leakage_violations: featureRegistryLeakageViolations,
        by_strategy: byStrategy,
        latest_rows: featureRegistryRows.slice(-20).reverse(),
        updated_at: featureRegistryUpdatedAt,
    };
}

function clearFeatureRegistry(): void {
    featureRegistryRows.length = 0;
    featureRegistryIndexById.clear();
    for (const strategy of Object.keys(featureRegistryCountsByStrategy)) {
        delete featureRegistryCountsByStrategy[strategy];
    }
    featureRegistryLabeledRows = 0;
    featureRegistryLeakageViolations = 0;
    featureRegistryUpdatedAt = Date.now();
    modelTrainerLastLabeledRows = 0;
}

function clearModelInferenceState(): void {
    modelPredictionByRowId.clear();
    modelInferenceTimestampByKey.clear();
    modelInferenceState = {
        ...modelInferenceState,
        tracked_rows: 0,
        latest: {},
        updated_at: Date.now(),
    };
}

function allowedFamiliesForRegime(regime: RegimeLabel): string[] {
    if (regime === 'TREND') {
        return ['FAIR_VALUE', 'FLOW_PRESSURE', 'ORDER_FLOW', 'CEX_MICROSTRUCTURE', 'BIAS_EXPLOITATION'];
    }
    if (regime === 'MEAN_REVERT') {
        return ['ARBITRAGE', 'CARRY_PARITY', 'MARKET_MAKING', 'ORDER_FLOW', 'BIAS_EXPLOITATION'];
    }
    if (regime === 'LOW_LIQUIDITY') {
        return ['ARBITRAGE', 'MARKET_MAKING', 'BIAS_EXPLOITATION'];
    }
    if (regime === 'CHOP') {
        return ['ARBITRAGE', 'MARKET_MAKING', 'CEX_MICROSTRUCTURE', 'ORDER_FLOW', 'BIAS_EXPLOITATION'];
    }
    return [
        'FAIR_VALUE',
        'FLOW_PRESSURE',
        'ORDER_FLOW',
        'ARBITRAGE',
        'CEX_MICROSTRUCTURE',
        'MARKET_MAKING',
        'CARRY_PARITY',
        'BIAS_EXPLOITATION',
        'GENERIC',
    ];
}

function buildMetaControllerState(now = Date.now()): MetaControllerState {
    const scans = getRecentStrategyScans(now);
    if (!META_CONTROLLER_ENABLED || scans.length === 0) {
        return {
            enabled: META_CONTROLLER_ENABLED,
            advisory_only: META_CONTROLLER_ADVISORY_ONLY,
            regime: 'UNKNOWN',
            confidence: 0,
            signal_count: scans.length,
            pass_rate_pct: 0,
            mean_abs_normalized_margin: 0,
            family_scores: {},
            allowed_families: allowedFamiliesForRegime('UNKNOWN'),
            strategy_overrides: {},
            updated_at: now,
        };
    }

    const passRate = scans.filter((scan) => scan.passes_threshold).length / scans.length;
    const normalizedMargins = scans.map((scan) => computeNormalizedMargin(scan));
    const meanAbsNormalizedMargin = normalizedMargins.length > 0
        ? normalizedMargins.reduce((sum, value) => sum + Math.abs(value), 0) / normalizedMargins.length
        : 0;
    const signs = normalizedMargins.map((value) => Math.sign(value)).filter((value) => value !== 0);
    const positiveSigns = signs.filter((value) => value > 0).length;
    const signSkew = signs.length > 0 ? Math.abs((positiveSigns / signs.length) - 0.5) * 2 : 0;

    let regime: RegimeLabel = 'CHOP';
    if (passRate >= 0.66 && meanAbsNormalizedMargin >= 0.22) {
        regime = 'TREND';
    } else if (passRate <= 0.25 && meanAbsNormalizedMargin <= 0.10) {
        regime = 'LOW_LIQUIDITY';
    } else if (passRate >= 0.35 && passRate <= 0.60 && signSkew <= 0.15) {
        regime = 'MEAN_REVERT';
    } else {
        regime = 'CHOP';
    }

    const confidence = Math.min(
        1,
        Math.max(
            0.1,
            (Math.min(scans.length / 12, 1) * 0.5)
            + (Math.min(meanAbsNormalizedMargin / 0.4, 1) * 0.3)
            + (Math.abs(passRate - 0.5) * 0.4),
        ),
    );
    const familyAccum = new Map<string, { sum: number; count: number }>();
    for (const scan of scans) {
        const family = strategyFamily(scan.strategy);
        const current = familyAccum.get(family) || { sum: 0, count: 0 };
        current.sum += computeNormalizedMargin(scan);
        current.count += 1;
        familyAccum.set(family, current);
    }
    const familyScores: Record<string, number> = {};
    for (const [family, current] of familyAccum.entries()) {
        familyScores[family] = current.count > 0 ? current.sum / current.count : 0;
    }

    const allowedFamilies = allowedFamiliesForRegime(regime);
    const strategyOverrides: MetaControllerState['strategy_overrides'] = {};
    for (const scan of scans) {
        const family = strategyFamily(scan.strategy);
        const familyScore = familyScores[family] ?? 0;
        const allowed = allowedFamilies.includes(family);
        const recommendedMultiplier = allowed
            ? Math.min(1.35, Math.max(0.85, 1 + (familyScore * 0.55)))
            : Math.min(0.85, Math.max(0.45, 0.70 + (familyScore * 0.25)));
        strategyOverrides[scan.strategy] = {
            family,
            recommended_multiplier: recommendedMultiplier,
            rationale: allowed
                ? `${family} allowed in ${regime}; score ${familyScore.toFixed(3)}`
                : `${family} de-prioritized in ${regime}; score ${familyScore.toFixed(3)}`,
        };
    }

    return {
        enabled: META_CONTROLLER_ENABLED,
        advisory_only: META_CONTROLLER_ADVISORY_ONLY,
        regime,
        confidence,
        signal_count: scans.length,
        pass_rate_pct: passRate * 100,
        mean_abs_normalized_margin: meanAbsNormalizedMargin,
        family_scores: familyScores,
        allowed_families: allowedFamilies,
        strategy_overrides: strategyOverrides,
        updated_at: now,
    };
}

function metaControllerPayload(): MetaControllerState {
    return {
        ...metaControllerState,
        confidence: Math.round(metaControllerState.confidence * 1000) / 1000,
        pass_rate_pct: Math.round(metaControllerState.pass_rate_pct * 10) / 10,
        mean_abs_normalized_margin: Math.round(metaControllerState.mean_abs_normalized_margin * 1000) / 1000,
        family_scores: Object.fromEntries(
            Object.entries(metaControllerState.family_scores).map(([family, score]) => [
                family,
                Math.round(score * 1000) / 1000,
            ]),
        ),
        strategy_overrides: Object.fromEntries(
            Object.entries(metaControllerState.strategy_overrides).map(([strategy, override]) => [
                strategy,
                {
                    ...override,
                    recommended_multiplier: Math.round(override.recommended_multiplier * 1000) / 1000,
                },
            ]),
        ),
    };
}

async function refreshMetaController(): Promise<void> {
    metaControllerState = buildMetaControllerState(Date.now());
    io.emit('meta_controller_update', metaControllerPayload());
    // Persist regime to Redis so Rust scanners can read it for sizing adaptation.
    try {
        await redisClient.set('meta_controller:regime', JSON.stringify({
            regime: metaControllerState.regime,
            confidence: Math.round(metaControllerState.confidence * 1000) / 1000,
            signal_count: metaControllerState.signal_count,
            updated_at: metaControllerState.updated_at,
        }));
    } catch { /* best-effort */ }
    touchRuntimeModule(
        'REGIME_ENGINE',
        'ONLINE',
        `regime ${metaControllerState.regime} conf ${metaControllerState.confidence.toFixed(2)}`,
    );
    touchRuntimeModule(
        'ENSEMBLE_ROUTER',
        'ONLINE',
        `${metaControllerState.signal_count} signals, ${metaControllerState.allowed_families.length} families allowed`,
    );
    touchRuntimeModule(
        'UNCERTAINTY_GATE',
        metaControllerState.confidence >= 0.25 && metaControllerState.signal_count >= 3 ? 'ONLINE' : 'DEGRADED',
        metaControllerState.confidence >= 0.25
            ? `confidence ${metaControllerState.confidence.toFixed(2)}`
            : `low confidence ${metaControllerState.confidence.toFixed(2)}`,
    );
}

async function runMetaControllerRefreshSafely(): Promise<void> {
    try {
        await refreshMetaController();
    } catch (error) {
        console.error('[MetaController] refresh error:', error);
        touchRuntimeModule('REGIME_ENGINE', 'DEGRADED', `meta controller error: ${String(error)}`);
        touchRuntimeModule('ENSEMBLE_ROUTER', 'DEGRADED', `meta controller error: ${String(error)}`);
    }
}

function requestMetaControllerRefresh(): void {
    metaControllerRefreshScheduled = true;
    if (metaControllerRefreshTimer) {
        return;
    }

    metaControllerRefreshTimer = setTimeout(() => {
        metaControllerRefreshTimer = null;
        if (metaControllerRefreshInFlight) {
            return;
        }

        metaControllerRefreshInFlight = true;
        void (async () => {
            try {
                while (metaControllerRefreshScheduled) {
                    metaControllerRefreshScheduled = false;
                    await runMetaControllerRefreshSafely();
                }
            } finally {
                metaControllerRefreshInFlight = false;
                if (metaControllerRefreshScheduled && !metaControllerRefreshTimer) {
                    requestMetaControllerRefresh();
                }
            }
        })();
    }, META_CONTROLLER_REFRESH_DEBOUNCE_MS);
}

async function readJsonFileSafe(filePath: string): Promise<Record<string, unknown> | null> {
    try {
        const raw = await fs.readFile(filePath, 'utf8');
        const parsed = JSON.parse(raw);
        return asRecord(parsed);
    } catch {
        return null;
    }
}

function toFiniteNumberArray(input: unknown): number[] {
    if (!Array.isArray(input)) {
        return [];
    }
    return input
        .map((value) => Number(value))
        .filter((value) => Number.isFinite(value));
}

function parseSignalModelArtifact(raw: Record<string, unknown> | null): SignalModelArtifact | null {
    if (!raw) {
        return null;
    }
    const featureColumns = Array.isArray(raw.feature_columns)
        ? raw.feature_columns.filter((value): value is string => typeof value === 'string')
        : [];
    const weights = toFiniteNumberArray(raw.weights);
    const bias = asNumber(raw.bias) ?? 0;
    const normalizationRaw = asRecord(raw.normalization);
    const means = toFiniteNumberArray(normalizationRaw?.means);
    const stds = toFiniteNumberArray(normalizationRaw?.stds);

    if (
        featureColumns.length === 0
        || weights.length !== featureColumns.length
        || means.length !== featureColumns.length
        || stds.length !== featureColumns.length
    ) {
        return null;
    }

    return {
        eligible: raw.eligible !== false,
        generated_at: asString(raw.generated_at),
        feature_columns: featureColumns,
        weights,
        bias,
        normalization: {
            means,
            stds: stds.map((value) => (Math.abs(value) <= 1e-9 ? 1 : value)),
        },
    };
}

function sigmoid(value: number): number {
    if (value >= 0) {
        const z = Math.exp(-value);
        return 1 / (1 + z);
    }
    const z = Math.exp(value);
    return z / (1 + z);
}

function clampUnitProbability(value: number): number {
    return Math.min(1 - 1e-12, Math.max(1e-12, value));
}

function average(values: number[]): number {
    if (values.length === 0) {
        return 0;
    }
    return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function computeLogLossBinary(labels: number[], probabilities: number[]): number {
    if (labels.length === 0 || labels.length !== probabilities.length) {
        return 0;
    }
    let total = 0;
    for (let index = 0; index < labels.length; index += 1) {
        const y = labels[index] > 0 ? 1 : 0;
        const p = clampUnitProbability(probabilities[index]);
        total += -((y * Math.log(p)) + ((1 - y) * Math.log(1 - p)));
    }
    return total / labels.length;
}

function computeBrierScoreBinary(labels: number[], probabilities: number[]): number {
    if (labels.length === 0 || labels.length !== probabilities.length) {
        return 0;
    }
    let total = 0;
    for (let index = 0; index < labels.length; index += 1) {
        const y = labels[index] > 0 ? 1 : 0;
        const p = probabilities[index];
        total += (y - p) ** 2;
    }
    return total / labels.length;
}

function computeAucBinary(labels: number[], probabilities: number[]): number {
    const positives: number[] = [];
    const negatives: number[] = [];
    for (let index = 0; index < labels.length; index += 1) {
        if (labels[index] > 0) {
            positives.push(probabilities[index]);
        } else {
            negatives.push(probabilities[index]);
        }
    }
    if (positives.length === 0 || negatives.length === 0) {
        return 0.5;
    }
    let wins = 0;
    let ties = 0;
    for (const pos of positives) {
        for (const neg of negatives) {
            if (pos > neg) {
                wins += 1;
            } else if (pos === neg) {
                ties += 1;
            }
        }
    }
    const totalPairs = positives.length * negatives.length;
    return totalPairs > 0 ? (wins + (ties * 0.5)) / totalPairs : 0.5;
}

function normalizeFeatureMatrix(matrix: number[][]): {
    normalized: number[][];
    means: number[];
    stds: number[];
} {
    if (matrix.length === 0) {
        return { normalized: [], means: [], stds: [] };
    }
    const cols = matrix[0].length;
    const means = Array.from({ length: cols }, () => 0);
    const stds = Array.from({ length: cols }, () => 1);
    for (let col = 0; col < cols; col += 1) {
        const column = matrix.map((row) => row[col]);
        const mean = average(column);
        const variance = column.length > 1
            ? column.reduce((sum, value) => sum + ((value - mean) ** 2), 0) / (column.length - 1)
            : 0;
        means[col] = mean;
        stds[col] = Math.max(Math.sqrt(Math.max(variance, 0)), 1e-9);
    }
    return {
        normalized: matrix.map((row) => row.map((value, col) => (value - means[col]) / stds[col])),
        means,
        stds,
    };
}

function applyNormalization(matrix: number[][], means: number[], stds: number[]): number[][] {
    if (matrix.length === 0) {
        return [];
    }
    return matrix.map((row) => row.map((value, col) => (value - means[col]) / Math.max(stds[col], 1e-9)));
}

function trainLogisticRegression(
    x: number[][],
    y: number[],
    options: { learningRate: number; epochs: number; l2: number },
): { weights: number[]; bias: number } {
    if (x.length === 0) {
        return { weights: [], bias: 0 };
    }
    const cols = x[0].length;
    const weights = Array.from({ length: cols }, () => 0);
    let bias = 0;
    const n = x.length;
    for (let epoch = 0; epoch < options.epochs; epoch += 1) {
        const gradientWeights = Array.from({ length: cols }, () => 0);
        let gradientBias = 0;
        for (let rowIndex = 0; rowIndex < n; rowIndex += 1) {
            const row = x[rowIndex];
            const yTrue = y[rowIndex] > 0 ? 1 : 0;
            let z = bias;
            for (let col = 0; col < cols; col += 1) {
                z += weights[col] * row[col];
            }
            const probability = sigmoid(z);
            const error = probability - yTrue;
            for (let col = 0; col < cols; col += 1) {
                gradientWeights[col] += error * row[col];
            }
            gradientBias += error;
        }
        for (let col = 0; col < cols; col += 1) {
            const grad = (gradientWeights[col] / n) + (options.l2 * weights[col]);
            weights[col] -= options.learningRate * grad;
        }
        bias -= options.learningRate * (gradientBias / n);
    }
    return { weights, bias };
}

function predictProbabilities(matrix: number[][], weights: number[], bias: number): number[] {
    if (matrix.length === 0) {
        return [];
    }
    const cols = weights.length;
    return matrix.map((row) => {
        let z = bias;
        for (let col = 0; col < cols; col += 1) {
            z += row[col] * weights[col];
        }
        return sigmoid(z);
    });
}

function buildPurgedEmbargoFolds(
    rows: number,
    splits: number,
    purgeRows: number,
    embargoRows: number,
): Array<{ trainIdx: number[]; testIdx: number[] }> {
    if (rows < Math.max(20, splits * 2)) {
        return [];
    }
    const baseSize = Math.floor(rows / splits);
    const extra = rows % splits;
    const ranges: Array<{ start: number; end: number }> = [];
    let cursor = 0;
    for (let split = 0; split < splits; split += 1) {
        const width = baseSize + (split < extra ? 1 : 0);
        const start = cursor;
        const end = Math.min(rows, start + width);
        cursor = end;
        if (end - start > 0) {
            ranges.push({ start, end });
        }
    }

    const folds: Array<{ trainIdx: number[]; testIdx: number[] }> = [];
    for (const range of ranges) {
        const testIdx = Array.from({ length: range.end - range.start }, (_v, idx) => range.start + idx);
        if (testIdx.length < 4) {
            continue;
        }
        const leftEnd = Math.max(0, range.start - purgeRows);
        const rightStart = Math.min(rows, range.end + embargoRows);
        const trainIdx = [
            ...Array.from({ length: leftEnd }, (_v, idx) => idx),
            ...Array.from({ length: rows - rightStart }, (_v, idx) => rightStart + idx),
        ];
        if (trainIdx.length < 20) {
            continue;
        }
        folds.push({ trainIdx, testIdx });
    }
    return folds;
}

async function persistSignalModelArtifacts(
    artifact: Record<string, unknown>,
    report: Record<string, unknown>,
): Promise<void> {
    await fs.mkdir(path.dirname(SIGNAL_MODEL_ARTIFACT_PATH), { recursive: true });
    await fs.mkdir(path.dirname(SIGNAL_MODEL_REPORT_PATH), { recursive: true });
    await fs.writeFile(SIGNAL_MODEL_ARTIFACT_PATH, `${JSON.stringify(artifact, null, 2)}\n`, 'utf8');
    await fs.writeFile(SIGNAL_MODEL_REPORT_PATH, `${JSON.stringify(report, null, 2)}\n`, 'utf8');
}

function buildScanFeatureMap(scan: StrategyScanState): Record<string, number> {
    const margin = computeScanMargin(scan);
    const normalizedMargin = computeNormalizedMargin(scan);
    return {
        ...(scan.meta_features || {}),
        score: scan.score,
        threshold: scan.threshold,
        margin,
        normalized_margin: normalizedMargin,
        pass_flag: scan.passes_threshold ? 1 : 0,
        abs_score: Math.abs(scan.score),
        abs_threshold: Math.abs(scan.threshold),
    };
}

function modelInferencePayload(): ModelInferenceState {
    return {
        ...modelInferenceState,
        latest: Object.fromEntries(
            Object.entries(modelInferenceState.latest).map(([key, value]) => [
                key,
                {
                    ...value,
                    probability_positive: value.probability_positive === null
                        ? null
                        : Math.round(value.probability_positive * 1000) / 1000,
                },
            ]),
        ),
    };
}

function modelDriftPayload(): ModelDriftState {
    return {
        ...modelDriftState,
        brier_ema: Math.round(modelDriftState.brier_ema * 100000) / 100000,
        logloss_ema: Math.round(modelDriftState.logloss_ema * 100000) / 100000,
        calibration_error_ema: Math.round(modelDriftState.calibration_error_ema * 100000) / 100000,
        accuracy_pct: Math.round(modelDriftState.accuracy_pct * 10) / 10,
        by_strategy: Object.fromEntries(
            Object.entries(modelDriftState.by_strategy).map(([strategy, state]) => [
                strategy,
                {
                    ...state,
                    brier_ema: Math.round(state.brier_ema * 100000) / 100000,
                    logloss_ema: Math.round(state.logloss_ema * 100000) / 100000,
                    accuracy_pct: Math.round(state.accuracy_pct * 10) / 10,
                },
            ]),
        ),
    };
}

function trimModelInferenceLatestIfNeeded(): void {
    if (modelInferenceState.tracked_rows <= MODEL_INFERENCE_TRIM_TRIGGER_ROWS) {
        return;
    }

    const removeCount = Math.max(1, modelInferenceState.tracked_rows - MODEL_INFERENCE_MAX_TRACKED_ROWS);
    const ordered = [...modelInferenceTimestampByKey.entries()]
        .sort((a, b) => a[1] - b[1]);

    let removed = 0;
    for (let index = 0; index < removeCount; index += 1) {
        const key = ordered[index]?.[0];
        if (!key) {
            break;
        }
        if (Object.prototype.hasOwnProperty.call(modelInferenceState.latest, key)) {
            delete modelInferenceState.latest[key];
            removed += 1;
        }
        modelInferenceTimestampByKey.delete(key);
    }

    if (removed > 0) {
        modelInferenceState.tracked_rows = Math.max(0, modelInferenceState.tracked_rows - removed);
    }
}

function upsertModelInferenceEntry(key: string, entry: ModelInferenceEntry): ModelInferenceEntry {
    const existed = Object.prototype.hasOwnProperty.call(modelInferenceState.latest, key);
    modelInferenceState.latest[key] = entry;
    modelInferenceTimestampByKey.set(key, entry.timestamp);
    if (!existed) {
        modelInferenceState.tracked_rows += 1;
    }
    trimModelInferenceLatestIfNeeded();
    modelInferenceState.updated_at = Date.now();
    return modelInferenceState.latest[key]!;
}

function pruneModelPredictionTraces(): void {
    if (modelPredictionByRowId.size <= MODEL_PREDICTION_TRACE_MAX) {
        return;
    }
    const overflow = modelPredictionByRowId.size - MODEL_PREDICTION_TRACE_MAX;
    const ordered = [...modelPredictionByRowId.entries()]
        .sort((a, b) => a[1].timestamp - b[1].timestamp);
    for (let index = 0; index < overflow; index += 1) {
        const rowId = ordered[index]?.[0];
        if (rowId) {
            modelPredictionByRowId.delete(rowId);
        }
    }
}

async function refreshSignalModelArtifact(): Promise<void> {
    const parsed = parseSignalModelArtifact(await readJsonFileSafe(SIGNAL_MODEL_ARTIFACT_PATH));
    signalModelArtifact = parsed && parsed.eligible ? parsed : null;
    modelInferenceState = {
        ...modelInferenceState,
        model_path: SIGNAL_MODEL_ARTIFACT_PATH,
        model_loaded: Boolean(signalModelArtifact),
        model_generated_at: signalModelArtifact?.generated_at || null,
        updated_at: Date.now(),
    };
}

function updateModelInferenceFromScan(scan: StrategyScanState, rowId: string | null = null): ModelInferenceEntry {
    const key = buildStrategyMarketKey(scan.strategy, scan.market_key);
    const features = buildScanFeatureMap(scan);
    const probabilityGate = MODEL_PROBABILITY_GATE_MIN_PROB;
    const fallback: ModelInferenceEntry = {
        strategy: scan.strategy,
        market_key: scan.market_key,
        timestamp: scan.timestamp,
        probability_positive: null,
        probability_gate: probabilityGate,
        pass_probability_gate: false,
        model_loaded: false,
        reason: 'model not loaded',
        feature_count: Object.keys(features).length,
        model_generated_at: null,
    };

    if (!signalModelArtifact) {
        const latest = upsertModelInferenceEntry(key, fallback);
        io.emit('model_inference_update', latest);
        if (rowId) {
            modelPredictionByRowId.set(rowId, {
                row_id: rowId,
                strategy: scan.strategy,
                market_key: scan.market_key,
                timestamp: scan.timestamp,
                probability_positive: null,
                probability_gate: latest.probability_gate,
                model_loaded: false,
            });
            pruneModelPredictionTraces();
        }
        return latest;
    }

    const vector: number[] = [];
    for (const featureName of signalModelArtifact.feature_columns) {
        vector.push(features[featureName] ?? 0);
    }

    let z = signalModelArtifact.bias;
    for (let index = 0; index < signalModelArtifact.feature_columns.length; index += 1) {
        const centered = (vector[index] - signalModelArtifact.normalization.means[index])
            / signalModelArtifact.normalization.stds[index];
        z += centered * signalModelArtifact.weights[index];
    }
    const probability = sigmoid(z);

    const latest = upsertModelInferenceEntry(key, {
        strategy: scan.strategy,
        market_key: scan.market_key,
        timestamp: scan.timestamp,
        probability_positive: probability,
        probability_gate: probabilityGate,
        pass_probability_gate: probability >= probabilityGate,
        model_loaded: true,
        reason: probability >= probabilityGate
            ? `prob ${probability.toFixed(3)} >= ${probabilityGate.toFixed(2)}`
            : `prob ${probability.toFixed(3)} < ${probabilityGate.toFixed(2)}`,
        feature_count: signalModelArtifact.feature_columns.length,
        model_generated_at: signalModelArtifact.generated_at,
    });
    io.emit('model_inference_update', latest);
    if (rowId) {
        modelPredictionByRowId.set(rowId, {
            row_id: rowId,
            strategy: scan.strategy,
            market_key: scan.market_key,
            timestamp: scan.timestamp,
            probability_positive: probability,
            probability_gate: probabilityGate,
            model_loaded: true,
        });
        pruneModelPredictionTraces();
    }
    return latest;
}

function updateModelDriftFromFeatureLabel(feature: FeatureSnapshot): void {
    const prediction = modelPredictionByRowId.get(feature.id);
    if (!prediction || prediction.probability_positive === null) {
        return;
    }

    const probability = clampUnitProbability(prediction.probability_positive);
    const label = (feature.label_net_return ?? 0) > 0 ? 1 : 0;
    const brier = (label - probability) ** 2;
    const logloss = -((label * Math.log(probability)) + ((1 - label) * Math.log(1 - probability)));
    const calibrationError = Math.abs(label - probability);
    const accuracy = (probability >= prediction.probability_gate ? 1 : 0) === label ? 1 : 0;
    const alpha = 0.08;
    const now = Date.now();

    if (modelDriftState.sample_count <= 0) {
        modelDriftState.sample_count = 1;
        modelDriftState.brier_ema = brier;
        modelDriftState.logloss_ema = logloss;
        modelDriftState.calibration_error_ema = calibrationError;
        modelDriftState.accuracy_pct = accuracy * 100;
    } else {
        modelDriftState.sample_count += 1;
        modelDriftState.brier_ema = ((1 - alpha) * modelDriftState.brier_ema) + (alpha * brier);
        modelDriftState.logloss_ema = ((1 - alpha) * modelDriftState.logloss_ema) + (alpha * logloss);
        modelDriftState.calibration_error_ema = ((1 - alpha) * modelDriftState.calibration_error_ema) + (alpha * calibrationError);
        modelDriftState.accuracy_pct = ((1 - alpha) * modelDriftState.accuracy_pct) + (alpha * (accuracy * 100));
    }

    const strategyState = modelDriftState.by_strategy[feature.strategy] || {
        sample_count: 0,
        brier_ema: brier,
        logloss_ema: logloss,
        accuracy_pct: accuracy * 100,
        updated_at: now,
    };
    if (strategyState.sample_count <= 0) {
        strategyState.sample_count = 1;
        strategyState.brier_ema = brier;
        strategyState.logloss_ema = logloss;
        strategyState.accuracy_pct = accuracy * 100;
    } else {
        strategyState.sample_count += 1;
        strategyState.brier_ema = ((1 - alpha) * strategyState.brier_ema) + (alpha * brier);
        strategyState.logloss_ema = ((1 - alpha) * strategyState.logloss_ema) + (alpha * logloss);
        strategyState.accuracy_pct = ((1 - alpha) * strategyState.accuracy_pct) + (alpha * (accuracy * 100));
    }
    strategyState.updated_at = now;
    modelDriftState.by_strategy[feature.strategy] = strategyState;

    const issues: string[] = [];
    let status: ModelDriftStatus = 'HEALTHY';
    if (modelDriftState.sample_count >= 50 && modelDriftState.brier_ema >= 0.30) {
        status = 'CRITICAL';
        issues.push(`brier ema ${modelDriftState.brier_ema.toFixed(3)} >= 0.300`);
    } else if (modelDriftState.sample_count >= 25 && modelDriftState.brier_ema >= 0.24) {
        status = 'WARN';
        issues.push(`brier ema ${modelDriftState.brier_ema.toFixed(3)} >= 0.240`);
    }
    if (modelDriftState.sample_count >= 50 && modelDriftState.calibration_error_ema >= 0.25) {
        status = 'CRITICAL';
        issues.push(`calibration error ${modelDriftState.calibration_error_ema.toFixed(3)} >= 0.250`);
    } else if (modelDriftState.sample_count >= 25 && modelDriftState.calibration_error_ema >= 0.18 && status !== 'CRITICAL') {
        status = 'WARN';
        issues.push(`calibration error ${modelDriftState.calibration_error_ema.toFixed(3)} >= 0.180`);
    }

    let gateDisabledUntil = modelDriftState.gate_disabled_until;
    if (
        MODEL_PROBABILITY_GATE_DISABLE_ON_DRIFT
        && status === 'CRITICAL'
        && now >= gateDisabledUntil
    ) {
        gateDisabledUntil = now + MODEL_PROBABILITY_GATE_DRIFT_DISABLE_MS;
        issues.push(`probability gate auto-disabled for ${Math.round(MODEL_PROBABILITY_GATE_DRIFT_DISABLE_MS / 1000)}s`);
    }
    const gateDisabled = now < gateDisabledUntil;
    const gateEnforcing = MODEL_PROBABILITY_GATE_ENABLED
        && MODEL_PROBABILITY_GATE_ENFORCE_PAPER
        && !gateDisabled;

    modelDriftState = {
        ...modelDriftState,
        status,
        gate_enabled: MODEL_PROBABILITY_GATE_ENABLED,
        gate_enforcing: gateEnforcing,
        gate_disabled_until: gateDisabledUntil,
        issues: issues.slice(0, 6),
        updated_at: now,
    };

    touchRuntimeModule(
        'DRIFT_MONITOR',
        status === 'CRITICAL' ? 'DEGRADED' : status === 'WARN' ? 'ONLINE' : 'ONLINE',
        `samples=${modelDriftState.sample_count} brier=${modelDriftState.brier_ema.toFixed(3)} cal=${modelDriftState.calibration_error_ema.toFixed(3)} gate=${gateEnforcing ? 'on' : 'off'}`,
    );
    io.emit('model_drift_update', modelDriftPayload());
    modelPredictionByRowId.delete(feature.id);
}

function clearModelDriftState(): void {
    modelPredictionByRowId.clear();
    modelDriftState = {
        status: 'HEALTHY',
        sample_count: 0,
        brier_ema: 0,
        logloss_ema: 0,
        calibration_error_ema: 0,
        accuracy_pct: 0,
        gate_enabled: MODEL_PROBABILITY_GATE_ENABLED,
        gate_enforcing: MODEL_PROBABILITY_GATE_ENABLED && MODEL_PROBABILITY_GATE_ENFORCE_PAPER,
        gate_disabled_until: 0,
        issues: [],
        by_strategy: {},
        updated_at: Date.now(),
    };
}

function refreshModelDriftRuntime(now = Date.now()): void {
    const gateDisabled = now < modelDriftState.gate_disabled_until;
    const nextGateEnforcing = MODEL_PROBABILITY_GATE_ENABLED
        && MODEL_PROBABILITY_GATE_ENFORCE_PAPER
        && !gateDisabled;
    const changed = modelDriftState.gate_enabled !== MODEL_PROBABILITY_GATE_ENABLED
        || modelDriftState.gate_enforcing !== nextGateEnforcing;
    if (!changed) {
        return;
    }
    modelDriftState = {
        ...modelDriftState,
        gate_enabled: MODEL_PROBABILITY_GATE_ENABLED,
        gate_enforcing: nextGateEnforcing,
        updated_at: now,
    };
    io.emit('model_drift_update', modelDriftPayload());
    touchRuntimeModule(
        'UNCERTAINTY_GATE',
        nextGateEnforcing ? 'ONLINE' : 'DEGRADED',
        nextGateEnforcing
            ? 'probability gate enforcing in PAPER'
            : gateDisabled
                ? `probability gate paused until ${modelDriftState.gate_disabled_until}`
                : 'probability gate not enforcing',
    );
}

function evaluateModelProbabilityGate(payload: unknown, tradingMode: TradingMode): {
    ok: boolean;
    blocked: boolean;
    reason: string;
    strategy: string | null;
    market_key: string | null;
    probability: number | null;
    gate: number;
    model_loaded: boolean;
} {
    const strategy = extractExecutionStrategy(payload);
    const marketKey = extractExecutionMarketKey(payload);
    const enforceMode = (tradingMode === 'PAPER' && MODEL_PROBABILITY_GATE_ENFORCE_PAPER)
        || (tradingMode === 'LIVE' && MODEL_PROBABILITY_GATE_ENFORCE_LIVE);
    if (!MODEL_PROBABILITY_GATE_ENABLED || !enforceMode) {
        return {
            ok: true,
            blocked: false,
            reason: 'model probability gate not enforcing in this mode',
            strategy,
            market_key: marketKey,
            probability: null,
            gate: MODEL_PROBABILITY_GATE_MIN_PROB,
            model_loaded: Boolean(signalModelArtifact),
        };
    }
    if (Date.now() < modelDriftState.gate_disabled_until) {
        return {
            ok: true,
            blocked: false,
            reason: `model gate temporarily disabled due to drift (${Math.max(0, modelDriftState.gate_disabled_until - Date.now())}ms left)`,
            strategy,
            market_key: marketKey,
            probability: null,
            gate: MODEL_PROBABILITY_GATE_MIN_PROB,
            model_loaded: Boolean(signalModelArtifact),
        };
    }
    if (!strategy) {
        return {
            ok: !MODEL_PROBABILITY_GATE_REQUIRE_MODEL,
            blocked: MODEL_PROBABILITY_GATE_REQUIRE_MODEL,
            reason: 'execution payload missing strategy identity for model gate',
            strategy,
            market_key: marketKey,
            probability: null,
            gate: MODEL_PROBABILITY_GATE_MIN_PROB,
            model_loaded: Boolean(signalModelArtifact),
        };
    }

    const exactKey = marketKey ? buildStrategyMarketKey(strategy, marketKey) : null;
    const exact = exactKey ? modelInferenceState.latest[exactKey] : null;
    const strategyRows = Object.values(modelInferenceState.latest)
        .filter((entry) => entry.strategy === strategy)
        .sort((a, b) => b.timestamp - a.timestamp);
    const fallback = exact || (!marketKey ? strategyRows[0] : null);
    if (!fallback) {
        return {
            ok: !MODEL_PROBABILITY_GATE_REQUIRE_MODEL,
            blocked: MODEL_PROBABILITY_GATE_REQUIRE_MODEL,
            reason: marketKey
                ? `no model inference row for ${strategy} on ${marketKey}`
                : `no model inference row for ${strategy}`,
            strategy,
            market_key: marketKey,
            probability: null,
            gate: MODEL_PROBABILITY_GATE_MIN_PROB,
            model_loaded: Boolean(signalModelArtifact),
        };
    }
    const ageMs = Date.now() - fallback.timestamp;
    if (ageMs > MODEL_PROBABILITY_GATE_MAX_STALENESS_MS) {
        return {
            ok: !MODEL_PROBABILITY_GATE_REQUIRE_MODEL,
            blocked: MODEL_PROBABILITY_GATE_REQUIRE_MODEL,
            reason: `model inference stale ${ageMs}ms > ${MODEL_PROBABILITY_GATE_MAX_STALENESS_MS}ms`,
            strategy,
            market_key: fallback.market_key,
            probability: fallback.probability_positive,
            gate: fallback.probability_gate,
            model_loaded: fallback.model_loaded,
        };
    }
    if (fallback.probability_positive === null) {
        return {
            ok: !MODEL_PROBABILITY_GATE_REQUIRE_MODEL,
            blocked: MODEL_PROBABILITY_GATE_REQUIRE_MODEL,
            reason: 'model probability unavailable',
            strategy,
            market_key: fallback.market_key,
            probability: null,
            gate: fallback.probability_gate,
            model_loaded: fallback.model_loaded,
        };
    }
    if (fallback.probability_positive < fallback.probability_gate) {
        return {
            ok: false,
            blocked: true,
            reason: `probability ${fallback.probability_positive.toFixed(3)} below gate ${fallback.probability_gate.toFixed(3)}`,
            strategy,
            market_key: fallback.market_key,
            probability: fallback.probability_positive,
            gate: fallback.probability_gate,
            model_loaded: fallback.model_loaded,
        };
    }
    return {
        ok: true,
        blocked: false,
        reason: `probability ${fallback.probability_positive.toFixed(3)} >= gate ${fallback.probability_gate.toFixed(3)}`,
        strategy,
        market_key: fallback.market_key,
        probability: fallback.probability_positive,
        gate: fallback.probability_gate,
        model_loaded: fallback.model_loaded,
    };
}

function trainSignalModelInProcess(): {
    artifact: Record<string, unknown>;
    report: Record<string, unknown>;
    labeled_rows: number;
} {
    const allLabeled = featureRegistryRows
        .filter((row) => row.label_net_return !== null && (row.label_timestamp ?? row.timestamp) >= row.timestamp)
        .slice()
        .sort((a, b) => a.timestamp - b.timestamp);
    const totalLabeledRows = allLabeled.length;
    const labeled = totalLabeledRows > MODEL_TRAINER_MAX_ROWS
        ? allLabeled.slice(totalLabeledRows - MODEL_TRAINER_MAX_ROWS)
        : allLabeled;
    const trainingRows = labeled.length;
    const generatedAt = new Date().toISOString();

    const featureColumns = Array.from(
        new Set(
            labeled.flatMap((row) => Object.keys(row.features || {})),
        ),
    ).sort();
    const featureCount = featureColumns.length;

    const ineligible = (reason: string, cvFolds = 0): {
        artifact: Record<string, unknown>;
        report: Record<string, unknown>;
        labeled_rows: number;
    } => ({
        artifact: {
            generated_at: generatedAt,
            eligible: false,
            feature_columns: featureColumns,
            weights: [],
            bias: 0,
            normalization: {
                means: [],
                stds: [],
            },
            reason,
        },
        report: {
            generated_at: generatedAt,
            eligible: false,
            reason,
            rows: trainingRows,
            total_labeled_rows: totalLabeledRows,
            feature_count: featureCount,
            cv_folds: cvFolds,
        },
        labeled_rows: totalLabeledRows,
    });

    if (totalLabeledRows < MODEL_TRAINER_MIN_LABELED_ROWS || featureCount === 0) {
        return ineligible(`insufficient labeled rows (${totalLabeledRows}/${MODEL_TRAINER_MIN_LABELED_ROWS})`);
    }

    const x = labeled.map((row) => featureColumns.map((key) => asNumber(row.features[key]) ?? 0));
    const y = labeled.map((row) => ((row.label_net_return ?? 0) > 0 ? 1 : 0));
    const folds = buildPurgedEmbargoFolds(
        trainingRows,
        MODEL_TRAINER_SPLITS,
        MODEL_TRAINER_PURGE_ROWS,
        MODEL_TRAINER_EMBARGO_ROWS,
    );
    if (folds.length < 2) {
        return ineligible('insufficient folds after purge/embargo', folds.length);
    }

    const grid = [
        { learningRate: 0.02, epochs: 120, l2: 0 },
        { learningRate: 0.05, epochs: 200, l2: 0 },
        { learningRate: 0.02, epochs: 180, l2: 1e-3 },
        { learningRate: 0.05, epochs: 260, l2: 1e-2 },
    ];
    let best: {
        learningRate: number;
        epochs: number;
        l2: number;
        avg_logloss: number;
        avg_brier: number;
        avg_auc: number;
        objective: number;
    } | null = null;
    const cvResults: Array<Record<string, number>> = [];

    for (const candidate of grid) {
        const foldLogloss: number[] = [];
        const foldBrier: number[] = [];
        const foldAuc: number[] = [];
        for (const fold of folds) {
            const xTrain = fold.trainIdx.map((idx) => x[idx]);
            const yTrain = fold.trainIdx.map((idx) => y[idx]);
            const xTest = fold.testIdx.map((idx) => x[idx]);
            const yTest = fold.testIdx.map((idx) => y[idx]);
            const normalized = normalizeFeatureMatrix(xTrain);
            const xTrainNorm = normalized.normalized;
            const xTestNorm = applyNormalization(xTest, normalized.means, normalized.stds);
            const trained = trainLogisticRegression(xTrainNorm, yTrain, candidate);
            const probs = predictProbabilities(xTestNorm, trained.weights, trained.bias);
            foldLogloss.push(computeLogLossBinary(yTest, probs));
            foldBrier.push(computeBrierScoreBinary(yTest, probs));
            foldAuc.push(computeAucBinary(yTest, probs));
        }

        const avgLogloss = average(foldLogloss);
        const avgBrier = average(foldBrier);
        const avgAuc = average(foldAuc);
        const objective = avgLogloss + (0.35 * avgBrier) + (0.15 * (1 - avgAuc));
        const row = {
            learning_rate: candidate.learningRate,
            epochs: candidate.epochs,
            l2: candidate.l2,
            avg_logloss: avgLogloss,
            avg_brier: avgBrier,
            avg_auc: avgAuc,
            objective,
        };
        cvResults.push(row);
        if (!best || objective < best.objective) {
            best = {
                learningRate: candidate.learningRate,
                epochs: candidate.epochs,
                l2: candidate.l2,
                avg_logloss: avgLogloss,
                avg_brier: avgBrier,
                avg_auc: avgAuc,
                objective,
            };
        }
    }

    if (!best) {
        return ineligible('model grid search failed', folds.length);
    }

    const normalizedFull = normalizeFeatureMatrix(x);
    const trainedFull = trainLogisticRegression(normalizedFull.normalized, y, {
        learningRate: best.learningRate,
        epochs: best.epochs,
        l2: best.l2,
    });
    const inSampleProbs = predictProbabilities(normalizedFull.normalized, trainedFull.weights, trainedFull.bias);

    const artifact: Record<string, unknown> = {
        generated_at: generatedAt,
        eligible: true,
        feature_columns: featureColumns,
        weights: trainedFull.weights,
        bias: trainedFull.bias,
        normalization: {
            means: normalizedFull.means,
            stds: normalizedFull.stds,
        },
        best_hyperparameters: {
            learning_rate: best.learningRate,
            epochs: best.epochs,
            l2: best.l2,
            objective: best.objective,
        },
        train_rows: trainingRows,
        total_labeled_rows: totalLabeledRows,
    };
    const report: Record<string, unknown> = {
        generated_at: generatedAt,
        eligible: true,
        rows: trainingRows,
        total_labeled_rows: totalLabeledRows,
        feature_count: featureCount,
        cv_folds: folds.length,
        purge_rows: MODEL_TRAINER_PURGE_ROWS,
        embargo_rows: MODEL_TRAINER_EMBARGO_ROWS,
        best_hyperparameters: artifact.best_hyperparameters,
        cv_results: cvResults.sort((a, b) => (asNumber(a.objective) ?? 0) - (asNumber(b.objective) ?? 0)),
        in_sample: {
            logloss: computeLogLossBinary(y, inSampleProbs),
            brier: computeBrierScoreBinary(y, inSampleProbs),
            auc: computeAucBinary(y, inSampleProbs),
            positive_label_rate: average(y),
        },
    };
    return { artifact, report, labeled_rows: totalLabeledRows };
}

async function runInProcessModelTraining(force = false): Promise<void> {
    if (!MODEL_TRAINER_ENABLED || modelTrainerInFlight) {
        return;
    }
    const labeledRows = featureRegistryRows.filter((row) => row.label_net_return !== null).length;
    if (!force) {
        if (labeledRows < MODEL_TRAINER_MIN_LABELED_ROWS && labeledRows === modelTrainerLastLabeledRows) {
            return;
        }
        if (signalModelArtifact && (labeledRows - modelTrainerLastLabeledRows) < MODEL_TRAINER_MIN_NEW_LABELS) {
            return;
        }
    }

    modelTrainerInFlight = true;
    try {
        const trained = trainSignalModelInProcess();
        modelTrainerLastLabeledRows = trained.labeled_rows;
        await persistSignalModelArtifacts(trained.artifact, trained.report);
        await refreshSignalModelArtifact();
        await refreshMlPipelineStatus();
        const eligible = trained.report.eligible === true;
        touchRuntimeModule(
            'ML_TRAINER',
            eligible ? 'ONLINE' : 'STANDBY',
            eligible
                ? `trained with ${trained.labeled_rows} rows`
                : `${asString(trained.report.reason) || 'training not eligible'}`,
        );
    } catch (error) {
        touchRuntimeModule('ML_TRAINER', 'DEGRADED', `in-process training failed: ${String(error)}`);
    } finally {
        modelTrainerInFlight = false;
    }
}

async function refreshMlPipelineStatus(): Promise<void> {
    await refreshSignalModelArtifact();
    const now = Date.now();
    const featureLogRows = await featureRegistryRecorder.countRows().catch(() => 0);
    const datasetManifest = await readJsonFileSafe(FEATURE_DATASET_MANIFEST_PATH);
    const modelReport = await readJsonFileSafe(SIGNAL_MODEL_REPORT_PATH);
    const registrySummary = featureRegistrySummary();

    const datasetRows = asNumber(datasetManifest?.rows) ?? registrySummary.rows;
    const datasetLabeledRows = asNumber(datasetManifest?.labeled_rows) ?? registrySummary.labeled_rows;
    const datasetFeatureCount = asNumber(datasetManifest?.feature_count) ?? (featureRegistryRows[0]
        ? Object.keys(featureRegistryRows[0].features).length
        : 0);
    const datasetLeakageViolations = asNumber(datasetManifest?.leakage_violations) ?? featureRegistryLeakageViolations;

    const modelEligible = modelReport?.eligible === true;
    const modelRows = asNumber(modelReport?.rows) ?? 0;
    const modelFeatureCount = asNumber(modelReport?.feature_count) ?? 0;
    const modelCvFolds = asNumber(modelReport?.cv_folds) ?? 0;
    const modelReason = asString(modelReport?.reason)
        || (!modelReport ? `missing report at ${SIGNAL_MODEL_REPORT_PATH}` : null);

    mlPipelineStatus = {
        feature_event_log_path: featureRegistryRecorder.getPath(),
        feature_event_log_rows: featureLogRows,
        dataset_manifest_path: FEATURE_DATASET_MANIFEST_PATH,
        dataset_rows: datasetRows,
        dataset_labeled_rows: datasetLabeledRows,
        dataset_feature_count: datasetFeatureCount,
        dataset_leakage_violations: datasetLeakageViolations,
        model_report_path: SIGNAL_MODEL_REPORT_PATH,
        model_eligible: modelEligible,
        model_rows: modelRows,
        model_feature_count: modelFeatureCount,
        model_cv_folds: modelCvFolds,
        model_reason: modelReason,
        updated_at: now,
    };

    touchRuntimeModule(
        'FEATURE_REGISTRY',
        datasetRows > 0 && datasetLeakageViolations === 0 ? 'ONLINE' : datasetRows > 0 ? 'DEGRADED' : 'STANDBY',
        `${datasetRows} rows, ${datasetLabeledRows} labeled, leakage ${datasetLeakageViolations}`,
    );
    touchRuntimeModule(
        'ML_TRAINER',
        modelEligible ? 'ONLINE' : datasetLabeledRows >= MODEL_TRAINER_MIN_LABELED_ROWS ? 'DEGRADED' : 'STANDBY',
        modelEligible
            ? `eligible report rows=${modelRows} folds=${modelCvFolds}`
            : `waiting labels ${datasetLabeledRows}/${MODEL_TRAINER_MIN_LABELED_ROWS}${modelReason ? ` (${modelReason})` : ''}`,
    );
    touchRuntimeModule(
        'MODEL_INFERENCE',
        signalModelArtifact ? 'ONLINE' : modelEligible ? 'DEGRADED' : 'STANDBY',
        signalModelArtifact
            ? `active model ${signalModelArtifact.feature_columns.length} features`
            : modelEligible
                ? `report eligible but artifact missing at ${SIGNAL_MODEL_ARTIFACT_PATH}`
                : 'model not eligible yet',
    );
    touchRuntimeModule(
        'DRIFT_MONITOR',
        modelEligible ? 'ONLINE' : 'STANDBY',
        modelEligible
            ? 'baseline drift monitor active'
            : 'drift monitoring requires trained model',
    );

    io.emit('ml_pipeline_status_update', mlPipelineStatus);
    io.emit('model_inference_snapshot', modelInferencePayload());
    io.emit('model_drift_update', modelDriftPayload());
}

function upsertFeatureSnapshot(scan: StrategyScanState): void {
    const id = `${scan.strategy}:${scan.market_key}:${scan.timestamp}`;
    const existingIndex = featureRegistryIndexById.get(id);
    const features = buildScanFeatureMap(scan);
    const snapshot: FeatureSnapshot = {
        id,
        strategy: scan.strategy,
        market_key: scan.market_key,
        timestamp: scan.timestamp,
        signal_type: scan.signal_type,
        metric_family: scan.metric_family,
        features,
        label_net_return: null,
        label_pnl: null,
        label_timestamp: null,
        label_source: null,
    };

    if (
        existingIndex !== undefined
        && existingIndex >= 0
        && existingIndex < featureRegistryRows.length
    ) {
        const existing = featureRegistryRows[existingIndex];
        featureRegistryRows[existingIndex] = {
            ...snapshot,
            label_net_return: existing.label_net_return,
            label_pnl: existing.label_pnl,
            label_timestamp: existing.label_timestamp,
            label_source: existing.label_source,
        };
        featureRegistryIndexById.set(id, existingIndex);
    } else {
        if (existingIndex !== undefined) {
            featureRegistryIndexById.delete(id);
        }
        featureRegistryRows.push(snapshot);
        featureRegistryIndexById.set(id, featureRegistryRows.length - 1);
        addFeatureRegistryRowStats(snapshot);
        trimFeatureRegistryRowsIfNeeded();
    }
    updateModelInferenceFromScan(scan, snapshot.id);
    featureRegistryRecorder.record({
        event_type: 'SCAN',
        event_ts: Date.now(),
        row_id: snapshot.id,
        strategy: snapshot.strategy,
        market_key: snapshot.market_key,
        scan_ts: snapshot.timestamp,
        signal_type: snapshot.signal_type,
        metric_family: snapshot.metric_family,
        features: snapshot.features,
    });
    featureRegistryUpdatedAt = Date.now();
}

function attachLabelToFeature(sample: StrategyTradeSample): boolean {
    // Skip ML label assignment for non-eligible closures (stale, manual, reset)
    if (sample.label_eligible === false) {
        return false;
    }
    const sampleMarketKey = sample.market_key || null;
    for (let index = featureRegistryRows.length - 1; index >= 0; index -= 1) {
        const row = featureRegistryRows[index];
        if (row.strategy !== sample.strategy) {
            continue;
        }
        if (sampleMarketKey && row.market_key !== sampleMarketKey) {
            continue;
        }
        if (row.label_net_return !== null || row.label_pnl !== null) {
            continue;
        }
        if (sample.timestamp < row.timestamp) {
            featureRegistryLeakageViolations += 1;
            continue;
        }
        if ((sample.timestamp - row.timestamp) > FEATURE_REGISTRY_LABEL_LOOKBACK_MS) {
            break;
        }
        row.label_net_return = sample.net_return;
        row.label_pnl = sample.pnl;
        row.label_timestamp = sample.timestamp;
        row.label_source = 'strategy:pnl';
        markFeatureRegistryRowLabeled(row);
        updateModelDriftFromFeatureLabel(row);
        featureRegistryRecorder.record({
            event_type: 'LABEL',
            event_ts: Date.now(),
            row_id: row.id,
            strategy: row.strategy,
            label_ts: sample.timestamp,
            label_net_return: sample.net_return,
            label_pnl: sample.pnl,
            label_source: 'strategy:pnl',
        });
        featureRegistryUpdatedAt = Date.now();
        void runInProcessModelTraining(false);
        return true;
    }
    return false;
}

function computeStrategyMultiplier(state: StrategyPerformance): number {
    const total = Math.max(1, state.win_count + state.loss_count);
    const winRate = state.win_count / total;
    const confidence = Math.min(1, state.sample_count / 40);
    const vol = Math.sqrt(Math.max(STRATEGY_ALLOCATOR_EPSILON, state.return_var_ema));
    const impliedSharpe = state.ema_return / vol;
    const downsidePenalty = state.downside_ema * (1.2 + (1 - confidence) * 0.6);
    const qualityScore = (impliedSharpe / STRATEGY_ALLOCATOR_TARGET_SHARPE) + ((winRate - 0.5) * 1.4);

    if (state.sample_count < STRATEGY_ALLOCATOR_MIN_SAMPLES) {
        const bootstrap = 1 + (qualityScore * confidence * 0.20);
        return Math.min(1.20, Math.max(0.80, bootstrap));
    }

    // Throttle (don't kill) strategies with persistent negative edge.
    // Thresholds tuned for binary option returns (±100% per trade).
    // EMA alpha=0.08 needs ~30 samples to converge, so require 30+ before judging.
    if (state.sample_count >= 30 && state.ema_return <= -0.25) {
        return STRATEGY_WEIGHT_FLOOR;
    }
    if (state.sample_count >= 40 && winRate <= 0.35 && state.ema_return <= -0.15) {
        return STRATEGY_WEIGHT_FLOOR;
    }

    const raw = 1 + (qualityScore * confidence * 0.45) - downsidePenalty;
    return Math.min(STRATEGY_WEIGHT_CAP, Math.max(STRATEGY_WEIGHT_FLOOR, raw));
}

async function publishStrategyMultiplier(strategyId: string, multiplier: number): Promise<void> {
    const bounded = Math.min(STRATEGY_WEIGHT_CAP, Math.max(STRATEGY_WEIGHT_FLOOR, multiplier));
    await redisClient.set(strategyRiskMultiplierKey(strategyId), bounded.toFixed(6));
    await redisClient.expire(strategyRiskMultiplierKey(strategyId), 86400); // 24 hours
}

async function applyDefaultStrategyStates(force = false): Promise<void> {
    for (const strategyId of STRATEGY_IDS) {
        const enabledKey = `strategy:enabled:${strategyId}`;
        const enabledByDefault = !DEFAULT_DISABLED_STRATEGIES.has(strategyId);
        if (force) {
            await redisClient.set(enabledKey, enabledByDefault ? '1' : '0');
        } else {
            await redisClient.setNX(enabledKey, enabledByDefault ? '1' : '0');

            // Re-enable strategies whose Risk Guard cooldown expired during a restart.
            // The setTimeout-based auto-resume is lost when the backend restarts, so
            // strategies can get stuck disabled forever. Check the cooldown key and
            // re-enable if expired or absent (for non-default-disabled strategies).
            const currentVal = await redisClient.get(enabledKey);
            if (currentVal === '0' && enabledByDefault) {
                const cooldownUntil = await redisClient.get(`risk_guard:cooldown:${strategyId}`);
                const now = Date.now();
                if (!cooldownUntil || Number(cooldownUntil) <= now) {
                    await redisClient.set(enabledKey, '1');
                    console.info(`[Boot] Re-enabled ${strategyId}: Risk Guard cooldown expired or absent`);
                } else {
                    // Cooldown still active — reschedule the resume timer
                    const remaining = Number(cooldownUntil) - now;
                    setTimeout(async () => {
                        try {
                            await redisClient.set(enabledKey, '1');
                            await redisClient.expire(enabledKey, 86400);
                            strategyStatus[strategyId] = true;
                            io.emit('strategy_status_update', strategyStatus);
                            console.info(`[RiskGuard] RESUMED ${strategyId} after boot-rescheduled cooldown`);
                        } catch (err) {
                            console.error(`[RiskGuard] Error resuming ${strategyId} after boot:`, err);
                        }
                    }, remaining);
                    console.info(`[Boot] ${strategyId} still in Risk Guard cooldown, resume in ${Math.ceil(remaining / 1000)}s`);
                }
            }
        }
        await redisClient.expire(enabledKey, 86400); // 24 hours
        const raw = await redisClient.get(enabledKey);
        strategyStatus[strategyId] = raw !== '0';
    }
}

async function updateStrategyAllocator(strategyId: string, pnl: number, notional: number | null): Promise<void> {
    if (!strategyId || !Number.isFinite(pnl)) {
        return;
    }
    const state = ensureStrategyPerformanceEntry(strategyId);
    const normalizedNotional = notional && Number.isFinite(notional) && notional > 0
        ? notional
        : Math.max(1, Math.abs(pnl));
    const realizedReturn = pnl / normalizedNotional;

    const alpha = Math.min(0.50, Math.max(0.01, Number(process.env.STRATEGY_ALLOCATOR_EMA_ALPHA || '0.05')));
    state.ema_return = state.sample_count === 0
        ? realizedReturn
        : ((1 - alpha) * state.ema_return) + (alpha * realizedReturn);
    const downside = Math.max(0, -realizedReturn);
    state.downside_ema = state.sample_count === 0
        ? downside
        : ((1 - alpha) * state.downside_ema) + (alpha * downside);
    const centered = realizedReturn - state.ema_return;
    const returnVariance = centered * centered;
    state.return_var_ema = state.sample_count === 0
        ? returnVariance
        : ((1 - alpha) * state.return_var_ema) + (alpha * returnVariance);
    state.sample_count += 1;
    if (pnl >= 0) {
        state.win_count += 1;
    } else {
        state.loss_count += 1;
    }
    state.multiplier = computeStrategyMultiplier(state);
    state.updated_at = Date.now();

    await publishStrategyMultiplier(strategyId, state.multiplier);
    io.emit('strategy_risk_multiplier_update', {
        strategy: strategyId,
        multiplier: state.multiplier,
        sample_count: state.sample_count,
        ema_return: state.ema_return,
        downside_ema: state.downside_ema,
        return_var_ema: state.return_var_ema,
        timestamp: state.updated_at,
    });
    touchRuntimeModule('RISK_ALLOCATOR', 'ONLINE', `${strategyId} multiplier ${state.multiplier.toFixed(3)} from ${state.sample_count} samples`);
}

async function bootstrapStrategyMultipliers(resetToNeutral = false): Promise<void> {
    for (const strategyId of STRATEGY_IDS) {
        const key = strategyRiskMultiplierKey(strategyId);
        const state = ensureStrategyPerformanceEntry(strategyId);
        const noPerformanceHistory = state.sample_count === 0;
        const baseline = DEFAULT_DISABLED_STRATEGIES.has(strategyId) ? 0 : 1;
        if (resetToNeutral || noPerformanceHistory) {
            await redisClient.set(key, baseline.toFixed(6));
        } else {
            await redisClient.setNX(key, baseline.toFixed(6));
        }
        await redisClient.expire(key, 86400); // 24 hours
        const raw = asNumber(await redisClient.get(key));
        const multiplier = Number.isFinite(raw) && raw !== null
            ? Math.min(STRATEGY_WEIGHT_CAP, Math.max(STRATEGY_WEIGHT_FLOOR, raw))
            : baseline;
        state.multiplier = multiplier;
        state.updated_at = Date.now();
        await publishStrategyMultiplier(strategyId, multiplier);
    }
}

async function persistStrategyMetrics(): Promise<void> {
    await redisClient.set(STRATEGY_METRICS_KEY, JSON.stringify(strategyMetrics));
}

async function loadStrategyMetrics(): Promise<void> {
    clearStrategyMetrics();
    const raw = await redisClient.get(STRATEGY_METRICS_KEY);
    if (!raw) {
        return;
    }

    try {
        const parsed = JSON.parse(raw) as Record<string, unknown>;
        for (const id of STRATEGY_IDS) {
            const entry = asRecord(parsed?.[id]);
            if (!entry) {
                continue;
            }

            const pnl = asNumber(entry.pnl) ?? 0;
            const dailyTrades = Math.max(0, Math.floor(asNumber(entry.daily_trades) ?? 0));
            const updatedAt = asNumber(entry.updated_at) ?? Date.now();
            strategyMetrics[id] = {
                pnl: Math.round(pnl * 100) / 100,
                daily_trades: dailyTrades,
                updated_at: updatedAt,
            };
        }
    } catch {
        clearStrategyMetrics();
    }
}

async function resetSimulationState(
    bankroll: number,
    options: { forceDefaults?: boolean } = {},
): Promise<void> {
    const timestamp = Date.now();
    const payload = {
        bankroll,
        timestamp,
    };

    await clearReservedMaps();
    await featureRegistryRecorder.reset('simulation_reset');
    await redisClient.set(SIM_BANKROLL_KEY, bankroll.toFixed(2));
    await redisClient.set(SIM_LEDGER_CASH_KEY, bankroll.toFixed(8));
    await redisClient.set(SIM_LEDGER_RESERVED_KEY, '0');
    await redisClient.set(SIM_LEDGER_REALIZED_PNL_KEY, '0');
    await redisClient.set('system:simulation_reset_ts', String(timestamp));
    if (RESET_VALIDATION_TRADES_ON_SIM_RESET) {
        await strategyTradeRecorder.reset();
    }
    clearStrategyMetrics();
    clearStrategyQuality();
    clearStrategyPerformance();
    clearStrategyCostDiagnostics();
    clearStrategyTradeSamples();
    clearDataIntegrityState();
    clearGovernanceState();
    clearFeatureRegistry();
    clearModelInferenceState();
    clearModelDriftState();
    // Preserve any operator-enabled/disabled strategy toggles by default.
    // For deterministic benchmark runs, callers can force defaults.
    await applyDefaultStrategyStates(options.forceDefaults === true);
    io.emit('strategy_status_update', strategyStatus);
    io.emit('strategy_quality_snapshot', strategyQuality);
    io.emit('strategy_cost_diagnostics_snapshot', strategyCostDiagnosticsPayload());
    io.emit('data_integrity_snapshot', dataIntegrityPayload());
    io.emit('strategy_governance_snapshot', governancePayload());
    io.emit('feature_registry_snapshot', featureRegistrySummary());
    io.emit('model_inference_snapshot', modelInferencePayload());
    io.emit('model_drift_update', modelDriftPayload());
    await bootstrapStrategyMultipliers(true);
    await runStrategyGovernanceCycle();
    await persistStrategyMetrics();
    await refreshLedgerHealth();
    await refreshMetaController();
    await refreshMlPipelineStatus();
    await redisClient.publish('system:simulation_reset', JSON.stringify(payload));
    touchRuntimeModule('PNL_LEDGER', 'ONLINE', `simulation reset to ${bankroll.toFixed(2)}`);
}

// Real-time streams from market data and arb services
marketDataService.on('ticker', (data: any) => {
    io.emit('market_update', data);
    const symbol = asString(asRecord(data)?.symbol) || 'UNKNOWN';
    touchRuntimeModule('MARKET_DATA_PIPELINE', 'ONLINE', `ticker ${symbol}`);
});

redisSubscriber.subscribe('arbitrage:opportunities', (message) => {
    try {
        io.emit('arbitrage_update', JSON.parse(message));
    } catch (error) {
        console.error('Error parsing arb message:', error);
    }
});

redisSubscriber.subscribe('arbitrage:execution', (message) => {
    try {
        const parsed = JSON.parse(message);
        const executionId = ensureExecutionId(parsed);
        const parsedStrategy = extractExecutionStrategy(parsed);
        const parsedMarketKey = extractExecutionMarketKey(parsed);
        pushExecutionTraceEvent(executionId, 'EXECUTION_EVENT', parsed, {
            strategy: parsedStrategy,
            market_key: parsedMarketKey,
        });
        io.emit('execution_log', parsed);

        void (async () => {
            try {
                const currentTradingMode = await getTradingMode();
                const intelligenceGate = evaluateIntelligenceGate(parsed, currentTradingMode);
                const gateMeta = {
                    strategy: intelligenceGate.strategy || parsedStrategy,
                    market_key: intelligenceGate.marketKey || parsedMarketKey,
                };
                if (!intelligenceGate.ok) {
                    touchRuntimeModule('INTELLIGENCE_GATE', 'DEGRADED', intelligenceGate.reason || 'execution blocked');
                    const blockPayload = {
                        execution_id: executionId,
                        timestamp: Date.now(),
                        strategy: intelligenceGate.strategy || 'UNKNOWN',
                        market_key: intelligenceGate.marketKey || intelligenceGate.scan?.market_key || null,
                        reason: intelligenceGate.reason || 'Intelligence gate rejected execution',
                        scan: intelligenceGate.scan || null,
                        margin: intelligenceGate.margin,
                        normalized_margin: intelligenceGate.normalizedMargin,
                        age_ms: intelligenceGate.ageMs,
                        peer_signals: intelligenceGate.peerSignals ?? 0,
                        peer_consensus: intelligenceGate.peerConsensus ?? 0,
                        peer_strategies: intelligenceGate.peerStrategies || [],
                    };
                    pushExecutionTraceEvent(executionId, 'INTELLIGENCE_GATE', blockPayload, gateMeta);
                    io.emit('strategy_intelligence_block', blockPayload);
                    io.emit('execution_log', {
                        execution_id: executionId,
                        timestamp: Date.now(),
                        side: 'INTEL_BLOCK',
                        market: asString(asRecord(parsed)?.market) || 'Polymarket',
                        price: blockPayload.reason,
                        size: blockPayload.strategy,
                        mode: 'LIVE_GUARD',
                        details: blockPayload,
                    });
                    return;
                }
                pushExecutionTraceEvent(
                    executionId,
                    'INTELLIGENCE_GATE',
                    {
                        execution_id: executionId,
                        ok: true,
                        timestamp: Date.now(),
                        strategy: intelligenceGate.strategy || parsedStrategy,
                        market_key: intelligenceGate.marketKey || parsedMarketKey,
                        margin: intelligenceGate.margin,
                        normalized_margin: intelligenceGate.normalizedMargin,
                        age_ms: intelligenceGate.ageMs,
                        peer_signals: intelligenceGate.peerSignals ?? 0,
                        peer_consensus: intelligenceGate.peerConsensus ?? 0,
                        peer_strategies: intelligenceGate.peerStrategies || [],
                    },
                    gateMeta,
                );
                touchRuntimeModule('INTELLIGENCE_GATE', 'ONLINE', `execution allowed for ${intelligenceGate.strategy || 'UNKNOWN'}`);
                const modelGate = evaluateModelProbabilityGate(parsed, currentTradingMode);
                if (!modelGate.ok) {
                    touchRuntimeModule(
                        'UNCERTAINTY_GATE',
                        'DEGRADED',
                        `${modelGate.strategy || 'UNKNOWN'} blocked: ${modelGate.reason}`,
                    );
                    const modelBlockPayload = {
                        execution_id: executionId,
                        timestamp: Date.now(),
                        strategy: modelGate.strategy || intelligenceGate.strategy || 'UNKNOWN',
                        market_key: modelGate.market_key || intelligenceGate.marketKey || intelligenceGate.scan?.market_key || null,
                        reason: `[MODEL_GATE] ${modelGate.reason}`,
                        probability: modelGate.probability,
                        gate: modelGate.gate,
                        model_loaded: modelGate.model_loaded,
                        mode: currentTradingMode,
                    };
                    pushExecutionTraceEvent(executionId, 'MODEL_GATE', modelBlockPayload, {
                        strategy: modelBlockPayload.strategy,
                        market_key: modelBlockPayload.market_key,
                    });
                    io.emit('strategy_model_block', modelBlockPayload);
                    // Reuse existing UI intelligence block stream for visibility.
                    io.emit('strategy_intelligence_block', modelBlockPayload);
                    io.emit('execution_log', {
                        execution_id: executionId,
                        timestamp: Date.now(),
                        side: 'MODEL_BLOCK',
                        market: asString(asRecord(parsed)?.market) || 'Polymarket',
                        price: modelBlockPayload.reason,
                        size: modelBlockPayload.strategy,
                        mode: currentTradingMode === 'PAPER' ? 'PAPER_MODEL_GUARD' : 'LIVE_MODEL_GUARD',
                        details: modelBlockPayload,
                    });
                    return;
                }
                pushExecutionTraceEvent(
                    executionId,
                    'MODEL_GATE',
                    {
                        execution_id: executionId,
                        ok: true,
                        timestamp: Date.now(),
                        strategy: modelGate.strategy || intelligenceGate.strategy || parsedStrategy,
                        market_key: modelGate.market_key || intelligenceGate.marketKey || parsedMarketKey,
                        probability: modelGate.probability,
                        gate: modelGate.gate,
                        model_loaded: modelGate.model_loaded,
                        mode: currentTradingMode,
                    },
                    {
                        strategy: modelGate.strategy || intelligenceGate.strategy || parsedStrategy,
                        market_key: modelGate.market_key || intelligenceGate.marketKey || parsedMarketKey,
                    },
                );
                touchRuntimeModule(
                    'UNCERTAINTY_GATE',
                    'ONLINE',
                    `${modelGate.strategy || 'UNKNOWN'} prob gate pass (${modelGate.reason})`,
                );

                const preflight = await polymarketPreflight.preflightFromExecution(parsed);
                if (!preflight) {
                    touchRuntimeModule('EXECUTION_PREFLIGHT', 'ONLINE', 'preflight bypassed (no polymarket order payload)');
                    pushExecutionTraceEvent(
                        executionId,
                        'PREFLIGHT',
                        {
                            execution_id: executionId,
                            timestamp: Date.now(),
                            bypassed: true,
                            mode: currentTradingMode,
                        },
                        {
                            strategy: modelGate.strategy || intelligenceGate.strategy || parsedStrategy,
                            market_key: modelGate.market_key || intelligenceGate.marketKey || parsedMarketKey,
                        },
                    );
                    const liveExecution = await polymarketPreflight.executeFromExecution(parsed, currentTradingMode);
                    if (!liveExecution) {
                        return;
                    }
                    (liveExecution as unknown as Record<string, unknown>).execution_id = executionId;
                    pushExecutionTraceEvent(executionId, 'LIVE_EXECUTION', liveExecution, {
                        strategy: liveExecution.strategy || modelGate.strategy || intelligenceGate.strategy || parsedStrategy,
                        market_key: parsedMarketKey,
                    });
                    io.emit('strategy_live_execution', liveExecution);
                    io.emit('execution_log', PolymarketPreflightService.toLiveExecutionLog(liveExecution));
                    const settlementEvents = await settlementService.registerAtomicExecution(liveExecution);
                    await emitSettlementEvents(settlementEvents);
                    return;
                }

                touchRuntimeModule('EXECUTION_PREFLIGHT', preflight.ok ? 'ONLINE' : 'DEGRADED', `${preflight.strategy} preflight ${preflight.ok ? 'ok' : 'failed'}`);
                (preflight as unknown as Record<string, unknown>).execution_id = executionId;
                pushExecutionTraceEvent(executionId, 'PREFLIGHT', preflight, {
                    strategy: preflight.strategy || modelGate.strategy || intelligenceGate.strategy || parsedStrategy,
                    market_key: parsedMarketKey,
                });
                io.emit('strategy_preflight', preflight);
                io.emit('execution_log', PolymarketPreflightService.toExecutionLog(preflight));

                const liveExecution = await polymarketPreflight.executeFromExecution(parsed, currentTradingMode);
                if (!liveExecution) {
                    return;
                }
                (liveExecution as unknown as Record<string, unknown>).execution_id = executionId;
                pushExecutionTraceEvent(executionId, 'LIVE_EXECUTION', liveExecution, {
                    strategy: liveExecution.strategy || preflight.strategy || modelGate.strategy || intelligenceGate.strategy || parsedStrategy,
                    market_key: parsedMarketKey,
                });
                io.emit('strategy_live_execution', liveExecution);
                io.emit('execution_log', PolymarketPreflightService.toLiveExecutionLog(liveExecution));
                const settlementEvents = await settlementService.registerAtomicExecution(liveExecution);
                await emitSettlementEvents(settlementEvents);
            } catch (workerError) {
                console.error('[PolymarketPreflight] execution worker error:', workerError);
            }
        })();
    } catch (error) {
        console.error('Error parsing execution message:', error);
    }
});

redisSubscriber.subscribe('strategy:pnl', (message) => {
    try {
        const parsed = JSON.parse(message);
        const executionId = ensureExecutionId(parsed);
        const pnlStrategy = asString(asRecord(parsed)?.strategy) || null;
        const pnlMarketKey = extractExecutionMarketKey(parsed);
        pushExecutionTraceEvent(executionId, 'PNL', parsed, { strategy: pnlStrategy, market_key: pnlMarketKey });
        io.emit('strategy_pnl', parsed);
        strategyTradeRecorder.record(parsed);
        touchRuntimeModule('PNL_LEDGER', 'ONLINE', `${pnlStrategy || 'UNKNOWN'} pnl event recorded`);

        const mode = normalizeTradingMode(parsed?.mode) || 'PAPER';
        if (mode !== 'PAPER') {
            return;
        }

        const sample = parseTradeSample(parsed);
        if (sample) {
            pushTradeSample(sample);
            const diagnostics = updateStrategyCostDiagnostics(sample);
            const labeled = attachLabelToFeature(sample);
            io.emit('strategy_cost_diagnostics_update', {
                strategy: sample.strategy,
                ...diagnostics,
                avg_cost_drag_bps: Math.round(diagnostics.avg_cost_drag_bps * 10) / 10,
                avg_net_return_bps: Math.round(diagnostics.avg_net_return_bps * 10) / 10,
                avg_gross_return_bps: Math.round(diagnostics.avg_gross_return_bps * 10) / 10,
            });
            if (labeled) {
                io.emit('feature_registry_update', featureRegistrySummary());
            }
        }

        const strategy = typeof parsed?.strategy === 'string' ? parsed.strategy : null;
        const pnl = Number(parsed?.pnl);
        if (strategy && Number.isFinite(pnl)) {
            if (!strategyMetrics[strategy]) {
                strategyMetrics[strategy] = { pnl: 0, daily_trades: 0, updated_at: 0 };
            }
            strategyMetrics[strategy] = {
                pnl: Math.round((strategyMetrics[strategy].pnl + pnl) * 100) / 100,
                daily_trades: strategyMetrics[strategy].daily_trades + 1,
                updated_at: Date.now(),
            };
            io.emit('strategy_metrics_update', strategyMetrics);
            void persistStrategyMetrics();

            const notional = asNumber(parsed?.notional);
            void updateStrategyAllocator(strategy, pnl, notional);
            void processRiskGuard(strategy, pnl, parsed as Record<string, unknown>);
            void sweepProfitsToVault();
        }
    } catch (error) {
        console.error('Error parsing pnl message:', error);
    }
});

redisSubscriber.subscribe('arbitrage:scan', (message) => {
    try {
        const normalized = typeof message === 'string' ? message.trim() : '';
        if (!normalized) {
            return;
        }
        if (
            normalized.includes('\r\n$9\r\nsubscribe\r\n')
            || normalized.startsWith('*3\r\n$9\r\nsubscribe')
        ) {
            return;
        }
        if (!normalized.includes('{') || !normalized.includes('}')) {
            return;
        }

        let parsed: unknown;
        try {
            parsed = JSON.parse(normalized);
        } catch (innerError) {
            // Some publishers occasionally include transport prefixes/suffixes.
            // Recover by parsing the largest JSON object slice instead of dropping the scan.
            const jsonStart = normalized.indexOf('{');
            const jsonEnd = normalized.lastIndexOf('}');
            if (jsonStart >= 0 && jsonEnd > jsonStart) {
                parsed = JSON.parse(normalized.slice(jsonStart, jsonEnd + 1));
            } else {
                throw innerError;
            }
        }

        const parsedRecord = asRecord(parsed);
        if (!parsedRecord) {
            return;
        }

        const strategy = asString(parsedRecord.strategy);
        if (strategy) {
            const timestamp = asNumber(parsedRecord.timestamp) || Date.now();
            const marketKey = extractScanMarketKey(parsedRecord) || strategy.toLowerCase();
            const rawSignalType = asString(parsedRecord.signal_type);
            const rawUnit = asString(parsedRecord.unit);
            const metaFeatures = extractScanMetaFeatures(parsedRecord.meta);
            const inferred = inferScanDescriptor(strategy, rawSignalType, rawUnit);
            const metricFamily = parseMetricFamily(parsedRecord.metric_family) || inferred.metric_family;
            const directionality = parseMetricDirection(parsedRecord.directionality) || inferred.directionality;
            const comparableGroup = asString(parsedRecord.comparable_group) || inferred.comparable_group;
            const scanState: StrategyScanState = {
                strategy,
                symbol: asString(parsedRecord.symbol) || strategy,
                market_key: marketKey,
                timestamp,
                passes_threshold: parsedRecord.passes_threshold === true,
                score: asNumber(parsedRecord.score) ?? asNumber(parsedRecord.gap) ?? 0,
                threshold: asNumber(parsedRecord.threshold) ?? 0,
                reason: asString(parsedRecord.reason) || '',
                signal_type: rawSignalType || inferred.signal_type,
                unit: normalizeMetricUnit(rawUnit || inferred.unit),
                metric_family: metricFamily,
                directionality,
                comparable_group: comparableGroup,
                meta_features: Object.keys(metaFeatures).length > 0 ? metaFeatures : undefined,
            };
            latestStrategyScans.set(strategy, scanState);
            latestStrategyScansByMarket.set(buildStrategyMarketKey(strategy, marketKey), scanState);
            pruneIntelligenceState();
            upsertFeatureSnapshot(scanState);
            touchRuntimeModule('FEATURE_REGISTRY', 'ONLINE', `feature row ${scanState.strategy} ${scanState.market_key}`);
            updateStrategyQuality(scanState);
            const dataIntegrityAlert = updateDataIntegrityFromScan(scanState);
            const quality = ensureStrategyQualityEntry(strategy);
            const integrity = ensureStrategyDataIntegrityEntry(strategy);
            io.emit('strategy_quality_update', {
                strategy,
                ...quality,
                pass_rate_pct: quality.total_scans > 0
                    ? (quality.pass_scans / quality.total_scans) * 100
                    : 0,
            });
            io.emit('data_integrity_update', {
                strategy,
                ...integrity,
            });
            if (dataIntegrityAlert) {
                touchRuntimeModule(
                    'SCAN_INGEST',
                    dataIntegrityAlert.severity === 'CRITICAL' ? 'DEGRADED' : 'ONLINE',
                    `${strategy} ${dataIntegrityAlert.category} ${dataIntegrityAlert.severity.toLowerCase()} x${dataIntegrityAlert.consecutive_holds}`,
                );
                io.emit('data_integrity_alert', dataIntegrityAlert);
            }
            io.emit('feature_registry_update', featureRegistrySummary());
            requestMetaControllerRefresh();
            touchRuntimeModule('SCAN_INGEST', 'ONLINE', `${strategy} ${scanState.passes_threshold ? 'PASS' : 'HOLD'} ${scanState.symbol}`);
            io.emit('intelligence_update', scanState);
        }
        io.emit('scanner_update', parsed);
    } catch (error) {
        const now = Date.now();
        if (now - lastScannerParseErrorLogMs >= 30_000) {
            lastScannerParseErrorLogMs = now;
            const preview = typeof message === 'string' ? message.slice(0, 160) : String(message);
            console.error('Error in scanner_update:', error, 'payload_preview=', preview);
        }
    }
});

// Persist ML features to Redis keys so Rust scanners can read RSI/SMA for momentum filtering.
redisSubscriber.subscribe('ml:features', (message) => {
    try {
        const parsed = JSON.parse(message);
        const symbol = parsed?.symbol;
        if (typeof symbol === 'string' && symbol.length > 0) {
            const key = `ml:features:latest:${symbol}`;
            void redisClient.set(key, JSON.stringify({
                symbol,
                rsi_14: parsed.rsi_14 ?? null,
                sma_20: parsed.sma_20 ?? null,
                price: parsed.price ?? null,
                returns: parsed.returns ?? null,
                timestamp: normalizeTimestampMs(parsed.timestamp),
            }), { EX: 120 }); // expire after 2 min if ML service stops
        }
    } catch { /* ignore */ }
});

redisSubscriber.subscribe('system:heartbeat', (msg) => {
    try {
        const { id, timestamp } = JSON.parse(msg);
        if (typeof id === 'string') {
            heartbeats[id] = Number(timestamp) || Date.now();
        }
    } catch {
        // ignore malformed heartbeat
    }
});

redisSubscriber.subscribe('system:trading_mode', (msg) => {
    try {
        const parsed = JSON.parse(msg);
        const mode = normalizeTradingMode(parsed?.mode);
        if (!mode) {
            return;
        }
        const livePostingEnabled = typeof parsed?.live_order_posting_enabled === 'boolean'
            ? parsed.live_order_posting_enabled
            : isLiveOrderPostingEnabled();

        io.emit('trading_mode_update', {
            mode,
            timestamp: Number(parsed?.timestamp) || Date.now(),
            live_order_posting_enabled: livePostingEnabled,
        });
    } catch {
        // ignore malformed trading mode update
    }
});

redisSubscriber.subscribe('system:simulation_reset', (msg) => {
    try {
        const parsed = JSON.parse(msg) as { bankroll?: unknown; timestamp?: unknown };
        const bankroll = normalizeResetBankroll(parsed?.bankroll);
        if (bankroll === null) {
            return;
        }

        io.emit('simulation_reset', {
            bankroll,
            timestamp: Number(parsed?.timestamp) || Date.now(),
        });
        clearStrategyMetrics();
        clearStrategyCostDiagnostics();
        clearStrategyTradeSamples();
        clearDataIntegrityState();
        clearGovernanceState();
        clearFeatureRegistry();
        clearModelInferenceState();
        clearModelDriftState();
        void persistStrategyMetrics();
        io.emit('strategy_metrics_update', strategyMetrics);
        io.emit('strategy_cost_diagnostics_snapshot', strategyCostDiagnosticsPayload());
        io.emit('data_integrity_snapshot', dataIntegrityPayload());
        io.emit('strategy_governance_snapshot', governancePayload());
        io.emit('feature_registry_snapshot', featureRegistrySummary());
        io.emit('model_inference_snapshot', modelInferencePayload());
        io.emit('model_drift_update', modelDriftPayload());
        requestMetaControllerRefresh();
        void refreshMlPipelineStatus();
        void runStrategyGovernanceCycle();
        void refreshLedgerHealth();
        io.emit('strategy_risk_multiplier_snapshot', Object.fromEntries(
            STRATEGY_IDS.map((strategyId) => [strategyId, ensureStrategyPerformanceEntry(strategyId)]),
        ));
    } catch {
        // ignore malformed simulation reset update
    }
});

io.on('connection', async (socket) => {
    console.log('Client connected:', socket.id);
    const socketControlAuthorized = isControlPlaneAuthorized(getControlTokenFromSocket(socket));

    socket.emit('system_status', {
        status: 'operational',
        active_agents: agentManager.getActiveAgents(),
    });

    socket.emit('agent_status_update', agentManager.getAllAgents());
    socket.emit('strategy_status_update', strategyStatus);
    socket.emit('strategy_metrics_update', strategyMetrics);
    socket.emit('strategy_risk_multiplier_snapshot', Object.fromEntries(
        STRATEGY_IDS.map((strategyId) => [strategyId, ensureStrategyPerformanceEntry(strategyId)]),
    ));
    socket.emit('strategy_quality_snapshot', Object.fromEntries(
        STRATEGY_IDS.map((strategyId) => {
            const quality = ensureStrategyQualityEntry(strategyId);
            return [strategyId, {
                ...quality,
                pass_rate_pct: quality.total_scans > 0
                    ? (quality.pass_scans / quality.total_scans) * 100
                    : 0,
            }];
        }),
    ));
    socket.emit('strategy_cost_diagnostics_snapshot', strategyCostDiagnosticsPayload());
    socket.emit('data_integrity_snapshot', dataIntegrityPayload());
    socket.emit('strategy_governance_snapshot', governancePayload());
    socket.emit('feature_registry_snapshot', featureRegistrySummary());
    socket.emit('risk_config_update', await getRiskConfig());
    socket.emit('trading_mode_update', {
        mode: await getTradingMode(),
        timestamp: Date.now(),
        live_order_posting_enabled: isLiveOrderPostingEnabled(),
    });
    socket.emit('intelligence_gate_config', {
        enabled: INTELLIGENCE_GATE_ENABLED,
        max_staleness_ms: INTELLIGENCE_GATE_MAX_STALENESS_MS,
        min_margin: INTELLIGENCE_GATE_MIN_MARGIN,
        confirmation_window_ms: INTELLIGENCE_GATE_CONFIRMATION_WINDOW_MS,
        require_peer_confirmation: INTELLIGENCE_GATE_REQUIRE_PEER_CONFIRMATION,
        strong_margin: INTELLIGENCE_GATE_STRONG_MARGIN,
    });
    pruneIntelligenceState();
    socket.emit('intelligence_snapshot', Object.fromEntries(
        [...latestStrategyScans.entries()].map(([strategy, scan]) => [strategy, {
            ...scan,
            age_ms: Date.now() - scan.timestamp,
            margin: computeScanMargin(scan),
            normalized_margin: computeNormalizedMargin(scan),
        }]),
    ));
    socket.emit('intelligence_market_snapshot', Object.fromEntries(
        [...latestStrategyScansByMarket.entries()].map(([key, scan]) => [key, {
            ...scan,
            age_ms: Date.now() - scan.timestamp,
            margin: computeScanMargin(scan),
            normalized_margin: computeNormalizedMargin(scan),
        }]),
    ));
    socket.emit('settlement_snapshot', settlementService.getSnapshot());
    socket.emit('runtime_status_update', runtimeStatusPayload());
    socket.emit('ledger_health_update', ledgerHealthState);
    socket.emit('meta_controller_update', metaControllerPayload());
    socket.emit('ml_pipeline_status_update', mlPipelineStatus);
    socket.emit('model_inference_snapshot', modelInferencePayload());
    socket.emit('model_drift_update', modelDriftPayload());

    socket.on('disconnect', () => {
        console.log('Client disconnected:', socket.id);
    });

    socket.on('toggle_agent', ({ name, active }: { name: string; active: boolean }) => {
        if (!enforceSocketControlAuth(socket, 'toggle_agent', socketControlAuthorized)) {
            return;
        }
        agentManager.toggleAgent(name, active);
        io.emit('agent_status_update', agentManager.getAllAgents());
    });

    socket.on('update_config', async (data) => {
        if (!enforceSocketControlAuth(socket, 'update_config', socketControlAuthorized)) {
            return;
        }
        await redisClient.publish('system:config', JSON.stringify(data));
        console.log('Config Updated:', data);
    });

    socket.on('update_risk_config', async (payload: Partial<RiskConfig>) => {
        if (!enforceSocketControlAuth(socket, 'update_risk_config', socketControlAuthorized)) {
            return;
        }
        const normalized = normalizeRiskConfig(payload);
        if (!normalized) {
            socket.emit('risk_config_error', { message: 'Invalid risk config payload' });
            return;
        }

        await redisClient.set('system:risk_config', JSON.stringify(normalized));
        await redisClient.publish('system:risk_config', JSON.stringify(normalized));
        io.emit('risk_config_update', normalized);
        console.log('Risk Config Updated:', normalized);
    });

    socket.on('request_risk_config', async () => {
        socket.emit('risk_config_update', await getRiskConfig());
    });

    socket.on('request_vault', async () => {
        const vault = parseFloat(await redisClient.get(VAULT_REDIS_KEY) || '0');
        socket.emit('vault_update', { vault: Math.round(vault * 100) / 100, ceiling: VAULT_BANKROLL_CEILING });
    });

    socket.on('request_strategy_metrics', () => {
        socket.emit('strategy_metrics_update', strategyMetrics);
    });

    socket.on('request_risk_guard', () => {
        for (const id of RISK_GUARD_STRATEGIES) {
            const state = riskGuardState[id];
            if (state) {
                socket.emit('risk_guard_update', { strategy: id, ...state });
            }
        }
    });

    socket.on('request_ledger', async () => {
        const ledger = await getSimulationLedgerSnapshot();
        socket.emit('sim_ledger_update', {
            available_cash: ledger.cash,
            reserved: ledger.reserved,
            realized_pnl: ledger.realized_pnl,
            equity: ledger.equity,
            utilization_pct: ledger.utilization_pct,
        });
    });

    socket.on('request_trading_mode', async () => {
        socket.emit('trading_mode_update', {
            mode: await getTradingMode(),
            timestamp: Date.now(),
            live_order_posting_enabled: isLiveOrderPostingEnabled(),
        });
    });

    socket.on('set_trading_mode', async (payload: Partial<TradingModePayload>) => {
        if (!enforceSocketControlAuth(socket, 'set_trading_mode', socketControlAuthorized)) {
            return;
        }
        const mode = normalizeTradingMode(payload?.mode);
        if (!mode) {
            socket.emit('trading_mode_error', { message: 'Invalid trading mode payload' });
            return;
        }

        if (mode === 'LIVE' && payload?.confirmation !== 'LIVE') {
            socket.emit('trading_mode_error', { message: 'LIVE mode requires explicit confirmation' });
            return;
        }

        if (mode === 'LIVE') {
            const readiness = await isLiveReady();
            if (!readiness.ready) {
                socket.emit('trading_mode_error', {
                    message: 'System not ready for LIVE mode',
                    failures: readiness.failures,
                });
                return;
            }
        }

        await setTradingMode(mode);
    });

    socket.on('reset_simulation', async (payload: Partial<SimulationResetPayload>) => {
        if (!enforceSocketControlAuth(socket, 'reset_simulation', socketControlAuthorized)) {
            return;
        }
        const currentMode = await getTradingMode();
        if (currentMode !== 'PAPER') {
            socket.emit('simulation_reset_error', { message: 'Simulation reset is only allowed in PAPER mode' });
            return;
        }

        if (payload?.confirmation !== 'RESET') {
            socket.emit('simulation_reset_error', { message: 'Simulation reset requires explicit RESET confirmation' });
            return;
        }

        const bankroll = normalizeResetBankroll(payload?.bankroll);
        if (bankroll === null) {
            socket.emit('simulation_reset_error', { message: 'Invalid reset bankroll value' });
            return;
        }

        const forceDefaults = payload?.force_defaults === true;
        await resetSimulationState(bankroll, { forceDefaults });
    });

    socket.on('toggle_strategy', async (payload: StrategyTogglePayload) => {
        if (!enforceSocketControlAuth(socket, 'toggle_strategy', socketControlAuthorized)) {
            return;
        }
        if (!payload || !STRATEGY_IDS.includes(payload.id) || typeof payload.active !== 'boolean') {
            socket.emit('strategy_control_error', { message: 'Invalid strategy toggle payload' });
            return;
        }

        strategyStatus[payload.id] = payload.active;

        await redisClient.set(`strategy:enabled:${payload.id}`, payload.active ? '1' : '0');
        await redisClient.expire(`strategy:enabled:${payload.id}`, 86400); // 24 hours
        await redisClient.publish('strategy:control', JSON.stringify({
            id: payload.id,
            active: payload.active,
            timestamp: Date.now(),
        }));

        io.emit('strategy_status_update', strategyStatus);
    });
});

app.get('/api/arb/bots', (_req, res) => {
    const now = Date.now();
    const bots = STRATEGY_IDS.map((id) => ({
        id,
        active: Boolean(strategyStatus[id]),
    }));

    const scannerLastBeat = Math.max(...SCANNER_HEARTBEAT_IDS.map((id) => heartbeats[id] || 0));

    res.json({
        bots,
        scanner: {
            alive: now - scannerLastBeat < 15_000,
            last_heartbeat_ms: scannerLastBeat,
        },
    });
});

app.post('/api/arb/risk', requireControlPlaneAuth, async (req, res) => {
    const normalized = normalizeRiskConfig(req.body ?? {});
    if (!normalized) {
        res.status(400).json({ error: 'Invalid risk config payload' });
        return;
    }

    await redisClient.set('system:risk_config', JSON.stringify(normalized));
    await redisClient.publish('system:risk_config', JSON.stringify(normalized));
    io.emit('risk_config_update', normalized);
    res.json({ ok: true, risk: normalized });
});

app.get('/api/arb/stats', async (_req, res) => {
    const ledger = await getSimulationLedgerSnapshot();
    const bankroll = ledger.equity;
    const now = Date.now();
    const activeSignals = getActiveSignalCount(now);
    const activeMarkets = getActiveMarketCount(now);
    const strategySignals = getStrategySignalSummaries(now);
    const comparableGroups = getComparableGroupSummaries(now);
    const scannerLastBeat = Math.max(...SCANNER_HEARTBEAT_IDS.map((id) => heartbeats[id] || 0));
    const settlementSnapshot = settlementService.getSnapshot();
    const trackedSettlements = settlementSnapshot.positions.filter((position) => position.status !== 'REDEEMED');
    const redeemableSettlements = settlementSnapshot.positions.filter((position) => position.status === 'REDEEMABLE');

    res.json({
        bankroll,
        active_signals: activeSignals,
        active_markets: activeMarkets,
        // Deprecated mixed aggregate retained for compatibility; intentionally null to avoid unit-mixing.
        signal_metrics: null,
        intelligence: {
            strategies: strategySignals,
            comparable_groups: comparableGroups.map((group) => ({
                ...group,
                pass_rate_pct: Math.round(group.pass_rate_pct * 10) / 10,
                mean_margin: Math.round(group.mean_margin * 10000) / 10000,
                mean_normalized_margin: Math.round(group.mean_normalized_margin * 1000) / 1000,
            })),
        },
        scanner_last_heartbeat_ms: scannerLastBeat,
        scanner_alive: now - scannerLastBeat < 15_000,
        simulation_ledger: {
            cash: Math.round(ledger.cash * 100) / 100,
            reserved: Math.round(ledger.reserved * 100) / 100,
            realized_pnl: Math.round(ledger.realized_pnl * 100) / 100,
            equity: Math.round(ledger.equity * 100) / 100,
            utilization_pct: Math.round(ledger.utilization_pct * 10) / 10,
        },
        risk: await getRiskConfig(),
        trading_mode: await getTradingMode(),
        live_order_posting_enabled: isLiveOrderPostingEnabled(),
        intelligence_gate_enabled: INTELLIGENCE_GATE_ENABLED,
        model_probability_gate: {
            enabled: MODEL_PROBABILITY_GATE_ENABLED,
            enforce_paper: MODEL_PROBABILITY_GATE_ENFORCE_PAPER,
            enforce_live: MODEL_PROBABILITY_GATE_ENFORCE_LIVE,
            min_probability: MODEL_PROBABILITY_GATE_MIN_PROB,
            require_model: MODEL_PROBABILITY_GATE_REQUIRE_MODEL,
            disable_on_drift: MODEL_PROBABILITY_GATE_DISABLE_ON_DRIFT,
            drift_disabled_until: modelDriftState.gate_disabled_until,
            currently_enforcing: modelDriftState.gate_enforcing,
        },
        build_runtime: runtimeStatusPayload(),
        settlements: {
            tracked: trackedSettlements.length,
            redeemable: redeemableSettlements.length,
            auto_redeem_enabled: settlementSnapshot.autoRedeemEnabled,
        },
        ledger_health: ledgerHealthState,
        meta_controller: metaControllerPayload(),
        ml_pipeline: mlPipelineStatus,
        model_inference: modelInferencePayload(),
        model_drift: modelDriftPayload(),
        strategy_allocator: Object.fromEntries(
            STRATEGY_IDS.map((strategyId) => {
                const perf = ensureStrategyPerformanceEntry(strategyId);
                return [strategyId, {
                    multiplier: Math.round(perf.multiplier * 1000) / 1000,
                    sample_count: perf.sample_count,
                    ema_return: Math.round(perf.ema_return * 10000) / 10000,
                    updated_at: perf.updated_at,
                }];
            }),
        ),
        strategy_quality: Object.fromEntries(
            STRATEGY_IDS.map((strategyId) => {
                const quality = ensureStrategyQualityEntry(strategyId);
                const passRatePct = quality.total_scans > 0
                    ? (quality.pass_scans / quality.total_scans) * 100
                    : 0;
                return [strategyId, {
                    ...quality,
                    pass_rate_pct: Math.round(passRatePct * 10) / 10,
                }];
            }),
        ),
        strategy_cost_diagnostics: strategyCostDiagnosticsPayload(),
        data_integrity: dataIntegrityPayload(),
        strategy_governance: governancePayload(),
        feature_registry: featureRegistrySummary(),
    });
});

app.get('/api/arb/execution-trace/:execution_id', (req, res) => {
    pruneExecutionTraces();
    const executionId = String(req.params.execution_id || '').trim();
    if (!executionId) {
        res.status(400).json({ error: 'Missing execution_id' });
        return;
    }
    const trace = executionTraces.get(executionId);
    if (!trace) {
        res.status(404).json({ error: 'Trace not found' });
        return;
    }
    res.json(trace);
});

app.get('/api/arb/intelligence', (_req, res) => {
    const now = Date.now();
    pruneIntelligenceState(now);
    const byStrategy = Object.fromEntries(
        [...latestStrategyScans.entries()]
            .sort((a, b) => b[1].timestamp - a[1].timestamp)
            .map(([strategy, scan]) => [strategy, {
                ...scan,
                age_ms: now - scan.timestamp,
                margin: computeScanMargin(scan),
                normalized_margin: computeNormalizedMargin(scan),
            }]),
    );
    const byStrategyMarket = Object.fromEntries(
        [...latestStrategyScansByMarket.entries()]
            .sort((a, b) => b[1].timestamp - a[1].timestamp)
            .map(([key, scan]) => [key, {
                ...scan,
                age_ms: now - scan.timestamp,
                margin: computeScanMargin(scan),
                normalized_margin: computeNormalizedMargin(scan),
            }]),
    );

    res.json({
        gate: {
            enabled: INTELLIGENCE_GATE_ENABLED,
            max_staleness_ms: INTELLIGENCE_GATE_MAX_STALENESS_MS,
            min_margin: INTELLIGENCE_GATE_MIN_MARGIN,
            confirmation_window_ms: INTELLIGENCE_GATE_CONFIRMATION_WINDOW_MS,
            require_peer_confirmation: INTELLIGENCE_GATE_REQUIRE_PEER_CONFIRMATION,
            strong_margin: INTELLIGENCE_GATE_STRONG_MARGIN,
        },
        strategies: byStrategy,
        strategies_by_market: byStrategyMarket,
        comparable_groups: getComparableGroupSummaries(now).map((group) => ({
            ...group,
            pass_rate_pct: Math.round(group.pass_rate_pct * 10) / 10,
            mean_margin: Math.round(group.mean_margin * 10000) / 10000,
            mean_normalized_margin: Math.round(group.mean_normalized_margin * 1000) / 1000,
        })),
    });
});

app.get('/api/arb/data-integrity', (_req, res) => {
    res.json(dataIntegrityPayload());
});

app.get('/api/arb/governance', (_req, res) => {
    res.json(governancePayload());
});

app.get('/api/arb/ledger-health', (_req, res) => {
    res.json(ledgerHealthState);
});

app.get('/api/ml/feature-registry', (_req, res) => {
    res.json(featureRegistrySummary());
});

app.get('/api/ml/feature-registry/log', async (_req, res) => {
    const rows = await featureRegistryRecorder.countRows();
    res.json({
        path: featureRegistryRecorder.getPath(),
        rows,
    });
});

app.get('/api/arb/meta-controller', (_req, res) => {
    res.json(metaControllerPayload());
});

app.get('/api/ml/pipeline-status', (_req, res) => {
    res.json(mlPipelineStatus);
});

app.get('/api/ml/model-inference', (_req, res) => {
    res.json(modelInferencePayload());
});

app.get('/api/ml/model-drift', (_req, res) => {
    res.json(modelDriftPayload());
});

app.post('/api/ml/train-now', requireControlPlaneAuth, async (req, res) => {
    const force = req.body?.force === true;
    await runInProcessModelTraining(force);
    await refreshMlPipelineStatus();
    res.json({
        ok: true,
        forced: force,
        ml_pipeline: mlPipelineStatus,
        model_inference: modelInferencePayload(),
        model_drift: modelDriftPayload(),
    });
});

app.post('/api/arb/governance/autopilot', requireControlPlaneAuth, async (req, res) => {
    if (typeof req.body?.enabled !== 'boolean') {
        res.status(400).json({ error: 'enabled must be a boolean' });
        return;
    }
    strategyGovernanceState.autopilot_enabled = req.body.enabled;
    const mode = await getTradingMode();
    strategyGovernanceState.trading_mode = mode;
    strategyGovernanceState.autopilot_effective = strategyGovernanceState.autopilot_enabled && mode === 'PAPER';
    strategyGovernanceState.updated_at = Date.now();
    await runStrategyGovernanceCycle();
    io.emit('strategy_governance_snapshot', governancePayload());
    res.json({
        ok: true,
        autopilot_enabled: strategyGovernanceState.autopilot_enabled,
        autopilot_effective: strategyGovernanceState.autopilot_effective,
    });
});

app.get('/api/arb/settlements', (_req, res) => {
    res.json(settlementService.getSnapshot());
});

app.post('/api/arb/settlements/process', requireControlPlaneAuth, async (_req, res) => {
    const events = await settlementService.runCycle(await getTradingMode(), isLiveOrderPostingEnabled());
    await emitSettlementEvents(events);
    res.json({
        ok: true,
        events,
        snapshot: settlementService.getSnapshot(),
    });
});

app.post('/api/arb/settlements/reset', requireControlPlaneAuth, async (_req, res) => {
    await settlementService.reset();
    io.emit('settlement_snapshot', settlementService.getSnapshot());
    res.json({
        ok: true,
        snapshot: settlementService.getSnapshot(),
    });
});

app.post('/api/arb/settlements/simulate-atomic', requireControlPlaneAuth, async (req, res) => {
    const currentMode = await getTradingMode();
    if (currentMode !== 'PAPER') {
        res.status(409).json({ error: 'Atomic settlement simulation is only allowed in PAPER mode' });
        return;
    }

    const conditionId = normalizeConditionId(req.body?.condition_id);
    if (!conditionId) {
        res.status(400).json({ error: 'Invalid condition_id (expected 0x + 64 hex chars)' });
        return;
    }

    const yesTokenId = asString(req.body?.yes_token_id);
    const noTokenId = asString(req.body?.no_token_id);
    if (!yesTokenId || !noTokenId || yesTokenId === noTokenId) {
        res.status(400).json({ error: 'yes_token_id and no_token_id must both be present and distinct' });
        return;
    }

    const shares = asNumber(req.body?.shares) ?? 1;
    if (!Number.isFinite(shares) || shares <= 0) {
        res.status(400).json({ error: 'shares must be a positive number' });
        return;
    }

    const yesPrice = clampProbability(req.body?.yes_price, 0.5);
    const noPrice = clampProbability(req.body?.no_price, 0.5);
    const market = asString(req.body?.market) || 'Atomic Settlement Simulation';
    const timestamp = Date.now();

    const simulated: PolymarketLiveExecutionResult = {
        market,
        strategy: 'ATOMIC_ARB',
        mode: 'SIMULATION',
        timestamp,
        total: 2,
        posted: 2,
        failed: 0,
        ok: true,
        dryRun: false,
        reason: 'Control-plane synthetic settlement registration',
        orders: [
            {
                tokenId: yesTokenId,
                conditionId,
                side: Side.BUY,
                price: yesPrice,
                inputSize: shares * yesPrice,
                sizeUnit: 'SHARES',
                notionalUsd: shares * yesPrice,
                size: shares,
                ok: true,
                status: 'SIMULATED',
            },
            {
                tokenId: noTokenId,
                conditionId,
                side: Side.BUY,
                price: noPrice,
                inputSize: shares * noPrice,
                sizeUnit: 'SHARES',
                notionalUsd: shares * noPrice,
                size: shares,
                ok: true,
                status: 'SIMULATED',
            },
        ],
    };

    const events = await settlementService.registerAtomicExecution(simulated);
    await emitSettlementEvents(events);
    if (events.length === 0) {
        io.emit('settlement_snapshot', settlementService.getSnapshot());
    }
    res.json({
        ok: true,
        events,
        snapshot: settlementService.getSnapshot(),
    });
});

app.get('/api/arb/validation-trades', async (_req, res) => {
    const rows = await strategyTradeRecorder.countRows();
    res.json({
        path: strategyTradeRecorder.getPath(),
        rows,
    });
});

app.get('/api/arb/strategy-trades', async (req, res) => {
    const strategy = typeof req.query.strategy === 'string' ? req.query.strategy.trim() : '';
    const rawLimit = typeof req.query.limit === 'string' ? Number(req.query.limit) : 200;
    const limit = Number.isFinite(rawLimit) ? Math.max(1, Math.min(2000, Math.floor(rawLimit))) : 200;
    if (!strategy) {
        res.status(400).json({ error: 'Missing strategy query parameter' });
        return;
    }

    const unquote = (value: string): string => {
        const trimmed = value.trim();
        if (trimmed.startsWith('"') && trimmed.endsWith('"')) {
            return trimmed.slice(1, -1).replace(/""/g, '"');
        }
        return trimmed;
    };

    const parseCsvLine = (line: string): string[] => {
        const out: string[] = [];
        let current = '';
        let inQuotes = false;
        for (let i = 0; i < line.length; i += 1) {
            const ch = line[i] as string;
            if (ch === '"') {
                const next = line[i + 1];
                if (inQuotes && next === '"') {
                    current += '"';
                    i += 1;
                    continue;
                }
                inQuotes = !inQuotes;
                continue;
            }
            if (ch === ',' && !inQuotes) {
                out.push(current);
                current = '';
                continue;
            }
            current += ch;
        }
        out.push(current);
        return out;
    };

    try {
        const filePath = strategyTradeRecorder.getPath();
        const content = await fs.readFile(filePath, 'utf8').catch(() => '');
        const lines = content.split('\n').map((line) => line.trim()).filter((line) => line.length > 0);
        if (lines.length <= 1) {
            res.json({ trades: [] });
            return;
        }

        const header = parseCsvLine(lines[0]).map((cell) => unquote(cell).toLowerCase());
        const idx = (name: string): number => header.indexOf(name.toLowerCase());

        const timestampIdx = idx('timestamp');
        const strategyIdx = idx('strategy');
        const variantIdx = idx('variant');
        const pnlIdx = idx('pnl');
        const notionalIdx = idx('notional');
        const modeIdx = idx('mode');
        const reasonIdx = idx('reason');
        const executionIdIdx = idx('execution_id');
        const sideIdx = idx('side');
        const entryPriceIdx = idx('entry_price');

        const scanWindow = Math.max(limit * 25, 1500);
        const start = Math.max(1, lines.length - scanWindow);
        const slice = lines.slice(start);

        const trades: any[] = [];
        for (let j = slice.length - 1; j >= 0; j -= 1) {
            const line = slice[j];
            const cells = parseCsvLine(line);
            const rowStrategy = strategyIdx >= 0 ? unquote(cells[strategyIdx] || '') : '';
            if (rowStrategy !== strategy) {
                continue;
            }
            const ts = timestampIdx >= 0 ? Number(unquote(cells[timestampIdx] || '')) : NaN;
            const pnl = pnlIdx >= 0 ? Number(unquote(cells[pnlIdx] || '')) : NaN;
            const notional = notionalIdx >= 0 ? Number(unquote(cells[notionalIdx] || '')) : NaN;
            const trade = {
                timestamp: Number.isFinite(ts) ? ts : Date.now(),
                strategy: rowStrategy,
                variant: variantIdx >= 0 ? unquote(cells[variantIdx] || '') : '',
                pnl: Number.isFinite(pnl) ? pnl : null,
                notional: Number.isFinite(notional) ? notional : null,
                mode: modeIdx >= 0 ? unquote(cells[modeIdx] || '') : '',
                reason: reasonIdx >= 0 ? unquote(cells[reasonIdx] || '') : '',
                execution_id: executionIdIdx >= 0 ? (unquote(cells[executionIdIdx] || '') || null) : null,
                side: sideIdx >= 0 ? (unquote(cells[sideIdx] || '') || null) : null,
                entry_price: entryPriceIdx >= 0 ? (Number(unquote(cells[entryPriceIdx] || '')) || null) : null,
            };
            trades.push(trade);
            if (trades.length >= limit) {
                break;
            }
        }

        res.json({ trades });
    } catch (error) {
        res.status(500).json({ error: `Failed to read strategy trades: ${String(error)}` });
    }
});

app.post('/api/arb/validation-trades/reset', requireControlPlaneAuth, async (_req, res) => {
    await strategyTradeRecorder.reset();
    res.json({
        ok: true,
        path: strategyTradeRecorder.getPath(),
        rows: 0,
    });
});

app.get('/api/system/trading-mode', async (_req, res) => {
    res.json({
        mode: await getTradingMode(),
        live_order_posting_enabled: isLiveOrderPostingEnabled(),
    });
});

app.get('/api/system/runtime-status', async (_req, res) => {
    res.json(runtimeStatusPayload());
});

app.post('/api/system/trading-mode', requireControlPlaneAuth, async (req, res) => {
    const mode = normalizeTradingMode(req.body?.mode);
    if (!mode) {
        res.status(400).json({ error: 'Invalid trading mode payload' });
        return;
    }

    if (mode === 'LIVE' && req.body?.confirmation !== 'LIVE') {
        res.status(400).json({ error: 'LIVE mode requires explicit confirmation' });
        return;
    }

    if (mode === 'LIVE') {
        const readiness = await isLiveReady();
        if (!readiness.ready) {
            res.status(412).json({
                error: 'Live mode preconditions not met',
                failures: readiness.failures,
            });
            return;
        }
    }

    await setTradingMode(mode);
    res.json({
        ok: true,
        mode,
        live_order_posting_enabled: isLiveOrderPostingEnabled(),
    });
});

app.post('/api/system/reset-simulation', requireControlPlaneAuth, async (req, res) => {
    const currentMode = await getTradingMode();
    if (currentMode !== 'PAPER') {
        res.status(409).json({ error: 'Simulation reset is only allowed in PAPER mode' });
        return;
    }

    if (req.body?.confirmation !== 'RESET') {
        res.status(400).json({ error: 'Simulation reset requires explicit RESET confirmation' });
        return;
    }

    const bankroll = normalizeResetBankroll(req.body?.bankroll);
    if (bankroll === null) {
        res.status(400).json({ error: 'Invalid reset bankroll value' });
        return;
    }

    const forceDefaults = req.body?.force_defaults === true;
    await resetSimulationState(bankroll, { forceDefaults });
    res.json({ ok: true, bankroll, force_defaults: forceDefaults });
});

async function runStrategyGovernanceCycle(): Promise<void> {
    if (!STRATEGY_GOVERNANCE_ENABLED) {
        return;
    }
    if (governanceCycleInFlight) {
        return;
    }
    governanceCycleInFlight = true;

    try {
        const tradingMode = await getTradingMode();
        const autopilotAllowed = strategyGovernanceState.autopilot_enabled && tradingMode === 'PAPER';
        strategyGovernanceState.trading_mode = tradingMode;
        strategyGovernanceState.autopilot_effective = autopilotAllowed;
        const nextDecisions: Record<string, StrategyGovernanceDecision> = {};
        for (const strategyId of STRATEGY_IDS) {
            const evaluated = evaluateStrategyGovernance(strategyId);
            const applied = await maybeApplyGovernanceDecision(evaluated, autopilotAllowed);
            const previous = strategyGovernanceState.decisions[strategyId];
            const materialChange = !previous
                || previous.action !== applied.action
                || previous.enabled_after !== applied.enabled_after
                || Math.abs(previous.multiplier_after - applied.multiplier_after) >= 0.05;

            if (materialChange || applied.autopilot_applied) {
                strategyGovernanceState.audit = [
                    applied,
                    ...strategyGovernanceState.audit,
                ].slice(0, STRATEGY_GOVERNANCE_AUDIT_LIMIT);
                io.emit('strategy_governance_audit', applied);
            }

            nextDecisions[strategyId] = applied;
            io.emit('strategy_governance_update', applied);
        }

        strategyGovernanceState.decisions = nextDecisions;
        strategyGovernanceState.updated_at = Date.now();
        io.emit('strategy_governance_snapshot', governancePayload());
        touchRuntimeModule(
            'FEATURE_REGISTRY',
            'ONLINE',
            `governance cycle ${STRATEGY_IDS.length} strategies (${autopilotAllowed ? 'autopilot' : 'advisory'} ${tradingMode})`,
        );
    } finally {
        governanceCycleInFlight = false;
    }
}

function scheduleNonOverlappingTask(taskName: string, intervalMs: number, task: () => Promise<void>): void {
    let inFlight = false;
    setInterval(() => {
        if (inFlight) {
            return;
        }
        inFlight = true;
        void task()
            .catch((error) => {
                console.error(`[${taskName}] interval error:`, error);
            })
            .finally(() => {
                inFlight = false;
            });
    }, intervalMs);
}

// Agentic Decision Loop — DISABLED by default. Set LEGACY_BRAIN_LOOP_ENABLED=true to re-enable.
if (LEGACY_BRAIN_LOOP_ENABLED) {
    console.warn('[Brain] LEGACY_BRAIN_LOOP_ENABLED=true — running deprecated agentic decision loop.');
    scheduleNonOverlappingTask('Brain', 30_000, async () => {
        try {
            const symbol = 'BTC-USD';

            if (agentManager.getActiveAgents().length === 0) {
                return;
            }

            const { decisions, context } = await agentManager.runAnalysis(symbol);
            io.emit('agent_decisions', decisions);

            const validDecision = decisionGate.evaluate(decisions);
            if (!validDecision) {
                return;
            }

            if (!riskGuard.validate(validDecision, context)) {
                return;
            }

            const tradingMode = await getTradingMode();
            const orderId = await executor.executeOrder(validDecision, symbol, tradingMode);

            io.emit('trade_execution', {
                ...validDecision,
                orderId,
                tradingMode,
                timestamp: Date.now(),
            });
        } catch (error) {
            console.error('[Brain] Error in decision loop:', error);
        }
    });
} else {
    console.info('[Brain] Legacy decision loop disabled (default). Set LEGACY_BRAIN_LOOP_ENABLED=true to re-enable.');
}

// System Health Heartbeat (Every 2s)
scheduleNonOverlappingTask('SystemHealth', 2000, async () => {
    const start = Date.now();
    let redisLatency = 0;
    try {
        await redisClient.ping();
        redisLatency = Date.now() - start;
    } catch {
        redisLatency = -1;
    }

    const memory = process.memoryUsage();
    const now = Date.now();
    refreshModelDriftRuntime(now);
    const scannerLastBeat = Math.max(...SCANNER_HEARTBEAT_IDS.map((id) => heartbeats[id] || 0));
    const isScannerAlive = now - scannerLastBeat < 15_000;

    const metrics = [
        {
            name: 'Trading Engine',
            status: 'operational',
            load: `${(memory.heapUsed / 1024 / 1024).toFixed(0)}MB`,
        },
        {
            name: 'Redis Queue',
            status: redisLatency >= 0 ? 'operational' : 'degraded',
            load: redisLatency >= 0 ? `${redisLatency}ms` : 'Err',
        },
        {
            name: 'ML Service',
            status: 'standby',
            load: '0%',
        },
        {
            name: 'Arb Scanner',
            status: isScannerAlive ? 'operational' : 'offline',
            load: isScannerAlive
                ? `Signals: ${getActiveSignalCount(now)} | Markets: ${getActiveMarketCount(now)}`
                : 'No Signal',
        },
    ];

    io.emit('system_health_update', metrics);
    io.emit('runtime_status_update', runtimeStatusPayload());
});

scheduleNonOverlappingTask('Settlement', settlementService.getPollIntervalMs(), async () => {
    try {
        const events = await settlementService.runCycle(await getTradingMode(), isLiveOrderPostingEnabled());
        touchRuntimeModule('SETTLEMENT_ENGINE', 'ONLINE', `cycle complete (${events.length} new event${events.length === 1 ? '' : 's'})`);
        await emitSettlementEvents(events);
        if (events.length === 0 && settlementService.getSnapshot().positions.length > 0) {
            io.emit('settlement_snapshot', settlementService.getSnapshot());
        }
    } catch (error) {
        console.error('[Settlement] cycle error:', error);
    }
});

scheduleNonOverlappingTask('Governance', STRATEGY_GOVERNANCE_INTERVAL_MS, async () => {
    try {
        await runStrategyGovernanceCycle();
    } catch (error) {
        console.error('[Governance] cycle error:', error);
        touchRuntimeModule('FEATURE_REGISTRY', 'DEGRADED', `governance error: ${String(error)}`);
    }
});

scheduleNonOverlappingTask('LedgerHealth', 5_000, async () => {
    try {
        await refreshLedgerHealth();
    } catch (error) {
        console.error('[LedgerHealth] refresh error:', error);
        touchRuntimeModule('PNL_LEDGER', 'DEGRADED', `ledger health error: ${String(error)}`);
    }
});

scheduleNonOverlappingTask('MetaController', 3_000, async () => {
    requestMetaControllerRefresh();
});

scheduleNonOverlappingTask('MLPipeline', 10_000, async () => {
    try {
        await refreshMlPipelineStatus();
    } catch (error) {
        console.error('[MLPipeline] refresh error:', error);
        touchRuntimeModule('ML_TRAINER', 'DEGRADED', `ml pipeline error: ${String(error)}`);
        touchRuntimeModule('MODEL_INFERENCE', 'DEGRADED', `ml pipeline error: ${String(error)}`);
        touchRuntimeModule('DRIFT_MONITOR', 'DEGRADED', `ml pipeline error: ${String(error)}`);
    }
});

scheduleNonOverlappingTask('MLTrainer', MODEL_TRAINER_INTERVAL_MS, async () => {
    try {
        await runInProcessModelTraining(false);
    } catch (error) {
        console.error('[MLTrainer] in-process training cycle error:', error);
        touchRuntimeModule('ML_TRAINER', 'DEGRADED', `trainer cycle error: ${String(error)}`);
    }
});

// Start Services
async function bootstrap() {
    await connectRedis();
    await strategyTradeRecorder.init();
    await featureRegistryRecorder.init();
    await settlementService.init();
    await marketDataService.start();

    if (!isControlPlaneTokenConfigured()) {
        console.error('CONTROL_PLANE_TOKEN is not configured. Control actions are locked until a token is set.');
    }

    await redisClient.setNX(SIM_BANKROLL_KEY, DEFAULT_SIM_BANKROLL.toFixed(2));
    await redisClient.setNX(SIM_LEDGER_CASH_KEY, DEFAULT_SIM_BANKROLL.toFixed(8));
    await redisClient.setNX(SIM_LEDGER_RESERVED_KEY, '0');
    await redisClient.setNX(SIM_LEDGER_REALIZED_PNL_KEY, '0');
    await redisClient.setNX('system:simulation_reset_ts', '0');
    if (SIM_RESET_ON_BOOT) {
        await resetSimulationState(DEFAULT_SIM_BANKROLL, { forceDefaults: true });
    } else {
        await getSimulationLedgerSnapshot();
    }
    await loadStrategyMetrics();
    await reconcileSimulationLedgerWithStrategyMetrics();
    await getRiskConfig();
    await redisClient.setNX('system:trading_mode', 'PAPER');
    touchRuntimeModule('TRADING_MODE_GUARD', 'ONLINE', `boot mode ${(await getTradingMode())}`);
    await applyDefaultStrategyStates(false);
    await bootstrapStrategyMultipliers();
    await loadRiskGuardStates();
    await runStrategyGovernanceCycle();
    await refreshLedgerHealth();
    await refreshMetaController();
    await refreshMlPipelineStatus();
    refreshModelDriftRuntime();
    await runInProcessModelTraining(false);

    // ── Periodic state broadcast: keep all dashboards live ──
    scheduleNonOverlappingTask('StateBroadcast', 5000, async () => {
        try {
            const ledger = await getSimulationLedgerSnapshot();
            const vault = parseFloat(await redisClient.get(VAULT_REDIS_KEY) || '0');
            io.emit('strategy_metrics_update', strategyMetrics);
            io.emit('strategy_status_update', strategyStatus);
            io.emit('vault_update', { vault: Math.round(vault * 100) / 100, ceiling: VAULT_BANKROLL_CEILING });
            io.emit('sim_ledger_update', {
                available_cash: ledger.cash,
                reserved: ledger.reserved,
                realized_pnl: ledger.realized_pnl,
                equity: ledger.equity,
                utilization_pct: ledger.utilization_pct,
            });
            for (const id of RISK_GUARD_STRATEGIES) {
                const state = riskGuardState[id];
                if (state) io.emit('risk_guard_update', { strategy: id, ...state });
            }
        } catch { /* non-critical broadcast */ }
    });

    const PORT = Number(process.env.PORT) || 5114;
    httpServer.listen(PORT, () => {
        console.log(`Backend server running on port ${PORT}`);
    });
}

bootstrap().catch((error) => {
    console.error('Failed to bootstrap backend:', error);
    process.exit(1);
});

let shutdownInProgress = false;

const shutdown = async (signal: string) => {
    if (shutdownInProgress) {
        return;
    }
    shutdownInProgress = true;
    console.info(`[System] ${signal} received, shutting down gracefully...`);

    await new Promise<void>((resolve) => {
        httpServer.close(() => resolve());
    });

    if (redisSubscriber.isOpen) {
        await redisSubscriber.quit().catch(() => undefined);
    }
    if (redisClient.isOpen) {
        await redisClient.quit().catch(() => undefined);
    }

    process.exit(0);
};
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
