import express from 'express';
import type { NextFunction, Request, Response } from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';
import cors from 'cors';
import helmet from 'helmet';
import dotenv from 'dotenv';
import fs from 'fs/promises';
import { createReadStream } from 'fs';
import * as readline from 'readline';
import path from 'path';
import { createHmac, randomUUID, timingSafeEqual } from 'crypto';
import { Side } from '@polymarket/clob-client';

import { MarketDataService, type MarketTickerMessage } from './services/MarketDataService';
import { ContextBuilder } from './services/ContextBuilder';
import { AgentManager } from './agents/AgentManager';
import { DecisionGate } from './agents/DecisionGate';
import { RiskGuard } from './services/RiskGuard';
import { HyperliquidExecutor, TradingMode } from './services/HyperliquidExecutor';
import { PolymarketPreflightService } from './services/PolymarketPreflightService';
import type { PolymarketLiveExecutionResult } from './services/PolymarketPreflightService';
import { PolymarketSettlementService, settlementEventToExecutionLog } from './services/PolymarketSettlementService';
import {
    StrategyTradeRecorder,
    classifyClosureReason,
    inferTradeClosureReason,
    type StrategyTradeRecorderSummary,
} from './services/StrategyTradeRecorder';
import { FeatureRegistryRecorder } from './services/FeatureRegistryRecorder';
import { RejectedSignalRecorder } from './services/RejectedSignalRecorder';
import { extractBearerToken } from './middleware/auth';
import { connectRedis, redisClient, subscriber as redisSubscriber } from './config/redis';
import { createRiskGuardModule } from './modules/risk/riskGuardModule';
import { decideBootResumeAction, shouldResumeFromBootTimer } from './modules/risk/bootResumeDecider';
import {
    normalizeTradingMode,
    validateSimulationResetRequest,
    validateStrategyToggleRequest,
    validateTradingModeChangeRequest,
} from './modules/control/controlPlaneValidation';
import {
    createLedgerHealthModule,
    type SimulationLedgerSnapshot,
    type LedgerHealthState,
} from './modules/ledger/ledgerHealthModule';
import {
    processArbitrageExecutionWorker,
    type RejectedSignalRecord,
} from './modules/execution/arbExecutionPipeline';
import {
    clearExecutionHistory as clearExecutionHistoryStore,
    persistExecutionHistoryEntry,
    restoreExecutionHistory,
    type StrategyExecutionHistoryStoreConfig,
} from './modules/execution/strategyExecutionHistoryStore';
import {
    classifyEntryFreshnessViolation,
    type EntryFreshnessViolationCategory,
} from './modules/execution/entryFreshnessSlo';
import { scheduleNonOverlappingTask } from './modules/runtime/scheduler';
import {
    buildModelGateCalibrationSamples,
    type ModelGateCalibrationSample,
} from './modules/model/modelGateCalibration';
import { validateArbitrageScanContract } from './modules/ingest/scanContract';
import { logger } from './utils/logger';

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
                logger.warn(`[CORS] blocked origin=${requestOrigin}`);
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
    EXECUTION_EVENT_HMAC_SECRET, EXECUTION_EVENT_REQUIRE_SIGNATURE,
    DEFAULT_SIM_BANKROLL, STRATEGY_METRICS_KEY,
    SIM_BANKROLL_KEY, SIM_LEDGER_CASH_KEY, SIM_LEDGER_RESERVED_KEY,
    SIM_LEDGER_REALIZED_PNL_KEY, SIM_LEDGER_RESERVED_BY_STRATEGY_PREFIX,
    SIM_LEDGER_RESERVED_BY_FAMILY_PREFIX, SIM_LEDGER_RESERVED_BY_UNDERLYING_PREFIX,
    SIM_STRATEGY_CONCENTRATION_CAP_PCT, SIM_FAMILY_CONCENTRATION_CAP_PCT,
    SIM_UNDERLYING_CONCENTRATION_CAP_PCT, SIM_GLOBAL_UTILIZATION_CAP_PCT,
    STRATEGY_RISK_MULTIPLIER_PREFIX, STRATEGY_WEIGHT_FLOOR, STRATEGY_WEIGHT_CAP,
    STRATEGY_ALLOCATOR_EPSILON, STRATEGY_ALLOCATOR_MIN_SAMPLES,
    STRATEGY_ALLOCATOR_TARGET_SHARPE, DEFAULT_DISABLED_STRATEGIES,
    META_ALLOCATOR_ENABLED, META_ALLOCATOR_META_BLEND,
    META_ALLOCATOR_MIN_OVERLAY, META_ALLOCATOR_MAX_OVERLAY,
    META_ALLOCATOR_FAMILY_CONCENTRATION_SOFT_CAP, META_ALLOCATOR_COST_DRAG_PENALTY_BPS,
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
    EXECUTION_SHORTFALL_OPTIMIZER_ENABLED, EXECUTION_SHORTFALL_TARGET_RETAIN_BPS,
    EXECUTION_SHORTFALL_MIN_RETAIN_BPS, EXECUTION_SHORTFALL_MAX_SIZE_REDUCTION_PCT,
    EXECUTION_SHORTFALL_MIN_SIGNAL_NOTIONAL_USD,
    EXECUTION_NETTING_ENABLED, EXECUTION_NETTING_TTL_MS,
    EXECUTION_NETTING_MARKET_CAP_USD, EXECUTION_NETTING_OPPOSING_BLOCK_RATIO,
    DATA_INTEGRITY_ALERT_COOLDOWN_MS, DATA_INTEGRITY_ALERT_MIN_CONSECUTIVE_WARN,
    DATA_INTEGRITY_ALERT_MIN_CONSECUTIVE_CRITICAL, DATA_INTEGRITY_ALERT_RING_LIMIT,
    STRATEGY_SAMPLE_RETENTION,
    LEDGER_PNL_PARITY_ENABLED, LEDGER_PNL_PARITY_INTERVAL_MS,
    LEDGER_PNL_PARITY_WARN_ABS_USD, LEDGER_PNL_PARITY_CRITICAL_ABS_USD,
    LEDGER_PNL_PARITY_MIN_PAPER_ROWS,
    STRATEGY_GOVERNANCE_ENABLED, STRATEGY_GOVERNANCE_AUTOPILOT,
    STRATEGY_GOVERNANCE_INTERVAL_MS, STRATEGY_GOVERNANCE_MIN_TRADES,
    STRATEGY_GOVERNANCE_MIN_TRADES_DEMOTE, STRATEGY_GOVERNANCE_ACTION_COOLDOWN_MS,
    STRATEGY_GOVERNANCE_AUDIT_LIMIT, STRATEGY_GOVERNANCE_WALK_FORWARD_SPLITS,
    STRATEGY_GOVERNANCE_MIN_TEST_TRADES,
    FEATURE_REGISTRY_MAX_ROWS, FEATURE_REGISTRY_LABEL_LOOKBACK_MS,
    META_CONTROLLER_ENABLED, META_CONTROLLER_ADVISORY_ONLY, META_CONTROLLER_REFRESH_DEBOUNCE_MS,
    CROSS_HORIZON_ROUTER_ENABLED, CROSS_HORIZON_ROUTER_INTERVAL_MS,
    CROSS_HORIZON_ROUTER_MIN_MARGIN, CROSS_HORIZON_ROUTER_STRONG_MARGIN,
    CROSS_HORIZON_ROUTER_CONFLICT_MULT, CROSS_HORIZON_ROUTER_CONFIRM_MULT,
    CROSS_HORIZON_ROUTER_MAX_BOOST, CROSS_HORIZON_OVERLAY_KEY_PREFIX,
    MODEL_PROBABILITY_GATE_ENABLED, MODEL_PROBABILITY_GATE_ENFORCE_PAPER,
    MODEL_PROBABILITY_GATE_ENFORCE_LIVE, LEGACY_BRAIN_LOOP_ENABLED, LEGACY_BRAIN_LOOP_UNSAFE_OK,
    MODEL_PROBABILITY_GATE_MIN_PROB, MODEL_PROBABILITY_GATE_MAX_STALENESS_MS,
    MODEL_PROBABILITY_GATE_REQUIRE_MODEL, MODEL_PROBABILITY_GATE_DISABLE_ON_DRIFT,
    MODEL_PROBABILITY_GATE_DRIFT_DISABLE_MS,
    MODEL_PROBABILITY_GATE_TUNER_ENABLED, MODEL_PROBABILITY_GATE_TUNER_ADVISORY_ONLY,
    MODEL_PROBABILITY_GATE_TUNER_INTERVAL_MS, MODEL_PROBABILITY_GATE_TUNER_LOOKBACK_MS,
    MODEL_PROBABILITY_GATE_COUNTERFACTUAL_WINDOW_MS,
    MODEL_PROBABILITY_GATE_TUNER_MIN_SAMPLES, MODEL_PROBABILITY_GATE_TUNER_STEP,
    MODEL_PROBABILITY_GATE_TUNER_MAX_DRIFT,
    MODEL_TRAINER_ENABLED, MODEL_TRAINER_INTERVAL_MS,
    MODEL_TRAINER_MIN_LABELED_ROWS, MODEL_TRAINER_MIN_NEW_LABELS, MODEL_TRAINER_MAX_ROWS,
    MODEL_TRAINER_SPLITS, MODEL_TRAINER_PURGE_ROWS, MODEL_TRAINER_EMBARGO_ROWS,
    MODEL_TRAINER_MIN_TRAIN_ROWS_PER_FOLD, MODEL_TRAINER_MIN_TEST_ROWS_PER_FOLD,
    MODEL_PREDICTION_TRACE_MAX, MODEL_INFERENCE_MAX_TRACKED_ROWS,
    BACKEND_APP_ROOT, FEATURE_REGISTRY_EVENT_LOG_PATH,
    FEATURE_DATASET_MANIFEST_PATH, SIGNAL_MODEL_REPORT_PATH,
    SIGNAL_MODEL_ARTIFACT_PATH,
    BUILD_ACTIVE_PHASE, RUNTIME_OFFLINE_MULTIPLIER, RUNTIME_MODULE_CATALOG,
} from './config/constants';

// Runtime state (not extractable — mutable per-process)
const executionTraces = new Map<string, ExecutionTrace>();
const rejectedSignalsByExecutionId = new Map<string, RejectedSignalRecord>();
const strategyExecutionHistory = new Map<string, Record<string, unknown>[]>();
type ExecutionHistoryContext = {
    execution_id: string;
    strategy: string | null;
    market_key: string | null;
    feature_row_id: string | null;
    timestamp: number;
};
const executionHistoryContextById = new Map<string, ExecutionHistoryContext>();
const STRATEGY_EXECUTION_HISTORY_LIMIT = Math.max(
    100,
    Number(process.env.STRATEGY_EXECUTION_HISTORY_LIMIT || '500'),
);
const EXECUTION_HISTORY_CONTEXT_MAX = Math.max(
    2_000,
    STRATEGY_IDS.length * STRATEGY_EXECUTION_HISTORY_LIMIT,
);
const STRATEGY_EXECUTION_HISTORY_REDIS_PREFIX = (
    process.env.STRATEGY_EXECUTION_HISTORY_REDIS_PREFIX
    || 'system:strategy_execution_history:v1:'
).trim() || 'system:strategy_execution_history:v1:';
const STRATEGY_EXECUTION_HISTORY_STORE_CONFIG: StrategyExecutionHistoryStoreConfig = {
    redisPrefix: STRATEGY_EXECUTION_HISTORY_REDIS_PREFIX,
    limit: STRATEGY_EXECUTION_HISTORY_LIMIT,
    strategyIds: STRATEGY_IDS,
};

const strategyStatus: Record<string, boolean> = Object.fromEntries(STRATEGY_IDS.map((id) => [id, true]));

interface SilentCatchTelemetryEntry {
    count: number;
    first_seen: number;
    last_seen: number;
    last_message: string;
    last_context: Record<string, string | number | boolean | null>;
    last_log_ms: number;
}

const silentCatchTelemetry: Record<string, SilentCatchTelemetryEntry> = {};
const SILENT_CATCH_LOG_COOLDOWN_MS = 30_000;
const FEATURE_LABEL_BACKFILL_INTERVAL_MS = Math.max(
    60_000,
    Number(process.env.FEATURE_LABEL_BACKFILL_INTERVAL_MS || '180000'),
);

function toTelemetryErrorMessage(error: unknown): string {
    if (error instanceof Error) {
        return `${error.name}: ${error.message}`.slice(0, 240);
    }
    if (typeof error === 'string') {
        return error.slice(0, 240);
    }
    try {
        return JSON.stringify(error).slice(0, 240);
    } catch {
        return String(error).slice(0, 240);
    }
}

function toTelemetryContext(
    input: Record<string, unknown> | undefined,
): Record<string, string | number | boolean | null> {
    if (!input) {
        return {};
    }
    const out: Record<string, string | number | boolean | null> = {};
    const entries = Object.entries(input).slice(0, 8);
    for (const [key, value] of entries) {
        if (value === null) {
            out[key] = null;
            continue;
        }
        if (typeof value === 'string') {
            out[key] = value.slice(0, 180);
            continue;
        }
        if (typeof value === 'number') {
            out[key] = Number.isFinite(value) ? value : null;
            continue;
        }
        if (typeof value === 'boolean') {
            out[key] = value;
            continue;
        }
        out[key] = String(value).slice(0, 180);
    }
    return out;
}

function recordSilentCatch(
    scope: string,
    error: unknown,
    context?: Record<string, unknown>,
): void {
    const now = Date.now();
    const entry = silentCatchTelemetry[scope] ?? {
        count: 0,
        first_seen: now,
        last_seen: now,
        last_message: '',
        last_context: {},
        last_log_ms: 0,
    };
    entry.count += 1;
    entry.last_seen = now;
    entry.last_message = toTelemetryErrorMessage(error);
    entry.last_context = toTelemetryContext(context);
    silentCatchTelemetry[scope] = entry;

    if (entry.last_log_ms === 0 || now - entry.last_log_ms >= SILENT_CATCH_LOG_COOLDOWN_MS) {
        entry.last_log_ms = now;
        logger.warn(
            `[SilentCatch][${scope}] count=${entry.count} error=${entry.last_message} context=${JSON.stringify(entry.last_context)}`,
        );
    }

    io.emit('silent_catch_telemetry_update', {
        scope,
        count: entry.count,
        first_seen: entry.first_seen,
        last_seen: entry.last_seen,
        last_message: entry.last_message,
        last_context: entry.last_context,
    });
}

function fireAndForget(
    scope: string,
    task: Promise<unknown>,
    context?: Record<string, unknown>,
): void {
    void task.catch((error) => {
        recordSilentCatch(scope, error, context);
    });
}

function silentCatchTelemetryPayload(limit = 80): Record<string, {
    count: number;
    first_seen: number;
    last_seen: number;
    last_message: string;
    last_context: Record<string, string | number | boolean | null>;
}> {
    return Object.fromEntries(
        Object.entries(silentCatchTelemetry)
            .sort((a, b) => b[1].last_seen - a[1].last_seen)
            .slice(0, limit)
            .map(([scope, entry]) => [scope, {
                count: entry.count,
                first_seen: entry.first_seen,
                last_seen: entry.last_seen,
                last_message: entry.last_message,
                last_context: entry.last_context,
            }]),
    );
}

// ── Risk Guard state ─────────────────────────────────────────────────
const {
    processRiskGuard,
    sweepProfitsToVault,
    loadRiskGuardStates,
    resetRiskGuardStates,
    clearStrategyPause,
    getRiskGuardState,
} = createRiskGuardModule({
    redis: redisClient,
    strategyStatus,
    emitStrategyStatusUpdate: (status) => {
        io.emit('strategy_status_update', status);
    },
    emitRiskGuardUpdate: (payload) => {
        io.emit('risk_guard_update', payload);
    },
    emitVaultUpdate: (payload) => {
        io.emit('vault_update', payload);
    },
    recordError: recordSilentCatch,
    config: {
        trailingStop: RISK_GUARD_TRAILING_STOP,
        consecutiveLossLimit: RISK_GUARD_CONSEC_LOSS_LIMIT,
        consecutiveLossCooldownMs: RISK_GUARD_CONSEC_LOSS_COOLDOWN_MS,
        postLossCooldownMs: RISK_GUARD_POST_LOSS_COOLDOWN_MS,
        antiMartingaleAfter: RISK_GUARD_ANTI_MARTINGALE_AFTER,
        antiMartingaleFactor: RISK_GUARD_ANTI_MARTINGALE_FACTOR,
        directionLimit: RISK_GUARD_DIRECTION_LIMIT,
        profitTaperStart: RISK_GUARD_PROFIT_TAPER_START,
        profitTaperFloor: RISK_GUARD_PROFIT_TAPER_FLOOR,
        dailyTarget: RISK_GUARD_DAILY_TARGET,
        dayResetHourUtc: RISK_GUARD_DAY_RESET_HOUR_UTC,
        strategies: RISK_GUARD_STRATEGIES,
        vaultEnabled: VAULT_ENABLED,
        vaultBankrollCeiling: VAULT_BANKROLL_CEILING,
        vaultRedisKey: VAULT_REDIS_KEY,
        simLedgerCashKey: SIM_LEDGER_CASH_KEY,
        simBankrollKey: SIM_BANKROLL_KEY,
    },
});

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
        base_multiplier: DEFAULT_DISABLED_STRATEGIES.has(id) ? 0 : 1,
        meta_overlay: 1,
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
const REJECTED_SIGNAL_RETENTION = 2_000;
const rejectedSignalHistory: RejectedSignalRecord[] = [];
type EntryFreshnessSloStrategy = {
    strategy: string;
    total: number;
    missing_telemetry: number;
    stale_source: number;
    stale_gate: number;
    other: number;
    last_violation_at: number;
    last_reason: string;
};
type EntryFreshnessSloSnapshot = {
    totals: {
        total: number;
        missing_telemetry: number;
        stale_source: number;
        stale_gate: number;
        other: number;
    };
    strategies: Record<string, EntryFreshnessSloStrategy>;
    recent_alerts: Array<{
        execution_id: string;
        strategy: string;
        market_key: string | null;
        category: EntryFreshnessViolationCategory;
        reason: string;
        timestamp: number;
    }>;
    updated_at: number;
};
const ENTRY_FRESHNESS_ALERT_RING_LIMIT = Math.max(
    10,
    Number(process.env.ENTRY_FRESHNESS_ALERT_RING_LIMIT || '150'),
);
const entryFreshnessSloState: EntryFreshnessSloSnapshot = {
    totals: {
        total: 0,
        missing_telemetry: 0,
        stale_source: 0,
        stale_gate: 0,
        other: 0,
    },
    strategies: {},
    recent_alerts: [],
    updated_at: 0,
};
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
const ledgerHealthModule = createLedgerHealthModule({
    redis: redisClient,
    config: {
        defaultSimBankroll: DEFAULT_SIM_BANKROLL,
        simBankrollKey: SIM_BANKROLL_KEY,
        simLedgerCashKey: SIM_LEDGER_CASH_KEY,
        simLedgerReservedKey: SIM_LEDGER_RESERVED_KEY,
        simLedgerRealizedPnlKey: SIM_LEDGER_REALIZED_PNL_KEY,
        reservedByStrategyPrefix: SIM_LEDGER_RESERVED_BY_STRATEGY_PREFIX,
        reservedByFamilyPrefix: SIM_LEDGER_RESERVED_BY_FAMILY_PREFIX,
        reservedByUnderlyingPrefix: SIM_LEDGER_RESERVED_BY_UNDERLYING_PREFIX,
        strategyConcentrationCapPct: SIM_STRATEGY_CONCENTRATION_CAP_PCT,
        familyConcentrationCapPct: SIM_FAMILY_CONCENTRATION_CAP_PCT,
        underlyingConcentrationCapPct: SIM_UNDERLYING_CONCENTRATION_CAP_PCT,
        globalUtilizationCapPct: SIM_GLOBAL_UTILIZATION_CAP_PCT,
    },
    emitLedgerHealthUpdate: (payload) => {
        io.emit('ledger_health_update', payload);
    },
    touchLedgerRuntime: (health, detail) => {
        touchRuntimeModule('PNL_LEDGER', health, detail);
    },
    recordError: recordSilentCatch,
});
let ledgerHealthState: LedgerHealthState = ledgerHealthModule.getLedgerHealthState();
type PnlParityStatus = 'HEALTHY' | 'WARN' | 'CRITICAL';
type PnlParityState = {
    enabled: boolean;
    status: PnlParityStatus;
    checked_at: number;
    ledger_realized_pnl: number;
    trade_log_paper_pnl: number;
    delta_abs_usd: number;
    delta_signed_usd: number;
    row_count: number;
    paper_row_count: number;
    min_paper_rows: number;
    warn_abs_usd: number;
    critical_abs_usd: number;
    issues: string[];
};
let pnlParityState: PnlParityState = {
    enabled: LEDGER_PNL_PARITY_ENABLED,
    status: 'HEALTHY',
    checked_at: 0,
    ledger_realized_pnl: 0,
    trade_log_paper_pnl: 0,
    delta_abs_usd: 0,
    delta_signed_usd: 0,
    row_count: 0,
    paper_row_count: 0,
    min_paper_rows: LEDGER_PNL_PARITY_MIN_PAPER_ROWS,
    warn_abs_usd: LEDGER_PNL_PARITY_WARN_ABS_USD,
    critical_abs_usd: LEDGER_PNL_PARITY_CRITICAL_ABS_USD,
    issues: LEDGER_PNL_PARITY_ENABLED ? ['pending first check'] : ['disabled via LEDGER_PNL_PARITY_ENABLED=false'],
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
const MODEL_INFERENCE_PAYLOAD_MAX_ROWS = Math.max(
    50,
    Math.min(MODEL_INFERENCE_MAX_TRACKED_ROWS, Number(process.env.MODEL_INFERENCE_PAYLOAD_MAX_ROWS || '300')),
);
let modelDriftState: ModelDriftState = {
    status: 'HEALTHY',
    sample_count: 0,
    brier_ema: 0,
    logloss_ema: 0,
    calibration_error_ema: 0,
    accuracy_pct: 0,
    gate_enabled: MODEL_PROBABILITY_GATE_ENABLED,
    gate_enforcing: MODEL_PROBABILITY_GATE_ENABLED
        && MODEL_PROBABILITY_GATE_ENFORCE_PAPER
        && !MODEL_PROBABILITY_GATE_REQUIRE_MODEL,
    gate_disabled_until: 0,
    issues: [],
    by_strategy: {},
    updated_at: 0,
};
type ModelGateCalibrationState = {
    enabled: boolean;
    advisory_only: boolean;
    baseline_gate: number;
    active_gate: number;
    recommended_gate: number;
    lookback_ms: number;
    min_samples: number;
    step_size: number;
    max_drift: number;
    sample_count: number;
    accepted_samples: number;
    rejected_samples: number;
    expected_pnl_baseline: number;
    expected_pnl_recommended: number;
    delta_expected_pnl: number;
    profitable_rejected_signals: number;
    loss_avoided_signals: number;
    updated_at: number;
    reason: string;
    strategy_active_gates: Record<string, number>;
    strategy_recommended_gates: Record<string, number>;
    strategy_sample_counts: Record<string, number>;
    market_active_gates: Record<string, number>;
    market_recommended_gates: Record<string, number>;
    market_sample_counts: Record<string, number>;
};
const MODEL_GATE_CALIBRATION_KEY = 'model_gate:calibration_state';
const MODEL_GATE_STRATEGY_MIN_SAMPLES = Math.max(25, Math.floor(MODEL_PROBABILITY_GATE_TUNER_MIN_SAMPLES * 0.35));
const MODEL_GATE_MARKET_MIN_SAMPLES = Math.max(15, Math.floor(MODEL_PROBABILITY_GATE_TUNER_MIN_SAMPLES * 0.20));
const MODEL_GATE_MARKET_MAX_KEYS = Math.max(
    20,
    Math.min(400, Number(process.env.MODEL_GATE_MARKET_MAX_KEYS || '120')),
);
let modelGateCalibrationState: ModelGateCalibrationState = {
    enabled: MODEL_PROBABILITY_GATE_TUNER_ENABLED,
    advisory_only: MODEL_PROBABILITY_GATE_TUNER_ADVISORY_ONLY,
    baseline_gate: MODEL_PROBABILITY_GATE_MIN_PROB,
    active_gate: MODEL_PROBABILITY_GATE_MIN_PROB,
    recommended_gate: MODEL_PROBABILITY_GATE_MIN_PROB,
    lookback_ms: MODEL_PROBABILITY_GATE_TUNER_LOOKBACK_MS,
    min_samples: MODEL_PROBABILITY_GATE_TUNER_MIN_SAMPLES,
    step_size: MODEL_PROBABILITY_GATE_TUNER_STEP,
    max_drift: MODEL_PROBABILITY_GATE_TUNER_MAX_DRIFT,
    sample_count: 0,
    accepted_samples: 0,
    rejected_samples: 0,
    expected_pnl_baseline: 0,
    expected_pnl_recommended: 0,
    delta_expected_pnl: 0,
    profitable_rejected_signals: 0,
    loss_avoided_signals: 0,
    updated_at: 0,
    reason: 'not yet calibrated',
    strategy_active_gates: {},
    strategy_recommended_gates: {},
    strategy_sample_counts: {},
    market_active_gates: {},
    market_recommended_gates: {},
    market_sample_counts: {},
};
const modelPredictionByRowId = new Map<string, ModelPredictionTrace>();
let modelTrainerInFlight = false;
let modelTrainerLastLabeledRows = 0;
let governanceCycleInFlight = false;
let lastScannerParseErrorLogMs = 0;
let metaControllerRefreshTimer: NodeJS.Timeout | null = null;
let metaControllerRefreshScheduled = false;
let metaControllerRefreshInFlight = false;
let crossHorizonRefreshTimer: NodeJS.Timeout | null = null;
let crossHorizonRefreshScheduled = false;
let crossHorizonRefreshInFlight = false;
const CROSS_HORIZON_REFRESH_DEBOUNCE_MS = Math.max(
    150,
    Math.min(1_500, Math.floor(CROSS_HORIZON_ROUTER_INTERVAL_MS / 2)),
);
const heartbeats: Record<string, number> = {};
const SCANNER_HEARTBEAT_MAX_AGE_MS = Math.max(
    15_000,
    Number(process.env.SCANNER_HEARTBEAT_MAX_AGE_MS || '60000'),
);
const OPTIONAL_SCANNER_HEARTBEAT_IDS = new Set<string>(
    (process.env.SCANNER_OPTIONAL_HEARTBEAT_IDS || '')
        .split(',')
        .map((value) => value.trim())
        .filter((value) => value.length > 0),
);
const STRATEGY_HEARTBEAT_ID_MAP: Record<string, string> = {
    BTC_5M: 'btc_5m',
    BTC_15M: 'btc_15m',
    ETH_5M: 'eth_5m',
    ETH_15M: 'eth_15m',
    SOL_5M: 'sol_5m',
    SOL_15M: 'sol_15m',
    CEX_SNIPER: 'cex_arb',
    SYNDICATE: 'SYNDICATE',
    ATOMIC_ARB: 'atomic_arb',
    OBI_SCALPER: 'obi_scalper',
    GRAPH_ARB: 'graph_arb',
    CONVERGENCE_CARRY: 'convergence_carry',
    MAKER_MM: 'maker_mm',
    AS_MARKET_MAKER: 'AS_MARKET_MAKER',
    LONGSHOT_BIAS: 'LONGSHOT_BIAS',
};

type ScannerHeartbeatSnapshot = {
    monitored_ids: string[];
    stale_ids: string[];
    missing_ids: string[];
    last_heartbeat_ms: number;
    alive: boolean;
};

function strategyHeartbeatId(strategyIdRaw: string): string {
    const strategyId = strategyIdRaw.trim().toUpperCase();
    return STRATEGY_HEARTBEAT_ID_MAP[strategyId] || strategyId.toLowerCase();
}

function monitoredScannerHeartbeatIds(): string[] {
    const monitored = new Set<string>();
    for (const strategyId of STRATEGY_IDS) {
        if (strategyStatus[strategyId] === false) {
            continue;
        }
        monitored.add(strategyHeartbeatId(strategyId));
    }

    // Fallback for edge cases where all strategies are disabled.
    if (monitored.size === 0) {
        for (const id of SCANNER_HEARTBEAT_IDS) {
            if ((heartbeats[id] || 0) > 0) {
                monitored.add(id);
            }
        }
    }
    return [...monitored.values()];
}

function scannerHeartbeatSnapshot(now = Date.now()): ScannerHeartbeatSnapshot {
    const monitoredIds = monitoredScannerHeartbeatIds();
    const staleIds: string[] = [];
    const missingIds: string[] = [];
    let lastHeartbeatMs = 0;

    for (const id of monitoredIds) {
        const beat = heartbeats[id] || 0;
        lastHeartbeatMs = Math.max(lastHeartbeatMs, beat);
        if (beat <= 0) {
            if (OPTIONAL_SCANNER_HEARTBEAT_IDS.has(id)) {
                continue;
            }
            missingIds.push(id);
            continue;
        }
        if (now - beat > SCANNER_HEARTBEAT_MAX_AGE_MS) {
            staleIds.push(id);
        }
    }

    return {
        monitored_ids: monitoredIds,
        stale_ids: staleIds,
        missing_ids: missingIds,
        last_heartbeat_ms: lastHeartbeatMs,
        alive: monitoredIds.length > 0 && staleIds.length === 0 && missingIds.length === 0,
    };
}
const riskGuardBootResumeTimers = new Map<string, NodeJS.Timeout>();
const scheduledTaskStops: Array<() => void> = [];
let bootstrapComplete = false;
const latestStrategyScans = new Map<string, StrategyScanState>();
const latestStrategyScansByMarket = new Map<string, StrategyScanState>();
type CrossHorizonPairAction = 'NEUTRAL' | 'CONFLICT_TAPER' | 'CONSENSUS_BOOST' | 'ASYMMETRIC' | 'NO_DATA';
type CrossHorizonPairState = {
    asset: string;
    fast_strategy: string;
    slow_strategy: string;
    fast_margin: number | null;
    slow_margin: number | null;
    fast_direction: number;
    slow_direction: number;
    fast_overlay: number;
    slow_overlay: number;
    action: CrossHorizonPairAction;
    reason: string;
    updated_at: number;
};
type CrossHorizonRouterState = {
    enabled: boolean;
    interval_ms: number;
    overlays: Record<string, number>;
    pairs: Record<string, CrossHorizonPairState>;
    updated_at: number;
    reason: string;
};
type ExecutionNettingMarketState = {
    market_key: string;
    net_directional_notional: number;
    long_notional: number;
    short_notional: number;
    strategy_directional_notional: Record<string, number>;
    updated_at: number;
};
type ScanFeatureSchemaState = {
    strategy: string;
    scans: number;
    expected_keys: string[];
    missing_streak: number;
    last_new_keys: string[];
    last_missing_keys: string[];
    overflow_events: number;
    overflow_dropped_features: number;
    last_overflow_at: number;
    last_alert_at: number;
    updated_at: number;
};
type ExecutionPreparationStage = 'SHORTFALL' | 'NETTING';
type ExecutionPreparationResult = {
    ok: boolean;
    payload: unknown;
    adjusted: boolean;
    stage?: ExecutionPreparationStage;
    reason?: string;
    details?: Record<string, unknown>;
};
const DEFAULT_CROSS_HORIZON_STRATEGY_PAIRS: Array<{ asset: string; fast: string; slow: string }> = [
    { asset: 'BTC', fast: 'BTC_5M', slow: 'BTC_15M' },
    { asset: 'ETH', fast: 'ETH_5M', slow: 'ETH_15M' },
    { asset: 'SOL', fast: 'SOL_5M', slow: 'SOL_15M' },
];
function parseCrossHorizonStrategyPairs(raw: string | null | undefined): Array<{ asset: string; fast: string; slow: string }> {
    const fallback = DEFAULT_CROSS_HORIZON_STRATEGY_PAIRS.filter((pair) => (
        STRATEGY_IDS.includes(pair.fast) && STRATEGY_IDS.includes(pair.slow) && pair.fast !== pair.slow
    ));
    const normalized = typeof raw === 'string' ? raw.trim() : '';
    if (!normalized) {
        return fallback;
    }
    const parsed = normalized
        .split(',')
        .map((entry) => entry.trim())
        .filter((entry) => entry.length > 0)
        .map((entry) => {
            const [assetRaw, fastRaw, slowRaw] = entry.split(':').map((part) => part.trim());
            if (!assetRaw || !fastRaw || !slowRaw) {
                return null;
            }
            const asset = assetRaw.toUpperCase();
            const fast = fastRaw.toUpperCase();
            const slow = slowRaw.toUpperCase();
            if (!STRATEGY_IDS.includes(fast) || !STRATEGY_IDS.includes(slow) || fast === slow) {
                return null;
            }
            return { asset, fast, slow };
        })
        .filter((pair): pair is { asset: string; fast: string; slow: string } => pair !== null);
    if (parsed.length === 0) {
        return fallback;
    }
    const deduped = new Map<string, { asset: string; fast: string; slow: string }>();
    for (const pair of parsed) {
        deduped.set(`${pair.asset}:${pair.fast}:${pair.slow}`, pair);
    }
    return [...deduped.values()];
}
const CROSS_HORIZON_STRATEGY_PAIRS = parseCrossHorizonStrategyPairs(process.env.CROSS_HORIZON_ROUTER_PAIRS);
const crossHorizonManagedStrategies = Array.from(
    new Set(
        CROSS_HORIZON_STRATEGY_PAIRS
            .flatMap((pair) => [pair.fast, pair.slow])
            .filter((strategyId) => STRATEGY_IDS.includes(strategyId)),
    ),
);
const crossHorizonOverlayCache: Record<string, number> = {};
let crossHorizonRouterState: CrossHorizonRouterState = {
    enabled: CROSS_HORIZON_ROUTER_ENABLED,
    interval_ms: CROSS_HORIZON_ROUTER_INTERVAL_MS,
    overlays: Object.fromEntries(STRATEGY_IDS.map((strategyId) => [strategyId, 1])),
    pairs: {},
    updated_at: 0,
    reason: 'cross-horizon router not initialized',
};
const executionNettingByMarket = new Map<string, ExecutionNettingMarketState>();
const scanFeatureSchemaByStrategy: Record<string, ScanFeatureSchemaState> = {};
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
const setRedisDedupeKey = redisClient.set.bind(redisClient) as (
    key: string,
    value: string,
    options?: { NX?: boolean; PX?: number },
) => Promise<unknown>;
const polymarketPreflight = new PolymarketPreflightService({
    set: setRedisDedupeKey,
});
const settlementService = new PolymarketSettlementService(redisClient);
const strategyTradeRecorder = new StrategyTradeRecorder();
const featureRegistryRecorder = new FeatureRegistryRecorder();
const rejectedSignalRecorder = new RejectedSignalRecorder();

function clearRiskGuardBootResumeTimer(strategyId: string): void {
    const timer = riskGuardBootResumeTimers.get(strategyId);
    if (!timer) {
        return;
    }
    clearTimeout(timer);
    riskGuardBootResumeTimers.delete(strategyId);
}

function registerScheduledTask(taskName: string, intervalMs: number, task: () => Promise<void>): void {
    const stop = scheduleNonOverlappingTask(taskName, intervalMs, async () => {
        if (!bootstrapComplete) {
            return;
        }
        await task();
    });
    scheduledTaskStops.push(stop);
}

function stopAllScheduledTasks(): void {
    while (scheduledTaskStops.length > 0) {
        const stop = scheduledTaskStops.pop();
        try {
            stop?.();
        } catch (error) {
            recordSilentCatch('Scheduler.StopTask', error);
        }
    }
}

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
    if (
        strategy === 'BTC_5M'
        || strategy === 'BTC_15M'
        || strategy === 'ETH_5M'
        || strategy === 'ETH_15M'
        || strategy === 'SOL_5M'
        || strategy === 'SOL_15M'
    ) {
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

function strategyUnderlying(strategyRaw: string): string {
    const strategy = strategyRaw.trim().toUpperCase();
    if (strategy === 'BTC_5M' || strategy === 'BTC_15M') {
        return 'BTC';
    }
    if (strategy === 'ETH_5M' || strategy === 'ETH_15M') {
        return 'ETH';
    }
    if (strategy === 'SOL_5M' || strategy === 'SOL_15M') {
        return 'SOL';
    }
    if (strategy === 'CEX_SNIPER' || strategy === 'OBI_SCALPER') {
        return 'BTC';
    }
    if (
        strategy === 'ATOMIC_ARB'
        || strategy === 'GRAPH_ARB'
        || strategy === 'CONVERGENCE_CARRY'
        || strategy === 'MAKER_MM'
        || strategy === 'AS_MARKET_MAKER'
        || strategy === 'LONGSHOT_BIAS'
        || strategy === 'SYNDICATE'
    ) {
        return 'POLY_EVENT';
    }
    return 'UNKNOWN';
}

function normalizeFeatureToken(raw: string): string {
    const normalized = raw
        .trim()
        .toUpperCase()
        .replace(/[^A-Z0-9_]+/g, '_')
        .replace(/_+/g, '_')
        .replace(/^_+|_+$/g, '');
    return normalized.length > 0 ? normalized : 'UNKNOWN';
}

function buildStrategyContextFeatures(scan: StrategyScanState): Record<string, number> {
    const strategy = normalizeFeatureToken(scan.strategy);
    const family = normalizeFeatureToken(strategyFamily(scan.strategy));
    const underlying = normalizeFeatureToken(strategyUnderlying(scan.strategy));
    const metricFamily = normalizeFeatureToken(scan.metric_family || 'UNKNOWN');
    const directionality = normalizeFeatureToken(scan.directionality || 'UNKNOWN');

    return {
        ctx_bias: 1,
        [`ctx_strategy__${strategy}`]: 1,
        [`ctx_family__${family}`]: 1,
        [`ctx_underlying__${underlying}`]: 1,
        [`ctx_metric_family__${metricFamily}`]: 1,
        [`ctx_directionality__${directionality}`]: 1,
    };
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
            directionality: 'SIGNED',
            comparable_group: 'CEX_SNIPER_MOMENTUM',
        };
    }

    if (strategy === 'OBI_SCALPER') {
        return {
            signal_type: signalType,
            unit: unit === 'RAW' ? 'RATIO' : unit,
            metric_family: 'ORDER_FLOW',
            directionality: 'SIGNED',
            comparable_group: 'OBI_SCALPER_IMBALANCE',
        };
    }

    if (strategy === 'MAKER_MM') {
        return {
            signal_type: signalType,
            unit: unit === 'RAW' ? 'RATIO' : unit,
            metric_family: 'MARKET_MAKING',
            directionality: 'SIGNED',
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

function constantTimeHexEqual(left: string, right: string): boolean {
    const leftBuffer = Buffer.from(left, 'hex');
    const rightBuffer = Buffer.from(right, 'hex');
    if (leftBuffer.length === 0 || rightBuffer.length === 0 || leftBuffer.length !== rightBuffer.length) {
        return false;
    }
    return timingSafeEqual(leftBuffer, rightBuffer);
}

function computeExecutionEventHmac(payload: unknown): string | null {
    if (!EXECUTION_EVENT_HMAC_SECRET) {
        return null;
    }
    try {
        const serialized = JSON.stringify(payload);
        return createHmac('sha256', EXECUTION_EVENT_HMAC_SECRET).update(serialized).digest('hex');
    } catch {
        return null;
    }
}

type ExecutionIngressValidation =
    | {
        ok: true;
        payload: unknown;
        signed: boolean;
    }
    | {
        ok: false;
        reason: string;
        payload: unknown;
    };

function validateExecutionIngressPayload(raw: unknown): ExecutionIngressValidation {
    const envelope = asRecord(raw);
    const hasPayloadField = envelope && Object.prototype.hasOwnProperty.call(envelope, 'payload');
    const payload = hasPayloadField ? envelope?.payload : raw;

    if (!hasPayloadField) {
        if (EXECUTION_EVENT_REQUIRE_SIGNATURE) {
            return {
                ok: false,
                reason: 'missing execution signature envelope',
                payload,
            };
        }
        return { ok: true, payload, signed: false };
    }

    const signature = asString(envelope?._sig)?.toLowerCase() || null;
    const algo = (asString(envelope?._sig_algo) || 'HMAC_SHA256').toUpperCase();
    if (algo !== 'HMAC_SHA256') {
        return {
            ok: false,
            reason: `unsupported execution signature algorithm ${algo}`,
            payload,
        };
    }

    if (!signature) {
        if (EXECUTION_EVENT_REQUIRE_SIGNATURE) {
            return {
                ok: false,
                reason: 'missing execution signature',
                payload,
            };
        }
        return { ok: true, payload, signed: false };
    }

    const expected = computeExecutionEventHmac(payload);
    if (!expected) {
        return {
            ok: false,
            reason: 'execution signature configured but backend secret is unavailable',
            payload,
        };
    }

    if (!constantTimeHexEqual(signature, expected)) {
        return {
            ok: false,
            reason: 'execution signature mismatch',
            payload,
        };
    }

    return {
        ok: true,
        payload,
        signed: true,
    };
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

const SCAN_META_FEATURE_LIMIT = Math.max(
    16,
    Math.min(256, Math.floor(Number(process.env.SCAN_META_FEATURE_LIMIT || '96'))),
);
const SCAN_META_FEATURE_MAX_DEPTH = Math.max(
    1,
    Math.min(6, Math.floor(Number(process.env.SCAN_META_FEATURE_MAX_DEPTH || '3'))),
);
const SCAN_META_FEATURE_DROPPED_KEY_PREVIEW = 8;
const SCAN_FEATURE_SCHEMA_WARMUP_SCANS = Math.max(
    2,
    Math.floor(Number(process.env.SCAN_FEATURE_SCHEMA_WARMUP_SCANS || '12')),
);
const SCAN_FEATURE_SCHEMA_MAX_TRACKED_KEYS = Math.max(
    SCAN_META_FEATURE_LIMIT,
    Math.min(512, Math.floor(Number(process.env.SCAN_FEATURE_SCHEMA_MAX_TRACKED_KEYS || '192'))),
);
const SCAN_FEATURE_SCHEMA_ALERT_COOLDOWN_MS = Math.max(
    10_000,
    Number(process.env.SCAN_FEATURE_SCHEMA_ALERT_COOLDOWN_MS || '60000'),
);
const SCAN_FEATURE_SCHEMA_MISSING_RATIO_ALERT = Math.min(
    0.95,
    Math.max(0.10, Number(process.env.SCAN_FEATURE_SCHEMA_MISSING_RATIO_ALERT || '0.45')),
);
const SCAN_FEATURE_SCHEMA_MISSING_STREAK_ALERT = Math.max(
    2,
    Math.floor(Number(process.env.SCAN_FEATURE_SCHEMA_MISSING_STREAK_ALERT || '4')),
);

type ScanMetaFeatureExtraction = {
    features: Record<string, number>;
    dropped_count: number;
    dropped_keys: string[];
};

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

type ScanMetaCollectionState = {
    count: number;
    dropped: number;
    dropped_keys: string[];
};

function pushDroppedMetaFeatureKey(state: ScanMetaCollectionState, key: string): void {
    if (!key || state.dropped_keys.length >= SCAN_META_FEATURE_DROPPED_KEY_PREVIEW) {
        return;
    }
    if (!state.dropped_keys.includes(key)) {
        state.dropped_keys.push(key);
    }
}

function countNumericMetaCandidates(input: unknown, depth = 0): number {
    if (depth > SCAN_META_FEATURE_MAX_DEPTH) {
        return 0;
    }
    if (Array.isArray(input)) {
        return 0;
    }
    const numeric = asNumber(input);
    if (numeric !== null && Number.isFinite(numeric)) {
        return 1;
    }
    const record = asRecord(input);
    if (!record) {
        return 0;
    }
    let total = 0;
    for (const value of Object.values(record)) {
        total += countNumericMetaCandidates(value, depth + 1);
    }
    return total;
}

function collectNumericMetaFeatures(
    input: unknown,
    prefix: string,
    out: Record<string, number>,
    state: ScanMetaCollectionState,
    depth = 0,
): void {
    if (depth > SCAN_META_FEATURE_MAX_DEPTH) {
        return;
    }

    if (Array.isArray(input)) {
        return;
    }

    const numeric = asNumber(input);
    if (numeric !== null && Number.isFinite(numeric)) {
        const key = normalizeFeatureColumn(prefix, 'meta');
        if (!key) {
            return;
        }
        if (Object.prototype.hasOwnProperty.call(out, key)) {
            return;
        }
        if (state.count >= SCAN_META_FEATURE_LIMIT) {
            state.dropped += 1;
            pushDroppedMetaFeatureKey(state, key);
            return;
        }
        if (key && !Object.prototype.hasOwnProperty.call(out, key)) {
            out[key] = numeric;
            state.count += 1;
        }
        return;
    }

    const record = asRecord(input);
    if (!record) {
        return;
    }

    const entries = Object.entries(record);
    for (let index = 0; index < entries.length; index += 1) {
        const [rawKey, value] = entries[index];
        const key = rawKey.trim();
        if (!key) {
            continue;
        }
        const childPrefix = depth === 0 ? key : `${prefix}_${key}`;
        if (state.count >= SCAN_META_FEATURE_LIMIT) {
            const remainingEntries = entries.slice(index);
            let dropped = 0;
            for (const [remainingKey, remainingValue] of remainingEntries) {
                const normalizedRemainingKey = remainingKey.trim();
                if (!normalizedRemainingKey) {
                    continue;
                }
                const remainingPrefix = depth === 0
                    ? normalizedRemainingKey
                    : `${prefix}_${normalizedRemainingKey}`;
                const normalizedFeatureKey = normalizeFeatureColumn(remainingPrefix, 'meta');
                if (normalizedFeatureKey) {
                    pushDroppedMetaFeatureKey(state, normalizedFeatureKey);
                }
                dropped += countNumericMetaCandidates(remainingValue, depth + 1);
            }
            state.dropped += dropped;
            break;
        }
        collectNumericMetaFeatures(value, childPrefix, out, state, depth + 1);
    }
}

function extractScanMetaFeatures(meta: unknown): ScanMetaFeatureExtraction {
    const out: Record<string, number> = {};
    const state: ScanMetaCollectionState = {
        count: 0,
        dropped: 0,
        dropped_keys: [],
    };
    collectNumericMetaFeatures(meta, 'meta', out, state, 0);
    return {
        features: out,
        dropped_count: state.dropped,
        dropped_keys: state.dropped_keys,
    };
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

type ParsedPreflightOrder = {
    index: number;
    side: 'BUY' | 'SELL';
    price: number;
    size: number;
    sizeUnit: 'USD_NOTIONAL' | 'SHARES';
    notionalUsd: number;
    conditionId: string | null;
};

type ExecutionIntentSnapshot = {
    strategy: string | null;
    family: string;
    marketKey: string | null;
    direction: number;
    notionalUsd: number;
    edgeBps: number | null;
    requiredEdgeBps: number | null;
    frictionBps: number | null;
    retainedBps: number | null;
};

function parseExecutionPreflightOrders(payload: unknown): ParsedPreflightOrder[] {
    const record = asRecord(payload);
    if (!record) {
        return [];
    }
    const details = asRecord(record.details);
    const preflight = details ? asRecord(details.preflight) : null;
    const rawOrders = Array.isArray(preflight?.orders) ? preflight.orders : [];
    const rows: ParsedPreflightOrder[] = [];
    for (let index = 0; index < rawOrders.length; index += 1) {
        const order = asRecord(rawOrders[index]);
        if (!order) {
            continue;
        }
        const price = asNumber(order.price);
        const size = asNumber(order.size);
        if (price === null || size === null || price <= 0 || size <= 0) {
            continue;
        }
        const sideRaw = (asString(order.side) || 'BUY').toUpperCase();
        const side: 'BUY' | 'SELL' = sideRaw === 'SELL' ? 'SELL' : 'BUY';
        const unitRaw = (
            asString(order.size_unit)
            || asString(order.sizeUnit)
            || 'USD_NOTIONAL'
        ).toUpperCase();
        const sizeUnit: 'USD_NOTIONAL' | 'SHARES' = unitRaw === 'SHARES' ? 'SHARES' : 'USD_NOTIONAL';
        const notionalUsd = sizeUnit === 'SHARES' ? size * price : size;
        if (!Number.isFinite(notionalUsd) || notionalUsd <= 0) {
            continue;
        }
        rows.push({
            index,
            side,
            price,
            size,
            sizeUnit,
            notionalUsd,
            conditionId: normalizeMarketKey(order.condition_id || order.conditionId),
        });
    }
    return rows;
}

function executionSignalNotionalUsd(payload: unknown): number {
    const orderRows = parseExecutionPreflightOrders(payload);
    if (orderRows.length > 0) {
        return orderRows.reduce((sum, row) => sum + row.notionalUsd, 0);
    }
    const direct = asNumber(asRecord(payload)?.size);
    return direct && direct > 0 ? direct : 0;
}

function cloneExecutionPayload(payload: unknown): Record<string, unknown> | null {
    try {
        const cloned = JSON.parse(JSON.stringify(payload)) as unknown;
        return asRecord(cloned);
    } catch {
        return null;
    }
}

function scaleExecutionPayloadNotional(
    payload: unknown,
    scale: number,
): { payload: unknown; notionalUsd: number } | null {
    if (!(scale > 0 && scale < 1)) {
        return {
            payload,
            notionalUsd: executionSignalNotionalUsd(payload),
        };
    }

    const cloned = cloneExecutionPayload(payload);
    if (!cloned) {
        return null;
    }
    const details = asRecord(cloned.details);
    const preflight = details ? asRecord(details.preflight) : null;
    if (!preflight || !Array.isArray(preflight.orders)) {
        return null;
    }
    const rawOrders = preflight.orders;

    const scaledOrders: Record<string, unknown>[] = [];
    let totalNotionalUsd = 0;
    for (const rawOrder of rawOrders) {
        const order = asRecord(rawOrder);
        if (!order) {
            continue;
        }
        const price = asNumber(order.price);
        const size = asNumber(order.size);
        if (price === null || size === null || price <= 0 || size <= 0) {
            continue;
        }
        const unitRaw = (
            asString(order.size_unit)
            || asString(order.sizeUnit)
            || 'USD_NOTIONAL'
        ).toUpperCase();
        const sizeUnit: 'USD_NOTIONAL' | 'SHARES' = unitRaw === 'SHARES' ? 'SHARES' : 'USD_NOTIONAL';
        const scaledSize = size * scale;
        if (!Number.isFinite(scaledSize) || scaledSize <= 0) {
            continue;
        }
        const next = { ...order };
        next.size = Number(scaledSize.toFixed(8));
        scaledOrders.push(next);
        const notionalUsd = sizeUnit === 'SHARES' ? scaledSize * price : scaledSize;
        totalNotionalUsd += notionalUsd;
    }

    if (scaledOrders.length === 0 || totalNotionalUsd <= 0) {
        return null;
    }

    preflight.orders = scaledOrders;
    if (asNumber(cloned.size) !== null) {
        const topSize = asNumber(cloned.size) || 0;
        if (topSize > 0) {
            cloned.size = Number((topSize * scale).toFixed(8));
        }
    }

    return {
        payload: cloned,
        notionalUsd: totalNotionalUsd,
    };
}

function inferDirectionalHint(raw: string | null): number {
    if (!raw) {
        return 0;
    }
    const normalized = raw.trim().toUpperCase();
    if (!normalized) {
        return 0;
    }
    if (
        normalized.includes('LONG')
        || normalized.includes('BUY')
        || normalized.includes('YES')
        || normalized.includes('UP')
    ) {
        return 1;
    }
    if (
        normalized.includes('SHORT')
        || normalized.includes('SELL')
        || normalized.includes('NO')
        || normalized.includes('DOWN')
    ) {
        return -1;
    }
    return 0;
}

function inferExecutionDirection(payload: unknown, strategyHint: string | null): number {
    const record = asRecord(payload);
    if (!record) {
        return 0;
    }
    const details = asRecord(record.details);
    const candidateHints: Array<string | null> = [
        asString(details?.direction),
        asString(details?.position_side),
        asString(details?.side),
        asString(record.side),
        asString(record.action),
    ];
    for (const hint of candidateHints) {
        const sign = inferDirectionalHint(hint);
        if (sign !== 0) {
            return sign;
        }
    }

    const marketLabel = (asString(record.market) || '').toLowerCase();
    if (marketLabel.includes(' long ') || marketLabel.startsWith('long ') || marketLabel.includes('long-')) {
        return 1;
    }
    if (marketLabel.includes(' short ') || marketLabel.startsWith('short ') || marketLabel.includes('short-')) {
        return -1;
    }

    if (strategyHint && strategyFamily(strategyHint) === 'FAIR_VALUE') {
        return 1;
    }

    const orderRows = parseExecutionPreflightOrders(payload);
    if (orderRows.length === 2) {
        const conditionIds = new Set(
            orderRows
                .map((row) => row.conditionId)
                .filter((value): value is string => typeof value === 'string' && value.length > 0),
        );
        if (conditionIds.size === 1) {
            return 0;
        }
    }

    return 0;
}

function extractExecutionEdgeTelemetry(payload: unknown): {
    edgeBps: number | null;
    requiredEdgeBps: number | null;
    frictionBps: number | null;
    retainedBps: number | null;
} {
    const record = asRecord(payload);
    const details = record ? asRecord(record.details) : null;

    const edgeRaw = asNumber(details?.net_edge)
        ?? asNumber(details?.edge)
        ?? asNumber(details?.margin);
    const requiredRaw = asNumber(details?.required_edge)
        ?? asNumber(details?.adaptive_entry_edge)
        ?? asNumber(details?.threshold);
    const roundTripCostRate = asNumber(details?.round_trip_cost_rate);
    const spread = Math.max(
        0,
        asNumber(details?.spread) ?? 0,
        asNumber(details?.yes_spread) ?? 0,
        asNumber(details?.no_spread) ?? 0,
    );

    const edgeBps = edgeRaw === null
        ? null
        : ((requiredRaw === null ? edgeRaw : edgeRaw - requiredRaw) * 10_000);
    const requiredEdgeBps = requiredRaw === null ? null : (requiredRaw * 10_000);
    const hasFrictionInputs = roundTripCostRate !== null || spread > 0;
    const frictionBps = hasFrictionInputs
        ? ((roundTripCostRate || 0) * 10_000) + (spread * 5_000)
        : null;
    const retainedBps = edgeBps === null
        ? null
        : edgeBps - (frictionBps || 0);

    return {
        edgeBps,
        requiredEdgeBps,
        frictionBps,
        retainedBps,
    };
}

function buildExecutionIntentSnapshot(
    payload: unknown,
    strategyHint: string | null,
    marketKeyHint: string | null,
): ExecutionIntentSnapshot {
    const strategy = strategyHint || extractExecutionStrategy(payload);
    const family = strategy ? strategyFamily(strategy) : 'GENERIC';
    const marketKey = marketKeyHint || extractExecutionMarketKey(payload);
    const direction = inferExecutionDirection(payload, strategy);
    const notionalUsd = executionSignalNotionalUsd(payload);
    const edgeTelemetry = extractExecutionEdgeTelemetry(payload);
    return {
        strategy,
        family,
        marketKey,
        direction,
        notionalUsd,
        edgeBps: edgeTelemetry.edgeBps,
        requiredEdgeBps: edgeTelemetry.requiredEdgeBps,
        frictionBps: edgeTelemetry.frictionBps,
        retainedBps: edgeTelemetry.retainedBps,
    };
}

function applyNettingDecay(state: ExecutionNettingMarketState, now = Date.now()): void {
    const elapsed = Math.max(0, now - state.updated_at);
    if (elapsed <= 0 || EXECUTION_NETTING_TTL_MS <= 0) {
        return;
    }
    const decay = Math.exp(-elapsed / EXECUTION_NETTING_TTL_MS);
    state.net_directional_notional *= decay;
    state.long_notional *= decay;
    state.short_notional *= decay;
    for (const strategy of Object.keys(state.strategy_directional_notional)) {
        const next = state.strategy_directional_notional[strategy] * decay;
        if (Math.abs(next) < 1e-6) {
            delete state.strategy_directional_notional[strategy];
        } else {
            state.strategy_directional_notional[strategy] = next;
        }
    }
    state.updated_at = now;
}

function pruneExecutionNettingState(now = Date.now()): void {
    if (!EXECUTION_NETTING_ENABLED) {
        executionNettingByMarket.clear();
        return;
    }
    for (const [marketKey, state] of executionNettingByMarket.entries()) {
        if (now - state.updated_at > EXECUTION_NETTING_TTL_MS) {
            executionNettingByMarket.delete(marketKey);
            continue;
        }
        applyNettingDecay(state, now);
        if (Math.abs(state.net_directional_notional) < 1e-6 && Object.keys(state.strategy_directional_notional).length === 0) {
            executionNettingByMarket.delete(marketKey);
        }
    }
}

function evaluateExecutionShortfall(
    payload: unknown,
    intent: ExecutionIntentSnapshot,
): ExecutionPreparationResult {
    if (!EXECUTION_SHORTFALL_OPTIMIZER_ENABLED) {
        return { ok: true, payload, adjusted: false };
    }
    const orderRows = parseExecutionPreflightOrders(payload);
    if (orderRows.length === 0 || intent.notionalUsd <= 0) {
        return { ok: true, payload, adjusted: false };
    }
    if (intent.retainedBps === null) {
        return { ok: true, payload, adjusted: false };
    }

    if (intent.retainedBps >= EXECUTION_SHORTFALL_TARGET_RETAIN_BPS) {
        return { ok: true, payload, adjusted: false };
    }

    const minScale = Math.max(0.01, 1 - EXECUTION_SHORTFALL_MAX_SIZE_REDUCTION_PCT);
    let scale = minScale;
    if (intent.retainedBps > EXECUTION_SHORTFALL_MIN_RETAIN_BPS) {
        const denominator = Math.max(1e-6, EXECUTION_SHORTFALL_TARGET_RETAIN_BPS - EXECUTION_SHORTFALL_MIN_RETAIN_BPS);
        const normalized = (intent.retainedBps - EXECUTION_SHORTFALL_MIN_RETAIN_BPS) / denominator;
        scale = minScale + (Math.max(0, Math.min(1, normalized)) * (1 - minScale));
    }

    if (!(scale > 0 && scale < 0.999)) {
        return { ok: true, payload, adjusted: false };
    }

    const scaled = scaleExecutionPayloadNotional(payload, scale);
    if (!scaled || scaled.notionalUsd < EXECUTION_SHORTFALL_MIN_SIGNAL_NOTIONAL_USD) {
        return {
            ok: false,
            payload,
            adjusted: false,
            stage: 'SHORTFALL',
            reason: `retained edge ${intent.retainedBps.toFixed(2)}bps too low after costs`,
            details: {
                strategy: intent.strategy,
                market_key: intent.marketKey,
                retained_bps: intent.retainedBps,
                edge_bps: intent.edgeBps,
                required_edge_bps: intent.requiredEdgeBps,
                friction_bps: intent.frictionBps,
                notional_usd: intent.notionalUsd,
                min_notional_usd: EXECUTION_SHORTFALL_MIN_SIGNAL_NOTIONAL_USD,
            },
        };
    }

    return {
        ok: true,
        payload: scaled.payload,
        adjusted: true,
        stage: 'SHORTFALL',
        details: {
            strategy: intent.strategy,
            market_key: intent.marketKey,
            retained_bps: intent.retainedBps,
            edge_bps: intent.edgeBps,
            friction_bps: intent.frictionBps,
            scale,
            notional_before: intent.notionalUsd,
            notional_after: scaled.notionalUsd,
        },
    };
}

function evaluateExecutionNetting(
    payload: unknown,
    intent: ExecutionIntentSnapshot,
): ExecutionPreparationResult {
    if (!EXECUTION_NETTING_ENABLED) {
        return { ok: true, payload, adjusted: false };
    }
    if (!intent.marketKey || intent.direction === 0 || intent.notionalUsd <= 0) {
        return { ok: true, payload, adjusted: false };
    }

    pruneExecutionNettingState();
    const state = executionNettingByMarket.get(intent.marketKey);
    if (!state) {
        return { ok: true, payload, adjusted: false };
    }

    const now = Date.now();
    applyNettingDecay(state, now);
    const currentNet = state.net_directional_notional;
    const sameDirection = currentNet === 0 || Math.sign(currentNet) === intent.direction;
    let scale = 1;

    if (!sameDirection) {
        const opposingNotional = Math.abs(currentNet);
        const blockThreshold = intent.notionalUsd * EXECUTION_NETTING_OPPOSING_BLOCK_RATIO;
        if (opposingNotional >= blockThreshold) {
            return {
                ok: false,
                payload,
                adjusted: false,
                stage: 'NETTING',
                reason: `opposing active exposure ${opposingNotional.toFixed(2)} blocks new signal`,
                details: {
                    strategy: intent.strategy,
                    market_key: intent.marketKey,
                    opposing_notional_usd: opposingNotional,
                    candidate_notional_usd: intent.notionalUsd,
                    block_ratio: EXECUTION_NETTING_OPPOSING_BLOCK_RATIO,
                },
            };
        }
        const reliefRatio = Math.min(1, opposingNotional / intent.notionalUsd);
        scale = Math.min(scale, Math.max(0.25, 1 - reliefRatio));
    } else {
        const currentAbs = Math.abs(currentNet);
        const headroom = Math.max(0, EXECUTION_NETTING_MARKET_CAP_USD - currentAbs);
        if (headroom <= 0) {
            return {
                ok: false,
                payload,
                adjusted: false,
                stage: 'NETTING',
                reason: `market directional cap ${EXECUTION_NETTING_MARKET_CAP_USD.toFixed(2)} reached`,
                details: {
                    strategy: intent.strategy,
                    market_key: intent.marketKey,
                    current_net_notional_usd: currentAbs,
                    candidate_notional_usd: intent.notionalUsd,
                    cap_usd: EXECUTION_NETTING_MARKET_CAP_USD,
                },
            };
        }
        if (intent.notionalUsd > headroom) {
            scale = Math.min(scale, headroom / intent.notionalUsd);
        }
    }

    if (!(scale > 0 && scale < 0.999)) {
        return { ok: true, payload, adjusted: false };
    }

    const scaled = scaleExecutionPayloadNotional(payload, scale);
    if (!scaled || scaled.notionalUsd < EXECUTION_SHORTFALL_MIN_SIGNAL_NOTIONAL_USD) {
        return {
            ok: false,
            payload,
            adjusted: false,
            stage: 'NETTING',
            reason: 'scaled netting-adjusted notional too small',
            details: {
                strategy: intent.strategy,
                market_key: intent.marketKey,
                scale,
                candidate_notional_usd: intent.notionalUsd,
                min_notional_usd: EXECUTION_SHORTFALL_MIN_SIGNAL_NOTIONAL_USD,
            },
        };
    }

    return {
        ok: true,
        payload: scaled.payload,
        adjusted: true,
        stage: 'NETTING',
        details: {
            strategy: intent.strategy,
            market_key: intent.marketKey,
            scale,
            notional_before: intent.notionalUsd,
            notional_after: scaled.notionalUsd,
            existing_net_notional_usd: currentNet,
            cap_usd: EXECUTION_NETTING_MARKET_CAP_USD,
        },
    };
}

function prepareExecutionPayloadForDispatch(
    payload: unknown,
    strategyHint: string | null,
    marketKeyHint: string | null,
): ExecutionPreparationResult {
    let workingPayload = payload;
    const prepDetails: Record<string, unknown> = {};
    let adjusted = false;

    const initialIntent = buildExecutionIntentSnapshot(workingPayload, strategyHint, marketKeyHint);
    const shortfall = evaluateExecutionShortfall(workingPayload, initialIntent);
    if (!shortfall.ok) {
        return shortfall;
    }
    if (shortfall.adjusted) {
        adjusted = true;
        workingPayload = shortfall.payload;
        prepDetails.shortfall = shortfall.details || {};
    }

    const intentAfterShortfall = buildExecutionIntentSnapshot(workingPayload, initialIntent.strategy, initialIntent.marketKey);
    const netting = evaluateExecutionNetting(workingPayload, intentAfterShortfall);
    if (!netting.ok) {
        if (Object.prototype.hasOwnProperty.call(prepDetails, 'shortfall')) {
            netting.details = {
                ...(netting.details || {}),
                shortfall: prepDetails.shortfall,
            };
        }
        return netting;
    }
    if (netting.adjusted) {
        adjusted = true;
        workingPayload = netting.payload;
        prepDetails.netting = netting.details || {};
    }

    if (!adjusted) {
        return {
            ok: true,
            payload: workingPayload,
            adjusted: false,
        };
    }

    const finalIntent = buildExecutionIntentSnapshot(workingPayload, initialIntent.strategy, initialIntent.marketKey);
    return {
        ok: true,
        payload: workingPayload,
        adjusted: true,
        details: {
            strategy: finalIntent.strategy,
            market_key: finalIntent.marketKey,
            notional_before: initialIntent.notionalUsd,
            notional_after: finalIntent.notionalUsd,
            shortfall: prepDetails.shortfall || null,
            netting: prepDetails.netting || null,
        },
    };
}

function upsertExecutionNettingStateFromAcceptedPayload(
    payload: unknown,
    strategyHint: string | null,
    marketKeyHint: string | null,
): void {
    if (!EXECUTION_NETTING_ENABLED) {
        return;
    }
    const intent = buildExecutionIntentSnapshot(payload, strategyHint, marketKeyHint);
    if (!intent.marketKey || intent.direction === 0 || intent.notionalUsd <= 0) {
        return;
    }
    const now = Date.now();
    pruneExecutionNettingState(now);
    const strategy = intent.strategy || 'UNKNOWN';
    const existing = executionNettingByMarket.get(intent.marketKey);
    const state: ExecutionNettingMarketState = existing || {
        market_key: intent.marketKey,
        net_directional_notional: 0,
        long_notional: 0,
        short_notional: 0,
        strategy_directional_notional: {},
        updated_at: now,
    };
    applyNettingDecay(state, now);
    if (intent.direction > 0) {
        state.long_notional += intent.notionalUsd;
    } else {
        state.short_notional += intent.notionalUsd;
    }
    state.net_directional_notional += intent.direction * intent.notionalUsd;
    state.strategy_directional_notional[strategy] = (
        state.strategy_directional_notional[strategy]
        || 0
    ) + (intent.direction * intent.notionalUsd);
    state.updated_at = now;
    executionNettingByMarket.set(intent.marketKey, state);
}

function executionNettingSnapshot(limit = 30): Array<Record<string, unknown>> {
    pruneExecutionNettingState();
    return [...executionNettingByMarket.values()]
        .sort((a, b) => b.updated_at - a.updated_at)
        .slice(0, Math.max(1, limit))
        .map((state) => ({
            market_key: state.market_key,
            net_directional_notional: Math.round(state.net_directional_notional * 100) / 100,
            long_notional: Math.round(state.long_notional * 100) / 100,
            short_notional: Math.round(state.short_notional * 100) / 100,
            strategy_directional_notional: Object.fromEntries(
                Object.entries(state.strategy_directional_notional).map(([strategy, notional]) => [
                    strategy,
                    Math.round(notional * 100) / 100,
                ]),
            ),
            updated_at: state.updated_at,
        }));
}

async function persistStrategyExecutionHistoryEntry(
    strategyId: string,
    payload: Record<string, unknown>,
): Promise<void> {
    await persistExecutionHistoryEntry(
        redisClient,
        STRATEGY_EXECUTION_HISTORY_STORE_CONFIG,
        strategyId,
        payload,
    );
}

function parseExecutionHistoryContext(
    payload: Record<string, unknown>,
    strategyHint: string | null = null,
): ExecutionHistoryContext | null {
    const details = asRecord(payload.details);
    const executionId = asString(payload.execution_id)
        || asString(payload.executionId)
        || asString(details?.execution_id)
        || asString(details?.executionId);
    if (!executionId) {
        return null;
    }

    const strategyRaw = asString(payload.strategy)
        || asString(payload.strategy_id)
        || strategyHint;
    const strategy = strategyRaw ? strategyRaw.toUpperCase() : null;
    const marketKey = extractExecutionMarketKey(payload)
        || asString(payload.market_key)
        || asString(payload.marketKey)
        || asString(details?.market_key)
        || asString(details?.marketKey)
        || null;
    const featureRowId = asString(payload.feature_row_id)
        || asString(payload.featureRowId)
        || asString(details?.feature_row_id)
        || asString(details?.featureRowId)
        || null;
    const timestamp = asNumber(payload.timestamp)
        ?? asNumber(details?.timestamp)
        ?? Date.now();

    return {
        execution_id: executionId,
        strategy,
        market_key: marketKey,
        feature_row_id: featureRowId,
        timestamp,
    };
}

function upsertExecutionHistoryContext(context: ExecutionHistoryContext): void {
    const existing = executionHistoryContextById.get(context.execution_id);
    if (existing) {
        existing.strategy = existing.strategy || context.strategy;
        existing.market_key = existing.market_key || context.market_key;
        existing.feature_row_id = existing.feature_row_id || context.feature_row_id;
        existing.timestamp = Math.max(existing.timestamp, context.timestamp);
    } else {
        executionHistoryContextById.set(context.execution_id, { ...context });
    }

    if (executionHistoryContextById.size <= EXECUTION_HISTORY_CONTEXT_MAX) {
        return;
    }

    const retained = [...executionHistoryContextById.values()]
        .sort((a, b) => b.timestamp - a.timestamp)
        .slice(0, EXECUTION_HISTORY_CONTEXT_MAX);
    executionHistoryContextById.clear();
    for (const entry of retained) {
        executionHistoryContextById.set(entry.execution_id, entry);
    }
}

function rebuildExecutionHistoryContextIndex(): void {
    executionHistoryContextById.clear();
    for (const [strategy, rows] of strategyExecutionHistory.entries()) {
        for (const row of rows) {
            const parsed = parseExecutionHistoryContext(row, strategy);
            if (!parsed) {
                continue;
            }
            upsertExecutionHistoryContext(parsed);
        }
    }
}

function resolveExecutionHistoryContext(
    executionId: string,
    strategyHint: string | null = null,
): ExecutionHistoryContext | null {
    const entry = executionHistoryContextById.get(executionId);
    if (!entry) {
        return null;
    }
    if (strategyHint && entry.strategy && entry.strategy !== strategyHint.toUpperCase()) {
        return null;
    }
    return entry;
}

async function loadStrategyExecutionHistoryFromRedis(): Promise<void> {
    const restored = await restoreExecutionHistory(
        redisClient,
        STRATEGY_EXECUTION_HISTORY_STORE_CONFIG,
        (scope, error, context) => {
            recordSilentCatch(scope, error, context);
        },
    );
    strategyExecutionHistory.clear();
    for (const [strategy, rows] of restored.entries()) {
        if (rows.length > 0) {
            strategyExecutionHistory.set(strategy, rows);
        }
    }
    rebuildExecutionHistoryContextIndex();
}

function pushStrategyExecutionHistory(strategyId: string, payload: Record<string, unknown>): void {
    const key = strategyId.trim().toUpperCase();
    if (!key) {
        return;
    }
    const existing = strategyExecutionHistory.get(key) || [];
    const normalized = {
        ...payload,
        strategy: key,
        timestamp: asNumber(payload.timestamp) ?? Date.now(),
    };
    existing.push(normalized);
    if (existing.length > STRATEGY_EXECUTION_HISTORY_LIMIT) {
        existing.splice(0, existing.length - STRATEGY_EXECUTION_HISTORY_LIMIT);
    }
    strategyExecutionHistory.set(key, existing);
    const parsed = parseExecutionHistoryContext(normalized, key);
    if (parsed) {
        upsertExecutionHistoryContext(parsed);
    }
    fireAndForget(
        'StrategyExecutionHistory.Persist',
        persistStrategyExecutionHistoryEntry(key, normalized),
        { strategy: key },
    );
}

async function clearStrategyExecutionHistory(): Promise<void> {
    strategyExecutionHistory.clear();
    executionHistoryContextById.clear();
    await clearExecutionHistoryStore(
        redisClient,
        STRATEGY_EXECUTION_HISTORY_STORE_CONFIG,
        (scope, error, context) => {
            recordSilentCatch(scope, error, context);
        },
    );
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

function featureRowIdForScan(scan: StrategyScanState | null): string | null {
    if (!scan) {
        return null;
    }
    return `${scan.strategy}:${scan.market_key}:${scan.timestamp}`;
}

function upsertExecutionTrace(executionId: string, meta: {
    strategy: string | null;
    market_key: string | null;
    scan_snapshot: StrategyScanState | null;
    feature_row_id: string | null;
}): ExecutionTrace {
    const now = Date.now();
    const existing = executionTraces.get(executionId);
    if (existing) {
        existing.strategy = existing.strategy || meta.strategy;
        existing.market_key = existing.market_key || meta.market_key;
        existing.scan_snapshot = existing.scan_snapshot || meta.scan_snapshot;
        existing.feature_row_id = existing.feature_row_id || meta.feature_row_id;
        existing.updated_at = now;
        return existing;
    }

    const created: ExecutionTrace = {
        execution_id: executionId,
        strategy: meta.strategy,
        market_key: meta.market_key,
        feature_row_id: meta.feature_row_id,
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
    const scanSnapshot = snapshotScanForExecution(meta.strategy, meta.market_key);
    const trace = upsertExecutionTrace(executionId, {
        strategy: meta.strategy,
        market_key: meta.market_key,
        scan_snapshot: scanSnapshot,
        feature_row_id: featureRowIdForScan(scanSnapshot),
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

function scanDirectionSign(scan: StrategyScanState): number {
    if (scan.directionality !== 'SIGNED') {
        return 0;
    }
    const sign = Math.sign(scan.score);
    if (!Number.isFinite(sign) || sign === 0) {
        return 0;
    }
    return sign > 0 ? 1 : -1;
}

function collectPeerSignals(strategy: string, marketKey: string | null, now: number, candidateDirection: number): {
    count: number;
    peers: string[];
    consensus: number;
    opposingCount: number;
} {
    if (!marketKey) {
        return { count: 0, peers: [], consensus: 0, opposingCount: 0 };
    }

    const peers: string[] = [];
    let opposingCount = 0;
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

        const peerDirection = scanDirectionSign(scan);
        if (candidateDirection !== 0) {
            if (peerDirection === 0) {
                continue;
            }
            if (peerDirection !== candidateDirection) {
                opposingCount += 1;
                consensus -= Math.max(0, computeNormalizedMargin(scan));
                continue;
            }
        }

        peers.push(scan.strategy);
        consensus += Math.max(0, computeNormalizedMargin(scan));
    }

    return { count: peers.length, peers: Array.from(new Set(peers)), consensus, opposingCount };
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

    const candidateDirection = scanDirectionSign(scan);
    const peerSignals = collectPeerSignals(strategy, marketKey, now, candidateDirection);
    const needsPeerConfirmation = INTELLIGENCE_GATE_REQUIRE_PEER_CONFIRMATION
        && INTELLIGENCE_GATE_CONFIRMATION_STRATEGIES.has(strategy);
    if (needsPeerConfirmation && normalizedMargin < INTELLIGENCE_GATE_STRONG_MARGIN) {
        if (candidateDirection === 0) {
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
                reason: `Directional peer confirmation unavailable for ${strategy}: scan is non-directional`,
            };
        }
        if (peerSignals.count === 0) {
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
                reason: `No corroborating directional peer signal for ${strategy} on ${scan.market_key} (margin ${normalizedMargin.toFixed(3)} < strong ${INTELLIGENCE_GATE_STRONG_MARGIN.toFixed(3)})`,
            };
        }
        if (peerSignals.consensus <= 0 || peerSignals.opposingCount >= peerSignals.count) {
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
                reason: `Peer consensus opposed ${strategy} direction on ${scan.market_key} (consensus ${peerSignals.consensus.toFixed(3)})`,
            };
        }
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
        } catch (error) {
            recordSilentCatch('RiskConfig.Parse', error, { key: 'system:risk_config' });
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

interface ExecutionHaltState {
    halted: boolean;
    reason: string;
    updated_at: number;
    source: string;
}

const EXECUTION_HALT_KEY = 'system:execution_halt';
const EXECUTION_HALT_REASON_MAX = 240;
let executionHaltState: ExecutionHaltState = {
    halted: false,
    reason: '',
    updated_at: 0,
    source: 'boot',
};

function sanitizeExecutionHaltReason(reason: unknown): string {
    const text = asString(reason);
    if (!text) {
        return '';
    }
    return text.slice(0, EXECUTION_HALT_REASON_MAX);
}

function defaultExecutionHaltState(): ExecutionHaltState {
    return {
        halted: false,
        reason: '',
        updated_at: Date.now(),
        source: 'boot',
    };
}

function normalizeExecutionHaltState(input: unknown): ExecutionHaltState {
    const record = asRecord(input);
    const halted = record?.halted === true;
    const reason = sanitizeExecutionHaltReason(record?.reason);
    const updatedAt = asNumber(record?.updated_at) ?? Date.now();
    const source = asString(record?.source) || 'unknown';
    return {
        halted,
        reason,
        updated_at: updatedAt,
        source,
    };
}

async function loadExecutionHaltState(): Promise<ExecutionHaltState> {
    const raw = await redisClient.get(EXECUTION_HALT_KEY);
    if (!raw) {
        const fallback = defaultExecutionHaltState();
        await redisClient.set(EXECUTION_HALT_KEY, JSON.stringify(fallback));
        executionHaltState = fallback;
        return fallback;
    }
    try {
        executionHaltState = normalizeExecutionHaltState(JSON.parse(raw));
    } catch (error) {
        recordSilentCatch('ExecutionHalt.Load', error, { key: EXECUTION_HALT_KEY });
        executionHaltState = defaultExecutionHaltState();
        await redisClient.set(EXECUTION_HALT_KEY, JSON.stringify(executionHaltState));
    }
    return executionHaltState;
}

async function setExecutionHaltState(
    halted: boolean,
    reason: string,
    source: string,
): Promise<ExecutionHaltState> {
    executionHaltState = {
        halted,
        reason: sanitizeExecutionHaltReason(reason),
        updated_at: Date.now(),
        source: asString(source) || 'unknown',
    };
    await redisClient.set(EXECUTION_HALT_KEY, JSON.stringify(executionHaltState));
    await redisClient.publish('system:execution_halt', JSON.stringify(executionHaltState));
    io.emit('execution_halt_update', executionHaltState);
    touchRuntimeModule(
        'TRADING_MODE_GUARD',
        executionHaltState.halted ? 'DEGRADED' : 'ONLINE',
        executionHaltState.halted
            ? `execution halt active: ${executionHaltState.reason || 'unspecified'}`
            : 'execution halt cleared',
    );
    return executionHaltState;
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
    const scannerHeartbeat = scannerHeartbeatSnapshot(now);
    if (!scannerHeartbeat.alive) {
        const scannerDetails = [
            scannerHeartbeat.missing_ids.length > 0
                ? `missing=${scannerHeartbeat.missing_ids.slice(0, 6).join(',')}`
                : null,
            scannerHeartbeat.stale_ids.length > 0
                ? `stale=${scannerHeartbeat.stale_ids.slice(0, 6).join(',')}`
                : null,
            scannerHeartbeat.monitored_ids.length === 0 ? 'no_monitored_ids' : null,
        ].filter((part): part is string => Boolean(part)).join(' | ');
        failures.push(`Scanner heartbeat unhealthy (${scannerDetails || 'unknown'})`);
    }

    try {
        await redisClient.ping();
    } catch (error) {
        recordSilentCatch('LiveReadiness.RedisPing', error, { op: 'PING' });
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

    if (!mlPipelineStatus.model_eligible) {
        failures.push(`ML model not eligible (${mlPipelineStatus.model_reason || 'no training report'})`);
    }

    if (!signalModelArtifact || !modelInferenceState.model_loaded) {
        failures.push('ML model artifact is not loaded');
    }

    const latestInferenceTs = Object.values(modelInferenceState.latest)
        .reduce((latest, entry) => Math.max(latest, entry.timestamp), 0);
    if (latestInferenceTs <= 0) {
        failures.push('ML inference stream has no rows');
    } else {
        const maxInferenceAgeMs = Math.max(15_000, MODEL_PROBABILITY_GATE_MAX_STALENESS_MS * 3);
        const inferenceAgeMs = now - latestInferenceTs;
        if (inferenceAgeMs > maxInferenceAgeMs) {
            failures.push(`ML inference stale (${inferenceAgeMs}ms > ${maxInferenceAgeMs}ms)`);
        }
    }

    const settlementReadiness = settlementService.getReadinessSnapshot();
    for (const issue of settlementReadiness.failures) {
        failures.push(`Settlement: ${issue}`);
    }

    if (ledgerHealthState.status === 'CRITICAL') {
        failures.push(`Ledger health CRITICAL (${ledgerHealthState.issues[0] || 'unknown'})`);
    }

    if (LEDGER_PNL_PARITY_ENABLED) {
        if (pnlParityState.checked_at <= 0) {
            failures.push('PnL parity watchdog has not completed an initial check');
        } else if (now - pnlParityState.checked_at > (LEDGER_PNL_PARITY_INTERVAL_MS * 3)) {
            failures.push(`PnL parity watchdog stale (${now - pnlParityState.checked_at}ms old)`);
        } else if (pnlParityState.status === 'CRITICAL') {
            failures.push(
                `PnL parity critical (delta ${pnlParityState.delta_abs_usd.toFixed(2)} > ${pnlParityState.critical_abs_usd.toFixed(2)})`,
            );
        }
    }

    return { ready: failures.length === 0, failures };
}

async function enforceBootTradingModeSafety(): Promise<void> {
    const mode = await getTradingMode();
    if (mode !== 'LIVE') {
        touchRuntimeModule('TRADING_MODE_GUARD', 'ONLINE', `boot mode ${mode}`);
        return;
    }

    const readiness = await isLiveReady();
    if (readiness.ready) {
        touchRuntimeModule('TRADING_MODE_GUARD', 'ONLINE', 'boot mode LIVE ready');
        return;
    }

    const summary = readiness.failures.slice(0, 3).join(' | ') || 'unknown readiness failure';
    await setTradingMode('PAPER');
    touchRuntimeModule('TRADING_MODE_GUARD', 'DEGRADED', `boot fail-safe forced PAPER: ${summary}`);
    io.emit('runtime_alert', {
        severity: 'CRITICAL',
        scope: 'TRADING_MODE_GUARD',
        timestamp: Date.now(),
        message: 'Forced PAPER mode at boot because LIVE readiness checks failed',
        failures: readiness.failures,
    });
    recordSilentCatch(
        'LiveReadiness.BootFailSafe',
        new Error('Forced PAPER mode at boot because LIVE readiness checks failed'),
        {
            failures: summary,
        },
    );
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
    return ledgerHealthModule.getSimulationLedgerSnapshot();
}

async function reconcileSimulationLedgerWithStrategyMetrics(): Promise<void> {
    await ledgerHealthModule.reconcileSimulationLedgerWithStrategyMetrics(strategyMetrics);
}

async function clearReservedMaps(): Promise<void> {
    await ledgerHealthModule.clearReservedMaps();
}

async function refreshLedgerHealth(): Promise<void> {
    await ledgerHealthModule.refreshLedgerHealth();
    ledgerHealthState = ledgerHealthModule.getLedgerHealthState();
}

function buildPnlParityState(
    ledger: SimulationLedgerSnapshot,
    summary: StrategyTradeRecorderSummary,
    status: PnlParityStatus,
    issues: string[],
): PnlParityState {
    const deltaSigned = ledger.realized_pnl - summary.paper_pnl_sum;
    const deltaAbs = Math.abs(deltaSigned);
    return {
        enabled: LEDGER_PNL_PARITY_ENABLED,
        status,
        checked_at: Date.now(),
        ledger_realized_pnl: Math.round(ledger.realized_pnl * 100) / 100,
        trade_log_paper_pnl: Math.round(summary.paper_pnl_sum * 100) / 100,
        delta_abs_usd: Math.round(deltaAbs * 100) / 100,
        delta_signed_usd: Math.round(deltaSigned * 100) / 100,
        row_count: summary.row_count,
        paper_row_count: summary.paper_row_count,
        min_paper_rows: LEDGER_PNL_PARITY_MIN_PAPER_ROWS,
        warn_abs_usd: LEDGER_PNL_PARITY_WARN_ABS_USD,
        critical_abs_usd: LEDGER_PNL_PARITY_CRITICAL_ABS_USD,
        issues,
    };
}

async function refreshPnlParityWatchdog(): Promise<void> {
    if (!LEDGER_PNL_PARITY_ENABLED) {
        pnlParityState = {
            enabled: false,
            status: 'HEALTHY',
            checked_at: Date.now(),
            ledger_realized_pnl: 0,
            trade_log_paper_pnl: 0,
            delta_abs_usd: 0,
            delta_signed_usd: 0,
            row_count: 0,
            paper_row_count: 0,
            min_paper_rows: LEDGER_PNL_PARITY_MIN_PAPER_ROWS,
            warn_abs_usd: LEDGER_PNL_PARITY_WARN_ABS_USD,
            critical_abs_usd: LEDGER_PNL_PARITY_CRITICAL_ABS_USD,
            issues: ['disabled via LEDGER_PNL_PARITY_ENABLED=false'],
        };
        io.emit('pnl_parity_update', pnlParityState);
        return;
    }

    try {
        const [ledger, summary] = await Promise.all([
            getSimulationLedgerSnapshot(),
            strategyTradeRecorder.getSummary(),
        ]);
        const deltaAbs = Math.abs(ledger.realized_pnl - summary.paper_pnl_sum);
        let status: PnlParityStatus = 'HEALTHY';
        const issues: string[] = [];

        if (summary.paper_row_count < LEDGER_PNL_PARITY_MIN_PAPER_ROWS) {
            issues.push(
                `warmup: ${summary.paper_row_count}/${LEDGER_PNL_PARITY_MIN_PAPER_ROWS} paper rows`,
            );
        } else if (deltaAbs > LEDGER_PNL_PARITY_CRITICAL_ABS_USD) {
            status = 'CRITICAL';
            issues.push(
                `realized pnl parity delta ${deltaAbs.toFixed(2)} exceeds critical ${LEDGER_PNL_PARITY_CRITICAL_ABS_USD.toFixed(2)}`,
            );
        } else if (deltaAbs > LEDGER_PNL_PARITY_WARN_ABS_USD) {
            status = 'WARN';
            issues.push(
                `realized pnl parity delta ${deltaAbs.toFixed(2)} exceeds warn ${LEDGER_PNL_PARITY_WARN_ABS_USD.toFixed(2)}`,
            );
        }

        pnlParityState = buildPnlParityState(ledger, summary, status, issues);
        io.emit('pnl_parity_update', pnlParityState);
        touchRuntimeModule(
            'PNL_LEDGER',
            status === 'CRITICAL' ? 'DEGRADED' : 'ONLINE',
            status === 'HEALTHY'
                ? `pnl parity delta ${deltaAbs.toFixed(2)}`
                : `pnl parity ${status.toLowerCase()}: ${issues[0] || 'drift detected'}`,
        );
    } catch (error) {
        const detail = `pnl parity watchdog failed: ${String(error)}`;
        recordSilentCatch('PnLParity.Refresh', error, { detail });
        pnlParityState = {
            enabled: true,
            status: 'CRITICAL',
            checked_at: Date.now(),
            ledger_realized_pnl: pnlParityState.ledger_realized_pnl,
            trade_log_paper_pnl: pnlParityState.trade_log_paper_pnl,
            delta_abs_usd: pnlParityState.delta_abs_usd,
            delta_signed_usd: pnlParityState.delta_signed_usd,
            row_count: pnlParityState.row_count,
            paper_row_count: pnlParityState.paper_row_count,
            min_paper_rows: LEDGER_PNL_PARITY_MIN_PAPER_ROWS,
            warn_abs_usd: LEDGER_PNL_PARITY_WARN_ABS_USD,
            critical_abs_usd: LEDGER_PNL_PARITY_CRITICAL_ABS_USD,
            issues: [detail],
        };
        io.emit('pnl_parity_update', pnlParityState);
        touchRuntimeModule('PNL_LEDGER', 'DEGRADED', detail);
    }
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
            base_multiplier: DEFAULT_DISABLED_STRATEGIES.has(id) ? 0 : 1,
            meta_overlay: 1,
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

function clearScanFeatureSchemaState(): void {
    for (const strategy of Object.keys(scanFeatureSchemaByStrategy)) {
        delete scanFeatureSchemaByStrategy[strategy];
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
            base_multiplier: DEFAULT_DISABLED_STRATEGIES.has(strategyId) ? 0 : 1,
            meta_overlay: 1,
            multiplier: DEFAULT_DISABLED_STRATEGIES.has(strategyId) ? 0 : 1,
            updated_at: 0,
        };
    }
    const entry = strategyPerformance[strategyId];
    if (!Number.isFinite(entry.base_multiplier)) {
        entry.base_multiplier = Number.isFinite(entry.multiplier) ? entry.multiplier : 1;
    }
    if (!Number.isFinite(entry.meta_overlay) || entry.meta_overlay <= 0) {
        entry.meta_overlay = 1;
    }
    return entry;
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

function ensureScanFeatureSchemaEntry(strategyId: string): ScanFeatureSchemaState {
    const key = strategyId.trim().toUpperCase() || 'UNKNOWN';
    if (!scanFeatureSchemaByStrategy[key]) {
        scanFeatureSchemaByStrategy[key] = {
            strategy: key,
            scans: 0,
            expected_keys: [],
            missing_streak: 0,
            last_new_keys: [],
            last_missing_keys: [],
            overflow_events: 0,
            overflow_dropped_features: 0,
            last_overflow_at: 0,
            last_alert_at: 0,
            updated_at: 0,
        };
    }
    return scanFeatureSchemaByStrategy[key];
}

function scanFeatureSchemaEntryPayload(strategyId: string): Record<string, unknown> {
    const state = ensureScanFeatureSchemaEntry(strategyId);
    const expectedKeyCount = state.expected_keys.length;
    const missingRatio = expectedKeyCount > 0
        ? state.last_missing_keys.length / expectedKeyCount
        : 0;
    return {
        strategy: state.strategy,
        scans: state.scans,
        expected_key_count: expectedKeyCount,
        expected_keys_preview: state.expected_keys.slice(0, 24),
        missing_streak: state.missing_streak,
        last_new_keys: state.last_new_keys.slice(0, 12),
        last_missing_keys: state.last_missing_keys.slice(0, 12),
        missing_ratio: missingRatio,
        overflow_events: state.overflow_events,
        overflow_dropped_features: state.overflow_dropped_features,
        last_overflow_at: state.last_overflow_at,
        updated_at: state.updated_at,
    };
}

function scanFeatureSchemaPayload(): Record<string, Record<string, unknown>> {
    const keys = Array.from(new Set([
        ...STRATEGY_IDS,
        ...Object.keys(scanFeatureSchemaByStrategy),
    ])).sort();
    return Object.fromEntries(
        keys.map((strategyId) => [strategyId, scanFeatureSchemaEntryPayload(strategyId)]),
    );
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

function recordScanSchemaDrift(strategy: string, marketKey: string, reason: string, issues: string[]): DataIntegrityAlert | null {
    const normalizedStrategy = strategy.trim().toUpperCase() || 'UNKNOWN';
    const normalizedMarket = marketKey.trim() || `${normalizedStrategy.toLowerCase()}:unknown`;
    const now = Date.now();
    const strategyState = ensureStrategyDataIntegrityEntry(normalizedStrategy);

    strategyState.total_scans += 1;
    strategyState.hold_scans += 1;
    strategyState.other += 1;
    strategyState.consecutive_holds += 1;
    strategyState.last_category = 'other';
    strategyState.last_reason = reason;
    strategyState.updated_at = now;

    dataIntegrityState.totals.total_scans += 1;
    dataIntegrityState.totals.hold_scans += 1;
    dataIntegrityState.totals.other += 1;
    dataIntegrityState.updated_at = now;

    if (now - strategyState.last_alert_at < DATA_INTEGRITY_ALERT_COOLDOWN_MS) {
        return null;
    }

    strategyState.last_alert_at = now;
    const alert: DataIntegrityAlert = {
        id: `${normalizedStrategy}:${normalizedMarket}:schema:${now}`,
        strategy: normalizedStrategy,
        market_key: normalizedMarket,
        category: 'other',
        severity: 'CRITICAL',
        reason: `${reason}${issues.length > 1 ? ` | ${issues.slice(1, 3).join(' | ')}` : ''}`,
        consecutive_holds: strategyState.consecutive_holds,
        timestamp: now,
    };
    dataIntegrityState.recent_alerts = [
        alert,
        ...dataIntegrityState.recent_alerts,
    ].slice(0, DATA_INTEGRITY_ALERT_RING_LIMIT);
    return alert;
}

function updateScanFeatureSchemaGovernance(
    scan: StrategyScanState,
    extraction: ScanMetaFeatureExtraction,
): void {
    const strategy = scan.strategy.trim().toUpperCase() || 'UNKNOWN';
    const state = ensureScanFeatureSchemaEntry(strategy);
    const now = Date.now();
    state.scans += 1;
    state.updated_at = now;

    if (extraction.dropped_count > 0) {
        state.overflow_events += 1;
        state.overflow_dropped_features += extraction.dropped_count;
        state.last_overflow_at = now;
    }

    const featureKeys = Object.keys(buildScanFeatureMap(scan))
        .filter((key) => key.trim().length > 0)
        .sort();
    if (state.expected_keys.length === 0) {
        state.expected_keys = featureKeys.slice(0, SCAN_FEATURE_SCHEMA_MAX_TRACKED_KEYS);
        if (extraction.dropped_count > 0) {
            io.emit('scan_feature_schema_update', scanFeatureSchemaEntryPayload(strategy));
        }
        return;
    }

    const expectedSet = new Set(state.expected_keys);
    const currentSet = new Set(featureKeys);
    const newKeys = featureKeys.filter((key) => !expectedSet.has(key));
    const missingKeys = state.expected_keys.filter((key) => !currentSet.has(key));

    if (newKeys.length > 0 && state.expected_keys.length < SCAN_FEATURE_SCHEMA_MAX_TRACKED_KEYS) {
        for (const key of newKeys) {
            if (state.expected_keys.includes(key)) {
                continue;
            }
            state.expected_keys.push(key);
            if (state.expected_keys.length >= SCAN_FEATURE_SCHEMA_MAX_TRACKED_KEYS) {
                break;
            }
        }
    }

    state.last_new_keys = newKeys.slice(0, 12);
    state.last_missing_keys = missingKeys.slice(0, 12);
    if (newKeys.length > 0) {
        state.missing_streak = 0;
    } else if (missingKeys.length > 0) {
        state.missing_streak += 1;
    } else {
        state.missing_streak = 0;
    }

    const expectedKeyCount = Math.max(1, state.expected_keys.length);
    const missingRatio = missingKeys.length / expectedKeyCount;
    const warmedUp = state.scans >= SCAN_FEATURE_SCHEMA_WARMUP_SCANS;
    const cooldownReady = (now - state.last_alert_at) >= SCAN_FEATURE_SCHEMA_ALERT_COOLDOWN_MS;
    let severity: 'WARN' | 'CRITICAL' | null = null;
    let reason = '';

    if (cooldownReady) {
        if (extraction.dropped_count > 0) {
            severity = 'WARN';
            reason = `meta feature overflow dropped ${extraction.dropped_count} column(s) (limit=${SCAN_META_FEATURE_LIMIT})`;
        } else if (warmedUp && newKeys.length > 0) {
            severity = 'WARN';
            reason = `feature schema expanded by ${newKeys.length} key(s)`;
        } else if (
            warmedUp
            && missingKeys.length > 0
            && missingRatio >= SCAN_FEATURE_SCHEMA_MISSING_RATIO_ALERT
            && state.missing_streak >= SCAN_FEATURE_SCHEMA_MISSING_STREAK_ALERT
        ) {
            severity = 'CRITICAL';
            reason = `feature schema missing ${missingKeys.length}/${expectedKeyCount} expected key(s) for ${state.missing_streak} scan(s)`;
        }
    }

    if (severity) {
        state.last_alert_at = now;
        const alertPayload = {
            severity,
            strategy,
            market_key: scan.market_key,
            reason,
            dropped_meta_features: extraction.dropped_count,
            dropped_meta_keys: extraction.dropped_keys.slice(0, SCAN_META_FEATURE_DROPPED_KEY_PREVIEW),
            new_keys: newKeys.slice(0, 12),
            missing_keys: missingKeys.slice(0, 12),
            missing_ratio: missingRatio,
            expected_key_count: expectedKeyCount,
            timestamp: now,
        };
        io.emit('scan_ingest_alert', alertPayload);
        io.emit('scan_feature_schema_alert', alertPayload);
        touchRuntimeModule(
            'SCAN_INGEST',
            severity === 'CRITICAL' ? 'DEGRADED' : 'ONLINE',
            `${strategy} feature schema ${severity.toLowerCase()}: ${reason}`,
        );
    }

    if (extraction.dropped_count > 0 || newKeys.length > 0 || missingKeys.length > 0) {
        io.emit('scan_feature_schema_update', scanFeatureSchemaEntryPayload(strategy));
    }
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
    const historyContext = executionId
        ? resolveExecutionHistoryContext(executionId, strategy)
        : null;
    const executionTrace = executionId ? executionTraces.get(executionId) || null : null;
    const marketKey = extractExecutionMarketKey(record)
        || executionTrace?.market_key
        || historyContext?.market_key
        || null;
    const recordFeatureRowId = asString(record.feature_row_id)
        || asString(record.featureRowId)
        || asString(details?.feature_row_id)
        || asString(details?.featureRowId)
        || null;
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

    const reason = inferTradeClosureReason(asString(record.reason), details) || '';
    const labelEligible = classifyClosureReason(reason).label_eligible;

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
        feature_row_id: recordFeatureRowId
            || executionTrace?.feature_row_id
            || historyContext?.feature_row_id
            || undefined,
    };
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

function ensureEntryFreshnessSloStrategy(strategy: string): EntryFreshnessSloStrategy {
    const key = strategy.trim().toUpperCase() || 'UNKNOWN';
    const existing = entryFreshnessSloState.strategies[key];
    if (existing) {
        return existing;
    }
    const created: EntryFreshnessSloStrategy = {
        strategy: key,
        total: 0,
        missing_telemetry: 0,
        stale_source: 0,
        stale_gate: 0,
        other: 0,
        last_violation_at: 0,
        last_reason: '',
    };
    entryFreshnessSloState.strategies[key] = created;
    return created;
}

function clearEntryFreshnessSloState(): void {
    entryFreshnessSloState.totals = {
        total: 0,
        missing_telemetry: 0,
        stale_source: 0,
        stale_gate: 0,
        other: 0,
    };
    entryFreshnessSloState.strategies = {};
    entryFreshnessSloState.recent_alerts = [];
    entryFreshnessSloState.updated_at = Date.now();
}

function entryFreshnessSloPayload(): EntryFreshnessSloSnapshot {
    return {
        totals: { ...entryFreshnessSloState.totals },
        strategies: Object.fromEntries(
            Object.entries(entryFreshnessSloState.strategies).map(([strategy, value]) => [strategy, { ...value }]),
        ),
        recent_alerts: entryFreshnessSloState.recent_alerts.slice(),
        updated_at: entryFreshnessSloState.updated_at,
    };
}

function recordEntryFreshnessViolation(record: RejectedSignalRecord): void {
    const strategy = normalizedStrategyFilter(record.strategy) || 'UNKNOWN';
    const category = classifyEntryFreshnessViolation(record.reason);
    const entry = ensureEntryFreshnessSloStrategy(strategy);
    const now = Date.now();
    entry.total += 1;
    entry.last_violation_at = now;
    entry.last_reason = record.reason;
    entryFreshnessSloState.totals.total += 1;

    if (category === 'MISSING_TELEMETRY') {
        entry.missing_telemetry += 1;
        entryFreshnessSloState.totals.missing_telemetry += 1;
    } else if (category === 'STALE_SOURCE') {
        entry.stale_source += 1;
        entryFreshnessSloState.totals.stale_source += 1;
    } else if (category === 'STALE_GATE') {
        entry.stale_gate += 1;
        entryFreshnessSloState.totals.stale_gate += 1;
    } else {
        entry.other += 1;
        entryFreshnessSloState.totals.other += 1;
    }

    entryFreshnessSloState.recent_alerts.push({
        execution_id: record.execution_id,
        strategy,
        market_key: record.market_key,
        category,
        reason: record.reason,
        timestamp: now,
    });
    if (entryFreshnessSloState.recent_alerts.length > ENTRY_FRESHNESS_ALERT_RING_LIMIT) {
        entryFreshnessSloState.recent_alerts.splice(
            0,
            entryFreshnessSloState.recent_alerts.length - ENTRY_FRESHNESS_ALERT_RING_LIMIT,
        );
    }
    entryFreshnessSloState.updated_at = now;
    io.emit('entry_freshness_alert', {
        execution_id: record.execution_id,
        strategy,
        market_key: record.market_key,
        category,
        reason: record.reason,
        timestamp: now,
    });
    io.emit('entry_freshness_slo_update', entryFreshnessSloPayload());
}

function recordRejectedSignal(record: RejectedSignalRecord): void {
    rejectedSignalHistory.push(record);
    if (rejectedSignalHistory.length > REJECTED_SIGNAL_RETENTION) {
        rejectedSignalHistory.splice(0, rejectedSignalHistory.length - REJECTED_SIGNAL_RETENTION);
    }
    const existing = rejectedSignalsByExecutionId.get(record.execution_id);
    if (!existing || record.timestamp >= existing.timestamp) {
        rejectedSignalsByExecutionId.set(record.execution_id, record);
    }
    if (rejectedSignalsByExecutionId.size > (REJECTED_SIGNAL_RETENTION * 2)) {
        rejectedSignalsByExecutionId.clear();
        for (const entry of rejectedSignalHistory) {
            const current = rejectedSignalsByExecutionId.get(entry.execution_id);
            if (!current || entry.timestamp >= current.timestamp) {
                rejectedSignalsByExecutionId.set(entry.execution_id, entry);
            }
        }
    }
    if (
        record.stage === 'INTELLIGENCE_GATE'
        && typeof record.reason === 'string'
        && record.reason.includes('[ENTRY_INVARIANT]')
    ) {
        recordEntryFreshnessViolation(record);
    }
    rejectedSignalRecorder.record(record);
    io.emit('rejected_signal', record);
}

function normalizedStrategyFilter(input: unknown): string | null {
    const parsed = asString(input);
    return parsed ? parsed.toUpperCase() : null;
}

function rejectedSignalSnapshot(limit = 200, strategy?: string | null): RejectedSignalRecord[] {
    const capped = Math.max(1, Math.min(REJECTED_SIGNAL_RETENTION, Math.floor(limit)));
    const normalized = normalizedStrategyFilter(strategy);
    if (!normalized) {
        return rejectedSignalHistory.slice(-capped).reverse();
    }
    const filtered = rejectedSignalHistory.filter((entry) => normalizedStrategyFilter(entry.strategy) === normalized);
    return filtered.slice(-capped).reverse();
}

async function bootstrapRejectedSignalHistory(): Promise<void> {
    try {
        const persisted = await rejectedSignalRecorder.readRecent(REJECTED_SIGNAL_RETENTION);
        rejectedSignalHistory.splice(0, rejectedSignalHistory.length, ...persisted);
        rejectedSignalsByExecutionId.clear();
        for (const record of rejectedSignalHistory) {
            const existing = rejectedSignalsByExecutionId.get(record.execution_id);
            if (!existing || record.timestamp >= existing.timestamp) {
                rejectedSignalsByExecutionId.set(record.execution_id, record);
            }
        }
    } catch (error) {
        recordSilentCatch('RejectedSignal.Bootstrap', error, { path: rejectedSignalRecorder.getPath() });
    }
}

function unquoteCsvCell(value: string): string {
    const trimmed = value.trim();
    if (trimmed.startsWith('"') && trimmed.endsWith('"')) {
        return trimmed.slice(1, -1).replace(/""/g, '"');
    }
    return trimmed;
}

function parseCsvLineLoose(line: string): string[] {
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
}

async function backfillFeatureLabelsFromTradeRecorder(
    maxRows = 3000,
    emitSummary = true,
): Promise<{ processed: number; labeled: number; skipped: number; rows_scanned: number }> {
    const safeRows = Math.max(200, Math.min(20_000, Math.floor(maxRows)));
    const filePath = strategyTradeRecorder.getPath();
    const content = await fs.readFile(filePath, 'utf8').catch((error) => {
        const code = (error as NodeJS.ErrnoException)?.code || '';
        if (code !== 'ENOENT') {
            recordSilentCatch('FeatureRegistry.LabelBackfillRead', error, { path: filePath });
        }
        return '';
    });
    const lines = content.split('\n').map((line) => line.trim()).filter((line) => line.length > 0);
    if (lines.length <= 1) {
        return {
            processed: 0,
            labeled: 0,
            skipped: 0,
            rows_scanned: 0,
        };
    }

    const header = parseCsvLineLoose(lines[0]).map((cell) => unquoteCsvCell(cell).toLowerCase());
    const idx = (name: string): number => header.indexOf(name.toLowerCase());

    const timestampIdx = idx('timestamp');
    const strategyIdx = idx('strategy');
    const pnlIdx = idx('pnl');
    const notionalIdx = idx('notional');
    const modeIdx = idx('mode');
    const reasonIdx = idx('reason');
    const executionIdIdx = idx('execution_id');
    const marketKeyIdx = idx('market_key');
    const featureRowIdIdx = idx('feature_row_id');
    const netReturnIdx = idx('net_return');
    const grossReturnIdx = idx('gross_return');
    const roundTripCostRateIdx = idx('round_trip_cost_rate');
    const costDragReturnIdx = idx('cost_drag_return');

    const start = Math.max(1, lines.length - safeRows);
    let processed = 0;
    let labeled = 0;
    let skipped = 0;

    for (let i = start; i < lines.length; i += 1) {
        const cells = parseCsvLineLoose(lines[i]);
        const mode = modeIdx >= 0 ? unquoteCsvCell(cells[modeIdx] || '') : '';
        if (mode && mode.toUpperCase() !== 'PAPER') {
            continue;
        }

        const strategy = strategyIdx >= 0 ? unquoteCsvCell(cells[strategyIdx] || '') : '';
        if (!strategy) {
            skipped += 1;
            continue;
        }

        const toNumberCell = (index: number): number | null => {
            if (index < 0) {
                return null;
            }
            const parsed = Number(unquoteCsvCell(cells[index] || ''));
            return Number.isFinite(parsed) ? parsed : null;
        };
        const details: Record<string, unknown> = {};
        const netReturn = toNumberCell(netReturnIdx);
        const grossReturn = toNumberCell(grossReturnIdx);
        const roundTripCostRate = toNumberCell(roundTripCostRateIdx);
        const costDragReturn = toNumberCell(costDragReturnIdx);
        if (netReturn !== null) details.net_return = netReturn;
        if (grossReturn !== null) details.gross_return = grossReturn;
        if (roundTripCostRate !== null) details.round_trip_cost_rate = roundTripCostRate;
        if (costDragReturn !== null) details.cost_drag_return = costDragReturn;

        const payload: Record<string, unknown> = {
            timestamp: toNumberCell(timestampIdx) ?? Date.now(),
            strategy,
            pnl: toNumberCell(pnlIdx),
            notional: toNumberCell(notionalIdx),
            mode: mode || 'PAPER',
            reason: reasonIdx >= 0 ? unquoteCsvCell(cells[reasonIdx] || '') : '',
            execution_id: executionIdIdx >= 0 ? (unquoteCsvCell(cells[executionIdIdx] || '') || undefined) : undefined,
            details,
        };
        if (marketKeyIdx >= 0) {
            const marketKey = unquoteCsvCell(cells[marketKeyIdx] || '');
            if (marketKey) {
                payload.market_key = marketKey;
            }
        }
        if (featureRowIdIdx >= 0) {
            const featureRowId = unquoteCsvCell(cells[featureRowIdIdx] || '');
            if (featureRowId) {
                payload.feature_row_id = featureRowId;
            }
        }

        const sample = parseTradeSample(payload);
        if (!sample || sample.label_eligible === false) {
            skipped += 1;
            continue;
        }
        processed += 1;
        if (attachLabelToFeature(sample)) {
            labeled += 1;
        }
    }

    if (labeled > 0) {
        io.emit('feature_registry_update', featureRegistrySummary());
    }
    const rowsScanned = Math.max(0, lines.length - start);
    if (emitSummary || labeled > 0) {
        logger.info(
            `[FeatureRegistryLabelBackfill] processed=${processed} labeled=${labeled} skipped=${skipped} source=${filePath} rows_scanned=${rowsScanned}`,
        );
    }
    return {
        processed,
        labeled,
        skipped,
        rows_scanned: rowsScanned,
    };
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
        const promotedBaseMultiplier = Math.min(
            STRATEGY_WEIGHT_CAP,
            Math.max(perf.base_multiplier, perf.multiplier, 1.1 + (decision.confidence * 0.2)),
        );
        const promotedMetaOverlay = computePortfolioMetaAllocatorOverlay(decision.strategy, perf);
        const promotedMultiplier = Math.min(
            STRATEGY_WEIGHT_CAP,
            Math.max(STRATEGY_WEIGHT_FLOOR, promotedBaseMultiplier * promotedMetaOverlay),
        );
        perf.base_multiplier = promotedBaseMultiplier;
        perf.meta_overlay = promotedMetaOverlay;
        perf.multiplier = promotedMultiplier;
        perf.updated_at = now;
        await clearStrategyPause(decision.strategy);
        clearRiskGuardBootResumeTimer(decision.strategy);
        strategyStatus[decision.strategy] = true;
        await publishStrategyMultiplier(decision.strategy, promotedMultiplier);
        await redisClient.set(`strategy:enabled:${decision.strategy}`, '1');
        await redisClient.persist(`strategy:enabled:${decision.strategy}`);
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
            base_multiplier: promotedBaseMultiplier,
            meta_overlay: promotedMetaOverlay,
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
        perf.base_multiplier = 0;
        perf.meta_overlay = 1;
        perf.multiplier = 0;
        perf.updated_at = now;
        await clearStrategyPause(decision.strategy);
        clearRiskGuardBootResumeTimer(decision.strategy);
        strategyStatus[decision.strategy] = false;
        await publishStrategyMultiplier(decision.strategy, 0);
        await redisClient.set(`strategy:enabled:${decision.strategy}`, '0');
        await redisClient.persist(`strategy:enabled:${decision.strategy}`);
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
            base_multiplier: 0,
            meta_overlay: 1,
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
    const removed: FeatureSnapshot[] = [];
    const kept: FeatureSnapshot[] = [];
    let remainingToRemove = removeCount;

    // Preserve labeled rows as long as possible and trim oldest unlabeled rows first.
    for (const row of featureRegistryRows) {
        const labeled = row.label_net_return !== null || row.label_pnl !== null;
        if (remainingToRemove > 0 && !labeled) {
            removed.push(row);
            remainingToRemove -= 1;
            continue;
        }
        kept.push(row);
    }

    if (remainingToRemove > 0) {
        removed.push(...kept.splice(0, remainingToRemove));
    }

    featureRegistryRows.length = 0;
    featureRegistryRows.push(...kept);
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

type FeatureRegistryReplayEvent =
    | {
        event_type: 'SCAN';
        event_ts: number;
        row_id: string;
        strategy: string;
        market_key: string;
        scan_ts: number;
        signal_type: string;
        metric_family: MetricFamily;
        features: Record<string, number>;
    }
    | {
        event_type: 'LABEL';
        event_ts: number;
        row_id: string;
        strategy: string;
        label_ts: number;
        label_net_return: number | null;
        label_pnl: number | null;
        label_source: string | null;
    }
    | {
        event_type: 'RESET';
        event_ts: number;
    };

function sanitizeReplayFeatureMap(input: unknown): Record<string, number> {
    const row = asRecord(input);
    if (!row) {
        return {};
    }
    const features: Record<string, number> = {};
    for (const [key, value] of Object.entries(row)) {
        if (!/^[a-zA-Z0-9_]+$/.test(key)) {
            continue;
        }
        const parsed = asNumber(value);
        if (parsed === null) {
            continue;
        }
        features[key] = parsed;
    }
    return features;
}

function parseReplayMetricFamily(input: unknown): MetricFamily {
    const raw = asString(input)?.toUpperCase() || '';
    switch (raw) {
    case 'ARBITRAGE_EDGE':
    case 'MOMENTUM':
    case 'FAIR_VALUE':
    case 'MARKET_MAKING':
    case 'ORDER_FLOW':
    case 'FLOW_PRESSURE':
    case 'UNKNOWN':
        return raw;
    default:
        return 'UNKNOWN';
    }
}

function parseFeatureRegistryReplayEvent(line: string): FeatureRegistryReplayEvent | null {
    let parsed: unknown;
    try {
        parsed = JSON.parse(line);
    } catch {
        return null;
    }

    const row = asRecord(parsed);
    if (!row) {
        return null;
    }

    const eventType = asString(row.event_type)?.toUpperCase() || '';
    const eventTs = asNumber(row.event_ts) ?? Date.now();
    if (eventType === 'SCAN') {
        const rowId = asString(row.row_id);
        const strategy = asString(row.strategy);
        const marketKey = asString(row.market_key);
        const scanTs = asNumber(row.scan_ts) ?? eventTs;
        if (!rowId || !strategy || !marketKey) {
            return null;
        }
        return {
            event_type: 'SCAN',
            event_ts: eventTs,
            row_id: rowId,
            strategy,
            market_key: marketKey,
            scan_ts: scanTs,
            signal_type: asString(row.signal_type) || 'UNKNOWN',
            metric_family: parseReplayMetricFamily(row.metric_family),
            features: sanitizeReplayFeatureMap(row.features),
        };
    }

    if (eventType === 'LABEL') {
        const rowId = asString(row.row_id);
        const strategy = asString(row.strategy);
        if (!rowId || !strategy) {
            return null;
        }
        return {
            event_type: 'LABEL',
            event_ts: eventTs,
            row_id: rowId,
            strategy,
            label_ts: asNumber(row.label_ts) ?? eventTs,
            label_net_return: row.label_net_return === null ? null : asNumber(row.label_net_return),
            label_pnl: row.label_pnl === null ? null : asNumber(row.label_pnl),
            label_source: asString(row.label_source),
        };
    }

    if (eventType === 'RESET') {
        return {
            event_type: 'RESET',
            event_ts: eventTs,
        };
    }

    return null;
}

function applyFeatureRegistryReplayEvent(event: FeatureRegistryReplayEvent): void {
    if (event.event_type === 'RESET') {
        clearFeatureRegistry();
        clearModelInferenceState();
        featureRegistryUpdatedAt = Math.max(featureRegistryUpdatedAt, event.event_ts);
        return;
    }

    if (event.event_type === 'SCAN') {
        const existingIndex = featureRegistryIndexById.get(event.row_id);
        const snapshot: FeatureSnapshot = {
            id: event.row_id,
            strategy: event.strategy,
            market_key: event.market_key,
            timestamp: event.scan_ts,
            signal_type: event.signal_type,
            metric_family: event.metric_family,
            features: event.features,
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
            featureRegistryIndexById.set(event.row_id, existingIndex);
        } else {
            if (existingIndex !== undefined) {
                featureRegistryIndexById.delete(event.row_id);
            }
            featureRegistryRows.push(snapshot);
            featureRegistryIndexById.set(event.row_id, featureRegistryRows.length - 1);
            addFeatureRegistryRowStats(snapshot);
        }
        featureRegistryUpdatedAt = Math.max(featureRegistryUpdatedAt, event.event_ts, event.scan_ts);
        return;
    }

    const index = featureRegistryIndexById.get(event.row_id);
    if (index === undefined || index < 0 || index >= featureRegistryRows.length) {
        return;
    }
    const row = featureRegistryRows[index];
    if (row.strategy !== event.strategy) {
        return;
    }
    if (row.label_net_return !== null || row.label_pnl !== null) {
        return;
    }
    if (event.label_ts < row.timestamp) {
        featureRegistryLeakageViolations += 1;
        return;
    }

    row.label_net_return = event.label_net_return;
    row.label_pnl = event.label_pnl;
    row.label_timestamp = event.label_ts;
    row.label_source = event.label_source;
    markFeatureRegistryRowLabeled(row);
    featureRegistryUpdatedAt = Math.max(featureRegistryUpdatedAt, event.event_ts, event.label_ts);
}

async function replayFeatureRegistryEventLog(filePath: string): Promise<{
    parsed: number;
    scans: number;
    labels: number;
    resets: number;
    errors: number;
}> {
    const summary = {
        parsed: 0,
        scans: 0,
        labels: 0,
        resets: 0,
        errors: 0,
    };

    const stream = createReadStream(filePath, { encoding: 'utf8' });
    const lineReader = readline.createInterface({
        input: stream,
        crlfDelay: Infinity,
    });

    for await (const rawLine of lineReader) {
        const line = rawLine.trim();
        if (line.length === 0) {
            continue;
        }
        const event = parseFeatureRegistryReplayEvent(line);
        if (!event) {
            summary.errors += 1;
            continue;
        }
        summary.parsed += 1;
        applyFeatureRegistryReplayEvent(event);
        if (event.event_type === 'SCAN') {
            summary.scans += 1;
            if (summary.scans % 256 === 0 && featureRegistryRows.length > FEATURE_REGISTRY_TRIM_TRIGGER_ROWS) {
                trimFeatureRegistryRowsIfNeeded();
            }
        } else if (event.event_type === 'LABEL') {
            summary.labels += 1;
        } else {
            summary.resets += 1;
        }
    }

    return summary;
}

async function bootstrapFeatureRegistryFromEventLogs(): Promise<void> {
    clearFeatureRegistry();
    clearModelInferenceState();

    const basePath = featureRegistryRecorder.getPath();
    const replayPaths = [`${basePath}.old`, basePath];
    const totals = {
        files: 0,
        parsed: 0,
        scans: 0,
        labels: 0,
        resets: 0,
        errors: 0,
    };

    for (const replayPath of replayPaths) {
        const stat = await fs.stat(replayPath).catch((error) => {
            const code = (error as NodeJS.ErrnoException)?.code || '';
            if (code !== 'ENOENT') {
                recordSilentCatch('FeatureRegistry.ReplayStat', error, { path: replayPath });
            }
            return null;
        });
        if (!stat || !stat.isFile() || stat.size <= 0) {
            continue;
        }
        const summary = await replayFeatureRegistryEventLog(replayPath).catch((error) => {
            recordSilentCatch('FeatureRegistry.ReplayRead', error, { path: replayPath });
            return null;
        });
        if (!summary) {
            continue;
        }
        totals.files += 1;
        totals.parsed += summary.parsed;
        totals.scans += summary.scans;
        totals.labels += summary.labels;
        totals.resets += summary.resets;
        totals.errors += summary.errors;
    }

    trimFeatureRegistryRowsIfNeeded();
    modelTrainerLastLabeledRows = featureRegistryRows.filter((row) => row.label_net_return !== null).length;

    if (totals.files > 0) {
        logger.info(
            `[FeatureRegistryBootstrap] replayed files=${totals.files} events=${totals.parsed} scans=${totals.scans} labels=${totals.labels} resets=${totals.resets} errors=${totals.errors} rows=${featureRegistryRows.length} labeled_rows=${featureRegistryLabeledRows}`,
        );
    }
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
    } catch (error) {
        recordSilentCatch('MetaController.PersistRegime', error, { key: 'meta_controller:regime' });
    }
    try {
        await refreshAllocatorMetaOverlays('meta_controller');
    } catch (error) {
        recordSilentCatch('MetaController.AllocatorOverlay', error);
    }
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
        recordSilentCatch('MetaController.Refresh', error);
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

const CROSS_HORIZON_STALE_MS = Math.max(
    INTELLIGENCE_GATE_MAX_STALENESS_MS,
    CROSS_HORIZON_ROUTER_INTERVAL_MS * 3,
);

function clampCrossHorizonOverlay(value: number): number {
    if (!Number.isFinite(value)) {
        return 1;
    }
    return Math.min(CROSS_HORIZON_ROUTER_MAX_BOOST, Math.max(0, value));
}

function mergeCrossHorizonOverlay(current: number, next: number): number {
    const currentSafe = clampCrossHorizonOverlay(current);
    const nextSafe = clampCrossHorizonOverlay(next);
    if (nextSafe < 1) {
        return Math.min(currentSafe, nextSafe);
    }
    if (nextSafe > 1) {
        return Math.max(currentSafe, nextSafe);
    }
    return currentSafe;
}

async function persistCrossHorizonOverlay(strategyId: string, overlay: number): Promise<void> {
    const sanitized = clampCrossHorizonOverlay(overlay);
    const previous = crossHorizonOverlayCache[strategyId];
    if (Number.isFinite(previous) && Math.abs(previous - sanitized) < 1e-4) {
        return;
    }
    const key = `${CROSS_HORIZON_OVERLAY_KEY_PREFIX}${strategyId}`;
    await redisClient.set(key, sanitized.toFixed(6));
    crossHorizonOverlayCache[strategyId] = sanitized;
}

function evaluateCrossHorizonPair(pair: { asset: string; fast: string; slow: string }, now: number): CrossHorizonPairState {
    const fastScan = latestStrategyScans.get(pair.fast);
    const slowScan = latestStrategyScans.get(pair.slow);
    const noDataState = (reason: string): CrossHorizonPairState => ({
        asset: pair.asset,
        fast_strategy: pair.fast,
        slow_strategy: pair.slow,
        fast_margin: fastScan ? computeNormalizedMargin(fastScan) : null,
        slow_margin: slowScan ? computeNormalizedMargin(slowScan) : null,
        fast_direction: fastScan ? scanDirectionSign(fastScan) : 0,
        slow_direction: slowScan ? scanDirectionSign(slowScan) : 0,
        fast_overlay: 1,
        slow_overlay: 1,
        action: 'NO_DATA',
        reason,
        updated_at: now,
    });
    if (!fastScan || !slowScan) {
        return noDataState('missing fast/slow scan');
    }
    if ((now - fastScan.timestamp) > CROSS_HORIZON_STALE_MS || (now - slowScan.timestamp) > CROSS_HORIZON_STALE_MS) {
        return noDataState('scan stale');
    }

    const fastMargin = computeNormalizedMargin(fastScan);
    const slowMargin = computeNormalizedMargin(slowScan);
    const fastDirection = scanDirectionSign(fastScan);
    const slowDirection = scanDirectionSign(slowScan);

    let fastOverlay = 1;
    let slowOverlay = 1;
    let action: CrossHorizonPairAction = 'NEUTRAL';
    let reason = 'neutral';

    if (!fastScan.passes_threshold || !slowScan.passes_threshold) {
        reason = 'one or more legs below threshold';
    } else if (Math.abs(fastMargin) < CROSS_HORIZON_ROUTER_MIN_MARGIN && Math.abs(slowMargin) < CROSS_HORIZON_ROUTER_MIN_MARGIN) {
        reason = 'both legs below minimum margin';
    } else if (fastDirection !== 0 && slowDirection !== 0 && fastDirection !== slowDirection) {
        fastOverlay = CROSS_HORIZON_ROUTER_CONFLICT_MULT;
        slowOverlay = CROSS_HORIZON_ROUTER_CONFLICT_MULT;
        action = 'CONFLICT_TAPER';
        reason = 'directional conflict between horizons';
    } else if (fastMargin >= CROSS_HORIZON_ROUTER_STRONG_MARGIN && slowMargin >= CROSS_HORIZON_ROUTER_STRONG_MARGIN) {
        const confidenceBoost = 1 + Math.min(0.25, Math.max(0, Math.min(fastMargin, slowMargin) - CROSS_HORIZON_ROUTER_STRONG_MARGIN));
        const boost = clampCrossHorizonOverlay(CROSS_HORIZON_ROUTER_CONFIRM_MULT * confidenceBoost);
        fastOverlay = boost;
        slowOverlay = boost;
        action = 'CONSENSUS_BOOST';
        reason = 'strong multi-horizon consensus';
    } else if (fastMargin >= CROSS_HORIZON_ROUTER_STRONG_MARGIN || slowMargin >= CROSS_HORIZON_ROUTER_STRONG_MARGIN) {
        const leadFast = fastMargin >= slowMargin;
        const boost = clampCrossHorizonOverlay(CROSS_HORIZON_ROUTER_CONFIRM_MULT);
        if (leadFast) {
            fastOverlay = boost;
        } else {
            slowOverlay = boost;
        }
        action = 'ASYMMETRIC';
        reason = leadFast ? 'fast horizon stronger' : 'slow horizon stronger';
    } else if (fastDirection !== 0 && slowDirection !== 0 && fastDirection === slowDirection) {
        const mildBoost = clampCrossHorizonOverlay(1 + ((fastMargin + slowMargin) * 0.20));
        if (mildBoost > 1) {
            fastOverlay = mildBoost;
            slowOverlay = mildBoost;
            action = 'CONSENSUS_BOOST';
            reason = 'mild directional consensus';
        } else {
            reason = 'consensus with insufficient margin';
        }
    } else {
        reason = 'non-directional or weak alignment';
    }

    return {
        asset: pair.asset,
        fast_strategy: pair.fast,
        slow_strategy: pair.slow,
        fast_margin: fastMargin,
        slow_margin: slowMargin,
        fast_direction: fastDirection,
        slow_direction: slowDirection,
        fast_overlay: clampCrossHorizonOverlay(fastOverlay),
        slow_overlay: clampCrossHorizonOverlay(slowOverlay),
        action,
        reason,
        updated_at: now,
    };
}

async function refreshCrossHorizonRouter(now = Date.now()): Promise<void> {
    const overlays: Record<string, number> = Object.fromEntries(STRATEGY_IDS.map((strategyId) => [strategyId, 1]));
    const pairStates: Record<string, CrossHorizonPairState> = {};
    const pairCount = CROSS_HORIZON_STRATEGY_PAIRS.length;
    const routerEnabled = CROSS_HORIZON_ROUTER_ENABLED && pairCount > 0 && crossHorizonManagedStrategies.length > 0;

    if (!routerEnabled) {
        const reason = !CROSS_HORIZON_ROUTER_ENABLED
            ? 'disabled by config'
            : 'no valid cross-horizon pairs configured';
        for (const strategyId of STRATEGY_IDS) {
            await persistCrossHorizonOverlay(strategyId, 1);
        }
        crossHorizonRouterState = {
            enabled: CROSS_HORIZON_ROUTER_ENABLED,
            interval_ms: CROSS_HORIZON_ROUTER_INTERVAL_MS,
            overlays,
            pairs: {},
            updated_at: now,
            reason,
        };
        io.emit('cross_horizon_router_update', crossHorizonRouterState);
        touchRuntimeModule('CROSS_HORIZON_ROUTER', CROSS_HORIZON_ROUTER_ENABLED ? 'DEGRADED' : 'STANDBY', reason);
        return;
    }

    for (const pair of CROSS_HORIZON_STRATEGY_PAIRS) {
        const pairState = evaluateCrossHorizonPair(pair, now);
        const pairKey = `${pair.asset}:${pair.fast}:${pair.slow}`;
        pairStates[pairKey] = pairState;
        overlays[pair.fast] = mergeCrossHorizonOverlay(overlays[pair.fast] ?? 1, pairState.fast_overlay);
        overlays[pair.slow] = mergeCrossHorizonOverlay(overlays[pair.slow] ?? 1, pairState.slow_overlay);
    }

    for (const strategyId of STRATEGY_IDS) {
        await persistCrossHorizonOverlay(strategyId, overlays[strategyId] ?? 1);
    }

    crossHorizonRouterState = {
        enabled: true,
        interval_ms: CROSS_HORIZON_ROUTER_INTERVAL_MS,
        overlays: Object.fromEntries(
            Object.entries(overlays).map(([strategy, value]) => [strategy, Math.round(value * 1000) / 1000]),
        ),
        pairs: Object.fromEntries(
            Object.entries(pairStates).map(([pairKey, state]) => [pairKey, {
                ...state,
                fast_margin: state.fast_margin === null ? null : Math.round(state.fast_margin * 1000) / 1000,
                slow_margin: state.slow_margin === null ? null : Math.round(state.slow_margin * 1000) / 1000,
                fast_overlay: Math.round(state.fast_overlay * 1000) / 1000,
                slow_overlay: Math.round(state.slow_overlay * 1000) / 1000,
            }]),
        ),
        updated_at: now,
        reason: `updated ${pairCount} pair${pairCount === 1 ? '' : 's'}`,
    };
    io.emit('cross_horizon_router_update', crossHorizonRouterState);
    touchRuntimeModule(
        'CROSS_HORIZON_ROUTER',
        'ONLINE',
        `${pairCount} pairs / ${crossHorizonManagedStrategies.length} managed strategies`,
    );
}

async function runCrossHorizonRefreshSafely(): Promise<void> {
    try {
        await refreshCrossHorizonRouter();
    } catch (error) {
        recordSilentCatch('CrossHorizonRouter.Refresh', error);
        touchRuntimeModule('CROSS_HORIZON_ROUTER', 'DEGRADED', `cross-horizon refresh error: ${String(error)}`);
    }
}

function requestCrossHorizonRefresh(): void {
    crossHorizonRefreshScheduled = true;
    if (crossHorizonRefreshTimer) {
        return;
    }

    crossHorizonRefreshTimer = setTimeout(() => {
        crossHorizonRefreshTimer = null;
        if (crossHorizonRefreshInFlight) {
            return;
        }
        crossHorizonRefreshInFlight = true;
        void (async () => {
            try {
                while (crossHorizonRefreshScheduled) {
                    crossHorizonRefreshScheduled = false;
                    await runCrossHorizonRefreshSafely();
                }
            } finally {
                crossHorizonRefreshInFlight = false;
                if (crossHorizonRefreshScheduled && !crossHorizonRefreshTimer) {
                    requestCrossHorizonRefresh();
                }
            }
        })();
    }, CROSS_HORIZON_REFRESH_DEBOUNCE_MS);
}

async function readJsonFileSafe(filePath: string): Promise<Record<string, unknown> | null> {
    try {
        const raw = await fs.readFile(filePath, 'utf8');
        const parsed = JSON.parse(raw);
        return asRecord(parsed);
    } catch (error) {
        recordSilentCatch('MLArtifact.ReadJsonSafe', error, { file_path: filePath });
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
    minTrainRows: number,
    minTestRows: number,
): Array<{ trainIdx: number[]; testIdx: number[] }> {
    if (rows < Math.max(minTrainRows + minTestRows, splits * 2)) {
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
        if (testIdx.length < minTestRows) {
            continue;
        }
        const leftEnd = Math.max(0, range.start - purgeRows);
        const rightStart = Math.min(rows, range.end + embargoRows);
        const trainIdx = [
            ...Array.from({ length: leftEnd }, (_v, idx) => idx),
            ...Array.from({ length: rows - rightStart }, (_v, idx) => rightStart + idx),
        ];
        if (trainIdx.length < minTrainRows) {
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
    const contextFeatures = buildStrategyContextFeatures(scan);
    return {
        ...(scan.meta_features || {}),
        ...contextFeatures,
        score: scan.score,
        threshold: scan.threshold,
        margin,
        normalized_margin: normalizedMargin,
        pass_flag: scan.passes_threshold ? 1 : 0,
        abs_score: Math.abs(scan.score),
        abs_threshold: Math.abs(scan.threshold),
    };
}

function modelInferencePayload(limitRows = MODEL_INFERENCE_PAYLOAD_MAX_ROWS): ModelInferenceState {
    const safeLimit = Number.isFinite(limitRows)
        ? Math.max(1, Math.min(MODEL_INFERENCE_MAX_TRACKED_ROWS, Math.floor(limitRows)))
        : MODEL_INFERENCE_PAYLOAD_MAX_ROWS;
    const selected = Object.entries(modelInferenceState.latest)
        .sort((a, b) => b[1].timestamp - a[1].timestamp)
        .slice(0, safeLimit);
    return {
        ...modelInferenceState,
        latest: Object.fromEntries(selected.map(([key, value]) => [
            key,
            {
                ...value,
                probability_positive: value.probability_positive === null
                    ? null
                    : Math.round(value.probability_positive * 1000) / 1000,
            },
        ])),
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

function clampProbabilityGate(value: number): number {
    if (!Number.isFinite(value)) {
        return MODEL_PROBABILITY_GATE_MIN_PROB;
    }
    return Math.min(0.95, Math.max(0.50, value));
}

function modelGateMarketScopeKey(strategy: string, marketKey: string): string {
    return `${strategy}::${marketKey}`;
}

function roundGateMap(input: Record<string, number>): Record<string, number> {
    return Object.fromEntries(
        Object.entries(input)
            .map(([key, value]) => [key, Math.round(clampProbabilityGate(value) * 1000) / 1000]),
    );
}

function roundCountMap(input: Record<string, number>): Record<string, number> {
    return Object.fromEntries(
        Object.entries(input)
            .map(([key, value]) => [key, Math.max(0, Math.round(value))]),
    );
}

function parseGateMap(raw: unknown, minGate: number, maxGate: number, maxEntries: number): Record<string, number> {
    const parsed = asRecord(raw);
    if (!parsed) {
        return {};
    }
    const entries = Object.entries(parsed)
        .map(([key, value]) => [key, asNumber(value)] as const)
        .filter(([key, value]) => key.trim().length > 0 && value !== null && Number.isFinite(value))
        .sort((a, b) => (b[1] as number) - (a[1] as number))
        .slice(0, maxEntries)
        .map(([key, value]) => [
            key.trim(),
            Math.min(maxGate, Math.max(minGate, clampProbabilityGate(value as number))),
        ] as const);
    return Object.fromEntries(entries);
}

function parseCountMap(raw: unknown, maxEntries: number): Record<string, number> {
    const parsed = asRecord(raw);
    if (!parsed) {
        return {};
    }
    const entries = Object.entries(parsed)
        .map(([key, value]) => [key, asNumber(value)] as const)
        .filter(([key, value]) => key.trim().length > 0 && value !== null && Number.isFinite(value))
        .sort((a, b) => (b[1] as number) - (a[1] as number))
        .slice(0, maxEntries)
        .map(([key, value]) => [key.trim(), Math.max(0, Math.round(value as number))] as const);
    return Object.fromEntries(entries);
}

function activeModelProbabilityGate(strategy?: string | null, marketKey?: string | null): number {
    if (strategy && marketKey) {
        const scopedMarket = modelGateMarketScopeKey(strategy, marketKey);
        const marketGate = asNumber(modelGateCalibrationState.market_active_gates[scopedMarket]);
        if (marketGate !== null) {
            return clampProbabilityGate(marketGate);
        }
    }
    if (strategy) {
        const strategyGate = asNumber(modelGateCalibrationState.strategy_active_gates[strategy]);
        if (strategyGate !== null) {
            return clampProbabilityGate(strategyGate);
        }
    }
    return clampProbabilityGate(modelGateCalibrationState.active_gate);
}

function modelGateCalibrationPayload(): ModelGateCalibrationState {
    return {
        ...modelGateCalibrationState,
        baseline_gate: Math.round(modelGateCalibrationState.baseline_gate * 1000) / 1000,
        active_gate: Math.round(modelGateCalibrationState.active_gate * 1000) / 1000,
        recommended_gate: Math.round(modelGateCalibrationState.recommended_gate * 1000) / 1000,
        expected_pnl_baseline: Math.round(modelGateCalibrationState.expected_pnl_baseline * 100) / 100,
        expected_pnl_recommended: Math.round(modelGateCalibrationState.expected_pnl_recommended * 100) / 100,
        delta_expected_pnl: Math.round(modelGateCalibrationState.delta_expected_pnl * 100) / 100,
        strategy_active_gates: roundGateMap(modelGateCalibrationState.strategy_active_gates),
        strategy_recommended_gates: roundGateMap(modelGateCalibrationState.strategy_recommended_gates),
        strategy_sample_counts: roundCountMap(modelGateCalibrationState.strategy_sample_counts),
        market_active_gates: roundGateMap(modelGateCalibrationState.market_active_gates),
        market_recommended_gates: roundGateMap(modelGateCalibrationState.market_recommended_gates),
        market_sample_counts: roundCountMap(modelGateCalibrationState.market_sample_counts),
    };
}

function modelGateProbabilityFromTrace(executionId: string): number | null {
    const trace = executionTraces.get(executionId);
    if (!trace || trace.events.length === 0) {
        return null;
    }
    for (let index = trace.events.length - 1; index >= 0; index -= 1) {
        const event = trace.events[index];
        if (!event || event.type !== 'MODEL_GATE') {
            continue;
        }
        const probability = asNumber(asRecord(event.payload)?.probability);
        if (probability !== null && Number.isFinite(probability)) {
            return clampUnitProbability(probability);
        }
    }
    return null;
}

function collectModelGateCalibrationSamples(now = Date.now()): ModelGateCalibrationSample[] {
    return buildModelGateCalibrationSamples({
        now,
        lookbackMs: MODEL_PROBABILITY_GATE_TUNER_LOOKBACK_MS,
        counterfactualWindowMs: MODEL_PROBABILITY_GATE_COUNTERFACTUAL_WINDOW_MS,
        strategyIds: STRATEGY_IDS,
        strategyTradeSamples,
        featureRegistryRows,
        rejectedSignalHistory,
        probabilityForExecutionId: modelGateProbabilityFromTrace,
        normalizeProbability: clampUnitProbability,
        fallbackNotionalUsd: 100,
    });
}

function scoreModelGate(samples: ModelGateCalibrationSample[], gate: number): number {
    let sum = 0;
    for (const sample of samples) {
        if (sample.probability >= gate) {
            sum += sample.pnl;
        }
    }
    return sum;
}

type GateOptimizationResult = {
    baseline_score: number;
    recommended_gate: number;
    recommended_score: number;
    active_gate: number;
};

function optimizeModelGate(
    samples: ModelGateCalibrationSample[],
    baselineGate: number,
    minGate: number,
    maxGate: number,
    stepSize: number,
    previousActive: number,
    advisoryOnly: boolean,
): GateOptimizationResult {
    const baselineScore = scoreModelGate(samples, baselineGate);
    let bestGate = baselineGate;
    let bestScore = baselineScore;
    const boundedPrevious = Math.min(maxGate, Math.max(minGate, clampProbabilityGate(previousActive)));
    for (let gate = minGate; gate <= maxGate + 1e-9; gate += stepSize) {
        const candidateGate = clampProbabilityGate(gate);
        const candidateScore = scoreModelGate(samples, candidateGate);
        const currentDelta = Math.abs(bestGate - boundedPrevious);
        const candidateDelta = Math.abs(candidateGate - boundedPrevious);
        if (candidateScore > bestScore + 1e-9 || (Math.abs(candidateScore - bestScore) <= 1e-9 && candidateDelta < currentDelta)) {
            bestGate = candidateGate;
            bestScore = candidateScore;
        }
    }

    const boundedRecommended = Math.min(maxGate, Math.max(minGate, bestGate));
    const nextActive = advisoryOnly
        ? baselineGate
        : (() => {
            const delta = boundedRecommended - boundedPrevious;
            if (Math.abs(delta) <= stepSize) {
                return boundedRecommended;
            }
            return clampProbabilityGate(boundedPrevious + Math.sign(delta) * stepSize);
        })();

    return {
        baseline_score: baselineScore,
        recommended_gate: boundedRecommended,
        recommended_score: bestScore,
        active_gate: Math.min(maxGate, Math.max(minGate, nextActive)),
    };
}

async function loadModelGateCalibrationState(): Promise<void> {
    try {
        const raw = await redisClient.get(MODEL_GATE_CALIBRATION_KEY);
        if (!raw) {
            return;
        }
        const parsed = asRecord(JSON.parse(raw));
        if (!parsed) {
            return;
        }
        const baselineGate = MODEL_PROBABILITY_GATE_MIN_PROB;
        const drift = MODEL_PROBABILITY_GATE_TUNER_MAX_DRIFT;
        const minGate = clampProbabilityGate(baselineGate - drift);
        const maxGate = clampProbabilityGate(baselineGate + drift);
        const restoredActive = clampProbabilityGate(asNumber(parsed.active_gate) ?? baselineGate);
        const restoredRecommended = clampProbabilityGate(asNumber(parsed.recommended_gate) ?? restoredActive);
        const strategyActive = parseGateMap(parsed.strategy_active_gates, minGate, maxGate, STRATEGY_IDS.length * 2);
        const strategyRecommended = parseGateMap(parsed.strategy_recommended_gates, minGate, maxGate, STRATEGY_IDS.length * 2);
        const strategyCounts = parseCountMap(parsed.strategy_sample_counts, STRATEGY_IDS.length * 2);
        const marketActive = parseGateMap(parsed.market_active_gates, minGate, maxGate, MODEL_GATE_MARKET_MAX_KEYS);
        const marketRecommended = parseGateMap(parsed.market_recommended_gates, minGate, maxGate, MODEL_GATE_MARKET_MAX_KEYS);
        const marketCounts = parseCountMap(parsed.market_sample_counts, MODEL_GATE_MARKET_MAX_KEYS);
        modelGateCalibrationState = {
            ...modelGateCalibrationState,
            baseline_gate: baselineGate,
            active_gate: Math.min(maxGate, Math.max(minGate, restoredActive)),
            recommended_gate: Math.min(maxGate, Math.max(minGate, restoredRecommended)),
            strategy_active_gates: strategyActive,
            strategy_recommended_gates: strategyRecommended,
            strategy_sample_counts: strategyCounts,
            market_active_gates: marketActive,
            market_recommended_gates: marketRecommended,
            market_sample_counts: marketCounts,
            updated_at: asNumber(parsed.updated_at) ?? Date.now(),
            reason: 'restored calibration state from redis',
        };
    } catch (error) {
        recordSilentCatch('ModelGateCalibration.Load', error, { key: MODEL_GATE_CALIBRATION_KEY });
    }
}

async function runModelProbabilityGateCalibration(now = Date.now()): Promise<void> {
    const baselineGate = MODEL_PROBABILITY_GATE_MIN_PROB;
    const stepSize = MODEL_PROBABILITY_GATE_TUNER_STEP;
    const maxDrift = MODEL_PROBABILITY_GATE_TUNER_MAX_DRIFT;
    const minGate = clampProbabilityGate(baselineGate - maxDrift);
    const maxGate = clampProbabilityGate(baselineGate + maxDrift);
    if (!MODEL_PROBABILITY_GATE_TUNER_ENABLED) {
        modelGateCalibrationState = {
            ...modelGateCalibrationState,
            enabled: false,
            advisory_only: MODEL_PROBABILITY_GATE_TUNER_ADVISORY_ONLY,
            baseline_gate: baselineGate,
            active_gate: baselineGate,
            recommended_gate: baselineGate,
            lookback_ms: MODEL_PROBABILITY_GATE_TUNER_LOOKBACK_MS,
            min_samples: MODEL_PROBABILITY_GATE_TUNER_MIN_SAMPLES,
            step_size: stepSize,
            max_drift: maxDrift,
            sample_count: 0,
            accepted_samples: 0,
            rejected_samples: 0,
            expected_pnl_baseline: 0,
            expected_pnl_recommended: 0,
            delta_expected_pnl: 0,
            profitable_rejected_signals: 0,
            loss_avoided_signals: 0,
            strategy_active_gates: {},
            strategy_recommended_gates: {},
            strategy_sample_counts: {},
            market_active_gates: {},
            market_recommended_gates: {},
            market_sample_counts: {},
            updated_at: now,
            reason: 'tuner disabled by env',
        };
        io.emit('model_gate_calibration_update', modelGateCalibrationPayload());
        return;
    }

    const samples = collectModelGateCalibrationSamples(now);
    const acceptedSamples = samples.filter((sample) => sample.accepted).length;
    const rejectedSamples = samples.length - acceptedSamples;
    const profitableRejectedSignals = samples.filter((sample) => !sample.accepted && sample.pnl > 0).length;
    const lossAvoidedSignals = samples.filter((sample) => !sample.accepted && sample.pnl <= 0).length;

    const samplesByStrategy = new Map<string, ModelGateCalibrationSample[]>();
    const samplesByMarket = new Map<string, ModelGateCalibrationSample[]>();
    for (const sample of samples) {
        if (sample.strategy && sample.strategy.trim().length > 0) {
            const strategy = sample.strategy.trim().toUpperCase();
            const strategyBucket = samplesByStrategy.get(strategy) || [];
            strategyBucket.push(sample);
            samplesByStrategy.set(strategy, strategyBucket);
            if (sample.market_key) {
                const marketScope = modelGateMarketScopeKey(strategy, sample.market_key);
                const marketBucket = samplesByMarket.get(marketScope) || [];
                marketBucket.push(sample);
                samplesByMarket.set(marketScope, marketBucket);
            }
        }
    }

    const strategyActiveGates: Record<string, number> = {};
    const strategyRecommendedGates: Record<string, number> = {};
    const strategySampleCounts: Record<string, number> = {};
    for (const [strategy, strategySamples] of samplesByStrategy.entries()) {
        strategySampleCounts[strategy] = strategySamples.length;
        if (strategySamples.length < MODEL_GATE_STRATEGY_MIN_SAMPLES) {
            continue;
        }
        const previousActive = asNumber(modelGateCalibrationState.strategy_active_gates[strategy])
            ?? asNumber(modelGateCalibrationState.active_gate)
            ?? baselineGate;
        const strategyOptimization = optimizeModelGate(
            strategySamples,
            baselineGate,
            minGate,
            maxGate,
            stepSize,
            previousActive,
            MODEL_PROBABILITY_GATE_TUNER_ADVISORY_ONLY,
        );
        strategyActiveGates[strategy] = strategyOptimization.active_gate;
        strategyRecommendedGates[strategy] = strategyOptimization.recommended_gate;
    }

    const marketBuckets = [...samplesByMarket.entries()]
        .map(([scope, bucket]) => ({
            scope,
            bucket,
            count: bucket.length,
            last_ts: bucket.reduce((latest, item) => Math.max(latest, item.timestamp), 0),
            strategy: scope.split('::')[0] || 'UNKNOWN',
        }))
        .filter((bucket) => bucket.count >= MODEL_GATE_MARKET_MIN_SAMPLES)
        .sort((a, b) => {
            if (b.count !== a.count) {
                return b.count - a.count;
            }
            return b.last_ts - a.last_ts;
        })
        .slice(0, MODEL_GATE_MARKET_MAX_KEYS);

    const marketActiveGates: Record<string, number> = {};
    const marketRecommendedGates: Record<string, number> = {};
    const marketSampleCounts: Record<string, number> = {};
    for (const bucket of marketBuckets) {
        marketSampleCounts[bucket.scope] = bucket.count;
        const strategyScopedActive = asNumber(strategyActiveGates[bucket.strategy])
            ?? asNumber(modelGateCalibrationState.strategy_active_gates[bucket.strategy])
            ?? asNumber(modelGateCalibrationState.active_gate)
            ?? baselineGate;
        const previousActive = asNumber(modelGateCalibrationState.market_active_gates[bucket.scope])
            ?? strategyScopedActive;
        const marketOptimization = optimizeModelGate(
            bucket.bucket,
            baselineGate,
            minGate,
            maxGate,
            stepSize,
            previousActive,
            MODEL_PROBABILITY_GATE_TUNER_ADVISORY_ONLY,
        );
        marketActiveGates[bucket.scope] = marketOptimization.active_gate;
        marketRecommendedGates[bucket.scope] = marketOptimization.recommended_gate;
    }

    if (samples.length < MODEL_PROBABILITY_GATE_TUNER_MIN_SAMPLES) {
        modelGateCalibrationState = {
            ...modelGateCalibrationState,
            enabled: true,
            advisory_only: MODEL_PROBABILITY_GATE_TUNER_ADVISORY_ONLY,
            baseline_gate: baselineGate,
            active_gate: MODEL_PROBABILITY_GATE_TUNER_ADVISORY_ONLY
                ? baselineGate
                : Math.min(maxGate, Math.max(minGate, modelGateCalibrationState.active_gate)),
            recommended_gate: modelGateCalibrationState.recommended_gate,
            lookback_ms: MODEL_PROBABILITY_GATE_TUNER_LOOKBACK_MS,
            min_samples: MODEL_PROBABILITY_GATE_TUNER_MIN_SAMPLES,
            step_size: stepSize,
            max_drift: maxDrift,
            sample_count: samples.length,
            accepted_samples: acceptedSamples,
            rejected_samples: rejectedSamples,
            expected_pnl_baseline: 0,
            expected_pnl_recommended: 0,
            delta_expected_pnl: 0,
            profitable_rejected_signals: profitableRejectedSignals,
            loss_avoided_signals: lossAvoidedSignals,
            strategy_active_gates: strategyActiveGates,
            strategy_recommended_gates: strategyRecommendedGates,
            strategy_sample_counts: strategySampleCounts,
            market_active_gates: marketActiveGates,
            market_recommended_gates: marketRecommendedGates,
            market_sample_counts: marketSampleCounts,
            updated_at: now,
            reason: `insufficient labeled samples (${samples.length}/${MODEL_PROBABILITY_GATE_TUNER_MIN_SAMPLES})`,
        };
        io.emit('model_gate_calibration_update', modelGateCalibrationPayload());
        return;
    }

    const globalOptimization = optimizeModelGate(
        samples,
        baselineGate,
        minGate,
        maxGate,
        stepSize,
        modelGateCalibrationState.active_gate,
        MODEL_PROBABILITY_GATE_TUNER_ADVISORY_ONLY,
    );

    modelGateCalibrationState = {
        ...modelGateCalibrationState,
        enabled: true,
        advisory_only: MODEL_PROBABILITY_GATE_TUNER_ADVISORY_ONLY,
        baseline_gate: baselineGate,
        active_gate: globalOptimization.active_gate,
        recommended_gate: globalOptimization.recommended_gate,
        lookback_ms: MODEL_PROBABILITY_GATE_TUNER_LOOKBACK_MS,
        min_samples: MODEL_PROBABILITY_GATE_TUNER_MIN_SAMPLES,
        step_size: stepSize,
        max_drift: maxDrift,
        sample_count: samples.length,
        accepted_samples: acceptedSamples,
        rejected_samples: rejectedSamples,
        expected_pnl_baseline: globalOptimization.baseline_score,
        expected_pnl_recommended: globalOptimization.recommended_score,
        delta_expected_pnl: globalOptimization.recommended_score - globalOptimization.baseline_score,
        profitable_rejected_signals: profitableRejectedSignals,
        loss_avoided_signals: lossAvoidedSignals,
        strategy_active_gates: strategyActiveGates,
        strategy_recommended_gates: strategyRecommendedGates,
        strategy_sample_counts: strategySampleCounts,
        market_active_gates: marketActiveGates,
        market_recommended_gates: marketRecommendedGates,
        market_sample_counts: marketSampleCounts,
        updated_at: now,
        reason: MODEL_PROBABILITY_GATE_TUNER_ADVISORY_ONLY
            ? `recommended ${globalOptimization.recommended_gate.toFixed(3)} from ${samples.length} samples (advisory), strategy/market overlays ready`
            : `active ${globalOptimization.active_gate.toFixed(3)} toward ${globalOptimization.recommended_gate.toFixed(3)} from ${samples.length} samples with strategy/market overlays`,
    };

    touchRuntimeModule(
        'UNCERTAINTY_GATE',
        'ONLINE',
        `model gate ${modelGateCalibrationState.active_gate.toFixed(3)} rec ${modelGateCalibrationState.recommended_gate.toFixed(3)} samples ${samples.length}`,
    );
    io.emit('model_gate_calibration_update', modelGateCalibrationPayload());
    await redisClient.set(MODEL_GATE_CALIBRATION_KEY, JSON.stringify(modelGateCalibrationState)).catch((error) => {
        recordSilentCatch('ModelGateCalibration.Persist', error, { key: MODEL_GATE_CALIBRATION_KEY });
    });
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
    const probabilityGate = activeModelProbabilityGate(scan.strategy, scan.market_key);
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

function isPaperModelBootstrapBypassActive(): boolean {
    if (!MODEL_PROBABILITY_GATE_REQUIRE_MODEL) {
        return false;
    }
    if (mlPipelineStatus.model_eligible) {
        return false;
    }
    if (signalModelArtifact && modelInferenceState.model_loaded) {
        return false;
    }
    return true;
}

function paperModelBootstrapBypassReason(): string {
    const labeledRows = Math.max(0, Math.floor(mlPipelineStatus.dataset_labeled_rows || 0));
    const minRows = MODEL_TRAINER_MIN_LABELED_ROWS;
    const modelReason = mlPipelineStatus.model_reason || 'model not eligible';
    return `paper bootstrap bypass: ${modelReason} (${labeledRows}/${minRows} labeled rows)`;
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
        && !gateDisabled
        && !isPaperModelBootstrapBypassActive();

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
        gate_enforcing: MODEL_PROBABILITY_GATE_ENABLED
            && MODEL_PROBABILITY_GATE_ENFORCE_PAPER
            && !isPaperModelBootstrapBypassActive(),
        gate_disabled_until: 0,
        issues: [],
        by_strategy: {},
        updated_at: Date.now(),
    };
}

function refreshModelDriftRuntime(now = Date.now()): void {
    const gateDisabled = now < modelDriftState.gate_disabled_until;
    const bootstrapBypass = isPaperModelBootstrapBypassActive();
    const nextGateEnforcing = MODEL_PROBABILITY_GATE_ENABLED
        && MODEL_PROBABILITY_GATE_ENFORCE_PAPER
        && !gateDisabled
        && !bootstrapBypass;
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
        nextGateEnforcing ? 'ONLINE' : bootstrapBypass ? 'STANDBY' : 'DEGRADED',
        nextGateEnforcing
            ? 'probability gate enforcing in PAPER'
            : gateDisabled
                ? `probability gate paused until ${modelDriftState.gate_disabled_until}`
                : bootstrapBypass
                    ? paperModelBootstrapBypassReason()
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
    const bootstrapBypass = tradingMode === 'PAPER' && isPaperModelBootstrapBypassActive();
    const requireModel = tradingMode === 'LIVE'
        || (MODEL_PROBABILITY_GATE_REQUIRE_MODEL && !bootstrapBypass);
    const probabilityGate = activeModelProbabilityGate(strategy, marketKey);
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
            gate: probabilityGate,
            model_loaded: Boolean(signalModelArtifact),
        };
    }
    if (bootstrapBypass) {
        return {
            ok: true,
            blocked: false,
            reason: paperModelBootstrapBypassReason(),
            strategy,
            market_key: marketKey,
            probability: null,
            gate: probabilityGate,
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
            gate: probabilityGate,
            model_loaded: Boolean(signalModelArtifact),
        };
    }
    if (!strategy) {
        return {
            ok: !requireModel,
            blocked: requireModel,
            reason: 'execution payload missing strategy identity for model gate',
            strategy,
            market_key: marketKey,
            probability: null,
            gate: probabilityGate,
            model_loaded: Boolean(signalModelArtifact),
        };
    }

    const exactKey = marketKey ? buildStrategyMarketKey(strategy, marketKey) : null;
    const exact = exactKey ? modelInferenceState.latest[exactKey] : null;
    const strategyRows = Object.values(modelInferenceState.latest)
        .filter((entry) => entry.strategy === strategy)
        .sort((a, b) => b.timestamp - a.timestamp);
    const fallback = marketKey
        ? (exact || null)
        : (strategyRows[0] || null);
    if (!fallback) {
        return {
            ok: !requireModel,
            blocked: requireModel,
            reason: marketKey
                ? `no model inference row for ${strategy} on ${marketKey}`
                : `no model inference row for ${strategy}`,
            strategy,
            market_key: marketKey,
            probability: null,
            gate: probabilityGate,
            model_loaded: Boolean(signalModelArtifact),
        };
    }
    const ageMs = Date.now() - fallback.timestamp;
    if (ageMs > MODEL_PROBABILITY_GATE_MAX_STALENESS_MS) {
        return {
            ok: !requireModel,
            blocked: requireModel,
            reason: `model inference stale ${ageMs}ms > ${MODEL_PROBABILITY_GATE_MAX_STALENESS_MS}ms`,
            strategy,
            market_key: fallback.market_key,
            probability: fallback.probability_positive,
            gate: probabilityGate,
            model_loaded: fallback.model_loaded,
        };
    }
    if (fallback.probability_positive === null) {
        return {
            ok: !requireModel,
            blocked: requireModel,
            reason: 'model probability unavailable',
            strategy,
            market_key: fallback.market_key,
            probability: null,
            gate: probabilityGate,
            model_loaded: fallback.model_loaded,
        };
    }
    if (fallback.probability_positive < probabilityGate) {
        return {
            ok: false,
            blocked: true,
            reason: `probability ${fallback.probability_positive.toFixed(3)} below gate ${probabilityGate.toFixed(3)}`,
            strategy,
            market_key: fallback.market_key,
            probability: fallback.probability_positive,
            gate: probabilityGate,
            model_loaded: fallback.model_loaded,
        };
    }
    return {
        ok: true,
        blocked: false,
        reason: `probability ${fallback.probability_positive.toFixed(3)} >= gate ${probabilityGate.toFixed(3)}`,
        strategy,
        market_key: fallback.market_key,
        probability: fallback.probability_positive,
        gate: probabilityGate,
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
    const configuredFoldSpec = {
        splits: MODEL_TRAINER_SPLITS,
        purgeRows: MODEL_TRAINER_PURGE_ROWS,
        embargoRows: MODEL_TRAINER_EMBARGO_ROWS,
        minTrainRows: MODEL_TRAINER_MIN_TRAIN_ROWS_PER_FOLD,
        minTestRows: MODEL_TRAINER_MIN_TEST_ROWS_PER_FOLD,
    };
    const bootstrapFoldSpec = {
        splits: 2,
        purgeRows: 0,
        embargoRows: 0,
        minTrainRows: 2,
        minTestRows: 1,
    };
    let effectiveFoldSpec = configuredFoldSpec;
    let foldMode: 'configured' | 'bootstrap_low_sample' = 'configured';
    let folds = buildPurgedEmbargoFolds(
        trainingRows,
        configuredFoldSpec.splits,
        configuredFoldSpec.purgeRows,
        configuredFoldSpec.embargoRows,
        configuredFoldSpec.minTrainRows,
        configuredFoldSpec.minTestRows,
    );
    if (folds.length < 2 && trainingRows >= (bootstrapFoldSpec.splits * 2)) {
        const bootstrapFolds = buildPurgedEmbargoFolds(
            trainingRows,
            bootstrapFoldSpec.splits,
            bootstrapFoldSpec.purgeRows,
            bootstrapFoldSpec.embargoRows,
            bootstrapFoldSpec.minTrainRows,
            bootstrapFoldSpec.minTestRows,
        );
        if (bootstrapFolds.length >= 2) {
            folds = bootstrapFolds;
            effectiveFoldSpec = bootstrapFoldSpec;
            foldMode = 'bootstrap_low_sample';
        }
    }
    if (folds.length < 2) {
        return ineligible(
            `insufficient folds after purge/embargo (configured: splits=${configuredFoldSpec.splits}, purge=${configuredFoldSpec.purgeRows}, embargo=${configuredFoldSpec.embargoRows}, min_train=${configuredFoldSpec.minTrainRows}, min_test=${configuredFoldSpec.minTestRows}; fallback: splits=${bootstrapFoldSpec.splits}, purge=${bootstrapFoldSpec.purgeRows}, embargo=${bootstrapFoldSpec.embargoRows}, min_train=${bootstrapFoldSpec.minTrainRows}, min_test=${bootstrapFoldSpec.minTestRows})`,
            folds.length,
        );
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
        fold_mode: foldMode,
    };
    const report: Record<string, unknown> = {
        generated_at: generatedAt,
        eligible: true,
        rows: trainingRows,
        total_labeled_rows: totalLabeledRows,
        feature_count: featureCount,
        cv_folds: folds.length,
        fold_mode: foldMode,
        splits: effectiveFoldSpec.splits,
        purge_rows: effectiveFoldSpec.purgeRows,
        embargo_rows: effectiveFoldSpec.embargoRows,
        min_train_rows_per_fold: effectiveFoldSpec.minTrainRows,
        min_test_rows_per_fold: effectiveFoldSpec.minTestRows,
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
        const eligible = trained.report.eligible === true;
        const reason = asString(trained.report.reason) || 'training not eligible';
        const hasPriorEligibleModel = Boolean(signalModelArtifact && modelInferenceState.model_loaded);

        // Protect the active model during low-label windows (common after sim resets):
        // avoid replacing a good artifact with an ineligible retrain result unless forced.
        if (!eligible && hasPriorEligibleModel && !force) {
            await refreshMlPipelineStatus();
            touchRuntimeModule(
                'ML_TRAINER',
                'DEGRADED',
                `retained prior model; skipped ineligible retrain (${reason})`,
            );
            return;
        }

        await persistSignalModelArtifacts(trained.artifact, trained.report);
        await refreshSignalModelArtifact();
        await refreshMlPipelineStatus();
        touchRuntimeModule(
            'ML_TRAINER',
            eligible ? 'ONLINE' : 'STANDBY',
            eligible
                ? `trained with ${trained.labeled_rows} rows`
                : reason,
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
    const featureLogRows = await featureRegistryRecorder.countRows().catch((error) => {
        recordSilentCatch('MlPipeline.FeatureLogRows', error, {
            path: featureRegistryRecorder.getPath(),
        });
        return 0;
    });
    const datasetManifest = await readJsonFileSafe(FEATURE_DATASET_MANIFEST_PATH);
    const modelReport = await readJsonFileSafe(SIGNAL_MODEL_REPORT_PATH);
    const registrySummary = featureRegistrySummary();

    const manifestRows = asNumber(datasetManifest?.rows);
    const manifestLabeledRows = asNumber(datasetManifest?.labeled_rows);
    const datasetRows = Math.max(registrySummary.rows, manifestRows ?? 0);
    const datasetLabeledRows = Math.max(registrySummary.labeled_rows, manifestLabeledRows ?? 0);
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

function applyFeatureLabelToRow(
    row: FeatureSnapshot,
    sample: StrategyTradeSample,
    labelSource: string,
): boolean {
    if (row.label_net_return !== null || row.label_pnl !== null) {
        return false;
    }
    if (sample.timestamp < row.timestamp) {
        featureRegistryLeakageViolations += 1;
        return false;
    }
    row.label_net_return = sample.net_return;
    row.label_pnl = sample.pnl;
    row.label_timestamp = sample.timestamp;
    row.label_source = labelSource;
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
        label_source: labelSource,
    });
    featureRegistryUpdatedAt = Date.now();
    fireAndForget('MLTrainer.InProcess', runInProcessModelTraining(false), {
        source: 'feature_label_attach',
        strategy: row.strategy,
    });
    return true;
}

function attachLabelToFeature(sample: StrategyTradeSample): boolean {
    // Skip ML label assignment for non-eligible closures (stale, manual, reset)
    if (sample.label_eligible === false) {
        return false;
    }

    const sampleMarketKey = sample.market_key || null;

    // Primary path: execution-linked labeling to avoid feature/label drift.
    if (sample.feature_row_id) {
        const executionLinkedIndex = featureRegistryIndexById.get(sample.feature_row_id);
        if (
            executionLinkedIndex !== undefined
            && executionLinkedIndex >= 0
            && executionLinkedIndex < featureRegistryRows.length
        ) {
            const row = featureRegistryRows[executionLinkedIndex];
            if (row.strategy === sample.strategy && (!sampleMarketKey || row.market_key === sampleMarketKey)) {
                return applyFeatureLabelToRow(row, sample, 'strategy:pnl:execution_linked');
            }
        }
        // Continue to fallback matching when the execution-linked row is unavailable.
    }

    // Legacy fallback for historical rows that predate execution-linked traces.
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
        return applyFeatureLabelToRow(
            row,
            sample,
            sample.feature_row_id ? 'strategy:pnl:fallback_after_execution_link_miss' : 'strategy:pnl:fallback',
        );
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

function computeFamilyAllocatorStats(family: string): {
    meanReturn: number;
    meanDownside: number;
    concentration: number;
} {
    let weightedReturn = 0;
    let weightedDownside = 0;
    let weightTotal = 0;
    let familyWeight = 0;
    let totalWeight = 0;

    for (const strategyId of STRATEGY_IDS) {
        const perf = ensureStrategyPerformanceEntry(strategyId);
        const weight = Math.max(0.25, perf.sample_count > 0 ? Math.min(2.0, perf.sample_count / 20) : 0.25);
        const effectiveBase = Number.isFinite(perf.base_multiplier) && perf.base_multiplier > 0
            ? perf.base_multiplier
            : Math.max(0, perf.multiplier);
        totalWeight += effectiveBase;
        if (strategyFamily(strategyId) !== family) {
            continue;
        }
        familyWeight += effectiveBase;
        weightedReturn += perf.ema_return * weight;
        weightedDownside += perf.downside_ema * weight;
        weightTotal += weight;
    }

    return {
        meanReturn: weightTotal > 0 ? weightedReturn / weightTotal : 0,
        meanDownside: weightTotal > 0 ? weightedDownside / weightTotal : 0,
        concentration: totalWeight > 0 ? familyWeight / totalWeight : 0,
    };
}

function computePortfolioMetaAllocatorOverlay(strategyId: string, state: StrategyPerformance): number {
    if (!META_ALLOCATOR_ENABLED) {
        return 1;
    }

    const family = strategyFamily(strategyId);
    const familyStats = computeFamilyAllocatorStats(family);
    const diagnostics = ensureStrategyCostDiagnosticsEntry(strategyId);
    const confidence = Math.min(1, state.sample_count / Math.max(STRATEGY_ALLOCATOR_MIN_SAMPLES, 24));

    let overlay = 1;

    const override = metaControllerState.strategy_overrides[strategyId]?.recommended_multiplier;
    if (Number.isFinite(override)) {
        const regimeConfidence = Math.max(0.20, Math.min(1, metaControllerState.confidence));
        overlay *= 1 + (((override || 1) - 1) * META_ALLOCATOR_META_BLEND * regimeConfidence);
    }

    const familyMomentum = Math.max(-1, Math.min(1, familyStats.meanReturn / 0.06));
    overlay *= 1 + (familyMomentum * 0.18 * Math.max(0.25, confidence));

    const downsidePenalty = Math.min(
        0.28,
        Math.max(0, Math.max(state.downside_ema, familyStats.meanDownside)) * (0.8 + ((1 - confidence) * 0.4)),
    );
    overlay *= (1 - downsidePenalty);

    if (familyStats.concentration > META_ALLOCATOR_FAMILY_CONCENTRATION_SOFT_CAP) {
        const room = Math.max(1e-6, 1 - META_ALLOCATOR_FAMILY_CONCENTRATION_SOFT_CAP);
        const concentrationOver = (familyStats.concentration - META_ALLOCATOR_FAMILY_CONCENTRATION_SOFT_CAP) / room;
        overlay *= 1 - Math.min(0.25, concentrationOver * 0.25);
    }

    if (diagnostics.cost_drag_samples >= 6) {
        const excessCostDrag = Math.max(0, diagnostics.avg_cost_drag_bps - META_ALLOCATOR_COST_DRAG_PENALTY_BPS);
        overlay *= 1 - Math.min(0.20, excessCostDrag / 220);
    }

    if (!Number.isFinite(overlay) || overlay <= 0) {
        return META_ALLOCATOR_MIN_OVERLAY;
    }

    return Math.min(META_ALLOCATOR_MAX_OVERLAY, Math.max(META_ALLOCATOR_MIN_OVERLAY, overlay));
}

function computeEffectiveStrategyMultiplier(strategyId: string, state: StrategyPerformance): {
    baseMultiplier: number;
    metaOverlay: number;
    multiplier: number;
} {
    const baseMultiplier = computeStrategyMultiplier(state);
    const metaOverlay = computePortfolioMetaAllocatorOverlay(strategyId, state);
    const combined = baseMultiplier * metaOverlay;
    const multiplier = combined <= 0
        ? 0
        : Math.min(STRATEGY_WEIGHT_CAP, Math.max(STRATEGY_WEIGHT_FLOOR, combined));
    return {
        baseMultiplier,
        metaOverlay,
        multiplier,
    };
}

async function refreshAllocatorMetaOverlays(source: string): Promise<void> {
    if (!META_ALLOCATOR_ENABLED) {
        return;
    }
    let updated = 0;
    const now = Date.now();
    for (const strategyId of STRATEGY_IDS) {
        const state = ensureStrategyPerformanceEntry(strategyId);
        if (state.sample_count <= 0) {
            continue;
        }
        const baseMultiplier = Number.isFinite(state.base_multiplier) && state.base_multiplier > 0
            ? state.base_multiplier
            : computeStrategyMultiplier(state);
        const metaOverlay = computePortfolioMetaAllocatorOverlay(strategyId, state);
        const nextMultiplier = Math.min(
            STRATEGY_WEIGHT_CAP,
            Math.max(STRATEGY_WEIGHT_FLOOR, baseMultiplier * metaOverlay),
        );
        const changed = Math.abs(nextMultiplier - state.multiplier) >= 0.01
            || Math.abs(metaOverlay - state.meta_overlay) >= 0.01;
        if (!changed) {
            continue;
        }
        state.base_multiplier = baseMultiplier;
        state.meta_overlay = metaOverlay;
        state.multiplier = nextMultiplier;
        state.updated_at = now;
        await publishStrategyMultiplier(strategyId, nextMultiplier);
        io.emit('strategy_risk_multiplier_update', {
            strategy: strategyId,
            multiplier: nextMultiplier,
            base_multiplier: baseMultiplier,
            meta_overlay: metaOverlay,
            sample_count: state.sample_count,
            ema_return: state.ema_return,
            downside_ema: state.downside_ema,
            return_var_ema: state.return_var_ema,
            timestamp: now,
            source,
        });
        updated += 1;
    }
    if (updated > 0) {
        touchRuntimeModule('RISK_ALLOCATOR', 'ONLINE', `meta allocator refreshed ${updated} strategies (${source})`);
    }
}

async function publishStrategyMultiplier(strategyId: string, multiplier: number): Promise<void> {
    const bounded = multiplier <= 0
        ? 0
        : Math.min(STRATEGY_WEIGHT_CAP, Math.max(STRATEGY_WEIGHT_FLOOR, multiplier));
    await redisClient.set(strategyRiskMultiplierKey(strategyId), bounded.toFixed(6));
    await redisClient.persist(strategyRiskMultiplierKey(strategyId));
}

async function applyDefaultStrategyStates(force = false): Promise<void> {
    for (const strategyId of STRATEGY_IDS) {
        const enabledKey = `strategy:enabled:${strategyId}`;
        const pauseUntilKey = `risk_guard:paused_until:${strategyId}`;
        const cooldownKey = `risk_guard:cooldown:${strategyId}`;
        const enabledByDefault = !DEFAULT_DISABLED_STRATEGIES.has(strategyId);
        clearRiskGuardBootResumeTimer(strategyId);
        if (force) {
            await redisClient.set(enabledKey, enabledByDefault ? '1' : '0');
            await redisClient.persist(enabledKey);
            if (enabledByDefault) {
                await redisClient.del(pauseUntilKey);
            }
        } else {
            await redisClient.setNX(enabledKey, enabledByDefault ? '1' : '0');
            await redisClient.persist(enabledKey);

            // Re-enable strategies whose Risk Guard cooldown expired during a restart.
            // The in-memory setTimeout-based auto-resume is lost when the backend restarts,
            // so strategies can get stuck disabled forever. Only attempt auto-resume for
            // strategies carrying a Risk Guard pause marker (or legacy cooldown key).
            const currentVal = await redisClient.get(enabledKey);
            const now = Date.now();
            let pauseUntil: number | null = null;
            let legacyCooldownUntil: number | null = null;
            if (currentVal === '0' && enabledByDefault) {
                pauseUntil = asNumber(await redisClient.get(pauseUntilKey));
                legacyCooldownUntil = asNumber(await redisClient.get(cooldownKey));
            }
            const decision = decideBootResumeAction({
                current_enabled: currentVal,
                enabled_by_default: enabledByDefault,
                now_ms: now,
                pause_until_ms: pauseUntil,
                legacy_cooldown_until_ms: legacyCooldownUntil,
            });
            if (decision.action === 'PRESERVE_MANUAL_DISABLE') {
                logger.info(`[Boot] preserving manual disable for ${strategyId}`);
            } else if (decision.action === 'REENABLE_NOW') {
                await redisClient.set(enabledKey, '1');
                await redisClient.del(pauseUntilKey);
                logger.info(`[Boot] re-enabled ${strategyId}: Risk Guard pause marker expired`);
            } else if (decision.action === 'SCHEDULE_RESUME') {
                const remaining = decision.remaining_ms;
                const expectedPauseUntil = decision.expected_pause_until;
                const timer = setTimeout(async () => {
                    riskGuardBootResumeTimers.delete(strategyId);
                    try {
                        const shouldResume = shouldResumeFromBootTimer({
                            current_enabled: await redisClient.get(enabledKey),
                            now_ms: Date.now(),
                            expected_pause_until_ms: expectedPauseUntil,
                            pause_until_ms: asNumber(await redisClient.get(pauseUntilKey)),
                            legacy_cooldown_until_ms: asNumber(await redisClient.get(cooldownKey)),
                        });
                        if (!shouldResume) {
                            return;
                        }
                        await redisClient.set(enabledKey, '1');
                        await redisClient.persist(enabledKey);
                        await redisClient.del(pauseUntilKey);
                        strategyStatus[strategyId] = true;
                        io.emit('strategy_status_update', strategyStatus);
                        logger.info(`[RiskGuard] resumed ${strategyId} after boot-rescheduled cooldown`);
                    } catch (err) {
                        recordSilentCatch('RiskGuard.BootResume', err, {
                            strategy: strategyId,
                            enabled_key: enabledKey,
                        });
                    }
                }, remaining);
                riskGuardBootResumeTimers.set(strategyId, timer);
                logger.info(`[Boot] ${strategyId} still in Risk Guard cooldown, resume in ${Math.ceil(remaining / 1000)}s`);
            }
        }
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
    const effective = computeEffectiveStrategyMultiplier(strategyId, state);
    state.base_multiplier = effective.baseMultiplier;
    state.meta_overlay = effective.metaOverlay;
    state.multiplier = effective.multiplier;
    state.updated_at = Date.now();

    await publishStrategyMultiplier(strategyId, state.multiplier);
    io.emit('strategy_risk_multiplier_update', {
        strategy: strategyId,
        multiplier: state.multiplier,
        base_multiplier: state.base_multiplier,
        meta_overlay: state.meta_overlay,
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
        const raw = asNumber(await redisClient.get(key));
        const multiplier = Number.isFinite(raw) && raw !== null
            ? (raw <= 0 ? 0 : Math.min(STRATEGY_WEIGHT_CAP, Math.max(STRATEGY_WEIGHT_FLOOR, raw)))
            : baseline;
        state.base_multiplier = multiplier;
        state.meta_overlay = 1;
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
    } catch (error) {
        recordSilentCatch('StrategyMetrics.Load', error, { key: STRATEGY_METRICS_KEY });
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
    await clearStrategyExecutionHistory();
    clearEntryFreshnessSloState();
    clearDataIntegrityState();
    clearGovernanceState();
    clearFeatureRegistry();
    clearModelInferenceState();
    clearModelDriftState();
    clearScanFeatureSchemaState();
    executionNettingByMarket.clear();
    await resetRiskGuardStates();
    // Preserve any operator-enabled/disabled strategy toggles by default.
    // For deterministic benchmark runs, callers can force defaults.
    await applyDefaultStrategyStates(options.forceDefaults === true);
    io.emit('strategy_status_update', strategyStatus);
    io.emit('strategy_quality_snapshot', strategyQuality);
    io.emit('strategy_cost_diagnostics_snapshot', strategyCostDiagnosticsPayload());
    io.emit('data_integrity_snapshot', dataIntegrityPayload());
    io.emit('strategy_governance_snapshot', governancePayload());
    io.emit('feature_registry_snapshot', featureRegistrySummary());
    io.emit('scan_feature_schema_snapshot', scanFeatureSchemaPayload());
    io.emit('model_inference_snapshot', modelInferencePayload());
    io.emit('model_drift_update', modelDriftPayload());
    io.emit('entry_freshness_slo_update', entryFreshnessSloPayload());
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
marketDataService.on('ticker', (data: MarketTickerMessage) => {
    io.emit('market_update', data);
    const symbol = asString(asRecord(data)?.symbol) || 'UNKNOWN';
    touchRuntimeModule('MARKET_DATA_PIPELINE', 'ONLINE', `ticker ${symbol}`);
});

redisSubscriber.subscribe('arbitrage:opportunities', (message) => {
    try {
        io.emit('arbitrage_update', JSON.parse(message));
    } catch (error) {
        recordSilentCatch('RedisSub.ArbOpportunities', error, {
            channel: 'arbitrage:opportunities',
            preview: typeof message === 'string' ? message.slice(0, 160) : String(message),
        });
    }
});

redisSubscriber.subscribe('arbitrage:execution', (message) => {
    try {
        const parsedEnvelope = JSON.parse(message);
        const ingress = validateExecutionIngressPayload(parsedEnvelope);
        const parsed = ingress.payload;
        const parsedRecord = asRecord(parsed);
        const executionId = parsedRecord ? ensureExecutionId(parsedRecord) : randomUUID();
        const parsedStrategy = extractExecutionStrategy(parsed);
        const parsedMarketKey = extractExecutionMarketKey(parsed);

        if (!ingress.ok) {
            const mode = normalizeTradingMode(asRecord(parsed)?.mode) || 'PAPER';
            const reason = `[EXECUTION_INGRESS] ${ingress.reason}`;
            const ingressRejectPayload = {
                execution_id: executionId,
                timestamp: Date.now(),
                strategy: parsedStrategy || 'UNKNOWN',
                market_key: parsedMarketKey,
                reason,
                mode,
            };
            pushExecutionTraceEvent(executionId, 'INTELLIGENCE_GATE', ingressRejectPayload, {
                strategy: parsedStrategy,
                market_key: parsedMarketKey,
            });
            io.emit('execution_log', {
                execution_id: executionId,
                timestamp: ingressRejectPayload.timestamp,
                side: 'INGRESS_REJECT',
                market: asString(asRecord(parsed)?.market) || 'Polymarket',
                price: ingressRejectPayload.reason,
                size: ingressRejectPayload.strategy,
                mode,
                details: ingressRejectPayload,
            });
            recordRejectedSignal({
                stage: 'INTELLIGENCE_GATE',
                execution_id: executionId,
                strategy: ingressRejectPayload.strategy,
                market_key: ingressRejectPayload.market_key,
                reason: ingressRejectPayload.reason,
                mode,
                timestamp: ingressRejectPayload.timestamp,
                payload: asRecord(ingressRejectPayload),
            });
            touchRuntimeModule('TRADING_MODE_GUARD', 'DEGRADED', `execution ingress rejected: ${ingress.reason}`);
            return;
        }

        if (parsedRecord) {
            const scanSnapshot = snapshotScanForExecution(parsedStrategy, parsedMarketKey);
            const inferredFeatureRowId = featureRowIdForScan(scanSnapshot);
            if (!asString(parsedRecord.execution_id) && !asString(parsedRecord.executionId)) {
                parsedRecord.execution_id = executionId;
            }
            if (!asString(parsedRecord.strategy) && parsedStrategy) {
                parsedRecord.strategy = parsedStrategy;
            }
            if (!asString(parsedRecord.market_key) && parsedMarketKey) {
                parsedRecord.market_key = parsedMarketKey;
            }
            if (!asString(parsedRecord.feature_row_id) && inferredFeatureRowId) {
                parsedRecord.feature_row_id = inferredFeatureRowId;
            }
            parsedRecord.execution_ingress_signed = ingress.signed;
        }
        if (parsedStrategy && parsedRecord) {
            pushStrategyExecutionHistory(parsedStrategy, parsedRecord);
        }
        pushExecutionTraceEvent(executionId, 'EXECUTION_EVENT', parsed, {
            strategy: parsedStrategy,
            market_key: parsedMarketKey,
        });
        io.emit('execution_log', parsed);

        void (async () => {
            try {
                if (executionHaltState.halted) {
                    const mode = await getTradingMode();
                    const reason = executionHaltState.reason || 'Execution halt active';
                    const haltPayload = {
                        execution_id: executionId,
                        timestamp: Date.now(),
                        strategy: parsedStrategy || 'UNKNOWN',
                        market_key: parsedMarketKey,
                        reason: `[KILL_SWITCH] ${reason}`,
                        halted: true,
                        source: executionHaltState.source,
                        halted_at: executionHaltState.updated_at,
                    };
                    pushExecutionTraceEvent(executionId, 'INTELLIGENCE_GATE', haltPayload, {
                        strategy: parsedStrategy,
                        market_key: parsedMarketKey,
                    });
                    io.emit('execution_log', {
                        execution_id: executionId,
                        timestamp: haltPayload.timestamp,
                        side: 'HALT_BLOCK',
                        market: asString(asRecord(parsed)?.market) || 'Polymarket',
                        price: haltPayload.reason,
                        size: haltPayload.strategy,
                        mode,
                        details: haltPayload,
                    });
                    recordRejectedSignal({
                        stage: 'LIVE_EXECUTION',
                        execution_id: executionId,
                        strategy: haltPayload.strategy,
                        market_key: haltPayload.market_key,
                        reason: haltPayload.reason,
                        mode,
                        timestamp: haltPayload.timestamp,
                        payload: asRecord(haltPayload),
                    });
                    touchRuntimeModule('TRADING_MODE_GUARD', 'DEGRADED', `execution halted: ${reason}`);
                    return;
                }
                if (parsedStrategy && strategyStatus[parsedStrategy] === false) {
                    const mode = await getTradingMode();
                    const disabledPayload = {
                        execution_id: executionId,
                        timestamp: Date.now(),
                        strategy: parsedStrategy,
                        market_key: parsedMarketKey,
                        reason: `[STRATEGY_DISABLED] ${parsedStrategy} is disabled`,
                        mode,
                    };
                    pushExecutionTraceEvent(executionId, 'INTELLIGENCE_GATE', disabledPayload, {
                        strategy: parsedStrategy,
                        market_key: parsedMarketKey,
                    });
                    io.emit('execution_log', {
                        execution_id: executionId,
                        timestamp: disabledPayload.timestamp,
                        side: 'STRATEGY_DISABLED_BLOCK',
                        market: asString(asRecord(parsed)?.market) || 'Polymarket',
                        price: disabledPayload.reason,
                        size: disabledPayload.strategy,
                        mode,
                        details: disabledPayload,
                    });
                    recordRejectedSignal({
                        stage: 'INTELLIGENCE_GATE',
                        execution_id: executionId,
                        strategy: disabledPayload.strategy,
                        market_key: disabledPayload.market_key,
                        reason: disabledPayload.reason,
                        mode,
                        timestamp: disabledPayload.timestamp,
                        payload: asRecord(disabledPayload),
                    });
                    touchRuntimeModule('INTELLIGENCE_GATE', 'DEGRADED', `${parsedStrategy} disabled`);
                    return;
                }

                const preparation = prepareExecutionPayloadForDispatch(parsed, parsedStrategy, parsedMarketKey);
                if (!preparation.ok) {
                    const mode = await getTradingMode();
                    const stage = preparation.stage || 'SHORTFALL';
                    const strategy = parsedStrategy || extractExecutionStrategy(parsed) || 'UNKNOWN';
                    const marketKey = parsedMarketKey || extractExecutionMarketKey(parsed);
                    const reason = `[${stage}] ${preparation.reason || 'execution blocked by preparation engine'}`;
                    const prepBlockPayload = {
                        execution_id: executionId,
                        timestamp: Date.now(),
                        strategy,
                        market_key: marketKey,
                        reason,
                        mode,
                        details: preparation.details || null,
                    };
                    pushExecutionTraceEvent(executionId, 'INTELLIGENCE_GATE', prepBlockPayload, {
                        strategy,
                        market_key: marketKey,
                    });
                    io.emit('execution_log', {
                        execution_id: executionId,
                        timestamp: prepBlockPayload.timestamp,
                        side: `${stage}_BLOCK`,
                        market: asString(asRecord(parsed)?.market) || 'Polymarket',
                        price: prepBlockPayload.reason,
                        size: prepBlockPayload.strategy,
                        mode,
                        details: prepBlockPayload,
                    });
                    recordRejectedSignal({
                        stage: 'INTELLIGENCE_GATE',
                        execution_id: executionId,
                        strategy: prepBlockPayload.strategy,
                        market_key: prepBlockPayload.market_key,
                        reason: prepBlockPayload.reason,
                        mode,
                        timestamp: prepBlockPayload.timestamp,
                        payload: asRecord(prepBlockPayload),
                    });
                    touchRuntimeModule('EXECUTION_PREFLIGHT', 'DEGRADED', `${stage} blocked ${strategy}`);
                    return;
                }

                const dispatchPayload = preparation.payload;
                const dispatchStrategy = parsedStrategy || extractExecutionStrategy(dispatchPayload);
                const dispatchMarketKey = parsedMarketKey || extractExecutionMarketKey(dispatchPayload);
                if (preparation.adjusted) {
                    const prepAdjustedPayload = {
                        execution_id: executionId,
                        timestamp: Date.now(),
                        strategy: dispatchStrategy || 'UNKNOWN',
                        market_key: dispatchMarketKey,
                        reason: '[EXECUTION_PREP] payload adjusted by shortfall/netting engine',
                        details: preparation.details || {},
                    };
                    pushExecutionTraceEvent(executionId, 'INTELLIGENCE_GATE', prepAdjustedPayload, {
                        strategy: dispatchStrategy,
                        market_key: dispatchMarketKey,
                    });
                    io.emit('execution_log', {
                        execution_id: executionId,
                        timestamp: prepAdjustedPayload.timestamp,
                        side: 'EXECUTION_PREP',
                        market: asString(asRecord(dispatchPayload)?.market) || 'Polymarket',
                        price: prepAdjustedPayload.reason,
                        size: prepAdjustedPayload.strategy,
                        mode: 'LIVE_GUARD',
                        details: prepAdjustedPayload,
                    });
                    touchRuntimeModule(
                        'EXECUTION_PREFLIGHT',
                        'ONLINE',
                        `execution prep adjusted ${dispatchStrategy || 'UNKNOWN'}`,
                    );
                }
                await processArbitrageExecutionWorker(
                    {
                        parsed: dispatchPayload,
                        executionId,
                        parsedStrategy: dispatchStrategy,
                        parsedMarketKey: dispatchMarketKey,
                    },
                    {
                        getTradingMode,
                        evaluateIntelligenceGate,
                        evaluateModelProbabilityGate,
                        preflightFromExecution: polymarketPreflight.preflightFromExecution.bind(polymarketPreflight),
                        executeFromExecution: async (payload, tradingMode) => {
                            const live = await polymarketPreflight.executeFromExecution(payload, tradingMode);
                            if (live && live.ok && tradingMode === 'LIVE' && !live.dryRun) {
                                upsertExecutionNettingStateFromAcceptedPayload(
                                    payload,
                                    dispatchStrategy || extractExecutionStrategy(payload),
                                    dispatchMarketKey || extractExecutionMarketKey(payload),
                                );
                            }
                            return live;
                        },
                        registerAtomicExecution: settlementService.registerAtomicExecution.bind(settlementService),
                        emitSettlementEvents,
                        pushExecutionTraceEvent,
                        touchRuntimeModule,
                        emit: (event, payload) => {
                            io.emit(event, payload);
                        },
                        preflightToExecutionLog: PolymarketPreflightService.toExecutionLog,
                        liveToExecutionLog: PolymarketPreflightService.toLiveExecutionLog,
                        marketLabelFromPayload: (payload) => asString(asRecord(payload)?.market) || 'Polymarket',
                        annotateExecutionId: (target, id) => {
                            (target as Record<string, unknown>).execution_id = id;
                        },
                        recordRejectedSignal,
                    },
                );
            } catch (workerError) {
                recordSilentCatch('RedisSub.ArbExecutionWorker', workerError, {
                    channel: 'arbitrage:execution',
                    execution_id: executionId,
                    strategy: parsedStrategy || 'UNKNOWN',
                    market_key: parsedMarketKey || 'UNKNOWN',
                });
            }
        })();
    } catch (error) {
        recordSilentCatch('RedisSub.ArbExecutionParse', error, {
            channel: 'arbitrage:execution',
            preview: typeof message === 'string' ? message.slice(0, 160) : String(message),
        });
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
        const mode = normalizeTradingMode(parsed?.mode) || 'PAPER';
        const sample = mode === 'PAPER' ? parseTradeSample(parsed) : null;
        const rejection = sample?.execution_id ? rejectedSignalsByExecutionId.get(sample.execution_id) || null : null;
        if (mode === 'PAPER' && rejection) {
            const overlapPayload = {
                execution_id: sample?.execution_id || executionId,
                timestamp: Date.now(),
                strategy: sample?.strategy || pnlStrategy || 'UNKNOWN',
                market_key: sample?.market_key || pnlMarketKey,
                reason: 'paper pnl matched rejected execution id; recording for ledger parity',
                rejected_stage: rejection.stage,
                rejected_reason: rejection.reason,
                rejected_timestamp: rejection.timestamp,
                mode,
            };
            io.emit('strategy_pnl_rejection_overlap', overlapPayload);
            io.emit('execution_log', {
                execution_id: overlapPayload.execution_id,
                timestamp: overlapPayload.timestamp,
                side: 'PNL_REJECTION_OVERLAP',
                market: asString(asRecord(parsed)?.market) || 'Polymarket',
                price: overlapPayload.reason,
                size: overlapPayload.strategy,
                mode,
                details: overlapPayload,
            });
            touchRuntimeModule('PNL_LEDGER', 'ONLINE', `${overlapPayload.strategy} pnl recorded with rejection overlap (${rejection.stage})`);
        }

        strategyTradeRecorder.record(parsed);
        touchRuntimeModule('PNL_LEDGER', 'ONLINE', `${pnlStrategy || 'UNKNOWN'} pnl event recorded`);

        if (mode === 'PAPER' && sample) {
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
            fireAndForget('StrategyMetrics.Persist', persistStrategyMetrics(), {
                source: 'strategy:pnl',
                strategy,
            });

            const notional = asNumber(parsed?.notional);
            fireAndForget('RiskAllocator.Update', updateStrategyAllocator(strategy, pnl, notional), { strategy });
            fireAndForget('RiskGuard.Process', processRiskGuard(strategy, pnl, parsed as Record<string, unknown>), { strategy });
            fireAndForget('Vault.Sweep', sweepProfitsToVault(), { strategy });
        }
    } catch (error) {
        recordSilentCatch('RedisSub.StrategyPnl', error, {
            channel: 'strategy:pnl',
            preview: typeof message === 'string' ? message.slice(0, 160) : String(message),
        });
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
        const scanContract = validateArbitrageScanContract(parsedRecord);
        if (!scanContract.ok) {
            const strategyHint = asString(parsedRecord.strategy)?.toUpperCase() || 'UNKNOWN';
            const marketHint = extractScanMarketKey(parsedRecord) || `${strategyHint.toLowerCase()}:unknown`;
            const reason = `scan schema drift: ${scanContract.error}`;
            const alert = recordScanSchemaDrift(strategyHint, marketHint, reason, scanContract.issues);
            if (alert) {
                io.emit('data_integrity_alert', alert);
            }
            io.emit('data_integrity_update', {
                strategy: strategyHint,
                ...ensureStrategyDataIntegrityEntry(strategyHint),
            });
            io.emit('scan_ingest_alert', {
                severity: 'CRITICAL',
                strategy: strategyHint,
                market_key: marketHint,
                reason,
                issues: scanContract.issues,
                timestamp: Date.now(),
            });
            touchRuntimeModule('SCAN_INGEST', 'DEGRADED', `${strategyHint} schema drift: ${scanContract.error}`);
            return;
        }

        const normalizedScan = scanContract.value;
        const strategy = normalizedScan.strategy;
        const timestamp = normalizedScan.timestamp;
        const marketKey = normalizeMarketKey(normalizedScan.market_key)
            || extractScanMarketKey(parsedRecord)
            || strategy.toLowerCase();
        const rawSignalType = normalizedScan.signal_type || asString(parsedRecord.signal_type);
        const rawUnit = normalizedScan.unit || asString(parsedRecord.unit);
        const metaExtraction = extractScanMetaFeatures(normalizedScan.meta ?? parsedRecord.meta);
        const metaFeatures = metaExtraction.features;
        const inferred = inferScanDescriptor(strategy, rawSignalType, rawUnit);
        const metricFamily = parseMetricFamily(normalizedScan.metric_family ?? parsedRecord.metric_family) || inferred.metric_family;
        const directionality = parseMetricDirection(normalizedScan.directionality ?? parsedRecord.directionality) || inferred.directionality;
        const comparableGroup = asString(normalizedScan.comparable_group ?? parsedRecord.comparable_group) || inferred.comparable_group;
        const scanState: StrategyScanState = {
            strategy,
            symbol: normalizedScan.symbol || asString(parsedRecord.symbol) || strategy,
            market_key: marketKey,
            timestamp,
            passes_threshold: normalizedScan.passes_threshold,
            score: normalizedScan.score,
            threshold: normalizedScan.threshold,
            reason: normalizedScan.reason,
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
        updateScanFeatureSchemaGovernance(scanState, metaExtraction);
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
        requestCrossHorizonRefresh();
        const droppedMetaHint = metaExtraction.dropped_count > 0
            ? ` dropped_meta=${metaExtraction.dropped_count}`
            : '';
        touchRuntimeModule('SCAN_INGEST', 'ONLINE', `${strategy} ${scanState.passes_threshold ? 'PASS' : 'HOLD'} ${scanState.symbol}${droppedMetaHint}`);
        io.emit('intelligence_update', scanState);
        io.emit('scanner_update', parsed);
    } catch (error) {
        const now = Date.now();
        if (now - lastScannerParseErrorLogMs >= 30_000) {
            lastScannerParseErrorLogMs = now;
            const preview = typeof message === 'string' ? message.slice(0, 160) : String(message);
            recordSilentCatch('RedisSub.ScannerUpdate', error, {
                channel: 'arbitrage:scan',
                preview,
            });
        }
    }
});

// Persist ML features to Redis keys so Rust scanners can read RSI/SMA for momentum filtering.
redisSubscriber.subscribe('ml:features', (message) => {
    try {
        const parsed = JSON.parse(message) as Record<string, unknown>;
        const symbol = parsed?.symbol;
        if (typeof symbol === 'string' && symbol.length > 0) {
            const normalized = {
                symbol,
                rsi_14: asNumber(parsed.rsi_14),
                sma_20: asNumber(parsed.sma_20),
                price: asNumber(parsed.price),
                returns: asNumber(parsed.returns),
                ml_prob_up: asNumber(parsed.ml_prob_up),
                ml_prob_down: asNumber(parsed.ml_prob_down),
                ml_signal_edge: asNumber(parsed.ml_signal_edge),
                ml_signal_confidence: asNumber(parsed.ml_signal_confidence),
                ml_signal_direction: asString(parsed.ml_signal_direction),
                ml_model: asString(parsed.ml_model),
                ml_model_ready: typeof parsed.ml_model_ready === 'boolean' ? parsed.ml_model_ready : null,
                ml_model_samples: asNumber(parsed.ml_model_samples),
                ml_model_horizon_ms: asNumber(parsed.ml_model_horizon_ms),
                ml_model_last_train_ts: asNumber(parsed.ml_model_last_train_ts),
                ml_model_age_ms: asNumber(parsed.ml_model_age_ms),
                ml_model_eval_logloss: asNumber(parsed.ml_model_eval_logloss),
                ml_model_eval_auc: asNumber(parsed.ml_model_eval_auc),
                ml_model_eval_accuracy: asNumber(parsed.ml_model_eval_accuracy),
                ml_model_eval_brier: asNumber(parsed.ml_model_eval_brier),
                micro_spread: asNumber(parsed.micro_spread),
                micro_book_age_ms: asNumber(parsed.micro_book_age_ms),
                micro_edge_to_spread_ratio: asNumber(parsed.micro_edge_to_spread_ratio),
                micro_parity_deviation: asNumber(parsed.micro_parity_deviation),
                micro_obi: asNumber(parsed.micro_obi),
                micro_buy_pressure: asNumber(parsed.micro_buy_pressure),
                micro_uptick_ratio: asNumber(parsed.micro_uptick_ratio),
                micro_cluster_confidence: asNumber(parsed.micro_cluster_confidence),
                micro_flow_acceleration: asNumber(parsed.micro_flow_acceleration),
                micro_dynamic_cost_rate: asNumber(parsed.micro_dynamic_cost_rate),
                micro_momentum_sigma: asNumber(parsed.micro_momentum_sigma),
                micro_data_fresh: asNumber(parsed.micro_data_fresh),
                micro_data_age_ms: asNumber(parsed.micro_data_age_ms),
                timestamp: normalizeTimestampMs(parsed.timestamp),
            };
            const key = `ml:features:latest:${symbol}`;
            fireAndForget(
                'RedisSub.MLFeaturesPersist',
                redisClient.set(key, JSON.stringify(normalized), { EX: 120 }),
                { key, symbol },
            ); // expire after 2 min if ML service stops
            io.emit('ml_features_update', normalized);
        }
    } catch (error) {
        recordSilentCatch('RedisSub.MLFeatures', error, {
            channel: 'ml:features',
            preview: typeof message === 'string' ? message.slice(0, 160) : String(message),
        });
    }
});

redisSubscriber.subscribe('system:heartbeat', (msg) => {
    try {
        const { id, timestamp } = JSON.parse(msg);
        if (typeof id === 'string') {
            heartbeats[id] = Number(timestamp) || Date.now();
        }
    } catch (error) {
        recordSilentCatch('RedisSub.SystemHeartbeat', error, {
            channel: 'system:heartbeat',
            preview: typeof msg === 'string' ? msg.slice(0, 160) : String(msg),
        });
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
    } catch (error) {
        recordSilentCatch('RedisSub.TradingMode', error, {
            channel: 'system:trading_mode',
            preview: typeof msg === 'string' ? msg.slice(0, 160) : String(msg),
        });
    }
});

redisSubscriber.subscribe('system:execution_halt', (msg) => {
    try {
        const parsed = normalizeExecutionHaltState(JSON.parse(msg));
        executionHaltState = parsed;
        io.emit('execution_halt_update', parsed);
        touchRuntimeModule(
            'TRADING_MODE_GUARD',
            parsed.halted ? 'DEGRADED' : 'ONLINE',
            parsed.halted
                ? `execution halt active: ${parsed.reason || 'unspecified'}`
                : 'execution halt cleared',
        );
    } catch (error) {
        recordSilentCatch('RedisSub.ExecutionHalt', error, {
            channel: 'system:execution_halt',
            preview: typeof msg === 'string' ? msg.slice(0, 160) : String(msg),
        });
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
        fireAndForget('StrategyExecutionHistory.Clear', clearStrategyExecutionHistory(), {
            source: 'system:simulation_reset',
        });
        clearEntryFreshnessSloState();
        clearDataIntegrityState();
        clearGovernanceState();
        clearFeatureRegistry();
        clearModelInferenceState();
        clearModelDriftState();
        clearScanFeatureSchemaState();
        executionNettingByMarket.clear();
        fireAndForget('RiskGuard.ResetState', resetRiskGuardStates(), {
            source: 'system:simulation_reset',
        });
        fireAndForget('StrategyMetrics.Persist', persistStrategyMetrics(), {
            source: 'system:simulation_reset',
        });
        io.emit('strategy_metrics_update', strategyMetrics);
        io.emit('strategy_cost_diagnostics_snapshot', strategyCostDiagnosticsPayload());
        io.emit('data_integrity_snapshot', dataIntegrityPayload());
        io.emit('strategy_governance_snapshot', governancePayload());
        io.emit('feature_registry_snapshot', featureRegistrySummary());
        io.emit('scan_feature_schema_snapshot', scanFeatureSchemaPayload());
        io.emit('model_inference_snapshot', modelInferencePayload());
        io.emit('model_drift_update', modelDriftPayload());
        io.emit('entry_freshness_slo_update', entryFreshnessSloPayload());
        requestMetaControllerRefresh();
        fireAndForget('CrossHorizonRouter.Refresh', runCrossHorizonRefreshSafely(), {
            source: 'system:simulation_reset',
        });
        fireAndForget('MLPipeline.Refresh', refreshMlPipelineStatus(), { source: 'system:simulation_reset' });
        fireAndForget('Governance.Cycle', runStrategyGovernanceCycle(), { source: 'system:simulation_reset' });
        fireAndForget('LedgerHealth.Refresh', refreshLedgerHealth(), { source: 'system:simulation_reset' });
        io.emit('strategy_risk_multiplier_snapshot', Object.fromEntries(
            STRATEGY_IDS.map((strategyId) => [strategyId, ensureStrategyPerformanceEntry(strategyId)]),
        ));
    } catch (error) {
        recordSilentCatch('RedisSub.SimulationReset', error, {
            channel: 'system:simulation_reset',
            preview: typeof msg === 'string' ? msg.slice(0, 160) : String(msg),
        });
    }
});

io.on('connection', async (socket) => {
    logger.info(`[Socket] client connected id=${socket.id}`);
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
    socket.emit('scan_feature_schema_snapshot', scanFeatureSchemaPayload());
    socket.emit('risk_config_update', await getRiskConfig());
    socket.emit('trading_mode_update', {
        mode: await getTradingMode(),
        timestamp: Date.now(),
        live_order_posting_enabled: isLiveOrderPostingEnabled(),
    });
    socket.emit('execution_halt_update', executionHaltState);
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
    socket.emit('silent_catch_telemetry_snapshot', silentCatchTelemetryPayload());
    socket.emit('ledger_health_update', ledgerHealthState);
    socket.emit('pnl_parity_update', pnlParityState);
    socket.emit('meta_controller_update', metaControllerPayload());
    socket.emit('cross_horizon_router_update', crossHorizonRouterState);
    socket.emit('ml_pipeline_status_update', mlPipelineStatus);
    socket.emit('model_inference_snapshot', modelInferencePayload());
    socket.emit('model_drift_update', modelDriftPayload());
    socket.emit('model_gate_calibration_update', modelGateCalibrationPayload());
    socket.emit('rejected_signal_snapshot', rejectedSignalSnapshot());
    socket.emit('entry_freshness_slo_update', entryFreshnessSloPayload());

    socket.on('disconnect', () => {
        logger.info(`[Socket] client disconnected id=${socket.id}`);
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
        logger.info(`[Control] config updated payload=${JSON.stringify(data).slice(0, 200)}`);
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
        logger.info(`[Control] risk config updated payload=${JSON.stringify(normalized)}`);
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
            const state = getRiskGuardState(id);
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

    socket.on('request_silent_catch_telemetry', () => {
        socket.emit('silent_catch_telemetry_snapshot', silentCatchTelemetryPayload());
    });

    socket.on('request_rejected_signals', (payload?: { limit?: unknown; strategy?: unknown }) => {
        const payloadRecord = asRecord(payload);
        const requestedLimit = asNumber(payloadRecord?.limit);
        const limit = Number.isFinite(requestedLimit) ? requestedLimit as number : 200;
        const strategy = normalizedStrategyFilter(payloadRecord?.strategy);
        socket.emit('rejected_signal_snapshot', rejectedSignalSnapshot(limit, strategy));
    });

    socket.on('request_model_gate_calibration', () => {
        socket.emit('model_gate_calibration_update', modelGateCalibrationPayload());
    });

    socket.on('request_trading_mode', async () => {
        socket.emit('trading_mode_update', {
            mode: await getTradingMode(),
            timestamp: Date.now(),
            live_order_posting_enabled: isLiveOrderPostingEnabled(),
        });
    });

    socket.on('request_execution_halt', () => {
        socket.emit('execution_halt_update', executionHaltState);
    });

    socket.on('set_trading_mode', async (payload: Partial<TradingModePayload>) => {
        if (!enforceSocketControlAuth(socket, 'set_trading_mode', socketControlAuthorized)) {
            return;
        }
        const mode = normalizeTradingMode(payload?.mode);
        const readiness = mode === 'LIVE' ? await isLiveReady() : null;
        const validation = validateTradingModeChangeRequest({
            mode: payload?.mode,
            confirmation: payload?.confirmation,
            execution_halted: executionHaltState.halted,
            live_readiness: readiness,
        });
        if (!validation.ok) {
            socket.emit('trading_mode_error', {
                message: validation.message,
                failures: validation.failures,
            });
            return;
        }

        await setTradingMode(validation.value.mode);
    });

    socket.on('reset_simulation', async (payload: Partial<SimulationResetPayload>) => {
        if (!enforceSocketControlAuth(socket, 'reset_simulation', socketControlAuthorized)) {
            return;
        }
        const validation = validateSimulationResetRequest({
            current_mode: await getTradingMode(),
            confirmation: payload?.confirmation,
            bankroll: payload?.bankroll,
            force_defaults: payload?.force_defaults,
            normalize_bankroll: normalizeResetBankroll,
        });
        if (!validation.ok) {
            socket.emit('simulation_reset_error', { message: validation.message });
            return;
        }

        await resetSimulationState(validation.value.bankroll, {
            forceDefaults: validation.value.force_defaults,
        });
    });

    socket.on('toggle_strategy', async (payload: StrategyTogglePayload) => {
        if (!enforceSocketControlAuth(socket, 'toggle_strategy', socketControlAuthorized)) {
            return;
        }
        const validation = validateStrategyToggleRequest({
            payload,
            strategy_ids: STRATEGY_IDS,
        });
        if (!validation.ok) {
            socket.emit('strategy_control_error', { message: 'Invalid strategy toggle payload' });
            return;
        }

        await clearStrategyPause(validation.value.id);
        clearRiskGuardBootResumeTimer(validation.value.id);
        strategyStatus[validation.value.id] = validation.value.active;

        await redisClient.set(`strategy:enabled:${validation.value.id}`, validation.value.active ? '1' : '0');
        await redisClient.persist(`strategy:enabled:${validation.value.id}`);
        await redisClient.publish('strategy:control', JSON.stringify({
            id: validation.value.id,
            active: validation.value.active,
            timestamp: Date.now(),
        }));

        io.emit('strategy_status_update', strategyStatus);
    });

    socket.on('set_execution_halt', async (payload: { halted?: unknown; reason?: unknown }) => {
        if (!enforceSocketControlAuth(socket, 'set_execution_halt', socketControlAuthorized)) {
            return;
        }
        if (typeof payload?.halted !== 'boolean') {
            socket.emit('execution_halt_error', { message: 'Invalid execution halt payload' });
            return;
        }
        const next = await setExecutionHaltState(
            payload.halted,
            sanitizeExecutionHaltReason(payload.reason),
            'socket_control',
        );
        socket.emit('execution_halt_update', next);
    });
});

app.get('/api/arb/bots', (_req, res) => {
    const now = Date.now();
    const bots = STRATEGY_IDS.map((id) => ({
        id,
        active: Boolean(strategyStatus[id]),
    }));
    const scannerHeartbeat = scannerHeartbeatSnapshot(now);

    res.json({
        bots,
        scanner: {
            alive: scannerHeartbeat.alive,
            last_heartbeat_ms: scannerHeartbeat.last_heartbeat_ms,
            monitored_ids: scannerHeartbeat.monitored_ids,
            stale_ids: scannerHeartbeat.stale_ids,
            missing_ids: scannerHeartbeat.missing_ids,
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
    const scannerHeartbeat = scannerHeartbeatSnapshot(now);
    const settlementSnapshot = settlementService.getSnapshot();
    const trackedSettlements = settlementSnapshot.positions.filter((position) => position.status !== 'REDEEMED');
    const redeemableSettlements = settlementSnapshot.positions.filter((position) => position.status === 'REDEEMABLE');
    const rejectedPersistedRows = await rejectedSignalRecorder.countRows().catch((error) => {
        recordSilentCatch('RejectedSignal.CountRows', error, { path: rejectedSignalRecorder.getPath() });
        return rejectedSignalHistory.length;
    });
    const rejectedByStage = rejectedSignalHistory.reduce<Record<string, number>>((acc, entry) => {
        acc[entry.stage] = (acc[entry.stage] ?? 0) + 1;
        return acc;
    }, {});

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
        scanner_last_heartbeat_ms: scannerHeartbeat.last_heartbeat_ms,
        scanner_alive: scannerHeartbeat.alive,
        simulation_ledger: {
            cash: Math.round(ledger.cash * 100) / 100,
            reserved: Math.round(ledger.reserved * 100) / 100,
            realized_pnl: Math.round(ledger.realized_pnl * 100) / 100,
            equity: Math.round(ledger.equity * 100) / 100,
            utilization_pct: Math.round(ledger.utilization_pct * 10) / 10,
        },
        risk: await getRiskConfig(),
        trading_mode: await getTradingMode(),
        execution_halt: executionHaltState,
        live_order_posting_enabled: isLiveOrderPostingEnabled(),
        intelligence_gate_enabled: INTELLIGENCE_GATE_ENABLED,
        model_probability_gate: {
            enabled: MODEL_PROBABILITY_GATE_ENABLED,
            enforce_paper: MODEL_PROBABILITY_GATE_ENFORCE_PAPER,
            enforce_live: MODEL_PROBABILITY_GATE_ENFORCE_LIVE,
            min_probability: MODEL_PROBABILITY_GATE_MIN_PROB,
            active_probability: activeModelProbabilityGate(),
            tuner_enabled: MODEL_PROBABILITY_GATE_TUNER_ENABLED,
            tuner_advisory_only: MODEL_PROBABILITY_GATE_TUNER_ADVISORY_ONLY,
            require_model: MODEL_PROBABILITY_GATE_REQUIRE_MODEL,
            disable_on_drift: MODEL_PROBABILITY_GATE_DISABLE_ON_DRIFT,
            drift_disabled_until: modelDriftState.gate_disabled_until,
            currently_enforcing: modelDriftState.gate_enforcing,
        },
        silent_catch_telemetry: silentCatchTelemetryPayload(),
        build_runtime: runtimeStatusPayload(),
        settlements: {
            tracked: trackedSettlements.length,
            redeemable: redeemableSettlements.length,
            auto_redeem_enabled: settlementSnapshot.autoRedeemEnabled,
            readiness: settlementService.getReadinessSnapshot(),
        },
        rejected_signals: {
            tracked: rejectedSignalHistory.length,
            persisted_rows: rejectedPersistedRows,
            log_path: rejectedSignalRecorder.getPath(),
            by_stage: rejectedByStage,
        },
        ledger_health: ledgerHealthState,
        pnl_parity: pnlParityState,
        meta_controller: metaControllerPayload(),
        ml_pipeline: mlPipelineStatus,
        model_inference: modelInferencePayload(),
        model_drift: modelDriftPayload(),
        model_gate_calibration: modelGateCalibrationPayload(),
        cross_horizon_router: crossHorizonRouterState,
        execution_netting: {
            enabled: EXECUTION_NETTING_ENABLED,
            ttl_ms: EXECUTION_NETTING_TTL_MS,
            market_cap_usd: EXECUTION_NETTING_MARKET_CAP_USD,
            entries: executionNettingSnapshot(),
        },
        strategy_allocator: Object.fromEntries(
            STRATEGY_IDS.map((strategyId) => {
                const perf = ensureStrategyPerformanceEntry(strategyId);
                return [strategyId, {
                    multiplier: Math.round(perf.multiplier * 1000) / 1000,
                    base_multiplier: Math.round(perf.base_multiplier * 1000) / 1000,
                    meta_overlay: Math.round(perf.meta_overlay * 1000) / 1000,
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
        entry_freshness_slo: entryFreshnessSloPayload(),
        strategy_governance: governancePayload(),
        feature_registry: featureRegistrySummary(),
        scan_feature_schema: scanFeatureSchemaPayload(),
    });
});

app.get('/api/arb/rejected-signals', async (req, res) => {
    const rawLimit = typeof req.query.limit === 'string' ? Number(req.query.limit) : 200;
    const limit = Number.isFinite(rawLimit) ? Math.max(1, Math.min(REJECTED_SIGNAL_RETENTION, Math.floor(rawLimit))) : 200;
    const stage = typeof req.query.stage === 'string' ? req.query.stage.trim().toUpperCase() : '';
    const strategy = normalizedStrategyFilter(typeof req.query.strategy === 'string' ? req.query.strategy : null);
    const source = typeof req.query.source === 'string' ? req.query.source.trim().toLowerCase() : 'memory';
    const fromFile = source === 'file';
    const persistedRows = await rejectedSignalRecorder.countRows().catch((error) => {
        recordSilentCatch('RejectedSignal.CountRows', error, { path: rejectedSignalRecorder.getPath() });
        return rejectedSignalHistory.length;
    });
    const base = fromFile
        ? await rejectedSignalRecorder.readRecent(REJECTED_SIGNAL_RETENTION).catch((error) => {
            recordSilentCatch('RejectedSignal.ReadRecent', error, { path: rejectedSignalRecorder.getPath() });
            return [] as RejectedSignalRecord[];
        })
        : rejectedSignalSnapshot(REJECTED_SIGNAL_RETENTION).reverse();
    const filteredByStage = stage ? base.filter((entry) => entry.stage === stage) : base;
    const filtered = strategy
        ? filteredByStage.filter((entry) => normalizedStrategyFilter(entry.strategy) === strategy)
        : filteredByStage;
    const entries = filtered.slice(Math.max(0, filtered.length - limit)).reverse();
    res.json({
        limit,
        stage: stage || null,
        strategy: strategy || null,
        source: fromFile ? 'file' : 'memory',
        log_path: rejectedSignalRecorder.getPath(),
        persisted_rows: persistedRows,
        count: entries.length,
        entries,
    });
});

app.get('/api/arb/entry-freshness-slo', (_req, res) => {
    res.json(entryFreshnessSloPayload());
});

app.get('/api/arb/model-gate-calibration', (_req, res) => {
    res.json(modelGateCalibrationPayload());
});

app.post('/api/arb/model-gate-calibration/recompute', requireControlPlaneAuth, async (_req, res) => {
    await runModelProbabilityGateCalibration();
    res.json({
        ok: true,
        calibration: modelGateCalibrationPayload(),
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
        cross_horizon_router: crossHorizonRouterState,
        scan_feature_schema: scanFeatureSchemaPayload(),
    });
});

app.get('/api/arb/cross-horizon-router', (_req, res) => {
    res.json(crossHorizonRouterState);
});

app.get('/api/arb/data-integrity', (_req, res) => {
    res.json(dataIntegrityPayload());
});

app.get('/api/arb/scan-feature-schema', (_req, res) => {
    res.json(scanFeatureSchemaPayload());
});

app.get('/api/arb/governance', (_req, res) => {
    res.json(governancePayload());
});

app.get('/api/arb/ledger-health', (_req, res) => {
    res.json(ledgerHealthState);
});

app.get('/api/arb/pnl-parity', (_req, res) => {
    res.json(pnlParityState);
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

app.get('/api/ml/model-inference', (req, res) => {
    const rawLimit = typeof req.query.limit === 'string' ? Number(req.query.limit) : NaN;
    const limit = Number.isFinite(rawLimit)
        ? Math.max(1, Math.min(MODEL_INFERENCE_MAX_TRACKED_ROWS, Math.floor(rawLimit)))
        : MODEL_INFERENCE_PAYLOAD_MAX_ROWS;
    res.json(modelInferencePayload(limit));
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

app.get('/api/arb/strategy-executions', (req, res) => {
    const strategy = typeof req.query.strategy === 'string' ? req.query.strategy.trim().toUpperCase() : '';
    const rawLimit = typeof req.query.limit === 'string' ? Number(req.query.limit) : 200;
    const limit = Number.isFinite(rawLimit) ? Math.max(1, Math.min(2000, Math.floor(rawLimit))) : 200;
    if (!strategy) {
        res.status(400).json({ error: 'Missing strategy query parameter' });
        return;
    }
    const history = strategyExecutionHistory.get(strategy) || [];
    const entries = history.slice(Math.max(0, history.length - limit)).reverse();
    res.json({ strategy, entries });
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
        const content = await fs.readFile(filePath, 'utf8').catch((error) => {
            const errorCode = asString(asRecord(error)?.code);
            if (errorCode !== 'ENOENT') {
                recordSilentCatch('StrategyTrades.ReadFile', error, { path: filePath });
            }
            return '';
        });
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

        type StrategyTradeHistoryEntry = {
            timestamp: number;
            strategy: string;
            variant: string;
            pnl: number | null;
            notional: number | null;
            mode: string;
            reason: string;
            execution_id: string | null;
            side: string | null;
            entry_price: number | null;
        };
        const trades: StrategyTradeHistoryEntry[] = [];
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
            const entryPrice = entryPriceIdx >= 0 ? Number(unquote(cells[entryPriceIdx] || '')) : NaN;
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
                entry_price: Number.isFinite(entryPrice) ? entryPrice : null,
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

app.get('/api/arb/rejected-signals/log', async (_req, res) => {
    const rows = await rejectedSignalRecorder.countRows().catch((error) => {
        recordSilentCatch('RejectedSignal.CountRows', error, { path: rejectedSignalRecorder.getPath() });
        return rejectedSignalHistory.length;
    });
    res.json({
        path: rejectedSignalRecorder.getPath(),
        rows,
    });
});

app.post('/api/arb/rejected-signals/reset', requireControlPlaneAuth, async (_req, res) => {
    await rejectedSignalRecorder.reset();
    rejectedSignalHistory.splice(0, rejectedSignalHistory.length);
    rejectedSignalsByExecutionId.clear();
    io.emit('rejected_signal_snapshot', rejectedSignalSnapshot());
    res.json({
        ok: true,
        path: rejectedSignalRecorder.getPath(),
        rows: 0,
    });
});

app.get('/api/system/trading-mode', async (_req, res) => {
    res.json({
        mode: await getTradingMode(),
        live_order_posting_enabled: isLiveOrderPostingEnabled(),
        execution_halt: executionHaltState,
    });
});

app.get('/api/system/runtime-status', async (_req, res) => {
    res.json(runtimeStatusPayload());
});

app.get('/api/system/execution-halt', (_req, res) => {
    res.json(executionHaltState);
});

app.post('/api/system/trading-mode', requireControlPlaneAuth, async (req, res) => {
    const mode = normalizeTradingMode(req.body?.mode);
    const readiness = mode === 'LIVE' ? await isLiveReady() : null;
    const validation = validateTradingModeChangeRequest({
        mode: req.body?.mode,
        confirmation: req.body?.confirmation,
        execution_halted: executionHaltState.halted,
        live_readiness: readiness,
    });
    if (!validation.ok) {
        res.status(validation.status).json({
            error: validation.message,
            failures: validation.failures,
        });
        return;
    }

    await setTradingMode(validation.value.mode);
    res.json({
        ok: true,
        mode: validation.value.mode,
        live_order_posting_enabled: isLiveOrderPostingEnabled(),
    });
});

app.post('/api/system/reset-simulation', requireControlPlaneAuth, async (req, res) => {
    const validation = validateSimulationResetRequest({
        current_mode: await getTradingMode(),
        confirmation: req.body?.confirmation,
        bankroll: req.body?.bankroll,
        force_defaults: req.body?.force_defaults,
        normalize_bankroll: normalizeResetBankroll,
    });
    if (!validation.ok) {
        res.status(validation.status).json({ error: validation.message });
        return;
    }

    await resetSimulationState(validation.value.bankroll, {
        forceDefaults: validation.value.force_defaults,
    });
    res.json({
        ok: true,
        bankroll: validation.value.bankroll,
        force_defaults: validation.value.force_defaults,
    });
});

app.post('/api/system/execution-halt', requireControlPlaneAuth, async (req, res) => {
    if (typeof req.body?.halted !== 'boolean') {
        res.status(400).json({ error: 'Invalid execution halt payload' });
        return;
    }
    const state = await setExecutionHaltState(
        req.body.halted,
        sanitizeExecutionHaltReason(req.body?.reason),
        'api_control',
    );
    res.json({
        ok: true,
        ...state,
    });
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

// Agentic Decision Loop — DISABLED by default.
// Re-enable requires both LEGACY_BRAIN_LOOP_ENABLED=true and LEGACY_BRAIN_LOOP_UNSAFE_OK=true.
const LEGACY_BRAIN_LOOP_ALLOW_PRODUCTION = process.env.LEGACY_BRAIN_LOOP_ALLOW_PRODUCTION === 'true';
const legacyBrainProductionBlocked = process.env.NODE_ENV === 'production' && !LEGACY_BRAIN_LOOP_ALLOW_PRODUCTION;
if (LEGACY_BRAIN_LOOP_ENABLED && LEGACY_BRAIN_LOOP_UNSAFE_OK && !legacyBrainProductionBlocked) {
    logger.warn('[Brain] legacy decision loop enabled in unsafe mode by explicit override.');
    registerScheduledTask('Brain', 30_000, async () => {
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

            if (executionHaltState.halted) {
                touchRuntimeModule(
                    'TRADING_MODE_GUARD',
                    'DEGRADED',
                    `legacy brain execution halted: ${executionHaltState.reason || 'unspecified'}`,
                );
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
            recordSilentCatch('Brain.DecisionLoop', error, { loop: 'legacy_brain' });
            touchRuntimeModule('MODEL_INFERENCE', 'DEGRADED', `decision loop error: ${String(error)}`);
        }
    });
} else {
    if (legacyBrainProductionBlocked && LEGACY_BRAIN_LOOP_ENABLED) {
        logger.warn('[Brain] LEGACY_BRAIN_LOOP_ENABLED=true ignored in production. Set LEGACY_BRAIN_LOOP_ALLOW_PRODUCTION=true to override.');
    } else if (LEGACY_BRAIN_LOOP_ENABLED && !LEGACY_BRAIN_LOOP_UNSAFE_OK) {
        logger.warn('[Brain] LEGACY_BRAIN_LOOP_ENABLED=true ignored because LEGACY_BRAIN_LOOP_UNSAFE_OK is not set.');
    } else {
        logger.info('[Brain] Legacy decision loop disabled (default).');
    }
}

// System Health Heartbeat (Every 2s)
registerScheduledTask('SystemHealth', 2000, async () => {
    const start = Date.now();
    let redisLatency = 0;
    try {
        await redisClient.ping();
        redisLatency = Date.now() - start;
    } catch (error) {
        recordSilentCatch('SystemHealth.RedisPing', error, { op: 'PING' });
        redisLatency = -1;
    }

    const memory = process.memoryUsage();
    const now = Date.now();
    refreshModelDriftRuntime(now);
    const scannerHeartbeat = scannerHeartbeatSnapshot(now);
    const isScannerAlive = scannerHeartbeat.alive;

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

registerScheduledTask('Settlement', settlementService.getPollIntervalMs(), async () => {
    try {
        const events = await settlementService.runCycle(await getTradingMode(), isLiveOrderPostingEnabled());
        touchRuntimeModule('SETTLEMENT_ENGINE', 'ONLINE', `cycle complete (${events.length} new event${events.length === 1 ? '' : 's'})`);
        await emitSettlementEvents(events);
        if (events.length === 0 && settlementService.getSnapshot().positions.length > 0) {
            io.emit('settlement_snapshot', settlementService.getSnapshot());
        }
    } catch (error) {
        recordSilentCatch('Settlement.Cycle', error);
        touchRuntimeModule('SETTLEMENT_ENGINE', 'DEGRADED', `settlement cycle error: ${String(error)}`);
    }
});

registerScheduledTask('Governance', STRATEGY_GOVERNANCE_INTERVAL_MS, async () => {
    try {
        await runStrategyGovernanceCycle();
    } catch (error) {
        recordSilentCatch('Governance.Cycle', error);
        touchRuntimeModule('FEATURE_REGISTRY', 'DEGRADED', `governance error: ${String(error)}`);
    }
});

registerScheduledTask('LedgerHealth', 5_000, async () => {
    try {
        await refreshLedgerHealth();
    } catch (error) {
        recordSilentCatch('LedgerHealth.Refresh', error);
        touchRuntimeModule('PNL_LEDGER', 'DEGRADED', `ledger health error: ${String(error)}`);
    }
});

registerScheduledTask('PnlParity', LEDGER_PNL_PARITY_INTERVAL_MS, async () => {
    try {
        await refreshPnlParityWatchdog();
    } catch (error) {
        recordSilentCatch('PnLParity.RefreshTask', error);
        touchRuntimeModule('PNL_LEDGER', 'DEGRADED', `pnl parity task error: ${String(error)}`);
    }
});

registerScheduledTask('MetaController', 3_000, async () => {
    requestMetaControllerRefresh();
});

registerScheduledTask('CrossHorizonRouter', CROSS_HORIZON_ROUTER_INTERVAL_MS, async () => {
    requestCrossHorizonRefresh();
});

registerScheduledTask('MLPipeline', 10_000, async () => {
    try {
        await refreshMlPipelineStatus();
    } catch (error) {
        recordSilentCatch('MLPipeline.Refresh', error);
        touchRuntimeModule('ML_TRAINER', 'DEGRADED', `ml pipeline error: ${String(error)}`);
        touchRuntimeModule('MODEL_INFERENCE', 'DEGRADED', `ml pipeline error: ${String(error)}`);
        touchRuntimeModule('DRIFT_MONITOR', 'DEGRADED', `ml pipeline error: ${String(error)}`);
    }
});

registerScheduledTask('MLTrainer', MODEL_TRAINER_INTERVAL_MS, async () => {
    try {
        await runInProcessModelTraining(false);
    } catch (error) {
        recordSilentCatch('MLTrainer.Cycle', error);
        touchRuntimeModule('ML_TRAINER', 'DEGRADED', `trainer cycle error: ${String(error)}`);
    }
});

registerScheduledTask('FeatureLabelBackfill', FEATURE_LABEL_BACKFILL_INTERVAL_MS, async () => {
    try {
        await backfillFeatureLabelsFromTradeRecorder(5000, false);
    } catch (error) {
        recordSilentCatch('FeatureRegistry.LabelBackfillCycle', error);
    }
});

registerScheduledTask('ModelGateCalibration', MODEL_PROBABILITY_GATE_TUNER_INTERVAL_MS, async () => {
    try {
        await runModelProbabilityGateCalibration();
    } catch (error) {
        recordSilentCatch('ModelGateCalibration.Cycle', error);
        touchRuntimeModule('UNCERTAINTY_GATE', 'DEGRADED', `model gate calibration error: ${String(error)}`);
    }
});

// Start Services
async function bootstrap() {
    await connectRedis();
    await strategyTradeRecorder.init();
    await featureRegistryRecorder.init();
    await bootstrapFeatureRegistryFromEventLogs();
    await settlementService.init();
    await marketDataService.start();

    if (!isControlPlaneTokenConfigured()) {
        logger.error('CONTROL_PLANE_TOKEN is not configured. Control actions are locked until a token is set.');
        touchRuntimeModule('TRADING_MODE_GUARD', 'DEGRADED', 'control plane token missing; control actions locked');
    } else {
        touchRuntimeModule('TRADING_MODE_GUARD', 'ONLINE', 'control plane token configured');
    }

    await redisClient.setNX(SIM_BANKROLL_KEY, DEFAULT_SIM_BANKROLL.toFixed(2));
    await redisClient.setNX(SIM_LEDGER_CASH_KEY, DEFAULT_SIM_BANKROLL.toFixed(8));
    await redisClient.setNX(SIM_LEDGER_RESERVED_KEY, '0');
    await redisClient.setNX(SIM_LEDGER_REALIZED_PNL_KEY, '0');
    await redisClient.setNX('system:simulation_reset_ts', '0');
    await redisClient.setNX(EXECUTION_HALT_KEY, JSON.stringify(defaultExecutionHaltState()));
    await loadExecutionHaltState();
    if (executionHaltState.halted) {
        touchRuntimeModule(
            'TRADING_MODE_GUARD',
            'DEGRADED',
            `execution halt active at boot: ${executionHaltState.reason || 'unspecified'}`,
        );
    }
    if (SIM_RESET_ON_BOOT) {
        await resetSimulationState(DEFAULT_SIM_BANKROLL, { forceDefaults: true });
    } else {
        await getSimulationLedgerSnapshot();
        await loadStrategyExecutionHistoryFromRedis();
    }
    await loadStrategyMetrics();
    await reconcileSimulationLedgerWithStrategyMetrics();
    await getRiskConfig();
    await redisClient.setNX('system:trading_mode', 'PAPER');
    await enforceBootTradingModeSafety();
    await applyDefaultStrategyStates(false);
    await bootstrapStrategyMultipliers();
    await loadRiskGuardStates();
    await backfillFeatureLabelsFromTradeRecorder();
    await bootstrapRejectedSignalHistory();
    await runStrategyGovernanceCycle();
    await refreshLedgerHealth();
    await refreshPnlParityWatchdog();
    await refreshMetaController();
    await runCrossHorizonRefreshSafely();
    await refreshMlPipelineStatus();
    refreshModelDriftRuntime();
    await loadModelGateCalibrationState();
    await runModelProbabilityGateCalibration();
    await runInProcessModelTraining(false);
    bootstrapComplete = true;

    // ── Periodic state broadcast: keep all dashboards live ──
    registerScheduledTask('StateBroadcast', 5000, async () => {
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
                const state = getRiskGuardState(id);
                if (state) io.emit('risk_guard_update', { strategy: id, ...state });
            }
        } catch (error) {
            recordSilentCatch('StateBroadcast.Emit', error, { task: 'StateBroadcast' });
        }
    });

    const PORT = Number(process.env.PORT) || 5114;
    httpServer.listen(PORT, () => {
        logger.info(`Backend server running on port ${PORT}`);
    });
}

bootstrap().catch((error) => {
    logger.error(`Failed to bootstrap backend: ${String(error)}`);
    process.exit(1);
});

let shutdownInProgress = false;

const shutdown = async (signal: string) => {
    if (shutdownInProgress) {
        return;
    }
    shutdownInProgress = true;
    logger.info(`[System] ${signal} received, shutting down gracefully...`);
    bootstrapComplete = false;
    stopAllScheduledTasks();
    for (const strategyId of [...riskGuardBootResumeTimers.keys()]) {
        clearRiskGuardBootResumeTimer(strategyId);
    }
    await marketDataService.stop().catch((error) => {
        recordSilentCatch('Shutdown.MarketDataStop', error, { signal });
    });

    await new Promise<void>((resolve) => {
        httpServer.close(() => resolve());
    });

    if (redisSubscriber.isOpen) {
        await redisSubscriber.quit().catch((error) => {
            recordSilentCatch('Shutdown.RedisSubscriberQuit', error, { signal });
        });
    }
    if (redisClient.isOpen) {
        await redisClient.quit().catch((error) => {
            recordSilentCatch('Shutdown.RedisClientQuit', error, { signal });
        });
    }

    process.exit(0);
};
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
