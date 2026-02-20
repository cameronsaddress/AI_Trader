import type {
    PolymarketLiveExecutionResult,
    PolymarketPreflightResult,
} from '../../services/PolymarketPreflightService';
import type { SettlementEvent } from '../../services/PolymarketSettlementService';
import { evaluateEntryFreshnessInvariant } from './entryFreshnessInvariant';

export type ExecutionPipelineTradingMode = 'PAPER' | 'LIVE';
type RuntimeHealth = 'ONLINE' | 'DEGRADED' | 'OFFLINE' | 'STANDBY';
type TraceEventType = 'INTELLIGENCE_GATE' | 'MODEL_GATE' | 'PREFLIGHT' | 'LIVE_EXECUTION';
type OpsAlertSeverity = 'INFO' | 'WARNING' | 'CRITICAL';

type TraceMeta = {
    strategy: string | null;
    market_key: string | null;
};

export type RejectedSignalStage =
    | 'INTELLIGENCE_GATE'
    | 'MODEL_GATE'
    | 'PREFLIGHT'
    | 'LIVE_EXECUTION'
    | 'SETTLEMENT';

export type RejectedSignalRecord = {
    stage: RejectedSignalStage;
    execution_id: string;
    strategy: string | null;
    market_key: string | null;
    reason: string;
    mode: ExecutionPipelineTradingMode;
    timestamp: number;
    payload: Record<string, unknown> | null;
};

export type SettlementDlqEntry = {
    execution_id: string;
    strategy: string | null;
    market_key: string | null;
    mode: ExecutionPipelineTradingMode;
    timestamp: number;
    reason: string;
    live_execution: PolymarketLiveExecutionResult;
};

export type OpsAlertPayload = {
    severity: OpsAlertSeverity;
    scope: string;
    message: string;
    execution_id?: string;
    strategy?: string | null;
    market_key?: string | null;
    details?: Record<string, unknown> | null;
    timestamp: number;
};

export type ArbExecutionWorkerResult = {
    decision: 'ACCEPTED' | 'REJECTED';
    stage: RejectedSignalStage | 'NONE';
    reason: string;
};

type IntelligenceGateResult = {
    ok: boolean;
    reason?: string;
    strategy?: string;
    marketKey?: string | null;
    scan?: unknown;
    margin?: number;
    normalizedMargin?: number;
    ageMs?: number;
    peerSignals?: number;
    peerStrategies?: string[];
    peerConsensus?: number;
};

type ModelGateResult = {
    ok: boolean;
    blocked: boolean;
    reason: string;
    strategy: string | null;
    market_key: string | null;
    probability: number | null;
    gate: number;
    model_loaded: boolean;
};

export type ArbExecutionWorkerContext = {
    parsed: unknown;
    executionId: string;
    parsedStrategy: string | null;
    parsedMarketKey: string | null;
};

export interface ArbExecutionPipelineDeps {
    getTradingMode: () => Promise<ExecutionPipelineTradingMode>;
    evaluateIntelligenceGate: (payload: unknown, tradingMode: ExecutionPipelineTradingMode) => IntelligenceGateResult;
    evaluateModelProbabilityGate: (payload: unknown, tradingMode: ExecutionPipelineTradingMode) => ModelGateResult;
    preflightFromExecution: (payload: unknown) => Promise<PolymarketPreflightResult | null>;
    executeFromExecution: (
        payload: unknown,
        tradingMode: ExecutionPipelineTradingMode,
    ) => Promise<PolymarketLiveExecutionResult | null>;
    registerAtomicExecution: (payload: PolymarketLiveExecutionResult) => Promise<SettlementEvent[]>;
    emitSettlementEvents: (events: SettlementEvent[]) => Promise<void>;
    pushExecutionTraceEvent: (
        executionId: string,
        type: TraceEventType,
        payload: unknown,
        meta?: TraceMeta,
    ) => void;
    touchRuntimeModule: (moduleId: string, health: RuntimeHealth, detail: string) => void;
    emit: (event: string, payload: unknown) => void;
    preflightToExecutionLog: (preflight: PolymarketPreflightResult) => Record<string, unknown>;
    liveToExecutionLog: (live: PolymarketLiveExecutionResult) => Record<string, unknown>;
    marketLabelFromPayload: (payload: unknown) => string;
    annotateExecutionId: (target: unknown, executionId: string) => void;
    recordRejectedSignal?: (record: RejectedSignalRecord) => void;
    enqueueSettlementDlq?: (entry: SettlementDlqEntry) => Promise<void>;
    notifyOps?: (payload: OpsAlertPayload) => Promise<void> | void;
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

function errorMessage(error: unknown): string {
    if (error instanceof Error) {
        return `${error.name}: ${error.message}`;
    }
    if (typeof error === 'string') {
        return error;
    }
    try {
        return JSON.stringify(error);
    } catch {
        return String(error);
    }
}

function accepted(reason: string): ArbExecutionWorkerResult {
    return {
        decision: 'ACCEPTED',
        stage: 'NONE',
        reason,
    };
}

function rejected(stage: RejectedSignalStage, reason: string): ArbExecutionWorkerResult {
    return {
        decision: 'REJECTED',
        stage,
        reason,
    };
}

const PREFLIGHT_TIMEOUT_MS = Math.max(500, Number(process.env.EXECUTION_PREFLIGHT_TIMEOUT_MS || '10000'));
const LIVE_EXECUTION_TIMEOUT_MS = Math.max(500, Number(process.env.EXECUTION_LIVE_TIMEOUT_MS || '10000'));
const SETTLEMENT_REGISTER_TIMEOUT_MS = Math.max(500, Number(process.env.EXECUTION_SETTLEMENT_TIMEOUT_MS || '12000'));
const SETTLEMENT_EMIT_TIMEOUT_MS = Math.max(500, Number(process.env.EXECUTION_SETTLEMENT_EMIT_TIMEOUT_MS || '8000'));
const SETTLEMENT_DLQ_TIMEOUT_MS = Math.max(500, Number(process.env.EXECUTION_SETTLEMENT_DLQ_TIMEOUT_MS || '5000'));

async function withTimeout<T>(operation: string, timeoutMs: number, task: () => Promise<T>): Promise<T> {
    let timeoutHandle: ReturnType<typeof setTimeout> | null = null;
    const timeoutPromise = new Promise<T>((_resolve, reject) => {
        timeoutHandle = setTimeout(() => {
            reject(new Error(`${operation} timed out after ${timeoutMs}ms`));
        }, timeoutMs);
    });

    try {
        return await Promise.race([task(), timeoutPromise]);
    } finally {
        if (timeoutHandle) {
            clearTimeout(timeoutHandle);
        }
    }
}

function toOpsDetails(payload: unknown): Record<string, unknown> | null {
    const record = asRecord(payload);
    return record || null;
}

async function emitOpsAlert(deps: ArbExecutionPipelineDeps, payload: OpsAlertPayload): Promise<void> {
    deps.emit('ops_alert', payload);
    if (!deps.notifyOps) {
        return;
    }
    try {
        await deps.notifyOps(payload);
    } catch {
        // Alert transport failure must never block execution loop.
    }
}

export async function processArbitrageExecutionWorker(
    context: ArbExecutionWorkerContext,
    deps: ArbExecutionPipelineDeps,
): Promise<ArbExecutionWorkerResult> {
    const { parsed, executionId, parsedStrategy, parsedMarketKey } = context;
    const currentTradingMode = await deps.getTradingMode();
    const intelligenceGate = deps.evaluateIntelligenceGate(parsed, currentTradingMode);
    const gateMeta = {
        strategy: intelligenceGate.strategy || parsedStrategy,
        market_key: intelligenceGate.marketKey || parsedMarketKey,
    };
    const entryFreshness = evaluateEntryFreshnessInvariant(parsed, intelligenceGate);
    if (entryFreshness.applicable && !entryFreshness.ok) {
        const invariantPayload = {
            execution_id: executionId,
            timestamp: Date.now(),
            strategy: intelligenceGate.strategy || parsedStrategy || 'UNKNOWN',
            market_key: intelligenceGate.marketKey || parsedMarketKey,
            reason: `[ENTRY_INVARIANT] ${entryFreshness.reason}`,
            side: entryFreshness.side,
            max_observed_age_ms: entryFreshness.max_observed_age_ms,
            checked_fields: entryFreshness.checked_fields,
            gate_reason: intelligenceGate.reason || null,
            mode: currentTradingMode,
            severity: 'CRITICAL',
        };
        deps.touchRuntimeModule('INTELLIGENCE_GATE', 'DEGRADED', `CRITICAL ${invariantPayload.reason}`);
        deps.pushExecutionTraceEvent(executionId, 'INTELLIGENCE_GATE', invariantPayload, gateMeta);
        deps.emit('execution_invariant_alert', invariantPayload);
        deps.emit('execution_log', {
            execution_id: executionId,
            timestamp: invariantPayload.timestamp,
            side: 'INVARIANT_CRITICAL',
            market: deps.marketLabelFromPayload(parsed),
            price: invariantPayload.reason,
            size: invariantPayload.strategy,
            mode: currentTradingMode,
            details: invariantPayload,
        });
        deps.recordRejectedSignal?.({
            stage: 'INTELLIGENCE_GATE',
            execution_id: executionId,
            strategy: invariantPayload.strategy,
            market_key: invariantPayload.market_key,
            reason: invariantPayload.reason,
            mode: currentTradingMode,
            timestamp: invariantPayload.timestamp,
            payload: asRecord(invariantPayload),
        });
        return rejected('INTELLIGENCE_GATE', invariantPayload.reason);
    }
    if (!intelligenceGate.ok) {
        deps.touchRuntimeModule('INTELLIGENCE_GATE', 'DEGRADED', intelligenceGate.reason || 'execution blocked');
        const blockPayload = {
            execution_id: executionId,
            timestamp: Date.now(),
            strategy: intelligenceGate.strategy || 'UNKNOWN',
            market_key: intelligenceGate.marketKey || asString(asRecord(intelligenceGate.scan)?.market_key) || null,
            reason: intelligenceGate.reason || 'Intelligence gate rejected execution',
            scan: intelligenceGate.scan || null,
            margin: intelligenceGate.margin,
            normalized_margin: intelligenceGate.normalizedMargin,
            age_ms: intelligenceGate.ageMs,
            peer_signals: intelligenceGate.peerSignals ?? 0,
            peer_consensus: intelligenceGate.peerConsensus ?? 0,
            peer_strategies: intelligenceGate.peerStrategies || [],
        };
        deps.pushExecutionTraceEvent(executionId, 'INTELLIGENCE_GATE', blockPayload, gateMeta);
        deps.emit('strategy_intelligence_block', blockPayload);
        deps.emit('execution_log', {
            execution_id: executionId,
            timestamp: Date.now(),
            side: 'INTEL_BLOCK',
            market: deps.marketLabelFromPayload(parsed),
            price: blockPayload.reason,
            size: blockPayload.strategy,
            mode: 'LIVE_GUARD',
            details: blockPayload,
        });
        deps.recordRejectedSignal?.({
            stage: 'INTELLIGENCE_GATE',
            execution_id: executionId,
            strategy: blockPayload.strategy,
            market_key: blockPayload.market_key,
            reason: blockPayload.reason,
            mode: currentTradingMode,
            timestamp: blockPayload.timestamp,
            payload: asRecord(blockPayload),
        });
        return rejected('INTELLIGENCE_GATE', blockPayload.reason);
    }
    deps.pushExecutionTraceEvent(
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
    deps.touchRuntimeModule('INTELLIGENCE_GATE', 'ONLINE', `execution allowed for ${intelligenceGate.strategy || 'UNKNOWN'}`);
    const modelGate = deps.evaluateModelProbabilityGate(parsed, currentTradingMode);
    if (!modelGate.ok) {
        deps.touchRuntimeModule(
            'UNCERTAINTY_GATE',
            'DEGRADED',
            `${modelGate.strategy || 'UNKNOWN'} blocked: ${modelGate.reason}`,
        );
        const modelBlockPayload = {
            execution_id: executionId,
            timestamp: Date.now(),
            strategy: modelGate.strategy || intelligenceGate.strategy || 'UNKNOWN',
            market_key: modelGate.market_key || intelligenceGate.marketKey || asString(asRecord(intelligenceGate.scan)?.market_key) || null,
            reason: `[MODEL_GATE] ${modelGate.reason}`,
            probability: modelGate.probability,
            gate: modelGate.gate,
            model_loaded: modelGate.model_loaded,
            mode: currentTradingMode,
        };
        deps.pushExecutionTraceEvent(executionId, 'MODEL_GATE', modelBlockPayload, {
            strategy: modelBlockPayload.strategy,
            market_key: modelBlockPayload.market_key,
        });
        deps.emit('strategy_model_block', modelBlockPayload);
        // Reuse existing UI intelligence block stream for visibility.
        deps.emit('strategy_intelligence_block', modelBlockPayload);
        deps.emit('execution_log', {
            execution_id: executionId,
            timestamp: Date.now(),
            side: 'MODEL_BLOCK',
            market: deps.marketLabelFromPayload(parsed),
            price: modelBlockPayload.reason,
            size: modelBlockPayload.strategy,
            mode: currentTradingMode === 'PAPER' ? 'PAPER_MODEL_GUARD' : 'LIVE_MODEL_GUARD',
            details: modelBlockPayload,
        });
        deps.recordRejectedSignal?.({
            stage: 'MODEL_GATE',
            execution_id: executionId,
            strategy: modelBlockPayload.strategy,
            market_key: modelBlockPayload.market_key,
            reason: modelBlockPayload.reason,
            mode: currentTradingMode,
            timestamp: modelBlockPayload.timestamp,
            payload: asRecord(modelBlockPayload),
        });
        return rejected('MODEL_GATE', modelBlockPayload.reason);
    }
    deps.pushExecutionTraceEvent(
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
    deps.touchRuntimeModule(
        'UNCERTAINTY_GATE',
        'ONLINE',
        `${modelGate.strategy || 'UNKNOWN'} prob gate pass (${modelGate.reason})`,
    );

    if (currentTradingMode === 'PAPER') {
        const paperBypassPayload = {
            execution_id: executionId,
            timestamp: Date.now(),
            strategy: modelGate.strategy || intelligenceGate.strategy || parsedStrategy || 'UNKNOWN',
            market_key: modelGate.market_key || intelligenceGate.marketKey || parsedMarketKey,
            bypassed: true,
            reason: 'paper mode bypasses preflight/live execution stages',
            mode: currentTradingMode,
        };
        deps.touchRuntimeModule('EXECUTION_PREFLIGHT', 'STANDBY', paperBypassPayload.reason);
        deps.pushExecutionTraceEvent(executionId, 'PREFLIGHT', paperBypassPayload, {
            strategy: paperBypassPayload.strategy,
            market_key: paperBypassPayload.market_key,
        });
        deps.emit('execution_log', {
            execution_id: executionId,
            timestamp: paperBypassPayload.timestamp,
            side: 'PAPER_EXEC_BYPASS',
            market: deps.marketLabelFromPayload(parsed),
            price: paperBypassPayload.reason,
            size: paperBypassPayload.strategy,
            mode: currentTradingMode,
            details: paperBypassPayload,
        });
        return accepted(paperBypassPayload.reason);
    }

    let preflight: PolymarketPreflightResult | null;
    try {
        preflight = await withTimeout(
            'preflightFromExecution',
            PREFLIGHT_TIMEOUT_MS,
            () => deps.preflightFromExecution(parsed),
        );
    } catch (error) {
        const reason = `preflight error: ${errorMessage(error)}`;
        const errorPayload = {
            execution_id: executionId,
            timestamp: Date.now(),
            strategy: modelGate.strategy || intelligenceGate.strategy || parsedStrategy || 'UNKNOWN',
            market_key: modelGate.market_key || intelligenceGate.marketKey || parsedMarketKey,
            reason,
            mode: currentTradingMode,
        };
        deps.touchRuntimeModule('EXECUTION_PREFLIGHT', 'DEGRADED', reason);
        deps.pushExecutionTraceEvent(executionId, 'PREFLIGHT', errorPayload, {
            strategy: errorPayload.strategy,
            market_key: errorPayload.market_key,
        });
        deps.emit('execution_log', {
            execution_id: executionId,
            timestamp: errorPayload.timestamp,
            side: 'PREFLIGHT_ERR',
            market: deps.marketLabelFromPayload(parsed),
            price: reason,
            size: errorPayload.strategy,
            mode: currentTradingMode,
            details: errorPayload,
        });
        deps.recordRejectedSignal?.({
            stage: 'PREFLIGHT',
            execution_id: executionId,
            strategy: errorPayload.strategy,
            market_key: errorPayload.market_key,
            reason,
            mode: currentTradingMode,
            timestamp: errorPayload.timestamp,
            payload: asRecord(errorPayload),
        });
        return rejected('PREFLIGHT', reason);
    }
    if (!preflight) {
        deps.touchRuntimeModule('EXECUTION_PREFLIGHT', 'ONLINE', 'preflight bypassed (no polymarket order payload)');
        deps.pushExecutionTraceEvent(
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
        let liveExecution: PolymarketLiveExecutionResult | null;
        try {
            liveExecution = await withTimeout(
                'executeFromExecution',
                LIVE_EXECUTION_TIMEOUT_MS,
                () => deps.executeFromExecution(parsed, currentTradingMode),
            );
        } catch (error) {
            const reason = `live execution error: ${errorMessage(error)}`;
            const errorPayload = {
                execution_id: executionId,
                timestamp: Date.now(),
                strategy: modelGate.strategy || intelligenceGate.strategy || parsedStrategy || 'UNKNOWN',
                market_key: parsedMarketKey,
                reason,
                mode: currentTradingMode,
            };
            deps.touchRuntimeModule('EXECUTION_PREFLIGHT', 'DEGRADED', reason);
            deps.pushExecutionTraceEvent(executionId, 'LIVE_EXECUTION', errorPayload, {
                strategy: errorPayload.strategy,
                market_key: errorPayload.market_key,
            });
            deps.emit('execution_log', {
                execution_id: executionId,
                timestamp: errorPayload.timestamp,
                side: 'LIVE_EXEC_ERR',
                market: deps.marketLabelFromPayload(parsed),
                price: reason,
                size: errorPayload.strategy,
                mode: currentTradingMode,
                details: errorPayload,
            });
            deps.recordRejectedSignal?.({
                stage: 'LIVE_EXECUTION',
                execution_id: executionId,
                strategy: errorPayload.strategy,
                market_key: errorPayload.market_key,
                reason,
                mode: currentTradingMode,
                timestamp: errorPayload.timestamp,
                payload: asRecord(errorPayload),
            });
            return rejected('LIVE_EXECUTION', reason);
        }
        if (!liveExecution) {
            const bypassPayload = {
                execution_id: executionId,
                timestamp: Date.now(),
                strategy: modelGate.strategy || intelligenceGate.strategy || parsedStrategy || 'UNKNOWN',
                market_key: parsedMarketKey,
                bypassed: true,
                reason: 'live execution bypassed (no polymarket order payload)',
                mode: currentTradingMode,
            };
            deps.touchRuntimeModule('EXECUTION_PREFLIGHT', 'ONLINE', bypassPayload.reason);
            deps.pushExecutionTraceEvent(executionId, 'LIVE_EXECUTION', bypassPayload, {
                strategy: bypassPayload.strategy,
                market_key: bypassPayload.market_key,
            });
            deps.emit('execution_log', {
                execution_id: executionId,
                timestamp: bypassPayload.timestamp,
                side: 'LIVE_EXEC_BYPASS',
                market: deps.marketLabelFromPayload(parsed),
                price: bypassPayload.reason,
                size: bypassPayload.strategy,
                mode: currentTradingMode,
                details: bypassPayload,
            });
            return accepted(bypassPayload.reason);
        }
        deps.annotateExecutionId(liveExecution, executionId);
        deps.pushExecutionTraceEvent(executionId, 'LIVE_EXECUTION', liveExecution, {
            strategy: liveExecution.strategy || modelGate.strategy || intelligenceGate.strategy || parsedStrategy,
            market_key: parsedMarketKey,
        });
        deps.emit('strategy_live_execution', liveExecution);
        deps.emit('execution_log', deps.liveToExecutionLog(liveExecution));
        if (!liveExecution.ok) {
            const liveFailureReason = liveExecution.reason || `${liveExecution.failed} order(s) failed`;
            deps.recordRejectedSignal?.({
                stage: 'LIVE_EXECUTION',
                execution_id: executionId,
                strategy: liveExecution.strategy || parsedStrategy,
                market_key: parsedMarketKey,
                reason: liveFailureReason,
                mode: currentTradingMode,
                timestamp: Date.now(),
                payload: asRecord(liveExecution),
            });
            return rejected('LIVE_EXECUTION', liveFailureReason);
        }
        if (currentTradingMode === 'LIVE' && liveExecution.ok && !liveExecution.dryRun) {
            try {
                const settlementEvents = await withTimeout(
                    'registerAtomicExecution',
                    SETTLEMENT_REGISTER_TIMEOUT_MS,
                    () => deps.registerAtomicExecution(liveExecution),
                );
                await withTimeout(
                    'emitSettlementEvents',
                    SETTLEMENT_EMIT_TIMEOUT_MS,
                    () => deps.emitSettlementEvents(settlementEvents),
                );
            } catch (error) {
                const reason = `settlement error: ${errorMessage(error)}`;
                const settlementPayload = {
                    execution_id: executionId,
                    timestamp: Date.now(),
                    strategy: liveExecution.strategy || modelGate.strategy || intelligenceGate.strategy || parsedStrategy || 'UNKNOWN',
                    market_key: parsedMarketKey,
                    reason,
                    mode: currentTradingMode,
                };
                deps.touchRuntimeModule('SETTLEMENT_ENGINE', 'DEGRADED', reason);
                deps.emit('execution_log', {
                    execution_id: executionId,
                    timestamp: settlementPayload.timestamp,
                    side: 'SETTLEMENT_ERR',
                    market: deps.marketLabelFromPayload(parsed),
                    price: reason,
                    size: settlementPayload.strategy,
                    mode: currentTradingMode,
                    details: settlementPayload,
                });
                deps.recordRejectedSignal?.({
                    stage: 'SETTLEMENT',
                    execution_id: executionId,
                    strategy: settlementPayload.strategy,
                    market_key: settlementPayload.market_key,
                    reason,
                    mode: currentTradingMode,
                    timestamp: settlementPayload.timestamp,
                    payload: asRecord(settlementPayload),
                });
                const dlqEntry: SettlementDlqEntry = {
                    execution_id: executionId,
                    strategy: settlementPayload.strategy,
                    market_key: settlementPayload.market_key,
                    mode: currentTradingMode,
                    timestamp: settlementPayload.timestamp,
                    reason,
                    live_execution: liveExecution,
                };
                if (deps.enqueueSettlementDlq) {
                    try {
                        await withTimeout(
                            'enqueueSettlementDlq',
                            SETTLEMENT_DLQ_TIMEOUT_MS,
                            () => deps.enqueueSettlementDlq!(dlqEntry),
                        );
                    } catch (dlqError) {
                        deps.emit('execution_log', {
                            execution_id: executionId,
                            timestamp: Date.now(),
                            side: 'SETTLEMENT_DLQ_ERR',
                            market: deps.marketLabelFromPayload(parsed),
                            price: `settlement DLQ enqueue failed: ${errorMessage(dlqError)}`,
                            size: settlementPayload.strategy,
                            mode: currentTradingMode,
                            details: {
                                ...dlqEntry,
                                enqueue_error: errorMessage(dlqError),
                            },
                        });
                    }
                }
                await emitOpsAlert(deps, {
                    severity: 'CRITICAL',
                    scope: 'SETTLEMENT',
                    message: reason,
                    execution_id: executionId,
                    strategy: settlementPayload.strategy,
                    market_key: settlementPayload.market_key,
                    details: toOpsDetails(dlqEntry),
                    timestamp: settlementPayload.timestamp,
                });
            }
        }
        return accepted('execution completed (preflight bypass path)');
    }

    const preflightHealth: RuntimeHealth = preflight.ok || preflight.throttled ? 'ONLINE' : 'DEGRADED';
    const preflightDetail = preflight.throttled
        ? `${preflight.strategy} preflight throttled`
        : `${preflight.strategy} preflight ${preflight.ok ? 'ok' : 'failed'}`;
    deps.touchRuntimeModule('EXECUTION_PREFLIGHT', preflightHealth, preflightDetail);
    deps.annotateExecutionId(preflight, executionId);
    deps.pushExecutionTraceEvent(executionId, 'PREFLIGHT', preflight, {
        strategy: preflight.strategy || modelGate.strategy || intelligenceGate.strategy || parsedStrategy,
        market_key: parsedMarketKey,
    });
    deps.emit('strategy_preflight', preflight);
    deps.emit('execution_log', deps.preflightToExecutionLog(preflight));
    if (!preflight.ok) {
        const preflightReason = preflight.error || `${preflight.failed} order(s) failed preflight`;
        deps.recordRejectedSignal?.({
            stage: 'PREFLIGHT',
            execution_id: executionId,
            strategy: preflight.strategy || parsedStrategy,
            market_key: parsedMarketKey,
            reason: preflightReason,
            mode: currentTradingMode,
            timestamp: preflight.timestamp,
            payload: asRecord(preflight),
        });
        return rejected('PREFLIGHT', preflightReason);
    }

    let liveExecution: PolymarketLiveExecutionResult | null;
    try {
        liveExecution = await withTimeout(
            'executeFromExecution',
            LIVE_EXECUTION_TIMEOUT_MS,
            () => deps.executeFromExecution(parsed, currentTradingMode),
        );
    } catch (error) {
        const reason = `live execution error: ${errorMessage(error)}`;
        const errorPayload = {
            execution_id: executionId,
            timestamp: Date.now(),
            strategy: preflight.strategy || modelGate.strategy || intelligenceGate.strategy || parsedStrategy || 'UNKNOWN',
            market_key: parsedMarketKey,
            reason,
            mode: currentTradingMode,
        };
        deps.touchRuntimeModule('EXECUTION_PREFLIGHT', 'DEGRADED', reason);
        deps.pushExecutionTraceEvent(executionId, 'LIVE_EXECUTION', errorPayload, {
            strategy: errorPayload.strategy,
            market_key: errorPayload.market_key,
        });
        deps.emit('execution_log', {
            execution_id: executionId,
            timestamp: errorPayload.timestamp,
            side: 'LIVE_EXEC_ERR',
            market: deps.marketLabelFromPayload(parsed),
            price: reason,
            size: errorPayload.strategy,
            mode: currentTradingMode,
            details: errorPayload,
        });
        deps.recordRejectedSignal?.({
            stage: 'LIVE_EXECUTION',
            execution_id: executionId,
            strategy: errorPayload.strategy,
            market_key: errorPayload.market_key,
            reason,
            mode: currentTradingMode,
            timestamp: errorPayload.timestamp,
            payload: asRecord(errorPayload),
        });
        return rejected('LIVE_EXECUTION', reason);
    }
    if (!liveExecution) {
        const bypassPayload = {
            execution_id: executionId,
            timestamp: Date.now(),
            strategy: preflight.strategy || modelGate.strategy || intelligenceGate.strategy || parsedStrategy || 'UNKNOWN',
            market_key: parsedMarketKey,
            bypassed: true,
            reason: 'live execution bypassed (no polymarket order payload)',
            mode: currentTradingMode,
        };
        deps.touchRuntimeModule('EXECUTION_PREFLIGHT', 'ONLINE', bypassPayload.reason);
        deps.pushExecutionTraceEvent(executionId, 'LIVE_EXECUTION', bypassPayload, {
            strategy: bypassPayload.strategy,
            market_key: bypassPayload.market_key,
        });
        deps.emit('execution_log', {
            execution_id: executionId,
            timestamp: bypassPayload.timestamp,
            side: 'LIVE_EXEC_BYPASS',
            market: deps.marketLabelFromPayload(parsed),
            price: bypassPayload.reason,
            size: bypassPayload.strategy,
            mode: currentTradingMode,
            details: bypassPayload,
        });
        return accepted(bypassPayload.reason);
    }
    deps.annotateExecutionId(liveExecution, executionId);
    deps.pushExecutionTraceEvent(executionId, 'LIVE_EXECUTION', liveExecution, {
        strategy: liveExecution.strategy || preflight.strategy || modelGate.strategy || intelligenceGate.strategy || parsedStrategy,
        market_key: parsedMarketKey,
    });
    deps.emit('strategy_live_execution', liveExecution);
    deps.emit('execution_log', deps.liveToExecutionLog(liveExecution));
    if (!liveExecution.ok) {
        const liveFailureReason = liveExecution.reason || `${liveExecution.failed} order(s) failed`;
        deps.recordRejectedSignal?.({
            stage: 'LIVE_EXECUTION',
            execution_id: executionId,
            strategy: liveExecution.strategy || preflight.strategy || parsedStrategy,
            market_key: parsedMarketKey,
            reason: liveFailureReason,
            mode: currentTradingMode,
            timestamp: Date.now(),
            payload: asRecord(liveExecution),
        });
        return rejected('LIVE_EXECUTION', liveFailureReason);
    }
    if (currentTradingMode === 'LIVE' && liveExecution.ok && !liveExecution.dryRun) {
        try {
            const settlementEvents = await withTimeout(
                'registerAtomicExecution',
                SETTLEMENT_REGISTER_TIMEOUT_MS,
                () => deps.registerAtomicExecution(liveExecution),
            );
            await withTimeout(
                'emitSettlementEvents',
                SETTLEMENT_EMIT_TIMEOUT_MS,
                () => deps.emitSettlementEvents(settlementEvents),
            );
        } catch (error) {
            const reason = `settlement error: ${errorMessage(error)}`;
            const settlementPayload = {
                execution_id: executionId,
                timestamp: Date.now(),
                strategy: liveExecution.strategy || preflight.strategy || modelGate.strategy || intelligenceGate.strategy || parsedStrategy || 'UNKNOWN',
                market_key: parsedMarketKey,
                reason,
                mode: currentTradingMode,
            };
            deps.touchRuntimeModule('SETTLEMENT_ENGINE', 'DEGRADED', reason);
            deps.emit('execution_log', {
                execution_id: executionId,
                timestamp: settlementPayload.timestamp,
                side: 'SETTLEMENT_ERR',
                market: deps.marketLabelFromPayload(parsed),
                price: reason,
                size: settlementPayload.strategy,
                mode: currentTradingMode,
                details: settlementPayload,
            });
            deps.recordRejectedSignal?.({
                stage: 'SETTLEMENT',
                execution_id: executionId,
                strategy: settlementPayload.strategy,
                market_key: settlementPayload.market_key,
                reason,
                mode: currentTradingMode,
                timestamp: settlementPayload.timestamp,
                payload: asRecord(settlementPayload),
            });
            const dlqEntry: SettlementDlqEntry = {
                execution_id: executionId,
                strategy: settlementPayload.strategy,
                market_key: settlementPayload.market_key,
                mode: currentTradingMode,
                timestamp: settlementPayload.timestamp,
                reason,
                live_execution: liveExecution,
            };
            if (deps.enqueueSettlementDlq) {
                try {
                    await withTimeout(
                        'enqueueSettlementDlq',
                        SETTLEMENT_DLQ_TIMEOUT_MS,
                        () => deps.enqueueSettlementDlq!(dlqEntry),
                    );
                } catch (dlqError) {
                    deps.emit('execution_log', {
                        execution_id: executionId,
                        timestamp: Date.now(),
                        side: 'SETTLEMENT_DLQ_ERR',
                        market: deps.marketLabelFromPayload(parsed),
                        price: `settlement DLQ enqueue failed: ${errorMessage(dlqError)}`,
                        size: settlementPayload.strategy,
                        mode: currentTradingMode,
                        details: {
                            ...dlqEntry,
                            enqueue_error: errorMessage(dlqError),
                        },
                    });
                }
            }
            await emitOpsAlert(deps, {
                severity: 'CRITICAL',
                scope: 'SETTLEMENT',
                message: reason,
                execution_id: executionId,
                strategy: settlementPayload.strategy,
                market_key: settlementPayload.market_key,
                details: toOpsDetails(dlqEntry),
                timestamp: settlementPayload.timestamp,
            });
        }
    }
    return accepted('execution completed');
}
