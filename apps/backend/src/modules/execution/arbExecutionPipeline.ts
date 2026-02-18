import type {
    PolymarketLiveExecutionResult,
    PolymarketPreflightResult,
} from '../../services/PolymarketPreflightService';
import type { SettlementEvent } from '../../services/PolymarketSettlementService';
import { evaluateEntryFreshnessInvariant } from './entryFreshnessInvariant';

export type ExecutionPipelineTradingMode = 'PAPER' | 'LIVE';
type RuntimeHealth = 'ONLINE' | 'DEGRADED' | 'OFFLINE' | 'STANDBY';
type TraceEventType = 'INTELLIGENCE_GATE' | 'MODEL_GATE' | 'PREFLIGHT' | 'LIVE_EXECUTION';

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

export async function processArbitrageExecutionWorker(
    context: ArbExecutionWorkerContext,
    deps: ArbExecutionPipelineDeps,
): Promise<void> {
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
        return;
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
        return;
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
        return;
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

    let preflight: PolymarketPreflightResult | null;
    try {
        preflight = await deps.preflightFromExecution(parsed);
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
        return;
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
            liveExecution = await deps.executeFromExecution(parsed, currentTradingMode);
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
            return;
        }
        if (!liveExecution) {
            return;
        }
        deps.annotateExecutionId(liveExecution, executionId);
        deps.pushExecutionTraceEvent(executionId, 'LIVE_EXECUTION', liveExecution, {
            strategy: liveExecution.strategy || modelGate.strategy || intelligenceGate.strategy || parsedStrategy,
            market_key: parsedMarketKey,
        });
        deps.emit('strategy_live_execution', liveExecution);
        deps.emit('execution_log', deps.liveToExecutionLog(liveExecution));
        if (!liveExecution.ok) {
            deps.recordRejectedSignal?.({
                stage: 'LIVE_EXECUTION',
                execution_id: executionId,
                strategy: liveExecution.strategy || parsedStrategy,
                market_key: parsedMarketKey,
                reason: liveExecution.reason || `${liveExecution.failed} order(s) failed`,
                mode: currentTradingMode,
                timestamp: Date.now(),
                payload: asRecord(liveExecution),
            });
        }
        try {
            const settlementEvents = await deps.registerAtomicExecution(liveExecution);
            await deps.emitSettlementEvents(settlementEvents);
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
        }
        return;
    }

    deps.touchRuntimeModule('EXECUTION_PREFLIGHT', preflight.ok ? 'ONLINE' : 'DEGRADED', `${preflight.strategy} preflight ${preflight.ok ? 'ok' : 'failed'}`);
    deps.annotateExecutionId(preflight, executionId);
    deps.pushExecutionTraceEvent(executionId, 'PREFLIGHT', preflight, {
        strategy: preflight.strategy || modelGate.strategy || intelligenceGate.strategy || parsedStrategy,
        market_key: parsedMarketKey,
    });
    deps.emit('strategy_preflight', preflight);
    deps.emit('execution_log', deps.preflightToExecutionLog(preflight));
    if (!preflight.ok) {
        deps.recordRejectedSignal?.({
            stage: 'PREFLIGHT',
            execution_id: executionId,
            strategy: preflight.strategy || parsedStrategy,
            market_key: parsedMarketKey,
            reason: preflight.error || `${preflight.failed} order(s) failed preflight`,
            mode: currentTradingMode,
            timestamp: preflight.timestamp,
            payload: asRecord(preflight),
        });
    }

    let liveExecution: PolymarketLiveExecutionResult | null;
    try {
        liveExecution = await deps.executeFromExecution(parsed, currentTradingMode);
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
        return;
    }
    if (!liveExecution) {
        return;
    }
    deps.annotateExecutionId(liveExecution, executionId);
    deps.pushExecutionTraceEvent(executionId, 'LIVE_EXECUTION', liveExecution, {
        strategy: liveExecution.strategy || preflight.strategy || modelGate.strategy || intelligenceGate.strategy || parsedStrategy,
        market_key: parsedMarketKey,
    });
    deps.emit('strategy_live_execution', liveExecution);
    deps.emit('execution_log', deps.liveToExecutionLog(liveExecution));
    if (!liveExecution.ok) {
        deps.recordRejectedSignal?.({
            stage: 'LIVE_EXECUTION',
            execution_id: executionId,
            strategy: liveExecution.strategy || preflight.strategy || parsedStrategy,
            market_key: parsedMarketKey,
            reason: liveExecution.reason || `${liveExecution.failed} order(s) failed`,
            mode: currentTradingMode,
            timestamp: Date.now(),
            payload: asRecord(liveExecution),
        });
    }
    try {
        const settlementEvents = await deps.registerAtomicExecution(liveExecution);
        await deps.emitSettlementEvents(settlementEvents);
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
    }
}
