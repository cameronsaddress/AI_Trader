import type {
    PolymarketLiveExecutionResult,
    PolymarketPreflightResult,
} from '../../services/PolymarketPreflightService';
import type { SettlementEvent } from '../../services/PolymarketSettlementService';
import {
    processArbitrageExecutionWorker,
    type ArbExecutionPipelineDeps,
    type ArbExecutionWorkerContext,
    type RejectedSignalRecord,
} from './arbExecutionPipeline';

function buildContext(): ArbExecutionWorkerContext {
    return {
        parsed: {
            market: 'Polymarket',
            strategy: 'ATOMIC_ARB',
            details: {},
        },
        executionId: 'exec-1',
        parsedStrategy: 'ATOMIC_ARB',
        parsedMarketKey: 'market-1',
    };
}

function buildLiveExecution(overrides: Partial<PolymarketLiveExecutionResult> = {}): PolymarketLiveExecutionResult {
    return {
        market: 'Polymarket',
        strategy: 'ATOMIC_ARB',
        mode: 'LIVE_DRY_RUN',
        timestamp: Date.now(),
        total: 1,
        posted: 1,
        failed: 0,
        ok: true,
        dryRun: false,
        orders: [],
        ...overrides,
    };
}

function buildPreflight(overrides: Partial<PolymarketPreflightResult> = {}): PolymarketPreflightResult {
    return {
        market: 'Polymarket',
        strategy: 'ATOMIC_ARB',
        mode: 'LIVE_DRY_RUN',
        timestamp: Date.now(),
        total: 1,
        signed: 1,
        failed: 0,
        ok: true,
        orders: [],
        ...overrides,
    };
}

function createDeps(overrides: Partial<ArbExecutionPipelineDeps> = {}): {
    deps: ArbExecutionPipelineDeps;
    emitCalls: Array<{ event: string; payload: unknown }>;
    rejected: RejectedSignalRecord[];
} {
    const emitCalls: Array<{ event: string; payload: unknown }> = [];
    const rejected: RejectedSignalRecord[] = [];

    const deps: ArbExecutionPipelineDeps = {
        getTradingMode: async () => 'LIVE',
        evaluateIntelligenceGate: () => ({
            ok: true,
            strategy: 'ATOMIC_ARB',
            marketKey: 'market-1',
        }),
        evaluateModelProbabilityGate: () => ({
            ok: true,
            blocked: false,
            reason: 'probability ok',
            strategy: 'ATOMIC_ARB',
            market_key: 'market-1',
            probability: 0.62,
            gate: 0.55,
            model_loaded: true,
        }),
        preflightFromExecution: async () => buildPreflight(),
        executeFromExecution: async () => buildLiveExecution(),
        registerAtomicExecution: async () => [],
        emitSettlementEvents: async () => undefined,
        pushExecutionTraceEvent: () => undefined,
        touchRuntimeModule: () => undefined,
        emit: (event, payload) => {
            emitCalls.push({ event, payload });
        },
        preflightToExecutionLog: () => ({ side: 'PREFLIGHT' }),
        liveToExecutionLog: () => ({ side: 'LIVE_EXECUTION' }),
        marketLabelFromPayload: () => 'Polymarket',
        annotateExecutionId: (target, executionId) => {
            (target as Record<string, unknown>).execution_id = executionId;
        },
        recordRejectedSignal: (record) => {
            rejected.push(record);
        },
        ...overrides,
    };

    return { deps, emitCalls, rejected };
}

describe('processArbitrageExecutionWorker', () => {
    it('blocks and records intelligence-gate rejects', async () => {
        const { deps, emitCalls, rejected } = createDeps({
            evaluateIntelligenceGate: () => ({
                ok: false,
                reason: 'peer confirmation missing',
                strategy: 'ATOMIC_ARB',
                marketKey: 'market-1',
            }),
            preflightFromExecution: jest.fn(async () => null),
        });
        const preflightSpy = deps.preflightFromExecution as jest.Mock;

        const result = await processArbitrageExecutionWorker(buildContext(), deps);

        expect(preflightSpy).not.toHaveBeenCalled();
        expect(result.decision).toBe('REJECTED');
        expect(result.stage).toBe('INTELLIGENCE_GATE');
        expect(emitCalls.some((entry) => entry.event === 'strategy_intelligence_block')).toBe(true);
        expect(rejected).toHaveLength(1);
        expect(rejected[0]?.stage).toBe('INTELLIGENCE_GATE');
    });

    it('raises critical invariant alert on stale entry freshness failure', async () => {
        const context = buildContext();
        context.parsed = {
            ...(context.parsed as Record<string, unknown>),
            side: 'ENTRY',
            details: {
                spot_age_ms: 9_500,
                book_age_ms: 1_200,
            },
        };
        const preflightSpy = jest.fn(async () => buildPreflight());
        const { deps, emitCalls, rejected } = createDeps({
            preflightFromExecution: preflightSpy,
        });

        const result = await processArbitrageExecutionWorker(context, deps);

        expect(preflightSpy).not.toHaveBeenCalled();
        expect(result.decision).toBe('REJECTED');
        expect(result.stage).toBe('INTELLIGENCE_GATE');
        expect(emitCalls.some((entry) => entry.event === 'execution_invariant_alert')).toBe(true);
        expect(rejected.some((entry) => entry.reason.includes('[ENTRY_INVARIANT]'))).toBe(true);
    });

    it('blocks and records model-gate rejects', async () => {
        const { deps, emitCalls, rejected } = createDeps({
            evaluateModelProbabilityGate: () => ({
                ok: false,
                blocked: true,
                reason: 'probability too low',
                strategy: 'ATOMIC_ARB',
                market_key: 'market-1',
                probability: 0.48,
                gate: 0.55,
                model_loaded: true,
            }),
            preflightFromExecution: jest.fn(async () => null),
        });
        const preflightSpy = deps.preflightFromExecution as jest.Mock;

        const result = await processArbitrageExecutionWorker(buildContext(), deps);

        expect(preflightSpy).not.toHaveBeenCalled();
        expect(result.decision).toBe('REJECTED');
        expect(result.stage).toBe('MODEL_GATE');
        expect(emitCalls.some((entry) => entry.event === 'strategy_model_block')).toBe(true);
        expect(rejected).toHaveLength(1);
        expect(rejected[0]?.stage).toBe('MODEL_GATE');
    });

    it('bypasses preflight/live stages in PAPER mode and accepts immediately', async () => {
        const preflightSpy = jest.fn(async () => buildPreflight());
        const executeSpy = jest.fn(async () => buildLiveExecution());
        const { deps, emitCalls } = createDeps({
            getTradingMode: async () => 'PAPER',
            preflightFromExecution: preflightSpy,
            executeFromExecution: executeSpy,
        });

        const result = await processArbitrageExecutionWorker(buildContext(), deps);

        expect(result.decision).toBe('ACCEPTED');
        expect(result.stage).toBe('NONE');
        expect(preflightSpy).not.toHaveBeenCalled();
        expect(executeSpy).not.toHaveBeenCalled();
        expect(emitCalls.some((entry) => entry.event === 'strategy_preflight')).toBe(false);
    });

    it('handles preflight bypass when there is no polymarket payload', async () => {
        const executeSpy = jest.fn(async () => null);
        const { deps, emitCalls } = createDeps({
            preflightFromExecution: async () => null,
            executeFromExecution: executeSpy,
        });

        const result = await processArbitrageExecutionWorker(buildContext(), deps);

        expect(executeSpy).toHaveBeenCalledTimes(1);
        expect(result.decision).toBe('ACCEPTED');
        expect(emitCalls.some((entry) => entry.event === 'strategy_preflight')).toBe(false);
    });

    it('runs preflight and settlement on successful live execution', async () => {
        const settlementEvents: SettlementEvent[] = [{
            type: 'INFO',
            timestamp: Date.now(),
            message: 'settled',
        }];
        const settlementSpy = jest.fn(async () => settlementEvents);
        const emitSettlementSpy = jest.fn(async () => undefined);
        const { deps, emitCalls } = createDeps({
            registerAtomicExecution: settlementSpy,
            emitSettlementEvents: emitSettlementSpy,
        });

        const result = await processArbitrageExecutionWorker(buildContext(), deps);

        expect(result.decision).toBe('ACCEPTED');
        expect(emitCalls.some((entry) => entry.event === 'strategy_preflight')).toBe(true);
        expect(emitCalls.some((entry) => entry.event === 'strategy_live_execution')).toBe(true);
        expect(settlementSpy).toHaveBeenCalledTimes(1);
        expect(emitSettlementSpy).toHaveBeenCalledWith(settlementEvents);
    });

    it('records preflight failures for replay analysis', async () => {
        const executeSpy = jest.fn(async () => buildLiveExecution());
        const { deps, rejected } = createDeps({
            preflightFromExecution: async () => buildPreflight({
                ok: false,
                failed: 1,
                error: 'signature failed',
            }),
            executeFromExecution: executeSpy,
        });

        const result = await processArbitrageExecutionWorker(buildContext(), deps);

        expect(result.decision).toBe('REJECTED');
        expect(result.stage).toBe('PREFLIGHT');
        expect(executeSpy).not.toHaveBeenCalled();
        expect(rejected.some((entry) => entry.stage === 'PREFLIGHT')).toBe(true);
    });

    it('records preflight throttle outcomes for replay analysis', async () => {
        const executeSpy = jest.fn(async () => buildLiveExecution());
        const { deps, rejected } = createDeps({
            preflightFromExecution: async () => buildPreflight({
                ok: false,
                throttled: true,
                error: 'Preflight throttled by dedupe/min-interval guard',
            }),
            executeFromExecution: executeSpy,
        });

        const result = await processArbitrageExecutionWorker(buildContext(), deps);

        expect(result.decision).toBe('REJECTED');
        expect(result.stage).toBe('PREFLIGHT');
        expect(executeSpy).not.toHaveBeenCalled();
        expect(rejected.some((entry) => entry.stage === 'PREFLIGHT')).toBe(true);
        expect(rejected.some((entry) => entry.reason.includes('throttled'))).toBe(true);
    });

    it('records live execution failures for replay analysis', async () => {
        const { deps, rejected } = createDeps({
            executeFromExecution: async () => buildLiveExecution({
                ok: false,
                failed: 1,
                reason: 'post failed',
            }),
        });

        const result = await processArbitrageExecutionWorker(buildContext(), deps);

        expect(result.decision).toBe('REJECTED');
        expect(result.stage).toBe('LIVE_EXECUTION');
        expect(rejected.some((entry) => entry.stage === 'LIVE_EXECUTION')).toBe(true);
    });

    it('skips settlement registration for dry-run live executions', async () => {
        const settlementSpy = jest.fn(async () => []);
        const { deps } = createDeps({
            executeFromExecution: async () => buildLiveExecution({
                ok: true,
                dryRun: true,
                reason: 'LIVE_ORDER_POSTING_ENABLED=false (safe dry-run)',
            }),
            registerAtomicExecution: settlementSpy,
        });

        const result = await processArbitrageExecutionWorker(buildContext(), deps);

        expect(result.decision).toBe('ACCEPTED');
        expect(settlementSpy).not.toHaveBeenCalled();
    });

    it('records preflight dependency failures without throwing', async () => {
        const { deps, rejected } = createDeps({
            preflightFromExecution: async () => {
                throw new Error('preflight timeout');
            },
        });

        const result = await processArbitrageExecutionWorker(buildContext(), deps);
        expect(result.decision).toBe('REJECTED');
        expect(result.stage).toBe('PREFLIGHT');
        expect(rejected.some((entry) => entry.stage === 'PREFLIGHT')).toBe(true);
        expect(rejected.some((entry) => entry.reason.includes('preflight error'))).toBe(true);
    });

    it('records live execution dependency failures without throwing', async () => {
        const { deps, rejected } = createDeps({
            preflightFromExecution: async () => null,
            executeFromExecution: async () => {
                throw new Error('post timeout');
            },
        });

        const result = await processArbitrageExecutionWorker(buildContext(), deps);
        expect(result.decision).toBe('REJECTED');
        expect(result.stage).toBe('LIVE_EXECUTION');
        expect(rejected.some((entry) => entry.stage === 'LIVE_EXECUTION')).toBe(true);
        expect(rejected.some((entry) => entry.reason.includes('live execution error'))).toBe(true);
    });

    it('records settlement dependency failures without throwing', async () => {
        const { deps, rejected } = createDeps({
            registerAtomicExecution: async () => {
                throw new Error('redis unavailable');
            },
        });

        const result = await processArbitrageExecutionWorker(buildContext(), deps);
        expect(result.decision).toBe('ACCEPTED');
        expect(result.stage).toBe('NONE');
        expect(rejected.some((entry) => entry.stage === 'SETTLEMENT')).toBe(true);
        expect(rejected.some((entry) => entry.reason.includes('settlement error'))).toBe(true);
    });

    it('enqueues settlement DLQ and sends ops alert when settlement registration fails', async () => {
        const enqueueSpy = jest.fn(async () => undefined);
        const notifySpy = jest.fn(async () => undefined);
        const { deps } = createDeps({
            registerAtomicExecution: async () => {
                throw new Error('redis unavailable');
            },
            enqueueSettlementDlq: enqueueSpy,
            notifyOps: notifySpy,
        });

        const result = await processArbitrageExecutionWorker(buildContext(), deps);

        expect(result.decision).toBe('ACCEPTED');
        expect(enqueueSpy).toHaveBeenCalledTimes(1);
        const [enqueueCall] = enqueueSpy.mock.calls as Array<Array<unknown>>;
        const dlqEntry = enqueueCall?.[0] as { execution_id?: string; strategy?: string } | undefined;
        expect(dlqEntry?.execution_id).toBe('exec-1');
        expect(dlqEntry?.strategy).toBe('ATOMIC_ARB');
        expect(notifySpy).toHaveBeenCalledTimes(1);
        const [notifyCall] = notifySpy.mock.calls as Array<Array<unknown>>;
        const alert = notifyCall?.[0] as { scope?: string; severity?: string } | undefined;
        expect(alert?.scope).toBe('SETTLEMENT');
        expect(alert?.severity).toBe('CRITICAL');
    });
});
