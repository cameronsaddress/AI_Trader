import { createRiskGuardModule } from './riskGuardModule';

class MockRedis {
    public readonly store = new Map<string, string>();

    async get(key: string): Promise<string | null> {
        return this.store.get(key) ?? null;
    }

    async set(key: string, value: string): Promise<void> {
        this.store.set(key, value);
    }

    async del(key: string): Promise<void> {
        this.store.delete(key);
    }

    async expire(_key: string, _seconds: number): Promise<void> {
        // no-op
    }

    async incrByFloat(key: string, increment: number): Promise<number> {
        const current = Number(this.store.get(key) || '0');
        const next = current + increment;
        this.store.set(key, String(next));
        return next;
    }
}

function createHarness(
    overrides: Partial<{
        consecutiveLossLimit: number;
        consecutiveLossCooldownMs: number;
        postLossCooldownMs: number;
    }> = {},
) {
    const redis = new MockRedis();
    const strategyStatus: Record<string, boolean> = { TEST_STRAT: true };
    const strategyStatusUpdates: Array<Record<string, boolean>> = [];
    const riskGuardUpdates: Array<{ strategy: string; cumPnl: number; consecutiveLosses: number; pausedUntil: number }> = [];
    const errors: Array<{ scope: string; context?: Record<string, unknown> }> = [];

    const module = createRiskGuardModule({
        redis,
        strategyStatus,
        emitStrategyStatusUpdate: (payload) => {
            strategyStatusUpdates.push({ ...payload });
        },
        emitRiskGuardUpdate: (payload) => {
            riskGuardUpdates.push({
                strategy: payload.strategy,
                cumPnl: payload.cumPnl,
                consecutiveLosses: payload.consecutiveLosses,
                pausedUntil: payload.pausedUntil,
            });
        },
        emitVaultUpdate: () => undefined,
        recordError: (scope, _error, context) => {
            errors.push({ scope, context });
        },
        config: {
            trailingStop: 0,
            consecutiveLossLimit: overrides.consecutiveLossLimit ?? 99,
            consecutiveLossCooldownMs: overrides.consecutiveLossCooldownMs ?? 1_000,
            postLossCooldownMs: overrides.postLossCooldownMs ?? 2_000,
            antiMartingaleAfter: 1,
            antiMartingaleFactor: 0.5,
            directionLimit: 99,
            profitTaperStart: 0,
            profitTaperFloor: 0.5,
            dailyTarget: 0,
            dayResetHourUtc: 0,
            strategies: new Set(['TEST_STRAT']),
            vaultEnabled: false,
            vaultBankrollCeiling: 0,
            vaultRedisKey: 'vault:key',
            simLedgerCashKey: 'sim_ledger:cash',
            simBankrollKey: 'sim_bankroll',
        },
    });

    return {
        module,
        redis,
        strategyStatus,
        strategyStatusUpdates,
        riskGuardUpdates,
        errors,
    };
}

describe('riskGuardModule', () => {
    afterEach(() => {
        jest.useRealTimers();
    });

    it('clears in-memory and redis risk-guard keys for managed strategies', async () => {
        const harness = createHarness();
        await harness.module.processRiskGuard('TEST_STRAT', -10, { side: 'BUY' });
        harness.redis.store.set('risk_guard:paused_until:TEST_STRAT', String(Date.now() + 60_000));

        expect(harness.module.getRiskGuardState('TEST_STRAT')).not.toBeNull();
        expect(harness.redis.store.has('risk_guard:state:TEST_STRAT')).toBe(true);
        expect(harness.redis.store.has('risk_guard:cooldown:TEST_STRAT')).toBe(true);
        expect(harness.redis.store.has('risk_guard:size_factor:TEST_STRAT')).toBe(true);
        expect(harness.redis.store.has('risk_guard:paused_until:TEST_STRAT')).toBe(true);

        await harness.module.resetRiskGuardStates();

        expect(harness.module.getRiskGuardState('TEST_STRAT')).toBeNull();
        expect(harness.redis.store.has('risk_guard:state:TEST_STRAT')).toBe(false);
        expect(harness.redis.store.has('risk_guard:cooldown:TEST_STRAT')).toBe(false);
        expect(harness.redis.store.has('risk_guard:size_factor:TEST_STRAT')).toBe(false);
        expect(harness.redis.store.has('risk_guard:paused_until:TEST_STRAT')).toBe(false);
    });

    it('does not auto-resume from a stale timer after reset', async () => {
        jest.useFakeTimers();
        const harness = createHarness({
            consecutiveLossLimit: 1,
            consecutiveLossCooldownMs: 100,
            postLossCooldownMs: 0,
        });

        await harness.module.processRiskGuard('TEST_STRAT', -10, { side: 'BUY' });
        expect(harness.strategyStatus.TEST_STRAT).toBe(false);
        expect(harness.redis.store.get('strategy:enabled:TEST_STRAT')).toBe('0');
        expect(harness.redis.store.has('risk_guard:paused_until:TEST_STRAT')).toBe(true);

        await harness.module.resetRiskGuardStates();
        expect(harness.module.getRiskGuardState('TEST_STRAT')).toBeNull();
        expect(harness.redis.store.has('risk_guard:paused_until:TEST_STRAT')).toBe(false);

        jest.advanceTimersByTime(150);
        await Promise.resolve();
        await Promise.resolve();

        expect(harness.strategyStatus.TEST_STRAT).toBe(false);
        expect(harness.redis.store.get('strategy:enabled:TEST_STRAT')).toBe('0');
        expect(harness.strategyStatusUpdates.every((entry) => entry.TEST_STRAT === false)).toBe(true);
        expect(harness.errors).toEqual([]);
    });

    it('cancels auto-resume when pause is cleared manually', async () => {
        jest.useFakeTimers();
        const harness = createHarness({
            consecutiveLossLimit: 1,
            consecutiveLossCooldownMs: 100,
            postLossCooldownMs: 0,
        });

        await harness.module.processRiskGuard('TEST_STRAT', -10, { side: 'BUY' });
        expect(harness.strategyStatus.TEST_STRAT).toBe(false);
        expect(harness.redis.store.get('strategy:enabled:TEST_STRAT')).toBe('0');
        expect(harness.redis.store.has('risk_guard:paused_until:TEST_STRAT')).toBe(true);

        await harness.module.clearStrategyPause('TEST_STRAT');

        const state = harness.module.getRiskGuardState('TEST_STRAT');
        expect(state?.pausedUntil).toBe(0);
        expect(harness.redis.store.has('risk_guard:paused_until:TEST_STRAT')).toBe(false);
        expect(harness.redis.store.has('risk_guard:cooldown:TEST_STRAT')).toBe(false);

        jest.advanceTimersByTime(150);
        await Promise.resolve();
        await Promise.resolve();

        expect(harness.strategyStatus.TEST_STRAT).toBe(false);
        expect(harness.redis.store.get('strategy:enabled:TEST_STRAT')).toBe('0');
        expect(harness.errors).toEqual([]);
    });

    it('persists and emits guard state updates while strategy is already paused', async () => {
        const harness = createHarness({
            consecutiveLossLimit: 1,
            consecutiveLossCooldownMs: 1_000,
            postLossCooldownMs: 0,
        });

        await harness.module.processRiskGuard('TEST_STRAT', -10, { side: 'BUY' });
        await harness.module.processRiskGuard('TEST_STRAT', -10, { side: 'BUY' });

        const stateRaw = harness.redis.store.get('risk_guard:state:TEST_STRAT');
        expect(stateRaw).toBeTruthy();
        const state = JSON.parse(String(stateRaw)) as { cumPnl: number; consecutiveLosses: number; pausedUntil: number };
        expect(state.cumPnl).toBe(-20);
        expect(state.consecutiveLosses).toBe(2);
        expect(state.pausedUntil).toBeGreaterThan(0);

        expect(harness.riskGuardUpdates.length).toBeGreaterThanOrEqual(2);
        const latest = harness.riskGuardUpdates[harness.riskGuardUpdates.length - 1];
        expect(latest?.cumPnl).toBe(-20);
        expect(latest?.consecutiveLosses).toBe(2);
    });
});
