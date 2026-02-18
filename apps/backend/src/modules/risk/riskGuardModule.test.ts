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

describe('riskGuardModule.resetRiskGuardStates', () => {
    it('clears in-memory and redis risk-guard keys for managed strategies', async () => {
        const redis = new MockRedis();
        const strategyStatus: Record<string, boolean> = { TEST_STRAT: true };

        const module = createRiskGuardModule({
            redis,
            strategyStatus,
            emitStrategyStatusUpdate: () => undefined,
            emitRiskGuardUpdate: () => undefined,
            emitVaultUpdate: () => undefined,
            recordError: () => undefined,
            config: {
                trailingStop: 0,
                consecutiveLossLimit: 99,
                consecutiveLossCooldownMs: 1_000,
                postLossCooldownMs: 2_000,
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

        await module.processRiskGuard('TEST_STRAT', -10, { side: 'BUY' });

        expect(module.getRiskGuardState('TEST_STRAT')).not.toBeNull();
        expect(redis.store.has('risk_guard:state:TEST_STRAT')).toBe(true);
        expect(redis.store.has('risk_guard:cooldown:TEST_STRAT')).toBe(true);
        expect(redis.store.has('risk_guard:size_factor:TEST_STRAT')).toBe(true);

        await module.resetRiskGuardStates();

        expect(module.getRiskGuardState('TEST_STRAT')).toBeNull();
        expect(redis.store.has('risk_guard:state:TEST_STRAT')).toBe(false);
        expect(redis.store.has('risk_guard:cooldown:TEST_STRAT')).toBe(false);
        expect(redis.store.has('risk_guard:size_factor:TEST_STRAT')).toBe(false);
    });
});
