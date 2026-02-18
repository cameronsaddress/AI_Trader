import { createLedgerHealthModule } from './ledgerHealthModule';

class MockRedis {
    public readonly store = new Map<string, string>();
    public failGetForKey: string | null = null;

    async get(key: string): Promise<string | null> {
        if (this.failGetForKey && key === this.failGetForKey) {
            throw new Error(`forced get failure for ${key}`);
        }
        return this.store.get(key) ?? null;
    }

    async set(key: string, value: string): Promise<void> {
        this.store.set(key, value);
    }

    async del(keys: string[] | string): Promise<void> {
        const list = Array.isArray(keys) ? keys : [keys];
        for (const key of list) {
            this.store.delete(key);
        }
    }

    async *scanIterator(options: { MATCH: string; COUNT: number }): AsyncIterable<string> {
        void options.COUNT;
        const prefix = options.MATCH.endsWith('*')
            ? options.MATCH.slice(0, -1)
            : options.MATCH;
        for (const key of this.store.keys()) {
            if (key.startsWith(prefix)) {
                yield key;
            }
        }
    }
}

function createHarness() {
    const redis = new MockRedis();
    const updates: Array<{ status: string; issues: string[] }> = [];
    const runtimeTouches: Array<{ health: 'ONLINE' | 'DEGRADED'; detail: string }> = [];
    const errors: Array<{ scope: string; context?: Record<string, unknown> }> = [];

    const module = createLedgerHealthModule({
        redis,
        config: {
            defaultSimBankroll: 1000,
            simBankrollKey: 'sim:bankroll',
            simLedgerCashKey: 'sim:cash',
            simLedgerReservedKey: 'sim:reserved',
            simLedgerRealizedPnlKey: 'sim:realized',
            reservedByStrategyPrefix: 'sim:reserved:strategy:',
            reservedByFamilyPrefix: 'sim:reserved:family:',
            reservedByUnderlyingPrefix: 'sim:reserved:underlying:',
            strategyConcentrationCapPct: 0.35,
            familyConcentrationCapPct: 0.60,
            underlyingConcentrationCapPct: 0.70,
            globalUtilizationCapPct: 0.90,
        },
        emitLedgerHealthUpdate: (payload) => {
            updates.push({
                status: payload.status,
                issues: payload.issues.slice(),
            });
        },
        touchLedgerRuntime: (health, detail) => {
            runtimeTouches.push({ health, detail });
        },
        recordError: (scope, _error, context) => {
            errors.push({ scope, context });
        },
    });

    return {
        module,
        redis,
        updates,
        runtimeTouches,
        errors,
    };
}

describe('ledgerHealthModule.refreshLedgerHealth', () => {
    it('reports critical mismatch when reserved maps drift from ledger reserved total', async () => {
        const harness = createHarness();
        harness.redis.store.set('sim:bankroll', '1000');
        harness.redis.store.set('sim:cash', '900');
        harness.redis.store.set('sim:reserved', '100');
        harness.redis.store.set('sim:realized', '0');
        harness.redis.store.set('sim:reserved:strategy:BTC_5M', '50');
        harness.redis.store.set('sim:reserved:family:FAIR_VALUE', '100');
        harness.redis.store.set('sim:reserved:underlying:BTC', '100');

        await harness.module.refreshLedgerHealth();

        expect(harness.updates.length).toBe(1);
        expect(harness.updates[0]?.status).toBe('CRITICAL');
        expect(harness.updates[0]?.issues.some((issue) => issue.includes('strategy reserved mismatch'))).toBe(true);
        expect(harness.runtimeTouches[harness.runtimeTouches.length - 1]?.health).toBe('DEGRADED');
    });

    it('degrades gracefully when refresh fails unexpectedly', async () => {
        const harness = createHarness();
        harness.redis.failGetForKey = 'sim:bankroll';

        await expect(harness.module.refreshLedgerHealth()).resolves.toBeUndefined();

        expect(harness.errors.some((entry) => entry.scope === 'Ledger.Refresh')).toBe(true);
        expect(harness.updates.length).toBe(1);
        expect(harness.updates[0]?.status).toBe('CRITICAL');
        expect(harness.updates[0]?.issues[0]).toContain('ledger refresh failed');
        expect(harness.runtimeTouches[harness.runtimeTouches.length - 1]?.health).toBe('DEGRADED');
    });
});
