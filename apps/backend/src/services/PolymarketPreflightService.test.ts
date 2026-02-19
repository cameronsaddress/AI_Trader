jest.mock('@polymarket/clob-client', () => {
    class MockClobClient {
        public constructor() {}
    }

    return {
        ClobClient: MockClobClient,
        OrderType: {
            FOK: 'FOK',
            FAK: 'FAK',
            GTC: 'GTC',
            GTD: 'GTD',
        },
        Side: {
            BUY: 'BUY',
            SELL: 'SELL',
        },
    };
});

import { PolymarketPreflightService } from './PolymarketPreflightService';

class MockDedupeStore {
    private readonly entries = new Map<string, { value: string; expiresAt: number | null }>();

    public async set(
        key: string,
        value: string,
        options?: { NX?: boolean; PX?: number },
    ): Promise<'OK' | null> {
        const now = Date.now();
        const existing = this.entries.get(key);
        if (existing && (existing.expiresAt === null || existing.expiresAt > now)) {
            if (options?.NX) {
                return null;
            }
        }

        const ttl = typeof options?.PX === 'number' && Number.isFinite(options.PX) ? Math.max(1, options.PX) : null;
        this.entries.set(key, {
            value,
            expiresAt: ttl === null ? null : now + ttl,
        });
        return 'OK';
    }
}

class ThrowingDedupeStore {
    async set(
        _key: string,
        _value: string,
        _options?: { NX?: boolean; PX?: number },
    ): Promise<never> {
        throw new Error('redis unavailable');
    }
}

const ORIGINAL_ENV = { ...process.env };

function buildExecutionPayload(strategy = 'BTC_5M'): Record<string, unknown> {
    return {
        mode: 'LIVE_DRY_RUN',
        market: 'Polymarket',
        timestamp: Date.now(),
        details: {
            preflight: {
                strategy,
                orders: [
                    {
                        token_id: '1001',
                        condition_id: '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                        side: 'BUY',
                        price: 0.44,
                        size: 50,
                        size_unit: 'USD_NOTIONAL',
                    },
                ],
            },
        },
    };
}

describe('PolymarketPreflightService redis dedupe', () => {
    beforeEach(() => {
        process.env = { ...ORIGINAL_ENV };
        process.env.POLY_REDIS_DEDUPE_ENABLED = 'true';
        process.env.POLY_PREFLIGHT_MIN_INTERVAL_MS = '0';
        process.env.POLY_EXECUTION_MIN_INTERVAL_MS = '0';
        process.env.POLY_PREFLIGHT_REDIS_DEDUPE_TTL_MS = '120000';
        process.env.POLY_EXECUTION_REDIS_DEDUPE_TTL_MS = '120000';
        process.env.POLY_REDIS_DEDUPE_PREFIX = 'test:poly:dedupe';
        process.env.LIVE_ORDER_POSTING_ENABLED = 'false';
    });

    afterAll(() => {
        process.env = ORIGINAL_ENV;
    });

    it('suppresses duplicate preflight after service restart via shared redis dedupe key', async () => {
        const dedupe = new MockDedupeStore();
        const payload = buildExecutionPayload();

        const first = new PolymarketPreflightService(dedupe);
        const firstResult = await first.preflightFromExecution(payload);
        expect(firstResult).not.toBeNull();
        expect(firstResult?.throttled).toBeUndefined();

        const restarted = new PolymarketPreflightService(dedupe);
        const secondResult = await restarted.preflightFromExecution(payload);
        expect(secondResult).not.toBeNull();
        expect(secondResult?.throttled).toBe(true);
        expect(secondResult?.ok).toBe(false);
    });

    it('suppresses duplicate live-execution posting attempt after restart via redis dedupe key', async () => {
        const dedupe = new MockDedupeStore();
        const payload = buildExecutionPayload('ATOMIC_ARB');

        const first = new PolymarketPreflightService(dedupe);
        const firstResult = await first.executeFromExecution(payload, 'LIVE');
        expect(firstResult).not.toBeNull();
        expect(firstResult?.throttled).toBeUndefined();

        const restarted = new PolymarketPreflightService(dedupe);
        const secondResult = await restarted.executeFromExecution(payload, 'LIVE');
        expect(secondResult).not.toBeNull();
        expect(secondResult?.throttled).toBe(true);
        expect(secondResult?.ok).toBe(false);
    });

    it('continues when redis dedupe store throws (degrades open)', async () => {
        const dedupe = new ThrowingDedupeStore();
        const payload = buildExecutionPayload('ATOMIC_ARB');
        const service = new PolymarketPreflightService(dedupe);

        await expect(service.preflightFromExecution(payload)).resolves.not.toBeNull();
        await expect(service.executeFromExecution(payload, 'LIVE')).resolves.not.toBeNull();
    });

    it('returns explicit bypass outcome when trading mode is PAPER', async () => {
        const dedupe = new MockDedupeStore();
        const payload = buildExecutionPayload('ATOMIC_ARB');
        const service = new PolymarketPreflightService(dedupe);

        const result = await service.executeFromExecution(payload, 'PAPER');
        expect(result).not.toBeNull();
        expect(result?.bypassed).toBe(true);
        expect(result?.dryRun).toBe(true);
        expect(result?.ok).toBe(true);
    });
});
