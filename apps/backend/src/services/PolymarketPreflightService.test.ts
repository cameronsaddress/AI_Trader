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

function buildPairedExecutionPayload(strategy = 'ATOMIC_ARB'): Record<string, unknown> {
    return {
        mode: 'LIVE_DRY_RUN',
        market: 'Polymarket',
        timestamp: Date.now(),
        details: {
            preflight: {
                strategy,
                orders: [
                    {
                        token_id: 'yes-token',
                        condition_id: '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                        side: 'BUY',
                        price: 0.44,
                        size: 40,
                        size_unit: 'USD_NOTIONAL',
                    },
                    {
                        token_id: 'no-token',
                        condition_id: '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                        side: 'BUY',
                        price: 0.56,
                        size: 40,
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
        process.env.POLY_EXECUTION_SLIPPAGE_CHECK_ENABLED = 'true';
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

    it('blocks live execution when slippage guard detects excessive drift', async () => {
        process.env.LIVE_ORDER_POSTING_ENABLED = 'true';
        process.env.POLY_EXECUTION_SLIPPAGE_MAX_BPS = '5';
        process.env.POLY_EXECUTION_SLIPPAGE_MIN_ABS = '0.001';
        const service = new PolymarketPreflightService(new MockDedupeStore());
        const postOrder = jest.fn(async () => ({ orderID: 'order-1', success: true }));
        (service as unknown as {
            ensureClient: () => Promise<boolean>;
            client: unknown;
        }).ensureClient = async () => true;
        (service as unknown as { client: unknown }).client = {
            getTickSize: async () => '0.01',
            getNegRisk: async () => false,
            createOrder: async () => ({ signature: '0xabc123', maker: 'maker', signer: 'signer' }),
            getOrderBook: async () => ({
                asks: [{ price: '0.60', size: '100' }],
                bids: [{ price: '0.40', size: '100' }],
            }),
            postOrder,
            cancelOrders: async () => ({ success: true }),
        };

        const result = await service.executeFromExecution(buildExecutionPayload('BTC_5M'), 'LIVE');

        expect(result).not.toBeNull();
        expect(result?.ok).toBe(false);
        expect(result?.slippageBlocked).toBe(true);
        expect(result?.failed).toBe(1);
        expect(result?.dryRun).toBe(true);
        expect(result?.reason?.toLowerCase()).toContain('slippage guard');
        expect(postOrder).not.toHaveBeenCalled();
    });

    it('blocks live execution when depth guard finds insufficient size within limit', async () => {
        process.env.LIVE_ORDER_POSTING_ENABLED = 'true';
        process.env.POLY_EXECUTION_SLIPPAGE_MAX_BPS = '100';
        process.env.POLY_EXECUTION_SLIPPAGE_MIN_ABS = '0.01';
        process.env.POLY_EXECUTION_DEPTH_CHECK_ENABLED = 'true';
        process.env.POLY_EXECUTION_MIN_BOOK_FILL_RATIO = '1.0';
        const service = new PolymarketPreflightService(new MockDedupeStore());
        const postOrder = jest.fn(async () => ({ orderID: 'order-1', success: true }));
        (service as unknown as {
            ensureClient: () => Promise<boolean>;
            client: unknown;
        }).ensureClient = async () => true;
        (service as unknown as { client: unknown }).client = {
            getTickSize: async () => '0.01',
            getNegRisk: async () => false,
            createOrder: async () => ({ signature: '0xabc123', maker: 'maker', signer: 'signer' }),
            getOrderBook: async () => ({
                asks: [{ price: '0.4410', size: '10' }],
                bids: [{ price: '0.4390', size: '100' }],
            }),
            postOrder,
            cancelOrders: async () => ({ success: true }),
        };

        const result = await service.executeFromExecution(buildExecutionPayload('BTC_5M'), 'LIVE');

        expect(result).not.toBeNull();
        expect(result?.ok).toBe(false);
        expect(result?.failed).toBe(1);
        expect(result?.reason?.toLowerCase()).toContain('depth guard');
        expect(postOrder).not.toHaveBeenCalled();
    });

    it('allows execution when depth is sufficient within slippage limit', async () => {
        process.env.LIVE_ORDER_POSTING_ENABLED = 'true';
        process.env.POLY_EXECUTION_SLIPPAGE_MAX_BPS = '100';
        process.env.POLY_EXECUTION_SLIPPAGE_MIN_ABS = '0.01';
        process.env.POLY_EXECUTION_DEPTH_CHECK_ENABLED = 'true';
        process.env.POLY_EXECUTION_MIN_BOOK_FILL_RATIO = '1.0';
        const service = new PolymarketPreflightService(new MockDedupeStore());
        const postOrder = jest.fn(async () => ({ orderID: 'order-1', status: 'filled', success: true }));
        (service as unknown as {
            ensureClient: () => Promise<boolean>;
            client: unknown;
        }).ensureClient = async () => true;
        (service as unknown as { client: unknown }).client = {
            getTickSize: async () => '0.01',
            getNegRisk: async () => false,
            createOrder: async () => ({ signature: '0xabc123', maker: 'maker', signer: 'signer' }),
            getOrderBook: async () => ({
                asks: [
                    { price: '0.4410', size: '60' },
                    { price: '0.4450', size: '80' },
                ],
                bids: [{ price: '0.4390', size: '100' }],
            }),
            postOrder,
            cancelOrders: async () => ({ success: true }),
        };

        const result = await service.executeFromExecution(buildExecutionPayload('BTC_5M'), 'LIVE');

        expect(result).not.toBeNull();
        expect(result?.ok).toBe(true);
        expect(result?.posted).toBe(1);
        expect(postOrder).toHaveBeenCalledTimes(1);
    });

    it('attempts rollback when paired execution partially fills', async () => {
        process.env.LIVE_ORDER_POSTING_ENABLED = 'true';
        const service = new PolymarketPreflightService(new MockDedupeStore());
        const postOrder = jest.fn()
            .mockResolvedValueOnce({ orderID: 'ord-1', status: 'live', success: true })
            .mockRejectedValueOnce(new Error('insufficient liquidity'));
        const cancelOrders = jest.fn(async (_ids: string[]) => ({ success: true }));
        (service as unknown as {
            ensureClient: () => Promise<boolean>;
            client: unknown;
        }).ensureClient = async () => true;
        (service as unknown as { client: unknown }).client = {
            getTickSize: async () => '0.01',
            getNegRisk: async () => false,
            createOrder: async (order: { tokenID: string; price: number; size: number; side: string }) => ({
                tokenID: order.tokenID,
                price: order.price,
                size: order.size,
                side: order.side,
                signature: '0xabc123',
                maker: 'maker',
                signer: 'signer',
            }),
            getOrderBook: async () => ({
                asks: [{ price: '0.4400', size: '100' }],
                bids: [{ price: '0.4300', size: '100' }],
            }),
            postOrder,
            cancelOrders,
        };

        const result = await service.executeFromExecution(buildPairedExecutionPayload(), 'LIVE');

        expect(result).not.toBeNull();
        expect(result?.ok).toBe(false);
        expect(result?.posted).toBe(1);
        expect(result?.failed).toBe(1);
        expect(result?.rollbackAttempted).toBe(true);
        expect(result?.rollbackCancelled).toBe(1);
        expect(result?.rollbackFailed).toBe(0);
        expect(cancelOrders).toHaveBeenCalledWith(['ord-1']);
    });
});
