import { PolymarketSettlementService } from './PolymarketSettlementService';

class MockRedis {
    public readonly data = new Map<string, string>();

    async get(key: string): Promise<string | null> {
        return this.data.get(key) ?? null;
    }

    async set(key: string, value: string): Promise<void> {
        this.data.set(key, value);
    }
}

type SettlementInternals = {
    positions: Map<string, {
        id: string;
        strategy: string;
        conditionId: string;
        market: string;
        yesTokenId: string;
        noTokenId: string;
        shares: number;
        totalNotionalUsd: number;
        openedAt: number;
        updatedAt: number;
        status: string;
        redeemAttempts: number;
        nextRedeemAttemptAt: number;
        closed?: boolean;
        winnerTokenId?: string;
        redeemableShares?: number;
        settlementCheckedAt?: number;
        redeemTxHash?: string;
        lastError?: string;
        note?: string;
    }>;
    fetchMarketState: (conditionId: string) => Promise<{ closed: boolean; winnerTokenId: string | null }>;
    fetchRedeemableShares: (conditionId: string) => Promise<number>;
    persist: () => Promise<void>;
};

function makePosition(overrides: Partial<SettlementInternals['positions'] extends Map<string, infer T> ? T : never> = {}) {
    return {
        id: 'pos-1',
        strategy: 'ATOMIC_ARB',
        conditionId: '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
        market: 'Polymarket',
        yesTokenId: 'yes-token',
        noTokenId: 'no-token',
        shares: 10,
        totalNotionalUsd: 100,
        openedAt: Date.now(),
        updatedAt: Date.now(),
        status: 'TRACKED',
        redeemAttempts: 0,
        nextRedeemAttemptAt: 0,
        closed: false,
        ...overrides,
    };
}

describe('PolymarketSettlementService.runCycle persistence', () => {
    it('persists when status materially changes (TRACKED -> AWAITING_RESOLUTION)', async () => {
        const redis = new MockRedis();
        const service = new PolymarketSettlementService(redis);
        const internals = service as unknown as SettlementInternals;
        const position = makePosition({ status: 'TRACKED' });
        internals.positions.set(position.id, position);

        internals.fetchMarketState = async () => ({ closed: false, winnerTokenId: null });
        internals.fetchRedeemableShares = async () => 0;

        let persistCalls = 0;
        internals.persist = async () => {
            persistCalls += 1;
        };

        await service.runCycle('PAPER', false);

        expect(position.status).toBe('AWAITING_RESOLUTION');
        expect(persistCalls).toBe(1);
    });

    it('does not persist when no tracked fields change', async () => {
        const redis = new MockRedis();
        const service = new PolymarketSettlementService(redis);
        const internals = service as unknown as SettlementInternals;
        const position = makePosition({ status: 'AWAITING_RESOLUTION' });
        internals.positions.set(position.id, position);

        internals.fetchMarketState = async () => ({ closed: false, winnerTokenId: null });
        internals.fetchRedeemableShares = async () => 0;

        let persistCalls = 0;
        internals.persist = async () => {
            persistCalls += 1;
        };

        await service.runCycle('PAPER', false);

        expect(position.status).toBe('AWAITING_RESOLUTION');
        expect(persistCalls).toBe(0);
    });
});

describe('PolymarketSettlementService.readiness', () => {
    it('reports not ready when settlement service is disabled', () => {
        const redis = new MockRedis();
        const service = new PolymarketSettlementService(redis);
        const readiness = service.getReadinessSnapshot();
        expect(readiness.ready).toBe(false);
        expect(readiness.failures.some((entry) => entry.includes('settlement service disabled'))).toBe(true);
    });

    it('reports auto-redeem misconfiguration when signer cannot redeem directly', () => {
        const redis = new MockRedis();
        const service = new PolymarketSettlementService(redis);
        const mutable = service as unknown as {
            enabled: boolean;
            autoRedeemEnabled: boolean;
            canDirectRedeem: boolean;
            signerAddress: string | null;
            funderAddress: string | null;
        };
        mutable.enabled = true;
        mutable.autoRedeemEnabled = true;
        mutable.canDirectRedeem = false;
        mutable.signerAddress = '0x1111111111111111111111111111111111111111';
        mutable.funderAddress = '0x2222222222222222222222222222222222222222';

        const readiness = service.getReadinessSnapshot();
        expect(readiness.ready).toBe(false);
        expect(readiness.failures.some((entry) => entry.includes('auto-redeem enabled'))).toBe(true);
    });
});
