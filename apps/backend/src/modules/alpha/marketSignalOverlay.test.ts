import {
    aggregateFundingBasisAsset,
    computeFundingBasisSignal,
    directionalOverlayMultiplier,
    extractPolymarketBookSnapshot,
    computePolymarketMicrostructureSignal,
    type FundingBasisAssetSnapshot,
} from './marketSignalOverlay';

describe('marketSignalOverlay', () => {
    it('aggregates venue snapshots with medians', () => {
        const aggregated = aggregateFundingBasisAsset('btc', [
            { venue: 'okx', funding_rate_8h: 0.0002, basis_bps: 9, updated_at: 10 },
            { venue: 'deribit', funding_rate_8h: 0.0001, basis_bps: 5, updated_at: 11 },
            { venue: 'ignored', funding_rate_8h: null, basis_bps: 1, updated_at: 12 },
        ], 100);
        expect(aggregated.asset).toBe('BTC');
        expect(aggregated.funding_rate_8h).toBeCloseTo(0.00015, 8);
        expect(aggregated.basis_bps).toBe(5);
        expect(aggregated.sample_count).toBe(3);
    });

    it('computes contrarian funding/basis bias', () => {
        const snapshot: FundingBasisAssetSnapshot = {
            asset: 'BTC',
            funding_rate_8h: 0.0003,
            basis_bps: 8,
            sample_count: 2,
            venues: [],
            updated_at: 1,
        };
        const signal = computeFundingBasisSignal(snapshot, {
            crowding_threshold_bps: 5,
            basis_weight: 0.5,
        });
        expect(signal.bias).toBe(-1);
        expect(signal.crowding_bps).toBeCloseTo(7, 6);
        expect(signal.strength).toBe(1);
    });

    it('applies directional overlay multiplier', () => {
        const aligned = directionalOverlayMultiplier(1, 1, 0.5, 1.2, 0.85);
        const opposed = directionalOverlayMultiplier(1, -1, 0.5, 1.2, 0.85);
        expect(aligned).toBeCloseTo(1.1, 6);
        expect(opposed).toBeCloseTo(0.925, 6);
    });

    it('extracts polymarket snapshot from mixed meta fields', () => {
        const snapshot = extractPolymarketBookSnapshot({
            meta_best_ask_yes: 0.64,
            meta_yes_mid: 0.635,
            meta_no_ask: 0.37,
            meta_no_mid: 0.365,
        }, 1234);
        expect(snapshot).not.toBeNull();
        expect(snapshot?.yes_bid).toBeCloseTo(0.63, 6);
        expect(snapshot?.yes_ask).toBeCloseTo(0.64, 6);
        expect(snapshot?.no_bid).toBeCloseTo(0.36, 6);
        expect(snapshot?.no_ask).toBeCloseTo(0.37, 6);
    });

    it('detects directional microstructure sweep impulse', () => {
        const previous = {
            yes_bid: 0.45,
            yes_ask: 0.46,
            no_bid: 0.54,
            no_ask: 0.55,
            timestamp: 1000,
        };
        const current = {
            yes_bid: 0.50,
            yes_ask: 0.51,
            no_bid: 0.49,
            no_ask: 0.50,
            timestamp: 1100,
        };
        const signal = computePolymarketMicrostructureSignal(previous, current, {
            min_shift_bps: 20,
            max_spread_bps: 300,
        });
        expect(signal.direction).toBe(1);
        expect(signal.strength).toBeGreaterThan(0.7);
        expect(signal.reason).toBe('sweep impulse');
    });

    it('suppresses microstructure signal when spread is too wide', () => {
        const current = {
            yes_bid: 0.3,
            yes_ask: 0.45,
            no_bid: 0.45,
            no_ask: 0.6,
            timestamp: 2000,
        };
        const signal = computePolymarketMicrostructureSignal(null, current, {
            min_shift_bps: 20,
            max_spread_bps: 150,
        });
        expect(signal.direction).toBe(0);
        expect(signal.strength).toBe(0);
        expect(signal.reason).toMatch(/spread/);
    });
});
