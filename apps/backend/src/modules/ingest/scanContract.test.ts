import { validateArbitrageScanContract } from './scanContract';

describe('validateArbitrageScanContract', () => {
    it('accepts canonical scan payload', () => {
        const result = validateArbitrageScanContract({
            strategy: 'btc_5m',
            timestamp: 1_700_000_000_000,
            score: 0.031,
            threshold: 0.02,
            passes_threshold: true,
            reason: 'edge above threshold',
            signal_type: 'EXPECTED_NET_RETURN',
            unit: 'RATIO',
        });

        expect(result.ok).toBe(true);
        if (!result.ok) {
            return;
        }
        expect(result.value.strategy).toBe('BTC_5M');
        expect(result.value.score).toBeCloseTo(0.031);
        expect(result.value.threshold).toBeCloseTo(0.02);
        expect(result.value.passes_threshold).toBe(true);
    });

    it('derives score from gap and pass/fail from threshold when explicit flag is missing', () => {
        const result = validateArbitrageScanContract({
            strategy: 'GRAPH_ARB',
            timestamp: 1_700_000_000_111,
            gap: 0.012,
            threshold: 0.02,
        });

        expect(result.ok).toBe(true);
        if (!result.ok) {
            return;
        }
        expect(result.value.score).toBeCloseTo(0.012);
        expect(result.value.passes_threshold).toBe(false);
    });

    it('rejects payloads with missing strategy', () => {
        const result = validateArbitrageScanContract({
            score: 0.1,
            threshold: 0.05,
        });

        expect(result.ok).toBe(false);
        if (result.ok) {
            return;
        }
        expect(result.error).toContain('strategy');
    });

    it('rejects payloads with no numeric score/gap', () => {
        const result = validateArbitrageScanContract({
            strategy: 'ATOMIC_ARB',
            threshold: 0.01,
            score: 'not-a-number',
        });

        expect(result.ok).toBe(false);
        if (result.ok) {
            return;
        }
        expect(result.issues.some((issue) => issue.includes('score'))).toBe(true);
    });
});

