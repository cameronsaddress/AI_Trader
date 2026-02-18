import { evaluateEntryFreshnessInvariant } from './entryFreshnessInvariant';

describe('evaluateEntryFreshnessInvariant', () => {
    it('passes for non-entry executions', () => {
        const result = evaluateEntryFreshnessInvariant(
            { side: 'SETTLEMENT', details: { spot_age_ms: 50_000 } },
            { ok: true },
            3_000,
        );
        expect(result.applicable).toBe(false);
        expect(result.ok).toBe(true);
    });

    it('fails when observed source age exceeds configured max', () => {
        const result = evaluateEntryFreshnessInvariant(
            { side: 'ENTRY', details: { spot_age_ms: 4_100, book_age_ms: 2_500 } },
            { ok: true },
            3_000,
        );
        expect(result.applicable).toBe(true);
        expect(result.ok).toBe(false);
        expect(result.reason).toContain('exceeds');
        expect(result.max_observed_age_ms).toBe(4_100);
    });

    it('fails when intelligence gate rejects entry due to staleness', () => {
        const result = evaluateEntryFreshnessInvariant(
            { side: 'ENTRY' },
            { ok: false, reason: 'Scan stale for BTC_5M' },
            3_000,
        );
        expect(result.applicable).toBe(true);
        expect(result.ok).toBe(false);
        expect(result.reason).toContain('freshness gate rejected entry');
    });

    it('passes entry without explicit age fields when gate is healthy', () => {
        const result = evaluateEntryFreshnessInvariant(
            { side: 'ENTRY', details: { expected_profit: 1.2 } },
            { ok: true },
            3_000,
        );
        expect(result.applicable).toBe(true);
        expect(result.ok).toBe(true);
    });
});

