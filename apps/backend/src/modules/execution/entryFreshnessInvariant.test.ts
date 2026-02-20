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
        expect(result.reason).toContain('spot_age_ms');
        expect(result.max_observed_age_ms).toBe(4_100);
    });

    it('allows slower IV telemetry while enforcing tight spot/book freshness', () => {
        const result = evaluateEntryFreshnessInvariant(
            { side: 'ENTRY', details: { spot_age_ms: 900, sigma_iv_age_ms: 8_500 } },
            { ok: true },
            3_000,
        );
        expect(result.applicable).toBe(true);
        expect(result.ok).toBe(true);
    });

    it('fails when intelligence gate rejects entry due to staleness', () => {
        const result = evaluateEntryFreshnessInvariant(
            { side: 'ENTRY', details: { spot_age_ms: 900 } },
            { ok: false, reason: 'Scan stale for BTC_5M' },
            3_000,
        );
        expect(result.applicable).toBe(true);
        expect(result.ok).toBe(false);
        expect(result.reason).toContain('freshness gate rejected entry');
    });

    it('falls back to gate health when explicit age fields are missing', () => {
        const result = evaluateEntryFreshnessInvariant(
            { side: 'ENTRY', details: { expected_profit: 1.2 } },
            { ok: true },
            3_000,
        );
        expect(result.applicable).toBe(true);
        expect(result.ok).toBe(true);
        expect(result.reason).toContain('accepted via gate fallback');
        expect(result.checked_fields).toContain('fallback=gate');
    });

    it('passes entry when at least one age field is fresh', () => {
        const result = evaluateEntryFreshnessInvariant(
            { side: 'ENTRY_SHORT', details: { book_age_ms: 1_250, expected_profit: 1.2 } },
            { ok: true },
            3_000,
        );
        expect(result.applicable).toBe(true);
        expect(result.ok).toBe(true);
    });
});
