import { classifyEntryFreshnessViolation } from './entryFreshnessSlo';

describe('classifyEntryFreshnessViolation', () => {
    it('classifies missing telemetry', () => {
        const category = classifyEntryFreshnessViolation('[ENTRY_INVARIANT] missing entry freshness telemetry (*_age_ms fields)');
        expect(category).toBe('MISSING_TELEMETRY');
    });

    it('classifies stale gate reasons before generic exceeds wording', () => {
        const category = classifyEntryFreshnessViolation(
            '[ENTRY_INVARIANT] freshness gate rejected entry: stale scan age 4100 exceeds 3000',
        );
        expect(category).toBe('STALE_GATE');
    });

    it('classifies stale source age checks', () => {
        const category = classifyEntryFreshnessViolation('[ENTRY_INVARIANT] source age 4100ms exceeds 3000ms');
        expect(category).toBe('STALE_SOURCE');
    });

    it('falls back to OTHER for unmatched reasons', () => {
        const category = classifyEntryFreshnessViolation('[ENTRY_INVARIANT] unexpected invariant branch');
        expect(category).toBe('OTHER');
    });
});
