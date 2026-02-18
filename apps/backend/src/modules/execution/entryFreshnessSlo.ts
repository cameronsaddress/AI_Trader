export type EntryFreshnessViolationCategory = 'MISSING_TELEMETRY' | 'STALE_SOURCE' | 'STALE_GATE' | 'OTHER';

export function classifyEntryFreshnessViolation(reason: string): EntryFreshnessViolationCategory {
    const normalized = reason.toLowerCase();
    if (normalized.includes('missing entry freshness telemetry')) {
        return 'MISSING_TELEMETRY';
    }
    if (
        normalized.includes('freshness gate rejected entry')
        || normalized.includes('stale scan')
        || normalized.includes('missing scan')
        || normalized.includes('scan not found')
    ) {
        return 'STALE_GATE';
    }
    if (normalized.includes('source age') || normalized.includes('exceeds')) {
        return 'STALE_SOURCE';
    }
    return 'OTHER';
}
