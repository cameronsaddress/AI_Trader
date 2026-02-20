type IntelligenceGateSnapshot = {
    ok: boolean;
    reason?: string;
};

type AgeField = {
    field: string;
    age_ms: number;
};

export type EntryFreshnessInvariantResult = {
    applicable: boolean;
    ok: boolean;
    reason: string;
    checked_fields: string[];
    max_observed_age_ms: number | null;
    side: string | null;
};

export const DEFAULT_ENTRY_MAX_SOURCE_AGE_MS = Math.max(
    500,
    Number(process.env.EXECUTION_ENTRY_MAX_SOURCE_AGE_MS || '3000'),
);
const DEFAULT_ENTRY_IV_MAX_AGE_MS = Math.max(
    DEFAULT_ENTRY_MAX_SOURCE_AGE_MS,
    Number(process.env.EXECUTION_ENTRY_IV_MAX_AGE_MS || '90000'),
);
const DEFAULT_ENTRY_ML_MAX_AGE_MS = Math.max(
    DEFAULT_ENTRY_MAX_SOURCE_AGE_MS,
    Number(process.env.EXECUTION_ENTRY_ML_MAX_AGE_MS || '90000'),
);
const ENTRY_FRESHNESS_REQUIRE_AGE_FIELDS = process.env.ENTRY_FRESHNESS_REQUIRE_AGE_FIELDS === 'true';

function asRecord(input: unknown): Record<string, unknown> | null {
    if (!input || typeof input !== 'object' || Array.isArray(input)) {
        return null;
    }
    return input as Record<string, unknown>;
}

function asString(input: unknown): string | null {
    if (typeof input !== 'string') {
        return null;
    }
    const trimmed = input.trim();
    return trimmed.length > 0 ? trimmed : null;
}

function asFiniteNumber(input: unknown): number | null {
    const parsed = Number(input);
    return Number.isFinite(parsed) ? parsed : null;
}

function isEntrySide(input: string | null): boolean {
    if (!input) {
        return false;
    }
    return input.toUpperCase().includes('ENTRY');
}

function isStaleGateReason(reason: string | undefined): boolean {
    if (!reason) {
        return false;
    }
    const normalized = reason.toLowerCase();
    return normalized.includes('stale')
        || normalized.includes('missing scan')
        || normalized.includes('no scan')
        || normalized.includes('scan not found');
}

function collectAgeFields(input: Record<string, unknown>, prefix: string, out: AgeField[]): void {
    for (const [key, value] of Object.entries(input)) {
        if (!key.toLowerCase().endsWith('_age_ms')) {
            continue;
        }
        const ageMs = asFiniteNumber(value);
        if (ageMs === null || ageMs < 0) {
            continue;
        }
        out.push({
            field: `${prefix}${key}`,
            age_ms: ageMs,
        });
    }
}

function freshnessBudgetForField(field: string, defaultMaxSourceAgeMs: number): number {
    const normalized = field.toLowerCase();
    if (normalized.endsWith('sigma_iv_age_ms')) {
        return DEFAULT_ENTRY_IV_MAX_AGE_MS;
    }
    if (normalized.endsWith('ml_features_age_ms')) {
        return DEFAULT_ENTRY_ML_MAX_AGE_MS;
    }
    return defaultMaxSourceAgeMs;
}

export function evaluateEntryFreshnessInvariant(
    payload: unknown,
    gate: IntelligenceGateSnapshot,
    maxSourceAgeMs = DEFAULT_ENTRY_MAX_SOURCE_AGE_MS,
): EntryFreshnessInvariantResult {
    const record = asRecord(payload);
    const details = record ? asRecord(record.details) : null;
    const side = asString(record?.side)?.toUpperCase() || null;
    if (!isEntrySide(side)) {
        return {
            applicable: false,
            ok: true,
            reason: 'not an entry execution',
            checked_fields: [],
            max_observed_age_ms: null,
            side,
        };
    }

    const ageFields: AgeField[] = [];
    if (record) {
        collectAgeFields(record, '', ageFields);
    }
    if (details) {
        collectAgeFields(details, 'details.', ageFields);
    }

    const maxObservedAge = ageFields.reduce<number | null>((max, field) => {
        if (max === null || field.age_ms > max) {
            return field.age_ms;
        }
        return max;
    }, null);

    const staleFields = ageFields
        .map((field) => ({
            ...field,
            budget_ms: freshnessBudgetForField(field.field, maxSourceAgeMs),
        }))
        .filter((field) => field.age_ms > field.budget_ms);

    if (staleFields.length > 0) {
        const worstField = staleFields.reduce((worst, current) => {
            if (!worst) {
                return current;
            }
            const worstDelta = worst.age_ms - worst.budget_ms;
            const currentDelta = current.age_ms - current.budget_ms;
            if (currentDelta > worstDelta) {
                return current;
            }
            if (currentDelta === worstDelta && current.age_ms > worst.age_ms) {
                return current;
            }
            return worst;
        }, staleFields[0]);
        return {
            applicable: true,
            ok: false,
            reason: `source age ${worstField.age_ms}ms exceeds ${worstField.budget_ms}ms budget for ${worstField.field}`,
            checked_fields: ageFields.map((field) => {
                const budget = freshnessBudgetForField(field.field, maxSourceAgeMs);
                return `${field.field}=${field.age_ms}/${budget}`;
            }),
            max_observed_age_ms: maxObservedAge,
            side,
        };
    }

    if (ageFields.length === 0) {
        if (!gate.ok && isStaleGateReason(gate.reason)) {
            return {
                applicable: true,
                ok: false,
                reason: `freshness gate rejected entry: ${gate.reason || 'stale scan'}`,
                checked_fields: ['fallback=gate'],
                max_observed_age_ms: null,
                side,
            };
        }
        if (!ENTRY_FRESHNESS_REQUIRE_AGE_FIELDS) {
            return {
                applicable: true,
                ok: true,
                reason: 'entry freshness telemetry missing; accepted via gate fallback',
                checked_fields: ['fallback=gate'],
                max_observed_age_ms: null,
                side,
            };
        }
        return {
            applicable: true,
            ok: false,
            reason: 'missing entry freshness telemetry (*_age_ms fields)',
            checked_fields: [],
            max_observed_age_ms: null,
            side,
        };
    }

    if (!gate.ok && isStaleGateReason(gate.reason)) {
        return {
            applicable: true,
            ok: false,
            reason: `freshness gate rejected entry: ${gate.reason || 'stale scan'}`,
            checked_fields: ageFields.map((field) => {
                const budget = freshnessBudgetForField(field.field, maxSourceAgeMs);
                return `${field.field}=${field.age_ms}/${budget}`;
            }),
            max_observed_age_ms: maxObservedAge,
            side,
        };
    }

    return {
        applicable: true,
        ok: true,
        reason: 'entry freshness invariant satisfied',
        checked_fields: ageFields.map((field) => {
            const budget = freshnessBudgetForField(field.field, maxSourceAgeMs);
            return `${field.field}=${field.age_ms}/${budget}`;
        }),
        max_observed_age_ms: maxObservedAge,
        side,
    };
}
