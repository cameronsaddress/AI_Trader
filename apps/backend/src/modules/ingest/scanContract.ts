import { z } from 'zod';

export type NormalizedArbitrageScan = {
    strategy: string;
    timestamp: number;
    score: number;
    threshold: number;
    passes_threshold: boolean;
    reason: string;
    symbol?: string;
    market_id?: string;
    market_key?: string;
    signal_type?: string;
    unit?: string;
    metric_family?: string;
    directionality?: string;
    comparable_group?: string;
    meta?: Record<string, unknown>;
};

export type ScanContractValidationResult =
    | {
        ok: true;
        value: NormalizedArbitrageScan;
    }
    | {
        ok: false;
        error: string;
        issues: string[];
    };

const finiteNumber = z.union([
    z.number(),
    z.string(),
]).transform((input, ctx) => {
    const parsed = Number(input);
    if (!Number.isFinite(parsed)) {
        ctx.addIssue({ code: z.ZodIssueCode.custom, message: `expected finite number, received "${String(input)}"` });
        return z.NEVER;
    }
    return parsed;
});

const scanContractSchema = z.object({
    strategy: z.string().trim().min(1),
    timestamp: finiteNumber.optional(),
    score: finiteNumber.optional(),
    gap: finiteNumber.optional(),
    threshold: finiteNumber.optional(),
    passes_threshold: z.boolean().optional(),
    reason: z.string().optional(),
    symbol: z.string().optional(),
    market_id: z.string().optional(),
    market_key: z.string().optional(),
    signal_type: z.string().optional(),
    unit: z.string().optional(),
    metric_family: z.string().optional(),
    directionality: z.string().optional(),
    comparable_group: z.string().optional(),
    meta: z.record(z.unknown()).optional(),
}).passthrough().superRefine((value, ctx) => {
    if (typeof value.score !== 'number' && typeof value.gap !== 'number') {
        ctx.addIssue({
            code: z.ZodIssueCode.custom,
            path: ['score'],
            message: 'missing numeric score/gap',
        });
    }
});

function formatIssues(error: z.ZodError): string[] {
    return error.issues.map((issue) => {
        const path = issue.path.length > 0 ? `${issue.path.join('.')}: ` : '';
        return `${path}${issue.message}`;
    });
}

function optionalText(input: unknown): string | undefined {
    if (typeof input !== 'string') {
        return undefined;
    }
    const trimmed = input.trim();
    return trimmed.length > 0 ? trimmed : undefined;
}

export function validateArbitrageScanContract(payload: unknown): ScanContractValidationResult {
    const parsed = scanContractSchema.safeParse(payload);
    if (!parsed.success) {
        const issues = formatIssues(parsed.error);
        return {
            ok: false,
            error: issues[0] || 'invalid scan payload',
            issues,
        };
    }

    const value = parsed.data;
    const score = typeof value.score === 'number'
        ? value.score
        : typeof value.gap === 'number'
            ? value.gap
            : 0;
    const threshold = typeof value.threshold === 'number' ? value.threshold : 0;
    const passesThreshold = typeof value.passes_threshold === 'boolean'
        ? value.passes_threshold
        : score >= threshold;

    return {
        ok: true,
        value: {
            strategy: value.strategy.trim().toUpperCase(),
            timestamp: typeof value.timestamp === 'number' ? value.timestamp : Date.now(),
            score,
            threshold,
            passes_threshold: passesThreshold,
            reason: optionalText(value.reason) || '',
            symbol: optionalText(value.symbol),
            market_id: optionalText(value.market_id),
            market_key: optionalText(value.market_key),
            signal_type: optionalText(value.signal_type),
            unit: optionalText(value.unit),
            metric_family: optionalText(value.metric_family),
            directionality: optionalText(value.directionality),
            comparable_group: optionalText(value.comparable_group),
            meta: value.meta,
        },
    };
}

