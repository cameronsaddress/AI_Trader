import type { FeatureSnapshot, StrategyTradeSample } from '../../types/backend-types';
import type { RejectedSignalRecord } from '../execution/arbExecutionPipeline';

export type ModelGateCalibrationSample = {
    probability: number;
    pnl: number;
    accepted: boolean;
    execution_id: string;
    strategy: string;
    timestamp: number;
};

type TradeOutcome = {
    execution_id: string;
    pnl: number;
    strategy: string;
    market_key: string | null;
    timestamp: number;
};

type CounterfactualFeatureRow = {
    row_id: string;
    strategy: string;
    market_key: string;
    timestamp: number;
    label_timestamp: number;
    net_return: number;
};

export type BuildModelGateCalibrationSamplesInput = {
    now: number;
    lookbackMs: number;
    counterfactualWindowMs: number;
    strategyIds: readonly string[];
    strategyTradeSamples: Record<string, StrategyTradeSample[]>;
    featureRegistryRows: FeatureSnapshot[];
    rejectedSignalHistory: RejectedSignalRecord[];
    probabilityForExecutionId: (executionId: string) => number | null;
    clampProbability: (value: number) => number;
    fallbackNotionalUsd?: number;
};

function buildStrategyMarketKey(strategy: string, marketKey: string): string {
    return `${strategy}::${marketKey}`;
}

function parseProbabilityFromRejectedPayload(payload: Record<string, unknown> | null): number | null {
    if (!payload) {
        return null;
    }
    const raw = Number(payload.probability);
    return Number.isFinite(raw) ? raw : null;
}

export function buildModelGateCalibrationSamples(
    input: BuildModelGateCalibrationSamplesInput,
): ModelGateCalibrationSample[] {
    const lookbackStart = input.now - input.lookbackMs;
    const fallbackNotionalUsd = Number.isFinite(input.fallbackNotionalUsd)
        ? Math.max(1, Number(input.fallbackNotionalUsd))
        : 100;

    const outcomesByExecutionId = new Map<string, TradeOutcome>();
    const strategyNotionalStats = new Map<string, { sum: number; count: number }>();
    const featureCounterfactualsByKey = new Map<string, CounterfactualFeatureRow[]>();

    for (const strategyId of input.strategyIds) {
        const samples = input.strategyTradeSamples[strategyId] || [];
        for (const sample of samples) {
            if (sample.timestamp < lookbackStart || !Number.isFinite(sample.pnl)) {
                continue;
            }

            if (Number.isFinite(sample.notional) && sample.notional > 0) {
                const stats = strategyNotionalStats.get(sample.strategy) || { sum: 0, count: 0 };
                stats.sum += sample.notional;
                stats.count += 1;
                strategyNotionalStats.set(sample.strategy, stats);
            }

            if (!sample.execution_id) {
                continue;
            }
            const existing = outcomesByExecutionId.get(sample.execution_id);
            if (!existing || sample.timestamp >= existing.timestamp) {
                outcomesByExecutionId.set(sample.execution_id, {
                    execution_id: sample.execution_id,
                    pnl: sample.pnl,
                    strategy: sample.strategy,
                    market_key: sample.market_key || null,
                    timestamp: sample.timestamp,
                });
            }
        }
    }

    const avgNotionalByStrategy = new Map<string, number>();
    for (const [strategy, stats] of strategyNotionalStats.entries()) {
        if (stats.count > 0 && Number.isFinite(stats.sum) && stats.sum > 0) {
            avgNotionalByStrategy.set(strategy, stats.sum / stats.count);
        }
    }

    for (const row of input.featureRegistryRows) {
        if (row.timestamp < lookbackStart) {
            continue;
        }
        if (row.label_net_return === null || !Number.isFinite(row.label_net_return)) {
            continue;
        }
        const labelTimestamp = row.label_timestamp ?? row.timestamp;
        if (labelTimestamp < row.timestamp || labelTimestamp < lookbackStart) {
            continue;
        }
        const key = buildStrategyMarketKey(row.strategy, row.market_key);
        const bucket = featureCounterfactualsByKey.get(key) || [];
        bucket.push({
            row_id: row.id,
            strategy: row.strategy,
            market_key: row.market_key,
            timestamp: row.timestamp,
            label_timestamp: labelTimestamp,
            net_return: row.label_net_return,
        });
        featureCounterfactualsByKey.set(key, bucket);
    }

    const samples: ModelGateCalibrationSample[] = [];
    for (const [executionId, outcome] of outcomesByExecutionId.entries()) {
        const probability = input.probabilityForExecutionId(executionId);
        if (probability === null) {
            continue;
        }
        samples.push({
            probability,
            pnl: outcome.pnl,
            accepted: true,
            execution_id: executionId,
            strategy: outcome.strategy,
            timestamp: outcome.timestamp,
        });
    }

    const usedCounterfactualRows = new Set<string>();
    for (const rejected of input.rejectedSignalHistory) {
        if (rejected.stage !== 'MODEL_GATE' || rejected.timestamp < lookbackStart) {
            continue;
        }
        const probability = parseProbabilityFromRejectedPayload(rejected.payload);
        if (probability === null) {
            continue;
        }

        const directOutcome = outcomesByExecutionId.get(rejected.execution_id);
        if (directOutcome) {
            samples.push({
                probability: input.clampProbability(probability),
                pnl: directOutcome.pnl,
                accepted: false,
                execution_id: rejected.execution_id,
                strategy: directOutcome.strategy || rejected.strategy || 'UNKNOWN',
                timestamp: directOutcome.timestamp,
            });
            continue;
        }

        const rejectedStrategy = rejected.strategy || null;
        const rejectedMarketKey = rejected.market_key || null;
        if (!rejectedStrategy || !rejectedMarketKey) {
            continue;
        }
        const key = buildStrategyMarketKey(rejectedStrategy, rejectedMarketKey);
        const candidates = featureCounterfactualsByKey.get(key) || [];
        let bestCandidate: CounterfactualFeatureRow | null = null;
        let bestDelta = Number.POSITIVE_INFINITY;
        for (const candidate of candidates) {
            if (usedCounterfactualRows.has(candidate.row_id)) {
                continue;
            }
            if (candidate.label_timestamp < rejected.timestamp) {
                continue;
            }
            const delta = Math.abs(candidate.timestamp - rejected.timestamp);
            if (delta > input.counterfactualWindowMs) {
                continue;
            }
            if (delta < bestDelta) {
                bestCandidate = candidate;
                bestDelta = delta;
            }
        }
        if (!bestCandidate) {
            continue;
        }
        usedCounterfactualRows.add(bestCandidate.row_id);
        const assumedNotional = avgNotionalByStrategy.get(bestCandidate.strategy) ?? fallbackNotionalUsd;
        const estimatedPnl = bestCandidate.net_return * assumedNotional;
        samples.push({
            probability: input.clampProbability(probability),
            pnl: estimatedPnl,
            accepted: false,
            execution_id: rejected.execution_id,
            strategy: bestCandidate.strategy || rejected.strategy || 'UNKNOWN',
            timestamp: bestCandidate.label_timestamp,
        });
    }

    return samples;
}
