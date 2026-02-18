import type { FeatureSnapshot, StrategyTradeSample } from '../../types/backend-types';
import type { RejectedSignalRecord } from '../execution/arbExecutionPipeline';
import { buildModelGateCalibrationSamples } from './modelGateCalibration';

function sampleTrade(overrides: Partial<StrategyTradeSample> = {}): StrategyTradeSample {
    return {
        strategy: 'BTC_5M',
        timestamp: 0,
        pnl: 0,
        notional: 100,
        net_return: 0,
        gross_return: null,
        cost_drag: null,
        reason: 'TEST',
        label_eligible: true,
        execution_id: 'exec-default',
        market_key: 'mkt-default',
        ...overrides,
    };
}

function featureRow(overrides: Partial<FeatureSnapshot> = {}): FeatureSnapshot {
    return {
        id: 'row-default',
        strategy: 'BTC_5M',
        market_key: 'mkt-default',
        timestamp: 0,
        signal_type: 'TEST',
        metric_family: 'UNKNOWN',
        features: {},
        label_net_return: 0,
        label_pnl: 0,
        label_timestamp: 0,
        label_source: 'TEST',
        ...overrides,
    };
}

function rejectedSignal(overrides: Partial<RejectedSignalRecord> = {}): RejectedSignalRecord {
    return {
        stage: 'MODEL_GATE',
        execution_id: 'exec-default',
        strategy: 'BTC_5M',
        market_key: 'mkt-default',
        reason: 'blocked',
        mode: 'PAPER',
        timestamp: 0,
        payload: { probability: 0.5 },
        ...overrides,
    };
}

describe('buildModelGateCalibrationSamples', () => {
    it('uses direct realized outcomes for rejected model-gate rows when execution_id matches', () => {
        const now = 1_700_000_000_000;
        const samples = buildModelGateCalibrationSamples({
            now,
            lookbackMs: 60_000,
            counterfactualWindowMs: 10_000,
            strategyIds: ['BTC_5M'],
            strategyTradeSamples: {
                BTC_5M: [
                    sampleTrade({
                        execution_id: 'exec-a',
                        timestamp: now - 2_000,
                        pnl: 25,
                        notional: 200,
                        market_key: 'mkt-1',
                    }),
                    sampleTrade({
                        execution_id: 'exec-b',
                        timestamp: now - 1_800,
                        pnl: -5,
                        notional: 120,
                        market_key: 'mkt-2',
                    }),
                ],
            },
            featureRegistryRows: [],
            rejectedSignalHistory: [
                rejectedSignal({
                    execution_id: 'exec-a',
                    strategy: 'BTC_5M',
                    market_key: 'mkt-1',
                    timestamp: now - 1_900,
                    payload: { probability: 0.48 },
                }),
            ],
            probabilityForExecutionId: (executionId) => {
                if (executionId === 'exec-a') return 0.61;
                if (executionId === 'exec-b') return 0.57;
                return null;
            },
            clampProbability: (value) => Math.max(0.5, Math.min(0.95, value)),
        });

        const accepted = samples.filter((sample) => sample.accepted);
        expect(accepted).toHaveLength(2);
        const rejectedDirect = samples.find((sample) => !sample.accepted && sample.execution_id === 'exec-a');
        expect(rejectedDirect).toBeDefined();
        expect(rejectedDirect?.pnl).toBe(25);
        expect(rejectedDirect?.probability).toBe(0.5);
    });

    it('matches nearest eligible counterfactual rows and does not reuse the same label row twice', () => {
        const now = 2_000_000;
        const samples = buildModelGateCalibrationSamples({
            now,
            lookbackMs: 100_000,
            counterfactualWindowMs: 1_000,
            strategyIds: ['BTC_5M'],
            strategyTradeSamples: {
                BTC_5M: [
                    sampleTrade({
                        execution_id: 'seed-avg-notional',
                        timestamp: now - 10_000,
                        pnl: 1,
                        notional: 250,
                        market_key: 'mkt-x',
                    }),
                ],
            },
            featureRegistryRows: [
                featureRow({
                    id: 'row-1',
                    market_key: 'mkt-1',
                    timestamp: now - 960,
                    label_timestamp: now - 940,
                    label_net_return: 0.02,
                }),
                featureRow({
                    id: 'row-2',
                    market_key: 'mkt-1',
                    timestamp: now - 930,
                    label_timestamp: now - 910,
                    label_net_return: 0.03,
                }),
            ],
            rejectedSignalHistory: [
                rejectedSignal({
                    execution_id: 'rej-a',
                    market_key: 'mkt-1',
                    timestamp: now - 950,
                    payload: { probability: 0.52 },
                }),
                rejectedSignal({
                    execution_id: 'rej-b',
                    market_key: 'mkt-1',
                    timestamp: now - 920,
                    payload: { probability: 0.53 },
                }),
            ],
            probabilityForExecutionId: () => null,
            clampProbability: (value) => Math.max(0.5, Math.min(0.95, value)),
        });

        const rejA = samples.find((sample) => !sample.accepted && sample.execution_id === 'rej-a');
        const rejB = samples.find((sample) => !sample.accepted && sample.execution_id === 'rej-b');
        expect(rejA).toBeDefined();
        expect(rejB).toBeDefined();
        expect(rejA?.pnl).toBeCloseTo(5, 6);
        expect(rejB?.pnl).toBeCloseTo(7.5, 6);
        expect(rejA?.timestamp).toBe(now - 940);
        expect(rejB?.timestamp).toBe(now - 910);
    });

    it('uses fallback notional and respects label timing/window constraints for counterfactuals', () => {
        const now = 3_000_000;
        const samples = buildModelGateCalibrationSamples({
            now,
            lookbackMs: 100_000,
            counterfactualWindowMs: 100,
            strategyIds: ['BTC_5M'],
            strategyTradeSamples: {
                BTC_5M: [],
            },
            featureRegistryRows: [
                featureRow({
                    id: 'row-early',
                    market_key: 'mkt-2',
                    timestamp: now - 520,
                    label_timestamp: now - 505,
                    label_net_return: 0.09,
                }),
                featureRow({
                    id: 'row-far',
                    market_key: 'mkt-2',
                    timestamp: now - 200,
                    label_timestamp: now - 180,
                    label_net_return: 0.08,
                }),
                featureRow({
                    id: 'row-ok',
                    market_key: 'mkt-2',
                    timestamp: now - 480,
                    label_timestamp: now - 470,
                    label_net_return: 0.04,
                }),
            ],
            rejectedSignalHistory: [
                rejectedSignal({
                    execution_id: 'rej-c',
                    market_key: 'mkt-2',
                    timestamp: now - 500,
                    payload: { probability: 0.54 },
                }),
            ],
            probabilityForExecutionId: () => null,
            clampProbability: (value) => Math.max(0.5, Math.min(0.95, value)),
            fallbackNotionalUsd: 100,
        });

        expect(samples).toHaveLength(1);
        expect(samples[0]?.execution_id).toBe('rej-c');
        expect(samples[0]?.accepted).toBe(false);
        expect(samples[0]?.pnl).toBeCloseTo(4, 6);
        expect(samples[0]?.timestamp).toBe(now - 470);
    });
});
