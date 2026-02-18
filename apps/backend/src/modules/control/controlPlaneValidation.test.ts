import {
    normalizeTradingMode,
    validateSimulationResetRequest,
    validateStrategyToggleRequest,
    validateTradingModeChangeRequest,
} from './controlPlaneValidation';

describe('controlPlaneValidation', () => {
    describe('normalizeTradingMode', () => {
        it('accepts PAPER/LIVE case-insensitively', () => {
            expect(normalizeTradingMode('paper')).toBe('PAPER');
            expect(normalizeTradingMode('LIVE')).toBe('LIVE');
        });

        it('rejects invalid values', () => {
            expect(normalizeTradingMode('')).toBeNull();
            expect(normalizeTradingMode('SIM')).toBeNull();
            expect(normalizeTradingMode(null)).toBeNull();
        });
    });

    describe('validateTradingModeChangeRequest', () => {
        it('rejects invalid mode payload', () => {
            const result = validateTradingModeChangeRequest({
                mode: 'INVALID',
                confirmation: null,
                execution_halted: false,
                live_readiness: null,
            });

            expect(result.ok).toBe(false);
            if (!result.ok) {
                expect(result.status).toBe(400);
            }
        });

        it('rejects LIVE without confirmation', () => {
            const result = validateTradingModeChangeRequest({
                mode: 'LIVE',
                confirmation: 'NOPE',
                execution_halted: false,
                live_readiness: { ready: true, failures: [] },
            });

            expect(result.ok).toBe(false);
            if (!result.ok) {
                expect(result.status).toBe(400);
            }
        });

        it('rejects LIVE when execution halt is active', () => {
            const result = validateTradingModeChangeRequest({
                mode: 'LIVE',
                confirmation: 'LIVE',
                execution_halted: true,
                live_readiness: { ready: true, failures: [] },
            });

            expect(result.ok).toBe(false);
            if (!result.ok) {
                expect(result.status).toBe(412);
                expect(result.message).toContain('Execution halt');
            }
        });

        it('rejects LIVE when readiness fails', () => {
            const result = validateTradingModeChangeRequest({
                mode: 'LIVE',
                confirmation: 'LIVE',
                execution_halted: false,
                live_readiness: { ready: false, failures: ['redis down'] },
            });

            expect(result.ok).toBe(false);
            if (!result.ok) {
                expect(result.status).toBe(412);
                expect(result.failures).toEqual(['redis down']);
            }
        });

        it('accepts valid PAPER/LIVE transitions', () => {
            const paper = validateTradingModeChangeRequest({
                mode: 'PAPER',
                confirmation: null,
                execution_halted: true,
                live_readiness: null,
            });
            expect(paper).toEqual({ ok: true, value: { mode: 'PAPER' } });

            const live = validateTradingModeChangeRequest({
                mode: 'LIVE',
                confirmation: 'LIVE',
                execution_halted: false,
                live_readiness: { ready: true, failures: [] },
            });
            expect(live).toEqual({ ok: true, value: { mode: 'LIVE' } });
        });
    });

    describe('validateSimulationResetRequest', () => {
        const normalizeBankroll = (value: unknown): number | null => {
            const parsed = Number(value);
            return Number.isFinite(parsed) ? parsed : null;
        };

        it('rejects reset outside PAPER mode', () => {
            const result = validateSimulationResetRequest({
                current_mode: 'LIVE',
                confirmation: 'RESET',
                bankroll: 1000,
                force_defaults: false,
                normalize_bankroll: normalizeBankroll,
            });

            expect(result.ok).toBe(false);
            if (!result.ok) {
                expect(result.status).toBe(409);
            }
        });

        it('rejects reset without explicit confirmation', () => {
            const result = validateSimulationResetRequest({
                current_mode: 'PAPER',
                confirmation: 'NO',
                bankroll: 1000,
                force_defaults: false,
                normalize_bankroll: normalizeBankroll,
            });

            expect(result.ok).toBe(false);
            if (!result.ok) {
                expect(result.status).toBe(400);
            }
        });

        it('rejects invalid bankroll', () => {
            const result = validateSimulationResetRequest({
                current_mode: 'PAPER',
                confirmation: 'RESET',
                bankroll: 'NaN',
                force_defaults: false,
                normalize_bankroll: normalizeBankroll,
            });

            expect(result.ok).toBe(false);
            if (!result.ok) {
                expect(result.status).toBe(400);
            }
        });

        it('accepts valid payload', () => {
            const result = validateSimulationResetRequest({
                current_mode: 'PAPER',
                confirmation: 'RESET',
                bankroll: 1234.56,
                force_defaults: true,
                normalize_bankroll: normalizeBankroll,
            });

            expect(result).toEqual({
                ok: true,
                value: {
                    bankroll: 1234.56,
                    force_defaults: true,
                },
            });
        });
    });

    describe('validateStrategyToggleRequest', () => {
        const strategyIds = ['BTC_5M', 'ATOMIC_ARB'];

        it('rejects malformed payload', () => {
            const result = validateStrategyToggleRequest({
                payload: { id: 'BTC_5M', active: 'yes' },
                strategy_ids: strategyIds,
            });

            expect(result.ok).toBe(false);
            if (!result.ok) {
                expect(result.status).toBe(400);
            }
        });

        it('rejects unknown strategy ids', () => {
            const result = validateStrategyToggleRequest({
                payload: { id: 'UNKNOWN', active: true },
                strategy_ids: strategyIds,
            });

            expect(result.ok).toBe(false);
            if (!result.ok) {
                expect(result.status).toBe(400);
            }
        });

        it('accepts valid toggle payload', () => {
            const result = validateStrategyToggleRequest({
                payload: { id: 'ATOMIC_ARB', active: false },
                strategy_ids: strategyIds,
            });

            expect(result).toEqual({
                ok: true,
                value: {
                    id: 'ATOMIC_ARB',
                    active: false,
                },
            });
        });
    });
});
