export type TradingMode = 'PAPER' | 'LIVE';

export type LiveReadinessResult = {
    ready: boolean;
    failures: string[];
};

type ValidationError = {
    ok: false;
    status: number;
    message: string;
    failures?: string[];
};

type ValidationSuccess<T> = {
    ok: true;
    value: T;
};

export type ValidationResult<T> = ValidationSuccess<T> | ValidationError;

export type TradingModeChangeValue = {
    mode: TradingMode;
};

export type SimulationResetValue = {
    bankroll: number;
    force_defaults: boolean;
};

export type StrategyToggleValue = {
    id: string;
    active: boolean;
};

function asRecord(input: unknown): Record<string, unknown> | null {
    if (!input || typeof input !== 'object' || Array.isArray(input)) {
        return null;
    }
    return input as Record<string, unknown>;
}

export function normalizeTradingMode(mode: unknown): TradingMode | null {
    if (typeof mode !== 'string') {
        return null;
    }

    const normalized = mode.trim().toUpperCase();
    if (normalized === 'PAPER' || normalized === 'LIVE') {
        return normalized as TradingMode;
    }
    return null;
}

export function validateTradingModeChangeRequest(input: {
    mode: unknown;
    confirmation: unknown;
    execution_halted: boolean;
    live_readiness: LiveReadinessResult | null;
}): ValidationResult<TradingModeChangeValue> {
    const mode = normalizeTradingMode(input.mode);
    if (!mode) {
        return {
            ok: false,
            status: 400,
            message: 'Invalid trading mode payload',
        };
    }

    if (mode === 'LIVE' && input.confirmation !== 'LIVE') {
        return {
            ok: false,
            status: 400,
            message: 'LIVE mode requires explicit confirmation',
        };
    }

    if (mode === 'LIVE' && input.execution_halted) {
        return {
            ok: false,
            status: 412,
            message: 'Execution halt is active; clear halt before enabling LIVE mode',
        };
    }

    if (mode === 'LIVE' && (!input.live_readiness || !input.live_readiness.ready)) {
        return {
            ok: false,
            status: 412,
            message: 'Live mode preconditions not met',
            failures: input.live_readiness?.failures || [],
        };
    }

    return {
        ok: true,
        value: { mode },
    };
}

export function validateSimulationResetRequest(input: {
    current_mode: TradingMode;
    confirmation: unknown;
    bankroll: unknown;
    force_defaults: unknown;
    normalize_bankroll: (value: unknown) => number | null;
}): ValidationResult<SimulationResetValue> {
    if (input.current_mode !== 'PAPER') {
        return {
            ok: false,
            status: 409,
            message: 'Simulation reset is only allowed in PAPER mode',
        };
    }

    if (input.confirmation !== 'RESET') {
        return {
            ok: false,
            status: 400,
            message: 'Simulation reset requires explicit RESET confirmation',
        };
    }

    const bankroll = input.normalize_bankroll(input.bankroll);
    if (bankroll === null) {
        return {
            ok: false,
            status: 400,
            message: 'Invalid reset bankroll value',
        };
    }

    return {
        ok: true,
        value: {
            bankroll,
            force_defaults: input.force_defaults === true,
        },
    };
}

export function validateStrategyToggleRequest(input: {
    payload: unknown;
    strategy_ids: string[];
}): ValidationResult<StrategyToggleValue> {
    const payload = asRecord(input.payload);
    const id = typeof payload?.id === 'string' ? payload.id : null;
    const active = payload?.active;

    if (!id || !input.strategy_ids.includes(id) || typeof active !== 'boolean') {
        return {
            ok: false,
            status: 400,
            message: 'Invalid strategy toggle payload',
        };
    }

    return {
        ok: true,
        value: {
            id,
            active,
        },
    };
}
