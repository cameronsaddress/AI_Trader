import { logger } from '../utils/logger';

type ValidationIssue = {
    key: string;
    message: string;
};

function hasNonEmptyEnv(name: string): boolean {
    const raw = process.env[name];
    return typeof raw === 'string' && raw.trim().length > 0;
}

function validateNumberEnv(
    issues: ValidationIssue[],
    name: string,
    bounds: { min: number; max: number },
    options: { required?: boolean } = {},
): void {
    const raw = process.env[name];
    if (raw === undefined || raw === null || raw.trim().length === 0) {
        if (options.required) {
            issues.push({
                key: name,
                message: `${name} is required`,
            });
        }
        return;
    }
    const parsed = Number(raw);
    if (!Number.isFinite(parsed)) {
        issues.push({
            key: name,
            message: `${name} must be a finite number`,
        });
        return;
    }
    if (parsed < bounds.min || parsed > bounds.max) {
        issues.push({
            key: name,
            message: `${name} out of range [${bounds.min}, ${bounds.max}] (got ${parsed})`,
        });
    }
}

export function validateStartupConfigOrThrow(): void {
    const issues: ValidationIssue[] = [];
    const livePostingEnabled = process.env.LIVE_ORDER_POSTING_ENABLED === 'true';
    const readAuthRequired = process.env.API_READ_AUTH_REQUIRED !== 'false';

    validateNumberEnv(issues, 'POLY_CLOB_CALL_TIMEOUT_MS', { min: 500, max: 60_000 });
    validateNumberEnv(issues, 'POLY_INIT_RETRY_COOLDOWN_MS', { min: 2_000, max: 600_000 });
    validateNumberEnv(issues, 'POLY_EXECUTION_SLIPPAGE_MAX_BPS', { min: 0, max: 2_000 }, {
        required: livePostingEnabled,
    });
    validateNumberEnv(issues, 'POLY_EXECUTION_SLIPPAGE_MIN_ABS', { min: 0, max: 0.25 }, {
        required: livePostingEnabled,
    });
    validateNumberEnv(issues, 'POLY_EXECUTION_MIN_BOOK_FILL_RATIO', { min: 0.5, max: 1.5 });
    validateNumberEnv(issues, 'EXECUTION_PREFLIGHT_TIMEOUT_MS', { min: 500, max: 120_000 }, {
        required: livePostingEnabled,
    });
    validateNumberEnv(issues, 'EXECUTION_LIVE_TIMEOUT_MS', { min: 500, max: 120_000 }, {
        required: livePostingEnabled,
    });
    validateNumberEnv(issues, 'EXECUTION_SETTLEMENT_TIMEOUT_MS', { min: 500, max: 180_000 }, {
        required: livePostingEnabled,
    });
    validateNumberEnv(issues, 'EXECUTION_SETTLEMENT_EMIT_TIMEOUT_MS', { min: 500, max: 60_000 }, {
        required: livePostingEnabled,
    });
    validateNumberEnv(issues, 'EXECUTION_NETTING_MARKET_CAP_USD', { min: 10, max: 1_000_000 });
    validateNumberEnv(issues, 'EXECUTION_NETTING_UNDERLYING_CAP_USD', { min: 10, max: 1_000_000 });
    validateNumberEnv(issues, 'FUNDING_BASIS_REFRESH_MS', { min: 5_000, max: 3_600_000 });
    validateNumberEnv(issues, 'FUNDING_BASIS_HTTP_TIMEOUT_MS', { min: 500, max: 60_000 });
    validateNumberEnv(issues, 'FUNDING_BASIS_SIGNAL_THRESHOLD_BPS', { min: 0.5, max: 200 });
    validateNumberEnv(issues, 'FUNDING_BASIS_BASIS_WEIGHT', { min: 0, max: 2 });
    validateNumberEnv(issues, 'FUNDING_BASIS_MAX_BOOST', { min: 1, max: 3 });
    validateNumberEnv(issues, 'FUNDING_BASIS_MAX_PENALTY', { min: 0.2, max: 1 });
    validateNumberEnv(issues, 'FUNDING_BASIS_STALE_MS', { min: 10_000, max: 7_200_000 });
    validateNumberEnv(issues, 'POLY_MICROSTRUCTURE_SIGNAL_TTL_MS', { min: 2_000, max: 600_000 });
    validateNumberEnv(issues, 'POLY_MICROSTRUCTURE_MIN_SHIFT_BPS', { min: 1, max: 1_000 });
    validateNumberEnv(issues, 'POLY_MICROSTRUCTURE_MAX_SPREAD_BPS', { min: 10, max: 2_000 });
    validateNumberEnv(issues, 'POLY_MICROSTRUCTURE_MAX_BOOST', { min: 1, max: 3 });
    validateNumberEnv(issues, 'POLY_MICROSTRUCTURE_MAX_PENALTY', { min: 0.2, max: 1 });
    validateNumberEnv(issues, 'CROSS_STRATEGY_CONFIRM_MAX_AGE_MS', { min: 1_000, max: 300_000 });
    validateNumberEnv(issues, 'CROSS_STRATEGY_CONFIRM_OBI_MAX_BOOST', { min: 1, max: 3 });
    validateNumberEnv(issues, 'CROSS_STRATEGY_CONFIRM_OBI_MAX_PENALTY', { min: 0.2, max: 1 });
    validateNumberEnv(issues, 'CROSS_STRATEGY_CONFIRM_CEX_MAX_BOOST', { min: 1, max: 3 });
    validateNumberEnv(issues, 'CROSS_STRATEGY_CONFIRM_CEX_MAX_PENALTY', { min: 0.2, max: 1 });
    validateNumberEnv(issues, 'COLLATERAL_RECONCILIATION_INTERVAL_MS', { min: 5_000, max: 3_600_000 });
    validateNumberEnv(issues, 'COLLATERAL_RECONCILIATION_TOLERANCE_USD', { min: 0.1, max: 10_000 });
    validateNumberEnv(issues, 'LIVE_LEDGER_BOOT_CASH', { min: 0, max: 100_000_000 });
    validateNumberEnv(issues, 'POLY_COLLATERAL_DECIMALS', { min: 0, max: 18 });
    validateNumberEnv(issues, 'MODE_LEDGER_DEFAULT_BANKROLL', { min: 100, max: 100_000_000 });
    validateNumberEnv(issues, 'SHUTDOWN_DRAIN_MS', { min: 0, max: 60_000 });
    validateNumberEnv(issues, 'LOG_MAX_SIZE_BYTES', { min: 1_000_000, max: 1_000_000_000 });
    validateNumberEnv(issues, 'LOG_MAX_FILES', { min: 1, max: 1000 });
    validateNumberEnv(issues, 'RISK_GUARD_TRAILING_STOP_PCT', { min: 0, max: 0.5 }, {
        required: livePostingEnabled,
    });
    validateNumberEnv(issues, 'RISK_GUARD_TRAILING_STOP_PCT_ABS_CAP', { min: 0, max: 1_000_000 });
    validateNumberEnv(issues, 'RISK_GUARD_CONSEC_LOSS_LIMIT', { min: 2, max: 100 }, {
        required: livePostingEnabled,
    });
    validateNumberEnv(issues, 'MODEL_PROBABILITY_GATE_MIN_PROB', { min: 0.52, max: 0.95 });
    validateNumberEnv(issues, 'MODEL_PROBABILITY_GATE_TUNER_MAX_DRIFT', { min: 0.01, max: 0.25 });
    validateNumberEnv(issues, 'MODEL_PROBABILITY_GATE_TUNER_MIN_FLOOR', { min: 0.52, max: 0.95 });
    validateNumberEnv(issues, 'META_ALLOCATOR_AGGREGATE_CAP', { min: 1, max: 10 });
    validateNumberEnv(issues, 'SIM_BANKROLL_BOOTSTRAP_VALUE', { min: 10, max: 100_000_000 });
    validateNumberEnv(issues, 'ML_MICROSTRUCTURE_MAX_AGE_MS', { min: 500, max: 120_000 });
    validateNumberEnv(issues, 'ML_MICROSTRUCTURE_BOOK_AGE_SCALE_MS', { min: 500, max: 120_000 });

    if (livePostingEnabled) {
        if (!hasNonEmptyEnv('POLY_PRIVATE_KEY') && !hasNonEmptyEnv('PRIVATE_KEY')) {
            issues.push({
                key: 'POLY_PRIVATE_KEY|PRIVATE_KEY',
                message: 'live posting requires signer private key',
            });
        }
        if (!hasNonEmptyEnv('POLY_FUNDER_ADDRESS') && !hasNonEmptyEnv('PROXY_WALLET_ADDRESS')) {
            issues.push({
                key: 'POLY_FUNDER_ADDRESS|PROXY_WALLET_ADDRESS',
                message: 'live posting requires funder/proxy wallet address',
            });
        }
        if (!hasNonEmptyEnv('POLY_LIVE_STRATEGY_ALLOWLIST')) {
            issues.push({
                key: 'POLY_LIVE_STRATEGY_ALLOWLIST',
                message: 'live posting requires an explicit strategy allowlist',
            });
        }
    }

    if (readAuthRequired && !hasNonEmptyEnv('READ_API_TOKEN') && !hasNonEmptyEnv('CONTROL_PLANE_TOKEN')) {
        issues.push({
            key: 'READ_API_TOKEN|CONTROL_PLANE_TOKEN',
            message: 'read API auth requires READ_API_TOKEN or CONTROL_PLANE_TOKEN',
        });
    }

    if (process.env.CORS_ALLOW_PRIVATE_IP_ORIGINS === 'true') {
        const privateOrigins = (process.env.CORS_ALLOWED_PRIVATE_IP_ORIGINS || '')
            .split(',')
            .map((origin) => origin.trim())
            .filter((origin) => origin.length > 0);
        if (privateOrigins.length === 0) {
            issues.push({
                key: 'CORS_ALLOWED_PRIVATE_IP_ORIGINS',
                message: 'required when CORS_ALLOW_PRIVATE_IP_ORIGINS=true',
            });
        } else {
            const invalid = privateOrigins.filter((origin) => !/^http:\/\/\d+\.\d+\.\d+\.\d+:3112$/.test(origin));
            if (invalid.length > 0) {
                issues.push({
                    key: 'CORS_ALLOWED_PRIVATE_IP_ORIGINS',
                    message: `invalid origin(s): ${invalid.join(', ')}`,
                });
            }
        }
    }

    const marketCapRaw = process.env.EXECUTION_NETTING_MARKET_CAP_USD;
    const underlyingCapRaw = process.env.EXECUTION_NETTING_UNDERLYING_CAP_USD;
    if (marketCapRaw && underlyingCapRaw) {
        const marketCap = Number(marketCapRaw);
        const underlyingCap = Number(underlyingCapRaw);
        if (Number.isFinite(marketCap) && Number.isFinite(underlyingCap) && underlyingCap < marketCap) {
            issues.push({
                key: 'EXECUTION_NETTING_UNDERLYING_CAP_USD',
                message: 'must be >= EXECUTION_NETTING_MARKET_CAP_USD',
            });
        }
    }

    if (issues.length > 0) {
        const details = issues.map((issue) => `- ${issue.key}: ${issue.message}`).join('\n');
        throw new Error(`Startup config validation failed:\n${details}`);
    }

    logger.info(`[Config] startup validation passed (live_posting=${livePostingEnabled ? 'true' : 'false'})`);
}
