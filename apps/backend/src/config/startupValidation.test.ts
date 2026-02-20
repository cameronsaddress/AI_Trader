import { validateStartupConfigOrThrow } from './startupValidation';

const ORIGINAL_ENV = { ...process.env };

describe('validateStartupConfigOrThrow', () => {
    beforeEach(() => {
        process.env = { ...ORIGINAL_ENV };
        process.env.API_READ_AUTH_REQUIRED = 'true';
        process.env.CONTROL_PLANE_TOKEN = process.env.CONTROL_PLANE_TOKEN || 'test-control-token';
        process.env.CORS_ALLOW_PRIVATE_IP_ORIGINS = 'false';
        process.env.CORS_ALLOWED_PRIVATE_IP_ORIGINS = '';
    });

    afterAll(() => {
        process.env = ORIGINAL_ENV;
    });

    it('passes when live posting is disabled and optional vars are absent', () => {
        process.env.LIVE_ORDER_POSTING_ENABLED = 'false';
        expect(() => validateStartupConfigOrThrow()).not.toThrow();
    });

    it('fails when live posting is enabled without required keys', () => {
        process.env.LIVE_ORDER_POSTING_ENABLED = 'true';
        process.env.POLY_PRIVATE_KEY = '';
        process.env.PRIVATE_KEY = '';
        process.env.POLY_FUNDER_ADDRESS = '';
        process.env.PROXY_WALLET_ADDRESS = '';
        process.env.POLY_LIVE_STRATEGY_ALLOWLIST = '';
        process.env.POLY_EXECUTION_SLIPPAGE_MAX_BPS = '45';
        process.env.POLY_EXECUTION_SLIPPAGE_MIN_ABS = '0.008';
        process.env.EXECUTION_PREFLIGHT_TIMEOUT_MS = '10000';
        process.env.EXECUTION_LIVE_TIMEOUT_MS = '10000';
        process.env.EXECUTION_SETTLEMENT_TIMEOUT_MS = '12000';
        process.env.EXECUTION_SETTLEMENT_EMIT_TIMEOUT_MS = '8000';
        process.env.RISK_GUARD_TRAILING_STOP_PCT = '0.06';
        process.env.RISK_GUARD_CONSEC_LOSS_LIMIT = '4';

        expect(() => validateStartupConfigOrThrow()).toThrow(/startup config validation failed/i);
    });

    it('fails when numeric config is malformed', () => {
        process.env.LIVE_ORDER_POSTING_ENABLED = 'false';
        process.env.EXECUTION_NETTING_MARKET_CAP_USD = 'not-a-number';
        expect(() => validateStartupConfigOrThrow()).toThrow(/EXECUTION_NETTING_MARKET_CAP_USD/);
    });

    it('fails when overlay config is out of range', () => {
        process.env.LIVE_ORDER_POSTING_ENABLED = 'false';
        process.env.FUNDING_BASIS_SIGNAL_THRESHOLD_BPS = '0.1';
        expect(() => validateStartupConfigOrThrow()).toThrow(/FUNDING_BASIS_SIGNAL_THRESHOLD_BPS/);
    });

    it('fails when new operational bounds are invalid', () => {
        process.env.LIVE_ORDER_POSTING_ENABLED = 'false';
        process.env.SHUTDOWN_DRAIN_MS = '120000';
        process.env.POLY_EXECUTION_MIN_BOOK_FILL_RATIO = '2';
        expect(() => validateStartupConfigOrThrow()).toThrow(/SHUTDOWN_DRAIN_MS|POLY_EXECUTION_MIN_BOOK_FILL_RATIO/);
    });

    it('fails when read API auth is required and no read/control token exists', () => {
        process.env.LIVE_ORDER_POSTING_ENABLED = 'false';
        process.env.API_READ_AUTH_REQUIRED = 'true';
        process.env.CONTROL_PLANE_TOKEN = '';
        process.env.READ_API_TOKEN = '';
        expect(() => validateStartupConfigOrThrow()).toThrow(/READ_API_TOKEN\|CONTROL_PLANE_TOKEN/);
    });

    it('fails when private-IP CORS compatibility is enabled without explicit origin allowlist', () => {
        process.env.LIVE_ORDER_POSTING_ENABLED = 'false';
        process.env.CORS_ALLOW_PRIVATE_IP_ORIGINS = 'true';
        process.env.CORS_ALLOWED_PRIVATE_IP_ORIGINS = '';
        expect(() => validateStartupConfigOrThrow()).toThrow(/CORS_ALLOWED_PRIVATE_IP_ORIGINS/);
    });
});
