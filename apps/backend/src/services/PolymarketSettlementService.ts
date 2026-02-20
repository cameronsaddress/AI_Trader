import axios from 'axios';
import { Contract, JsonRpcProvider, Wallet, ZeroHash, isAddress } from 'ethers';
import type { PolymarketLiveExecutionResult } from './PolymarketPreflightService';
import { logger } from '../utils/logger';

const CONDITIONAL_TOKENS_ABI = [
    'function redeemPositions(address collateralToken, bytes32 parentCollectionId, bytes32 conditionId, uint256[] indexSets)',
];
const ERC20_BALANCE_ABI = [
    'function balanceOf(address owner) view returns (uint256)',
];

const DEFAULT_DATA_API_HOST = 'https://data-api.polymarket.com';
const DEFAULT_CLOB_HOST = 'https://clob.polymarket.com';
const DEFAULT_POLYGON_RPC_URL = 'https://polygon-rpc.com';
const DEFAULT_CONDITIONAL_TOKENS_ADDRESS = '0x4D97DCd97eC945f40cF65F87097ACe5EA0476045';
const DEFAULT_COLLATERAL_TOKEN_ADDRESS = '0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174';
const DEFAULT_COLLATERAL_DECIMALS = 6;
const DEFAULT_POLL_INTERVAL_MS = 30_000;
const DEFAULT_RETRY_COOLDOWN_MS = 60_000;
const DEFAULT_MIN_REDEEMABLE_SHARES = 0.01;
const DEFAULT_SETTLEMENT_STALE_MS = 60 * 60 * 1000;
const DEFAULT_NOT_FOUND_BACKOFF_MS = 15 * 60 * 1000;
const DEFAULT_LIVE_LEDGER_BOOT_CASH = 0;
const LIVE_LEDGER_CASH_KEY = 'live_ledger:cash';
const LIVE_LEDGER_RESERVED_KEY = 'live_ledger:reserved';
const LIVE_LEDGER_REALIZED_PNL_KEY = 'live_ledger:realized_pnl';
const LIVE_BANKROLL_KEY = 'live_bankroll';
const POSITION_INDEX_SETS = [1n, 2n];
const REDIS_KEY = 'system:polymarket:settlement:atomic:v1';
const TRACKED_PAIRED_STRATEGIES = new Set(['ATOMIC_ARB', 'GRAPH_ARB']);

type PositionStatus =
    | 'TRACKED'
    | 'AWAITING_RESOLUTION'
    | 'RESOLVED'
    | 'REDEEMABLE'
    | 'REDEEMED'
    | 'FAILED'
    | 'MANUAL_ACTION_REQUIRED';

type AtomicSettlementPosition = {
    id: string;
    strategy: string;
    conditionId: string;
    market: string;
    yesTokenId: string;
    noTokenId: string;
    shares: number;
    totalNotionalUsd: number;
    openedAt: number;
    updatedAt: number;
    status: PositionStatus;
    winnerTokenId?: string;
    closed?: boolean;
    redeemableShares?: number;
    settlementCheckedAt?: number;
    redeemTxHash?: string;
    redeemAttempts: number;
    nextRedeemAttemptAt: number;
    marketNotFoundCount?: number;
    marketNotFoundUntil?: number;
    entryBookedNotionalUsd?: number;
    redeemProceedsUsd?: number;
    realizedPnlUsd?: number;
    lastError?: string;
    note?: string;
};

type MarketStateResult = {
    closed: boolean;
    winnerTokenId: string | null;
    invalid: boolean;
    notFound: boolean;
};

export type SettlementEventType = 'INFO' | 'WARNING' | 'ERROR' | 'REDEEMED';

export type SettlementEvent = {
    type: SettlementEventType;
    timestamp: number;
    message: string;
    conditionId?: string;
    positionId?: string;
    details?: Record<string, unknown>;
};

export type PolymarketSettlementReadiness = {
    ready: boolean;
    enabled: boolean;
    autoRedeemEnabled: boolean;
    canDirectRedeem: boolean;
    signerAddress: string | null;
    funderAddress: string | null;
    failures: string[];
};

type SettlementSnapshot = {
    enabled: boolean;
    autoRedeemEnabled: boolean;
    canDirectRedeem: boolean;
    signerAddress: string | null;
    funderAddress: string | null;
    pollIntervalMs: number;
    positions: AtomicSettlementPosition[];
    liveLedger: {
        cash: number;
        reserved: number;
        realizedPnl: number;
        equity: number;
    } | null;
    collateralBalanceUsd: number | null;
    collateralReconcileDeltaUsd: number | null;
    collateralReconcileAt: number;
    lastRunAt: number;
    running: boolean;
};

type RedisLike = {
    get(key: string): Promise<string | null>;
    set(key: string, value: string): Promise<unknown>;
    eval?(
        script: string,
        options: {
            keys?: string[];
            arguments?: string[];
        },
    ): Promise<unknown>;
};

function asRecord(input: unknown): Record<string, unknown> | null {
    if (!input || typeof input !== 'object' || Array.isArray(input)) {
        return null;
    }
    return input as Record<string, unknown>;
}

function asString(input: unknown): string | null {
    return typeof input === 'string' && input.trim().length > 0 ? input.trim() : null;
}

function asNumber(input: unknown): number | null {
    if (input === null || input === undefined || input === '') {
        return null;
    }
    const parsed = Number(input);
    return Number.isFinite(parsed) ? parsed : null;
}

function normalizeConditionId(raw: string | null): string | null {
    if (!raw) {
        return null;
    }
    const value = raw.trim();
    if (!/^0x[a-fA-F0-9]{64}$/.test(value)) {
        return null;
    }
    return value.toLowerCase();
}

function parseConditionId(input: unknown): string | null {
    const record = asRecord(input);
    if (!record) {
        return null;
    }

    return normalizeConditionId(
        asString(record.conditionId)
        || asString(record.condition_id)
        || asString(record.conditionID),
    );
}

function parseMarketClosed(raw: unknown): boolean | null {
    if (typeof raw === 'boolean') {
        return raw;
    }
    if (typeof raw === 'number') {
        return raw !== 0;
    }
    if (typeof raw === 'string') {
        const normalized = raw.trim().toLowerCase();
        if (normalized === 'true' || normalized === '1') {
            return true;
        }
        if (normalized === 'false' || normalized === '0') {
            return false;
        }
    }
    return null;
}

export class PolymarketSettlementService {
    private readonly redis: RedisLike;
    private readonly dataApiHost: string;
    private readonly clobHost: string;
    private readonly pollIntervalMs: number;
    private readonly retryCooldownMs: number;
    private readonly notFoundBackoffMs: number;
    private readonly autoRedeemEnabled: boolean;
    private readonly minRedeemableShares: number;
    private readonly staleMs: number;
    private readonly enabled: boolean;
    private readonly canDirectRedeem: boolean;
    private readonly funderAddress: string | null;
    private readonly signerAddress: string | null;
    private readonly conditionalTokensAddress: string;
    private readonly collateralTokenAddress: string;
    private readonly collateralDecimals: number;
    private readonly liveLedgerBootCash: number;
    private readonly contract: Contract | null;
    private readonly collateralReadContract: Contract | null;

    private readonly positions = new Map<string, AtomicSettlementPosition>();
    private running = false;
    private runCycleTail: Promise<void> = Promise.resolve();
    private lastRunAt = 0;
    private lastLiveLedgerSnapshot: {
        cash: number;
        reserved: number;
        realizedPnl: number;
        equity: number;
    } | null = null;
    private lastCollateralBalanceUsd: number | null = null;
    private lastCollateralReconcileDeltaUsd: number | null = null;
    private lastCollateralReconcileAt = 0;

    constructor(redis: RedisLike) {
        this.redis = redis;

        this.dataApiHost = (process.env.POLY_DATA_API_HOST || DEFAULT_DATA_API_HOST).replace(/\/+$/, '');
        this.clobHost = (process.env.POLYMARKET_CLOB_HOST || DEFAULT_CLOB_HOST).replace(/\/+$/, '');
        this.pollIntervalMs = Math.max(5_000, Number(process.env.POLY_SETTLEMENT_POLL_MS || DEFAULT_POLL_INTERVAL_MS));
        this.retryCooldownMs = Math.max(5_000, Number(process.env.POLY_REDEEM_RETRY_COOLDOWN_MS || DEFAULT_RETRY_COOLDOWN_MS));
        this.notFoundBackoffMs = Math.max(
            this.pollIntervalMs,
            Number(process.env.POLY_SETTLEMENT_NOT_FOUND_BACKOFF_MS || DEFAULT_NOT_FOUND_BACKOFF_MS),
        );
        this.autoRedeemEnabled = process.env.POLY_AUTO_REDEEM_ENABLED === 'true';
        this.minRedeemableShares = Math.max(0.0001, Number(process.env.POLY_MIN_REDEEMABLE_SHARES || DEFAULT_MIN_REDEEMABLE_SHARES));
        this.staleMs = Math.max(60_000, Number(process.env.POLY_SETTLEMENT_STALE_MS || DEFAULT_SETTLEMENT_STALE_MS));

        const privateKey = asString(process.env.POLY_PRIVATE_KEY) || asString(process.env.PRIVATE_KEY);
        const rpcUrl = asString(process.env.POLY_RPC_URL) || asString(process.env.POLYGON_RPC_URL) || DEFAULT_POLYGON_RPC_URL;
        const funderRaw = asString(process.env.POLY_FUNDER_ADDRESS) || asString(process.env.PROXY_WALLET_ADDRESS);

        this.conditionalTokensAddress = asString(process.env.POLY_CONDITIONAL_TOKENS_ADDRESS) || DEFAULT_CONDITIONAL_TOKENS_ADDRESS;
        this.collateralTokenAddress = asString(process.env.POLY_COLLATERAL_TOKEN_ADDRESS) || DEFAULT_COLLATERAL_TOKEN_ADDRESS;
        this.collateralDecimals = Math.max(
            0,
            Math.min(18, Math.floor(Number(process.env.POLY_COLLATERAL_DECIMALS || DEFAULT_COLLATERAL_DECIMALS))),
        );
        this.liveLedgerBootCash = Math.max(
            0,
            Number(process.env.LIVE_LEDGER_BOOT_CASH || DEFAULT_LIVE_LEDGER_BOOT_CASH),
        );

        let signerAddress: string | null = null;
        let contract: Contract | null = null;
        let collateralReadContract: Contract | null = null;
        let enabled = false;
        let canDirectRedeem = false;

        if (!privateKey) {
            logger.warn('[Settlement] disabled: missing PRIVATE_KEY/POLY_PRIVATE_KEY');
        } else if (!isAddress(this.conditionalTokensAddress) || !isAddress(this.collateralTokenAddress)) {
            logger.warn('[Settlement] disabled: invalid conditional token or collateral token address');
        } else {
            try {
                const provider = new JsonRpcProvider(rpcUrl);
                const wallet = new Wallet(privateKey, provider);
                signerAddress = wallet.address.toLowerCase();
                contract = new Contract(this.conditionalTokensAddress, CONDITIONAL_TOKENS_ABI, wallet);
                collateralReadContract = new Contract(this.collateralTokenAddress, ERC20_BALANCE_ABI, provider);
                enabled = true;

                const normalizedFunder = funderRaw && isAddress(funderRaw) ? funderRaw.toLowerCase() : signerAddress;
                this.funderAddress = normalizedFunder;
                canDirectRedeem = normalizedFunder === signerAddress;
                this.signerAddress = signerAddress;
                this.contract = contract;
                this.collateralReadContract = collateralReadContract;
                this.enabled = enabled;
                this.canDirectRedeem = canDirectRedeem;

                if (!this.canDirectRedeem) {
                    logger.warn(
                        `[Settlement] funder ${normalizedFunder} differs from signer ${signerAddress}; direct on-chain redemption requires proxy/relayer flow`,
                    );
                }

                logger.info(
                    `[Settlement] initialized: funder=${normalizedFunder} signer=${signerAddress} autoRedeem=${this.autoRedeemEnabled}`,
                );
                return;
            } catch (error) {
                const message = error instanceof Error ? error.message : String(error);
                logger.error(`[Settlement] disabled: initialization failed: ${message}`);
            }
        }

        this.funderAddress = funderRaw && isAddress(funderRaw) ? funderRaw.toLowerCase() : null;
        this.signerAddress = signerAddress;
        this.contract = contract;
        this.collateralReadContract = collateralReadContract;
        this.enabled = enabled;
        this.canDirectRedeem = canDirectRedeem;
    }

    public getPollIntervalMs(): number {
        return this.pollIntervalMs;
    }

    public getSnapshot(): SettlementSnapshot {
        return {
            enabled: this.enabled,
            autoRedeemEnabled: this.autoRedeemEnabled,
            canDirectRedeem: this.canDirectRedeem,
            signerAddress: this.signerAddress,
            funderAddress: this.funderAddress,
            pollIntervalMs: this.pollIntervalMs,
            positions: [...this.positions.values()].sort((a, b) => b.openedAt - a.openedAt),
            liveLedger: this.lastLiveLedgerSnapshot,
            collateralBalanceUsd: this.lastCollateralBalanceUsd,
            collateralReconcileDeltaUsd: this.lastCollateralReconcileDeltaUsd,
            collateralReconcileAt: this.lastCollateralReconcileAt,
            lastRunAt: this.lastRunAt,
            running: this.running,
        };
    }

    public getReadinessSnapshot(): PolymarketSettlementReadiness {
        const failures: string[] = [];
        if (!this.enabled) {
            failures.push('settlement service disabled (missing signer/provider initialization)');
        }
        if (!this.funderAddress) {
            failures.push('missing POLY_FUNDER_ADDRESS/PROXY_WALLET_ADDRESS');
        }
        if (this.autoRedeemEnabled && !this.canDirectRedeem) {
            failures.push('auto-redeem enabled but signer cannot directly redeem for configured funder');
        }
        return {
            ready: failures.length === 0,
            enabled: this.enabled,
            autoRedeemEnabled: this.autoRedeemEnabled,
            canDirectRedeem: this.canDirectRedeem,
            signerAddress: this.signerAddress,
            funderAddress: this.funderAddress,
            failures,
        };
    }

    private async withRunCycleLock<T>(runner: () => Promise<T>): Promise<T> {
        let release!: () => void;
        const previous = this.runCycleTail;
        this.runCycleTail = new Promise<void>((resolve) => {
            release = resolve;
        });
        await previous;
        try {
            return await runner();
        } finally {
            release();
        }
    }

    private async ensureLiveLedgerSeed(): Promise<void> {
        const [cashRaw, reservedRaw, realizedRaw] = await Promise.all([
            this.redis.get(LIVE_LEDGER_CASH_KEY),
            this.redis.get(LIVE_LEDGER_RESERVED_KEY),
            this.redis.get(LIVE_LEDGER_REALIZED_PNL_KEY),
        ]);

        const cash = asNumber(cashRaw);
        const reserved = asNumber(reservedRaw);
        const realized = asNumber(realizedRaw);
        const tasks: Array<Promise<unknown>> = [];
        if (cash === null) {
            tasks.push(this.redis.set(LIVE_LEDGER_CASH_KEY, this.liveLedgerBootCash.toFixed(8)));
            tasks.push(this.redis.set(LIVE_BANKROLL_KEY, this.liveLedgerBootCash.toFixed(8)));
        }
        if (reserved === null) {
            tasks.push(this.redis.set(LIVE_LEDGER_RESERVED_KEY, '0'));
        }
        if (realized === null) {
            tasks.push(this.redis.set(LIVE_LEDGER_REALIZED_PNL_KEY, '0'));
        }
        if (tasks.length > 0) {
            await Promise.all(tasks);
        }
    }

    private async getLiveLedgerSnapshot(): Promise<{
        cash: number;
        reserved: number;
        realizedPnl: number;
        equity: number;
    }> {
        await this.ensureLiveLedgerSeed();
        const [cashRaw, reservedRaw, realizedRaw] = await Promise.all([
            this.redis.get(LIVE_LEDGER_CASH_KEY),
            this.redis.get(LIVE_LEDGER_RESERVED_KEY),
            this.redis.get(LIVE_LEDGER_REALIZED_PNL_KEY),
        ]);
        const cash = asNumber(cashRaw) ?? this.liveLedgerBootCash;
        const reserved = asNumber(reservedRaw) ?? 0;
        const realizedPnl = asNumber(realizedRaw) ?? 0;
        const equity = cash + reserved;
        await this.redis.set(LIVE_BANKROLL_KEY, equity.toFixed(8));
        this.lastLiveLedgerSnapshot = { cash, reserved, realizedPnl, equity };
        return this.lastLiveLedgerSnapshot;
    }

    private async reserveLiveEntryNotional(
        notionalUsd: number,
    ): Promise<{ cash: number; reserved: number; realizedPnl: number; equity: number }> {
        const safeNotional = Number.isFinite(notionalUsd) ? Math.max(0, notionalUsd) : 0;
        return this.applyLiveLedgerMutationAtomic({
            cashDelta: -safeNotional,
            reservedDelta: safeNotional,
            realizedPnlDelta: 0,
        });
    }

    private async settleLiveRedeem(
        entryNotionalUsd: number,
        proceedsUsd: number,
    ): Promise<{ cash: number; reserved: number; realizedPnl: number; equity: number; pnl: number }> {
        const safeEntry = Number.isFinite(entryNotionalUsd) ? Math.max(0, entryNotionalUsd) : 0;
        const safeProceeds = Number.isFinite(proceedsUsd) ? Math.max(0, proceedsUsd) : 0;
        const pnl = safeProceeds - safeEntry;
        const next = await this.applyLiveLedgerMutationAtomic({
            cashDelta: safeProceeds,
            reservedDelta: -safeEntry,
            realizedPnlDelta: pnl,
        });
        return {
            cash: next.cash,
            reserved: next.reserved,
            realizedPnl: next.realizedPnl,
            equity: next.equity,
            pnl,
        };
    }

    private async applyLiveLedgerMutationAtomic(input: {
        cashDelta: number;
        reservedDelta: number;
        realizedPnlDelta: number;
    }): Promise<{ cash: number; reserved: number; realizedPnl: number; equity: number }> {
        await this.ensureLiveLedgerSeed();
        const cashDelta = Number.isFinite(input.cashDelta) ? input.cashDelta : 0;
        const reservedDelta = Number.isFinite(input.reservedDelta) ? input.reservedDelta : 0;
        const realizedPnlDelta = Number.isFinite(input.realizedPnlDelta) ? input.realizedPnlDelta : 0;

        if (typeof this.redis.eval === 'function') {
            const script = `
                local cash = tonumber(redis.call("GET", KEYS[1]) or "0")
                local reserved = tonumber(redis.call("GET", KEYS[2]) or "0")
                local realized = tonumber(redis.call("GET", KEYS[3]) or "0")
                cash = cash + tonumber(ARGV[1])
                reserved = reserved + tonumber(ARGV[2])
                if reserved < 0 then
                    reserved = 0
                end
                realized = realized + tonumber(ARGV[3])
                local equity = cash + reserved
                redis.call("SET", KEYS[1], string.format("%.8f", cash))
                redis.call("SET", KEYS[2], string.format("%.8f", reserved))
                redis.call("SET", KEYS[3], string.format("%.8f", realized))
                redis.call("SET", KEYS[4], string.format("%.8f", equity))
                return {cash, reserved, realized, equity}
            `;
            const result = await this.redis.eval(script, {
                keys: [
                    LIVE_LEDGER_CASH_KEY,
                    LIVE_LEDGER_RESERVED_KEY,
                    LIVE_LEDGER_REALIZED_PNL_KEY,
                    LIVE_BANKROLL_KEY,
                ],
                arguments: [
                    cashDelta.toString(),
                    reservedDelta.toString(),
                    realizedPnlDelta.toString(),
                ],
            }) as unknown[];
            const cash = asNumber(result?.[0]) ?? 0;
            const reserved = asNumber(result?.[1]) ?? 0;
            const realizedPnl = asNumber(result?.[2]) ?? 0;
            const equity = asNumber(result?.[3]) ?? (cash + reserved);
            this.lastLiveLedgerSnapshot = { cash, reserved, realizedPnl, equity };
            return this.lastLiveLedgerSnapshot;
        }

        logger.warn('[Settlement] redis.eval unavailable; using non-atomic live ledger mutation fallback');
        const ledger = await this.getLiveLedgerSnapshot();
        const cash = ledger.cash + cashDelta;
        const reserved = Math.max(0, ledger.reserved + reservedDelta);
        const realizedPnl = ledger.realizedPnl + realizedPnlDelta;
        const equity = cash + reserved;
        await Promise.all([
            this.redis.set(LIVE_LEDGER_CASH_KEY, cash.toFixed(8)),
            this.redis.set(LIVE_LEDGER_RESERVED_KEY, reserved.toFixed(8)),
            this.redis.set(LIVE_LEDGER_REALIZED_PNL_KEY, realizedPnl.toFixed(8)),
            this.redis.set(LIVE_BANKROLL_KEY, equity.toFixed(8)),
        ]);
        this.lastLiveLedgerSnapshot = { cash, reserved, realizedPnl, equity };
        return this.lastLiveLedgerSnapshot;
    }

    private async fetchCollateralBalanceUsd(): Promise<number | null> {
        if (!this.collateralReadContract || !this.funderAddress) {
            return null;
        }
        try {
            const rawBalance = await this.collateralReadContract.balanceOf(this.funderAddress);
            const asFloat = Number(rawBalance) / (10 ** this.collateralDecimals);
            if (!Number.isFinite(asFloat)) {
                return null;
            }
            return asFloat;
        } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            logger.warn(`[Settlement] collateral balance fetch failed: ${message}`);
            return null;
        }
    }

    public async reconcileCollateralBalance(toleranceUsd = 2): Promise<{
        ok: boolean;
        onchainBalanceUsd: number | null;
        ledgerCashUsd: number;
        deltaUsd: number | null;
        checkedAt: number;
        issues: string[];
    }> {
        const [ledger, onchainBalanceUsd] = await Promise.all([
            this.getLiveLedgerSnapshot(),
            this.fetchCollateralBalanceUsd(),
        ]);
        const issues: string[] = [];
        let deltaUsd: number | null = null;
        let ok = true;
        if (onchainBalanceUsd === null) {
            issues.push('on-chain collateral balance unavailable');
            ok = false;
        } else {
            deltaUsd = onchainBalanceUsd - ledger.cash;
            if (Math.abs(deltaUsd) > Math.max(0.1, toleranceUsd)) {
                ok = false;
                issues.push(`collateral/ledger cash delta ${deltaUsd.toFixed(2)} exceeds tolerance ${toleranceUsd.toFixed(2)}`);
            }
        }
        this.lastCollateralBalanceUsd = onchainBalanceUsd;
        this.lastCollateralReconcileDeltaUsd = deltaUsd;
        this.lastCollateralReconcileAt = Date.now();
        return {
            ok,
            onchainBalanceUsd,
            ledgerCashUsd: ledger.cash,
            deltaUsd,
            checkedAt: this.lastCollateralReconcileAt,
            issues,
        };
    }

    public async init(): Promise<void> {
        await this.getLiveLedgerSnapshot().catch((error) => {
            const message = error instanceof Error ? error.message : String(error);
            logger.warn(`[Settlement] live ledger seed failed: ${message}`);
        });
        const raw = await this.redis.get(REDIS_KEY);
        if (!raw) {
            return;
        }

        try {
            const parsed = JSON.parse(raw) as { positions?: AtomicSettlementPosition[] } | AtomicSettlementPosition[];
            const entries = Array.isArray(parsed) ? parsed : Array.isArray(parsed?.positions) ? parsed.positions : [];
            for (const entry of entries) {
                const conditionId = normalizeConditionId(entry?.conditionId || null);
                if (!conditionId || typeof entry?.id !== 'string') {
                    continue;
                }
                this.positions.set(entry.id, {
                    ...entry,
                    conditionId,
                });
            }
        } catch (error) {
            logger.warn(`[Settlement] failed to parse persisted state; starting with empty state error=${String(error)}`);
        }
    }

    public async reset(): Promise<void> {
        this.positions.clear();
        this.lastRunAt = Date.now();
        await this.persist();
    }

    public async registerAtomicExecution(result: PolymarketLiveExecutionResult): Promise<SettlementEvent[]> {
        const strategy = (result.strategy || '').trim().toUpperCase();
        if (!TRACKED_PAIRED_STRATEGIES.has(strategy) || result.dryRun) {
            return [];
        }

        const now = Date.now();
        const successOrders = result.orders.filter((order) => order.ok && order.side === 'BUY');
        const conditionGroups = new Map<string, typeof successOrders>();

        for (const order of successOrders) {
            const conditionId = normalizeConditionId(order.conditionId || null);
            if (!conditionId) {
                continue;
            }
            if (!conditionGroups.has(conditionId)) {
                conditionGroups.set(conditionId, []);
            }
            conditionGroups.get(conditionId)!.push(order);
        }

        const events: SettlementEvent[] = [];
        let dirty = false;

        for (const [conditionId, orders] of conditionGroups.entries()) {
            if (orders.length < 2) {
                events.push({
                    type: 'WARNING',
                    timestamp: now,
                    conditionId,
                    message: 'Paired execution posted fewer than two BUY legs; manual hedge review required',
                    details: { postedLegs: orders.length },
                });
                continue;
            }

            const uniqueTokenIds = new Set(orders.map((order) => String(order.tokenId)));
            if (uniqueTokenIds.size < 2) {
                events.push({
                    type: 'WARNING',
                    timestamp: now,
                    conditionId,
                    message: 'Paired execution BUY legs did not span two distinct outcomes',
                    details: { tokenIds: [...uniqueTokenIds] },
                });
                continue;
            }

            const dedupe = [...this.positions.values()].some((position) =>
                position.conditionId === conditionId
                && Math.abs(position.openedAt - result.timestamp) <= 10_000
                && position.status !== 'REDEEMED',
            );
            if (dedupe) {
                continue;
            }

            const sorted = [...orders].sort((a, b) => a.tokenId.localeCompare(b.tokenId));
            const first = sorted[0];
            const second = sorted[1];
            const shares = Math.max(0, Math.min(first.size, second.size));
            const totalNotionalUsd = Math.max(0, first.notionalUsd + second.notionalUsd);
            if (!Number.isFinite(shares) || shares <= 0) {
                continue;
            }

            const id = `${conditionId}:${result.timestamp}:${Math.round(shares * 1e6)}`;
            const position: AtomicSettlementPosition = {
                id,
                strategy: result.strategy,
                conditionId,
                market: result.market,
                yesTokenId: String(first.tokenId),
                noTokenId: String(second.tokenId),
                shares,
                totalNotionalUsd,
                openedAt: result.timestamp,
                updatedAt: now,
                status: 'TRACKED',
                entryBookedNotionalUsd: totalNotionalUsd,
                redeemAttempts: 0,
                nextRedeemAttemptAt: now,
            };
            this.positions.set(id, position);
            dirty = true;

            const ledgerAfterReserve = await this.reserveLiveEntryNotional(totalNotionalUsd).catch((error) => {
                const message = error instanceof Error ? error.message : String(error);
                events.push({
                    type: 'ERROR',
                    timestamp: Date.now(),
                    conditionId,
                    positionId: id,
                    message: `Live ledger reserve failed: ${message}`,
                    details: {
                        totalNotionalUsd,
                    },
                });
                return null;
            });

            events.push({
                type: 'INFO',
                timestamp: now,
                conditionId,
                positionId: id,
                message: `Tracked paired position for settlement (${shares.toFixed(4)} shares)`,
                details: {
                    yesTokenId: position.yesTokenId,
                    noTokenId: position.noTokenId,
                    totalNotionalUsd,
                    ledger: ledgerAfterReserve ? {
                        cash: ledgerAfterReserve.cash,
                        reserved: ledgerAfterReserve.reserved,
                        realizedPnl: ledgerAfterReserve.realizedPnl,
                        equity: ledgerAfterReserve.equity,
                    } : undefined,
                },
            });
        }

        if (dirty) {
            await this.persist();
        }
        return events;
    }

    public async runCycle(tradingMode: 'PAPER' | 'LIVE', livePostingEnabled: boolean): Promise<SettlementEvent[]> {
        return this.withRunCycleLock(async () => {
            if (this.running) {
                return [];
            }
            this.running = true;
            this.lastRunAt = Date.now();

            const events: SettlementEvent[] = [];
            let dirty = false;
            const setIfChanged = <K extends keyof AtomicSettlementPosition>(
                position: AtomicSettlementPosition,
                key: K,
                value: AtomicSettlementPosition[K],
            ) => {
                if (position[key] !== value) {
                    position[key] = value;
                    dirty = true;
                }
            };

            try {
                for (const position of this.positions.values()) {
                if (position.status === 'REDEEMED') {
                    continue;
                }

                const cycleNow = Date.now();
                if ((position.marketNotFoundUntil ?? 0) > cycleNow) {
                    continue;
                }
                if (cycleNow - position.openedAt > this.staleMs && position.status !== 'REDEEMABLE') {
                    setIfChanged(position, 'status', 'MANUAL_ACTION_REQUIRED');
                    setIfChanged(position, 'note', 'Position stale without redeemable signal');
                    setIfChanged(position, 'updatedAt', cycleNow);
                }

                const marketState = await this.fetchMarketState(position.conditionId);
                if (marketState.notFound) {
                    const misses = (position.marketNotFoundCount ?? 0) + 1;
                    const resumeAt = cycleNow + this.notFoundBackoffMs;
                    setIfChanged(position, 'marketNotFoundCount', misses);
                    setIfChanged(position, 'marketNotFoundUntil', resumeAt);
                    setIfChanged(position, 'status', 'MANUAL_ACTION_REQUIRED');
                    setIfChanged(position, 'note', `Condition not found on CLOB (404); retry after ${new Date(resumeAt).toISOString()}`);
                    setIfChanged(position, 'updatedAt', cycleNow);
                    continue;
                }
                if ((position.marketNotFoundCount ?? 0) > 0 || (position.marketNotFoundUntil ?? 0) > 0) {
                    setIfChanged(position, 'marketNotFoundCount', 0);
                    setIfChanged(position, 'marketNotFoundUntil', undefined);
                }
                setIfChanged(position, 'closed', marketState.closed ?? position.closed);
                if (marketState.winnerTokenId) {
                    setIfChanged(position, 'winnerTokenId', marketState.winnerTokenId);
                } else if (marketState.invalid) {
                    setIfChanged(position, 'winnerTokenId', 'INVALID');
                }
                position.settlementCheckedAt = Date.now();

                if (!marketState.closed || (!marketState.winnerTokenId && !marketState.invalid)) {
                    if (position.status === 'TRACKED') {
                        setIfChanged(position, 'status', 'AWAITING_RESOLUTION');
                        setIfChanged(position, 'updatedAt', Date.now());
                    }
                    continue;
                }

                if (marketState.invalid) {
                    setIfChanged(position, 'note', 'Market resolved as invalid/cancelled; attempting refund redemption');
                }

                const redeemableShares = await this.fetchRedeemableShares(position.conditionId);
                setIfChanged(position, 'redeemableShares', redeemableShares);

                if (redeemableShares < this.minRedeemableShares) {
                    setIfChanged(position, 'status', 'RESOLVED');
                    setIfChanged(position, 'note', 'No redeemable balance detected (likely already redeemed or dust)');
                    setIfChanged(position, 'updatedAt', Date.now());
                    continue;
                }

                setIfChanged(position, 'status', 'REDEEMABLE');
                setIfChanged(position, 'note', `Redeemable balance detected: ${redeemableShares.toFixed(4)} shares`);
                setIfChanged(position, 'updatedAt', Date.now());

                if (!this.autoRedeemEnabled) {
                    setIfChanged(position, 'status', 'MANUAL_ACTION_REQUIRED');
                    setIfChanged(position, 'note', 'POLY_AUTO_REDEEM_ENABLED=false');
                    setIfChanged(position, 'updatedAt', Date.now());
                    continue;
                }

                if (tradingMode !== 'LIVE' || !livePostingEnabled) {
                    setIfChanged(position, 'note', 'Awaiting LIVE mode with live posting enabled for auto-redeem');
                    setIfChanged(position, 'updatedAt', Date.now());
                    continue;
                }

                if (!this.enabled || !this.contract || !this.canDirectRedeem) {
                    setIfChanged(position, 'status', 'MANUAL_ACTION_REQUIRED');
                    setIfChanged(position, 'note', 'Direct redemption unavailable (proxy wallet relayer flow required)');
                    setIfChanged(position, 'updatedAt', Date.now());
                    continue;
                }

                if (Date.now() < position.nextRedeemAttemptAt) {
                    continue;
                }

                const redeemResult = await this.tryRedeem(position.conditionId);
                setIfChanged(position, 'redeemAttempts', position.redeemAttempts + 1);
                setIfChanged(position, 'nextRedeemAttemptAt', Date.now() + this.retryCooldownMs);

                if (redeemResult.ok) {
                    setIfChanged(position, 'status', 'REDEEMED');
                    setIfChanged(position, 'redeemTxHash', redeemResult.txHash || undefined);
                    setIfChanged(position, 'lastError', undefined);
                    setIfChanged(position, 'note', 'Redeemed on-chain');
                    setIfChanged(position, 'updatedAt', Date.now());
                    const entryNotionalUsd = Math.max(
                        0,
                        Number(position.entryBookedNotionalUsd ?? position.totalNotionalUsd),
                    );
                    const proceedsUsd = Math.max(0, Number(redeemableShares));
                    const ledgerAfterSettle = await this.settleLiveRedeem(entryNotionalUsd, proceedsUsd).catch((error) => {
                        const message = error instanceof Error ? error.message : String(error);
                        events.push({
                            type: 'ERROR',
                            timestamp: Date.now(),
                            conditionId: position.conditionId,
                            positionId: position.id,
                            message: `Live ledger settle failed: ${message}`,
                            details: {
                                entryNotionalUsd,
                                proceedsUsd,
                            },
                        });
                        return null;
                    });
                    const pnlUsd = proceedsUsd - entryNotionalUsd;
                    setIfChanged(position, 'redeemProceedsUsd', proceedsUsd);
                    setIfChanged(position, 'realizedPnlUsd', pnlUsd);
                    events.push({
                        type: 'REDEEMED',
                        timestamp: Date.now(),
                        conditionId: position.conditionId,
                        positionId: position.id,
                        message: `Redeemed condition ${position.conditionId}`,
                        details: {
                            txHash: redeemResult.txHash,
                            redeemableShares,
                            proceedsUsd,
                            entryNotionalUsd,
                            realizedPnlUsd: pnlUsd,
                            ledger: ledgerAfterSettle ? {
                                cash: ledgerAfterSettle.cash,
                                reserved: ledgerAfterSettle.reserved,
                                realizedPnl: ledgerAfterSettle.realizedPnl,
                                equity: ledgerAfterSettle.equity,
                            } : undefined,
                        },
                    });
                } else {
                    const failure = redeemResult.error || 'Unknown redeem failure';
                    setIfChanged(position, 'status', 'FAILED');
                    setIfChanged(position, 'lastError', failure);
                    setIfChanged(position, 'note', failure);
                    setIfChanged(position, 'updatedAt', Date.now());
                    events.push({
                        type: 'ERROR',
                        timestamp: Date.now(),
                        conditionId: position.conditionId,
                        positionId: position.id,
                        message: 'Redeem attempt failed',
                        details: {
                            error: failure,
                            attempts: position.redeemAttempts,
                        },
                    });
                }
                }
            } finally {
                this.running = false;
                if (dirty) {
                    await this.persist();
                }
            }

            return events;
        });
    }

    private async fetchMarketState(conditionId: string): Promise<MarketStateResult> {
        try {
            const url = `${this.clobHost}/markets/${conditionId}`;
            const response = await axios.get(url, {
                timeout: 10_000,
                headers: { 'User-Agent': 'ai-trader-settlement-worker/1.0' },
            });
            const market = asRecord(response.data);
            if (!market) {
                return { closed: false, winnerTokenId: null, invalid: false, notFound: false };
            }

            const closed = parseMarketClosed(market.closed) || false;
            const invalidHints = [
                asString(market.resolution),
                asString(market.result),
                asString(market.outcome),
                asString(market.status),
                asString(market.finalOutcome),
            ]
                .filter((value): value is string => Boolean(value))
                .map((value) => value.toLowerCase());
            let invalid = invalidHints.some((value) => (
                value.includes('invalid')
                || value.includes('cancel')
                || value.includes('void')
            ));
            const tokens = Array.isArray(market.tokens) ? market.tokens : [];
            let winnerTokenId: string | null = null;
            for (const token of tokens) {
                const t = asRecord(token);
                if (!t) {
                    continue;
                }
                const winner = t.winner === true;
                if (winner) {
                    winnerTokenId = asString(t.token_id) || asString(t.tokenId) || null;
                    if (winnerTokenId) {
                        break;
                    }
                }
            }
            if (closed && !winnerTokenId) {
                invalid = true;
            }

            return {
                closed,
                winnerTokenId,
                invalid,
                notFound: false,
            };
        } catch (error) {
            if (axios.isAxiosError(error) && error.response?.status === 404) {
                return { closed: false, winnerTokenId: null, invalid: false, notFound: true };
            }
            const message = error instanceof Error ? error.message : String(error);
            logger.warn(`[Settlement] market state fetch failed for ${conditionId}: ${message}`);
            return { closed: false, winnerTokenId: null, invalid: false, notFound: false };
        }
    }

    private async fetchRedeemableShares(conditionId: string): Promise<number> {
        if (!this.funderAddress) {
            return 0;
        }

        try {
            const params = new URLSearchParams({
                user: this.funderAddress,
                redeemable: 'true',
                conditionId,
                sizeThreshold: String(this.minRedeemableShares),
                limit: '500',
            });
            const url = `${this.dataApiHost}/positions?${params.toString()}`;
            const response = await axios.get(url, {
                timeout: 12_000,
                headers: { 'User-Agent': 'ai-trader-settlement-worker/1.0' },
            });

            const rows = Array.isArray(response.data)
                ? response.data
                : Array.isArray(asRecord(response.data)?.data)
                ? (asRecord(response.data)!.data as unknown[])
                : [];

            let totalShares = 0;
            for (const row of rows) {
                const record = asRecord(row);
                if (!record) {
                    continue;
                }
                const rowConditionId = normalizeConditionId(
                    asString(record.conditionId)
                    || asString(record.condition_id)
                    || asString(record.conditionID),
                );
                if (rowConditionId !== conditionId) {
                    continue;
                }

                const size = asNumber(record.size)
                    ?? asNumber(record.balance)
                    ?? asNumber(record.amount)
                    ?? 0;
                if (size > 0) {
                    totalShares += size;
                }
            }
            return totalShares;
        } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            logger.warn(`[Settlement] redeemable positions fetch failed for ${conditionId}: ${message}`);
            return 0;
        }
    }

    private async tryRedeem(conditionId: string): Promise<{ ok: boolean; txHash?: string; error?: string }> {
        if (!this.contract) {
            return { ok: false, error: 'Settlement contract not initialized' };
        }

        try {
            const tx = await this.contract.redeemPositions(
                this.collateralTokenAddress,
                ZeroHash,
                conditionId,
                POSITION_INDEX_SETS,
            );
            const receipt = await tx.wait();
            const status = Number(receipt?.status ?? 0);
            if (status !== 1) {
                return {
                    ok: false,
                    txHash: tx.hash,
                    error: `redeem transaction reverted (status=${status})`,
                };
            }

            logger.info(`[Settlement] redeemed ${conditionId} tx=${tx.hash}`);
            return { ok: true, txHash: tx.hash };
        } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            return { ok: false, error: message };
        }
    }

    private async persist(): Promise<void> {
        const payload = JSON.stringify({
            positions: [...this.positions.values()],
            updatedAt: Date.now(),
        });
        await this.redis.set(REDIS_KEY, payload);
    }
}

export function settlementEventToExecutionLog(event: SettlementEvent): Record<string, unknown> {
    const side = event.type === 'REDEEMED'
        ? 'SETTLE_REDEEM'
        : event.type === 'ERROR'
        ? 'SETTLE_ERR'
        : event.type === 'WARNING'
        ? 'SETTLE_WARN'
        : 'SETTLE_INFO';

    return {
        timestamp: event.timestamp,
        side,
        market: event.conditionId || 'Settlement',
        price: event.message,
        size: event.positionId || '',
        mode: 'SETTLEMENT',
        details: event.details || {},
    };
}

export function extractAtomicExecutionConditionIds(result: PolymarketLiveExecutionResult): string[] {
    if (result.strategy !== 'ATOMIC_ARB') {
        return [];
    }

    const ids = new Set<string>();
    for (const order of result.orders) {
        const conditionId = normalizeConditionId(order.conditionId || null);
        if (conditionId) {
            ids.add(conditionId);
        }
    }
    return [...ids];
}
