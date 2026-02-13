import axios from 'axios';
import { Contract, JsonRpcProvider, Wallet, ZeroHash, isAddress } from 'ethers';
import type { PolymarketLiveExecutionResult } from './PolymarketPreflightService';
import { logger } from '../utils/logger';

const CONDITIONAL_TOKENS_ABI = [
    'function redeemPositions(address collateralToken, bytes32 parentCollectionId, bytes32 conditionId, uint256[] indexSets)',
];

const DEFAULT_DATA_API_HOST = 'https://data-api.polymarket.com';
const DEFAULT_CLOB_HOST = 'https://clob.polymarket.com';
const DEFAULT_POLYGON_RPC_URL = 'https://polygon-rpc.com';
const DEFAULT_CONDITIONAL_TOKENS_ADDRESS = '0x4D97DCd97eC945f40cF65F87097ACe5EA0476045';
const DEFAULT_COLLATERAL_TOKEN_ADDRESS = '0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174';
const DEFAULT_POLL_INTERVAL_MS = 30_000;
const DEFAULT_RETRY_COOLDOWN_MS = 60_000;
const DEFAULT_MIN_REDEEMABLE_SHARES = 0.01;
const DEFAULT_SETTLEMENT_STALE_MS = 60 * 60 * 1000;
const POSITION_INDEX_SETS = [1n, 2n];
const REDIS_KEY = 'system:polymarket:settlement:atomic:v1';

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
    lastError?: string;
    note?: string;
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

type SettlementSnapshot = {
    enabled: boolean;
    autoRedeemEnabled: boolean;
    canDirectRedeem: boolean;
    signerAddress: string | null;
    funderAddress: string | null;
    pollIntervalMs: number;
    positions: AtomicSettlementPosition[];
    lastRunAt: number;
    running: boolean;
};

type RedisLike = {
    get(key: string): Promise<string | null>;
    set(key: string, value: string): Promise<unknown>;
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
    private readonly autoRedeemEnabled: boolean;
    private readonly minRedeemableShares: number;
    private readonly staleMs: number;
    private readonly enabled: boolean;
    private readonly canDirectRedeem: boolean;
    private readonly funderAddress: string | null;
    private readonly signerAddress: string | null;
    private readonly conditionalTokensAddress: string;
    private readonly collateralTokenAddress: string;
    private readonly contract: Contract | null;

    private readonly positions = new Map<string, AtomicSettlementPosition>();
    private running = false;
    private lastRunAt = 0;

    constructor(redis: RedisLike) {
        this.redis = redis;

        this.dataApiHost = (process.env.POLY_DATA_API_HOST || DEFAULT_DATA_API_HOST).replace(/\/+$/, '');
        this.clobHost = (process.env.POLYMARKET_CLOB_HOST || DEFAULT_CLOB_HOST).replace(/\/+$/, '');
        this.pollIntervalMs = Math.max(5_000, Number(process.env.POLY_SETTLEMENT_POLL_MS || DEFAULT_POLL_INTERVAL_MS));
        this.retryCooldownMs = Math.max(5_000, Number(process.env.POLY_REDEEM_RETRY_COOLDOWN_MS || DEFAULT_RETRY_COOLDOWN_MS));
        this.autoRedeemEnabled = process.env.POLY_AUTO_REDEEM_ENABLED === 'true';
        this.minRedeemableShares = Math.max(0.0001, Number(process.env.POLY_MIN_REDEEMABLE_SHARES || DEFAULT_MIN_REDEEMABLE_SHARES));
        this.staleMs = Math.max(60_000, Number(process.env.POLY_SETTLEMENT_STALE_MS || DEFAULT_SETTLEMENT_STALE_MS));

        const privateKey = asString(process.env.POLY_PRIVATE_KEY) || asString(process.env.PRIVATE_KEY);
        const rpcUrl = asString(process.env.POLY_RPC_URL) || asString(process.env.POLYGON_RPC_URL) || DEFAULT_POLYGON_RPC_URL;
        const funderRaw = asString(process.env.POLY_FUNDER_ADDRESS) || asString(process.env.PROXY_WALLET_ADDRESS);

        this.conditionalTokensAddress = asString(process.env.POLY_CONDITIONAL_TOKENS_ADDRESS) || DEFAULT_CONDITIONAL_TOKENS_ADDRESS;
        this.collateralTokenAddress = asString(process.env.POLY_COLLATERAL_TOKEN_ADDRESS) || DEFAULT_COLLATERAL_TOKEN_ADDRESS;

        let signerAddress: string | null = null;
        let contract: Contract | null = null;
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
                enabled = true;

                const normalizedFunder = funderRaw && isAddress(funderRaw) ? funderRaw.toLowerCase() : signerAddress;
                this.funderAddress = normalizedFunder;
                canDirectRedeem = normalizedFunder === signerAddress;
                this.signerAddress = signerAddress;
                this.contract = contract;
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
            lastRunAt: this.lastRunAt,
            running: this.running,
        };
    }

    public async init(): Promise<void> {
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
        } catch {
            logger.warn('[Settlement] failed to parse persisted state; starting with empty state');
        }
    }

    public async reset(): Promise<void> {
        this.positions.clear();
        this.lastRunAt = Date.now();
        await this.persist();
    }

    public async registerAtomicExecution(result: PolymarketLiveExecutionResult): Promise<SettlementEvent[]> {
        if (result.strategy !== 'ATOMIC_ARB' || result.dryRun) {
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
                    message: 'Atomic execution posted fewer than two BUY legs; manual hedge review required',
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
                    message: 'Atomic execution BUY legs did not span two distinct outcomes',
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
                redeemAttempts: 0,
                nextRedeemAttemptAt: now,
            };
            this.positions.set(id, position);
            dirty = true;

            events.push({
                type: 'INFO',
                timestamp: now,
                conditionId,
                positionId: id,
                message: `Tracked atomic pair for settlement (${shares.toFixed(4)} shares)`,
                details: {
                    yesTokenId: position.yesTokenId,
                    noTokenId: position.noTokenId,
                    totalNotionalUsd,
                },
            });
        }

        if (dirty) {
            await this.persist();
        }
        return events;
    }

    public async runCycle(tradingMode: 'PAPER' | 'LIVE', livePostingEnabled: boolean): Promise<SettlementEvent[]> {
        if (this.running) {
            return [];
        }
        this.running = true;
        this.lastRunAt = Date.now();

        const events: SettlementEvent[] = [];
        let dirty = false;

        try {
            for (const position of this.positions.values()) {
                if (position.status === 'REDEEMED') {
                    continue;
                }

                if (Date.now() - position.openedAt > this.staleMs && position.status !== 'REDEEMABLE') {
                    position.status = 'MANUAL_ACTION_REQUIRED';
                    position.note = 'Position stale without redeemable signal';
                    position.updatedAt = Date.now();
                    dirty = true;
                }

                const marketState = await this.fetchMarketState(position.conditionId);
                position.closed = marketState.closed ?? position.closed;
                position.winnerTokenId = marketState.winnerTokenId || position.winnerTokenId;
                position.settlementCheckedAt = Date.now();

                if (!marketState.closed || !marketState.winnerTokenId) {
                    if (position.status === 'TRACKED' || position.status === 'AWAITING_RESOLUTION') {
                        position.status = 'AWAITING_RESOLUTION';
                    }
                    position.updatedAt = Date.now();
                    continue;
                }

                const redeemableShares = await this.fetchRedeemableShares(position.conditionId);
                position.redeemableShares = redeemableShares;
                position.updatedAt = Date.now();

                if (redeemableShares < this.minRedeemableShares) {
                    position.status = 'RESOLVED';
                    position.note = 'No redeemable balance detected (likely already redeemed or dust)';
                    continue;
                }

                position.status = 'REDEEMABLE';
                position.note = `Redeemable balance detected: ${redeemableShares.toFixed(4)} shares`;

                if (!this.autoRedeemEnabled) {
                    position.status = 'MANUAL_ACTION_REQUIRED';
                    position.note = 'POLY_AUTO_REDEEM_ENABLED=false';
                    continue;
                }

                if (tradingMode !== 'LIVE' || !livePostingEnabled) {
                    position.note = 'Awaiting LIVE mode with live posting enabled for auto-redeem';
                    continue;
                }

                if (!this.enabled || !this.contract || !this.canDirectRedeem) {
                    position.status = 'MANUAL_ACTION_REQUIRED';
                    position.note = 'Direct redemption unavailable (proxy wallet relayer flow required)';
                    continue;
                }

                if (Date.now() < position.nextRedeemAttemptAt) {
                    continue;
                }

                const redeemResult = await this.tryRedeem(position.conditionId);
                position.redeemAttempts += 1;
                position.nextRedeemAttemptAt = Date.now() + this.retryCooldownMs;

                if (redeemResult.ok) {
                    position.status = 'REDEEMED';
                    position.redeemTxHash = redeemResult.txHash || undefined;
                    position.lastError = undefined;
                    position.note = 'Redeemed on-chain';
                    events.push({
                        type: 'REDEEMED',
                        timestamp: Date.now(),
                        conditionId: position.conditionId,
                        positionId: position.id,
                        message: `Redeemed condition ${position.conditionId}`,
                        details: {
                            txHash: redeemResult.txHash,
                            redeemableShares,
                        },
                    });
                } else {
                    position.status = 'FAILED';
                    position.lastError = redeemResult.error || 'Unknown redeem failure';
                    position.note = position.lastError;
                    events.push({
                        type: 'ERROR',
                        timestamp: Date.now(),
                        conditionId: position.conditionId,
                        positionId: position.id,
                        message: 'Redeem attempt failed',
                        details: {
                            error: position.lastError,
                            attempts: position.redeemAttempts,
                        },
                    });
                }
                dirty = true;
            }
        } finally {
            this.running = false;
            if (dirty) {
                await this.persist();
            }
        }

        return events;
    }

    private async fetchMarketState(conditionId: string): Promise<{ closed: boolean; winnerTokenId: string | null }> {
        try {
            const url = `${this.clobHost}/markets/${conditionId}`;
            const response = await axios.get(url, {
                timeout: 10_000,
                headers: { 'User-Agent': 'ai-trader-settlement-worker/1.0' },
            });
            const market = asRecord(response.data);
            if (!market) {
                return { closed: false, winnerTokenId: null };
            }

            const closed = parseMarketClosed(market.closed) || false;
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

            return {
                closed,
                winnerTokenId,
            };
        } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            logger.warn(`[Settlement] market state fetch failed for ${conditionId}: ${message}`);
            return { closed: false, winnerTokenId: null };
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
