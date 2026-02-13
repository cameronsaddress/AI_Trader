import { ClobClient, OrderType, Side } from '@polymarket/clob-client';
import { Wallet } from '@ethersproject/wallet';
import { logger } from '../utils/logger';

type PreflightOrderCandidate = {
    tokenId: string;
    conditionId?: string;
    side: Side;
    price: number;
    inputSize: number;
    sizeUnit: 'USD_NOTIONAL' | 'SHARES';
    notionalUsd: number;
    size: number;
    tickSize?: string;
    negRisk?: boolean;
};

type ParsedExecutionCandidate = {
    market: string;
    strategy: string;
    timestamp: number;
    mode: string;
    orders: PreflightOrderCandidate[];
    fingerprint: string;
};

export type PreflightOrderResult = {
    tokenId: string;
    conditionId?: string;
    side: Side;
    price: number;
    inputSize: number;
    sizeUnit: 'USD_NOTIONAL' | 'SHARES';
    notionalUsd: number;
    size: number;
    tickSize?: string;
    negRisk?: boolean;
    ok: boolean;
    error?: string;
    maker?: string;
    signer?: string;
    signaturePreview?: string;
};

export type PolymarketPreflightResult = {
    market: string;
    strategy: string;
    mode: string;
    timestamp: number;
    total: number;
    signed: number;
    failed: number;
    ok: boolean;
    orders: PreflightOrderResult[];
    error?: string;
};

export type LiveExecutionOrderResult = {
    tokenId: string;
    conditionId?: string;
    side: Side;
    price: number;
    inputSize: number;
    sizeUnit: 'USD_NOTIONAL' | 'SHARES';
    notionalUsd: number;
    size: number;
    ok: boolean;
    orderId?: string;
    status?: string;
    transactions?: string[];
    error?: string;
};

export type PolymarketLiveExecutionResult = {
    market: string;
    strategy: string;
    mode: string;
    timestamp: number;
    total: number;
    posted: number;
    failed: number;
    ok: boolean;
    dryRun: boolean;
    reason?: string;
    orders: LiveExecutionOrderResult[];
};

type SignedOrderCandidate = {
    tokenId: string;
    conditionId?: string;
    side: Side;
    price: number;
    inputSize: number;
    sizeUnit: 'USD_NOTIONAL' | 'SHARES';
    notionalUsd: number;
    size: number;
    signedOrder: unknown;
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

function parseSide(input: unknown): Side | null {
    const side = asString(input)?.toUpperCase();
    if (side === 'BUY') {
        return Side.BUY;
    }
    if (side === 'SELL') {
        return Side.SELL;
    }
    return null;
}

function parseLiveOrderType(input: string | undefined): OrderType {
    const normalized = (input || '').trim().toUpperCase();
    if (normalized === 'GTC') {
        return OrderType.GTC;
    }
    if (normalized === 'GTD') {
        return OrderType.GTD;
    }
    if (normalized === 'FAK') {
        return OrderType.FAK;
    }
    return OrderType.FOK;
}

function maskSignature(signature: string | undefined): string | undefined {
    if (!signature) {
        return undefined;
    }
    if (signature.length <= 18) {
        return signature;
    }
    return `${signature.slice(0, 10)}...${signature.slice(-8)}`;
}

function parseExecutionCandidate(input: unknown): ParsedExecutionCandidate | null {
    const payload = asRecord(input);
    if (!payload) {
        return null;
    }

    const mode = asString(payload.mode) || '';
    if (mode.toUpperCase() !== 'LIVE_DRY_RUN') {
        return null;
    }

    const details = asRecord(payload.details);
    const preflight = details ? asRecord(details.preflight) : null;
    const rawOrders = preflight?.orders;
    if (!Array.isArray(rawOrders) || rawOrders.length === 0) {
        return null;
    }

    const orders: PreflightOrderCandidate[] = [];
    for (const rawOrder of rawOrders) {
        const order = asRecord(rawOrder);
        if (!order) {
            continue;
        }

        const tokenId = asString(order.token_id);
        const side = parseSide(order.side);
        const price = asNumber(order.price);
        const inputSize = asNumber(order.size);
        const sizeUnitRaw = asString(order.size_unit)?.toUpperCase();
        const sizeUnit = sizeUnitRaw === 'SHARES' ? 'SHARES' : 'USD_NOTIONAL';

        if (!tokenId || !side || price === null || inputSize === null) {
            continue;
        }

        let size = inputSize;
        let notionalUsd = price * size;
        if (sizeUnit === 'USD_NOTIONAL') {
            notionalUsd = inputSize;
            size = inputSize / price;
        }

        if (!(price > 0 && price < 1 && size > 0 && notionalUsd > 0)) {
            continue;
        }

        const tickSize = asString(order.tick_size) || undefined;
        const negRisk = typeof order.neg_risk === 'boolean' ? order.neg_risk : undefined;

        orders.push({
            tokenId,
            conditionId: asString(order.condition_id) || undefined,
            side,
            price,
            inputSize,
            sizeUnit,
            notionalUsd,
            size,
            tickSize,
            negRisk,
        });
    }

    if (orders.length === 0) {
        return null;
    }

    const strategy = asString(preflight?.strategy) || asString(payload.side) || 'UNKNOWN';
    const market = asString(payload.market) || 'Polymarket';
    const timestamp = asNumber(payload.timestamp) ?? Date.now();
    const fingerprint = `${strategy}:${orders
        .map((order) => `${order.tokenId}:${order.side}:${order.price.toFixed(4)}:${order.sizeUnit}:${order.inputSize.toFixed(4)}`)
        .join('|')}`;

    return {
        market,
        strategy,
        timestamp,
        mode: 'LIVE_DRY_RUN',
        orders,
        fingerprint,
    };
}

export class PolymarketPreflightService {
    private readonly host: string;
    private readonly chainId: number;
    private readonly signatureType: number;
    private readonly funderAddress: string | null;
    private readonly privateKey: string | null;
    private readonly minIntervalMs: number;
    private readonly executionMinIntervalMs: number;
    private readonly maxOrderNotionalUsd: number;
    private readonly maxSignalNotionalUsd: number;
    private readonly liveOrderType: OrderType;
    private readonly lastPreflightByFingerprint = new Map<string, number>();
    private readonly lastExecutionByFingerprint = new Map<string, number>();
    private readonly retryOnError: boolean;
    private readonly livePostingEnabled: boolean;

    private client: ClobClient | null = null;
    private initPromise: Promise<void> | null = null;
    private disabledReason: string | null = null;

    constructor() {
        this.host = process.env.POLYMARKET_CLOB_HOST || 'https://clob.polymarket.com';
        this.chainId = Number(process.env.POLY_CHAIN_ID || '137');
        this.signatureType = Number(process.env.POLY_SIGNATURE_TYPE || '1');
        this.funderAddress = process.env.POLY_FUNDER_ADDRESS || process.env.PROXY_WALLET_ADDRESS || null;
        this.privateKey = process.env.POLY_PRIVATE_KEY || process.env.PRIVATE_KEY || null;
        this.minIntervalMs = Math.max(0, Number(process.env.POLY_PREFLIGHT_MIN_INTERVAL_MS || '1200'));
        this.executionMinIntervalMs = Math.max(0, Number(process.env.POLY_EXECUTION_MIN_INTERVAL_MS || '2500'));
        this.maxOrderNotionalUsd = Math.max(1, Number(process.env.POLY_MAX_ORDER_NOTIONAL_USD || '500'));
        this.maxSignalNotionalUsd = Math.max(1, Number(process.env.POLY_MAX_SIGNAL_NOTIONAL_USD || '2000'));
        this.liveOrderType = parseLiveOrderType(process.env.POLY_LIVE_ORDER_TYPE);
        this.retryOnError = process.env.POLY_PREFLIGHT_RETRY_ON_ERROR === 'true';
        this.livePostingEnabled = process.env.LIVE_ORDER_POSTING_ENABLED === 'true';

        if (!this.privateKey) {
            this.disabledReason = 'PRIVATE_KEY/POLY_PRIVATE_KEY not set';
            logger.warn('[PolymarketPreflight] disabled: missing private key');
        } else if (!this.funderAddress) {
            this.disabledReason = 'PROXY_WALLET_ADDRESS/POLY_FUNDER_ADDRESS not set';
            logger.warn('[PolymarketPreflight] disabled: missing funder address');
        } else if (!Number.isFinite(this.chainId) || this.chainId <= 0) {
            this.disabledReason = 'POLY_CHAIN_ID is invalid';
            logger.warn('[PolymarketPreflight] disabled: invalid chain id');
        } else if (!Number.isFinite(this.signatureType) || this.signatureType < 0) {
            this.disabledReason = 'POLY_SIGNATURE_TYPE is invalid';
            logger.warn('[PolymarketPreflight] disabled: invalid signature type');
        }
    }

    private async ensureClient(): Promise<boolean> {
        if (this.client) {
            return true;
        }
        if (this.disabledReason) {
            return false;
        }
        if (!this.initPromise) {
            this.initPromise = this.initClient();
        }
        await this.initPromise;
        return this.client !== null;
    }

    private async initClient(): Promise<void> {
        if (this.disabledReason) {
            return;
        }

        try {
            const signer = new Wallet(this.privateKey!);
            const l1Client = new ClobClient(this.host, this.chainId as 137 | 80002, signer);
            const creds = await l1Client.createOrDeriveApiKey();

            this.client = new ClobClient(
                this.host,
                this.chainId as 137 | 80002,
                signer,
                creds,
                this.signatureType as 0 | 1 | 2,
                this.funderAddress!,
                undefined,
                true,
                undefined,
                undefined,
                this.retryOnError,
            );

            logger.info(
                `[PolymarketPreflight] initialized for funder ${this.funderAddress} on chain ${this.chainId} (${this.host})`,
            );
        } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            this.disabledReason = `Initialization failed: ${message}`;
            logger.error(`[PolymarketPreflight] initialization failed: ${message}`);
        }
    }

    private shouldThrottlePreflight(candidate: ParsedExecutionCandidate): boolean {
        const now = Date.now();
        const last = this.lastPreflightByFingerprint.get(candidate.fingerprint) || 0;
        if (now - last < this.minIntervalMs) {
            return true;
        }
        this.lastPreflightByFingerprint.set(candidate.fingerprint, now);
        return false;
    }

    private shouldThrottleExecution(candidate: ParsedExecutionCandidate): boolean {
        const now = Date.now();
        const last = this.lastExecutionByFingerprint.get(candidate.fingerprint) || 0;
        if (now - last < this.executionMinIntervalMs) {
            return true;
        }
        this.lastExecutionByFingerprint.set(candidate.fingerprint, now);
        return false;
    }

    private buildUnavailablePreflight(candidate: ParsedExecutionCandidate): PolymarketPreflightResult {
        return {
            market: candidate.market,
            strategy: candidate.strategy,
            mode: candidate.mode,
            timestamp: candidate.timestamp,
            total: candidate.orders.length,
            signed: 0,
            failed: candidate.orders.length,
            ok: false,
            orders: candidate.orders.map((order) => ({
                tokenId: order.tokenId,
                conditionId: order.conditionId,
                side: order.side,
                price: order.price,
                inputSize: order.inputSize,
                sizeUnit: order.sizeUnit,
                notionalUsd: order.notionalUsd,
                size: order.size,
                ok: false,
                error: this.disabledReason || 'Polymarket preflight client unavailable',
            })),
            error: this.disabledReason || 'Polymarket preflight client unavailable',
        };
    }

    private async signCandidateOrders(
        candidate: ParsedExecutionCandidate,
    ): Promise<{ results: PreflightOrderResult[]; signedOrders: SignedOrderCandidate[] }> {
        const results: PreflightOrderResult[] = [];
        const signedOrders: SignedOrderCandidate[] = [];

        for (const order of candidate.orders) {
            try {
                const tickSize = order.tickSize || (await this.client!.getTickSize(order.tokenId));
                const negRisk = typeof order.negRisk === 'boolean'
                    ? order.negRisk
                    : await this.client!.getNegRisk(order.tokenId);

                const signedOrder = await this.client!.createOrder(
                    {
                        tokenID: order.tokenId,
                        price: order.price,
                        size: order.size,
                        side: order.side,
                    },
                    {
                        tickSize: tickSize as any,
                        negRisk,
                    },
                );

                signedOrders.push({
                    tokenId: order.tokenId,
                    conditionId: order.conditionId,
                    side: order.side,
                    price: order.price,
                    inputSize: order.inputSize,
                    sizeUnit: order.sizeUnit,
                    notionalUsd: order.notionalUsd,
                    size: order.size,
                    signedOrder,
                });

                results.push({
                    tokenId: order.tokenId,
                    conditionId: order.conditionId,
                    side: order.side,
                    price: order.price,
                    inputSize: order.inputSize,
                    sizeUnit: order.sizeUnit,
                    notionalUsd: order.notionalUsd,
                    size: order.size,
                    tickSize,
                    negRisk,
                    ok: true,
                    maker: signedOrder.maker,
                    signer: signedOrder.signer,
                    signaturePreview: maskSignature(signedOrder.signature),
                });
            } catch (error) {
                const message = error instanceof Error ? error.message : String(error);
                results.push({
                    tokenId: order.tokenId,
                    conditionId: order.conditionId,
                    side: order.side,
                    price: order.price,
                    inputSize: order.inputSize,
                    sizeUnit: order.sizeUnit,
                    notionalUsd: order.notionalUsd,
                    size: order.size,
                    ok: false,
                    error: message,
                });
            }
        }

        return { results, signedOrders };
    }

    private validateExecutionNotional(candidate: ParsedExecutionCandidate): string | null {
        let totalNotional = 0;

        for (const order of candidate.orders) {
            const notional = order.notionalUsd;
            if (!Number.isFinite(notional) || notional <= 0) {
                return `Invalid notional for token ${order.tokenId}`;
            }
            if (notional > this.maxOrderNotionalUsd) {
                return `Order notional ${notional.toFixed(2)} exceeds per-order limit ${this.maxOrderNotionalUsd.toFixed(2)}`;
            }
            totalNotional += notional;
        }

        if (totalNotional > this.maxSignalNotionalUsd) {
            return `Signal notional ${totalNotional.toFixed(2)} exceeds max ${this.maxSignalNotionalUsd.toFixed(2)}`;
        }

        return null;
    }

    public static toExecutionLog(result: PolymarketPreflightResult): Record<string, unknown> {
        return {
            timestamp: result.timestamp,
            side: result.ok ? 'PRECHECK' : 'PRECHECK_ERR',
            market: `${result.market} SDK`,
            price: `${result.signed}/${result.total} signed`,
            size: result.strategy,
            mode: result.mode,
            details: {
                ...result,
            },
        };
    }

    public static toLiveExecutionLog(result: PolymarketLiveExecutionResult): Record<string, unknown> {
        const side = result.dryRun
            ? 'LIVE_SAFE'
            : result.ok
            ? 'LIVE_POST'
            : 'LIVE_POST_ERR';

        return {
            timestamp: result.timestamp,
            side,
            market: `${result.market} SDK`,
            price: `${result.posted}/${result.total} posted`,
            size: result.strategy,
            mode: result.mode,
            details: {
                ...result,
            },
        };
    }

    public isLivePostingEnabled(): boolean {
        return this.livePostingEnabled;
    }

    public async preflightFromExecution(input: unknown): Promise<PolymarketPreflightResult | null> {
        const candidate = parseExecutionCandidate(input);
        if (!candidate) {
            return null;
        }

        if (this.shouldThrottlePreflight(candidate)) {
            return null;
        }

        if (!(await this.ensureClient()) || !this.client) {
            return this.buildUnavailablePreflight(candidate);
        }

        const { results: orderResults } = await this.signCandidateOrders(candidate);

        const signed = orderResults.filter((order) => order.ok).length;
        const failed = orderResults.length - signed;

        return {
            market: candidate.market,
            strategy: candidate.strategy,
            mode: candidate.mode,
            timestamp: candidate.timestamp,
            total: orderResults.length,
            signed,
            failed,
            ok: failed === 0,
            orders: orderResults,
            error: failed === 0 ? undefined : `${failed} order(s) failed preflight`,
        };
    }

    public async executeFromExecution(
        input: unknown,
        tradingMode: 'PAPER' | 'LIVE',
    ): Promise<PolymarketLiveExecutionResult | null> {
        const candidate = parseExecutionCandidate(input);
        if (!candidate || tradingMode !== 'LIVE') {
            return null;
        }

        if (this.shouldThrottleExecution(candidate)) {
            return null;
        }

        const notionalError = this.validateExecutionNotional(candidate);
        if (notionalError) {
            return {
                market: candidate.market,
                strategy: candidate.strategy,
                mode: candidate.mode,
                timestamp: candidate.timestamp,
                total: candidate.orders.length,
                posted: 0,
                failed: candidate.orders.length,
                ok: false,
                dryRun: true,
                reason: notionalError,
                orders: candidate.orders.map((order) => ({
                    tokenId: order.tokenId,
                    conditionId: order.conditionId,
                    side: order.side,
                    price: order.price,
                    inputSize: order.inputSize,
                    sizeUnit: order.sizeUnit,
                    notionalUsd: order.notionalUsd,
                    size: order.size,
                    ok: false,
                    error: notionalError,
                })),
            };
        }

        if (!(await this.ensureClient()) || !this.client) {
            return {
                market: candidate.market,
                strategy: candidate.strategy,
                mode: candidate.mode,
                timestamp: candidate.timestamp,
                total: candidate.orders.length,
                posted: 0,
                failed: candidate.orders.length,
                ok: false,
                dryRun: true,
                reason: this.disabledReason || 'Polymarket client unavailable',
                orders: candidate.orders.map((order) => ({
                    tokenId: order.tokenId,
                    conditionId: order.conditionId,
                    side: order.side,
                    price: order.price,
                    inputSize: order.inputSize,
                    sizeUnit: order.sizeUnit,
                    notionalUsd: order.notionalUsd,
                    size: order.size,
                    ok: false,
                    error: this.disabledReason || 'Polymarket client unavailable',
                })),
            };
        }

        const { results: preflightResults, signedOrders } = await this.signCandidateOrders(candidate);
        const preflightFailed = preflightResults.some((order) => !order.ok);
        if (preflightFailed) {
            return {
                market: candidate.market,
                strategy: candidate.strategy,
                mode: candidate.mode,
                timestamp: candidate.timestamp,
                total: preflightResults.length,
                posted: 0,
                failed: preflightResults.filter((order) => !order.ok).length,
                ok: false,
                dryRun: true,
                reason: 'Live posting skipped because preflight signing failed',
                orders: preflightResults.map((order) => ({
                    tokenId: order.tokenId,
                    conditionId: order.conditionId,
                    side: order.side,
                    price: order.price,
                    inputSize: order.inputSize,
                    sizeUnit: order.sizeUnit,
                    notionalUsd: order.notionalUsd,
                    size: order.size,
                    ok: false,
                    error: order.error || 'Preflight signing failed',
                })),
            };
        }

        if (!this.livePostingEnabled) {
            return {
                market: candidate.market,
                strategy: candidate.strategy,
                mode: candidate.mode,
                timestamp: candidate.timestamp,
                total: signedOrders.length,
                posted: 0,
                failed: 0,
                ok: true,
                dryRun: true,
                reason: 'LIVE_ORDER_POSTING_ENABLED=false (safe dry-run)',
                orders: signedOrders.map((order) => ({
                    tokenId: order.tokenId,
                    conditionId: order.conditionId,
                    side: order.side,
                    price: order.price,
                    inputSize: order.inputSize,
                    sizeUnit: order.sizeUnit,
                    notionalUsd: order.notionalUsd,
                    size: order.size,
                    ok: true,
                })),
            };
        }

        const orderResults = await Promise.all(signedOrders.map(async (order): Promise<LiveExecutionOrderResult> => {
            try {
                const response = await this.client!.postOrder(order.signedOrder as any, this.liveOrderType);
                const orderId = asString(asRecord(response)?.orderID);
                const status = asString(asRecord(response)?.status);
                const txHashesRaw = asRecord(response)?.transactionsHashes;
                const transactions = Array.isArray(txHashesRaw)
                    ? txHashesRaw.map((entry) => asString(entry)).filter((entry): entry is string => Boolean(entry))
                    : [];
                const success = Boolean(asRecord(response)?.success) || Boolean(orderId);

                return {
                    tokenId: order.tokenId,
                    conditionId: order.conditionId,
                    side: order.side,
                    price: order.price,
                    inputSize: order.inputSize,
                    sizeUnit: order.sizeUnit,
                    notionalUsd: order.notionalUsd,
                    size: order.size,
                    ok: success,
                    orderId: orderId || undefined,
                    status: status || undefined,
                    transactions: transactions.length > 0 ? transactions : undefined,
                    error: success ? undefined : asString(asRecord(response)?.errorMsg) || 'Order post failed',
                };
            } catch (error) {
                const message = error instanceof Error ? error.message : String(error);
                return {
                    tokenId: order.tokenId,
                    conditionId: order.conditionId,
                    side: order.side,
                    price: order.price,
                    inputSize: order.inputSize,
                    sizeUnit: order.sizeUnit,
                    notionalUsd: order.notionalUsd,
                    size: order.size,
                    ok: false,
                    error: message,
                };
            }
        }));

        const posted = orderResults.filter((order) => order.ok).length;
        const failed = orderResults.length - posted;

        return {
            market: candidate.market,
            strategy: candidate.strategy,
            mode: candidate.mode,
            timestamp: candidate.timestamp,
            total: orderResults.length,
            posted,
            failed,
            ok: failed === 0,
            dryRun: false,
            reason: failed === 0 ? undefined : `${failed} order(s) failed to post`,
            orders: orderResults,
        };
    }
}
