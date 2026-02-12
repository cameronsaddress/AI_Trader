import express from 'express';
import type { NextFunction, Request, Response } from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';
import cors from 'cors';
import helmet from 'helmet';
import dotenv from 'dotenv';

import { MarketDataService } from './services/MarketDataService';
import { ContextBuilder } from './services/ContextBuilder';
import { AgentManager } from './agents/AgentManager';
import { DecisionGate } from './agents/DecisionGate';
import { RiskGuard } from './services/RiskGuard';
import { HyperliquidExecutor, TradingMode } from './services/HyperliquidExecutor';
import { PolymarketPreflightService } from './services/PolymarketPreflightService';
import { PolymarketSettlementService, settlementEventToExecutionLog } from './services/PolymarketSettlementService';
import { StrategyTradeRecorder } from './services/StrategyTradeRecorder';
import { connectRedis, redisClient, subscriber as redisSubscriber } from './config/redis';

dotenv.config();

const app = express();
const httpServer = createServer(app);

const io = new Server(httpServer, {
    cors: {
        origin: (requestOrigin, callback) => {
            const allowed = [
                'http://localhost:3112',
                'http://localhost:5114',
                process.env.FRONTEND_URL,
            ];

            if (!requestOrigin || allowed.includes(requestOrigin) || requestOrigin.match(/^http:\/\/\d+\.\d+\.\d+\.\d+:3112$/)) {
                callback(null, true);
            } else {
                console.warn(`[CORS] Blocked origin: ${requestOrigin}`);
                callback(new Error('Not allowed by CORS'));
            }
        },
        methods: ['GET', 'POST'],
        credentials: true,
    },
});

app.use(helmet());
app.use(cors({
    origin: (requestOrigin, callback) => {
        const allowed = [
            'http://localhost:3112',
            'http://localhost:5114',
            process.env.FRONTEND_URL,
        ];

        if (!requestOrigin || allowed.includes(requestOrigin) || requestOrigin.match(/^http:\/\/\d+\.\d+\.\d+\.\d+:3112$/)) {
            callback(null, true);
        } else {
            callback(new Error('Not allowed by CORS'));
        }
    },
    methods: ['GET', 'POST'],
    credentials: true,
}));
app.use(express.json());

app.get('/health', (_req, res) => {
    res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

type RiskModel = 'FIXED' | 'PERCENT';

type RiskConfig = {
    model: RiskModel;
    value: number;
    timestamp: number;
};

type StrategyTogglePayload = {
    id: string;
    active: boolean;
    timestamp?: number;
};

type TradingModePayload = {
    mode: TradingMode;
    confirmation?: string;
    timestamp?: number;
    live_order_posting_enabled?: boolean;
};

type SimulationResetPayload = {
    bankroll?: number;
    confirmation?: string;
    timestamp?: number;
};

type StrategyMetric = {
    pnl: number;
    daily_trades: number;
    updated_at: number;
};

type StrategyScanState = {
    strategy: string;
    symbol: string;
    timestamp: number;
    passes_threshold: boolean;
    score: number;
    threshold: number;
    reason: string;
};

const STRATEGY_IDS = ['BTC_15M', 'ETH_15M', 'SOL_15M', 'CEX_SNIPER', 'SYNDICATE', 'ATOMIC_ARB', 'OBI_SCALPER'];
const SCANNER_HEARTBEAT_IDS = ['scanner_swarms', 'btc_15m', 'eth_15m', 'sol_15m', 'cex_arb', 'copy_bot', 'atomic_arb', 'obi_scalper'];
const DEFAULT_SIM_BANKROLL = 1000;
const SIM_RESET_ON_BOOT = process.env.SIM_RESET_ON_BOOT === 'true';
const RESET_VALIDATION_TRADES_ON_SIM_RESET = process.env.RESET_VALIDATION_TRADES_ON_SIM_RESET !== 'false';
const CONTROL_PLANE_TOKEN = (process.env.CONTROL_PLANE_TOKEN || '').trim();
const INTELLIGENCE_GATE_ENABLED = process.env.INTELLIGENCE_GATE_ENABLED !== 'false';
const INTELLIGENCE_GATE_MAX_STALENESS_MS = Math.max(500, Number(process.env.INTELLIGENCE_GATE_MAX_STALENESS_MS || '3000'));
const INTELLIGENCE_GATE_MIN_MARGIN = Number(process.env.INTELLIGENCE_GATE_MIN_MARGIN || '0');

const strategyStatus: Record<string, boolean> = Object.fromEntries(STRATEGY_IDS.map((id) => [id, true]));
const strategyMetrics: Record<string, StrategyMetric> = Object.fromEntries(
    STRATEGY_IDS.map((id) => [id, { pnl: 0, daily_trades: 0, updated_at: 0 }]),
);
const heartbeats: Record<string, number> = {};
const latestScans = new Map<string, number>();
const latestStrategyScans = new Map<string, StrategyScanState>();

const marketDataService = new MarketDataService();
const contextBuilder = new ContextBuilder(marketDataService);
const agentManager = new AgentManager(contextBuilder);
const decisionGate = new DecisionGate();
const riskGuard = new RiskGuard();
const executor = new HyperliquidExecutor();
const polymarketPreflight = new PolymarketPreflightService();
const settlementService = new PolymarketSettlementService(redisClient);
const strategyTradeRecorder = new StrategyTradeRecorder();

function extractBearerToken(raw: unknown): string | null {
    if (typeof raw !== 'string') {
        return null;
    }

    const trimmed = raw.trim();
    if (!trimmed) {
        return null;
    }

    if (trimmed.toLowerCase().startsWith('bearer ')) {
        const token = trimmed.slice(7).trim();
        return token.length > 0 ? token : null;
    }
    return trimmed;
}

function isControlPlaneTokenConfigured(): boolean {
    return CONTROL_PLANE_TOKEN.length > 0;
}

function isControlPlaneAuthorized(token: string | null): boolean {
    if (!isControlPlaneTokenConfigured()) {
        return false;
    }
    return token === CONTROL_PLANE_TOKEN;
}

function getControlTokenFromRequest(req: Request): string | null {
    const authHeader = req.header('authorization');
    const directHeader = req.header('x-control-plane-token');
    return extractBearerToken(authHeader) || extractBearerToken(directHeader);
}

function getControlTokenFromSocket(socket: {
    handshake: {
        auth?: Record<string, unknown>;
        headers?: Record<string, unknown>;
    };
}): string | null {
    const authToken = extractBearerToken(socket.handshake.auth?.token);
    const authHeader = extractBearerToken(socket.handshake.headers?.authorization);
    const directHeader = extractBearerToken(socket.handshake.headers?.['x-control-plane-token']);
    return authToken || authHeader || directHeader;
}

function requireControlPlaneAuth(req: Request, res: Response, next: NextFunction): void {
    if (!isControlPlaneTokenConfigured()) {
        res.status(503).json({ error: 'Control plane token is not configured' });
        return;
    }

    if (!isControlPlaneAuthorized(getControlTokenFromRequest(req))) {
        res.status(401).json({ error: 'Unauthorized control plane request' });
        return;
    }

    next();
}

function enforceSocketControlAuth(socket: { emit: (event: string, payload: Record<string, unknown>) => boolean }, action: string, authorized: boolean): boolean {
    if (authorized) {
        return true;
    }
    socket.emit('auth_error', {
        action,
        message: isControlPlaneTokenConfigured()
            ? 'Unauthorized control plane operation'
            : 'Control plane token is not configured',
    });
    return false;
}

function getActiveScannedMarkets(): number {
    const now = Date.now();
    for (const [symbol, ts] of latestScans.entries()) {
        if (now - ts > 30_000) {
            latestScans.delete(symbol);
        }
    }
    return latestScans.size;
}

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
    const parsed = Number(input);
    return Number.isFinite(parsed) ? parsed : null;
}

function extractExecutionStrategy(payload: unknown): string | null {
    const record = asRecord(payload);
    if (!record) {
        return null;
    }

    const details = asRecord(record.details);
    const preflight = details ? asRecord(details.preflight) : null;
    const strategy = asString(preflight?.strategy)
        || asString(details?.strategy)
        || asString(record.strategy)
        || asString(record.side);

    if (!strategy) {
        return null;
    }

    if (STRATEGY_IDS.includes(strategy)) {
        return strategy;
    }

    if (strategy === 'CEX_ARB') {
        return 'CEX_SNIPER';
    }

    return strategy;
}

function computeScanMargin(scan: StrategyScanState): number {
    const raw = Math.abs(scan.score) - Math.abs(scan.threshold);
    return scan.passes_threshold ? Math.abs(raw) : -Math.abs(raw);
}

function evaluateIntelligenceGate(payload: unknown, tradingMode: TradingMode): {
    ok: boolean;
    reason?: string;
    strategy?: string;
    scan?: StrategyScanState;
    margin?: number;
    ageMs?: number;
} {
    if (!INTELLIGENCE_GATE_ENABLED || tradingMode !== 'LIVE') {
        return { ok: true };
    }

    const strategy = extractExecutionStrategy(payload);
    if (!strategy) {
        return { ok: false, reason: 'Execution payload missing strategy identity' };
    }

    const scan = latestStrategyScans.get(strategy);
    if (!scan) {
        return { ok: false, strategy, reason: `No scan intelligence found for ${strategy}` };
    }

    const ageMs = Date.now() - scan.timestamp;
    if (ageMs > INTELLIGENCE_GATE_MAX_STALENESS_MS) {
        return {
            ok: false,
            strategy,
            scan,
            ageMs,
            reason: `Scan stale for ${strategy} (${ageMs}ms > ${INTELLIGENCE_GATE_MAX_STALENESS_MS}ms)`,
        };
    }

    if (!scan.passes_threshold) {
        return {
            ok: false,
            strategy,
            scan,
            ageMs,
            reason: `Latest scan for ${strategy} does not pass threshold`,
        };
    }

    const margin = computeScanMargin(scan);
    if (margin < INTELLIGENCE_GATE_MIN_MARGIN) {
        return {
            ok: false,
            strategy,
            scan,
            ageMs,
            margin,
            reason: `Signal margin ${margin.toFixed(6)} below minimum ${INTELLIGENCE_GATE_MIN_MARGIN.toFixed(6)}`,
        };
    }

    return { ok: true, strategy, scan, margin, ageMs };
}

async function emitSettlementEvents(events: Awaited<ReturnType<typeof settlementService.runCycle>>): Promise<void> {
    if (!events.length) {
        return;
    }
    for (const event of events) {
        io.emit('settlement_event', event);
        io.emit('execution_log', settlementEventToExecutionLog(event));
    }
    io.emit('settlement_snapshot', settlementService.getSnapshot());
}

function normalizeRiskConfig(input: Partial<RiskConfig>, preserveTimestamp = false): RiskConfig | null {
    const model = input.model === 'PERCENT' ? 'PERCENT' : input.model === 'FIXED' ? 'FIXED' : null;
    const rawValue = Number(input.value);

    if (!model || !Number.isFinite(rawValue)) {
        return null;
    }

    const value = model === 'PERCENT'
        ? Math.max(0.1, Math.min(5.0, rawValue))
        : Math.max(10, Math.min(5000, rawValue));

    return {
        model,
        value,
        timestamp: preserveTimestamp && Number.isFinite(Number(input.timestamp))
            ? Number(input.timestamp)
            : Date.now(),
    };
}

async function getRiskConfig(): Promise<RiskConfig> {
    const raw = await redisClient.get('system:risk_config');
    if (raw) {
        try {
            const parsed = JSON.parse(raw) as RiskConfig;
            const normalized = normalizeRiskConfig(parsed, true);
            if (normalized) {
                return normalized;
            }
        } catch {
            // ignore malformed payload and fall back to default
        }
    }

    const fallback: RiskConfig = { model: 'FIXED', value: 50, timestamp: Date.now() };
    await redisClient.set('system:risk_config', JSON.stringify(fallback));
    return fallback;
}

function normalizeTradingMode(mode: unknown): TradingMode | null {
    if (typeof mode !== 'string') {
        return null;
    }

    const normalized = mode.trim().toUpperCase();
    if (normalized === 'PAPER' || normalized === 'LIVE') {
        return normalized as TradingMode;
    }
    return null;
}

async function getTradingMode(): Promise<TradingMode> {
    const raw = await redisClient.get('system:trading_mode');
    const normalized = normalizeTradingMode(raw);
    if (normalized) {
        return normalized;
    }

    const fallback: TradingMode = 'PAPER';
    await redisClient.set('system:trading_mode', fallback);
    return fallback;
}

async function setTradingMode(mode: TradingMode): Promise<void> {
    const payload = {
        mode,
        timestamp: Date.now(),
        live_order_posting_enabled: isLiveOrderPostingEnabled(),
    };

    await redisClient.set('system:trading_mode', mode);
    await redisClient.publish('system:trading_mode', JSON.stringify(payload));
}

function isLiveOrderPostingEnabled(): boolean {
    return process.env.LIVE_ORDER_POSTING_ENABLED === 'true';
}

function normalizeResetBankroll(value: unknown): number | null {
    if (value === undefined || value === null) {
        return DEFAULT_SIM_BANKROLL;
    }

    const numeric = Number(value);
    if (!Number.isFinite(numeric) || numeric < 0 || numeric > 1_000_000_000) {
        return null;
    }

    return Math.round(numeric * 100) / 100;
}

function clearStrategyMetrics(): void {
    for (const id of STRATEGY_IDS) {
        strategyMetrics[id] = { pnl: 0, daily_trades: 0, updated_at: Date.now() };
    }
}

async function resetSimulationState(bankroll: number): Promise<void> {
    const timestamp = Date.now();
    const payload = {
        bankroll,
        timestamp,
    };

    await redisClient.set('sim_bankroll', bankroll.toFixed(2));
    await redisClient.set('system:simulation_reset_ts', String(timestamp));
    if (RESET_VALIDATION_TRADES_ON_SIM_RESET) {
        await strategyTradeRecorder.reset();
    }
    await redisClient.publish('system:simulation_reset', JSON.stringify(payload));
}

// Real-time streams from market data and arb services
marketDataService.on('ticker', (data: any) => {
    io.emit('market_update', data);
});

redisSubscriber.subscribe('arbitrage:opportunities', (message) => {
    try {
        io.emit('arbitrage_update', JSON.parse(message));
    } catch (error) {
        console.error('Error parsing arb message:', error);
    }
});

redisSubscriber.subscribe('arbitrage:execution', (message) => {
    try {
        const parsed = JSON.parse(message);
        io.emit('execution_log', parsed);

        void (async () => {
            try {
                const currentTradingMode = await getTradingMode();
                const intelligenceGate = evaluateIntelligenceGate(parsed, currentTradingMode);
                if (!intelligenceGate.ok) {
                    const blockPayload = {
                        timestamp: Date.now(),
                        strategy: intelligenceGate.strategy || 'UNKNOWN',
                        reason: intelligenceGate.reason || 'Intelligence gate rejected execution',
                        scan: intelligenceGate.scan || null,
                        margin: intelligenceGate.margin,
                        age_ms: intelligenceGate.ageMs,
                    };
                    io.emit('strategy_intelligence_block', blockPayload);
                    io.emit('execution_log', {
                        timestamp: Date.now(),
                        side: 'INTEL_BLOCK',
                        market: asString(asRecord(parsed)?.market) || 'Polymarket',
                        price: blockPayload.reason,
                        size: blockPayload.strategy,
                        mode: 'LIVE_GUARD',
                        details: blockPayload,
                    });
                    return;
                }

                const preflight = await polymarketPreflight.preflightFromExecution(parsed);
                if (!preflight) {
                    const liveExecution = await polymarketPreflight.executeFromExecution(parsed, currentTradingMode);
                    if (!liveExecution) {
                        return;
                    }
                    io.emit('strategy_live_execution', liveExecution);
                    io.emit('execution_log', PolymarketPreflightService.toLiveExecutionLog(liveExecution));
                    const settlementEvents = await settlementService.registerAtomicExecution(liveExecution);
                    await emitSettlementEvents(settlementEvents);
                    return;
                }

                io.emit('strategy_preflight', preflight);
                io.emit('execution_log', PolymarketPreflightService.toExecutionLog(preflight));

                const liveExecution = await polymarketPreflight.executeFromExecution(parsed, currentTradingMode);
                if (!liveExecution) {
                    return;
                }
                io.emit('strategy_live_execution', liveExecution);
                io.emit('execution_log', PolymarketPreflightService.toLiveExecutionLog(liveExecution));
                const settlementEvents = await settlementService.registerAtomicExecution(liveExecution);
                await emitSettlementEvents(settlementEvents);
            } catch (workerError) {
                console.error('[PolymarketPreflight] execution worker error:', workerError);
            }
        })();
    } catch (error) {
        console.error('Error parsing execution message:', error);
    }
});

redisSubscriber.subscribe('strategy:pnl', (message) => {
    try {
        const parsed = JSON.parse(message);
        io.emit('strategy_pnl', parsed);
        strategyTradeRecorder.record(parsed);

        const strategy = typeof parsed?.strategy === 'string' ? parsed.strategy : null;
        const pnl = Number(parsed?.pnl);
        if (strategy && Number.isFinite(pnl)) {
            if (!strategyMetrics[strategy]) {
                strategyMetrics[strategy] = { pnl: 0, daily_trades: 0, updated_at: 0 };
            }
            strategyMetrics[strategy] = {
                pnl: Math.round((strategyMetrics[strategy].pnl + pnl) * 100) / 100,
                daily_trades: strategyMetrics[strategy].daily_trades + 1,
                updated_at: Date.now(),
            };
            io.emit('strategy_metrics_update', strategyMetrics);
        }
    } catch (error) {
        console.error('Error parsing pnl message:', error);
    }
});

redisSubscriber.subscribe('arbitrage:scan', (message) => {
    try {
        const parsed = JSON.parse(message);
        const symbol = typeof parsed?.symbol === 'string' ? parsed.symbol : parsed?.market_id;
        if (symbol) {
            latestScans.set(symbol, Date.now());
        }
        const strategy = asString(parsed?.strategy);
        if (strategy) {
            const timestamp = asNumber(parsed?.timestamp) || Date.now();
            latestStrategyScans.set(strategy, {
                strategy,
                symbol: asString(parsed?.symbol) || strategy,
                timestamp,
                passes_threshold: parsed?.passes_threshold === true,
                score: asNumber(parsed?.score) ?? asNumber(parsed?.gap) ?? 0,
                threshold: asNumber(parsed?.threshold) ?? 0,
                reason: asString(parsed?.reason) || '',
            });
            io.emit('intelligence_update', latestStrategyScans.get(strategy));
        }
        io.emit('scanner_update', parsed);
    } catch (error) {
        console.error('Error in scanner_update:', error);
    }
});

redisSubscriber.subscribe('system:heartbeat', (msg) => {
    try {
        const { id, timestamp } = JSON.parse(msg);
        if (typeof id === 'string') {
            heartbeats[id] = Number(timestamp) || Date.now();
        }
    } catch {
        // ignore malformed heartbeat
    }
});

redisSubscriber.subscribe('system:trading_mode', (msg) => {
    try {
        const parsed = JSON.parse(msg);
        const mode = normalizeTradingMode(parsed?.mode);
        if (!mode) {
            return;
        }
        const livePostingEnabled = typeof parsed?.live_order_posting_enabled === 'boolean'
            ? parsed.live_order_posting_enabled
            : isLiveOrderPostingEnabled();

        io.emit('trading_mode_update', {
            mode,
            timestamp: Number(parsed?.timestamp) || Date.now(),
            live_order_posting_enabled: livePostingEnabled,
        });
    } catch {
        // ignore malformed trading mode update
    }
});

redisSubscriber.subscribe('system:simulation_reset', (msg) => {
    try {
        const parsed = JSON.parse(msg) as { bankroll?: unknown; timestamp?: unknown };
        const bankroll = normalizeResetBankroll(parsed?.bankroll);
        if (bankroll === null) {
            return;
        }

        io.emit('simulation_reset', {
            bankroll,
            timestamp: Number(parsed?.timestamp) || Date.now(),
        });
        clearStrategyMetrics();
        io.emit('strategy_metrics_update', strategyMetrics);
    } catch {
        // ignore malformed simulation reset update
    }
});

io.on('connection', async (socket) => {
    console.log('Client connected:', socket.id);
    const socketControlAuthorized = isControlPlaneAuthorized(getControlTokenFromSocket(socket));

    socket.emit('system_status', {
        status: 'operational',
        active_agents: agentManager.getActiveAgents(),
    });

    socket.emit('agent_status_update', agentManager.getAllAgents());
    socket.emit('strategy_status_update', strategyStatus);
    socket.emit('strategy_metrics_update', strategyMetrics);
    socket.emit('risk_config_update', await getRiskConfig());
    socket.emit('trading_mode_update', {
        mode: await getTradingMode(),
        timestamp: Date.now(),
        live_order_posting_enabled: isLiveOrderPostingEnabled(),
    });
    socket.emit('intelligence_gate_config', {
        enabled: INTELLIGENCE_GATE_ENABLED,
        max_staleness_ms: INTELLIGENCE_GATE_MAX_STALENESS_MS,
        min_margin: INTELLIGENCE_GATE_MIN_MARGIN,
    });
    socket.emit('intelligence_snapshot', Object.fromEntries(
        [...latestStrategyScans.entries()].map(([strategy, scan]) => [strategy, {
            ...scan,
            age_ms: Date.now() - scan.timestamp,
            margin: computeScanMargin(scan),
        }]),
    ));
    socket.emit('settlement_snapshot', settlementService.getSnapshot());

    socket.on('disconnect', () => {
        console.log('Client disconnected:', socket.id);
    });

    socket.on('toggle_agent', ({ name, active }: { name: string; active: boolean }) => {
        if (!enforceSocketControlAuth(socket, 'toggle_agent', socketControlAuthorized)) {
            return;
        }
        agentManager.toggleAgent(name, active);
        io.emit('agent_status_update', agentManager.getAllAgents());
    });

    socket.on('update_config', async (data) => {
        if (!enforceSocketControlAuth(socket, 'update_config', socketControlAuthorized)) {
            return;
        }
        await redisClient.publish('system:config', JSON.stringify(data));
        console.log('Config Updated:', data);
    });

    socket.on('update_risk_config', async (payload: Partial<RiskConfig>) => {
        if (!enforceSocketControlAuth(socket, 'update_risk_config', socketControlAuthorized)) {
            return;
        }
        const normalized = normalizeRiskConfig(payload);
        if (!normalized) {
            socket.emit('risk_config_error', { message: 'Invalid risk config payload' });
            return;
        }

        await redisClient.set('system:risk_config', JSON.stringify(normalized));
        await redisClient.publish('system:risk_config', JSON.stringify(normalized));
        io.emit('risk_config_update', normalized);
        console.log('Risk Config Updated:', normalized);
    });

    socket.on('request_risk_config', async () => {
        socket.emit('risk_config_update', await getRiskConfig());
    });

    socket.on('request_trading_mode', async () => {
        socket.emit('trading_mode_update', {
            mode: await getTradingMode(),
            timestamp: Date.now(),
            live_order_posting_enabled: isLiveOrderPostingEnabled(),
        });
    });

    socket.on('set_trading_mode', async (payload: Partial<TradingModePayload>) => {
        if (!enforceSocketControlAuth(socket, 'set_trading_mode', socketControlAuthorized)) {
            return;
        }
        const mode = normalizeTradingMode(payload?.mode);
        if (!mode) {
            socket.emit('trading_mode_error', { message: 'Invalid trading mode payload' });
            return;
        }

        if (mode === 'LIVE' && payload?.confirmation !== 'LIVE') {
            socket.emit('trading_mode_error', { message: 'LIVE mode requires explicit confirmation' });
            return;
        }

        await setTradingMode(mode);
    });

    socket.on('reset_simulation', async (payload: Partial<SimulationResetPayload>) => {
        if (!enforceSocketControlAuth(socket, 'reset_simulation', socketControlAuthorized)) {
            return;
        }
        const currentMode = await getTradingMode();
        if (currentMode !== 'PAPER') {
            socket.emit('simulation_reset_error', { message: 'Simulation reset is only allowed in PAPER mode' });
            return;
        }

        if (payload?.confirmation !== 'RESET') {
            socket.emit('simulation_reset_error', { message: 'Simulation reset requires explicit RESET confirmation' });
            return;
        }

        const bankroll = normalizeResetBankroll(payload?.bankroll);
        if (bankroll === null) {
            socket.emit('simulation_reset_error', { message: 'Invalid reset bankroll value' });
            return;
        }

        await resetSimulationState(bankroll);
    });

    socket.on('toggle_strategy', async (payload: StrategyTogglePayload) => {
        if (!enforceSocketControlAuth(socket, 'toggle_strategy', socketControlAuthorized)) {
            return;
        }
        if (!payload || !STRATEGY_IDS.includes(payload.id) || typeof payload.active !== 'boolean') {
            socket.emit('strategy_control_error', { message: 'Invalid strategy toggle payload' });
            return;
        }

        strategyStatus[payload.id] = payload.active;

        await redisClient.set(`strategy:enabled:${payload.id}`, payload.active ? '1' : '0');
        await redisClient.publish('strategy:control', JSON.stringify({
            id: payload.id,
            active: payload.active,
            timestamp: Date.now(),
        }));

        io.emit('strategy_status_update', strategyStatus);
    });
});

app.get('/api/arb/bots', (_req, res) => {
    const now = Date.now();
    const bots = STRATEGY_IDS.map((id) => ({
        id,
        active: Boolean(strategyStatus[id]),
    }));

    const scannerLastBeat = Math.max(...SCANNER_HEARTBEAT_IDS.map((id) => heartbeats[id] || 0));

    res.json({
        bots,
        scanner: {
            alive: now - scannerLastBeat < 15_000,
            last_heartbeat_ms: scannerLastBeat,
        },
    });
});

app.post('/api/arb/risk', requireControlPlaneAuth, async (req, res) => {
    const normalized = normalizeRiskConfig(req.body ?? {});
    if (!normalized) {
        res.status(400).json({ error: 'Invalid risk config payload' });
        return;
    }

    await redisClient.set('system:risk_config', JSON.stringify(normalized));
    await redisClient.publish('system:risk_config', JSON.stringify(normalized));
    io.emit('risk_config_update', normalized);
    res.json({ ok: true, risk: normalized });
});

app.get('/api/arb/stats', async (_req, res) => {
    const bankrollRaw = Number(await redisClient.get('sim_bankroll'));
    const bankroll = Number.isFinite(bankrollRaw) ? bankrollRaw : DEFAULT_SIM_BANKROLL;
    const scannerLastBeat = Math.max(...SCANNER_HEARTBEAT_IDS.map((id) => heartbeats[id] || 0));
    const settlementSnapshot = settlementService.getSnapshot();
    const trackedSettlements = settlementSnapshot.positions.filter((position) => position.status !== 'REDEEMED');
    const redeemableSettlements = settlementSnapshot.positions.filter((position) => position.status === 'REDEEMABLE');

    res.json({
        bankroll,
        active_markets: getActiveScannedMarkets(),
        scanner_alive: Date.now() - scannerLastBeat < 15_000,
        risk: await getRiskConfig(),
        trading_mode: await getTradingMode(),
        live_order_posting_enabled: isLiveOrderPostingEnabled(),
        intelligence_gate_enabled: INTELLIGENCE_GATE_ENABLED,
        settlements: {
            tracked: trackedSettlements.length,
            redeemable: redeemableSettlements.length,
            auto_redeem_enabled: settlementSnapshot.autoRedeemEnabled,
        },
    });
});

app.get('/api/arb/intelligence', (_req, res) => {
    const byStrategy = Object.fromEntries(
        [...latestStrategyScans.entries()]
            .sort((a, b) => b[1].timestamp - a[1].timestamp)
            .map(([strategy, scan]) => [strategy, {
                ...scan,
                age_ms: Date.now() - scan.timestamp,
                margin: computeScanMargin(scan),
            }]),
    );

    res.json({
        gate: {
            enabled: INTELLIGENCE_GATE_ENABLED,
            max_staleness_ms: INTELLIGENCE_GATE_MAX_STALENESS_MS,
            min_margin: INTELLIGENCE_GATE_MIN_MARGIN,
        },
        strategies: byStrategy,
    });
});

app.get('/api/arb/settlements', (_req, res) => {
    res.json(settlementService.getSnapshot());
});

app.post('/api/arb/settlements/process', requireControlPlaneAuth, async (_req, res) => {
    const events = await settlementService.runCycle(await getTradingMode(), isLiveOrderPostingEnabled());
    await emitSettlementEvents(events);
    res.json({
        ok: true,
        events,
        snapshot: settlementService.getSnapshot(),
    });
});

app.get('/api/arb/validation-trades', async (_req, res) => {
    const rows = await strategyTradeRecorder.countRows();
    res.json({
        path: strategyTradeRecorder.getPath(),
        rows,
    });
});

app.post('/api/arb/validation-trades/reset', requireControlPlaneAuth, async (_req, res) => {
    await strategyTradeRecorder.reset();
    res.json({
        ok: true,
        path: strategyTradeRecorder.getPath(),
        rows: 0,
    });
});

app.get('/api/system/trading-mode', async (_req, res) => {
    res.json({
        mode: await getTradingMode(),
        live_order_posting_enabled: isLiveOrderPostingEnabled(),
    });
});

app.post('/api/system/trading-mode', requireControlPlaneAuth, async (req, res) => {
    const mode = normalizeTradingMode(req.body?.mode);
    if (!mode) {
        res.status(400).json({ error: 'Invalid trading mode payload' });
        return;
    }

    if (mode === 'LIVE' && req.body?.confirmation !== 'LIVE') {
        res.status(400).json({ error: 'LIVE mode requires explicit confirmation' });
        return;
    }

    await setTradingMode(mode);
    res.json({
        ok: true,
        mode,
        live_order_posting_enabled: isLiveOrderPostingEnabled(),
    });
});

app.post('/api/system/reset-simulation', requireControlPlaneAuth, async (req, res) => {
    const currentMode = await getTradingMode();
    if (currentMode !== 'PAPER') {
        res.status(409).json({ error: 'Simulation reset is only allowed in PAPER mode' });
        return;
    }

    if (req.body?.confirmation !== 'RESET') {
        res.status(400).json({ error: 'Simulation reset requires explicit RESET confirmation' });
        return;
    }

    const bankroll = normalizeResetBankroll(req.body?.bankroll);
    if (bankroll === null) {
        res.status(400).json({ error: 'Invalid reset bankroll value' });
        return;
    }

    await resetSimulationState(bankroll);
    res.json({ ok: true, bankroll });
});

// Run Agentic Decision Loop
setInterval(async () => {
    try {
        const symbol = 'BTC-USD';

        if (agentManager.getActiveAgents().length === 0) {
            return;
        }

        const { decisions, context } = await agentManager.runAnalysis(symbol);
        io.emit('agent_decisions', decisions);

        const validDecision = decisionGate.evaluate(decisions);
        if (!validDecision) {
            return;
        }

        if (!riskGuard.validate(validDecision, context)) {
            return;
        }

        const tradingMode = await getTradingMode();
        const orderId = await executor.executeOrder(validDecision, symbol, tradingMode);

        io.emit('trade_execution', {
            ...validDecision,
            orderId,
            tradingMode,
            timestamp: Date.now(),
        });
    } catch (error) {
        console.error('[Brain] Error in decision loop:', error);
    }
}, 30_000);

// System Health Heartbeat (Every 2s)
setInterval(async () => {
    const start = Date.now();
    let redisLatency = 0;
    try {
        await redisClient.ping();
        redisLatency = Date.now() - start;
    } catch {
        redisLatency = -1;
    }

    const memory = process.memoryUsage();
    const now = Date.now();
    const scannerLastBeat = Math.max(...SCANNER_HEARTBEAT_IDS.map((id) => heartbeats[id] || 0));
    const isScannerAlive = now - scannerLastBeat < 15_000;

    const metrics = [
        {
            name: 'Trading Engine',
            status: 'operational',
            load: `${(memory.heapUsed / 1024 / 1024).toFixed(0)}MB`,
        },
        {
            name: 'Redis Queue',
            status: redisLatency >= 0 ? 'operational' : 'degraded',
            load: redisLatency >= 0 ? `${redisLatency}ms` : 'Err',
        },
        {
            name: 'ML Service',
            status: 'standby',
            load: '0%',
        },
        {
            name: 'Arb Scanner',
            status: isScannerAlive ? 'operational' : 'offline',
            load: isScannerAlive ? `Markets: ${getActiveScannedMarkets()}` : 'No Signal',
        },
    ];

    io.emit('system_health_update', metrics);
}, 2000);

setInterval(async () => {
    try {
        const events = await settlementService.runCycle(await getTradingMode(), isLiveOrderPostingEnabled());
        await emitSettlementEvents(events);
        if (events.length === 0 && settlementService.getSnapshot().positions.length > 0) {
            io.emit('settlement_snapshot', settlementService.getSnapshot());
        }
    } catch (error) {
        console.error('[Settlement] cycle error:', error);
    }
}, settlementService.getPollIntervalMs());

// Start Services
async function bootstrap() {
    await connectRedis();
    await strategyTradeRecorder.init();
    await settlementService.init();
    await marketDataService.start();

    if (!isControlPlaneTokenConfigured()) {
        console.error('CONTROL_PLANE_TOKEN is not configured. Control actions are locked until a token is set.');
    }

    await redisClient.setNX('sim_bankroll', DEFAULT_SIM_BANKROLL.toFixed(2));
    await redisClient.setNX('system:simulation_reset_ts', '0');
    if (SIM_RESET_ON_BOOT) {
        await resetSimulationState(DEFAULT_SIM_BANKROLL);
    }
    clearStrategyMetrics();
    await getRiskConfig();
    await redisClient.setNX('system:trading_mode', 'PAPER');

    for (const strategyId of STRATEGY_IDS) {
        const key = `strategy:enabled:${strategyId}`;
        await redisClient.setNX(key, '1');
        const enabled = await redisClient.get(key);
        strategyStatus[strategyId] = enabled !== '0';
    }

    const PORT = Number(process.env.PORT) || 5114;
    httpServer.listen(PORT, () => {
        console.log(`Backend server running on port ${PORT}`);
    });
}

bootstrap().catch((error) => {
    console.error('Failed to bootstrap backend:', error);
    process.exit(1);
});
