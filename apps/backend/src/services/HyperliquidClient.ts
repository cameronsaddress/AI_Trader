import WebSocket from 'ws';
import axios from 'axios';
import { EventEmitter } from 'events';
import { logger } from '../utils/logger';
import { toHyperliquidSymbol, toCoinbaseSymbol } from '../utils/symbolUtils';

const HYPERLIQUID_WS_URL = 'wss://api.hyperliquid.xyz/ws';
const HYPERLIQUID_REST_URL = 'https://api.hyperliquid.xyz';

type HyperliquidSubscription = {
    type: 'l2Book' | 'trades' | 'candle';
    coin: string;
    interval?: '1m';
};

export interface HyperliquidTickerEvent {
    product_id: string;
    price: number;
    timestamp: string;
}

export interface HyperliquidCandleEvent {
    symbol: string;
    candle: {
        time: number;
        open: number;
        high: number;
        low: number;
        close: number;
    };
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
    if (input === null || input === undefined || input === '') {
        return null;
    }
    const parsed = Number(input);
    return Number.isFinite(parsed) ? parsed : null;
}

function isHyperliquidSubscription(input: unknown): input is HyperliquidSubscription {
    const record = asRecord(input);
    if (!record) {
        return false;
    }
    const type = asString(record.type);
    const coin = asString(record.coin);
    if (!type || !coin) {
        return false;
    }
    return type === 'l2Book' || type === 'trades' || type === 'candle';
}

function extractBestPx(levels: unknown, sideIndex: number): number | null {
    if (!Array.isArray(levels) || sideIndex < 0 || sideIndex >= levels.length) {
        return null;
    }
    const sideLevels = levels[sideIndex];
    if (!Array.isArray(sideLevels) || sideLevels.length === 0) {
        return null;
    }
    const level = asRecord(sideLevels[0]);
    if (!level) {
        return null;
    }
    return asNumber(level.px);
}

export class HyperliquidClient extends EventEmitter {
    private ws: WebSocket | null = null;
    private pingInterval: NodeJS.Timeout | null = null;
    private reconnectTimer: NodeJS.Timeout | null = null;
    private subscriptions: Set<string> = new Set();
    private connected: boolean = false;
    private manualDisconnect: boolean = false;

    constructor() {
        super();
    }

    public connect() {
        this.manualDisconnect = false;
        if (this.ws && (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING)) {
            return;
        }
        this.clearReconnectTimer();

        logger.info('[Hyperliquid] Connecting to WebSocket...');
        this.ws = new WebSocket(HYPERLIQUID_WS_URL);

        this.ws.on('open', () => {
            logger.info('[Hyperliquid] WebSocket connected');
            this.connected = true;
            this.startPing();
            this.resubscribe();
            this.emit('open');
        });

        this.ws.on('message', (data: WebSocket.Data) => {
            try {
                const msg = JSON.parse(data.toString());
                this.handleMessage(msg);
            } catch (e) {
                logger.error('[Hyperliquid] Failed to parse message', e);
            }
        });

        this.ws.on('close', () => {
            this.connected = false;
            this.ws = null;
            this.stopPing();
            if (this.manualDisconnect) {
                logger.info('[Hyperliquid] WebSocket closed by manual disconnect');
                return;
            }
            logger.warn('[Hyperliquid] WebSocket closed, reconnecting...');
            this.clearReconnectTimer();
            this.reconnectTimer = setTimeout(() => {
                this.reconnectTimer = null;
                this.connect();
            }, 5000);
        });

        this.ws.on('error', (err) => {
            logger.error('[Hyperliquid] WebSocket error', err);
        });
    }

    private startPing() {
        this.stopPing();
        this.pingInterval = setInterval(() => {
            if (this.ws && this.connected) {
                this.ws.send(JSON.stringify({ method: 'ping' }));
            }
        }, 30000);
    }

    private stopPing() {
        if (this.pingInterval) {
            clearInterval(this.pingInterval);
            this.pingInterval = null;
        }
    }

    private clearReconnectTimer() {
        if (this.reconnectTimer) {
            clearTimeout(this.reconnectTimer);
            this.reconnectTimer = null;
        }
    }

    public disconnect() {
        this.manualDisconnect = true;
        this.connected = false;
        this.stopPing();
        this.clearReconnectTimer();
        if (this.ws && (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING)) {
            this.ws.close();
        }
        this.ws = null;
    }

    public subscribe(symbols: string[]) {
        symbols.forEach(symbol => {
            const hlSymbol = toHyperliquidSymbol(symbol);
            if (!hlSymbol) return;

            const sub: HyperliquidSubscription = {
                type: 'l2Book',
                coin: hlSymbol,
            };
            const tradesSub: HyperliquidSubscription = {
                type: 'trades',
                coin: hlSymbol,
            };
            const candleSub: HyperliquidSubscription = {
                type: 'candle',
                coin: hlSymbol,
                interval: '1m',
            };

            this.sendSubscription(sub);
            this.sendSubscription(tradesSub);
            this.sendSubscription(candleSub); // Subscribe to candles for chart
        });
    }

    private sendSubscription(subscription: HyperliquidSubscription) {
        // Track subscription type:coin key to prevent dups if needed, 
        // but for now simplistic resubscribe logic is fine
        this.subscriptions.add(JSON.stringify(subscription));

        if (this.connected && this.ws) {
            this.ws.send(JSON.stringify({
                method: 'subscribe',
                subscription
            }));
        }
    }

    private resubscribe() {
        this.subscriptions.forEach(subStr => {
            if (this.connected && this.ws) {
                let parsed: unknown = null;
                try {
                    parsed = JSON.parse(subStr);
                } catch {
                    return;
                }
                if (!isHyperliquidSubscription(parsed)) {
                    return;
                }
                this.ws.send(JSON.stringify({
                    method: 'subscribe',
                    subscription: parsed,
                }));
            }
        });
    }

    private handleMessage(msg: unknown) {
        const parsed = asRecord(msg);
        if (!parsed) {
            return;
        }
        const channel = asString(parsed.channel);
        const data = asRecord(parsed.data);

        if (channel === 'l2Book' && data) {
            const coin = asString(data.coin);
            const timeMs = asNumber(data.time);
            const bestBid = extractBestPx(data.levels, 0);
            const bestAsk = extractBestPx(data.levels, 1);
            if (
                bestBid === null
                || bestAsk === null
                || !Number.isFinite(bestBid)
                || !Number.isFinite(bestAsk)
                || bestBid <= 0
                || bestAsk <= 0
            ) {
                return;
            }
            if (!coin) {
                return;
            }

            const midPrice = (bestBid + bestAsk) / 2;
            const ticker: HyperliquidTickerEvent = {
                product_id: toCoinbaseSymbol(coin),
                price: midPrice,
                timestamp: new Date(timeMs ?? Date.now()).toISOString(),
            };
            this.emit('ticker', ticker);
        } else if (channel === 'trades') {
            // Optional: emit trade events
        } else if (channel === 'candle' && data) {
            // "candle": {"t":170...,"T":170...,"s":"BTC","i":"1m","o":43000,"c":43050,"h":43100,"l":42900,"v":100...}
            const symbol = asString(data.s);
            const timeMs = asNumber(data.t);
            const open = asNumber(data.o);
            const high = asNumber(data.h);
            const low = asNumber(data.l);
            const close = asNumber(data.c);
            if (!symbol || timeMs === null || open === null || high === null || low === null || close === null) {
                return;
            }
            const candle: HyperliquidCandleEvent = {
                symbol: toCoinbaseSymbol(symbol),
                candle: {
                    time: Math.floor(timeMs / 1000), // lightweight-charts seconds
                    open,
                    high,
                    low,
                    close,
                },
            };
            this.emit('candle', candle);
        }
    }
}
