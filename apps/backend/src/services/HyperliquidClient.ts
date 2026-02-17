import WebSocket from 'ws';
import axios from 'axios';
import { EventEmitter } from 'events';
import { logger } from '../utils/logger';
import { toHyperliquidSymbol, toCoinbaseSymbol } from '../utils/symbolUtils';

const HYPERLIQUID_WS_URL = 'wss://api.hyperliquid.xyz/ws';
const HYPERLIQUID_REST_URL = 'https://api.hyperliquid.xyz';

export class HyperliquidClient extends EventEmitter {
    private ws: WebSocket | null = null;
    private pingInterval: NodeJS.Timeout | null = null;
    private subscriptions: Set<string> = new Set();
    private connected: boolean = false;

    constructor() {
        super();
    }

    public connect() {
        if (this.ws && (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING)) {
            return;
        }

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
            logger.warn('[Hyperliquid] WebSocket closed, reconnecting...');
            this.connected = false;
            this.stopPing();
            setTimeout(() => this.connect(), 5000);
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

    public subscribe(symbols: string[]) {
        symbols.forEach(symbol => {
            const hlSymbol = toHyperliquidSymbol(symbol);
            if (!hlSymbol) return;

            const sub = {
                type: 'l2Book',
                coin: hlSymbol
            };
            const tradesSub = {
                type: 'trades',
                coin: hlSymbol
            };
            const candleSub = {
                type: 'candle',
                coin: hlSymbol,
                interval: '1m'
            };

            this.sendSubscription(sub);
            this.sendSubscription(tradesSub);
            this.sendSubscription(candleSub); // Subscribe to candles for chart
        });
    }

    private sendSubscription(subscription: any) {
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
                this.ws.send(JSON.stringify({
                    method: 'subscribe',
                    subscription: JSON.parse(subStr)
                }));
            }
        });
    }

    private handleMessage(msg: any) {
        if (msg.channel === 'l2Book') {
            const { coin, time, levels } = msg.data;
            const bestBidRaw = levels?.[0]?.[0]?.px;
            const bestAskRaw = levels?.[1]?.[0]?.px;
            const bestBid = Number(bestBidRaw);
            const bestAsk = Number(bestAskRaw);
            if (!Number.isFinite(bestBid) || !Number.isFinite(bestAsk) || bestBid <= 0 || bestAsk <= 0) {
                return;
            }

            const midPrice = (bestBid + bestAsk) / 2;
            this.emit('ticker', {
                product_id: toCoinbaseSymbol(coin),
                price: midPrice,
                timestamp: new Date(time).toISOString()
            });
        } else if (msg.channel === 'trades') {
            // Optional: emit trade events
        } else if (msg.channel === 'candle') {
            // "candle": {"t":170...,"T":170...,"s":"BTC","i":"1m","o":43000,"c":43050,"h":43100,"l":42900,"v":100...}
            const candle = msg.data;
            if (candle) {
                this.emit('candle', {
                    symbol: toCoinbaseSymbol(candle.s),
                    candle: {
                        time: Math.floor(candle.t / 1000), // lightweight-charts seconds
                        open: parseFloat(candle.o),
                        high: parseFloat(candle.h),
                        low: parseFloat(candle.l),
                        close: parseFloat(candle.c),
                    }
                });
            }
        }
    }
}
