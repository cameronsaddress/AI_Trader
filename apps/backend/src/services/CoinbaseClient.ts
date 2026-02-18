import WebSocket from 'ws';
import { EventEmitter } from 'events';
import { logger } from '../utils/logger';

interface CoinbaseConfig {
    apiKey: string;
    apiSecret: string;
    sandbox?: boolean;
}

export class CoinbaseClient extends EventEmitter {
    private ws: WebSocket | null = null;
    private pingInterval: NodeJS.Timeout | null = null;
    private reconnectTimer: NodeJS.Timeout | null = null;
    private url: string;
    private subscriptions: Set<string> = new Set();
    private reconnectAttempts = 0;
    private maxReconnectAttempts = 5;
    private manualDisconnect = false;

    constructor(private config: CoinbaseConfig) {
        super();
        this.url = config.sandbox
            ? 'wss://ws-feed-public.sandbox.exchange.coinbase.com'
            : 'wss://ws-feed.exchange.coinbase.com';
    }

    public connect() {
        this.manualDisconnect = false;
        if (this.ws && (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING)) {
            return;
        }
        this.clearReconnectTimer();
        try {
            this.ws = new WebSocket(this.url);

            this.ws.on('open', () => {
                logger.info('Connected to Coinbase WebSocket');
                this.reconnectAttempts = 0;
                this.resubscribe();
                this.startHeartbeat();
                this.emit('open');
            });

            this.ws.on('message', (data: WebSocket.Data) => {
                try {
                    const message = JSON.parse(data.toString());
                    this.emit('message', message);

                    if (message.type === 'ticker') {
                        this.emit('ticker', message);
                    } else if (message.type === 'l2update') {
                        this.emit('l2update', message);
                    } else if (message.type === 'snapshot') {
                        this.emit('snapshot', message);
                    }
                } catch (error) {
                    logger.error('Error parsing Coinbase message', { error });
                }
            });

            this.ws.on('close', () => {
                logger.warn('Coinbase WebSocket closed');
                this.ws = null;
                this.stopHeartbeat();
                if (this.manualDisconnect) {
                    logger.info('Coinbase WebSocket closed by manual disconnect');
                    return;
                }
                this.reconnect();
            });

            this.ws.on('error', (error) => {
                logger.error('Coinbase WebSocket error', { error });
            });

        } catch (error) {
            logger.error('Failed to create WebSocket connection', { error });
            this.reconnect();
        }
    }

    public disconnect() {
        this.manualDisconnect = true;
        this.stopHeartbeat();
        this.clearReconnectTimer();
        if (this.ws && (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING)) {
            this.ws.close();
        }
        this.ws = null;
    }

    public subscribe(productIds: string[], channels: string[]) {
        if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
            logger.warn('WebSocket not open, queuing subscription');
            // Logic to queue could go here, for now rely on reconnect
            const subscriptionKey = JSON.stringify({
                product_ids: [...productIds].sort(),
                channels: [...channels].sort(),
            });
            this.subscriptions.add(subscriptionKey);
            return;
        }

        const startMsg = {
            type: 'subscribe',
            product_ids: productIds,
            channels: channels
        };

        this.ws.send(JSON.stringify(startMsg));
        const subscriptionKey = JSON.stringify({
            product_ids: [...productIds].sort(),
            channels: [...channels].sort(),
        });
        this.subscriptions.add(subscriptionKey);
        logger.info(`Subscribed to ${productIds.join(', ')} on channels ${channels.join(', ')}`);
    }

    private resubscribe() {
        if (this.subscriptions.size > 0) {
            logger.info('Resubscribing to previous channels...');
            for (const sub of this.subscriptions) {
                try {
                    const parsed = JSON.parse(sub) as {
                        product_ids?: unknown;
                        channels?: unknown;
                    };
                    const productIds = Array.isArray(parsed.product_ids)
                        ? parsed.product_ids.filter((id): id is string => typeof id === 'string')
                        : [];
                    const channels = Array.isArray(parsed.channels)
                        ? parsed.channels.filter((id): id is string => typeof id === 'string')
                        : [];
                    if (productIds.length > 0 && channels.length > 0) {
                        this.subscribe(productIds, channels);
                    }
                } catch (error) {
                    logger.error('Failed to parse stored Coinbase subscription', { error });
                }
            }
        }
    }

    private reconnect() {
        if (this.manualDisconnect) {
            return;
        }
        if (this.reconnectAttempts < this.maxReconnectAttempts) {
            this.reconnectAttempts++;
            const delay = Math.min(1000 * Math.pow(2, this.reconnectAttempts), 30000);
            logger.info(`Attempting reconnect in ${delay}ms...`);
            this.clearReconnectTimer();
            this.reconnectTimer = setTimeout(() => {
                this.reconnectTimer = null;
                this.connect();
            }, delay);
        } else {
            logger.error('Max reconnect attempts reached. Manual intervention required.');
            this.emit('fatal_error');
        }
    }

    private startHeartbeat() {
        this.pingInterval = setInterval(() => {
            if (this.ws && this.ws.readyState === WebSocket.OPEN) {
                // Coinbase specific heartbeat logic or simple ping
                // this.ws.ping(); // ws library handles ping/pong automatically mostly
            }
        }, 30000);
    }

    private stopHeartbeat() {
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
}
