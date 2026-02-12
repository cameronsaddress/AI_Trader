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
    private url: string;
    private subscriptions: Set<string> = new Set();
    private reconnectAttempts = 0;
    private maxReconnectAttempts = 5;

    constructor(private config: CoinbaseConfig) {
        super();
        this.url = config.sandbox
            ? 'wss://ws-feed-public.sandbox.exchange.coinbase.com'
            : 'wss://ws-feed.exchange.coinbase.com';
    }

    public connect() {
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
                this.stopHeartbeat();
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

    public subscribe(productIds: string[], channels: string[]) {
        if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
            logger.warn('WebSocket not open, queuing subscription');
            // Logic to queue could go here, for now rely on reconnect
            return;
        }

        const startMsg = {
            type: 'subscribe',
            product_ids: productIds,
            channels: channels
        };

        this.ws.send(JSON.stringify(startMsg));
        productIds.forEach(id => this.subscriptions.add(id));
        logger.info(`Subscribed to ${productIds.join(', ')} on channels ${channels.join(', ')}`);
    }

    private resubscribe() {
        if (this.subscriptions.size > 0) {
            logger.info('Resubscribing to previous channels...');
            this.subscribe(Array.from(this.subscriptions), ['level2', 'ticker', 'matches']);
        }
    }

    private reconnect() {
        if (this.reconnectAttempts < this.maxReconnectAttempts) {
            this.reconnectAttempts++;
            const delay = Math.min(1000 * Math.pow(2, this.reconnectAttempts), 30000);
            logger.info(`Attempting reconnect in ${delay}ms...`);
            setTimeout(() => this.connect(), delay);
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
}
