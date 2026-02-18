import { HyperliquidClient, type HyperliquidCandleEvent, type HyperliquidTickerEvent } from './HyperliquidClient';
import { logger } from '../utils/logger';
import { redisClient } from '../config/redis';
import { EventEmitter } from 'events';

export interface MarketTickerMessage {
    symbol: string;
    price: number;
    timestamp: string;
    candle?: HyperliquidCandleEvent['candle'];
}

export class MarketDataService extends EventEmitter {
    private hlClient: HyperliquidClient;
    private symbols: string[] = ['BTC-USD', 'ETH-USD', 'SOL-USD'];

    constructor() {
        super();
        this.hlClient = new HyperliquidClient();
        this.setupListeners();
    }

    public async start() {
        logger.info('Starting Market Data Service (Hyperliquid Live)...');
        this.hlClient.connect();
    }

    public async stop() {
        this.hlClient.disconnect();
    }

    private setupListeners() {
        this.hlClient.on('open', () => {
            logger.info('Connected to Hyperliquid WebSocket');
            this.hlClient.subscribe(this.symbols);
        });

        this.hlClient.on('ticker', (data: HyperliquidTickerEvent) => {
            const normalized: MarketTickerMessage = {
                symbol: data.product_id,
                price: data.price,
                timestamp: data.timestamp,
            };

            // Publish to Redis for other services
            if (redisClient.isOpen) {
                void redisClient.publish('market_data:ticker', JSON.stringify(normalized)).catch((error) => {
                    logger.error('[MarketDataService] failed to publish market_data:ticker', error);
                });
            }

            // Emit for Socket.IO
            this.emit('ticker', normalized);
        });

        this.hlClient.on('candle', (data: HyperliquidCandleEvent) => {
            // Emit candle data specifically for the chart
            const payload: MarketTickerMessage = {
                symbol: data.symbol,
                price: data.candle.close, // Also update price with close of candle
                timestamp: new Date().toISOString(),
                candle: data.candle,
            };
            this.emit('ticker', payload); // Reuse ticker channel or create new one
        });
    }
}
