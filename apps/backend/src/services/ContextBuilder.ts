import { AgentContext } from '../agents/interfaces';
import { MarketDataService } from './MarketDataService';
import { logger } from '../utils/logger';
import { redisClient, subscriber } from '../config/redis';

export class ContextBuilder {
    private featuresCache: Map<string, any> = new Map();
    private priceCache: Map<string, number> = new Map();

    constructor(private marketDataService: MarketDataService) {
        this.setupSubscriptions();
    }

    private setupSubscriptions() {
        // Subscribe to ML Features
        subscriber.subscribe('ml:features', (message: string) => {
            try {
                const features = JSON.parse(message);
                if (features.symbol) {
                    this.featuresCache.set(features.symbol, features);
                    // logger.debug(`Updated features for ${features.symbol}`);
                }
            } catch (err) {
                logger.error('Error parsing ML features:', err);
            }
        });

        // Listen to local MarketDataService for prices
        this.marketDataService.on('ticker', (data) => {
            this.priceCache.set(data.symbol, data.price);
        });
    }

    public async buildContext(symbol: string): Promise<AgentContext> {
        const price = this.priceCache.get(symbol) || 0;
        const features = this.featuresCache.get(symbol) || {};

        // TODO: Fetch real Account and Risk data
        const mockAccount = {
            balance: 100000,
            equity: 100000,
            positions: []
        };

        const mockRisk = {
            var95: 100,
            maxDrawdown: 0.0,
            exposure: 0
        };

        return {
            marketData: {
                symbol,
                price,
                timestamp: new Date().toISOString(),
                indicators: {
                    rsi: features.rsi_14 || 50,
                    sma20: features.sma_20 || price,
                    returns: features.returns || 0
                }
            },
            account: mockAccount,
            risk: mockRisk
        };
    }
}
