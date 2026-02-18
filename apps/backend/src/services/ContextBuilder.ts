import type { AgentAccountContext, AgentContext, AgentRiskContext } from '../agents/interfaces';
import { MarketDataService, type MarketTickerMessage } from './MarketDataService';
import { logger } from '../utils/logger';
import { redisClient, subscriber } from '../config/redis';

function asFiniteNumber(value: unknown, fallback = 0): number {
    const parsed = typeof value === 'number' ? value : Number(value);
    return Number.isFinite(parsed) ? parsed : fallback;
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

interface MlFeatureSnapshot {
    symbol: string;
    rsi_14?: unknown;
    sma_20?: unknown;
    returns?: unknown;
}

export class ContextBuilder {
    private featuresCache: Map<string, MlFeatureSnapshot> = new Map();
    private priceCache: Map<string, number> = new Map();

    constructor(private marketDataService: MarketDataService) {
        this.setupSubscriptions();
    }

    private setupSubscriptions() {
        // Subscribe to ML Features
        subscriber.subscribe('ml:features', (message: string) => {
            try {
                const parsed = asRecord(JSON.parse(message));
                const symbol = asString(parsed?.symbol);
                if (symbol && parsed) {
                    this.featuresCache.set(symbol, {
                        symbol,
                        rsi_14: parsed.rsi_14,
                        sma_20: parsed.sma_20,
                        returns: parsed.returns,
                    });
                    // logger.debug(`Updated features for ${features.symbol}`);
                }
            } catch (err) {
                logger.error('Error parsing ML features:', err);
            }
        });

        // Listen to local MarketDataService for prices
        this.marketDataService.on('ticker', (data: MarketTickerMessage) => {
            this.priceCache.set(data.symbol, data.price);
        });
    }

    public async buildContext(symbol: string): Promise<AgentContext> {
        const price = this.priceCache.get(symbol) || 0;
        const features: MlFeatureSnapshot = this.featuresCache.get(symbol) || { symbol };

        let account: AgentAccountContext = {
            balance: 100000,
            equity: 100000,
            reservedNotional: 0,
            realizedPnl: 0,
            positions: [] as unknown[],
        };

        let risk: AgentRiskContext = {
            var95: 100,
            maxDrawdown: 0,
            exposure: 0,
        };

        try {
            const [cashRaw, equityRaw, reservedRaw, realizedRaw, peakRaw] = await redisClient.mGet([
                'sim_ledger:cash',
                'sim_bankroll',
                'sim_ledger:reserved',
                'sim_ledger:realized_pnl',
                'sim_ledger:peak_equity',
            ]);

            const balance = asFiniteNumber(cashRaw, account.balance);
            const equity = asFiniteNumber(equityRaw, account.equity);
            const reservedNotional = asFiniteNumber(reservedRaw, 0);
            const realizedPnl = asFiniteNumber(realizedRaw, 0);
            const peakEquity = asFiniteNumber(peakRaw, equity);

            account = {
                balance,
                equity,
                reservedNotional,
                realizedPnl,
                positions: [],
            };

            const utilization = equity > 0 ? reservedNotional / equity : 0;
            const drawdown = peakEquity > 0 ? (equity - peakEquity) / peakEquity : 0;

            risk = {
                // Simple proxy risk budget until dedicated VaR service is wired in.
                var95: Math.max(0, equity * 0.01),
                maxDrawdown: drawdown,
                exposure: Math.max(0, utilization),
            };
        } catch (err) {
            logger.error('Error building account/risk context from Redis:', err);
        }

        return {
            marketData: {
                symbol,
                price,
                timestamp: new Date().toISOString(),
                indicators: {
                    rsi: asFiniteNumber(features.rsi_14, 50),
                    sma20: asFiniteNumber(features.sma_20, price),
                    returns: asFiniteNumber(features.returns, 0),
                },
            },
            account,
            risk,
        };
    }
}
