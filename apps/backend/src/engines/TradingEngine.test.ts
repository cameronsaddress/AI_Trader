import type { AgentContext } from '../agents/interfaces';
import { TradingEngine } from './TradingEngine';
import { logger } from '../utils/logger';

function sampleContext(symbol: string): AgentContext {
    return {
        marketData: {
            symbol,
            price: 100,
            timestamp: new Date().toISOString(),
            indicators: {
                rsi: 50,
                sma20: 100,
                returns: 0,
            },
        },
        account: {
            balance: 1000,
            equity: 1000,
            reservedNotional: 0,
            realizedPnl: 0,
            positions: [],
        },
        risk: {
            var95: 10,
            maxDrawdown: 0,
            exposure: 0,
        },
    };
}

describe('TradingEngine', () => {
    beforeEach(() => {
        jest.spyOn(logger, 'info').mockImplementation(() => logger);
        jest.spyOn(logger, 'warn').mockImplementation(() => logger);
        jest.spyOn(logger, 'error').mockImplementation(() => logger);
    });

    afterEach(() => {
        jest.restoreAllMocks();
    });

    it('skips overlapping cycles while a prior cycle is still in flight', async () => {
        const context = sampleContext('BTC-USD');
        let releaseFirstCycle: () => void = () => undefined;
        const firstPromise = new Promise<AgentContext>((resolve) => {
            releaseFirstCycle = () => resolve(context);
        });

        const contextBuilder = {
            buildContext: jest
                .fn<Promise<AgentContext>, [string]>()
                .mockImplementationOnce(async () => firstPromise)
                .mockResolvedValue(context),
        };
        const agents = {
            broadcast: jest.fn(async () => new Map()),
        };
        const riskGuard = {
            validate: jest.fn(() => true),
        };
        const execution = {
            execute: jest.fn(async () => undefined),
        };

        const engine = new TradingEngine(
            {} as never,
            agents as never,
            contextBuilder as never,
            riskGuard as never,
            execution as never,
        );

        (engine as unknown as { isRunning: boolean }).isRunning = true;

        const firstRun = (engine as unknown as { runCycle: () => Promise<void> }).runCycle();
        const overlappingRun = (engine as unknown as { runCycle: () => Promise<void> }).runCycle();

        // Only the first symbol of the first cycle should be in progress at this point.
        expect(contextBuilder.buildContext).toHaveBeenCalledTimes(1);
        expect(logger.warn).toHaveBeenCalledWith('[TradingEngine] cycle skipped: previous cycle still in flight');

        releaseFirstCycle();
        await firstRun;
        await overlappingRun;

        // The first cycle completes both symbols once unblocked.
        expect(contextBuilder.buildContext).toHaveBeenCalledTimes(2);

        await (engine as unknown as { runCycle: () => Promise<void> }).runCycle();
        expect(contextBuilder.buildContext).toHaveBeenCalledTimes(4);
    });
});
