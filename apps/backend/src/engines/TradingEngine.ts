import { MarketDataService } from '../services/MarketDataService';
import { AgentManager } from '../services/AgentManager';
import { ContextBuilder } from '../services/ContextBuilder';
import { RiskGuard } from '../services/RiskGuard';
import { ExecutionService } from '../services/ExecutionService';
import { logger } from '../utils/logger';

export class TradingEngine {
    private isRunning: boolean = false;
    private intervalId: NodeJS.Timeout | null = null;
    private intervalMs: number = 60000; // 1 minute default

    constructor(
        private marketData: MarketDataService,
        private agents: AgentManager,
        private contextBuilder: ContextBuilder,
        private riskGuard: RiskGuard,
        private execution: ExecutionService
    ) { }

    public async start() {
        if (this.isRunning) return;
        this.isRunning = true;

        logger.info('Trading Engine Started');

        // Initial run
        await this.runCycle();

        this.intervalId = setInterval(() => {
            this.runCycle();
        }, this.intervalMs);
    }

    public stop() {
        this.isRunning = false;
        if (this.intervalId) {
            clearInterval(this.intervalId);
            this.intervalId = null;
        }
        logger.info('Trading Engine Stopped');
    }

    private async runCycle() {
        if (!this.isRunning) return;

        logger.info('--- Starting Trading Cycle ---');
        const symbols = ['BTC-USD', 'ETH-USD']; // Configurable

        for (const symbol of symbols) {
            try {
                // 1. Build Context
                const context = await this.contextBuilder.buildContext(symbol);

                // 2. Get Agent Decisions
                const decisions = await this.agents.broadcast(context);

                // 3. Process Decisions
                for (const [agentName, response] of decisions) {
                    logger.info(`Agent ${agentName} decision for ${symbol}: ${response.decision} (${response.confidence})`);

                    // 4. Risk Check
                    if (this.riskGuard.validate(response, context)) {
                        // 5. Execute
                        await this.execution.execute(agentName, response, symbol);
                    } else {
                        logger.warn(`Trade rejected by RiskGuard for ${agentName} on ${symbol}`);
                    }
                }
            } catch (error) {
                logger.error(`Error processing symbol ${symbol}`, error);
            }
        }
        logger.info('--- Trading Cycle Complete ---');
    }
}
