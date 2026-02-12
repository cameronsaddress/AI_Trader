import { IAgent, AgentContext, AgentResponse } from './interfaces';
import { logger } from '../utils/logger';

export class SimulatedAgent implements IAgent {
    public name: string;
    public provider: string = 'simulation';

    constructor(name: string) {
        this.name = name;
    }

    async analyze(context: AgentContext): Promise<AgentResponse> {
        logger.info(`[${this.name}] Analyzing context for ${context.marketData.symbol}...`);

        // Simulate thinking time
        await new Promise(resolve => setTimeout(resolve, 1000 + Math.random() * 2000));

        // Simple logic based on RSI
        const rsi = context.marketData.indicators.rsi;
        let decision: 'BUY' | 'SELL' | 'HOLD' = 'HOLD';
        let confidence = 0.5;

        if (rsi < 30) {
            decision = 'BUY';
            confidence = 0.8;
        } else if (rsi > 70) {
            decision = 'SELL';
            confidence = 0.8;
        }

        return {
            decision,
            confidence,
            reasoning: `Simulated decision based on RSI ${rsi.toFixed(2)}`,
            suggestedSize: 0.1
        };
    }
}
