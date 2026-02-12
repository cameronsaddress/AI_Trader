import { AgentResponse } from '../agents/interfaces';
import { logger } from '../utils/logger';
import { Account, Trade, Position } from '../models';

export class ExecutionService {
    private mode: 'simulation' | 'live' = 'simulation';

    constructor(mode: 'simulation' | 'live' = 'simulation') {
        this.mode = mode;
    }

    public async execute(agentName: string, decision: AgentResponse, symbol: string) {
        logger.info(`ExecutionService: Executing ${decision.decision} for ${symbol} on ${this.mode} mode`);

        if (this.mode === 'simulation') {
            await this.executeSimulation(agentName, decision, symbol);
        } else {
            await this.executeLive(agentName, decision, symbol);
        }
    }

    private async executeSimulation(agentName: string, decision: AgentResponse, symbol: string) {
        // 1. Create Trade record
        // 2. Update Position
        // 3. Update Account Balance
        logger.info(`Simulating trade: ${decision.decision} ${symbol} by ${agentName}`);

        // Detailed logic to be implemented with Sequelize transactions
    }

    private async executeLive(agentName: string, decision: AgentResponse, symbol: string) {
        // 1. Validate Live Arm Latch
        // 2. Send Order to Coinbase
        // 3. Record Trade
        logger.warn(`Live execution not yet fully implemented for safety.`);
    }

    public setMode(mode: 'simulation' | 'live') {
        this.mode = mode;
    }
}
