import { AgentResponse } from '../agents/interfaces';
import { logger } from '../utils/logger';
import { Account, Trade, Position } from '../models';

export class ExecutionService {
    private mode: 'simulation' | 'live' = 'simulation';
    private readonly legacyLivePathEnabled: boolean;

    constructor(mode: 'simulation' | 'live' = 'simulation') {
        this.mode = mode;
        this.legacyLivePathEnabled = process.env.LEGACY_EXECUTION_SERVICE_LIVE_ENABLED === 'true';
        if (mode === 'live' && !this.legacyLivePathEnabled) {
            logger.error('ExecutionService live mode requested but legacy live path is disabled.');
        }
    }

    public async execute(agentName: string, decision: AgentResponse, symbol: string) {
        logger.info(`ExecutionService: Executing ${decision.decision} for ${symbol} on ${this.mode} mode`);

        if (this.mode === 'simulation') {
            await this.executeSimulation(agentName, decision, symbol);
        } else {
            if (!this.legacyLivePathEnabled) {
                logger.error('ExecutionService live execution blocked: LEGACY_EXECUTION_SERVICE_LIVE_ENABLED is not set.');
                return;
            }
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
        logger.error(
            `ExecutionService legacy live path invoked for ${agentName} ${decision.decision} ${symbol}; this path is deprecated.`,
        );
        // 1. Validate Live Arm Latch
        // 2. Send Order to Coinbase
        // 3. Record Trade
    }

    public setMode(mode: 'simulation' | 'live') {
        this.mode = mode;
    }
}
