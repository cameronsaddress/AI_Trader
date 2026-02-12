import { ethers } from 'ethers';
import { AgentResponse } from '../agents/interfaces';
import { logger } from '../utils/logger';

export type TradingMode = 'PAPER' | 'LIVE';

// Mock Hyperliquid SDK interaction for now
// In real implementation, this would sign L1/L2 actions
export class HyperliquidExecutor {
    private wallet: ethers.Wallet | null = null;
    private apiUrl: string;
    private liveTradingEnabled: boolean;

    constructor() {
        this.apiUrl = process.env.HYPERLIQUID_API_URL || 'https://api.hyperliquid.xyz';
        this.liveTradingEnabled = process.env.LIVE_ORDER_POSTING_ENABLED === 'true'
            || process.env.LIVE_TRADING_ENABLED === 'true';
        const privateKey = process.env.HYPERLIQUID_PRIVATE_KEY;

        if (privateKey) {
            this.wallet = new ethers.Wallet(privateKey);
            logger.info(`Initialized Hyperliquid Wallet: ${this.wallet.address}`);
            if (!this.liveTradingEnabled) {
                logger.warn('Wallet loaded but LIVE_TRADING_ENABLED=false, keeping execution in simulation mode.');
            }
        } else if (this.liveTradingEnabled) {
            throw new Error('LIVE_TRADING_ENABLED=true requires HYPERLIQUID_PRIVATE_KEY.');
        } else {
            logger.warn('No Hyperliquid Private Key found. Execution will be SIMULATED.');
        }
    }

    public async executeOrder(decision: AgentResponse, symbol: string, mode: TradingMode): Promise<string> {
        logger.info(`[Executor] Preparing ${decision.decision} order for ${symbol}...`);

        if (decision.decision === 'HOLD') return 'SKIPPED';

        const size = decision.suggestedSize || 0.0001; // Default size if not specified
        const isBuy = decision.decision === 'BUY';

        if (mode !== 'LIVE') {
            logger.info(`[Executor] PAPER MODE: Simulated ${isBuy ? 'BUY' : 'SELL'} order for ${size} ${symbol}`);
            return 'PAPER_SIMULATED_ORDER_ID_' + Date.now();
        }

        if (!this.liveTradingEnabled) {
            logger.info(`[Executor] LIVE mode dry-run: ${isBuy ? 'BUY' : 'SELL'} ${size} ${symbol} (posting disabled)`);
            return 'LIVE_DRY_RUN_ORDER_ID_' + Date.now();
        }

        if (!this.wallet) {
            logger.error('[Executor] Blocking live order: no HYPERLIQUID_PRIVATE_KEY loaded.');
            return 'LIVE_BLOCKED_NO_KEY_' + Date.now();
        }

        logger.error('[Executor] Blocking live order because execution path is stubbed.', {
            symbol,
            decision: decision.decision,
            suggestedSize: size,
        });
        return 'LIVE_BLOCKED_UNIMPLEMENTED_' + Date.now();
    }
}
