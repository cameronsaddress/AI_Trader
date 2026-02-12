import { AgentResponse, AgentContext } from '../agents/interfaces';
import { logger } from '../utils/logger';

export class RiskGuard {
    private maxDrawdown: number = 0.15; // 15%
    private maxExposure: number = 0.85; // 85%
    private minConfidence: number = 0.7; // 70%

    public validate(decision: AgentResponse, context: AgentContext): boolean {
        if (decision.decision === 'HOLD') return true;

        // 1. Confidence Check
        if (decision.confidence < this.minConfidence) {
            logger.warn(`RiskGuard: Confidence too low (${decision.confidence})`);
            return false;
        }

        // 2. Drawdown Check
        // if (context.risk.maxDrawdown > this.maxDrawdown) {
        //   logger.warn(`RiskGuard: Max drawdown exceeded`);
        //   // Only allow reducing exposure?
        //   if (decision.decision === 'BUY') return false;
        // }

        // 3. Exposure Check
        // ...

        return true;
    }
}
