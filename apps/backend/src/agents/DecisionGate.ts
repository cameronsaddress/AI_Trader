import { AgentResponse } from './interfaces';
import { logger } from '../utils/logger';

export class DecisionGate {
    private MIN_CONFIDENCE = 0.7;

    public evaluate(decisions: Record<string, AgentResponse>): AgentResponse | null {
        let bestDecision: AgentResponse | null = null;

        for (const [agentName, decision] of Object.entries(decisions)) {
            // 1. Schema Validation (Implicitly done by interface, but good to check specifics)
            if (!['BUY', 'SELL', 'HOLD'].includes(decision.decision)) {
                logger.warn(`[DecisionGate] Invalid decision value from ${agentName}: ${decision.decision}`);
                continue;
            }

            // 2. Confidence Check
            if (decision.confidence < this.MIN_CONFIDENCE) {
                logger.info(`[DecisionGate] Rejected ${agentName} decision due to low confidence: ${decision.confidence}`);
                continue;
            }

            // 3. Selection Strategy (e.g., Highest Confidence)
            if (!bestDecision || decision.confidence > bestDecision.confidence) {
                bestDecision = decision;
            }
        }

        if (bestDecision) {
            logger.info(`[DecisionGate] Approved consensus decision: ${bestDecision.decision} (${bestDecision.confidence})`);
        } else {
            logger.info('[DecisionGate] No valid decisions passed the gate.');
        }

        return bestDecision;
    }
}
