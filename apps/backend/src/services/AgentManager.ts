import { IAgent, AgentContext, AgentResponse } from '../agents/interfaces';
import { logger } from '../utils/logger';

export class AgentManager {
  private agents: Map<string, IAgent> = new Map();

  public registerAgent(agent: IAgent) {
    if (this.agents.has(agent.name)) {
      logger.warn(`Agent ${agent.name} already registered. Overwriting.`);
    }
    this.agents.set(agent.name, agent);
    logger.info(`Agent registered: ${agent.name} (${agent.provider})`);
  }

  public async broadcast(context: AgentContext): Promise<Map<string, AgentResponse>> {
    logger.info(`Broadcasting context to ${this.agents.size} agents`);
    
    const promises = Array.from(this.agents.values()).map(async (agent) => {
      try {
        const response = await agent.analyze(context);
        return { name: agent.name, response };
      } catch (error) {
        logger.error(`Error executing agent ${agent.name}`, error);
        return { 
          name: agent.name, 
          response: { 
            decision: 'HOLD' as const, 
            confidence: 0, 
            reasoning: 'Execution Failure' 
          } 
        };
      }
    });

    const results = await Promise.all(promises);
    const decisionMap = new Map<string, AgentResponse>();
    
    results.forEach(r => decisionMap.set(r.name, r.response));
    return decisionMap;
  }

  public getAgents(): IAgent[] {
    return Array.from(this.agents.values());
  }
}
