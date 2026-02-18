import { IAgent, AgentContext, AgentResponse } from './interfaces';
import { OpenAIAgent } from './OpenAIAgent';
import { AnthropicAgent } from './AnthropicAgent';
import { SimulatedAgent } from './SimulatedAgent';
import { ContextBuilder } from '../services/ContextBuilder';
import { logger } from '../utils/logger';

export class AgentManager {
    private agents: IAgent[] = [];

    constructor(private contextBuilder: ContextBuilder) {
        this.initializeAgents();
    }

    private initializeAgents() {
        // OpenAI Agent (GPT-4)
        if (process.env.OPENAI_API_KEY) {
            this.agents.push(new OpenAIAgent('Alpha-GPT4', 'gpt-4-turbo-preview', process.env.OPENAI_API_KEY));
            logger.info('Initialized Alpha-GPT4');
        }

        // Anthropic Agent (Claude 3.5 Sonnet)
        if (process.env.ANTHROPIC_API_KEY) {
            this.agents.push(new AnthropicAgent('Beta-Claude3', 'claude-3-5-sonnet-20240620', process.env.ANTHROPIC_API_KEY));
            logger.info('Initialized Beta-Claude3');
        }

        // Fallback to Simulated Agent if no real agents are active
        if (this.agents.length === 0) {
            this.agents.push(new SimulatedAgent('Sim-Agent-1'));
            logger.warn('Initialized Sim-Agent-1 (No API keys found)');
        }
    }

    public async runAnalysis(symbol: string): Promise<{ decisions: Record<string, AgentResponse>; context: AgentContext }> {
        const context = await this.contextBuilder.buildContext(symbol);
        const activeAgents = this.agents.filter((agent) => !this.disabledAgents.has(agent.name));

        logger.info(`Running analysis for ${symbol} with ${activeAgents.length} active agents...`);

        const decisions: Record<string, AgentResponse> = {};

        const promises = activeAgents.map(async (agent) => {
            try {
                const start = Date.now();
                const response = await agent.analyze(context);
                const duration = Date.now() - start;

                logger.info(`Agent ${agent.name} finished in ${duration}ms: ${response.decision} (${response.confidence})`);

                decisions[agent.name] = response;
            } catch (error) {
                logger.error(`Agent ${agent.name} failed:`, error);
            }
        });

        await Promise.allSettled(promises);
        return { decisions, context };
    }

    private disabledAgents: Set<string> = new Set();

    public getActiveAgents(): string[] {
        return this.agents
            .filter(a => !this.disabledAgents.has(a.name))
            .map(a => a.name);
    }

    public toggleAgent(agentName: string, active: boolean) {
        if (active) {
            this.disabledAgents.delete(agentName);
            logger.info(`Agent ${agentName} ENABLED via Control Panel`);
        } else {
            this.disabledAgents.add(agentName);
            logger.info(`Agent ${agentName} DISABLED via Control Panel`);
        }
    }

    public getAllAgents(): { name: string, active: boolean }[] {
        return this.agents.map(a => ({
            name: a.name,
            active: !this.disabledAgents.has(a.name)
        }));
    }
}
