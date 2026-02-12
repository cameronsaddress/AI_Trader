import Anthropic from '@anthropic-ai/sdk';
import { IAgent, AgentContext, AgentResponse } from './interfaces';
import { logger } from '../utils/logger';

export class AnthropicAgent implements IAgent {
    private client: Anthropic;
    public name: string;
    public provider: string = 'anthropic';
    private model: string;

    constructor(name: string, model: string, apiKey: string) {
        this.name = name;
        this.model = model;
        this.client = new Anthropic({ apiKey });
    }

    async analyze(context: AgentContext): Promise<AgentResponse> {
        try {
            const msg = await (this.client as any).messages.create({
                model: this.model,
                max_tokens: 1024,
                system: this.buildSystemPrompt(),
                messages: [
                    { role: 'user', content: JSON.stringify(context) }
                ]
            });

            const content = msg.content[0].text;
            const parsed = this.parseResponse(content);

            return parsed;

        } catch (error) {
            logger.error(`Error in Anthropic agent ${this.name}:`, error);
            return {
                decision: 'HOLD',
                confidence: 0,
                reasoning: 'Error during analysis'
            };
        }
    }

    private parseResponse(content: string): AgentResponse {
        try {
            // Basic JSON extraction if LLM wraps it in markdown blocks
            const jsonMatch = content.match(/\{[\s\S]*\}/);
            const jsonStr = jsonMatch ? jsonMatch[0] : content;
            const parsed = JSON.parse(jsonStr);
            return {
                decision: parsed.decision,
                confidence: parsed.confidence,
                reasoning: parsed.reasoning,
                suggestedSize: parsed.suggestedSize
            };
        } catch (e) {
            logger.error('Failed to parse Anthropic response', e);
            return {
                decision: 'HOLD',
                confidence: 0,
                reasoning: 'Failed to parse response'
            };
        }
    }

    private buildSystemPrompt(): string {
        return `You are an elite cryptocurrency trader. Analyze the market data provided in JSON format.
    You must respond with a JSON object following this schema:
    {
      "decision": "BUY" | "SELL" | "HOLD",
      "confidence": number (0-1),
      "reasoning": string,
      "suggestedSize": number (0-1)
    }
    `;
    }
}
