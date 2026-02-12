import OpenAI from 'openai';
import { IAgent, AgentContext, AgentResponse } from './interfaces';
import { logger } from '../utils/logger';

export class OpenAIAgent implements IAgent {
    private client: OpenAI;
    public name: string;
    public provider: string = 'openai';
    private model: string;

    constructor(name: string, model: string, apiKey: string) {
        this.name = name;
        this.model = model;
        this.client = new OpenAI({ apiKey });
    }

    async analyze(context: AgentContext): Promise<AgentResponse> {
        try {
            const completion = await this.client.chat.completions.create({
                messages: [
                    { role: 'system', content: this.buildSystemPrompt() },
                    { role: 'user', content: JSON.stringify(context) }
                ],
                model: this.model,
                response_format: { type: "json_object" }
            });

            const content = completion.choices[0].message.content;
            if (!content) {
                throw new Error('Empty response from OpenAI');
            }

            const parsed = JSON.parse(content);
            return {
                decision: parsed.decision,
                confidence: parsed.confidence,
                reasoning: parsed.reasoning,
                suggestedSize: parsed.suggestedSize
            };

        } catch (error) {
            logger.error(`Error in OpenAI agent ${this.name}:`, error);
            return {
                decision: 'HOLD',
                confidence: 0,
                reasoning: 'Error during analysis'
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
