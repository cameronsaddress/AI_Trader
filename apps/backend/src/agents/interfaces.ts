export interface AgentResponse {
    decision: 'BUY' | 'SELL' | 'HOLD';
    confidence: number;
    reasoning: string;
    suggestedSize?: number; // 0-1 percentage
}

export interface AgentContext {
    marketData: any; // Ticker, OrderBook, Indicators
    account: any;    // Balance, Positions
    risk: any;       // VaR, Drawdown
}

export interface IAgent {
    name: string;
    provider: string; // 'openai' | 'anthropic'
    analyze(context: AgentContext): Promise<AgentResponse>;
}
