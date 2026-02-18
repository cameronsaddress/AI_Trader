export interface AgentResponse {
    decision: 'BUY' | 'SELL' | 'HOLD';
    confidence: number;
    reasoning: string;
    suggestedSize?: number; // 0-1 percentage
}

export interface AgentMarketIndicators {
    rsi: number;
    sma20: number;
    returns: number;
}

export interface AgentMarketData {
    symbol: string;
    price: number;
    timestamp: string;
    indicators: AgentMarketIndicators;
}

export interface AgentAccountContext {
    balance: number;
    equity: number;
    reservedNotional: number;
    realizedPnl: number;
    positions: unknown[];
}

export interface AgentRiskContext {
    var95: number;
    maxDrawdown: number;
    exposure: number;
}

export interface AgentContext {
    marketData: AgentMarketData;
    account: AgentAccountContext;
    risk: AgentRiskContext;
}

export interface IAgent {
    name: string;
    provider: string; // 'openai' | 'anthropic'
    analyze(context: AgentContext): Promise<AgentResponse>;
}
