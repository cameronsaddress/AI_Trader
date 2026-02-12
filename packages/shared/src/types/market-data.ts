export interface Ticker {
    symbol: string;
    price: number;
    open_24h: number;
    volume_24h: number;
    low_24h: number;
    high_24h: number;
    volume_30d: number;
    best_bid: number;
    best_ask: number;
    side: string;
    time: string;
    trade_id: number;
    last_size: number;
}

export interface Trade {
    side: 'buy' | 'sell';
    trade_id: number;
    product_id: string;
    price: string;
    size: string;
    time: string;
}

export interface OrderBook {
    product_id: string;
    bids: [string, string][]; // [price, size]
    asks: [string, string][];
    sequence: number;
    time: string;
}

export interface Candle {
    timestamp: number;
    open: number;
    high: number;
    low: number;
    close: number;
    volume: number;
}

export type MarketDataEvent =
    | { type: 'ticker'; data: Ticker }
    | { type: 'trade'; data: Trade }
    | { type: 'l2update'; data: OrderBook };
