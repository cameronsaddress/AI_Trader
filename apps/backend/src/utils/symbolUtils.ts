
/**
 * Map Coinbase-style symbols to Hyperliquid format
 * Coinbase: BTC-USD, ETH-USD
 * Hyperliquid: BTC, ETH
 */
export function toHyperliquidSymbol(coinbaseSymbol: string): string {
    if (!coinbaseSymbol) return '';
    // Remove -USD, -USDT, -PERP suffixes
    return String(coinbaseSymbol)
        .toUpperCase()
        .replace(/-USD$/, '')
        .replace(/-USDT$/, '')
        .replace(/-PERP$/, '');
}

export function toCoinbaseSymbol(hlSymbol: string): string {
    if (!hlSymbol) return '';
    return `${String(hlSymbol).toUpperCase()}-USD`;
}
