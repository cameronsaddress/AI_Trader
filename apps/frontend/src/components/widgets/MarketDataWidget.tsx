import { useEffect, useRef, useState } from 'react';
import { createChart, ColorType, ISeriesApi } from 'lightweight-charts';
import { useSocket } from '../../context/SocketContext';

export function MarketDataWidget() {
    const chartContainerRef = useRef<HTMLDivElement>(null);
    const candleSeriesRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
    // Safely destructure socket
    const { socket } = useSocket();
    const [currentPrice, setCurrentPrice] = useState<number | null>(null);
    const [_lastCandle, setLastCandle] = useState<any>(null);

    useEffect(() => {
        if (!chartContainerRef.current) return;

        const chart = createChart(chartContainerRef.current, {
            layout: {
                background: { type: ColorType.Solid, color: 'transparent' },
                textColor: 'rgba(255, 255, 255, 0.5)',
            },
            grid: {
                vertLines: { color: 'rgba(255, 255, 255, 0.05)' },
                horzLines: { color: 'rgba(255, 255, 255, 0.05)' },
            },
            width: chartContainerRef.current.clientWidth,
            height: 400,
            timeScale: {
                timeVisible: true,
                secondsVisible: false,
            },
        });

        const candleSeries = chart.addCandlestickSeries({
            upColor: '#10B981',
            downColor: '#EF4444',
            borderVisible: false,
            wickUpColor: '#10B981',
            wickDownColor: '#EF4444',
        });
        candleSeriesRef.current = candleSeries;

        const handleResize = () => {
            if (chartContainerRef.current) {
                chart.applyOptions({ width: chartContainerRef.current.clientWidth });
            }
        };

        window.addEventListener('resize', handleResize);

        return () => {
            window.removeEventListener('resize', handleResize);
            chart.remove();
        };
    }, []);

    // Socket Data Handler
    useEffect(() => {
        if (!socket) return;
        console.log("Setting up market_update listener");

        const handleMarketUpdate = (data: any) => {
            // data matches payload from MarketDataService: { symbol, price, timestamp, candle? }
            if (data.symbol === 'BTC-USD') {
                if (data.price) {
                    setCurrentPrice(data.price);
                }

                if (data.candle) {
                    // Hyperliquid candle: { time (seconds), open, high, low, close }
                    // lightweight-charts expects time in seconds (UTCTimestamp) or string 'yyyy-mm-dd'
                    candleSeriesRef.current?.update(data.candle);
                    setLastCandle(data.candle);
                }
            }
        };

        socket.on('market_update', handleMarketUpdate);

        return () => {
            socket.off('market_update', handleMarketUpdate);
        };
    }, [socket]);

    return (
        <div className="p-6 rounded-xl bg-white/5 border border-white/10 backdrop-blur-md">
            <div className="flex justify-between items-center mb-6">
                <div>
                    <h2 className="text-lg font-semibold text-white">BTC-USD</h2>
                    <p className="text-sm text-emerald-400 font-mono flex items-center gap-2">
                        {currentPrice ? `$${currentPrice.toLocaleString()}` : 'Connecting...'}
                        <span className="text-xs px-1.5 py-0.5 rounded bg-emerald-500/10">Live</span>
                    </p>
                </div>
                <div className="flex gap-2">
                    {['1H', '4H', '1D', '1W'].map((tf) => (
                        <button key={tf} className="px-3 py-1 text-xs rounded-md bg-white/5 hover:bg-white/10 text-gray-400 hover:text-white transition-colors">
                            {tf}
                        </button>
                    ))}
                </div>
            </div>
            <div ref={chartContainerRef} className="w-full h-[400px]" />
        </div>
    );
}
