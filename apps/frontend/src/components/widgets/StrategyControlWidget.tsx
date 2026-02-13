import React, { useState, useEffect } from 'react';
import { Play, Square, Activity, Zap, Copy, BarChart2 } from 'lucide-react';
import { useSocket } from '../../context/SocketContext';

interface Strategy {
    id: string;
    name: string;
    description: string;
    icon: React.ReactNode;
    color: string;
    status: 'active' | 'paused' | 'error';
    stats: {
        pnl: string;
        daily_trades: number;
    }
}

const STRATEGIES: Strategy[] = [
    {
        id: 'BTC_5M',
        name: 'BTC 5m Engine',
        description: 'High-frequency BTC 5-minute up/down engine. Compares Coinbase spot vs Polymarket odds and enters one directional position per window (UP or DOWN), holding to resolution.',
        icon: <Zap size={18} />,
        color: 'text-rose-300',
        status: 'active',
        stats: { pnl: '$0.00', daily_trades: 0 }
    },
    {
        id: 'BTC_15M',
        name: 'BTC Fair Value',
        description: 'Directional fair-value model for BTC 15-minute up/down contracts. This strategy can lose money and is not a guaranteed arbitrage.',
        icon: <Zap size={18} />,
        color: 'text-amber-400',
        status: 'active', // Default active
        stats: { pnl: '$0.00', daily_trades: 0 }
    },
    {
        id: 'ETH_15M',
        name: 'ETH Fair Value',
        description: 'Directional fair-value model for ETH 15-minute up/down contracts with adaptive volatility and execution filters.',
        icon: <Activity size={18} />,
        color: 'text-indigo-400',
        status: 'active',
        stats: { pnl: '$0.00', daily_trades: 0 }
    },
    {
        id: 'SOL_15M',
        name: 'SOL Fair Value',
        description: 'Directional fair-value model for SOL 15-minute up/down contracts. Uses the same guarded execution framework as BTC/ETH.',
        icon: <Zap size={18} />,
        color: 'text-purple-400',
        status: 'active',
        stats: { pnl: '$0.00', daily_trades: 0 }
    },
    {
        id: 'CEX_SNIPER',
        name: 'CEX Latency Arb',
        description: 'Front-runs Polymarket orderbook using real-time Coinbase WebSocket feeds. Capitalizes on the ~400ms oracle delay.',
        icon: <BarChart2 size={18} />,
        color: 'text-blue-400',
        status: 'active',
        stats: { pnl: '$0.00', daily_trades: 0 }
    },
    {
        id: 'SYNDICATE',
        name: 'Whale Syndicate Detector',
        description: 'Instantly mirrors trades from top 10 profitable wallets (e.g. @gabagool22). Auto-scales size based on confidence.',
        icon: <Copy size={18} />,
        color: 'text-emerald-400',
        status: 'active',
        stats: { pnl: '$0.00', daily_trades: 0 }
    },
    {
        id: 'ATOMIC_ARB',
        name: 'Atomic Pair Arb',
        description: 'Buys equal YES/NO shares when combined ask implies positive locked edge after modeled costs and safety buffer.',
        icon: <Activity size={18} />,
        color: 'text-cyan-400',
        status: 'active',
        stats: { pnl: '$0.00', daily_trades: 0 }
    },
    {
        id: 'OBI_SCALPER',
        name: 'OBI Scalper',
        description: 'Trades Coinbase microstructure momentum from level2 imbalance and short-lived order-flow pressure.',
        icon: <BarChart2 size={18} />,
        color: 'text-orange-400',
        status: 'active',
        stats: { pnl: '$0.00', daily_trades: 0 }
    },
    {
        id: 'GRAPH_ARB',
        name: 'Graph Constraint Arb',
        description: 'Scans a multi-market universe for constraint-graph violations and allocates to the best net locked edge after modeled costs.',
        icon: <Activity size={18} />,
        color: 'text-teal-300',
        status: 'active',
        stats: { pnl: '$0.00', daily_trades: 0 }
    },
    {
        id: 'CONVERGENCE_CARRY',
        name: 'Convergence Carry',
        description: 'Targets cross-outcome parity reversion in binary books near expiry with disciplined exits and bounded hold windows.',
        icon: <BarChart2 size={18} />,
        color: 'text-sky-300',
        status: 'active',
        stats: { pnl: '$0.00', daily_trades: 0 }
    },
    {
        id: 'MAKER_MM',
        name: 'Maker Micro-MM',
        description: 'Captures spread where expected maker edge survives fee/rebate/adverse-selection model constraints.',
        icon: <Activity size={18} />,
        color: 'text-lime-300',
        status: 'active',
        stats: { pnl: '$0.00', daily_trades: 0 }
    }
];

export const StrategyControlWidget: React.FC = () => {
    // UI mirrors backend state; local defaults are replaced on first socket update.
    const [strategies, setStrategies] = useState(STRATEGIES);
    const [riskModel, setRiskModel] = useState<'FIXED' | 'PERCENT'>('FIXED');
    const [riskValue, setRiskValue] = useState(50);
    const { socket } = useSocket();
    const [selectedStrategyId, setSelectedStrategyId] = useState<string | null>(null);
    const [tradeHistory, setTradeHistory] = useState<Record<string, any[]>>({});

    useEffect(() => {
        if (!socket) return;

        const handleStrategyPnl = (data: { strategy: string, pnl: number, timestamp: number, details?: any }) => {
            // Update local trade history view only; aggregate metrics come from backend.
            setTradeHistory(prev => ({
                ...prev,
                [data.strategy]: [
                    {
                        pnl: data.pnl,
                        timestamp: data.timestamp,
                        details: data.details || {}
                    },
                    ...(prev[data.strategy] || []).slice(0, 49)
                ]
            }));
        };

        const handleStrategyMetrics = (metrics: Record<string, { pnl?: number; daily_trades?: number }>) => {
            setStrategies(prev => prev.map((s) => {
                const metric = metrics?.[s.id];
                if (!metric) {
                    return s;
                }

                const pnl = typeof metric.pnl === 'number' && Number.isFinite(metric.pnl) ? metric.pnl : 0;
                const trades = typeof metric.daily_trades === 'number' && Number.isFinite(metric.daily_trades)
                    ? Math.max(0, Math.floor(metric.daily_trades))
                    : 0;
                const pnlPrefix = pnl > 0 ? '+' : pnl < 0 ? '-' : '';

                return {
                    ...s,
                    stats: {
                        pnl: `${pnlPrefix}$${Math.abs(pnl).toFixed(2)}`,
                        daily_trades: trades,
                    },
                };
            }));
        };

        const handleStrategyStatus = (statusMap: Record<string, boolean>) => {
            setStrategies(prev => prev.map((s) => ({
                ...s,
                status: statusMap[s.id] === false ? 'paused' : 'active',
            })));
        };

        const handleRiskConfig = (cfg: { model?: 'FIXED' | 'PERCENT'; value?: number }) => {
            if (!cfg || !cfg.model || typeof cfg.value !== 'number') return;
            setRiskModel(cfg.model);
            setRiskValue(cfg.value);
        };

        const handleSimulationReset = () => {
            setStrategies(prev => prev.map((s) => ({
                ...s,
                stats: {
                    pnl: '$0.00',
                    daily_trades: 0,
                },
            })));
            setTradeHistory({});
            setSelectedStrategyId(null);
        };

        socket.on('strategy_pnl', handleStrategyPnl);
        socket.on('strategy_metrics_update', handleStrategyMetrics);
        socket.on('strategy_status_update', handleStrategyStatus);
        socket.on('risk_config_update', handleRiskConfig);
        socket.on('simulation_reset', handleSimulationReset);

        socket.emit('request_risk_config');

        return () => {
            socket.off('strategy_pnl', handleStrategyPnl);
            socket.off('strategy_metrics_update', handleStrategyMetrics);
            socket.off('strategy_status_update', handleStrategyStatus);
            socket.off('risk_config_update', handleRiskConfig);
            socket.off('simulation_reset', handleSimulationReset);
        }
    }, [socket]);

    const toggleStrategy = (e: React.MouseEvent, id: string) => {
        e.stopPropagation();
        setStrategies(prev => prev.map((s) => {
            if (s.id !== id) return s;
            const active = s.status !== 'active';
            if (socket) {
                socket.emit('toggle_strategy', { id, active, timestamp: Date.now() });
            }
            return { ...s, status: active ? 'active' : 'paused' };
        }));
    };

    const updateRiskConfig = (model: 'FIXED' | 'PERCENT', value: number) => {
        setRiskModel(model);
        setRiskValue(value);
        if (socket) {
            socket.emit('update_risk_config', {
                model,
                value,
                timestamp: Date.now()
            });
        }
    };

    const selectedStrategy = strategies.find(s => s.id === selectedStrategyId);

    return (
        <div className="bg-black/40 backdrop-blur-md border border-white/10 rounded-xl p-6">
            <div className="flex flex-col md:flex-row justify-between items-start md:items-center mb-6 gap-4">
                <h2 className="text-sm uppercase tracking-wider text-gray-400 font-mono flex items-center gap-2">
                    <Activity size={14} /> Active Strategy Swarm
                </h2>

                {/* Risk Management Panel */}
                <div className="flex items-center gap-3 bg-black/60 p-2 rounded-lg border border-white/5">
                    <span className="text-[10px] text-gray-500 font-mono uppercase px-2">Risk Model</span>

                    {/* Toggle */}
                    <div className="flex bg-white/5 rounded p-1">
                        <button
                            onClick={() => updateRiskConfig('FIXED', 50)}
                            className={`px-3 py-1 text-[10px] rounded font-bold transition-all ${riskModel === 'FIXED' ? 'bg-emerald-500 text-black' : 'text-gray-400 hover:text-white'}`}
                        >
                            FIXED ($)
                        </button>
                        <button
                            onClick={() => updateRiskConfig('PERCENT', 1.0)}
                            className={`px-3 py-1 text-[10px] rounded font-bold transition-all ${riskModel === 'PERCENT' ? 'bg-purple-500 text-white' : 'text-gray-400 hover:text-white'}`}
                        >
                            RISK (%)
                        </button>
                    </div>

                    {/* Value Input */}
                    <div className="flex items-center gap-2 border-l border-white/10 pl-3">
                        <input
                            type="range"
                            min={riskModel === 'FIXED' ? "10" : "0.1"}
                            max={riskModel === 'FIXED' ? "500" : "5.0"}
                            step={riskModel === 'FIXED' ? "10" : "0.1"}
                            value={riskValue}
                            onChange={(e) => updateRiskConfig(riskModel, parseFloat(e.target.value))}
                            className={`w-24 h-1 rounded-lg appearance-none cursor-pointer ${riskModel === 'FIXED' ? 'accent-emerald-500 bg-emerald-900/30' : 'accent-purple-500 bg-purple-900/30'}`}
                        />
                        <span className={`text-xs font-mono font-bold w-16 text-right ${riskModel === 'FIXED' ? 'text-emerald-400' : 'text-purple-400'}`}>
                            {riskModel === 'FIXED' ? `$${riskValue}` : `${riskValue.toFixed(1)}%`}
                        </span>
                    </div>
                </div>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                {strategies.map(strategy => (
                    <div
                        key={strategy.id}
                        onClick={() => setSelectedStrategyId(strategy.id)}
                        className={`
                            relative group border rounded-lg p-4 transition-all duration-300 cursor-pointer
                            ${strategy.status === 'active'
                                ? 'bg-white/5 border-white/10 hover:border-white/20 hover:bg-white/10'
                                : 'bg-black/20 border-white/5 opacity-60 grayscale'}
                        `}
                    >
                        {/* Status Dot */}
                        <div className={`absolute top-3 right-3 w-2 h-2 rounded-full ${strategy.status === 'active' ? 'bg-emerald-500 shadow-[0_0_8px_rgba(16,185,129,0.5)]' : 'bg-red-500'}`} />

                        {/* Note: Tooltip removed to avoid clutter, using Modal instead */}

                        {/* Header */}
                        <div className="flex items-center gap-3 mb-3">
                            <div className={`${strategy.color}`}>
                                {strategy.icon}
                            </div>
                            <h3 className="text-sm font-bold text-gray-200 leading-tight">
                                {strategy.name}
                            </h3>
                        </div>

                        {/* Stats */}
                        <div className="space-y-1 mb-4">
                            <div className="flex justify-between text-xs">
                                <span className="text-gray-500">Daily PnL</span>
                                <span className={strategy.stats.pnl.startsWith('+') ? 'text-emerald-400' : 'text-rose-400'}>
                                    {strategy.stats.pnl}
                                </span>
                            </div>
                            <div className="flex justify-between text-xs">
                                <span className="text-gray-500">Trades</span>
                                <span className="text-gray-300">{strategy.stats.daily_trades}</span>
                            </div>
                        </div>

                        {/* Control Button */}
                        <button
                            onClick={(e) => toggleStrategy(e, strategy.id)}
                            className={`
                                w-full py-2 px-3 rounded flex items-center justify-center gap-2 text-xs font-bold transition-colors
                                ${strategy.status === 'active'
                                    ? 'bg-red-500/10 text-red-400 hover:bg-red-500/20 border border-red-500/20'
                                    : 'bg-emerald-500/10 text-emerald-400 hover:bg-emerald-500/20 border border-emerald-500/20'}
                            `}
                        >
                            {strategy.status === 'active' ? (
                                <>
                                    <Square size={12} fill="currentColor" /> STOP ENGINE
                                </>
                            ) : (
                                <>
                                    <Play size={12} fill="currentColor" /> START ENGINE
                                </>
                            )}
                        </button>
                    </div>
                ))}
            </div>

            {/* Strategy Detail Modal - Inline for simplicity */}
            {selectedStrategy && (
                <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/80 backdrop-blur-sm p-4" onClick={() => setSelectedStrategyId(null)}>
                    <div className="bg-[#111] border border-white/10 rounded-xl w-full max-w-2xl max-h-[80vh] flex flex-col overflow-hidden shadow-2xl" onClick={e => e.stopPropagation()}>
                        <div className="p-6 border-b border-white/10 flex justify-between items-center bg-white/5">
                            <div className="flex items-center gap-4">
                                <div className={`p-3 rounded-lg bg-black/40 ${selectedStrategy.color}`}>
                                    {selectedStrategy.icon}
                                </div>
                                <div>
                                    <h3 className="text-xl font-bold text-white">{selectedStrategy.name}</h3>
                                    <p className="text-xs text-gray-400 mt-1 max-w-md">{selectedStrategy.description}</p>
                                </div>
                            </div>
                            <button onClick={() => setSelectedStrategyId(null)} className="text-gray-500 hover:text-white transition-colors">
                                ✕
                            </button>
                        </div>

                        <div className="flex-1 overflow-y-auto p-0 custom-scrollbar">
                            <table className="w-full text-left text-sm">
                                <thead className="bg-white/5 text-xs text-gray-500 uppercase font-mono sticky top-0 backdrop-blur-md">
                                    <tr>
                                        <th className="p-4">Time</th>
                                        <th className="p-4">Action</th>
                                        <th className="p-4">Details</th>
                                        <th className="p-4 text-right">PnL</th>
                                    </tr>
                                </thead>
                                <tbody className="divide-y divide-white/5">
                                    {(tradeHistory[selectedStrategy.id] || []).length === 0 ? (
                                        <tr>
                                            <td colSpan={4} className="p-8 text-center text-gray-500 italic">No trades recorded yet this session.</td>
                                        </tr>
                                    ) : (
                                        (tradeHistory[selectedStrategy.id] || []).map((trade, idx) => (
                                            <tr key={idx} className="hover:bg-white/5 transition-colors group">
                                                <td className="p-4 text-gray-400 font-mono text-xs">
                                                    {new Date(trade.timestamp).toLocaleTimeString()}
                                                </td>
                                                <td className="p-4">
                                                    <span className={`px-2 py-1 rounded text-[10px] font-bold tracking-wider ${trade.pnl >= 0 ? 'bg-emerald-500/10 text-emerald-400' : 'bg-red-500/10 text-red-400'
                                                        }`}>
                                                        {trade.details.action || 'EXECUTE'}
                                                    </span>
                                                </td>
                                                <td className="p-4 text-xs text-gray-300">
                                                    {trade.details.asset && (
                                                        <div className="flex flex-col gap-1">
                                                            <div className="flex items-center gap-2">
                                                                <span className="text-gray-500">ASSET:</span> {trade.details.asset}
                                                            </div>
                                                            {trade.details.entry_price && (
                                                                <div className="flex items-center gap-2">
                                                                    <span className="text-gray-500">ENTRY:</span> ${trade.details.entry_price.toFixed(4)}
                                                                </div>
                                                            )}
                                                            {trade.details.whale_size && (
                                                                <div className="flex items-center gap-2">
                                                                    <span className="text-purple-400">WHALE:</span> ${trade.details.whale_size.toFixed(0)}
                                                                </div>
                                                            )}
                                                            <div className="text-[10px] text-gray-600 font-mono mt-1 group-hover:text-blue-400 cursor-pointer flex items-center gap-1">
                                                                TX: {trade.details.tx_hash ? trade.details.tx_hash.substring(0, 16) + '...' : '---'}
                                                            </div>
                                                        </div>
                                                    )}
                                                </td>
                                                <td className={`p-4 text-right font-mono font-bold ${trade.pnl >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                                                    {trade.pnl >= 0 ? '+' : ''}${trade.pnl.toFixed(2)}
                                                </td>
                                            </tr>
                                        ))
                                    )}
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
};
