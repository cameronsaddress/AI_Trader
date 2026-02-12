import React from 'react';
import { AlertTriangle, RotateCcw } from 'lucide-react';
import { DashboardLayout } from '../components/layout/DashboardLayout';
import { ArbitrageScannerWidget } from '../components/widgets/ArbitrageScannerWidget';
import { StrategyControlWidget } from '../components/widgets/StrategyControlWidget';
import { useSocket } from '../context/SocketContext';

type TradingMode = 'PAPER' | 'LIVE';

type StatsState = {
    latency: string;
    markets: number;
    gap: string;
    bankroll: number;
};

type SystemHealthEntry = {
    name: string;
    status: string;
    load?: string;
};

type ExecutionLog = {
    timestamp: number;
    side: string;
    market: string;
    price: string;
    size: string;
};

type StrategyPnlPayload = {
    strategy?: string;
    pnl?: number;
    timestamp?: number;
    bankroll?: number;
    details?: {
        action?: string;
        exit?: number;
    };
};

type TradingModePayload = {
    mode?: TradingMode;
    live_order_posting_enabled?: boolean;
};

type SimulationResetEvent = {
    bankroll?: number;
};

type StrategyLiveExecutionEvent = {
    strategy?: string;
    total?: number;
    posted?: number;
    failed?: number;
    dryRun?: boolean;
    ok?: boolean;
    reason?: string;
    timestamp?: number;
};

const DEFAULT_SIM_BANKROLL = 1000;

function formatUsd(value: number): string {
    return `$${value.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function parseNumber(value: unknown): number | null {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
}

function toExecutionLog(input: unknown): ExecutionLog {
    if (!input || typeof input !== 'object') {
        return {
            timestamp: Date.now(),
            side: 'INFO',
            market: 'SYSTEM',
            price: 'UNKNOWN',
            size: '--',
        };
    }

    const payload = input as Partial<ExecutionLog>;
    const ts = parseNumber(payload.timestamp) ?? Date.now();

    return {
        timestamp: ts,
        side: typeof payload.side === 'string' ? payload.side : 'INFO',
        market: typeof payload.market === 'string' ? payload.market : 'SYSTEM',
        price: typeof payload.price === 'string' ? payload.price : String(payload.price ?? '---'),
        size: typeof payload.size === 'string' ? payload.size : String(payload.size ?? '--'),
    };
}

export const PolymarketPage: React.FC = () => {
    const { socket } = useSocket();
    const [stats, setStats] = React.useState<StatsState>({
        latency: '--',
        markets: 0,
        gap: '> 0.5%',
        bankroll: DEFAULT_SIM_BANKROLL,
    });
    const [execLogs, setExecLogs] = React.useState<ExecutionLog[]>([]);
    const [tradingMode, setTradingMode] = React.useState<TradingMode>('PAPER');
    const [liveOrderPostingEnabled, setLiveOrderPostingEnabled] = React.useState(false);
    const [lastLiveExecution, setLastLiveExecution] = React.useState<StrategyLiveExecutionEvent | null>(null);
    const [resetError, setResetError] = React.useState<string | null>(null);
    const [showResetConfirm, setShowResetConfirm] = React.useState(false);
    const [resetText, setResetText] = React.useState('');

    const isPaperMode = tradingMode === 'PAPER';
    const canConfirmReset = resetText.trim().toUpperCase() === 'RESET';

    React.useEffect(() => {
        let mounted = true;

        const fetchStats = async () => {
            try {
                const response = await fetch('/api/arb/stats');
                if (!response.ok || !mounted) {
                    return;
                }

                const data = await response.json() as {
                    bankroll?: number;
                    active_markets?: number;
                    trading_mode?: TradingMode;
                    live_order_posting_enabled?: boolean;
                };

                const bankroll = parseNumber(data.bankroll);
                const markets = parseNumber(data.active_markets);

                setStats((prev) => ({
                    ...prev,
                    bankroll: bankroll ?? prev.bankroll,
                    markets: markets ?? prev.markets,
                }));

                if (data.trading_mode === 'PAPER' || data.trading_mode === 'LIVE') {
                    setTradingMode(data.trading_mode);
                }
                if (typeof data.live_order_posting_enabled === 'boolean') {
                    setLiveOrderPostingEnabled(data.live_order_posting_enabled);
                }
            } catch {
                // keep last known stats on transient network errors
            }
        };

        void fetchStats();
        const intervalId = window.setInterval(() => {
            void fetchStats();
        }, 2000);

        return () => {
            mounted = false;
            window.clearInterval(intervalId);
        };
    }, []);

    React.useEffect(() => {
        if (!socket) return;

        const handleSystemHealth = (data: SystemHealthEntry[]) => {
            const scanner = data.find((s) => s.name === 'Arb Scanner');
            const latency = data.find((s) => s.name === 'Redis Queue')?.load || '--';
            const scannerCount = typeof scanner?.load === 'string' && scanner.load.startsWith('Markets:')
                ? parseInt(scanner.load.split(':')[1]?.trim() || '0', 10)
                : 0;

            setStats((prev) => ({
                ...prev,
                latency,
                markets: scanner?.status === 'operational' ? scannerCount : 0,
            }));
        };

        const handleExecutionLog = (log: unknown) => {
            setExecLogs((prev) => [toExecutionLog(log), ...prev].slice(0, 50));
        };

        const handleStrategyPnl = (data: StrategyPnlPayload) => {
            const bankroll = parseNumber(data.bankroll);
            if (bankroll !== null) {
                setStats((prev) => ({
                    ...prev,
                    bankroll,
                }));
            }

            const pnl = parseNumber(data.pnl) ?? 0;
            const exit = parseNumber(data.details?.exit);
            const logEntry: ExecutionLog = {
                timestamp: parseNumber(data.timestamp) ?? Date.now(),
                side: pnl >= 0 ? 'WIN' : 'LOSS',
                market: typeof data.strategy === 'string' ? data.strategy : 'UNKNOWN',
                price: `${data.details?.action || 'CLOSE'} @ ${exit !== null ? exit.toFixed(2) : '---'}`,
                size: `$${Math.abs(pnl).toFixed(2)}`,
            };
            setExecLogs((prev) => [logEntry, ...prev].slice(0, 50));
        };

        const handleTradingMode = (payload: TradingModePayload) => {
            if (payload.mode === 'PAPER' || payload.mode === 'LIVE') {
                setTradingMode(payload.mode);
            }
            if (typeof payload.live_order_posting_enabled === 'boolean') {
                setLiveOrderPostingEnabled(payload.live_order_posting_enabled);
            }
        };

        const handleSimulationReset = (payload: SimulationResetEvent) => {
            const bankroll = parseNumber(payload.bankroll) ?? DEFAULT_SIM_BANKROLL;
            setStats((prev) => ({
                ...prev,
                bankroll,
            }));
            setExecLogs([]);
            setResetError(null);
            setShowResetConfirm(false);
            setResetText('');
        };

        const handleSimulationResetError = (payload: { message?: string }) => {
            setResetError(payload.message || 'Failed to reset simulation');
        };

        const handleStrategyLiveExecution = (payload: StrategyLiveExecutionEvent) => {
            setLastLiveExecution(payload);
        };

        socket.on('system_health_update', handleSystemHealth);
        socket.on('execution_log', handleExecutionLog);
        socket.on('strategy_pnl', handleStrategyPnl);
        socket.on('strategy_live_execution', handleStrategyLiveExecution);
        socket.on('trading_mode_update', handleTradingMode);
        socket.on('simulation_reset', handleSimulationReset);
        socket.on('simulation_reset_error', handleSimulationResetError);
        socket.emit('request_trading_mode');

        return () => {
            socket.off('system_health_update', handleSystemHealth);
            socket.off('execution_log', handleExecutionLog);
            socket.off('strategy_pnl', handleStrategyPnl);
            socket.off('strategy_live_execution', handleStrategyLiveExecution);
            socket.off('trading_mode_update', handleTradingMode);
            socket.off('simulation_reset', handleSimulationReset);
            socket.off('simulation_reset_error', handleSimulationResetError);
        };
    }, [socket]);

    const resetSimulation = () => {
        if (!socket || !canConfirmReset) return;
        socket.emit('reset_simulation', {
            bankroll: DEFAULT_SIM_BANKROLL,
            confirmation: 'RESET',
            timestamp: Date.now(),
        });
    };

    return (
        <DashboardLayout>
            <div className="p-6 space-y-6">
            <header className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between mb-8">
                <div>
                    <h1 className="text-2xl font-bold bg-gradient-to-r from-blue-400 to-emerald-400 bg-clip-text text-transparent">
                        Polymarket Execution Engine
                    </h1>
                    <p className="text-gray-400 text-sm mt-1">
                        High-frequency arbitrage scanner & execution monitor
                    </p>
                </div>
                <div className="flex flex-wrap items-center gap-3">
                    <div className="flex items-center gap-2">
                        <span className="flex h-3 w-3 relative">
                            <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
                            <span className="relative inline-flex rounded-full h-3 w-3 bg-emerald-500"></span>
                        </span>
                        <span className="text-xs text-emerald-400 font-mono">ENGINE ACTIVE</span>
                    </div>

                    <div className={`px-3 py-1 rounded-md text-[10px] font-mono border ${isPaperMode ? 'text-blue-300 border-blue-500/30 bg-blue-500/10' : 'text-red-300 border-red-500/40 bg-red-500/10'}`}>
                        {isPaperMode ? 'PAPER TRADING' : liveOrderPostingEnabled ? 'LIVE ARMED' : 'LIVE DRY-RUN'}
                    </div>

                    <button
                        type="button"
                        onClick={() => {
                            setResetError(null);
                            setResetText('');
                            setShowResetConfirm(true);
                        }}
                        disabled={!isPaperMode}
                        className={`px-3 py-1.5 rounded-md text-xs font-mono border transition-colors ${
                            isPaperMode
                                ? 'border-amber-500/40 text-amber-300 bg-amber-500/10 hover:bg-amber-500/20'
                                : 'border-gray-600 text-gray-500 bg-gray-800/40 cursor-not-allowed'
                        }`}
                    >
                        <span className="inline-flex items-center gap-1">
                            <RotateCcw size={13} />
                            Reset Sim
                        </span>
                    </button>
                </div>
            </header>
            <div className="text-xs font-mono text-gray-500">
                Trading mode is controlled from the top header toggle.
            </div>
            {!isPaperMode && !liveOrderPostingEnabled && (
                <div className="text-xs font-mono text-amber-300">
                    LIVE is in dry-run mode. Real order posting is blocked until <span className="text-white">LIVE_ORDER_POSTING_ENABLED=true</span>.
                </div>
            )}
            {resetError && (
                <div className="text-xs font-mono text-red-400">{resetError}</div>
            )}
            {lastLiveExecution && (
                <div className={`text-xs font-mono ${
                    lastLiveExecution.dryRun
                        ? 'text-amber-300'
                        : lastLiveExecution.ok
                        ? 'text-emerald-300'
                        : 'text-red-300'
                }`}>
                    LIVE EXEC {lastLiveExecution.dryRun ? 'DRY-RUN' : 'POST'}: {parseNumber(lastLiveExecution.posted) ?? 0}/{parseNumber(lastLiveExecution.total) ?? 0} {lastLiveExecution.strategy || 'UNKNOWN'}
                    {lastLiveExecution.reason ? ` | ${lastLiveExecution.reason}` : ''}
                </div>
            )}

            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                <div className="lg:col-span-2 space-y-6">
                    <section>
                        <h2 className="text-sm uppercase tracking-wider text-gray-500 mb-4 font-mono">Live Opportunities</h2>
                        <ArbitrageScannerWidget />
                    </section>

                    <section>
                        <StrategyControlWidget />
                    </section>

                    <section className="bg-black/40 backdrop-blur-sm border border-white/5 rounded-xl p-4 h-[300px] font-mono text-xs overflow-y-auto">
                        <h2 className="text-xs uppercase tracking-wider text-gray-500 mb-2 sticky top-0 bg-black/40 backdrop-blur-md py-2">Execution Stream</h2>
                        <div className="space-y-1 text-gray-400">
                            {execLogs.length === 0 && (
                                <div className="flex gap-2">
                                    <span className="text-gray-600">[SAFE_MODE]</span>
                                    <span className="text-blue-400">Scanner</span>
                                    <span>Scanning active markets...</span>
                                </div>
                            )}
                            {execLogs.map((log, i) => (
                                <div key={`${log.timestamp}-${i}`} className="flex gap-2 border-b border-white/5 pb-1 mb-1">
                                    <span className="text-gray-600">[{new Date(log.timestamp).toLocaleTimeString()}]</span>
                                    <span className={`font-bold ${(log.side === 'BUY' || log.side === 'WIN') ? 'text-green-400' : 'text-red-400'}`}>
                                        {log.side}
                                    </span>
                                    <span className="text-white">{log.market}</span>
                                    <span className="text-gray-500">
                                        {log.price} | Size: {log.size}
                                    </span>
                                </div>
                            ))}
                        </div>
                    </section>
                </div>

                <div className="space-y-6">
                    <div className="bg-white/5 border border-white/10 rounded-xl p-6">
                        <h3 className="text-sm font-medium text-gray-300 mb-4">Engine Stats</h3>
                        <div className="space-y-4">
                            <div className="flex justify-between items-center">
                                <span className="text-gray-500 text-sm">Latency</span>
                                <span className="text-emerald-400 font-mono">{stats.latency}</span>
                            </div>
                            <div className="flex justify-between items-center">
                                <span className="text-gray-500 text-sm">Markets</span>
                                <span className="text-white font-mono">{stats.markets}</span>
                            </div>
                            <div className="flex justify-between items-center">
                                <span className="text-gray-500 text-sm">Target Gap</span>
                                <span className="text-white font-mono">{stats.gap}</span>
                            </div>
                            <div className="flex justify-between items-center">
                                <span className="text-gray-500 text-sm">Mode</span>
                                <span className={`font-mono ${isPaperMode ? 'text-blue-300' : 'text-red-300'}`}>
                                    {isPaperMode ? 'PAPER' : liveOrderPostingEnabled ? 'LIVE_ARMED' : 'LIVE_DRY_RUN'}
                                </span>
                            </div>
                            <div className="flex justify-between items-center">
                                <span className="text-gray-500 text-sm">Bankroll</span>
                                <span className="text-blue-400 font-mono">{formatUsd(stats.bankroll)}</span>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            {showResetConfirm && (
                <div className="fixed inset-0 z-[100] bg-black/80 backdrop-blur-sm flex items-center justify-center p-4">
                    <div className="w-full max-w-md bg-[#111] border border-amber-500/30 rounded-xl p-6 shadow-2xl">
                        <div className="flex items-center gap-2 text-amber-300 mb-3">
                            <AlertTriangle size={18} />
                            <h3 className="font-bold">Reset Simulation</h3>
                        </div>
                        <p className="text-sm text-gray-300 mb-4">
                            This clears paper bankroll back to <span className="font-mono text-white">{formatUsd(DEFAULT_SIM_BANKROLL)}</span> and resets UI PnL/logs. Type <span className="font-mono text-white">RESET</span> to confirm.
                        </p>
                        <input
                            value={resetText}
                            onChange={(e) => setResetText(e.target.value)}
                            placeholder="Type RESET"
                            className="w-full bg-black/50 border border-white/20 rounded px-3 py-2 text-sm text-white mb-4 focus:outline-none focus:border-amber-400"
                        />
                        <div className="flex justify-end gap-2">
                            <button
                                onClick={() => {
                                    setShowResetConfirm(false);
                                    setResetText('');
                                }}
                                className="px-3 py-2 text-sm rounded border border-white/20 text-gray-300 hover:bg-white/5"
                            >
                                Cancel
                            </button>
                            <button
                                onClick={resetSimulation}
                                disabled={!canConfirmReset}
                                className={`px-3 py-2 text-sm rounded font-bold ${canConfirmReset ? 'bg-amber-500 text-black hover:bg-amber-400' : 'bg-gray-700 text-gray-400 cursor-not-allowed'}`}
                            >
                                Confirm Reset
                            </button>
                        </div>
                    </div>
                </div>
            )}
            </div>
        </DashboardLayout>
    );
};
