import { useEffect, useMemo, useState } from 'react';
import { Shield, Zap, Power, AlertTriangle, KeyRound } from 'lucide-react';
import { useSocket } from '../../context/SocketContext';

type TradingMode = 'PAPER' | 'LIVE';

export const Header = () => {
    const { socket, controlPlaneToken, setControlPlaneToken } = useSocket();
    const [tradingMode, setTradingMode] = useState<TradingMode>('PAPER');
    const [liveOrderPostingEnabled, setLiveOrderPostingEnabled] = useState(false);
    const [intelligenceGateEnabled, setIntelligenceGateEnabled] = useState(true);
    const [liveSignals, setLiveSignals] = useState(0);
    const [pendingSettlements, setPendingSettlements] = useState(0);
    const [showLiveConfirm, setShowLiveConfirm] = useState(false);
    const [showAuthPrompt, setShowAuthPrompt] = useState(false);
    const [confirmText, setConfirmText] = useState('');
    const [authText, setAuthText] = useState('');
    const [modeError, setModeError] = useState<string | null>(null);

    const isPaperMode = tradingMode === 'PAPER';
    const canConfirmLive = useMemo(() => confirmText.trim().toUpperCase() === 'LIVE', [confirmText]);
    const hasControlToken = Boolean(controlPlaneToken && controlPlaneToken.trim().length > 0);

    useEffect(() => {
        if (!socket) return;
        const activeSignals = new Map<string, number>();

        const handleTradingMode = (payload: { mode?: TradingMode; live_order_posting_enabled?: boolean }) => {
            if (payload?.mode === 'PAPER' || payload?.mode === 'LIVE') {
                setTradingMode(payload.mode);
                setModeError(null);
            }
            if (typeof payload?.live_order_posting_enabled === 'boolean') {
                setLiveOrderPostingEnabled(payload.live_order_posting_enabled);
            }
        };

        const handleTradingModeError = (payload: { message?: string }) => {
            setModeError(payload?.message || 'Failed to update trading mode');
        };

        const handleAuthError = (payload: { message?: string }) => {
            setModeError(payload?.message || 'Unauthorized control-plane action');
        };

        const handleIntelligenceGateConfig = (payload: { enabled?: boolean }) => {
            if (typeof payload?.enabled === 'boolean') {
                setIntelligenceGateEnabled(payload.enabled);
            }
        };

        const handleIntelligenceSnapshot = (payload: Record<string, { passes_threshold?: boolean; timestamp?: number }>) => {
            if (!payload || typeof payload !== 'object') {
                return;
            }
            activeSignals.clear();
            for (const [strategy, entry] of Object.entries(payload)) {
                if (!entry || entry.passes_threshold !== true) {
                    continue;
                }
                const ts = Number(entry.timestamp);
                if (Number.isFinite(ts)) {
                    activeSignals.set(strategy, ts);
                }
            }
            recomputeLiveSignals();
        };

        const handleIntelligenceUpdate = (payload: { strategy?: string; passes_threshold?: boolean; timestamp?: number }) => {
            if (!payload || typeof payload.strategy !== 'string') {
                return;
            }
            const ts = Number(payload.timestamp);
            if (payload.passes_threshold === true && Number.isFinite(ts)) {
                activeSignals.set(payload.strategy, ts);
            } else {
                activeSignals.delete(payload.strategy);
            }
            recomputeLiveSignals();
        };

        const handleSettlementSnapshot = (payload: { positions?: Array<{ status?: string }> }) => {
            const positions = Array.isArray(payload?.positions) ? payload.positions : [];
            const pending = positions.filter((position) => position?.status !== 'REDEEMED').length;
            setPendingSettlements(pending);
        };

        const recomputeLiveSignals = () => {
            const now = Date.now();
            for (const [strategy, ts] of activeSignals.entries()) {
                if (now - ts > 3000) {
                    activeSignals.delete(strategy);
                }
            }
            setLiveSignals(activeSignals.size);
        };

        socket.on('trading_mode_update', handleTradingMode);
        socket.on('trading_mode_error', handleTradingModeError);
        socket.on('auth_error', handleAuthError);
        socket.on('intelligence_gate_config', handleIntelligenceGateConfig);
        socket.on('intelligence_snapshot', handleIntelligenceSnapshot);
        socket.on('intelligence_update', handleIntelligenceUpdate);
        socket.on('settlement_snapshot', handleSettlementSnapshot);
        socket.emit('request_trading_mode');
        const timer = window.setInterval(recomputeLiveSignals, 1000);

        return () => {
            socket.off('trading_mode_update', handleTradingMode);
            socket.off('trading_mode_error', handleTradingModeError);
            socket.off('auth_error', handleAuthError);
            socket.off('intelligence_gate_config', handleIntelligenceGateConfig);
            socket.off('intelligence_snapshot', handleIntelligenceSnapshot);
            socket.off('intelligence_update', handleIntelligenceUpdate);
            socket.off('settlement_snapshot', handleSettlementSnapshot);
            window.clearInterval(timer);
        };
    }, [socket]);

    const toggleTradingMode = () => {
        if (!socket) return;

        if (isPaperMode) {
            setShowLiveConfirm(true);
            setConfirmText('');
            return;
        }

        socket.emit('set_trading_mode', { mode: 'PAPER', timestamp: Date.now() });
    };

    const confirmLiveMode = () => {
        if (!socket || !canConfirmLive) return;
        socket.emit('set_trading_mode', { mode: 'LIVE', confirmation: 'LIVE', timestamp: Date.now() });
        setShowLiveConfirm(false);
        setConfirmText('');
    };

    const openAuthPrompt = () => {
        setAuthText(controlPlaneToken || '');
        setShowAuthPrompt(true);
    };

    const saveAuthToken = () => {
        setControlPlaneToken(authText || null);
        setModeError(null);
        setShowAuthPrompt(false);
    };

    return (
        <>
            <header className="h-16 border-b border-white/10 bg-black/20 backdrop-blur-md flex items-center justify-between px-8 sticky top-0 z-50">
                {/* Left: Branding */}
                <div className="flex items-center gap-3">
                    <div className="w-8 h-8 bg-emerald-500 rounded-lg flex items-center justify-center shadow-lg shadow-emerald-500/20">
                        <Zap className="text-black fill-black" size={20} />
                    </div>
                    <div>
                        <h1 className="text-lg font-bold text-white leading-none tracking-tight">AI TRADER <span className="text-emerald-500">V2.0</span></h1>
                        <div className="flex items-center gap-2 text-[10px] text-gray-500 font-mono">
                            <span className="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse" />
                            SYSTEM OPERATIONAL
                        </div>
                    </div>
                </div>

                {/* Right: Controls & Mode Toggle */}
                <div className="flex items-center gap-6">
                    <button
                        type="button"
                        onClick={openAuthPrompt}
                        className={`px-3 py-1 rounded-md text-[10px] font-mono border inline-flex items-center gap-1 ${hasControlToken ? 'text-emerald-300 border-emerald-500/30 bg-emerald-500/10' : 'text-amber-300 border-amber-500/30 bg-amber-500/10'}`}
                    >
                        <KeyRound size={12} />
                        {hasControlToken ? 'CONTROL AUTH SET' : 'SET CONTROL AUTH'}
                    </button>

                    <div className={`px-3 py-1 rounded-md text-[10px] font-mono border ${isPaperMode ? 'text-blue-300 border-blue-500/30 bg-blue-500/10' : 'text-red-300 border-red-500/40 bg-red-500/10'}`}>
                        {isPaperMode ? 'PAPER TRADING' : liveOrderPostingEnabled ? 'LIVE ARMED' : 'LIVE DRY-RUN'}
                    </div>

                    <div className={`px-3 py-1 rounded-md text-[10px] font-mono border ${intelligenceGateEnabled ? 'text-emerald-300 border-emerald-500/30 bg-emerald-500/10' : 'text-gray-300 border-gray-500/30 bg-gray-500/10'}`}>
                        {intelligenceGateEnabled ? `INTEL GATE ON · ${liveSignals}` : 'INTEL GATE OFF'}
                    </div>

                    <div className={`px-3 py-1 rounded-md text-[10px] font-mono border ${pendingSettlements > 0 ? 'text-amber-300 border-amber-500/30 bg-amber-500/10' : 'text-gray-400 border-white/10 bg-white/5'}`}>
                        SETTLEMENTS {pendingSettlements}
                    </div>

                    {/* Mode Toggle Switch */}
                    <div
                        className={`
                        relative flex items-center p-1 rounded-full cursor-pointer transition-all duration-300 border
                        ${isPaperMode
                            ? 'bg-blue-500/10 border-blue-500/30'
                            : 'bg-red-500/10 border-red-500/30 shadow-[0_0_15px_rgba(239,68,68,0.3)]'
                        }
                    `}
                        onClick={toggleTradingMode}
                    >
                        <div
                            className={`
                            absolute w-8 h-8 rounded-full shadow-md transition-all duration-300 flex items-center justify-center
                            ${isPaperMode
                                ? 'bg-blue-500 left-1'
                                : 'bg-red-500 left-[calc(100%-2.25rem)]'
                            }
                        `}
                        >
                            {isPaperMode ? <Shield size={14} className="text-white" /> : <Power size={14} className="text-white" />}
                        </div>

                        <div className="flex items-center gap-8 px-4 py-1.5 font-mono text-xs font-bold">
                            <span className={`transition-opacity duration-300 ${isPaperMode ? 'opacity-100 text-blue-400' : 'opacity-30 text-gray-500'}`}>
                                PAPER
                            </span>
                            <span className={`transition-opacity duration-300 ${!isPaperMode ? 'opacity-100 text-red-500 drop-shadow-[0_0_8px_rgba(239,68,68,0.8)]' : 'opacity-30 text-gray-500'}`}>
                                LIVE
                            </span>
                        </div>
                    </div>

                    {/* Account / Wallet (Mock) */}
                    <div className="hidden md:flex items-center gap-3 ml-4 border-l border-white/10 pl-6">
                        <div className="text-right">
                            <div className="text-[10px] text-gray-400 uppercase tracking-wider">Connected Wallet</div>
                            <div className="text-xs font-mono text-white">0x71...3A92</div>
                        </div>
                        <div className="w-8 h-8 rounded-full bg-gradient-to-tr from-purple-500 to-indigo-500 border border-white/20" />
                    </div>

                    {modeError && (
                        <div className="text-xs text-red-400 font-mono">
                            {modeError}
                        </div>
                    )}
                </div>
            </header>

            {showLiveConfirm && (
                <div className="fixed inset-0 z-[100] bg-black/80 backdrop-blur-sm flex items-center justify-center p-4">
                    <div className="w-full max-w-md bg-[#111] border border-red-500/30 rounded-xl p-6 shadow-2xl">
                        <div className="flex items-center gap-2 text-red-400 mb-3">
                            <AlertTriangle size={18} />
                            <h3 className="font-bold">Enable Live Trading</h3>
                        </div>
                        <p className="text-sm text-gray-300 mb-4">
                            Live mode switches the engine to real-market conditions. It remains dry-run until posting is explicitly armed. Type <span className="font-mono text-white">LIVE</span> to confirm.
                        </p>
                        <input
                            value={confirmText}
                            onChange={(e) => setConfirmText(e.target.value)}
                            placeholder="Type LIVE"
                            className="w-full bg-black/50 border border-white/20 rounded px-3 py-2 text-sm text-white mb-4 focus:outline-none focus:border-red-500"
                        />
                        <div className="flex justify-end gap-2">
                            <button
                                onClick={() => {
                                    setShowLiveConfirm(false);
                                    setConfirmText('');
                                }}
                                className="px-3 py-2 text-sm rounded border border-white/20 text-gray-300 hover:bg-white/5"
                            >
                                Cancel
                            </button>
                            <button
                                onClick={confirmLiveMode}
                                disabled={!canConfirmLive}
                                className={`px-3 py-2 text-sm rounded font-bold ${canConfirmLive ? 'bg-red-500 text-white hover:bg-red-600' : 'bg-gray-700 text-gray-400 cursor-not-allowed'}`}
                            >
                                Confirm Live
                            </button>
                        </div>
                    </div>
                </div>
            )}

            {showAuthPrompt && (
                <div className="fixed inset-0 z-[100] bg-black/80 backdrop-blur-sm flex items-center justify-center p-4">
                    <div className="w-full max-w-md bg-[#111] border border-amber-500/30 rounded-xl p-6 shadow-2xl">
                        <div className="flex items-center gap-2 text-amber-300 mb-3">
                            <KeyRound size={18} />
                            <h3 className="font-bold">Control-Plane Auth</h3>
                        </div>
                        <p className="text-sm text-gray-300 mb-4">
                            Enter the control-plane token for privileged actions (mode toggle, risk updates, resets, strategy start/stop). Stored for this browser session only.
                        </p>
                        <input
                            value={authText}
                            onChange={(e) => setAuthText(e.target.value)}
                            placeholder="Paste CONTROL_PLANE_TOKEN"
                            className="w-full bg-black/50 border border-white/20 rounded px-3 py-2 text-sm text-white mb-4 focus:outline-none focus:border-amber-500"
                        />
                        <div className="flex justify-end gap-2">
                            <button
                                onClick={() => {
                                    setShowAuthPrompt(false);
                                    setAuthText('');
                                }}
                                className="px-3 py-2 text-sm rounded border border-white/20 text-gray-300 hover:bg-white/5"
                            >
                                Cancel
                            </button>
                            <button
                                onClick={() => {
                                    setControlPlaneToken(null);
                                    setAuthText('');
                                    setShowAuthPrompt(false);
                                }}
                                className="px-3 py-2 text-sm rounded border border-white/20 text-gray-300 hover:bg-white/5"
                            >
                                Clear
                            </button>
                            <button
                                onClick={saveAuthToken}
                                className="px-3 py-2 text-sm rounded font-bold bg-amber-500 text-black hover:bg-amber-400"
                            >
                                Save Token
                            </button>
                        </div>
                    </div>
                </div>
            )}
        </>
    );
};
