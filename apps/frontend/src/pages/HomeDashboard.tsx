import { useEffect, useMemo, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useSocket } from '../context/SocketContext';
import { DashboardLayout } from '../components/layout/DashboardLayout';
import type {
  ScannerUpdateEvent,
  ExecutionLogEvent,
  VaultUpdateEvent,
  BankrollUpdateEvent,
  HeartbeatEvent,
  RiskGuardEvent,
  TradingModeEvent,
} from '../types/socket-events';
import {
  Zap, TrendingUp, Shield, Activity, DollarSign,
  ArrowUpRight, Lock, AlertTriangle,
} from 'lucide-react';

type StrategyMetric = {
  pnl: number;
  daily_trades: number;
  status: 'active' | 'paused' | 'error';
};

type ScanSnapshot = {
  strategy: string;
  passes_threshold: boolean;
  score: number;
  timestamp: number;
  reason: string;
  meta: Record<string, any>;
};

type ExecEntry = {
  type: string;
  strategy: string;
  execution_id: string;
  question?: string;
  side?: string;
  entry_price?: number;
  size?: number;
  pnl?: number;
  timestamp: number;
};

type RiskGuardState = {
  daily_pnl: number;
  daily_target: number;
  cooldown_active: boolean;
  open_notional: number;
  drawdown_pct: number;
};

const ALL_STRATEGIES = [
  { id: 'BTC_5M', label: 'BTC 5m Engine', family: 'FAIR_VALUE', color: 'rose' },
  { id: 'BTC_15M', label: 'BTC 15m FV', family: 'FAIR_VALUE', color: 'amber' },
  { id: 'ETH_15M', label: 'ETH 15m FV', family: 'FAIR_VALUE', color: 'indigo' },
  { id: 'SOL_15M', label: 'SOL 15m FV', family: 'FAIR_VALUE', color: 'sky' },
  { id: 'CEX_SNIPER', label: 'CEX Sniper', family: 'CEX_MICROSTRUCTURE', color: 'fuchsia' },
  { id: 'OBI_SCALPER', label: 'OBI Scalper', family: 'ORDER_FLOW', color: 'pink' },
  { id: 'SYNDICATE', label: 'Syndicate', family: 'FLOW_PRESSURE', color: 'orange' },
  { id: 'AS_MARKET_MAKER', label: 'A-S Market Maker', family: 'MARKET_MAKING', color: 'cyan' },
  { id: 'LONGSHOT_BIAS', label: 'Longshot Bias Fade', family: 'BIAS_EXPLOIT', color: 'purple' },
  { id: 'MAKER_MM', label: 'Maker Micro-MM', family: 'MARKET_MAKING', color: 'lime' },
  { id: 'ATOMIC_ARB', label: 'Atomic Arb', family: 'ARBITRAGE', color: 'cyan' },
  { id: 'GRAPH_ARB', label: 'Graph Arb', family: 'ARBITRAGE', color: 'teal' },
  { id: 'CONVERGENCE_CARRY', label: 'Conv Carry', family: 'CARRY_PARITY', color: 'emerald' },
];

const FAMILY_ORDER = [
  'FAIR_VALUE',
  'CEX_MICROSTRUCTURE',
  'ORDER_FLOW',
  'FLOW_PRESSURE',
  'MARKET_MAKING',
  'ARBITRAGE',
  'CARRY_PARITY',
  'BIAS_EXPLOIT',
];
const FAMILY_LABELS: Record<string, string> = {
  FAIR_VALUE: 'Fair Value',
  CEX_MICROSTRUCTURE: 'CEX Microstructure',
  ORDER_FLOW: 'Order Flow',
  FLOW_PRESSURE: 'Flow Pressure',
  MARKET_MAKING: 'Market Making',
  ARBITRAGE: 'Arbitrage',
  CARRY_PARITY: 'Carry/Parity',
  BIAS_EXPLOIT: 'Bias Exploit',
};

function normalizeStrategyFromMarketLabel(market?: string): string | null {
  const raw = typeof market === 'string' ? market.toLowerCase() : '';
  if (!raw) return null;
  if (raw.includes('cex sniper')) return 'CEX_SNIPER';
  if (raw.includes('syndicate')) return 'SYNDICATE';
  if (raw.includes('as market maker')) return 'AS_MARKET_MAKER';
  if (raw.includes('longshot bias')) return 'LONGSHOT_BIAS';
  if (raw.includes('maker micro-mm') || raw.includes('maker mm')) return 'MAKER_MM';
  if (raw.includes('graph arb')) return 'GRAPH_ARB';
  if (raw.includes('atomic arb')) return 'ATOMIC_ARB';
  if (raw.includes('convergence carry')) return 'CONVERGENCE_CARRY';
  return null;
}

function extractExecutionStrategy(event: ExecutionLogEvent): string | null {
  const details = event.details && typeof event.details === 'object'
    ? event.details as Record<string, unknown>
    : null;
  const preflight = details?.preflight && typeof details.preflight === 'object'
    ? details.preflight as Record<string, unknown>
    : null;

  const raw = typeof event.strategy === 'string' ? event.strategy
    : typeof preflight?.strategy === 'string' ? preflight.strategy
    : typeof details?.strategy === 'string' ? details.strategy
    : normalizeStrategyFromMarketLabel(event.market);

  if (!raw) return null;
  return raw.trim().toUpperCase();
}

function parseNumber(value: unknown): number | null {
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : null;
  }
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (!trimmed) return null;
    const parsed = Number(trimmed);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

export function HomeDashboard() {
  const { socket } = useSocket();
  const navigate = useNavigate();

  const [metrics, setMetrics] = useState<Record<string, StrategyMetric>>({});
  const [statuses, setStatuses] = useState<Record<string, boolean>>({});
  const [scans, setScans] = useState<ScanSnapshot[]>([]);
  const [executions, setExecutions] = useState<ExecEntry[]>([]);
  const [vaultBalance, setVaultBalance] = useState(0);
  const [bankroll, setBankroll] = useState(1000);
  const [heartbeats, setHeartbeats] = useState<Record<string, number>>({});
  const [riskGuard, setRiskGuard] = useState<RiskGuardState>({
    daily_pnl: 0, daily_target: 300, cooldown_active: false, open_notional: 0, drawdown_pct: 0,
  });
  const [tradingMode, setTradingMode] = useState<'PAPER' | 'LIVE'>('PAPER');
  const [lastDataUpdate, setLastDataUpdate] = useState(Date.now());
  const [now, setNow] = useState(Date.now());

  useEffect(() => {
    if (!socket) return;

    const touchData = () => {
      const stamp = Date.now();
      setLastDataUpdate((prev) => (stamp - prev >= 250 ? stamp : prev));
    };

    const handleMetrics = (data: Record<string, { pnl?: number; daily_trades?: number }>) => {
      touchData();
      setMetrics((prev) => {
        const next = { ...prev };
        for (const [id, m] of Object.entries(data)) {
          next[id] = {
            pnl: typeof m.pnl === 'number' && Number.isFinite(m.pnl) ? m.pnl : (prev[id]?.pnl || 0),
            daily_trades: typeof m.daily_trades === 'number' && Number.isFinite(m.daily_trades) ? m.daily_trades : (prev[id]?.daily_trades || 0),
            status: prev[id]?.status || 'active',
          };
        }
        return next;
      });
    };

    const handleStatus = (data: Record<string, boolean>) => {
      touchData();
      setStatuses(data);
    };

    const handleScan = (data: ScannerUpdateEvent) => {
      if (!data?.strategy) return;
      touchData();
      setScans((prev) => {
        const filtered = prev.filter((s) => s.strategy !== data.strategy);
        return [{
          strategy: data.strategy,
          passes_threshold: Boolean(data.passes_threshold),
          score: Number(data.score) || 0,
          timestamp: Number(data.timestamp) || Date.now(),
          reason: data.reason || '',
          meta: data.meta || {},
        }, ...filtered].slice(0, 30);
      });
    };

    const handleExec = (data: ExecutionLogEvent) => {
      const strategy = extractExecutionStrategy(data);
      if (!strategy) return;
      touchData();

      const executionType = typeof data.type === 'string' && data.type.trim().length > 0
        ? data.type
        : typeof data.side === 'string' && data.side.trim().length > 0
          ? data.side
          : 'EXEC';
      const executionId = typeof data.execution_id === 'string' && data.execution_id.trim().length > 0
        ? data.execution_id
        : `exec-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
      const entryPrice = parseNumber(data.entry_price) ?? parseNumber(data.price) ?? undefined;
      const size = parseNumber(data.size) ?? undefined;
      const question = typeof data.question === 'string'
        ? data.question
        : typeof data.market === 'string'
          ? data.market
          : undefined;
      const pnl = parseNumber(data.pnl) ?? undefined;
      const timestamp = parseNumber(data.timestamp) ?? Date.now();

      const entry: ExecEntry = {
        type: executionType,
        strategy,
        execution_id: executionId,
        question,
        side: typeof data.side === 'string' ? data.side : undefined,
        entry_price: entryPrice,
        size,
        pnl,
        timestamp,
      };

      setExecutions((prev) => [entry, ...prev].slice(0, 20));

      const normalizedType = executionType.toUpperCase();
      const isCloseEvent = normalizedType === 'EXIT'
        || normalizedType === 'WIN'
        || normalizedType === 'LOSS'
        || normalizedType === 'SETTLEMENT'
        || normalizedType === 'CLOSE';
      if (isCloseEvent && pnl !== undefined) {
        setMetrics((prev) => {
          const s = prev[strategy] || { pnl: 0, daily_trades: 0, status: 'active' };
          return { ...prev, [strategy]: { ...s, pnl: s.pnl + pnl, daily_trades: s.daily_trades + 1 } };
        });
      }
    };

    const handleVault = (data: VaultUpdateEvent) => {
      if (typeof data?.vault === 'number') {
        touchData();
        setVaultBalance(data.vault);
      }
    };

    const handleBankroll = (data: BankrollUpdateEvent) => {
      touchData();
      if (typeof data?.available_cash === 'number') setBankroll(data.available_cash);
      if (typeof data?.equity === 'number') setBankroll(data.equity);
      const reserved = typeof data?.reserved === 'number' ? data.reserved : null;
      if (reserved !== null) {
        setRiskGuard((prev) => ({ ...prev, open_notional: reserved }));
      }
    };

    const handleHeartbeat = (data: HeartbeatEvent) => {
      const heartbeatId = typeof data?.id === 'string' && data.id.trim().length > 0 ? data.id : null;
      if (heartbeatId) {
        touchData();
        setHeartbeats((prev) => ({ ...prev, [heartbeatId]: Date.now() }));
      }
    };

    const handleRiskGuard = (data: RiskGuardEvent) => {
      if (!data) return;
      touchData();
      // Backend sends RiskGuardState: { strategy, cumPnl, peakPnl, consecutiveLosses, pausedUntil, ... }
      const cumPnl = typeof data.cumPnl === 'number' ? data.cumPnl : undefined;
      const peakPnl = typeof data.peakPnl === 'number' ? data.peakPnl : 0;
      const drawdown = cumPnl !== undefined && peakPnl > 0 ? ((cumPnl - peakPnl) / peakPnl) * 100 : 0;
      const cooldown = typeof data.pausedUntil === 'number' && data.pausedUntil > Date.now();
      setRiskGuard((prev) => ({
        daily_pnl: cumPnl !== undefined ? cumPnl : prev.daily_pnl,
        daily_target: prev.daily_target,
        cooldown_active: cooldown,
        open_notional: prev.open_notional,
        drawdown_pct: cumPnl !== undefined ? drawdown : prev.drawdown_pct,
      }));
    };

    const handleMode = (data: TradingModeEvent) => {
      if (data?.mode === 'PAPER' || data?.mode === 'LIVE') {
        touchData();
        setTradingMode(data.mode);
      }
    };
    const handleSimulationReset = (payload: { bankroll?: number }) => {
      touchData();
      const resetBankroll = parseNumber(payload?.bankroll) ?? 1000;
      setMetrics({});
      setScans([]);
      setExecutions([]);
      setBankroll(resetBankroll);
      setVaultBalance(0);
      setRiskGuard((prev) => ({
        ...prev,
        daily_pnl: 0,
        cooldown_active: false,
        open_notional: 0,
        drawdown_pct: 0,
      }));
    };

    // Request all initial state on connect/reconnect
    const requestAllState = () => {
      socket.emit('request_vault');
      socket.emit('request_trading_mode');
      socket.emit('request_strategy_metrics');
      socket.emit('request_risk_guard');
      socket.emit('request_ledger');
      socket.emit('request_risk_config');
    };

    socket.on('strategy_metrics_update', handleMetrics);
    socket.on('strategy_status_update', handleStatus);
    socket.on('scanner_update', handleScan);
    socket.on('execution_log', handleExec);
    socket.on('vault_update', handleVault);
    socket.on('sim_ledger_update', handleBankroll);
    socket.on('heartbeat', handleHeartbeat);
    socket.on('risk_guard_update', handleRiskGuard);
    socket.on('trading_mode_update', handleMode);
    socket.on('simulation_reset', handleSimulationReset);
    socket.on('connect', requestAllState);

    // Initial request
    requestAllState();

    return () => {
      socket.off('strategy_metrics_update', handleMetrics);
      socket.off('strategy_status_update', handleStatus);
      socket.off('scanner_update', handleScan);
      socket.off('execution_log', handleExec);
      socket.off('vault_update', handleVault);
      socket.off('sim_ledger_update', handleBankroll);
      socket.off('heartbeat', handleHeartbeat);
      socket.off('risk_guard_update', handleRiskGuard);
      socket.off('trading_mode_update', handleMode);
      socket.off('simulation_reset', handleSimulationReset);
      socket.off('connect', requestAllState);
    };
  }, [socket]);

  // Timer to re-render for staleness check
  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), 5000);
    return () => window.clearInterval(timer);
  }, []);

  // Computed stats
  const totalPnl = useMemo(() => Object.values(metrics).reduce((sum, m) => sum + m.pnl, 0), [metrics]);
  const totalTrades = useMemo(() => Object.values(metrics).reduce((sum, m) => sum + m.daily_trades, 0), [metrics]);
  const activeStrategies = useMemo(
    () => ALL_STRATEGIES.filter((s) => statuses[s.id] !== false).length,
    [statuses],
  );
  const liveScans = useMemo(() => scans.filter((s) => s.passes_threshold).length, [scans]);
  const netWorth = bankroll + vaultBalance;

  // Group strategies by family
  const familyGroups = useMemo(() => {
    const groups: Record<string, typeof ALL_STRATEGIES> = {};
    for (const s of ALL_STRATEGIES) {
      if (!groups[s.family]) groups[s.family] = [];
      groups[s.family].push(s);
    }
    return groups;
  }, []);

  const familyPnl = useMemo(() => {
    const fp: Record<string, number> = {};
    for (const s of ALL_STRATEGIES) {
      fp[s.family] = (fp[s.family] || 0) + (metrics[s.id]?.pnl || 0);
    }
    return fp;
  }, [metrics]);

  return (
    <DashboardLayout>
      <div className="space-y-6">
        {/* ── Hero Stats Bar ── */}
        <div className="grid grid-cols-6 gap-3">
          <StatCard icon={<DollarSign size={16} />} label="Net Worth" value={`$${netWorth.toFixed(2)}`}
            accent={netWorth >= 1000 ? 'emerald' : 'red'} />
          <StatCard icon={<TrendingUp size={16} />} label="Total P&L" value={`${totalPnl >= 0 ? '+' : ''}$${totalPnl.toFixed(2)}`}
            accent={totalPnl >= 0 ? 'emerald' : 'red'} />
          <StatCard icon={<Activity size={16} />} label="Trades Today" value={String(totalTrades)} accent="white" />
          <StatCard icon={<Zap size={16} />} label="Active Strategies" value={`${activeStrategies}/${ALL_STRATEGIES.length}`} accent="cyan" />
          <StatCard icon={<Lock size={16} />} label="Vault" value={`$${Math.round(vaultBalance)}`} accent="amber" />
          <StatCard icon={<Shield size={16} />} label="Mode" value={tradingMode}
            accent={tradingMode === 'LIVE' ? 'red' : 'blue'} />
        </div>

        {now - lastDataUpdate > 30000 && (
          <span className="text-yellow-400 text-xs animate-pulse">
            DATA STALE ({Math.round((now - lastDataUpdate) / 1000)}s)
          </span>
        )}

        {/* ── Risk Guard Strip ── */}
        <div className={`flex items-center gap-6 px-4 py-2 rounded-lg border text-[10px] font-mono ${
          riskGuard.cooldown_active ? 'border-red-500/30 bg-red-500/10' : 'border-white/10 bg-white/5'
        }`}>
          <span className="text-gray-400 uppercase">Risk Guard</span>
          <span className="text-white">Daily P&L: <b className={riskGuard.daily_pnl >= 0 ? 'text-emerald-400' : 'text-red-400'}>${riskGuard.daily_pnl.toFixed(2)}</b> / ${riskGuard.daily_target}</span>
          <span className="text-white">Open: <b className="text-cyan-300">${riskGuard.open_notional.toFixed(0)}</b></span>
          <span className="text-white">DD: <b className={riskGuard.drawdown_pct > -5 ? 'text-emerald-400' : 'text-red-400'}>{riskGuard.drawdown_pct.toFixed(1)}%</b></span>
          {riskGuard.cooldown_active && (
            <span className="flex items-center gap-1 text-red-400"><AlertTriangle size={12} /> COOLDOWN ACTIVE</span>
          )}
          <span className="text-gray-400">Bankroll: ${bankroll.toFixed(2)}</span>
        </div>

        <div className="grid grid-cols-12 gap-4">
          {/* ── Strategy P&L Grid ── */}
          <div className="col-span-8 space-y-3">
            <h3 className="text-xs font-bold text-gray-400 uppercase">Strategy Performance</h3>
            <div className="grid grid-cols-2 gap-3">
              {FAMILY_ORDER.map((family) => {
                const strats = familyGroups[family];
                if (!strats) return null;
                const fp = familyPnl[family] || 0;
                return (
                  <div key={family} className="bg-white/5 border border-white/10 rounded-xl p-4">
                    <div className="flex items-center justify-between mb-3">
                      <span className="text-[10px] font-mono text-gray-400 uppercase">{FAMILY_LABELS[family]}</span>
                      <span className={`text-xs font-mono font-bold ${fp >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                        {fp >= 0 ? '+' : ''}${fp.toFixed(2)}
                      </span>
                    </div>
                    <div className="space-y-2">
                      {strats.map((s) => {
                        const m = metrics[s.id];
                        const pnl = m?.pnl || 0;
                        const trades = m?.daily_trades || 0;
                        const isActive = statuses[s.id] !== false;
                        const lastHeartbeat = heartbeats[s.id];
                        const alive = typeof lastHeartbeat === 'number' && now - lastHeartbeat < 15000;
                        return (
                          <div key={s.id} className="flex items-center gap-2 text-[10px] font-mono">
                            <div className={`w-2 h-2 rounded-full ${alive ? 'bg-emerald-500 animate-pulse' : isActive ? 'bg-gray-500' : 'bg-red-500/50'}`} />
                            <span className="w-32 text-gray-300 truncate">{s.label}</span>
                            <div className="flex-1 h-1.5 bg-white/5 rounded overflow-hidden">
                              {pnl !== 0 && (
                                <div
                                  className={`h-full rounded ${pnl > 0 ? 'bg-emerald-500/60' : 'bg-red-500/60'}`}
                                  style={{ width: `${Math.min(100, (Math.abs(pnl) / Math.max(Math.abs(totalPnl), 1)) * 100)}%` }}
                                />
                              )}
                            </div>
                            <span className="w-10 text-right text-gray-500">{trades}t</span>
                            <span className={`w-16 text-right font-bold ${pnl >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                              ${pnl.toFixed(2)}
                            </span>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                );
              })}
            </div>
          </div>

          {/* ── Right Column: Live Scans + Quick Links ── */}
          <div className="col-span-4 space-y-4">
            {/* Active Signals */}
            <div className="bg-white/5 border border-white/10 rounded-xl p-4">
              <h3 className="text-xs font-bold text-gray-400 uppercase mb-3">
                Live Signals <span className="text-emerald-400">{liveScans} active</span>
              </h3>
              <div className="space-y-1 max-h-[200px] overflow-y-auto">
                {scans.filter((s) => s.passes_threshold).length === 0 ? (
                  <div className="text-[10px] text-gray-600 text-center py-4">No active signals</div>
                ) : (
                  scans.filter((s) => s.passes_threshold).slice(0, 8).map((s, i) => (
                    <div key={`${s.strategy}-${i}`} className="flex items-center gap-2 text-[10px] font-mono px-2 py-1 bg-emerald-500/10 rounded border border-emerald-500/20">
                      <span className="w-2 h-2 rounded-full bg-emerald-500 animate-pulse" />
                      <span className="flex-1 text-gray-300 truncate">{s.strategy}</span>
                      <span className="text-emerald-400">{s.score.toFixed(1)}bp</span>
                    </div>
                  ))
                )}
              </div>
            </div>

            {/* Quick Navigation */}
            <div className="bg-white/5 border border-white/10 rounded-xl p-4">
              <h3 className="text-xs font-bold text-gray-400 uppercase mb-3">Quick Access</h3>
              <div className="space-y-2">
                {[
                  { label: 'BTC 5m Engine', path: '/btc-5m-engine', icon: <Zap size={14} />, desc: 'Primary profit engine' },
                  { label: 'HFT Strategies', path: '/hft', icon: <Activity size={14} />, desc: 'A-S MM, Longshot, Arb' },
                  { label: 'Polymarket', path: '/polymarket', icon: <TrendingUp size={14} />, desc: 'Market scanner' },
                  { label: 'Strategy Coverage', path: '/strategies', icon: <ArrowUpRight size={14} />, desc: 'Per-strategy execution + pnl' },
                ].map((link) => (
                  <button
                    key={link.path}
                    onClick={() => navigate(link.path)}
                    className="w-full flex items-center gap-3 px-3 py-2.5 bg-white/5 hover:bg-white/10 border border-white/10 rounded-lg transition group text-left"
                  >
                    <div className="text-gray-400 group-hover:text-emerald-400 transition">{link.icon}</div>
                    <div>
                      <div className="text-xs font-bold text-white group-hover:text-emerald-300 transition">{link.label}</div>
                      <div className="text-[9px] text-gray-500">{link.desc}</div>
                    </div>
                    <ArrowUpRight size={14} className="ml-auto text-gray-600 group-hover:text-emerald-400 transition" />
                  </button>
                ))}
              </div>
            </div>
          </div>
        </div>

        {/* ── Recent Executions ── */}
        <div className="bg-white/5 border border-white/10 rounded-xl p-4">
          <h3 className="text-xs font-bold text-gray-400 uppercase mb-3">
            Recent Executions <span className="text-cyan-400">({executions.length})</span>
          </h3>
          <div className="overflow-x-auto">
            <table className="w-full text-[10px] font-mono">
              <thead>
                <tr className="text-gray-500 border-b border-white/10">
                  <th className="text-left py-1 px-2">Time</th>
                  <th className="text-left py-1 px-2">Type</th>
                  <th className="text-left py-1 px-2">Strategy</th>
                  <th className="text-left py-1 px-2">Market</th>
                  <th className="text-left py-1 px-2">Side</th>
                  <th className="text-right py-1 px-2">Price</th>
                  <th className="text-right py-1 px-2">Size</th>
                  <th className="text-right py-1 px-2">P&L</th>
                </tr>
              </thead>
              <tbody>
                {executions.length === 0 ? (
                  <tr><td colSpan={8} className="text-center py-6 text-gray-600">No executions yet — strategies scanning...</td></tr>
                ) : (
                  executions.slice(0, 12).map((e, i) => (
                    <tr key={`${e.execution_id}-${i}`} className="border-b border-white/5 hover:bg-white/5">
                      <td className="py-1.5 px-2 text-gray-400">
                        {new Date(e.timestamp).toLocaleTimeString('en-US', { hour12: false })}
                      </td>
                      <td className={`py-1.5 px-2 font-bold ${e.type === 'ENTRY' ? 'text-cyan-400' : 'text-amber-400'}`}>
                        {e.type}
                      </td>
                      <td className="py-1.5 px-2 text-gray-300">
                        {ALL_STRATEGIES.find((s) => s.id === e.strategy)?.label || e.strategy}
                      </td>
                      <td className="py-1.5 px-2 text-gray-400 truncate max-w-[200px]">
                        {e.question || '--'}
                      </td>
                      <td className="py-1.5 px-2 text-white">{e.side || '--'}</td>
                      <td className="py-1.5 px-2 text-right text-white">
                        {e.entry_price ? `${(Number(e.entry_price) * 100).toFixed(1)}c` : '--'}
                      </td>
                      <td className="py-1.5 px-2 text-right text-gray-300">
                        {e.size ? `$${Number(e.size).toFixed(2)}` : '--'}
                      </td>
                      <td className={`py-1.5 px-2 text-right font-bold ${
                        e.pnl != null ? (Number(e.pnl) >= 0 ? 'text-emerald-400' : 'text-red-400') : 'text-gray-600'
                      }`}>
                        {e.pnl != null ? `$${Number(e.pnl).toFixed(2)}` : '--'}
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </DashboardLayout>
  );
}

// ── Reusable stat card ──
function StatCard({ icon, label, value, accent }: { icon: React.ReactNode; label: string; value: string; accent: string }) {
  const colorMap: Record<string, string> = {
    emerald: 'border-emerald-500/30 bg-emerald-500/10 text-emerald-400',
    red: 'border-red-500/30 bg-red-500/10 text-red-400',
    amber: 'border-amber-500/30 bg-amber-500/10 text-amber-300',
    cyan: 'border-cyan-500/30 bg-cyan-500/10 text-cyan-400',
    blue: 'border-blue-500/30 bg-blue-500/10 text-blue-300',
    white: 'border-white/10 bg-white/5 text-white',
  };
  const cls = colorMap[accent] || colorMap.white;
  return (
    <div className={`px-4 py-3 rounded-lg border ${cls}`}>
      <div className="flex items-center gap-1.5 text-[10px] text-gray-400 uppercase mb-1">
        {icon} {label}
      </div>
      <div className="text-lg font-mono font-bold">{value}</div>
    </div>
  );
}
