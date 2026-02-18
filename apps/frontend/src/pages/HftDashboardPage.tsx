import { useEffect, useMemo, useRef, useState } from 'react';
import { useSocket } from '../context/SocketContext';
import { DashboardLayout } from '../components/layout/DashboardLayout';
import type {
  ScannerUpdateEvent,
  ExecutionLogEvent,
  VaultUpdateEvent,
  BankrollUpdateEvent,
  StrategyMetricsEvent,
} from '../types/socket-events';

type ScanEntry = {
  strategy: string;
  market_id: string;
  symbol: string;
  score: number;
  threshold: number;
  passes_threshold: boolean;
  reason: string;
  timestamp: number;
  meta: Record<string, any>;
};

type ExecEntry = {
  type: string;
  strategy: string;
  execution_id: string;
  market_id?: string;
  question?: string;
  side?: string;
  entry_price?: number;
  exit_price?: number;
  size?: number;
  pnl?: number;
  net_edge_bps?: number;
  bias_edge_pct?: number;
  net_edge_pct?: number;
  timestamp: number;
  [key: string]: any;
};

type StrategyStats = {
  pnl: number;
  trades: number;
  wins: number;
  active_scans: number;
  last_scan_ts: number;
  positions: number;
};

const HFT_STRATEGIES = [
  'AS_MARKET_MAKER',
  'LONGSHOT_BIAS',
  'MAKER_MM',
  'ATOMIC_ARB',
  'GRAPH_ARB',
  'CONVERGENCE_CARRY',
];

const STRATEGY_LABELS: Record<string, string> = {
  AS_MARKET_MAKER: 'A-S Market Maker',
  LONGSHOT_BIAS: 'Longshot Bias Fade',
  MAKER_MM: 'Maker Micro-MM',
  ATOMIC_ARB: 'Atomic Arbitrage',
  GRAPH_ARB: 'Graph Arbitrage',
  CONVERGENCE_CARRY: 'Convergence Carry',
};

const STRATEGY_FAMILIES: Record<string, string> = {
  AS_MARKET_MAKER: 'MARKET_MAKING',
  LONGSHOT_BIAS: 'BIAS_EXPLOITATION',
  MAKER_MM: 'MARKET_MAKING',
  ATOMIC_ARB: 'ARBITRAGE',
  GRAPH_ARB: 'ARBITRAGE',
  CONVERGENCE_CARRY: 'CARRY_PARITY',
};

const FAMILY_COLORS: Record<string, string> = {
  MARKET_MAKING: 'text-cyan-400 border-cyan-500/30 bg-cyan-500/10',
  BIAS_EXPLOITATION: 'text-purple-400 border-purple-500/30 bg-purple-500/10',
  ARBITRAGE: 'text-amber-400 border-amber-500/30 bg-amber-500/10',
  CARRY_PARITY: 'text-emerald-400 border-emerald-500/30 bg-emerald-500/10',
};

const MAX_SCAN_ROWS = 50;
const MAX_EXEC_ROWS = 30;

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

function normalizeStrategyFromMarketLabel(market?: string): string | null {
  const raw = typeof market === 'string' ? market.toLowerCase() : '';
  if (!raw) return null;
  if (raw.includes('as market maker')) return 'AS_MARKET_MAKER';
  if (raw.includes('longshot bias')) return 'LONGSHOT_BIAS';
  if (raw.includes('maker micro-mm') || raw.includes('maker mm')) return 'MAKER_MM';
  if (raw.includes('atomic arb')) return 'ATOMIC_ARB';
  if (raw.includes('graph arb')) return 'GRAPH_ARB';
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

export function HftDashboardPage() {
  const { socket } = useSocket();
  const [scans, setScans] = useState<ScanEntry[]>([]);
  const [executions, setExecutions] = useState<ExecEntry[]>([]);
  const [stats, setStats] = useState<Record<string, StrategyStats>>({});
  const [feeCurve, setFeeCurve] = useState<{ rate: number; prices: number[] } | null>(null);
  const [vaultBalance, setVaultBalance] = useState(0);
  const [_bankroll, setBankroll] = useState(1000);
  const [now, setNow] = useState(Date.now());
  const [lastDataUpdate, setLastDataUpdate] = useState(Date.now());
  const statsRef = useRef(stats);
  statsRef.current = stats;

  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), 5000);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    if (!socket) return;

    const touchData = () => {
      const stamp = Date.now();
      setLastDataUpdate((prev) => (stamp - prev >= 250 ? stamp : prev));
    };

    const handleScan = (data: ScannerUpdateEvent) => {
      if (!data?.strategy || !HFT_STRATEGIES.includes(data.strategy)) return;
      touchData();
      const entry: ScanEntry = {
        strategy: data.strategy,
        market_id: data.market_id || '',
        symbol: data.symbol || data.meta?.question || '',
        score: Number(data.score) || 0,
        threshold: Number(data.threshold) || 0,
        passes_threshold: Boolean(data.passes_threshold),
        reason: data.reason || '',
        timestamp: Number(data.timestamp) || Date.now(),
        meta: data.meta || {},
      };

      // Extract fee curve info from BTC_5M-style scans
      if (entry.meta.fee_curve_base_rate) {
        setFeeCurve({
          rate: Number(entry.meta.fee_curve_base_rate),
          prices: [0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90],
        });
      }

      setScans((prev) => [entry, ...prev].slice(0, MAX_SCAN_ROWS));

      // Update strategy stats
      setStats((prev) => {
        const s = prev[entry.strategy] || { pnl: 0, trades: 0, wins: 0, active_scans: 0, last_scan_ts: 0, positions: 0 };
        return {
          ...prev,
          [entry.strategy]: {
            ...s,
            active_scans: parseNumber(entry.meta.active_scans) ?? (entry.passes_threshold ? 1 : 0),
            last_scan_ts: entry.timestamp,
            positions: parseNumber(entry.meta.positions_count) ?? s.positions,
          },
        };
      });
    };

    const handleExec = (data: ExecutionLogEvent) => {
      const strategy = extractExecutionStrategy(data);
      if (!strategy || !HFT_STRATEGIES.includes(strategy)) return;
      touchData();

      const executionType = typeof data.type === 'string' && data.type.trim().length > 0
        ? data.type
        : typeof data.side === 'string' && data.side.trim().length > 0
          ? data.side
          : 'EXEC';
      const executionId = typeof data.execution_id === 'string' && data.execution_id.trim().length > 0
        ? data.execution_id
        : `exec-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
      const pnl = parseNumber(data.pnl) ?? undefined;
      const entry: ExecEntry = {
        market_id: typeof data.market_id === 'string' ? data.market_id : undefined,
        question: typeof data.question === 'string' ? data.question : undefined,
        side: typeof data.side === 'string' ? data.side : undefined,
        entry_side: typeof data.entry_side === 'string' ? data.entry_side : undefined,
        entry_price: parseNumber(data.entry_price) ?? parseNumber(data.price) ?? undefined,
        exit_price: parseNumber(data.exit_price) ?? undefined,
        size: parseNumber(data.size) ?? undefined,
        pnl,
        net_edge_bps: parseNumber(data.net_edge_bps) ?? undefined,
        bias_edge_pct: parseNumber(data.bias_edge_pct) ?? undefined,
        net_edge_pct: parseNumber(data.net_edge_pct) ?? undefined,
        strategy,
        type: executionType,
        execution_id: executionId,
        timestamp: parseNumber(data.timestamp) ?? Date.now(),
      };
      setExecutions((prev) => [entry, ...prev].slice(0, MAX_EXEC_ROWS));

      const normalizedType = executionType.toUpperCase();
      const isCloseEvent = normalizedType === 'EXIT'
        || normalizedType === 'WIN'
        || normalizedType === 'LOSS'
        || normalizedType === 'SETTLEMENT'
        || normalizedType === 'CLOSE';
      if (isCloseEvent && pnl !== undefined) {
        setStats((prev) => {
          const s = prev[strategy] || { pnl: 0, trades: 0, wins: 0, active_scans: 0, last_scan_ts: 0, positions: 0 };
          return {
            ...prev,
            [strategy]: {
              ...s,
              pnl: s.pnl + pnl,
              trades: s.trades + 1,
              wins: pnl > 0 ? s.wins + 1 : s.wins,
            },
          };
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
      if (typeof data?.available_cash === 'number') {
        touchData();
        setBankroll(data.available_cash);
      }
    };

    const handleMetrics = (data: StrategyMetricsEvent) => {
      if (!data) return;
      touchData();
      setStats((prev) => {
        const next = { ...prev };
        for (const id of HFT_STRATEGIES) {
          const m = data[id];
          if (!m) continue;
          next[id] = {
            pnl: typeof m.pnl === 'number' ? m.pnl : (prev[id]?.pnl || 0),
            trades: typeof m.daily_trades === 'number' ? m.daily_trades : (prev[id]?.trades || 0),
            wins: prev[id]?.wins || 0,
            active_scans: prev[id]?.active_scans || 0,
            last_scan_ts: prev[id]?.last_scan_ts || 0,
            positions: prev[id]?.positions || 0,
          };
        }
        return next;
      });
    };
    const handleSimulationReset = (payload: { bankroll?: number }) => {
      touchData();
      const resetBankroll = parseNumber(payload?.bankroll) ?? 1000;
      setScans([]);
      setExecutions([]);
      setStats({});
      setFeeCurve(null);
      setVaultBalance(0);
      setBankroll(resetBankroll);
    };

    const requestAllState = () => {
      socket.emit('request_vault');
      socket.emit('request_strategy_metrics');
      socket.emit('request_ledger');
    };

    socket.on('scanner_update', handleScan);
    socket.on('execution_log', handleExec);
    socket.on('vault_update', handleVault);
    socket.on('sim_ledger_update', handleBankroll);
    socket.on('strategy_metrics_update', handleMetrics);
    socket.on('simulation_reset', handleSimulationReset);
    socket.on('connect', requestAllState);

    requestAllState();

    return () => {
      socket.off('scanner_update', handleScan);
      socket.off('execution_log', handleExec);
      socket.off('vault_update', handleVault);
      socket.off('sim_ledger_update', handleBankroll);
      socket.off('strategy_metrics_update', handleMetrics);
      socket.off('simulation_reset', handleSimulationReset);
      socket.off('connect', requestAllState);
    };
  }, [socket]);

  const totalPnl = useMemo(() => Object.values(stats).reduce((sum, s) => sum + s.pnl, 0), [stats]);
  const totalTrades = useMemo(() => Object.values(stats).reduce((sum, s) => sum + s.trades, 0), [stats]);
  const totalWins = useMemo(() => Object.values(stats).reduce((sum, s) => sum + s.wins, 0), [stats]);
  const winRate = totalTrades > 0 ? ((totalWins / totalTrades) * 100).toFixed(1) : '--';

  // Fee curve visualization data
  const feeCurveData = useMemo(() => {
    const rate = feeCurve?.rate || 0.02;
    return [0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50].map((p) => ({
      price: p,
      fee_bps: p * (1 - p) * rate * 10000,
    }));
  }, [feeCurve]);

  const maxFeeBps = Math.max(...feeCurveData.map((d) => d.fee_bps));

  return (
    <DashboardLayout>
      <div className="space-y-6">
        {/* Header Stats */}
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-2xl font-bold text-white tracking-tight">HFT Strategies Dashboard</h2>
            <p className="text-xs text-gray-500 font-mono mt-1">
              A-S Market Maker + Longshot Bias Fade + Maker Micro-MM + Arbitrage
            </p>
            {now - lastDataUpdate > 30000 && (
              <div className="text-yellow-400 text-[10px] font-mono mt-1">
                DATA STALE ({Math.round((now - lastDataUpdate) / 1000)}s)
              </div>
            )}
          </div>
          <div className="flex gap-3">
            <div className={`px-4 py-2 rounded-lg border ${totalPnl >= 0 ? 'border-emerald-500/30 bg-emerald-500/10' : 'border-red-500/30 bg-red-500/10'}`}>
              <div className="text-[10px] text-gray-400 uppercase">HFT P&L</div>
              <div className={`text-lg font-mono font-bold ${totalPnl >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                ${totalPnl.toFixed(2)}
              </div>
            </div>
            <div className="px-4 py-2 rounded-lg border border-white/10 bg-white/5">
              <div className="text-[10px] text-gray-400 uppercase">Trades</div>
              <div className="text-lg font-mono font-bold text-white">{totalTrades}</div>
            </div>
            <div className="px-4 py-2 rounded-lg border border-white/10 bg-white/5">
              <div className="text-[10px] text-gray-400 uppercase">Win Rate</div>
              <div className="text-lg font-mono font-bold text-white">{winRate}%</div>
            </div>
            <div className="px-4 py-2 rounded-lg border border-amber-500/30 bg-amber-500/10">
              <div className="text-[10px] text-gray-400 uppercase">Vault</div>
              <div className="text-lg font-mono font-bold text-amber-300">${Math.round(vaultBalance)}</div>
            </div>
          </div>
        </div>

        {/* Strategy Cards Grid */}
        <div className="grid grid-cols-3 gap-4">
          {HFT_STRATEGIES.map((id) => {
            const s = stats[id] || { pnl: 0, trades: 0, wins: 0, active_scans: 0, last_scan_ts: 0, positions: 0 };
            const family = STRATEGY_FAMILIES[id] || 'GENERIC';
            const colorClass = FAMILY_COLORS[family] || 'text-gray-400 border-white/10 bg-white/5';
            const wr = s.trades > 0 ? ((s.wins / s.trades) * 100).toFixed(0) : '--';
            const alive = now - s.last_scan_ts < 10000;
            return (
              <div key={id} className="bg-white/5 border border-white/10 rounded-xl p-4 space-y-3">
                <div className="flex items-center justify-between">
                  <div>
                    <div className="text-sm font-bold text-white">{STRATEGY_LABELS[id] || id}</div>
                    <span className={`inline-block text-[9px] font-mono px-1.5 py-0.5 rounded border mt-1 ${colorClass}`}>
                      {family}
                    </span>
                  </div>
                  <div className={`w-2.5 h-2.5 rounded-full ${alive ? 'bg-emerald-500 animate-pulse' : 'bg-gray-600'}`} />
                </div>
                <div className="grid grid-cols-4 gap-2 text-center">
                  <div>
                    <div className="text-[9px] text-gray-500 uppercase">P&L</div>
                    <div className={`text-sm font-mono font-bold ${s.pnl >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                      ${s.pnl.toFixed(2)}
                    </div>
                  </div>
                  <div>
                    <div className="text-[9px] text-gray-500 uppercase">Trades</div>
                    <div className="text-sm font-mono text-white">{s.trades}</div>
                  </div>
                  <div>
                    <div className="text-[9px] text-gray-500 uppercase">WR</div>
                    <div className="text-sm font-mono text-white">{wr}%</div>
                  </div>
                  <div>
                    <div className="text-[9px] text-gray-500 uppercase">Open</div>
                    <div className="text-sm font-mono text-cyan-300">{s.positions}</div>
                  </div>
                </div>
              </div>
            );
          })}
        </div>

        <div className="grid grid-cols-12 gap-4">
          {/* Fee Curve Visualization */}
          <div className="col-span-4 bg-white/5 border border-white/10 rounded-xl p-4">
            <h3 className="text-xs font-bold text-gray-400 uppercase mb-3">Polymarket Fee Curve</h3>
            <p className="text-[10px] text-gray-500 mb-3">fee(p) = p * (1-p) * {((feeCurve?.rate || 0.02) * 100).toFixed(1)}%</p>
            <div className="space-y-1.5">
              {feeCurveData.map((d) => (
                <div key={d.price} className="flex items-center gap-2 text-[10px] font-mono">
                  <span className="w-8 text-gray-400 text-right">{(d.price * 100).toFixed(0)}c</span>
                  <div className="flex-1 h-4 bg-white/5 rounded overflow-hidden">
                    <div
                      className="h-full bg-gradient-to-r from-emerald-600 to-cyan-500 rounded"
                      style={{ width: `${(d.fee_bps / maxFeeBps) * 100}%` }}
                    />
                  </div>
                  <span className="w-12 text-cyan-300 text-right">{d.fee_bps.toFixed(1)}bp</span>
                </div>
              ))}
            </div>
            <div className="mt-3 text-[9px] text-gray-600">
              Max fee at 50c = {maxFeeBps.toFixed(1)} bps | Near 0 at extremes
            </div>
          </div>

          {/* Live Scan Feed */}
          <div className="col-span-8 bg-white/5 border border-white/10 rounded-xl p-4">
            <h3 className="text-xs font-bold text-gray-400 uppercase mb-3">
              Live Scan Feed <span className="text-emerald-400">({scans.filter((s) => s.passes_threshold).length} active)</span>
            </h3>
            <div className="overflow-y-auto max-h-[320px] space-y-1">
              {scans.length === 0 ? (
                <div className="text-xs text-gray-600 text-center py-8">Waiting for scan data...</div>
              ) : (
                scans.slice(0, 20).map((s, i) => (
                  <div
                    key={`${s.strategy}-${s.market_id}-${s.timestamp}-${i}`}
                    className={`flex items-center gap-2 text-[10px] font-mono px-2 py-1 rounded ${
                      s.passes_threshold ? 'bg-emerald-500/10 border border-emerald-500/20' : 'bg-white/5'
                    }`}
                  >
                    <span className={`w-2 h-2 rounded-full ${s.passes_threshold ? 'bg-emerald-500' : 'bg-gray-600'}`} />
                    <span className="w-28 text-gray-400 truncate">{STRATEGY_LABELS[s.strategy] || s.strategy}</span>
                    <span className="flex-1 text-gray-300 truncate">{s.symbol || s.market_id.slice(0, 12)}</span>
                    <span className={`w-16 text-right ${s.score > 0 ? 'text-emerald-400' : 'text-gray-500'}`}>
                      {s.score.toFixed(1)}bp
                    </span>
                    <span className="w-12 text-right text-gray-500">{s.threshold.toFixed(1)}bp</span>
                    <span className="w-16 text-gray-600">
                      {new Date(s.timestamp).toLocaleTimeString('en-US', { hour12: false })}
                    </span>
                  </div>
                ))
              )}
            </div>
          </div>
        </div>

        {/* Execution Log */}
        <div className="bg-white/5 border border-white/10 rounded-xl p-4">
          <h3 className="text-xs font-bold text-gray-400 uppercase mb-3">
            Execution Log <span className="text-cyan-400">({executions.length})</span>
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
                  <tr>
                    <td colSpan={8} className="text-center py-6 text-gray-600">
                      No executions yet
                    </td>
                  </tr>
                ) : (
                  executions.slice(0, 15).map((e, i) => (
                    <tr key={`${e.execution_id}-${i}`} className="border-b border-white/5 hover:bg-white/5">
                      <td className="py-1.5 px-2 text-gray-400">
                        {new Date(e.timestamp).toLocaleTimeString('en-US', { hour12: false })}
                      </td>
                      <td className={`py-1.5 px-2 font-bold ${e.type === 'ENTRY' ? 'text-cyan-400' : 'text-amber-400'}`}>
                        {e.type}
                      </td>
                      <td className="py-1.5 px-2 text-gray-300">{STRATEGY_LABELS[e.strategy] || e.strategy}</td>
                      <td className="py-1.5 px-2 text-gray-400 truncate max-w-[200px]">
                        {e.question || e.market_id?.slice(0, 16)}
                      </td>
                      <td className="py-1.5 px-2 text-white">{e.side || e.entry_side || '--'}</td>
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
