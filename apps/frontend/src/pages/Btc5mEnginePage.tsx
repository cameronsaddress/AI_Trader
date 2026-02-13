import React, { useEffect, useMemo, useRef, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useSocket } from '../context/SocketContext';
import { ExecutionLayout } from '../components/layout/ExecutionLayout';

type TradingMode = 'PAPER' | 'LIVE';

type ExecutionLog = {
  execution_id?: string;
  timestamp: number;
  side?: string;
  market?: string;
  price?: unknown;
  size?: unknown;
  mode?: string;
  details?: any;
};

type StrategyPnlPayload = {
  execution_id?: string;
  strategy?: string;
  pnl?: number;
  notional?: number;
  timestamp?: number;
  details?: any;
};

type StrategyMetrics = Record<string, { pnl?: number; daily_trades?: number; updated_at?: number }>;

type TradeStatus = 'OPEN' | 'RESOLVED' | 'STOPPED';

type TradeRecord = {
  execution_id: string;
  direction: 'UP' | 'DOWN' | null;
  window_label: string;
  entry_ts: number;
  entry_price: number | null;
  notional: number;
  exit_ts: number | null;
  pnl: number | null;
  status: TradeStatus;
};

type ExecutionTraceEvent = {
  type: string;
  timestamp: number;
  payload: unknown;
};

type ExecutionTrace = {
  execution_id: string;
  strategy: string | null;
  market_key: string | null;
  created_at: number;
  updated_at: number;
  scan_snapshot: unknown | null;
  events: ExecutionTraceEvent[];
};

const STRATEGY_ID = 'BTC_5M';
const DEFAULT_SIM_BANKROLL = 1000;
const SERIES_MAX_POINTS = 720;
const ORDER_FEED_ROWS = 24;
const POSITIONS_ROWS = 64;

function parseNumber(value: unknown): number | null {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function parseText(value: unknown): string | null {
  if (typeof value !== 'string') return null;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function formatIntSpaces(value: number): string {
  const rounded = Math.round(value);
  // Use spaces to match the reference UI (e.g., 124 367 instead of 124,367).
  return rounded.toLocaleString('en-US').replace(/,/g, ' ');
}

function formatUsd0Spaces(value: number): string {
  const abs = Math.abs(value);
  const prefix = value < 0 ? '-' : '';
  return `${prefix}$${formatIntSpaces(abs)}`;
}

function formatUsdSigned0Spaces(value: number): string {
  const sign = value >= 0 ? '+' : '-';
  return `${sign}$${formatIntSpaces(Math.abs(value))}`;
}

function formatUsd2SignedNoPlus(value: number): string {
  const abs = Math.abs(value);
  const prefix = value < 0 ? '-' : '';
  return `${prefix}$${abs.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function formatUsd0Commas(value: number): string {
  const abs = Math.abs(value);
  const prefix = value < 0 ? '-' : '';
  return `${prefix}$${Math.round(abs).toLocaleString('en-US')}`;
}

function formatBtcPrice(value: number | null): string {
  if (value === null || !Number.isFinite(value)) return '--';
  return `$${value.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function formatCents0(value: number | null): string {
  if (value === null || !Number.isFinite(value)) return '--';
  return `${Math.round(value * 100)}c`;
}

function formatCountdown(seconds: number): string {
  const s = Math.max(0, Math.floor(seconds));
  const mm = Math.floor(s / 60);
  const ss = s % 60;
  return `${mm}:${ss.toString().padStart(2, '0')}`;
}

function formatTime24h(tsMs: number): string {
  return new Intl.DateTimeFormat('en-US', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  }).format(new Date(tsMs));
}

function formatEtWindow(startEpochSec: number, endEpochSec: number): string {
  const fmt = new Intl.DateTimeFormat('en-US', {
    hour: 'numeric',
    minute: '2-digit',
    hour12: true,
    timeZone: 'America/New_York',
  });
  const start = fmt.format(new Date(startEpochSec * 1000)).replace(' ', '');
  const end = fmt.format(new Date(endEpochSec * 1000)).replace(' ', '');
  return `${start}-${end} ET`;
}

function windowLabelForTs(tsMs: number): { start: number; end: number; label: string } {
  const epoch = Math.floor(tsMs / 1000);
  const start = epoch - (epoch % 300);
  const end = start + 300;
  return { start, end, label: formatEtWindow(start, end) };
}

function etMidnightEpochMs(nowMs: number): number {
  // Convert now into "ET clock time" using stringification/parsing trick.
  // This avoids adding a heavy timezone library and is stable enough for UI telemetry.
  const now = new Date(nowMs);
  const utcNow = new Date(now.toLocaleString('en-US', { timeZone: 'UTC' }));
  const etNow = new Date(now.toLocaleString('en-US', { timeZone: 'America/New_York' }));
  const offsetMs = utcNow.getTime() - etNow.getTime();
  const etMidnight = new Date(etNow.getTime());
  etMidnight.setHours(0, 0, 0, 0);
  return etMidnight.getTime() + offsetMs;
}

function safeDirection(input: unknown): 'UP' | 'DOWN' | null {
  const raw = parseText(input)?.toUpperCase();
  return raw === 'UP' ? 'UP' : raw === 'DOWN' ? 'DOWN' : null;
}

function computeSharpe(returns: number[]): number | null {
  if (returns.length < 8) return null;
  const mean = returns.reduce((a, b) => a + b, 0) / returns.length;
  const variance = returns.reduce((acc, r) => acc + (r - mean) * (r - mean), 0) / Math.max(1, returns.length - 1);
  const stdev = Math.sqrt(Math.max(0, variance));
  if (stdev <= 1e-12) return null;
  // Per-trade Sharpe (not annualized) is fine for this live dashboard display.
  return mean / stdev;
}

function computeMaxDrawdown(values: number[]): number {
  let peak = -Infinity;
  let maxDd = 0;
  for (const v of values) {
    if (v > peak) peak = v;
    const dd = peak - v;
    if (dd > maxDd) maxDd = dd;
  }
  return maxDd;
}

export const Btc5mEnginePage: React.FC = () => {
  const { socket } = useSocket();
  const navigate = useNavigate();
  const [mode, setMode] = useState<TradingMode>('PAPER');
  const [liveOrderPostingEnabled, setLiveOrderPostingEnabled] = useState(false);

  const [strategyMetrics, setStrategyMetrics] = useState<StrategyMetrics>({});
  const [latestScanMeta, setLatestScanMeta] = useState<any>(null);
  const [latestScanPasses, setLatestScanPasses] = useState(false);

  const [spotSeries, setSpotSeries] = useState<Array<{ timestamp: number; value: number }>>([]);
  const [equitySeries, setEquitySeries] = useState<Array<{ timestamp: number; value: number }>>([]);
  const [edgeSeries, setEdgeSeries] = useState<Array<{ timestamp: number; value: number }>>([]);

  const [tradeBlotter, setTradeBlotter] = useState<Record<string, TradeRecord>>({});

  const lastSpotPush = useRef(0);
  const lastEdgePush = useRef(0);

  const nowMs = useNowMs(250);
  const nowSec = Math.floor(nowMs / 1000);
  const windowStart = nowSec - (nowSec % 300);
  const windowEnd = windowStart + 300;
  const countdown = windowEnd - nowSec;
  const windowLabel = useMemo(() => formatEtWindow(windowStart, windowEnd), [windowStart, windowEnd]);

  const [traceExecutionId, setTraceExecutionId] = useState<string | null>(null);
  const [executionTrace, setExecutionTrace] = useState<ExecutionTrace | null>(null);
  const [executionTraceError, setExecutionTraceError] = useState<string | null>(null);
  const [executionTraceLoading, setExecutionTraceLoading] = useState(false);

  const closeTrace = React.useCallback(() => {
    setTraceExecutionId(null);
    setExecutionTrace(null);
    setExecutionTraceError(null);
    setExecutionTraceLoading(false);
  }, []);

  const openTrace = React.useCallback(async (executionId: string) => {
    if (!executionId) return;
    setTraceExecutionId(executionId);
    setExecutionTrace(null);
    setExecutionTraceError(null);
    setExecutionTraceLoading(true);
    try {
      const response = await fetch(`/api/arb/execution-trace/${encodeURIComponent(executionId)}`);
      if (!response.ok) {
        const payload = await response.json().catch(() => null) as { error?: string } | null;
        setExecutionTraceError(payload?.error || `Trace lookup failed (${response.status})`);
        return;
      }
      const data = await response.json() as ExecutionTrace;
      setExecutionTrace(data);
    } catch (error) {
      setExecutionTraceError(error instanceof Error ? error.message : String(error));
    } finally {
      setExecutionTraceLoading(false);
    }
  }, []);

  useEffect(() => {
    if (!socket) return;

    const handleTradingMode = (payload: { mode?: TradingMode; live_order_posting_enabled?: boolean }) => {
      if (payload?.mode === 'PAPER' || payload?.mode === 'LIVE') {
        setMode(payload.mode);
      }
      if (typeof payload?.live_order_posting_enabled === 'boolean') {
        setLiveOrderPostingEnabled(payload.live_order_posting_enabled);
      }
    };

    const handleStrategyMetrics = (payload: unknown) => {
      if (!payload || typeof payload !== 'object') return;
      setStrategyMetrics(payload as StrategyMetrics);
    };

    const handleExecutionLog = (payload: unknown) => {
      if (!payload || typeof payload !== 'object') return;
      const log = payload as ExecutionLog;

      const details = log?.details && typeof log.details === 'object' ? log.details : null;
      const detailsStrategy = details?.preflight?.strategy || details?.strategy;
      const isBtc5m = detailsStrategy === STRATEGY_ID || log?.market === 'BTC 5m Engine';
      if (!isBtc5m) return;

      const ts = typeof log.timestamp === 'number' ? log.timestamp : Date.now();
      const executionId = parseText(log.execution_id)
        || parseText((details as any)?.execution_id)
        || parseText((details as any)?.executionId)
        || undefined;

      const side = (log.side || '').toUpperCase();

      if (side === 'ENTRY' && executionId) {
        const direction = safeDirection(details?.side) || safeDirection(details?.position_side);
        const notional = parseNumber(log.size) ?? parseNumber(details?.size) ?? 0;
        const entryPrice = parseNumber(log.price);
        const label = windowLabelForTs(ts).label;
        const record: TradeRecord = {
          execution_id: executionId,
          direction,
          window_label: label,
          entry_ts: ts,
          entry_price: entryPrice,
          notional,
          exit_ts: null,
          pnl: null,
          status: 'OPEN',
        };
        setTradeBlotter((prev) => ({ ...prev, [executionId]: { ...(prev[executionId] || record), ...record } }));
      }

      if ((side === 'WIN' || side === 'LOSS' || side === 'SETTLEMENT') && executionId) {
        const pnl = parseNumber(details?.pnl);
        setTradeBlotter((prev) => {
          const existing = prev[executionId];
          const fallbackLabel = existing?.window_label || windowLabelForTs(ts).label;
          const next: TradeRecord = {
            execution_id: executionId,
            direction: existing?.direction ?? safeDirection(details?.side) ?? safeDirection(details?.position_side),
            window_label: fallbackLabel,
            entry_ts: existing?.entry_ts ?? ts,
            entry_price: existing?.entry_price ?? parseNumber(log.price),
            notional: existing?.notional ?? parseNumber(log.size) ?? 0,
            exit_ts: ts,
            pnl: pnl ?? existing?.pnl ?? null,
            status: 'RESOLVED',
          };
          return { ...prev, [executionId]: next };
        });
      }
    };

    const handleStrategyPnl = (payload: unknown) => {
      if (!payload || typeof payload !== 'object') return;
      const parsed = payload as StrategyPnlPayload;
      if (parsed?.strategy !== STRATEGY_ID) return;

      const executionId = parseText((parsed as any).execution_id)
        || parseText((parsed as any).executionId)
        || parseText(parsed?.details?.execution_id)
        || parseText(parsed?.details?.executionId)
        || null;
      if (!executionId) return;

      const ts = parseNumber(parsed.timestamp) ?? Date.now();
      const pnl = parseNumber(parsed.pnl);
      const notional = parseNumber((parsed as any).notional) ?? parseNumber(parsed?.details?.notional) ?? null;

      setTradeBlotter((prev) => {
        const existing = prev[executionId];
        const fallbackLabel = existing?.window_label || windowLabelForTs(ts).label;
        const next: TradeRecord = {
          execution_id: executionId,
          direction: existing?.direction ?? null,
          window_label: fallbackLabel,
          entry_ts: existing?.entry_ts ?? ts,
          entry_price: existing?.entry_price ?? null,
          notional: existing?.notional ?? (notional ?? 0),
          exit_ts: existing?.exit_ts ?? ts,
          pnl: pnl ?? existing?.pnl ?? null,
          status: 'RESOLVED',
        };
        return { ...prev, [executionId]: next };
      });
    };

    const handleScannerUpdate = (payload: unknown) => {
      if (!payload || typeof payload !== 'object') return;
      const scan = payload as any;
      if (scan?.strategy !== STRATEGY_ID) return;

      const ts = typeof scan.timestamp === 'number' ? scan.timestamp : Date.now();
      setLatestScanPasses(Boolean(scan?.passes_threshold));

      const spot = parseNumber(scan?.meta?.spot ?? scan?.prices?.[0]);
      if (spot !== null && ts - lastSpotPush.current >= 800) {
        lastSpotPush.current = ts;
        setSpotSeries((prev) => [...prev, { timestamp: ts, value: spot }].slice(-SERIES_MAX_POINTS));
      }

      const edge = parseNumber(scan?.meta?.best_net_expected_roi ?? scan?.score ?? scan?.gap);
      if (edge !== null && ts - lastEdgePush.current >= 250) {
        lastEdgePush.current = ts;
        setEdgeSeries((prev) => [...prev, { timestamp: ts, value: edge }].slice(-SERIES_MAX_POINTS));
      }

      if (scan?.meta) {
        setLatestScanMeta(scan.meta);
      }
    };

    socket.on('trading_mode_update', handleTradingMode);
    socket.on('strategy_metrics_update', handleStrategyMetrics);
    socket.on('execution_log', handleExecutionLog);
    socket.on('strategy_pnl', handleStrategyPnl);
    socket.on('scanner_update', handleScannerUpdate);
    socket.emit('request_trading_mode');

    return () => {
      socket.off('trading_mode_update', handleTradingMode);
      socket.off('strategy_metrics_update', handleStrategyMetrics);
      socket.off('execution_log', handleExecutionLog);
      socket.off('strategy_pnl', handleStrategyPnl);
      socket.off('scanner_update', handleScannerUpdate);
    };
  }, [socket]);

  useEffect(() => {
    let active = true;
    const fetchStats = async () => {
      try {
        const res = await fetch('/api/arb/stats');
        if (!res.ok) return;
        const json = await res.json();
        const equity = parseNumber(json?.simulation_ledger?.equity) ?? parseNumber(json?.bankroll);
        const ts = Date.now();
        if (!active || equity === null) return;
        setEquitySeries((prev) => [...prev, { timestamp: ts, value: equity }].slice(-SERIES_MAX_POINTS));
        const tradingMode = json?.trading_mode;
        if (tradingMode === 'PAPER' || tradingMode === 'LIVE') {
          setMode(tradingMode);
        }
        if (typeof json?.live_order_posting_enabled === 'boolean') {
          setLiveOrderPostingEnabled(json.live_order_posting_enabled);
        }
      } catch {
        // ignore
      }
    };
    fetchStats();
    const timer = window.setInterval(fetchStats, 1500);
    return () => {
      active = false;
      window.clearInterval(timer);
    };
  }, []);

  const spot = parseNumber(latestScanMeta?.spot) ?? (spotSeries.length ? spotSeries[spotSeries.length - 1].value : null);
  const fairYes = parseNumber(latestScanMeta?.fair_yes);
  const yesAsk = parseNumber(latestScanMeta?.yes_ask);
  const noAsk = parseNumber(latestScanMeta?.no_ask);
  const bestSide = safeDirection(latestScanMeta?.best_side);
  const bestPrice = parseNumber(latestScanMeta?.best_price);
  const bestProb = parseNumber(latestScanMeta?.best_prob);
  const bestNetExpected = parseNumber(latestScanMeta?.best_net_expected_roi);
  const kelly = parseNumber(latestScanMeta?.kelly_fraction);

  const strat = strategyMetrics[STRATEGY_ID] || {};
  const cumulativePnl = parseNumber(strat.pnl)
    ?? (() => {
      const lastEquity = equitySeries.length ? equitySeries[equitySeries.length - 1].value : DEFAULT_SIM_BANKROLL;
      return lastEquity - DEFAULT_SIM_BANKROLL;
    })();
  const allTradesSorted = useMemo(
    () => Object.values(tradeBlotter).sort((a, b) => b.entry_ts - a.entry_ts),
    [tradeBlotter],
  );
  const closedTrades = useMemo(
    () => allTradesSorted.filter((t) => t.status !== 'OPEN'),
    [allTradesSorted],
  );
  const trades = strat.daily_trades ?? closedTrades.length;
  const wins = closedTrades.filter((t) => (t.pnl ?? 0) > 0).length;
  const winRate = closedTrades.length > 0 ? wins / closedTrades.length : 0;

  const todayStartMs = etMidnightEpochMs(nowMs);
  const todayPnl = closedTrades
    .filter((t) => (t.exit_ts ?? 0) >= todayStartMs)
    .reduce((sum, t) => sum + (t.pnl ?? 0), 0);

  const equityValues = equitySeries.map((p) => p.value);
  const maxDd = equityValues.length > 5 ? computeMaxDrawdown(equityValues) : 0;

  const perTradeReturns = closedTrades
    .map((t) => {
      if (!t.notional || t.notional <= 0) return null;
      if (t.pnl === null || !Number.isFinite(t.pnl)) return null;
      return t.pnl / t.notional;
    })
    .filter((v): v is number => v !== null);
  const sharpe = computeSharpe(perTradeReturns);
  const avgPerTrade = trades > 0 ? cumulativePnl / trades : 0;
  const roiPct = DEFAULT_SIM_BANKROLL > 0 ? (cumulativePnl / DEFAULT_SIM_BANKROLL) * 100 : 0;

  const openNotional = useMemo(
    () => Object.values(tradeBlotter).filter((t) => t.status === 'OPEN').reduce((sum, t) => sum + (t.notional || 0), 0),
    [tradeBlotter],
  );
  const lastNotional = useMemo(
    () => allTradesSorted.find((t) => t.notional > 0)?.notional ?? 0,
    [allTradesSorted],
  );

  const execDecision = latestScanPasses && bestNetExpected !== null && bestNetExpected > 0
    ? 'PASS'
    : 'HOLD';

  const orderFeed = allTradesSorted.slice(0, ORDER_FEED_ROWS);
  const positions = allTradesSorted.slice(0, POSITIONS_ROWS);

  return (
    <ExecutionLayout>
      <div className="h-screen w-screen flex flex-col">
        <TopBar
          btcPrice={formatBtcPrice(spot)}
          pnl={cumulativePnl}
          today={todayPnl}
          winRate={winRate}
          trades={trades}
          openNotional={openNotional}
          countdown={formatCountdown(countdown)}
          windowLabel={windowLabel}
          onExit={() => navigate('/polymarket')}
        />

        <div className="flex-1 min-h-0 p-3">
          <div
            className="h-full w-full grid gap-px bg-white/10 p-px"
            style={{
              gridTemplateColumns: 'minmax(340px, 3.4fr) minmax(420px, 4fr) minmax(420px, 4fr) minmax(320px, 2.4fr)',
              gridTemplateRows: 'minmax(0, 1fr) minmax(0, 1fr) 240px',
            }}
          >
            <PanelCell style={{ gridColumn: '1', gridRow: '1 / span 2' }}>
              <CumulativePnlPanel
                cumulativePnl={cumulativePnl}
                todayPnl={todayPnl}
                roiPct={roiPct}
                series={equitySeries.map((p) => p.value - DEFAULT_SIM_BANKROLL)}
              />
            </PanelCell>

            <PanelCell style={{ gridColumn: '2', gridRow: '1' }}>
              <BigLinePanel
                title="BTC/USD - AGGREGATED"
                headline={formatBtcPrice(spot)}
                series={spotSeries.map((p) => p.value)}
              />
            </PanelCell>

            <PanelCell style={{ gridColumn: '3', gridRow: '1' }}>
              <BigLinePanel
                title="EQUITY CURVE"
                headline=""
                series={equitySeries.map((p) => p.value)}
              />
            </PanelCell>

            <PanelCell style={{ gridColumn: '2', gridRow: '2' }}>
              <OrderFeedPanel
                rows={orderFeed}
                onTrace={(id) => void openTrace(id)}
              />
            </PanelCell>

            <PanelCell style={{ gridColumn: '3', gridRow: '2' }}>
              <SignalFlowPanel values={edgeSeries.map((p) => p.value)} />
            </PanelCell>

            <PanelCell style={{ gridColumn: '1', gridRow: '3' }}>
              <StatsPanel
                avgPerTrade={avgPerTrade}
                sharpe={sharpe}
                maxDrawdown={maxDd}
                openNotional={openNotional}
                kelly={kelly}
                ddLimitPct={-5.0}
              />
            </PanelCell>

            <PanelCell style={{ gridColumn: '2 / span 2', gridRow: '3' }}>
              <ExecutionPipelinePanel
                spot={spot}
                yesAsk={yesAsk}
                noAsk={noAsk}
                fairYes={fairYes}
                bestSide={bestSide}
                bestPrice={bestPrice}
                bestProb={bestProb}
                bestNetExpected={bestNetExpected}
                kelly={kelly}
                execDecision={execDecision}
                liveModeLabel={mode === 'PAPER' ? 'PAPER' : liveOrderPostingEnabled ? 'LIVE' : 'DRY-RUN'}
                notionalHint={openNotional > 0 ? openNotional : lastNotional}
              />
            </PanelCell>

            <PanelCell style={{ gridColumn: '4', gridRow: '1 / span 3' }}>
              <PositionsPanel
                rows={positions}
                onTrace={(id) => void openTrace(id)}
              />
            </PanelCell>
          </div>
        </div>

        {traceExecutionId && (
          <div className="fixed inset-0 z-[120] bg-black/80 backdrop-blur-sm flex items-center justify-center p-4">
            <div className="w-full max-w-5xl bg-[#0b0b0b] border border-white/10 shadow-2xl">
              <div className="flex items-center justify-between gap-3 px-4 py-3 border-b border-white/10">
                <div className="font-mono text-xs text-gray-400">
                  TRACE <span className="text-gray-200">{traceExecutionId}</span>
                </div>
                <button
                  type="button"
                  onClick={closeTrace}
                  className="px-3 py-1.5 text-[11px] font-mono border border-white/20 text-gray-200 hover:bg-white/5"
                >
                  CLOSE
                </button>
              </div>
              <div className="p-4">
                {executionTraceLoading && (
                  <div className="text-xs font-mono text-cyan-300">Loading trace...</div>
                )}
                {executionTraceError && (
                  <div className="text-xs font-mono text-rose-300">{executionTraceError}</div>
                )}
                {executionTrace && (
                  <pre className="text-[11px] font-mono text-gray-200 bg-black/40 border border-white/10 p-3 overflow-auto max-h-[70vh]">
{JSON.stringify(executionTrace, null, 2)}
                  </pre>
                )}
              </div>
            </div>
          </div>
        )}
      </div>
    </ExecutionLayout>
  );
};

function TopBar(props: {
  btcPrice: string;
  pnl: number;
  today: number;
  winRate: number;
  trades: number;
  openNotional: number;
  countdown: string;
  windowLabel: string;
  onExit?: () => void;
}) {
  const metrics = [
    { label: 'BTC', value: props.btcPrice, className: 'text-gray-100' },
    { label: 'PNL', value: formatUsdSigned0Spaces(props.pnl), className: props.pnl >= 0 ? 'text-emerald-300' : 'text-rose-300' },
    { label: 'TODAY', value: formatUsdSigned0Spaces(props.today), className: props.today >= 0 ? 'text-emerald-300' : 'text-rose-300' },
    { label: 'WIN', value: `${(props.winRate * 100).toFixed(1)}%`, className: props.winRate >= 0.55 ? 'text-emerald-300' : 'text-gray-200' },
    { label: 'TRADES', value: formatIntSpaces(props.trades), className: 'text-gray-100' },
    { label: 'OPEN', value: formatUsd0Spaces(props.openNotional), className: props.openNotional > 0 ? 'text-amber-300' : 'text-gray-200' },
  ];

  return (
    <div className="h-14 px-5 flex items-center justify-between bg-black/60 border-b border-white/10">
      <div className="flex items-center">
        <button
          type="button"
          onClick={props.onExit}
          className="w-2 h-2 rounded-full bg-emerald-400 shadow-[0_0_10px_rgba(16,185,129,0.55)] mr-4"
          aria-label="Exit dashboard"
        />
        <div className="flex items-center font-mono text-[11px]">
          {metrics.map((m, idx) => (
            <React.Fragment key={m.label}>
              <div className="flex items-center gap-2 px-4">
                <span className="uppercase tracking-widest text-gray-500">{m.label}</span>
                <span className={`font-semibold ${m.className}`}>{m.value}</span>
              </div>
              {idx < metrics.length - 1 && <div className="h-6 w-px bg-white/10" />}
            </React.Fragment>
          ))}
        </div>
      </div>

      <div className="border border-white/10 bg-black/40">
        <div className="grid grid-cols-2 gap-px bg-white/10">
          <div className="bg-black/40 px-4 py-2">
            <div className="text-[10px] font-mono uppercase tracking-widest text-gray-500">NEXT WINDOW</div>
            <div className="text-xl font-mono font-semibold text-gray-100 leading-tight">{props.countdown}</div>
          </div>
          <div className="bg-black/40 px-4 py-2 min-w-[240px]">
            <div className="text-[10px] font-mono uppercase tracking-widest text-gray-500">MARKET</div>
            <div className="text-[12px] font-mono text-gray-100 leading-tight">{props.windowLabel}</div>
          </div>
        </div>
      </div>
    </div>
  );
}

function PanelCell({ children, style }: { children: React.ReactNode; style?: React.CSSProperties }) {
  return (
    <section
      className="bg-[#070707] relative overflow-hidden"
      style={style}
    >
      <div className="absolute inset-0 pointer-events-none bg-[linear-gradient(180deg,rgba(255,255,255,0.04),transparent_45%)]" />
      <div className="absolute inset-0 pointer-events-none shadow-[inset_0_0_0_1px_rgba(255,255,255,0.02)]" />
      <div className="relative z-10 h-full">
        {children}
      </div>
    </section>
  );
}

function PanelTitle({ children }: { children: React.ReactNode }) {
  return (
    <div className="text-[10px] font-mono uppercase tracking-[0.22em] text-gray-500">
      {children}
    </div>
  );
}

function CumulativePnlPanel(props: {
  cumulativePnl: number;
  todayPnl: number;
  roiPct: number;
  series: number[];
}) {
  const headline = `$${formatIntSpaces(Math.abs(props.cumulativePnl))}`;
  const pnlClass = props.cumulativePnl >= 0 ? 'text-gray-100' : 'text-gray-100';
  const todayClass = props.todayPnl >= 0 ? 'text-emerald-300' : 'text-rose-300';
  const roiClass = props.roiPct >= 0 ? 'text-gray-400' : 'text-gray-400';

  return (
    <div className="h-full relative">
      <div className="absolute inset-0">
        <GlowLineChart series={props.series} stroke="rgba(255,255,255,0.92)" />
      </div>
      <div className="relative p-6">
        <PanelTitle>CUMULATIVE PNL</PanelTitle>
        <div className={`mt-3 text-5xl font-mono font-semibold leading-none ${pnlClass}`}>
          {headline}
        </div>
        <div className="mt-2 text-[12px] font-mono flex items-center gap-4">
          <span className={todayClass}>{formatUsdSigned0Spaces(props.todayPnl)} today</span>
          <span className={roiClass}>{props.roiPct >= 0 ? '+' : '-'}{Math.abs(props.roiPct).toFixed(1)}%</span>
        </div>
      </div>
    </div>
  );
}

function BigLinePanel(props: { title: string; headline: string; series: number[] }) {
  return (
    <div className="h-full relative">
      <div className="absolute inset-0">
        <GlowLineChart series={props.series} stroke="rgba(255,255,255,0.85)" />
      </div>
      <div className="relative h-full p-5">
        <div className="flex items-start justify-between gap-4">
          <PanelTitle>{props.title}</PanelTitle>
          {props.headline && (
            <div className="text-2xl font-mono font-semibold text-gray-100">{props.headline}</div>
          )}
        </div>
      </div>
    </div>
  );
}

function OrderFeedPanel(props: { rows: TradeRecord[]; onTrace: (id: string) => void }) {
  return (
    <div className="h-full flex flex-col">
      <div className="px-5 pt-4">
        <PanelTitle>ORDER FEED</PanelTitle>
      </div>
      <div className="px-5 pt-3 text-[10px] font-mono uppercase tracking-widest text-gray-600 grid grid-cols-[84px_120px_80px_84px_110px_1fr] gap-3 border-b border-white/10 pb-2">
        <div>TIME</div>
        <div>MARKET</div>
        <div>SIDE</div>
        <div>ENTRY</div>
        <div>SIZE</div>
        <div>STATUS</div>
      </div>
      <div className="flex-1 min-h-0 overflow-y-auto px-5 py-3">
        {props.rows.length === 0 && (
          <div className="text-xs font-mono text-gray-600">No trades yet.</div>
        )}
        <div className="space-y-2">
          {props.rows.map((row) => {
            const pnl = row.pnl;
            const pnlText = pnl === null ? '--' : formatUsdSigned0Spaces(pnl);
            const pnlClass = pnl === null ? 'text-gray-600' : pnl >= 0 ? 'text-emerald-300' : 'text-rose-300';
            const sideLabel = row.direction || '--';
            const sideClass = sideLabel === 'UP' ? 'text-emerald-300' : sideLabel === 'DOWN' ? 'text-rose-300' : 'text-gray-400';
            const barClass = sideLabel === 'UP' ? 'bg-emerald-400' : sideLabel === 'DOWN' ? 'bg-rose-400' : 'bg-white/15';
            const status = row.status === 'STOPPED' ? 'stopped ×' : row.status === 'OPEN' ? 'open' : 'resolved ✓';
            const statusClass = row.status === 'STOPPED' ? 'text-rose-300' : row.status === 'OPEN' ? 'text-gray-400' : 'text-gray-400';

            const windowShort = row.window_label.replace(' ET', '');

            return (
              <button
                key={row.execution_id}
                type="button"
                onClick={() => props.onTrace(row.execution_id)}
                className="w-full text-left group"
                title={`Open trace ${row.execution_id}`}
              >
                <div className="grid grid-cols-[2px_84px_120px_80px_84px_110px_1fr] gap-3 items-center">
                  <div className={`h-10 ${barClass}`} />
                  <div className="font-mono text-[11px] text-gray-500">{formatTime24h(row.entry_ts)}</div>
                  <div className="font-mono text-[11px] text-gray-300">{windowShort}</div>
                  <div className={`font-mono text-[11px] font-semibold ${sideClass}`}>{sideLabel}</div>
                  <div className="font-mono text-[11px] text-gray-200">{formatCents0(row.entry_price)}</div>
                  <div className="font-mono text-[11px] text-gray-200">{formatUsd0Spaces(row.notional)}</div>
                  <div className={`font-mono text-[11px] ${statusClass} flex items-center justify-between gap-3`}>
                    <span className="truncate">{status}</span>
                    <span className={pnlClass}>
                      {pnlText}
                    </span>
                  </div>
                </div>
              </button>
            );
          })}
        </div>
      </div>
    </div>
  );
}

function SignalFlowPanel(props: { values: number[] }) {
  return (
    <div className="h-full flex flex-col">
      <div className="px-5 pt-4">
        <PanelTitle>CROSS-EXCHANGE SIGNAL FLOW</PanelTitle>
      </div>
      <div className="flex-1 min-h-0 p-5">
        <SignalFlowMatrix values={props.values} />
      </div>
    </div>
  );
}

function StatsPanel(props: {
  avgPerTrade: number;
  sharpe: number | null;
  maxDrawdown: number;
  openNotional: number;
  kelly: number | null;
  ddLimitPct: number;
}) {
  return (
    <div className="h-full p-5">
      <div className="grid grid-cols-2 gap-x-8 gap-y-6">
        <MiniStat
          label="AVG / TRADE"
          value={formatUsd2SignedNoPlus(props.avgPerTrade)}
          valueClass={props.avgPerTrade >= 0 ? 'text-emerald-300' : 'text-rose-300'}
        />
        <MiniStat
          label="SHARPE"
          value={props.sharpe === null ? '--' : props.sharpe.toFixed(2)}
          valueClass="text-gray-100"
        />
        <MiniStat
          label="MAX DD"
          value={props.maxDrawdown > 0 ? formatUsd0Commas(-props.maxDrawdown) : '$0'}
          valueClass={props.maxDrawdown > 0 ? 'text-rose-300' : 'text-gray-200'}
        />
        <MiniStat
          label="OPEN POS"
          value={formatUsd0Spaces(props.openNotional)}
          valueClass={props.openNotional > 0 ? 'text-amber-300' : 'text-gray-200'}
        />
        <MiniStat
          label="KELLY F*"
          value={props.kelly === null ? '--' : `${(Math.max(0, Math.min(1, props.kelly)) * 100).toFixed(2)}%`}
          valueClass="text-gray-100"
        />
        <MiniStat
          label="DD LIMIT"
          value={`${props.ddLimitPct.toFixed(1)}%`}
          valueClass="text-rose-300"
        />
      </div>
    </div>
  );
}

function MiniStat(props: { label: string; value: string; valueClass: string }) {
  return (
    <div>
      <div className="text-[10px] font-mono uppercase tracking-widest text-gray-600">{props.label}</div>
      <div className={`mt-2 font-mono text-lg font-semibold ${props.valueClass}`}>{props.value}</div>
    </div>
  );
}

function ExecutionPipelinePanel(props: {
  spot: number | null;
  yesAsk: number | null;
  noAsk: number | null;
  fairYes: number | null;
  bestSide: 'UP' | 'DOWN' | null;
  bestPrice: number | null;
  bestProb: number | null;
  bestNetExpected: number | null;
  kelly: number | null;
  execDecision: 'PASS' | 'HOLD';
  liveModeLabel: string;
  notionalHint: number;
}) {
  const impliedYes = props.yesAsk;
  const cexProb = props.fairYes;
  const edge = impliedYes !== null && cexProb !== null ? (cexProb - impliedYes) : null;
  const edgeBps = edge !== null ? edge * 10_000 : null;
  const netBps = props.bestNetExpected !== null ? props.bestNetExpected * 10_000 : null;
  const evUsd = props.bestNetExpected !== null && Number.isFinite(props.notionalHint) && props.notionalHint > 0
    ? props.notionalHint * props.bestNetExpected
    : null;

  return (
    <div className="h-full flex flex-col">
      <div className="px-5 pt-4">
        <PanelTitle>EXECUTION PIPELINE</PanelTitle>
      </div>
      <div className="flex-1 min-h-0 p-5">
        <div className="grid grid-cols-5 gap-px bg-white/10 h-full">
          <PipeCard title="01" subtitle="CEX FEEDS">
            <PipeRow k="coinbase" v={props.spot === null ? '--' : formatUsd0Spaces(props.spot)} />
            <PipeRow k="binance" v="--" />
            <PipeRow k="okx" v="--" />
            <PipeRow k="bybit" v="--" />
            <PipeRow k="kraken" v="--" />
            <PipeRow k="bitfinex" v="--" />
          </PipeCard>
          <PipeCard title="02" subtitle="PM ODDS">
            <PipeRow k="UP" v={props.yesAsk === null ? '--' : formatCents0(props.yesAsk)} vClass="text-emerald-300" />
            <PipeRow k="DN" v={props.noAsk === null ? '--' : formatCents0(props.noAsk)} vClass="text-rose-300" />
            <PipeRow k="implied" v={impliedYes === null ? '--' : `${(Math.max(0, Math.min(1, impliedYes)) * 100).toFixed(1)}%`} />
          </PipeCard>
          <PipeCard title="03" subtitle="EDGE">
            <PipeRow k="cex" v={cexProb === null ? '--' : `${(Math.max(0, Math.min(1, cexProb)) * 100).toFixed(1)}%`} />
            <PipeRow k="pm" v={impliedYes === null ? '--' : `${(Math.max(0, Math.min(1, impliedYes)) * 100).toFixed(1)}%`} />
            <PipeRow
              k="edge"
              v={edgeBps === null ? '--' : `${edgeBps >= 0 ? '+' : ''}${edgeBps.toFixed(1)}bps`}
              vClass={edgeBps !== null && edgeBps >= 0 ? 'text-emerald-300' : 'text-rose-300'}
            />
            <PipeRow
              k="net"
              v={netBps === null ? '--' : `${netBps >= 0 ? '+' : ''}${netBps.toFixed(1)}bps`}
              vClass={netBps !== null && netBps >= 0 ? 'text-emerald-300' : 'text-rose-300'}
            />
          </PipeCard>
          <PipeCard title="04" subtitle="KELLY">
            <PipeRow k="f*" v={props.kelly === null ? '--' : `${(Math.max(0, Math.min(1, props.kelly)) * 100).toFixed(2)}%`} />
            <PipeRow k="p" v={props.bestProb === null ? '--' : `${(Math.max(0, Math.min(1, props.bestProb)) * 100).toFixed(1)}%`} />
            <PipeRow k="entry" v={props.bestPrice === null ? '--' : formatCents0(props.bestPrice)} />
          </PipeCard>
          <PipeCard title="05" subtitle="EXEC">
            <PipeRow k="dir" v={props.bestSide || '--'} vClass={props.bestSide === 'UP' ? 'text-emerald-300' : props.bestSide === 'DOWN' ? 'text-rose-300' : 'text-gray-200'} />
            <PipeRow k="@" v={props.bestPrice === null ? '--' : formatCents0(props.bestPrice)} />
            <div className="mt-3 text-[11px] font-mono">
              <span className="text-gray-500">EV </span>
              <span className={evUsd !== null && evUsd >= 0 ? 'text-emerald-300' : 'text-rose-300'}>
                {evUsd === null ? '--' : formatUsdSigned0Spaces(evUsd)}
              </span>
            </div>
          </PipeCard>
        </div>
      </div>
    </div>
  );
}

function PipeCard(props: { title: string; subtitle: string; children: React.ReactNode }) {
  return (
    <div className="bg-[#070707] p-4 flex flex-col justify-between">
      <div>
        <div className="flex items-center gap-3 font-mono text-[10px] uppercase tracking-widest text-gray-600">
          <span className="text-gray-500">{props.title}</span>
          <span>{props.subtitle}</span>
        </div>
        <div className="mt-3 space-y-1.5">
          {props.children}
        </div>
      </div>
    </div>
  );
}

function PipeRow(props: { k: string; v: string; vClass?: string }) {
  return (
    <div className="flex items-center justify-between gap-3 font-mono text-[12px]">
      <span className="text-gray-500">{props.k}</span>
      <span className={props.vClass || 'text-gray-200'}>{props.v}</span>
    </div>
  );
}

function PositionsPanel(props: { rows: TradeRecord[]; onTrace: (id: string) => void }) {
  return (
    <div className="h-full flex flex-col">
      <div className="px-5 pt-4 pb-3 border-b border-white/10">
        <PanelTitle>POSITIONS</PanelTitle>
      </div>
      <div className="flex-1 min-h-0 overflow-y-auto">
        <div className="divide-y divide-white/10">
          {props.rows.length === 0 && (
            <div className="px-5 py-4 text-xs font-mono text-gray-600">No positions yet.</div>
          )}
          {props.rows.map((row) => {
            const pnl = row.pnl;
            const pnlText = pnl === null ? '--' : formatUsdSigned0Spaces(pnl);
            const pnlClass = pnl === null ? 'text-gray-600' : pnl >= 0 ? 'text-emerald-300' : 'text-rose-300';
            const edgeBar = pnl === null ? 'bg-white/15' : pnl >= 0 ? 'bg-emerald-400' : 'bg-rose-400';
            const sideClass = row.direction === 'UP' ? 'text-emerald-300' : row.direction === 'DOWN' ? 'text-rose-300' : 'text-gray-400';
            const arrow = row.direction === 'UP' ? '▲' : row.direction === 'DOWN' ? '▼' : '•';
            const status = row.status === 'STOPPED' ? 'stopped ×' : row.status === 'OPEN' ? 'open' : 'resolved ✓';

            return (
              <button
                key={row.execution_id}
                type="button"
                onClick={() => props.onTrace(row.execution_id)}
                className="w-full text-left hover:bg-white/[0.03]"
                title={`Open trace ${row.execution_id}`}
              >
                <div className="grid grid-cols-[3px_1fr] gap-0">
                  <div className={edgeBar} />
                  <div className="px-5 py-4">
                    <div className="flex items-center justify-between gap-4">
                      <div className={`font-mono text-[13px] font-semibold ${sideClass}`}>
                        {arrow} {row.direction || '--'}
                      </div>
                      <div className={`font-mono text-[13px] font-semibold ${pnlClass}`}>
                        {pnlText}
                      </div>
                    </div>
                    <div className="mt-2 font-mono text-[11px] text-gray-500 flex items-center gap-3">
                      <span>{row.window_label.replace(' ET', '')}</span>
                      <span>·</span>
                      <span>{formatCents0(row.entry_price)}</span>
                      <span>·</span>
                      <span>{formatUsd0Spaces(row.notional)}</span>
                    </div>
                    <div className="mt-2 font-mono text-[11px] text-gray-400">
                      {status}
                    </div>
                  </div>
                </div>
              </button>
            );
          })}
        </div>
      </div>
    </div>
  );
}

function GlowLineChart(props: { series: number[]; stroke: string }) {
  const points = props.series.filter((v) => Number.isFinite(v));
  if (points.length < 2) {
    return <div className="h-full w-full" />;
  }

  const width = 1000;
  const height = 420;
  const padX = 14;
  const padY = 14;
  const min = Math.min(...points);
  const max = Math.max(...points);
  const spread = Math.max(1e-9, max - min);

  const poly = points
    .map((v, i) => {
      const x = padX + (i / (points.length - 1)) * (width - padX * 2);
      const y = height - padY - ((v - min) / spread) * (height - padY * 2);
      return `${x.toFixed(2)},${y.toFixed(2)}`;
    })
    .join(' ');

  const id = useMemo(() => `glow-${Math.random().toString(16).slice(2)}`, []);
  const gridLines = 6;

  return (
    <svg width="100%" height="100%" viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none">
      <defs>
        <filter id={id} x="-20%" y="-20%" width="140%" height="140%">
          <feDropShadow dx="0" dy="0" stdDeviation="2.4" floodColor="rgba(255,255,255,0.20)" />
          <feDropShadow dx="0" dy="0" stdDeviation="6.0" floodColor="rgba(255,255,255,0.08)" />
        </filter>
      </defs>

      {/* subtle horizontal grid */}
      {Array.from({ length: gridLines }).map((_, idx) => {
        const y = padY + (idx / (gridLines - 1)) * (height - padY * 2);
        return (
          <line
            key={idx}
            x1={0}
            x2={width}
            y1={y}
            y2={y}
            stroke="rgba(255,255,255,0.05)"
            strokeWidth="1"
          />
        );
      })}

      <polyline
        points={poly}
        fill="none"
        stroke={props.stroke}
        strokeWidth="2.2"
        strokeLinejoin="round"
        strokeLinecap="round"
        filter={`url(#${id})`}
      />
    </svg>
  );
}

function SignalFlowMatrix(props: { values: number[] }) {
  const cols = 64;
  const rows = 18;
  const needed = cols * rows;
  const slice = props.values.slice(-needed);
  const padded = slice.length < needed
    ? new Array(needed - slice.length).fill(0).concat(slice)
    : slice;

  const maxAbs = Math.max(1e-9, ...padded.map((v) => Math.abs(v)));

  return (
    <div
      className="grid gap-[2px] h-full"
      style={{ gridTemplateColumns: `repeat(${cols}, minmax(0, 1fr))` }}
    >
      {padded.map((v, idx) => {
        const intensity = Math.min(1, Math.abs(v) / maxAbs);
        // grayscale with slightly higher brightness for positive edges
        const base = 0.06;
        const bright = base + intensity * 0.72;
        const alpha = v >= 0 ? bright : bright * 0.42;
        const bg = `rgba(255,255,255,${alpha.toFixed(3)})`;
        return (
          <div
            key={idx}
            className="rounded-[1px] border border-white/5"
            style={{ background: bg }}
          />
        );
      })}
    </div>
  );
}

function useNowMs(intervalMs: number): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), Math.max(50, intervalMs));
    return () => window.clearInterval(timer);
  }, [intervalMs]);
  return now;
}
