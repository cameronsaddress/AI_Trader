import React, { useEffect, useMemo, useRef, useState } from 'react';
import { DashboardLayout } from '../components/layout/DashboardLayout';
import { useSocket } from '../context/SocketContext';

type TradingMode = 'PAPER' | 'LIVE';

type ExecutionLog = {
  timestamp: number;
  side?: string;
  market?: string;
  price?: unknown;
  size?: unknown;
  mode?: string;
  details?: any;
};

type StrategyPnlPayload = {
  strategy?: string;
  pnl?: number;
  timestamp?: number;
  details?: any;
};

type StrategyMetrics = Record<string, { pnl?: number; daily_trades?: number; updated_at?: number }>;

function parseNumber(value: unknown): number | null {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function formatUsd(value: number): string {
  return `$${value.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function formatUsdCompact(value: number): string {
  const abs = Math.abs(value);
  if (abs >= 1_000_000) {
    return `$${(value / 1_000_000).toFixed(2)}M`;
  }
  if (abs >= 1_000) {
    return `$${(value / 1_000).toFixed(1)}k`;
  }
  return `$${value.toFixed(0)}`;
}

function formatCents(value: number | null): string {
  if (value === null || !Number.isFinite(value)) return '--';
  return `${(value * 100).toFixed(2)}c`;
}

function formatBps(value: number | null): string {
  if (value === null || !Number.isFinite(value)) return '--';
  const bps = value * 10_000;
  const sign = bps > 0 ? '+' : '';
  return `${sign}${bps.toFixed(1)}bps`;
}

function clamp01(value: number): number {
  if (!Number.isFinite(value)) return 0;
  return Math.max(0, Math.min(1, value));
}

function formatCountdown(seconds: number): string {
  const s = Math.max(0, Math.floor(seconds));
  const mm = Math.floor(s / 60);
  const ss = s % 60;
  return `${mm}:${ss.toString().padStart(2, '0')}`;
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

const STRATEGY_ID = 'BTC_5M';
const MAX_LOGS = 250;
const SERIES_MAX_POINTS = 480;

export const Btc5mEnginePage: React.FC = () => {
  const { socket } = useSocket();
  const [mode, setMode] = useState<TradingMode>('PAPER');
  const [liveOrderPostingEnabled, setLiveOrderPostingEnabled] = useState(false);

  const [strategyMetrics, setStrategyMetrics] = useState<StrategyMetrics>({});
  const [execLogs, setExecLogs] = useState<ExecutionLog[]>([]);
  const [pnlEvents, setPnlEvents] = useState<Array<{ timestamp: number; pnl: number }>>([]);

  const [spotSeries, setSpotSeries] = useState<Array<{ timestamp: number; value: number }>>([]);
  const [equitySeries, setEquitySeries] = useState<Array<{ timestamp: number; value: number }>>([]);
  const [edgeSeries, setEdgeSeries] = useState<Array<{ timestamp: number; value: number }>>([]);

  const [openPosition, setOpenPosition] = useState<{
    side: 'UP' | 'DOWN';
    notional: number;
    entryPrice: number | null;
    timestamp: number;
  } | null>(null);

  const [latestScanMeta, setLatestScanMeta] = useState<any>(null);
  const [latestScanPasses, setLatestScanPasses] = useState(false);

  const lastSpotPush = useRef(0);
  const lastEdgePush = useRef(0);

  const nowMs = useNowMs(250);
  const nowSec = Math.floor(nowMs / 1000);
  const windowStart = nowSec - (nowSec % 300);
  const windowEnd = windowStart + 300;
  const countdown = windowEnd - nowSec;

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
      const strategy = log?.details?.preflight?.strategy || log?.details?.strategy;
      if (strategy !== STRATEGY_ID && log?.market !== 'BTC 5m Engine') {
        return;
      }

      setExecLogs((prev) => [log, ...prev].slice(0, MAX_LOGS));

      if (log?.side === 'ENTRY') {
        const side = (log?.details?.side || log?.details?.position_side || '').toUpperCase();
        const notional = parseNumber(log.size) ?? parseNumber(log?.details?.size) ?? 0;
        const entryPrice = parseNumber(log.price);
        if ((side === 'UP' || side === 'DOWN') && notional > 0) {
          setOpenPosition({
            side: side as 'UP' | 'DOWN',
            notional,
            entryPrice,
            timestamp: typeof log.timestamp === 'number' ? log.timestamp : Date.now(),
          });
        }
      }

      if (log?.side === 'WIN' || log?.side === 'LOSS' || log?.side === 'SETTLEMENT') {
        setOpenPosition(null);
      }
    };

    const handleStrategyPnl = (payload: unknown) => {
      const parsed = payload as StrategyPnlPayload;
      if (parsed?.strategy !== STRATEGY_ID) return;
      const pnl = parseNumber(parsed.pnl) ?? 0;
      const ts = parseNumber(parsed.timestamp) ?? Date.now();
      setPnlEvents((prev) => [{ timestamp: ts, pnl }, ...prev].slice(0, 500));
      setOpenPosition(null);
    };

    const handleScannerUpdate = (payload: unknown) => {
      if (!payload || typeof payload !== 'object') return;
      const scan = payload as any;
      if (scan?.strategy !== STRATEGY_ID) return;

      const ts = typeof scan.timestamp === 'number' ? scan.timestamp : Date.now();
      setLatestScanPasses(Boolean(scan?.passes_threshold));
      const spot = parseNumber(scan?.prices?.[0]);
      if (spot !== null && ts - lastSpotPush.current >= 900) {
        lastSpotPush.current = ts;
        setSpotSeries((prev) => [...prev, { timestamp: ts, value: spot }].slice(-SERIES_MAX_POINTS));
      }

      const edge = parseNumber(scan?.score ?? scan?.gap);
      if (edge !== null && ts - lastEdgePush.current >= 350) {
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

  const strat = strategyMetrics[STRATEGY_ID] || {};
  const cumulativePnl = parseNumber(strat.pnl) ?? pnlEvents.reduce((sum, e) => sum + e.pnl, 0);
  const trades = strat.daily_trades ?? pnlEvents.length;
  const wins = pnlEvents.filter((e) => e.pnl > 0).length;
  const winRate = pnlEvents.length > 0 ? wins / pnlEvents.length : 0;

  const spot = latestScanMeta?.spot ?? (spotSeries.length ? spotSeries[spotSeries.length - 1].value : null);
  const fairYes = parseNumber(latestScanMeta?.fair_yes);
  const yesAsk = parseNumber(latestScanMeta?.yes_ask);
  const noAsk = parseNumber(latestScanMeta?.no_ask);
  const bestSide = typeof latestScanMeta?.best_side === 'string' ? latestScanMeta.best_side : null;
  const bestPrice = parseNumber(latestScanMeta?.best_price);
  const bestProb = parseNumber(latestScanMeta?.best_prob);
  const kelly = parseNumber(latestScanMeta?.kelly_fraction);
  const bestNetExpected = parseNumber(latestScanMeta?.best_net_expected_roi);
  const minNetExpected = parseNumber(latestScanMeta?.min_expected_net_return);
  const execDecision = latestScanPasses && bestNetExpected !== null && minNetExpected !== null
    ? (bestNetExpected >= minNetExpected ? 'PASS' : 'HOLD')
    : latestScanPasses
      ? 'PASS'
      : 'HOLD';

  const windowLabel = useMemo(() => formatEtWindow(windowStart, windowEnd), [windowStart, windowEnd]);

  const topMetrics = [
    { label: 'BTC', value: spot ? `$${Number(spot).toLocaleString('en-US', { maximumFractionDigits: 2 })}` : '--' },
    { label: 'PNL', value: `${cumulativePnl >= 0 ? '+' : '-'}${formatUsd(Math.abs(cumulativePnl))}`, valueClass: cumulativePnl >= 0 ? 'text-emerald-300' : 'text-rose-300' },
    { label: 'WIN', value: `${(winRate * 100).toFixed(1)}%`, valueClass: winRate >= 0.55 ? 'text-emerald-300' : 'text-gray-300' },
    { label: 'TRADES', value: trades.toLocaleString('en-US') },
    { label: 'OPEN', value: openPosition ? formatUsdCompact(openPosition.notional) : '$0', valueClass: openPosition ? 'text-amber-300' : 'text-gray-300' },
  ] as Array<{ label: string; value: string; valueClass?: string }>;

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div className="flex flex-col gap-3">
          <div className="flex items-center justify-between gap-4 flex-wrap">
            <div>
              <h1 className="text-2xl font-bold bg-gradient-to-r from-emerald-300 to-cyan-300 bg-clip-text text-transparent">
                BTC 5m Execution Loop
              </h1>
              <div className="text-xs font-mono text-gray-500 mt-1">
                Coinbase spot vs Polymarket 5-minute up/down odds. Paper-first. Live mode stays dry-run until posting is armed.
              </div>
            </div>

            <div className="flex items-center gap-3 font-mono text-[10px]">
              <span className={`px-3 py-1 rounded-md border ${mode === 'PAPER' ? 'text-blue-300 border-blue-500/30 bg-blue-500/10' : 'text-red-300 border-red-500/30 bg-red-500/10'}`}>
                {mode === 'PAPER' ? 'PAPER' : liveOrderPostingEnabled ? 'LIVE ARMED' : 'LIVE DRY-RUN'}
              </span>
              <span className="px-3 py-1 rounded-md border border-white/10 bg-white/5 text-gray-300">
                NEXT WINDOW <span className="text-white">{formatCountdown(countdown)}</span>
              </span>
              <span className="px-3 py-1 rounded-md border border-white/10 bg-white/5 text-gray-300">
                MARKET <span className="text-white">{windowLabel}</span>
              </span>
            </div>
          </div>

          <div className="rounded-xl border border-white/10 bg-black/40 backdrop-blur-md px-5 py-3">
            <div className="flex flex-wrap items-center gap-6">
              {topMetrics.map((m) => (
                <div key={m.label} className="flex items-baseline gap-3">
                  <div className="text-[10px] uppercase tracking-widest text-gray-500">{m.label}</div>
                  <div className={`text-sm font-semibold ${m.valueClass || 'text-white'}`}>{m.value}</div>
                </div>
              ))}
            </div>
          </div>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
          <div className="lg:col-span-8 space-y-6">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              <Panel title="BTC/USD (Coinbase)">
                <div className="h-44">
                  <Sparkline
                    points={spotSeries.map((p) => p.value)}
                    strokeClass="text-white/80"
                    fillTop="rgba(255, 255, 255, 0.14)"
                    fillBottom="rgba(255, 255, 255, 0.00)"
                  />
                </div>
                <div className="mt-3 grid grid-cols-3 gap-2 text-[11px] font-mono">
                  <Stat label="Spot" value={spot ? `$${Number(spot).toLocaleString('en-US', { maximumFractionDigits: 2 })}` : '--'} valueClass="text-white" />
                  <Stat label="Sigma" value={latestScanMeta?.sigma_annualized ? `${Number(latestScanMeta.sigma_annualized).toFixed(2)}` : '--'} />
                  <Stat label="Fair YES" value={fairYes !== null ? `${(clamp01(fairYes) * 100).toFixed(2)}%` : '--'} valueClass="text-emerald-200" />
                </div>
              </Panel>

              <Panel title="Equity Curve (Sim Ledger)">
                <div className="h-44">
                  <Sparkline
                    points={equitySeries.map((p) => p.value)}
                    strokeClass="text-emerald-300"
                    fillTop="rgba(16, 185, 129, 0.20)"
                    fillBottom="rgba(16, 185, 129, 0.00)"
                  />
                </div>
                <div className="mt-3 grid grid-cols-3 gap-2 text-[11px] font-mono">
                  <Stat label="Equity" value={equitySeries.length ? formatUsd(equitySeries[equitySeries.length - 1].value) : '--'} valueClass="text-blue-200" />
                  <Stat label="PnL" value={`${cumulativePnl >= 0 ? '+' : '-'}${formatUsd(Math.abs(cumulativePnl))}`} valueClass={cumulativePnl >= 0 ? 'text-emerald-200' : 'text-rose-200'} />
                  <Stat label="Trades" value={`${trades}`} />
                </div>
              </Panel>
            </div>

            <Panel title="Order Feed">
              <div className="h-[320px] overflow-y-auto font-mono text-xs">
                <div className="grid grid-cols-5 gap-2 text-[10px] uppercase tracking-wider text-gray-500 border-b border-white/10 pb-2">
                  <div>Time</div>
                  <div>Side</div>
                  <div>Price</div>
                  <div>Size</div>
                  <div>Why</div>
                </div>

                <div className="divide-y divide-white/5">
                  {execLogs.length === 0 && (
                    <div className="py-6 text-gray-500 text-sm">Waiting for BTC_5M execution events...</div>
                  )}
                  {execLogs.map((log, idx) => {
                    const side = (log.side || '').toUpperCase();
                    const ts = typeof log.timestamp === 'number' ? log.timestamp : Date.now();
                    const price = parseNumber(log.price);
                    const size = parseNumber(log.size);
                    const reason = typeof log?.details?.reason === 'string'
                      ? log.details.reason
                      : typeof log?.details?.position_side === 'string'
                        ? `Position ${log.details.position_side}`
                        : typeof log?.details?.side === 'string'
                          ? `Side ${log.details.side}`
                          : '';

                    const sideClass =
                      side === 'WIN'
                        ? 'text-emerald-300'
                        : side === 'LOSS'
                          ? 'text-rose-300'
                          : side.includes('LIVE_DRY_RUN')
                            ? 'text-amber-300'
                            : 'text-white';

                    return (
                      <div key={`${ts}-${idx}`} className="grid grid-cols-5 gap-2 py-2">
                        <div className="text-gray-500">{new Date(ts).toLocaleTimeString()}</div>
                        <div className={`font-semibold ${sideClass}`}>{side}</div>
                        <div className="text-gray-200">{price !== null && price <= 1 ? formatCents(price) : price !== null ? price.toFixed(2) : '--'}</div>
                        <div className="text-gray-200">{size !== null ? formatUsdCompact(size) : '--'}</div>
                        <div className="text-gray-500 truncate" title={reason}>{reason || '--'}</div>
                      </div>
                    );
                  })}
                </div>
              </div>
            </Panel>
          </div>

          <div className="lg:col-span-4 space-y-6">
            <Panel title="Execution Pipeline">
              <div className="grid grid-cols-1 gap-3 font-mono text-xs">
                <PipelineCard
                  title="01 CEX FEED"
                  rows={[
                    { k: 'Coinbase', v: spot ? `$${Number(spot).toLocaleString('en-US', { maximumFractionDigits: 2 })}` : '--' },
                    { k: 'Window', v: windowLabel },
                  ]}
                />
                <PipelineCard
                  title="02 PM ODDS"
                  rows={[
                    { k: 'YES Ask', v: yesAsk !== null ? formatCents(yesAsk) : '--' },
                    { k: 'NO Ask', v: noAsk !== null ? formatCents(noAsk) : '--' },
                  ]}
                />
                <PipelineCard
                  title="03 EDGE"
                  rows={[
                    { k: 'Best Side', v: bestSide || '--', vClass: bestSide === 'UP' ? 'text-emerald-200' : bestSide === 'DOWN' ? 'text-rose-200' : 'text-gray-200' },
                    { k: 'Net EV', v: formatBps(bestNetExpected), vClass: bestNetExpected !== null && bestNetExpected < 0 ? 'text-rose-200' : 'text-emerald-200' },
                  ]}
                />
                <PipelineCard
                  title="04 KELLY (ADVISORY)"
                  rows={[
                    { k: 'Kelly f*', v: kelly !== null ? `${(clamp01(kelly) * 100).toFixed(1)}%` : '--' },
                    { k: 'Fair Prob', v: bestProb !== null ? `${(clamp01(bestProb) * 100).toFixed(2)}%` : '--' },
                    { k: 'Entry', v: bestPrice !== null ? formatCents(bestPrice) : '--' },
                  ]}
                />
                <PipelineCard
                  title="05 EXEC"
                  rows={[
                    { k: 'Decision', v: execDecision, vClass: execDecision === 'PASS' ? 'text-emerald-300' : 'text-gray-300' },
                    { k: 'Mode', v: mode === 'PAPER' ? 'PAPER' : liveOrderPostingEnabled ? 'LIVE ARMED' : 'LIVE DRY-RUN' },
                  ]}
                />
              </div>
            </Panel>

            <Panel title="Signal Flow Heatmap">
              <div className="h-40">
                <Heatmap values={edgeSeries.map((p) => p.value)} />
              </div>
              <div className="mt-2 text-[10px] text-gray-500 font-mono">
                Each cell = expected net return (bps) from the BTC_5M scanner update stream.
              </div>
            </Panel>

            <Panel title="Positions">
              <div className="font-mono text-xs">
                {!openPosition ? (
                  <div className="text-gray-500">No open position.</div>
                ) : (
                  <div className="space-y-2">
                    <div className="flex justify-between">
                      <span className="text-gray-500">Side</span>
                      <span className={openPosition.side === 'UP' ? 'text-emerald-300 font-semibold' : 'text-rose-300 font-semibold'}>
                        {openPosition.side}
                      </span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500">Notional</span>
                      <span className="text-white">{formatUsd(openPosition.notional)}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500">Entry</span>
                      <span className="text-white">{formatCents(openPosition.entryPrice)}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-500">Opened</span>
                      <span className="text-gray-300">{new Date(openPosition.timestamp).toLocaleTimeString()}</span>
                    </div>
                  </div>
                )}
              </div>
            </Panel>
          </div>
        </div>
      </div>
    </DashboardLayout>
  );
};

const Panel = ({ title, children }: { title: string; children: React.ReactNode }) => (
  <section className="bg-black/40 backdrop-blur-md border border-white/10 rounded-xl p-5">
    <div className="flex items-center justify-between mb-3">
      <h2 className="text-xs uppercase tracking-widest text-gray-500 font-mono">{title}</h2>
    </div>
    {children}
  </section>
);

const Stat = ({ label, value, valueClass }: { label: string; value: string; valueClass?: string }) => (
  <div className="rounded border border-white/10 bg-black/30 p-2">
    <div className="text-gray-500 text-[10px] uppercase tracking-widest">{label}</div>
    <div className={`mt-1 text-sm font-semibold ${valueClass || 'text-gray-200'}`}>{value}</div>
  </div>
);

const PipelineCard = ({
  title,
  rows,
}: {
  title: string;
  rows: Array<{ k: string; v: string; vClass?: string }>;
}) => (
  <div className="rounded-lg border border-white/10 bg-black/30 p-3">
    <div className="text-[10px] uppercase tracking-widest text-gray-500 mb-2">{title}</div>
    <div className="space-y-1">
      {rows.map((row) => (
        <div key={row.k} className="flex items-center justify-between gap-3">
          <div className="text-gray-500">{row.k}</div>
          <div className={`text-gray-200 ${row.vClass || ''}`}>{row.v}</div>
        </div>
      ))}
    </div>
  </div>
);

const Sparkline = ({
  points,
  strokeClass,
  fillTop,
  fillBottom,
}: {
  points: number[];
  strokeClass: string;
  fillTop: string;
  fillBottom: string;
}) => {
  if (points.length < 2) {
    return <div className="h-full text-xs text-gray-600 font-mono">--</div>;
  }
  const width = 420;
  const height = 170;
  const min = Math.min(...points);
  const max = Math.max(...points);
  const spread = Math.max(1e-9, max - min);
  const poly = points
    .map((v, i) => {
      const x = (i / (points.length - 1)) * width;
      const y = height - ((v - min) / spread) * height;
      return `${x.toFixed(2)},${y.toFixed(2)}`;
    })
    .join(' ');
  const area = `0,${height} ${poly} ${width},${height}`;
  const gradientId = useMemo(() => `spark-${Math.random().toString(16).slice(2)}`, []);

  return (
    <svg width="100%" height="100%" viewBox={`0 0 ${width} ${height}`}>
      <defs>
        <linearGradient id={gradientId} x1="0" x2="0" y1="0" y2="1">
          <stop offset="0%" stopColor={fillTop} />
          <stop offset="100%" stopColor={fillBottom} />
        </linearGradient>
      </defs>
      <polygon points={area} fill={`url(#${gradientId})`} />
      <polyline
        points={poly}
        fill="none"
        stroke="currentColor"
        className={strokeClass}
        strokeWidth="2"
        strokeLinejoin="round"
        strokeLinecap="round"
        opacity="0.95"
      />
    </svg>
  );
};

const Heatmap = ({ values }: { values: number[] }) => {
  const cells = values.slice(-144); // last ~minute of scans (ish)
  const cols = 24;
  const rows = Math.ceil(cells.length / cols);
  const maxAbs = Math.max(1e-9, ...cells.map((v) => Math.abs(v)));

  return (
    <div
      className="grid gap-1"
      style={{
        gridTemplateColumns: `repeat(${cols}, minmax(0, 1fr))`,
        gridTemplateRows: `repeat(${rows}, minmax(0, 1fr))`,
        height: '100%',
      }}
    >
      {cells.map((v, idx) => {
        const intensity = Math.min(1, Math.abs(v) / maxAbs);
        const alpha = 0.15 + intensity * 0.65;
        const bg = v >= 0 ? `rgba(16, 185, 129, ${alpha})` : `rgba(244, 63, 94, ${alpha})`;
        return <div key={idx} className="rounded-sm border border-white/5" style={{ background: bg }} />;
      })}
    </div>
  );
};

function useNowMs(intervalMs: number): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), Math.max(50, intervalMs));
    return () => window.clearInterval(timer);
  }, [intervalMs]);
  return now;
}
