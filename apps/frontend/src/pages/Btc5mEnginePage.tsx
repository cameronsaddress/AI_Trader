import React, { useEffect, useMemo, useRef, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useSocket } from '../context/SocketContext';
import { ExecutionLayout } from '../components/layout/ExecutionLayout';
import { apiUrl } from '../lib/api';
import type {
  TradingModeEvent,
  StrategyMetricsEvent,
  ExecutionLogEvent,
  StrategyPnlEvent,
  ScannerUpdateEvent,
  RiskConfigEvent,
  StrategyRiskMultiplierEvent,
  StrategyRiskMultiplierSnapshot,
  StrategyStatusEvent,
  VaultUpdateEvent,
} from '../types/socket-events';

type TradingMode = 'PAPER' | 'LIVE';

type ExecutionLog = {
  execution_id?: string;
  timestamp: number;
  side?: string;
  market?: string;
  price?: unknown;
  size?: unknown;
  mode?: string;
  details?: Record<string, unknown> | null;
};

type StrategyPnlPayload = {
  execution_id?: string;
  executionId?: string;
  strategy?: string;
  side?: string;
  pnl?: number;
  notional?: number;
  timestamp?: number;
  details?: Record<string, unknown> | null;
};

type StrategyMetrics = Record<string, { pnl?: number; daily_trades?: number; updated_at?: number }>;

type MlFeatureUpdate = {
  symbol?: string;
  timestamp?: number;
  ml_prob_up?: number | null;
  ml_prob_down?: number | null;
  ml_signal_edge?: number | null;
  ml_signal_confidence?: number | null;
  ml_model?: string | null;
  ml_model_ready?: boolean | null;
  [key: string]: unknown;
};

type ModelGateCalibrationState = {
  enabled: boolean;
  advisory_only: boolean;
  active_gate: number;
  recommended_gate: number;
  sample_count: number;
  delta_expected_pnl: number;
  updated_at: number;
  reason: string;
};

type RejectedSignalSummaryState = {
  tracked: number;
  persisted_rows: number;
  by_stage: Record<string, number>;
};

type EntryFreshnessViolationCategory = 'MISSING_TELEMETRY' | 'STALE_SOURCE' | 'STALE_GATE' | 'OTHER';

type EntryFreshnessSloAlert = {
  execution_id: string;
  strategy: string;
  market_key: string | null;
  category: EntryFreshnessViolationCategory;
  reason: string;
  timestamp: number;
};

type EntryFreshnessSloStrategy = {
  strategy: string;
  total: number;
  missing_telemetry: number;
  stale_source: number;
  stale_gate: number;
  other: number;
  last_violation_at: number;
  last_reason: string;
};

type EntryFreshnessSloState = {
  totals: {
    total: number;
    missing_telemetry: number;
    stale_source: number;
    stale_gate: number;
    other: number;
  };
  strategies: Record<string, EntryFreshnessSloStrategy>;
  recent_alerts: EntryFreshnessSloAlert[];
  updated_at: number;
};

type SilentCatchContextValue = string | number | boolean | null;

type SilentCatchTelemetryEntry = {
  count: number;
  first_seen: number;
  last_seen: number;
  last_message: string;
  last_context: Record<string, SilentCatchContextValue>;
};

type SilentCatchTelemetryState = Record<string, SilentCatchTelemetryEntry>;

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
const FLOW_COLS = 108;
// Cross-exchange signal flow rail: one row per wired CEX feed.
const FLOW_ROWS: Array<{ id: string; label: string; key: string }> = [
  { id: 'BIN', label: 'BIN', key: 'binance' },
  { id: 'CB', label: 'CB', key: 'coinbase' },
  { id: 'OKX', label: 'OKX', key: 'okx' },
  { id: 'BYB', label: 'BYB', key: 'bybit' },
  { id: 'KRK', label: 'KRK', key: 'kraken' },
  { id: 'BFX', label: 'BFX', key: 'bitfinex' },
];

function computeMedian(values: number[]): number | null {
  const clean = values.filter((v) => Number.isFinite(v));
  if (clean.length === 0) return null;
  const sorted = [...clean].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) return sorted[mid] as number;
  const a = sorted[mid - 1] as number;
  const b = sorted[mid] as number;
  return (a + b) / 2;
}

function parseNumber(value: unknown): number | null {
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : null;
  }
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (trimmed.length === 0) return null;
    const parsed = Number(trimmed);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function parseText(value: unknown): string | null {
  if (typeof value !== 'string') return null;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function asRecord(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return null;
  }
  return value as Record<string, unknown>;
}

function parseModelGateCalibration(input: unknown): ModelGateCalibrationState {
  const row = asRecord(input);
  if (!row) {
    return {
      enabled: false,
      advisory_only: true,
      active_gate: 0.55,
      recommended_gate: 0.55,
      sample_count: 0,
      delta_expected_pnl: 0,
      updated_at: 0,
      reason: '',
    };
  }
  return {
    enabled: row.enabled === true,
    advisory_only: row.advisory_only !== false,
    active_gate: parseNumber(row.active_gate) ?? 0.55,
    recommended_gate: parseNumber(row.recommended_gate) ?? 0.55,
    sample_count: parseNumber(row.sample_count) ?? 0,
    delta_expected_pnl: parseNumber(row.delta_expected_pnl) ?? 0,
    updated_at: parseNumber(row.updated_at) ?? 0,
    reason: parseText(row.reason) || '',
  };
}

function emptyEntryFreshnessSloState(updatedAt = 0): EntryFreshnessSloState {
  return {
    totals: {
      total: 0,
      missing_telemetry: 0,
      stale_source: 0,
      stale_gate: 0,
      other: 0,
    },
    strategies: {},
    recent_alerts: [],
    updated_at: updatedAt,
  };
}

function parseEntryFreshnessCategory(value: unknown): EntryFreshnessViolationCategory {
  if (value === 'MISSING_TELEMETRY') return value;
  if (value === 'STALE_SOURCE') return value;
  if (value === 'STALE_GATE') return value;
  return 'OTHER';
}

function parseEntryFreshnessSloStrategy(input: unknown, strategyFallback: string): EntryFreshnessSloStrategy | null {
  const row = asRecord(input);
  if (!row) return null;
  const strategy = (parseText(row.strategy) || strategyFallback || 'UNKNOWN').toUpperCase();
  return {
    strategy,
    total: parseNumber(row.total) ?? 0,
    missing_telemetry: parseNumber(row.missing_telemetry) ?? 0,
    stale_source: parseNumber(row.stale_source) ?? 0,
    stale_gate: parseNumber(row.stale_gate) ?? 0,
    other: parseNumber(row.other) ?? 0,
    last_violation_at: parseNumber(row.last_violation_at) ?? 0,
    last_reason: parseText(row.last_reason) || '',
  };
}

function parseEntryFreshnessSloAlert(input: unknown): EntryFreshnessSloAlert | null {
  const row = asRecord(input);
  if (!row) return null;
  const strategy = (parseText(row.strategy) || 'UNKNOWN').toUpperCase();
  const executionId = parseText(row.execution_id) || `${strategy}:${Date.now()}`;
  return {
    execution_id: executionId,
    strategy,
    market_key: parseText(row.market_key),
    category: parseEntryFreshnessCategory(row.category),
    reason: parseText(row.reason) || '',
    timestamp: parseNumber(row.timestamp) ?? Date.now(),
  };
}

function parseEntryFreshnessSloState(input: unknown): EntryFreshnessSloState {
  const row = asRecord(input);
  if (!row) return emptyEntryFreshnessSloState();
  const totals = asRecord(row.totals) || {};
  const strategiesRaw = asRecord(row.strategies) || {};
  const strategies: Record<string, EntryFreshnessSloStrategy> = {};
  Object.entries(strategiesRaw).forEach(([key, value]) => {
    const parsed = parseEntryFreshnessSloStrategy(value, key);
    if (parsed) strategies[parsed.strategy] = parsed;
  });
  const recentAlerts = Array.isArray(row.recent_alerts)
    ? row.recent_alerts
      .map((entry) => parseEntryFreshnessSloAlert(entry))
      .filter((entry): entry is EntryFreshnessSloAlert => entry !== null)
    : [];
  return {
    totals: {
      total: parseNumber(totals.total) ?? 0,
      missing_telemetry: parseNumber(totals.missing_telemetry) ?? 0,
      stale_source: parseNumber(totals.stale_source) ?? 0,
      stale_gate: parseNumber(totals.stale_gate) ?? 0,
      other: parseNumber(totals.other) ?? 0,
    },
    strategies,
    recent_alerts: recentAlerts,
    updated_at: parseNumber(row.updated_at) ?? 0,
  };
}

function parseSilentCatchContext(input: unknown): Record<string, SilentCatchContextValue> {
  const row = asRecord(input);
  if (!row) return {};
  const parsed: Record<string, SilentCatchContextValue> = {};
  Object.entries(row).forEach(([key, value]) => {
    if (value === null || typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
      parsed[key] = value;
    } else {
      parsed[key] = String(value);
    }
  });
  return parsed;
}

function parseSilentCatchTelemetry(input: unknown): SilentCatchTelemetryState {
  const row = asRecord(input);
  if (!row) return {};
  const parsed: SilentCatchTelemetryState = {};
  Object.entries(row).forEach(([scope, value]) => {
    const entry = asRecord(value);
    if (!entry) return;
    parsed[scope] = {
      count: parseNumber(entry.count) ?? 0,
      first_seen: parseNumber(entry.first_seen) ?? 0,
      last_seen: parseNumber(entry.last_seen) ?? 0,
      last_message: parseText(entry.last_message) || '',
      last_context: parseSilentCatchContext(entry.last_context),
    };
  });
  return parsed;
}

function formatAgeMs(timestamp: number, now: number): string {
  if (!Number.isFinite(timestamp) || timestamp <= 0) return '--';
  return `${Math.max(0, now - timestamp)}ms`;
}

function rejectedSignalStrategy(input: unknown): string | null {
  const row = asRecord(input);
  if (!row) return null;
  const direct = parseText(row.strategy);
  if (direct) return direct.toUpperCase();
  const payload = asRecord(row.payload);
  const nested = parseText(payload?.strategy) || parseText(asRecord(payload?.preflight)?.strategy);
  return nested ? nested.toUpperCase() : null;
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
  // Get today's date in ET as an ISO "YYYY-MM-DD" string (en-CA locale).
  const etDateStr = new Date(nowMs).toLocaleDateString('en-CA', { timeZone: 'America/New_York' });
  // Build midnight UTC of that calendar date, then offset to ET.
  const midnightUTC = new Date(etDateStr + 'T00:00:00Z').getTime();
  // EST (UTC-5) → midnight ET = midnightUTC + 5h; EDT (UTC-4) → +4h.
  // Check which offset lands on the correct ET date.
  const est = midnightUTC + 5 * 3_600_000;
  if (new Date(est).toLocaleDateString('en-CA', { timeZone: 'America/New_York' }) === etDateStr) return est;
  return midnightUTC + 4 * 3_600_000;
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

function computeCorrelation(xs: number[], ys: number[]): number | null {
  const n = Math.min(xs.length, ys.length);
  if (n < 10) return null;
  const x = xs.slice(xs.length - n);
  const y = ys.slice(ys.length - n);
  const meanX = x.reduce((a, b) => a + b, 0) / n;
  const meanY = y.reduce((a, b) => a + b, 0) / n;
  let cov = 0;
  let varX = 0;
  let varY = 0;
  for (let i = 0; i < n; i += 1) {
    const dx = x[i] - meanX;
    const dy = y[i] - meanY;
    cov += dx * dy;
    varX += dx * dx;
    varY += dy * dy;
  }
  const denom = Math.sqrt(varX * varY);
  if (denom <= 1e-12) return null;
  return cov / denom;
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

function isProbablyUuid(value: string): boolean {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value);
}

function windowLabelForResolvedTs(tsMs: number): { start: number; end: number; label: string } {
  const epoch = Math.floor(tsMs / 1000);
  // Trades resolve at (or immediately after) the 5m boundary, so treat the boundary as window end.
  const end = epoch - (epoch % 300);
  const start = end - 300;
  return { start, end, label: formatEtWindow(start, end) };
}

export const Btc5mEnginePage: React.FC = () => {
  const { socket } = useSocket();
  const navigate = useNavigate();
  const [mode, setMode] = useState<TradingMode>('PAPER');
  const [liveOrderPostingEnabled, setLiveOrderPostingEnabled] = useState(false);

  const [strategyMetrics, setStrategyMetrics] = useState<StrategyMetrics>({});
  const [latestScanMeta, setLatestScanMeta] = useState<Record<string, unknown> | null>(null);
  const [latestMlFeatures, setLatestMlFeatures] = useState<Record<string, unknown> | null>(null);
  const [latestScanPasses, setLatestScanPasses] = useState(false);

  const [spotSeries, setSpotSeries] = useState<Array<{ timestamp: number; value: number }>>([]);
  const [impliedSeries, setImpliedSeries] = useState<Array<{ timestamp: number; value: number }>>([]);

  const [tradeBlotter, setTradeBlotter] = useState<Record<string, TradeRecord>>({});

  const [flowColumns, setFlowColumns] = useState<number[][]>(
    () => Array.from({ length: FLOW_COLS }, () => Array(FLOW_ROWS.length).fill(0)),
  );
  const flowMaxAbsRef = useRef<number[]>(Array(FLOW_ROWS.length).fill(1e-9));
  const lastGoodSpotRef = useRef<number | null>(null);
  const lastCexSnapshotRef = useRef<Record<string, number>>({});

  const lastSpotPush = useRef(0);
  const lastImpliedPush = useRef(0);

  const nowMs = useNowMs(250);
  const nowSec = Math.floor(nowMs / 1000);
  const windowStart = nowSec - (nowSec % 300);
  const windowEnd = windowStart + 300;
  const countdown = windowEnd - nowSec;
  const windowLabel = useMemo(() => formatEtWindow(windowStart, windowEnd), [windowStart, windowEnd]);

  const [lastScannerUpdate, setLastScannerUpdate] = useState(Date.now());

  const [traceExecutionId, setTraceExecutionId] = useState<string | null>(null);
  const [executionTrace, setExecutionTrace] = useState<ExecutionTrace | null>(null);
  const [executionTraceError, setExecutionTraceError] = useState<string | null>(null);
  const [executionTraceLoading, setExecutionTraceLoading] = useState(false);

  // Engine Tuning panel state
  const [tuningOpen, setTuningOpen] = useState(false);
  const [riskModel, setRiskModel] = useState<'FIXED' | 'PERCENT'>('PERCENT');
  const [riskValue, setRiskValue] = useState(1.7);
  const [strategyMultiplierInfo, setStrategyMultiplierInfo] = useState<{
    multiplier: number; sample_count: number; ema_return: number;
  } | null>(null);
  const [strategyStatuses, setStrategyStatuses] = useState<Record<string, boolean>>({});
  const [vaultBalance, setVaultBalance] = useState(0);
  const [modelGateCalibration, setModelGateCalibration] = useState<ModelGateCalibrationState>({
    enabled: false,
    advisory_only: true,
    active_gate: 0.55,
    recommended_gate: 0.55,
    sample_count: 0,
    delta_expected_pnl: 0,
    updated_at: 0,
    reason: '',
  });
  const [rejectedSignalSummary, setRejectedSignalSummary] = useState<RejectedSignalSummaryState>({
    tracked: 0,
    persisted_rows: 0,
    by_stage: {},
  });
  const [entryFreshnessSlo, setEntryFreshnessSlo] = useState<EntryFreshnessSloState>(emptyEntryFreshnessSloState());
  const [silentCatchTelemetry, setSilentCatchTelemetry] = useState<SilentCatchTelemetryState>({});
  const [strategyQualityTotalScans, setStrategyQualityTotalScans] = useState(0);

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
      const response = await fetch(apiUrl(`/api/arb/execution-trace/${encodeURIComponent(executionId)}`));
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

    const handleTradingMode = (payload: TradingModeEvent) => {
      if (payload?.mode === 'PAPER' || payload?.mode === 'LIVE') {
        setMode(payload.mode);
      }
      if (typeof payload?.live_order_posting_enabled === 'boolean') {
        setLiveOrderPostingEnabled(payload.live_order_posting_enabled);
      }
    };

    const handleStrategyMetrics = (payload: StrategyMetricsEvent) => {
      if (!payload || typeof payload !== 'object') return;
      setStrategyMetrics(payload as StrategyMetrics);
    };

    const handleExecutionLog = (payload: ExecutionLogEvent) => {
      if (!payload || typeof payload !== 'object') return;
      const log = payload as ExecutionLog;

      const details = asRecord(log?.details);
      const detailsPreflight = asRecord(details?.preflight);
      const detailsStrategy = parseText(detailsPreflight?.strategy) || parseText(details?.strategy);
      const isBtc5m = detailsStrategy === STRATEGY_ID || log?.market === 'BTC 5m Engine';
      if (!isBtc5m) return;

      const ts = typeof log.timestamp === 'number' ? log.timestamp : Date.now();
      const executionId = parseText(log.execution_id)
        || parseText(details?.execution_id)
        || parseText(details?.executionId)
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

    const handleStrategyPnl = (payload: StrategyPnlEvent) => {
      if (!payload || typeof payload !== 'object') return;
      const parsed = payload as StrategyPnlPayload;
      if (parsed?.strategy !== STRATEGY_ID) return;
      const details = asRecord(parsed?.details);

      const executionId = parseText(parsed.execution_id)
        || parseText(parsed.executionId)
        || parseText(details?.execution_id)
        || parseText(details?.executionId)
        || null;
      if (!executionId) return;

      const ts = parseNumber(parsed.timestamp) ?? Date.now();
      const pnl = parseNumber(parsed.pnl);
      const notional = parseNumber(parsed.notional) ?? parseNumber(details?.notional) ?? null;
      const direction = safeDirection(parsed.side)
        ?? safeDirection(details?.side)
        ?? safeDirection(details?.position_side)
        ?? null;
      const entryPrice = parseNumber(details?.entry_price);
      const windowStartTs = parseNumber(details?.window_start_ts);
      const holdMs = parseNumber(details?.hold_ms);

      setTradeBlotter((prev) => {
        const existing = prev[executionId];
        const fallbackLabel = existing?.window_label
          || (windowStartTs !== null ? formatEtWindow(windowStartTs, windowStartTs + 300) : windowLabelForTs(ts).label);
        const inferredEntryTs = holdMs !== null && holdMs > 0 ? ts - holdMs : null;
        const next: TradeRecord = {
          execution_id: executionId,
          direction: existing?.direction ?? direction,
          window_label: fallbackLabel,
          entry_ts: existing?.entry_ts ?? (inferredEntryTs !== null ? inferredEntryTs : ts),
          entry_price: existing?.entry_price ?? entryPrice ?? null,
          notional: existing?.notional ?? (notional ?? 0),
          exit_ts: existing?.exit_ts ?? ts,
          pnl: pnl ?? existing?.pnl ?? null,
          status: 'RESOLVED',
        };
        return { ...prev, [executionId]: next };
      });
    };

    const handleScannerUpdate = (payload: ScannerUpdateEvent) => {
      if (!payload || typeof payload !== 'object') return;
      const scan = payload as ScannerUpdateEvent;
      if (scan?.strategy !== STRATEGY_ID) return;

      const ts = typeof scan.timestamp === 'number' ? scan.timestamp : Date.now();
      setLastScannerUpdate(Date.now());
      setLatestScanPasses(Boolean(scan?.passes_threshold));

      const meta = scan?.meta ?? {};
      const rawCexPrices = meta?.cex_prices && typeof meta.cex_prices === 'object' && !Array.isArray(meta.cex_prices)
        ? meta.cex_prices as Record<string, unknown>
        : null;
      const parsedCexPrices: Record<string, number> = {};
      if (rawCexPrices) {
        Object.entries(rawCexPrices).forEach(([k, v]) => {
          const n = parseNumber(v);
          if (n !== null && n > 1_000 && n < 500_000) {
            parsedCexPrices[k] = n;
          }
        });
      }
      const cexMid = parseNumber(meta?.cex_mid) ?? computeMedian(Object.values(parsedCexPrices));

      // Prefer cross-exchange median as the "aggregated" spot for UI charts.
      const spot = cexMid ?? parseNumber(meta?.spot ?? scan?.prices?.[0]);
      if (spot !== null) {
        const withinBounds = spot > 1_000 && spot < 500_000;
        const last = lastGoodSpotRef.current;
        const saneJump = last === null ? true : Math.abs(spot - last) / Math.max(1e-9, last) < 0.08;
        if (withinBounds && saneJump) {
          lastGoodSpotRef.current = spot;
          if (ts - lastSpotPush.current >= 800) {
            lastSpotPush.current = ts;
            setSpotSeries((prev) => [...prev, { timestamp: ts, value: spot }].slice(-SERIES_MAX_POINTS));
          }
        }
      }

      const implied = parseNumber(scan?.meta?.yes_ask);
      if (implied !== null && ts - lastImpliedPush.current >= 800) {
        lastImpliedPush.current = ts;
        setImpliedSeries((prev) => [...prev, { timestamp: ts, value: implied }].slice(-SERIES_MAX_POINTS));
      }

      if (scan?.meta) {
        setLatestScanMeta({ ...scan.meta, cex_mid: cexMid, cex_prices: parsedCexPrices });
      }

      // Hydrate the currently open position from scanner telemetry, so a page refresh mid-window
      // doesn't leave the POSITIONS rail empty until the next ENTRY/SETTLEMENT execution event.
      const openPos = meta?.open_position;
      const openPosRecord = asRecord(openPos);
      if (openPosRecord) {
        const execId = parseText(openPosRecord.execution_id)
          || parseText(openPosRecord.executionId)
          || parseText(openPosRecord.id);
        const dir = safeDirection(openPosRecord.side);
        const entryPrice = parseNumber(openPosRecord.entry_price);
        const notional = parseNumber(openPosRecord.notional) ?? parseNumber(openPosRecord.size) ?? 0;
        const entryTs = parseNumber(openPosRecord.entry_ts_ms) ?? ts;
        const windowStartTs = parseNumber(openPosRecord.window_start_ts);
        const label = windowStartTs !== null ? formatEtWindow(windowStartTs, windowStartTs + 300) : windowLabelForTs(entryTs).label;
        if (execId) {
          setTradeBlotter((prev) => {
            const existing = prev[execId];
            if (existing?.status === 'RESOLVED') {
              return prev;
            }
            const next: TradeRecord = {
              execution_id: execId,
              direction: existing?.direction ?? dir,
              window_label: existing?.window_label ?? label,
              entry_ts: existing?.entry_ts ?? entryTs,
              entry_price: existing?.entry_price ?? entryPrice,
              notional: existing?.notional ?? notional,
              exit_ts: null,
              pnl: existing?.pnl ?? null,
              status: 'OPEN',
            };
            return { ...prev, [execId]: next };
          });
        }
      }

      // Stream the signal-flow matrix right-to-left by pushing one column per scan.
      // Each row is the exchange premium (bps) vs the median spot.
      const denom = cexMid !== null && cexMid > 0 ? cexMid : null;
      const column = FLOW_ROWS.map((row) => {
        const px = parsedCexPrices[row.key];
        if (!denom || !px) return 0;
        return ((px - denom) / denom) * 10_000;
      });

      flowMaxAbsRef.current = flowMaxAbsRef.current.map((prevMax, idx) => {
        const decayed = Math.max(1e-9, prevMax * 0.992);
        return Math.max(decayed, Math.abs(column[idx] ?? 0) || 0);
      });

      setFlowColumns((prevCols) => {
        const next = prevCols.length >= FLOW_COLS ? prevCols.slice(1) : prevCols.slice();
        next.push(column);
        return next;
      });

      lastCexSnapshotRef.current = parsedCexPrices;
    };

    const handleMlFeaturesUpdate = (payload: MlFeatureUpdate) => {
      if (!payload || typeof payload !== 'object') return;
      if (payload.symbol !== 'BTC-USD') return;
      setLatestMlFeatures(payload as Record<string, unknown>);
    };

    // Engine Tuning panel listeners
    const handleRiskConfig = (cfg: RiskConfigEvent) => {
      if (cfg?.model === 'FIXED' || cfg?.model === 'PERCENT') setRiskModel(cfg.model);
      if (typeof cfg?.value === 'number') setRiskValue(cfg.value);
    };
    const handleMultiplierUpdate = (payload: StrategyRiskMultiplierEvent) => {
      if (payload?.strategy === STRATEGY_ID) {
        setStrategyMultiplierInfo({
          multiplier: payload.multiplier ?? 1,
          sample_count: payload.sample_count ?? 0,
          ema_return: payload.ema_return ?? 0,
        });
      }
    };
    const handleMultiplierSnapshot = (snapshot: StrategyRiskMultiplierSnapshot) => {
      const entry = snapshot?.[STRATEGY_ID];
      if (entry) {
        setStrategyMultiplierInfo({
          multiplier: entry.multiplier ?? 1,
          sample_count: entry.sample_count ?? 0,
          ema_return: entry.ema_return ?? 0,
        });
      }
    };
    const handleStrategyStatus = (statusMap: StrategyStatusEvent) => {
      setStrategyStatuses(statusMap);
    };

    socket.on('trading_mode_update', handleTradingMode);
    socket.on('strategy_metrics_update', handleStrategyMetrics);
    socket.on('execution_log', handleExecutionLog);
    socket.on('strategy_pnl', handleStrategyPnl);
    socket.on('scanner_update', handleScannerUpdate);
    socket.on('risk_config_update', handleRiskConfig);
    socket.on('strategy_risk_multiplier_update', handleMultiplierUpdate);
    socket.on('strategy_risk_multiplier_snapshot', handleMultiplierSnapshot);
    socket.on('strategy_status_update', handleStrategyStatus);
    socket.on('ml_features_update', handleMlFeaturesUpdate);
    const handleVault = (data: VaultUpdateEvent) => {
      if (typeof data?.vault === 'number') setVaultBalance(data.vault);
    };
    const handleModelGateCalibration = (payload: unknown) => {
      setModelGateCalibration(parseModelGateCalibration(payload));
    };
    const handleRejectedSignal = (payload: unknown) => {
      const row = asRecord(payload);
      const stage = parseText(row?.stage)?.toUpperCase();
      if (!stage || rejectedSignalStrategy(row) !== STRATEGY_ID) return;
      setRejectedSignalSummary((prev) => ({
        ...prev,
        tracked: prev.tracked + 1,
        by_stage: {
          ...prev.by_stage,
          [stage]: (prev.by_stage[stage] ?? 0) + 1,
        },
      }));
    };
    const handleRejectedSignalSnapshot = (payload: unknown) => {
      if (!Array.isArray(payload)) return;
      const byStage: Record<string, number> = {};
      payload.forEach((entry) => {
        const row = asRecord(entry);
        const stage = parseText(row?.stage)?.toUpperCase();
        if (!stage || rejectedSignalStrategy(row) !== STRATEGY_ID) return;
        byStage[stage] = (byStage[stage] ?? 0) + 1;
      });
      const tracked = Object.values(byStage).reduce((sum, value) => sum + value, 0);
      setRejectedSignalSummary((prev) => ({
        ...prev,
        tracked,
        by_stage: byStage,
      }));
    };
    const handleEntryFreshnessSloUpdate = (payload: unknown) => {
      setEntryFreshnessSlo(parseEntryFreshnessSloState(payload));
    };
    const handleEntryFreshnessAlert = (payload: unknown) => {
      const alert = parseEntryFreshnessSloAlert(payload);
      if (!alert) return;
      setEntryFreshnessSlo((prev) => ({
        ...prev,
        recent_alerts: [
          alert,
          ...prev.recent_alerts.filter((entry) => !(
            entry.execution_id === alert.execution_id
            && entry.timestamp === alert.timestamp
          )),
        ].slice(0, 150),
        updated_at: alert.timestamp,
      }));
    };
    const handleSilentCatchTelemetrySnapshot = (payload: unknown) => {
      setSilentCatchTelemetry(parseSilentCatchTelemetry(payload));
    };
    const handleSilentCatchTelemetryUpdate = (payload: unknown) => {
      const row = asRecord(payload);
      const scope = parseText(row?.scope);
      if (!scope || !row) return;
      const parsed = parseSilentCatchTelemetry({ [scope]: row });
      if (!parsed[scope]) return;
      setSilentCatchTelemetry((prev) => ({
        ...prev,
        [scope]: parsed[scope],
      }));
    };
    const handleSimulationReset = () => {
      setTradeBlotter({});
      setSpotSeries([]);
      setImpliedSeries([]);
      setLatestScanMeta(null);
      setLatestMlFeatures(null);
      setLatestScanPasses(false);
      setEntryFreshnessSlo(emptyEntryFreshnessSloState(Date.now()));
      setRejectedSignalSummary({
        tracked: 0,
        persisted_rows: 0,
        by_stage: {},
      });
      setStrategyMetrics((prev) => ({
        ...prev,
        [STRATEGY_ID]: {
          pnl: 0,
          daily_trades: 0,
        },
      }));
      setStrategyQualityTotalScans(0);
      setLastScannerUpdate(Date.now());
      setVaultBalance(0);
      flowMaxAbsRef.current = Array(FLOW_ROWS.length).fill(1e-9);
      lastGoodSpotRef.current = null;
      lastCexSnapshotRef.current = {};
      lastSpotPush.current = 0;
      lastImpliedPush.current = 0;
      setFlowColumns(Array.from({ length: FLOW_COLS }, () => Array(FLOW_ROWS.length).fill(0)));
    };
    socket.on('vault_update', handleVault);
    socket.on('model_gate_calibration_update', handleModelGateCalibration);
    socket.on('rejected_signal', handleRejectedSignal);
    socket.on('rejected_signal_snapshot', handleRejectedSignalSnapshot);
    socket.on('entry_freshness_alert', handleEntryFreshnessAlert);
    socket.on('entry_freshness_slo_update', handleEntryFreshnessSloUpdate);
    socket.on('silent_catch_telemetry_snapshot', handleSilentCatchTelemetrySnapshot);
    socket.on('silent_catch_telemetry_update', handleSilentCatchTelemetryUpdate);
    socket.on('simulation_reset', handleSimulationReset);

    const requestAllState = () => {
      socket.emit('request_trading_mode');
      socket.emit('request_risk_config');
      socket.emit('request_vault');
      socket.emit('request_strategy_metrics');
      socket.emit('request_model_gate_calibration');
      socket.emit('request_rejected_signals', { limit: 200, strategy: STRATEGY_ID });
    };
    socket.on('connect', requestAllState);
    requestAllState();

    return () => {
      socket.off('trading_mode_update', handleTradingMode);
      socket.off('strategy_metrics_update', handleStrategyMetrics);
      socket.off('execution_log', handleExecutionLog);
      socket.off('strategy_pnl', handleStrategyPnl);
      socket.off('scanner_update', handleScannerUpdate);
      socket.off('risk_config_update', handleRiskConfig);
      socket.off('strategy_risk_multiplier_update', handleMultiplierUpdate);
      socket.off('strategy_risk_multiplier_snapshot', handleMultiplierSnapshot);
      socket.off('strategy_status_update', handleStrategyStatus);
      socket.off('ml_features_update', handleMlFeaturesUpdate);
      socket.off('vault_update', handleVault);
      socket.off('model_gate_calibration_update', handleModelGateCalibration);
      socket.off('rejected_signal', handleRejectedSignal);
      socket.off('rejected_signal_snapshot', handleRejectedSignalSnapshot);
      socket.off('entry_freshness_alert', handleEntryFreshnessAlert);
      socket.off('entry_freshness_slo_update', handleEntryFreshnessSloUpdate);
      socket.off('silent_catch_telemetry_snapshot', handleSilentCatchTelemetrySnapshot);
      socket.off('silent_catch_telemetry_update', handleSilentCatchTelemetryUpdate);
      socket.off('simulation_reset', handleSimulationReset);
      socket.off('connect', requestAllState);
    };
  }, [socket]);

  useEffect(() => {
    let active = true;
    const fetchStats = async () => {
      try {
        const res = await fetch(apiUrl('/api/arb/stats'));
        if (!res.ok) return;
        const json = await res.json() as {
          trading_mode?: TradingMode;
          live_order_posting_enabled?: boolean;
          model_gate_calibration?: unknown;
          entry_freshness_slo?: unknown;
          silent_catch_telemetry?: unknown;
          strategy_quality?: Record<string, { total_scans?: unknown; updated_at?: unknown }>;
          scanner_last_heartbeat_ms?: unknown;
        };
        if (!active) return;
        const tradingMode = json?.trading_mode;
        if (tradingMode === 'PAPER' || tradingMode === 'LIVE') {
          setMode(tradingMode);
        }
        if (typeof json?.live_order_posting_enabled === 'boolean') {
          setLiveOrderPostingEnabled(json.live_order_posting_enabled);
        }
        setModelGateCalibration(parseModelGateCalibration(json?.model_gate_calibration));
        setEntryFreshnessSlo(parseEntryFreshnessSloState(json?.entry_freshness_slo));
        setSilentCatchTelemetry(parseSilentCatchTelemetry(json?.silent_catch_telemetry));
        const strategyTotalScans = parseNumber(json?.strategy_quality?.[STRATEGY_ID]?.total_scans);
        if (strategyTotalScans !== null) {
          setStrategyQualityTotalScans(strategyTotalScans);
        }
        const strategyUpdatedAt = parseNumber(json?.strategy_quality?.[STRATEGY_ID]?.updated_at);
        const scannerHeartbeat = parseNumber(json?.scanner_last_heartbeat_ms);
        const fallbackTs = strategyUpdatedAt ?? scannerHeartbeat;
        if (fallbackTs !== null && fallbackTs > 0) {
          setLastScannerUpdate((prev) => Math.max(prev, fallbackTs));
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

  useEffect(() => {
    let cancelled = false;
    const loadTradeHistory = async () => {
      try {
        const res = await fetch(apiUrl(`/api/arb/strategy-trades?strategy=${encodeURIComponent(STRATEGY_ID)}&limit=600`));
        if (!res.ok) return;
        const payload = await res.json() as { trades?: unknown[] };
        const rows = Array.isArray(payload?.trades) ? payload.trades : [];
        if (cancelled || rows.length === 0) return;
        setTradeBlotter((prev) => {
          const next = { ...prev };
          rows.forEach((row, idx) => {
            const parsed = asRecord(row);
            if (!parsed) {
              return;
            }
            const ts = parseNumber(parsed.timestamp) ?? Date.now();
            const pnl = parseNumber(parsed.pnl);
            const notional = parseNumber(parsed.notional) ?? 0;
            const executionId = parseText(parsed.execution_id) || `hist-${ts}-${idx}`;
            const resolvedWindow = windowLabelForResolvedTs(ts);
            next[executionId] = {
              execution_id: executionId,
              direction: safeDirection(parsed.side) ?? null,
              window_label: resolvedWindow.label,
              entry_ts: resolvedWindow.start * 1000,
              entry_price: parseNumber(parsed.entry_price),
              notional,
              exit_ts: ts,
              pnl: pnl ?? null,
              status: 'RESOLVED',
            };
          });
          return next;
        });
      } catch {
        // ignore
      }
    };

    void loadTradeHistory();
    return () => {
      cancelled = true;
    };
  }, []);

  const cexMid = parseNumber(latestScanMeta?.cex_mid);
  const spot = cexMid ?? parseNumber(latestScanMeta?.spot) ?? (spotSeries.length ? spotSeries[spotSeries.length - 1].value : null);
  const cexPrices = useMemo(() => {
    const raw = latestScanMeta?.cex_prices;
    if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return {} as Record<string, number>;
    const out: Record<string, number> = {};
    Object.entries(raw as Record<string, unknown>).forEach(([k, v]) => {
      const n = parseNumber(v);
      if (n !== null) out[k] = n;
    });
    return out;
  }, [latestScanMeta]);
  const fairYes = parseNumber(latestScanMeta?.fair_yes);
  const yesAsk = parseNumber(latestScanMeta?.yes_ask);
  const noAsk = parseNumber(latestScanMeta?.no_ask);
  const bestSide = safeDirection(latestScanMeta?.best_side);
  const bestPrice = parseNumber(latestScanMeta?.best_price);
  const bestProb = parseNumber(latestScanMeta?.best_prob);
  const bestNetExpected = parseNumber(latestScanMeta?.best_net_expected_roi);
  const kelly = parseNumber(latestScanMeta?.kelly_fraction);
  const sigma = parseNumber(latestScanMeta?.sigma_annualized);
  const mlProbUp = parseNumber(latestScanMeta?.ml_prob_up ?? latestMlFeatures?.ml_prob_up);
  const mlSignalSideProb = parseNumber(latestScanMeta?.model_signal_side_prob);
  const mlSignalConfidence = parseNumber(latestScanMeta?.model_signal_confidence)
    ?? parseNumber(latestScanMeta?.ml_signal_confidence_raw)
    ?? parseNumber(latestScanMeta?.ml_signal_confidence)
    ?? parseNumber(latestMlFeatures?.ml_signal_confidence);
  const mlSignalEdge = parseNumber(latestScanMeta?.model_signal_edge)
    ?? parseNumber(latestScanMeta?.ml_signal_edge_raw)
    ?? parseNumber(latestScanMeta?.ml_signal_edge)
    ?? parseNumber(latestMlFeatures?.ml_signal_edge);
  const mlModelName = parseText(latestScanMeta?.model_signal_model)
    || parseText(latestScanMeta?.ml_model)
    || parseText(latestMlFeatures?.ml_model)
    || 'NONE';
  const mlModelReady = typeof latestScanMeta?.model_signal_ready === 'boolean'
    ? latestScanMeta.model_signal_ready
    : typeof latestScanMeta?.ml_model_ready === 'boolean'
      ? latestScanMeta.ml_model_ready
      : typeof latestMlFeatures?.ml_model_ready === 'boolean'
        ? latestMlFeatures.ml_model_ready
      : false;
  const mlSignalLabel = parseText(latestScanMeta?.model_signal_label)
    || (mlModelReady ? 'MODEL_READY' : mlModelName !== 'NONE' ? 'MODEL_WARMUP' : 'NONE');
  const mlSignalBonusBps = parseNumber(latestScanMeta?.model_signal_bonus_bps);
  const mlSignalPenaltyBps = parseNumber(latestScanMeta?.model_signal_penalty_bps);
  const ddLimitPct = parseNumber(latestScanMeta?.max_drawdown_pct) ?? -5.0;

  const corr = useMemo(() => {
    const n = Math.min(spotSeries.length, impliedSeries.length, 120);
    if (n < 24) return null;
    const spotVals = spotSeries.slice(-n).map((p) => p.value);
    const impliedVals = impliedSeries.slice(-n).map((p) => p.value);
    const spotRet: number[] = [];
    const impliedChg: number[] = [];
    for (let i = 1; i < n; i += 1) {
      const s0 = spotVals[i - 1];
      const s1 = spotVals[i];
      if (!Number.isFinite(s0) || !Number.isFinite(s1) || s0 <= 0 || s1 <= 0) continue;
      spotRet.push(Math.log(s1 / s0));
      impliedChg.push(impliedVals[i] - impliedVals[i - 1]);
    }
    return computeCorrelation(spotRet, impliedChg);
  }, [spotSeries, impliedSeries]);

  const strat = strategyMetrics[STRATEGY_ID] || {};
  const allTradesSorted = useMemo(
    () => Object.values(tradeBlotter).sort((a, b) => b.entry_ts - a.entry_ts),
    [tradeBlotter],
  );
  const closedTrades = useMemo(
    () => allTradesSorted.filter((t) => t.status !== 'OPEN'),
    [allTradesSorted],
  );
  const closedTradePnl = useMemo(
    () => closedTrades.reduce((sum, t) => sum + (t.pnl ?? 0), 0),
    [closedTrades],
  );
  const cumulativePnl = parseNumber(strat.pnl) ?? closedTradePnl;
  const trades = closedTrades.length;
  const wins = closedTrades.filter((t) => (t.pnl ?? 0) > 0).length;
  const winRate = closedTrades.length > 0 ? wins / closedTrades.length : 0;

  const todayStartMs = etMidnightEpochMs(nowMs);
  const todayPnl = closedTrades
    .filter((t) => (t.exit_ts ?? 0) >= todayStartMs)
    .reduce((sum, t) => sum + (t.pnl ?? 0), 0);

  const equitySeries = useMemo(() => {
    const ordered = [...closedTrades]
      .filter((t) => (t.exit_ts ?? 0) > 0)
      .sort((a, b) => (a.exit_ts ?? 0) - (b.exit_ts ?? 0));
    let equity = DEFAULT_SIM_BANKROLL;
    const series = ordered.map((t) => {
      equity += t.pnl ?? 0;
      return { timestamp: t.exit_ts ?? t.entry_ts, value: equity };
    });
    if (series.length === 0) {
      return [{ timestamp: Date.now(), value: DEFAULT_SIM_BANKROLL }];
    }
    return series.slice(-SERIES_MAX_POINTS);
  }, [closedTrades]);

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
  const modelGateDeltaBps = (modelGateCalibration.recommended_gate - modelGateCalibration.active_gate) * 10_000;
  const modelGateBlocks = rejectedSignalSummary.by_stage.MODEL_GATE ?? 0;
  const liveExecBlocks = rejectedSignalSummary.by_stage.LIVE_EXECUTION ?? 0;
  const strategyFreshness = entryFreshnessSlo.strategies[STRATEGY_ID]
    || entryFreshnessSlo.strategies[STRATEGY_ID.toUpperCase()]
    || null;
  const freshnessDenominator = Math.max(
    strategyQualityTotalScans,
    strategyFreshness?.total ?? 0,
  );
  const entryFreshnessRatePct = freshnessDenominator > 0
    ? ((strategyFreshness?.total ?? 0) / freshnessDenominator) * 100
    : 0;
  const entryFreshnessSeverity = entryFreshnessRatePct >= 2
    ? 'CRITICAL'
    : entryFreshnessRatePct >= 0.5
      ? 'WARN'
      : 'HEALTHY';
  const latestEntryFreshnessAlert = [...entryFreshnessSlo.recent_alerts]
    .filter((alert) => alert.strategy === STRATEGY_ID)
    .sort((a, b) => b.timestamp - a.timestamp)[0] || null;
  const silentCatchRows = Object.entries(silentCatchTelemetry)
    .map(([scope, entry]) => ({ scope, ...entry }))
    .sort((a, b) => b.last_seen - a.last_seen)
    .slice(0, 3);
  const silentCatchTotal = silentCatchRows.reduce((sum, row) => sum + row.count, 0);
  const staleSourceCount = strategyFreshness?.stale_source ?? 0;
  const staleGateCount = strategyFreshness?.stale_gate ?? 0;
  const missingTelemetryCount = strategyFreshness?.missing_telemetry ?? 0;

  const orderFeed = allTradesSorted.slice(0, ORDER_FEED_ROWS);
  const positions = allTradesSorted.slice(0, POSITIONS_ROWS);

  const layoutDebugEnabled = useMemo(() => {
    try {
      return new URLSearchParams(window.location.search).has('debug_layout');
    } catch {
      return false;
    }
  }, []);
  const layoutRefs = {
    outer: useRef<HTMLDivElement | null>(null),
    left: useRef<HTMLDivElement | null>(null),
    top: useRef<HTMLDivElement | null>(null),
    bottom: useRef<HTMLDivElement | null>(null),
    right: useRef<HTMLDivElement | null>(null),
  };
  const [layoutDebugInfo, setLayoutDebugInfo] = useState<Record<string, unknown> | null>(null);

  useEffect(() => {
    if (!layoutDebugEnabled) return;
    console.log('[BTC5M] layout version=2026-02-13.1');
    const sample = () => {
      const rect = (el: HTMLElement | null) => {
        if (!el) return null;
        const r = el.getBoundingClientRect();
        return {
          w: Math.round(r.width),
          h: Math.round(r.height),
          top: Math.round(r.top),
          left: Math.round(r.left),
          scrollH: el.scrollHeight,
          clientH: el.clientHeight,
        };
      };
      setLayoutDebugInfo({
        pathname: window.location.pathname,
        search: window.location.search,
        inner: { w: window.innerWidth, h: window.innerHeight, dpr: window.devicePixelRatio },
        outer: rect(layoutRefs.outer.current),
        left: rect(layoutRefs.left.current),
        top: rect(layoutRefs.top.current),
        bottom: rect(layoutRefs.bottom.current),
        right: rect(layoutRefs.right.current),
      });
    };
    sample();
    const t = window.setInterval(sample, 750);
    window.addEventListener('resize', sample);
    return () => {
      window.clearInterval(t);
      window.removeEventListener('resize', sample);
    };
  }, [layoutDebugEnabled]);

  return (
    <ExecutionLayout>
      <div className="h-full w-full flex flex-col" data-btc5m-layout="2026-02-13.1">
        <TopBar
          btcPrice={formatBtcPrice(spot)}
          pnl={cumulativePnl}
          today={todayPnl}
          winRate={winRate}
          trades={trades}
          openNotional={openNotional}
          vault={vaultBalance}
          countdown={formatCountdown(countdown)}
          windowLabel={windowLabel}
          onExit={() => navigate('/polymarket')}
        />

        {nowMs - lastScannerUpdate > 60000 && (
          <div className="bg-red-900/50 text-red-300 px-3 py-1 text-sm rounded mx-3 mt-1">
            SCANNER OFFLINE — No updates for {Math.round((nowMs - lastScannerUpdate) / 1000)}s
          </div>
        )}
        <div className="mx-3 mt-1 border border-white/10 rounded bg-black/35 px-3 py-1.5 text-[10px] font-mono flex flex-wrap items-center gap-x-4 gap-y-1">
          <span className="text-gray-500 uppercase">Ops Guard</span>
          <span className="text-gray-300">
            Entry Freshness: <b className={`${
              entryFreshnessSeverity === 'CRITICAL'
                ? 'text-rose-300'
                : entryFreshnessSeverity === 'WARN'
                  ? 'text-amber-300'
                  : 'text-emerald-300'
            }`}>{entryFreshnessSeverity}</b>
          </span>
          <span className="text-gray-400">
            rate <span className={`${
              entryFreshnessSeverity === 'CRITICAL'
                ? 'text-rose-300'
                : entryFreshnessSeverity === 'WARN'
                  ? 'text-amber-300'
                  : 'text-emerald-300'
            }`}>{entryFreshnessRatePct.toFixed(2)}%</span>
            {' '}({strategyFreshness?.total ?? 0}/{freshnessDenominator || 0})
          </span>
          <span className="text-gray-500">
            miss {missingTelemetryCount} | src {staleSourceCount} | gate {staleGateCount}
          </span>
          <span className="text-gray-400">
            Silent Catches <span className={silentCatchTotal > 25 ? 'text-rose-300' : silentCatchTotal > 5 ? 'text-amber-300' : 'text-emerald-300'}>{silentCatchTotal}</span>
          </span>
          {silentCatchRows[0] && (
            <span className="text-gray-500 truncate max-w-[480px]" title={`${silentCatchRows[0].scope} | ${silentCatchRows[0].last_message}`}>
              top {silentCatchRows[0].scope} x{silentCatchRows[0].count}
            </span>
          )}
          {latestEntryFreshnessAlert && (
            <span className="text-gray-500 truncate max-w-[520px]" title={latestEntryFreshnessAlert.reason}>
              last {latestEntryFreshnessAlert.category} {formatAgeMs(latestEntryFreshnessAlert.timestamp, nowMs)}
            </span>
          )}
        </div>

        <EngineTuningPanel
          open={tuningOpen}
          onToggle={() => setTuningOpen((v) => !v)}
          riskModel={riskModel}
          riskValue={riskValue}
          onRiskChange={(model, value) => {
            setRiskModel(model);
            setRiskValue(value);
            socket?.emit('update_risk_config', { model, value, timestamp: Date.now() });
          }}
          multiplier={strategyMultiplierInfo}
          strategyStatuses={strategyStatuses}
          onToggleStrategy={(id, active) => {
            socket?.emit('toggle_strategy', { id, active, timestamp: Date.now() });
          }}
          perSideCostRate={latestScanMeta?.per_side_cost_rate != null ? Number(latestScanMeta.per_side_cost_rate) : null}
          minExpectedNetReturn={latestScanMeta?.min_expected_net_return != null ? Number(latestScanMeta.min_expected_net_return) : null}
        />

        <div className="flex-1 min-h-0 min-w-0 p-3 flex">
          {/* Hard-layout the reference UI: left column is split into (top grid) + (fixed bottom band),
              right column is the positions rail spanning full height. */}
          <div
            className="flex-1 min-h-0 min-w-0 grid gap-px bg-white/10 p-px"
            ref={layoutRefs.outer}
            style={{
              gridTemplateColumns: 'minmax(0, 11.4fr) minmax(320px, 2.4fr)',
              gridTemplateRows: 'minmax(0, 1fr)',
            }}
          >
            <div className="h-full min-h-0 min-w-0 flex flex-col gap-px bg-white/10" ref={layoutRefs.left}>
              <div
                className="flex-1 min-h-0 grid gap-px bg-white/10"
                ref={layoutRefs.top}
                style={{
                  gridTemplateColumns: 'minmax(340px, 3.4fr) minmax(420px, 4fr) minmax(420px, 4fr)',
                  gridTemplateRows: 'minmax(0, 1fr) minmax(0, 1fr)',
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
                    series={spotSeries.map((p) => p.value).filter((v) => v > 1_000 && v < 500_000)}
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
                    onTrace={(id) => {
                      if (!isProbablyUuid(id)) return;
                      void openTrace(id);
                    }}
                  />
                </PanelCell>

                <PanelCell style={{ gridColumn: '3', gridRow: '2' }}>
                  <SignalFlowPanel columns={flowColumns} maxAbs={flowMaxAbsRef.current} />
                </PanelCell>
              </div>

              <div
                className="h-[240px] grid gap-px bg-white/10"
                id="btc5m-bottom-band"
                ref={layoutRefs.bottom}
                style={{
                  gridTemplateColumns: 'minmax(340px, 3.4fr) minmax(420px, 4fr) minmax(420px, 4fr)',
                }}
              >
                <PanelCell>
                  <StatsPanel
                    avgPerTrade={avgPerTrade}
                    sharpe={sharpe}
                    maxDrawdown={maxDd}
                    openNotional={openNotional}
                    kelly={kelly}
                    ddLimitPct={ddLimitPct}
                  />
                </PanelCell>

                <PanelCell style={{ gridColumn: '2 / span 2' }}>
                  <ExecutionPipelinePanel
                    spot={spot}
                    cexPrices={cexPrices}
                    yesAsk={yesAsk}
                    noAsk={noAsk}
                    fairYes={fairYes}
                    sigma={sigma}
                    corr={corr}
                    bestSide={bestSide}
                    bestPrice={bestPrice}
                    bestProb={bestProb}
                    bestNetExpected={bestNetExpected}
                    kelly={kelly}
                    mlProbUp={mlProbUp}
                    mlSignalSideProb={mlSignalSideProb}
                    mlSignalConfidence={mlSignalConfidence}
                    mlSignalEdge={mlSignalEdge}
                    mlModelName={mlModelName}
                    mlModelReady={mlModelReady}
                    mlSignalLabel={mlSignalLabel}
                    mlSignalBonusBps={mlSignalBonusBps}
                    mlSignalPenaltyBps={mlSignalPenaltyBps}
                    execDecision={execDecision}
                    liveModeLabel={mode === 'PAPER' ? 'PAPER' : liveOrderPostingEnabled ? 'LIVE' : 'DRY-RUN'}
                    notionalHint={openNotional > 0 ? openNotional : lastNotional}
                    modelGateEnabled={modelGateCalibration.enabled}
                    modelGateAdvisory={modelGateCalibration.advisory_only}
                    modelGateActive={modelGateCalibration.active_gate}
                    modelGateRecommended={modelGateCalibration.recommended_gate}
                    modelGateDeltaBps={modelGateDeltaBps}
                    modelGateSamples={modelGateCalibration.sample_count}
                    modelGateDeltaPnl={modelGateCalibration.delta_expected_pnl}
                    modelGateBlocks={modelGateBlocks}
                    liveExecBlocks={liveExecBlocks}
                  />
                </PanelCell>
              </div>
            </div>

            <PanelCell>
              <PositionsPanel
                rows={positions}
                onTrace={(id) => {
                  if (!isProbablyUuid(id)) return;
                  void openTrace(id);
                }}
              />
            </PanelCell>
          </div>
        </div>

        {layoutDebugEnabled && layoutDebugInfo && (
          <div className="fixed bottom-3 left-3 z-[200] bg-black/80 border border-white/20 backdrop-blur-sm px-3 py-2 text-[10px] font-mono text-gray-100 max-w-[520px]">
            <div className="text-gray-400 uppercase tracking-widest">layout debug</div>
            <pre className="mt-1 whitespace-pre-wrap">{JSON.stringify(layoutDebugInfo, null, 2)}</pre>
          </div>
        )}

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
  vault: number;
  countdown: string;
  windowLabel: string;
  onExit?: () => void;
}) {
  const metrics = [
    { label: 'BTC', value: props.btcPrice, className: 'text-gray-100' },
    { label: 'PNL', value: formatUsdSigned0Spaces(props.pnl), className: props.pnl >= 0 ? 'text-emerald-300' : 'text-rose-300' },
    { label: 'TODAY', value: formatUsdSigned0Spaces(props.today), className: props.today >= 0 ? 'text-emerald-300' : 'text-rose-300' },
    { label: 'VAULT', value: `$${props.vault.toFixed(0)}`, className: props.vault > 0 ? 'text-amber-300' : 'text-gray-500' },
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
      className="bg-[#070707] relative overflow-hidden min-h-0 min-w-0"
      style={style}
    >
      <div className="absolute inset-0 pointer-events-none bg-[linear-gradient(180deg,rgba(255,255,255,0.04),transparent_45%)]" />
      <div className="absolute inset-0 pointer-events-none shadow-[inset_0_0_0_1px_rgba(255,255,255,0.02)]" />
      <div className="relative z-10 h-full min-h-0">
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
  const headline = formatUsd0Spaces(props.cumulativePnl);
  const pnlClass = 'text-gray-100';
  const todayClass = props.todayPnl >= 0 ? 'text-emerald-300' : 'text-rose-300';
  const roiClass = props.roiPct >= 0 ? 'text-gray-400' : 'text-gray-400';

  return (
    <div className="h-full min-h-0 relative">
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
    <div className="h-full min-h-0 relative">
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
    <div className="h-full min-h-0 flex flex-col">
      <div className="px-5 pt-4">
        <PanelTitle>ORDER FEED</PanelTitle>
      </div>
      <div className="px-5 pt-3 text-[10px] font-mono uppercase tracking-widest text-gray-600 grid grid-cols-[2px_84px_140px_70px_84px_110px_100px] gap-3 border-b border-white/10 pb-2">
        <div />
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
            const sideLabel = row.direction || '--';
            const sideClass = sideLabel === 'UP' ? 'text-emerald-300' : sideLabel === 'DOWN' ? 'text-rose-300' : 'text-gray-400';
            const barClass = sideLabel === 'UP' ? 'bg-emerald-400' : sideLabel === 'DOWN' ? 'bg-rose-400' : 'bg-white/15';
            const statusLabel = row.status === 'STOPPED'
              ? 'stopped ×'
              : row.status === 'OPEN'
                ? 'open'
                : 'resolved ✓';

            const windowShort = row.window_label.replace(' ET', '');

            return (
              <button
                key={row.execution_id}
                type="button"
                onClick={() => props.onTrace(row.execution_id)}
                className="w-full text-left group"
                title={`Open trace ${row.execution_id}`}
              >
                <div className="grid grid-cols-[2px_84px_140px_70px_84px_110px_100px] gap-3 items-center">
                  <div className={`h-10 ${barClass}`} />
                  <div className="font-mono text-[11px] text-gray-500">{formatTime24h(row.entry_ts)}</div>
                  <div className="font-mono text-[11px] text-gray-300">{windowShort}</div>
                  <div className={`font-mono text-[11px] font-semibold ${sideClass}`}>{sideLabel}</div>
                  <div className="font-mono text-[11px] text-gray-200">{formatCents0(row.entry_price)}</div>
                  <div className="font-mono text-[11px] text-gray-200">{formatUsd0Spaces(row.notional)}</div>
                  <div className="font-mono text-[11px] text-gray-500">{statusLabel}</div>
                </div>
              </button>
            );
          })}
        </div>
      </div>
    </div>
  );
}

function SignalFlowPanel(props: { columns: number[][]; maxAbs: number[] }) {
  return (
    <div className="h-full min-h-0 flex flex-col">
      <div className="px-5 pt-4">
        <PanelTitle>CROSS-EXCHANGE SIGNAL FLOW</PanelTitle>
      </div>
      <div className="flex-1 min-h-0 p-5">
        <SignalFlowMatrix columns={props.columns} maxAbs={props.maxAbs} />
      </div>
    </div>
  );
}

/* ─── Engine Tuning Panel ─────────────────────────────────────────── */

const TUNING_STRATEGIES = ['BTC_5M', 'BTC_15M', 'ETH_15M', 'SOL_15M'] as const;

function EngineTuningPanel(props: {
  open: boolean;
  onToggle: () => void;
  riskModel: 'FIXED' | 'PERCENT';
  riskValue: number;
  onRiskChange: (model: 'FIXED' | 'PERCENT', value: number) => void;
  multiplier: { multiplier: number; sample_count: number; ema_return: number } | null;
  strategyStatuses: Record<string, boolean>;
  onToggleStrategy: (id: string, active: boolean) => void;
  perSideCostRate: number | null;
  minExpectedNetReturn: number | null;
}) {
  const costBps = props.perSideCostRate != null ? (props.perSideCostRate * 10_000).toFixed(1) : '--';
  const edgeBps = props.minExpectedNetReturn != null ? (props.minExpectedNetReturn * 10_000).toFixed(0) : '--';
  const mult = props.multiplier;

  return (
    <div className="mx-3 mt-1 border border-white/10 rounded-lg bg-black/40 backdrop-blur-sm overflow-hidden">
      {/* Header — always visible */}
      <button
        type="button"
        onClick={props.onToggle}
        className="w-full flex items-center justify-between px-4 py-2 hover:bg-white/5 transition-colors"
      >
        <span className="text-[10px] font-mono uppercase tracking-[0.22em] text-gray-400">
          Engine Tuning
        </span>
        <svg
          className={`w-3 h-3 text-gray-500 transition-transform ${props.open ? 'rotate-180' : ''}`}
          fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}
        >
          <path strokeLinecap="round" strokeLinejoin="round" d="M19 9l-7 7-7-7" />
        </svg>
      </button>

      {/* Collapsible body */}
      {props.open && (
        <div className="px-4 pb-3 grid grid-cols-5 gap-4 border-t border-white/5 pt-3">

          {/* 1. Risk Model */}
          <div>
            <div className="text-[9px] font-mono uppercase tracking-widest text-gray-600 mb-2">Position Size</div>
            <div className="flex bg-white/5 rounded p-0.5 mb-2">
              <button
                type="button"
                onClick={() => props.onRiskChange('FIXED', 50)}
                className={`flex-1 px-2 py-1 text-[10px] rounded font-bold transition-all ${props.riskModel === 'FIXED' ? 'bg-emerald-500 text-black' : 'text-gray-400 hover:text-white'}`}
              >
                FIXED ($)
              </button>
              <button
                type="button"
                onClick={() => props.onRiskChange('PERCENT', 1.0)}
                className={`flex-1 px-2 py-1 text-[10px] rounded font-bold transition-all ${props.riskModel === 'PERCENT' ? 'bg-purple-500 text-white' : 'text-gray-400 hover:text-white'}`}
              >
                RISK (%)
              </button>
            </div>
            <div className="flex items-center gap-2">
              <input
                type="range"
                min={props.riskModel === 'FIXED' ? 10 : 0.5}
                max={props.riskModel === 'FIXED' ? 500 : 10.0}
                step={props.riskModel === 'FIXED' ? 10 : 0.5}
                value={props.riskValue}
                onChange={(e) => props.onRiskChange(props.riskModel, parseFloat(e.target.value))}
                className={`flex-1 h-1 rounded-lg appearance-none cursor-pointer ${props.riskModel === 'FIXED' ? 'accent-emerald-500 bg-emerald-900/30' : 'accent-purple-500 bg-purple-900/30'}`}
              />
              <span className={`text-xs font-mono font-bold w-14 text-right ${props.riskModel === 'FIXED' ? 'text-emerald-400' : 'text-purple-400'}`}>
                {props.riskModel === 'FIXED' ? `$${props.riskValue}` : `${props.riskValue.toFixed(1)}%`}
              </span>
            </div>
          </div>

          {/* 2. Multiplier */}
          <div>
            <div className="text-[9px] font-mono uppercase tracking-widest text-gray-600 mb-2">Risk Multiplier</div>
            <div className="text-lg font-mono font-semibold text-amber-300">
              {mult ? `${mult.multiplier.toFixed(3)}x` : '--'}
            </div>
            <div className="text-[10px] font-mono text-gray-500 mt-1">
              {mult ? `${mult.sample_count} samples` : 'no data'}
            </div>
            <div className="text-[10px] font-mono text-gray-500">
              EMA {mult ? `${(mult.ema_return * 100).toFixed(1)}%` : '--'}
            </div>
          </div>

          {/* 3. Cost Model */}
          <div>
            <div className="text-[9px] font-mono uppercase tracking-widest text-gray-600 mb-2">Cost / Side</div>
            <div className="text-lg font-mono font-semibold text-gray-100">{costBps} bps</div>
            <div className="text-[10px] font-mono text-gray-600 mt-1">restart to change</div>
          </div>

          {/* 4. Edge Threshold */}
          <div>
            <div className="text-[9px] font-mono uppercase tracking-widest text-gray-600 mb-2">Min Edge</div>
            <div className="text-lg font-mono font-semibold text-gray-100">{edgeBps} bps</div>
            <div className="text-[10px] font-mono text-gray-600 mt-1">restart to change</div>
          </div>

          {/* 5. Strategy Toggles */}
          <div>
            <div className="text-[9px] font-mono uppercase tracking-widest text-gray-600 mb-2">Strategies</div>
            <div className="space-y-1">
              {TUNING_STRATEGIES.map((id) => {
                const active = props.strategyStatuses[id] !== false;
                return (
                  <button
                    key={id}
                    type="button"
                    onClick={() => props.onToggleStrategy(id, !active)}
                    className={`w-full flex items-center gap-2 px-2 py-1 rounded text-[10px] font-mono font-bold transition-colors ${
                      active
                        ? 'bg-emerald-500/10 text-emerald-400 border border-emerald-500/20 hover:bg-emerald-500/20'
                        : 'bg-red-500/10 text-red-400 border border-red-500/20 hover:bg-red-500/20'
                    }`}
                  >
                    <div className={`w-1.5 h-1.5 rounded-full ${active ? 'bg-emerald-400' : 'bg-red-400'}`} />
                    {id.replace('_', ' ')}
                  </button>
                );
              })}
            </div>
          </div>
        </div>
      )}
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
      <div className="grid grid-cols-2 grid-rows-3 gap-px bg-white/10 h-full">
        <div className="bg-black/40 px-5 py-4">
          <MiniStat
            label="AVG / TRADE"
            value={formatUsd2SignedNoPlus(props.avgPerTrade)}
            valueClass={props.avgPerTrade >= 0 ? 'text-emerald-300' : 'text-rose-300'}
          />
        </div>
        <div className="bg-black/40 px-5 py-4">
          <MiniStat
            label="SHARPE"
            value={props.sharpe === null ? '--' : props.sharpe.toFixed(2)}
            valueClass="text-gray-100"
          />
        </div>
        <div className="bg-black/40 px-5 py-4">
          <MiniStat
            label="MAX DD"
            value={props.maxDrawdown > 0 ? formatUsd0Commas(-props.maxDrawdown) : '$0'}
            valueClass={props.maxDrawdown > 0 ? 'text-rose-300' : 'text-gray-200'}
          />
        </div>
        <div className="bg-black/40 px-5 py-4">
          <MiniStat
            label="OPEN POS"
            value={formatUsd0Spaces(props.openNotional)}
            valueClass={props.openNotional > 0 ? 'text-amber-300' : 'text-gray-200'}
          />
        </div>
        <div className="bg-black/40 px-5 py-4">
          <MiniStat
            label="KELLY F*"
            value={props.kelly === null ? '--' : `${(Math.max(0, Math.min(1, props.kelly)) * 100).toFixed(2)}%`}
            valueClass="text-gray-100"
          />
        </div>
        <div className="bg-black/40 px-5 py-4">
          <MiniStat
            label="DD LIMIT"
            value={`${props.ddLimitPct.toFixed(1)}%`}
            valueClass="text-rose-300"
          />
        </div>
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

function usePipelineSweep(stepCount: number, stepMs: number): { activeIndex: number; warmIndex: number; warmProgress: number } {
  const now = useNowMs(50);
  const cycleMs = Math.max(1, stepCount * stepMs);
  const t = ((now % cycleMs) + cycleMs) % cycleMs;
  const activeIndex = Math.floor(t / stepMs) % stepCount;
  const warmIndex = (activeIndex + 1) % stepCount;
  const warmProgress = Math.min(1, Math.max(0, (t % stepMs) / stepMs));
  return { activeIndex, warmIndex, warmProgress };
}

function ExecutionPipelinePanel(props: {
  spot: number | null;
  cexPrices: Record<string, number>;
  yesAsk: number | null;
  noAsk: number | null;
  fairYes: number | null;
  sigma: number | null;
  corr: number | null;
  bestSide: 'UP' | 'DOWN' | null;
  bestPrice: number | null;
  bestProb: number | null;
  bestNetExpected: number | null;
  kelly: number | null;
  mlProbUp: number | null;
  mlSignalSideProb: number | null;
  mlSignalConfidence: number | null;
  mlSignalEdge: number | null;
  mlModelName: string;
  mlModelReady: boolean;
  mlSignalLabel: string;
  mlSignalBonusBps: number | null;
  mlSignalPenaltyBps: number | null;
  execDecision: 'PASS' | 'HOLD';
  liveModeLabel: string;
  notionalHint: number;
  modelGateEnabled: boolean;
  modelGateAdvisory: boolean;
  modelGateActive: number;
  modelGateRecommended: number;
  modelGateDeltaBps: number;
  modelGateSamples: number;
  modelGateDeltaPnl: number;
  modelGateBlocks: number;
  liveExecBlocks: number;
}) {
  const impliedYes = props.yesAsk;
  const cexProb = props.fairYes;
  const edgePct = impliedYes !== null && cexProb !== null ? (cexProb - impliedYes) * 100 : null;
  const evUsd = props.bestNetExpected !== null && Number.isFinite(props.notionalHint) && props.notionalHint > 0
    ? props.notionalHint * props.bestNetExpected
    : null;
  const mlUpProb = props.mlProbUp !== null ? Math.max(0, Math.min(1, props.mlProbUp)) : null;
  const mlSideProb = props.mlSignalSideProb !== null
    ? Math.max(0, Math.min(1, props.mlSignalSideProb))
    : (() => {
      if (mlUpProb === null) return null;
      if (props.bestSide === 'DOWN') return 1 - mlUpProb;
      return mlUpProb;
    })();
  const mlConfidence = props.mlSignalConfidence !== null
    ? Math.max(0, Math.min(1, props.mlSignalConfidence))
    : (mlSideProb !== null ? Math.abs(mlSideProb - 0.5) * 2 : null);
  const mlEdgePct = props.mlSignalEdge !== null
    ? props.mlSignalEdge * 100
    : (mlUpProb !== null ? (mlUpProb - 0.5) * 200 : null);
  const modelImpactBps = props.mlSignalBonusBps !== null || props.mlSignalPenaltyBps !== null
    ? (props.mlSignalBonusBps ?? 0) - (props.mlSignalPenaltyBps ?? 0)
    : null;
  const modelLabelClass = props.mlSignalLabel.includes('ALIGNED')
    ? 'text-emerald-300'
    : props.mlSignalLabel.includes('DISAGREE')
      ? 'text-rose-300'
      : props.mlSignalLabel.includes('LOW_CONF')
        || props.mlSignalLabel.includes('QUALITY_LOW')
        || props.mlSignalLabel.includes('WARMUP')
        || props.mlSignalLabel.includes('NOT_READY')
        || props.mlSignalLabel.includes('STALE')
        ? 'text-amber-300'
        : 'text-gray-400';
  const modelGateDeltaClass = props.modelGateDeltaBps > 0.1
    ? 'text-amber-300'
    : props.modelGateDeltaBps < -0.1
      ? 'text-emerald-300'
      : 'text-gray-400';
  const modelGateModeLabel = props.modelGateEnabled
    ? (props.modelGateAdvisory ? 'ADVISORY' : 'ACTIVE')
    : 'DISABLED';
  const sweep = usePipelineSweep(5, 200);
  const px = (key: string): string => {
    const value = props.cexPrices?.[key];
    return typeof value === 'number' && Number.isFinite(value) ? formatUsd0Spaces(value) : '--';
  };

  return (
    <div className="h-full flex flex-col">
      <div className="px-5 pt-4">
        <PanelTitle>EXECUTION PIPELINE</PanelTitle>
      </div>
      <div className="flex-1 min-h-0 p-5">
        <div className="grid gap-px bg-white/10 h-full grid-cols-[minmax(0,1fr)_18px_minmax(0,1fr)_18px_minmax(0,1fr)_18px_minmax(0,1fr)_18px_minmax(0,1fr)]">
          <PipeCard idx={0} sweep={sweep} title="01" subtitle="CEX FEEDS">
            <PipeRow k="binance" v={px('binance')} />
            <PipeRow k="coinbase" v={px('coinbase')} kClass="text-gray-200" vClass="text-gray-100" />
            <PipeRow k="okx" v={px('okx')} />
            <PipeRow k="bybit" v={px('bybit')} />
            <PipeRow k="kraken" v={px('kraken')} />
            <PipeRow k="bitfinex" v={px('bitfinex')} />
          </PipeCard>
          <PipeArrow />
          <PipeCard idx={1} sweep={sweep} title="02" subtitle="PM ODDS">
            <PipeRow k="UP" v={props.yesAsk === null ? '--' : formatCents0(props.yesAsk)} vClass="text-emerald-300" />
            <PipeRow k="DN" v={props.noAsk === null ? '--' : formatCents0(props.noAsk)} vClass="text-rose-300" />
            <PipeRow k="implied" v={impliedYes === null ? '--' : `${(Math.max(0, Math.min(1, impliedYes)) * 100).toFixed(1)}%`} />
            <div className="mt-3 text-[11px] font-mono text-gray-500">
              vol <span className="text-gray-200">{props.sigma === null ? '--' : `${(props.sigma * 100).toFixed(0)}%`}</span>
            </div>
          </PipeCard>
          <PipeArrow />
          <PipeCard idx={2} sweep={sweep} title="03" subtitle="EDGE">
            <PipeRow k="cex" v={cexProb === null ? '--' : `${(Math.max(0, Math.min(1, cexProb)) * 100).toFixed(1)}%`} />
            <PipeRow k="pm" v={impliedYes === null ? '--' : `${(Math.max(0, Math.min(1, impliedYes)) * 100).toFixed(1)}%`} />
            <PipeRow
              k="edge"
              v={edgePct === null ? '--' : `${edgePct >= 0 ? '+' : ''}${edgePct.toFixed(1)}%`}
              vClass={edgePct !== null && edgePct >= 0 ? 'text-emerald-300' : 'text-rose-300'}
            />
            <PipeRow
              k="ml up"
              v={mlUpProb === null ? '--' : `${(mlUpProb * 100).toFixed(1)}%`}
              vClass={mlUpProb === null ? 'text-gray-500' : 'text-cyan-300'}
            />
            <PipeRow
              k="ml conf"
              v={mlConfidence === null ? '--' : `${(mlConfidence * 100).toFixed(0)}%`}
              vClass={mlConfidence === null ? 'text-gray-500' : mlConfidence >= 0.55 ? 'text-emerald-300' : 'text-amber-300'}
            />
            <div className="mt-3 text-[11px] font-mono text-gray-500">
              σ <span className="text-gray-200">{props.sigma === null ? '--' : props.sigma.toFixed(1)}</span>
            </div>
          </PipeCard>
          <PipeArrow />
          <PipeCard idx={3} sweep={sweep} title="04" subtitle="KELLY">
            <PipeRow k="f*" v={props.kelly === null ? '--' : `${(Math.max(0, Math.min(1, props.kelly)) * 100).toFixed(2)}%`} />
            <PipeRow k="½k" v={props.kelly === null ? '--' : `${(Math.max(0, Math.min(1, props.kelly / 2)) * 100).toFixed(2)}%`} />
            <PipeRow k="corr" v={props.corr === null ? '--' : props.corr.toFixed(2)} />
            <div className="mt-3 text-[11px] font-mono">
              <span className="text-gray-500">$ </span>
              <span className="text-gray-200">{props.notionalHint > 0 ? formatIntSpaces(props.notionalHint) : '--'}</span>
            </div>
          </PipeCard>
          <PipeArrow />
          <PipeCard idx={4} sweep={sweep} title="05" subtitle="EXEC">
            <PipeRow k="dir" v={props.bestSide || '--'} vClass={props.bestSide === 'UP' ? 'text-emerald-300' : props.bestSide === 'DOWN' ? 'text-rose-300' : 'text-gray-200'} />
            <PipeRow k="@" v={props.bestPrice === null ? '--' : formatCents0(props.bestPrice)} />
            <PipeRow
              k="gate"
              v={props.execDecision}
              vClass={props.execDecision === 'PASS' ? 'text-emerald-300' : 'text-gray-500'}
            />
            <PipeRow
              k="ml gate"
              v={props.mlSignalLabel}
              vClass={modelLabelClass}
            />
            <PipeRow
              k="p gate"
              v={`${(Math.max(0, Math.min(1, props.modelGateActive)) * 100).toFixed(1)}%`}
              vClass="text-cyan-300"
            />
            <PipeRow
              k="p rec"
              v={`${(Math.max(0, Math.min(1, props.modelGateRecommended)) * 100).toFixed(1)}%`}
              vClass={modelGateDeltaClass}
            />
            <div className="mt-3 text-[11px] font-mono flex items-center justify-between gap-3">
              <div>
                <span className="text-gray-500">EV </span>
                <span className={evUsd !== null && evUsd >= 0 ? 'text-emerald-300' : 'text-rose-300'}>
                  {evUsd === null ? '--' : formatUsdSigned0Spaces(evUsd)}
                </span>
              </div>
              <span className="text-gray-600">{props.liveModeLabel}</span>
            </div>
            <div className="mt-2 text-[10px] font-mono flex items-center justify-between gap-3">
              <span className={props.mlModelReady ? 'text-cyan-300 truncate' : 'text-amber-300 truncate'}>
                {props.mlModelReady ? 'READY' : 'WARMUP'} · {props.mlModelName}
              </span>
              <span className={modelImpactBps === null ? 'text-gray-600' : modelImpactBps >= 0 ? 'text-emerald-300' : 'text-rose-300'}>
                {modelImpactBps === null ? '--' : `${modelImpactBps >= 0 ? '+' : ''}${modelImpactBps.toFixed(1)}bps`}
              </span>
            </div>
            <div className="mt-1 text-[10px] font-mono text-gray-500">
              edge <span className={mlEdgePct !== null && mlEdgePct >= 0 ? 'text-emerald-300' : 'text-rose-300'}>
                {mlEdgePct === null ? '--' : `${mlEdgePct >= 0 ? '+' : ''}${mlEdgePct.toFixed(1)}%`}
              </span>
            </div>
            <div className="mt-1 text-[10px] font-mono text-gray-500 flex items-center justify-between gap-2">
              <span>{modelGateModeLabel} · n={props.modelGateSamples}</span>
              <span className={modelGateDeltaClass}>
                Δp {props.modelGateDeltaBps >= 0 ? '+' : ''}{props.modelGateDeltaBps.toFixed(1)}bps
              </span>
            </div>
            <div className="mt-1 text-[10px] font-mono text-gray-500 flex items-center justify-between gap-2">
              <span>ΔEV {props.modelGateDeltaPnl >= 0 ? '+' : '-'}${Math.abs(props.modelGateDeltaPnl).toFixed(2)}</span>
              <span>rej {props.modelGateBlocks} | live {props.liveExecBlocks}</span>
            </div>
          </PipeCard>
        </div>
      </div>
    </div>
  );
}

function PipeArrow() {
  return (
    <div className="bg-[#070707] flex items-center justify-center text-gray-600 font-mono text-[14px] select-none">
      →
    </div>
  );
}

function PipeCard(props: {
  idx: number;
  sweep: { activeIndex: number; warmIndex: number; warmProgress: number };
  title: string;
  subtitle: string;
  children: React.ReactNode;
}) {
  const isActive = props.idx === props.sweep.activeIndex;
  const isWarm = props.idx === props.sweep.warmIndex;
  const warmOpacity = isWarm ? props.sweep.warmProgress * 0.25 : 0;
  const topLineOpacity = isActive ? 0.92 : warmOpacity;
  return (
    <div className={`bg-[#070707] p-4 flex flex-col justify-between relative ${isActive ? 'bg-white/[0.03]' : ''}`}>
      <div
        className="absolute left-0 right-0 top-0 h-[2px]"
        style={{
          background: `rgba(255,255,255,${topLineOpacity.toFixed(3)})`,
          boxShadow: isActive
            ? '0 0 10px rgba(255,255,255,0.22)'
            : 'none',
        }}
      />
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

function PipeRow(props: { k: string; v: string; kClass?: string; vClass?: string }) {
  return (
    <div className="flex items-center justify-between gap-3 font-mono text-[12px]">
      <span className={props.kClass || 'text-gray-500'}>{props.k}</span>
      <span className={props.vClass || 'text-gray-200'}>{props.v}</span>
    </div>
  );
}

function PositionsPanel(props: { rows: TradeRecord[]; onTrace: (id: string) => void }) {
  return (
    <div className="h-full min-h-0 flex flex-col">
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
            const edgeBar = row.direction === 'UP' ? 'bg-emerald-400' : row.direction === 'DOWN' ? 'bg-rose-400' : 'bg-white/15';
            const sideClass = row.direction === 'UP' ? 'text-emerald-300' : row.direction === 'DOWN' ? 'text-rose-300' : 'text-gray-400';
            const arrow = row.direction === 'UP' ? '▲' : row.direction === 'DOWN' ? '▼' : '•';
            const status = row.status === 'STOPPED' ? 'stopped ×' : row.status === 'OPEN' ? 'open' : 'resolved ✓';
            const progress = (() => {
              const entry = row.entry_ts || 0;
              const end = row.exit_ts ?? Date.now();
              const denom = 300_000;
              if (entry <= 0) return 1;
              return Math.max(0.05, Math.min(1, (end - entry) / denom));
            })();
            const progressClass = row.status === 'STOPPED'
              ? 'bg-rose-400'
              : pnl === null
                ? 'bg-white/15'
                : pnl >= 0
                  ? 'bg-emerald-400'
                  : 'bg-rose-400';

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
                    <div className="mt-3 h-[2px] w-full bg-white/10 overflow-hidden">
                      <div className={`h-full ${progressClass}`} style={{ width: `${(progress * 100).toFixed(1)}%` }} />
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
  // Winsorize against single bad ticks so charts don't explode vertically.
  const sorted = [...points].sort((a, b) => a - b);
  const lowIdx = Math.floor(sorted.length * 0.02);
  const highIdx = Math.max(lowIdx + 1, Math.ceil(sorted.length * 0.98) - 1);
  let min = sorted[Math.max(0, Math.min(sorted.length - 1, lowIdx))] as number;
  let max = sorted[Math.max(0, Math.min(sorted.length - 1, highIdx))] as number;
  if (!Number.isFinite(min) || !Number.isFinite(max) || Math.abs(max - min) < 1e-9) {
    min = Math.min(...points);
    max = Math.max(...points);
  }
  const spread = Math.max(1e-9, max - min);

  const poly = points
    .map((v, i) => {
      const clamped = Math.min(max, Math.max(min, v));
      const x = padX + (i / (points.length - 1)) * (width - padX * 2);
      const y = height - padY - ((clamped - min) / spread) * (height - padY * 2);
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

function SignalFlowMatrix(props: { columns: number[][]; maxAbs: number[] }) {
  const cols = props.columns.length;
  const rows = FLOW_ROWS.length;
  const maxAbs = props.maxAbs.length === rows ? props.maxAbs : new Array(rows).fill(1);

  return (
    <div className="h-full flex">
      <div
        className="w-12 pr-2 grid"
        style={{ gridTemplateRows: `repeat(${rows}, minmax(0, 1fr))` }}
      >
        {FLOW_ROWS.map((row) => (
          <div key={row.id} className="text-[9px] font-mono uppercase tracking-widest text-gray-600 leading-none">
            {row.label}
          </div>
        ))}
      </div>
      <div className="flex-1 min-w-0">
        <div
          className="grid gap-px bg-white/10 p-px h-full"
          style={{
            gridTemplateColumns: `repeat(${cols}, minmax(0, 1fr))`,
            gridTemplateRows: `repeat(${rows}, minmax(0, 1fr))`,
          }}
        >
          {Array.from({ length: rows }).flatMap((_, rowIdx) => (
            props.columns.map((col, colIdx) => {
              const raw = Number(col?.[rowIdx] ?? 0);
              const denom = Math.max(1e-9, Number(maxAbs[rowIdx] ?? 1e-9));
              const intensity = Math.min(1, Math.abs(raw) / denom);
              const base = 0.04;
              const bright = base + intensity * 0.86;
              const alpha = raw >= 0 ? bright : bright * 0.46;
              const bg = `rgba(255,255,255,${alpha.toFixed(3)})`;
              return (
                <div
                  key={`${rowIdx}-${colIdx}`}
                  className="bg-[#070707]"
                  style={{ background: bg }}
                />
              );
            })
          ))}
        </div>
      </div>
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
