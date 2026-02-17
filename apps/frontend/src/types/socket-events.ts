/**
 * Typed interfaces for all WebSocket event payloads used across the frontend.
 *
 * Each interface corresponds to a specific socket event emitted by the backend
 * and consumed by one or more page components.
 */

// ── scanner_update ──────────────────────────────────────────────────────────

/** Payload for the `scanner_update` socket event. */
export interface ScannerUpdateEvent {
  strategy: string;
  passes_threshold?: boolean;
  score?: number;
  timestamp?: number;
  reason?: string;
  meta?: Record<string, any>;
  /** Used by HFT strategies */
  market_id?: string;
  symbol?: string;
  threshold?: number;
  /** Nested price data sometimes attached to top-level */
  prices?: number[];
}

// ── execution_log ───────────────────────────────────────────────────────────

/** Payload for the `execution_log` socket event. */
export interface ExecutionLogEvent {
  type?: string;
  strategy?: string;
  execution_id?: string;
  question?: string;
  market_id?: string;
  market?: string;
  side?: string;
  entry_side?: string;
  entry_price?: number;
  exit_price?: number;
  price?: unknown;
  size?: unknown;
  pnl?: number;
  net_edge_bps?: number;
  bias_edge_pct?: number;
  net_edge_pct?: number;
  timestamp?: number;
  mode?: string;
  details?: any;
  [key: string]: any;
}

// ── vault_update ────────────────────────────────────────────────────────────

/** Payload for the `vault_update` socket event. */
export interface VaultUpdateEvent {
  vault?: number;
}

// ── sim_ledger_update ───────────────────────────────────────────────────────

/** Payload for the `sim_ledger_update` (bankroll) socket event. */
export interface BankrollUpdateEvent {
  available_cash?: number;
  equity?: number;
  reserved?: number;
}

// ── heartbeat ───────────────────────────────────────────────────────────────

/** Payload for the `heartbeat` socket event. */
export interface HeartbeatEvent {
  id?: string;
}

// ── risk_guard_update ───────────────────────────────────────────────────────

/** Payload for the `risk_guard_update` socket event. */
export interface RiskGuardEvent {
  strategy?: string;
  cumPnl?: number;
  peakPnl?: number;
  consecutiveLosses?: number;
  pausedUntil?: number;
}

// ── trading_mode_update ─────────────────────────────────────────────────────

/** Payload for the `trading_mode_update` socket event. */
export interface TradingModeEvent {
  mode?: 'PAPER' | 'LIVE';
  live_order_posting_enabled?: boolean;
}

// ── strategy_metrics_update ─────────────────────────────────────────────────

/** Per-strategy metric entry within a `strategy_metrics_update` event. */
export interface StrategyMetricEntry {
  pnl?: number;
  daily_trades?: number;
  updated_at?: number;
}

/** Payload for the `strategy_metrics_update` socket event (keyed by strategy id). */
export type StrategyMetricsEvent = Record<string, StrategyMetricEntry>;

// ── strategy_pnl ────────────────────────────────────────────────────────────

/** Payload for the `strategy_pnl` socket event. */
export interface StrategyPnlEvent {
  execution_id?: string;
  executionId?: string;
  strategy?: string;
  pnl?: number;
  notional?: number;
  timestamp?: number;
  side?: string;
  details?: {
    execution_id?: string;
    executionId?: string;
    notional?: number;
    side?: string;
    position_side?: string;
    entry_price?: number;
    window_start_ts?: number;
    hold_ms?: number;
    pnl?: number;
    [key: string]: any;
  };
  [key: string]: any;
}

// ── strategy_status_update ──────────────────────────────────────────────────

/** Payload for the `strategy_status_update` socket event (keyed by strategy id). */
export type StrategyStatusEvent = Record<string, boolean>;

// ── risk_config_update ──────────────────────────────────────────────────────

/** Payload for the `risk_config_update` socket event. */
export interface RiskConfigEvent {
  model?: string;
  value?: number;
}

// ── strategy_risk_multiplier_update ─────────────────────────────────────────

/** Payload for the `strategy_risk_multiplier_update` socket event. */
export interface StrategyRiskMultiplierEvent {
  strategy?: string;
  multiplier?: number;
  sample_count?: number;
  ema_return?: number;
}

// ── strategy_risk_multiplier_snapshot ────────────────────────────────────────

/** Payload for the `strategy_risk_multiplier_snapshot` socket event (keyed by strategy id). */
export type StrategyRiskMultiplierSnapshot = Record<string, {
  multiplier?: number;
  sample_count?: number;
  ema_return?: number;
}>;
