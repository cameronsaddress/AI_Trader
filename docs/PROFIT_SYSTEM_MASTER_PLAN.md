# Profit System Master Plan (Phased Execution)

## Objective
Build a transparent, risk-controlled, continuously improving Polymarket trading system that maximizes repeatable long-run profit using only currently available data feeds (Polymarket + Coinbase + internal logs/telemetry).

## Constraints
- No real order posting unless system is in `LIVE` and explicitly confirmed.
- Use currently integrated data sources only.
- No hidden automations: every critical module must expose live runtime telemetry to UI.
- Profit objective is risk-adjusted and repeatable, not short-lived overfit gains.

## Core Success Metrics
- `Net PnL` (paper/live, daily/weekly/monthly)
- `Sharpe / Sortino` (trade-level and daily equity-level)
- `Max drawdown` and `time-under-water`
- `Hit rate` + `expectancy` per strategy/family
- `Edge realization`: realized return vs predicted edge
- `Execution quality`: slippage, spread capture, fill validity, stale-signal rejects
- `Risk discipline`: utilization, concentration, kill-switch events, blocked unsafe executions

---

## Phase 1: Standard Techniques (Foundation)

### P1.1 Data Integrity + Market Microstructure Hygiene
Goal: prevent garbage-in decisions.

Deliverables:
- Sequence-gap/staleness detection on all relevant feeds.
- Book validity checks (bid/ask sanity, parity, spread bounds, timestamp freshness).
- Strategy-level data quality gates before signal pass.
- Data quality counters with per-strategy visibility in UI.

Acceptance:
- `0` silent stale-book trades.
- Data quality violations are visible with reason and count.

### P1.2 Signal Contract Normalization
Goal: make all bots comparable and composable.

Deliverables:
- Standard scan payload schema across strategies:
  - `strategy`, `market_key`, `score`, `threshold`, `margin`, `unit`, `metric_family`, `reason`, `timestamp`.
- Comparable-group normalization for mixed signal units.
- Unified pass/hold semantics and reason taxonomy.

Acceptance:
- All strategy rows appear in one normalized control-plane feed.
- No strategy emits ad-hoc fields without schema mapping.

### P1.3 Backtest/Replay Harness with Costs
Goal: evaluate strategy logic under realistic constraints.

Deliverables:
- Event-level replay runner from historical scan/execution logs.
- Explicit transaction cost model:
  - spread, round-trip fee proxy, adverse selection penalty.
- Per-strategy report:
  - expectancy, variance, tail loss, turnover, regime breakdown.

Acceptance:
- Replay results reproduce live paper PnL directionally for known windows.
- Reports generated automatically and persisted.

### P1.4 Walk-Forward Validation + Promotion Gates
Goal: reduce overfitting and bad promotions.

Deliverables:
- Rolling train/validation/test windows (walk-forward).
- Minimum sample and stability requirements for promotion.
- Auto-disable/reduce sizing for persistent negative expectancy strategies.
- Promotion/hold/demotion decisions logged and visible in UI.

Acceptance:
- No promotion decision without required sample size and stability checks.
- Every promotion decision has machine-readable audit reason.

### P1.5 Portfolio Risk Allocation (Cross-Strategy)
Goal: allocate capital where edge is strongest and most stable.

Deliverables:
- Strategy multipliers from performance quality (return, downside, variance, sample confidence).
- Concentration caps per strategy and per family.
- Kill-switch/circuit-breaker for persistent underperformance.

Acceptance:
- Allocation updates happen automatically with clear telemetry.
- Underperformers size down before causing large drawdowns.

### P1.6 Execution Risk and Settlement Discipline
Goal: ensure safe and coherent position lifecycle.

Deliverables:
- Entry gates: spread, parity, staleness, cooldown, expiry distance.
- Exit rules: TP/SL/time/edge-decay with consistent accounting.
- Settlement tracking/reconciliation loops with anomaly flags.

Acceptance:
- Ledger/trade-log reconciliation within tolerance.
- No orphaned reserved capital after close/reset.

### P1.7 Full Observability in UI (No Ghosts)
Goal: every critical function is visible live.

Deliverables:
- Runtime module panel showing:
  - module id, status, last heartbeat, event counts, last reason.
- Per-strategy telemetry:
  - pass-rate, sample count, allocator multiplier, gate blocks.
- Pipeline status:
  - data integrity, intelligence gate, allocator, settlement, replay, tuner.

Acceptance:
- Operators can identify which module blocked/allowed each action in real time.
- Dedicated strategy pages (example: BTC 5m execution loop) show full pipeline telemetry end-to-end: feed -> fair value -> edge -> sizing -> (dry-run/live) execution -> settlement -> PnL.

---

## Phase 2: ML + ML Best Practices (Constrained, Production-Safe)

### P2.1 Feature Registry + Labeling Pipeline
Goal: stable supervised learning inputs/targets.

Deliverables:
- Feature registry for each strategy family:
  - order-book imbalance features, momentum/vol features, parity/spread features, timing-to-expiry features.
- Label definitions:
  - forward return horizons, hit/miss against threshold, risk-adjusted payoff labels.
- Leakage checks (no future data in features).

Acceptance:
- Deterministic feature snapshots for replay.
- Label leakage tests pass.

### P2.2 Model Training with Time-Series CV
Goal: robust generalization.

Deliverables:
- Purged/embargoed time-series cross-validation.
- Walk-forward model retraining schedule.
- Hyperparameter search with bounded search space and cost-aware objective.
- Calibration (probability reliability) and uncertainty estimation.

Acceptance:
- Out-of-sample objective > baseline with statistical confidence.
- Calibration error below target threshold.

### P2.3 Online Inference + Gated Deployment
Goal: safely insert ML into execution loop.

Deliverables:
- Model registry with versioning and promotion states.
- Shadow mode before active gating.
- Canary activation per strategy with rollback path.
- Fallback to deterministic baseline on model health failure.

Acceptance:
- Model can be disabled without downtime.
- Live telemetry includes model version and confidence.

### P2.4 Drift Monitoring + Auto De-Promotion
Goal: avoid decay from regime changes.

Deliverables:
- Feature drift, label drift, and performance drift monitors.
- Threshold-based demotion to baseline.
- Drift events in UI and execution logs.

Acceptance:
- Drift-triggered de-promotion works automatically and audibly.

### P2.5 ML Governance
Goal: keep ML from becoming uncontrolled overfitting machinery.

Deliverables:
- Min sample gates before any model promotion.
- Stability tests across regimes/sessions.
- Risk caps independent of model confidence.
- Audit trail for training data window, metrics, and release reason.

Acceptance:
- No model reaches active unless governance checks pass.

---

## Phase 3: Novel Alpha Layers (After Foundation + ML Safety)

### P3.1 Meta-Controller / Ensemble Router
Goal: combine bots intelligently, not independently.

Deliverables:
- Contextual router selecting which strategy families can deploy in current regime.
- Ensemble confidence score from independent evidence streams.
- Capital routing based on marginal contribution to portfolio Sharpe.

Acceptance:
- Ensemble beats best-single baseline out-of-sample.

### P3.2 Regime Engine
Goal: adapt strategy behavior to market state.

Deliverables:
- Regime classifier (`trend`, `mean-revert`, `high-vol`, `liquidity-thin`).
- Regime-conditioned thresholds/sizing/exit timing.
- Regime transition penalties to reduce churn.

Acceptance:
- Lower drawdown and better consistency through regime shifts.

### P3.3 Uncertainty-Aware Trade Admission
Goal: avoid low-confidence trades.

Deliverables:
- Conformal/quantile uncertainty bounds around edge estimates.
- Trade suppression in high-uncertainty states.
- Uncertainty telemetry in UI per strategy.

Acceptance:
- Tail-loss reduction without collapsing opportunity capture.

### P3.4 Cross-Strategy Causal Lead/Lag Layer
Goal: exploit timing advantages from feed interactions.

Deliverables:
- Lead/lag detectors between Coinbase microstructure and Polymarket response.
- Time-decay alpha scoring for short-horizon opportunities.
- Strict latency and stale cutoff controls.

Acceptance:
- Positive incremental edge net of costs in replay and paper.

### P3.5 Advanced Exit Optimizer
Goal: improve realized edge capture.

Deliverables:
- Hazard-based timed exits (expiry-aware).
- Dynamic TP/SL scaled by volatility and signal decay.
- Partial-profit and stop-tightening logic.

Acceptance:
- Better edge realization and reduced giveback.

---

## UI Transparency Requirements (Cross-Phase)

Every phase must add/maintain live UI views for:
- Module status: `online/offline/degraded`, last heartbeat, event counters.
- Decision provenance:
  - why signal passed/held/blocked.
- Risk provenance:
  - current multiplier, concentration, gate reasons.
- Model provenance (Phase 2+):
  - version, confidence, calibration status, drift status.
- Portfolio provenance:
  - equity, cash, reserved, realized PnL, utilization, by-strategy contribution.

No hidden optimization loops are allowed without live operator visibility.

---

## Execution Cadence

### Sprint Order
1. Phase 1 observability + data integrity + validation harness.
2. Phase 1 portfolio allocation + promotion/demotion automation.
3. Phase 2 feature/label/train/calibration + shadow deployment.
4. Phase 2 online gating + drift auto-demotion.
5. Phase 3 meta-controller + regime engine + uncertainty layer.
6. Phase 3 lead/lag + advanced exits + final integration hardening.

### Release Gates
- `DEV`: unit/integration/replay checks pass.
- `PAPER_CANARY`: no critical risk regressions for target duration.
- `PAPER_FULL`: stable performance and reconciliation.
- `LIVE_DRY_RUN`: preflight passes and intelligence blocks behave as expected.
- `LIVE_ARMED`: explicit operator confirmation + all safety checks green.

---

## Immediate Build Start (Current Sprint)

Current sprint implementation focuses on:
1. Runtime module telemetry and UI visibility (“no ghosts” baseline).
2. Standard-technique observability wiring for strategy/risk/intelligence/settlement modules.
3. Next sprint kickoff hooks for replay and walk-forward automation.
