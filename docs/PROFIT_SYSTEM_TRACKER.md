# Profit System Tracker

Last updated: 2026-02-12

## Overall Progress
- Phase 1 (Standard techniques): ~78% complete
- Phase 2 (ML + safe deployment): ~41% complete
- Phase 3 (Novel alpha layers): ~25% complete

## Phase 1 Status

### P1.1 Data integrity + microstructure hygiene
- Status: In progress (strong)
- Completed:
  - Strategy-level hold reason taxonomy and integrity categorization in backend.
  - Live integrity counters, recent alert stream, per-strategy consecutive-hold tracking.
  - UI visibility for integrity alerts and hold-rate.
- Remaining:
  - Explicit per-feed sequence-gap KPIs and scanner-level stale-book SLO checks.
  - Hard fail-safe assertions proving zero silent stale-book executions.

### P1.2 Signal contract normalization
- Status: Complete (for current strategy stack)
- Completed:
  - Normalized scan model (`strategy`, `market_key`, `score`, `threshold`, `unit`, `metric_family`, reason/timestamp).
  - Comparable-group summaries and normalized margins in stats/UI.
- Remaining:
  - Add strict contract tests that fail CI on any schema drift.

### P1.3 Replay/backtest harness with costs
- Status: In progress
- Completed:
  - Paper validation scripts and strategy validation harness.
  - Cost diagnostics in runtime telemetry (cost drag bps, missing-field counters).
  - Extended trade recorder schema with gross/net/cost fields (backward compatible).
  - Deterministic replay consistency report script (`scripts/replay_consistency.py`).
- Remaining:
  - True event-level replay from scan+execution logs with deterministic re-simulation.
  - Automated per-regime replay report persisted on schedule.

### P1.4 Walk-forward validation + promotion gates
- Status: In progress (strong)
- Completed:
  - Strategy governance cycle with walk-forward summary and sample gates.
  - Promote/hold/demote decisions emitted live + audit log.
  - Governance panel in UI with action provenance.
- Remaining:
  - Calibrate thresholds on longer horizon and add confidence-bound checks.
  - Add stricter guardrails before any automatic strategy disable/promote action.

### P1.5 Portfolio risk allocation
- Status: In progress
- Completed:
  - Strategy risk multipliers with ongoing updates.
  - Underperformer demotion path via governance + allocator.
- Remaining:
  - Family-level concentration caps and correlated-exposure budget.
  - Portfolio-level kill-switch tied to drawdown + volatility state.

### P1.6 Execution risk + settlement discipline
- Status: In progress
- Completed:
  - Sim/live guardrails, explicit live confirmation, preflight checks.
  - Intelligence gate block events and atomic settlement flow checks.
  - Paper checklist automation covering key safety flows.
  - Added `BTC_5M` window-resolve execution lifecycle (reserve -> hold -> settle) in PAPER, and LIVE preflight dry-run payloads.
  - Simulation reset now preserves operator strategy toggles by default (optional `force_defaults` for deterministic benchmarks).
- Remaining:
  - More rigorous ledger reconciliation tolerances + anomaly escalation policy.
  - End-to-end invariants across reserve/release/settle on all strategy paths.

### P1.7 Full observability in UI
- Status: In progress (strong)
- Completed:
  - Runtime control plane panel (phase/module health/events/heartbeat/detail).
  - Intelligence block visibility, risk allocator view, quality and governance views.
  - Cost drag and integrity alerts surfaced in live UI.
  - New `/btc-5m-engine` page showing BTC 5m loop telemetry (spot, EV, Kelly advisory, order feed, positions, equity curve, heatmap).
- Remaining:
  - Alert severity rollups and operator playbook links in-panel.
  - Compact per-strategy provenance timeline (scan -> gate -> execution -> pnl).

## Phase 2 Status (ML)

### P2.1 Feature registry + labeling
- Status: In progress
- Completed:
  - Deterministic in-memory feature registry fed from normalized scan stream.
  - Label attachment from `strategy:pnl` events with lookback window.
  - Leakage violation counter and live UI telemetry for registry health.
  - API/socket exposure: `/api/ml/feature-registry`, stats embedding, live updates.
  - Persistent JSONL event recorder for scan/label/reset lifecycle.
  - Deterministic dataset export script and manifest (`scripts/export_feature_dataset.py`).
- Remaining:
  - Richer feature sets (book depth, volatility windows, expiry transforms).
  - Formal leakage tests in CI with synthetic data.

### P2.2 Training with time-series CV
- Status: In progress
- Completed:
  - Purged/embargoed time-series CV training harness (`scripts/train_signal_model.py`).
  - Hyperparameter search, calibration metrics, and model/report artifacts.
  - Runtime/UI ML readiness telemetry (`/api/ml/pipeline-status`, socket updates).
- Remaining:
  - Lift labeled sample counts above minimum threshold for eligible training.
  - Add training schedule and model lineage/version controls.

### P2.3 Online inference + gated deploy
- Status: In progress
- Completed:
  - Advisory online inference scaffold (model artifact load + per-scan probability telemetry).
  - API/socket exposure (`/api/ml/model-inference`, stats embedding, live update stream).
  - Runtime/UI visibility for model-loaded state, gate threshold, and per-strategy probabilities.
  - PAPER-only probability gate in execution path with block telemetry and audit visibility.
  - Control-plane endpoint for forced in-process training (`POST /api/ml/train-now`).
- Remaining:
  - Connect probability output to explicit PAPER-only execution gating policy.
  - Add model version pinning + rollback controls before any LIVE influence.

### P2.4 Drift monitoring + auto de-promotion
- Status: In progress
- Completed:
  - Online drift metrics from realized labels (Brier/logloss/calibration/accuracy EMA).
  - Drift-aware temporary gate disable window and runtime health signaling.
  - API/socket/UI visibility (`/api/ml/model-drift`, live drift panel).
- Remaining:
  - Formal strategy-level de-promotion coupling to drift thresholds.
  - Alert routing/escalation playbook for sustained CRITICAL drift.

### P2.5 ML governance
- Status: Not started

## Phase 3 Status (Novel)

### P3.1 Meta-controller / ensemble router
- Status: In progress
- Completed:
  - Regime-adaptive family allowlist and strategy multiplier recommendations.
  - Live socket/API telemetry and UI panel for overrides/rationale.
  - Runtime module health updates for `REGIME_ENGINE`, `ENSEMBLE_ROUTER`, `UNCERTAINTY_GATE`.
- Remaining:
  - Optional non-advisory routing path with explicit safety gates.
  - Backtested uplift measurement before any capital-weighted activation.

### P3.2 Regime engine
- Status: In progress

### P3.3 Uncertainty-aware admission
- Status: In progress

### P3.4 Cross-strategy lead/lag layer
- Status: Not started

### P3.5 Advanced exit optimizer
- Status: Not started

## Current Sprint Priorities (Next)
1. Increase labeled feature density and run first eligible model training cycle.
2. Build online inference gating in advisory mode with full provenance in UI.
3. Calibrate governance thresholds from long paper samples and lock promotion policy.
