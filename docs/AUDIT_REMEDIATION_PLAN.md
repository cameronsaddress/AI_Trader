# Audit Remediation Plan (Go-Live Hardening)

## Phase 0 (Implemented in this patch set)

1. Execution slippage guard at order-time:
- Status: Done
- Files:
  - `apps/backend/src/services/PolymarketPreflightService.ts`
- Notes:
  - Added top-of-book slippage validation before posting orders.
  - Added env controls: `POLY_EXECUTION_SLIPPAGE_CHECK_ENABLED`, `POLY_EXECUTION_SLIPPAGE_MAX_BPS`, `POLY_EXECUTION_SLIPPAGE_MIN_ABS`.

2. Partial-fill rollback for paired/atomic order sets:
- Status: Done
- Files:
  - `apps/backend/src/services/PolymarketPreflightService.ts`
- Notes:
  - If multi-leg execution partially posts, successful order IDs are canceled via `cancelOrders`.
  - Execution result now records rollback metadata.

3. Settlement durability (DLQ path):
- Status: Done
- Files:
  - `apps/backend/src/modules/execution/arbExecutionPipeline.ts`
  - `apps/backend/src/index.ts`
  - `apps/backend/src/config/constants.ts`
- Notes:
  - Settlement registration failures now enqueue a Redis DLQ entry.
  - Added periodic replay worker with retry cap + critical alert on exhaustion.

4. External alerting infrastructure:
- Status: Done
- Files:
  - `apps/backend/src/services/OpsAlertService.ts`
  - `apps/backend/src/index.ts`
  - `apps/backend/src/modules/execution/arbExecutionPipeline.ts`
  - `apps/backend/src/modules/risk/riskGuardModule.ts`
- Notes:
  - Added Telegram + generic webhook alert transport with dedupe/cooldown.
  - Wired to settlement failures, DLQ exhaustion, and risk-guard pause events.

5. Execution-stage timeout envelopes:
- Status: Done
- Files:
  - `apps/backend/src/modules/execution/arbExecutionPipeline.ts`
  - `apps/backend/src/services/PolymarketPreflightService.ts`
- Notes:
  - Added explicit timeouts around preflight, live execution, settlement register/emit, DLQ enqueue, and CLOB SDK calls.

6. Inline risk guard precheck (reduce async race window):
- Status: Done
- Files:
  - `apps/backend/src/index.ts`
  - `apps/backend/src/config/constants.ts`
- Notes:
  - Added hot-path cooldown/pause key checks before worker dispatch.

7. Vault sweep atomicity:
- Status: Done
- Files:
  - `apps/backend/src/modules/risk/riskGuardModule.ts`
- Notes:
  - Converted 3-step sweep into Redis `MULTI/EXEC`.

8. CLOB client transient init retry:
- Status: Done
- Files:
  - `apps/backend/src/services/PolymarketPreflightService.ts`
- Notes:
  - Startup initialization failures are now retryable after cooldown (transient class only).

9. Dead ML dependency cleanup:
- Status: Done
- Files:
  - `apps/ml-service/requirements.txt`
- Notes:
  - Removed unused TensorFlow dependency.

10. Vol regime estimator reliability + naming coherence:
- Status: Done
- Files:
  - `apps/arb-scanner/src/strategies/vol_regime.rs`
- Notes:
  - Hurst now uses adaptive multi-scale chunking.
  - Vol estimator naming/comments corrected to close-to-close proxy.

11. Soak regression check for profit-closure correctness:
- Status: Done
- Files:
  - `scripts/paper_soak_report.js`
- Notes:
  - Added invariant check for negative-net `TAKE_PROFIT` closures.

12. ML service unit tests baseline:
- Status: Done (initial)
- Files:
  - `apps/ml-service/tests/test_ensemble_signal_model.py`
- Notes:
  - Added baseline tests for bootstrap output sanity and invalid-input neutral behavior.

## Phase 1 (Implemented in this patch set)

1. Maker strategy realism:
- Status: Done
- Files:
  - `apps/arb-scanner/src/strategies/simulation.rs`
  - `apps/arb-scanner/src/strategies/maker_mm.rs`
  - `apps/arb-scanner/src/strategies/as_market_maker.rs`
- Notes:
  - Added passive fill probability + adverse-selection entry simulation.
  - Added explicit `ENTRY_MISS` events when passive fills fail.
  - Added modeled fill telemetry on both paper entries and live previews.

2. Cross-strategy directional cap:
- Status: Done
- Files:
  - `apps/backend/src/index.ts`
  - `apps/backend/src/config/constants.ts`
- Notes:
  - Added underlying-level directional netting cap (`EXECUTION_NETTING_UNDERLYING_CAP_USD`).
  - Added underlying exposure snapshot reporting in `/api/arb/stats`.

3. Config validation framework:
- Status: Done
- Files:
  - `apps/backend/src/config/startupValidation.ts`
  - `apps/backend/src/config/startupValidation.test.ts`
  - `apps/backend/src/index.ts`
- Notes:
  - Added startup fail-fast validation for critical risk/execution configuration.
  - Enforces stricter required controls when `LIVE_ORDER_POSTING_ENABLED=true`.

4. ML reliability hardening:
- Status: Done
- Files:
  - `apps/ml-service/src/services/EnsembleSignalModel.py`
  - `apps/ml-service/tests/test_ensemble_signal_model.py`
- Notes:
  - Added model state/artifact version metadata + library version stamping.
  - Added feature-distribution drift telemetry (instant + EMA + alerting).
  - Added feature-importance extraction and top-feature telemetry.
  - Added drift-aware prediction blending fallback.

## Phase 2 (Alpha expansion)

1. Cross-horizon momentum cascade scoring.
  - Status: Done
  - Notes:
    - Cross-horizon router now applies conflict taper + consensus boost + asymmetric lead/lag boosts.
    - Router persists per-strategy overlays consumed by scanner sizing.
  - Files:
    - `apps/backend/src/index.ts`
    - `apps/backend/src/config/constants.ts`

2. IV/RV divergence standalone alpha signal.
  - Status: Implemented as FAIR_VALUE strategy overlay in meta-controller.
  - Files:
    - `apps/backend/src/index.ts`
    - `apps/backend/src/config/constants.ts`

3. Funding-rate/perp-basis ingestion and signal integration.
  - Status: Done
  - Notes:
    - Added OKX + Deribit funding/basis ingestion with periodic refresh and stale handling.
    - Aggregates venue snapshots and computes contrarian crowding signal.
    - Integrates directional overlay into FAIR_VALUE strategy multipliers.
    - Added runtime/API/socket visibility for live monitoring.
  - Files:
    - `apps/backend/src/modules/alpha/marketSignalOverlay.ts`
    - `apps/backend/src/modules/alpha/marketSignalOverlay.test.ts`
    - `apps/backend/src/index.ts`
    - `apps/backend/src/config/constants.ts`
    - `.env.example`

4. Polymarket-native microstructure signals (sweep, depth asymmetry, whale behavior).
  - Status: Done (book-impulse phase)
  - Notes:
    - Extracts Polymarket book snapshots from live scan metadata.
    - Tracks per-underlying sweep/impulse state (mid-shift + spread-aware filter).
    - Applies directional overlay into FAIR_VALUE multipliers and exposes telemetry.
  - Files:
    - `apps/backend/src/modules/alpha/marketSignalOverlay.ts`
    - `apps/backend/src/modules/alpha/marketSignalOverlay.test.ts`
    - `apps/backend/src/index.ts`
    - `apps/backend/src/config/constants.ts`
    - `.env.example`
