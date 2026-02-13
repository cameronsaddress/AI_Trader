# Profit System Backlog (Detailed, Operator-Visible)

This is the detailed execution backlog behind `docs/PROFIT_SYSTEM_MASTER_PLAN.md`.

Goals:
- Maximize *repeatable, risk-adjusted* profit over time (not a single run).
- Keep the system "air tight" for paper -> live: no hidden automations, no silent failures.
- Only use currently wired data sources (Coinbase Advanced Trade WS + Polymarket Gamma/CLOB WS + internal telemetry/logs).

How to use this file:
- Items are ordered roughly by dependency (foundation first).
- Anything that changes behavior must come with:
  - an acceptance test (script/command),
  - UI telemetry (no ghosts),
  - and a clear rollback/safety story.

Legend:
- `[ ]` not started
- `[~]` in progress
- `[x]` complete

---

## Definition Of Done (Paper -> Live Readiness)

Any strategy is "live-eligible" only when ALL are true:
- Data hygiene:
  - [ ] Feed staleness/sequence-gap detection exists and is visible per feed.
  - [ ] Strategy refuses to act on stale books/prices with explicit hold reason.
- Signal contract:
  - [ ] Emits normalized scan schema (`strategy`, `market_key`, `score`, `threshold`, `passes_threshold`, `unit`, `metric_family`, `reason`, `timestamp`, `meta`).
  - [ ] Direction semantics are correct and consistent (signed vs absolute).
- Execution safety:
  - [ ] No order posting unless global mode is `LIVE` AND `LIVE_ORDER_POSTING_ENABLED=true` AND UI confirmation happened.
  - [ ] Preflight payload includes enough information to reconstruct the exact intended order.
  - [ ] Intelligence gate + model gate (if enabled) can block, with a UI-visible reason.
- Accounting:
  - [ ] Paper sim reserves/release/settle are invariant-safe (no stuck reserved capital).
  - [ ] `strategy:pnl` emits with `pnl`, `notional`, `net_return`, `gross_return` (when available), and cost fields.
  - [ ] Backend ledger reconciliation passes within tolerance.
- Observability:
  - [ ] UI shows the full pipeline: feed -> features -> signal -> gate -> sizing -> execution -> settlement -> PnL.
  - [ ] UI shows latest scan + last N events + last block reasons for the strategy.
- Validation:
  - [ ] `scripts/run_live_paper_checklist.sh` passes.
  - [ ] `scripts/run_paper_long_validation.sh` passes for a meaningful duration (>= 2h).
  - [ ] Replay consistency (`scripts/run_replay_consistency.sh`) passes on recorded trades.

---

## Current Strategy Library (What We Have)

All of the following are implemented in `apps/arb-scanner/src/strategies/` and run via swarm workers in `infrastructure/docker-compose.yml`:
- `BTC_5M`: Coinbase spot vs Polymarket 5-minute window odds, hold-to-resolution (PAPER), live dry-run preflight only.
- `BTC_15M` / `ETH_15M` / `SOL_15M`: fair-value edge vs Polymarket 15-minute windows (PAPER execution).
- `CEX_SNIPER`: latency/momentum style signal using Coinbase vs Polymarket.
- `OBI_SCALPER`: order book imbalance admission + decay logic (PAPER execution).
- `ATOMIC_ARB`: YES/NO ask-sum parity arb scanner.
- `SYNDICATE`: whale/cluster flow detector (PAPER execution).
- `GRAPH_ARB`: constraint/parity-style arb across related markets (PAPER execution).
- `CONVERGENCE_CARRY`: convergence/carry style mean reversion (PAPER execution).
- `MAKER_MM`: micro market-making / spread capture (PAPER execution).

Dedicated UI pages:
- `/polymarket`
- `/btc-5m-engine`

Backlog below includes adding dedicated pages for each strategy (required for “no ghosts”).

---

## Phase 1 Backlog (Standard Techniques / Foundation)

### P1.1 Data Integrity + Microstructure Hygiene

- [~] P1.1.1 Feed-level sequence gap + staleness KPIs
  - Add per-feed counters:
    - Coinbase WS reconnects, last message age, missing heartbeats
    - Polymarket WS reconnects, last book update age, book invalid intervals
  - UI: “Feed Health” panel per strategy (pass/hold/blocked breakdown with staleness counters).
  - Acceptance: induce a WS disconnect and confirm:
    - strategy holds with `stale` category
    - UI shows feed degraded within 3s

- [ ] P1.1.2 Hard “no stale execution” invariants
  - Add explicit assertions in execution paths:
    - if a trade is emitted (`ENTRY`), prove spot/book timestamps were within bounds.
  - Backend: raise a CRITICAL runtime alert if any execution event violates freshness invariants.
  - Acceptance: unit/integration test with synthetic stale timestamps forces HOLD and logs a CRITICAL if violated.

- [ ] P1.1.3 Clock consistency + time-bucket correctness
  - Ensure window strategies compute window start/expiry consistently (UTC vs ET display only).
  - Add a UI debug row that displays:
    - `now`, `window_start_ts`, `expiry_ts`, `seconds_to_expiry`
  - Acceptance: window label matches expected ET time for the current epoch bucket.

### P1.2 Signal Contract Normalization

- [ ] P1.2.1 Contract tests
  - Add a small schema validator (Zod) in backend for `arbitrage:scan` messages.
  - On schema drift: mark `SCAN_INGEST` as DEGRADED and surface a UI alert.
  - Acceptance: inject malformed scan message and confirm alert.

- [ ] P1.2.2 Strict directionality policy
  - Document per-signal rule:
    - SIGNED: sign means direction (UP/DOWN, LONG/SHORT)
    - ABSOLUTE: magnitude only (arb edge)
  - Enforce in backend descriptor inference and ensure peer-consensus uses the correct semantics.
  - Acceptance: strategy consensus direction matches `meta.best_side`.

### P1.3 Replay / Backtest With Costs

- [ ] P1.3.1 Deterministic event recorder (scan + exec + pnl)
  - Persist a compact JSONL stream for:
    - scans (normalized schema)
    - execution events
    - pnl events
  - Include stable ids: `strategy`, `market_key`, and correlation id if present.

- [ ] P1.3.2 Event-level replay runner
  - Re-simulate strategies from recorded events (no live WS required).
  - Include cost model and slippage assumptions.
  - Output:
    - equity curve
    - per-strategy expectancy + tail stats
    - “edge realization” charts (predicted vs realized)

- [ ] P1.3.3 Automated daily replay report
  - Cron-like script that generates a replay report from the last N hours of logs.
  - UI link to the most recent replay artifact.

### P1.4 Walk-Forward Validation + Promotion Gates

- [ ] P1.4.1 Promotion gates per strategy family
  - Each family gets explicit requirements:
    - min trades, max drawdown, min edge realization, max cost drag
  - UI: show “eligible / not eligible” with exact failing constraint.

- [ ] P1.4.2 Guarded autopilot improvements
  - Autopilot remains PAPER-only until:
    - replay is stable
    - drift monitors exist
  - Add an operator toggle:
    - “Autopilot Enabled”
    - “Autopilot Effective” (depends on mode + health)

### P1.5 Portfolio Risk Allocation

- [~] P1.5.1 Family concentration caps enforced in reserve logic
  - Ensure family caps are:
    - configurable,
    - visible in UI,
    - and enforced uniformly across strategies.

- [ ] P1.5.2 Correlated exposure budget (simple first)
  - Group strategies by:
    - asset (BTC/ETH/SOL)
    - horizon (5m/15m)
  - Set caps per group to avoid stacking correlated bets.
  - UI: show “exposure by group” + “cap remaining”.

### P1.6 Execution Risk + Settlement Discipline

- [x] P1.6.1 Mode toggle + confirmation + arm flag
  - Verified by `scripts/run_live_paper_checklist.sh`

- [ ] P1.6.2 Unified execution provenance id
  - Add `execution_id` (UUID) to all `ENTRY` / `WIN` / `LOSS` / `SETTLEMENT` events and `strategy:pnl`.
  - Backend stores and shows a trace:
    - scan -> gate -> sizing -> execution -> pnl
  - UI: click an event row and see full trace.

- [ ] P1.6.3 Settlement staleness + anomaly escalations
  - Flag:
    - delayed settlement
    - inconsistent token ids
    - repeated retries
  - UI: Settlement health panel with last errors and counts.

- [ ] P1.6.4 Crash-safe position state persistence (required before real money)
  - Problem: scanners currently hold open positions in memory; a restart can strand reserved capital and lose settlement logic.
  - Persist PAPER positions in Redis on `ENTRY`:
    - key: `sim_position:{strategy}:{market_key}:{execution_id}`
    - value: JSON including expiry, side, notional, entry, and any required settlement fields
  - On settlement:
    - delete the key
    - update ledger and emit `strategy:pnl`
  - On scanner boot:
    - load any open positions for that strategy and continue to settlement (or reconcile if expired).
  - Backend:
    - expose “open positions” snapshot per strategy in `/api/arb/stats`
    - show “reserved but no positions” as a WARN/CRITICAL anomaly.

- [ ] P1.6.5 Paper-to-live parity: identical lifecycle semantics
  - Ensure the live path (when enabled later) mirrors the paper lifecycle:
    - entry -> confirmation -> settlement tracking -> pnl attribution
  - Add an explicit “dry-run vs live” execution trace in UI so an operator can verify the venue interactions.

### P1.7 UI Coherence (No Ghosts)

- [ ] P1.7.1 Remove duplicated/ambiguous widgets
  - Decide:
    - “Arbitrage Scanner” = per-strategy row view
    - “Strategy Intelligence” = portfolio-level summary + sortable list
  - Make them linked (click strategy row -> deep dive page).

- [ ] P1.7.2 System stats are never misleading
  - Ensure any “pass rate”, “avg threshold”, etc is clearly labeled:
    - system-wide
    - per-family
    - per-strategy
    - per-asset
  - Add UI filters for these scopes (default: system-wide + per-strategy table).

- [ ] P1.7.3 Dedicated pages for each strategy (required)
  - `/atomic-arb`
  - `/obi-scalper`
  - `/cex-sniper`
  - `/fair-value` (BTC/ETH/SOL)
  - `/syndicate`
  - `/graph-arb`
  - `/convergence-carry`
  - `/maker-mm`
  - Each page must show:
    - live inputs
    - computed signal + thresholds
    - pass/hold reasons
    - risk sizing
    - last 100 executions + pnl events

### P1.8 Standard Strategy Expansion (Using Existing Feeds Only)

- [ ] P1.8.1 Window engines beyond BTC
  - Implement ETH and SOL 5-minute window engines:
    - `ETH_5M`, `SOL_5M` (same lifecycle as `BTC_5M`)
  - Use Coinbase spot feeds (`ETH-USD`, `SOL-USD`) and Polymarket window odds if available.
  - Acceptance:
    - scanners publish signed expected net return
    - paper sim reserves/settles correctly
    - UI tiles + dedicated pages exist

- [ ] P1.8.2 Auto-discover window markets (stop hardcoding slug patterns)
  - Prefer Gamma search for currently active window markets for each asset/horizon and pick the correct market by start/end time.
  - Acceptance:
    - strategy survives when Polymarket changes slug formats
    - strategy always trades the correct active market for the current window

- [ ] P1.8.3 Cross-horizon consistency checks
  - Compare short-horizon and longer-horizon probabilities for sanity:
    - flag extreme inconsistencies as potential data issues or structural mispricing
  - UI: show “horizon consistency” metric and recent violations.

- [ ] P1.8.4 Horizon router (portfolio-level)
  - If multiple window markets are available (5m/15m/etc), route capital to the best risk-adjusted edge:
    - respect caps
    - avoid stacking correlated bets in the same direction
  - Must be fully visible in UI (why a horizon was selected).

---

## Phase 2 Backlog (ML, Production-Safe)

Principle: ML is advisory first. It must never become a black box that silently changes behavior.

### P2.1 Feature Registry + Deterministic Labeling

- [ ] P2.1.1 Canonical join keys (required)
  - Every scan/execution/pnl event must share stable keys:
    - `strategy`, `market_key`, `execution_id`
  - This is required for:
    - correct labeling
    - leak-free training
    - replay and “edge realization” measurement

- [~] P2.1.2 Feature schema (per strategy family)
  - Only derived from current feeds:
    - Coinbase spot/L2 features
    - Polymarket book features
    - time-to-expiry/window transforms
  - Produce a stable map per scan:
    - base: spread, parity, staleness, latency, seconds_to_expiry, liquidity proxies
    - family-specific: OBI, momentum, fair-value edge, etc
  - UI requirement:
    - show “top N features” contributing to the score for each decision (even if ML is advisory)

- [~] P2.1.3 Label definitions (simple first)
  - Primary label: realized `net_return` from `strategy:pnl`.
  - Secondary labels:
    - win/loss classification
    - realized cost drag (if gross_return is available)
  - UI requirement:
    - per-strategy “predicted EV vs realized” chart (reliability curve)

- [~] P2.1.4 Leakage tests
  - Fail training if:
    - a feature uses post-entry prices
    - a label horizon overlaps the feature timestamp window incorrectly

### P2.2 Training (Time-Series CV, Calibrated Outputs)

- [~] P2.2.1 Baseline models per family
  - Start with a boring baseline per family:
    - logistic/linear model
    - tree model (XGBoost/LightGBM style)
  - Acceptance:
    - out-of-sample beats “no ML gate” baseline by a meaningful margin net of costs

- [~] P2.2.2 Purged/embargoed CV (strict)
  - Use time-series CV with embargo and purge so that:
    - window markets don’t leak future labels into earlier folds
  - Acceptance:
    - CV metrics are stable across folds (no single-fold “miracle”)

- [~] P2.2.3 Calibration + uncertainty
  - Calibrate probabilities (isotonic or Platt) and expose:
    - reliability curve
    - calibration error
  - Add uncertainty bounds (conformal/quantile) as advisory telemetry.

- [ ] P2.2.4 Model artifacts + model cards
  - Persist:
    - training window
    - feature schema version
    - metrics (CV + OOS)
    - calibration and drift baselines
  - UI: model card view per promoted model.

### P2.3 Online Inference + Safe Deployment (PAPER-First)

- [~] P2.3.1 Shadow mode (no gating)
  - ML runs, emits probability, but never blocks.
  - UI: per-strategy probability stream + last model version.

- [~] P2.3.2 PAPER-only probability gate (bounded)
  - Gate only in PAPER mode at first.
  - Must emit a `strategy_model_block` event with:
    - probability
    - threshold
    - model version
    - blocking reason
  - Acceptance:
    - turning off the gate reverts to deterministic baseline immediately.

- [ ] P2.3.3 Canary + rollback for any “active” ML influence
  - Canary = 1 strategy at a time, bounded notional.
  - Rollback path:
    - config toggle
    - model version pin
    - auto-disable on drift

### P2.4 Drift Monitoring + De-Promotion

- [~] P2.4.1 Online drift metrics
  - Track:
    - performance drift (net_return EMA)
    - calibration drift (Brier/logloss)
    - feature distribution drift (simple PSI/KL)
  - UI: drift panel per strategy/family.

- [ ] P2.4.2 Automatic “safety demotion” actions (PAPER-first)
  - When drift is CRITICAL:
    - gate goes advisory-only
    - strategy multiplier reduces
    - operator alert + audit entry created

- [ ] P2.4.3 Operator playbook
  - Provide a UI link that explains:
    - what drift means
    - what actions were taken
    - how to recover safely

### P2.5 ML Governance

- [ ] P2.5.1 Model registry + version pinning
  - Explicit states:
    - `TRAINED`, `SHADOW`, `PAPER_GATE`, `LIVE_GATE` (future), `RETIRED`
  - Hard rule: only one active version per strategy family at a time.

- [ ] P2.5.2 Promotion workflow
  - No model can promote without:
    - min samples
    - stable CV
    - stable calibration
    - replay uplift vs baseline
  - UI: “promote model” requires explicit operator action and shows a diff vs baseline.

### P2.6 Bounded Auto-Tuning (Knobs, Not Magic)

- [ ] P2.6.1 Enumerate tunable knobs + safe bounds
  - Per strategy define:
    - threshold ranges
    - spread filters
    - cooldowns
    - sizing caps
  - Hard safety: auto-tuning never changes `LIVE` behavior without explicit promotion.

- [ ] P2.6.2 Offline evaluation harness
  - For each knob:
    - sensitivity curves
    - overfit detection (walk-forward stability)

- [ ] P2.6.3 Paper exploration (bandit/Bayes, bounded)
  - Only in PAPER mode.
  - Log every parameter selection with a unique tuning id.
  - UI:
    - “active knob values”
    - “why chosen”
    - performance by knob version

- [ ] P2.6.4 Freeze + promote
  - Once a configuration proves stable:
    - freeze it
    - promote it as a versioned config with rollback

---

## Phase 3 Backlog (Novel Alpha Layers)

Principle: only after Phase 1+2 are stable do we add novel alpha layers.

### P3.1 Meta-Controller / Ensemble Router (Operator-Visible)

- [~] P3.1.1 Regime features and classification
  - Produce a stable regime vector:
    - vol state
    - trend state
    - liquidity state
    - latency state
  - UI: “regime tape” (last 60 minutes).

- [~] P3.1.2 Router recommendations (advisory first)
  - Suggest:
    - family allowlist
    - strategy multipliers
    - direction conflict suppression (don’t stack correlated risk)
  - UI: show rationale and the exact inputs used.

- [ ] P3.1.3 Router A/B measurement
  - Measure uplift over:
    - baseline allocator
    - best-single strategy
  - Only promote to influence capital when uplift is stable.

### P3.2 Lead/Lag + Micro-Latency Alpha

- [ ] P3.2.1 Reaction-time measurement
  - Measure:
    - time from Coinbase move -> Polymarket book adjustment
    - distribution of lag by market liquidity and spread
  - UI: lag histogram + “current lag score”.

- [ ] P3.2.2 Trade admission based on statistically significant lag
  - Only trade if:
    - lag score exceeds threshold
    - expected net edge clears costs
    - staleness checks pass

### P3.3 Exit Optimizer (Edge Realization)

- [ ] P3.3.1 Baseline vs optimized exits in replay
  - Compare:
    - hold-to-expiry
    - edge decay exit
    - time stop / hazard exit
  - Choose the simplest exit that improves realized edge.

- [ ] P3.3.2 UI exit provenance
  - Every close must show:
    - which exit rule fired
    - the exact inputs (edge, time, volatility)

### P3.4 Novel “Alpha Stack” Experiments (Strictly Paper)

- [ ] P3.4.1 Cross-horizon relative value
  - Exploit mispricings between 5m and 15m windows (if both exist).
  - Requires sanity checks + strict caps due to correlation risk.

- [ ] P3.4.2 Inventory-aware market making
  - Maker quotes adapt to inventory and regime to reduce adverse selection.
  - Must show live inventory + quote shading in UI.

---

## Next Concrete Work Items (Recommended Order)

1. P1.7.2: fix any misleading aggregated UI stats (scope labels + filters).
2. P1.6.2: add execution provenance id and trace view.
3. P1.3.1 + P1.3.2: event-level recorder + deterministic replay.
4. P1.7.3: dedicated pages for each strategy (copy the BTC 5m page pattern).
5. P1.1.2: hard invariant checks and CRITICAL alerting on any stale execution.
