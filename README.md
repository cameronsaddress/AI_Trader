# AI Trader 2: Go-Live Operations Manual

Last updated: 2026-02-19

This document is the operator guide for running AI Trader 2 in paper, canary, and live modes.

## 1. Scope and Operating Rules

- Goal: maximize repeatable, risk-adjusted profit while preserving capital and preventing silent failures.
- System is built for disciplined edge capture, not guaranteed profit.
- Any live promotion must pass objective readiness gates first.
- Paper mode is the default and should remain the default unless all checks pass.

## 2. Architecture Overview

AI Trader 2 is a multi-service trading stack with:

- Rust scanner swarm for strategy signal generation and paper/live execution intents.
- Node.js backend as control plane, risk layer, execution gatekeeper, and telemetry aggregator.
- Redis for pub/sub and runtime control keys.
- PostgreSQL for persistence.
- React frontend for real-time operations and strategy pages.
- Python ML service for supervised model signals.

Core flow:

1. Market data and Polymarket books stream in.
2. Strategies emit `arbitrage:scan` and execution payloads.
3. Backend applies intelligence/model/preflight/risk checks.
4. Accepted executions become paper fills or live posts.
5. PnL, settlement, allocator, governance, drift, and ledger modules update continuously.

## 3. Services and Containers

`infrastructure/docker-compose.yml` currently defines 21 services:

| Service | Role |
|---|---|
| `backend` | Control plane, risk engine, execution pipeline, APIs, sockets |
| `frontend` | Dashboard + strategy pages |
| `postgres` | DB storage |
| `redis` | Pub/sub + runtime state |
| `ml-service` | ML inference/training endpoints |
| `arb-builder` | One-time Rust build artifact producer |
| `arb-scanner-btc` | `BTC_15M` |
| `arb-scanner-btc-5m` | `BTC_5M` |
| `arb-scanner-eth` | `ETH_15M` |
| `arb-scanner-eth-5m` | `ETH_5M` |
| `arb-scanner-sol` | `SOL_15M` |
| `arb-scanner-sol-5m` | `SOL_5M` |
| `arb-scanner-cex` | `CEX_ARB` -> canonical `CEX_SNIPER` |
| `arb-scanner-copy` | `COPY_BOT` -> canonical `SYNDICATE` |
| `arb-scanner-atomic` | `ATOMIC_ARB` |
| `arb-scanner-obi` | `OBI_SCALPER` |
| `arb-scanner-graph` | `GRAPH_ARB` |
| `arb-scanner-convergence` | `CONVERGENCE_CARRY` |
| `arb-scanner-maker` | `MAKER_MM` |
| `arb-scanner-as-mm` | `AS_MARKET_MAKER` |
| `arb-scanner-longshot` | `LONGSHOT_BIAS` |

## 4. Strategy Catalog

Canonical strategy IDs (`STRATEGY_IDS` in `apps/backend/src/config/constants.ts`):

| Strategy | Family | Primary edge |
|---|---|---|
| `BTC_5M` | `FAIR_VALUE` | 5m fair-value mispricing with multi-venue spot + model filters |
| `BTC_15M` | `FAIR_VALUE` | 15m fair-value edge on BTC binary windows |
| `ETH_5M` | `FAIR_VALUE` | 5m fair-value edge on ETH |
| `ETH_15M` | `FAIR_VALUE` | 15m fair-value edge on ETH |
| `SOL_5M` | `FAIR_VALUE` | 5m fair-value edge on SOL |
| `SOL_15M` | `FAIR_VALUE` | 15m fair-value edge on SOL |
| `CEX_SNIPER` | `CEX_MICROSTRUCTURE` | Coinbase momentum dislocation vs Polymarket |
| `SYNDICATE` | `FLOW_PRESSURE` | On-chain flow cluster following |
| `ATOMIC_ARB` | `ARBITRAGE` | YES+NO pair underpricing vs payout |
| `OBI_SCALPER` | `ORDER_FLOW` | Order-book imbalance scalp |
| `GRAPH_ARB` | `ARBITRAGE` | Multi-market no-arb constraint violations |
| `CONVERGENCE_CARRY` | `CARRY_PARITY` | YES/NO parity mean reversion carry |
| `MAKER_MM` | `MARKET_MAKING` | Spread capture with entry/exit regime controls |
| `AS_MARKET_MAKER` | `MARKET_MAKING` | Avellaneda-Stoikov reservation/half-spread logic |
| `LONGSHOT_BIAS` | `BIAS_EXPLOITATION` | Longshot mispricing fade |

## 5. Baseline Strategy Tuning (Current Build)

Baseline values come from scanner code defaults plus `infrastructure/docker-compose.yml` overrides.

Key runtime overrides currently set in compose:

| Strategy | Key overrides |
|---|---|
| `BTC_5M` | `BTC_5M_MIN_EXPECTED_NET_RETURN=0.008`, `BTC_5M_MAX_POSITION_FRACTION=0.10`, `BTC_5M_MAX_ENTRY_SPREAD=0.05`, plus momentum/regime/model filter penalties and Kelly/edge scaling |
| `ATOMIC_ARB` | `ATOMIC_ARB_MIN_NET_EDGE=0.0010`, `ATOMIC_ARB_MIN_BUFFER_FLOOR=0.0020` |
| `GRAPH_ARB` | `GRAPH_ARB_MIN_NET_EDGE=0.0015`, `GRAPH_ARB_MIN_BUFFER_FLOOR=0.0010`, `GRAPH_ARB_HORIZON_PENALTY_COEFF=0.0005` |
| `CONVERGENCE_CARRY` | `CONVERGENCE_CARRY_MIN_PARITY_EDGE=0.008`, `CONVERGENCE_CARRY_EXIT_PARITY_EDGE=0.002` |
| `MAKER_MM` | `MAKER_MM_MIN_EXPECTED_NET_RETURN=0.001` |
| `AS_MARKET_MAKER` | `AS_MM_GAMMA=0.3`, `AS_MM_KAPPA=1.5`, `AS_MM_MIN_HALF_SPREAD=0.015`, `AS_MM_MIN_EDGE_BPS=15` |
| `LONGSHOT_BIAS` | `LONGSHOT_PRICE_CEILING=0.15`, `LONGSHOT_MIN_NET_EDGE=0.04` |

Shared simulation knobs applied across most scanners:

- `SIM_FEE_BPS_PER_SIDE=0`
- `SIM_SLIPPAGE_BPS_PER_SIDE=2`

## 6. Auto-Tuning and Control Loops

Major auto-adjusting modules:

- Strategy allocator: updates per-strategy multipliers from PnL/quality (`strategy:risk_multiplier:*`).
- Meta allocator: portfolio-level overlay (`META_ALLOCATOR_*`).
- Cross-horizon router: resolves fast/slow conflicts and boosts confirmations (`CROSS_HORIZON_ROUTER_*`).
- Strategy governance cycle: advisory or autopilot policy adjustments (`STRATEGY_GOVERNANCE_*`).
- Model probability gate tuner: recalibrates threshold by counterfactual PnL (`MODEL_PROBABILITY_GATE_TUNER_*`).
- In-process model trainer: periodic retraining (`MODEL_TRAINER_*`), with guardrails to retain the last eligible model when a low-label retrain attempt is ineligible.
- Drift monitor: can disable model gate enforcement when drift breaches thresholds.
- Risk guard: trailing stop, consecutive-loss cooldowns, anti-martingale taper.
- Execution shortfall optimizer: reduces/blocks low-retained-edge entries.
- Execution netting: blocks excess same-market directional concentration.
- Vault sweeper: parks profits beyond bankroll ceiling.
- Ledger health monitor: concentration/reserve invariants.
- PnL parity watchdog: checks paper trade-log PnL vs ledger realized PnL.

## 7. Safety and Go-Live Latches

Hard controls:

- `LIVE_ORDER_POSTING_ENABLED=false` by default.
- `/api/system/trading-mode` requires control-plane auth and `confirmation: "LIVE"` when setting LIVE.
- Live readiness checks enforce:
1. scanner heartbeat health
2. Redis connectivity
3. Polymarket preflight readiness
4. ML eligibility + loaded artifact + fresh inference
5. settlement readiness
6. ledger critical checks
7. PnL parity watchdog freshness/critical status

Control plane auth:

- Set `CONTROL_PLANE_TOKEN`.
- Pass `Authorization: Bearer <token>` for mutating API calls.
- Socket privileged actions require the same token via socket auth.

## 8. Environment and Requirements

### Runtime Requirements

- Docker + Docker Compose
- Node.js 20+
- Rust toolchain (for local scanner builds)
- Redis + Postgres (via compose)
- API credentials:
1. Coinbase keys for market feed/execution paths
2. Polymarket signer/funder keys for preflight/live posting
3. LLM/ML provider keys as needed

### Configuration Sources

- Primary defaults/template: `.env.example`
- Conservative canary profile: `config/live_canary_baseline.env`
- Compose runtime wiring: `infrastructure/docker-compose.yml`

Critical env groups:

- Safety: `LIVE_ORDER_POSTING_ENABLED`, `CONTROL_PLANE_TOKEN`, `EXECUTION_HALT_*`.
- Heartbeat and runtime health: `SCANNER_HEARTBEAT_MAX_AGE_MS`, `SCANNER_OPTIONAL_HEARTBEAT_IDS`.
- Ledger caps: `SIM_STRATEGY_CONCENTRATION_CAP_PCT`, `SIM_FAMILY_CONCENTRATION_CAP_PCT`, `SIM_UNDERLYING_CONCENTRATION_CAP_PCT`, `SIM_GLOBAL_UTILIZATION_CAP_PCT`.
- PnL parity watchdog: `LEDGER_PNL_PARITY_*`.
- ML and model gate: `MODEL_TRAINER_*`, `MODEL_PROBABILITY_GATE_*`.
- Execution controls: `EXECUTION_SHORTFALL_*`, `EXECUTION_NETTING_*`.
- Polymarket preflight/live limits: `POLY_*` and `POLY_LIVE_STRATEGY_ALLOWLIST`.

## 9. Start, Stop, Rebuild

### Initial Startup

```bash
cp .env.example .env
# fill real secrets in .env and/or apps/backend/.env
./stack.sh start
./stack.sh status
```

### Stop/Restart

```bash
./stack.sh stop
./stack.sh restart
```

### Rebuild Scanners After Rust Changes

```bash
docker compose -f infrastructure/docker-compose.yml run --rm arb-builder cargo build
docker compose -f infrastructure/docker-compose.yml up -d --no-deps --force-recreate \
  arb-scanner-btc arb-scanner-btc-5m arb-scanner-eth arb-scanner-eth-5m \
  arb-scanner-sol arb-scanner-sol-5m arb-scanner-cex arb-scanner-copy \
  arb-scanner-atomic arb-scanner-obi arb-scanner-graph arb-scanner-convergence \
  arb-scanner-maker arb-scanner-as-mm arb-scanner-longshot
```

## 10. Operator UI and Pages

Frontend routes in `apps/frontend/src/App.tsx`:

- `/` home dashboard
- `/overview`
- `/polymarket` main operations cockpit
- `/btc-5m-engine` dedicated BTC 5m deep-dive
- `/hft`
- `/atomic-arb`
- `/obi-scalper`
- `/cex-sniper`
- `/fair-value`
- `/syndicate`
- `/graph-arb`
- `/convergence-carry`
- `/maker-mm`
- `/strategies` unified multi-strategy coverage

## 11. API Quick Reference

Read-only ops endpoints:

- `GET /health`
- `GET /api/system/runtime-status`
- `GET /api/system/trading-mode`
- `GET /api/arb/bots`
- `GET /api/arb/stats`
- `GET /api/arb/intelligence`
- `GET /api/arb/ledger-health`
- `GET /api/arb/pnl-parity`
- `GET /api/arb/governance`
- `GET /api/arb/cross-horizon-router`
- `GET /api/ml/pipeline-status`
- `GET /api/ml/model-inference`
- `GET /api/ml/model-drift`
- `GET /api/arb/settlements`
- `GET /api/arb/strategy-executions?strategy=<ID>&limit=<N>`
- `GET /api/arb/strategy-trades?strategy=<ID>&limit=<N>`
- `GET /api/arb/rejected-signals`

Control-plane endpoints (Bearer token required):

- `POST /api/system/trading-mode`
- `POST /api/system/reset-simulation`
- `POST /api/system/execution-halt`
- `POST /api/arb/risk`
- `POST /api/ml/train-now`
- `POST /api/arb/governance/autopilot`
- `POST /api/arb/settlements/process`
- `POST /api/arb/settlements/reset`
- `POST /api/arb/validation-trades/reset`
- `POST /api/arb/rejected-signals/reset`

Set LIVE mode example:

```bash
curl -X POST http://localhost:5114/api/system/trading-mode \
  -H "Authorization: Bearer $CONTROL_PLANE_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"mode":"LIVE","confirmation":"LIVE"}'
```

## 12. Validation and Promotion Gates

Required paper validation commands:

```bash
scripts/run_live_paper_checklist.sh
scripts/run_paper_soak_report.sh
scripts/run_paper_long_validation.sh
scripts/run_strategy_validation.sh apps/backend/logs/strategy_trades.csv reports/strategy_validation.json
```

ML warmup (without forcing a state reset) when `MODEL_PROBABILITY_GATE_REQUIRE_MODEL=true`:

```bash
CONTROL_PLANE_TOKEN="$CONTROL_PLANE_TOKEN" \
API_URL="http://localhost:5114" \
TRAIN_LABEL_THRESHOLD=20 \
node scripts/soak_checkpoint_monitor.js
```

Notes:
- `scripts/run_paper_soak_report.sh` intentionally resets simulation/trade/reject state before sampling.
- Use the checkpoint monitor for continuous label accumulation and auto-train trigger visibility.

Optional staged live-fire drill:

```bash
scripts/run_live_fire_drill.sh
```

Default soak acceptance thresholds:

- `SOAK_MIN_PREFLIGHT_PASS_RATE_PCT=96`
- `SOAK_MAX_SHORTFALL_BLOCK_RATE_PCT=18`
- `SOAK_MAX_NETTING_BLOCK_RATE_PCT=12`
- `SOAK_MIN_EDGE_CAPTURE_RATIO=0.55`

Recommended go-live minimums:

1. No critical runtime modules in `/api/system/runtime-status`
2. Scanner alive with no missing required heartbeat IDs
3. `ledger_health.status != CRITICAL`
4. `pnl_parity.status != CRITICAL` after warmup rows
5. ML pipeline eligible, model loaded, inference fresh
6. Live paper checklist passing
7. Soak report passing thresholds over meaningful duration

## 13. Go-Live Procedure (Phased)

### Phase A: Paper Hardening

1. Keep `LIVE_ORDER_POSTING_ENABLED=false`.
2. Run long paper soak and checklist scripts.
3. Fix all critical or recurring WARN classes.
4. Verify strategy coverage and no orphaned disabled strategy.

### Phase B: Live-Readiness Config

1. Apply `config/live_canary_baseline.env` values to runtime env.
2. Keep allowlist constrained (`POLY_LIVE_STRATEGY_ALLOWLIST`).
3. Ensure model gate enforcement for live is enabled:
`MODEL_PROBABILITY_GATE_ENFORCE_LIVE=true`
`MODEL_PROBABILITY_GATE_REQUIRE_MODEL=true`

### Phase C: Controlled Activation

1. Set `LIVE_ORDER_POSTING_ENABLED=true` and restart backend.
2. Request LIVE mode with confirmation via API/UI.
3. Start with a reduced strategy allowlist and low notional ceilings.
4. Monitor fills, rejects, ledger, parity, and settlement continuously.

### Phase D: Gradual Expansion

1. Increase allowlist breadth slowly.
2. Scale notional caps only after stable checkpoint windows.
3. Re-run soak/checklist after each material config change.

## 14. Incident Response and Rollback

Immediate rollback actions:

1. Halt execution:
```bash
curl -X POST http://localhost:5114/api/system/execution-halt \
  -H "Authorization: Bearer $CONTROL_PLANE_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"halted":true,"reason":"operator emergency stop"}'
```
2. Force PAPER mode:
```bash
curl -X POST http://localhost:5114/api/system/trading-mode \
  -H "Authorization: Bearer $CONTROL_PLANE_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"mode":"PAPER"}'
```
3. If needed, set `LIVE_ORDER_POSTING_ENABLED=false` and restart backend.
4. Review:
- `/api/arb/rejected-signals`
- `/api/arb/stats`
- `/api/system/runtime-status`
- container logs from `docker compose -f infrastructure/docker-compose.yml logs -f`

## 15. Scheduled Runtime Tasks

Current backend schedulers (`apps/backend/src/index.ts`):

| Task | Interval |
|---|---|
| `SystemHealth` | 2s |
| `Settlement` | `POLY_SETTLEMENT_POLL_MS` (default 30s) |
| `Governance` | `STRATEGY_GOVERNANCE_INTERVAL_MS` (default 60s) |
| `LedgerHealth` | 5s |
| `PnlParity` | `LEDGER_PNL_PARITY_INTERVAL_MS` (default 30s) |
| `MetaController` | 3s |
| `CrossHorizonRouter` | `CROSS_HORIZON_ROUTER_INTERVAL_MS` (default 5s) |
| `MLPipeline` | 10s |
| `MLTrainer` | `MODEL_TRAINER_INTERVAL_MS` (default 120s) |
| `FeatureLabelBackfill` | `FEATURE_LABEL_BACKFILL_INTERVAL_MS` (default 180s) |
| `ModelGateCalibration` | `MODEL_PROBABILITY_GATE_TUNER_INTERVAL_MS` (default 120s) |
| `StateBroadcast` | 5s |
| `Brain` | 30s only if legacy unsafe flags are explicitly enabled |

## 16. Data and Artifact Paths

Important files and logs:

- Strategy trade dataset: `apps/backend/logs/strategy_trades.csv`
- Rejected signal log: `apps/backend/logs/rejected_signals.jsonl`
- Soak reports: `reports/paper_soak_report.json`, `reports/paper_soak_report.md`
- Long validation report: `reports/paper_long_validation_report.json`
- Model report: `reports/models/signal_model_report.json`
- Model artifact: `reports/models/signal_model_latest.json`

## 17. Opportunities Not Yet Fully Leveraged

High-value additions still available:

1. Cross-market event relationship graph (shared event factors beyond current pair/arbitrage links).
2. Fill-probability/queue-position model for maker paths to improve expected edge realism.
3. Dynamic venue-weighted microstructure fusion for OBI/CEX strategies beyond current Coinbase-dominant path.
4. Explicit execution-cost model calibration loop that learns slippage/fee drift per market regime.
5. Strategy-family hedging layer that offsets correlated downside across active positions in real time.

These should be added only with the same gating discipline: paper soak, validation reports, and controlled canary.
