# Strategy Validation Harness

This repo now includes a research-oriented validator:

- `scripts/strategy_validation.py`

It provides:

1. Anchored walk-forward out-of-sample metrics.
2. CSCV-style Probability of Backtest Overfitting (PBO) across variants.
3. Deflated Sharpe Ratio (DSR) estimate to penalize multiple testing.

## Live Dataset Capture

Backend now persists each `strategy:pnl` event to:

- `apps/backend/logs/strategy_trades.csv` (default)

This path is configurable with:

- `STRATEGY_TRADE_LOG_PATH`

Quick status endpoint:

- `GET /api/arb/validation-trades`
- `POST /api/arb/validation-trades/reset` (control-plane auth required)

## Input Data

CSV columns:

1. `timestamp` (unix seconds/ms or ISO datetime)
2. `strategy` (e.g. `CEX_SNIPER`)
3. `variant` (parameter set id, default `baseline`)
4. `pnl` (realized pnl in USD)

Optional:

1. `notional` (position notional used for return normalization)
2. `mode` (`PAPER` or `LIVE`)
3. `reason` (exit/decision rationale)

Example header:

```csv
timestamp,strategy,variant,pnl,notional,mode,reason
2026-02-11T18:00:00Z,CEX_SNIPER,v1,12.50,250.00,PAPER,TAKE_PROFIT
2026-02-11T18:00:02Z,CEX_SNIPER,v2,-4.20,200.00,PAPER,STOP_LOSS
```

## Run

```bash
scripts/run_strategy_validation.sh \
  apps/backend/logs/strategy_trades.csv \
  reports/strategy_validation.json
```

## Interpretation

1. `avg_oos_sharpe` should stay positive and stable across splits.
2. `pbo` near 0 is better; values > 0.5 indicate likely overfitting.
3. `deflated_sharpe_ratio` should be high (commonly > 0.8 for stronger confidence).
4. Use this report as a promotion gate before enabling any live posting latch.
