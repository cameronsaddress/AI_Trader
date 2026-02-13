# BTC 5m Execution Loop (Coinbase Lag vs Polymarket Odds)

This document describes the BTC 5-minute window strategy and its UI page.

## What It Is
- Strategy ID: `BTC_5M`
- Heartbeat ID: `btc_5m`
- UI page route: `/btc-5m-engine`
- Data feeds (existing only):
  - Coinbase Advanced Trade WS ticker (`BTC-USD`)
  - Polymarket Gamma events lookup by slug
  - Polymarket CLOB market WS book updates for the window's YES/NO tokens

## Market Selection
- Window size: 5 minutes (`300s`)
- Gamma slug pattern: `btc-updown-5m-{window_start_epoch_sec}`
- The scanner uses current UTC time to bucket into the active 5-minute window and requests that slug.

## Signal / Decision Model (Paper + Live Dry-Run)
For each tick:
1. Read Coinbase spot (mid).
2. Estimate the window-start spot (from local history) to avoid look-ahead.
3. Estimate annualized volatility from recent spot history (rolling log-returns).
4. Compute a "fair" probability the window ends UP (digital/GBM style).
5. Compare fair probability to Polymarket best asks:
   - `EV_UP = fair_up / ask_up - 1`
   - `EV_DOWN = fair_down / ask_down - 1`
6. Subtract modeled entry cost (paper cost model) from EV and pick the best side.

Admission filters:
- Entry price band: `[0.15, 0.85]`
- Parity sanity: `|YES_mid + NO_mid - 1| <= 0.02`
- Max entry spread (per-side ask-bid) from env or default `0.08`
- Must have > `ENTRY_EXPIRY_CUTOFF_SECS` remaining in the window (default `12s`)
- Must clear expected net return threshold (default `60bps`)

Scan semantics (important for UI + intelligence consensus):
- `score` is *direction-signed magnitude* of expected net return:
  - `UP` = `+abs(net_ev)`
  - `DOWN` = `-abs(net_ev)`

## Execution Lifecycle
### PAPER mode
- Enters at most 1 position per window.
- Reserves notional in the shared sim ledger (`reserve_sim_notional_for_strategy`).
- Holds to resolution; on expiry, settles PnL using Coinbase spot vs window-start spot:
  - Win payout return: `1/entry_price - 1`
  - Loss return: `-1`
  - Net return = gross return - entry-side cost rate
- Emits:
  - `arbitrage:execution` events (`ENTRY`, then `WIN`/`LOSS`)
  - `strategy:pnl` event with details for labeling, allocator, and UI

### LIVE mode (safe)
- Does not post real orders by itself.
- Emits `arbitrage:execution` payloads with:
  - `mode: "LIVE_DRY_RUN"`
  - `details.preflight.orders[]` describing the candidate order(s)
- Backend will only post real orders if:
  - global mode is `LIVE`, and
  - `LIVE_ORDER_POSTING_ENABLED=true`

## Environment Knobs
Strategy-specific:
- `BTC_5M_MIN_EXPECTED_NET_RETURN` (decimal, default `0.006` = 60bps)
- `BTC_5M_MAX_ENTRY_SPREAD` (decimal price, default `0.08` = 8c)
- `BTC_5M_MAX_POSITION_FRACTION` (0..1, default `0.25`)

Shared sim cost model:
- `SIM_FEE_BPS_PER_SIDE`
- `SIM_SLIPPAGE_BPS_PER_SIDE`
- `SIM_MAKER_REBATE_BPS_PER_SIDE`
- `SIM_MAKER_ADVERSE_BPS_PER_SIDE`

## Run / Deployment
Docker compose worker:
- Service: `arb-scanner-btc-5m` in `infrastructure/docker-compose.yml`

Start just the worker:
```bash
docker compose -f infrastructure/docker-compose.yml up -d arb-scanner-btc-5m
```

## Known Limitations (Current)
- "Across exchanges" is not implemented because only Coinbase is currently wired as a CEX feed.
- Entry/exit assumes taker fills at best ask/bid; no partial-fill modeling.
- Probability model is simplified; it is a baseline intended to be tuned/validated with replay + walk-forward.
