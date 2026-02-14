# AI Trading Arena

[![React](https://img.shields.io/badge/React-18.2.0-61DAFB?logo=react)](https://reactjs.org/)
[![Node.js](https://img.shields.io/badge/Node.js-20+-339933?logo=node.js)](https://nodejs.org/)
[![Socket.IO](https://img.shields.io/badge/Socket.IO-4.7.5-010101?logo=socket.io)](https://socket.io/)
[![Coinbase](https://img.shields.io/badge/Coinbase-Advanced_Trade-0052FF?logo=coinbase)](https://developers.coinbase.com/)
[![TensorFlow](https://img.shields.io/badge/TensorFlow-2.x-FF6F00?logo=tensorflow)](https://www.tensorflow.org/)
[![XGBoost](https://img.shields.io/badge/XGBoost-GPU-76B900)](https://xgboost.ai/)
[![Rust](https://img.shields.io/badge/Rust-2021-DEA584?logo=rust)](https://www.rust-lang.org/)
[![Docker](https://img.shields.io/badge/Docker-24.0+-2496ED?logo=docker)](https://www.docker.com/)

An institutional-grade, multi-agent AI cryptocurrency trading platform where Large Language Models compete head-to-head using live Coinbase Advanced Trade market data. Features parallel AI execution, machine learning predictions, perpetual futures trading, high-frequency prediction market arbitrage, and comprehensive risk management.

---

## Going Live — From Paper to Real Money

The system ships with **three safety latches** that prevent real money from being traded.
All three must be deliberately enabled before any real order leaves the system.

### Current State (Paper Mode)

Everything runs against **real market data** but no real orders are placed.
The simulation ledger starts at $1,000 and tracks paper PnL in Redis.
All trades log with `mode: "PAPER"` in the CSV.

### What Needs USD

| System | What To Fund | Why |
|---|---|---|
| **Polygon wallet** (USDC) | Deposit USDC on Polygon PoS to the wallet whose private key you configure below. This is the account Polymarket draws from when filling orders. | Polymarket CLOB settles in USDC on Polygon. |
| **Polygon wallet** (MATIC) | Small amount of MATIC in the same wallet for gas. ~0.5 MATIC is plenty for weeks of trading. | On-chain transactions (order approval, position redemption) require gas. |
| **Coinbase Advanced Trade** (USD) | Fund your Coinbase account if you plan to run the LLM spot/perp trading engine against real markets. | The multi-agent LLM engine places spot and perpetual futures orders on Coinbase. |

> Polymarket is the primary venue for the Rust HFT strategies (BTC 5m, Atomic Arb, etc.).
> Coinbase is used by the LLM trading engine and also provides CEX price feeds to the Rust scanner (read-only — no Coinbase funds needed for HFT strategies).

### Step-by-Step: Enable Live Trading

#### 1. Create and fund a Polygon wallet

Generate a fresh Ethereum-compatible wallet (e.g. via MetaMask or `cast wallet new`).
Transfer USDC and a small amount of MATIC to it on the **Polygon PoS** network.
Approve the Polymarket CLOB contract to spend your USDC (one-time on-chain approval).

#### 2. Set credentials in `infrastructure/.env`

```env
# Polygon wallet private key (with 0x prefix). This signs all Polymarket orders.
PRIVATE_KEY=0xYOUR_POLYGON_WALLET_PRIVATE_KEY

# The public address of the wallet above.
PROXY_WALLET_ADDRESS=0xYOUR_POLYGON_WALLET_ADDRESS

# Flip the master safety latch.
LIVE_ORDER_POSTING_ENABLED=true
```

If you want automatic on-chain redemption when markets resolve, also set:

```env
POLY_AUTO_REDEEM_ENABLED=true
POLY_RPC_URL=https://polygon-rpc.com
POLY_CONDITIONAL_TOKENS_ADDRESS=0x4D97DCd97eC945f40cF65F87097ACe5EA0476045
POLY_COLLATERAL_TOKEN_ADDRESS=0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174
```

#### 3. (Optional) Coinbase credentials for LLM engine

If using the multi-agent LLM trading engine with real Coinbase orders, set in `.env.example` → `.env`:

```env
COINBASE_API_KEY=your_coinbase_api_key
COINBASE_API_SECRET=your_coinbase_api_secret
COINBASE_SANDBOX=false
```

Or if using CDP (Coinbase Developer Platform) JSON key, ensure `apps/backend/.env` has:

```env
CDP_API_KEY_PATH=/app/apps/backend/YOUR_CDP_KEY.json
```

#### 4. Restart the stack

```bash
cd infrastructure
docker compose down && docker compose up -d --build
```

#### 5. Switch trading mode to LIVE

From the frontend UI, toggle the trading mode from **PAPER** to **LIVE**.
Or set it directly in Redis:

```bash
docker exec infrastructure-redis-1 redis-cli SET system:trading_mode LIVE
```

The backend will log: `[TradingMode] Switched to LIVE`.

#### 6. Verify live execution is active

Check backend logs for the preflight service initializing with your key:

```bash
docker logs infrastructure-backend-1 2>&1 | grep -i "preflight\|live.*post\|private.key"
```

You should see the preflight service reporting **enabled** (not "disabled: missing private key").

### Safety Controls Summary

| Latch | Where | Default | What It Does |
|---|---|---|---|
| `LIVE_ORDER_POSTING_ENABLED` | `infrastructure/.env` | `false` | Master kill-switch. When false, orders are signed but never posted — returned as `{ dryRun: true }`. |
| `PRIVATE_KEY` | `infrastructure/.env` | empty | Wallet key for signing Polymarket orders. Preflight service disables itself when missing. |
| `system:trading_mode` | Redis (UI toggle) | `PAPER` | Rust engine and backend skip live execution paths entirely in PAPER mode. |
| `POLY_AUTO_REDEEM_ENABLED` | `infrastructure/.env` | `false` | Controls whether the settlement service submits on-chain redeem transactions when markets resolve. |
| `POLY_MAX_ORDER_NOTIONAL_USD` | `.env.example` | `500` | Per-order notional ceiling (USD). Orders above this are rejected. |
| `POLY_MAX_SIGNAL_NOTIONAL_USD` | `.env.example` | `2000` | Per-signal aggregate ceiling. |
| `INTELLIGENCE_GATE_ENABLED` | `infrastructure/.env` | `true` | Requires fresh intelligence confirmation before posting live orders. |

### What Happens When You Go Live

1. Rust scanner generates signals exactly as it does in paper mode.
2. Backend receives signals on `arbitrage:execution` Redis channel.
3. `PolymarketPreflightService` signs the order with your private key.
4. Because `LIVE_ORDER_POSTING_ENABLED=true`, the signed order is **posted** to Polymarket's CLOB API via `client.postOrder()`.
5. Polymarket fills the order against its order book. USDC is debited from your wallet.
6. When the market resolves, the settlement service detects the outcome and (if `POLY_AUTO_REDEEM_ENABLED=true`) submits an on-chain redeem transaction to collect winnings.

### Reverting to Paper

Set `LIVE_ORDER_POSTING_ENABLED=false` in `infrastructure/.env` and restart the backend, or toggle trading mode back to PAPER from the UI. Either action immediately stops real order posting.

---

## How the BTC 5m Engine Thinks

The BTC 5m strategy (`apps/arb-scanner/src/strategies/btc_5m_lag.rs`) trades binary options on Polymarket that resolve every 5 minutes: "Will BTC be higher or lower than its starting price at the end of this window?" The engine uses Black-Scholes fair value pricing against a cross-exchange median spot feed to find mispriced contracts, then sizes positions via Kelly criterion with multi-layer risk caps.

### The Decision Loop (every 90ms)

```
    CEX Feeds (6 exchanges)                Polymarket WS
    Coinbase · Binance · OKX          YES/NO order book
    Bybit · Kraken · Bitfinex          for current window
            │                                  │
            ▼                                  ▼
    ┌──────────────────┐              ┌─────────────────┐
    │  Cross-Exchange   │              │  Book Parser     │
    │  Median (cex_mid) │              │  best bid/ask    │
    └────────┬─────────┘              └────────┬────────┘
             │                                  │
             ▼                                  ▼
    ┌────────────────────────────────────────────────────┐
    │              FAIR VALUE (Black-Scholes)             │
    │  fair_yes = e^(-r·t) · N(d2)                       │
    │  d2 = [ln(cex_mid/strike) + (r - σ²/2)·t] / σ√t  │
    │  strike = BTC spot at window open                   │
    │  σ = annualized vol from 5-min CEX history          │
    └───────────────────────┬────────────────────────────┘
                            │
                            ▼
    ┌────────────────────────────────────────────────────┐
    │              EDGE CALCULATION                       │
    │  expected_roi = (fair_prob / market_price) - 1      │
    │  net_expected  = expected_roi - cost_per_side        │
    │  Pick best side: max(net_UP, net_DOWN)              │
    └───────────────────────┬────────────────────────────┘
                            │
                            ▼
    ┌────────────────────────────────────────────────────┐
    │              ENTRY GATE (all must pass)             │
    │  ✓ Parity OK:  YES_mid + NO_mid within 2c of $1   │
    │  ✓ Price band:  entry between 15c and 85c          │
    │  ✓ Spread OK:   bid-ask spread ≤ 8c               │
    │  ✓ Edge:        net_expected ≥ 40 bps (0.40%)      │
    │  ✓ Time:        > 12 seconds to window expiry      │
    │  ✓ No open position for this window                │
    └───────────────────────┬────────────────────────────┘
                            │
                            ▼
    ┌────────────────────────────────────────────────────┐
    │              POSITION SIZING (Kelly)                │
    │  kelly_f = (fair_prob - price) / (1 - price)       │
    │  base_bet = bankroll × risk_pct (1.7%)             │
    │  sized = base_bet × risk_multiplier                │
    │  Capped by: strategy 35%, family 60%, global 90%   │
    │  Minimum bet: $10                                   │
    └───────────────────────┬────────────────────────────┘
                            │
                            ▼
                     ENTER POSITION
                (reserve notional in sim ledger)
```

### Step 1: Market Data Collection

Six CEX WebSocket feeds run in parallel Tokio tasks, each maintaining a latest price with millisecond timestamp:

| Exchange | Feed | Staleness Threshold |
|---|---|---|
| Coinbase | Advanced Trade ticker WS | 2.5 seconds |
| Binance | US book ticker WS | 2.5 seconds |
| OKX | Public tickers WS (BTC-USDT) | 2.5 seconds |
| Bybit | Spot tickers WS (BTCUSDT) | 2.5 seconds |
| Kraken | Public ticker WS (XBT/USD) | 2.5 seconds |
| Bitfinex | Public WS (tBTCUSD) | 2.5 seconds |

The **cross-exchange median** (`cex_mid`) is computed from all feeds with fresh timestamps (within 2.5s). This is more robust than any single exchange — it rejects outliers and reduces manipulation risk.

Simultaneously, a Polymarket WebSocket subscribes to the YES/NO order book for the current 5-minute window's binary contract.

### Step 2: Fair Value Pricing (Black-Scholes Digital Option)

The fair probability that BTC will be above its window-opening price at expiry:

```
fair_yes = e^(-r·t) × N(d2)

where:
  d2    = [ln(cex_mid / window_start_spot) + (r - σ²/2) × t] / (σ × √t)
  r     = 0.045 (risk-free rate, negligible over 5 min)
  t     = seconds remaining / 31,536,000 (time to expiry in years)
  σ     = annualized volatility estimated from 5-min rolling CEX price history
  N(·)  = standard normal CDF
```

**Volatility estimation**: Log returns are computed from the spot price history buffer (last 5 minutes, minimum 12 samples). Sample standard deviation is annualized by multiplying by `√(steps_per_year)`. Clamped to `[0.10, 2.00]`.

**Why this works**: Polymarket prices for 5-minute BTC up/down contracts are set by human traders and market makers. The Black-Scholes model computes what these contracts *should* be worth given the current spot price, time to expiry, and realized volatility. When Polymarket prices deviate from this fair value, the engine trades the mispricing.

### Step 3: Edge Calculation

```
expected_roi(side) = (fair_prob / market_ask_price) - 1

Example: fair_yes = 0.63, market YES ask = 0.50
  expected_roi = (0.63 / 0.50) - 1 = +26%

net_expected = expected_roi - per_side_cost
  per_side_cost = (10 bps fee + 8 bps slippage) / 10,000 = 0.18%
  net_expected = 26% - 0.18% = 25.82%
```

The engine computes this for both UP (YES) and DOWN (NO) sides and picks whichever has the higher net expected return.

### Step 4: Entry Gate

All conditions must pass simultaneously:

| Check | Threshold | Why |
|---|---|---|
| **Parity** | YES_mid + NO_mid within 2c of $1.00 | Detects broken/illiquid books |
| **Price band** | Entry price between $0.15 and $0.85 | Avoids extreme prices that amplify cost drag or produce near-certain outcomes |
| **Spread** | Bid-ask spread ≤ 8 cents | Rejects illiquid markets where slippage eats edge |
| **Net edge** | Net expected return ≥ 40 bps (0.40%) | Only trades when edge exceeds modeled costs |
| **Time** | > 12 seconds to window expiry | Prevents entering with insufficient time for the book to update before resolution |
| **No open position** | One position per window max | Prevents doubling down |
| **Strategy enabled** | Redis flag `strategy:enabled:BTC_5M` = 1 | Allows remote kill switch |
| **Book freshness** | YES and NO book data < 1.5s old | Ensures pricing is current |
| **Spot freshness** | CEX spot price < 2.5s old | Won't trade on stale fair value |

### Step 5: Position Sizing

```
kelly_fraction = (fair_prob - market_price) / (1 - market_price)
base_bet       = bankroll × 1.7%           (from Redis risk config)
risk_adjusted  = base_bet × risk_multiplier (from adaptive allocator)
```

Then capped through three concentration layers:

| Cap | Limit | Purpose |
|---|---|---|
| **Strategy cap** | 35% of equity | No single strategy dominates |
| **Family cap** | 60% of equity | FAIR_VALUE family (BTC_5M + BTC_15M + ETH_15M + SOL_15M) |
| **Global cap** | 90% of equity | Always keep 10% as reserve |
| **Minimum bet** | $10 | Below this, execution costs dominate |

If the final size is below $10 after all caps, the trade is skipped.

### Step 6: Settlement

When the 5-minute window expires (`now >= expiry_ts`):

1. Look up the BTC spot price at the boundary (from price history buffer, within ±5 seconds of expiry).
2. If no spot available, fall back to cross-exchange median. If still unavailable, wait up to 12 seconds.
3. Determine outcome: **UP wins** if `end_spot > window_start_spot`, **DOWN wins** otherwise.
4. Compute return:
   - **Win**: `gross_return = (1 / entry_price) - 1` (e.g., bought at $0.50 → +100%)
   - **Loss**: `gross_return = -1.0` (total loss of premium, the contract expires worthless)
   - `net_return = gross_return - per_side_cost`
   - `pnl = notional × net_return`
5. Update the sim ledger (release reservation, apply PnL to cash balance).
6. Publish `strategy:pnl` event to Redis → backend records to CSV and feeds the adaptive allocator.

### Cost Model

```
Simulated costs per side (entry OR exit):
  Fee:       10 bps  (Polymarket taker fee)
  Slippage:   8 bps  (price impact estimate)
  Total:     18 bps per side (0.18%)

For binary options, cost is applied once at entry
(exit is automatic at resolution — no execution needed).
```

### Adaptive Risk Allocator (Backend Circuit Breaker)

The backend Node.js process (`apps/backend/src/index.ts`) maintains a per-strategy performance tracker that adjusts each strategy's `risk_multiplier` between 0.25 and 1.65:

**EMA Tracker** (updated on every settled trade):
```
alpha = 0.08
ema_return  = (1 - alpha) × ema_return  + alpha × realized_return
downside_ema = (1 - alpha) × downside_ema + alpha × max(0, -realized_return)
return_var   = EMA of squared deviations from ema_return
```

**Multiplier Calculation**:
```
implied_sharpe = ema_return / sqrt(return_var)
quality_score  = (implied_sharpe / 0.55) + (win_rate - 0.5) × 1.4

If < 30 samples (bootstrap phase):
  multiplier = clamp(1 + quality_score × confidence × 0.20, 0.80, 1.20)
  confidence = min(1, sample_count / 40)

If ≥ 30 samples (full evaluation):
  multiplier = clamp(1 + quality_score × confidence × 0.45 - downside_penalty, 0.25, 1.65)
```

**Circuit Breakers** (throttle, don't kill):
- If ≥ 30 samples AND ema_return ≤ -0.25 → multiplier = 0.25 (floor)
- If ≥ 40 samples AND win_rate ≤ 35% AND ema_return ≤ -0.15 → multiplier = 0.25

These thresholds are calibrated for binary option returns (±100% per trade). The EMA with alpha=0.08 needs ~30 samples to converge, so the circuit breaker waits for statistical significance before judging.

**Why 0.25 floor, not 0**: A zeroed multiplier permanently kills a strategy because it stops producing new trade data, so the allocator can never observe recovery. The 0.25 floor keeps the strategy alive at reduced size, letting it prove itself if conditions change.

### Complete Data Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│  Rust Scanner (arb-scanner-btc-5m container)                        │
│                                                                      │
│  6 CEX WS feeds → spot_history buffer → annualized vol              │
│  6 CEX WS feeds → median(fresh prices) → cex_mid                   │
│  Polymarket WS  → YES/NO best bid/ask                               │
│                                                                      │
│  Every 90ms tick:                                                    │
│    fair_yes = Black-Scholes(cex_mid, strike, σ, t)                  │
│    edge = (fair_prob / market_ask) - 1 - cost                       │
│    if edge > 40bps AND all gates pass → ENTER at market_ask         │
│    Publish scan telemetry → Redis "arbitrage:scan"                  │
│    Publish heartbeat → Redis "system:heartbeat"                     │
│    On window expiry → settle → Redis "strategy:pnl"                 │
└──────────────────────┬──────────────────────────────────────────────┘
                       │ Redis Pub/Sub
                       ▼
┌──────────────────────────────────────────────────────────────────────┐
│  Node.js Backend (backend container)                                 │
│                                                                      │
│  Subscribes to:                                                      │
│    "arbitrage:scan"      → emit Socket.IO "scanner_update"          │
│    "arbitrage:execution" → emit Socket.IO "execution_log"           │
│    "strategy:pnl"        → record to CSV, update adaptive allocator │
│    "system:heartbeat"    → track scanner liveness                    │
│                                                                      │
│  Adaptive allocator: ema_return → quality_score → risk_multiplier   │
│  Multiplier published to Redis for scanner to read on next trade    │
└──────────────────────┬──────────────────────────────────────────────┘
                       │ Socket.IO
                       ▼
┌──────────────────────────────────────────────────────────────────────┐
│  React Frontend (Btc5mEnginePage.tsx)                                │
│                                                                      │
│  Listens to:                                                         │
│    "scanner_update"  → populates EXECUTION PIPELINE panel            │
│    "execution_log"   → populates ORDER FEED + POSITIONS rail         │
│    "strategy_pnl"    → updates CUMULATIVE PNL, TODAY PNL, win rate   │
│                                                                      │
│  On mount: fetches /api/arb/strategy-trades?strategy=BTC_5M         │
│  to backfill trade blotter from CSV history                          │
└──────────────────────────────────────────────────────────────────────┘
```

### Key Constants

| Constant | Value | Location |
|---|---|---|
| `WINDOW_SECONDS` | 300 (5 min) | btc_5m_lag.rs |
| `RISK_FREE_RATE` | 0.045 | btc_5m_lag.rs |
| `MIN_ENTRY_PRICE` | 0.15 | btc_5m_lag.rs |
| `MAX_ENTRY_PRICE` | 0.85 | btc_5m_lag.rs |
| `MAX_PARITY_DEVIATION` | 0.02 | btc_5m_lag.rs |
| `DEFAULT_MIN_EXPECTED_NET_RETURN` | 0.004 (40 bps) | btc_5m_lag.rs |
| `DEFAULT_MAX_ENTRY_SPREAD` | 0.08 (8c) | btc_5m_lag.rs |
| `DEFAULT_MAX_POSITION_FRACTION` | 0.35 | btc_5m_lag.rs |
| `SPOT_STALE_MS` | 2,500 | btc_5m_lag.rs |
| `BOOK_STALE_MS` | 1,500 | btc_5m_lag.rs |
| `ENTRY_EXPIRY_CUTOFF_SECS` | 12 | btc_5m_lag.rs |
| `VOL_WINDOW_MS` | 300,000 (5 min) | btc_5m_lag.rs |
| `VOL_MIN_SAMPLES` | 12 | btc_5m_lag.rs |
| `fee_bps_per_side` | 0 (tuned from 10) | simulation.rs / env |
| `slippage_bps_per_side` | 2 (tuned from 8) | simulation.rs / env |
| `STRATEGY_WEIGHT_FLOOR` | 0.25 | index.ts |
| `STRATEGY_WEIGHT_CAP` | 2.0 (tuned from 1.65) | index.ts / env |
| `STRATEGY_ALLOCATOR_MIN_SAMPLES` | 30 | index.ts |
| `SIM_STRATEGY_CONCENTRATION_CAP_PCT` | 0.35 | control.rs |
| `SIM_FAMILY_CONCENTRATION_CAP_PCT` | 0.60 | control.rs |
| `SIM_GLOBAL_UTILIZATION_CAP_PCT` | 0.90 | control.rs |

### Profit Optimization Tuning

Seven configurable levers that improve expected PnL. All changes are safe to reverse.

| # | Lever | Tuned Value | Previous Default | Config Location |
|---|-------|-------------|-----------------|-----------------|
| 1 | **Cost Model** | 0 fee + 2 bps slip = 2 bps/side | 10 fee + 8 slip = 18 bps/side | `SIM_FEE_BPS_PER_SIDE`, `SIM_SLIPPAGE_BPS_PER_SIDE` env on scanner |
| 2 | **Position Size** | 3.5% of bankroll | 1.7% of bankroll | Redis `system:risk_config` or Engine Tuning UI |
| 3 | **Disable ETH_15M** | Disabled (43.8% WR, -$20 PnL) | Enabled | `DEFAULT_DISABLED_STRATEGIES` env on backend |
| 4 | **EMA Alpha** | 0.05 (smoother) | 0.08 (reactive) | `STRATEGY_ALLOCATOR_EMA_ALPHA` env on backend |
| 5 | **Edge Threshold** | 30 bps min net return | 40 bps | `BTC_5M_MIN_EXPECTED_NET_RETURN` env on scanner |
| 6 | **Multiplier Cap** | 2.0x | 1.65x | `STRATEGY_WEIGHT_CAP` env on backend |
| 7 | **Rust Clamp** | 3.0 ceiling | 2.0 ceiling | `control.rs:read_strategy_risk_multiplier()` |

#### How to Adjust

**Scanner env vars** (levers 1, 5): Edit `infrastructure/docker-compose.yml` scanner services, then:
```bash
cd infrastructure && docker compose down && docker compose up -d --build
```

**Backend env vars** (levers 3, 4, 6): Edit `infrastructure/docker-compose.yml` backend service, then restart.

**Risk config** (lever 2): Use the **Engine Tuning** panel in the BTC 5m page UI, or:
```bash
docker exec infrastructure-redis-1 redis-cli SET system:risk_config '{"model":"PERCENT","value":3.5}'
```

**Strategy toggles** (lever 3 runtime): Use Engine Tuning panel toggle buttons, or:
```bash
docker exec infrastructure-redis-1 redis-cli SET strategy:enabled:ETH_15M 0
```

#### Rationale

- **Cost model fix**: Polymarket charges 0 fees for limit orders. The 18 bps default was calibrated for CEX spot trading and artificially rejected profitable trades.
- **Position size increase**: At 58.2% win rate with near-symmetric payoffs, Kelly optimal is ~16%. Moving from 1.7% to 3.5% is still conservative (1/5th Kelly).
- **ETH_15M disable**: 226 trades at 43.8% WR and negative PnL. Consumes FAIR_VALUE family concentration cap shared with profitable BTC strategies.
- **EMA smoothing**: Binary option returns (win=+100%, loss=-100%) cause violent EMA swings at alpha=0.08. Lower alpha stabilizes the risk multiplier.
- **Edge threshold reduction**: With corrected costs (2 bps vs 18 bps), the effective net edge is higher, so 30 bps is still conservative.
- **Multiplier cap raise**: BTC_5M's 58.2% WR over 98 trades earns a higher allocation. Cap of 2.0 means up to 2x base bet for top performers.

---

## Table of Contents

- [System Overview](#system-overview)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Directory Structure](#directory-structure)
- [Core Systems](#core-systems)
  - [Trading Engine](#1-trading-engine)
  - [AI Models & LLM Integration](#2-ai-models--llm-integration)
  - [Risk Management](#3-risk-management)
  - [Machine Learning Pipeline](#4-machine-learning-pipeline)
  - [Market Data Infrastructure](#5-market-data-infrastructure)
  - [Simulation & Backtesting](#6-simulation--backtesting)
  - [Rust HFT Engine](#7-rust-hft-engine-arb-scanner) **(NEW)**
  - [Capital Allocation](#8-capital-allocation)
  - [Trade Execution Audit](#9-trade-execution-audit)
  - [Sentiment Engine](#10-sentiment-engine)
  - [ProFiT Module](#11-profit-module)
- [Trading Modes](#trading-modes)
- [LLM Prompt Architecture](#llm-prompt-architecture)
- [Frontend Dashboard](#frontend-dashboard)
- [Real-Time Communication](#real-time-communication)
- [API Reference](#api-reference)
- [Infrastructure](#infrastructure)
- [Security Model](#security-model)
- [Database Schema](#database-schema)
- [Testing](#testing)
- [Configuration](#configuration)
- [Startup & Initialization](#startup--initialization)
- [Scripts & Utilities](#scripts--utilities)
- [Critical Design Decisions](#critical-design-decisions)

---

## System Overview

The AI Trading Arena orchestrates 6+ competing AI models (Claude, GPT, Gemini, Grok, DeepSeek, Qwen) that independently analyze live market data and execute trades on Coinbase. Simultaneously, a high-performance **Rust HFT Engine** scans for arbitrage and microstructure opportunities on Polymarket and Coinbase L2 data.

### Key Capabilities

| Capability | Implementation |
|---|---|
| **Multi-Agent Trading** | 6+ LLMs trade in parallel via `Promise.allSettled`, each with isolated accounts |
| **Dual Market Support** | Spot (BTC-USD, ETH-USD) + Perpetual futures (CFM nano-contracts) |
| **Live & Simulation** | Toggle between real Coinbase orders and paper trading with slippage simulation |
| **ML-Enhanced Decisions** | XGBoost (90.15% accuracy) with 138 engineered features, GPU-accelerated |
| **Chaos Theory Signals** | Lyapunov exponents, Hurst exponents, fractal dimension across 3 timeframes |
| **HFT Strategies** | **Rust-based** Atomic Arb, OBI Scalping using Tokio runtime (sub-ms latency) |
| **Dynamic Risk Engine** | Real-time broadcasting of risk parameters (Start/Stop, Risk %, Max Drawdown) via Redis |
| **Circuit Breaker** | Portfolio-level (15% drawdown) and per-model (10%) halt with disk persistence |
| **Risk Analytics** | VaR (95%/99%), correlation matrix, portfolio Greeks, risk attribution |
| **Trade Audit** | Decision forensics with latency percentiles, slippage tracking, Sharpe analytics |
| **Adaptive Learning** | Performance-injected prompts with rolling Sharpe targets and behavioral adjustments |
| **ProFiT Optimization** | Evolutionary strategy search with Python backtesting and AST-validated code execution |

### Technology Stack

| Layer | Technologies |
|---|---|
| **Backend** | Node.js 20+, Express 4.18, Socket.IO 4.7, Sequelize ORM |
| **HFT Engine** | **Rust 2021**, Tokio, Tungstenite (WS), Ethers.rs, Redis Pub/Sub |
| **Frontend** | React 18.2, CSS3, real-time WebSocket, Recharts |
| **AI/LLM** | Anthropic Claude, OpenAI GPT, Google Gemini, xAI Grok, DeepSeek, Qwen, vLLM (local) |
| **Trading Venues** | Coinbase Advanced Trade (spot + perp), Hyperliquid, Polymarket (Polygon PoS) |
| **Database** | PostgreSQL 16 (primary), SQLite (local cache fallback), Redis (Signals/Config) |
| **ML** | TensorFlow/Keras, XGBoost (GPU), scikit-learn, Python 3.12, backtesting.py |
| **Infrastructure** | Docker Compose, vLLM (optional GPU inference), NVIDIA CUDA |
| **Languages** | JavaScript (ES2020+), Python 3.12, Rust 2021 |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        FRONTEND (React 18)                      │
│  LiveDashboard │ Arbitrage │ Models │ Positions │ PromptLab     │
│  WebSocket ←→ Socket.IO ←→ Backend                              │
└─────────────────────────────┬───────────────────────────────────┘
                              │
┌─────────────────────────────┴───────────────────────────────────┐
│                     BACKEND (Express + Socket.IO)                │
│                                                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐   │
│  │ Trading      │  │ Controllers  │  │ Middleware            │   │
│  │ Engine       │  │ ─ trading    │  │ ─ auth (JWT/Token)    │   │
│  │ (8500+ LOC)  │  │ ─ models     │  │ ─ rate limiting       │   │
│  │ Parallel LLM │  │ ─ sentiment  │  │ ─ helmet (CSP)        │   │
│  │ Execution    │  │ ─ promptLab  │  └──────────────────────┘   │
│  └──────┬───────┘  └──────────────┘                             │
│         │                                                        │
│  ┌──────┴─────────────────────────────────────────────────────┐  │
│  │                     SERVICES LAYER                          │  │
│  │  ┌─────────────┐ ┌──────────────┐ ┌──────────────────┐     │  │
│  │  │ Dynamic     │ │ Risk         │ │ Trade Audit      │     │  │
│  │  │ Risk Engine │ │ Analytics    │ │ (forensics)      │     │  │
│  │  │ ─Redis Sync │ │ ─VaR 95/99   │ │ ─ring buffer     │     │  │
│  │  └─────────────┘ └──────────────┘ └──────────────────┘     │  │
│  └────────────────────────────────────────────────────────────┘  │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐   │
│  │                 RUST HFT ENGINE (`arb-scanner`)            │   │
│  │  Strategies: Atomic Arb, OBI Scalper, Copy Bot, CEX Arb    │   │
│  │  Tech: Tokio, Tungstenite, Ethers-rs, Redis Pub/Sub        │   │
│  │  Direct Feeds: Coinbase L2, Polymarket Gamma/CLOB/WS       │   │
│  └────────────────────────────────────────────────────────────┘   │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐   │
│  │                  ML PIPELINE (Python)                        │   │
│  │  XGBoost (GPU) │ TensorFlow/Keras │ DeepLOB │ Hybrid CNN    │   │
│  └────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────┴───────────────────────────────────┐
│                    DATA SOURCES                                  │
│  Coinbase WS (prices, books, trades) │ CoinGecko (sentiment)    │
│  Yahoo Finance (ETF flows) │ RSS feeds │ Reddit │ Twitter        │
│  Polymarket CLOB WS │ Kalshi API │ PredictIt API                 │
│  Hyperliquid WS (perp futures)                                   │
└──────────────────────────────────────────────────────────────────┘
```

---

## Quick Start

### Prerequisites
- Node.js 20+
- Rust 1.75+ (for HFT engine)
- Docker & Docker Compose (recommended)
- Coinbase Advanced Trade API credentials (CDP key)

### 1. Start with Docker (Recommended)

```bash
./stack.sh start
```

This starts 4 services:
- `backend` — Express API on port 5002
- `frontend` — React dashboard on port 3003
- `postgres` — PostgreSQL 16 on port 5436
- `arb-scanner` — Rust HFT Engine (variable strategies)

---

## Core Systems

### 1. Trading Engine
*See `agents/tradingEngine.js`.*
Orchestrates the LLM-based discretionary trading. Cycles every 3-minutes, gathering 138 ML features, aggregating sentiment, and prompting 6 parallel models. Includes strict risk gates (Quality Gate, Portfolio Guard).

### 2. AI Models & LLM Integration
Supports 6+ models via OpenRouter, Anthropic, Google, and OpenAI. Features adaptive prompts that inject rolling performance metrics (Sharpe, Win Rate) to self-correct behavior.

### 3. Risk Management
**Dynamic Risk Engine:**
- **Configurable Risk Model:** Fixed ($) or Percent (%) of bankroll.
- **Broadcast System:** Backend sends `update_risk_config` to Redis.
- **Consumer:** Rust bots and Node engines listen on `system:risk_config` channel updates in real-time.

**Layers:**
1.  **Circuit Breaker:** Halts trading on 15% drawdown.
2.  **Portfolio Guard:** Caps gross exposure at 85%.
3.  **Dynamic Sizing:** Adjusts bet size based on confidence and volatility (Kelly criterion).

### 4. Machine Learning Pipeline
**XGBoost & Deep Learning:**
- 138-feature vector (OHLCV, Microstructure, Sentiment, Chaos).
- Latency-optimized inference via Python bridge.
- Continuous retraining scheduler.

### 5. Market Data Infrastructure
- **Coinbase WS:** Dedicated connections for Ticker, L2, and User stream.
- **Data Orchestrator:** Central hub for distributing normalized market data.
- **Caches:** Redis-backed caching for high-speed access.

### 6. Simulation & Backtesting
- **Simulation Ledger:** Full paper-trading engine with realistic slippage (1-3 bps).
- **Backtester:** Walk-forward framework for strategy validation.
- **Research Validator:** `scripts/strategy_validation.py` computes walk-forward OOS metrics, CSCV/PBO, and deflated Sharpe diagnostics before live promotion.
- **Paper/Live Checklist:** `scripts/run_live_paper_checklist.sh` runs a control-plane validation pass for mode toggle safety, intelligence blocks, atomic settlement simulation, and syndicate timed exits.

### 7. Rust HFT Engine (`arb-scanner`)
**Location:** `apps/arb-scanner/`
**Architecture:**
- **Runtime:** Tokio Async Runtime.
- **Communication:** Redis Pub/Sub (`arbitrage:execution`, `system:heartbeat`).
- **WebSockets:** `tungstenite` for low-latency feeds (Polymarket, Coinbase).
- **Blockchain:** `ethers-rs` for Polygon PoS interaction.

**Strategies:**

| Strategy | Name | Logic | Status |
|---|---|---|---|
| **Atomic Arb** | "The Accumulator" | Buys equal YES/NO shares only when locked net edge remains after modeled costs and safety buffer. | ✅ Active |
| **OBI Scalper** | "The Tsunami" | Coinbase L2 Order Book Imbalance (OBI) prediction. | ✅ Active |
| **Copy Bot** | "The Syndicate" | Decodes Polygon calldata to mimic profitable wallet clusters. | ✅ Active |
| **CEX Arb** | "The Bridge" | Spatial arbitrage between Coinbase (Spot) and Polymarket (Outcome). | ✅ Active |
| **Market Neutral** | "The Hedge" | Directional fair-value pricing for 15m up/down contracts with parity/spread/cooldown/risk-budget filters. | ✅ Active |
| **Graph Arb** | "The Constraint Graph" | Scans active binary universe for no-arbitrage graph violations and allocates to best net edge. | ✅ Active |
| **Convergence Carry** | "The Mean Reverter" | Trades cross-outcome parity dislocations into convergence with bounded hold and risk exits. | ✅ Active |
| **Maker MM** | "The Quoter" | Captures spread where maker expectancy survives adverse-selection and fee model constraints. | ✅ Active |

**Execution Safety Note:** Rust scanner processes signals/paper logic; Polymarket live posting, intelligence gating, and settlement tracking/redeem flow are centralized in backend SDK controls.

**Run Command:**
```bash
# Run specific strategy in Docker
docker exec -e STRATEGY_TYPE=OBI_SCALPER -w /usr/src/app infrastructure-arb-scanner-copy-1 cargo run --bin arb-scanner
```

---

## Frontend Dashboard

### New Widgets & Panels
- **Strategy Control Widget:** Real-time toggle for Risk Model (Fixed/%) and Risk Value.
- **Arbitrage Matrix:** Visual grid of active arbitrage opportunities across strategies.
- **Risk Analytics:** Live view of VaR, Portfolio Beta, and Sharpe Ratio.

### Core Pages
- **Live Dashboard:** Main command center.
- **Arbitrage:** HFT bot monitoring.
- **Models:** LLM performance leaderboard.
- **Prompt Lab:** Engineering and testing LLM prompts.

---

## API Reference

### HFT / Arbitrage
| Method | Path | Description |
|---|---|---|
| `GET` | `/api/arb/bots` | List status of all Rust bots |
| `POST` | `/api/arb/risk` | Update global risk configuration |
| `GET` | `/api/arb/stats` | Aggregate performance stats |
| `GET` | `/api/system/trading-mode` | Current PAPER/LIVE mode and live-post latch state |
| `POST` | `/api/system/trading-mode` | Set PAPER/LIVE mode (`confirmation=LIVE` required for LIVE) |
| `POST` | `/api/system/reset-simulation` | Reset paper bankroll/PnL state (`confirmation=RESET`) |
| `GET` | `/api/arb/intelligence` | Latest per-strategy scan intelligence + live gate settings |
| `GET` | `/api/arb/settlements` | Atomic settlement tracker snapshot (positions + redeem state) |
| `POST` | `/api/arb/settlements/process` | Force a settlement/redeem processing cycle (control-plane auth) |
| `POST` | `/api/arb/settlements/reset` | Clear tracked settlement positions (control-plane auth) |
| `POST` | `/api/arb/settlements/simulate-atomic` | Register synthetic atomic pair for PAPER validation (control-plane auth) |
| `GET` | `/api/arb/validation-trades` | Validation dataset path + row count |
| `POST` | `/api/arb/validation-trades/reset` | Truncate validation dataset (control-plane auth) |

*(See full API documentation in `docs/`)*

---

## Critical Design Decisions

### Rust for HFT
Node.js is excellent for orchestration and LLM I/O, but Rust was chosen for the HFT engine to minimize garbage collection pauses and ensure sub-millisecond execution latency during fast market moves.

### Redis as Central Nervous System
Redis is not just a cache but the primary communication bus.
- **Config:** `system:risk_config` broadcasts parameter changes.
- **Signals:** `alpha:obi` broadcasts high-frequency signals.
- **Execution:** `arbitrage:execution` aggregates fills from all engines.

### Dynamic Risk
Hardcoded position sizes are dangerous. We moved to a dynamic model where risk is a function of:
1.  **Bankroll:** `%` of current equity.
2.  **Confidence:** Model/Strategy certainty.
3.  **Volatility:** Market regime (High Vol = Lower Size).

---

**© 2026 AI Trading Arena**
