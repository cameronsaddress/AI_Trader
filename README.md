# AI Trading Arena

Institutional-grade, multi-agent AI cryptocurrency trading platform. Six competing LLMs (Claude, GPT, Gemini, Grok, DeepSeek, Qwen) analyze live market data and execute trades in parallel, while a high-frequency Rust engine runs quantitative strategies on prediction markets with sub-millisecond latency.

## Architecture

```
                         React 18 Dashboard
                    Live | Arbitrage | Models | PromptLab
                              |
                         Socket.IO (real-time)
                              |
          +-------------------+-------------------+
          |                                       |
   Node.js Backend (Express)             Rust HFT Engine (Tokio)
   - 6 LLM agents (parallel)            - 8 quantitative strategies
   - Adaptive risk allocator             - Black-Scholes pricing
   - Trade audit forensics               - Kelly criterion sizing
   - Settlement tracking                 - Circuit breakers
          |                                       |
     +----+----+                          +-------+-------+
     |         |                          |               |
  PostgreSQL  Redis ---- pub/sub ----  Coinbase WS    Polymarket WS
     16     (config, signals,           (6 CEX feeds)   (CLOB order
             execution bus)                              books)
```

## Tech Stack

| Layer | Technologies |
|---|---|
| **Backend** | Node.js 20, Express 4.18, Socket.IO 4.7, Sequelize ORM |
| **HFT Engine** | Rust 2021, Tokio, tungstenite WebSockets, ethers-rs |
| **Frontend** | React 18.2, Recharts, real-time WebSocket |
| **AI/LLM** | Claude, GPT, Gemini, Grok, DeepSeek, Qwen (via OpenRouter, Anthropic, Google, OpenAI APIs) |
| **ML Pipeline** | XGBoost (GPU), TensorFlow/Keras, scikit-learn, Python 3.12 |
| **Trading Venues** | Coinbase Advanced Trade (spot + perps), Polymarket (Polygon PoS), Hyperliquid |
| **Database** | PostgreSQL 16, Redis (pub/sub + config store) |
| **Infrastructure** | Docker Compose (16 containers), NVIDIA CUDA |

## Key Features

### Multi-Agent LLM Trading
- 6+ models trade independently with isolated accounts and bankrolls
- Adaptive prompts inject rolling performance metrics (Sharpe, win rate) to self-correct behavior
- 138-feature ML vector (OHLCV, microstructure, sentiment, chaos theory signals)
- Chaos theory indicators: Lyapunov exponents, Hurst exponents, fractal dimension

### Rust HFT Engine (8 Strategies)

| Strategy | Description |
|---|---|
| **BTC 5m Binary** | Black-Scholes fair value pricing against cross-exchange median spot (6 CEX feeds) |
| **Atomic Arb** | Buys equal YES/NO shares when locked net edge survives costs |
| **OBI Scalper** | Coinbase L2 order book imbalance prediction |
| **Copy Bot** | Decodes Polygon calldata to replicate profitable wallet clusters |
| **CEX Arb** | Spatial arbitrage between Coinbase spot and Polymarket outcomes |
| **Market Neutral** | Directional fair-value pricing for 15m contracts |
| **Graph Arb** | No-arbitrage graph violations across active binary universe |
| **Convergence Carry** | Cross-outcome parity dislocations with bounded hold and risk exits |

### Risk Management
- Portfolio-level circuit breaker (15% drawdown halt)
- Per-strategy adaptive allocator (EMA-based quality scoring, 0.25x-2.0x multiplier range)
- Kelly criterion position sizing with three concentration caps (strategy 35%, family 60%, global 90%)
- VaR (95%/99%), correlation matrix, portfolio Greeks

### ML Pipeline
- XGBoost GPU-accelerated model (90.15% accuracy, 138 features)
- Walk-forward out-of-sample validation with CSCV/PBO and deflated Sharpe diagnostics
- Continuous retraining scheduler

## Project Structure

```
apps/
  arb-scanner/         Rust HFT engine (Tokio + 8 strategies)
  backend/             Node.js Express API + trading engine
  frontend/            React 18 dashboard
  ml-service/          Python ML pipeline (XGBoost, TensorFlow)
packages/
  shared/              Shared TypeScript types
infrastructure/
  docker-compose.yml   16-service orchestration
  postgres/            Database initialization
config/                Configuration templates
docs/                  Strategy documentation and backlogs
scripts/               Training, validation, and deployment scripts
```

## Infrastructure (Docker Compose)

| Service | Technology | Purpose |
|---|---|---|
| backend | Node.js 20 | Express API, LLM orchestration, risk management |
| frontend | React 18.2 | Real-time trading dashboard |
| postgres | PostgreSQL 16 | Trade history, model state |
| redis | Redis 7 | Pub/sub bus, config store, risk broadcasting |
| ml-service | Python 3.12 | XGBoost/TensorFlow inference |
| arb-scanner-btc-5m | Rust | BTC 5-minute binary options strategy |
| arb-scanner-btc | Rust | BTC 15-minute fair value strategy |
| arb-scanner-eth | Rust | ETH 15-minute fair value strategy |
| arb-scanner-sol | Rust | SOL 15-minute fair value strategy |
| arb-scanner-cex | Rust | Cross-exchange arbitrage |
| arb-scanner-copy | Rust | On-chain copy trading |
| arb-scanner-atomic | Rust | Atomic arbitrage |
| arb-scanner-obi | Rust | Order book imbalance scalping |
| arb-scanner-graph | Rust | Graph-based arbitrage |
| arb-scanner-convergence | Rust | Mean reversion carry |
| arb-scanner-maker | Rust | Market making |

## Getting Started

### Prerequisites
- Node.js 20+
- Rust 1.75+
- Docker and Docker Compose
- Coinbase Advanced Trade API credentials

### Quick Start

```bash
# Clone and configure
cp .env.example .env
# Edit .env with your API credentials

# Start all services
./stack.sh start

# Dashboard available at http://localhost:3003
# API available at http://localhost:5002
```

The system starts in paper trading mode by default. All trades execute against real market data with simulated fills.

## Live Canary Prep

Use the conservative baseline profile before enabling any live posting:

```bash
# Review and copy values into your runtime .env
cat config/live_canary_baseline.env
```

Run a paper soak report before canary promotion:

```bash
scripts/run_paper_soak_report.sh
```

Outputs:

1. `reports/paper_soak_report.json`
2. `reports/paper_soak_report.md`
3. `reports/paper_soak_stats.jsonl`

## Security Model

- Three independent safety latches prevent live trading (env flag, wallet key, Redis mode)
- Control plane mutations require authentication token
- Paper mode is the default — real orders require deliberate multi-step activation
- Per-order and per-signal notional ceilings
- Intelligence gate requires fresh confirmation before live order posting
