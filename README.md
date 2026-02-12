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
