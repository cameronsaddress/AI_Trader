# Phase 10: Future Features (Post-Launch)

Tracked from V2.1 audit remediation plan (Gap 20).

## Features

- [ ] **DEPTH_IMPACT_ARB strategy** — New strategy exploiting order book depth imbalances across venues
- [ ] **SYNDICATE wallet reputation scoring** — Score wallets by historical copy-trade accuracy to weight signals
- [ ] **Portfolio risk overlay engine** — Cross-strategy VaR (Value at Risk) computation for correlated exposure monitoring
- [ ] **Dynamic execution routing** — Per-trade maker vs taker decision based on urgency, spread, and queue position

## Backend Modularization (continued from Phase 7)

The foundational extraction (types + constants) is complete. Remaining handler extractions
require integration testing between each step:

- [ ] Extract Socket.IO handlers → `handlers/socket-handlers.ts`
- [ ] Extract HTTP routes → `handlers/http-routes.ts`
- [ ] Extract Redis subscribers → `handlers/redis-subscribers.ts`
- [ ] `index.ts` becomes thin composition root (~2000 lines target)
