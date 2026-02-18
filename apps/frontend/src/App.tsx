import React, { Suspense } from 'react';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { HomeDashboard } from './pages/HomeDashboard';
import { SocketProvider } from './context/SocketContext';

// Lazy-load heavy routes for code-splitting
const Dashboard = React.lazy(() => import('./pages/Dashboard').then(m => ({ default: m.Dashboard })));
const PolymarketPage = React.lazy(() => import('./pages/PolymarketPage').then(m => ({ default: m.PolymarketPage })));
const Btc5mEnginePage = React.lazy(() => import('./pages/Btc5mEnginePage').then(m => ({ default: m.Btc5mEnginePage })));
const HftDashboardPage = React.lazy(() => import('./pages/HftDashboardPage').then(m => ({ default: m.HftDashboardPage })));
const StrategyPage = React.lazy(() => import('./pages/StrategyPage').then(m => ({ default: m.StrategyPage })));

const LoadingFallback = () => (
  <div className="min-h-screen bg-[#050505] flex items-center justify-center">
    <div className="text-white/50 font-mono text-sm">Loading...</div>
  </div>
);

function App() {
  return (
    <SocketProvider>
      <BrowserRouter>
        <Suspense fallback={<LoadingFallback />}>
          <Routes>
            <Route path="/" element={<HomeDashboard />} />
            <Route path="/overview" element={<Dashboard />} />
            <Route path="/polymarket" element={<PolymarketPage />} />
            <Route path="/btc-5m-engine" element={<Btc5mEnginePage />} />
            <Route path="/hft" element={<HftDashboardPage />} />
            <Route
              path="/atomic-arb"
              element={<StrategyPage title="Atomic Arb Engine" description="Paired-outcome arbitrage with settlement tracking and edge controls." strategyIds={['ATOMIC_ARB']} />}
            />
            <Route
              path="/obi-scalper"
              element={<StrategyPage title="OBI Scalper Engine" description="Order-book imbalance scalping with spread and stale-book discipline." strategyIds={['OBI_SCALPER']} />}
            />
            <Route
              path="/cex-sniper"
              element={<StrategyPage title="CEX Sniper Engine" description="Momentum/latency strategy bridging CEX microstructure and Polymarket books." strategyIds={['CEX_SNIPER']} />}
            />
            <Route
              path="/fair-value"
              element={<StrategyPage title="Fair Value Engine" description="Cross-asset fair-value windows with directional policy and threshold controls." strategyIds={['BTC_15M', 'ETH_15M', 'SOL_15M']} />}
            />
            <Route
              path="/syndicate"
              element={<StrategyPage title="Syndicate Engine" description="Wallet-flow pressure strategy with guarded copy-style entry logic." strategyIds={['SYNDICATE']} />}
            />
            <Route
              path="/graph-arb"
              element={<StrategyPage title="Graph Arb Engine" description="Constraint/parity arbitrage across related market structures." strategyIds={['GRAPH_ARB']} />}
            />
            <Route
              path="/convergence-carry"
              element={<StrategyPage title="Convergence Carry Engine" description="Carry/convergence mean-reversion strategy with strict execution filters." strategyIds={['CONVERGENCE_CARRY']} />}
            />
            <Route
              path="/maker-mm"
              element={<StrategyPage title="Maker MM Engine" description="Spread capture and maker microstructure logic with risk-aware entry gating." strategyIds={['MAKER_MM', 'AS_MARKET_MAKER']} />}
            />
            <Route path="/markets" element={<PolymarketPage />} />
            <Route path="/intelligence" element={<HftDashboardPage />} />
            <Route
              path="/strategies"
              element={<StrategyPage title="Strategy Coverage" description="Unified execution and PnL telemetry across all active production strategies." strategyIds={['BTC_5M', 'BTC_15M', 'ETH_15M', 'SOL_15M', 'CEX_SNIPER', 'SYNDICATE', 'ATOMIC_ARB', 'OBI_SCALPER', 'GRAPH_ARB', 'CONVERGENCE_CARRY', 'MAKER_MM', 'AS_MARKET_MAKER', 'LONGSHOT_BIAS']} />}
            />
            <Route path="*" element={<Navigate to="/" replace />} />
          </Routes>
        </Suspense>
      </BrowserRouter>
    </SocketProvider>
  );
}

export default App;
