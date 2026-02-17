import React, { Suspense } from 'react';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { HomeDashboard } from './pages/HomeDashboard';
import { SocketProvider } from './context/SocketContext';

// Lazy-load heavy routes for code-splitting
const Dashboard = React.lazy(() => import('./pages/Dashboard').then(m => ({ default: m.Dashboard })));
const PolymarketPage = React.lazy(() => import('./pages/PolymarketPage').then(m => ({ default: m.PolymarketPage })));
const Btc5mEnginePage = React.lazy(() => import('./pages/Btc5mEnginePage').then(m => ({ default: m.Btc5mEnginePage })));
const HftDashboardPage = React.lazy(() => import('./pages/HftDashboardPage').then(m => ({ default: m.HftDashboardPage })));

const LoadingFallback = () => (
  <div className="min-h-screen bg-[#050505] flex items-center justify-center">
    <div className="text-white/50 font-mono text-sm">Loading...</div>
  </div>
);

// Placeholders for now
const Placeholder = ({ title }: { title: string }) => (
  <div className="p-10 text-white font-mono text-xl">{title} (Under Construction)</div>
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
            <Route path="/markets" element={<Placeholder title="Market Analysis" />} />
            <Route path="/intelligence" element={<Placeholder title="Swarm Intelligence" />} />
            <Route path="/strategies" element={<Placeholder title="Strategy Lab" />} />
            <Route path="*" element={<Navigate to="/" replace />} />
          </Routes>
        </Suspense>
      </BrowserRouter>
    </SocketProvider>
  );
}

export default App;
