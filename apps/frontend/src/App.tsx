import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { Dashboard } from './pages/Dashboard';
import { PolymarketPage } from './pages/PolymarketPage';
import { Btc5mEnginePage } from './pages/Btc5mEnginePage';
import { SocketProvider } from './context/SocketContext';

// Placeholders for now
const Placeholder = ({ title }: { title: string }) => (
  <div className="p-10 text-white font-mono text-xl">{title} (Under Construction)</div>
);

function App() {
  return (
    <SocketProvider>
      <BrowserRouter>
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/polymarket" element={<PolymarketPage />} />
          <Route path="/btc-5m-engine" element={<Btc5mEnginePage />} />
          <Route path="/markets" element={<Placeholder title="Market Analysis" />} />
          <Route path="/intelligence" element={<Placeholder title="Swarm Intelligence" />} />
          <Route path="/strategies" element={<Placeholder title="Strategy Lab" />} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </BrowserRouter>
    </SocketProvider>
  );
}

export default App;
