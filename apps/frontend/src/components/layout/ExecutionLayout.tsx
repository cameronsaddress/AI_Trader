import React from 'react';

// Full-screen layout used for execution dashboards that need pixel-level control.
// Intentionally avoids the global sidebar/header chrome to match "terminal wall" UIs.
export function ExecutionLayout({ children }: { children: React.ReactNode }) {
  return (
    <div className="min-h-screen bg-[#050505] text-white overflow-hidden selection:bg-emerald-500/30">
      {/* Subtle vignette */}
      <div className="fixed inset-0 pointer-events-none bg-[radial-gradient(1200px_600px_at_35%_20%,rgba(255,255,255,0.06),transparent_65%)]" />
      <div className="fixed inset-0 pointer-events-none bg-[radial-gradient(900px_500px_at_70%_35%,rgba(16,185,129,0.05),transparent_70%)]" />
      <main className="relative z-10 h-screen w-screen">
        {children}
      </main>
    </div>
  );
}

