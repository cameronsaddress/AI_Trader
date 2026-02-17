import React from 'react';
import { CollapsibleSidebar } from './CollapsibleSidebar';

// Full-screen layout used for execution dashboards that need pixel-level control.
// Now includes a collapsible sidebar for navigation while preserving the terminal-wall feel.
export function ExecutionLayout({ children }: { children: React.ReactNode }) {
  return (
    <div className="h-[100dvh] w-[100dvw] bg-[#050505] text-white overflow-hidden selection:bg-emerald-500/30 flex">
      {/* Subtle vignette */}
      <div className="fixed inset-0 pointer-events-none bg-[radial-gradient(1200px_600px_at_35%_20%,rgba(255,255,255,0.06),transparent_65%)]" />
      <div className="fixed inset-0 pointer-events-none bg-[radial-gradient(900px_500px_at_70%_35%,rgba(16,185,129,0.05),transparent_70%)]" />
      <CollapsibleSidebar defaultExpanded={false} />
      <main className="relative z-10 flex-1 min-w-0 h-full overflow-x-hidden overflow-y-auto">
        {children}
      </main>
    </div>
  );
}
