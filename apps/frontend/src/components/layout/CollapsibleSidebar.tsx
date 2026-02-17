import { useState } from 'react';
import { Home, LineChart, Brain, Workflow, History, Settings, Zap, Timer, Activity, ChevronLeft, ChevronRight } from 'lucide-react';
import { clsx } from 'clsx';
import { useLocation, useNavigate } from 'react-router-dom';

const navItems = [
    { icon: Home, label: 'Home', path: '/' },
    { icon: Timer, label: 'BTC 5m Engine', path: '/btc-5m-engine' },
    { icon: Activity, label: 'HFT Strategies', path: '/hft' },
    { icon: Zap, label: 'Polymarket', path: '/polymarket' },
    { icon: LineChart, label: 'Markets', path: '/markets' },
    { icon: Brain, label: 'Intelligence', path: '/intelligence' },
    { icon: Workflow, label: 'Strategies', path: '/strategies' },
    { icon: History, label: 'History', path: '/history' },
    { icon: Settings, label: 'Settings', path: '/settings' },
];

interface CollapsibleSidebarProps {
    defaultExpanded?: boolean;
}

export function CollapsibleSidebar({ defaultExpanded = false }: CollapsibleSidebarProps) {
    const [expanded, setExpanded] = useState(defaultExpanded);
    const location = useLocation();
    const navigate = useNavigate();

    return (
        <div
            className={clsx(
                'h-full border-r border-white/10 bg-black/60 backdrop-blur-md flex flex-col shrink-0 transition-all duration-200 z-50 relative',
                expanded ? 'w-56' : 'w-12',
            )}
        >
            {/* Logo / toggle */}
            <div className={clsx('flex items-center border-b border-white/10', expanded ? 'px-4 py-4 justify-between' : 'justify-center py-4')}>
                {expanded && (
                    <span className="text-sm font-bold bg-clip-text text-transparent bg-gradient-to-r from-emerald-400 to-cyan-400 leading-none whitespace-nowrap">
                        AI TRADER
                    </span>
                )}
                <button
                    type="button"
                    onClick={() => setExpanded((v) => !v)}
                    className="text-gray-500 hover:text-white transition-colors"
                    aria-label={expanded ? 'Collapse sidebar' : 'Expand sidebar'}
                >
                    {expanded ? <ChevronLeft size={16} /> : <ChevronRight size={16} />}
                </button>
            </div>

            {/* Nav items */}
            <nav className="flex-1 py-2 space-y-1 overflow-hidden">
                {navItems.map((item) => {
                    const isActive = location.pathname === item.path;
                    return (
                        <button
                            key={item.label}
                            onClick={() => navigate(item.path)}
                            title={expanded ? undefined : item.label}
                            className={clsx(
                                'w-full flex items-center gap-2 text-xs font-medium transition-all duration-200 group',
                                expanded ? 'px-4 py-2.5' : 'justify-center py-2.5',
                                isActive
                                    ? 'bg-emerald-500/10 text-emerald-400 border-r-2 border-emerald-400'
                                    : 'text-gray-500 hover:text-white hover:bg-white/5',
                            )}
                        >
                            <item.icon className={clsx('shrink-0', isActive ? 'text-emerald-400' : 'text-gray-500 group-hover:text-white')} size={16} />
                            {expanded && <span className="truncate">{item.label}</span>}
                        </button>
                    );
                })}
            </nav>

            {/* User badge */}
            <div className={clsx('border-t border-white/10 py-3', expanded ? 'px-4' : 'flex justify-center')}>
                {expanded ? (
                    <div className="flex items-center gap-2">
                        <div className="w-6 h-6 rounded-full bg-gradient-to-br from-purple-500 to-blue-500 flex items-center justify-center text-[9px] font-bold text-white shrink-0">
                            CA
                        </div>
                        <span className="text-xs text-gray-400 truncate">canderson</span>
                    </div>
                ) : (
                    <div className="w-6 h-6 rounded-full bg-gradient-to-br from-purple-500 to-blue-500 flex items-center justify-center text-[9px] font-bold text-white">
                        CA
                    </div>
                )}
            </div>
        </div>
    );
}
