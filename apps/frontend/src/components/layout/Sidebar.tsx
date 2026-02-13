import { Home, LineChart, Brain, Workflow, History, Settings, Zap, Timer } from 'lucide-react';
import { clsx } from 'clsx';
import { motion } from 'framer-motion';
import { useLocation, useNavigate } from 'react-router-dom';

const navItems = [
    { icon: Home, label: 'Overview', path: '/' },
    { icon: Zap, label: 'Polymarket', path: '/polymarket' },
    { icon: Timer, label: 'BTC 5m Engine', path: '/btc-5m-engine' },
    { icon: LineChart, label: 'Markets', path: '/markets' },
    { icon: Brain, label: 'Intelligence', path: '/intelligence' },
    { icon: Workflow, label: 'Strategies', path: '/strategies' },
    { icon: History, label: 'History', path: '/history' },
    { icon: Settings, label: 'Settings', path: '/settings' },
];

export function Sidebar() {
    const location = useLocation();
    const navigate = useNavigate();

    return (
        <motion.div
            initial={{ x: -20, opacity: 0 }}
            animate={{ x: 0, opacity: 1 }}
            className="w-64 h-screen border-r border-white/10 bg-black/40 backdrop-blur-md flex flex-col fixed left-0 top-0 z-50"
        >
            <div className="p-6 border-b border-white/10">
                <h1 className="text-xl font-bold bg-clip-text text-transparent bg-gradient-to-r from-emerald-400 to-cyan-400">
                    AI TRADER <span className="text-xs text-white/50 block font-normal tracking-widest mt-1">SYSTEM V2</span>
                </h1>
            </div>

            <nav className="flex-1 p-4 space-y-2">
                {navItems.map((item) => {
                    const isActive = location.pathname === item.path;
                    return (
                        <button
                            key={item.label}
                            onClick={() => navigate(item.path)}
                            className={clsx(
                                "w-full flex items-center gap-3 px-4 py-3 rounded-lg text-sm font-medium transition-all duration-300 group",
                                isActive
                                    ? "bg-emerald-500/10 text-emerald-400 border border-emerald-500/20 shadow-[0_0_15px_rgba(16,185,129,0.1)]"
                                    : "text-gray-400 hover:text-white hover:bg-white/5"
                            )}
                        >
                            <item.icon className={clsx("w-5 h-5", isActive ? "text-emerald-400" : "text-gray-500 group-hover:text-white")} />
                            {item.label}
                            {isActive && (
                                <motion.div className="ml-auto w-1.5 h-1.5 rounded-full bg-emerald-400 shadow-[0_0_8px_rgba(52,211,153,0.8)]" layoutId="activeDot" />
                            )}
                        </button>
                    );
                })}
            </nav>

            <div className="p-4 border-t border-white/10">
                <div className="flex items-center gap-3 px-4 py-3 rounded-lg bg-white/5 border border-white/5">
                    <div className="w-8 h-8 rounded-full bg-gradient-to-br from-purple-500 to-blue-500 flex items-center justify-center font-bold text-xs text-white">
                        CA
                    </div>
                    <div className="flex-1 min-w-0">
                        <div className="text-sm font-medium text-white truncate">canderson</div>
                        <div className="text-xs text-gray-500">Admin</div>
                    </div>
                </div>
            </div>
        </motion.div>
    );
}
