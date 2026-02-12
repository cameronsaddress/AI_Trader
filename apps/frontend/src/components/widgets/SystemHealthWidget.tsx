import { Activity, Server, Database, Brain } from 'lucide-react';
import { motion } from 'framer-motion';
import { useSocket } from '../../context/SocketContext';
import { useEffect, useState } from 'react';

const initialSystems = [
    { name: 'Trading Engine', status: 'operational', icon: Activity, load: '12%' },
    { name: 'Redis Queue', status: 'operational', icon: Database, load: '45ms' },
    { name: 'ML Service', status: 'processing', icon: Brain, load: '89%' },
    { name: 'Arb Scanner', status: 'operational', load: 'Active' },
];

const iconMap: Record<string, any> = {
    'Trading Engine': Activity,
    'Redis Queue': Database,
    'ML Service': Brain,
    'Arb Scanner': Server,
};

export function SystemHealthWidget() {
    // Destructure isConnected directly from context
    const { socket, isConnected } = useSocket();
    const [lastError, setLastError] = useState<string>('');
    const [systems, setSystems] = useState(initialSystems);

    useEffect(() => {
        if (!socket) return;

        const onError = (err: any) => {
            setLastError(err.message || 'Connection Error');
        };

        const onConnect = () => setLastError('');

        socket.on('connect_error', onError);
        socket.on('connect', onConnect);

        socket.on('system_health_update', (data: any[]) => {
            // Merge backend data with local icons? 
            // Actually, just storing the data is fine if we look up icons by name.
            setSystems(data);
        });

        return () => {
            socket.off('connect_error', onError);
            socket.off('connect', onConnect);
            socket.off('system_health_update');
        };
    }, [socket]);

    return (
        <div className="h-full flex flex-col bg-white/5 border border-white/10 rounded-xl p-6 backdrop-blur-md overflow-hidden">
            <div className="flex justify-between items-center mb-4">
                <h2 className="text-lg font-semibold text-white">System Status</h2>
                <div className={`flex items-center gap-2 text-xs font-mono px-2 py-1 rounded-full ${isConnected ? 'bg-emerald-500/20 text-emerald-400' : 'bg-red-500/20 text-red-400'}`}>
                    <div className={`w-2 h-2 rounded-full ${isConnected ? 'bg-emerald-400 animate-pulse' : 'bg-red-400'}`} />
                    {isConnected ? 'CONNECTED' : 'OFFLINE'}
                </div>
            </div>

            {/* Debug Info Overlay */}
            <div className="mb-4 p-2 rounded bg-black/40 text-[10px] font-mono text-gray-400">
                <div>Socket ID: {socket?.id || 'None'}</div>
                {lastError && <div className="text-red-400">Error: {lastError}</div>}
                <div>Transport: {socket?.io?.engine?.transport?.name || 'Unknown'}</div>
                <div>Path: {socket?.io?.opts?.path || 'Unknown'}</div>
            </div>

            <div className="space-y-4 flex-1 overflow-y-auto custom-scrollbar pr-2">
                {systems.map((sys, idx) => {
                    const Icon = iconMap[sys.name] || Activity;
                    return (
                        <motion.div
                            key={sys.name}
                            initial={{ opacity: 0, y: 10 }}
                            animate={{ opacity: 1, y: 0 }}
                            transition={{ delay: idx * 0.1 }}
                            className="flex items-center justify-between p-3 rounded-lg bg-white/5 border border-white/5 hover:bg-white/10 transition-colors"
                        >
                            <div className="flex items-center gap-3">
                                <div className={`p-2 rounded-lg ${sys.status === 'operational' || sys.status === 'active' ? 'bg-emerald-500/20 text-emerald-400' : 'bg-amber-500/20 text-amber-400'}`}>
                                    <Icon className="w-4 h-4" />
                                </div>
                                <div>
                                    <div className="text-sm font-medium text-white">{sys.name}</div>
                                    <div className="text-xs text-gray-500 capitalize">{sys.status}</div>
                                </div>
                            </div>
                            <div className="text-right">
                                <div className="text-sm font-mono text-white">{sys.load}</div>
                                <div className="text-[10px] text-gray-500">Load</div>
                            </div>
                        </motion.div>
                    );
                })}
            </div>
        </div>
    );
}

