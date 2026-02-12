import { useEffect, useState } from 'react';
import { useSocket } from '../../context/SocketContext';
import { motion, AnimatePresence } from 'framer-motion';

interface AgentResponse {
    decision: 'BUY' | 'SELL' | 'HOLD';
    confidence: number;
    reasoning: string;
    suggestedSize?: number;
}

interface AgentDecisions {
    [key: string]: AgentResponse;
}

export const IntelligenceWidget = () => {
    const { socket } = useSocket();
    const [decisions, setDecisions] = useState<AgentDecisions>({});
    const [lastUpdate, setLastUpdate] = useState<Date>(new Date());

    useEffect(() => {
        if (!socket) return;

        socket.on('agent_decisions', (data: AgentDecisions) => {
            console.log('Received agent decisions:', data);
            setDecisions(data);
            setLastUpdate(new Date());
        });

        return () => {
            socket.off('agent_decisions');
        };
    }, [socket]);

    return (
        <div className="bg-black/40 backdrop-blur-md border border-white/10 rounded-xl p-6 h-full flex flex-col">
            <div className="flex justify-between items-center mb-6">
                <h2 className="text-xl font-bold text-white flex items-center gap-2">
                    <span className="w-2 h-2 rounded-full bg-purple-500 animate-pulse" />
                    Swarm Intelligence
                </h2>
                <div className="text-xs text-white/40 font-mono">
                    LAST UPDATE: {lastUpdate.toLocaleTimeString()}
                </div>
            </div>

            <div className="flex-1 overflow-y-auto space-y-4 pr-2 custom-scrollbar">
                <AnimatePresence mode='popLayout'>
                    {Object.entries(decisions).length === 0 ? (
                        <div className="text-center text-white/30 py-10 font-mono text-sm">
                            WAITING FOR AGENT SIGNAL...
                        </div>
                    ) : (
                        Object.entries(decisions).map(([agentName, decision]) => (
                            <motion.div
                                key={agentName}
                                initial={{ opacity: 0, x: -20 }}
                                animate={{ opacity: 1, x: 0 }}
                                exit={{ opacity: 0, x: 20 }}
                                className="bg-white/5 border border-white/10 rounded-lg p-4 hover:bg-white/10 transition-colors"
                            >
                                <div className="flex justify-between items-start mb-2">
                                    <div className="font-bold text-purple-400 font-mono">{agentName}</div>
                                    <div className={`px-2 py-1 rounded text-xs font-bold ${decision.decision === 'BUY' ? 'bg-green-500/20 text-green-400' :
                                            decision.decision === 'SELL' ? 'bg-red-500/20 text-red-400' :
                                                'bg-gray-500/20 text-gray-400'
                                        }`}>
                                        {decision.decision}
                                    </div>
                                </div>
                                <div className="text-sm text-white/80 mb-3 font-light leading-relaxed">
                                    {decision.reasoning}
                                </div>
                                <div className="flex items-center gap-3 text-xs font-mono">
                                    <div className="text-white/40">CONFIDENCE</div>
                                    <div className="flex-1 h-1 bg-white/10 rounded-full overflow-hidden">
                                        <div
                                            className="h-full bg-purple-500 rounded-full transition-all duration-500"
                                            style={{ width: `${decision.confidence * 100}%` }}
                                        />
                                    </div>
                                    <div className="text-purple-400">{(decision.confidence * 100).toFixed(0)}%</div>
                                </div>
                            </motion.div>
                        ))
                    )}
                </AnimatePresence>
            </div>
        </div>
    );
};
