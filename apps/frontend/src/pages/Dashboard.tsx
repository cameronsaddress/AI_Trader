import { DashboardLayout } from '../components/layout/DashboardLayout';
import { MarketDataWidget } from '../components/widgets/MarketDataWidget';
import { SystemHealthWidget } from '../components/widgets/SystemHealthWidget';
import { IntelligenceWidget } from '../components/widgets/IntelligenceWidget';
import { StrategyControlWidget } from '../components/widgets/StrategyControlWidget';
import { ArbitrageScannerWidget } from '../components/widgets/ArbitrageScannerWidget';

export const Dashboard = () => {
    return (
        <DashboardLayout>
            <div className="grid grid-cols-12 gap-6">
                {/* Header Stats (Future) */}
                <div className="col-span-12 flex gap-4 mb-2">
                    {['Net Worth', 'Daily PnL', 'Active Agents', 'Risk Level'].map((label) => (
                        <div key={label} className="bg-white/5 border border-white/10 rounded-lg p-4 flex-1">
                            <div className="text-xs text-gray-500 uppercase">{label}</div>
                            <div className="text-xl font-mono text-white">--</div>
                        </div>
                    ))}
                </div>

                {/* Main Chart Section */}
                <div className="col-span-12 lg:col-span-8 h-[500px]">
                    <MarketDataWidget />
                </div>

                {/* Intelligence Panel */}
                <div className="col-span-12 lg:col-span-4 h-[500px]">
                    <IntelligenceWidget />
                </div>

                {/* System Health */}
                <div className="col-span-12 lg:col-span-4 h-[300px]">
                    <SystemHealthWidget />
                </div>

                {/* Strategy/Scanner (Future) */}
                <div className="col-span-12 lg:col-span-8 h-[300px]">
                    <StrategyControlWidget />
                </div>

                <div className="col-span-12 lg:col-span-4 h-[300px]">
                    <ArbitrageScannerWidget />
                </div>
            </div>
        </DashboardLayout>
    );
};
