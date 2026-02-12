import { Sidebar } from '../components/layout/Sidebar';

export default function PlaceholderPage({ title }: { title: string }) {
    return (
        <div className="flex min-h-screen bg-black text-white font-sans selection:bg-emerald-500/30">
            <Sidebar />
            <main className="flex-1 pl-64 transition-all duration-300">
                <div className="p-8 max-w-7xl mx-auto flex items-center justify-center h-screen">
                    <div className="text-center">
                        <h1 className="text-4xl font-bold text-transparent bg-clip-text bg-gradient-to-r from-emerald-400 to-cyan-400 mb-4">{title}</h1>
                        <p className="text-gray-500">This module is currently under development.</p>
                    </div>
                </div>
            </main>
        </div>
    );
}
