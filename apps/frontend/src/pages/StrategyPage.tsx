import React from 'react';
import { DashboardLayout } from '../components/layout/DashboardLayout';
import { apiFetch } from '../lib/api';

type StrategyPageProps = {
    title: string;
    description: string;
    strategyIds: string[];
};

type SignalSnapshot = {
    strategy: string;
    market_key: string;
    timestamp: number;
    age_ms: number;
    passes_threshold: boolean;
    score: number;
    threshold: number;
    margin: number;
    normalized_margin: number;
    reason: string;
    meta_features: Record<string, number>;
};

type StrategyAllocatorEntry = {
    multiplier: number;
    sample_count: number;
    ema_return: number;
    updated_at: number;
};

type StrategyQualityEntry = {
    total_scans: number;
    pass_rate_pct: number;
    hold_scans: number;
    stale_blocks: number;
    risk_blocks: number;
    other_blocks: number;
    last_reason: string;
};

type StrategyExecutionRow = {
    strategy: string;
    timestamp: number;
    side: string;
    market: string;
    price: string;
    size: string;
    mode: string;
    execution_id: string | null;
};

type StrategyTradeRow = {
    strategy: string;
    timestamp: number;
    pnl: number | null;
    notional: number | null;
    reason: string;
    mode: string;
    side: string | null;
    execution_id: string | null;
};

type LedgerConcentrationEntry = {
    id: string;
    share_pct: number;
};

const REFRESH_MS = 5_000;

function asRecord(input: unknown): Record<string, unknown> | null {
    if (!input || typeof input !== 'object' || Array.isArray(input)) {
        return null;
    }
    return input as Record<string, unknown>;
}

function asString(input: unknown): string | null {
    if (typeof input !== 'string') {
        return null;
    }
    const trimmed = input.trim();
    return trimmed.length > 0 ? trimmed : null;
}

function asNumber(input: unknown): number | null {
    const parsed = Number(input);
    return Number.isFinite(parsed) ? parsed : null;
}

function parseSignalSnapshot(strategy: string, input: unknown): SignalSnapshot | null {
    const row = asRecord(input);
    if (!row) {
        return null;
    }
    const meta = asRecord(row.meta_features);
    const metaFeatures: Record<string, number> = {};
    if (meta) {
        for (const [key, value] of Object.entries(meta)) {
            const numeric = asNumber(value);
            if (numeric !== null) {
                metaFeatures[key] = numeric;
            }
        }
    }
    return {
        strategy,
        market_key: asString(row.market_key) || strategy.toLowerCase(),
        timestamp: asNumber(row.timestamp) ?? 0,
        age_ms: asNumber(row.age_ms) ?? 0,
        passes_threshold: row.passes_threshold === true,
        score: asNumber(row.score) ?? 0,
        threshold: asNumber(row.threshold) ?? 0,
        margin: asNumber(row.margin) ?? 0,
        normalized_margin: asNumber(row.normalized_margin) ?? 0,
        reason: asString(row.reason) || '',
        meta_features: metaFeatures,
    };
}

function parseAllocator(input: unknown): Record<string, StrategyAllocatorEntry> {
    const map = asRecord(input) || {};
    const out: Record<string, StrategyAllocatorEntry> = {};
    for (const [strategy, raw] of Object.entries(map)) {
        const row = asRecord(raw);
        if (!row) {
            continue;
        }
        out[strategy.toUpperCase()] = {
            multiplier: asNumber(row.multiplier) ?? 1,
            sample_count: asNumber(row.sample_count) ?? 0,
            ema_return: asNumber(row.ema_return) ?? 0,
            updated_at: asNumber(row.updated_at) ?? 0,
        };
    }
    return out;
}

function parseQuality(input: unknown): Record<string, StrategyQualityEntry> {
    const map = asRecord(input) || {};
    const out: Record<string, StrategyQualityEntry> = {};
    for (const [strategy, raw] of Object.entries(map)) {
        const row = asRecord(raw);
        if (!row) {
            continue;
        }
        out[strategy.toUpperCase()] = {
            total_scans: asNumber(row.total_scans) ?? 0,
            pass_rate_pct: asNumber(row.pass_rate_pct) ?? 0,
            hold_scans: asNumber(row.hold_scans) ?? 0,
            stale_blocks: asNumber(row.stale_blocks) ?? 0,
            risk_blocks: asNumber(row.risk_blocks) ?? 0,
            other_blocks: asNumber(row.other_blocks) ?? 0,
            last_reason: asString(row.last_reason) || '',
        };
    }
    return out;
}

function parseConcentration(input: unknown): LedgerConcentrationEntry[] {
    if (!Array.isArray(input)) {
        return [];
    }
    return input
        .map((entry) => {
            const row = asRecord(entry);
            if (!row) {
                return null;
            }
            const id = asString(row.id);
            if (!id) {
                return null;
            }
            return {
                id: id.toUpperCase(),
                share_pct: asNumber(row.share_pct) ?? 0,
            };
        })
        .filter((entry): entry is LedgerConcentrationEntry => entry !== null);
}

function parseExecutionRows(strategy: string, input: unknown): StrategyExecutionRow[] {
    const payload = asRecord(input);
    const entries = Array.isArray(payload?.entries) ? payload.entries : [];
    return entries
        .map((entry) => {
            const row = asRecord(entry);
            if (!row) {
                return null;
            }
            const details = asRecord(row.details);
            const executionId = asString(row.execution_id)
                || asString(row.executionId)
                || asString(details?.execution_id)
                || asString(details?.executionId)
                || null;
            return {
                strategy,
                timestamp: asNumber(row.timestamp) ?? Date.now(),
                side: asString(row.side) || 'INFO',
                market: asString(row.market) || strategy,
                price: String(row.price ?? '--'),
                size: String(row.size ?? '--'),
                mode: asString(row.mode) || 'PAPER',
                execution_id: executionId,
            };
        })
        .filter((entry): entry is StrategyExecutionRow => entry !== null);
}

function parseTradeRows(strategy: string, input: unknown): StrategyTradeRow[] {
    const payload = asRecord(input);
    const rows = Array.isArray(payload?.trades) ? payload.trades : [];
    return rows
        .map((entry) => {
            const row = asRecord(entry);
            if (!row) {
                return null;
            }
            return {
                strategy,
                timestamp: asNumber(row.timestamp) ?? Date.now(),
                pnl: asNumber(row.pnl),
                notional: asNumber(row.notional),
                reason: asString(row.reason) || '',
                mode: asString(row.mode) || 'PAPER',
                side: asString(row.side),
                execution_id: asString(row.execution_id) || null,
            };
        })
        .filter((entry): entry is StrategyTradeRow => entry !== null);
}

export const StrategyPage: React.FC<StrategyPageProps> = ({ title, description, strategyIds }) => {
    const [loading, setLoading] = React.useState(true);
    const [error, setError] = React.useState<string | null>(null);
    const [signals, setSignals] = React.useState<Record<string, SignalSnapshot | null>>({});
    const [allocator, setAllocator] = React.useState<Record<string, StrategyAllocatorEntry>>({});
    const [quality, setQuality] = React.useState<Record<string, StrategyQualityEntry>>({});
    const [strategyExposure, setStrategyExposure] = React.useState<LedgerConcentrationEntry[]>([]);
    const [executions, setExecutions] = React.useState<StrategyExecutionRow[]>([]);
    const [trades, setTrades] = React.useState<StrategyTradeRow[]>([]);

    React.useEffect(() => {
        let active = true;
        let inFlight = false;
        let rerunRequested = false;
        let activeController: AbortController | null = null;

        const load = async () => {
            const controller = new AbortController();
            activeController = controller;
            try {
                const [statsRes, intelligenceRes] = await Promise.all([
                    apiFetch('/api/arb/stats', { signal: controller.signal }),
                    apiFetch('/api/arb/intelligence', { signal: controller.signal }),
                ]);
                if (!statsRes.ok || !intelligenceRes.ok) {
                    throw new Error(`HTTP ${statsRes.status}/${intelligenceRes.status}`);
                }

                const [statsPayload, intelligencePayload] = await Promise.all([
                    statsRes.json(),
                    intelligenceRes.json(),
                ]);
                if (!active) {
                    return;
                }

                const strategySignalsRaw = asRecord(asRecord(intelligencePayload)?.strategies) || {};
                const nextSignals: Record<string, SignalSnapshot | null> = {};
                for (const strategyId of strategyIds) {
                    const normalized = strategyId.toUpperCase();
                    nextSignals[normalized] = parseSignalSnapshot(normalized, strategySignalsRaw[normalized]);
                }

                const allocatorMap = parseAllocator(asRecord(statsPayload)?.strategy_allocator);
                const qualityMap = parseQuality(asRecord(statsPayload)?.strategy_quality);
                const topExposure = parseConcentration(asRecord(statsPayload)?.ledger_health
                    ? asRecord(asRecord(statsPayload)!.ledger_health)?.top_strategy_exposure
                    : undefined);

                const executionRequests = strategyIds.map(async (strategyId) => {
                    const normalized = strategyId.toUpperCase();
                    const response = await apiFetch(
                        `/api/arb/strategy-executions?strategy=${encodeURIComponent(normalized)}&limit=100`,
                        { signal: controller.signal },
                    );
                    if (!response.ok) {
                        return [] as StrategyExecutionRow[];
                    }
                    return parseExecutionRows(normalized, await response.json());
                });
                const tradeRequests = strategyIds.map(async (strategyId) => {
                    const normalized = strategyId.toUpperCase();
                    const response = await apiFetch(
                        `/api/arb/strategy-trades?strategy=${encodeURIComponent(normalized)}&limit=100`,
                        { signal: controller.signal },
                    );
                    if (!response.ok) {
                        return [] as StrategyTradeRow[];
                    }
                    return parseTradeRows(normalized, await response.json());
                });
                const [executionBatches, tradeBatches] = await Promise.all([
                    Promise.all(executionRequests),
                    Promise.all(tradeRequests),
                ]);
                if (!active) {
                    return;
                }

                const mergedExecutions = executionBatches
                    .flat()
                    .sort((a, b) => b.timestamp - a.timestamp)
                    .slice(0, 100);
                const mergedTrades = tradeBatches
                    .flat()
                    .sort((a, b) => b.timestamp - a.timestamp)
                    .slice(0, 100);

                setSignals(nextSignals);
                setAllocator(allocatorMap);
                setQuality(qualityMap);
                setStrategyExposure(topExposure);
                setExecutions(mergedExecutions);
                setTrades(mergedTrades);
                setError(null);
            } catch (loadError) {
                if (loadError instanceof DOMException && loadError.name === 'AbortError') {
                    return;
                }
                if (!active) {
                    return;
                }
                setError(loadError instanceof Error ? loadError.message : String(loadError));
            } finally {
                if (activeController === controller) {
                    activeController = null;
                }
                if (active) {
                    setLoading(false);
                }
            }
        };

        const runLoad = async () => {
            if (!active) {
                return;
            }
            if (inFlight) {
                rerunRequested = true;
                return;
            }
            inFlight = true;
            try {
                await load();
            } finally {
                inFlight = false;
                if (active && rerunRequested) {
                    rerunRequested = false;
                    void runLoad();
                }
            }
        };

        void runLoad();
        const intervalId = window.setInterval(() => {
            void runLoad();
        }, REFRESH_MS);

        return () => {
            active = false;
            rerunRequested = false;
            activeController?.abort();
            activeController = null;
            window.clearInterval(intervalId);
        };
    }, [strategyIds]);

    return (
        <DashboardLayout>
            <div className="space-y-4">
                <div className="rounded-xl border border-white/10 bg-black/35 px-4 py-4">
                    <div className="text-xs text-cyan-300 uppercase tracking-wider font-mono mb-1">Strategy Engine</div>
                    <h1 className="text-2xl font-semibold text-white">{title}</h1>
                    <p className="text-sm text-gray-400 mt-1">{description}</p>
                    <div className="text-xs text-gray-500 font-mono mt-2">
                        Coverage: {strategyIds.join(', ')} {loading ? '| refreshing...' : ''}
                    </div>
                    {error && (
                        <div className="text-xs text-rose-300 font-mono mt-2">Data fetch error: {error}</div>
                    )}
                </div>

                <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
                    {strategyIds.map((strategyId) => {
                        const strategy = strategyId.toUpperCase();
                        const signal = signals[strategy];
                        const alloc = allocator[strategy];
                        const perf = quality[strategy];
                        const exposure = strategyExposure.find((entry) => entry.id === strategy);
                        return (
                            <div key={strategy} className="rounded-xl border border-white/10 bg-black/35 p-4">
                                <div className="flex items-center justify-between">
                                    <div className="text-sm font-semibold text-white">{strategy}</div>
                                    <div className={`text-xs font-mono ${signal?.passes_threshold ? 'text-emerald-300' : 'text-amber-300'}`}>
                                        {signal ? (signal.passes_threshold ? 'PASS' : 'HOLD') : 'NO_SIGNAL'}
                                    </div>
                                </div>
                                <div className="mt-2 text-xs text-gray-400 font-mono">
                                    signal {signal ? signal.score.toFixed(4) : '--'} vs threshold {signal ? signal.threshold.toFixed(4) : '--'}
                                </div>
                                <div className="text-xs text-gray-400 font-mono">
                                    margin {signal ? signal.margin.toFixed(4) : '--'} | normalized {signal ? signal.normalized_margin.toFixed(3) : '--'} | age {signal ? `${signal.age_ms}ms` : '--'}
                                </div>
                                <div className="text-xs text-gray-500 font-mono mt-1 truncate" title={signal?.reason || perf?.last_reason || ''}>
                                    reason: {signal?.reason || perf?.last_reason || '--'}
                                </div>
                                <div className="text-xs text-gray-400 font-mono mt-2">
                                    risk size {alloc ? `${alloc.multiplier.toFixed(2)}x` : '--'} | samples {alloc?.sample_count ?? 0}
                                </div>
                                <div className="text-xs text-gray-400 font-mono">
                                    pass rate {perf ? `${perf.pass_rate_pct.toFixed(1)}%` : '--'} | holds {perf?.hold_scans ?? 0} | stale blocks {perf?.stale_blocks ?? 0}
                                </div>
                                <div className="text-xs text-gray-400 font-mono">
                                    exposure {exposure ? `${exposure.share_pct.toFixed(1)}%` : '--'}
                                </div>
                                <div className="mt-2 text-[11px] text-gray-500 font-mono">
                                    Live inputs:
                                    {signal && Object.keys(signal.meta_features).length > 0
                                        ? ` ${Object.entries(signal.meta_features).slice(0, 6).map(([key, value]) => `${key}=${value.toFixed(4)}`).join(' | ')}`
                                        : ' --'}
                                </div>
                            </div>
                        );
                    })}
                </div>

                <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
                    <div className="rounded-xl border border-white/10 bg-black/35 p-4">
                        <div className="text-xs text-gray-400 uppercase tracking-wider font-mono mb-2">Last 100 Executions</div>
                        <div className="max-h-[420px] overflow-auto space-y-1">
                            {executions.length === 0 && (
                                <div className="text-xs text-gray-500 font-mono">No execution events yet</div>
                            )}
                            {executions.map((row) => (
                                <div key={`${row.timestamp}:${row.execution_id || row.side}:${row.strategy}`} className="border-b border-white/10 pb-1 text-[11px] font-mono">
                                    <div className="flex justify-between text-gray-200">
                                        <span>{row.strategy} {row.side}</span>
                                        <span>{new Date(row.timestamp).toLocaleTimeString()}</span>
                                    </div>
                                    <div className="text-gray-500 truncate">
                                        {row.market} | px {row.price} | size {row.size} | {row.mode}
                                    </div>
                                </div>
                            ))}
                        </div>
                    </div>

                    <div className="rounded-xl border border-white/10 bg-black/35 p-4">
                        <div className="text-xs text-gray-400 uppercase tracking-wider font-mono mb-2">Last 100 PnL Events</div>
                        <div className="max-h-[420px] overflow-auto space-y-1">
                            {trades.length === 0 && (
                                <div className="text-xs text-gray-500 font-mono">No PnL events yet</div>
                            )}
                            {trades.map((row) => (
                                <div key={`${row.timestamp}:${row.execution_id || row.strategy}:${row.reason}`} className="border-b border-white/10 pb-1 text-[11px] font-mono">
                                    <div className="flex justify-between">
                                        <span className="text-gray-200">{row.strategy} {row.side || 'CLOSE'}</span>
                                        <span className="text-gray-400">{new Date(row.timestamp).toLocaleTimeString()}</span>
                                    </div>
                                    <div className={`${(row.pnl ?? 0) >= 0 ? 'text-emerald-300' : 'text-rose-300'}`}>
                                        pnl {row.pnl !== null ? row.pnl.toFixed(2) : '--'} | notional {row.notional !== null ? row.notional.toFixed(2) : '--'} | {row.mode}
                                    </div>
                                    <div className="text-gray-500 truncate" title={row.reason}>
                                        {row.reason || '--'}
                                    </div>
                                </div>
                            ))}
                        </div>
                    </div>
                </div>
            </div>
        </DashboardLayout>
    );
};
