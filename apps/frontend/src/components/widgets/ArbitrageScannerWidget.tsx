import { useEffect, useMemo, useRef, useState } from 'react';
import { useSocket } from '../../context/SocketContext';

type ScanKind = 'ATOMIC' | 'SNIPER' | 'SYNDICATE' | 'FAIR_VALUE' | 'OBI' | 'UNKNOWN';

type SignalPoint = {
    timestamp: number;
    score: number;
};

type ScanMessage = {
    market_id: string;
    symbol: string;
    prices: [number, number];
    sum: number;
    gap: number;
    timestamp: number;
    strategy?: string;
    signal_type?: string;
    unit?: string;
    threshold?: number;
    score?: number;
    passes_threshold?: boolean;
    reason?: string;
    metric_label?: string;
};

type DisplayRow = {
    key: string;
    marketLabel: string;
    dataText: string;
    metricText: string;
    signalText: string;
    signalValue: number;
    kind: ScanKind;
    stale: boolean;
    timestamp: number;
    actionable: boolean;
    reasonText: string;
    thresholdText: string;
    sparklineDomain?: { min: number; max: number };
    history: SignalPoint[];
};

const ATOMIC_MIN_NET_EDGE = 0.0035;
const SNIPER_ENTRY_THRESHOLD = 0.0008;
const FV_ENTRY_EDGE = 0.04;
const OBI_SIGNAL_THRESHOLD = 0.45;
const SYNDICATE_MIN_BUY_RATIO = 0.65;
const SYNDICATE_MIN_WALLETS = 3;
const SYNDICATE_MIN_VOL = 10_000;
const STALE_MS = 3000;
const HISTORY_WINDOW_MS = 60_000;
const HISTORY_MAX_POINTS = 240;
const MAX_ROWS = 8;
const ROW_ORDER: Record<ScanKind, number> = {
    ATOMIC: 0,
    SNIPER: 1,
    SYNDICATE: 2,
    FAIR_VALUE: 3,
    OBI: 4,
    UNKNOWN: 5,
};

function parseNumber(input: unknown): number {
    const value = Number(input);
    return Number.isFinite(value) ? value : 0;
}

function hasNumber(input: unknown): input is number {
    return typeof input === 'number' && Number.isFinite(input);
}

function parseScanMessage(input: unknown): ScanMessage | null {
    if (!input || typeof input !== 'object') {
        return null;
    }

    const payload = input as Partial<ScanMessage>;
    const symbol = typeof payload.symbol === 'string' ? payload.symbol : '';
    const marketId = typeof payload.market_id === 'string' ? payload.market_id : '';
    if (!symbol) {
        return null;
    }

    const pricesRaw = Array.isArray(payload.prices) ? payload.prices : [];
    const first = parseNumber(pricesRaw[0]);
    const second = parseNumber(pricesRaw[1]);

    const strategy = typeof payload.strategy === 'string' ? payload.strategy : undefined;
    const signalType = typeof payload.signal_type === 'string' ? payload.signal_type : undefined;
    const unit = typeof payload.unit === 'string' ? payload.unit : undefined;
    const reason = typeof payload.reason === 'string' ? payload.reason : undefined;
    const metricLabel = typeof payload.metric_label === 'string' ? payload.metric_label : undefined;
    const passesThreshold = typeof payload.passes_threshold === 'boolean' ? payload.passes_threshold : undefined;

    return {
        market_id: marketId,
        symbol,
        prices: [first, second],
        sum: parseNumber(payload.sum),
        gap: parseNumber(payload.gap),
        timestamp: parseNumber(payload.timestamp) || Date.now(),
        strategy,
        signal_type: signalType,
        unit,
        threshold: hasNumber(payload.threshold) ? payload.threshold : undefined,
        score: hasNumber(payload.score) ? payload.score : undefined,
        passes_threshold: passesThreshold,
        reason,
        metric_label: metricLabel,
    };
}

function classify(strategy: string | undefined, symbol: string): ScanKind {
    if (strategy === 'ATOMIC_ARB' || symbol === 'ATOMIC-ARB') return 'ATOMIC';
    if (strategy === 'CEX_SNIPER' || symbol.includes('SNIPER') || symbol.includes('COINBASE')) return 'SNIPER';
    if (strategy === 'SYNDICATE' || symbol.includes('SYNDICATE')) return 'SYNDICATE';
    if (strategy === 'OBI_SCALPER' || symbol.includes('OBI')) return 'OBI';
    if ((strategy && strategy.endsWith('_15M')) || symbol.endsWith(' FV')) return 'FAIR_VALUE';
    return 'UNKNOWN';
}

function formatMoneyCompact(value: number): string {
    if (Math.abs(value) >= 1000) {
        return `$${(value / 1000).toFixed(1)}k`;
    }
    return `$${value.toFixed(0)}`;
}

function formatCents(value: number): string {
    return `${(value * 100).toFixed(2)}c`;
}

function formatSignedPercent(value: number): string {
    const pct = value * 100;
    const sign = pct > 0 ? '+' : '';
    return `${sign}${pct.toFixed(2)}%`;
}

function formatSignedCents(value: number): string {
    const cents = value * 100;
    const sign = cents > 0 ? '+' : '';
    return `${sign}${cents.toFixed(2)}c`;
}

function defaultThreshold(kind: ScanKind): number {
    if (kind === 'ATOMIC') return ATOMIC_MIN_NET_EDGE;
    if (kind === 'SNIPER') return SNIPER_ENTRY_THRESHOLD;
    if (kind === 'FAIR_VALUE') return FV_ENTRY_EDGE;
    if (kind === 'OBI') return OBI_SIGNAL_THRESHOLD;
    return 0;
}

function buildRow(rowKey: string, scan: ScanMessage, history: SignalPoint[]): DisplayRow {
    const kind = classify(scan.strategy, scan.symbol);
    const score = hasNumber(scan.score) ? scan.score : scan.gap;
    const threshold = hasNumber(scan.threshold) ? scan.threshold : defaultThreshold(kind);
    const now = Date.now();
    const stale = now - scan.timestamp > STALE_MS;

    let marketLabel = scan.symbol;
    let dataText = `P1 ${scan.prices[0].toFixed(4)} | P2 ${scan.prices[1].toFixed(4)}`;
    let metricText = scan.metric_label ? `${scan.metric_label}: ${scan.sum.toFixed(4)}` : scan.sum.toFixed(4);
    let signalText = formatSignedPercent(score);
    let actionable = score > 0;
    let fallbackReason = 'Signal positive';
    let sparklineDomain: { min: number; max: number } | undefined;

    if (kind === 'ATOMIC') {
        marketLabel = 'Atomic Pair Arb';
        dataText = `YES ${formatCents(scan.prices[0])} | NO ${formatCents(scan.prices[1])}`;
        metricText = `Ask Sum ${scan.sum.toFixed(4)}`;
        signalText = `Net Edge ${formatSignedPercent(score)}`;
        actionable = score >= threshold;
        fallbackReason = actionable
            ? `Net edge cleared ${(threshold * 100).toFixed(2)}% threshold`
            : `Net edge below ${(threshold * 100).toFixed(2)}% threshold`;
    } else if (kind === 'SNIPER') {
        marketLabel = 'CEX Latency Arb';
        dataText = `Coinbase ${formatMoneyCompact(scan.prices[0])} | Poly ${formatCents(scan.prices[1])}`;
        metricText = `Mid Sum ${scan.sum.toFixed(4)}`;
        signalText = `Momentum ${formatSignedPercent(score)}`;
        actionable = Math.abs(score) >= Math.abs(threshold);
        fallbackReason = actionable
            ? `|momentum| cleared ${(Math.abs(threshold) * 100).toFixed(2)}% trigger`
            : `|momentum| below ${(Math.abs(threshold) * 100).toFixed(2)}% trigger`;
    } else if (kind === 'SYNDICATE') {
        const volumeUsd = scan.prices[0];
        const wallets = Math.round(scan.prices[1]);
        const buyRatio = Math.max(0, Math.min(1, score + 0.5));
        marketLabel = 'Whale Syndicate';
        dataText = `${formatMoneyCompact(volumeUsd)} | ${wallets} wallets`;
        metricText = `Avg Entry ${formatCents(scan.sum)}`;
        signalText = `Buy Pressure ${(buyRatio * 100).toFixed(2)}%`;
        actionable = buyRatio >= SYNDICATE_MIN_BUY_RATIO && wallets >= SYNDICATE_MIN_WALLETS && volumeUsd >= SYNDICATE_MIN_VOL;
        fallbackReason = actionable
            ? 'Wallet-count, volume, and buy-pressure filters passed'
            : 'Cluster below volume/wallet/buy-pressure filters';
    } else if (kind === 'FAIR_VALUE') {
        const asset = scan.symbol.replace(' FV', '');
        marketLabel = `${asset} Fair Value`;
        dataText = `YES Mid ${formatCents(scan.prices[0])} | FV ${formatCents(scan.prices[1])}`;
        metricText = `Mid Sum ${scan.sum.toFixed(4)}`;
        signalText = `Edge ${formatSignedCents(score)}`;
        actionable = score >= threshold;
        fallbackReason = actionable
            ? `Fair-value edge cleared ${(threshold * 100).toFixed(2)}c threshold`
            : `Fair-value edge below ${(threshold * 100).toFixed(2)}c threshold`;
    } else if (kind === 'OBI') {
        marketLabel = 'OBI Scalper';
        dataText = `Bid ${formatMoneyCompact(scan.prices[0])} | Ask ${formatMoneyCompact(scan.prices[1])}`;
        metricText = `${scan.metric_label || 'Mid'} ${formatMoneyCompact(scan.sum)}`;
        signalText = `OBI ${score.toFixed(3)}`;
        actionable = Math.abs(score) >= Math.abs(threshold);
        // Keep OBI chart vertically stable to avoid visual "bouncing" caused by dynamic auto-scaling.
        sparklineDomain = { min: -1, max: 1 };
        fallbackReason = actionable
            ? `|OBI| cleared ${Math.abs(threshold).toFixed(3)} trigger`
            : `|OBI| below ${Math.abs(threshold).toFixed(3)} trigger`;
    }

    const reasonText = (scan.reason && scan.reason.trim().length > 0) ? scan.reason : fallbackReason;
    const actionableFromPayload = typeof scan.passes_threshold === 'boolean' ? scan.passes_threshold : actionable;
    const thresholdText = kind === 'FAIR_VALUE'
        ? `Threshold ${(threshold * 100).toFixed(2)}c`
        : kind === 'OBI'
        ? `Threshold ${Math.abs(threshold).toFixed(3)}`
        : `Threshold ${(Math.abs(threshold) * 100).toFixed(2)}%`;

    return {
        key: rowKey,
        marketLabel,
        dataText,
        metricText,
        signalText,
        signalValue: score,
        kind,
        stale,
        timestamp: scan.timestamp,
        actionable: actionableFromPayload,
        reasonText,
        thresholdText,
        sparklineDomain,
        history,
    };
}

function signalClass(kind: ScanKind, value: number, actionable: boolean): string {
    if (!actionable) {
        return 'text-gray-500';
    }

    if (kind === 'FAIR_VALUE' || kind === 'ATOMIC' || kind === 'SYNDICATE' || kind === 'OBI') {
        return 'text-emerald-400';
    }

    if (kind === 'SNIPER') {
        return value > 0 ? 'text-emerald-400' : 'text-rose-400';
    }

    return value > 0 ? 'text-emerald-400' : 'text-gray-500';
}

function rowKeyFor(scan: ScanMessage): string {
    const strategy = scan.strategy?.trim();
    if (strategy) {
        return strategy;
    }

    const kind = classify(scan.strategy, scan.symbol);
    if (kind === 'OBI') return 'OBI_SCALPER';
    if (kind === 'ATOMIC') return 'ATOMIC_ARB';

    return scan.symbol;
}

export const ArbitrageScannerWidget = () => {
    const { socket } = useSocket();
    const [scansByKey, setScansByKey] = useState<Record<string, ScanMessage>>({});
    const [historyByKey, setHistoryByKey] = useState<Record<string, SignalPoint[]>>({});
    const [lastUpdateMs, setLastUpdateMs] = useState<number>(Date.now());

    useEffect(() => {
        if (!socket) return;

        const handleScannerUpdate = (payload: unknown) => {
            const parsed = parseScanMessage(payload);
            if (!parsed) return;

            const key = rowKeyFor(parsed);
            const score = hasNumber(parsed.score) ? parsed.score : parsed.gap;
            const timestamp = parsed.timestamp || Date.now();
            const cutoff = Date.now() - HISTORY_WINDOW_MS;

            setScansByKey((prev) => ({
                ...prev,
                [key]: parsed,
            }));

            setHistoryByKey((prev) => {
                const existing = prev[key] || [];
                const next = [...existing, { timestamp, score }]
                    .filter((point) => point.timestamp >= cutoff)
                    .slice(-HISTORY_MAX_POINTS);
                return {
                    ...prev,
                    [key]: next,
                };
            });

            setLastUpdateMs(Date.now());
        };

        socket.on('scanner_update', handleScannerUpdate);
        return () => {
            socket.off('scanner_update', handleScannerUpdate);
        };
    }, [socket]);

    const rows = useMemo(() => {
        return Object.entries(scansByKey)
            .map(([key, scan]) => buildRow(key, scan, historyByKey[key] || []))
            .sort((a, b) => {
                const orderDelta = (ROW_ORDER[a.kind] ?? 99) - (ROW_ORDER[b.kind] ?? 99);
                if (orderDelta !== 0) {
                    return orderDelta;
                }
                return a.key.localeCompare(b.key);
            })
            .slice(0, MAX_ROWS);
    }, [historyByKey, scansByKey]);

    return (
        <div className="bg-black/40 backdrop-blur-md border border-white/10 rounded-xl p-6 h-full flex flex-col">
            <div className="flex justify-between items-start mb-4">
                <h2 className="text-xl font-bold text-white flex items-center gap-2">
                    <span className={`w-2 h-2 rounded-full ${rows.length > 0 ? 'bg-emerald-500 shadow-[0_0_10px_rgba(16,185,129,0.5)]' : 'bg-amber-500'} transition-colors duration-300`} />
                    Arbitrage Scanner
                </h2>
                <div className="text-[10px] font-mono text-gray-500">
                    <span className="animate-pulse text-emerald-500/50">●</span> {new Date(lastUpdateMs).toLocaleTimeString()}
                </div>
            </div>

            <div className="flex-1 overflow-y-auto space-y-2 custom-scrollbar">
                <div className="grid grid-cols-5 text-[10px] text-gray-600 border-b border-white/5 pb-1 mb-1 px-2 font-mono uppercase">
                    <span>Strategy</span>
                    <span>Data</span>
                    <span>Metric</span>
                    <span>Signal (60s)</span>
                    <span>Why</span>
                </div>

                {rows.length === 0 && (
                    <div className="px-2 py-4 text-xs text-gray-500 font-mono">
                        Waiting for scanner feed...
                    </div>
                )}

                {rows.map((row) => (
                    <div
                        key={row.key}
                        className="grid grid-cols-5 text-xs text-gray-300 border-b border-white/5 pb-2 px-2 hover:bg-white/5 transition-colors"
                    >
                        <span className="truncate pr-2 flex items-center gap-2">
                            <span className="font-bold text-white">{row.marketLabel}</span>
                            <span className={`w-1.5 h-1.5 rounded-full ${row.stale ? 'bg-gray-600' : 'bg-emerald-500 animate-pulse'}`} />
                        </span>
                        <span className="text-[10px] font-mono text-gray-300 truncate pr-2">{row.dataText}</span>
                        <ValueCell value={row.metricText} className="text-[10px] font-mono text-cyan-300 truncate pr-2" />
                        <div className="pr-2">
                            <ValueCell
                                value={row.signalText}
                                className={`text-[10px] font-mono font-bold ${signalClass(row.kind, row.signalValue, row.actionable)}`}
                            />
                            <SignalSparkline
                                points={row.history}
                                domain={row.sparklineDomain}
                                className={signalClass(row.kind, row.signalValue, row.actionable)}
                            />
                        </div>
                        <div className="text-[10px] font-mono truncate" title={row.reasonText}>
                            <div className={row.actionable ? 'text-emerald-400 font-bold' : 'text-gray-500 font-bold'}>
                                {row.actionable ? 'PASS' : 'HOLD'}
                            </div>
                            <div className="text-gray-500 truncate">{row.thresholdText}</div>
                            <div className="text-gray-400 truncate">{row.reasonText}</div>
                        </div>
                    </div>
                ))}
            </div>

            <div className="flex justify-between items-center mt-2 px-2">
                <div className="flex items-center gap-1.5 bg-emerald-500/10 px-2 py-0.5 rounded border border-emerald-500/20">
                    <div className="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse shadow-[0_0_8px_rgba(16,185,129,0.8)]" />
                    <span className="text-[9px] font-bold text-emerald-400 tracking-wider">LIVE DATA FEED</span>
                </div>
                <div className="text-[8px] text-gray-700">
                    LAST UPDATE: {new Date(lastUpdateMs).toLocaleTimeString()}
                </div>
            </div>
        </div>
    );
};

const ValueCell = ({ value, className }: { value: string; className?: string }) => {
    const [highlight, setHighlight] = useState(false);
    const previous = useRef(value);

    useEffect(() => {
        if (previous.current !== value) {
            setHighlight(true);
            const timeoutId = window.setTimeout(() => setHighlight(false), 220);
            previous.current = value;
            return () => window.clearTimeout(timeoutId);
        }
        return undefined;
    }, [value]);

    return (
        <span className={`transition-colors duration-200 ${highlight ? 'text-white' : className}`}>
            {value}
        </span>
    );
};

const SignalSparkline = ({
    points,
    className,
    domain,
}: {
    points: SignalPoint[];
    className?: string;
    domain?: { min: number; max: number };
}) => {
    if (points.length < 2) {
        return <div className="h-5 text-[9px] text-gray-600 font-mono">--</div>;
    }

    const width = 72;
    const height = 18;
    const scores = points.map((point) => point.score);
    const minScore = domain?.min ?? Math.min(...scores);
    const maxScore = domain?.max ?? Math.max(...scores);
    const spread = Math.max(1e-9, maxScore - minScore);

    const polylinePoints = points.map((point, index) => {
        const x = (index / (points.length - 1)) * width;
        const clamped = Math.min(maxScore, Math.max(minScore, point.score));
        const normalized = (clamped - minScore) / spread;
        const y = height - normalized * height;
        return `${x.toFixed(2)},${y.toFixed(2)}`;
    }).join(' ');

    return (
        <svg width={width} height={height} viewBox={`0 0 ${width} ${height}`} className={`mt-0.5 ${className || 'text-gray-500'}`}>
            <polyline
                points={polylinePoints}
                fill="none"
                stroke="currentColor"
                strokeWidth="1.5"
                strokeLinecap="round"
                strokeLinejoin="round"
                opacity="0.9"
            />
        </svg>
    );
};
