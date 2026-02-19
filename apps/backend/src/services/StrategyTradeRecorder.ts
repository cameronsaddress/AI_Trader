import fs from 'fs/promises';
import path from 'path';
import { logger } from '../utils/logger';

type ClosureClass = 'SIGNAL' | 'TIMEOUT' | 'FORCED' | 'MANUAL' | 'UNKNOWN';

export function classifyClosureReason(reason: string | undefined): { closure_class: ClosureClass; label_eligible: boolean } {
    if (!reason) {
        return { closure_class: 'UNKNOWN', label_eligible: false };
    }
    const upper = reason.trim().toUpperCase();
    // Signal-driven exits: model produced a definitive TP/SL/edge-decay
    if ([
        'TP', 'SL', 'TAKE_PROFIT', 'STOP_LOSS',
        'SIGNAL_TP', 'SIGNAL_SL', 'EDGE_DECAY',
        'EXPIRY', 'EXPIRY_EXIT',
    ].includes(upper)) {
        return { closure_class: 'SIGNAL', label_eligible: true };
    }
    // Time-based exits: still usable for labelling
    if (['TIMEOUT', 'TIME_EXPIRY', 'MAX_HOLD', 'TIME_EXIT'].includes(upper)) {
        return { closure_class: 'TIMEOUT', label_eligible: true };
    }
    // Forced exits: data quality issues — should NOT be used for ML labels
    if (['STALE_FORCE_CLOSE', 'STALE_TIMEOUT', 'RESET', 'DRAWDOWN_BREACH', 'COOLDOWN'].includes(upper)) {
        return { closure_class: 'FORCED', label_eligible: false };
    }
    // Manual exits
    if (['MANUAL', 'USER'].includes(upper)) {
        return { closure_class: 'MANUAL', label_eligible: false };
    }
    return { closure_class: 'UNKNOWN', label_eligible: false };
}

export function inferTradeClosureReason(
    reason: string | null | undefined,
    details: Record<string, unknown> | null = null,
): string | undefined {
    const direct = parseString(reason);
    if (direct) {
        return direct.trim().toUpperCase();
    }

    const detailReason = parseString(details?.reason);
    if (detailReason) {
        return detailReason.trim().toUpperCase();
    }

    const action = parseString(details?.action);
    if (!action) {
        return undefined;
    }

    const upperAction = action.trim().toUpperCase();
    if (upperAction === 'RESOLVE_WINDOW' || upperAction === 'WINDOW_RESOLVE') {
        return 'EXPIRY';
    }
    if (upperAction === 'SETTLEMENT_TIMEOUT') {
        return 'TIMEOUT';
    }
    if (upperAction === 'EARLY_EXIT') {
        return 'SIGNAL_SL';
    }
    if (upperAction.includes('EXPIRY')) {
        return 'EXPIRY';
    }
    if (upperAction.includes('TIMEOUT')) {
        return 'TIMEOUT';
    }
    if (upperAction.includes('MAX_HOLD')) {
        return 'MAX_HOLD';
    }

    return upperAction;
}

type StrategyTradeRecord = {
    timestamp: number;
    strategy: string;
    variant: string;
    pnl: number;
    execution_id?: string;
    notional?: number;
    mode?: string;
    reason?: string;
    gross_return?: number;
    net_return?: number;
    round_trip_cost_rate?: number;
    cost_drag_return?: number;
    side?: string;
    entry_price?: number;
    closure_class?: ClosureClass;
    label_eligible?: boolean;
};

type Row = Record<string, unknown>;

const CSV_HEADER_V4 = 'timestamp,strategy,variant,pnl,notional,mode,reason,gross_return,net_return,round_trip_cost_rate,cost_drag_return,execution_id,side,entry_price,closure_class,label_eligible\n';
const CSV_HEADER_V3 = 'timestamp,strategy,variant,pnl,notional,mode,reason,gross_return,net_return,round_trip_cost_rate,cost_drag_return,execution_id,side,entry_price\n';
const CSV_HEADER_V2 = 'timestamp,strategy,variant,pnl,notional,mode,reason,gross_return,net_return,round_trip_cost_rate,cost_drag_return,execution_id\n';
const CSV_HEADER_V1 = 'timestamp,strategy,variant,pnl,notional,mode,reason,gross_return,net_return,round_trip_cost_rate,cost_drag_return\n';
const LEGACY_CSV_HEADER = 'timestamp,strategy,variant,pnl,notional,mode,reason';
const DEFAULT_RELATIVE_PATH = path.join('logs', 'strategy_trades.csv');
const APP_ROOT = path.resolve(__dirname, '..', '..');
const LEGACY_PREFIX_RE = /^apps[\\/]+backend[\\/]+(.+)$/i;

function parseNumber(input: unknown): number | null {
    const value = Number(input);
    return Number.isFinite(value) ? value : null;
}

function parseString(input: unknown): string | null {
    if (typeof input !== 'string') {
        return null;
    }
    const trimmed = input.trim();
    return trimmed.length > 0 ? trimmed : null;
}

function csvEscape(value: string): string {
    const escaped = value.replace(/"/g, '""');
    return `"${escaped}"`;
}

function parseCsvLine(line: string): string[] {
    const out: string[] = [];
    let current = '';
    let inQuotes = false;
    for (let i = 0; i < line.length; i += 1) {
        const ch = line[i] as string;
        if (ch === '"') {
            const next = line[i + 1];
            if (inQuotes && next === '"') {
                current += '"';
                i += 1;
                continue;
            }
            inQuotes = !inQuotes;
            continue;
        }
        if (ch === ',' && !inQuotes) {
            out.push(current);
            current = '';
            continue;
        }
        current += ch;
    }
    out.push(current);
    return out;
}

async function migrateCsvToLatestHeader(filePath: string): Promise<void> {
    const content = await fs.readFile(filePath, 'utf8');
    const lines = content.split('\n');
    const trimmedLines = lines.map((line) => line.trim()).filter((line) => line.length > 0);
    if (trimmedLines.length === 0) {
        await fs.writeFile(filePath, CSV_HEADER_V4, { encoding: 'utf8' });
        return;
    }

    const headerLine = trimmedLines[0] as string;
    const headerCols = parseCsvLine(headerLine).map((cell) => cell.trim().replace(/^"|"$/g, '').toLowerCase());
    const hasSide = headerCols.includes('side');
    const hasEntry = headerCols.includes('entry_price');
    const hasExecution = headerCols.includes('execution_id');
    const hasClosureClass = headerCols.includes('closure_class');
    const hasLabelEligible = headerCols.includes('label_eligible');
    const isLatest = hasSide && hasEntry && hasExecution && hasClosureClass && hasLabelEligible;
    if (isLatest) {
        return;
    }

    const targetCols = parseCsvLine(CSV_HEADER_V4.trim()).length;
    const updated: string[] = [CSV_HEADER_V4.trim()];
    for (let i = 1; i < trimmedLines.length; i += 1) {
        const line = trimmedLines[i] as string;
        const cells = parseCsvLine(line);
        const missing = targetCols - cells.length;
        if (missing > 0) {
            updated.push(`${line}${','.repeat(missing)}`);
        } else {
            updated.push(line);
        }
    }

    await fs.writeFile(filePath, `${updated.join('\n')}\n`, { encoding: 'utf8' });
}

function toRecord(payload: unknown): StrategyTradeRecord | null {
    if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
        return null;
    }

    const row = payload as Row;
    const strategy = parseString(row.strategy);
    const pnl = parseNumber(row.pnl);
    if (!strategy || pnl === null) {
        return null;
    }

    const ts = parseNumber(row.timestamp) ?? Date.now();
    const details = (row.details && typeof row.details === 'object' && !Array.isArray(row.details))
        ? row.details as Row
        : null;
    const variant = parseString(row.variant)
        || parseString(details?.variant)
        || 'baseline';
    const executionId = parseString(row.execution_id)
        || parseString(row.executionId)
        || parseString(details?.execution_id)
        || parseString(details?.executionId)
        || undefined;
    const notional = parseNumber(row.notional)
        ?? parseNumber(details?.notional)
        ?? parseNumber(details?.size)
        ?? undefined;
    const mode = parseString(row.mode) || undefined;
    const reason = inferTradeClosureReason(parseString(row.reason), details);
    const grossReturn = parseNumber(details?.gross_return) ?? undefined;
    const netReturn = parseNumber(details?.net_return) ?? (
        notional && notional > 0 ? pnl / notional : undefined
    );
    const roundTripCostRate = parseNumber(details?.round_trip_cost_rate) ?? undefined;
    const costDragReturn = grossReturn !== undefined && netReturn !== undefined
        ? grossReturn - netReturn
        : roundTripCostRate;
    const side = parseString(row.side)
        || parseString(details?.side)
        || parseString(details?.position_side)
        || undefined;
    const entryPrice = parseNumber(row.entry_price)
        ?? parseNumber(details?.entry_price)
        ?? undefined;

    const { closure_class, label_eligible } = classifyClosureReason(reason);

    return {
        timestamp: ts,
        strategy,
        variant,
        pnl,
        execution_id: executionId,
        notional,
        mode,
        reason,
        gross_return: grossReturn,
        net_return: netReturn,
        round_trip_cost_rate: roundTripCostRate,
        cost_drag_return: costDragReturn,
        side,
        entry_price: entryPrice,
        closure_class,
        label_eligible,
    };
}

function recordToCsv(record: StrategyTradeRecord, config: { extended: boolean; includeExecutionId: boolean; includeSide: boolean; includeEntryPrice: boolean; includeClosureTaxonomy: boolean }): string {
    const base = [
        String(Math.round(record.timestamp)),
        csvEscape(record.strategy),
        csvEscape(record.variant),
        record.pnl.toFixed(8),
        record.notional !== undefined ? record.notional.toFixed(8) : '',
        csvEscape(record.mode || ''),
        csvEscape(record.reason || ''),
    ];
    if (!config.extended) {
        return `${base.join(',')}\n`;
    }

    const cells = [
        ...base,
        record.gross_return !== undefined ? record.gross_return.toFixed(8) : '',
        record.net_return !== undefined ? record.net_return.toFixed(8) : '',
        record.round_trip_cost_rate !== undefined ? record.round_trip_cost_rate.toFixed(8) : '',
        record.cost_drag_return !== undefined ? record.cost_drag_return.toFixed(8) : '',
    ];
    if (config.includeExecutionId) {
        cells.push(csvEscape(record.execution_id || ''));
    }
    if (config.includeSide) {
        cells.push(csvEscape(record.side || ''));
    }
    if (config.includeEntryPrice) {
        cells.push(record.entry_price !== undefined ? record.entry_price.toFixed(8) : '');
    }
    if (config.includeClosureTaxonomy) {
        cells.push(csvEscape(record.closure_class || ''));
        cells.push(record.label_eligible !== undefined ? String(record.label_eligible) : '');
    }
    return `${cells.join(',')}\n`;
}

function normalizeConfiguredPath(input: string): string {
    const trimmed = input.trim();
    if (trimmed.length === 0) {
        return DEFAULT_RELATIVE_PATH;
    }

    const legacy = LEGACY_PREFIX_RE.exec(trimmed);
    return legacy?.[1] || trimmed;
}

function resolveRecorderPath(filePath?: string): string {
    const configured = (filePath || process.env.STRATEGY_TRADE_LOG_PATH || '').trim();
    if (configured.length > 0 && path.isAbsolute(configured)) {
        return configured;
    }
    return path.resolve(APP_ROOT, normalizeConfiguredPath(configured));
}

export class StrategyTradeRecorder {
    private readonly filePath: string;
    private initialized = false;
    private writeQueue: Promise<void> = Promise.resolve();
    private rowCount = 0;
    private extendedCsv = true;
    private includeExecutionId = true;
    private includeSide = true;
    private includeEntryPrice = true;
    private includeClosureTaxonomy = true;

    constructor(filePath?: string) {
        this.filePath = resolveRecorderPath(filePath);
    }

    public async init(): Promise<void> {
        if (this.initialized) {
            return;
        }

        const dir = path.dirname(this.filePath);
        await fs.mkdir(dir, { recursive: true });

        try {
            await fs.access(this.filePath);
            await migrateCsvToLatestHeader(this.filePath);
            const content = await fs.readFile(this.filePath, 'utf8');
            const nonEmptyLines = content.split('\n').map((line) => line.trim()).filter((line) => line.length > 0);
            this.rowCount = Math.max(0, nonEmptyLines.length - 1);
            const firstLine = nonEmptyLines[0] || '';
            if (firstLine === LEGACY_CSV_HEADER) {
                this.extendedCsv = false;
                this.includeExecutionId = false;
                this.includeSide = false;
                this.includeEntryPrice = false;
                this.includeClosureTaxonomy = false;
            } else if (firstLine.length > 0) {
                const columns = firstLine.split(',').map((col) => col.trim());
                this.extendedCsv = columns.includes('gross_return') && columns.includes('cost_drag_return');
                this.includeExecutionId = columns.includes('execution_id');
                this.includeSide = columns.includes('side');
                this.includeEntryPrice = columns.includes('entry_price');
                this.includeClosureTaxonomy = columns.includes('closure_class') && columns.includes('label_eligible');
            }
        } catch (error) {
            logger.warn(`[StrategyTradeRecorder] initializing new dataset file path=${this.filePath} error=${String(error)}`);
            await fs.writeFile(this.filePath, CSV_HEADER_V4, { encoding: 'utf8' });
            this.rowCount = 0;
            this.extendedCsv = true;
            this.includeExecutionId = true;
            this.includeSide = true;
            this.includeEntryPrice = true;
            this.includeClosureTaxonomy = true;
        }

        this.initialized = true;
        logger.info(`[StrategyTradeRecorder] writing dataset to ${this.filePath}`);
    }

    public record(payload: unknown): void {
        const parsed = toRecord(payload);
        if (!parsed) {
            return;
        }

        this.writeQueue = this.writeQueue.then(async () => {
            if (!this.initialized) {
                await this.init();
            }
            await fs.appendFile(this.filePath, recordToCsv(parsed, {
                extended: this.extendedCsv,
                includeExecutionId: this.includeExecutionId,
                includeSide: this.includeSide,
                includeEntryPrice: this.includeEntryPrice,
                includeClosureTaxonomy: this.includeClosureTaxonomy,
            }), { encoding: 'utf8' });
            this.rowCount += 1;
        }).catch((error) => {
            logger.error(`[StrategyTradeRecorder] failed to persist trade row: ${String(error)}`);
        });
    }

    public getPath(): string {
        return this.filePath;
    }

    public async reset(): Promise<void> {
        const task = this.writeQueue.then(async () => {
            if (!this.initialized) {
                await this.init();
            }
            await fs.writeFile(this.filePath, CSV_HEADER_V4, { encoding: 'utf8' });
            this.extendedCsv = true;
            this.includeExecutionId = true;
            this.includeSide = true;
            this.includeEntryPrice = true;
            this.includeClosureTaxonomy = true;
            this.rowCount = 0;
        });

        this.writeQueue = task.catch((error) => {
            logger.error(`[StrategyTradeRecorder] failed to reset dataset: ${String(error)}`);
        });

        await task;
    }

    public async countRows(): Promise<number> {
        if (!this.initialized) {
            await this.init();
        }
        await this.writeQueue;
        return this.rowCount;
    }
}
