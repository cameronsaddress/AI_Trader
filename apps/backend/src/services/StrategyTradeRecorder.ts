import fs from 'fs/promises';
import path from 'path';
import { logger } from '../utils/logger';

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
};

type Row = Record<string, unknown>;

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

async function migrateCsvToHeaderV3(filePath: string): Promise<void> {
    const content = await fs.readFile(filePath, 'utf8');
    const lines = content.split('\n');
    const trimmedLines = lines.map((line) => line.trim()).filter((line) => line.length > 0);
    if (trimmedLines.length === 0) {
        await fs.writeFile(filePath, CSV_HEADER_V3, { encoding: 'utf8' });
        return;
    }

    const headerLine = trimmedLines[0] as string;
    const headerCols = parseCsvLine(headerLine).map((cell) => cell.trim().replace(/^"|"$/g, '').toLowerCase());
    const hasSide = headerCols.includes('side');
    const hasEntry = headerCols.includes('entry_price');
    const hasExecution = headerCols.includes('execution_id');
    const needs = !(hasSide && hasEntry && hasExecution);
    if (!needs) {
        return;
    }

    const targetCols = parseCsvLine(CSV_HEADER_V3.trim()).length;
    const updated: string[] = [CSV_HEADER_V3.trim()];
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
    const reason = parseString(row.reason) || parseString(details?.reason) || undefined;
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
    };
}

function recordToCsv(record: StrategyTradeRecord, config: { extended: boolean; includeExecutionId: boolean; includeSide: boolean; includeEntryPrice: boolean }): string {
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
    private extendedCsv = true;
    private includeExecutionId = true;
    private includeSide = true;
    private includeEntryPrice = true;

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
            await migrateCsvToHeaderV3(this.filePath);
            const content = await fs.readFile(this.filePath, 'utf8');
            const firstLine = content.split('\n', 1)[0]?.trim() || '';
            if (firstLine === LEGACY_CSV_HEADER) {
                this.extendedCsv = false;
                this.includeExecutionId = false;
                this.includeSide = false;
                this.includeEntryPrice = false;
            } else if (firstLine.length > 0) {
                const columns = firstLine.split(',').map((col) => col.trim());
                this.extendedCsv = columns.includes('gross_return') && columns.includes('cost_drag_return');
                this.includeExecutionId = columns.includes('execution_id');
                this.includeSide = columns.includes('side');
                this.includeEntryPrice = columns.includes('entry_price');
            }
        } catch {
            await fs.writeFile(this.filePath, CSV_HEADER_V3, { encoding: 'utf8' });
            this.extendedCsv = true;
            this.includeExecutionId = true;
            this.includeSide = true;
            this.includeEntryPrice = true;
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
            }), { encoding: 'utf8' });
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
            await fs.writeFile(this.filePath, CSV_HEADER_V3, { encoding: 'utf8' });
            this.extendedCsv = true;
            this.includeExecutionId = true;
            this.includeSide = true;
            this.includeEntryPrice = true;
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

        const content = await fs.readFile(this.filePath, 'utf8');
        const lines = content.split('\n').filter((line) => line.trim().length > 0);
        return Math.max(0, lines.length - 1);
    }
}
