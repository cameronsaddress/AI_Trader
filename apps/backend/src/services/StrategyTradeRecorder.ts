import fs from 'fs/promises';
import path from 'path';
import { logger } from '../utils/logger';

type StrategyTradeRecord = {
    timestamp: number;
    strategy: string;
    variant: string;
    pnl: number;
    notional?: number;
    mode?: string;
    reason?: string;
};

type Row = Record<string, unknown>;

const CSV_HEADER = 'timestamp,strategy,variant,pnl,notional,mode,reason\n';

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
    const notional = parseNumber(row.notional)
        ?? parseNumber(details?.notional)
        ?? parseNumber(details?.size)
        ?? undefined;
    const mode = parseString(row.mode) || undefined;
    const reason = parseString(row.reason) || parseString(details?.reason) || undefined;

    return {
        timestamp: ts,
        strategy,
        variant,
        pnl,
        notional,
        mode,
        reason,
    };
}

function recordToCsv(record: StrategyTradeRecord): string {
    const cells = [
        String(Math.round(record.timestamp)),
        csvEscape(record.strategy),
        csvEscape(record.variant),
        record.pnl.toFixed(8),
        record.notional !== undefined ? record.notional.toFixed(8) : '',
        csvEscape(record.mode || ''),
        csvEscape(record.reason || ''),
    ];
    return `${cells.join(',')}\n`;
}

export class StrategyTradeRecorder {
    private readonly filePath: string;
    private initialized = false;
    private writeQueue: Promise<void> = Promise.resolve();

    constructor(filePath?: string) {
        const configured = (filePath || process.env.STRATEGY_TRADE_LOG_PATH || '').trim();
        const relativeDefault = path.join('logs', 'strategy_trades.csv');
        this.filePath = path.isAbsolute(configured)
            ? configured
            : path.resolve(process.cwd(), configured || relativeDefault);
    }

    public async init(): Promise<void> {
        if (this.initialized) {
            return;
        }

        const dir = path.dirname(this.filePath);
        await fs.mkdir(dir, { recursive: true });

        try {
            await fs.access(this.filePath);
        } catch {
            await fs.writeFile(this.filePath, CSV_HEADER, { encoding: 'utf8' });
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
            await fs.appendFile(this.filePath, recordToCsv(parsed), { encoding: 'utf8' });
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
            await fs.writeFile(this.filePath, CSV_HEADER, { encoding: 'utf8' });
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
