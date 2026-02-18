import fs from 'fs/promises';
import path from 'path';
import { logger } from '../utils/logger';
import type {
    ExecutionPipelineTradingMode,
    RejectedSignalRecord,
    RejectedSignalStage,
} from '../modules/execution/arbExecutionPipeline';

type Row = Record<string, unknown>;

const DEFAULT_RELATIVE_PATH = path.join('logs', 'rejected_signals.jsonl');
const APP_ROOT = path.resolve(__dirname, '..', '..');
const LEGACY_PREFIX_RE = /^apps[\\/]+backend[\\/]+(.+)$/i;
const REJECTED_STAGES = new Set<RejectedSignalStage>([
    'INTELLIGENCE_GATE',
    'MODEL_GATE',
    'PREFLIGHT',
    'LIVE_EXECUTION',
    'SETTLEMENT',
]);
const MODES = new Set<ExecutionPipelineTradingMode>(['PAPER', 'LIVE']);

function normalizeConfiguredPath(input: string): string {
    const trimmed = input.trim();
    if (trimmed.length === 0) {
        return DEFAULT_RELATIVE_PATH;
    }

    const legacy = LEGACY_PREFIX_RE.exec(trimmed);
    return legacy?.[1] || trimmed;
}

function resolveRecorderPath(filePath?: string): string {
    const configured = (filePath || process.env.REJECTED_SIGNAL_LOG_PATH || '').trim();
    if (configured.length > 0 && path.isAbsolute(configured)) {
        return configured;
    }
    return path.resolve(APP_ROOT, normalizeConfiguredPath(configured));
}

function asRecord(input: unknown): Row | null {
    if (!input || typeof input !== 'object' || Array.isArray(input)) {
        return null;
    }
    return input as Row;
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

function parseRejectedSignalRecord(payload: unknown): RejectedSignalRecord | null {
    const row = asRecord(payload);
    if (!row) {
        return null;
    }
    const stage = asString(row.stage);
    const executionId = asString(row.execution_id);
    const reason = asString(row.reason);
    if (!stage || !executionId || !reason || !REJECTED_STAGES.has(stage as RejectedSignalStage)) {
        return null;
    }
    const modeRaw = asString(row.mode);
    const mode = modeRaw && MODES.has(modeRaw as ExecutionPipelineTradingMode)
        ? modeRaw as ExecutionPipelineTradingMode
        : 'PAPER';
    const timestamp = asNumber(row.timestamp) ?? Date.now();
    const payloadRecord = asRecord(row.payload);

    return {
        stage: stage as RejectedSignalStage,
        execution_id: executionId,
        strategy: asString(row.strategy),
        market_key: asString(row.market_key),
        reason,
        mode,
        timestamp,
        payload: payloadRecord,
    };
}

export class RejectedSignalRecorder {
    private readonly filePath: string;
    private initialized = false;
    private writeQueue: Promise<void> = Promise.resolve();
    private rowCount = 0;

    constructor(filePath?: string) {
        this.filePath = resolveRecorderPath(filePath);
    }

    public async init(): Promise<void> {
        if (this.initialized) {
            return;
        }
        await fs.mkdir(path.dirname(this.filePath), { recursive: true });
        try {
            await fs.access(this.filePath);
            const content = await fs.readFile(this.filePath, 'utf8');
            this.rowCount = content.split('\n').map((line) => line.trim()).filter((line) => line.length > 0).length;
        } catch (error) {
            logger.warn(`[RejectedSignalRecorder] creating missing log file path=${this.filePath} error=${String(error)}`);
            await fs.writeFile(this.filePath, '', { encoding: 'utf8' });
            this.rowCount = 0;
        }
        this.initialized = true;
        logger.info(`[RejectedSignalRecorder] writing rejected-signal events to ${this.filePath}`);
    }

    public getPath(): string {
        return this.filePath;
    }

    public record(payload: unknown): void {
        const parsed = parseRejectedSignalRecord(payload);
        if (!parsed) {
            return;
        }
        this.writeQueue = this.writeQueue.then(async () => {
            if (!this.initialized) {
                await this.init();
            }
            await fs.appendFile(this.filePath, `${JSON.stringify(parsed)}\n`, { encoding: 'utf8' });
            this.rowCount += 1;
        }).catch((error) => {
            logger.error(`[RejectedSignalRecorder] failed to persist rejected signal: ${String(error)}`);
        });
    }

    public async readRecent(limit = 200): Promise<RejectedSignalRecord[]> {
        if (!this.initialized) {
            await this.init();
        }
        await this.writeQueue;
        const safeLimit = Math.max(1, Math.floor(limit));
        let content = '';
        try {
            content = await fs.readFile(this.filePath, 'utf8');
        } catch (error) {
            logger.error(`[RejectedSignalRecorder] failed to read event log: ${String(error)}`);
            return [];
        }

        const lines = content.split('\n').map((line) => line.trim()).filter((line) => line.length > 0);
        const start = Math.max(0, lines.length - safeLimit);
        const records: RejectedSignalRecord[] = [];
        for (let index = start; index < lines.length; index += 1) {
            const line = lines[index];
            if (!line) {
                continue;
            }
            try {
                const parsed = parseRejectedSignalRecord(JSON.parse(line));
                if (parsed) {
                    records.push(parsed);
                }
            } catch {
                // Ignore malformed rows to keep the stream readable.
            }
        }
        return records;
    }

    public async countRows(): Promise<number> {
        if (!this.initialized) {
            await this.init();
        }
        await this.writeQueue;
        return this.rowCount;
    }

    public async reset(): Promise<void> {
        const task = this.writeQueue.then(async () => {
            if (!this.initialized) {
                await this.init();
            }
            await fs.writeFile(this.filePath, '', { encoding: 'utf8' });
            this.rowCount = 0;
        });
        this.writeQueue = task.catch((error) => {
            logger.error(`[RejectedSignalRecorder] failed to reset log: ${String(error)}`);
        });
        await task;
    }
}

