import fs from 'fs/promises';
import path from 'path';
import { logger } from '../utils/logger';

type Row = Record<string, unknown>;

type FeatureRegistryEvent =
    | {
        event_type: 'SCAN';
        event_ts: number;
        row_id: string;
        strategy: string;
        market_key: string;
        scan_ts: number;
        signal_type: string;
        metric_family: string;
        features: Record<string, number>;
    }
    | {
        event_type: 'LABEL';
        event_ts: number;
        row_id: string;
        strategy: string;
        label_ts: number;
        label_net_return: number | null;
        label_pnl: number | null;
        label_source: string | null;
    }
    | {
        event_type: 'RESET';
        event_ts: number;
        reason: string;
    };

const DEFAULT_RELATIVE_PATH = path.join('logs', 'feature_registry_events.jsonl');
const APP_ROOT = path.resolve(__dirname, '..', '..');
const LEGACY_PREFIX_RE = /^apps[\\/]+backend[\\/]+(.+)$/i;

function normalizeConfiguredPath(input: string): string {
    const trimmed = input.trim();
    if (trimmed.length === 0) {
        return DEFAULT_RELATIVE_PATH;
    }

    const legacy = LEGACY_PREFIX_RE.exec(trimmed);
    return legacy?.[1] || trimmed;
}

function resolveRecorderPath(filePath?: string): string {
    const configured = (filePath || process.env.FEATURE_REGISTRY_EVENT_LOG_PATH || '').trim();
    if (configured.length > 0 && path.isAbsolute(configured)) {
        return configured;
    }
    return path.resolve(APP_ROOT, normalizeConfiguredPath(configured));
}

function toObject(input: unknown): Row | null {
    if (!input || typeof input !== 'object' || Array.isArray(input)) {
        return null;
    }
    return input as Row;
}

function toNumber(input: unknown): number | null {
    const parsed = Number(input);
    return Number.isFinite(parsed) ? parsed : null;
}

function toStringValue(input: unknown): string | null {
    if (typeof input !== 'string') {
        return null;
    }
    const trimmed = input.trim();
    return trimmed.length > 0 ? trimmed : null;
}

function sanitizeFeatureMap(input: unknown): Record<string, number> {
    const row = toObject(input);
    if (!row) {
        return {};
    }
    const features: Record<string, number> = {};
    for (const [key, value] of Object.entries(row)) {
        if (!/^[a-zA-Z0-9_]+$/.test(key)) {
            continue;
        }
        const parsed = toNumber(value);
        if (parsed === null) {
            continue;
        }
        features[key] = parsed;
    }
    return features;
}

function parseEvent(payload: unknown): FeatureRegistryEvent | null {
    const row = toObject(payload);
    if (!row) {
        return null;
    }
    const eventType = toStringValue(row.event_type);
    const eventTs = toNumber(row.event_ts) ?? Date.now();

    if (eventType === 'SCAN') {
        const rowId = toStringValue(row.row_id);
        const strategy = toStringValue(row.strategy);
        const marketKey = toStringValue(row.market_key);
        const scanTs = toNumber(row.scan_ts) ?? eventTs;
        if (!rowId || !strategy || !marketKey) {
            return null;
        }
        return {
            event_type: 'SCAN',
            event_ts: eventTs,
            row_id: rowId,
            strategy,
            market_key: marketKey,
            scan_ts: scanTs,
            signal_type: toStringValue(row.signal_type) || 'UNKNOWN',
            metric_family: toStringValue(row.metric_family) || 'UNKNOWN',
            features: sanitizeFeatureMap(row.features),
        };
    }

    if (eventType === 'LABEL') {
        const rowId = toStringValue(row.row_id);
        const strategy = toStringValue(row.strategy);
        if (!rowId || !strategy) {
            return null;
        }
        const netReturn = row.label_net_return === null ? null : toNumber(row.label_net_return);
        const pnl = row.label_pnl === null ? null : toNumber(row.label_pnl);
        return {
            event_type: 'LABEL',
            event_ts: eventTs,
            row_id: rowId,
            strategy,
            label_ts: toNumber(row.label_ts) ?? eventTs,
            label_net_return: netReturn,
            label_pnl: pnl,
            label_source: toStringValue(row.label_source),
        };
    }

    if (eventType === 'RESET') {
        return {
            event_type: 'RESET',
            event_ts: eventTs,
            reason: toStringValue(row.reason) || 'manual_reset',
        };
    }
    return null;
}

const MAX_FILE_SIZE_BYTES = Number(process.env.FEATURE_REGISTRY_MAX_SIZE_MB || '100') * 1024 * 1024;
const ROTATION_CHECK_INTERVAL = 500; // check every N writes

export class FeatureRegistryRecorder {
    private readonly filePath: string;
    private initialized = false;
    private writeQueue: Promise<void> = Promise.resolve();
    private rowCount = 0;
    private writesSinceRotationCheck = 0;

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
        } catch {
            await fs.writeFile(this.filePath, '', { encoding: 'utf8' });
        }
        try {
            const stat = await fs.stat(this.filePath);
            // Estimate row count from file size (~200 bytes per row avg)
            this.rowCount = Math.round(stat.size / 200);
            // If file exceeds max size, rotate immediately on boot
            if (stat.size > MAX_FILE_SIZE_BYTES) {
                await this.rotateFile();
            }
        } catch {
            this.rowCount = 0;
        }
        this.initialized = true;
        logger.info(`[FeatureRegistryRecorder] writing feature events to ${this.filePath} (max ${MAX_FILE_SIZE_BYTES / 1024 / 1024}MB)`);
    }

    public getPath(): string {
        return this.filePath;
    }

    private async rotateFile(): Promise<void> {
        const oldPath = this.filePath + '.old';
        try {
            await fs.unlink(oldPath);
        } catch { /* may not exist */ }
        try {
            await fs.rename(this.filePath, oldPath);
        } catch { /* may not exist */ }
        await fs.writeFile(this.filePath, '', { encoding: 'utf8' });
        this.rowCount = 0;
        this.writesSinceRotationCheck = 0;
        logger.info(`[FeatureRegistryRecorder] rotated log file (old preserved at ${oldPath})`);
    }

    public record(payload: unknown): void {
        const parsed = parseEvent(payload);
        if (!parsed) {
            return;
        }
        this.writeQueue = this.writeQueue.then(async () => {
            if (!this.initialized) {
                await this.init();
            }
            await fs.appendFile(this.filePath, `${JSON.stringify(parsed)}\n`, { encoding: 'utf8' });
            this.rowCount += 1;
            this.writesSinceRotationCheck += 1;
            // Periodic size check for rotation
            if (this.writesSinceRotationCheck >= ROTATION_CHECK_INTERVAL) {
                this.writesSinceRotationCheck = 0;
                try {
                    const stat = await fs.stat(this.filePath);
                    if (stat.size > MAX_FILE_SIZE_BYTES) {
                        await this.rotateFile();
                    }
                } catch { /* best-effort */ }
            }
        }).catch((error) => {
            logger.error(`[FeatureRegistryRecorder] failed to persist event: ${String(error)}`);
        });
    }

    public async reset(reason = 'simulation_reset'): Promise<void> {
        const task = this.writeQueue.then(async () => {
            if (!this.initialized) {
                await this.init();
            }
            await fs.writeFile(this.filePath, '', { encoding: 'utf8' });
            const resetEvent: FeatureRegistryEvent = {
                event_type: 'RESET',
                event_ts: Date.now(),
                reason,
            };
            await fs.appendFile(this.filePath, `${JSON.stringify(resetEvent)}\n`, { encoding: 'utf8' });
            this.rowCount = 1;
        });
        this.writeQueue = task.catch((error) => {
            logger.error(`[FeatureRegistryRecorder] failed to reset event log: ${String(error)}`);
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
