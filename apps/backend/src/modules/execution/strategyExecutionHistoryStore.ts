export interface StrategyExecutionHistoryRedisMulti {
    rPush: (key: string, value: string) => StrategyExecutionHistoryRedisMulti;
    lTrim: (key: string, start: number, stop: number) => StrategyExecutionHistoryRedisMulti;
    exec: () => Promise<unknown>;
}

export interface StrategyExecutionHistoryRedisLike {
    isOpen: boolean;
    multi: () => StrategyExecutionHistoryRedisMulti;
    lRange: (key: string, start: number, stop: number) => Promise<string[]>;
    scanIterator: (options: { MATCH: string; COUNT: number }) => AsyncIterable<string>;
    del: (keys: string[]) => Promise<unknown>;
}

export interface StrategyExecutionHistoryStoreConfig {
    redisPrefix: string;
    limit: number;
    strategyIds: string[];
    now?: () => number;
}

type ErrorScope =
    | 'StrategyExecutionHistory.RestoreScan'
    | 'StrategyExecutionHistory.RestoreLoad'
    | 'StrategyExecutionHistory.RestoreParse'
    | 'StrategyExecutionHistory.ClearScan'
    | 'StrategyExecutionHistory.ClearDelete';

export type ErrorRecorder = (
    scope: ErrorScope,
    error: unknown,
    context?: Record<string, unknown>,
) => void;

function asRecord(input: unknown): Record<string, unknown> | null {
    if (!input || typeof input !== 'object' || Array.isArray(input)) {
        return null;
    }
    return input as Record<string, unknown>;
}

function asNumber(input: unknown): number | null {
    const parsed = Number(input);
    return Number.isFinite(parsed) ? parsed : null;
}

function normalizeStrategyId(strategyId: string): string | null {
    const normalized = strategyId.trim().toUpperCase();
    return normalized.length > 0 ? normalized : null;
}

export function strategyExecutionHistoryRedisKey(redisPrefix: string, strategyId: string): string {
    return `${redisPrefix}${strategyId.trim().toUpperCase()}`;
}

export function strategyIdFromExecutionHistoryRedisKey(redisPrefix: string, key: string): string | null {
    if (!key.startsWith(redisPrefix)) {
        return null;
    }
    const suffix = key.slice(redisPrefix.length).trim().toUpperCase();
    return suffix.length > 0 ? suffix : null;
}

async function discoverExecutionHistoryKeys(
    redis: StrategyExecutionHistoryRedisLike,
    redisPrefix: string,
    strategyIds: string[],
    onError?: ErrorRecorder,
): Promise<Set<string>> {
    const keys = new Set<string>(strategyIds.map((strategyId) => strategyExecutionHistoryRedisKey(redisPrefix, strategyId)));
    try {
        const pattern = `${redisPrefix}*`;
        for await (const redisKey of redis.scanIterator({ MATCH: pattern, COUNT: 200 })) {
            if (typeof redisKey === 'string' && redisKey.startsWith(redisPrefix)) {
                keys.add(redisKey);
            }
        }
    } catch (error) {
        onError?.('StrategyExecutionHistory.RestoreScan', error, { prefix: redisPrefix });
    }
    return keys;
}

export async function persistExecutionHistoryEntry(
    redis: StrategyExecutionHistoryRedisLike,
    config: StrategyExecutionHistoryStoreConfig,
    strategyId: string,
    payload: Record<string, unknown>,
): Promise<void> {
    if (!redis.isOpen) {
        return;
    }
    const strategy = normalizeStrategyId(strategyId);
    if (!strategy) {
        return;
    }
    const now = config.now ? config.now() : Date.now();
    const normalized: Record<string, unknown> = {
        ...payload,
        strategy,
        timestamp: asNumber(payload.timestamp) ?? now,
    };
    const key = strategyExecutionHistoryRedisKey(config.redisPrefix, strategy);
    await redis
        .multi()
        .rPush(key, JSON.stringify(normalized))
        .lTrim(key, -config.limit, -1)
        .exec();
}

export async function restoreExecutionHistory(
    redis: StrategyExecutionHistoryRedisLike,
    config: StrategyExecutionHistoryStoreConfig,
    onError?: ErrorRecorder,
): Promise<Map<string, Record<string, unknown>[]>> {
    const restored = new Map<string, Record<string, unknown>[]>();
    if (!redis.isOpen) {
        return restored;
    }
    const now = config.now ? config.now() : Date.now();
    const keys = await discoverExecutionHistoryKeys(redis, config.redisPrefix, config.strategyIds, onError);
    for (const redisKey of keys) {
        const strategy = strategyIdFromExecutionHistoryRedisKey(config.redisPrefix, redisKey);
        if (!strategy) {
            continue;
        }
        try {
            const rows = await redis.lRange(redisKey, -config.limit, -1);
            if (rows.length === 0) {
                continue;
            }
            const normalizedRows: Record<string, unknown>[] = [];
            for (const row of rows) {
                try {
                    const parsed = JSON.parse(row);
                    const record = asRecord(parsed);
                    if (!record) {
                        continue;
                    }
                    normalizedRows.push({
                        ...record,
                        strategy,
                        timestamp: asNumber(record.timestamp) ?? now,
                    });
                } catch (error) {
                    onError?.('StrategyExecutionHistory.RestoreParse', error, {
                        strategy,
                        preview: row.slice(0, 180),
                    });
                }
            }
            if (normalizedRows.length > 0) {
                restored.set(strategy, normalizedRows);
            }
        } catch (error) {
            onError?.('StrategyExecutionHistory.RestoreLoad', error, {
                key: redisKey,
            });
        }
    }
    return restored;
}

export async function clearExecutionHistory(
    redis: StrategyExecutionHistoryRedisLike,
    config: StrategyExecutionHistoryStoreConfig,
    onError?: ErrorRecorder,
): Promise<void> {
    if (!redis.isOpen) {
        return;
    }
    const keys = new Set<string>(
        config.strategyIds.map((strategyId) => strategyExecutionHistoryRedisKey(config.redisPrefix, strategyId)),
    );
    try {
        const pattern = `${config.redisPrefix}*`;
        for await (const redisKey of redis.scanIterator({ MATCH: pattern, COUNT: 200 })) {
            if (typeof redisKey === 'string' && redisKey.startsWith(config.redisPrefix)) {
                keys.add(redisKey);
            }
        }
    } catch (error) {
        onError?.('StrategyExecutionHistory.ClearScan', error, {
            prefix: config.redisPrefix,
        });
    }
    if (keys.size === 0) {
        return;
    }
    try {
        await redis.del(Array.from(keys));
    } catch (error) {
        onError?.('StrategyExecutionHistory.ClearDelete', error, {
            keys: keys.size,
        });
    }
}
