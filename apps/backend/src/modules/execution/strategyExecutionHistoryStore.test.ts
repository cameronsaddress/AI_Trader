import {
    clearExecutionHistory,
    persistExecutionHistoryEntry,
    restoreExecutionHistory,
    strategyExecutionHistoryRedisKey,
    strategyIdFromExecutionHistoryRedisKey,
    type StrategyExecutionHistoryRedisLike,
    type StrategyExecutionHistoryRedisMulti,
    type StrategyExecutionHistoryStoreConfig,
} from './strategyExecutionHistoryStore';

class MockRedis implements StrategyExecutionHistoryRedisLike {
    public isOpen = true;
    public readonly store = new Map<string, string[]>();
    public failScan = false;
    public failDel = false;

    multi(): StrategyExecutionHistoryRedisMulti {
        const ops: Array<() => void> = [];
        return {
            rPush: (key: string, value: string) => {
                ops.push(() => {
                    const existing = this.store.get(key) || [];
                    existing.push(value);
                    this.store.set(key, existing);
                });
                return this.multiProxy(ops);
            },
            lTrim: (key: string, start: number, stop: number) => {
                ops.push(() => {
                    const existing = this.store.get(key) || [];
                    this.store.set(key, this.sliceRange(existing, start, stop));
                });
                return this.multiProxy(ops);
            },
            exec: async () => {
                for (const op of ops) {
                    op();
                }
                return [];
            },
        };
    }

    private multiProxy(ops: Array<() => void>): StrategyExecutionHistoryRedisMulti {
        return {
            rPush: (key: string, value: string) => {
                ops.push(() => {
                    const existing = this.store.get(key) || [];
                    existing.push(value);
                    this.store.set(key, existing);
                });
                return this.multiProxy(ops);
            },
            lTrim: (key: string, start: number, stop: number) => {
                ops.push(() => {
                    const existing = this.store.get(key) || [];
                    this.store.set(key, this.sliceRange(existing, start, stop));
                });
                return this.multiProxy(ops);
            },
            exec: async () => {
                for (const op of ops) {
                    op();
                }
                return [];
            },
        };
    }

    async lRange(key: string, start: number, stop: number): Promise<string[]> {
        const existing = this.store.get(key) || [];
        return this.sliceRange(existing, start, stop);
    }

    async *scanIterator(options: { MATCH: string; COUNT: number }): AsyncIterable<string> {
        if (this.failScan) {
            throw new Error('scan failed');
        }
        const prefix = options.MATCH.endsWith('*') ? options.MATCH.slice(0, -1) : options.MATCH;
        for (const key of this.store.keys()) {
            if (key.startsWith(prefix)) {
                yield key;
            }
        }
    }

    async del(keys: string[]): Promise<unknown> {
        if (this.failDel) {
            throw new Error('del failed');
        }
        let deleted = 0;
        for (const key of keys) {
            if (this.store.delete(key)) {
                deleted += 1;
            }
        }
        return deleted;
    }

    private normalizeIndex(length: number, index: number): number {
        const normalized = index < 0 ? length + index : index;
        if (normalized < 0) {
            return 0;
        }
        if (normalized >= length) {
            return length - 1;
        }
        return normalized;
    }

    private sliceRange(values: string[], start: number, stop: number): string[] {
        if (values.length === 0) {
            return [];
        }
        const from = this.normalizeIndex(values.length, start);
        const to = this.normalizeIndex(values.length, stop);
        if (from > to) {
            return [];
        }
        return values.slice(from, to + 1);
    }
}

describe('strategyExecutionHistoryStore', () => {
    const config: StrategyExecutionHistoryStoreConfig = {
        redisPrefix: 'system:strategy_execution_history:test:',
        limit: 3,
        strategyIds: ['BTC_5M', 'ATOMIC_ARB'],
        now: () => 1_700_000_000_000,
    };

    it('builds and parses redis keys', () => {
        const key = strategyExecutionHistoryRedisKey(config.redisPrefix, 'btc_5m');
        expect(key).toBe('system:strategy_execution_history:test:BTC_5M');
        expect(strategyIdFromExecutionHistoryRedisKey(config.redisPrefix, key)).toBe('BTC_5M');
        expect(strategyIdFromExecutionHistoryRedisKey(config.redisPrefix, 'other:key')).toBeNull();
    });

    it('persists entries with normalization and redis trim', async () => {
        const redis = new MockRedis();
        await persistExecutionHistoryEntry(redis, config, 'btc_5m', { side: 'ENTRY' });
        await persistExecutionHistoryEntry(redis, config, 'btc_5m', { side: 'ENTRY', timestamp: 2 });
        await persistExecutionHistoryEntry(redis, config, 'btc_5m', { side: 'ENTRY', timestamp: 3 });
        await persistExecutionHistoryEntry(redis, config, 'btc_5m', { side: 'ENTRY', timestamp: 4 });
        const restored = await restoreExecutionHistory(redis, config);
        const rows = restored.get('BTC_5M') || [];
        expect(rows).toHaveLength(3);
        expect(rows[0]?.timestamp).toBe(2);
        expect(rows[2]?.timestamp).toBe(4);
        expect(rows.every((row) => row.strategy === 'BTC_5M')).toBe(true);
    });

    it('restores discovered strategies and reports malformed row parse errors', async () => {
        const redis = new MockRedis();
        const discoveredKey = strategyExecutionHistoryRedisKey(config.redisPrefix, 'CUSTOM');
        redis.store.set(discoveredKey, [
            '{"side":"ENTRY","timestamp":10}',
            '{bad-json',
            '[]',
        ]);
        const errors: Array<{ scope: string; context?: Record<string, unknown> }> = [];
        const restored = await restoreExecutionHistory(redis, config, (scope, _error, context) => {
            errors.push({ scope, context });
        });
        const rows = restored.get('CUSTOM') || [];
        expect(rows).toHaveLength(1);
        expect(rows[0]?.strategy).toBe('CUSTOM');
        expect(errors.some((entry) => entry.scope === 'StrategyExecutionHistory.RestoreParse')).toBe(true);
    });

    it('clears configured and discovered keys', async () => {
        const redis = new MockRedis();
        redis.store.set(strategyExecutionHistoryRedisKey(config.redisPrefix, 'BTC_5M'), ['{}']);
        redis.store.set(strategyExecutionHistoryRedisKey(config.redisPrefix, 'FOO'), ['{}']);
        redis.store.set('unrelated:key', ['{}']);
        await clearExecutionHistory(redis, config);
        expect(redis.store.has(strategyExecutionHistoryRedisKey(config.redisPrefix, 'BTC_5M'))).toBe(false);
        expect(redis.store.has(strategyExecutionHistoryRedisKey(config.redisPrefix, 'FOO'))).toBe(false);
        expect(redis.store.has('unrelated:key')).toBe(true);
    });

    it('is a no-op when redis is closed', async () => {
        const redis = new MockRedis();
        redis.isOpen = false;
        await persistExecutionHistoryEntry(redis, config, 'BTC_5M', { side: 'ENTRY' });
        const restored = await restoreExecutionHistory(redis, config);
        expect(restored.size).toBe(0);
        await clearExecutionHistory(redis, config);
    });
});
