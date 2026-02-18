export type SimulationLedgerSnapshot = {
    cash: number;
    reserved: number;
    realized_pnl: number;
    equity: number;
    utilization_pct: number;
};

export type LedgerHealthStatus = 'HEALTHY' | 'WARN' | 'CRITICAL';

export type LedgerConcentrationEntry = {
    id: string;
    reserved: number;
    share_pct: number;
};

export type LedgerHealthState = {
    status: LedgerHealthStatus;
    issues: string[];
    checked_at: number;
    ledger: SimulationLedgerSnapshot;
    caps: {
        strategy_pct: number;
        family_pct: number;
        underlying_pct: number;
        utilization_pct: number;
    };
    strategy_reserved_total: number;
    family_reserved_total: number;
    underlying_reserved_total: number;
    reserved_gap_vs_strategy: number;
    reserved_gap_vs_family: number;
    reserved_gap_vs_underlying: number;
    top_strategy_exposure: LedgerConcentrationEntry[];
    top_family_exposure: LedgerConcentrationEntry[];
    top_underlying_exposure: LedgerConcentrationEntry[];
};

interface RedisLike {
    get(key: string): Promise<string | null>;
    set(key: string, value: string): Promise<unknown>;
    del(key: string[] | string): Promise<unknown>;
    scanIterator(options: { MATCH: string; COUNT: number }): AsyncIterable<string>;
}

interface LedgerConfig {
    defaultSimBankroll: number;
    simBankrollKey: string;
    simLedgerCashKey: string;
    simLedgerReservedKey: string;
    simLedgerRealizedPnlKey: string;
    reservedByStrategyPrefix: string;
    reservedByFamilyPrefix: string;
    reservedByUnderlyingPrefix: string;
    strategyConcentrationCapPct: number;
    familyConcentrationCapPct: number;
    underlyingConcentrationCapPct: number;
    globalUtilizationCapPct: number;
}

interface LedgerHealthModuleDeps {
    redis: RedisLike;
    config: LedgerConfig;
    emitLedgerHealthUpdate: (payload: LedgerHealthState) => void;
    touchLedgerRuntime: (health: 'ONLINE' | 'DEGRADED', detail: string) => void;
    recordError: (scope: string, error: unknown, context?: Record<string, unknown>) => void;
}

function asNumber(input: unknown): number | null {
    if (input === null || input === undefined || input === '') {
        return null;
    }
    const parsed = Number(input);
    return Number.isFinite(parsed) ? parsed : null;
}

function rankConcentration(entries: Record<string, number>, equity: number, limit = 8): LedgerConcentrationEntry[] {
    const denominator = equity > 0 ? equity : 1;
    return Object.entries(entries)
        .map(([id, reserved]) => ({
            id,
            reserved,
            share_pct: (reserved / denominator) * 100,
        }))
        .sort((a, b) => b.reserved - a.reserved)
        .slice(0, limit)
        .map((entry) => ({
            ...entry,
            reserved: Math.round(entry.reserved * 100) / 100,
            share_pct: Math.round(entry.share_pct * 10) / 10,
        }));
}

export function createLedgerHealthModule(deps: LedgerHealthModuleDeps) {
    let ledgerHealthState: LedgerHealthState = {
        status: 'HEALTHY',
        issues: [],
        checked_at: 0,
        ledger: {
            cash: deps.config.defaultSimBankroll,
            reserved: 0,
            realized_pnl: 0,
            equity: deps.config.defaultSimBankroll,
            utilization_pct: 0,
        },
        caps: {
            strategy_pct: deps.config.strategyConcentrationCapPct * 100,
            family_pct: deps.config.familyConcentrationCapPct * 100,
            underlying_pct: deps.config.underlyingConcentrationCapPct * 100,
            utilization_pct: deps.config.globalUtilizationCapPct * 100,
        },
        strategy_reserved_total: 0,
        family_reserved_total: 0,
        underlying_reserved_total: 0,
        reserved_gap_vs_strategy: 0,
        reserved_gap_vs_family: 0,
        reserved_gap_vs_underlying: 0,
        top_strategy_exposure: [],
        top_family_exposure: [],
        top_underlying_exposure: [],
    };

    async function getSimulationLedgerSnapshot(): Promise<SimulationLedgerSnapshot> {
        const fallbackBankroll = asNumber(await deps.redis.get(deps.config.simBankrollKey)) ?? deps.config.defaultSimBankroll;
        const cashRaw = asNumber(await deps.redis.get(deps.config.simLedgerCashKey));
        const reservedRaw = asNumber(await deps.redis.get(deps.config.simLedgerReservedKey));
        const realizedRaw = asNumber(await deps.redis.get(deps.config.simLedgerRealizedPnlKey));

        const cash = cashRaw ?? fallbackBankroll;
        const reserved = reservedRaw ?? 0;
        const realizedPnl = realizedRaw ?? 0;
        const equity = cash + reserved;
        const utilizationPct = equity > 0 ? (reserved / equity) * 100 : 0;

        if (cashRaw === null) {
            await deps.redis.set(deps.config.simLedgerCashKey, cash.toFixed(8));
        }
        if (reservedRaw === null) {
            await deps.redis.set(deps.config.simLedgerReservedKey, reserved.toFixed(8));
        }
        if (realizedRaw === null) {
            await deps.redis.set(deps.config.simLedgerRealizedPnlKey, realizedPnl.toFixed(8));
        }
        await deps.redis.set(deps.config.simBankrollKey, equity.toFixed(8));

        return {
            cash,
            reserved,
            realized_pnl: realizedPnl,
            equity,
            utilization_pct: utilizationPct,
        };
    }

    async function reconcileSimulationLedgerWithStrategyMetrics(
        strategyMetrics: Record<string, { pnl?: number }>,
    ): Promise<void> {
        const ledger = await getSimulationLedgerSnapshot();
        const cumulativePnl = Object.values(strategyMetrics)
            .reduce((sum, entry) => sum + (asNumber(entry?.pnl) ?? 0), 0);

        const ledgerLooksFresh = Math.abs(ledger.realized_pnl) < 1e-6
            && Math.abs(ledger.reserved) < 1e-6
            && Math.abs(ledger.cash - deps.config.defaultSimBankroll) < 1e-6;
        if (!ledgerLooksFresh || Math.abs(cumulativePnl) < 0.01) {
            return;
        }

        const reconciledCash = Math.max(0, deps.config.defaultSimBankroll + cumulativePnl);
        const reconciledEquity = reconciledCash + ledger.reserved;
        await deps.redis.set(deps.config.simLedgerCashKey, reconciledCash.toFixed(8));
        await deps.redis.set(deps.config.simLedgerRealizedPnlKey, cumulativePnl.toFixed(8));
        await deps.redis.set(deps.config.simBankrollKey, reconciledEquity.toFixed(8));
    }

    async function scanReservedMap(prefix: string): Promise<Record<string, number>> {
        const pattern = `${prefix}*`;
        const entries: Record<string, number> = {};
        try {
            for await (const key of deps.redis.scanIterator({ MATCH: pattern, COUNT: 200 })) {
                if (typeof key !== 'string' || !key.startsWith(prefix)) {
                    continue;
                }
                const id = key.slice(prefix.length).trim().toUpperCase();
                if (!id) {
                    continue;
                }
                const value = asNumber(await deps.redis.get(key)) ?? 0;
                if (!Number.isFinite(value) || Math.abs(value) < 1e-9) {
                    continue;
                }
                entries[id] = Math.max(0, value);
            }
        } catch (error) {
            deps.recordError('Ledger.ScanReservedMap', error, { pattern });
        }
        return entries;
    }

    async function clearReservedMaps(): Promise<void> {
        const patterns = [
            `${deps.config.reservedByStrategyPrefix}*`,
            `${deps.config.reservedByFamilyPrefix}*`,
            `${deps.config.reservedByUnderlyingPrefix}*`,
        ];
        for (const pattern of patterns) {
            const deleteBatch: string[] = [];
            try {
                for await (const key of deps.redis.scanIterator({ MATCH: pattern, COUNT: 200 })) {
                    if (typeof key === 'string') {
                        deleteBatch.push(key);
                    }
                    if (deleteBatch.length >= 100) {
                        await deps.redis.del(deleteBatch);
                        deleteBatch.length = 0;
                    }
                }
                if (deleteBatch.length > 0) {
                    await deps.redis.del(deleteBatch);
                }
            } catch (error) {
                deps.recordError('Ledger.ClearReservedMap', error, { pattern });
            }
        }
    }

    async function evaluateLedgerHealth(): Promise<LedgerHealthState> {
        const ledger = await getSimulationLedgerSnapshot();
        const strategyReserved = await scanReservedMap(deps.config.reservedByStrategyPrefix);
        const familyReserved = await scanReservedMap(deps.config.reservedByFamilyPrefix);
        const underlyingReserved = await scanReservedMap(deps.config.reservedByUnderlyingPrefix);
        const strategyTotal = Object.values(strategyReserved).reduce((sum, value) => sum + value, 0);
        const familyTotal = Object.values(familyReserved).reduce((sum, value) => sum + value, 0);
        const underlyingTotal = Object.values(underlyingReserved).reduce((sum, value) => sum + value, 0);
        const gapVsStrategy = Math.abs(ledger.reserved - strategyTotal);
        const gapVsFamily = Math.abs(ledger.reserved - familyTotal);
        const gapVsUnderlying = Math.abs(ledger.reserved - underlyingTotal);
        const issues: string[] = [];
        const equity = Math.max(0, ledger.equity);

        if (ledger.cash < -0.01) {
            issues.push(`cash is negative (${ledger.cash.toFixed(2)})`);
        }
        if (ledger.reserved < -0.01) {
            issues.push(`reserved is negative (${ledger.reserved.toFixed(2)})`);
        }
        if (equity < 0.01 && Math.abs(ledger.realized_pnl) > 0.01) {
            issues.push('equity nearly zero with non-trivial realized pnl');
        }

        const warnGapAbs = Math.max(1, equity * 0.005);
        const criticalGapAbs = Math.max(5, equity * 0.02);
        if (gapVsStrategy > criticalGapAbs) {
            issues.push(`strategy reserved mismatch ${gapVsStrategy.toFixed(2)} exceeds critical ${criticalGapAbs.toFixed(2)}`);
        } else if (gapVsStrategy > warnGapAbs) {
            issues.push(`strategy reserved mismatch ${gapVsStrategy.toFixed(2)} exceeds warn ${warnGapAbs.toFixed(2)}`);
        }
        if (gapVsFamily > criticalGapAbs) {
            issues.push(`family reserved mismatch ${gapVsFamily.toFixed(2)} exceeds critical ${criticalGapAbs.toFixed(2)}`);
        } else if (gapVsFamily > warnGapAbs) {
            issues.push(`family reserved mismatch ${gapVsFamily.toFixed(2)} exceeds warn ${warnGapAbs.toFixed(2)}`);
        }
        if (gapVsUnderlying > criticalGapAbs) {
            issues.push(`underlying reserved mismatch ${gapVsUnderlying.toFixed(2)} exceeds critical ${criticalGapAbs.toFixed(2)}`);
        } else if (gapVsUnderlying > warnGapAbs) {
            issues.push(`underlying reserved mismatch ${gapVsUnderlying.toFixed(2)} exceeds warn ${warnGapAbs.toFixed(2)}`);
        }

        const strategyCapPct = deps.config.strategyConcentrationCapPct * 100;
        const familyCapPct = deps.config.familyConcentrationCapPct * 100;
        const underlyingCapPct = deps.config.underlyingConcentrationCapPct * 100;
        const utilizationCapPct = deps.config.globalUtilizationCapPct * 100;
        const topStrategies = rankConcentration(strategyReserved, equity);
        const topFamilies = rankConcentration(familyReserved, equity);
        const topUnderlyings = rankConcentration(underlyingReserved, equity);
        const maxStrategyShare = topStrategies[0]?.share_pct || 0;
        const maxFamilyShare = topFamilies[0]?.share_pct || 0;
        const maxUnderlyingShare = topUnderlyings[0]?.share_pct || 0;
        if (maxStrategyShare > strategyCapPct + 0.5) {
            issues.push(`strategy concentration ${maxStrategyShare.toFixed(1)}% above cap ${strategyCapPct.toFixed(1)}%`);
        }
        if (maxFamilyShare > familyCapPct + 0.5) {
            issues.push(`family concentration ${maxFamilyShare.toFixed(1)}% above cap ${familyCapPct.toFixed(1)}%`);
        }
        if (maxUnderlyingShare > underlyingCapPct + 0.5) {
            issues.push(`underlying concentration ${maxUnderlyingShare.toFixed(1)}% above cap ${underlyingCapPct.toFixed(1)}%`);
        }
        if (ledger.utilization_pct > utilizationCapPct + 0.5) {
            issues.push(`utilization ${ledger.utilization_pct.toFixed(1)}% above cap ${utilizationCapPct.toFixed(1)}%`);
        }

        const hasCritical = issues.some((issue) => issue.includes('critical') || issue.includes('negative'));
        const status: LedgerHealthStatus = hasCritical
            ? 'CRITICAL'
            : issues.length > 0
                ? 'WARN'
                : 'HEALTHY';

        return {
            status,
            issues: issues.slice(0, 12),
            checked_at: Date.now(),
            ledger: {
                cash: Math.round(ledger.cash * 100) / 100,
                reserved: Math.round(ledger.reserved * 100) / 100,
                realized_pnl: Math.round(ledger.realized_pnl * 100) / 100,
                equity: Math.round(ledger.equity * 100) / 100,
                utilization_pct: Math.round(ledger.utilization_pct * 10) / 10,
            },
            caps: {
                strategy_pct: Math.round(strategyCapPct * 10) / 10,
                family_pct: Math.round(familyCapPct * 10) / 10,
                underlying_pct: Math.round(underlyingCapPct * 10) / 10,
                utilization_pct: Math.round(utilizationCapPct * 10) / 10,
            },
            strategy_reserved_total: Math.round(strategyTotal * 100) / 100,
            family_reserved_total: Math.round(familyTotal * 100) / 100,
            underlying_reserved_total: Math.round(underlyingTotal * 100) / 100,
            reserved_gap_vs_strategy: Math.round(gapVsStrategy * 100) / 100,
            reserved_gap_vs_family: Math.round(gapVsFamily * 100) / 100,
            reserved_gap_vs_underlying: Math.round(gapVsUnderlying * 100) / 100,
            top_strategy_exposure: topStrategies,
            top_family_exposure: topFamilies,
            top_underlying_exposure: topUnderlyings,
        };
    }

    async function refreshLedgerHealth(): Promise<void> {
        ledgerHealthState = await evaluateLedgerHealth();
        deps.emitLedgerHealthUpdate(ledgerHealthState);
        deps.touchLedgerRuntime(
            ledgerHealthState.status === 'CRITICAL'
                ? 'DEGRADED'
                : 'ONLINE',
            ledgerHealthState.status === 'HEALTHY'
                ? 'ledger healthy'
                : `ledger ${ledgerHealthState.status.toLowerCase()}: ${ledgerHealthState.issues[0] || 'issue detected'}`,
        );
    }

    function getLedgerHealthState(): LedgerHealthState {
        return ledgerHealthState;
    }

    return {
        getSimulationLedgerSnapshot,
        reconcileSimulationLedgerWithStrategyMetrics,
        clearReservedMaps,
        refreshLedgerHealth,
        getLedgerHealthState,
    };
}
