import { logger } from '../../utils/logger';

type ParsedRecord = Record<string, unknown>;

export interface RiskGuardState {
    cumPnl: number;
    peakPnl: number;
    consecutiveLosses: number;
    lastLossAt: number;
    lastDirection: string | null;
    consecutiveSameDirLosses: number;
    pausedUntil: number;
    dayStartMs: number;
}

interface RiskGuardConfig {
    trailingStop: number;
    trailingStopPct: number;
    trailingStopPctAbsCap: number;
    consecutiveLossLimit: number;
    consecutiveLossCooldownMs: number;
    postLossCooldownMs: number;
    antiMartingaleAfter: number;
    antiMartingaleFactor: number;
    directionLimit: number;
    profitTaperStart: number;
    profitTaperFloor: number;
    dailyTarget: number;
    dayResetHourUtc: number;
    strategies: Set<string>;
    vaultEnabled: boolean;
    vaultBankrollCeiling: number;
    vaultRedisKey: string;
    simLedgerCashKey: string;
    simBankrollKey: string;
}

interface RedisLike {
    get(key: string): Promise<string | null>;
    set(key: string, value: string): Promise<unknown>;
    del(key: string): Promise<unknown>;
    expire(key: string, seconds: number): Promise<unknown>;
    incrByFloat(key: string, increment: number): Promise<unknown>;
    multi?: () => {
        incrByFloat(key: string, increment: number): unknown;
        exec(): Promise<unknown>;
    };
}

interface RiskGuardModuleDeps {
    redis: RedisLike;
    strategyStatus: Record<string, boolean>;
    emitStrategyStatusUpdate: (status: Record<string, boolean>) => void;
    emitRiskGuardUpdate: (payload: { strategy: string } & RiskGuardState) => void;
    emitVaultUpdate: (payload: { swept?: number; vault: number; ceiling: number }) => void;
    emitRiskGuardPause?: (payload: {
        strategy: string;
        reason: string;
        paused_until: number;
        pause_duration_ms: number;
        timestamp: number;
    }) => void;
    recordError: (scope: string, error: unknown, context?: Record<string, unknown>) => void;
    config: RiskGuardConfig;
}

function asRecord(input: unknown): ParsedRecord | null {
    if (!input || typeof input !== 'object' || Array.isArray(input)) {
        return null;
    }
    return input as ParsedRecord;
}

export function createRiskGuardModule(deps: RiskGuardModuleDeps) {
    const riskGuardState: Record<string, RiskGuardState> = {};
    const resumeTimers: Record<string, ReturnType<typeof setTimeout>> = {};
    const stateKey = (strategyId: string): string => `risk_guard:state:${strategyId}`;
    const cooldownKey = (strategyId: string): string => `risk_guard:cooldown:${strategyId}`;
    const pausedUntilKey = (strategyId: string): string => `risk_guard:paused_until:${strategyId}`;

    function clearResumeTimer(strategyId: string): void {
        const timer = resumeTimers[strategyId];
        if (timer) {
            clearTimeout(timer);
            delete resumeTimers[strategyId];
        }
    }

    async function persistRiskGuardState(strategyId: string, state: RiskGuardState): Promise<void> {
        await deps.redis.set(stateKey(strategyId), JSON.stringify(state));
        await deps.redis.expire(stateKey(strategyId), 72 * 60 * 60);
    }

    async function clearStrategyPause(strategyId: string): Promise<void> {
        clearResumeTimer(strategyId);

        const state = riskGuardState[strategyId];
        if (state && state.pausedUntil !== 0) {
            state.pausedUntil = 0;
            deps.emitRiskGuardUpdate({ strategy: strategyId, ...state });
            await persistRiskGuardState(strategyId, state);
        }

        await deps.redis.del(pausedUntilKey(strategyId));
        await deps.redis.del(cooldownKey(strategyId));
    }

    function computeDayStartMs(): number {
        const now = new Date();
        const boundary = new Date(now);
        boundary.setUTCHours(deps.config.dayResetHourUtc, 0, 0, 0);
        if (now.getTime() < boundary.getTime()) {
            boundary.setUTCDate(boundary.getUTCDate() - 1);
        }
        return boundary.getTime();
    }

    function nextDayBoundaryMs(): number {
        const dayStart = computeDayStartMs();
        return dayStart + 86_400_000;
    }

    function ensureRiskGuardState(strategyId: string): RiskGuardState {
        const dayStart = computeDayStartMs();
        if (!riskGuardState[strategyId] || riskGuardState[strategyId].dayStartMs < dayStart) {
            riskGuardState[strategyId] = {
                cumPnl: 0,
                peakPnl: 0,
                consecutiveLosses: 0,
                lastLossAt: 0,
                lastDirection: null,
                consecutiveSameDirLosses: 0,
                pausedUntil: 0,
                dayStartMs: dayStart,
            };
        }
        return riskGuardState[strategyId];
    }

    function extractRiskGuardDirection(parsed: ParsedRecord): string | null {
        const topLevel = typeof parsed?.side === 'string' ? parsed.side : null;
        if (topLevel && topLevel.trim().length > 0) {
            return topLevel;
        }
        const details = asRecord(parsed?.details);
        if (!details) {
            return null;
        }
        const nested = typeof details.side === 'string'
            ? details.side
            : (typeof details.position_side === 'string' ? details.position_side : null);
        return nested && nested.trim().length > 0 ? nested : null;
    }

    async function processRiskGuard(strategyId: string, pnl: number, parsed: ParsedRecord): Promise<void> {
        if (!deps.config.strategies.has(strategyId)) {
            return;
        }

        const state = ensureRiskGuardState(strategyId);
        const now = Date.now();
        const direction = extractRiskGuardDirection(parsed);
        const isLoss = pnl < 0;

        state.cumPnl += pnl;
        if (state.cumPnl > state.peakPnl) {
            state.peakPnl = state.cumPnl;
        }

        if (isLoss) {
            state.consecutiveLosses += 1;
            state.lastLossAt = now;
            if (direction && direction === state.lastDirection) {
                state.consecutiveSameDirLosses += 1;
            } else {
                state.consecutiveSameDirLosses = 1;
            }
        } else {
            state.consecutiveLosses = 0;
            state.consecutiveSameDirLosses = 0;
        }
        state.lastDirection = direction;

        let sizeFactor = 1.0;
        if (state.consecutiveLosses >= deps.config.antiMartingaleAfter) {
            const reductions = state.consecutiveLosses - deps.config.antiMartingaleAfter + 1;
            sizeFactor = Math.min(
                sizeFactor,
                Math.max(0.1, Math.pow(deps.config.antiMartingaleFactor, reductions)),
            );
        }

        if (deps.config.profitTaperStart > 0 && state.cumPnl > deps.config.profitTaperStart) {
            const excess = state.cumPnl - deps.config.profitTaperStart;
            const taperFactor = Math.max(
                deps.config.profitTaperFloor,
                1.0 - (excess / (deps.config.profitTaperStart * 2)),
            );
            sizeFactor = Math.min(sizeFactor, taperFactor);
        }

        if (sizeFactor < 1.0) {
            await deps.redis.set(`risk_guard:size_factor:${strategyId}`, String(sizeFactor));
            await deps.redis.expire(`risk_guard:size_factor:${strategyId}`, 7200);
        } else {
            await deps.redis.del(`risk_guard:size_factor:${strategyId}`);
        }

        if (isLoss && deps.config.postLossCooldownMs > 0) {
            const cooldownUntil = now + deps.config.postLossCooldownMs;
            await deps.redis.set(cooldownKey(strategyId), String(cooldownUntil));
            await deps.redis.expire(
                cooldownKey(strategyId),
                Math.ceil(deps.config.postLossCooldownMs / 1000) + 60,
            );
        }

        let shouldPause = false;
        let pauseReason = '';
        let pauseDurationMs = 0;

        if (deps.config.trailingStop > 0) {
            const drawdown = state.peakPnl - state.cumPnl;
            if (drawdown >= deps.config.trailingStop) {
                shouldPause = true;
                pauseReason = `trailing_stop: drawdown $${drawdown.toFixed(2)} >= $${deps.config.trailingStop}`;
                pauseDurationMs = nextDayBoundaryMs() - now;
            }
        }

        if (!shouldPause && deps.config.trailingStopPct > 0) {
            const drawdown = state.peakPnl - state.cumPnl;
            const bankrollRaw = await deps.redis.get(deps.config.simBankrollKey);
            let bankroll = Number(bankrollRaw);
            if (!Number.isFinite(bankroll) || bankroll <= 0) {
                const cashRaw = await deps.redis.get(deps.config.simLedgerCashKey);
                bankroll = Number(cashRaw);
            }
            if (Number.isFinite(bankroll) && bankroll > 0) {
                let pctThreshold = bankroll * deps.config.trailingStopPct;
                if (deps.config.trailingStopPctAbsCap > 0) {
                    pctThreshold = Math.min(pctThreshold, deps.config.trailingStopPctAbsCap);
                }
                if (drawdown >= pctThreshold) {
                    shouldPause = true;
                    pauseReason = `trailing_stop_pct: drawdown $${drawdown.toFixed(2)} >= ${(deps.config.trailingStopPct * 100).toFixed(1)}% equity ($${pctThreshold.toFixed(2)})`;
                    pauseDurationMs = nextDayBoundaryMs() - now;
                }
            }
        }

        if (!shouldPause && deps.config.dailyTarget > 0 && state.cumPnl >= deps.config.dailyTarget) {
            shouldPause = true;
            pauseReason = `daily_target: cumPnl $${state.cumPnl.toFixed(2)} >= target $${deps.config.dailyTarget}`;
            pauseDurationMs = nextDayBoundaryMs() - now;
        }

        if (!shouldPause && state.consecutiveLosses >= deps.config.consecutiveLossLimit) {
            shouldPause = true;
            pauseReason = `consecutive_losses: ${state.consecutiveLosses} >= ${deps.config.consecutiveLossLimit}`;
            pauseDurationMs = deps.config.consecutiveLossCooldownMs;
        }

        if (!shouldPause && state.consecutiveSameDirLosses >= deps.config.directionLimit) {
            shouldPause = true;
            pauseReason = `direction_lock: ${state.consecutiveSameDirLosses} same-dir losses (${direction}) >= ${deps.config.directionLimit}`;
            pauseDurationMs = deps.config.postLossCooldownMs;
        }

        if (shouldPause && now < state.pausedUntil) {
            deps.emitRiskGuardUpdate({ strategy: strategyId, ...state });
            await persistRiskGuardState(strategyId, state);
            return;
        }

        if (shouldPause) {
            state.pausedUntil = now + pauseDurationMs;
            await deps.redis.set(`strategy:enabled:${strategyId}`, '0');
            await deps.redis.set(pausedUntilKey(strategyId), String(state.pausedUntil));
            await deps.redis.expire(
                pausedUntilKey(strategyId),
                Math.max(300, Math.ceil(pauseDurationMs / 1000) + 3600),
            );
            deps.strategyStatus[strategyId] = false;
            deps.emitStrategyStatusUpdate(deps.strategyStatus);
            logger.info(`[RiskGuard] paused ${strategyId}: ${pauseReason} | resume at ${new Date(state.pausedUntil).toISOString()}`);
            deps.emitRiskGuardPause?.({
                strategy: strategyId,
                reason: pauseReason,
                paused_until: state.pausedUntil,
                pause_duration_ms: pauseDurationMs,
                timestamp: now,
            });

            if (pauseDurationMs > 0 && pauseDurationMs < 86_400_000) {
                clearResumeTimer(strategyId);
                const expectedPausedUntil = state.pausedUntil;
                resumeTimers[strategyId] = setTimeout(async () => {
                    try {
                        delete resumeTimers[strategyId];
                        const current = riskGuardState[strategyId];
                        if (!current) {
                            return;
                        }
                        if (current.pausedUntil !== expectedPausedUntil) {
                            return;
                        }
                        if (current.pausedUntil <= Date.now()) {
                            await deps.redis.set(`strategy:enabled:${strategyId}`, '1');
                            await deps.redis.del(pausedUntilKey(strategyId));
                            deps.strategyStatus[strategyId] = true;
                            deps.emitStrategyStatusUpdate(deps.strategyStatus);
                            logger.info(`[RiskGuard] resumed ${strategyId} after cooldown`);
                        }
                    } catch (error) {
                        deps.recordError('RiskGuard.Resume', error, { strategy: strategyId });
                    }
                }, pauseDurationMs);
            }
        }

        deps.emitRiskGuardUpdate({ strategy: strategyId, ...state });
        await persistRiskGuardState(strategyId, state);
    }

    async function sweepProfitsToVault(): Promise<void> {
        if (!deps.config.vaultEnabled || deps.config.vaultBankrollCeiling <= 0) {
            return;
        }

        try {
            const cash = parseFloat(await deps.redis.get(deps.config.simLedgerCashKey) || '0');
            if (cash <= deps.config.vaultBankrollCeiling) {
                return;
            }
            const excess = Math.round((cash - deps.config.vaultBankrollCeiling) * 100) / 100;
            if (excess <= 0.01) {
                return;
            }

            if (typeof deps.redis.multi !== 'function') {
                throw new Error('vault sweep requires atomic Redis MULTI/EXEC support');
            }
            const tx = deps.redis.multi();
            tx.incrByFloat(deps.config.simLedgerCashKey, -excess);
            tx.incrByFloat(deps.config.simBankrollKey, -excess);
            tx.incrByFloat(deps.config.vaultRedisKey, excess);
            const txResult = await tx.exec();
            if (txResult === null) {
                throw new Error('vault sweep transaction aborted');
            }

            const vault = parseFloat(await deps.redis.get(deps.config.vaultRedisKey) || '0');
            logger.info(`[Vault] swept $${excess.toFixed(2)} to vault (total locked: $${vault.toFixed(2)})`);
            deps.emitVaultUpdate({
                swept: excess,
                vault: Math.round(vault * 100) / 100,
                ceiling: deps.config.vaultBankrollCeiling,
            });
        } catch (error) {
            deps.recordError('Vault.Sweep', error, {
                vault_key: deps.config.vaultRedisKey,
                ceiling: deps.config.vaultBankrollCeiling,
            });
        }
    }

    async function loadRiskGuardStates(): Promise<void> {
        for (const id of deps.config.strategies) {
            try {
                const raw = await deps.redis.get(`risk_guard:state:${id}`);
                if (!raw) {
                    continue;
                }
                const saved = JSON.parse(raw) as RiskGuardState;
                const dayStart = computeDayStartMs();
                if (saved.dayStartMs >= dayStart) {
                    riskGuardState[id] = saved;
                    logger.info(
                        `[RiskGuard] restored state for ${id}: cumPnl=$${saved.cumPnl.toFixed(2)}, peak=$${saved.peakPnl.toFixed(2)}, consec=${saved.consecutiveLosses}`,
                    );
                }
            } catch (error) {
                deps.recordError('RiskGuard.LoadState', error, { strategy: id });
            }
        }
    }

    async function resetRiskGuardStates(): Promise<void> {
        for (const id of deps.config.strategies) {
            try {
                clearResumeTimer(id);
                delete riskGuardState[id];
                await deps.redis.del(stateKey(id));
                await deps.redis.del(cooldownKey(id));
                await deps.redis.del(`risk_guard:size_factor:${id}`);
                await deps.redis.del(pausedUntilKey(id));
            } catch (error) {
                deps.recordError('RiskGuard.ResetState', error, { strategy: id });
            }
        }
    }

    function getRiskGuardState(strategyId: string): RiskGuardState | null {
        return riskGuardState[strategyId] || null;
    }

    return {
        riskGuardState,
        ensureRiskGuardState,
        processRiskGuard,
        sweepProfitsToVault,
        loadRiskGuardStates,
        resetRiskGuardStates,
        clearStrategyPause,
        getRiskGuardState,
    };
}
