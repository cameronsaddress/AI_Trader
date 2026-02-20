#!/usr/bin/env node

/*
Staged live-fire drill runner.

Stages:
1) LIVE_DRY_RUN throughput/latency/fill SLO gate.
2) Optional constrained LIVE fill-rate gate.
3) Settlement completion-rate gate.

Environment:
- API_URL (default: http://localhost:5114)
- CONTROL_PLANE_TOKEN (required)
- REDIS_URL (default: redis://localhost:6491)
- DRILL_STRATEGY (default: ATOMIC_ARB)
- DRILL_ORDER_PRICE (default: 0.52)
- DRILL_ORDER_NOTIONAL_USD (default: 25)
- DRILL_DRY_RUN_SAMPLES (default: 8)
- DRILL_CONSTRAINED_LIVE_ENABLED (default: false)
- DRILL_CONSTRAINED_LIVE_SAMPLES (default: 5)
- DRILL_SETTLEMENT_SAMPLES (default: 4)
- DRILL_MAX_P95_LATENCY_MS (default: 2500)
- DRILL_MIN_ACK_RATE (default: 0.95)
- DRILL_MIN_DRY_RUN_FILL_RATE (default: 0.90)
- DRILL_MIN_LIVE_FILL_RATE (default: 0.75)
- DRILL_MIN_SETTLEMENT_COMPLETION_RATE (default: 0.95)
- DRILL_EVENT_TIMEOUT_MS (default: 10000)
- DRILL_AUTO_HALT_ON_FAIL (default: true)
*/

const { io } = require('socket.io-client');
const { createClient } = require('redis');

const API_URL = process.env.API_URL || 'http://localhost:5114';
const CONTROL_PLANE_TOKEN = process.env.CONTROL_PLANE_TOKEN || '';
const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6491';
const DRILL_STRATEGY = (process.env.DRILL_STRATEGY || 'ATOMIC_ARB').trim().toUpperCase();
const DRILL_ORDER_PRICE = Number(process.env.DRILL_ORDER_PRICE || '0.52');
const DRILL_ORDER_NOTIONAL_USD = Number(process.env.DRILL_ORDER_NOTIONAL_USD || '25');
const DRILL_DRY_RUN_SAMPLES = Math.max(1, Number(process.env.DRILL_DRY_RUN_SAMPLES || '8'));
const DRILL_CONSTRAINED_LIVE_ENABLED = process.env.DRILL_CONSTRAINED_LIVE_ENABLED === 'true';
const DRILL_CONSTRAINED_LIVE_SAMPLES = Math.max(1, Number(process.env.DRILL_CONSTRAINED_LIVE_SAMPLES || '5'));
const DRILL_SETTLEMENT_SAMPLES = Math.max(1, Number(process.env.DRILL_SETTLEMENT_SAMPLES || '4'));
const DRILL_MAX_P95_LATENCY_MS = Math.max(100, Number(process.env.DRILL_MAX_P95_LATENCY_MS || '2500'));
const DRILL_MIN_ACK_RATE = Math.max(0, Math.min(1, Number(process.env.DRILL_MIN_ACK_RATE || '0.95')));
const DRILL_MIN_DRY_RUN_FILL_RATE = Math.max(0, Math.min(1, Number(process.env.DRILL_MIN_DRY_RUN_FILL_RATE || '0.90')));
const DRILL_MIN_LIVE_FILL_RATE = Math.max(0, Math.min(1, Number(process.env.DRILL_MIN_LIVE_FILL_RATE || '0.75')));
const DRILL_MIN_SETTLEMENT_COMPLETION_RATE = Math.max(
    0,
    Math.min(1, Number(process.env.DRILL_MIN_SETTLEMENT_COMPLETION_RATE || '0.95')),
);
const DRILL_EVENT_TIMEOUT_MS = Math.max(2000, Number(process.env.DRILL_EVENT_TIMEOUT_MS || '10000'));
const DRILL_AUTO_HALT_ON_FAIL = process.env.DRILL_AUTO_HALT_ON_FAIL !== 'false';

if (!CONTROL_PLANE_TOKEN) {
    console.error('CONTROL_PLANE_TOKEN is required');
    process.exit(2);
}

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

function percentile(values, q) {
    if (!values.length) {
        return null;
    }
    const sorted = [...values].sort((a, b) => a - b);
    const idx = Math.min(sorted.length - 1, Math.max(0, Math.ceil(sorted.length * q) - 1));
    return sorted[idx];
}

function effectiveFillRate(result) {
    const total = Number(result?.total || 0);
    if (!Number.isFinite(total) || total <= 0) {
        return 0;
    }
    if (result?.dryRun === true) {
        return result?.ok ? 1 : 0;
    }
    const posted = Number(result?.posted || 0);
    if (!Number.isFinite(posted)) {
        return 0;
    }
    return Math.max(0, Math.min(1, posted / total));
}

async function api(path, options = {}) {
    const { method = 'GET', body, auth = true } = options;
    const headers = { 'content-type': 'application/json' };
    if (auth) {
        headers.authorization = `Bearer ${CONTROL_PLANE_TOKEN}`;
    }

    const response = await fetch(`${API_URL}${path}`, {
        method,
        headers,
        body: body === undefined ? undefined : JSON.stringify(body),
    });

    let json = null;
    try {
        json = await response.json();
    } catch {
        // ignore non-JSON responses
    }

    return {
        ok: response.ok,
        status: response.status,
        json,
    };
}

async function setTradingMode(mode) {
    if (mode === 'LIVE') {
        return api('/api/system/trading-mode', { method: 'POST', body: { mode: 'LIVE', confirmation: 'LIVE' } });
    }
    return api('/api/system/trading-mode', { method: 'POST', body: { mode: 'PAPER' } });
}

function syntheticExecutionPayload(executionId, idx, runLabel) {
    const conditionHex = (idx + 1).toString(16).padStart(64, 'a').slice(0, 64);
    return {
        execution_id: executionId,
        market: `Live Fire Drill ${runLabel}`,
        side: 'LIVE_DRY_RUN',
        price: DRILL_ORDER_PRICE,
        size: DRILL_ORDER_NOTIONAL_USD,
        timestamp: Date.now(),
        mode: 'LIVE_DRY_RUN',
        details: {
            strategy: DRILL_STRATEGY,
            preflight: {
                venue: 'POLYMARKET',
                strategy: DRILL_STRATEGY,
                orders: [
                    {
                        token_id: `${9_000_000 + idx}`,
                        condition_id: `0x${conditionHex}`,
                        side: 'BUY',
                        price: DRILL_ORDER_PRICE,
                        size: DRILL_ORDER_NOTIONAL_USD,
                        size_unit: 'USD_NOTIONAL',
                    },
                ],
            },
        },
    };
}

async function connectSocket() {
    const socket = io(API_URL, {
        transports: ['websocket'],
        auth: { token: CONTROL_PLANE_TOKEN },
        timeout: 7000,
    });

    await new Promise((resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error('socket connect timeout')), 8000);
        socket.on('connect', () => {
            clearTimeout(timeout);
            resolve();
        });
        socket.on('connect_error', (error) => {
            clearTimeout(timeout);
            reject(error);
        });
    });

    return socket;
}

async function runExecutionStage({
    stageName,
    sampleCount,
    minFillRate,
    requireRealLivePosts,
}) {
    const modeSet = await setTradingMode('LIVE');
    if (!modeSet.ok) {
        return {
            stage: stageName,
            passed: false,
            skipped: false,
            details: {
                modeSetStatus: modeSet.status,
                modeSetError: modeSet.json?.error || null,
                readinessFailures: Array.isArray(modeSet.json?.failures) ? modeSet.json.failures : [],
            },
        };
    }

    const modeStatus = await api('/api/system/trading-mode', { auth: true });
    const liveOrderPostingEnabled = Boolean(modeStatus.json?.live_order_posting_enabled);
    if (requireRealLivePosts && !liveOrderPostingEnabled) {
        await setTradingMode('PAPER');
        return {
            stage: stageName,
            passed: false,
            skipped: false,
            details: {
                error: 'LIVE_ORDER_POSTING_ENABLED=false, cannot run constrained LIVE stage',
                live_order_posting_enabled: liveOrderPostingEnabled,
            },
        };
    }

    const socket = await connectSocket();
    const redis = createClient({ url: REDIS_URL });
    await redis.connect();

    const pending = new Map();
    const latencies = [];
    const liveExecutions = [];
    const blockedByGate = [];
    const unmatchedEvents = [];

    const markDone = (executionId, kind, payload) => {
        const row = pending.get(executionId);
        if (!row || row.done) {
            return;
        }
        row.done = true;
        row.kind = kind;
        row.doneAt = Date.now();
        row.payload = payload;
        latencies.push(row.doneAt - row.sentAt);
    };

    socket.on('strategy_live_execution', (payload) => {
        const executionId = payload?.execution_id;
        if (!executionId || !pending.has(executionId)) {
            unmatchedEvents.push({ type: 'strategy_live_execution', execution_id: executionId || null });
            return;
        }
        liveExecutions.push(payload);
        markDone(executionId, 'strategy_live_execution', payload);
    });

    const blockHandler = (eventType) => (payload) => {
        const executionId = payload?.execution_id;
        if (!executionId || !pending.has(executionId)) {
            unmatchedEvents.push({ type: eventType, execution_id: executionId || null });
            return;
        }
        blockedByGate.push({
            type: eventType,
            execution_id: executionId,
            reason: payload?.reason || null,
        });
        markDone(executionId, eventType, payload);
    };

    socket.on('strategy_intelligence_block', blockHandler('strategy_intelligence_block'));
    socket.on('strategy_model_block', blockHandler('strategy_model_block'));

    for (let i = 0; i < sampleCount; i += 1) {
        const executionId = `live-fire-${stageName.toLowerCase()}-${Date.now()}-${i}`;
        pending.set(executionId, { sentAt: Date.now(), done: false, kind: null, doneAt: null, payload: null });
        const payload = syntheticExecutionPayload(executionId, i, stageName);
        await redis.publish('arbitrage:execution', JSON.stringify(payload));
        await sleep(120);
    }

    const waitStart = Date.now();
    while (Date.now() - waitStart < DRILL_EVENT_TIMEOUT_MS) {
        const unresolved = [...pending.values()].filter((row) => !row.done).length;
        if (unresolved === 0) {
            break;
        }
        await sleep(100);
    }

    await redis.quit().catch(() => {});
    socket.disconnect();
    await setTradingMode('PAPER');

    const rows = [...pending.values()];
    const acked = rows.filter((row) => row.done).length;
    const unresolved = rows.length - acked;
    const ackRate = rows.length > 0 ? acked / rows.length : 0;
    const fillRates = liveExecutions.map((entry) => effectiveFillRate(entry));
    const avgFillRate = fillRates.length > 0
        ? fillRates.reduce((sum, value) => sum + value, 0) / fillRates.length
        : 0;
    const p95 = percentile(latencies, 0.95);
    const hasLivePosts = liveExecutions.some((entry) => entry?.dryRun === false);

    const passed = ackRate >= DRILL_MIN_ACK_RATE
        && p95 !== null && p95 <= DRILL_MAX_P95_LATENCY_MS
        && avgFillRate >= minFillRate
        && (!requireRealLivePosts || hasLivePosts);

    return {
        stage: stageName,
        passed,
        skipped: false,
        details: {
            samples: rows.length,
            acked,
            unresolved,
            ack_rate: Number(ackRate.toFixed(4)),
            p50_latency_ms: percentile(latencies, 0.5),
            p95_latency_ms: p95,
            avg_effective_fill_rate: Number(avgFillRate.toFixed(4)),
            live_execution_events: liveExecutions.length,
            gate_blocks: blockedByGate.length,
            require_real_live_posts: requireRealLivePosts,
            has_real_live_posts: hasLivePosts,
            live_order_posting_enabled: liveOrderPostingEnabled,
            thresholds: {
                min_ack_rate: DRILL_MIN_ACK_RATE,
                max_p95_latency_ms: DRILL_MAX_P95_LATENCY_MS,
                min_fill_rate: minFillRate,
            },
            recent_block_reasons: blockedByGate.slice(0, 5),
            unmatched_events: unmatchedEvents.slice(0, 5),
        },
    };
}

async function runSettlementStage() {
    await setTradingMode('PAPER');
    const reset = await api('/api/arb/settlements/reset', { method: 'POST', body: {} });
    if (!reset.ok) {
        return {
            stage: 'SETTLEMENT_COMPLETION',
            passed: false,
            skipped: false,
            details: {
                error: 'failed to reset settlements',
                status: reset.status,
            },
        };
    }

    let simulateOk = 0;
    for (let i = 0; i < DRILL_SETTLEMENT_SAMPLES; i += 1) {
        const condHex = (100_000 + i).toString(16).padStart(64, 'b').slice(0, 64);
        const simulated = await api('/api/arb/settlements/simulate-atomic', {
            method: 'POST',
            body: {
                condition_id: `0x${condHex}`,
                yes_token_id: `SIM_YES_${i}`,
                no_token_id: `SIM_NO_${i}`,
                shares: 3,
                yes_price: 0.44,
                no_price: 0.56,
                market: `Settlement Drill ${i}`,
            },
        });
        if (simulated.ok) {
            simulateOk += 1;
        }
    }

    const processed = await api('/api/arb/settlements/process', { method: 'POST', body: {} });
    const snapshot = await api('/api/arb/settlements', { auth: true });
    const positions = Array.isArray(snapshot.json?.positions) ? snapshot.json.positions : [];
    const progressed = positions.filter((position) => position?.status && position.status !== 'TRACKED').length;
    const completionRate = positions.length > 0 ? progressed / positions.length : 0;
    const passed = simulateOk === DRILL_SETTLEMENT_SAMPLES
        && processed.ok
        && snapshot.ok
        && completionRate >= DRILL_MIN_SETTLEMENT_COMPLETION_RATE;

    return {
        stage: 'SETTLEMENT_COMPLETION',
        passed,
        skipped: false,
        details: {
            simulated_ok: simulateOk,
            simulated_total: DRILL_SETTLEMENT_SAMPLES,
            process_ok: processed.ok,
            tracked_positions: positions.length,
            progressed_positions: progressed,
            completion_rate: Number(completionRate.toFixed(4)),
            thresholds: {
                min_completion_rate: DRILL_MIN_SETTLEMENT_COMPLETION_RATE,
            },
            process_events: Array.isArray(processed.json?.events) ? processed.json.events.length : null,
        },
    };
}

async function main() {
    const report = {
        timestamp: new Date().toISOString(),
        target: API_URL,
        strategy: DRILL_STRATEGY,
        stages: [],
    };

    try {
        report.stages.push(await runExecutionStage({
            stageName: 'LIVE_DRY_RUN',
            sampleCount: DRILL_DRY_RUN_SAMPLES,
            minFillRate: DRILL_MIN_DRY_RUN_FILL_RATE,
            requireRealLivePosts: false,
        }));

        if (DRILL_CONSTRAINED_LIVE_ENABLED) {
            report.stages.push(await runExecutionStage({
                stageName: 'CONSTRAINED_LIVE',
                sampleCount: DRILL_CONSTRAINED_LIVE_SAMPLES,
                minFillRate: DRILL_MIN_LIVE_FILL_RATE,
                requireRealLivePosts: true,
            }));
        } else {
            report.stages.push({
                stage: 'CONSTRAINED_LIVE',
                passed: true,
                skipped: true,
                details: {
                    note: 'Skipped because DRILL_CONSTRAINED_LIVE_ENABLED=false',
                },
            });
        }

        report.stages.push(await runSettlementStage());
    } finally {
        try {
            await setTradingMode('PAPER');
        } catch {
            // keep process exit behavior deterministic
        }
    }

    report.passCount = report.stages.filter((stage) => stage.passed).length;
    report.failCount = report.stages.filter((stage) => !stage.passed).length;
    report.passed = report.failCount === 0;
    report.auto_halt = {
        enabled: DRILL_AUTO_HALT_ON_FAIL,
        triggered: false,
        status: null,
    };

    if (!report.passed && DRILL_AUTO_HALT_ON_FAIL) {
        const haltReason = `live_fire_drill failed at ${new Date().toISOString()} (failCount=${report.failCount})`;
        const haltResponse = await api('/api/system/execution-halt', {
            method: 'POST',
            body: {
                halted: true,
                reason: haltReason,
            },
        });
        report.auto_halt = {
            enabled: DRILL_AUTO_HALT_ON_FAIL,
            triggered: true,
            status: haltResponse.status,
            ok: haltResponse.ok,
            reason: haltReason,
            error: haltResponse.json?.error || null,
        };
    }

    console.log(JSON.stringify(report, null, 2));
    process.exit(report.passed ? 0 : 1);
}

main().catch((error) => {
    console.error(error);
    process.exit(1);
});
