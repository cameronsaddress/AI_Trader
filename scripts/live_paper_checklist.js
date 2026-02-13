#!/usr/bin/env node

/*
Live paper-validation checklist runner.

Checks:
1) Mode toggle guardrails (LIVE requires confirmation).
2) Intelligence gate block event for uncorroborated execution.
3) Atomic settlement tracking flow via PAPER-safe simulation endpoint.
4) Syndicate timed exits evidence from strategy trade log.

Environment:
- API_URL (default: http://localhost:5114)
- CONTROL_PLANE_TOKEN (required)
- REDIS_URL (default: redis://localhost:6491)
- CHECKLIST_TRADE_LOG_PATH (default: apps/backend/logs/strategy_trades.csv)
*/

const fs = require('fs');
const { io } = require('socket.io-client');
const { createClient } = require('redis');

const API_URL = process.env.API_URL || 'http://localhost:5114';
const CONTROL_PLANE_TOKEN = process.env.CONTROL_PLANE_TOKEN || '';
const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6491';
const TRADE_LOG_PATH = process.env.CHECKLIST_TRADE_LOG_PATH || 'apps/backend/logs/strategy_trades.csv';

if (!CONTROL_PLANE_TOKEN) {
    console.error('CONTROL_PLANE_TOKEN is required');
    process.exit(2);
}

function parseCsvLine(line) {
    return line
        .split(/,(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)/)
        .map((value) => {
            const trimmed = value.trim();
            if (trimmed.startsWith('"') && trimmed.endsWith('"')) {
                return trimmed.slice(1, -1).replace(/""/g, '"');
            }
            return trimmed;
        });
}

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
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
        // ignore non-json responses
    }
    return { status: response.status, ok: response.ok, json };
}

async function checkModeToggle() {
    const ensurePaper = await api('/api/system/trading-mode', { method: 'POST', body: { mode: 'PAPER' } });
    const noConfirm = await api('/api/system/trading-mode', { method: 'POST', body: { mode: 'LIVE' } });
    const confirmLive = await api('/api/system/trading-mode', { method: 'POST', body: { mode: 'LIVE', confirmation: 'LIVE' } });
    const modeAfterLive = await api('/api/system/trading-mode', { auth: false });
    const backPaper = await api('/api/system/trading-mode', { method: 'POST', body: { mode: 'PAPER' } });
    const modeAfterPaper = await api('/api/system/trading-mode', { auth: false });

    const passed = ensurePaper.ok
        && noConfirm.status === 400
        && confirmLive.ok && confirmLive.json?.mode === 'LIVE'
        && modeAfterLive.ok && modeAfterLive.json?.mode === 'LIVE'
        && backPaper.ok && modeAfterPaper.ok && modeAfterPaper.json?.mode === 'PAPER';

    return {
        name: 'mode_toggle',
        passed,
        details: {
            noConfirmStatus: noConfirm.status,
            liveSet: confirmLive.json?.mode,
            modeAfterLive: modeAfterLive.json?.mode,
            modeAfterPaper: modeAfterPaper.json?.mode,
        },
    };
}

async function checkIntelligenceBlockEvent() {
    const toLive = await api('/api/system/trading-mode', { method: 'POST', body: { mode: 'LIVE', confirmation: 'LIVE' } });
    if (!toLive.ok) {
        throw new Error(`failed to set LIVE (${toLive.status})`);
    }

    const conditionId = `0x${'1'.repeat(64)}`;
    let blocked = null;
    let matchingLiveExecution = null;

    const socket = io(API_URL, {
        transports: ['websocket'],
        auth: { token: CONTROL_PLANE_TOKEN },
        timeout: 6000,
    });

    await new Promise((resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error('socket connect timeout')), 7000);
        socket.on('connect', () => {
            clearTimeout(timeout);
            resolve();
        });
        socket.on('connect_error', (error) => {
            clearTimeout(timeout);
            reject(error);
        });
    });

    socket.on('strategy_intelligence_block', (payload) => {
        if (payload?.strategy === 'CEX_SNIPER' && payload?.market_key === conditionId) {
            blocked = payload;
        }
    });

    socket.on('strategy_live_execution', (payload) => {
        if (payload?.strategy === 'CEX_SNIPER' && String(payload?.market || '').includes('Checklist')) {
            matchingLiveExecution = payload;
        }
    });

    const redis = createClient({ url: REDIS_URL });
    await redis.connect();
    const syntheticExecution = {
        market: 'Checklist Intelligence Gate',
        side: 'LIVE_DRY_RUN_LONG',
        price: 0.52,
        size: 25,
        timestamp: Date.now(),
        mode: 'LIVE_DRY_RUN',
        details: {
            strategy: 'CEX_SNIPER',
            preflight: {
                venue: 'POLYMARKET',
                strategy: 'CEX_SNIPER',
                orders: [
                    {
                        token_id: '1000001',
                        condition_id: conditionId,
                        side: 'BUY',
                        price: 0.52,
                        size: 25,
                        size_unit: 'USD_NOTIONAL',
                    },
                ],
            },
        },
    };
    await redis.publish('arbitrage:execution', JSON.stringify(syntheticExecution));
    await sleep(2500);

    await redis.quit();
    socket.disconnect();

    const toPaper = await api('/api/system/trading-mode', { method: 'POST', body: { mode: 'PAPER' } });
    const passed = Boolean(blocked) && !matchingLiveExecution && toPaper.ok;

    return {
        name: 'intelligence_block_event',
        passed,
        details: {
            blockedReason: blocked?.reason || null,
            blockedPeerSignals: blocked?.peer_signals ?? null,
            matchingLiveExecution: Boolean(matchingLiveExecution),
            revertedToPaper: toPaper.ok,
        },
    };
}

async function checkAtomicSettlementFlow() {
    await api('/api/arb/settlements/reset', { method: 'POST', body: {} });
    const conditionId = `0x${'2'.repeat(64)}`;

    const simulated = await api('/api/arb/settlements/simulate-atomic', {
        method: 'POST',
        body: {
            condition_id: conditionId,
            yes_token_id: 'SIM_YES_1',
            no_token_id: 'SIM_NO_1',
            shares: 10,
            yes_price: 0.44,
            no_price: 0.56,
            market: 'Checklist Atomic Settlement',
        },
    });
    const process = await api('/api/arb/settlements/process', { method: 'POST', body: {} });
    const snapshot = await api('/api/arb/settlements', { auth: false });
    const reset = await api('/api/arb/settlements/reset', { method: 'POST', body: {} });

    const trackedAfterSim = Array.isArray(simulated.json?.snapshot?.positions)
        ? simulated.json.snapshot.positions.length
        : null;

    const passed = simulated.ok && simulated.json?.ok === true
        && trackedAfterSim !== null && trackedAfterSim >= 1
        && process.ok && process.json?.ok === true
        && snapshot.ok && Array.isArray(snapshot.json?.positions)
        && reset.ok && reset.json?.ok === true;

    return {
        name: 'atomic_settlement_flow',
        passed,
        details: {
            simulateEvents: Array.isArray(simulated.json?.events) ? simulated.json.events.length : null,
            trackedAfterSim,
            processEvents: Array.isArray(process.json?.events) ? process.json.events.length : null,
            trackedBeforeReset: Array.isArray(snapshot.json?.positions) ? snapshot.json.positions.length : null,
            resetOk: reset.ok,
        },
    };
}

async function checkSyndicateTimedExits() {
    const raw = fs.readFileSync(TRADE_LOG_PATH, 'utf8');
    const lines = raw.split(/\r?\n/).filter((line) => line.trim().length > 0);
    if (lines.length <= 1) {
        return {
            name: 'syndicate_timed_exits',
            passed: true,
            details: {
                paperSyndicateTrades: 0,
                timedExitCount: 0,
                skippedNoSample: true,
                note: 'Trade log empty in current window',
            },
        };
    }

    const header = parseCsvLine(lines[0]);
    const indexByName = Object.fromEntries(header.map((column, idx) => [column.trim(), idx]));
    let paperSyndicateTrades = 0;
    let timedExitCount = 0;

    for (let i = 1; i < lines.length; i += 1) {
        const cols = parseCsvLine(lines[i]);
        const strategy = cols[indexByName.strategy] || '';
        const mode = cols[indexByName.mode] || '';
        const reason = cols[indexByName.reason] || '';
        if (strategy === 'SYNDICATE' && mode === 'PAPER') {
            paperSyndicateTrades += 1;
            if (reason === 'TIME_EXIT') {
                timedExitCount += 1;
            }
        }
    }

    if (paperSyndicateTrades === 0) {
        return {
            name: 'syndicate_timed_exits',
            passed: true,
            details: {
                paperSyndicateTrades,
                timedExitCount,
                skippedNoSample: true,
                note: 'No PAPER syndicate trades observed in current log window',
            },
        };
    }

    return {
        name: 'syndicate_timed_exits',
        passed: timedExitCount > 0,
        details: {
            paperSyndicateTrades,
            timedExitCount,
            skippedNoSample: false,
        },
    };
}

async function main() {
    const report = {
        timestamp: new Date().toISOString(),
        target: API_URL,
        checks: [],
    };

    const checks = [
        checkModeToggle,
        checkIntelligenceBlockEvent,
        checkAtomicSettlementFlow,
        checkSyndicateTimedExits,
    ];

    for (const check of checks) {
        try {
            const result = await check();
            report.checks.push(result);
        } catch (error) {
            report.checks.push({
                name: check.name,
                passed: false,
                details: { error: error instanceof Error ? error.message : String(error) },
            });
            // Keep safe default mode on failures.
            try {
                await api('/api/system/trading-mode', { method: 'POST', body: { mode: 'PAPER' } });
            } catch {
                // ignore
            }
        }
    }

    report.passCount = report.checks.filter((check) => check.passed).length;
    report.failCount = report.checks.length - report.passCount;
    report.passed = report.failCount === 0;

    console.log(JSON.stringify(report, null, 2));
    process.exit(report.passed ? 0 : 1);
}

main().catch((error) => {
    console.error(error);
    process.exit(1);
});
