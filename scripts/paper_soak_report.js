#!/usr/bin/env node

/*
Paper soak report runner.

Purpose:
1) Force PAPER mode and reset validation state.
2) Observe execution/preflight behavior over a long soak window.
3) Report block rates (shortfall/netting), preflight pass rate,
   edge-capture quality, and final strategy multiplier posture.
*/

const fs = require('fs');
const path = require('path');
const { io } = require('socket.io-client');

const API_URL = process.env.API_URL || 'http://localhost:5114';
const CONTROL_PLANE_TOKEN = process.env.CONTROL_PLANE_TOKEN || '';
const DURATION_SECONDS = Math.max(5, Number(process.env.SOAK_DURATION_SECONDS || '86400'));
const SAMPLE_INTERVAL_SECONDS = Math.max(1, Number(process.env.SOAK_SAMPLE_INTERVAL_SECONDS || '5'));
const BASELINE_BANKROLL = Math.max(100, Number(process.env.SOAK_BASELINE_BANKROLL || '1000'));
const OUTPUT_PATH = process.env.SOAK_OUTPUT_PATH || 'reports/paper_soak_report.json';
const OUTPUT_MD_PATH = process.env.SOAK_OUTPUT_MD_PATH || OUTPUT_PATH.replace(/\.json$/i, '.md');
const STATS_JSONL_PATH = process.env.SOAK_STATS_JSONL_PATH || 'reports/paper_soak_stats.jsonl';
const FALLBACK_TRADE_LOG_PATH = process.env.SOAK_TRADE_LOG_PATH || 'apps/backend/logs/strategy_trades.csv';
const REJECTED_LIMIT = Math.max(200, Math.min(5000, Number(process.env.SOAK_REJECTED_LIMIT || '2000')));

const MIN_PREFLIGHT_SAMPLES = Math.max(5, Number(process.env.SOAK_MIN_PREFLIGHT_SAMPLES || '20'));
const MIN_PREFLIGHT_PASS_RATE_PCT = Number(process.env.SOAK_MIN_PREFLIGHT_PASS_RATE_PCT || '96');
const MAX_SHORTFALL_BLOCK_RATE_PCT = Number(process.env.SOAK_MAX_SHORTFALL_BLOCK_RATE_PCT || '18');
const MAX_NETTING_BLOCK_RATE_PCT = Number(process.env.SOAK_MAX_NETTING_BLOCK_RATE_PCT || '12');
const MIN_EDGE_CAPTURE_RATIO = Number(process.env.SOAK_MIN_EDGE_CAPTURE_RATIO || '0.55');
const MIN_NET_PNL_USD = Number(process.env.SOAK_MIN_NET_PNL_USD || '0');
const MAX_REJECT_RATE_PCT = Number(process.env.SOAK_MAX_REJECT_RATE_PCT || '20');
const MAX_STALE_FORCED_CLOSES = Math.max(0, Number(process.env.SOAK_MAX_STALE_FORCED_CLOSES || '0'));
const MAX_NEGATIVE_TAKE_PROFIT_CLOSES = Math.max(0, Number(process.env.SOAK_MAX_NEGATIVE_TAKE_PROFIT_CLOSES || '0'));

if (!CONTROL_PLANE_TOKEN) {
    console.error('CONTROL_PLANE_TOKEN is required');
    process.exit(2);
}

function ensureParent(filePath) {
    const parent = path.dirname(path.resolve(filePath));
    fs.mkdirSync(parent, { recursive: true });
}

function asNumber(value, fallback = 0) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : fallback;
}

function parseTimestampMs(value) {
    if (value === null || value === undefined || value === '') {
        return Date.now();
    }
    const numeric = Number(value);
    if (Number.isFinite(numeric)) {
        if (numeric > 1e12) return Math.trunc(numeric);
        if (numeric > 1e9) return Math.trunc(numeric * 1000);
        if (numeric > 1e6) return Math.trunc(numeric);
    }
    const parsed = Date.parse(String(value));
    return Number.isFinite(parsed) ? parsed : Date.now();
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

async function api(pathname, options = {}) {
    const { method = 'GET', body, auth = true } = options;
    const headers = { 'content-type': 'application/json' };
    if (auth) {
        headers.authorization = `Bearer ${CONTROL_PLANE_TOKEN}`;
    }
    const response = await fetch(`${API_URL}${pathname}`, {
        method,
        headers,
        body: body === undefined ? undefined : JSON.stringify(body),
    });
    let json = null;
    try {
        json = await response.json();
    } catch {
        // ignore
    }
    return { ok: response.ok, status: response.status, json };
}

function resolveTradeLogPath(reportedPath, fallbackPath) {
    const candidates = [];
    if (reportedPath && typeof reportedPath === 'string' && reportedPath.trim().length > 0) {
        candidates.push(path.resolve(reportedPath.trim()));
        if (reportedPath.trim().startsWith('/app/')) {
            const mapped = reportedPath.trim().replace(/^\/app\//, '');
            candidates.push(path.resolve(mapped));
        }
    }
    candidates.push(path.resolve(fallbackPath));
    for (const candidate of candidates) {
        if (fs.existsSync(candidate)) {
            return candidate;
        }
    }
    return path.resolve(fallbackPath);
}

function parseTradeRows(tradeLogPath, startMs) {
    if (!fs.existsSync(tradeLogPath)) {
        return [];
    }
    const raw = fs.readFileSync(tradeLogPath, 'utf8');
    const lines = raw.split(/\r?\n/).filter((line) => line.trim().length > 0);
    if (lines.length <= 1) {
        return [];
    }
    const header = parseCsvLine(lines[0]);
    const indexByName = Object.fromEntries(header.map((name, idx) => [String(name || '').trim().toLowerCase(), idx]));

    const rows = [];
    for (let i = 1; i < lines.length; i += 1) {
        const cols = parseCsvLine(lines[i]);
        const ts = parseTimestampMs(cols[indexByName.timestamp]);
        if (ts < startMs) continue;
        const mode = String(cols[indexByName.mode] || '').trim().toUpperCase();
        if (mode !== 'PAPER') continue;

        const strategy = String(cols[indexByName.strategy] || '').trim().toUpperCase();
        const pnl = asNumber(cols[indexByName.pnl], 0);
        const notionalRaw = Math.abs(asNumber(cols[indexByName.notional], 0));
        const notional = notionalRaw > 0 ? notionalRaw : Math.max(1, Math.abs(pnl));
        const reason = String(cols[indexByName.reason] || '').trim().toUpperCase();
        const closureClass = String(cols[indexByName.closure_class] || '').trim().toUpperCase();
        const grossReturn = indexByName.gross_return !== undefined
            ? asNumber(cols[indexByName.gross_return], NaN)
            : NaN;
        const netReturn = indexByName.net_return !== undefined
            ? asNumber(cols[indexByName.net_return], pnl / notional)
            : (pnl / notional);
        const costDrag = indexByName.cost_drag_return !== undefined
            ? asNumber(cols[indexByName.cost_drag_return], NaN)
            : NaN;

        rows.push({
            timestamp: ts,
            strategy,
            pnl,
            notional,
            reason,
            closure_class: closureClass,
            net_return: netReturn,
            gross_return: Number.isFinite(grossReturn) ? grossReturn : null,
            cost_drag_return: Number.isFinite(costDrag) ? costDrag : null,
        });
    }
    return rows;
}

function summarizeTrades(rows) {
    const byStrategy = {};
    let netPnl = 0;
    let tradeCount = 0;
    let grossSum = 0;
    let netSumWithGross = 0;
    let grossSamples = 0;
    let costDragSum = 0;
    let costDragSamples = 0;
    let staleForcedCloses = 0;
    let negativeTakeProfitCloses = 0;

    for (const row of rows) {
        tradeCount += 1;
        netPnl += row.pnl;
        if (!byStrategy[row.strategy]) {
            byStrategy[row.strategy] = {
                trades: 0,
                net_pnl: 0,
                avg_net_return_bps: 0,
                avg_gross_return_bps: 0,
            };
        }
        const bucket = byStrategy[row.strategy];
        bucket.trades += 1;
        bucket.net_pnl += row.pnl;
        bucket.avg_net_return_bps += row.net_return * 10_000;
        if (
            row.reason.includes('STALE')
            || row.closure_class === 'FORCED'
        ) {
            staleForcedCloses += 1;
        }
        if (row.reason.includes('TAKE_PROFIT') && row.net_return < 0) {
            negativeTakeProfitCloses += 1;
        }

        if (row.gross_return !== null) {
            grossSamples += 1;
            grossSum += row.gross_return;
            netSumWithGross += row.net_return;
            bucket.avg_gross_return_bps += row.gross_return * 10_000;
        }
        if (row.cost_drag_return !== null) {
            costDragSamples += 1;
            costDragSum += row.cost_drag_return;
        }
    }

    for (const strategy of Object.keys(byStrategy)) {
        const bucket = byStrategy[strategy];
        bucket.net_pnl = Math.round(bucket.net_pnl * 100) / 100;
        bucket.avg_net_return_bps = bucket.trades > 0
            ? Math.round((bucket.avg_net_return_bps / bucket.trades) * 10) / 10
            : 0;
        bucket.avg_gross_return_bps = grossSamples > 0
            ? Math.round((bucket.avg_gross_return_bps / Math.max(1, bucket.trades)) * 10) / 10
            : 0;
    }

    const edgeCaptureRatio = grossSamples > 0 && Math.abs(grossSum) > 1e-9
        ? (netSumWithGross / grossSum)
        : null;
    const avgCostDragBps = costDragSamples > 0
        ? ((costDragSum / costDragSamples) * 10_000)
        : null;

    return {
        trade_count: tradeCount,
        net_pnl: Math.round(netPnl * 100) / 100,
        gross_samples: grossSamples,
        edge_capture_ratio: edgeCaptureRatio,
        avg_cost_drag_bps: avgCostDragBps,
        stale_forced_closes: staleForcedCloses,
        negative_take_profit_closes: negativeTakeProfitCloses,
        by_strategy: byStrategy,
    };
}

function upper(value) {
    return String(value || '').trim().toUpperCase();
}

function buildChecks(metrics) {
    const checks = {};
    const preflightRate = metrics.preflight.pass_rate_pct;
    const preflightSamples = metrics.preflight.total;
    checks.preflight_pass_rate = preflightSamples < MIN_PREFLIGHT_SAMPLES
        ? {
            ok: true,
            skipped: true,
            actual: preflightRate,
            threshold: MIN_PREFLIGHT_PASS_RATE_PCT,
            note: `insufficient samples (${preflightSamples} < ${MIN_PREFLIGHT_SAMPLES})`,
        }
        : {
            ok: preflightRate !== null && preflightRate >= MIN_PREFLIGHT_PASS_RATE_PCT,
            skipped: false,
            actual: preflightRate,
            threshold: MIN_PREFLIGHT_PASS_RATE_PCT,
        };

    checks.shortfall_block_rate = preflightSamples < MIN_PREFLIGHT_SAMPLES
        ? {
            ok: true,
            skipped: true,
            actual: metrics.blocks.shortfall_block_rate_pct,
            threshold: MAX_SHORTFALL_BLOCK_RATE_PCT,
            note: `insufficient preflight denominator (${preflightSamples} < ${MIN_PREFLIGHT_SAMPLES})`,
        }
        : {
            ok: (metrics.blocks.shortfall_block_rate_pct || 0) <= MAX_SHORTFALL_BLOCK_RATE_PCT,
            skipped: false,
            actual: metrics.blocks.shortfall_block_rate_pct,
            threshold: MAX_SHORTFALL_BLOCK_RATE_PCT,
        };

    checks.netting_block_rate = preflightSamples < MIN_PREFLIGHT_SAMPLES
        ? {
            ok: true,
            skipped: true,
            actual: metrics.blocks.netting_block_rate_pct,
            threshold: MAX_NETTING_BLOCK_RATE_PCT,
            note: `insufficient preflight denominator (${preflightSamples} < ${MIN_PREFLIGHT_SAMPLES})`,
        }
        : {
            ok: (metrics.blocks.netting_block_rate_pct || 0) <= MAX_NETTING_BLOCK_RATE_PCT,
            skipped: false,
            actual: metrics.blocks.netting_block_rate_pct,
            threshold: MAX_NETTING_BLOCK_RATE_PCT,
        };

    checks.edge_capture_ratio = metrics.trades.edge_capture_ratio === null
        ? {
            ok: true,
            skipped: true,
            actual: null,
            threshold: MIN_EDGE_CAPTURE_RATIO,
            note: 'no gross/net paired trade samples in window',
        }
        : {
            ok: metrics.trades.edge_capture_ratio >= MIN_EDGE_CAPTURE_RATIO,
            skipped: false,
            actual: metrics.trades.edge_capture_ratio,
            threshold: MIN_EDGE_CAPTURE_RATIO,
        };

    checks.net_pnl_positive = {
        ok: (metrics.trades.net_pnl || 0) >= MIN_NET_PNL_USD,
        skipped: false,
        actual: metrics.trades.net_pnl,
        threshold: MIN_NET_PNL_USD,
    };

    checks.stale_forced_closes = {
        ok: (metrics.trades.stale_forced_closes || 0) <= MAX_STALE_FORCED_CLOSES,
        skipped: false,
        actual: metrics.trades.stale_forced_closes || 0,
        threshold: MAX_STALE_FORCED_CLOSES,
    };

    checks.no_negative_take_profit_closes = {
        ok: (metrics.trades.negative_take_profit_closes || 0) <= MAX_NEGATIVE_TAKE_PROFIT_CLOSES,
        skipped: false,
        actual: metrics.trades.negative_take_profit_closes || 0,
        threshold: MAX_NEGATIVE_TAKE_PROFIT_CLOSES,
    };

    checks.reject_rate = metrics.rejected_signals.preflight_reject_rate_pct === null
        ? {
            ok: true,
            skipped: true,
            actual: null,
            threshold: MAX_REJECT_RATE_PCT,
            note: 'no preflight samples available',
        }
        : {
            ok: metrics.rejected_signals.preflight_reject_rate_pct <= MAX_REJECT_RATE_PCT,
            skipped: false,
            actual: metrics.rejected_signals.preflight_reject_rate_pct,
            threshold: MAX_REJECT_RATE_PCT,
        };

    checks.paper_mode_enforced = {
        ok: metrics.activity.paper_mode_ratio_pct >= 99.9,
        skipped: false,
        actual: metrics.activity.paper_mode_ratio_pct,
        threshold: 99.9,
    };

    checks.live_execution_zero = {
        ok: metrics.activity.live_execution_events === 0,
        skipped: false,
        actual: metrics.activity.live_execution_events,
        threshold: 0,
    };

    const overallOk = Object.values(checks).every((check) => check.ok);
    return { checks, overall_ok: overallOk };
}

function toMarkdown(report) {
    const lines = [];
    lines.push('# Paper Soak Report');
    lines.push('');
    lines.push(`- Started: ${report.started_at}`);
    lines.push(`- Finished: ${report.finished_at}`);
    lines.push(`- Duration Seconds: ${report.duration_seconds}`);
    lines.push(`- Overall Pass: ${report.pass ? 'YES' : 'NO'}`);
    lines.push('');
    lines.push('## Key Metrics');
    lines.push('');
    lines.push(`- Preflight Pass Rate: ${report.metrics.preflight.pass_rate_pct === null ? 'n/a' : report.metrics.preflight.pass_rate_pct.toFixed(2) + '%'}`);
    lines.push(`- Shortfall Block Rate: ${report.metrics.blocks.shortfall_block_rate_pct === null ? 'n/a' : report.metrics.blocks.shortfall_block_rate_pct.toFixed(2) + '%'}`);
    lines.push(`- Netting Block Rate: ${report.metrics.blocks.netting_block_rate_pct === null ? 'n/a' : report.metrics.blocks.netting_block_rate_pct.toFixed(2) + '%'}`);
    lines.push(`- Edge Capture Ratio: ${report.metrics.trades.edge_capture_ratio === null ? 'n/a' : report.metrics.trades.edge_capture_ratio.toFixed(3)}`);
    lines.push(`- Net PnL: $${Number(report.metrics.trades.net_pnl || 0).toFixed(2)}`);
    lines.push(`- Stale Forced Closes: ${report.metrics.trades.stale_forced_closes || 0}`);
    lines.push(`- Negative TAKE_PROFIT Closes: ${report.metrics.trades.negative_take_profit_closes || 0}`);
    lines.push(`- Reject Rate: ${report.metrics.rejected_signals.preflight_reject_rate_pct === null ? 'n/a' : report.metrics.rejected_signals.preflight_reject_rate_pct.toFixed(2) + '%'}`);
    lines.push(`- Live Execution Events While In PAPER: ${report.metrics.activity.live_execution_events}`);
    lines.push('');
    lines.push('## Checks');
    lines.push('');
    lines.push('| Check | Result | Actual | Threshold | Note |');
    lines.push('| --- | --- | --- | --- | --- |');
    for (const [name, check] of Object.entries(report.checks)) {
        lines.push(
            `| ${name} | ${check.ok ? 'PASS' : 'FAIL'}${check.skipped ? ' (SKIP)' : ''} | ${check.actual === null || check.actual === undefined ? 'n/a' : check.actual} | ${check.threshold} | ${check.note || ''} |`,
        );
    }
    lines.push('');
    return `${lines.join('\n')}\n`;
}

async function main() {
    ensureParent(OUTPUT_PATH);
    ensureParent(OUTPUT_MD_PATH);
    ensureParent(STATS_JSONL_PATH);

    const startWall = Date.now();

    const setPaper = await api('/api/system/trading-mode', {
        method: 'POST',
        body: { mode: 'PAPER' },
    });
    if (!setPaper.ok) {
        throw new Error(`failed to set PAPER mode (${setPaper.status})`);
    }

    const resetSimulation = await api('/api/system/reset-simulation', {
        method: 'POST',
        body: {
            confirmation: 'RESET',
            bankroll: BASELINE_BANKROLL,
            force_defaults: true,
        },
    });
    if (!resetSimulation.ok) {
        throw new Error(`failed to reset simulation (${resetSimulation.status})`);
    }

    const resetTrades = await api('/api/arb/validation-trades/reset', { method: 'POST', body: {} });
    const tradeInfo = await api('/api/arb/validation-trades', { auth: true });
    await api('/api/arb/rejected-signals/reset', { method: 'POST', body: {} });

    const reportedTradePath = resetTrades.json?.path || tradeInfo.json?.path || null;
    const tradeLogPath = resolveTradeLogPath(reportedTradePath, FALLBACK_TRADE_LOG_PATH);

    const eventCounters = {
        preflight_total: 0,
        preflight_ok: 0,
        preflight_failed: 0,
        shortfall_blocks: 0,
        netting_blocks: 0,
        intelligence_blocks: 0,
        live_execution_events: 0,
        live_execution_failures: 0,
        strategy_pnl_events: 0,
        strategy_pnl_sum: 0,
    };
    const multiplierLatest = {};

    const socket = io(API_URL, {
        transports: ['websocket'],
        auth: { token: CONTROL_PLANE_TOKEN },
        timeout: 10000,
    });
    await new Promise((resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error('socket connect timeout')), 12_000);
        socket.on('connect', () => {
            clearTimeout(timeout);
            resolve();
        });
        socket.on('connect_error', (error) => {
            clearTimeout(timeout);
            reject(error);
        });
    });

    const runStartMs = Date.now();
    socket.on('strategy_preflight', (payload) => {
        const ts = parseTimestampMs(payload?.timestamp);
        if (ts < runStartMs) return;
        eventCounters.preflight_total += 1;
        if (payload?.ok === true) {
            eventCounters.preflight_ok += 1;
        } else {
            eventCounters.preflight_failed += 1;
        }
    });
    socket.on('strategy_intelligence_block', (payload) => {
        const ts = parseTimestampMs(payload?.timestamp);
        if (ts < runStartMs) return;
        eventCounters.intelligence_blocks += 1;
        const reason = upper(payload?.reason);
        if (reason.includes('[SHORTFALL]')) eventCounters.shortfall_blocks += 1;
        if (reason.includes('[NETTING]')) eventCounters.netting_blocks += 1;
    });
    socket.on('execution_log', (payload) => {
        const ts = parseTimestampMs(payload?.timestamp);
        if (ts < runStartMs) return;
        const side = upper(payload?.side);
        if (side === 'SHORTFALL_BLOCK') eventCounters.shortfall_blocks += 1;
        if (side === 'NETTING_BLOCK') eventCounters.netting_blocks += 1;
    });
    socket.on('strategy_live_execution', (payload) => {
        const ts = parseTimestampMs(payload?.timestamp);
        if (ts < runStartMs) return;
        eventCounters.live_execution_events += 1;
        if (payload?.ok === false) {
            eventCounters.live_execution_failures += 1;
        }
    });
    socket.on('strategy_pnl', (payload) => {
        const ts = parseTimestampMs(payload?.timestamp);
        if (ts < runStartMs) return;
        eventCounters.strategy_pnl_events += 1;
        eventCounters.strategy_pnl_sum += asNumber(payload?.pnl, 0);
    });
    socket.on('strategy_risk_multiplier_update', (payload) => {
        const strategy = upper(payload?.strategy);
        if (!strategy) return;
        multiplierLatest[strategy] = {
            multiplier: asNumber(payload?.multiplier, 0),
            base_multiplier: asNumber(payload?.base_multiplier, payload?.multiplier),
            meta_overlay: asNumber(payload?.meta_overlay, 1),
            sample_count: asNumber(payload?.sample_count, 0),
            timestamp: parseTimestampMs(payload?.timestamp),
        };
    });

    const soakEndAt = Date.now() + Math.floor(DURATION_SECONDS * 1000);
    let sampleCount = 0;
    let paperModeSamples = 0;
    let activeSignalsSum = 0;
    let activeMarketsSum = 0;
    let firstAllocator = null;
    let lastStats = null;

    const statsOut = fs.createWriteStream(path.resolve(STATS_JSONL_PATH), { flags: 'w' });
    while (Date.now() < soakEndAt) {
        const response = await api('/api/arb/stats', { auth: true });
        const now = Date.now();
        const stats = response.json || {};
        sampleCount += 1;
        if (upper(stats.trading_mode) === 'PAPER') {
            paperModeSamples += 1;
        }
        activeSignalsSum += asNumber(stats.active_signals, 0);
        activeMarketsSum += asNumber(stats.active_markets, 0);
        if (!firstAllocator && stats.strategy_allocator && typeof stats.strategy_allocator === 'object') {
            firstAllocator = stats.strategy_allocator;
        }
        lastStats = stats;
        statsOut.write(`${JSON.stringify({
            ts: now,
            ok: response.ok,
            status: response.status,
            trading_mode: stats.trading_mode || null,
            active_signals: stats.active_signals || 0,
            active_markets: stats.active_markets || 0,
            rejected_signals: stats.rejected_signals || null,
            strategy_allocator: stats.strategy_allocator || null,
        })}\n`);

        await sleep(Math.floor(SAMPLE_INTERVAL_SECONDS * 1000));
    }
    statsOut.end();
    socket.disconnect();
    await api('/api/system/trading-mode', { method: 'POST', body: { mode: 'PAPER' } });

    const trades = parseTradeRows(tradeLogPath, runStartMs);
    const tradeSummary = summarizeTrades(trades);

    const rejectedResp = await api(`/api/arb/rejected-signals?source=file&limit=${REJECTED_LIMIT}`, { auth: true });
    const rejectedEntriesRaw = Array.isArray(rejectedResp.json?.entries) ? rejectedResp.json.entries : [];
    const rejectedEntries = rejectedEntriesRaw.filter((entry) => parseTimestampMs(entry?.timestamp) >= runStartMs);
    const preflightRejectedEntries = rejectedEntries.filter((entry) => upper(entry?.stage) === 'PREFLIGHT');
    const rejectedByStage = {};
    let rejectedShortfall = 0;
    let rejectedNetting = 0;
    for (const entry of rejectedEntries) {
        const stage = upper(entry?.stage) || 'UNKNOWN';
        rejectedByStage[stage] = (rejectedByStage[stage] || 0) + 1;
        const reason = upper(entry?.reason);
        if (reason.includes('[SHORTFALL]')) rejectedShortfall += 1;
        if (reason.includes('[NETTING]')) rejectedNetting += 1;
    }

    const observedShortfall = Math.max(eventCounters.shortfall_blocks, rejectedShortfall);
    const observedNetting = Math.max(eventCounters.netting_blocks, rejectedNetting);
    const preflightTotal = eventCounters.preflight_total;
    const preflightPassRate = preflightTotal > 0
        ? (eventCounters.preflight_ok / preflightTotal) * 100
        : null;
    const shortfallBlockRate = preflightTotal > 0
        ? (observedShortfall / preflightTotal) * 100
        : null;
    const nettingBlockRate = preflightTotal > 0
        ? (observedNetting / preflightTotal) * 100
        : null;
    const preflightRejectRate = preflightTotal > 0
        ? (preflightRejectedEntries.length / preflightTotal) * 100
        : null;

    const finalAllocator = (lastStats && typeof lastStats.strategy_allocator === 'object')
        ? lastStats.strategy_allocator
        : {};
    const allocatorDelta = {};
    const allAllocatorStrategies = new Set([
        ...Object.keys(firstAllocator || {}),
        ...Object.keys(finalAllocator || {}),
        ...Object.keys(multiplierLatest || {}),
    ]);
    for (const strategy of allAllocatorStrategies) {
        const start = asNumber(firstAllocator?.[strategy]?.multiplier, 0);
        const end = asNumber(finalAllocator?.[strategy]?.multiplier, multiplierLatest?.[strategy]?.multiplier || 0);
        allocatorDelta[strategy] = {
            start_multiplier: Math.round(start * 1000) / 1000,
            end_multiplier: Math.round(end * 1000) / 1000,
            delta: Math.round((end - start) * 1000) / 1000,
            base_multiplier: Math.round(asNumber(finalAllocator?.[strategy]?.base_multiplier, multiplierLatest?.[strategy]?.base_multiplier || 0) * 1000) / 1000,
            meta_overlay: Math.round(asNumber(finalAllocator?.[strategy]?.meta_overlay, multiplierLatest?.[strategy]?.meta_overlay || 1) * 1000) / 1000,
            sample_count: asNumber(finalAllocator?.[strategy]?.sample_count, multiplierLatest?.[strategy]?.sample_count || 0),
        };
    }

    const finishedMs = Date.now();
    const metrics = {
        activity: {
            sample_count: sampleCount,
            avg_active_signals: sampleCount > 0 ? activeSignalsSum / sampleCount : 0,
            avg_active_markets: sampleCount > 0 ? activeMarketsSum / sampleCount : 0,
            paper_mode_ratio_pct: sampleCount > 0 ? (paperModeSamples / sampleCount) * 100 : 0,
            live_execution_events: eventCounters.live_execution_events,
            live_execution_failures: eventCounters.live_execution_failures,
            strategy_pnl_events: eventCounters.strategy_pnl_events,
            strategy_pnl_sum: Math.round(eventCounters.strategy_pnl_sum * 100) / 100,
        },
        preflight: {
            total: preflightTotal,
            ok: eventCounters.preflight_ok,
            failed: eventCounters.preflight_failed,
            pass_rate_pct: preflightPassRate,
        },
        blocks: {
            shortfall_blocks: observedShortfall,
            netting_blocks: observedNetting,
            intelligence_blocks_total: eventCounters.intelligence_blocks,
            shortfall_block_rate_pct: shortfallBlockRate,
            netting_block_rate_pct: nettingBlockRate,
        },
        trades: tradeSummary,
        rejected_signals: {
            count: rejectedEntries.length,
            preflight_count: preflightRejectedEntries.length,
            by_stage: rejectedByStage,
            shortfall_tagged: rejectedShortfall,
            netting_tagged: rejectedNetting,
            preflight_reject_rate_pct: preflightRejectRate,
        },
        strategy_allocator: allocatorDelta,
        execution_netting: lastStats?.execution_netting || null,
    };

    const checkResults = buildChecks(metrics);
    const report = {
        started_at: new Date(runStartMs).toISOString(),
        finished_at: new Date(finishedMs).toISOString(),
        duration_seconds: Math.round((finishedMs - runStartMs) / 1000),
        baseline_bankroll: BASELINE_BANKROLL,
        target: API_URL,
        trade_log_path: tradeLogPath,
        thresholds: {
            min_preflight_samples: MIN_PREFLIGHT_SAMPLES,
            min_preflight_pass_rate_pct: MIN_PREFLIGHT_PASS_RATE_PCT,
            max_shortfall_block_rate_pct: MAX_SHORTFALL_BLOCK_RATE_PCT,
            max_netting_block_rate_pct: MAX_NETTING_BLOCK_RATE_PCT,
            min_edge_capture_ratio: MIN_EDGE_CAPTURE_RATIO,
            min_net_pnl_usd: MIN_NET_PNL_USD,
            max_reject_rate_pct: MAX_REJECT_RATE_PCT,
            max_stale_forced_closes: MAX_STALE_FORCED_CLOSES,
            max_negative_take_profit_closes: MAX_NEGATIVE_TAKE_PROFIT_CLOSES,
        },
        metrics,
        checks: checkResults.checks,
        pass: checkResults.overall_ok,
        generated_at: new Date(startWall).toISOString(),
    };

    fs.writeFileSync(path.resolve(OUTPUT_PATH), JSON.stringify(report, null, 2), 'utf8');
    fs.writeFileSync(path.resolve(OUTPUT_MD_PATH), toMarkdown(report), 'utf8');
    console.log(JSON.stringify(report, null, 2));
    process.exit(report.pass ? 0 : 1);
}

main().catch((error) => {
    console.error(error);
    process.exit(1);
});
