#!/usr/bin/env node

/*
Periodic soak checkpoint logger.

Writes JSONL snapshots with trading/risk/model state and optionally triggers
model training when in-memory labeled rows cross a threshold.
*/

const fs = require('fs');
const path = require('path');

const API_URL = process.env.API_URL || 'http://localhost:5114';
const CONTROL_PLANE_TOKEN = process.env.CONTROL_PLANE_TOKEN || '';
const intervalSeconds = Math.max(5, Number(process.env.CHECKPOINT_INTERVAL_SECONDS || '60'));
const INTERVAL_MS = intervalSeconds * 1000;
const OUTPUT_PATH = process.env.CHECKPOINT_OUTPUT_PATH || 'reports/soak_checkpoints.jsonl';
const SOAK_STATS_FILE = process.env.SOAK_STATS_FILE || '';
const TRAIN_LABEL_THRESHOLD = Math.max(1, Number(process.env.TRAIN_LABEL_THRESHOLD || '4'));
const TRAIN_MIN_NEW_LABELS = Math.max(1, Number(process.env.TRAIN_MIN_NEW_LABELS || '1'));
const MAX_ITERATIONS = Math.max(0, Number(process.env.CHECKPOINT_MAX_ITERATIONS || '0'));

function ensureParent(filePath) {
    const parent = path.dirname(path.resolve(filePath));
    fs.mkdirSync(parent, { recursive: true });
}

function countLines(filePath) {
    if (!filePath) {
        return null;
    }
    const resolved = path.resolve(filePath);
    if (!fs.existsSync(resolved)) {
        return null;
    }
    const data = fs.readFileSync(resolved);
    let lines = 0;
    for (let i = 0; i < data.length; i += 1) {
        if (data[i] === 10) {
            lines += 1;
        }
    }
    return lines;
}

async function fetchJson(pathname, options = {}) {
    const method = options.method || 'GET';
    const auth = options.auth === true;
    const body = options.body;
    const headers = { 'content-type': 'application/json' };
    if (auth && CONTROL_PLANE_TOKEN) {
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
        json = null;
    }
    return { ok: response.ok, status: response.status, json };
}

async function main() {
    ensureParent(OUTPUT_PATH);
    const out = fs.createWriteStream(path.resolve(OUTPUT_PATH), { flags: 'a' });
    let lastTrainAttemptLabeledRows = -1;
    let iterations = 0;

    const stop = () => {
        out.end();
        process.exit(0);
    };
    process.on('SIGINT', stop);
    process.on('SIGTERM', stop);

    while (true) {
        iterations += 1;

        const [statsResp, rejectedResp, pipelineResp, featureResp, tradesResp] = await Promise.all([
            fetchJson('/api/arb/stats'),
            fetchJson('/api/arb/rejected-signals?source=file&limit=2000'),
            fetchJson('/api/ml/pipeline-status'),
            fetchJson('/api/ml/feature-registry'),
            fetchJson('/api/arb/validation-trades'),
        ]);

        const stats = statsResp.json || {};
        const rejected = rejectedResp.json || {};
        const pipeline = pipelineResp.json || {};
        const feature = featureResp.json || {};
        const trades = tradesResp.json || {};

        const featureLabeledRows = Number(feature.labeled_rows || 0);
        const modelEligible = Boolean(pipeline.model_eligible);
        let trainAttempted = false;
        let trainResult = null;

        const trainReady = featureLabeledRows >= TRAIN_LABEL_THRESHOLD
            && (lastTrainAttemptLabeledRows < 0
                || (featureLabeledRows - lastTrainAttemptLabeledRows) >= TRAIN_MIN_NEW_LABELS);

        if (trainReady && !modelEligible) {
            trainAttempted = true;
            if (!CONTROL_PLANE_TOKEN) {
                trainResult = { ok: false, error: 'missing CONTROL_PLANE_TOKEN' };
            } else {
                const trainResp = await fetchJson('/api/ml/train-now', {
                    method: 'POST',
                    auth: true,
                    body: { force: false },
                });
                trainResult = {
                    ok: trainResp.ok,
                    status: trainResp.status,
                    response: trainResp.json,
                };
            }
            lastTrainAttemptLabeledRows = featureLabeledRows;
        }

        const row = {
            timestamp: new Date().toISOString(),
            soak_stats_file: SOAK_STATS_FILE || null,
            soak_stats_lines: countLines(SOAK_STATS_FILE),
            trading_mode: stats.trading_mode ?? null,
            live_order_posting_enabled: stats.live_order_posting_enabled ?? null,
            execution_halt: stats.execution_halt ?? null,
            active_signals: stats.active_signals ?? null,
            active_markets: stats.active_markets ?? null,
            rejected_signal_count: Number(rejected.count || 0),
            rejected_by_stage: rejected.by_stage || {},
            validation_trade_rows: Number(trades.rows || 0),
            feature_registry_rows: Number(feature.rows || 0),
            feature_registry_labeled_rows: featureLabeledRows,
            dataset_labeled_rows: Number(pipeline.dataset_labeled_rows || 0),
            model_eligible: modelEligible,
            model_reason: pipeline.model_reason || null,
            train_attempted: trainAttempted,
            train_result: trainResult,
        };

        out.write(`${JSON.stringify(row)}\n`);
        process.stdout.write(`${JSON.stringify(row)}\n`);

        if (MAX_ITERATIONS > 0 && iterations >= MAX_ITERATIONS) {
            break;
        }
        await new Promise((resolve) => setTimeout(resolve, INTERVAL_MS));
    }

    out.end();
}

main().catch((error) => {
    console.error(error);
    process.exit(1);
});

