import fs from 'fs/promises';
import os from 'os';
import path from 'path';
import { RejectedSignalRecorder } from './RejectedSignalRecorder';
import type { RejectedSignalRecord } from '../modules/execution/arbExecutionPipeline';

describe('RejectedSignalRecorder', () => {
    let tempDir: string;
    let logPath: string;

    beforeEach(async () => {
        tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'rejected-signal-recorder-'));
        logPath = path.join(tempDir, 'rejected_signals.jsonl');
    });

    afterEach(async () => {
        await fs.rm(tempDir, { recursive: true, force: true });
    });

    it('records and replays rejected-signal rows', async () => {
        const recorder = new RejectedSignalRecorder(logPath);
        const row: RejectedSignalRecord = {
            stage: 'MODEL_GATE',
            execution_id: 'exec-1',
            strategy: 'ATOMIC_ARB',
            market_key: 'market-1',
            reason: 'probability below gate',
            mode: 'LIVE',
            timestamp: Date.now(),
            payload: { probability: 0.48, gate: 0.55 },
        };

        recorder.record(row);

        expect(await recorder.countRows()).toBe(1);
        const replay = await recorder.readRecent(10);
        expect(replay).toHaveLength(1);
        expect(replay[0]?.execution_id).toBe('exec-1');
        expect(replay[0]?.stage).toBe('MODEL_GATE');
    });

    it('ignores malformed rows and supports reset', async () => {
        const recorder = new RejectedSignalRecorder(logPath);
        recorder.record({ stage: 'INVALID', execution_id: 'exec-bad' });
        expect(await recorder.countRows()).toBe(0);

        recorder.record({
            stage: 'PREFLIGHT',
            execution_id: 'exec-2',
            reason: 'preflight signing error',
            mode: 'PAPER',
            timestamp: Date.now(),
            payload: null,
        });
        expect(await recorder.countRows()).toBe(1);

        await recorder.reset();
        expect(await recorder.countRows()).toBe(0);
        expect(await recorder.readRecent(10)).toHaveLength(0);
    });
});

