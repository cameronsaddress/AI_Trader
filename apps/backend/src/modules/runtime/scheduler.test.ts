import { scheduleNonOverlappingTask } from './scheduler';
import { logger } from '../../utils/logger';

async function flushMicrotasks(): Promise<void> {
    await Promise.resolve();
    await Promise.resolve();
}

describe('scheduleNonOverlappingTask', () => {
    beforeEach(() => {
        jest.useFakeTimers();
        jest.spyOn(logger, 'error').mockImplementation(() => logger);
        jest.spyOn(logger, 'warn').mockImplementation(() => logger);
    });

    afterEach(() => {
        jest.runOnlyPendingTimers();
        jest.useRealTimers();
        jest.restoreAllMocks();
    });

    it('clears inFlight even when task throws synchronously', async () => {
        let calls = 0;
        const stop = scheduleNonOverlappingTask('SyncThrow', 10, () => {
            calls += 1;
            if (calls === 1) {
                throw new Error('boom');
            }
            return Promise.resolve();
        });

        jest.advanceTimersByTime(11);
        await flushMicrotasks();
        expect(calls).toBe(1);

        jest.advanceTimersByTime(11);
        await flushMicrotasks();
        expect(calls).toBe(2);
        stop();
    });

    it('uses a safe default interval when input interval is invalid', async () => {
        const task = jest.fn(async () => undefined);
        const stop = scheduleNonOverlappingTask('BadInterval', Number.NaN, task);

        jest.advanceTimersByTime(999);
        await flushMicrotasks();
        expect(task).not.toHaveBeenCalled();

        jest.advanceTimersByTime(1);
        await flushMicrotasks();
        expect(task).toHaveBeenCalledTimes(1);
        expect(logger.warn).toHaveBeenCalled();
        stop();
    });

    it('returns an idempotent stop function that prevents future runs', async () => {
        const task = jest.fn(async () => undefined);
        const stop = scheduleNonOverlappingTask('StopTask', 10, task);

        jest.advanceTimersByTime(11);
        await flushMicrotasks();
        expect(task).toHaveBeenCalledTimes(1);

        stop();
        stop();
        jest.advanceTimersByTime(100);
        await flushMicrotasks();
        expect(task).toHaveBeenCalledTimes(1);
    });
});
