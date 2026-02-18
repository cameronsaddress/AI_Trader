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
        scheduleNonOverlappingTask('SyncThrow', 10, () => {
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
    });

    it('uses a safe default interval when input interval is invalid', async () => {
        const task = jest.fn(async () => undefined);
        scheduleNonOverlappingTask('BadInterval', Number.NaN, task);

        jest.advanceTimersByTime(999);
        await flushMicrotasks();
        expect(task).not.toHaveBeenCalled();

        jest.advanceTimersByTime(1);
        await flushMicrotasks();
        expect(task).toHaveBeenCalledTimes(1);
        expect(logger.warn).toHaveBeenCalled();
    });
});
