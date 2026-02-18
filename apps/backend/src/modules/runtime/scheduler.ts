import { logger } from '../../utils/logger';

export type ScheduledTaskStop = () => void;

export function scheduleNonOverlappingTask(taskName: string, intervalMs: number, task: () => Promise<void>): ScheduledTaskStop {
    const safeIntervalMs = Number.isFinite(intervalMs) && intervalMs > 0
        ? Math.floor(intervalMs)
        : 1000;
    if (safeIntervalMs !== intervalMs) {
        logger.warn(`[${taskName}] invalid interval "${intervalMs}", defaulting to ${safeIntervalMs}ms`);
    }

    let inFlight = false;
    const intervalId = setInterval(() => {
        if (inFlight) {
            return;
        }
        inFlight = true;
        void Promise.resolve()
            .then(() => task())
            .catch((error) => {
                logger.error(`[${taskName}] interval error: ${String(error)}`);
            })
            .finally(() => {
                inFlight = false;
            });
    }, safeIntervalMs);

    let stopped = false;
    return () => {
        if (stopped) {
            return;
        }
        stopped = true;
        clearInterval(intervalId);
    };
}
