import { describe, expect, it } from 'vitest';
import { SequencedAbortController } from './SequencedAbortController';

describe('SequencedAbortController', () => {
    it('aborts the previous request when a new request begins', () => {
        const controller = new SequencedAbortController();

        const first = controller.begin();
        expect(first.seq).toBe(1);
        expect(first.signal.aborted).toBe(false);
        expect(controller.isCurrent(first.seq)).toBe(true);

        const second = controller.begin();
        expect(second.seq).toBe(2);
        expect(controller.isCurrent(second.seq)).toBe(true);
        expect(controller.isCurrent(first.seq)).toBe(false);
        expect(first.signal.aborted).toBe(true);
        expect(second.signal.aborted).toBe(false);
    });

    it('only completes the active request sequence', () => {
        const controller = new SequencedAbortController();
        const first = controller.begin();
        const second = controller.begin();

        expect(controller.complete(first.seq)).toBe(false);
        expect(controller.complete(second.seq)).toBe(true);
        expect(controller.currentSeq()).toBe(second.seq);
    });

    it('cancels and invalidates all in-flight requests', () => {
        const controller = new SequencedAbortController();
        const first = controller.begin();

        controller.cancelAll();
        expect(first.signal.aborted).toBe(true);
        expect(controller.isCurrent(first.seq)).toBe(false);
        expect(controller.currentSeq()).toBe(first.seq + 1);
    });
});
