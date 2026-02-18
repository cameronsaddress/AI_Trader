import { decideBootResumeAction, shouldResumeFromBootTimer } from './bootResumeDecider';

describe('bootResumeDecider', () => {
    describe('decideBootResumeAction', () => {
        it('returns NONE when strategy is not disabled', () => {
            const decision = decideBootResumeAction({
                current_enabled: '1',
                enabled_by_default: true,
                now_ms: 10_000,
                pause_until_ms: null,
                legacy_cooldown_until_ms: null,
            });

            expect(decision).toEqual({ action: 'NONE' });
        });

        it('returns NONE for default-disabled strategies', () => {
            const decision = decideBootResumeAction({
                current_enabled: '0',
                enabled_by_default: false,
                now_ms: 10_000,
                pause_until_ms: 12_000,
                legacy_cooldown_until_ms: null,
            });

            expect(decision).toEqual({ action: 'NONE' });
        });

        it('preserves manual disable when no pause markers exist', () => {
            const decision = decideBootResumeAction({
                current_enabled: '0',
                enabled_by_default: true,
                now_ms: 10_000,
                pause_until_ms: null,
                legacy_cooldown_until_ms: null,
            });

            expect(decision).toEqual({ action: 'PRESERVE_MANUAL_DISABLE' });
        });

        it('re-enables immediately when pause marker already expired', () => {
            const decision = decideBootResumeAction({
                current_enabled: '0',
                enabled_by_default: true,
                now_ms: 10_000,
                pause_until_ms: 9_999,
                legacy_cooldown_until_ms: null,
            });

            expect(decision).toEqual({ action: 'REENABLE_NOW' });
        });

        it('schedules resume when pause marker is in the future', () => {
            const decision = decideBootResumeAction({
                current_enabled: '0',
                enabled_by_default: true,
                now_ms: 10_000,
                pause_until_ms: 13_500,
                legacy_cooldown_until_ms: null,
            });

            expect(decision).toEqual({
                action: 'SCHEDULE_RESUME',
                expected_pause_until: 13_500,
                remaining_ms: 3_500,
            });
        });

        it('uses legacy cooldown marker when pause marker is absent', () => {
            const decision = decideBootResumeAction({
                current_enabled: '0',
                enabled_by_default: true,
                now_ms: 10_000,
                pause_until_ms: null,
                legacy_cooldown_until_ms: 10_500,
            });

            expect(decision).toEqual({
                action: 'SCHEDULE_RESUME',
                expected_pause_until: 10_500,
                remaining_ms: 500,
            });
        });
    });

    describe('shouldResumeFromBootTimer', () => {
        it('returns false when strategy is no longer disabled', () => {
            const shouldResume = shouldResumeFromBootTimer({
                current_enabled: '1',
                now_ms: 10_000,
                expected_pause_until_ms: 9_000,
                pause_until_ms: 9_000,
                legacy_cooldown_until_ms: null,
            });

            expect(shouldResume).toBe(false);
        });

        it('returns false when pause marker disappeared', () => {
            const shouldResume = shouldResumeFromBootTimer({
                current_enabled: '0',
                now_ms: 10_000,
                expected_pause_until_ms: 9_000,
                pause_until_ms: null,
                legacy_cooldown_until_ms: null,
            });

            expect(shouldResume).toBe(false);
        });

        it('returns false when marker still indicates active pause', () => {
            const shouldResume = shouldResumeFromBootTimer({
                current_enabled: '0',
                now_ms: 10_000,
                expected_pause_until_ms: 12_000,
                pause_until_ms: 12_000,
                legacy_cooldown_until_ms: null,
            });

            expect(shouldResume).toBe(false);
        });

        it('returns false when pause marker changed', () => {
            const shouldResume = shouldResumeFromBootTimer({
                current_enabled: '0',
                now_ms: 10_000,
                expected_pause_until_ms: 9_500,
                pause_until_ms: 9_000,
                legacy_cooldown_until_ms: null,
            });

            expect(shouldResume).toBe(false);
        });

        it('returns true when expected pause marker has elapsed', () => {
            const shouldResume = shouldResumeFromBootTimer({
                current_enabled: '0',
                now_ms: 10_000,
                expected_pause_until_ms: 9_500,
                pause_until_ms: 9_500,
                legacy_cooldown_until_ms: null,
            });

            expect(shouldResume).toBe(true);
        });
    });
});
