export type BootResumeDecision =
    | { action: 'NONE' }
    | { action: 'PRESERVE_MANUAL_DISABLE' }
    | { action: 'REENABLE_NOW' }
    | { action: 'SCHEDULE_RESUME'; expected_pause_until: number; remaining_ms: number };

export type BootResumeDecisionInput = {
    current_enabled: string | null;
    enabled_by_default: boolean;
    now_ms: number;
    pause_until_ms: number | null;
    legacy_cooldown_until_ms: number | null;
};

export type BootResumeTimerCheckInput = {
    current_enabled: string | null;
    now_ms: number;
    expected_pause_until_ms: number;
    pause_until_ms: number | null;
    legacy_cooldown_until_ms: number | null;
};

export function decideBootResumeAction(input: BootResumeDecisionInput): BootResumeDecision {
    if (input.current_enabled !== '0' || !input.enabled_by_default) {
        return { action: 'NONE' };
    }

    const hasPauseMarker = input.pause_until_ms !== null || input.legacy_cooldown_until_ms !== null;
    if (!hasPauseMarker) {
        return { action: 'PRESERVE_MANUAL_DISABLE' };
    }

    const effectivePauseUntil = input.pause_until_ms ?? input.legacy_cooldown_until_ms;
    if (effectivePauseUntil === null || effectivePauseUntil <= input.now_ms) {
        return { action: 'REENABLE_NOW' };
    }

    return {
        action: 'SCHEDULE_RESUME',
        expected_pause_until: effectivePauseUntil,
        remaining_ms: Math.max(1, effectivePauseUntil - input.now_ms),
    };
}

export function shouldResumeFromBootTimer(input: BootResumeTimerCheckInput): boolean {
    if (input.current_enabled !== '0') {
        return false;
    }
    const latestPauseUntil = input.pause_until_ms ?? input.legacy_cooldown_until_ms;
    if (latestPauseUntil === null) {
        return false;
    }
    if (latestPauseUntil > input.now_ms) {
        return false;
    }
    return latestPauseUntil === input.expected_pause_until_ms;
}
