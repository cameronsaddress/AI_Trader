import { classifyClosureReason } from './StrategyTradeRecorder';

describe('classifyClosureReason', () => {
    test('marks TIME_EXIT as timeout and label-eligible', () => {
        expect(classifyClosureReason('TIME_EXIT')).toEqual({
            closure_class: 'TIMEOUT',
            label_eligible: true,
        });
    });

    test('marks EXPIRY_EXIT as signal and label-eligible', () => {
        expect(classifyClosureReason('EXPIRY_EXIT')).toEqual({
            closure_class: 'SIGNAL',
            label_eligible: true,
        });
    });

    test('marks stale forced closes as not label-eligible', () => {
        expect(classifyClosureReason('STALE_FORCE_CLOSE')).toEqual({
            closure_class: 'FORCED',
            label_eligible: false,
        });
    });
});
