import { EventEmitter } from 'events';
import { HyperliquidClient } from './HyperliquidClient';
import { logger } from '../utils/logger';

type MockWebSocketInstance = EventEmitter & {
    readyState: number;
    send: jest.Mock<void, [string]>;
    close: jest.Mock<void, []>;
};

type MockWebSocketClass = {
    new (url: string): MockWebSocketInstance;
    OPEN: number;
    CONNECTING: number;
    CLOSED: number;
    instances: MockWebSocketInstance[];
};

jest.mock('ws', () => {
    class MockWebSocket extends EventEmitter {
        static OPEN = 1;
        static CONNECTING = 0;
        static CLOSED = 3;
        static instances: MockWebSocket[] = [];

        public readyState = MockWebSocket.CONNECTING;
        public send = jest.fn<void, [string]>();
        public close = jest.fn<void, []>(() => {
            this.readyState = MockWebSocket.CLOSED;
            this.emit('close');
        });

        constructor(_url: string) {
            super();
            MockWebSocket.instances.push(this);
        }
    }

    return {
        __esModule: true,
        default: MockWebSocket,
    };
});

function getMockWebSocketClass(): MockWebSocketClass {
    return (jest.requireMock('ws') as { default: MockWebSocketClass }).default;
}

function latestSocket(): MockWebSocketInstance {
    const sockets = getMockWebSocketClass().instances;
    return sockets[sockets.length - 1]!;
}

describe('HyperliquidClient', () => {
    beforeEach(() => {
        jest.useFakeTimers();
        jest.spyOn(logger, 'info').mockImplementation(() => logger);
        jest.spyOn(logger, 'warn').mockImplementation(() => logger);
        jest.spyOn(logger, 'error').mockImplementation(() => logger);
        getMockWebSocketClass().instances.length = 0;
    });

    afterEach(() => {
        jest.runOnlyPendingTimers();
        jest.useRealTimers();
        jest.restoreAllMocks();
    });

    it('replays queued subscriptions on open', () => {
        const client = new HyperliquidClient();
        client.connect();
        const socket = latestSocket();

        client.subscribe(['BTC-USD']);
        expect(socket.send).not.toHaveBeenCalled();

        socket.readyState = getMockWebSocketClass().OPEN;
        socket.emit('open');

        expect(socket.send).toHaveBeenCalledWith(JSON.stringify({
            method: 'subscribe',
            subscription: {
                type: 'l2Book',
                coin: 'BTC',
            },
        }));
        expect(socket.send).toHaveBeenCalledWith(JSON.stringify({
            method: 'subscribe',
            subscription: {
                type: 'trades',
                coin: 'BTC',
            },
        }));
        expect(socket.send).toHaveBeenCalledWith(JSON.stringify({
            method: 'subscribe',
            subscription: {
                type: 'candle',
                coin: 'BTC',
                interval: '1m',
            },
        }));
    });

    it('schedules reconnect after unexpected close', () => {
        const client = new HyperliquidClient();
        client.connect();
        const socket = latestSocket();

        socket.readyState = getMockWebSocketClass().OPEN;
        socket.emit('open');
        socket.emit('close');

        expect(getMockWebSocketClass().instances).toHaveLength(1);
        jest.advanceTimersByTime(4999);
        expect(getMockWebSocketClass().instances).toHaveLength(1);
        jest.advanceTimersByTime(1);
        expect(getMockWebSocketClass().instances).toHaveLength(2);
    });

    it('does not reconnect after manual disconnect', () => {
        const client = new HyperliquidClient();
        client.connect();
        const socket = latestSocket();

        socket.readyState = getMockWebSocketClass().OPEN;
        socket.emit('open');
        client.disconnect();

        jest.advanceTimersByTime(10_000);
        expect(getMockWebSocketClass().instances).toHaveLength(1);
    });
});
