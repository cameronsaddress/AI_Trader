import { EventEmitter } from 'events';
import { CoinbaseClient } from './CoinbaseClient';
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

function getLatestSocket(): MockWebSocketInstance {
    const sockets = getMockWebSocketClass().instances;
    return sockets[sockets.length - 1]!;
}

describe('CoinbaseClient', () => {
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

    it('queues subscriptions while disconnected and replays on open', () => {
        const client = new CoinbaseClient({
            apiKey: 'key',
            apiSecret: 'secret',
        });

        client.connect();
        const socket = getLatestSocket();
        client.subscribe(['BTC-USD'], ['ticker']);

        socket.readyState = getMockWebSocketClass().OPEN;
        socket.emit('open');

        expect(socket.send).toHaveBeenCalledTimes(1);
        const payload = JSON.parse(socket.send.mock.calls[0]![0]) as {
            type: string;
            product_ids: string[];
            channels: string[];
        };
        expect(payload.type).toBe('subscribe');
        expect(payload.product_ids).toEqual(['BTC-USD']);
        expect(payload.channels).toEqual(['ticker']);
    });

    it('schedules reconnect on close and cancels it on disconnect', () => {
        const client = new CoinbaseClient({
            apiKey: 'key',
            apiSecret: 'secret',
        });

        client.connect();
        const socket = getLatestSocket();
        socket.emit('close');

        jest.advanceTimersByTime(1000);
        expect(getMockWebSocketClass().instances).toHaveLength(1);

        client.disconnect();
        jest.advanceTimersByTime(10_000);
        expect(getMockWebSocketClass().instances).toHaveLength(1);
    });

    it('does not reconnect after manual disconnect closes the socket', () => {
        const client = new CoinbaseClient({
            apiKey: 'key',
            apiSecret: 'secret',
        });

        client.connect();
        const socket = getLatestSocket();
        socket.readyState = getMockWebSocketClass().OPEN;
        socket.emit('open');

        client.disconnect();
        jest.advanceTimersByTime(30_000);

        expect(getMockWebSocketClass().instances).toHaveLength(1);
    });
});
