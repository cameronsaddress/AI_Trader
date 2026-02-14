import React, { createContext, useContext, useEffect, useState } from 'react';
import { io, Socket } from 'socket.io-client';
import { socketUrl as getSocketUrl } from '../lib/api';

interface SocketContextType {
    socket: Socket | null;
    isConnected: boolean;
    controlPlaneToken: string | null;
    setControlPlaneToken: (token: string | null) => void;
}

const STORAGE_KEY = 'ai_trader_control_plane_token';

const SocketContext = createContext<SocketContextType>({
    socket: null,
    isConnected: false,
    controlPlaneToken: null,
    setControlPlaneToken: () => {},
});

export const useSocket = () => {
    return useContext(SocketContext);
};

export const SocketProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
    const [socket, setSocket] = useState<Socket | null>(null);
    const [isConnected, setIsConnected] = useState(false);
    const [controlPlaneToken, setControlPlaneTokenState] = useState<string | null>(() => {
        if (typeof window === 'undefined') {
            return null;
        }

        const sessionToken = window.sessionStorage.getItem(STORAGE_KEY)?.trim();
        if (sessionToken) {
            return sessionToken;
        }
        return null;
    });

    const setControlPlaneToken = (token: string | null) => {
        const next = (token || '').trim();
        if (typeof window !== 'undefined') {
            if (next) {
                window.sessionStorage.setItem(STORAGE_KEY, next);
            } else {
                window.sessionStorage.removeItem(STORAGE_KEY);
            }
        }
        setControlPlaneTokenState(next || null);
    };

    useEffect(() => {
        // Connect to same origin (Vite proxy forwards to backend in dev).
        const socketUrl = getSocketUrl();

        console.log('[Socket] Connecting to:', socketUrl);

        const newSocket = io(socketUrl, {
            transports: ['polling', 'websocket'],
            reconnection: true,
            reconnectionAttempts: 10,
            reconnectionDelay: 1000,
            withCredentials: true,
            path: '/socket.io/',
            auth: controlPlaneToken ? { token: controlPlaneToken } : undefined,
        });

        newSocket.on('connect', () => {
            console.log('[Socket] Connected:', newSocket.id);
            setIsConnected(true);
        });

        newSocket.on('disconnect', (reason) => {
            console.warn('[Socket] Disconnected:', reason);
            setIsConnected(false);
        });

        newSocket.on('connect_error', (err) => {
            console.error('[Socket] Connection Error:', err.message);
            setIsConnected(false);
        });

        setSocket(newSocket);

        return () => {
            newSocket.disconnect();
        };
    }, [controlPlaneToken]);

    return (
        <SocketContext.Provider value={{ socket, isConnected, controlPlaneToken, setControlPlaneToken }}>
            {children}
        </SocketContext.Provider>
    );
};
