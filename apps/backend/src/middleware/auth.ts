import { Request, Response, NextFunction } from 'express';
import { timingSafeEqual } from 'crypto';

export interface AuthRequest extends Request {
    user?: unknown;
}

export function extractBearerToken(raw: unknown): string | null {
    if (typeof raw !== 'string') {
        return null;
    }

    const trimmed = raw.trim();
    if (!trimmed) {
        return null;
    }

    if (trimmed.toLowerCase().startsWith('bearer ')) {
        const token = trimmed.slice(7).trim();
        return token.length > 0 ? token : null;
    }
    return trimmed;
}

export function validateControlPlaneToken(token: string | null, expected: string): boolean {
    if (!token || !expected || expected.length === 0) {
        return false;
    }
    const leftBuffer = Buffer.from(token, 'utf8');
    const rightBuffer = Buffer.from(expected, 'utf8');
    if (leftBuffer.length !== rightBuffer.length) {
        // Keep timing profile stable for mismatch lengths.
        const maxLength = Math.max(leftBuffer.length, rightBuffer.length);
        const leftPadded = Buffer.alloc(maxLength);
        const rightPadded = Buffer.alloc(maxLength);
        leftBuffer.copy(leftPadded);
        rightBuffer.copy(rightPadded);
        timingSafeEqual(leftPadded, rightPadded);
        return false;
    }
    return timingSafeEqual(leftBuffer, rightBuffer);
}

export const authenticate = (req: AuthRequest, res: Response, next: NextFunction) => {
    const expected = (process.env.CONTROL_PLANE_TOKEN || '').trim();
    if (!expected) {
        res.status(503).json({ error: 'Control plane token is not configured' });
        return;
    }

    const authHeader = extractBearerToken(req.header('Authorization'));
    const directHeader = extractBearerToken(req.header('x-control-plane-token'));
    const token = authHeader || directHeader;

    if (!validateControlPlaneToken(token, expected)) {
        res.status(401).json({ error: 'Unauthorized' });
        return;
    }

    next();
};
