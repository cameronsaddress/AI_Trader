import { Request, Response, NextFunction } from 'express';

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
    if (!expected || expected.length === 0) {
        return false;
    }
    return token === expected;
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
