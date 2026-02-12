import { Request, Response, NextFunction } from 'express';

export interface AuthRequest extends Request {
    user?: any;
}

function parseToken(raw: string | undefined): string | null {
    if (!raw) {
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

export const authenticate = (req: AuthRequest, res: Response, next: NextFunction) => {
    const expected = (process.env.CONTROL_PLANE_TOKEN || '').trim();
    if (!expected) {
        res.status(503).json({ error: 'Control plane token is not configured' });
        return;
    }

    const authHeader = parseToken(req.header('Authorization'));
    const directHeader = parseToken(req.header('x-control-plane-token'));
    const token = authHeader || directHeader;

    if (token !== expected) {
        res.status(401).json({ error: 'Unauthorized' });
        return;
    }

    next();
};
