function normalizeBaseUrl(raw: unknown): string | null {
  if (typeof raw !== 'string') return null;
  const trimmed = raw.trim();
  if (!trimmed) return null;
  return trimmed.endsWith('/') ? trimmed.slice(0, -1) : trimmed;
}

const TOKEN_STORAGE_KEY = 'ai_trader_control_plane_token';
const TOKEN_SET_AT_KEY = 'ai_trader_token_set_at';
const TOKEN_MAX_AGE_MS = 24 * 60 * 60 * 1000;

export function backendOrigin(): string | null {
  return normalizeBaseUrl(import.meta.env.VITE_API_URL);
}

function browserControlToken(): string | null {
  if (typeof window === 'undefined') return null;
  try {
    const sessionToken = window.sessionStorage.getItem(TOKEN_STORAGE_KEY)?.trim() || '';
    const setAt = Number(window.sessionStorage.getItem(TOKEN_SET_AT_KEY) || '0');
    if (sessionToken) {
      if (setAt > 0 && Date.now() - setAt > TOKEN_MAX_AGE_MS) {
        window.sessionStorage.removeItem(TOKEN_STORAGE_KEY);
        window.sessionStorage.removeItem(TOKEN_SET_AT_KEY);
        return null;
      }
      return sessionToken;
    }

    // Backward-compat migration: localStorage -> sessionStorage with TTL.
    const legacy = window.localStorage.getItem(TOKEN_STORAGE_KEY)?.trim() || '';
    if (!legacy) return null;
    window.localStorage.removeItem(TOKEN_STORAGE_KEY);
    window.sessionStorage.setItem(TOKEN_STORAGE_KEY, legacy);
    window.sessionStorage.setItem(TOKEN_SET_AT_KEY, String(Date.now()));
    return legacy;
  } catch {
    return null;
  }
}

export function apiUrl(path: string): string {
  const base = backendOrigin();
  if (!base) return path;
  if (!path.startsWith('/')) return `${base}/${path}`;
  return `${base}${path}`;
}

export function apiAuthHeaders(headers?: HeadersInit): HeadersInit {
  const token = browserControlToken();
  if (!token) return headers ?? {};
  const merged = new Headers(headers ?? {});
  if (!merged.has('authorization')) {
    merged.set('authorization', `Bearer ${token}`);
  }
  return merged;
}

export function apiFetch(path: string, init: RequestInit = {}): Promise<Response> {
  return fetch(apiUrl(path), {
    ...init,
    headers: apiAuthHeaders(init.headers),
  });
}

export function socketUrl(): string {
  return backendOrigin() || window.location.origin;
}
