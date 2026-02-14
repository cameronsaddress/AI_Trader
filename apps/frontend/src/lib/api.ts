function normalizeBaseUrl(raw: unknown): string | null {
  if (typeof raw !== 'string') return null;
  const trimmed = raw.trim();
  if (!trimmed) return null;
  return trimmed.endsWith('/') ? trimmed.slice(0, -1) : trimmed;
}

export function backendOrigin(): string | null {
  return normalizeBaseUrl(import.meta.env.VITE_API_URL);
}

export function apiUrl(path: string): string {
  const base = backendOrigin();
  if (!base) return path;
  if (!path.startsWith('/')) return `${base}/${path}`;
  return `${base}${path}`;
}

export function socketUrl(): string {
  return backendOrigin() || window.location.origin;
}

