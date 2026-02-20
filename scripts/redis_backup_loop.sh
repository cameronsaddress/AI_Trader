#!/usr/bin/env bash
set -euo pipefail

REDIS_BACKUP_INTERVAL_SEC="${REDIS_BACKUP_INTERVAL_SEC:-300}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKUP_SCRIPT_PATH="${REDIS_BACKUP_SCRIPT_PATH:-/usr/local/bin/redis_backup.sh}"

if ! [[ "${REDIS_BACKUP_INTERVAL_SEC}" =~ ^[0-9]+$ ]] || (( REDIS_BACKUP_INTERVAL_SEC < 30 )); then
    echo "[redis-backup] REDIS_BACKUP_INTERVAL_SEC must be an integer >= 30" >&2
    exit 1
fi

if [[ ! -x "${BACKUP_SCRIPT_PATH}" ]]; then
    if [[ -x "${SCRIPT_DIR}/redis_backup.sh" ]]; then
        BACKUP_SCRIPT_PATH="${SCRIPT_DIR}/redis_backup.sh"
    else
        echo "[redis-backup] backup script not executable: ${BACKUP_SCRIPT_PATH}" >&2
        exit 1
    fi
fi

while true; do
    "${BACKUP_SCRIPT_PATH}" || echo "[redis-backup] snapshot failed at $(date -u +%Y-%m-%dT%H:%M:%SZ)" >&2
    sleep "${REDIS_BACKUP_INTERVAL_SEC}"
done
