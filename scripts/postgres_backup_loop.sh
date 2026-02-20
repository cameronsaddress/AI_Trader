#!/usr/bin/env bash
set -euo pipefail

POSTGRES_BACKUP_INTERVAL_SEC="${POSTGRES_BACKUP_INTERVAL_SEC:-900}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKUP_SCRIPT_PATH="${POSTGRES_BACKUP_SCRIPT_PATH:-/usr/local/bin/postgres_backup.sh}"

if ! [[ "${POSTGRES_BACKUP_INTERVAL_SEC}" =~ ^[0-9]+$ ]] || (( POSTGRES_BACKUP_INTERVAL_SEC < 60 )); then
    echo "[postgres-backup] POSTGRES_BACKUP_INTERVAL_SEC must be an integer >= 60" >&2
    exit 1
fi

if [[ ! -x "${BACKUP_SCRIPT_PATH}" ]]; then
    if [[ -x "${SCRIPT_DIR}/postgres_backup.sh" ]]; then
        BACKUP_SCRIPT_PATH="${SCRIPT_DIR}/postgres_backup.sh"
    else
        echo "[postgres-backup] backup script not executable: ${BACKUP_SCRIPT_PATH}" >&2
        exit 1
    fi
fi

while true; do
    "${BACKUP_SCRIPT_PATH}" || echo "[postgres-backup] snapshot failed at $(date -u +%Y-%m-%dT%H:%M:%SZ)" >&2
    sleep "${POSTGRES_BACKUP_INTERVAL_SEC}"
done
