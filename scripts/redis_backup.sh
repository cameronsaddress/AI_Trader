#!/usr/bin/env bash
set -euo pipefail

REDIS_URL="${REDIS_URL:-redis://localhost:6379}"
REDIS_HOST="${REDIS_HOST:-}"
REDIS_PORT="${REDIS_PORT:-}"
REDIS_BACKUP_DIR="${REDIS_BACKUP_DIR:-backups/redis}"
REDIS_BACKUP_KEEP="${REDIS_BACKUP_KEEP:-288}"
REDIS_BACKUP_S3_URI="${REDIS_BACKUP_S3_URI:-}"
REDIS_BACKUP_S3_RETRIES="${REDIS_BACKUP_S3_RETRIES:-3}"
REDIS_BACKUP_S3_RETRY_DELAY_SEC="${REDIS_BACKUP_S3_RETRY_DELAY_SEC:-3}"
AWS_REGION="${AWS_REGION:-us-east-1}"
REDIS_BACKUP_ALLOW_SIDECAR_FALLBACK="${REDIS_BACKUP_ALLOW_SIDECAR_FALLBACK:-true}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-${ROOT_DIR}/infrastructure/docker-compose.yml}"

if [[ -z "${REDIS_PASSWORD:-}" ]] && [[ "${REDIS_URL}" =~ redis://:([^@]+)@ ]]; then
    REDIS_PASSWORD="${BASH_REMATCH[1]}"
fi

infer_redis_endpoint() {
    local source_url="$1"
    if [[ "${source_url}" =~ ^redis://([^@/]+@)?([^:/?#]+)(:([0-9]+))?(/.*)?$ ]]; then
        if [[ -z "${REDIS_HOST}" ]]; then
            REDIS_HOST="${BASH_REMATCH[2]}"
        fi
        if [[ -z "${REDIS_PORT}" ]]; then
            REDIS_PORT="${BASH_REMATCH[4]:-6379}"
        fi
        return 0
    fi
    return 1
}

if [[ -z "${REDIS_HOST}" ]] || [[ -z "${REDIS_PORT}" ]]; then
    infer_redis_endpoint "${REDIS_URL}" || true
fi

REDIS_HOST="${REDIS_HOST:-localhost}"
REDIS_PORT="${REDIS_PORT:-6379}"

if ! command -v redis-cli >/dev/null 2>&1; then
    if [[ "${REDIS_BACKUP_ALLOW_SIDECAR_FALLBACK}" == "true" ]] \
        && command -v docker >/dev/null 2>&1 \
        && docker compose version >/dev/null 2>&1 \
        && [[ -f "${COMPOSE_FILE}" ]]; then
        if [[ -z "${REDIS_PASSWORD:-}" ]]; then
            echo "[redis-backup] redis-cli missing and REDIS_PASSWORD is not set; cannot run compose fallback safely." >&2
            echo "[redis-backup] export REDIS_PASSWORD (or REDIS_URL with password) and rerun." >&2
            exit 127
        fi
        REDIS_HOST_FOR_SIDECAR="${REDIS_HOST}"
        REDIS_PORT_FOR_SIDECAR="${REDIS_PORT}"
        if [[ "${REDIS_HOST_FOR_SIDECAR}" == "localhost" ]] || [[ "${REDIS_HOST_FOR_SIDECAR}" == "127.0.0.1" ]]; then
            REDIS_HOST_FOR_SIDECAR="redis"
            REDIS_PORT_FOR_SIDECAR="6379"
        fi
        echo "[redis-backup] redis-cli missing on host; running snapshot via redis-backup sidecar" >&2
        POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-unused_for_redis_backup}" \
        docker compose -f "${COMPOSE_FILE}" --profile ops run --rm \
            -e REDIS_PASSWORD="${REDIS_PASSWORD}" \
            -e REDIS_HOST="${REDIS_HOST_FOR_SIDECAR}" \
            -e REDIS_PORT="${REDIS_PORT_FOR_SIDECAR}" \
            -e REDIS_URL="redis://${REDIS_HOST_FOR_SIDECAR}:${REDIS_PORT_FOR_SIDECAR}" \
            redis-backup \
            /bin/sh -lc "apk add --no-cache bash redis aws-cli >/dev/null && /usr/local/bin/redis_backup.sh"
        exit $?
    fi

    echo "[redis-backup] redis-cli not found. Install redis-cli or run backup via compose sidecar:" >&2
    echo "  docker compose -f infrastructure/docker-compose.yml --profile ops run --rm redis-backup /usr/local/bin/redis_backup.sh" >&2
    exit 127
fi

mkdir -p "${REDIS_BACKUP_DIR}"

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
base="redis-${timestamp}"
rdb_path="${REDIS_BACKUP_DIR}/${base}.rdb"
archive_path="${rdb_path}.gz"
checksum_path="${archive_path}.sha256"

if [[ -n "${REDIS_PASSWORD:-}" ]]; then
    REDISCLI_AUTH="${REDIS_PASSWORD}" redis-cli -h "${REDIS_HOST}" -p "${REDIS_PORT}" --rdb "${rdb_path}" >/dev/null
else
    redis-cli -h "${REDIS_HOST}" -p "${REDIS_PORT}" --rdb "${rdb_path}" >/dev/null
fi
gzip -f "${rdb_path}"
gzip -t "${archive_path}"
sha256sum "${archive_path}" > "${checksum_path}"
sha256sum --status -c "${checksum_path}"

if [[ -n "${REDIS_BACKUP_S3_URI}" ]]; then
    if command -v aws >/dev/null 2>&1; then
        if ! [[ "${REDIS_BACKUP_S3_RETRIES}" =~ ^[0-9]+$ ]] || (( REDIS_BACKUP_S3_RETRIES < 1 )); then
            REDIS_BACKUP_S3_RETRIES=3
        fi
        if ! [[ "${REDIS_BACKUP_S3_RETRY_DELAY_SEC}" =~ ^[0-9]+$ ]] || (( REDIS_BACKUP_S3_RETRY_DELAY_SEC < 1 )); then
            REDIS_BACKUP_S3_RETRY_DELAY_SEC=3
        fi
        s3_data_path="${REDIS_BACKUP_S3_URI%/}/${base}.rdb.gz"
        s3_checksum_path="${s3_data_path}.sha256"
        uploaded=0
        attempt=1
        while (( attempt <= REDIS_BACKUP_S3_RETRIES )); do
            if AWS_REGION="${AWS_REGION}" aws s3 cp "${archive_path}" "${s3_data_path}" >/dev/null \
                && AWS_REGION="${AWS_REGION}" aws s3 cp "${checksum_path}" "${s3_checksum_path}" >/dev/null; then
                uploaded=1
                break
            fi
            if (( attempt < REDIS_BACKUP_S3_RETRIES )); then
                echo "[redis-backup] S3 upload attempt ${attempt}/${REDIS_BACKUP_S3_RETRIES} failed; retrying in ${REDIS_BACKUP_S3_RETRY_DELAY_SEC}s" >&2
                sleep "${REDIS_BACKUP_S3_RETRY_DELAY_SEC}"
            fi
            attempt=$((attempt + 1))
        done
        if (( uploaded == 0 )); then
            echo "[redis-backup] failed S3 upload after ${REDIS_BACKUP_S3_RETRIES} attempts: ${s3_data_path}" >&2
            exit 1
        fi
    else
        echo "[redis-backup] aws cli missing; skipped S3 upload for ${archive_path}" >&2
    fi
fi

if [[ "${REDIS_BACKUP_KEEP}" =~ ^[0-9]+$ ]] && (( REDIS_BACKUP_KEEP >= 1 )); then
    mapfile -t old_archives < <(ls -1t "${REDIS_BACKUP_DIR}"/redis-*.rdb.gz 2>/dev/null | tail -n +"$((REDIS_BACKUP_KEEP + 1))")
    if ((${#old_archives[@]} > 0)); then
        rm -f "${old_archives[@]}"
    fi
    mapfile -t old_checksums < <(ls -1t "${REDIS_BACKUP_DIR}"/redis-*.rdb.gz.sha256 2>/dev/null | tail -n +"$((REDIS_BACKUP_KEEP + 1))")
    if ((${#old_checksums[@]} > 0)); then
        rm -f "${old_checksums[@]}"
    fi
fi

echo "[redis-backup] wrote ${archive_path}"
