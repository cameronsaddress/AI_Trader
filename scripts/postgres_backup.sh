#!/usr/bin/env bash
set -euo pipefail

POSTGRES_HOST="${POSTGRES_HOST:-postgres}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-}"
POSTGRES_DB="${POSTGRES_DB:-ai_trader}"
POSTGRES_BACKUP_DIR="${POSTGRES_BACKUP_DIR:-backups/postgres}"
POSTGRES_BACKUP_KEEP="${POSTGRES_BACKUP_KEEP:-168}"
POSTGRES_BACKUP_S3_URI="${POSTGRES_BACKUP_S3_URI:-}"
POSTGRES_BACKUP_S3_RETRIES="${POSTGRES_BACKUP_S3_RETRIES:-3}"
POSTGRES_BACKUP_S3_RETRY_DELAY_SEC="${POSTGRES_BACKUP_S3_RETRY_DELAY_SEC:-3}"
AWS_REGION="${AWS_REGION:-us-east-1}"

if [[ -z "${POSTGRES_PASSWORD}" ]]; then
    echo "[postgres-backup] POSTGRES_PASSWORD is required" >&2
    exit 2
fi

if ! command -v pg_dump >/dev/null 2>&1; then
    echo "[postgres-backup] pg_dump not found" >&2
    exit 127
fi

mkdir -p "${POSTGRES_BACKUP_DIR}"

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
base="postgres-${POSTGRES_DB}-${timestamp}.sql.gz"
archive_path="${POSTGRES_BACKUP_DIR}/${base}"
checksum_path="${archive_path}.sha256"

PGPASSWORD="${POSTGRES_PASSWORD}" \
    pg_dump \
    --format=plain \
    --no-owner \
    --no-privileges \
    --host="${POSTGRES_HOST}" \
    --port="${POSTGRES_PORT}" \
    --username="${POSTGRES_USER}" \
    "${POSTGRES_DB}" \
    | gzip -9 > "${archive_path}"

gzip -t "${archive_path}"
sha256sum "${archive_path}" > "${checksum_path}"
sha256sum --status -c "${checksum_path}"

if [[ -n "${POSTGRES_BACKUP_S3_URI}" ]]; then
    if command -v aws >/dev/null 2>&1; then
        if ! [[ "${POSTGRES_BACKUP_S3_RETRIES}" =~ ^[0-9]+$ ]] || (( POSTGRES_BACKUP_S3_RETRIES < 1 )); then
            POSTGRES_BACKUP_S3_RETRIES=3
        fi
        if ! [[ "${POSTGRES_BACKUP_S3_RETRY_DELAY_SEC}" =~ ^[0-9]+$ ]] || (( POSTGRES_BACKUP_S3_RETRY_DELAY_SEC < 1 )); then
            POSTGRES_BACKUP_S3_RETRY_DELAY_SEC=3
        fi
        s3_data_path="${POSTGRES_BACKUP_S3_URI%/}/${base}"
        s3_checksum_path="${s3_data_path}.sha256"
        uploaded=0
        attempt=1
        while (( attempt <= POSTGRES_BACKUP_S3_RETRIES )); do
            if AWS_REGION="${AWS_REGION}" aws s3 cp "${archive_path}" "${s3_data_path}" >/dev/null \
                && AWS_REGION="${AWS_REGION}" aws s3 cp "${checksum_path}" "${s3_checksum_path}" >/dev/null; then
                uploaded=1
                break
            fi
            if (( attempt < POSTGRES_BACKUP_S3_RETRIES )); then
                echo "[postgres-backup] S3 upload attempt ${attempt}/${POSTGRES_BACKUP_S3_RETRIES} failed; retrying in ${POSTGRES_BACKUP_S3_RETRY_DELAY_SEC}s" >&2
                sleep "${POSTGRES_BACKUP_S3_RETRY_DELAY_SEC}"
            fi
            attempt=$((attempt + 1))
        done
        if (( uploaded == 0 )); then
            echo "[postgres-backup] failed S3 upload after ${POSTGRES_BACKUP_S3_RETRIES} attempts: ${s3_data_path}" >&2
            exit 1
        fi
    else
        echo "[postgres-backup] aws cli missing; skipped S3 upload for ${archive_path}" >&2
    fi
fi

if [[ "${POSTGRES_BACKUP_KEEP}" =~ ^[0-9]+$ ]] && (( POSTGRES_BACKUP_KEEP >= 1 )); then
    mapfile -t old_archives < <(ls -1t "${POSTGRES_BACKUP_DIR}"/postgres-*.sql.gz 2>/dev/null | tail -n +"$((POSTGRES_BACKUP_KEEP + 1))")
    if ((${#old_archives[@]} > 0)); then
        rm -f "${old_archives[@]}"
    fi
    mapfile -t old_checksums < <(ls -1t "${POSTGRES_BACKUP_DIR}"/postgres-*.sql.gz.sha256 2>/dev/null | tail -n +"$((POSTGRES_BACKUP_KEEP + 1))")
    if ((${#old_checksums[@]} > 0)); then
        rm -f "${old_checksums[@]}"
    fi
fi

echo "[postgres-backup] wrote ${archive_path}"
