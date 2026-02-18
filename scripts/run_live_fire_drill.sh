#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-$ROOT_DIR/infrastructure/docker-compose.yml}"
API_URL="${API_URL:-http://localhost:5114}"
REDIS_URL="${REDIS_URL:-redis://localhost:6491}"

TOKEN="$(docker compose -f "$COMPOSE_FILE" exec -T backend sh -lc 'printf "%s" "$CONTROL_PLANE_TOKEN"')"
if [[ -z "${TOKEN}" ]]; then
  echo "CONTROL_PLANE_TOKEN is not set in backend container." >&2
  exit 2
fi

CONTROL_PLANE_TOKEN="$TOKEN" API_URL="$API_URL" REDIS_URL="$REDIS_URL" \
  node "$ROOT_DIR/scripts/live_fire_drill.js"
