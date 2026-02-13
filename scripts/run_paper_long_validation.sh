#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE_FILE="${COMPOSE_FILE:-$ROOT_DIR/infrastructure/docker-compose.yml}"
API_URL="${API_URL:-http://localhost:5114}"
DURATION_SECONDS="${DURATION_SECONDS:-7200}"
SAMPLE_INTERVAL_SECONDS="${SAMPLE_INTERVAL_SECONDS:-5}"
BASELINE_BANKROLL="${BASELINE_BANKROLL:-1000}"
MIN_TRADES_FOR_PROMOTION="${MIN_TRADES_FOR_PROMOTION:-25}"
OUTPUT_PATH="${OUTPUT_PATH:-$ROOT_DIR/reports/paper_long_validation_report.json}"
STATS_JSONL="${STATS_JSONL:-$ROOT_DIR/reports/paper_long_validation_stats.jsonl}"
TRADE_LOG_PATH="${TRADE_LOG_PATH:-$ROOT_DIR/apps/backend/logs/strategy_trades.csv}"

TOKEN="$(docker compose -f "$COMPOSE_FILE" exec -T backend sh -lc 'printf "%s" "$CONTROL_PLANE_TOKEN"')"
if [[ -z "${TOKEN}" ]]; then
  echo "CONTROL_PLANE_TOKEN is not set in backend container." >&2
  exit 2
fi

python3 "$ROOT_DIR/scripts/paper_long_validation.py" \
  --api-url "$API_URL" \
  --token "$TOKEN" \
  --duration-seconds "$DURATION_SECONDS" \
  --sample-interval-seconds "$SAMPLE_INTERVAL_SECONDS" \
  --baseline-bankroll "$BASELINE_BANKROLL" \
  --min-trades-for-promotion "$MIN_TRADES_FOR_PROMOTION" \
  --trade-log-path "$TRADE_LOG_PATH" \
  --output "$OUTPUT_PATH" \
  --stats-jsonl "$STATS_JSONL"
