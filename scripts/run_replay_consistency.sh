#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INPUT_PATH="${1:-$ROOT_DIR/apps/backend/logs/strategy_trades.csv}"
OUTPUT_PATH="${2:-$ROOT_DIR/reports/replay_consistency_report.json}"
MODE_FILTER="${MODE_FILTER:-PAPER}"
STARTING_EQUITY="${STARTING_EQUITY:-1000}"

python3 "$ROOT_DIR/scripts/replay_consistency.py" \
  --input "$INPUT_PATH" \
  --output "$OUTPUT_PATH" \
  --mode "$MODE_FILTER" \
  --starting-equity "$STARTING_EQUITY"

