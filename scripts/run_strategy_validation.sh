#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
INPUT_PATH="${1:-$ROOT_DIR/apps/backend/logs/strategy_trades.csv}"
OUTPUT_PATH="${2:-$ROOT_DIR/reports/strategy_validation.json}"
WF_SPLITS="${WF_SPLITS:-6}"
MIN_TEST_TRADES="${MIN_TEST_TRADES:-40}"
CSCV_PARTITIONS="${CSCV_PARTITIONS:-8}"

if [ ! -f "$INPUT_PATH" ]; then
  echo "Missing input dataset: $INPUT_PATH" >&2
  exit 1
fi

mkdir -p "$(dirname "$OUTPUT_PATH")"

ROW_COUNT="$(tail -n +2 "$INPUT_PATH" | sed '/^[[:space:]]*$/d' | wc -l | tr -d ' ')"
echo "Running validation on $ROW_COUNT rows from $INPUT_PATH"

python3 "$ROOT_DIR/scripts/strategy_validation.py" \
  --input "$INPUT_PATH" \
  --walk-forward-splits "$WF_SPLITS" \
  --min-test-trades "$MIN_TEST_TRADES" \
  --cscv-partitions "$CSCV_PARTITIONS" \
  --output "$OUTPUT_PATH"

echo "Validation report written to $OUTPUT_PATH"
