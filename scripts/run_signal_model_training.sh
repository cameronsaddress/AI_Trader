#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FEATURE_CSV="${1:-$ROOT_DIR/reports/feature_dataset.csv}"
MODEL_PATH="${2:-$ROOT_DIR/reports/models/signal_model_latest.json}"
REPORT_PATH="${3:-$ROOT_DIR/reports/models/signal_model_report.json}"
SPLITS="${SPLITS:-5}"
PURGE_ROWS="${PURGE_ROWS:-24}"
EMBARGO_ROWS="${EMBARGO_ROWS:-24}"

python3 "$ROOT_DIR/scripts/train_signal_model.py" \
  --input-csv "$FEATURE_CSV" \
  --output-model "$MODEL_PATH" \
  --output-report "$REPORT_PATH" \
  --splits "$SPLITS" \
  --purge "$PURGE_ROWS" \
  --embargo "$EMBARGO_ROWS"

