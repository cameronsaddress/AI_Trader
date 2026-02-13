#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INPUT_PATH="${1:-$ROOT_DIR/apps/backend/logs/feature_registry_events.jsonl}"
OUTPUT_CSV="${2:-$ROOT_DIR/reports/feature_dataset.csv}"
OUTPUT_MANIFEST="${3:-$ROOT_DIR/reports/feature_dataset_manifest.json}"
LABELED_ONLY="${LABELED_ONLY:-false}"

ARGS=(
  --input "$INPUT_PATH"
  --output-csv "$OUTPUT_CSV"
  --output-manifest "$OUTPUT_MANIFEST"
)

if [[ "$LABELED_ONLY" == "true" ]]; then
  ARGS+=(--labeled-only)
fi

python3 "$ROOT_DIR/scripts/export_feature_dataset.py" "${ARGS[@]}"

