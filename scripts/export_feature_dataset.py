#!/usr/bin/env python3
"""
Materialize deterministic feature dataset from feature registry event log.

Input:
  JSONL event log produced by backend FeatureRegistryRecorder

Outputs:
  - Feature CSV for training
  - Manifest JSON with counts and leakage diagnostics
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
from pathlib import Path
from typing import Any, Dict, List


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
        if math.isfinite(parsed):
            return parsed
    except Exception:  # noqa: BLE001
        pass
    return default


def as_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:  # noqa: BLE001
        return default


def load_events(path: Path) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    if not path.exists():
        return events
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            blob = line.strip()
            if not blob:
                continue
            try:
                parsed = json.loads(blob)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict):
                events.append(parsed)
    events.sort(key=lambda event: as_int(event.get("event_ts"), 0))
    return events


def main() -> None:
    parser = argparse.ArgumentParser(description="Export ML feature dataset from event log.")
    parser.add_argument("--input", default="apps/backend/logs/feature_registry_events.jsonl")
    parser.add_argument("--output-csv", default="reports/feature_dataset.csv")
    parser.add_argument("--output-manifest", default="reports/feature_dataset_manifest.json")
    parser.add_argument("--labeled-only", action="store_true")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_csv = Path(args.output_csv)
    output_manifest = Path(args.output_manifest)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    output_manifest.parent.mkdir(parents=True, exist_ok=True)

    events = load_events(input_path)
    rows_by_id: Dict[str, Dict[str, Any]] = {}
    orphan_labels = 0

    for event in events:
        event_type = str(event.get("event_type", "")).strip().upper()
        if event_type == "RESET":
            rows_by_id = {}
            continue

        if event_type == "SCAN":
            row_id = str(event.get("row_id", "")).strip()
            if not row_id:
                continue
            features = event.get("features")
            if not isinstance(features, dict):
                features = {}
            rows_by_id[row_id] = {
                "id": row_id,
                "strategy": str(event.get("strategy", "")).strip(),
                "market_key": str(event.get("market_key", "")).strip(),
                "timestamp": as_int(event.get("scan_ts"), as_int(event.get("event_ts"), 0)),
                "signal_type": str(event.get("signal_type", "UNKNOWN")).strip() or "UNKNOWN",
                "metric_family": str(event.get("metric_family", "UNKNOWN")).strip() or "UNKNOWN",
                "features": {
                    str(key): as_float(value, 0.0)
                    for key, value in features.items()
                    if isinstance(key, str)
                },
                "label_net_return": None,
                "label_pnl": None,
                "label_timestamp": None,
                "label_source": None,
            }
            continue

        if event_type == "LABEL":
            row_id = str(event.get("row_id", "")).strip()
            if not row_id:
                continue
            row = rows_by_id.get(row_id)
            if not row:
                orphan_labels += 1
                continue
            row["label_net_return"] = (
                None if event.get("label_net_return") is None else as_float(event.get("label_net_return"), 0.0)
            )
            row["label_pnl"] = (
                None if event.get("label_pnl") is None else as_float(event.get("label_pnl"), 0.0)
            )
            row["label_timestamp"] = as_int(event.get("label_ts"), as_int(event.get("event_ts"), 0))
            row["label_source"] = str(event.get("label_source", "")).strip() or None

    rows = list(rows_by_id.values())
    rows.sort(key=lambda row: (as_int(row.get("timestamp"), 0), str(row.get("id", ""))))

    if args.labeled_only:
        rows = [row for row in rows if row.get("label_net_return") is not None]

    feature_keys = sorted({
        key
        for row in rows
        for key in row.get("features", {}).keys()
    })

    leakage_violations = 0
    for row in rows:
        label_ts = row.get("label_timestamp")
        scan_ts = row.get("timestamp")
        if label_ts is not None and scan_ts is not None and int(label_ts) < int(scan_ts):
            leakage_violations += 1

    fieldnames = [
        "id",
        "strategy",
        "market_key",
        "timestamp",
        "signal_type",
        "metric_family",
        *feature_keys,
        "label_net_return",
        "label_pnl",
        "label_timestamp",
        "label_source",
    ]

    with output_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            payload = {
                "id": row.get("id"),
                "strategy": row.get("strategy"),
                "market_key": row.get("market_key"),
                "timestamp": row.get("timestamp"),
                "signal_type": row.get("signal_type"),
                "metric_family": row.get("metric_family"),
                "label_net_return": row.get("label_net_return"),
                "label_pnl": row.get("label_pnl"),
                "label_timestamp": row.get("label_timestamp"),
                "label_source": row.get("label_source"),
            }
            for key in feature_keys:
                payload[key] = row.get("features", {}).get(key)
            writer.writerow(payload)

    manifest = {
        "generated_at": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
        "input_path": str(input_path),
        "output_csv": str(output_csv),
        "rows": len(rows),
        "labeled_rows": len([row for row in rows if row.get("label_net_return") is not None]),
        "unlabeled_rows": len([row for row in rows if row.get("label_net_return") is None]),
        "feature_count": len(feature_keys),
        "feature_keys": feature_keys,
        "orphan_labels": orphan_labels,
        "leakage_violations": leakage_violations,
        "labeled_only": bool(args.labeled_only),
    }

    with output_manifest.open("w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2)

    print(json.dumps({
        "ok": True,
        "rows": manifest["rows"],
        "labeled_rows": manifest["labeled_rows"],
        "feature_count": manifest["feature_count"],
        "leakage_violations": manifest["leakage_violations"],
        "output_csv": str(output_csv),
        "output_manifest": str(output_manifest),
    }))


if __name__ == "__main__":
    main()

