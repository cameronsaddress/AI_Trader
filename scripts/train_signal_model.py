#!/usr/bin/env python3
"""
Time-series model training harness with purged/embargoed CV and calibration stats.

Input:
  - feature CSV from export_feature_dataset.py

Output:
  - model JSON (weights + normalization + metadata)
  - training report JSON
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import itertools
import json
import math
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple


META_COLUMNS = {
    "id",
    "strategy",
    "market_key",
    "timestamp",
    "signal_type",
    "metric_family",
    "label_net_return",
    "label_pnl",
    "label_timestamp",
    "label_source",
}


def as_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
        if math.isfinite(parsed):
            return parsed
    except Exception:  # noqa: BLE001
        pass
    return default


def sigmoid(x: float) -> float:
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


def log_loss(y_true: Sequence[int], y_prob: Sequence[float]) -> float:
    if not y_true:
        return 0.0
    eps = 1e-12
    total = 0.0
    for truth, prob in zip(y_true, y_prob):
        clipped = min(1.0 - eps, max(eps, prob))
        total += -((truth * math.log(clipped)) + ((1 - truth) * math.log(1.0 - clipped)))
    return total / len(y_true)


def brier_score(y_true: Sequence[int], y_prob: Sequence[float]) -> float:
    if not y_true:
        return 0.0
    return sum((truth - prob) ** 2 for truth, prob in zip(y_true, y_prob)) / len(y_true)


def calibration_ece(y_true: Sequence[int], y_prob: Sequence[float], bins: int = 10) -> float:
    if not y_true:
        return 0.0
    total = len(y_true)
    ece = 0.0
    for bucket in range(bins):
        lo = bucket / bins
        hi = (bucket + 1) / bins
        indices = [i for i, prob in enumerate(y_prob) if lo <= prob < hi or (bucket == bins - 1 and prob == 1.0)]
        if not indices:
            continue
        avg_prob = sum(y_prob[i] for i in indices) / len(indices)
        avg_true = sum(y_true[i] for i in indices) / len(indices)
        ece += (len(indices) / total) * abs(avg_true - avg_prob)
    return ece


def compute_auc(y_true: Sequence[int], y_prob: Sequence[float]) -> float:
    positives = [(p, y) for p, y in zip(y_prob, y_true) if y == 1]
    negatives = [(p, y) for p, y in zip(y_prob, y_true) if y == 0]
    if not positives or not negatives:
        return 0.5
    wins = 0.0
    ties = 0.0
    for pos, _ in positives:
        for neg, _ in negatives:
            if pos > neg:
                wins += 1.0
            elif pos == neg:
                ties += 1.0
    total = len(positives) * len(negatives)
    return (wins + (0.5 * ties)) / total


def normalize_matrix(matrix: List[List[float]]) -> Tuple[List[List[float]], List[float], List[float]]:
    if not matrix:
        return [], [], []
    cols = len(matrix[0])
    means = [0.0] * cols
    stds = [1.0] * cols
    for col in range(cols):
        col_values = [row[col] for row in matrix]
        mean = sum(col_values) / len(col_values)
        var = sum((value - mean) ** 2 for value in col_values) / max(1, len(col_values) - 1)
        std = math.sqrt(max(var, 1e-12))
        means[col] = mean
        stds[col] = std
    normalized: List[List[float]] = []
    for row in matrix:
        normalized.append([(value - means[col]) / stds[col] for col, value in enumerate(row)])
    return normalized, means, stds


def apply_normalization(matrix: List[List[float]], means: List[float], stds: List[float]) -> List[List[float]]:
    if not matrix:
        return []
    return [
        [(value - means[col]) / stds[col] for col, value in enumerate(row)]
        for row in matrix
    ]


def train_logistic_regression(
    x: List[List[float]],
    y: List[int],
    *,
    lr: float,
    epochs: int,
    l2: float,
) -> Tuple[List[float], float]:
    if not x:
        return [], 0.0
    cols = len(x[0])
    weights = [0.0] * cols
    bias = 0.0
    n = len(x)
    for _ in range(epochs):
        grad_w = [0.0] * cols
        grad_b = 0.0
        for row, truth in zip(x, y):
            z = bias + sum(weights[col] * row[col] for col in range(cols))
            prob = sigmoid(z)
            error = prob - truth
            for col in range(cols):
                grad_w[col] += error * row[col]
            grad_b += error
        for col in range(cols):
            grad_w[col] = (grad_w[col] / n) + (l2 * weights[col])
            weights[col] -= lr * grad_w[col]
        bias -= lr * (grad_b / n)
    return weights, bias


def predict_proba(x: List[List[float]], weights: List[float], bias: float) -> List[float]:
    if not x:
        return []
    cols = len(weights)
    probs: List[float] = []
    for row in x:
        z = bias + sum(weights[col] * row[col] for col in range(cols))
        probs.append(sigmoid(z))
    return probs


def purged_embargo_folds(
    n: int,
    splits: int,
    purge: int,
    embargo: int,
) -> List[Tuple[List[int], List[int]]]:
    if n < max(20, splits * 2):
        return []
    base = n // splits
    extra = n % splits
    ranges: List[Tuple[int, int]] = []
    start = 0
    for idx in range(splits):
        width = base + (1 if idx < extra else 0)
        end = min(n, start + width)
        ranges.append((start, end))
        start = end

    folds: List[Tuple[List[int], List[int]]] = []
    for test_start, test_end in ranges:
        if test_end - test_start < 4:
            continue
        train_left_end = max(0, test_start - purge)
        train_right_start = min(n, test_end + embargo)
        train_idx = list(range(0, train_left_end)) + list(range(train_right_start, n))
        test_idx = list(range(test_start, test_end))
        if len(train_idx) < 20 or len(test_idx) < 4:
            continue
        folds.append((train_idx, test_idx))
    return folds


def load_dataset(path: Path) -> Tuple[List[str], List[Dict[str, Any]]]:
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        return [], rows
    with path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        headers = reader.fieldnames or []
        feature_cols = [column for column in headers if column not in META_COLUMNS]
        for raw in reader:
            label = raw.get("label_net_return")
            if label in (None, ""):
                continue
            ts = int(float(raw.get("timestamp", "0") or 0))
            label_return = float(label)
            features = [as_float(raw.get(column), 0.0) for column in feature_cols]
            rows.append(
                {
                    "timestamp": ts,
                    "features": features,
                    "label_return": label_return,
                    "label_class": 1 if label_return > 0 else 0,
                }
            )
    rows.sort(key=lambda row: row["timestamp"])
    return feature_cols, rows


def build_matrix(rows: Sequence[Dict[str, Any]]) -> Tuple[List[List[float]], List[int], List[float]]:
    x = [list(row["features"]) for row in rows]
    y = [int(row["label_class"]) for row in rows]
    returns = [float(row["label_return"]) for row in rows]
    return x, y, returns


def main() -> None:
    parser = argparse.ArgumentParser(description="Train baseline signal model with purged time-series CV.")
    parser.add_argument("--input-csv", default="reports/feature_dataset.csv")
    parser.add_argument("--output-model", default="reports/models/signal_model_latest.json")
    parser.add_argument("--output-report", default="reports/models/signal_model_report.json")
    parser.add_argument("--splits", type=int, default=5)
    parser.add_argument("--purge", type=int, default=24)
    parser.add_argument("--embargo", type=int, default=24)
    parser.add_argument("--min-train-rows", type=int, default=80)
    args = parser.parse_args()

    input_csv = Path(args.input_csv)
    output_model = Path(args.output_model)
    output_report = Path(args.output_report)
    output_model.parent.mkdir(parents=True, exist_ok=True)
    output_report.parent.mkdir(parents=True, exist_ok=True)

    feature_cols, rows = load_dataset(input_csv)
    if len(rows) < max(args.min_train_rows, 60) or len(feature_cols) == 0:
        report = {
            "generated_at": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
            "eligible": False,
            "reason": "insufficient labeled dataset",
            "rows": len(rows),
            "feature_count": len(feature_cols),
        }
        with output_report.open("w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2)
        with output_model.open("w", encoding="utf-8") as handle:
            json.dump({
                "generated_at": report["generated_at"],
                "eligible": False,
                "feature_columns": feature_cols,
                "weights": [],
                "bias": 0.0,
            }, handle, indent=2)
        print(json.dumps({"ok": True, "eligible": False, "rows": len(rows), "feature_count": len(feature_cols)}))
        return

    folds = purged_embargo_folds(len(rows), args.splits, args.purge, args.embargo)
    if len(folds) < 2:
        report = {
            "generated_at": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
            "eligible": False,
            "reason": "insufficient folds after purge/embargo",
            "rows": len(rows),
            "folds": len(folds),
        }
        with output_report.open("w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2)
        print(json.dumps({"ok": True, "eligible": False, "rows": len(rows), "folds": len(folds)}))
        return

    grid = list(itertools.product([0.02, 0.05], [120, 220], [0.0, 1e-4, 1e-3, 1e-2]))
    cv_results: List[Dict[str, Any]] = []
    best = None

    for lr, epochs, l2 in grid:
        fold_metrics: List[Dict[str, float]] = []
        for train_idx, test_idx in folds:
            train_rows = [rows[i] for i in train_idx]
            test_rows = [rows[i] for i in test_idx]
            x_train, y_train, _ = build_matrix(train_rows)
            x_test, y_test, _ = build_matrix(test_rows)
            x_train_norm, means, stds = normalize_matrix(x_train)
            x_test_norm = apply_normalization(x_test, means, stds)
            weights, bias = train_logistic_regression(x_train_norm, y_train, lr=lr, epochs=epochs, l2=l2)
            probs = predict_proba(x_test_norm, weights, bias)
            fold_metrics.append(
                {
                    "logloss": log_loss(y_test, probs),
                    "brier": brier_score(y_test, probs),
                    "ece": calibration_ece(y_test, probs, bins=10),
                    "auc": compute_auc(y_test, probs),
                }
            )
        avg = {
            "lr": lr,
            "epochs": epochs,
            "l2": l2,
            "folds": len(fold_metrics),
            "avg_logloss": sum(m["logloss"] for m in fold_metrics) / len(fold_metrics),
            "avg_brier": sum(m["brier"] for m in fold_metrics) / len(fold_metrics),
            "avg_ece": sum(m["ece"] for m in fold_metrics) / len(fold_metrics),
            "avg_auc": sum(m["auc"] for m in fold_metrics) / len(fold_metrics),
        }
        avg["objective"] = avg["avg_logloss"] + (0.5 * avg["avg_ece"]) + (0.25 * avg["avg_brier"])
        cv_results.append(avg)
        if best is None or avg["objective"] < best["objective"]:
            best = avg

    assert best is not None
    x_full, y_full, returns_full = build_matrix(rows)
    x_full_norm, means, stds = normalize_matrix(x_full)
    weights, bias = train_logistic_regression(
        x_full_norm,
        y_full,
        lr=float(best["lr"]),
        epochs=int(best["epochs"]),
        l2=float(best["l2"]),
    )
    probs_full = predict_proba(x_full_norm, weights, bias)

    model = {
        "generated_at": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
        "eligible": True,
        "feature_columns": feature_cols,
        "weights": weights,
        "bias": bias,
        "normalization": {
            "means": means,
            "stds": stds,
        },
        "best_hyperparameters": best,
        "train_rows": len(rows),
    }

    report = {
        "generated_at": model["generated_at"],
        "eligible": True,
        "rows": len(rows),
        "feature_count": len(feature_cols),
        "cv_folds": len(folds),
        "purge_rows": args.purge,
        "embargo_rows": args.embargo,
        "best_hyperparameters": best,
        "cv_results": sorted(cv_results, key=lambda result: result["objective"]),
        "in_sample": {
            "logloss": log_loss(y_full, probs_full),
            "brier": brier_score(y_full, probs_full),
            "ece": calibration_ece(y_full, probs_full, bins=10),
            "auc": compute_auc(y_full, probs_full),
            "avg_label_return": sum(returns_full) / len(returns_full),
            "positive_label_rate": sum(y_full) / len(y_full),
        },
    }

    with output_model.open("w", encoding="utf-8") as handle:
        json.dump(model, handle, indent=2)
    with output_report.open("w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2)

    print(json.dumps({
        "ok": True,
        "eligible": True,
        "rows": len(rows),
        "feature_count": len(feature_cols),
        "best_objective": round(float(best["objective"]), 8),
        "output_model": str(output_model),
        "output_report": str(output_report),
    }))


if __name__ == "__main__":
    main()

