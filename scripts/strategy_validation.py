#!/usr/bin/env python3
"""
Research-style strategy validation harness:
- Anchored walk-forward out-of-sample metrics
- CSCV-style PBO estimate across strategy variants
- Deflated Sharpe Ratio (DSR) estimate for multiple testing

Input CSV schema (required):
  timestamp,strategy,pnl

Optional columns:
  variant   : parameter/config identifier (default: "default")
  notional  : trade notional used to normalize returns (default: max(abs(pnl), 1.0))

Example:
  python3 scripts/strategy_validation.py \
    --input data/strategy_trades.csv \
    --walk-forward-splits 6 \
    --min-test-trades 40 \
    --cscv-partitions 8 \
    --output reports/strategy_validation.json
"""

from __future__ import annotations

import argparse
import csv
import itertools
import json
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from statistics import NormalDist
from typing import Dict, List, Optional, Sequence, Tuple


ND = NormalDist()
EULER_GAMMA = 0.5772156649015329


@dataclass
class TradeRow:
    timestamp: float
    strategy: str
    variant: str
    pnl: float
    notional: float
    ret: float


def parse_timestamp(value: str) -> float:
    value = (value or "").strip()
    if not value:
        raise ValueError("missing timestamp")

    # unix seconds or ms
    try:
        numeric = float(value)
        if numeric > 1_000_000_000_000:
            return numeric / 1000.0
        return numeric
    except ValueError:
        pass

    # iso datetime
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt.timestamp()


def safe_float(value: str, default: float = 0.0) -> float:
    try:
        parsed = float(value)
        if math.isfinite(parsed):
            return parsed
    except Exception:
        pass
    return default


def load_rows(path: str) -> List[TradeRow]:
    rows: List[TradeRow] = []
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        required = {"timestamp", "strategy", "pnl"}
        missing = required.difference(set(reader.fieldnames or []))
        if missing:
            raise ValueError(f"input CSV is missing required columns: {sorted(missing)}")

        for raw in reader:
            strategy = (raw.get("strategy") or "").strip()
            if not strategy:
                continue

            ts = parse_timestamp(raw.get("timestamp", ""))
            pnl = safe_float(raw.get("pnl", "0"), 0.0)
            variant = (raw.get("variant") or "default").strip() or "default"
            notional = safe_float(raw.get("notional", ""), 0.0)
            if notional <= 0:
                notional = max(abs(pnl), 1.0)
            ret = pnl / notional

            rows.append(
                TradeRow(
                    timestamp=ts,
                    strategy=strategy,
                    variant=variant,
                    pnl=pnl,
                    notional=notional,
                    ret=ret,
                )
            )

    rows.sort(key=lambda r: r.timestamp)
    return rows


def sample_mean(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def sample_variance(values: Sequence[float]) -> float:
    n = len(values)
    if n < 2:
        return 0.0
    mu = sample_mean(values)
    return sum((x - mu) ** 2 for x in values) / (n - 1)


def sample_std(values: Sequence[float]) -> float:
    return math.sqrt(max(sample_variance(values), 0.0))


def sample_skew(values: Sequence[float]) -> float:
    n = len(values)
    if n < 3:
        return 0.0
    mu = sample_mean(values)
    std = sample_std(values)
    if std <= 0:
        return 0.0
    m3 = sum((x - mu) ** 3 for x in values) / n
    g1 = m3 / (std ** 3)
    return math.sqrt(n * (n - 1)) / (n - 2) * g1


def sample_excess_kurtosis(values: Sequence[float]) -> float:
    n = len(values)
    if n < 4:
        return 0.0
    mu = sample_mean(values)
    std = sample_std(values)
    if std <= 0:
        return 0.0
    m4 = sum((x - mu) ** 4 for x in values) / n
    g2 = m4 / (std ** 4) - 3.0
    a = (n - 1) / ((n - 2) * (n - 3))
    b = ((n + 1) * g2 + 6.0)
    return a * b


def sharpe_ratio(returns: Sequence[float]) -> float:
    n = len(returns)
    if n < 2:
        return 0.0
    mu = sample_mean(returns)
    std = sample_std(returns)
    if std <= 0:
        return 0.0
    # per-trade Sharpe
    return mu / std


def max_drawdown_from_pnl(pnls: Sequence[float]) -> float:
    equity = 0.0
    peak = 0.0
    worst_dd = 0.0
    for pnl in pnls:
        equity += pnl
        peak = max(peak, equity)
        dd = equity - peak
        worst_dd = min(worst_dd, dd)
    return worst_dd


def walk_forward_metrics(
    rows: Sequence[TradeRow],
    splits: int,
    min_test_trades: int,
) -> Dict[str, object]:
    n = len(rows)
    if n < max(min_test_trades * 2, 20):
        return {
            "splits": [],
            "summary": {
                "oos_splits": 0,
                "avg_oos_return": 0.0,
                "avg_oos_sharpe": 0.0,
                "worst_oos_drawdown": 0.0,
            },
        }

    test_size = max(min_test_trades, n // (splits + 1))
    oos = []
    for i in range(splits):
        train_end = test_size * (i + 1)
        test_end = min(n, train_end + test_size)
        if train_end >= n or (test_end - train_end) < min_test_trades:
            break

        test_rows = rows[train_end:test_end]
        test_rets = [r.ret for r in test_rows]
        test_pnls = [r.pnl for r in test_rows]
        oos.append(
            {
                "split": i + 1,
                "train_trades": train_end,
                "test_trades": len(test_rows),
                "oos_mean_return": sample_mean(test_rets),
                "oos_sharpe": sharpe_ratio(test_rets),
                "oos_total_pnl": sum(test_pnls),
                "oos_max_drawdown": max_drawdown_from_pnl(test_pnls),
                "oos_win_rate": sum(1 for x in test_pnls if x > 0) / len(test_pnls),
            }
        )

    if not oos:
        return {
            "splits": [],
            "summary": {
                "oos_splits": 0,
                "avg_oos_return": 0.0,
                "avg_oos_sharpe": 0.0,
                "worst_oos_drawdown": 0.0,
            },
        }

    return {
        "splits": oos,
        "summary": {
            "oos_splits": len(oos),
            "avg_oos_return": sample_mean([x["oos_mean_return"] for x in oos]),
            "avg_oos_sharpe": sample_mean([x["oos_sharpe"] for x in oos]),
            "worst_oos_drawdown": min([x["oos_max_drawdown"] for x in oos]),
        },
    }


def contiguous_partitions(length: int, parts: int) -> List[Tuple[int, int]]:
    base = length // parts
    extra = length % parts
    ranges: List[Tuple[int, int]] = []
    start = 0
    for i in range(parts):
        width = base + (1 if i < extra else 0)
        end = start + width
        ranges.append((start, end))
        start = end
    return ranges


def cscv_pbo(
    variant_returns: Dict[str, List[float]],
    partitions: int,
) -> Dict[str, object]:
    variants = sorted(variant_returns.keys())
    if len(variants) < 2:
        return {
            "eligible": False,
            "reason": "Need >=2 variants for PBO/DSR",
        }

    min_len = min(len(variant_returns[v]) for v in variants)
    if min_len < max(partitions * 2, 40):
        return {
            "eligible": False,
            "reason": "Insufficient aligned trade count for CSCV",
        }

    if partitions < 4:
        partitions = 4
    if partitions % 2 != 0:
        partitions += 1

    # align to equal-length prefix for all variants
    aligned = {v: variant_returns[v][:min_len] for v in variants}
    part_ranges = contiguous_partitions(min_len, partitions)
    part_indices = list(range(partitions))
    in_sample_size = partitions // 2

    total = 0
    overfit = 0
    selected_oos_percentiles: List[float] = []

    for train_parts in itertools.combinations(part_indices, in_sample_size):
        train_set = set(train_parts)

        train_idx: List[int] = []
        test_idx: List[int] = []
        for i, (a, b) in enumerate(part_ranges):
            if b <= a:
                continue
            bucket = list(range(a, b))
            if i in train_set:
                train_idx.extend(bucket)
            else:
                test_idx.extend(bucket)

        if len(train_idx) < 10 or len(test_idx) < 10:
            continue

        is_sr = {}
        oos_sr = {}
        for variant in variants:
            r = aligned[variant]
            is_ret = [r[j] for j in train_idx]
            oos_ret = [r[j] for j in test_idx]
            is_sr[variant] = sharpe_ratio(is_ret)
            oos_sr[variant] = sharpe_ratio(oos_ret)

        winner = max(is_sr, key=is_sr.get)
        ordered = sorted(oos_sr.items(), key=lambda kv: kv[1])
        rank = next((idx for idx, (v, _) in enumerate(ordered) if v == winner), 0)
        percentile = (rank + 1) / len(ordered)
        selected_oos_percentiles.append(percentile)
        if percentile < 0.5:
            overfit += 1
        total += 1

    if total == 0:
        return {
            "eligible": False,
            "reason": "No valid CSCV splits created",
        }

    # DSR for best full-sample variant.
    full_sr = {v: sharpe_ratio(aligned[v]) for v in variants}
    best_variant = max(full_sr, key=full_sr.get)
    best_returns = aligned[best_variant]
    sr = full_sr[best_variant]
    t = len(best_returns)
    m = len(variants)
    skew = sample_skew(best_returns)
    kurt = sample_excess_kurtosis(best_returns) + 3.0
    sr_std = math.sqrt(1.0 / max(t - 1, 1))

    z1 = ND.inv_cdf(1.0 - 1.0 / m)
    z2 = ND.inv_cdf(1.0 - 1.0 / (m * math.e))
    sr_star = sr_std * ((1.0 - EULER_GAMMA) * z1 + EULER_GAMMA * z2)
    denom = math.sqrt(max(1e-12, 1.0 - skew * sr + ((kurt - 1.0) / 4.0) * (sr ** 2)))
    z = ((sr - sr_star) * math.sqrt(max(t - 1, 1))) / denom
    dsr = ND.cdf(z)

    return {
        "eligible": True,
        "variants": m,
        "aligned_trades": min_len,
        "cscv_splits": total,
        "pbo": overfit / total,
        "median_selected_oos_percentile": sorted(selected_oos_percentiles)[len(selected_oos_percentiles) // 2],
        "best_variant": best_variant,
        "best_variant_sr": sr,
        "deflated_sharpe_ratio": dsr,
        "sr_star": sr_star,
    }


def validate_strategies(
    rows: Sequence[TradeRow],
    walk_forward_splits: int,
    min_test_trades: int,
    cscv_partitions: int,
) -> Dict[str, object]:
    by_strategy_variant: Dict[str, Dict[str, List[TradeRow]]] = defaultdict(lambda: defaultdict(list))
    for row in rows:
        by_strategy_variant[row.strategy][row.variant].append(row)

    report = {"strategies": {}, "summary": {}}
    total_rows = 0

    for strategy in sorted(by_strategy_variant.keys()):
        variants = by_strategy_variant[strategy]
        all_rows = sorted([r for rows_v in variants.values() for r in rows_v], key=lambda r: r.timestamp)
        total_rows += len(all_rows)

        wf = walk_forward_metrics(all_rows, walk_forward_splits, min_test_trades)
        variant_returns = {
            variant: [r.ret for r in sorted(rows_v, key=lambda x: x.timestamp)]
            for variant, rows_v in variants.items()
        }
        pbo = cscv_pbo(variant_returns, cscv_partitions)

        in_sample = [r.ret for r in all_rows]
        pnls = [r.pnl for r in all_rows]
        report["strategies"][strategy] = {
            "trades": len(all_rows),
            "variants": len(variants),
            "total_pnl": sum(pnls),
            "mean_return": sample_mean(in_sample),
            "sharpe": sharpe_ratio(in_sample),
            "max_drawdown": max_drawdown_from_pnl(pnls),
            "win_rate": (sum(1 for p in pnls if p > 0) / len(pnls)) if pnls else 0.0,
            "walk_forward": wf,
            "overfitting_diagnostics": pbo,
        }

    report["summary"] = {
        "strategies": len(report["strategies"]),
        "trades": total_rows,
        "walk_forward_splits": walk_forward_splits,
        "min_test_trades": min_test_trades,
        "cscv_partitions": cscv_partitions,
    }
    return report


def print_console_summary(report: Dict[str, object]) -> None:
    print("Strategy Validation Summary")
    print("==========================")
    summary = report.get("summary", {})
    print(f"strategies: {summary.get('strategies', 0)} | trades: {summary.get('trades', 0)}")
    print("")

    strategies = report.get("strategies", {})
    for name, stats in strategies.items():
        wf = stats.get("walk_forward", {}).get("summary", {})
        of = stats.get("overfitting_diagnostics", {})
        pbo_text = "n/a"
        dsr_text = "n/a"
        if of.get("eligible"):
            pbo_text = f"{of.get('pbo', 0.0):.3f}"
            dsr_text = f"{of.get('deflated_sharpe_ratio', 0.0):.3f}"

        print(
            f"- {name}: trades={stats.get('trades', 0)}, pnl={stats.get('total_pnl', 0.0):.2f}, "
            f"sharpe={stats.get('sharpe', 0.0):.3f}, wf_sharpe={wf.get('avg_oos_sharpe', 0.0):.3f}, "
            f"pbo={pbo_text}, dsr={dsr_text}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run research-style strategy validation.")
    parser.add_argument("--input", required=True, help="Input CSV path.")
    parser.add_argument("--output", default="", help="Optional JSON report output path.")
    parser.add_argument("--walk-forward-splits", type=int, default=6, help="Number of anchored walk-forward OOS splits.")
    parser.add_argument("--min-test-trades", type=int, default=40, help="Minimum trades in each OOS split.")
    parser.add_argument("--cscv-partitions", type=int, default=8, help="Number of contiguous partitions for CSCV.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rows = load_rows(args.input)
    report = validate_strategies(
        rows=rows,
        walk_forward_splits=max(1, args.walk_forward_splits),
        min_test_trades=max(10, args.min_test_trades),
        cscv_partitions=max(4, args.cscv_partitions),
    )

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, sort_keys=True)

    print_console_summary(report)


if __name__ == "__main__":
    main()
