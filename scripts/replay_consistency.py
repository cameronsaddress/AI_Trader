#!/usr/bin/env python3
"""
Deterministic event-level replay and consistency report from strategy trade logs.

Inputs:
- strategy trade CSV (default apps/backend/logs/strategy_trades.csv)

Outputs:
- JSON report with:
  - portfolio replay metrics
  - per-strategy replay metrics
  - hour-of-day regime slices
  - transaction-cost diagnostics
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
from pathlib import Path
from typing import Any, Dict, List, Optional


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


def compute_max_drawdown(equity_curve: List[float]) -> float:
    if not equity_curve:
        return 0.0
    peak = equity_curve[0]
    worst = 0.0
    for equity in equity_curve:
        if equity > peak:
            peak = equity
        drawdown = equity - peak
        if drawdown < worst:
            worst = drawdown
    return worst


def compute_sharpe(returns: List[float]) -> float:
    if len(returns) < 2:
        return 0.0
    mean = sum(returns) / len(returns)
    var = sum((value - mean) ** 2 for value in returns) / (len(returns) - 1)
    std = math.sqrt(max(var, 0.0))
    if std <= 1e-12:
        return 0.0
    return mean / std


def parse_rows(path: Path, only_mode: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for raw in reader:
            ts = as_int(raw.get("timestamp"), 0)
            if ts <= 0:
                continue
            mode = str(raw.get("mode", "")).strip().upper()
            if only_mode and mode != only_mode:
                continue
            strategy = str(raw.get("strategy", "")).strip()
            if not strategy:
                continue

            pnl = as_float(raw.get("pnl"), 0.0)
            notional = abs(as_float(raw.get("notional"), 0.0))
            if notional <= 1e-9:
                notional = max(1.0, abs(pnl))
            net_return = as_float(raw.get("net_return"), pnl / notional)
            gross_return = raw.get("gross_return")
            gross_return_value: Optional[float] = None
            if gross_return not in (None, ""):
                gross_return_value = as_float(gross_return, 0.0)
            round_trip_cost_rate = raw.get("round_trip_cost_rate")
            round_trip_cost_value: Optional[float] = None
            if round_trip_cost_rate not in (None, ""):
                round_trip_cost_value = as_float(round_trip_cost_rate, 0.0)
            cost_drag = raw.get("cost_drag_return")
            cost_drag_value: Optional[float] = None
            if cost_drag not in (None, ""):
                cost_drag_value = as_float(cost_drag, 0.0)
            elif gross_return_value is not None:
                cost_drag_value = gross_return_value - net_return
            elif round_trip_cost_value is not None:
                cost_drag_value = round_trip_cost_value

            rows.append(
                {
                    "timestamp": ts,
                    "strategy": strategy,
                    "pnl": pnl,
                    "notional": notional,
                    "net_return": net_return,
                    "gross_return": gross_return_value,
                    "cost_drag_return": cost_drag_value,
                }
            )
    rows.sort(key=lambda row: row["timestamp"])
    return rows


def summarize_bucket(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    trades = len(rows)
    pnls = [row["pnl"] for row in rows]
    returns = [row["net_return"] for row in rows]
    equity = 0.0
    curve: List[float] = []
    for pnl in pnls:
        equity += pnl
        curve.append(equity)
    cost_drag_samples = [row["cost_drag_return"] for row in rows if row["cost_drag_return"] is not None]
    cost_drag_bps = (
        (sum(cost_drag_samples) / len(cost_drag_samples)) * 10_000.0
        if cost_drag_samples
        else 0.0
    )
    return {
        "trades": trades,
        "wins": len([pnl for pnl in pnls if pnl > 0]),
        "losses": len([pnl for pnl in pnls if pnl < 0]),
        "net_pnl": round(sum(pnls), 8),
        "avg_pnl": round((sum(pnls) / trades) if trades else 0.0, 8),
        "win_rate_pct": round((len([p for p in pnls if p > 0]) / trades * 100.0) if trades else 0.0, 4),
        "avg_return": round((sum(returns) / trades) if trades else 0.0, 8),
        "trade_sharpe": round(compute_sharpe(returns), 8),
        "max_drawdown": round(compute_max_drawdown(curve), 8),
        "cost_drag_bps": round(cost_drag_bps, 4),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run deterministic replay consistency report.")
    parser.add_argument("--input", default="apps/backend/logs/strategy_trades.csv")
    parser.add_argument("--output", default="reports/replay_consistency_report.json")
    parser.add_argument("--mode", default="PAPER", help="Filter mode (PAPER/LIVE), empty for all")
    parser.add_argument("--starting-equity", type=float, default=1000.0)
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    rows = parse_rows(input_path, args.mode.strip().upper())
    start_equity = float(args.starting_equity)
    portfolio_curve = [start_equity]
    for row in rows:
        portfolio_curve.append(portfolio_curve[-1] + row["pnl"])

    strategies = sorted({row["strategy"] for row in rows})
    strategy_summary: Dict[str, Dict[str, Any]] = {}
    for strategy in strategies:
        bucket = [row for row in rows if row["strategy"] == strategy]
        strategy_summary[strategy] = summarize_bucket(bucket)

    regimes: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        ts = dt.datetime.fromtimestamp(row["timestamp"] / 1000.0, tz=dt.timezone.utc)
        regime = f"UTC_HOUR_{ts.hour:02d}"
        regimes.setdefault(regime, []).append(row)
    regime_summary = {
        regime: summarize_bucket(bucket)
        for regime, bucket in sorted(regimes.items(), key=lambda item: item[0])
    }

    portfolio_returns = [
        row["pnl"] / max(1e-9, row["notional"])
        for row in rows
    ]
    cost_samples = [row["cost_drag_return"] for row in rows if row["cost_drag_return"] is not None]
    report = {
        "generated_at": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
        "input_path": str(input_path),
        "rows": len(rows),
        "mode_filter": args.mode,
        "window": {
            "start_ts": rows[0]["timestamp"] if rows else None,
            "end_ts": rows[-1]["timestamp"] if rows else None,
        },
        "portfolio": {
            "starting_equity": round(start_equity, 8),
            "ending_equity": round(portfolio_curve[-1], 8),
            "net_pnl": round(portfolio_curve[-1] - start_equity, 8),
            "trade_count": len(rows),
            "trade_sharpe": round(compute_sharpe(portfolio_returns), 8),
            "max_drawdown": round(compute_max_drawdown(portfolio_curve), 8),
            "avg_cost_drag_bps": round(
                ((sum(cost_samples) / len(cost_samples)) * 10_000.0) if cost_samples else 0.0,
                4,
            ),
        },
        "strategies": strategy_summary,
        "regime_hour_utc": regime_summary,
    }

    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2)

    print(json.dumps({
        "ok": True,
        "output": str(output_path),
        "rows": len(rows),
        "strategies": len(strategy_summary),
        "portfolio_net_pnl": report["portfolio"]["net_pnl"],
    }))


if __name__ == "__main__":
    main()

