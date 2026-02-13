#!/usr/bin/env python3
"""
Long-horizon paper validation runner for Polymarket strategy stack.

What it does:
1) Forces PAPER mode.
2) Resets simulation ledger and validation trade dataset.
3) Samples /api/arb/stats over a long horizon.
4) Computes per-strategy performance from strategy_trades.csv.
5) Checks feed health from scanner container logs (sequence gaps / WS errors).
6) Emits promote/hold/disable recommendations.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import re
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional


STRATEGY_IDS = [
    "BTC_15M",
    "ETH_15M",
    "SOL_15M",
    "CEX_SNIPER",
    "SYNDICATE",
    "ATOMIC_ARB",
    "OBI_SCALPER",
    "GRAPH_ARB",
    "CONVERGENCE_CARRY",
    "MAKER_MM",
]

FEED_CONTAINERS = {
    "CEX_SNIPER": "infrastructure-arb-scanner-cex-1",
    "OBI_SCALPER": "infrastructure-arb-scanner-obi-1",
    "BTC_15M": "infrastructure-arb-scanner-btc-1",
    "ETH_15M": "infrastructure-arb-scanner-eth-1",
    "SOL_15M": "infrastructure-arb-scanner-sol-1",
}


def api_request(
    api_url: str,
    path: str,
    *,
    method: str = "GET",
    token: Optional[str] = None,
    payload: Optional[Dict[str, Any]] = None,
    timeout: float = 10.0,
) -> Dict[str, Any]:
    url = f"{api_url.rstrip('/')}{path}"
    data = None
    headers = {"content-type": "application/json"}
    if token:
        headers["authorization"] = f"Bearer {token}"
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")

    request = urllib.request.Request(url=url, method=method, headers=headers, data=data)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
            body = json.loads(raw) if raw else {}
            return {"ok": True, "status": response.status, "body": body}
    except urllib.error.HTTPError as error:
        raw = error.read().decode("utf-8") if error.fp else ""
        try:
            body = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            body = {"raw": raw}
        return {"ok": False, "status": error.code, "body": body}
    except Exception as error:  # noqa: BLE001
        return {"ok": False, "status": 0, "body": {"error": str(error)}}


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


def compute_max_drawdown(values: List[float]) -> float:
    if not values:
        return 0.0
    peak = values[0]
    max_dd = 0.0
    for value in values:
        if value > peak:
            peak = value
        dd = peak - value
        if dd > max_dd:
            max_dd = dd
    return max_dd


def compute_sharpe(returns: List[float]) -> float:
    if len(returns) < 2:
        return 0.0
    mean = sum(returns) / len(returns)
    var = sum((x - mean) ** 2 for x in returns) / (len(returns) - 1)
    std = math.sqrt(max(0.0, var))
    if std <= 1e-12:
        return 0.0
    return (mean / std) * math.sqrt(len(returns))


def classify_strategy(stats: Dict[str, Any], min_trades: int) -> Dict[str, str]:
    trades = int(stats.get("trades", 0))
    net_pnl = as_float(stats.get("net_pnl", 0.0))
    win_rate = as_float(stats.get("win_rate_pct", 0.0))
    sharpe = as_float(stats.get("trade_sharpe", 0.0))

    if trades < min_trades:
        return {"action": "HOLD", "reason": f"insufficient sample ({trades} < {min_trades})"}
    if net_pnl <= 0.0 or sharpe <= 0.0 or win_rate < 50.0:
        return {
            "action": "DISABLE_OR_REWORK",
            "reason": f"negative expectancy (pnl={net_pnl:.2f}, sharpe={sharpe:.2f}, win_rate={win_rate:.1f}%)",
        }
    if sharpe >= 0.6 and win_rate >= 55.0:
        return {"action": "PROMOTE_CANDIDATE", "reason": "positive expectancy with stable trade distribution"}
    return {"action": "HOLD", "reason": "positive but not yet stable enough for promotion"}


def docker_log_pattern_count(container: str, since_iso: str, pattern: str) -> int:
    command = ["docker", "logs", "--since", since_iso, container]
    proc = subprocess.run(command, capture_output=True, text=True, check=False)
    blob = (proc.stdout or "") + "\n" + (proc.stderr or "")
    return len(re.findall(pattern, blob, flags=re.IGNORECASE))


def parse_trade_rows(trade_log_path: Path, start_ms: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not trade_log_path.exists():
        return rows
    with trade_log_path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            ts = as_int(row.get("timestamp"), 0)
            if ts < start_ms:
                continue
            mode = str(row.get("mode", "")).strip().upper()
            if mode != "PAPER":
                continue
            strategy = str(row.get("strategy", "")).strip()
            pnl = as_float(row.get("pnl"), 0.0)
            notional = max(1e-9, abs(as_float(row.get("notional"), 0.0)))
            reason = str(row.get("reason", "")).strip()
            rows.append(
                {
                    "timestamp": ts,
                    "strategy": strategy,
                    "pnl": pnl,
                    "notional": notional,
                    "return": pnl / notional,
                    "reason": reason,
                }
            )
    return rows


def summarize_strategies(trades: List[Dict[str, Any]], min_trades: int) -> Dict[str, Dict[str, Any]]:
    summary: Dict[str, Dict[str, Any]] = {}
    for strategy_id in STRATEGY_IDS:
        strategy_trades = [row for row in trades if row["strategy"] == strategy_id]
        pnl_values = [row["pnl"] for row in strategy_trades]
        returns = [row["return"] for row in strategy_trades]
        wins = len([value for value in pnl_values if value >= 0.0])
        losses = len([value for value in pnl_values if value < 0.0])
        reasons: Dict[str, int] = {}
        for row in strategy_trades:
            reasons[row["reason"]] = reasons.get(row["reason"], 0) + 1

        net_pnl = sum(pnl_values)
        trades_count = len(strategy_trades)
        win_rate_pct = (wins / trades_count * 100.0) if trades_count else 0.0
        avg_pnl = (net_pnl / trades_count) if trades_count else 0.0
        avg_return = (sum(returns) / len(returns)) if returns else 0.0
        trade_sharpe = compute_sharpe(returns)

        stats = {
            "trades": trades_count,
            "wins": wins,
            "losses": losses,
            "net_pnl": round(net_pnl, 8),
            "avg_pnl": round(avg_pnl, 8),
            "win_rate_pct": round(win_rate_pct, 4),
            "avg_return": round(avg_return, 8),
            "trade_sharpe": round(trade_sharpe, 8),
            "reasons": reasons,
        }
        stats.update(classify_strategy(stats, min_trades))
        summary[strategy_id] = stats
    return summary


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def resolve_trade_log_path(reported_path: Optional[str], fallback: str) -> Path:
    candidates: List[Path] = []
    if reported_path:
        trimmed = reported_path.strip()
        if trimmed:
            raw_path = Path(trimmed)
            candidates.append(raw_path)
            if trimmed.startswith("/app/"):
                mapped = Path(trimmed.removeprefix("/app/"))
                candidates.append(Path.cwd() / mapped)
                candidates.append(mapped)

    fallback_path = Path(fallback)
    candidates.append(Path.cwd() / fallback_path)
    candidates.append(fallback_path)

    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        if candidate.exists():
            return candidate
    return fallback_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Run long-horizon paper validation.")
    parser.add_argument("--api-url", default="http://localhost:5114")
    parser.add_argument("--token", required=True)
    parser.add_argument("--duration-seconds", type=int, default=7200)
    parser.add_argument("--sample-interval-seconds", type=float, default=5.0)
    parser.add_argument("--baseline-bankroll", type=float, default=1000.0)
    parser.add_argument("--min-trades-for-promotion", type=int, default=25)
    parser.add_argument("--trade-log-path", default="apps/backend/logs/strategy_trades.csv")
    parser.add_argument("--output", default="reports/paper_long_validation_report.json")
    parser.add_argument("--stats-jsonl", default="reports/paper_long_validation_stats.jsonl")
    args = parser.parse_args()

    output_path = Path(args.output)
    stats_path = Path(args.stats_jsonl)
    ensure_parent(output_path)
    ensure_parent(stats_path)

    mode_set = api_request(
        args.api_url,
        "/api/system/trading-mode",
        method="POST",
        token=args.token,
        payload={"mode": "PAPER"},
    )
    if not mode_set["ok"]:
        raise SystemExit(f"failed to set PAPER mode: {mode_set}")

    reset = api_request(
        args.api_url,
        "/api/system/reset-simulation",
        method="POST",
        token=args.token,
        payload={
            "confirmation": "RESET",
            "bankroll": args.baseline_bankroll,
            "force_defaults": True,
        },
    )
    if not reset["ok"]:
        raise SystemExit(f"failed to reset simulation: {reset}")

    reset_trades = api_request(
        args.api_url,
        "/api/arb/validation-trades/reset",
        method="POST",
        token=args.token,
        payload={},
    )
    validation_trade_info = api_request(args.api_url, "/api/arb/validation-trades", method="GET", token=None)

    reported_trade_path = None
    if reset_trades.get("ok"):
        body = reset_trades.get("body") or {}
        if isinstance(body, dict):
            reported_trade_path = body.get("path")
    if not reported_trade_path and validation_trade_info.get("ok"):
        body = validation_trade_info.get("body") or {}
        if isinstance(body, dict):
            reported_trade_path = body.get("path")

    trade_log_path = resolve_trade_log_path(str(reported_trade_path or ""), args.trade_log_path)

    start_ms = int(time.time() * 1000)
    start_iso = dt.datetime.fromtimestamp(start_ms / 1000.0, tz=dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_at = time.time() + float(args.duration_seconds)

    stats_samples: List[Dict[str, Any]] = []
    with stats_path.open("w", encoding="utf-8") as stats_handle:
        while time.time() < end_at:
            ts = int(time.time() * 1000)
            response = api_request(args.api_url, "/api/arb/stats", method="GET", token=None)
            row = {
                "ts": ts,
                "ok": response["ok"],
                "status": response["status"],
                "stats": response.get("body", {}),
            }
            stats_samples.append(row)
            stats_handle.write(json.dumps(row) + "\n")
            stats_handle.flush()
            time.sleep(max(0.5, args.sample_interval_seconds))

    end_ms = int(time.time() * 1000)
    trades = parse_trade_rows(trade_log_path, start_ms)

    equity_series: List[float] = []
    signals_series: List[float] = []
    markets_series: List[float] = []
    paper_mode_samples = 0
    for sample in stats_samples:
        stats = sample.get("stats") or {}
        ledger = stats.get("simulation_ledger") or {}
        equity = as_float(ledger.get("equity"), as_float(stats.get("bankroll"), math.nan))
        if math.isfinite(equity):
            equity_series.append(equity)
        signals_series.append(as_float(stats.get("active_signals"), 0.0))
        markets_series.append(as_float(stats.get("active_markets"), 0.0))
        if str(stats.get("trading_mode", "")).upper() == "PAPER":
            paper_mode_samples += 1

    feed_health: Dict[str, Dict[str, int]] = {}
    for strategy_id, container in FEED_CONTAINERS.items():
        feed_health[strategy_id] = {
            "sequence_gap_events": docker_log_pattern_count(container, start_iso, r"sequence gap"),
            "ws_error_events": docker_log_pattern_count(container, start_iso, r"WS error|WebSocket .* error"),
        }

    strategy_summary = summarize_strategies(trades, args.min_trades_for_promotion)
    promote_candidates = [k for k, v in strategy_summary.items() if v["action"] == "PROMOTE_CANDIDATE"]
    disable_candidates = [k for k, v in strategy_summary.items() if v["action"] == "DISABLE_OR_REWORK"]

    total_net_pnl = sum(as_float(row.get("pnl"), 0.0) for row in trades)
    start_equity = equity_series[0] if equity_series else args.baseline_bankroll
    end_equity = equity_series[-1] if equity_series else start_equity
    max_drawdown_abs = compute_max_drawdown(equity_series)
    max_drawdown_pct = (max_drawdown_abs / start_equity * 100.0) if start_equity > 0 else 0.0

    mode_safety_pass = paper_mode_samples == len(stats_samples)
    feed_health_pass = all(
        v.get("sequence_gap_events", 0) == 0 and v.get("ws_error_events", 0) == 0
        for v in feed_health.values()
    )

    final_stats = (stats_samples[-1].get("stats") or {}) if stats_samples else {}
    final_ledger = final_stats.get("simulation_ledger") or {}
    final_realized_pnl = as_float(final_ledger.get("realized_pnl"), 0.0)
    ledger_trade_pnl_diff = final_realized_pnl - total_net_pnl
    ledger_reconciliation_pass = abs(ledger_trade_pnl_diff) <= 0.05

    report = {
        "started_at": start_iso,
        "finished_at": dt.datetime.fromtimestamp(end_ms / 1000.0, tz=dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "duration_seconds": round((end_ms - start_ms) / 1000.0, 3),
        "sample_count": len(stats_samples),
        "sample_interval_seconds": args.sample_interval_seconds,
        "reset": {
            "simulation_reset_ok": bool(reset.get("ok")),
            "validation_trade_reset_ok": bool(reset_trades.get("ok")),
            "baseline_bankroll": args.baseline_bankroll,
        },
        "trade_log": {
            "configured_path": args.trade_log_path,
            "reported_path": reported_trade_path,
            "resolved_path": str(trade_log_path),
            "exists": trade_log_path.exists(),
        },
        "equity": {
            "start": round(start_equity, 8),
            "end": round(end_equity, 8),
            "delta": round(end_equity - start_equity, 8),
            "min": round(min(equity_series), 8) if equity_series else round(start_equity, 8),
            "max": round(max(equity_series), 8) if equity_series else round(start_equity, 8),
            "max_drawdown_abs": round(max_drawdown_abs, 8),
            "max_drawdown_pct": round(max_drawdown_pct, 4),
        },
        "activity": {
            "avg_active_signals": round(sum(signals_series) / max(1, len(signals_series)), 6),
            "avg_active_markets": round(sum(markets_series) / max(1, len(markets_series)), 6),
            "paper_mode_samples": paper_mode_samples,
        },
        "trades": {
            "count": len(trades),
            "net_pnl": round(total_net_pnl, 8),
        },
        "reconciliation": {
            "ledger_realized_pnl": round(final_realized_pnl, 8),
            "trade_log_net_pnl": round(total_net_pnl, 8),
            "ledger_minus_trade_log": round(ledger_trade_pnl_diff, 8),
            "tolerance_abs": 0.05,
            "pass": ledger_reconciliation_pass,
        },
        "feed_health": feed_health,
        "strategies": strategy_summary,
        "recommendations": {
            "promote_candidates": promote_candidates,
            "disable_or_rework": disable_candidates,
            "hold": [k for k, v in strategy_summary.items() if v["action"] == "HOLD"],
        },
        "checks": {
            "mode_safety_pass": mode_safety_pass,
            "feed_health_pass": feed_health_pass,
            "ledger_reconciliation_pass": ledger_reconciliation_pass,
            "overall_pass": mode_safety_pass and feed_health_pass and ledger_reconciliation_pass,
        },
    }

    output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
