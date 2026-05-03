#!/usr/bin/env python3
"""Compare baseline vs stale-loser displacement variants for the path_s
top-2 candidate. Verifier-guarded: refuses to compare runs whose configs
differ on anything other than the displacement fields.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
RESULTS = HERE / "results"

# Default 5-run lineup. Each entry is (label, suffix). The actual directory
# is found by globbing `*_<suffix>` under --results-dir, which makes us
# resilient to date-prefix differences between local runs (TZ-local) and
# GitHub Actions runs (UTC). Override via --runs label=dir,... at the CLI
# to pin to specific paths.
DEFAULT_RUNS = [
    ("baseline (no disp)",        "n2_disp_none"),
    ("A: h>=20, ret<=0%, z>=3",   "n2_disp_h20_ret0_z3"),
    ("B: h>=20, ret<=-5%, z>=3",  "n2_disp_h20_retminus5_z3"),
    ("C: h>=30, ret<=0%, z>=3",   "n2_disp_h30_ret0_z3"),
    ("D: h>=10, ret<=-5%, z>=3",  "n2_disp_h10_retminus5_z3"),
]

DISPLACEMENT_ALLOW = [
    "displacement_enabled",
    "displacement_min_hold",
    "displacement_max_return",
    "displacement_z_min",
    "displacement_max_swaps_per_day",
]


def _load(path: Path):
    if not path.exists():
        return None, None, None
    summary = json.loads((path / "summary.json").read_text())
    trades = pd.read_csv(path / "trade_log.csv")
    disp_log_path = path / "displacement_log.csv"
    disp_log = pd.read_csv(disp_log_path) if disp_log_path.exists() else pd.DataFrame()
    return summary, trades, disp_log


def _verify_compatible(run_paths: list[Path]) -> None:
    from verify_run_config_match import assert_runs_compatible
    if len(run_paths) < 2:
        return
    assert_runs_compatible(run_paths[0], run_paths[1:], allow=DISPLACEMENT_ALLOW)


def _worst_month(equity_df: pd.DataFrame) -> float | None:
    if equity_df.empty:
        return None
    e = equity_df.copy()
    e["date"] = pd.to_datetime(e["date"])
    e = e.set_index("date")
    monthly = e["portfolio_value"].resample("ME").last().pct_change()
    if monthly.empty:
        return None
    return float(monthly.min())


def _drawdown_duration_days(equity_df: pd.DataFrame) -> int | None:
    if equity_df.empty or "drawdown" not in equity_df.columns:
        return None
    dd = equity_df["drawdown"].values
    longest = 0
    cur = 0
    for v in dd:
        if v < 0:
            cur += 1
            longest = max(longest, cur)
        else:
            cur = 0
    return int(longest)


def _calmar(cagr: float | None, mdd: float | None) -> float | None:
    if cagr is None or mdd is None or mdd >= 0:
        return None
    return round(cagr / abs(mdd), 3)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--runs", default=None,
                    help="comma-separated label=dir pairs to override the default lineup")
    ap.add_argument("--results-dir", default=str(RESULTS),
                    help=f"results parent directory (default: {RESULTS})")
    args = ap.parse_args()

    results_dir = Path(args.results_dir)

    if args.runs:
        runs_input: list[tuple[str, str]] = []
        for part in args.runs.split(","):
            label, _, run_name = part.partition("=")
            if not run_name:
                raise SystemExit(f"bad --runs entry: {part!r} (need label=dir)")
            runs_input.append((label.strip(), run_name.strip()))
        # When explicit dirs are given, treat them as exact paths (not suffix globs).
        runs_resolved: list[tuple[str, Path | None]] = [
            (lbl, results_dir / r if (results_dir / r).exists() else None)
            for lbl, r in runs_input
        ]
    else:
        # Suffix-based resolution: glob for any directory matching *_<suffix>.
        # If multiple match (e.g. cloud + local on different dates), pick the
        # newest by mtime — that's the run we just kicked off.
        runs_resolved = []
        for label, suffix in DEFAULT_RUNS:
            cand = sorted(
                [p for p in results_dir.glob(f"*_{suffix}") if p.is_dir()],
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
            runs_resolved.append((label, cand[0] if cand else None))

    runs = [(lbl, p.name if p else "") for lbl, p in runs_resolved]
    existing_paths = [p for _, p in runs_resolved if p is not None]
    if len(existing_paths) < 2:
        print("Need at least 2 runs to compare; only found:", file=sys.stderr)
        for lbl, p in runs_resolved:
            print(f"  {lbl}: {p if p else 'MISSING'}", file=sys.stderr)
        return 2

    # Verifier guard — bail before printing anything if configs disagree.
    _verify_compatible(existing_paths)

    # Identify baseline (first run that exists). Comparisons are anchored to it.
    baseline_path: Path | None = None
    for _, p in runs_resolved:
        if p is not None:
            baseline_path = p
            break

    base_summary = json.loads((baseline_path / "summary.json").read_text()) if baseline_path else None
    base_perf = base_summary["performance"] if base_summary else {}
    base_cagr = base_perf.get("cagr")
    base_mdd = base_perf.get("max_drawdown")
    base_sharpe = base_perf.get("sharpe")

    rows = []
    disp_logs: dict[str, pd.DataFrame] = {}
    trade_dfs: dict[str, pd.DataFrame] = {}
    for (label, _), (_, path) in zip(runs, runs_resolved):
        if path is None:
            rows.append({"variant": label, "status": "MISSING"})
            continue
        summary, trades, disp_log = _load(path)
        if summary is None:
            rows.append({"variant": label, "status": "MISSING"})
            continue
        perf = summary["performance"]
        act = summary["activity"]
        equity_df = pd.read_csv(path / "daily_equity.csv")
        winners = trades[trades["return_pct"] > 0]
        losers = trades[trades["return_pct"] <= 0]
        n_trades = act["n_trades"]
        cagr = perf.get("cagr")
        mdd = perf.get("max_drawdown")
        sharpe = perf.get("sharpe")
        rows.append({
            "variant": label,
            "total_return": (f"{perf['total_return']*100:+.1f}%"
                             if perf.get("total_return") is not None else "—"),
            "CAGR":        f"{cagr*100:+.1f}%" if cagr is not None else "—",
            "Sharpe":      sharpe if sharpe is not None else "—",
            "MDD":         f"{mdd*100:+.1f}%" if mdd is not None else "—",
            "Calmar":      _calmar(cagr, mdd),
            "trades":      n_trades,
            "win_rate":    f"{len(winners)/max(1,n_trades)*100:.0f}%",
            "avg_winner":  f"{winners['return_pct'].mean()*100:+.1f}%" if len(winners) else "—",
            "avg_loser":   f"{losers['return_pct'].mean()*100:+.1f}%"  if len(losers)  else "—",
            "swaps":       act.get("displacement_swaps", 0),
            "worst_month": (f"{_worst_month(equity_df)*100:+.1f}%"
                            if _worst_month(equity_df) is not None else "—"),
            "dd_dur_d":    _drawdown_duration_days(equity_df),
            "ΔCAGR":       (f"{(cagr - base_cagr)*100:+.1f}pp"
                            if cagr is not None and base_cagr is not None else "—"),
            "ΔMDD":        (f"{(mdd - base_mdd)*100:+.1f}pp"
                            if mdd is not None and base_mdd is not None else "—"),
            "ΔSharpe":     (round(sharpe - base_sharpe, 3)
                            if sharpe is not None and base_sharpe is not None else "—"),
        })
        if disp_log is not None and not disp_log.empty:
            disp_logs[label] = disp_log
        trade_dfs[label] = trades

    df = pd.DataFrame(rows)
    print("\n=== Stale-loser displacement vs z=3.0 top-2 baseline ===\n")
    print(df.to_string(index=False))

    if disp_logs:
        print("\n=== Displacement log summary ===\n")
        for label, log in disp_logs.items():
            n = len(log)
            exited = log["exited_ticker"].tolist()
            challengers = log["challenger_ticker"].tolist()
            avg_exit_ret = log["exited_return_so_far"].mean()
            avg_exit_hd  = log["exited_hold_days"].mean()
            # Best/worst displacement by post-swap challenger trade outcome.
            # Match displacement log row to the corresponding trade for
            # the challenger using (entry_date, ticker).
            tdf = trade_dfs.get(label, pd.DataFrame())
            best = worst = None
            if not tdf.empty:
                challenger_keys = list(zip(log["date"], log["challenger_ticker"]))
                # The trade's entry_date == swap date + 1 trading day; we'll
                # match on ticker + entry_date >= swap date instead.
                joined: list[tuple[str, str, float]] = []
                for swap_date, c_tkr in challenger_keys:
                    sub = tdf[(tdf["ticker"] == c_tkr) & (tdf["entry_date"] >= swap_date)]
                    if sub.empty:
                        continue
                    sub = sub.sort_values("entry_date").iloc[0]
                    joined.append((swap_date, c_tkr, float(sub["return_pct"])))
                if joined:
                    joined.sort(key=lambda x: x[2])
                    worst = joined[0]
                    best = joined[-1]
            print(f"\n[{label}]")
            print(f"  swaps:                       {n}")
            print(f"  exited:                      {exited}")
            print(f"  challengers:                 {challengers}")
            print(f"  avg exited return-so-far:    {avg_exit_ret*100:+.2f}%")
            print(f"  avg exited hold days:        {avg_exit_hd:.1f}")
            if best is not None:
                print(f"  best challenger trade:       {best[1]} (entered ~{best[0]}) {best[2]*100:+.1f}%")
            if worst is not None:
                print(f"  worst challenger trade:      {worst[1]} (entered ~{worst[0]}) {worst[2]*100:+.1f}%")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
