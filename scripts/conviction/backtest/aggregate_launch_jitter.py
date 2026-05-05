#!/usr/bin/env python3
"""Aggregate Path-S launch-date jitter results.

Consumes a directory of artifacts from path_s_launch_jitter.yml workflow
(downloaded via `gh run download`) and produces the comparison tables
specified in the spec:

  offset_summary_single.csv
  offset_summary_top2.csv
  offset_trade_overlap_single.csv
  offset_trade_overlap_top2.csv
  baseline_trade_capture_single.csv
  baseline_trade_capture_top2.csv
  first_trade_branching_single.csv
  first_trade_branching_top2.csv
  report.md

Usage:
  python aggregate_launch_jitter.py --artifacts /tmp/jitter_results --out /tmp/jitter_report
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd
import numpy as np


def load_runs(artifacts_dir: Path) -> dict[str, dict]:
    """Returns {label: {summary, trades, equity}} keyed by 'single_off000' style."""
    runs = {}
    for summary_path in sorted(artifacts_dir.rglob("summary.json")):
        run_dir = summary_path.parent
        run_name = run_dir.name
        # Match 'YYYY-MM-DD_replay_<days>d_<source>_<mode>_off<NNN>'
        if "_off" not in run_name:
            continue
        # Extract suffix after _<source>_
        parts = run_name.split("_")
        # find "off" index
        for i, p in enumerate(parts):
            if p.startswith("off"):
                mode = parts[i-1]
                offset_str = p[3:]  # off000 -> 000
                break
        else:
            continue
        label = f"{mode}_off{offset_str}"
        summary = json.loads(summary_path.read_text())
        trade_log = run_dir / "trade_log.csv"
        trades = pd.read_csv(trade_log) if trade_log.exists() else pd.DataFrame()
        equity = pd.read_csv(run_dir / "daily_equity.csv") if (run_dir / "daily_equity.csv").exists() else pd.DataFrame()
        runs[label] = {"summary": summary, "trades": trades, "equity": equity}
    return runs


def offset_summary(runs: dict, mode: str) -> pd.DataFrame:
    rows = []
    for label, data in sorted(runs.items()):
        if not label.startswith(mode + "_"):
            continue
        s = data["summary"]
        p = s["performance"]; a = s["activity"]; w = s["window"]
        trades = data["trades"]
        first_trade = trades.iloc[0] if not trades.empty else None
        offset_str = label.split("_off")[-1]
        rows.append({
            "launch_offset_td": int(offset_str),
            "launch_date": w.get("start"),
            "end_date": w.get("end"),
            "TR": p["total_return"],
            "CAGR": p["cagr"],
            "Sharpe": p["sharpe"],
            "MDD": p["max_drawdown"],
            "Calmar": p["cagr"] / abs(p["max_drawdown"]) if p["max_drawdown"] else None,
            "trades": a["n_trades"],
            "pct_days_in_position": a.get("pct_time_invested"),
            "first_trade_ticker": first_trade["ticker"] if first_trade is not None else None,
            "first_trade_entry_date": first_trade["entry_date"] if first_trade is not None else None,
            "first_trade_return": first_trade["return_pct"] if first_trade is not None else None,
            "top_contributor": _top_contributor(trades)[0],
            "top_contributor_return": _top_contributor(trades)[1],
        })
    df = pd.DataFrame(rows).sort_values("launch_offset_td").reset_index(drop=True)
    return df


def _top_contributor(trades: pd.DataFrame) -> tuple[str | None, float | None]:
    if trades.empty:
        return None, None
    top = trades.iloc[trades["return_pct"].idxmax()]
    return top["ticker"], float(top["return_pct"])


def trade_overlap(runs: dict, mode: str, baseline_label: str) -> pd.DataFrame:
    """Per-offset overlap with baseline trade list."""
    if baseline_label not in runs:
        return pd.DataFrame()
    base_trades = runs[baseline_label]["trades"]
    if base_trades.empty:
        return pd.DataFrame()
    base_tickers = set(base_trades["ticker"])
    base_top5 = set(base_trades.nlargest(5, "return_pct")["ticker"])
    base_top3_pnl = set(base_trades.nlargest(3, "return_pct")["ticker"])
    base_total_pnl = (base_trades["return_pct"]).sum()

    rows = []
    for label, data in sorted(runs.items()):
        if not label.startswith(mode + "_"):
            continue
        offset_str = label.split("_off")[-1]
        offset = int(offset_str)
        trades = data["trades"]
        if trades.empty:
            continue
        tickers = set(trades["ticker"])
        ovlp = base_tickers & tickers
        rows.append({
            "launch_offset_td": offset,
            "exact_ticker_overlap_count": len(ovlp),
            "exact_ticker_overlap_pct": len(ovlp) / len(base_tickers) if base_tickers else 0,
            "ticker_overlap_jaccard": len(ovlp) / len(base_tickers | tickers) if (base_tickers | tickers) else 0,
            "baseline_top5_winners_captured": len(base_top5 & tickers),
            "baseline_top5_winners_missed": len(base_top5 - tickers),
            "baseline_top3_pnl_contributors_captured": len(base_top3_pnl & tickers),
            "pnl_overlap_pct": (
                trades[trades["ticker"].isin(base_tickers)]["return_pct"].sum()
                / base_total_pnl
                if base_total_pnl else 0
            ),
        })
    return pd.DataFrame(rows).sort_values("launch_offset_td").reset_index(drop=True)


def baseline_trade_capture(runs: dict, mode: str, baseline_label: str) -> pd.DataFrame:
    """For each baseline trade, how many offsets captured it."""
    if baseline_label not in runs:
        return pd.DataFrame()
    base_trades = runs[baseline_label]["trades"]
    if base_trades.empty:
        return pd.DataFrame()
    rows = []
    n_offsets = sum(1 for label in runs if label.startswith(mode + "_"))
    for _, bt in base_trades.iterrows():
        captures = []
        for label, data in runs.items():
            if not label.startswith(mode + "_"):
                continue
            other = data["trades"]
            if other.empty:
                continue
            same = other[other["ticker"] == bt["ticker"]]
            if not same.empty:
                # find closest entry date
                base_d = pd.to_datetime(bt["entry_date"])
                other_d = pd.to_datetime(same["entry_date"]).min()
                captures.append({
                    "delay_days": (other_d - base_d).days,
                    "return": float(same["return_pct"].iloc[0]),
                })
        rows.append({
            "baseline_ticker": bt["ticker"],
            "baseline_entry_date": bt["entry_date"],
            "baseline_exit_date": bt["exit_date"],
            "baseline_return": float(bt["return_pct"]),
            "captured_by_n_offsets": len(captures),
            "capture_rate": len(captures) / n_offsets if n_offsets else 0,
            "median_entry_delay_if_captured": (
                float(np.median([c["delay_days"] for c in captures])) if captures else None
            ),
            "avg_return_when_captured": (
                float(np.mean([c["return"] for c in captures])) if captures else None
            ),
        })
    return pd.DataFrame(rows)


def first_trade_branching(runs: dict, mode: str) -> pd.DataFrame:
    """Group offsets by their first traded ticker, summarize each group's outcome."""
    groups: dict[str, list[dict]] = {}
    for label, data in runs.items():
        if not label.startswith(mode + "_"):
            continue
        trades = data["trades"]
        if trades.empty:
            continue
        first_t = trades.iloc[0]["ticker"]
        s = data["summary"]; p = s["performance"]; a = s["activity"]
        groups.setdefault(first_t, []).append({
            "TR": p["total_return"], "CAGR": p["cagr"],
            "MDD": p["max_drawdown"], "trades": a["n_trades"],
        })
    rows = []
    for tkr, items in sorted(groups.items()):
        rows.append({
            "first_trade_ticker": tkr,
            "n_offsets": len(items),
            "avg_TR": float(np.mean([r["TR"] for r in items])),
            "median_TR": float(np.median([r["TR"] for r in items])),
            "avg_CAGR": float(np.mean([r["CAGR"] for r in items])),
            "avg_MDD": float(np.mean([r["MDD"] for r in items])),
            "avg_later_trade_count": float(np.mean([r["trades"] for r in items])),
        })
    return pd.DataFrame(rows).sort_values("avg_TR", ascending=False).reset_index(drop=True)


def percentile_summary(summary_df: pd.DataFrame, mode_label: str) -> dict:
    if summary_df.empty:
        return {}
    s = summary_df
    base = s[s["launch_offset_td"] == 0]
    base_cagr = float(base["CAGR"].iloc[0]) if not base.empty else None
    return {
        "mode": mode_label,
        "n_offsets": len(s),
        "median_CAGR": float(s["CAGR"].median()),
        "p10_CAGR": float(s["CAGR"].quantile(0.10)),
        "p25_CAGR": float(s["CAGR"].quantile(0.25)),
        "p75_CAGR": float(s["CAGR"].quantile(0.75)),
        "p90_CAGR": float(s["CAGR"].quantile(0.90)),
        "min_CAGR": float(s["CAGR"].min()),
        "max_CAGR": float(s["CAGR"].max()),
        "median_MDD": float(s["MDD"].median()),
        "min_MDD": float(s["MDD"].min()),
        "max_MDD": float(s["MDD"].max()),
        "median_Sharpe": float(s["Sharpe"].median()),
        "baseline_CAGR": base_cagr,
        "baseline_percentile": (
            float((s["CAGR"] <= base_cagr).mean() * 100) if base_cagr is not None else None
        ),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--artifacts", required=True, type=Path,
                    help="Directory containing 'gh run download' output")
    ap.add_argument("--out", required=True, type=Path,
                    help="Output directory for CSVs and report")
    args = ap.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)

    runs = load_runs(args.artifacts)
    print(f"Loaded {len(runs)} runs from {args.artifacts}", file=sys.stderr)
    for label in sorted(runs.keys()):
        s = runs[label]["summary"]["performance"]
        print(f"  {label}: TR={s['total_return']*100:+.1f}%  Sharpe={s['sharpe']:.2f}", file=sys.stderr)

    summaries = {}
    for mode in ["single", "top2"]:
        df = offset_summary(runs, mode)
        df.to_csv(args.out / f"offset_summary_{mode}.csv", index=False)
        summaries[mode] = df

        ovlp = trade_overlap(runs, mode, f"{mode}_off000")
        ovlp.to_csv(args.out / f"offset_trade_overlap_{mode}.csv", index=False)

        cap = baseline_trade_capture(runs, mode, f"{mode}_off000")
        cap.to_csv(args.out / f"baseline_trade_capture_{mode}.csv", index=False)

        branch = first_trade_branching(runs, mode)
        branch.to_csv(args.out / f"first_trade_branching_{mode}.csv", index=False)

    # Report
    lines = ["# Path-S Launch Date Jitter — Results", ""]
    for mode in ["single", "top2"]:
        s = summaries[mode]
        if s.empty:
            continue
        ps = percentile_summary(s, mode)
        lines += [
            f"## {mode} ({ps['n_offsets']} offsets)",
            "",
            f"- median CAGR: **{ps['median_CAGR']*100:+.1f}%**",
            f"- p10 / p25 / p75 / p90: {ps['p10_CAGR']*100:+.1f}% / {ps['p25_CAGR']*100:+.1f}% / {ps['p75_CAGR']*100:+.1f}% / {ps['p90_CAGR']*100:+.1f}%",
            f"- min / max CAGR: {ps['min_CAGR']*100:+.1f}% / {ps['max_CAGR']*100:+.1f}%",
            f"- median MDD: {ps['median_MDD']*100:+.1f}% (range {ps['min_MDD']*100:+.1f}% to {ps['max_MDD']*100:+.1f}%)",
            f"- median Sharpe: {ps['median_Sharpe']:.2f}",
            f"- **baseline (offset 0) CAGR**: {ps['baseline_CAGR']*100:+.1f}%",
            f"- **baseline percentile rank**: {ps['baseline_percentile']:.0f}th",
            "",
            "### Per-offset summary",
            "",
            s.to_markdown(index=False, floatfmt=".4f"),
            "",
        ]
    (args.out / "report.md").write_text("\n".join(lines))
    print(f"\nWrote report to {args.out / 'report.md'}", file=sys.stderr)


if __name__ == "__main__":
    main()
