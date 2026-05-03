#!/usr/bin/env python3
"""
High-flyer scan — calculate all flyers, but default the output to a
theme-collapsed bench (leader / runner_up / early_successor per theme).

Universe: every ticker in themes.yaml + conviction holdings + watchlist.
For each name, computes the full high-flyer ranking; then collapses the
readout by theme so you don't see twelve semi names pretending to be
diversification.

Run:
    bash scripts/conviction/run.sh flyers
    bash scripts/conviction/run.sh flyers --raw
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

sys.path.append(os.path.dirname(__file__))
from data import fetch_daily_bars  # noqa: E402
from replacement_queue import build_theme_bench  # noqa: E402
from stability import compute_stability_factors, rank_universe  # noqa: E402
from theme_rotation import compute_theme_rotation  # noqa: E402

REPO_DIR = Path(__file__).resolve().parent
REPORTS_DIR = REPO_DIR / "reports"
REPORTS_DIR.mkdir(exist_ok=True)


def _load_yaml(path: Path) -> dict:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text()) or {}


def build_universe() -> list[str]:
    """Union of themes.yaml tickers + conviction holdings + watchlist."""
    universe: set[str] = set()
    themes_cfg = _load_yaml(REPO_DIR / "themes.yaml")
    for theme in (themes_cfg.get("themes") or {}).values():
        for t in theme.get("tickers") or []:
            universe.add(str(t).upper())

    cfg = _load_yaml(REPO_DIR / "tickers.yaml")
    for c in cfg.get("conviction") or []:
        if c.get("ticker"):
            universe.add(str(c["ticker"]).upper())
    for t in cfg.get("watchlist") or []:
        universe.add(str(t).upper())

    universe.discard("$SPX")  # not a stock
    return sorted(universe)


def _safe_log_returns(close: pd.Series) -> pd.Series:
    return np.log(close / close.shift(1)).dropna()


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--refresh", action="store_true", help="bust the daily-bar cache")
    ap.add_argument("--top", type=int, default=25, help="how many to print")
    ap.add_argument("--min-dv", type=float, default=25e6, help="min 20d $ volume")
    ap.add_argument("--raw", action="store_true", help="also print the raw ticker leaderboard")
    args = ap.parse_args()

    universe = build_universe()
    print(f"Stable-compounder scan: {len(universe)} tickers", file=sys.stderr)

    # Pull SPX bars first to derive return series for correlations
    spx_results = fetch_daily_bars(["$SPX"], refresh=args.refresh)
    spx_fr = spx_results.get("$SPX")
    spx_log_ret = None
    if spx_fr and not spx_fr.bars.empty:
        spx_close = spx_fr.bars.sort_values("date")["close"].astype(float).reset_index(drop=True)
        spx_log_ret = _safe_log_returns(spx_close)

    bars = fetch_daily_bars(universe, refresh=args.refresh)
    factors_by_ticker = {}
    for tkr, fr in bars.items():
        if fr.error or fr.bars is None or fr.bars.empty:
            continue
        f = compute_stability_factors(tkr, fr.bars, spx_log_returns=spx_log_ret)
        if f is not None:
            factors_by_ticker[tkr] = f

    df = rank_universe(factors_by_ticker, min_dollar_volume=args.min_dv)
    if df.empty:
        print("no eligible names", file=sys.stderr)
        return

    eligible = df[df["eligible"]].copy()
    rotations = compute_theme_rotation(factors_by_ticker, df)
    bench = build_theme_bench(rotations, top=args.top)

    today = datetime.now().strftime("%Y-%m-%d")
    out_path = REPORTS_DIR / f"highflyers_{today}.md"

    n_eligible = int(df['eligible'].sum())
    lines = [
        f"# High-flyer bench — {today}\n",
        f"_Universe: {len(universe)} tickers; {len(factors_by_ticker)} with enough history; "
        f"{n_eligible} currently flying (INTACT or PULLBACK trend)._\n",
        "Default view is collapsed by theme. All names are still scored under the hood; "
        "this just surfaces the bench in a readable way.\n",
        "  - `leader` — highest composite eligible flyer in the theme",
        "  - `runner_up` — second-best eligible flyer in the theme",
        "  - `early_successor` — lower-ranked but cleaner / less-extended bench name",
        "",
        "| Rank | Theme | Label | Score | Leader | Runner-up | Early successor |",
        "|------|-------|-------|-------|--------|-----------|-----------------|",
    ]
    for rank, row in enumerate(bench, 1):
        score_s = f"{row.rotation_score:+.2f}" if row.rotation_score is not None else "—"
        lines.append(
            f"| {rank} | **{row.theme}** | `{row.rotation_label or '—'}` | {score_s} "
            f"| {row.leader or '—'} | {row.runner_up or '—'} | {row.early_successor or '—'} |"
        )

    if args.raw:
        raw_top = eligible.head(args.top).copy()
        lines += [
            "",
            "## Raw ticker leaderboard",
            "",
            "Same computed ranking as before, shown only when `--raw` is passed.",
            "",
            "| Rank | Ticker | Status | 12m | 60d | from 52wH | DSH | from 60dH | R² | Calmar | Smoothness | MDD | β | $ Vol | Composite |",
            "|------|--------|--------|-----|-----|-----------|-----|-----------|-----|--------|------------|-----|----|------|-----------|",
        ]
        for rank, (tkr, row) in enumerate(raw_top.iterrows(), 1):
            def _pct(x, d=1):
                return f"{x*100:+.{d}f}%" if pd.notna(x) else "—"
            def _f(x, d=2):
                return f"{x:.{d}f}" if pd.notna(x) else "—"
            def _i(x):
                return f"{int(x)}" if pd.notna(x) else "—"
            dv = row.get("dollar_vol_20d")
            dv_s = f"${dv/1e6:.0f}M" if pd.notna(dv) else "—"
            status = row.get("trend_status") or "—"
            lines.append(
                f"| {rank} | **{tkr}** | `{status}` "
                f"| {_pct(row['ret_12m'])} | {_pct(row.get('recent_60d_ret'))} "
                f"| {_pct(row['pct_from_52w_high'])} | {_i(row['days_since_52w_high'])} "
                f"| {_pct(row['pct_from_recent_high_60d'])} "
                f"| {_f(row['r_sq_126'])} | {_f(row['calmar'])} | {_f(row['smoothness'])} "
                f"| {_pct(row['mdd_252'])} | {_f(row['beta_spx'])} "
                f"| {dv_s} | **{_f(row['composite'])}** |"
            )
    body = "\n".join(lines) + "\n"
    out_path.write_text(body)
    print(body)
    print(f"\n[wrote {out_path}]", file=sys.stderr)


if __name__ == "__main__":
    main()
