#!/usr/bin/env python3
"""
Test (a): does the slope of the 200d MA at the moment of cross-down filter
the false-positive rate of the 200d trend signal?

Reads episodes_200d.csv produced by spy_ma_regime_study.py and re-runs the
recovery probabilities + depth distribution conditioned on slope regime
(rising / flat / falling).

Falsifiable claim being tested:
  Among the 118 below-200d episodes since 1990, those starting with a
  *falling* 200d MA have:
    - 20-day recovery rate < 50% (vs 83% unconditional)
    - meaningfully fatter intra-episode drawdown tail
    - higher share of "bear" depth bucket (>=15% below MA)

If both hold, slope is a viable single-condition filter.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

DEEP_DD = 0.20
DEPTH_BUCKETS = ["noise", "shallow", "correction", "bear"]


def load(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, parse_dates=["start", "end"])
    df["year"] = df["start"].dt.year
    return df


def slope_breakdown(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for regime in ["rising", "flat", "falling", "unknown"]:
        sub = df[df["slope_regime"] == regime]
        if sub.empty:
            continue
        rows.append({
            "slope_regime": regime,
            "n_episodes": len(sub),
            "share_of_total": round(len(sub) / len(df), 3),
            "median_duration_td": int(sub["duration_td"].median()),
            "p90_duration_td": int(sub["duration_td"].quantile(0.90)),
            "max_duration_td": int(sub["duration_td"].max()),
            "median_depth_below_ma": round(sub["depth_pct_below_ma"].median(), 4),
            "p90_depth_below_ma": round(sub["depth_pct_below_ma"].quantile(0.90), 4),
            "max_depth_below_ma": round(sub["depth_pct_below_ma"].max(), 4),
            "median_intra_dd": round(sub["intra_drawdown"].median(), 4),
            "p90_intra_dd": round(sub["intra_drawdown"].quantile(0.90), 4),
            "max_intra_dd": round(sub["intra_drawdown"].max(), 4),
            "p_deep_dd_ge_20": round((sub["intra_drawdown"] >= DEEP_DD).mean(), 3),
        })
    return pd.DataFrame(rows)


def depth_bucket_table(df: pd.DataFrame) -> pd.DataFrame:
    ct = (
        df.groupby(["slope_regime", "depth_bucket"])
        .size()
        .unstack(fill_value=0)
    )
    for b in DEPTH_BUCKETS:
        if b not in ct.columns:
            ct[b] = 0
    ct = ct[DEPTH_BUCKETS]
    ct["total"] = ct.sum(axis=1)
    for b in DEPTH_BUCKETS:
        ct[f"{b}_pct"] = (ct[b] / ct["total"] * 100).round(1)
    return ct.reset_index()


def recovery_rates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Approximate recovery rates from the duration column:
      duration_td == k  ⇔  the episode lasted exactly k td below the MA.
      So P(recover within H td) = P(duration_td <= H).
    This is an exact computation, not an approximation, given the way
    episodes are defined (consecutive runs below MA).
    """
    rows = []
    for regime in ["rising", "flat", "falling", "all"]:
        sub = df if regime == "all" else df[df["slope_regime"] == regime]
        if sub.empty:
            continue
        n = len(sub)
        rows.append({
            "slope_regime": regime,
            "n": n,
            "p_recover_5td": round((sub["duration_td"] <= 5).mean(), 3),
            "p_recover_10td": round((sub["duration_td"] <= 10).mean(), 3),
            "p_recover_20td": round((sub["duration_td"] <= 20).mean(), 3),
            "p_lasts_60td_plus": round((sub["duration_td"] >= 60).mean(), 3),
        })
    return pd.DataFrame(rows)


def list_falling_episodes(df: pd.DataFrame) -> pd.DataFrame:
    sub = df[df["slope_regime"] == "falling"].sort_values("intra_drawdown",
                                                          ascending=False)
    return sub[[
        "start", "end", "duration_td", "depth_pct_below_ma",
        "intra_drawdown", "depth_bucket", "ma_slope_20d_at_start",
        "vix_at_start", "vix_vix3m_at_start"
    ]].copy()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--episodes", default="/tmp/spy_ma_artifact/episodes_200d.csv")
    ap.add_argument("--out-dir", default="/tmp/spy_ma_artifact")
    args = ap.parse_args()

    out = Path(args.out_dir)
    df = load(Path(args.episodes))

    print(f"# Test (a): slope-of-200d conditional, n={len(df)} episodes\n")

    sb = slope_breakdown(df)
    print("## Per-regime distribution of duration / depth / intra-DD\n")
    print(sb.to_string(index=False))
    print()

    rr = recovery_rates(df)
    print("## Recovery rates by slope regime\n")
    print(rr.to_string(index=False))
    print()

    bt = depth_bucket_table(df)
    print("## Depth-bucket counts by slope regime\n")
    print(bt.to_string(index=False))
    print()

    fall = list_falling_episodes(df)
    print(f"## All falling-200d cross-downs (n={len(fall)})\n")
    print(fall.to_string(index=False))
    print()

    sb.to_csv(out / "slope_breakdown.csv", index=False)
    rr.to_csv(out / "slope_recovery_rates.csv", index=False)
    bt.to_csv(out / "slope_depth_buckets.csv", index=False)
    fall.to_csv(out / "slope_falling_episodes.csv", index=False)
    print(f"[wrote] {out}/slope_*.csv", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
