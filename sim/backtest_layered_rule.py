#!/usr/bin/env python3
"""
Backtest: Layered IC_LONG → RR_SHORT Rule
==========================================

EXACT RULE (two independent layers, OR logic):

  Switch IC_LONG to RR_SHORT when EITHER:

    Layer 1 (IV/RV ratio rule):
      VIX_RV10_ratio >= 1.95   (VIX / RV10_annualized, both in %)
      AND rv5_rv20_ratio <= 1.10  (RV5_ann / RV20_ann)

    Layer 2 (VRP regime classifier):
      signal_confirmed == "AVOID"
      where AVOID = VRP_pctile <= 0.30 OR (BH_danger AND GEX_amplifying)
      with 2-day persistence requirement

  All other structures (IC_SHORT, RR_LONG, RR_SHORT) are UNCHANGED.

DATA SOURCE:
  - IC_LONG trades: leo_ic_long_pattern_panel.csv (315 trades, 2016-2026)
    Has: orig_pnl_pts, orig_pnl_sized_pts, rr_short_pnl_pts, rr_short_pnl_sized_pts
    Has: VIX_RV10_ratio, rv5_rv20_ratio (precomputed from Leo signal data)
  - VRP signals: computed by regime_classifier.py from cache + CBOE data

ASSUMPTIONS:
  1. VIX_RV10_ratio in the pattern panel uses VIX (30-day), NOT VIX1D.
     Column name is "VIX_RV10_ratio" (uppercase VIX).
  2. rv5_rv20_ratio uses annualized RV5 / annualized RV20, both from Leo/GW data.
  3. rr_short_pnl_sized_pts assumes the RR_SHORT was entered at the SAME strikes
     and credits as the IC_LONG would have been, with position sizing applied.
  4. VRP AVOID signal uses trailing 252-day rolling percentile for VRP,
     Bekaert-Hoerova VP_share for harvest/danger, volume-weighted GEX proxy
     for compression/amplification, and 2-day persistence filter.
  5. VRP classifier now includes CBOE VIX3M, VIX9D, and SKEW data
     (fetched from cdn.cboe.com), but these feed the calm-but-scared overlay
     only — the core AVOID signal does NOT depend on them.
  6. NO lookahead: all inputs (VIX, RV, VRP, GEX) are known at time of trade entry.
     VRP uses trailing realized variance (not forward). GEX uses prior-day chain data.

INFERENCES:
  None. All ratios are precomputed in the source data or derived from
  trailing (backward-looking) quantities. No forward-looking data is used.
"""

import sys
import os
import math
from pathlib import Path

import numpy as np
import pandas as pd

# ── Import the regime classifier to get VRP signals ──
sys.path.insert(0, os.path.dirname(__file__))
from regime_classifier import (
    build_daily_panel,
    compute_vrp,
    compute_bh_decomposition,
    compute_gex_daily,
    compute_0dte_vrp,
    compute_calm_scared_overlay,
    compute_composite_signal,
    IC_LONG_RR_SHORT_VIX_RV10_THRESHOLD,
    IC_LONG_RR_SHORT_RV5_RV20_THRESHOLD,
)

# ── Config ──
SIM_DIR = Path(__file__).resolve().parent
DATA_DIR = SIM_DIR / "data"
PANEL_PATH = DATA_DIR / "leo_ic_long_pattern_panel.csv"
VIX_RV10_THRESH = IC_LONG_RR_SHORT_VIX_RV10_THRESHOLD  # 1.95
RV5_RV20_THRESH = IC_LONG_RR_SHORT_RV5_RV20_THRESHOLD  # 1.10
OUT_DETAIL = DATA_DIR / "backtest_layered_rule_detail.csv"
OUT_WINDOW = DATA_DIR / "backtest_layered_rule_window_summary.csv"
OUT_YEARLY = DATA_DIR / "backtest_layered_rule_yearly_summary.csv"
OUT_2026_MONTHLY = DATA_DIR / "backtest_layered_rule_2026_monthly_summary.csv"
OUT_SWITCHED = DATA_DIR / "backtest_layered_rule_switched_trades.csv"


def build_vrp_signals():
    """Run the full regime classifier pipeline and return daily signals."""
    print("=" * 80)
    print("BUILDING VRP REGIME SIGNALS")
    print("=" * 80)
    daily = build_daily_panel()
    daily = compute_vrp(daily)
    daily = compute_bh_decomposition(daily)
    daily = compute_gex_daily(daily)
    daily = compute_0dte_vrp(daily)
    daily = compute_calm_scared_overlay(daily)
    daily = compute_composite_signal(daily)
    return daily


def load_trades():
    """Load IC_LONG trade panel."""
    panel = pd.read_csv(PANEL_PATH)
    panel["date"] = pd.to_datetime(panel["date"])
    return panel


def apply_rules(panel, daily):
    """Apply both layers and combine."""
    # Merge VRP signal onto trades
    vrp_cols = ["date", "signal_confirmed", "harvest_score", "vrp_pctile",
                "vp_share", "bh_signal", "gex_regime"]
    available = [c for c in vrp_cols if c in daily.columns]
    merged = panel.merge(daily[available], on="date", how="left")

    # ── Layer 1: IV/RV ratio rule ──
    merged["L1_vix_rv10_passes"] = merged["VIX_RV10_ratio"] >= VIX_RV10_THRESH
    merged["L1_rv5_rv20_passes"] = merged["rv5_rv20_ratio"] <= RV5_RV20_THRESH
    merged["L1_switch"] = merged["L1_vix_rv10_passes"] & merged["L1_rv5_rv20_passes"]

    # ── Layer 2: VRP AVOID ──
    merged["L2_switch"] = merged["signal_confirmed"] == "AVOID"

    # ── Combined: OR logic ──
    merged["combined_switch"] = merged["L1_switch"] | merged["L2_switch"]

    # ── Compute PnL for each scenario ──
    # Baseline: always IC_LONG
    merged["pnl_baseline"] = merged["orig_pnl_sized_pts"]

    # L1 only: switch to RR_SHORT when L1 fires
    merged["pnl_L1"] = np.where(
        merged["L1_switch"],
        merged["rr_short_pnl_sized_pts"],
        merged["orig_pnl_sized_pts"])

    # L2 only: switch when VRP AVOID
    merged["pnl_L2"] = np.where(
        merged["L2_switch"],
        merged["rr_short_pnl_sized_pts"],
        merged["orig_pnl_sized_pts"])

    # Combined: switch when either fires
    merged["pnl_combined"] = np.where(
        merged["combined_switch"],
        merged["rr_short_pnl_sized_pts"],
        merged["orig_pnl_sized_pts"])

    return merged


def stats(pnl_series, label=""):
    """Compute stats for a PnL series."""
    n = len(pnl_series)
    if n == 0:
        return {"label": label, "n": 0, "total": 0, "avg": 0,
                "wr": 0, "pf": 0, "sharpe": 0}
    total = pnl_series.sum()
    avg = pnl_series.mean()
    wins = (pnl_series > 0).sum()
    wr = wins / n * 100
    gross_win = pnl_series[pnl_series > 0].sum()
    gross_loss = abs(pnl_series[pnl_series < 0].sum())
    pf = gross_win / gross_loss if gross_loss > 0 else float("inf")
    sharpe = avg / pnl_series.std() * math.sqrt(252) if pnl_series.std() > 0 else 0
    return {"label": label, "n": n, "total": total, "avg": avg,
            "wr": wr, "pf": pf, "sharpe": sharpe}


def build_detail_output(merged):
    """Trade-level CSV for QA."""
    detail = merged.copy()
    detail["date"] = pd.to_datetime(detail["date"]).dt.strftime("%Y-%m-%d")
    detail["year"] = pd.to_datetime(detail["date"]).dt.year
    detail["month"] = pd.to_datetime(detail["date"]).dt.strftime("%Y-%m")
    detail["combined_layer"] = np.where(
        detail["L1_switch"] & detail["L2_switch"], "L1+L2",
        np.where(detail["L1_switch"], "L1",
                 np.where(detail["L2_switch"], "L2", "")))
    detail["edge_L1_vs_baseline"] = detail["pnl_L1"] - detail["pnl_baseline"]
    detail["edge_L2_vs_baseline"] = detail["pnl_L2"] - detail["pnl_baseline"]
    detail["edge_combined_vs_baseline"] = detail["pnl_combined"] - detail["pnl_baseline"]

    cols = [
        "date", "year", "month", "settle_date", "structure", "size_mult",
        "VIX_RV10_ratio", "rv5_rv20_ratio", "signal_confirmed", "harvest_score",
        "vrp_pctile", "vp_share", "bh_signal", "gex_regime",
        "L1_vix_rv10_passes", "L1_rv5_rv20_passes", "L1_switch", "L2_switch",
        "combined_switch", "combined_layer",
        "orig_pnl_pts", "rr_short_pnl_pts", "orig_pnl_sized_pts", "rr_short_pnl_sized_pts",
        "pnl_baseline", "pnl_L1", "pnl_L2", "pnl_combined",
        "edge_L1_vs_baseline", "edge_L2_vs_baseline", "edge_combined_vs_baseline",
    ]
    available = [c for c in cols if c in detail.columns]
    return detail[available].sort_values("date").reset_index(drop=True)


def _scenario_metrics(frame, prefix, col):
    """Scenario metrics for summary CSVs."""
    pnl = frame[col]
    s = stats(pnl)
    return {
        f"{prefix}_total": round(float(s["total"]), 3),
        f"{prefix}_avg": round(float(s["avg"]), 4),
        f"{prefix}_wr_pct": round(float(s["wr"]), 1),
        f"{prefix}_pf": round(float(s["pf"]), 4) if s["pf"] < 1e9 else float("inf"),
        f"{prefix}_sharpe": round(float(s["sharpe"]), 4),
    }


def _summary_row(label, frame):
    """One summary row with all scenarios and switch counts."""
    row = {
        "label": label,
        "rows": len(frame),
        "L1_switch_n": int(frame["L1_switch"].sum()),
        "L2_switch_n": int(frame["L2_switch"].sum()),
        "combined_switch_n": int(frame["combined_switch"].sum()),
        "overlap_switch_n": int((frame["L1_switch"] & frame["L2_switch"]).sum()),
    }
    row.update(_scenario_metrics(frame, "baseline", "pnl_baseline"))
    row.update(_scenario_metrics(frame, "L1", "pnl_L1"))
    row.update(_scenario_metrics(frame, "L2", "pnl_L2"))
    row.update(_scenario_metrics(frame, "combined", "pnl_combined"))
    row["edge_L1_vs_baseline"] = round(float(frame["pnl_L1"].sum() - frame["pnl_baseline"].sum()), 3)
    row["edge_L2_vs_baseline"] = round(float(frame["pnl_L2"].sum() - frame["pnl_baseline"].sum()), 3)
    row["edge_combined_vs_baseline"] = round(float(frame["pnl_combined"].sum() - frame["pnl_baseline"].sum()), 3)
    return row


def build_window_summary(merged):
    """All/live/2026 summary CSV."""
    windows = {
        "All": merged,
        "2024-07+": merged[merged["date"] >= "2024-07-01"],
        "2026": merged[merged["date"].dt.year == 2026],
    }
    return pd.DataFrame([_summary_row(name, frame.copy()) for name, frame in windows.items()])


def build_yearly_summary(merged):
    """Per-year summary CSV."""
    rows = []
    for year, frame in merged.groupby(merged["date"].dt.year, sort=True):
        row = _summary_row(str(year), frame.copy())
        row["year"] = int(year)
        rows.append(row)
    out = pd.DataFrame(rows)
    ordered = ["year"] + [c for c in out.columns if c not in {"year", "label"}]
    return out[ordered]


def build_2026_monthly_summary(merged):
    """Per-month 2026 summary CSV."""
    df = merged[merged["date"].dt.year == 2026].copy()
    rows = []
    for month, frame in df.groupby(df["date"].dt.strftime("%Y-%m"), sort=True):
        row = _summary_row(month, frame.copy())
        row["month"] = month
        rows.append(row)
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    ordered = ["month"] + [c for c in out.columns if c not in {"month", "label"}]
    return out[ordered]


def build_switched_detail(merged):
    """Switched rows only for quick inspection."""
    switched = build_detail_output(merged)
    switched = switched[switched["combined_switch"]].copy()
    return switched.reset_index(drop=True)


def write_outputs(merged):
    """Persist CSV artifacts for QA."""
    detail = build_detail_output(merged)
    window = build_window_summary(merged)
    yearly = build_yearly_summary(merged)
    monthly = build_2026_monthly_summary(merged)
    switched = build_switched_detail(merged)

    detail.to_csv(OUT_DETAIL, index=False)
    window.to_csv(OUT_WINDOW, index=False)
    yearly.to_csv(OUT_YEARLY, index=False)
    monthly.to_csv(OUT_2026_MONTHLY, index=False)
    switched.to_csv(OUT_SWITCHED, index=False)

    return {
        "detail": OUT_DETAIL,
        "window": OUT_WINDOW,
        "yearly": OUT_YEARLY,
        "monthly": OUT_2026_MONTHLY,
        "switched": OUT_SWITCHED,
    }


def print_stats_table(rows, title=""):
    """Print a formatted stats table."""
    if title:
        print(f"\n  {title}")
    print(f"  {'Scenario':<22} {'N':>5} {'Total':>10} {'Avg':>9} "
          f"{'WR%':>7} {'PF':>6} {'Sharpe':>7}")
    print(f"  {'─' * 22} {'─' * 5} {'─' * 10} {'─' * 9} "
          f"{'─' * 7} {'─' * 6} {'─' * 7}")
    for r in rows:
        pf_str = f"{r['pf']:.2f}" if r['pf'] < 100 else "inf"
        print(f"  {r['label']:<22} {r['n']:>5} {r['total']:>+10.2f} "
              f"{r['avg']:>+9.4f} {r['wr']:>6.1f}% {pf_str:>6} "
              f"{r['sharpe']:>+7.2f}")


def report(merged):
    """Full report."""
    print("\n" + "#" * 80)
    print("#  BACKTEST: LAYERED IC_LONG → RR_SHORT RULE")
    print("#" * 80)
    print(f"\n  Output CSVs:")
    print(f"    Detail:    {OUT_DETAIL}")
    print(f"    Window:    {OUT_WINDOW}")
    print(f"    Yearly:    {OUT_YEARLY}")
    print(f"    2026 Mo:   {OUT_2026_MONTHLY}")
    print(f"    Switched:  {OUT_SWITCHED}")

    # ── Rule specification ──
    print(f"""
  EXACT RULE SPECIFICATION
  ========================

  Layer 1 (IV/RV ratio):
    IF  VIX_RV10_ratio >= {VIX_RV10_THRESH}
    AND rv5_rv20_ratio <= {RV5_RV20_THRESH}
    THEN switch IC_LONG → RR_SHORT

  Layer 2 (VRP regime):
    IF  signal_confirmed == "AVOID"
    THEN switch IC_LONG → RR_SHORT

  Combined:
    Switch when Layer 1 OR Layer 2 fires.
    All other structures unchanged.

  Thresholds imported from: regime_classifier.py
    IC_LONG_RR_SHORT_VIX_RV10_THRESHOLD = {VIX_RV10_THRESH}
    IC_LONG_RR_SHORT_RV5_RV20_THRESHOLD = {RV5_RV20_THRESH}
""")

    # ── Switch counts ──
    n = len(merged)
    n_l1 = merged["L1_switch"].sum()
    n_l2 = merged["L2_switch"].sum()
    n_both = (merged["L1_switch"] & merged["L2_switch"]).sum()
    n_combined = merged["combined_switch"].sum()

    print(f"  SWITCH COUNTS (out of {n} IC_LONG trades)")
    print(f"  {'─' * 50}")
    print(f"    Layer 1 fires:          {n_l1:>4}")
    print(f"    Layer 2 fires:          {n_l2:>4}")
    print(f"    Both fire (overlap):    {n_both:>4}")
    print(f"    Combined (L1 OR L2):    {n_combined:>4}")
    print(f"    Unchanged:              {n - n_combined:>4}")

    # ── Overall results ──
    scenarios = [
        stats(merged["pnl_baseline"], "Baseline (all IC_LONG)"),
        stats(merged["pnl_L1"], "L1 only (IV/RV rule)"),
        stats(merged["pnl_L2"], "L2 only (VRP AVOID)"),
        stats(merged["pnl_combined"], "Combined (L1 OR L2)"),
    ]
    print_stats_table(scenarios, "OVERALL PERFORMANCE (315 IC_LONG trades, sized pts)")

    # ── Edge calculation ──
    base_total = merged["pnl_baseline"].sum()
    for label, col in [("L1", "pnl_L1"), ("L2", "pnl_L2"),
                        ("Combined", "pnl_combined")]:
        edge = merged[col].sum() - base_total
        print(f"    {label} edge vs baseline: {edge:>+10.2f} sized pts "
              f"(${edge * 100:>+,.0f})")

    # ── By year ──
    merged["year"] = merged["date"].dt.year
    years = sorted(merged["year"].unique())

    print(f"\n  BY YEAR")
    print(f"  {'Year':<6} {'N':>4} │ {'Baseline':>10} {'L1':>10} "
          f"{'L2':>10} {'Combined':>10} │ {'L1 Sw':>5} {'L2 Sw':>5} "
          f"{'Comb':>5}")
    print(f"  {'─' * 6} {'─' * 4} │ {'─' * 10} {'─' * 10} "
          f"{'─' * 10} {'─' * 10} │ {'─' * 5} {'─' * 5} {'─' * 5}")

    for yr in years:
        ydf = merged[merged["year"] == yr]
        yn = len(ydf)
        b = ydf["pnl_baseline"].sum()
        l1 = ydf["pnl_L1"].sum()
        l2 = ydf["pnl_L2"].sum()
        cb = ydf["pnl_combined"].sum()
        s1 = ydf["L1_switch"].sum()
        s2 = ydf["L2_switch"].sum()
        sc = ydf["combined_switch"].sum()
        print(f"  {yr:<6} {yn:>4} │ {b:>+10.2f} {l1:>+10.2f} "
              f"{l2:>+10.2f} {cb:>+10.2f} │ {s1:>5} {s2:>5} {sc:>5}")

    # Total row
    print(f"  {'─' * 6} {'─' * 4} │ {'─' * 10} {'─' * 10} "
          f"{'─' * 10} {'─' * 10} │ {'─' * 5} {'─' * 5} {'─' * 5}")
    b = merged["pnl_baseline"].sum()
    l1 = merged["pnl_L1"].sum()
    l2 = merged["pnl_L2"].sum()
    cb = merged["pnl_combined"].sum()
    s1 = merged["L1_switch"].sum()
    s2 = merged["L2_switch"].sum()
    sc = merged["combined_switch"].sum()
    print(f"  {'TOTAL':<6} {n:>4} │ {b:>+10.2f} {l1:>+10.02f} "
          f"{l2:>+10.2f} {cb:>+10.2f} │ {s1:>5} {s2:>5} {sc:>5}")

    # ── By year: edge vs baseline ──
    print(f"\n  EDGE vs BASELINE BY YEAR (sized pts)")
    print(f"  {'Year':<6} {'N':>4} │ {'L1 Edge':>10} {'L2 Edge':>10} "
          f"{'Combined':>10}")
    print(f"  {'─' * 6} {'─' * 4} │ {'─' * 10} {'─' * 10} {'─' * 10}")
    for yr in years:
        ydf = merged[merged["year"] == yr]
        b = ydf["pnl_baseline"].sum()
        print(f"  {yr:<6} {len(ydf):>4} │ "
              f"{ydf['pnl_L1'].sum() - b:>+10.2f} "
              f"{ydf['pnl_L2'].sum() - b:>+10.2f} "
              f"{ydf['pnl_combined'].sum() - b:>+10.2f}")
    b = merged["pnl_baseline"].sum()
    print(f"  {'─' * 6} {'─' * 4} │ {'─' * 10} {'─' * 10} {'─' * 10}")
    print(f"  {'TOTAL':<6} {n:>4} │ "
          f"{merged['pnl_L1'].sum() - b:>+10.2f} "
          f"{merged['pnl_L2'].sum() - b:>+10.2f} "
          f"{merged['pnl_combined'].sum() - b:>+10.2f}")

    # ── 2026 monthly breakdown ──
    t26 = merged[merged["year"] == 2026].copy()
    if len(t26) > 0:
        t26["month"] = t26["date"].dt.month
        t26["month_label"] = t26["date"].dt.strftime("%b")

        print(f"\n  2026 MONTHLY BREAKDOWN")
        print(f"  {'Month':<6} {'N':>3} │ {'Baseline':>9} {'Combined':>9} "
              f"{'Edge':>9} │ {'Switches':>8} {'Base WR':>8} {'Comb WR':>8}")
        print(f"  {'─' * 6} {'─' * 3} │ {'─' * 9} {'─' * 9} "
              f"{'─' * 9} │ {'─' * 8} {'─' * 8} {'─' * 8}")

        for mo in sorted(t26["month"].unique()):
            mdf = t26[t26["month"] == mo]
            mn = len(mdf)
            mlabel = mdf["month_label"].iloc[0]
            b = mdf["pnl_baseline"].sum()
            cb = mdf["pnl_combined"].sum()
            edge = cb - b
            sw = mdf["combined_switch"].sum()
            bwr = (mdf["pnl_baseline"] > 0).sum() / mn * 100
            cwr = (mdf["pnl_combined"] > 0).sum() / mn * 100
            print(f"  {mlabel:<6} {mn:>3} │ {b:>+9.2f} {cb:>+9.2f} "
                  f"{edge:>+9.2f} │ {sw:>8} {bwr:>7.1f}% {cwr:>7.1f}%")

        b26 = t26["pnl_baseline"].sum()
        c26 = t26["pnl_combined"].sum()
        print(f"  {'─' * 6} {'─' * 3} │ {'─' * 9} {'─' * 9} "
              f"{'─' * 9} │ {'─' * 8} {'─' * 8} {'─' * 8}")
        print(f"  {'2026':<6} {len(t26):>3} │ {b26:>+9.2f} {c26:>+9.2f} "
              f"{c26 - b26:>+9.2f} │ {t26['combined_switch'].sum():>8} "
              f"{(t26['pnl_baseline'] > 0).sum() / len(t26) * 100:>7.1f}% "
              f"{(t26['pnl_combined'] > 0).sum() / len(t26) * 100:>7.1f}%")

    # ── 2026 trade-by-trade ──
    if len(t26) > 0:
        print(f"\n  2026 TRADE-BY-TRADE")
        print(f"  {'Date':<12} {'VIX':>5} {'VIX/RV10':>8} {'RV5/20':>7} "
              f"{'L1':>3} {'VRP':>8} {'L2':>3} {'Comb':>4} │ "
              f"{'IC_L PnL':>9} {'RR_S PnL':>9} {'Final':>9} {'W/L':>4}")
        print(f"  {'─' * 12} {'─' * 5} {'─' * 8} {'─' * 7} "
              f"{'─' * 3} {'─' * 8} {'─' * 3} {'─' * 4} │ "
              f"{'─' * 9} {'─' * 9} {'─' * 9} {'─' * 4}")

        for _, r in t26.sort_values("date").iterrows():
            dt = r["date"].strftime("%Y-%m-%d")
            vix = r["vix_pct"]
            vr = r["VIX_RV10_ratio"]
            rv = r["rv5_rv20_ratio"]
            l1 = "Y" if r["L1_switch"] else "."
            vrp_sig = r.get("signal_confirmed", "?")
            l2 = "Y" if r["L2_switch"] else "."
            cb = "SW" if r["combined_switch"] else ".."
            ic = r["orig_pnl_sized_pts"]
            rr = r["rr_short_pnl_sized_pts"]
            final = r["pnl_combined"]
            wl = "W" if final > 0 else "L"

            print(f"  {dt:<12} {vix:>5.1f} {vr:>8.4f} {rv:>7.4f} "
                  f" {l1:>2} {vrp_sig:>8} {l2:>3} {cb:>4} │ "
                  f"{ic:>+9.2f} {rr:>+9.2f} {final:>+9.2f} {wl:>4}")

    # ── Switched trades detail ──
    switched = merged[merged["combined_switch"]]
    if len(switched) > 0:
        print(f"\n  ALL SWITCHED TRADES ({len(switched)} trades)")
        print(f"  {'Date':<12} {'Year':>4} {'Layer':>6} │ "
              f"{'IC_L PnL':>9} {'RR_S PnL':>9} {'Edge':>9}")
        print(f"  {'─' * 12} {'─' * 4} {'─' * 6} │ "
              f"{'─' * 9} {'─' * 9} {'─' * 9}")

        for _, r in switched.sort_values("date").iterrows():
            dt = r["date"].strftime("%Y-%m-%d")
            yr = r["year"]
            layer = "L1+L2" if r["L1_switch"] and r["L2_switch"] else (
                "L1" if r["L1_switch"] else "L2")
            ic = r["orig_pnl_sized_pts"]
            rr = r["rr_short_pnl_sized_pts"]
            edge = rr - ic
            print(f"  {dt:<12} {yr:>4} {layer:>6} │ "
                  f"{ic:>+9.2f} {rr:>+9.2f} {edge:>+9.2f}")

        total_ic = switched["orig_pnl_sized_pts"].sum()
        total_rr = switched["rr_short_pnl_sized_pts"].sum()
        print(f"  {'─' * 12} {'─' * 4} {'─' * 6} │ "
              f"{'─' * 9} {'─' * 9} {'─' * 9}")
        print(f"  {'TOTAL':<12} {'':>4} {'':>6} │ "
              f"{total_ic:>+9.2f} {total_rr:>+9.2f} "
              f"{total_rr - total_ic:>+9.2f}")
        sw_wins = (switched["rr_short_pnl_sized_pts"] > 0).sum()
        sw_losses = len(switched) - sw_wins
        print(f"    Switched trade RR_SHORT WR: {sw_wins}/{len(switched)} "
              f"= {sw_wins / len(switched) * 100:.1f}%")

    # ── Live period (2024-07-01+) ──
    live = merged[merged["date"] >= "2024-07-01"]
    if len(live) > 0:
        print(f"\n  LIVE PERIOD (2024-07-01 onward): {len(live)} trades")
        live_scenarios = [
            stats(live["pnl_baseline"], "Baseline"),
            stats(live["pnl_combined"], "Combined"),
        ]
        print_stats_table(live_scenarios)
        live_edge = live["pnl_combined"].sum() - live["pnl_baseline"].sum()
        print(f"    Live edge: {live_edge:>+.2f} sized pts (${live_edge * 100:>+,.0f})")

    # ── Assumptions and inferences ──
    print(f"""
  ════════════════════════════════════════════════════════════════════════════════
  ASSUMPTIONS
  ════════════════════════════════════════════════════════════════════════════════

  1. COLUMN MAPPING:
     - VIX_RV10_ratio uses VIX (30-day implied vol, NOT VIX1D) / RV10 (10-day
       annualized realized vol). Both from Leo/GammaWizard signal data.
     - rv5_rv20_ratio uses RV5_ann / RV20_ann, from Leo/GammaWizard signal data.
     - These ratios are PRE-COMPUTED in the pattern panel CSV. This backtest
       does NOT recompute them — it uses the values Leo provided.

  2. RR_SHORT PnL:
     - rr_short_pnl_sized_pts is precomputed in the pattern panel. It represents
       the PnL of selling a put spread + buying a call spread (reverse risk
       reversal) at the SAME strikes the IC would have used, with Leo's
       position sizing (size_mult) applied.
     - This assumes same entry timing and fill quality as the IC_LONG.

  3. VRP AVOID SIGNAL:
     - Computed from: VRP = VIX² - RV20² (trailing 20-day realized variance)
     - Bekaert-Hoerova decomposition: VP_share = (VIX² - HAR_CV) / VIX²
     - GEX: volume-weighted gamma asymmetry from 1DTE option chain data
     - AVOID fires when: VRP_pctile <= 30th percentile (trailing 252d)
       OR (BH_signal == "danger" AND GEX == "amplification")
     - 2-day persistence required before signal confirms
     - Now enriched with CBOE VIX3M, VIX9D, SKEW data (98%+ coverage)

  4. NO LOOKAHEAD:
     - Layer 1 ratios: computed from data available at close5 (4:05 PM) the
       day before the trade. RV uses trailing realized vol.
     - Layer 2 VRP: uses trailing 252-day VRP percentile + prior-day GEX.
     - RR_SHORT PnL: uses actual settlement, which is correct for backtesting
       (we're measuring what would have happened, not predicting it).

  INFERENCES: NONE
  ════════════════════════════════════════════════════════════════════════════════
  All inputs are either directly observed market data or trailing calculations.
  No model-based predictions, interpolations, or forward-looking adjustments
  are used in the switch decision.
  ════════════════════════════════════════════════════════════════════════════════
""")


def main():
    daily = build_vrp_signals()
    panel = load_trades()
    merged = apply_rules(panel, daily)
    write_outputs(merged)
    report(merged)


if __name__ == "__main__":
    main()
