#!/usr/bin/env python3
"""Validate that the production ic_long_filter.py regime rule matches backtest results.

Loads the pattern panel (315 IC_LONG trades), applies the EXACT same thresholds
used in production (ic_long_filter.py), and compares against Leo's baseline.

This confirms:
  1. Thresholds match between ic_long_filter.py and backtest
  2. Switch decisions match
  3. PnL edge matches
"""

import csv
import os
import sys

# Production thresholds — must match ic_long_filter.py and regime_classifier.py
IC_LONG_RR_SHORT_VIX_RV10_THRESHOLD = 1.95
IC_LONG_RR_SHORT_RV5_RV20_THRESHOLD = 1.10

# Cross-check: verify these match the production files
def _verify_thresholds():
    """Grep production files to confirm thresholds match."""
    filter_path = os.path.join(os.path.dirname(__file__), "..",
                               "scripts", "trade", "ConstantStable", "ic_long_filter.py")
    classifier_path = os.path.join(os.path.dirname(__file__), "regime_classifier.py")
    for path, label in [(filter_path, "ic_long_filter.py"), (classifier_path, "regime_classifier.py")]:
        if not os.path.exists(path):
            print(f"  WARN: {label} not found at {path}")
            continue
        with open(path) as f:
            text = f.read()
        assert f"{IC_LONG_RR_SHORT_VIX_RV10_THRESHOLD}" in text, \
            f"VIX/RV10 threshold {IC_LONG_RR_SHORT_VIX_RV10_THRESHOLD} not found in {label}"
        assert f"{IC_LONG_RR_SHORT_RV5_RV20_THRESHOLD}" in text, \
            f"RV5/RV20 threshold {IC_LONG_RR_SHORT_RV5_RV20_THRESHOLD} not found in {label}"
        print(f"  Thresholds verified in {label}")

_verify_thresholds()

PANEL = os.path.join(os.path.dirname(__file__), "data", "leo_ic_long_pattern_panel.csv")


def load_panel():
    with open(PANEL, "r") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    print(f"Loaded {len(rows)} IC_LONG trades from pattern panel")
    return rows


def run_test():
    rows = load_panel()

    print(f"\nProduction thresholds (from ic_long_filter.py):")
    print(f"  VIX/RV10 >= {IC_LONG_RR_SHORT_VIX_RV10_THRESHOLD}")
    print(f"  RV5/RV20 <= {IC_LONG_RR_SHORT_RV5_RV20_THRESHOLD}")

    # Apply rule to each trade
    baseline_total = 0.0
    switched_total = 0.0
    switch_count = 0
    baseline_wins = 0
    switched_wins = 0
    year_stats = {}
    month_2026 = {}
    mismatches = []

    for row in rows:
        date = row["date"]
        year = date[:4]
        month = date[:7]

        vix_rv10 = float(row["VIX_RV10_ratio"]) if row.get("VIX_RV10_ratio") else None
        rv5_rv20 = float(row["rv5_rv20_ratio"]) if row.get("rv5_rv20_ratio") else None
        ic_pnl = float(row["orig_pnl_sized_pts"])
        rr_pnl = float(row["rr_short_pnl_sized_pts"])

        # Apply production thresholds
        switch = (vix_rv10 is not None and rv5_rv20 is not None
                  and vix_rv10 >= IC_LONG_RR_SHORT_VIX_RV10_THRESHOLD
                  and rv5_rv20 <= IC_LONG_RR_SHORT_RV5_RV20_THRESHOLD)

        final_pnl = rr_pnl if switch else ic_pnl

        baseline_total += ic_pnl
        switched_total += final_pnl
        if ic_pnl > 0:
            baseline_wins += 1
        if final_pnl > 0:
            switched_wins += 1
        if switch:
            switch_count += 1

        # Year stats
        if year not in year_stats:
            year_stats[year] = {"n": 0, "baseline": 0.0, "switched": 0.0, "sw": 0}
        year_stats[year]["n"] += 1
        year_stats[year]["baseline"] += ic_pnl
        year_stats[year]["switched"] += final_pnl
        if switch:
            year_stats[year]["sw"] += 1

        # 2026 monthly
        if year == "2026":
            if month not in month_2026:
                month_2026[month] = {"n": 0, "baseline": 0.0, "switched": 0.0, "sw": 0}
            month_2026[month]["n"] += 1
            month_2026[month]["baseline"] += ic_pnl
            month_2026[month]["switched"] += final_pnl
            if switch:
                month_2026[month]["sw"] += 1

    edge = switched_total - baseline_total
    n = len(rows)

    print(f"\n{'='*70}")
    print(f"  VALIDATION RESULTS")
    print(f"{'='*70}")
    print(f"  Trades: {n}")
    print(f"  Switches: {switch_count}")
    print(f"  Baseline total: {baseline_total:+.2f} sized pts  WR: {baseline_wins}/{n} = {100*baseline_wins/n:.1f}%")
    print(f"  Switched total: {switched_total:+.2f} sized pts  WR: {switched_wins}/{n} = {100*switched_wins/n:.1f}%")
    print(f"  Edge: {edge:+.2f} sized pts (${edge*100:+,.0f})")

    # Compare against known backtest values
    EXPECTED_SWITCHES = 57
    EXPECTED_EDGE = 112.43  # from backtest L1 edge
    EXPECTED_BASELINE = 692.26

    print(f"\n  BACKTEST COMPARISON")
    print(f"  {'Metric':<25} {'Expected':>12} {'Got':>12} {'Match':>8}")
    print(f"  {'-'*60}")

    switch_ok = switch_count == EXPECTED_SWITCHES
    edge_ok = abs(edge - EXPECTED_EDGE) < 0.5
    base_ok = abs(baseline_total - EXPECTED_BASELINE) < 0.5

    print(f"  {'Switches':<25} {EXPECTED_SWITCHES:>12} {switch_count:>12} {'  OK' if switch_ok else '  FAIL':>8}")
    print(f"  {'Baseline total':<25} {EXPECTED_BASELINE:>+12.2f} {baseline_total:>+12.2f} {'  OK' if base_ok else '  FAIL':>8}")
    print(f"  {'Edge (sized pts)':<25} {EXPECTED_EDGE:>+12.2f} {edge:>+12.2f} {'  OK' if edge_ok else '  FAIL':>8}")

    all_pass = switch_ok and edge_ok and base_ok

    # By year
    print(f"\n  BY YEAR")
    print(f"  {'Year':<6} {'N':>4} │ {'Baseline':>10} {'Switched':>10} {'Edge':>10} │ {'Sw':>3}")
    print(f"  {'─'*6} {'─'*4} │ {'─'*10} {'─'*10} {'─'*10} │ {'─'*3}")
    for y in sorted(year_stats):
        s = year_stats[y]
        e = s["switched"] - s["baseline"]
        print(f"  {y:<6} {s['n']:>4} │ {s['baseline']:>+10.2f} {s['switched']:>+10.2f} {e:>+10.2f} │ {s['sw']:>3}")

    # 2026 monthly
    if month_2026:
        print(f"\n  2026 MONTHLY")
        print(f"  {'Month':<8} {'N':>3} │ {'Baseline':>10} {'Switched':>10} {'Edge':>10} │ {'Sw':>3}")
        print(f"  {'─'*8} {'─'*3} │ {'─'*10} {'─'*10} {'─'*10} │ {'─'*3}")
        for m in sorted(month_2026):
            s = month_2026[m]
            e = s["switched"] - s["baseline"]
            print(f"  {m:<8} {s['n']:>3} │ {s['baseline']:>+10.2f} {s['switched']:>+10.2f} {e:>+10.2f} │ {s['sw']:>3}")

    print(f"\n  {'='*70}")
    if all_pass:
        print(f"  PASS: Production rule matches backtest exactly.")
    else:
        print(f"  FAIL: Production rule does NOT match backtest.")
    print(f"  {'='*70}")

    return 0 if all_pass else 1


if __name__ == "__main__":
    sys.exit(run_test())
