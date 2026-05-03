#!/usr/bin/env python3
"""Compare baseline vs earnings-blackout variants for the path_s replay.

Usage: invoked after the three variant runs complete. Reads each run's
summary.json + trade_log.csv and prints a side-by-side table.
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
RESULTS = HERE / "results"

RUNS = [
    ("baseline",          "2026-05-03_replay_1460d_massive_n2_eb_none"),
    ("eb -3d replace",    "2026-05-03_replay_1460d_massive_n2_eb_minus3_replace"),
    ("eb -3d skip",       "2026-05-03_replay_1460d_massive_n2_eb_minus3_skip"),
    ("eb -7d replace",    "2026-05-03_replay_1460d_massive_n2_eb_minus7_replace"),
    ("eb -7d skip",       "2026-05-03_replay_1460d_massive_n2_eb_minus7_skip"),
    ("eb -7d/+1d replace","2026-05-03_replay_1460d_massive_n2_eb_minus7p1_replace"),
    ("eb -7d/+1d skip",   "2026-05-03_replay_1460d_massive_n2_eb_minus7p1_skip"),
]


def _load(path: Path) -> tuple[dict, pd.DataFrame] | tuple[None, None]:
    if not path.exists():
        return None, None
    summary = json.loads((path / "summary.json").read_text())
    trades = pd.read_csv(path / "trade_log.csv")
    return summary, trades


def _verify_compatible(run_paths: list[Path]) -> None:
    """Guard: refuse to compare runs whose configs differ on anything other
    than the earnings-blackout fields. Prevents the prior class of bug where
    a blackout test from z=1.5/top-500 got compared against the z=3.0/top-2000
    baseline."""
    from verify_run_config_match import assert_runs_compatible
    if len(run_paths) < 2:
        return
    baseline = run_paths[0]
    others = run_paths[1:]
    assert_runs_compatible(
        baseline, others,
        allow=["earnings_blackout_before",
               "earnings_blackout_after",
               "earnings_blackout_mode"],
    )


def main():
    rows = []
    trade_dfs: dict[str, pd.DataFrame] = {}
    # Config-integrity guard: bails out before any table is printed if the
    # variants don't actually share the candidate baseline config.
    existing_paths = [RESULTS / r for _, r in RUNS if (RESULTS / r).exists()]
    if len(existing_paths) >= 2:
        _verify_compatible(existing_paths)
    for label, run_name in RUNS:
        s, t = _load(RESULTS / run_name)
        if s is None:
            rows.append({"label": label, "run": run_name, "status": "MISSING"})
            continue
        perf = s["performance"]
        act = s["activity"]
        winners = t[t["return_pct"] > 0]
        losers = t[t["return_pct"] <= 0]
        big_losses = (t["return_pct"] <= -0.10).sum()
        rows.append({
            "label": label,
            "trades": act["n_trades"],
            "winners": len(winners),
            "losers": len(losers),
            "win_rate": f"{len(winners)/max(1,act['n_trades'])*100:.0f}%",
            "avg_winner": f"{winners['return_pct'].mean()*100:+.1f}%" if len(winners) else "—",
            "avg_loser":  f"{losers['return_pct'].mean()*100:+.1f}%"  if len(losers)  else "—",
            "lt_-10pct": int(big_losses),
            "total_return": f"{perf['total_return']*100:+.1f}%",
            "CAGR": f"{perf['cagr']*100:+.1f}%",
            "MDD":  f"{perf['max_drawdown']*100:+.1f}%",
            "Sharpe": perf["sharpe"],
            "avg_hold_d": act["avg_holding_days"],
            "pct_invested": f"{act['pct_time_invested']*100:.0f}%",
        })
        trade_dfs[label] = t

    df = pd.DataFrame(rows)
    if "status" in df.columns and df["status"].notna().any():
        print("Some runs missing — re-run them first.")
        print(df[df["status"] == "MISSING"][["label", "run", "status"]].to_string(index=False))
        return

    print("\n=== Earnings-blackout vs baseline (path_s, n=2, 1460d) ===\n")
    print(df.to_string(index=False))

    # Trades dropped per variant (relative to baseline).
    base = trade_dfs.get("baseline")
    if base is not None:
        base_keys = set(zip(base["entry_date"], base["ticker"]))
        print("\n=== Entries the blackout dropped (vs baseline) ===\n")
        for label in [r[0] for r in RUNS[1:]]:
            t = trade_dfs.get(label)
            if t is None:
                continue
            keys = set(zip(t["entry_date"], t["ticker"]))
            dropped = base_keys - keys
            added = keys - base_keys
            print(f"\n[{label}]  dropped {len(dropped)} baseline entries, "
                  f"made {len(added)} new entries")
            if dropped:
                blocks = base[base.apply(lambda r: (r["entry_date"], r["ticker"]) in dropped, axis=1)]
                print(blocks[["entry_date", "ticker", "return_pct", "exit_reason"]]
                      .sort_values("entry_date").to_string(index=False))


if __name__ == "__main__":
    main()
