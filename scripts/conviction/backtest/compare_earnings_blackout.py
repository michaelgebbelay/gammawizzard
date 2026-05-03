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
    ("baseline",        "2026-05-03_replay_1460d_massive_n2_eb_baseline"),
    ("eb -7d before",   "2026-05-03_replay_1460d_massive_n2_eb_minus7"),
    ("eb -3d before",   "2026-05-03_replay_1460d_massive_n2_eb_minus3"),
    ("eb -7d / +1d",    "2026-05-03_replay_1460d_massive_n2_eb_minus7_plus1"),
]


def _load(path: Path) -> tuple[dict, pd.DataFrame] | tuple[None, None]:
    if not path.exists():
        return None, None
    summary = json.loads((path / "summary.json").read_text())
    trades = pd.read_csv(path / "trade_log.csv")
    return summary, trades


def main():
    rows = []
    trade_dfs: dict[str, pd.DataFrame] = {}
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
