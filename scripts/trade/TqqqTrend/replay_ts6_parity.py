#!/usr/bin/env python3
"""
Replay the live TS_6 overlay state machine over historical adjusted QQQ data
and compare it to the research TS_6 target path.

This is the implementation-parity gate between:
  - research reference: strict-timing TS_6 backtest logic
  - production candidate: persisted overlay_state.advance_state()

Outputs:
  parity_summary.txt
  parity_daily.csv
  parity_mismatches.csv
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
REPO_SCRIPTS = HERE.parent.parent
CONVICTION_BT = REPO_SCRIPTS / "conviction" / "backtest"
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))
if str(CONVICTION_BT) not in sys.path:
    sys.path.insert(0, str(CONVICTION_BT))

from overlay_state import default_state, advance_state  # noqa: E402
from dd_fast_overlay_sweep import DATA_START, compute_signal, load_price_bars  # noqa: E402
from ts_band_strict_robustness import WIN_START, build_variant_target  # noqa: E402


def _target_label(value: float) -> str:
    return "TQQQ" if float(value) > 0 else "BIL"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--end", default=pd.Timestamp.today().strftime("%Y-%m-%d"))
    parser.add_argument("--out-dir", default="out/tqqq_ts6_replay_parity")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    bars, _ = load_price_bars(["QQQ"], DATA_START, args.end)
    qqq = bars["QQQ"].set_index("date")["close"].sort_index()
    sig = compute_signal(qqq)
    sig_ready = sig.dropna(subset=["sma50", "sma150", "sma200", "ret63"])
    full_idx = qqq.index.intersection(sig.index)
    full_idx = full_idx[
        (full_idx >= max(WIN_START, sig_ready.index[0]))
        & (full_idx <= pd.Timestamp(args.end))
    ]

    ref_target, ref_stop_signal = build_variant_target(sig, full_idx, qqq, 0.06)
    state = default_state(enabled=True)

    rows = []
    for date in full_idx:
        baseline_state = str(sig.loc[date, "state"])
        adj_close = float(qqq.loc[date])
        state = advance_state(
            state,
            signal_date=date.date().isoformat(),
            baseline_state=baseline_state,
            adj_close=adj_close,
            enabled=True,
        )
        ref_value = float(ref_target.loc[date])
        ref_label = _target_label(ref_value)
        live_label = state.target_sleeve()
        live_stop_triggered = state.last_decision == "EXIT_TO_BIL" and state.stopped_on_date == date.date().isoformat()
        rows.append({
            "date": date.date().isoformat(),
            "adj_qqq_close": adj_close,
            "baseline_state": baseline_state,
            "reference_target_tqqq": ref_value,
            "reference_target_sleeve": ref_label,
            "reference_stop_triggered": bool(ref_stop_signal.loc[date]),
            "live_overlay_state": state.overlay_state,
            "live_target_sleeve": live_label,
            "live_peak_adj": state.qqq_peak_adj,
            "live_stopped_on_date": state.stopped_on_date,
            "live_last_decision": state.last_decision,
            "live_stop_triggered": live_stop_triggered,
            "match_target": live_label == ref_label,
            "match_stop_trigger": live_stop_triggered == bool(ref_stop_signal.loc[date]),
        })

    daily = pd.DataFrame(rows)
    mismatches = daily[(~daily["match_target"]) | (~daily["match_stop_trigger"])].copy()
    daily.to_csv(out_dir / "parity_daily.csv", index=False)
    mismatches.to_csv(out_dir / "parity_mismatches.csv", index=False)

    lines = []
    lines.append("=" * 100)
    lines.append("TS_6 Replay Parity")
    lines.append("=" * 100)
    lines.append(f"Window: {full_idx[0].date()} -> {full_idx[-1].date()}")
    lines.append(f"Rows compared: {len(daily)}")
    lines.append(f"Target mismatches: {int((~daily['match_target']).sum())}")
    lines.append(f"Stop-trigger mismatches: {int((~daily['match_stop_trigger']).sum())}")
    lines.append(f"Overall parity: {mismatches.empty}")
    lines.append("")

    if mismatches.empty:
        lines.append("Result: PASS")
        lines.append("The live overlay_state state machine reproduces the research TS_6 target path.")
    else:
        lines.append("Result: FAIL")
        lines.append("First mismatches:")
        for _, row in mismatches.head(20).iterrows():
            lines.append(
                f"  {row['date']}: ref={row['reference_target_sleeve']} "
                f"live={row['live_target_sleeve']} "
                f"ref_stop={row['reference_stop_triggered']} "
                f"live_stop={row['live_stop_triggered']}"
            )

    (out_dir / "parity_summary.txt").write_text("\n".join(lines))
    print((out_dir / "parity_summary.txt").read_text())


if __name__ == "__main__":
    main()
