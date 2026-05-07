#!/usr/bin/env python3
"""
One-shot bootstrap of the TS_6 shadow state file.

Walks the live overlay state machine over yfinance-adjusted QQQ from default
state through the most recent close, then persists. Used to seed the GH
secret `TQQQ_TS6_STATE_JSON` before turning the workflow loose on it.

After this runs, the persisted state's qqq_peak_adj reflects the actual
peak since the current baseline RISKON entry, not just today's close —
matching what the research reference TS_6 would carry today.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from overlay_state import (  # noqa: E402
    DEFAULT_STATE_PATH,
    advance_state,
    default_state,
    save_state,
)
from place import _baseline_state_series  # noqa: E402


def build_history(start: str = "2024-06-01") -> pd.DataFrame:
    import yfinance as yf

    qqq = yf.Ticker("QQQ").history(start=start, auto_adjust=True)["Close"].astype(float)
    qqq.index = pd.to_datetime(qqq.index).tz_localize(None).normalize()
    if qqq.empty:
        raise RuntimeError("adjusted QQQ history is empty")
    df = pd.DataFrame({"close": qqq})
    df["sma50"] = df["close"].rolling(50).mean()
    df["sma150"] = df["close"].rolling(150).mean()
    df["sma200"] = df["close"].rolling(200).mean()
    df["ret63"] = df["close"].pct_change(63)
    df["score"] = (
        (df["close"] > df["sma150"]).astype(int)
        + (df["sma50"] > df["sma200"]).astype(int)
        + (df["ret63"] > 0).astype(int)
    )
    df["baseline_state"] = _baseline_state_series(df)
    return df.dropna(subset=["sma50", "sma150", "sma200", "ret63", "baseline_state"])


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default=str(DEFAULT_STATE_PATH))
    parser.add_argument(
        "--start",
        default="2024-06-01",
        help="yfinance history start; needs >=200 trading days before "
             "the first signal date",
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="walk and print the final state JSON, do not persist")
    args = parser.parse_args()

    df = build_history(start=args.start)
    state = default_state(enabled=False)
    for date, row in df.iterrows():
        state = advance_state(
            state,
            signal_date=date.date().isoformat(),
            baseline_state=str(row["baseline_state"]),
            adj_close=float(row["close"]),
            enabled=False,
        )

    payload = state.to_payload()
    print(json.dumps(payload, indent=2, sort_keys=True))

    if args.dry_run:
        print("\n(dry-run; not writing)", file=sys.stderr)
        return 0

    out_path = Path(args.out)
    save_state(state, out_path)
    print(f"\nwrote {out_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
