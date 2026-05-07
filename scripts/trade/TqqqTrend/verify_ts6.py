#!/usr/bin/env python3
"""
TS_6 promotion-readiness verifier.

Runs 8 checks against the persisted TS_6 shadow state and the live signal
code path. Use before flipping TQQQ_TS6_ENABLED=true (and as a recurring
post-run sanity check while the overlay is live).

Checks:
  1. State file exists at TQQQ_TS6_STATE_PATH and parses to a valid Ts6OverlayState.
  2. signal_source == "adjusted_qqq_close" (overlay basis matches live signal).
  3. If overlay_state == ACTIVE, qqq_peak_adj is populated and finite-positive.
     If INACTIVE/BLOCKED, peak is None (per state-machine invariant).
  4. overlay_state is one of {ACTIVE, BLOCKED, INACTIVE}.
  5. last_signal_date is the most recent completed QQQ session, OR one
     session behind it (intentional: workflow has not yet run today).
  6. Re-walking yfinance-adjusted QQQ from default state through the
     persisted last_signal_date reproduces the persisted state exactly.
     Catches drift between the persisted state and a fresh replay.
  7. compute_strategy_snapshot keeps baseline_target_sleeve invariant
     across enabled=False vs enabled=True. Only effective_target may
     differ (and only via the overlay's own ACTIVE/BLOCKED logic).
  8. load_state with a missing path and enabled=True raises (fail-closed).

Exit 0 if all pass, else 1.
"""
from __future__ import annotations

import argparse
import json
import sys
import tempfile
from dataclasses import asdict
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from overlay_state import (  # noqa: E402
    DEFAULT_SIGNAL_SOURCE,
    VALID_OVERLAY_STATES,
    advance_state,
    default_state,
    load_state,
    state_path_from_env,
)
from place import _baseline_state_series, load_adjusted_qqq_history  # noqa: E402


def _ok(label: str, detail: str = "") -> tuple[bool, str]:
    msg = f"[ OK ] {label}"
    if detail:
        msg += f"  ({detail})"
    return True, msg


def _fail(label: str, detail: str) -> tuple[bool, str]:
    return False, f"[FAIL] {label}  ({detail})"


def check_1_state_loads(path: Path) -> tuple[bool, str, object]:
    if not path.exists():
        passed, msg = _fail("1. state file exists", f"missing: {path}")
        return passed, msg, None
    try:
        state = load_state(path, enabled=False)
    except Exception as exc:
        passed, msg = _fail("1. state parses + validates", str(exc))
        return passed, msg, None
    passed, msg = _ok("1. state file exists and validates", f"path={path}")
    return passed, msg, state


def check_2_signal_basis(state) -> tuple[bool, str]:
    if state.signal_source == DEFAULT_SIGNAL_SOURCE:
        return _ok("2. signal basis is adjusted QQQ", state.signal_source)
    return _fail("2. signal basis is adjusted QQQ", f"got {state.signal_source!r}")


def check_3_peak_populated(state) -> tuple[bool, str]:
    if state.overlay_state == "ACTIVE":
        if state.qqq_peak_adj is not None and state.qqq_peak_adj > 0:
            return _ok("3. qqq_peak_adj populated for ACTIVE",
                       f"{state.qqq_peak_adj:.4f}")
        return _fail("3. qqq_peak_adj populated for ACTIVE",
                     f"got {state.qqq_peak_adj!r}")
    if state.qqq_peak_adj is None:
        return _ok("3. qqq_peak_adj None outside ACTIVE",
                   f"overlay_state={state.overlay_state}")
    return _fail("3. qqq_peak_adj None outside ACTIVE",
                 f"overlay_state={state.overlay_state} but peak={state.qqq_peak_adj}")


def check_4_overlay_state(state) -> tuple[bool, str]:
    if state.overlay_state in VALID_OVERLAY_STATES:
        return _ok("4. overlay_state in {ACTIVE,BLOCKED,INACTIVE}", state.overlay_state)
    return _fail("4. overlay_state in {ACTIVE,BLOCKED,INACTIVE}",
                 f"got {state.overlay_state!r}")


def check_5_recent_date(state, qqq_index: pd.DatetimeIndex) -> tuple[bool, str]:
    if not state.last_signal_date:
        return _fail("5. last_signal_date current or one session behind", "is None")
    most_recent_close = qqq_index[-1].date().isoformat()
    one_behind = qqq_index[-2].date().isoformat() if len(qqq_index) >= 2 else None
    if state.last_signal_date == most_recent_close:
        return _ok("5. last_signal_date is most recent close", state.last_signal_date)
    if state.last_signal_date == one_behind:
        return _ok("5. last_signal_date one session behind (workflow not yet run today)",
                   f"persisted={state.last_signal_date} latest_close={most_recent_close}")
    return _fail("5. last_signal_date current or one session behind",
                 f"persisted={state.last_signal_date} latest_close={most_recent_close}")


def check_6_replay_matches(state, qqq: pd.Series) -> tuple[bool, str]:
    """Re-walk from default through state.last_signal_date; expect identity."""
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
    df = df.dropna(subset=["sma50", "sma150", "sma200", "ret63", "baseline_state"])
    cutoff = pd.Timestamp(state.last_signal_date)
    df = df[df.index <= cutoff]
    if df.empty:
        return _fail("6. replay reproduces persisted state",
                     "no rows up to last_signal_date — yfinance history too short")
    walked = default_state(enabled=False)
    for date, row in df.iterrows():
        walked = advance_state(
            walked,
            signal_date=date.date().isoformat(),
            baseline_state=str(row["baseline_state"]),
            adj_close=float(row["close"]),
            enabled=False,
        )
    # Compare structural fields; ignore enabled/updated_at_utc.
    fields = (
        "baseline_state", "overlay_state", "qqq_peak_adj", "stop_threshold_pct",
        "stopped_on_date", "stopped_on_adj_close", "last_signal_date",
        "last_adj_qqq_close", "last_decision", "signal_source",
        "execution_source", "overlay_name",
    )
    diffs = []
    for f in fields:
        a = getattr(state, f)
        b = getattr(walked, f)
        if a != b:
            diffs.append(f"{f}: persisted={a!r} replay={b!r}")
    if not diffs:
        return _ok("6. replay reproduces persisted state",
                   f"walked {len(df)} rows, identical")
    return _fail("6. replay reproduces persisted state", "; ".join(diffs))


def check_7_baseline_invariant() -> tuple[bool, str]:
    """compute_strategy_snapshot must keep baseline_target_sleeve identical
    across enabled flag toggles."""
    from place import compute_strategy_snapshot  # noqa: WPS433
    disabled = compute_strategy_snapshot(persist_overlay_state=False)
    # Temporarily flip enabled via env-style override by passing through
    # overlay_state machinery directly.
    import os
    prior = os.environ.get("TQQQ_TS6_ENABLED")
    os.environ["TQQQ_TS6_ENABLED"] = "true"
    try:
        enabled = compute_strategy_snapshot(persist_overlay_state=False)
    finally:
        if prior is None:
            os.environ.pop("TQQQ_TS6_ENABLED", None)
        else:
            os.environ["TQQQ_TS6_ENABLED"] = prior
    if disabled["baseline_target_sleeve"] != enabled["baseline_target_sleeve"]:
        return _fail("7. enabling TS_6 leaves baseline_target unchanged",
                     f"disabled={disabled['baseline_target_sleeve']} "
                     f"enabled={enabled['baseline_target_sleeve']}")
    if disabled["baseline_state"] != enabled["baseline_state"]:
        return _fail("7. enabling TS_6 leaves baseline_state unchanged",
                     f"disabled={disabled['baseline_state']} "
                     f"enabled={enabled['baseline_state']}")
    detail = (f"baseline_target={disabled['baseline_target_sleeve']} "
              f"effective(off)={disabled['target_sleeve']} "
              f"effective(on)={enabled['target_sleeve']}")
    return _ok("7. enabling TS_6 changes only effective_target", detail)


def check_8_fail_closed() -> tuple[bool, str]:
    """Enabled overlay with missing state must raise."""
    with tempfile.TemporaryDirectory() as td:
        missing = Path(td) / "missing.json"
        try:
            load_state(missing, enabled=True)
        except RuntimeError:
            return _ok("8. enabled + missing state raises", "RuntimeError as expected")
        except Exception as exc:
            return _fail("8. enabled + missing state raises",
                         f"raised {type(exc).__name__} not RuntimeError")
        return _fail("8. enabled + missing state raises", "no exception raised")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state-path", default=None,
                        help="override TQQQ_TS6_STATE_PATH env / default")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    state_path = Path(args.state_path) if args.state_path else state_path_from_env()
    print("=" * 72)
    print("TS_6 promotion-readiness verification")
    print("=" * 72)

    results: list[tuple[bool, str]] = []

    passed1, msg1, state = check_1_state_loads(state_path)
    results.append((passed1, msg1))
    if not passed1:
        for _, m in results:
            print(m)
        print("\nABORT: cannot continue without valid state.")
        return 1
    print(msg1)

    for fn in (check_2_signal_basis, check_3_peak_populated, check_4_overlay_state):
        ok, msg = fn(state)
        results.append((ok, msg))
        print(msg)

    qqq = load_adjusted_qqq_history()
    ok5, msg5 = check_5_recent_date(state, qqq.index)
    results.append((ok5, msg5))
    print(msg5)

    ok6, msg6 = check_6_replay_matches(state, qqq)
    results.append((ok6, msg6))
    print(msg6)

    ok7, msg7 = check_7_baseline_invariant()
    results.append((ok7, msg7))
    print(msg7)

    ok8, msg8 = check_8_fail_closed()
    results.append((ok8, msg8))
    print(msg8)

    print("-" * 72)
    n_pass = sum(1 for ok, _ in results if ok)
    n_total = len(results)
    print(f"summary: {n_pass}/{n_total} passed")
    if n_pass != n_total:
        return 1
    print("\nALL 8 CONDITIONS PASS — TS_6 plumbing is promotion-ready.")
    print("Promotion still requires explicit:  vars TQQQ_TS6_ENABLED=true")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
