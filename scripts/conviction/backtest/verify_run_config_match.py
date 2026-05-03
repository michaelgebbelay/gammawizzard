#!/usr/bin/env python3
"""
verify_run_config_match.py — config-integrity guard for backtest comparisons.

Refuses to compare two (or more) replay.py runs unless their canonical
config blocks match on every CORE field, except those explicitly allowed
to differ via --allow.

Usage:

    python verify_run_config_match.py \
        --baseline scripts/conviction/backtest/results/<baseline-run>/ \
        --compare scripts/conviction/backtest/results/<other-run>/ \
        --allow cost_bps

Exit codes:
    0 — configs match (after allow-list)
    1 — at least one mismatch on a non-allowed core field
    2 — bad invocation (missing files / args)

This is the only sanctioned way to compare runs in any new comparison
helper. Any script that prints a side-by-side table without calling
this first will eventually compare two species of backtest and produce
a number that looks profitable until it isn't.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Every field a comparison helper might cross-reference. Keep in sync with
# the `canonical_config` dict written by replay.py — adding a new strategy
# knob means adding it here too.
CORE_FIELDS: list[str] = [
    "signal",
    "skew_z_min",
    "skew_z_persistence_days",
    "universe_top_n",
    "ignore_themes",
    "dynamic_themes",
    "positions",
    "regime_gate",
    "exit_rule",
    "trailing_pct",
    "max_hold_days",
    "signal_decay_z",
    "signal_decay_days",
    "days",
    "window_start",
    "window_end",
    "cost_bps",
    "earnings_blackout_before",
    "earnings_blackout_after",
    "earnings_blackout_mode",
    "speculative_only",
    "trend_floor_pct_above_200d",
    "trend_floor_min_ret_60d",
    "displacement_enabled",
    "displacement_min_hold",
    "displacement_max_return",
    "displacement_z_min",
    "displacement_max_swaps_per_day",
]


def load_run_config(run_dir: Path) -> dict:
    """Read summary.json from a run dir and return its config block.

    Falls back to extracting params if `config` block is missing — this
    handles older runs that pre-date the canonical-config schema. Such runs
    can be compared but the verifier will warn loudly.
    """
    summary_path = run_dir / "summary.json"
    if not summary_path.exists():
        raise SystemExit(f"missing summary.json in {run_dir}")
    summary = json.loads(summary_path.read_text())

    if "config" in summary:
        cfg = dict(summary["config"])
        cfg["__schema__"] = "canonical_v1"
        cfg["__run_dir__"] = str(run_dir)
        return cfg

    # Legacy fallback — extract from params + n_positions + window
    print(f"[verify] WARNING: {run_dir} has no 'config' block (older run). "
          f"Comparison may be incomplete.", file=sys.stderr)
    params = summary.get("params", {})
    path_s = params.get("path_s_config") or {}
    cfg = {
        "signal": (f"pathS_{path_s.get('direction', 'bullish')}_skew_flip"
                   if params.get("strategy") == "pathS"
                   else params.get("strategy")),
        "skew_z_min": path_s.get("abs_skew_z_min"),
        "skew_z_persistence_days": 1,
        "universe_top_n": None,  # not in legacy schema
        "ignore_themes": None,
        "dynamic_themes": None,
        "positions": summary.get("n_positions", 1),
        "regime_gate": params.get("regime_gate"),
        "exit_rule": params.get("exit_rule"),
        "trailing_pct": params.get("trailing_pct"),
        "max_hold_days": params.get("max_hold_days"),
        "signal_decay_z": None,
        "signal_decay_days": None,
        "days": None,
        "window_start": summary.get("window", {}).get("start"),
        "window_end": summary.get("window", {}).get("end"),
        "cost_bps": params.get("slippage_bps"),
        "earnings_blackout_before": None,
        "earnings_blackout_after": None,
        "earnings_blackout_mode": None,
        "speculative_only": path_s.get("speculative_only", False),
        "trend_floor_pct_above_200d": path_s.get("min_pct_above_200d"),
        "trend_floor_min_ret_60d": path_s.get("min_ret_60d"),
        "__schema__": "legacy",
        "__run_dir__": str(run_dir),
    }
    return cfg


def compare_configs(baseline: dict, other: dict, allow: set[str]) -> list[tuple[str, object, object]]:
    """Return list of (field, baseline_value, other_value) for mismatches
    on fields not in `allow`."""
    mismatches = []
    for field in CORE_FIELDS:
        if field in allow:
            continue
        b = baseline.get(field)
        o = other.get(field)
        if b != o:
            mismatches.append((field, b, o))
    return mismatches


def format_diff_line(field: str, baseline_val: object, other_val: object) -> str:
    return (f"  {field}:\n"
            f"    baseline: {baseline_val!r}\n"
            f"    compare:  {other_val!r}")


def assert_runs_compatible(
    baseline: Path,
    others: list[Path],
    *,
    allow: list[str] | set[str] | None = None,
    strict: bool = False,
) -> None:
    """Convenience entry-point for comparison scripts.

    Call this at the top of any helper that prints a side-by-side table.
    Raises SystemExit(1) if any non-allowed core field differs between
    runs. Returns None if comparison is safe.

    Example:
        from verify_run_config_match import assert_runs_compatible
        assert_runs_compatible(baseline, [r1, r2], allow={"cost_bps"})
    """
    allow_set = set(allow) if allow else set()
    unknown = allow_set - set(CORE_FIELDS)
    if unknown:
        raise SystemExit(f"[verify] FATAL: unknown allow fields {sorted(unknown)}")

    baseline_cfg = load_run_config(baseline)
    if strict and baseline_cfg.get("__schema__") != "canonical_v1":
        raise SystemExit(f"[verify] FATAL: --strict + baseline is legacy ({baseline})")

    fail = False
    for compare_dir in others:
        other_cfg = load_run_config(compare_dir)
        if strict and other_cfg.get("__schema__") != "canonical_v1":
            print(f"[verify] FATAL: --strict + compare is legacy ({compare_dir})",
                  file=sys.stderr)
            fail = True
            continue
        mismatches = compare_configs(baseline_cfg, other_cfg, allow_set)
        if mismatches:
            print(f"[verify] FAIL {compare_dir}", file=sys.stderr)
            print(f"  CONFIG MISMATCH on {len(mismatches)} field(s):", file=sys.stderr)
            for field, b, o in mismatches:
                print(format_diff_line(field, b, o), file=sys.stderr)
            print("  Refusing comparison.", file=sys.stderr)
            fail = True
    if fail:
        raise SystemExit(1)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--baseline", required=True, type=Path,
                    help="path to baseline run directory")
    ap.add_argument("--compare", required=True, action="append", type=Path,
                    help="path to comparison run directory (repeatable)")
    ap.add_argument("--allow", action="append", default=[],
                    help="field allowed to differ (repeatable). E.g. --allow cost_bps")
    ap.add_argument("--strict", action="store_true",
                    help="treat legacy (pre-canonical) runs as a mismatch")
    args = ap.parse_args()

    allow_set: set[str] = set(args.allow)
    unknown = allow_set - set(CORE_FIELDS)
    if unknown:
        print(f"[verify] FATAL: --allow names not in CORE_FIELDS: {sorted(unknown)}",
              file=sys.stderr)
        return 2

    try:
        baseline_cfg = load_run_config(args.baseline)
    except SystemExit as e:
        print(f"[verify] FATAL: {e}", file=sys.stderr)
        return 2

    if args.strict and baseline_cfg.get("__schema__") != "canonical_v1":
        print(f"[verify] FATAL: --strict + baseline is legacy schema "
              f"({args.baseline})", file=sys.stderr)
        return 1

    print(f"[verify] baseline: {args.baseline}")
    print(f"[verify] allow: {sorted(allow_set) if allow_set else '(none)'}")
    print()

    any_fail = False
    for compare_dir in args.compare:
        try:
            other_cfg = load_run_config(compare_dir)
        except SystemExit as e:
            print(f"[verify] FATAL: {e}", file=sys.stderr)
            any_fail = True
            continue

        if args.strict and other_cfg.get("__schema__") != "canonical_v1":
            print(f"[verify] FATAL: --strict + compare is legacy schema "
                  f"({compare_dir})", file=sys.stderr)
            any_fail = True
            continue

        mismatches = compare_configs(baseline_cfg, other_cfg, allow_set)
        if not mismatches:
            schema_note = ""
            if other_cfg.get("__schema__") == "legacy":
                schema_note = " (legacy schema — incomplete check)"
            print(f"[verify] OK   {compare_dir}{schema_note}")
        else:
            print(f"[verify] FAIL {compare_dir}")
            print(f"  CONFIG MISMATCH on {len(mismatches)} field(s):")
            for field, b, o in mismatches:
                print(format_diff_line(field, b, o))
            print()
            print(f"  Refusing comparison.")
            print(f"  If a field in this list is intentional, pass it via --allow.")
            print(f"  If you don't know, do NOT compare these runs.")
            print()
            any_fail = True

    return 1 if any_fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
