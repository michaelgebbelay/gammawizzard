#!/usr/bin/env python3
"""Focused safety checks for reduced-universe Path S selectors.

This script is intentionally non-destructive:
  - no live order placement
  - no mutation of live Path S state
  - no selector default changes

It validates four things:
  1. core_2000 still behaves as the production default
  2. non-core selector divergence emits a warning when flat
  3. held-name force-include works for tracking/exits
  4. signal fast-path invalidates when held-state changes
"""
from __future__ import annotations

import json
import tempfile
import time
from datetime import date
from pathlib import Path

import pandas as pd

from data_refresh import (
    OUT_DIR,
    UNIVERSE_SELECTOR_CORE,
    UNIVERSE_SELECTOR_PIT_126,
    _build_selector_parity,
    _load_dotenv,
    _signal_fast_path_status,
    _resolve_universe_selector,
    compute_signal,
)


OUT_BASE = OUT_DIR / "selector_safety"


def _flat_held_context() -> dict:
    return {
        "held_ticker": None,
        "held_qty": 0,
        "held_force_include_reason": None,
        "state_as_of": None,
    }


def _held_kim_context() -> dict:
    return {
        "held_ticker": "KIM",
        "held_qty": 430,
        "held_force_include_reason": "current_position",
        "state_as_of": "2026-05-04",
    }


def _action(payload: dict) -> str:
    if payload.get("regime", {}).get("open") and payload.get("n_candidates", 0) > 0:
        return "ENTER"
    return "NONE"


def _top(payload: dict) -> str:
    return payload["candidates"][0]["ticker"] if payload.get("candidates") else ""


def _emit_row(
    *,
    case_id: str,
    run_date: date,
    selector: str,
    held_ticker: str | None,
    core_payload: dict,
    selector_payload: dict,
    selector_allowed: set[str],
    selector_parity: dict | None,
    pass_ok: bool,
    failure_reason: str,
) -> dict:
    core_top = _top(core_payload)
    selector_top = _top(selector_payload)
    parity = selector_parity or {}
    held_meta = selector_payload.get("held_force_include") or {}
    return {
        "case_id": case_id,
        "date": run_date.isoformat(),
        "selector": selector,
        "held_ticker": held_ticker or "",
        "core_top_candidate": core_top,
        "selector_top_candidate": selector_top,
        "same_top_candidate": bool(core_top == selector_top),
        "action_core": _action(core_payload),
        "action_selector": _action(selector_payload),
        "same_action": bool(_action(core_payload) == _action(selector_payload)),
        "selector_parity_warning": bool(parity.get("warning")),
        "core_top_candidate_included": (
            bool(parity.get("core_top_candidate_included_in_selector"))
            if parity
            else bool(not core_top or core_top in selector_allowed)
        ),
        "core_top_candidate_excluded_reason": (
            parity.get("core_top_candidate_excluded_reason", "")
            if parity
            else ("" if (not core_top or core_top in selector_allowed) else "not_in_selector_universe")
        ),
        "held_force_included": bool(held_meta.get("held_force_included")),
        "target_universe_has_held_ticker": bool(held_meta.get("target_universe_has_held_ticker")),
        "allowed_universe_size": len(selector_allowed),
        "pass": bool(pass_ok),
        "failure_reason": failure_reason,
    }


def _write_temp_state(path: Path, ticker: str | None) -> None:
    payload = {
        "schema_version": 1,
        "as_of": "2026-05-04",
        "position": None,
    }
    if ticker:
        payload["position"] = {
            "ticker": ticker,
            "qty": 430,
        }
    path.write_text(json.dumps(payload, indent=2))


def main() -> int:
    _load_dotenv()
    out_dir = OUT_BASE / f"run_{pd.Timestamp.now():%Y%m%d_%H%M%S}"
    out_dir.mkdir(parents=True, exist_ok=True)

    rows: list[dict] = []

    flat = _flat_held_context()

    # Case 1: core default on 2026-05-01 while flat.
    core_date = date(2026, 5, 1)
    core_allowed_by_date, core_meta = _resolve_universe_selector(
        UNIVERSE_SELECTOR_CORE,
        [core_date],
        force_include_ticker=flat.get("held_ticker"),
        force_include_reason=flat.get("held_force_include_reason"),
    )
    core_payload, _ = compute_signal(
        core_date,
        allowed_tickers_by_date=core_allowed_by_date,
        universe_selector=UNIVERSE_SELECTOR_CORE,
        universe_meta=core_meta,
        held_context=flat,
        return_metrics=True,
    )
    case1_failures: list[str] = []
    if UNIVERSE_SELECTOR_CORE != "core_2000":
        case1_failures.append("core_2000 default changed")
    if _top(core_payload) != "KIM":
        case1_failures.append(f"expected core top KIM, got {_top(core_payload) or 'NONE'}")
    if _action(core_payload) != "ENTER":
        case1_failures.append(f"expected core action ENTER, got {_action(core_payload)}")
    if core_payload.get("selector_parity"):
        case1_failures.append("core payload unexpectedly contains selector_parity")
    rows.append(
        _emit_row(
            case_id="case1_core_default",
            run_date=core_date,
            selector=UNIVERSE_SELECTOR_CORE,
            held_ticker=None,
            core_payload=core_payload,
            selector_payload=core_payload,
            selector_allowed=core_allowed_by_date[core_date],
            selector_parity=None,
            pass_ok=not case1_failures,
            failure_reason="; ".join(case1_failures),
        )
    )

    # Case 2: known flat divergence on 2026-05-01 under PIT 126d.
    pit_date = date(2026, 5, 1)
    pit_allowed_by_date, pit_meta = _resolve_universe_selector(
        UNIVERSE_SELECTOR_PIT_126,
        [pit_date],
        force_include_ticker=None,
        force_include_reason=None,
    )
    pit_payload, _ = compute_signal(
        pit_date,
        allowed_tickers_by_date=pit_allowed_by_date,
        universe_selector=UNIVERSE_SELECTOR_PIT_126,
        universe_meta=pit_meta,
        held_context=flat,
        return_metrics=True,
    )
    pit_parity = _build_selector_parity(
        selector_payload=pit_payload,
        core_payload=core_payload,
        selector_allowed_tickers=pit_allowed_by_date[pit_date],
    )
    case2_failures: list[str] = []
    if not pit_parity.get("warning"):
        case2_failures.append("non-core divergence did not emit warning")
    if pit_parity.get("core_top_candidate") != "KIM":
        case2_failures.append("core top candidate should be KIM")
    if pit_parity.get("selector_top_candidate") != "ETR":
        case2_failures.append(
            f"selector top candidate should be ETR, got {pit_parity.get('selector_top_candidate') or 'NONE'}"
        )
    if pit_parity.get("same_top_candidate"):
        case2_failures.append("same_top_candidate should be false")
    if not pit_parity.get("same_action"):
        case2_failures.append("same_action should still be true")
    if pit_parity.get("core_top_candidate_excluded_reason") != "not_in_selector_universe":
        case2_failures.append("expected excluded reason not_in_selector_universe")
    rows.append(
        _emit_row(
            case_id="case2_flat_divergence",
            run_date=pit_date,
            selector=UNIVERSE_SELECTOR_PIT_126,
            held_ticker=None,
            core_payload=core_payload,
            selector_payload=pit_payload,
            selector_allowed=pit_allowed_by_date[pit_date],
            selector_parity=pit_parity,
            pass_ok=not case2_failures,
            failure_reason="; ".join(case2_failures),
        )
    )

    # Case 3: held-name force-include on 2026-05-04..2026-05-06.
    held = _held_kim_context()
    held_dates = [date(2026, 5, 4), date(2026, 5, 5), date(2026, 5, 6)]
    held_allowed_by_date, held_meta = _resolve_universe_selector(
        UNIVERSE_SELECTOR_PIT_126,
        held_dates,
        force_include_ticker=held.get("held_ticker"),
        force_include_reason=held.get("held_force_include_reason"),
    )
    for d in held_dates:
        selector_payload, _ = compute_signal(
            d,
            allowed_tickers_by_date=held_allowed_by_date,
            universe_selector=UNIVERSE_SELECTOR_PIT_126,
            universe_meta=held_meta,
            held_context=held,
            return_metrics=True,
        )
        core_day_allowed, core_day_meta = _resolve_universe_selector(UNIVERSE_SELECTOR_CORE, [d])
        core_day_payload, _ = compute_signal(
            d,
            allowed_tickers_by_date=core_day_allowed,
            universe_selector=UNIVERSE_SELECTOR_CORE,
            universe_meta=core_day_meta,
            held_context=flat,
            return_metrics=True,
        )
        parity = _build_selector_parity(
            selector_payload=selector_payload,
            core_payload=core_day_payload,
            selector_allowed_tickers=held_allowed_by_date[d],
        )
        failures: list[str] = []
        held_meta_block = selector_payload.get("held_force_include") or {}
        if held.get("held_ticker") != "KIM":
            failures.append(f"expected held ticker KIM, got {held.get('held_ticker')}")
        if not held_meta_block.get("held_force_included"):
            failures.append("held_force_included should be true")
        if held_meta_block.get("held_force_include_reason") != "current_position":
            failures.append("expected held_force_include_reason current_position")
        if not held_meta_block.get("target_universe_has_held_ticker"):
            failures.append("held ticker missing from target universe")
        if len(held_allowed_by_date[d]) != 1001:
            failures.append(f"expected allowed universe size 1001, got {len(held_allowed_by_date[d])}")
        if "KIM" not in held_allowed_by_date[d]:
            failures.append("KIM not force-included in selector universe")
        if any(str(row.get("ticker", "")).upper() == "KIM" for row in selector_payload.get("candidates", [])):
            failures.append("force-included held ticker incorrectly promoted as fresh candidate")
        rows.append(
            _emit_row(
                case_id="case3_held_force_include",
                run_date=d,
                selector=UNIVERSE_SELECTOR_PIT_126,
                held_ticker="KIM",
                core_payload=core_day_payload,
                selector_payload=selector_payload,
                selector_allowed=held_allowed_by_date[d],
                selector_parity=parity,
                pass_ok=not failures,
                failure_reason="; ".join(failures),
            )
        )

    # Case 4: cache invalidation when held state changes.
    cache_date = date(2026, 5, 4)
    with tempfile.TemporaryDirectory(prefix="paths_selector_safety_") as tmp:
        tmpdir = Path(tmp)
        signal_path = tmpdir / "signal_today.json"
        state_a = tmpdir / "state_a.json"
        state_b = tmpdir / "state_b.json"

        run_a_allowed, run_a_meta = _resolve_universe_selector(
            UNIVERSE_SELECTOR_PIT_126,
            [cache_date],
            force_include_ticker="KIM",
            force_include_reason="current_position",
        )
        run_a_payload, _ = compute_signal(
            cache_date,
            allowed_tickers_by_date=run_a_allowed,
            universe_selector=UNIVERSE_SELECTOR_PIT_126,
            universe_meta=run_a_meta,
            held_context={
                "held_ticker": "KIM",
                "held_qty": 430,
                "held_force_include_reason": "current_position",
                "state_as_of": "2026-05-04",
            },
            return_metrics=True,
        )
        signal_path.write_text(json.dumps(run_a_payload, indent=2))
        now = time.time()
        signal_path.touch()
        state_a.touch()
        state_b.touch()
        # Ensure signal is the newest artifact for run A, then state_b becomes newer.
        osig = now + 1
        statea = now
        stateb = now + 2
        import os
        os.utime(signal_path, (osig, osig))
        _write_temp_state(state_a, "KIM")
        os.utime(state_a, (statea, statea))
        _write_temp_state(state_b, None)
        os.utime(state_b, (stateb, stateb))

        fp_a = _signal_fast_path_status(
            signal_path=signal_path,
            state_path=state_a,
            selector=UNIVERSE_SELECTOR_PIT_126,
            target=cache_date,
        )
        run_b_allowed, _ = _resolve_universe_selector(
            UNIVERSE_SELECTOR_PIT_126,
            [cache_date],
            force_include_ticker=None,
            force_include_reason=None,
        )
        fp_b = _signal_fast_path_status(
            signal_path=signal_path,
            state_path=state_b,
            selector=UNIVERSE_SELECTOR_PIT_126,
            target=cache_date,
        )
        core_case4_allowed, core_case4_meta = _resolve_universe_selector(UNIVERSE_SELECTOR_CORE, [cache_date])
        core_case4_payload, _ = compute_signal(
            cache_date,
            allowed_tickers_by_date=core_case4_allowed,
            universe_selector=UNIVERSE_SELECTOR_CORE,
            universe_meta=core_case4_meta,
            held_context=flat,
            return_metrics=True,
        )

        failures_a: list[str] = []
        if len(run_a_allowed[cache_date]) != 1001:
            failures_a.append(f"run A expected universe size 1001, got {len(run_a_allowed[cache_date])}")
        if not fp_a.get("eligible"):
            failures_a.append(f"run A fast path should be eligible, got {fp_a.get('reason')}")
        rows.append(
            _emit_row(
                case_id="case4_cache_invalidation_run_a",
                run_date=cache_date,
                selector=UNIVERSE_SELECTOR_PIT_126,
                held_ticker="KIM",
                core_payload=core_case4_payload,
                selector_payload=run_a_payload,
                selector_allowed=run_a_allowed[cache_date],
                selector_parity=_build_selector_parity(
                    selector_payload=run_a_payload,
                    core_payload=core_case4_payload,
                    selector_allowed_tickers=run_a_allowed[cache_date],
                ),
                pass_ok=not failures_a,
                failure_reason="; ".join(failures_a),
            )
        )

        run_b_payload, _ = compute_signal(
            cache_date,
            allowed_tickers_by_date=run_b_allowed,
            universe_selector=UNIVERSE_SELECTOR_PIT_126,
            universe_meta={"selector": UNIVERSE_SELECTOR_PIT_126},
            held_context=flat,
            return_metrics=True,
        )
        failures_b: list[str] = []
        if len(run_b_allowed[cache_date]) != 1000:
            failures_b.append(f"run B expected universe size 1000, got {len(run_b_allowed[cache_date])}")
        if fp_b.get("eligible"):
            failures_b.append("run B fast path should be invalidated after state change")
        if fp_b.get("reason") != "input_mtime_newer":
            failures_b.append(f"run B expected input_mtime_newer, got {fp_b.get('reason')}")
        rows.append(
            _emit_row(
                case_id="case4_cache_invalidation_run_b",
                run_date=cache_date,
                selector=UNIVERSE_SELECTOR_PIT_126,
                held_ticker="",
                core_payload=core_case4_payload,
                selector_payload=run_b_payload,
                selector_allowed=run_b_allowed[cache_date],
                selector_parity=_build_selector_parity(
                    selector_payload=run_b_payload,
                    core_payload=core_case4_payload,
                    selector_allowed_tickers=run_b_allowed[cache_date],
                ),
                pass_ok=not failures_b,
                failure_reason="; ".join(failures_b),
            )
        )

    df = pd.DataFrame(rows)
    df.to_csv(out_dir / "selector_safety_check.csv", index=False)

    overall_pass = bool(df["pass"].all()) if not df.empty else False
    failed = df[~df["pass"]].copy()
    lines = [
        "# Selector Safety Check",
        "",
        f"Overall pass: {'YES' if overall_pass else 'NO'}",
        "",
        "## Results",
        "",
        "```text",
        df.to_string(index=False),
        "```",
        "",
    ]
    if not failed.empty:
        lines.extend(
            [
                "## Failures",
                "",
                "```text",
                failed[["case_id", "date", "failure_reason"]].to_string(index=False),
                "```",
                "",
            ]
        )
    (out_dir / "selector_safety_report.md").write_text("\n".join(lines) + "\n")

    print(f"[selector-safety] wrote {out_dir}")
    print(df.to_string(index=False))
    return 0 if overall_pass else 1


if __name__ == "__main__":
    raise SystemExit(main())
