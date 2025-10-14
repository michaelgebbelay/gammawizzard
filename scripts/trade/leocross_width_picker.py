#!/usr/bin/env python3
"""Utility script for choosing a LeoCross credit spread width.

The workflow step surfaces a toggle that can calculate a preferred
width before invoking the orchestrator.  The picker is intentionally
minimal here – it primarily normalises the configured inputs and
exposes the resulting width to subsequent steps via ``GITHUB_OUTPUT``.

If downstream logic grows more sophisticated we already have the
parsing and error handling in place, and the script can be extended to
call remote services or perform richer analytics without requiring
further workflow changes.
"""

from __future__ import annotations

import json
import os
import sys
from typing import Iterable, List

DEFAULT_WIDTH_FALLBACK = 20


def _parse_candidate_widths(raw: str | None) -> List[int]:
    """Convert the comma-delimited candidate list into integers."""
    if not raw:
        return []
    out: List[int] = []
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        try:
            out.append(int(float(chunk)))
        except ValueError:
            print(f"WIDTH_PICKER WARN: ignoring non-numeric candidate '{chunk}'", file=sys.stderr)
    return out


def _parse_default_width(raw: str | None) -> int:
    raw = (raw or "").strip()
    if not raw:
        return DEFAULT_WIDTH_FALLBACK
    try:
        return int(float(raw))
    except ValueError:
        print(
            f"WIDTH_PICKER WARN: invalid DEFAULT_CREDIT_WIDTH='{raw}', using fallback {DEFAULT_WIDTH_FALLBACK}",
            file=sys.stderr,
        )
        return DEFAULT_WIDTH_FALLBACK


def _pick_width(candidates: Iterable[int], default_width: int) -> int:
    """Choose the width to use.

    We honour the explicitly configured default when possible; otherwise we
    fall back to the first available candidate or the global fallback.
    """

    candidates = list(dict.fromkeys(int(x) for x in candidates if x > 0))
    if default_width > 0 and default_width in candidates:
        return default_width
    if candidates:
        return candidates[0]
    return default_width if default_width > 0 else DEFAULT_WIDTH_FALLBACK


def _emit_output(width: int) -> None:
    """Expose the selected width to subsequent workflow steps."""

    github_output = os.environ.get("GITHUB_OUTPUT")
    if not github_output:
        raise RuntimeError("WIDTH_PICKER_MISSING_GITHUB_OUTPUT")
    with open(github_output, "a", encoding="utf-8") as fh:
        fh.write(f"picked_width={width}\n")


def main() -> int:
    candidates = _parse_candidate_widths(os.environ.get("CANDIDATE_CREDIT_WIDTHS"))
    default_width = _parse_default_width(os.environ.get("DEFAULT_CREDIT_WIDTH"))
    width = _pick_width(candidates, default_width)

    push_out_shorts = (os.environ.get("PUSH_OUT_SHORTS", "").strip().lower() == "true")
    selector_mode = (os.environ.get("SELECTOR_USE") or "MID").strip().upper() or "MID"
    selector_tol = (os.environ.get("SELECTOR_TICK_TOL") or "").strip() or "0"

    print(
        "WIDTH_PICKER SUMMARY: width=%s push_out_shorts=%s selector=%s tol=%s" %
        (width, "ON" if push_out_shorts else "OFF", selector_mode, selector_tol)
    )

    ratio_std = os.environ.get("RATIO_STD_JSON", "")
    ratio_push = os.environ.get("RATIO_PUSH_JSON", "")
    if ratio_std:
        try:
            json.loads(ratio_std)
        except json.JSONDecodeError:
            print("WIDTH_PICKER WARN: RATIO_STD_JSON is not valid JSON", file=sys.stderr)
    if ratio_push:
        try:
            json.loads(ratio_push)
        except json.JSONDecodeError:
            print("WIDTH_PICKER WARN: RATIO_PUSH_JSON is not valid JSON", file=sys.stderr)

    _emit_output(width)
    return 0


if __name__ == "__main__":  # pragma: no cover - convenience entrypoint
    sys.exit(main())
