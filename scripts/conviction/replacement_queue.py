"""
Replacement queue + theme-collapsed flyer bench.

This module is the "bench intelligence" layer. It does two things:

1. Collapse the raw flyer ranking into one readable row per theme
   (leader / runner_up / early_successor).
2. Persist a hidden replacement queue that monitor.py can reveal only after
   the current holding breaks.

The current-holding monitor stays deterministic: it checks the held name's
trend / stop state and only surfaces the queue when the holding ejects.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd

from theme_rotation import ThemeRotation, rank_themes


REPO_DIR = Path(__file__).resolve().parent
QUEUE_PATH = REPO_DIR / "cache" / "replacement_queue.json"


@dataclass
class ThemeBenchRow:
    theme: str
    rotation_label: str | None
    rotation_score: float | None
    leader: str | None
    runner_up: str | None
    early_successor: str | None


@dataclass
class ReplacementCandidate:
    replacement_rank: int
    ticker: str
    theme: str
    theme_role: str
    trend_status: str | None
    stage_of_move: str | None
    composite: float | None
    from_52w_high: float | None
    recent_60d_ret: float | None
    r_sq_126: float | None
    calmar: float | None
    reason: str
    visible: bool


def _fmt_pct(x: float | None, digits: int = 1) -> str:
    if x is None or x != x:
        return "—"
    return f"{x * 100:+.{digits}f}%"


def build_theme_bench(
    rotations: dict[str, ThemeRotation],
    *,
    exclude_ticker: str | None = None,
    top: int | None = None,
) -> list[ThemeBenchRow]:
    """Return one readable row per theme, sorted by rotation score."""
    rows: list[ThemeBenchRow] = []
    held = exclude_ticker.upper() if exclude_ticker else None
    for rot in rank_themes(rotations):
        if not rot.eligible_members:
            continue
        leader = rot.leader
        runner_up = rot.runner_up
        early_successor = rot.early_successor
        if held:
            if leader == held:
                leader = None
            if runner_up == held:
                runner_up = None
            if early_successor == held:
                early_successor = None
        if not any([leader, runner_up, early_successor]):
            continue
        rows.append(
            ThemeBenchRow(
                theme=rot.name,
                rotation_label=rot.rotation_label,
                rotation_score=rot.rotation_score,
                leader=leader,
                runner_up=runner_up,
                early_successor=early_successor,
            )
        )
    return rows[:top] if top is not None else rows


def _candidate_reason(
    ticker: str,
    theme: str,
    role: str,
    factors_by_ticker: dict,
) -> str:
    f = factors_by_ticker.get(ticker)
    if f is None:
        return f"{theme} {role}; factor data unavailable"
    status = (f.trend_status or "UNKNOWN").lower()
    near_high = _fmt_pct(f.pct_from_52w_high, 1)
    ret60 = _fmt_pct(f.recent_60d_ret, 1)
    if role == "leader":
        return f"{theme} leader; {status} trend; {near_high} from 52w high; 60d {ret60}"
    if role == "runner_up":
        return f"{theme} runner-up; {status} trend; strong backup if leader fails"
    return f"{theme} early successor; {status} trend; cleaner / less-extended bench name"


def build_replacement_queue(
    *,
    current_holding: str | None,
    current_holding_status: str | None,
    show_replacements: bool | None = None,
    rotations: dict[str, ThemeRotation],
    flyer_ranking: pd.DataFrame,
    factors_by_ticker: dict,
    top: int = 24,
) -> dict:
    """Build queue payload for persistence and optional display on EJECT."""
    held = current_holding.upper() if current_holding else None
    if show_replacements is None:
        show_replacements = current_holding_status in ("WARNING", "BROKEN")
    bench = build_theme_bench(rotations, exclude_ticker=held)

    items: list[ReplacementCandidate] = []
    seen: set[str] = set()
    role_order = ("leader", "runner_up", "early_successor")
    for role in role_order:
        for bench_row in bench:
            theme_roles = {
                "leader": bench_row.leader,
                "runner_up": bench_row.runner_up,
                "early_successor": bench_row.early_successor,
            }
            ticker = theme_roles.get(role)
            if not ticker or ticker in seen:
                continue
            f = factors_by_ticker.get(ticker)
            composite = None
            if ticker in flyer_ranking.index and flyer_ranking.loc[ticker, "composite"] == flyer_ranking.loc[ticker, "composite"]:
                composite = float(flyer_ranking.loc[ticker, "composite"])
            items.append(
                ReplacementCandidate(
                    replacement_rank=len(items) + 1,
                    ticker=ticker,
                    theme=bench_row.theme,
                    theme_role=role,
                    trend_status=f.trend_status if f else None,
                    stage_of_move=None,
                    composite=composite,
                    from_52w_high=f.pct_from_52w_high if f else None,
                    recent_60d_ret=f.recent_60d_ret if f else None,
                    r_sq_126=f.r_sq_126 if f else None,
                    calmar=f.calmar if f else None,
                    reason=_candidate_reason(ticker, bench_row.theme, role, factors_by_ticker),
                    visible=show_replacements,
                )
            )
            seen.add(ticker)
            if len(items) >= top:
                break
        if len(items) >= top:
            break

    return {
        "as_of": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "current_holding": held,
        "current_holding_status": current_holding_status,
        "show_replacements": show_replacements,
        "theme_bench": [asdict(r) for r in bench],
        "queue": [asdict(i) for i in items],
    }


def save_replacement_queue(payload: dict) -> Path:
    QUEUE_PATH.parent.mkdir(exist_ok=True)
    QUEUE_PATH.write_text(json.dumps(payload, indent=2))
    return QUEUE_PATH


def queue_items_df(payload: dict) -> pd.DataFrame:
    rows = payload.get("queue") or []
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)
