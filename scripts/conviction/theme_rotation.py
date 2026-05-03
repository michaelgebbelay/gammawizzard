"""
Theme rotation engine — what's hot, who leads, and who's catching up.

Built on top of the flyer scan. Per theme (from themes.yaml):
    heat        — how many of the theme's tickers are in the top-10/25/50
                  flyer list, average composite, average 60d return, avg
                  distance from 52w high
    breadth     — fraction of the theme that is currently above 50d, above
                  200d, or in INTACT/PULLBACK trend. Broad strength = real
                  rotation; one-name strength = fragile.
    leadership  — leader (highest eligible composite), runner-up,
                  weak link
    successor   — lower-ranked but cleaner / less-extended name within
                  the same eligible theme bench
    rotation_score — z-scored composite of share/breadth/composite

The point of all this: when you hold the leader of a hot theme, you don't
rotate at the first wobble — you ride. When the theme cools, you rotate
to the next-strongest theme. When the leader breaks but the theme is
still hot, you rotate WITHIN the theme.
"""
from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

sys.path.append(os.path.dirname(__file__))

REPO_DIR = Path(__file__).resolve().parent


def _load_themes_yaml() -> dict[str, dict]:
    p = REPO_DIR / "themes.yaml"
    if not p.exists():
        return {}
    return (yaml.safe_load(p.read_text()) or {}).get("themes") or {}


def build_theme_membership(
    theme_dict: dict[str, list[str]] | None = None,
) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    """Returns (theme_to_tickers, ticker_to_themes).

    `theme_dict` is an optional override of the form {theme_name: [tickers]}.
    When provided, the yaml file is bypassed entirely — used by the backtest
    to pass *dynamic* themes built from SIC codes per as-of date instead of
    the hindsight-curated themes.yaml file. When None (default), reads from
    themes.yaml as before so the live system path is unaffected.
    """
    if theme_dict is not None:
        theme_to_tickers: dict[str, list[str]] = {
            name: [str(t).upper() for t in members]
            for name, members in theme_dict.items()
        }
    else:
        cfg = _load_themes_yaml()
        theme_to_tickers = {}
        for theme_name, theme_def in cfg.items():
            members = [str(t).upper() for t in (theme_def.get("tickers") or [])]
            theme_to_tickers[theme_name] = members

    ticker_to_themes: dict[str, list[str]] = {}
    for theme_name, members in theme_to_tickers.items():
        for t in members:
            ticker_to_themes.setdefault(t, []).append(theme_name)
    return theme_to_tickers, ticker_to_themes


def get_themes_for_ticker(
    ticker: str,
    theme_dict: dict[str, list[str]] | None = None,
) -> list[str]:
    _, ticker_to_themes = build_theme_membership(theme_dict)
    return ticker_to_themes.get(ticker.upper(), [])


@dataclass
class ThemeRotation:
    name: str
    members: list[str] = field(default_factory=list)            # all tickers in the theme
    members_with_data: list[str] = field(default_factory=list)  # subset with computed factors
    eligible_members: list[str] = field(default_factory=list)   # flyer-eligible subset, sorted by composite
    # Heat: how many of the theme's names are flying right now
    count_top10: int = 0
    count_top25: int = 0
    count_top50: int = 0
    count_in_universe: int = 0
    avg_composite: float | None = None
    avg_60d_ret: float | None = None
    avg_pct_from_52w_high: float | None = None
    # Breadth: how deep is the strength
    pct_above_50d: float | None = None
    pct_above_200d: float | None = None
    pct_intact: float | None = None
    pct_near_high: float | None = None      # within 8% of 52w high
    # Leadership
    leader: str | None = None
    runner_up: str | None = None
    weak_link: str | None = None
    early_successor: str | None = None
    # Single-number rotation score
    rotation_score: float | None = None
    rotation_label: str | None = None       # HOT / WARM / COOL / COLD


def _zscore(s: pd.Series) -> pd.Series:
    valid = s.dropna()
    if len(valid) < 2 or valid.std(ddof=0) == 0:
        return pd.Series([0.0] * len(s), index=s.index)
    return ((s - valid.mean()) / valid.std(ddof=0)).fillna(0.0)


def compute_theme_rotation(
    factors_by_ticker: dict,           # ticker -> StabilityFactors
    flyer_ranking: pd.DataFrame,        # output of stability.rank_universe (sorted desc)
    *,
    theme_dict: dict[str, list[str]] | None = None,
) -> dict[str, ThemeRotation]:
    """Returns theme_name -> ThemeRotation.

    `theme_dict` lets the backtest inject dynamically-built themes per
    as-of date. None falls back to themes.yaml.
    """
    theme_to_tickers, _ = build_theme_membership(theme_dict)

    eligible = flyer_ranking[flyer_ranking["eligible"]].sort_values("composite", ascending=False)
    top10 = set(eligible.head(10).index)
    top25 = set(eligible.head(25).index)
    top50 = set(eligible.head(50).index)

    # Per-ticker stats from StabilityFactors
    stats = {}
    for tkr, f in factors_by_ticker.items():
        composite = (
            flyer_ranking.loc[tkr, "composite"]
            if tkr in flyer_ranking.index else None
        )
        stats[tkr] = {
            "composite": composite if composite is not None and composite == composite else None,
            "60d_ret": f.recent_60d_ret,
            "pct_from_52w_high": f.pct_from_52w_high,
            "above_50d": f.above_50d_sma,
            "above_200d": (f.above_200d is not None and f.above_200d > 0),
            "trend_status": f.trend_status,
            "12m_ret": f.ret_12m,
            "r_sq": f.r_sq_126,
        }

    out: dict[str, ThemeRotation] = {}
    for theme_name, members in theme_to_tickers.items():
        members_with_data = [m for m in members if m in stats]
        eligible_members = [m for m in eligible.index if m in members_with_data]
        rot = ThemeRotation(
            name=theme_name,
            members=members,
            members_with_data=members_with_data,
            eligible_members=eligible_members,
        )
        rot.count_in_universe = len(members_with_data)

        if not members_with_data:
            out[theme_name] = rot
            continue

        rot.count_top10 = sum(1 for m in members_with_data if m in top10)
        rot.count_top25 = sum(1 for m in members_with_data if m in top25)
        rot.count_top50 = sum(1 for m in members_with_data if m in top50)

        comp_vals = [stats[m]["composite"] for m in members_with_data if stats[m]["composite"] is not None]
        if comp_vals:
            rot.avg_composite = float(np.mean(comp_vals))
        ret60 = [stats[m]["60d_ret"] for m in members_with_data if stats[m]["60d_ret"] is not None]
        if ret60:
            rot.avg_60d_ret = float(np.mean(ret60))
        d52 = [stats[m]["pct_from_52w_high"] for m in members_with_data
               if stats[m]["pct_from_52w_high"] is not None]
        if d52:
            rot.avg_pct_from_52w_high = float(np.mean(d52))

        denom = len(members_with_data)
        rot.pct_above_50d = sum(1 for m in members_with_data if stats[m]["above_50d"]) / denom
        rot.pct_above_200d = sum(1 for m in members_with_data if stats[m]["above_200d"]) / denom
        rot.pct_intact = sum(1 for m in members_with_data if stats[m]["trend_status"] == "INTACT") / denom
        rot.pct_near_high = sum(
            1 for m in members_with_data
            if stats[m]["pct_from_52w_high"] is not None and stats[m]["pct_from_52w_high"] > -0.08
        ) / denom

        # Leadership comes from the eligible flyer set only. This keeps the
        # theme bench consistent with the main scan the user actually sees.
        if eligible_members:
            rot.leader = eligible_members[0]
            if len(eligible_members) > 1:
                rot.runner_up = eligible_members[1]
        if members_with_data:
            weakest = sorted(
                members_with_data,
                key=lambda m: (
                    {"BROKEN": 0, "WARNING": 1, "PULLBACK": 2, "INTACT": 3}.get(stats[m]["trend_status"], -1),
                    stats[m]["composite"] if stats[m]["composite"] is not None else -np.inf,
                ),
            )
            rot.weak_link = weakest[0] if weakest else None

        # Early successor: a lower-ranked eligible name with a cleaner / less
        # extended entry profile than the leader/runner-up. Batch 1 keeps this
        # intentionally simple; Batch 2 can add RS acceleration / stage logic.
        candidates = [
            m for m in eligible_members
            if m not in {rot.leader, rot.runner_up}
        ]
        if candidates:
            def _successor_score(m):
                status_rank = {"INTACT": 2, "PULLBACK": 1}.get(stats[m]["trend_status"], 0)
                # Prefer names a few percent off the high over names already
                # pressing the exact high or deeply pulled back. Ideal zone:
                # roughly 3%-10% below the 52w high.
                dist = stats[m]["pct_from_52w_high"]
                if dist is None:
                    extension_score = -999.0
                else:
                    extension_score = -abs(dist - (-0.06))
                composite = stats[m]["composite"] if stats[m]["composite"] is not None else -np.inf
                return (status_rank, extension_score, composite)

            candidates.sort(key=_successor_score, reverse=True)
            rot.early_successor = candidates[0]

        out[theme_name] = rot

    # Cross-theme rotation score
    rotations = list(out.values())
    if rotations:
        share_top25 = pd.Series(
            {r.name: (r.count_top25 / max(1, r.count_in_universe)) for r in rotations}
        )
        breadth = pd.Series({r.name: (r.pct_intact or 0.0) for r in rotations})
        avg_comp = pd.Series({r.name: (r.avg_composite or 0.0) for r in rotations})
        score = (
            1.0 * _zscore(share_top25)
            + 1.0 * _zscore(breadth)
            + 1.0 * _zscore(avg_comp)
        )
        for r in rotations:
            r.rotation_score = float(score.get(r.name, 0.0))
            if r.rotation_score > 0.75:
                r.rotation_label = "HOT"
            elif r.rotation_score > 0.0:
                r.rotation_label = "WARM"
            elif r.rotation_score > -0.75:
                r.rotation_label = "COOL"
            else:
                r.rotation_label = "COLD"
    return out


def rank_themes(rotations: dict[str, ThemeRotation]) -> list[ThemeRotation]:
    """Themes sorted by rotation_score, descending. Skip themes with no data."""
    rs = [r for r in rotations.values() if r.count_in_universe > 0]
    rs.sort(key=lambda r: (r.rotation_score is None, -(r.rotation_score or 0)))
    return rs


def same_theme_replacement(
    held_ticker: str,
    rotations: dict[str, ThemeRotation],
    factors_by_ticker: dict,
    flyer_ranking: pd.DataFrame,
    *,
    require_intact: bool = True,
    theme_dict: dict[str, list[str]] | None = None,
) -> tuple[str | None, str | None]:
    """When a held ticker breaks, find the next best name in the same theme.

    Replacement must pass the SAME gates the main flyer scan uses (eligible
    in `flyer_ranking` AND INTACT/PULLBACK trend). Otherwise the monitor
    could rotate into a name the main scan would never have surfaced — e.g.
    a theme leader that flunked the liquidity floor or 12m return cap.

    Returns (replacement_ticker, theme_name). Returns (None, None) when the
    theme is COOL/COLD or when no eligible same-theme candidate exists, so
    the caller can fall back to cross-theme leaders.
    """
    held = held_ticker.upper()
    themes = get_themes_for_ticker(held, theme_dict)
    if not themes:
        return None, None

    # Eligibility from the main scan (liquidity, return-cap, trend).
    eligible_set = set()
    if flyer_ranking is not None and not flyer_ranking.empty and "eligible" in flyer_ranking.columns:
        eligible_set = set(flyer_ranking.index[flyer_ranking["eligible"].fillna(False)])

    candidate_themes = sorted(
        [rotations[t] for t in themes if t in rotations],
        key=lambda r: (r.rotation_score or -np.inf),
        reverse=True,
    )
    for theme in candidate_themes:
        if theme.rotation_label in ("COOL", "COLD"):
            continue
        candidates = [theme.leader, theme.runner_up, theme.early_successor]
        for c in candidates:
            if not c or c == held:
                continue
            # MUST pass the main flyer gates — same constraint set the rest
            # of the system uses.
            if eligible_set and c not in eligible_set:
                continue
            if require_intact:
                f = factors_by_ticker.get(c)
                if f is None or f.trend_status not in ("INTACT", "PULLBACK"):
                    continue
            return c, theme.name
    return None, None
