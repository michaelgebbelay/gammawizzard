#!/usr/bin/env python3
"""
Daily replay engine for the conviction system — Batch 1 baseline.

Runs the live deterministic logic (no LLM) day-by-day over a historical
window. For each trading day t:

    1. Truncate every ticker's bars to dates <= t          (no lookahead)
    2. Recompute stability factors for the universe
    3. Rebuild flyer ranking, theme rotation, replacement queue
    4. Run the one-position state machine:
           CASH       — looking for entry
           HOLDING X  — riding X, watching for WARNING/BROKEN
       Exit signals are generated at close of t and executed at OPEN of t+1
       to avoid same-bar lookahead. Slippage applied on both legs of any
       rotation.

Outputs (under `backtest/results/<run_name>/`):
    trade_log.csv         one row per closed trade
    daily_equity.csv      one row per trading day
    summary.json          headline metrics + benchmark comparison
    report.md             short narrative

Run:
    bash scripts/conviction/backtest/run.sh                     # 1y default
    bash scripts/conviction/backtest/run.sh --days 30           # quick test
    bash scripts/conviction/backtest/run.sh --lookahead-check NVDA 2026-03-15
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

# Allow direct import of the conviction modules.
HERE = Path(__file__).resolve().parent
CONVICTION_DIR = HERE.parent
sys.path.insert(0, str(CONVICTION_DIR))

from data import fetch_daily_bars  # noqa: E402
from replacement_queue import (  # noqa: E402
    build_replacement_queue,
    build_theme_bench,
)
from scan_stable import build_universe  # noqa: E402
from stability import compute_stability_factors, rank_universe  # noqa: E402
from theme_rotation import (  # noqa: E402
    compute_theme_rotation,
    same_theme_replacement,
)

# Optional Massive bars source — only loaded when --source massive is used.
try:
    from massive_ingest import load_parquet, to_bars_by_ticker  # type: ignore
    _MASSIVE_AVAILABLE = True
except Exception:  # pragma: no cover
    _MASSIVE_AVAILABLE = False

try:
    from dynamic_themes import build_dynamic_themes, build_static_universe_top_n  # type: ignore
    from massive_reference import load_metadata  # type: ignore
    _DYNAMIC_THEMES_AVAILABLE = True
except Exception:  # pragma: no cover
    _DYNAMIC_THEMES_AVAILABLE = False


RESULTS_DIR = HERE / "results"
RESULTS_DIR.mkdir(exist_ok=True)


# ---------------------------------------------------------------------------
# Lookahead-prevention primitive
# ---------------------------------------------------------------------------

def truncate_bars(bars_by_ticker: dict[str, pd.DataFrame], as_of: pd.Timestamp) -> dict[str, pd.DataFrame]:
    """Slice each ticker's bars to dates <= as_of. Returns a new dict; does
    not mutate the input. This is the *single* place where the as-of cutoff
    lives — every downstream factor / ranking call operates on the slice
    and has no awareness of "today" at all."""
    out = {}
    for tkr, bars in bars_by_ticker.items():
        if bars is None or bars.empty:
            continue
        sliced = bars[bars["date"] <= as_of]
        if not sliced.empty:
            out[tkr] = sliced
    return out


def _spx_log_returns_from(bars: pd.DataFrame | None) -> pd.Series | None:
    if bars is None or bars.empty:
        return None
    close = bars.sort_values("date")["close"].astype(float).reset_index(drop=True)
    return np.log(close / close.shift(1)).dropna()


# ---------------------------------------------------------------------------
# Daily reconstruction (the heart of the replay)
# ---------------------------------------------------------------------------

@dataclass
class DayState:
    """Everything we computed for a single trading day."""
    date: pd.Timestamp
    factors_by_ticker: dict
    flyer_ranking: pd.DataFrame
    rotations: dict
    queue_payload: dict
    # Optional options-derived signals, populated when a skew lookup is passed
    # to reconstruct_day. Maps ticker -> skew_z (rolling 60d z-score of 25Δ
    # skew). Used by Path S (skew-flip strategy).
    skew_z_by_ticker: dict = field(default_factory=dict)


def reconstruct_day(
    bars_by_ticker: dict[str, pd.DataFrame],
    as_of: pd.Timestamp,
    spx_bars_full: pd.DataFrame | None,
    *,
    held_ticker: str | None = None,
    metadata_df: pd.DataFrame | None = None,
    use_dynamic_themes: bool = False,
    skew_lookup: dict | None = None,
) -> DayState:
    """For one trading day, slice bars and rebuild the full state.

    When `use_dynamic_themes=True`, themes are built from SIC codes for the
    eligible factor universe at this date — no themes.yaml dependency, no
    hindsight curation. `metadata_df` (from massive_reference parquet)
    must be provided in that case.
    """
    truncated = truncate_bars(bars_by_ticker, as_of)
    spx_log = _spx_log_returns_from(
        truncate_bars({"$SPX": spx_bars_full}, as_of).get("$SPX") if spx_bars_full is not None else None
    )

    factors = {}
    for tkr, bars in truncated.items():
        if tkr.startswith("$") or tkr in {"SPY", "QQQ"}:
            continue
        f = compute_stability_factors(tkr, bars, spx_log_returns=spx_log)
        if f is not None:
            factors[tkr] = f

    if not factors:
        return DayState(
            date=as_of, factors_by_ticker={}, flyer_ranking=pd.DataFrame(),
            rotations={}, queue_payload={"queue": [], "theme_bench": []},
        )

    ranked = rank_universe(factors)

    # Dynamic themes: cluster the *eligible* (passed flyer-rank gates) tickers
    # by SIC industry. Anything outside the eligible set isn't a theme member.
    theme_dict = None
    if use_dynamic_themes:
        eligible_tickers = (
            ranked.index[ranked["eligible"].fillna(False)].tolist()
            if "eligible" in ranked.columns else list(factors.keys())
        )
        theme_dict = build_dynamic_themes(eligible_tickers, metadata_df, min_tickers_per_theme=4)

    rotations = compute_theme_rotation(factors, ranked, theme_dict=theme_dict)
    held_status = factors[held_ticker].trend_status if held_ticker and held_ticker in factors else None
    queue_payload = build_replacement_queue(
        current_holding=held_ticker,
        current_holding_status=held_status,
        rotations=rotations,
        flyer_ranking=ranked,
        factors_by_ticker=factors,
    )
    # Hydrate skew z-scores for this day. Layout:
    # skew_lookup[ticker][as_of_date] -> {"z": float, "qualifies": bool}
    # `z` is used by exits (signal-decay reads raw z); `qualifies` is used by
    # the entry picker (encodes direction-aware persistence).
    skew_z_today: dict = {}
    skew_qualifies_today: dict = {}
    if skew_lookup:
        as_of_key = as_of.normalize() if isinstance(as_of, pd.Timestamp) else pd.Timestamp(as_of).normalize()
        for tkr in factors.keys():
            per_t = skew_lookup.get(tkr)
            if per_t is None:
                continue
            entry = per_t.get(as_of_key)
            if entry is None:
                continue
            # Backward-compat: if entry is a scalar (old format), treat as raw z
            # with qualifies inferred from caller's threshold (handled in picker).
            if isinstance(entry, dict):
                skew_z_today[tkr] = entry["z"]
                skew_qualifies_today[tkr] = entry.get("qualifies", True)
            else:
                skew_z_today[tkr] = float(entry)
                skew_qualifies_today[tkr] = True

    state = DayState(
        date=as_of,
        factors_by_ticker=factors,
        flyer_ranking=ranked,
        rotations=rotations,
        queue_payload=queue_payload,
        skew_z_by_ticker=skew_z_today,
    )
    # Attach qualification flags as a parallel attribute. Picker reads it via
    # getattr — backward-compatible if not present.
    state.skew_qualifies_by_ticker = skew_qualifies_today  # type: ignore[attr-defined]
    # Stash the theme dict on the state so the replacement pickers can reuse
    # it without re-deriving — keeps same-theme rotation consistent with
    # what compute_theme_rotation just saw.
    state.theme_dict = theme_dict  # type: ignore[attr-defined]
    return state


# ---------------------------------------------------------------------------
# Skew lookup loader (for Path S)
# ---------------------------------------------------------------------------

def load_skew_lookup(
    skew_path: Path | None = None,
    *,
    z_window: int = 60,
    persistence_days: int = 1,
    abs_skew_z_min: float = 1.5,
    direction: str = "bullish",
) -> dict:
    """Load `skew_daily.parquet`, compute per-underlying rolling z-score of
    `skew_5otm`, and return a {ticker: {date: (z, qualifies_for_entry)}} dict.

    `persistence_days`: require z to satisfy the threshold for this many
    consecutive trading days before `qualifies_for_entry` is True. With
    persistence_days=1 (default), it's just "today's z passes." With 2, it's
    "today AND yesterday both passed." Filters out one-day skew noise spikes.

    Returns empty dict if parquet missing.
    """
    if skew_path is None:
        skew_path = Path(__file__).resolve().parent / "data" / "skew_daily.parquet"
    if not skew_path.exists():
        print(f"[skew-lookup] WARNING: {skew_path} missing — Path S will be empty",
              file=sys.stderr)
        return {}

    print(f"[skew-lookup] loading {skew_path}...", file=sys.stderr)
    df = pd.read_parquet(skew_path, columns=["underlying", "date", "skew_5otm"])
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df = df.sort_values(["underlying", "date"])
    grp = df.groupby("underlying")["skew_5otm"]
    # Shift by 1 so today's z-score compares against the prior z_window days
    # (excluding today itself). Without the shift, today is inside its own
    # normalization window — a look-ahead leak that contaminates threshold
    # crossings around z>1.5/2.0.
    rolling_mean = grp.transform(lambda s: s.shift(1).rolling(z_window, min_periods=20).mean())
    rolling_std = grp.transform(lambda s: s.shift(1).rolling(z_window, min_periods=20).std())
    df["skew_60d_mean"] = rolling_mean
    df["skew_60d_std"] = rolling_std
    df["skew_z"] = (df["skew_5otm"] - df["skew_60d_mean"]) / df["skew_60d_std"]
    df = df.dropna(subset=["skew_z"])

    # Direction-aware "passes threshold today"
    if direction == "bullish":
        df["passes"] = df["skew_z"] >= abs_skew_z_min
    else:  # bearish
        df["passes"] = df["skew_z"] <= -abs_skew_z_min

    # Persistence: rolling sum over last `persistence_days` rows == persistence_days
    # means every one of those days passed. Requires at least N rows of history.
    if persistence_days > 1:
        df["qualifies"] = df.groupby("underlying")["passes"].transform(
            lambda s: s.rolling(persistence_days, min_periods=persistence_days).sum()
                     == persistence_days
        ).fillna(False).astype(bool)
        n_qualifying = df["qualifies"].sum()
        n_passing_today = df["passes"].sum()
        print(f"[skew-lookup] persistence={persistence_days}d, dir={direction}: "
              f"{n_qualifying:,} qualifying days "
              f"(vs {n_passing_today:,} 1-day passes — "
              f"{(n_qualifying/max(n_passing_today,1)):.1%} survive)",
              file=sys.stderr)
    else:
        df["qualifies"] = df["passes"]

    print(f"[skew-lookup] {len(df):,} (ticker, date) skew_z values across "
          f"{df['underlying'].nunique():,} tickers", file=sys.stderr)
    # Output structure: {ticker: {date: {"z": float, "qualifies": bool}}}
    out: dict = {}
    for u, sub in df.groupby("underlying"):
        out[u] = {
            row.date: {"z": row.skew_z, "qualifies": bool(row.qualifies)}
            for row in sub.itertuples(index=False)
        }
    return out


# ---------------------------------------------------------------------------
# Price helpers
# ---------------------------------------------------------------------------

def _row_for_date(bars: pd.DataFrame, date: pd.Timestamp) -> pd.Series | None:
    if bars is None or bars.empty:
        return None
    matches = bars[bars["date"] == date]
    if matches.empty:
        return None
    return matches.iloc[0]


def get_open(bars_by_ticker: dict, ticker: str, date: pd.Timestamp) -> float | None:
    bars = bars_by_ticker.get(ticker)
    row = _row_for_date(bars, date)
    if row is None:
        return None
    return float(row["open"])


def get_close(bars_by_ticker: dict, ticker: str, date: pd.Timestamp) -> float | None:
    bars = bars_by_ticker.get(ticker)
    row = _row_for_date(bars, date)
    if row is None:
        return None
    return float(row["close"])


# ---------------------------------------------------------------------------
# State machine + execution
# ---------------------------------------------------------------------------

def _absolute_composite_leader(
    day_state: DayState, *, exclude=None,
) -> tuple[str | None, str | None, str]:
    """Path A — no themes, no rotation. Just pick the top eligible name by
    composite score. Used when --ignore-themes is set."""
    excl: set = set()
    if isinstance(exclude, str):
        excl = {exclude}
    elif isinstance(exclude, (set, frozenset, list, tuple)):
        excl = set(exclude)

    df = day_state.flyer_ranking
    if df is None or df.empty or "composite" not in df.columns:
        return None, None, "none"
    eligible = df[df.get("eligible", False).fillna(False)] if "eligible" in df.columns else df
    if eligible.empty:
        return None, None, "none"
    eligible = eligible.sort_values("composite", ascending=False)
    for tkr, _row in eligible.iterrows():
        if tkr in excl:
            continue
        return tkr, "ABSOLUTE_LEADER", "absolute"
    return None, None, "none"


def _path_p_pullback_continuation(
    day_state: DayState,
    *,
    exclude: str | set[str] | None = None,
    pullback_min: float = -0.40,   # at least 15% below 52w high
    pullback_max: float = -0.15,   # not more than 40% below
    min_ret_252d: float = 0.15,    # +15% trailing 12m
    min_atr_pct: float = 0.03,     # 3% ATR
    min_coil_ratio: float = 0.95,  # vol expanding (or at least not deeply coiling)
) -> tuple[str | None, str | None, str]:
    """Path P — pullback continuation in strong trenders.

    Pattern from winner_pattern.py v2 univariate analysis: name is above
    200d, has +15%+ trailing 12m return, currently 15-40% below 52w high
    (active pullback, not death spiral), high ATR, vol regime expanding.
    Among survivors, rank by trailing 12m return. Highest 12m return name
    in active pullback wins.
    """
    excl = set()
    if isinstance(exclude, str):
        excl = {exclude}
    elif isinstance(exclude, (set, frozenset, list, tuple)):
        excl = set(exclude)

    candidates = []
    for tkr, f in day_state.factors_by_ticker.items():
        if tkr in excl:
            continue
        if f.above_200d is None or f.above_200d <= 0:
            continue
        if f.ret_12m is None or f.ret_12m < min_ret_252d:
            continue
        if f.pct_from_52w_high is None:
            continue
        if not (pullback_min <= f.pct_from_52w_high <= pullback_max):
            continue
        if f.atr_pct is None or f.atr_pct < min_atr_pct:
            continue
        if f.coil_ratio is None or f.coil_ratio < min_coil_ratio:
            continue
        candidates.append((tkr, f.ret_12m))
    if not candidates:
        return None, None, "none"
    candidates.sort(key=lambda x: x[1], reverse=True)
    return candidates[0][0], "PATH_P_PULLBACK", "path_p"


def _path_v_vol_expansion(
    day_state: DayState,
    *,
    exclude: str | set[str] | None = None,
    min_coil_ratio: float = 1.20,   # vol regime expanding (20d > 60d * 1.2)
    min_ret_60d: float = 0.15,      # already +15% over last 60 trading days
) -> tuple[str | None, str | None, str]:
    """Path V — vol-expansion-with-strength.

    Structural rule (NOT data-mined): a high flyer should be (1) in an uptrend
    (above 50d), (2) in a vol expansion regime (coil_ratio > 1.2 — recent
    realized vol elevated vs longer baseline), and (3) already moving
    (60d return > +15%). Rank by 60d return — the name moving hardest in
    the vol-up regime wins.

    Designed as a forward-looking proxy for "options skew flipping" since
    we don't have historical options data. Vol expansion + strong price
    is the realized-vol shadow of the same regime.
    """
    excl = set()
    if isinstance(exclude, str):
        excl = {exclude}
    elif isinstance(exclude, (set, frozenset, list, tuple)):
        excl = set(exclude)

    candidates = []
    for tkr, f in day_state.factors_by_ticker.items():
        if tkr in excl:
            continue
        if f.above_50d_sma is None or not f.above_50d_sma:
            continue
        if f.coil_ratio is None or f.coil_ratio < min_coil_ratio:
            continue
        if f.recent_60d_ret is None or f.recent_60d_ret < min_ret_60d:
            continue
        candidates.append((tkr, f.recent_60d_ret))
    if not candidates:
        return None, None, "none"
    candidates.sort(key=lambda x: x[1], reverse=True)
    return candidates[0][0], "PATH_V_VOL_EXPANSION", "path_v"


# Module-level config for Path S — set by main() so the picker (called via
# pick_entry_target which has a fixed signature) can read direction +
# speculative-filter without us having to thread two more parameters
# through every call site.
PATH_S_CONFIG: dict = {
    "direction": "bullish",      # "bullish" → z > +threshold; "bearish" → z < -threshold
    "abs_skew_z_min": 1.5,       # |z| threshold
    "speculative_only": False,   # if True, apply price + vol filters instead of eligibility
    "spec_price_min": 1.0,
    "spec_price_max": 50.0,
    "spec_min_vol_60d": 0.50,    # annualized realized vol > 50%
    "spec_min_dollar_vol": 5_000_000,  # liquidity floor (looser than $25M)
    # Trend-quality floors (None = inactive). above_200d here is the *fraction*
    # above SMA-200 (e.g. 0.10 = 10% above). recent_60d_ret is the trailing
    # 60-trading-day return (e.g. 0.20 = +20%).
    "min_pct_above_200d": None,
    "min_ret_60d": None,
}


def _path_s_skew_flip(
    day_state: DayState,
    *,
    exclude: str | set[str] | None = None,
) -> tuple[str | None, str | None, str]:
    """Path S — skew-flip entry. Behavior parametrized via PATH_S_CONFIG.

    A 'high flyer' is hypothesized to be a name where the options market is
    bidding upside premium harder than usual (bullish flip) — OR where puts
    have become unusually expensive (bearish flip), which the cohort test
    showed actually predicts higher fwd returns ("fear → opportunity").

    Filters (all required):
      1. abs(skew_z_today) > threshold, sign matches `direction`
      2. close > 50d SMA       — uptrend filter
      3. ret_60d > 0           — already moving up
      4. EITHER eligible-set membership OR speculative price+vol filter

    Among survivors, rank by abs(skew_z) (most extreme flip first).
    """
    cfg = PATH_S_CONFIG
    direction = cfg["direction"]
    z_min = cfg["abs_skew_z_min"]
    spec = cfg["speculative_only"]

    excl: set = set()
    if isinstance(exclude, str):
        excl = {exclude}
    elif isinstance(exclude, (set, frozenset, list, tuple)):
        excl = set(exclude)

    if not day_state.skew_z_by_ticker:
        return None, None, "none"

    qualifies_lookup = getattr(day_state, "skew_qualifies_by_ticker", {})

    candidates = []
    for tkr, z in day_state.skew_z_by_ticker.items():
        if tkr in excl:
            continue
        if z is None:
            continue
        # Direction-aware threshold (current day must pass)
        if direction == "bullish" and z < z_min:
            continue
        if direction == "bearish" and z > -z_min:
            continue
        # Persistence: when persistence_days > 1, only enter if z has been
        # over threshold for the required consecutive trading days. Falls
        # back to True when no qualification info (1-day persistence default).
        if qualifies_lookup and not qualifies_lookup.get(tkr, True):
            continue
        f = day_state.factors_by_ticker.get(tkr)
        if f is None:
            continue
        if f.above_50d_sma is None or not f.above_50d_sma:
            continue
        if f.recent_60d_ret is None or f.recent_60d_ret < 0.0:
            continue
        # Trend-quality floors (no-ops when None)
        min_above_200 = cfg.get("min_pct_above_200d")
        if min_above_200 is not None:
            if f.above_200d is None or f.above_200d < min_above_200:
                continue
        min_ret_60 = cfg.get("min_ret_60d")
        if min_ret_60 is not None:
            if f.recent_60d_ret < min_ret_60:
                continue

        if spec:
            # Speculative filter: price band + high realized vol + liquidity floor
            if f.last_close is None or not (cfg["spec_price_min"] <= f.last_close <= cfg["spec_price_max"]):
                continue
            if f.vol_60d is None or f.vol_60d < cfg["spec_min_vol_60d"]:
                continue
            if f.dollar_vol_20d is None or f.dollar_vol_20d < cfg["spec_min_dollar_vol"]:
                continue
        else:
            # Standard: defer to flyer-rank eligibility (composite + $25M $vol)
            df = day_state.flyer_ranking
            if df is not None and "eligible" in df.columns and tkr in df.index:
                if not bool(df.loc[tkr, "eligible"]):
                    continue
        candidates.append((tkr, abs(float(z))))
    if not candidates:
        return None, None, "none"
    candidates.sort(key=lambda x: x[1], reverse=True)
    label = "PATH_S_SKEW_FLIP" if direction == "bullish" else "PATH_S_BEARISH_FLIP"
    return candidates[0][0], label, "path_s"


def _path_v2_quality_vol_expansion(
    day_state: DayState,
    *,
    exclude: str | set[str] | None = None,
    min_coil_ratio: float = 1.20,
) -> tuple[str | None, str | None, str]:
    """Path V2 — Path A's quality gate AND vol-expansion gate.

    Take Path A's eligible-and-ranked-by-composite universe (already encodes
    smoothness, Calmar, R², RS vs SPY, 50d/200d alignment). Among those names,
    additionally require `coil_ratio > 1.2` (recent vol > baseline vol) so we
    enter when the trend is *already strong* AND the vol regime is bidding the
    move up. Rank by Path A's composite (the quality score), not by raw return.
    """
    excl = set()
    if isinstance(exclude, str):
        excl = {exclude}
    elif isinstance(exclude, (set, frozenset, list, tuple)):
        excl = set(exclude)

    df = day_state.flyer_ranking
    if df is None or df.empty or "composite" not in df.columns:
        return None, None, "none"
    if "eligible" in df.columns:
        eligible = df[df["eligible"].fillna(False)]
    else:
        eligible = df
    if eligible.empty:
        return None, None, "none"
    eligible = eligible.sort_values("composite", ascending=False)

    for tkr, _row in eligible.iterrows():
        if tkr in excl:
            continue
        f = day_state.factors_by_ticker.get(tkr)
        if f is None or f.coil_ratio is None or f.coil_ratio < min_coil_ratio:
            continue
        return tkr, "PATH_V2_QUALITY_VOL", "path_v2"
    return None, None, "none"


def pick_entry_target(
    day_state: DayState, *, exclude=None, ignore_themes: bool = False,
    strategy: str = "pathA",
) -> tuple[str | None, str | None, str]:
    """Return (ticker, theme, source). Source ∈ {leader, runner_up, absolute, path_p, path_v, path_v2, none}.

    `strategy`:
        pathA  — absolute composite leader (default, requires ignore_themes=True too)
        pathP  — pullback-continuation entry (winner_pattern v2 rule)
        pathV  — vol-expansion-with-strength (no quality gate; broken)
        pathV2 — Path A composite eligibility AND coil_ratio > 1.2
    """
    if strategy == "pathP":
        return _path_p_pullback_continuation(day_state, exclude=exclude)
    if strategy == "pathV":
        return _path_v_vol_expansion(day_state, exclude=exclude)
    if strategy == "pathV2":
        return _path_v2_quality_vol_expansion(day_state, exclude=exclude)
    if strategy == "pathS":
        return _path_s_skew_flip(day_state, exclude=exclude)
    if ignore_themes:
        return _absolute_composite_leader(day_state, exclude=exclude)
    bench = build_theme_bench(day_state.rotations, exclude_ticker=exclude)
    for row in bench:
        if row.leader and row.leader != exclude:
            return row.leader, row.theme, "leader"
        if row.runner_up and row.runner_up != exclude:
            return row.runner_up, row.theme, "runner_up"
    return None, None, "none"


def pick_replacement(
    held: str,
    day_state: DayState,
    *,
    ignore_themes: bool = False,
    strategy: str = "pathA",
) -> tuple[str | None, str | None, str]:
    """Same-theme preferred (if hot); else cross-theme leader from queue.
    Returns (ticker, theme, source). source ∈ {same_theme, cross_theme, absolute, path_p, cash}.

    When ignore_themes=True, falls straight to the absolute composite leader —
    no theme-rotation logic at all.
    """
    if strategy == "pathP":
        tkr, theme, _ = _path_p_pullback_continuation(day_state, exclude=held)
        return tkr, theme, "path_p" if tkr else "cash"
    if strategy == "pathV":
        tkr, theme, _ = _path_v_vol_expansion(day_state, exclude=held)
        return tkr, theme, "path_v" if tkr else "cash"
    if strategy == "pathV2":
        tkr, theme, _ = _path_v2_quality_vol_expansion(day_state, exclude=held)
        return tkr, theme, "path_v2" if tkr else "cash"
    if strategy == "pathS":
        tkr, theme, _ = _path_s_skew_flip(day_state, exclude=held)
        return tkr, theme, "path_s" if tkr else "cash"
    if ignore_themes:
        tkr, theme, _ = _absolute_composite_leader(day_state, exclude=held)
        return tkr, theme, "absolute" if tkr else "cash"
    same_pick, same_theme = same_theme_replacement(
        held, day_state.rotations, day_state.factors_by_ticker, day_state.flyer_ranking,
        require_intact=True,
        theme_dict=getattr(day_state, "theme_dict", None),
    )
    if same_pick:
        return same_pick, same_theme, "same_theme"
    target, theme, _ = pick_entry_target(day_state, exclude=held)
    if target:
        return target, theme, "cross_theme"
    return None, None, "cash"


# ---------------------------------------------------------------------------
# Main replay
# ---------------------------------------------------------------------------

def _normalize_bars(bars: pd.DataFrame) -> pd.DataFrame:
    df = bars.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df = df.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
    return df


@dataclass
class Position:
    """One held name in a multi-position portfolio."""
    ticker: str
    shares: float
    entry_price: float        # post-slippage cost basis
    entry_date: pd.Timestamp
    peak_close: float         # high-water mark for trailing stop
    source: str
    theme: str | None
    last_known_close: float   # for valuation when today's bar is missing
    signal_decay_streak: int = 0  # consecutive days where the entry signal has decayed


def run_replay_multi(
    *,
    end_date: pd.Timestamp,
    lookback_days: int,
    source: str = "schwab",
    parquet_path: str | None = None,
    slippage_bps: float = 15.0,
    initial_capital: float = 100_000.0,
    refresh: bool = False,
    progress_every: int = 25,
    exit_rule: str = "trailing_pct",
    trailing_pct: float = 20.0,
    dynamic_themes: bool = False,
    universe_top_n: int = 500,
    ignore_themes: bool = False,
    n_positions: int = 2,
    strategy: str = "pathA",
    skew_lookup: dict | None = None,
    max_hold_days: int | None = None,
    signal_decay_z: float | None = None,
    signal_decay_days: int = 2,
    skew_direction: str = "bullish",
    regime_lookup: dict | None = None,
    regime_gate: str = "none",
    run_suffix: str | None = None,
    earnings_lookup=None,
    earnings_blackout_before: int = 0,
    earnings_blackout_after: int = 0,
    earnings_blackout_mode: str = "replace",
    displacement_enabled: bool = False,
    displacement_min_hold: int = 20,
    displacement_max_return: float = 0.0,
    displacement_z_min: float = 3.0,
    displacement_max_swaps_per_day: int = 1,
) -> dict:
    """Multi-position equal-weight portfolio replay.

    Holds up to `n_positions` simultaneously, equally weighted at entry.
    Each position has its own peak_close trailing-stop tracker. When one
    position fires its stop, it's replaced by the next-best name not
    already held — at next open, with slippage on both legs of the
    rotation.

    Equity-tracking is share-based (not return-haircut): each position's
    contribution to portfolio value = shares × today's close. Cash holds
    the unallocated portion (briefly nonzero on rotation days).

    n_positions=1 collapses to single-position behavior but is implemented
    here as a special case of the multi-position logic — call run_replay()
    instead for the original code path.
    """
    if n_positions < 1:
        raise SystemExit("n_positions must be >= 1")

    # ------ Universe + bar prefetch — same as single-position ------
    universe = build_universe()
    bars_by_ticker: dict[str, pd.DataFrame] = {}
    metadata_df: pd.DataFrame | None = None

    if source == "schwab":
        fetch_set = list(set(universe + ["$SPX", "SPY", "QQQ"]))
        fetch_results = fetch_daily_bars(fetch_set, refresh=refresh)
        for tkr, fr in fetch_results.items():
            if fr.error or fr.bars is None or fr.bars.empty:
                continue
            bars_by_ticker[tkr] = _normalize_bars(fr.bars)
    elif source == "massive":
        if not _MASSIVE_AVAILABLE:
            raise SystemExit("massive_ingest not importable.")
        from pathlib import Path as _Path
        ppath = _Path(parquet_path) if parquet_path else None
        df = load_parquet(ppath)
        broad_universe = dynamic_themes or ignore_themes
        if broad_universe:
            if not _DYNAMIC_THEMES_AVAILABLE:
                raise SystemExit("dynamic_themes / massive_reference not importable.")
            metadata_df = load_metadata()
            from massive_reference import allowed_ticker_set as _allowed
            allowed = _allowed(require_type="CS", exclude_pharma_biotech=True,
                               require_optionable=True)
            allowed.update({"SPY", "QQQ"})
            df = df[df["ticker"].isin(allowed)]
            tmp_bars = to_bars_by_ticker(df)
            for tkr, b in tmp_bars.items():
                tmp_bars[tkr] = _normalize_bars(b)
            top_universe = build_static_universe_top_n(tmp_bars, metadata_df, top_n=universe_top_n)
            keep = set(top_universe) | {"SPY", "QQQ"}
            bars_by_ticker = {t: b for t, b in tmp_bars.items() if t in keep}
        else:
            wanted = set(universe) | {"SPY", "QQQ"}
            df = df[df["ticker"].isin(wanted)]
            bars_by_ticker = to_bars_by_ticker(df)
            for tkr, b in bars_by_ticker.items():
                bars_by_ticker[tkr] = _normalize_bars(b)
        if "SPY" in bars_by_ticker and "$SPX" not in bars_by_ticker:
            bars_by_ticker["$SPX"] = bars_by_ticker["SPY"].copy()
    else:
        raise SystemExit(f"unknown source: {source}")

    spx_bars = bars_by_ticker.get("$SPX")
    if spx_bars is None or spx_bars.empty:
        raise RuntimeError("SPX bars unavailable")

    # ------ Trading day calendar ------
    spx_dates = pd.to_datetime(spx_bars["date"]).dt.normalize().sort_values().unique()
    end_date = pd.Timestamp(end_date).normalize()
    start_date = end_date - pd.Timedelta(days=lookback_days)
    trading_days = [d for d in spx_dates if start_date <= d <= end_date]
    if len(trading_days) < 2:
        raise RuntimeError(f"too few trading days: {len(trading_days)}")

    print(f"[replay-multi] window {trading_days[0].date()} → {trading_days[-1].date()} "
          f"({len(trading_days)} sessions)  n_positions={n_positions}  "
          f"exit={exit_rule}{f' trail={trailing_pct}%' if exit_rule == 'trailing_pct' else ''}",
          file=sys.stderr)

    # ------ Earnings blackout pre-fetch ------
    eb_before = max(0, int(earnings_blackout_before))
    eb_after = max(0, int(earnings_blackout_after))
    if earnings_lookup is not None and (eb_before > 0 or eb_after > 0):
        prefetch_tickers = [t for t in bars_by_ticker.keys()
                            if t not in {"$SPX", "SPY", "QQQ"}]
        earnings_lookup.prefetch(prefetch_tickers, max_workers=8)
        print(f"[replay-multi] earnings blackout: -{eb_before}d / +{eb_after}d "
              f"({len(prefetch_tickers)} tickers)", file=sys.stderr)

    # ------ State ------
    positions: list[Position] = []
    cash: float = initial_capital
    slip = slippage_bps / 10_000.0
    trail_thresh = trailing_pct / 100.0

    held_during_day: list[list[str]] = [[] for _ in trading_days]
    equity = [initial_capital]
    daily_returns = [0.0]
    trades: list[dict] = []

    # ------ Displacement diagnostic state ------
    displacement_log: list[dict] = []
    disp_days_considered = 0
    disp_candidates_seen = 0
    disp_swaps = 0
    disp_no_eligible_current = 0
    disp_no_eligible_challenger = 0

    for i, today in enumerate(trading_days):
        # ---- 1. Mark portfolio to today's close ----
        if i > 0:
            value = cash
            for p in positions:
                c_today = get_close(bars_by_ticker, p.ticker, today)
                if c_today is not None:
                    p.last_known_close = c_today
                    value += p.shares * c_today
                else:
                    value += p.shares * p.last_known_close
            equity.append(value)
            yest = equity[-2]
            daily_returns.append(value / yest - 1.0 if yest > 0 else 0.0)

        # ---- 2. Update peak_close per position ----
        for p in positions:
            c_today = get_close(bars_by_ticker, p.ticker, today)
            if c_today is not None:
                p.peak_close = max(p.peak_close, c_today)

        # ---- 3. Reconstruct day for entry signals + per-position trend status ----
        held_tickers_now = {p.ticker for p in positions}
        day_state = reconstruct_day(
            bars_by_ticker, today, spx_bars,
            held_ticker=next(iter(held_tickers_now), None),  # status check is per-position below
            metadata_df=metadata_df,
            use_dynamic_themes=dynamic_themes,
            skew_lookup=skew_lookup,
        )

        next_day = trading_days[i + 1] if i + 1 < len(trading_days) else None

        # ---- 4. Identify positions to close at next open ----
        regime_off_today = (
            regime_lookup is not None
            and regime_lookup.get(today, "RISK_ON") == "RISK_OFF"
        )
        to_close: list[tuple[Position, str]] = []

        # ---- 4-pre. Regime force-exit (top priority): close ALL on RISK_OFF ----
        if regime_off_today:
            label = f"REGIME_OFF_{regime_gate.upper()}"
            for p in positions:
                to_close.append((p, label))

        for p in positions:
            if regime_off_today:
                break  # already queued for force-close above
            c_today = get_close(bars_by_ticker, p.ticker, today)
            if c_today is None:
                c_today = p.last_known_close

            # ---- 4a. Signal-decay exit (highest priority) ----
            # If the original skew condition has decayed below threshold for
            # `signal_decay_days` consecutive days, exit. Lets winners run as
            # long as the signal is alive; cuts losers when the signal fades.
            if signal_decay_z is not None and day_state.skew_z_by_ticker:
                z_now = day_state.skew_z_by_ticker.get(p.ticker)
                if z_now is not None:
                    if skew_direction == "bullish":
                        decayed = z_now < signal_decay_z
                    else:  # bearish
                        decayed = z_now > -signal_decay_z
                    if decayed:
                        p.signal_decay_streak += 1
                    else:
                        p.signal_decay_streak = 0
                # Days with no skew_z are treated as neutral (no change).
                if p.signal_decay_streak >= signal_decay_days:
                    to_close.append((p, f"SIGNAL_DECAY_{signal_decay_days}D"))
                    continue

            # ---- 4b. Max-hold check ----
            if max_hold_days is not None and max_hold_days > 0:
                hd = (today - p.entry_date).days
                if hd >= max_hold_days:
                    to_close.append((p, f"MAX_HOLD_{max_hold_days}D"))
                    continue

            # ---- 4c. Trailing-stop / MA exit ----
            if exit_rule == "trailing_pct":
                if p.peak_close > 0 and c_today <= p.peak_close * (1.0 - trail_thresh):
                    to_close.append((p, f"TRAILING_STOP_{int(trailing_pct)}PCT"))
            else:  # ma_50d
                f = day_state.factors_by_ticker.get(p.ticker)
                if f is None or f.trend_status in ("WARNING", "BROKEN"):
                    status = f.trend_status if f else "MISSING_FACTORS"
                    to_close.append((p, status))

        # ---- 5. Execute closes at next open with slippage ----
        if next_day and to_close:
            for p, exit_reason in to_close:
                exit_open = get_open(bars_by_ticker, p.ticker, next_day)
                if exit_open is None or exit_open <= 0:
                    continue  # skip closure, retry tomorrow
                exit_price = exit_open * (1.0 - slip)
                proceeds = p.shares * exit_price
                cash += proceeds
                hold_days = (next_day - p.entry_date).days
                trades.append({
                    "entry_date": p.entry_date.strftime("%Y-%m-%d"),
                    "exit_date": next_day.strftime("%Y-%m-%d"),
                    "ticker": p.ticker,
                    "theme": p.theme or "",
                    "entry_price": round(p.entry_price, 4),
                    "exit_price": round(exit_price, 4),
                    "holding_period_days": hold_days,
                    "return_pct": round(exit_price / p.entry_price - 1.0, 5)
                                  if p.entry_price else 0.0,
                    "exit_reason": exit_reason,
                    "entry_source": p.source,
                })
                positions.remove(p)

        # ---- 6. Fill empty slots with new entries at next open ----
        if next_day and not regime_off_today:
            held_tickers = {p.ticker for p in positions}
            blackout_active = (earnings_lookup is not None
                               and (eb_before > 0 or eb_after > 0))
            slot_break = False
            while len(positions) < n_positions and cash > 1.0:
                if slot_break:
                    break
                excluded: set[str] = set(held_tickers)
                target, theme, src = pick_entry_target(
                    day_state,
                    exclude=excluded if excluded else None,
                    ignore_themes=ignore_themes,
                    strategy=strategy,
                )
                if not target or target in held_tickers:
                    break
                if blackout_active and earnings_lookup.is_blackout(
                        target, today, eb_before, eb_after):
                    if earnings_blackout_mode == "skip":
                        # Don't fill this slot OR subsequent slots today —
                        # tomorrow's signal gets a fresh look.
                        slot_break = True
                        target = None
                    else:  # "replace": walk down ranks until non-blackout
                        for _ in range(50):
                            excluded.add(target)
                            target, theme, src = pick_entry_target(
                                day_state,
                                exclude=excluded,
                                ignore_themes=ignore_themes,
                                strategy=strategy,
                            )
                            if not target or target in held_tickers:
                                target = None
                                break
                            if not earnings_lookup.is_blackout(
                                    target, today, eb_before, eb_after):
                                break
                if not target:
                    break
                open_price = get_open(bars_by_ticker, target, next_day)
                if open_price is None or open_price <= 0:
                    break
                buy_price = open_price * (1.0 + slip)
                empty_slots = n_positions - len(positions)
                slice_amount = cash / empty_slots
                if slice_amount <= 0 or buy_price <= 0:
                    break
                shares = slice_amount / buy_price
                cash -= slice_amount
                positions.append(Position(
                    ticker=target,
                    shares=shares,
                    entry_price=buy_price,
                    entry_date=next_day,
                    peak_close=open_price,
                    source=src,
                    theme=theme,
                    last_known_close=open_price,
                ))
                held_tickers.add(target)

        # ---- 7. Stale-loser displacement (multi-position) ----
        # Runs ONLY after normal exits (5) and entries (6). Triggers iff:
        #   - all slots are full (no naturally empty slot)
        #   - regime is on (RISK_OFF takes priority and was handled in 4-pre)
        #   - a fresh non-held challenger meets z >= displacement_z_min today
        #   - at least one current holding has hold_days >= min_hold AND
        #     unrealized return so far <= displacement_max_return
        # Worst-eligible (lowest cur_ret; tiebreak: longer hold_days; second
        # tiebreak: earlier entry_date) is exited at next_day's open and the
        # challenger is bought at next_day's open. Both legs pay slip.
        if (displacement_enabled and next_day is not None
                and not regime_off_today
                and len(positions) == n_positions):
            disp_days_considered += 1
            swaps_today = 0
            tried_challengers: set[str] = set()
            while swaps_today < max(1, int(displacement_max_swaps_per_day)):
                if len(positions) < n_positions:
                    break  # a previous swap somehow freed a slot — stop
                held_set = {p.ticker for p in positions}
                # Find next-best non-held qualifying challenger using the
                # same picker as normal entry. tried_challengers excludes
                # any we already evaluated this day.
                excluded = held_set | tried_challengers
                target_c, theme_c, src_c = pick_entry_target(
                    day_state,
                    exclude=excluded if excluded else None,
                    ignore_themes=ignore_themes,
                    strategy=strategy,
                )
                if not target_c or target_c in held_set:
                    disp_no_eligible_challenger += 1
                    break
                tried_challengers.add(target_c)
                challenger_z = day_state.skew_z_by_ticker.get(target_c)
                if challenger_z is None:
                    # No skew_z for this candidate — can't evaluate; skip it.
                    continue
                if float(challenger_z) < float(displacement_z_min):
                    # Picker returned its top, but it doesn't clear the
                    # displacement-specific z bar. Stop (any further pick
                    # would have a lower abs-z by construction of the picker).
                    disp_no_eligible_challenger += 1
                    break
                disp_candidates_seen += 1

                # Score current holdings for displacement eligibility.
                eligible: list[tuple[Position, float, int, float]] = []
                for p in positions:
                    if p.ticker == target_c:
                        continue
                    hold_days_now = (today - p.entry_date).days
                    if hold_days_now < int(displacement_min_hold):
                        continue
                    c_today = get_close(bars_by_ticker, p.ticker, today)
                    if c_today is None:
                        c_today = p.last_known_close
                    if c_today is None or c_today <= 0 or p.entry_price <= 0:
                        continue
                    cur_ret = c_today / p.entry_price - 1.0
                    if cur_ret > float(displacement_max_return):
                        continue
                    eligible.append((p, cur_ret, hold_days_now, c_today))

                if not eligible:
                    disp_no_eligible_current += 1
                    break

                # Worst eligible: lowest cur_ret, tiebreak older (longer
                # hold_days), second tiebreak earlier entry_date for
                # determinism across re-runs.
                eligible.sort(key=lambda x: (x[1], -x[2], x[0].entry_date))
                worst_p, worst_ret, worst_hd, worst_c_today = eligible[0]

                # Price both legs at next_day's open (same convention as
                # normal exits and entries).
                exit_open = get_open(bars_by_ticker, worst_p.ticker, next_day)
                chal_open = get_open(bars_by_ticker, target_c, next_day)
                if (exit_open is None or chal_open is None
                        or exit_open <= 0 or chal_open <= 0):
                    # Can't price the swap; skip this challenger and try the
                    # next-best non-held name.
                    continue

                # Snapshot portfolio value before the swap (today's close
                # for held names, last_known_close otherwise).
                pv_before = cash
                for p in positions:
                    c_t = get_close(bars_by_ticker, p.ticker, today) or p.last_known_close
                    pv_before += p.shares * c_t

                # Close worst at next_day's open with slippage.
                exit_price = exit_open * (1.0 - slip)
                proceeds = worst_p.shares * exit_price
                cash += proceeds
                hold_days_at_exit = (next_day - worst_p.entry_date).days
                exited_slot = positions.index(worst_p)
                exited_entry_date_str = worst_p.entry_date.strftime("%Y-%m-%d")
                exited_ticker = worst_p.ticker
                trades.append({
                    "entry_date": exited_entry_date_str,
                    "exit_date": next_day.strftime("%Y-%m-%d"),
                    "ticker": worst_p.ticker,
                    "theme": worst_p.theme or "",
                    "entry_price": round(worst_p.entry_price, 4),
                    "exit_price": round(exit_price, 4),
                    "holding_period_days": hold_days_at_exit,
                    "return_pct": (round(exit_price / worst_p.entry_price - 1.0, 5)
                                   if worst_p.entry_price else 0.0),
                    "exit_reason": "DISPLACE_STALE_LOSER",
                    "entry_source": worst_p.source,
                })
                positions.remove(worst_p)

                # Open challenger at next_day's open with slippage. Same
                # equal-weight cash-allocation rule as normal fills.
                buy_price = chal_open * (1.0 + slip)
                empty_slots = n_positions - len(positions)
                slice_amount = cash / max(1, empty_slots)
                shares = slice_amount / buy_price
                cash -= slice_amount
                positions.append(Position(
                    ticker=target_c,
                    shares=shares,
                    entry_price=buy_price,
                    entry_date=next_day,
                    peak_close=chal_open,
                    source=src_c,
                    theme=theme_c,
                    last_known_close=chal_open,
                ))

                # Snapshot portfolio value after the swap, marked to today's
                # close for the kept holding and to the challenger's open
                # for the just-bought position (no close yet on next_day at
                # this point in the loop).
                pv_after = cash
                for p in positions:
                    if p.ticker == target_c:
                        pv_after += p.shares * chal_open
                    else:
                        c_t = (get_close(bars_by_ticker, p.ticker, today)
                               or p.last_known_close)
                        pv_after += p.shares * c_t

                disp_swaps += 1
                swaps_today += 1
                challenger_rank = len(tried_challengers)
                displacement_log.append({
                    "date": today.strftime("%Y-%m-%d"),
                    "exited_ticker": exited_ticker,
                    "exited_slot": exited_slot,
                    "exited_entry_date": exited_entry_date_str,
                    "exited_hold_days": worst_hd,
                    "exited_return_so_far": round(worst_ret, 5),
                    "challenger_ticker": target_c,
                    "challenger_rank": challenger_rank,
                    "challenger_skew_z": round(float(challenger_z), 4),
                    "challenger_entry_price": round(buy_price, 4),
                    "exited_exit_price": round(exit_price, 4),
                    "reason": "DISPLACE_STALE_LOSER",
                    "portfolio_value_before": round(pv_before, 2),
                    "portfolio_value_after": round(pv_after, 2),
                })

        held_during_day[i] = [p.ticker for p in positions]

        if progress_every and (i % progress_every == 0):
            tickers_str = ",".join(p.ticker for p in positions) or "-"
            print(f"[replay-multi] {today.date()}  hold=[{tickers_str}]  "
                  f"equity=${equity[-1]:,.0f}", file=sys.stderr)

    # ---- Force-close remaining positions at end-of-window ----
    last_day = trading_days[-1]
    for p in list(positions):
        c_last = get_close(bars_by_ticker, p.ticker, last_day) or p.last_known_close
        exit_price = c_last * (1.0 - slip)
        hold_days = (last_day - p.entry_date).days
        trades.append({
            "entry_date": p.entry_date.strftime("%Y-%m-%d"),
            "exit_date": last_day.strftime("%Y-%m-%d"),
            "ticker": p.ticker,
            "theme": p.theme or "",
            "entry_price": round(p.entry_price, 4),
            "exit_price": round(exit_price, 4),
            "holding_period_days": hold_days,
            "return_pct": round(exit_price / p.entry_price - 1.0, 5)
                          if p.entry_price else 0.0,
            "exit_reason": "END_OF_WINDOW",
            "entry_source": p.source,
        })

    # ---- Outputs (same shape as run_replay) ----
    run_name = (f"{datetime.now().strftime('%Y-%m-%d')}_replay_{lookback_days}d_"
                f"{source}_n{n_positions}")
    if run_suffix:
        run_name = f"{run_name}_{run_suffix}"
    out_dir = RESULTS_DIR / run_name
    out_dir.mkdir(parents=True, exist_ok=True)

    trade_df = pd.DataFrame(trades)
    trade_df.to_csv(out_dir / "trade_log.csv", index=False)

    # Always write displacement_log.csv — even when empty — so downstream
    # tooling can rely on the file existing for every run.
    disp_log_cols = [
        "date", "exited_ticker", "exited_slot", "exited_entry_date",
        "exited_hold_days", "exited_return_so_far",
        "challenger_ticker", "challenger_rank", "challenger_skew_z",
        "challenger_entry_price", "exited_exit_price", "reason",
        "portfolio_value_before", "portfolio_value_after",
    ]
    disp_log_df = (pd.DataFrame(displacement_log, columns=disp_log_cols)
                   if displacement_log else pd.DataFrame(columns=disp_log_cols))
    disp_log_df.to_csv(out_dir / "displacement_log.csv", index=False)

    # Equity curve uses comma-joined active_ticker for display
    active_tickers_per_day = [",".join(ts) if ts else "" for ts in held_during_day[:len(equity)]]
    equity_df = pd.DataFrame({
        "date": [d.strftime("%Y-%m-%d") for d in trading_days[:len(equity)]],
        "active_tickers": active_tickers_per_day,
        "portfolio_value": equity,
        "daily_return": daily_returns,
    })
    equity_df["drawdown"] = equity_df["portfolio_value"] / equity_df["portfolio_value"].cummax() - 1.0
    equity_df.to_csv(out_dir / "daily_equity.csv", index=False)

    def _bench_curve(sym: str) -> dict:
        bars = bars_by_ticker.get(sym)
        if bars is None or bars.empty:
            return {"total_return": None, "cagr": None, "max_drawdown": None}
        sub = bars[(bars["date"] >= trading_days[0]) & (bars["date"] <= trading_days[-1])].sort_values("date")
        if sub.empty:
            return {"total_return": None, "cagr": None, "max_drawdown": None}
        first = float(sub["close"].iloc[0])
        last = float(sub["close"].iloc[-1])
        years = max(1e-9, len(trading_days) / 252.0)
        tr = last / first - 1.0
        cagr = (last / first) ** (1.0 / years) - 1.0
        cum = sub["close"] / sub["close"].cummax()
        return {"total_return": round(tr, 5), "cagr": round(cagr, 5),
                "max_drawdown": round(float(cum.min() - 1.0), 5)}

    total_return = equity[-1] / equity[0] - 1.0
    years = max(1e-9, len(trading_days) / 252.0)
    cagr = (equity[-1] / equity[0]) ** (1.0 / years) - 1.0
    rets = pd.Series(daily_returns).iloc[1:]
    sharpe = float(rets.mean() / rets.std() * np.sqrt(252)) if rets.std() > 0 else None
    pct_invested = float(np.mean([1.0 if ts else 0.0 for ts in held_during_day]))
    avg_hold = float(np.mean([t["holding_period_days"] for t in trades])) if trades else 0.0

    # Canonical config block — every field the verifier checks must live here.
    # When adding a new strategy knob, add it here AND update
    # verify_run_config_match.py CORE_FIELDS at the same time.
    canonical_config = {
        "signal": (f"pathS_{PATH_S_CONFIG.get('direction', 'bullish')}_skew_flip"
                   if strategy == "pathS" else strategy),
        "skew_z_min": (PATH_S_CONFIG.get("abs_skew_z_min")
                       if strategy == "pathS" else None),
        "skew_z_persistence_days": 1,  # baked into load_skew_lookup; captured at lookup build, not run time
        "universe_top_n": universe_top_n,
        "ignore_themes": ignore_themes,
        "dynamic_themes": dynamic_themes,
        "positions": n_positions,
        "regime_gate": regime_gate,
        "exit_rule": exit_rule,
        "trailing_pct": trailing_pct,
        "max_hold_days": max_hold_days,
        "signal_decay_z": locals().get("signal_decay_z"),
        "signal_decay_days": locals().get("signal_decay_days", 2),
        "days": lookback_days,
        "window_start": trading_days[0].strftime("%Y-%m-%d"),
        "window_end": trading_days[-1].strftime("%Y-%m-%d"),
        "cost_bps": slippage_bps,
        "initial_capital": initial_capital,
        "earnings_blackout_before": (earnings_blackout_before
                                      if (earnings_blackout_before or earnings_blackout_after)
                                      else None),
        "earnings_blackout_after": (earnings_blackout_after
                                     if (earnings_blackout_before or earnings_blackout_after)
                                     else None),
        "earnings_blackout_mode": (earnings_blackout_mode
                                    if (earnings_blackout_before or earnings_blackout_after)
                                    else None),
        "speculative_only": (PATH_S_CONFIG.get("speculative_only", False)
                             if strategy == "pathS" else False),
        "trend_floor_pct_above_200d": (PATH_S_CONFIG.get("min_pct_above_200d")
                                        if strategy == "pathS" else None),
        "trend_floor_min_ret_60d": (PATH_S_CONFIG.get("min_ret_60d")
                                     if strategy == "pathS" else None),
        "displacement_enabled": displacement_enabled,
        "displacement_min_hold": displacement_min_hold if displacement_enabled else None,
        "displacement_max_return": displacement_max_return if displacement_enabled else None,
        "displacement_z_min": displacement_z_min if displacement_enabled else None,
        "displacement_max_swaps_per_day": (displacement_max_swaps_per_day
                                            if displacement_enabled else None),
    }

    summary = {
        "run_name": run_name,
        "n_positions": n_positions,
        "window": {
            "start": trading_days[0].strftime("%Y-%m-%d"),
            "end": trading_days[-1].strftime("%Y-%m-%d"),
            "n_sessions": len(trading_days),
        },
        "config": canonical_config,
        "params": {
            "slippage_bps": slippage_bps,
            "initial_capital": initial_capital,
            "exit_rule": exit_rule,
            "trailing_pct": trailing_pct,
            "strategy": strategy,
            "max_hold_days": max_hold_days,
            "regime_gate": regime_gate,
            "path_s_config": dict(PATH_S_CONFIG) if strategy == "pathS" else None,
        },
        "performance": {
            "final_equity": round(float(equity[-1]), 2),
            "total_return": round(float(total_return), 5),
            "cagr": round(float(cagr), 5),
            "max_drawdown": round(float(equity_df["drawdown"].min()), 5),
            "sharpe": round(sharpe, 3) if sharpe is not None else None,
        },
        "activity": {
            "n_trades": len(trades),
            "avg_holding_days": round(avg_hold, 1),
            "pct_time_invested": round(pct_invested, 4),
            "displacement_candidates_seen": disp_candidates_seen,
            "displacement_days_considered": disp_days_considered,
            "displacement_swaps": disp_swaps,
            "displacement_no_eligible_current": disp_no_eligible_current,
            "displacement_no_eligible_challenger": disp_no_eligible_challenger,
            "displacement_avg_exited_return_so_far": (
                round(float(np.mean([r["exited_return_so_far"]
                                     for r in displacement_log])), 5)
                if displacement_log else None
            ),
            "displacement_avg_exited_hold_days": (
                round(float(np.mean([r["exited_hold_days"]
                                     for r in displacement_log])), 1)
                if displacement_log else None
            ),
        },
        "benchmarks": {"SPY": _bench_curve("SPY"), "QQQ": _bench_curve("QQQ")},
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2, default=str))

    perf = summary["performance"]
    bench = summary["benchmarks"]
    lines = [
        f"# Multi-position backtest report — {summary['run_name']}\n",
        f"Window: {summary['window']['start']} → {summary['window']['end']}  "
        f"({summary['window']['n_sessions']} sessions)",
        f"n_positions={n_positions}  exit={exit_rule}"
        + (f" trail={trailing_pct}%" if exit_rule == "trailing_pct" else ""),
        f"Slippage: {slippage_bps:.0f} bps each side; capital ${initial_capital:,.0f}",
        "",
        "## Headline",
        "",
        f"- final equity:    ${perf['final_equity']:,.2f}",
        f"- total return:    {perf['total_return']*100:+.2f}%",
        f"- CAGR:            {perf['cagr']*100:+.2f}%",
        f"- max drawdown:    {perf['max_drawdown']*100:+.2f}%",
        f"- Sharpe (252-d):  {perf['sharpe'] if perf['sharpe'] is not None else '—'}",
        "",
        "## Activity",
        "",
        f"- trades:                       {summary['activity']['n_trades']}",
        f"- avg holding days:             {summary['activity']['avg_holding_days']}",
        f"- % time invested:              {summary['activity']['pct_time_invested']*100:.1f}%",
        "",
        "## Benchmarks (same window)",
        "",
        f"- SPY total return: {bench['SPY']['total_return']*100:+.2f}%  "
        f"CAGR {bench['SPY']['cagr']*100:+.2f}%  MDD {bench['SPY']['max_drawdown']*100:+.2f}%"
        if bench["SPY"]["total_return"] is not None else "- SPY: data unavailable",
        f"- QQQ total return: {bench['QQQ']['total_return']*100:+.2f}%  "
        f"CAGR {bench['QQQ']['cagr']*100:+.2f}%  MDD {bench['QQQ']['max_drawdown']*100:+.2f}%"
        if bench["QQQ"]["total_return"] is not None else "- QQQ: data unavailable",
        "",
    ]
    if not trade_df.empty:
        winners = trade_df[trade_df["return_pct"] > 0]
        losers = trade_df[trade_df["return_pct"] <= 0]
        lines += [
            "## Trade summary",
            "",
            f"- winners: {len(winners)}  (avg return {winners['return_pct'].mean()*100:+.2f}%)"
            if not winners.empty else "- winners: 0",
            f"- losers:  {len(losers)}  (avg return {losers['return_pct'].mean()*100:+.2f}%)"
            if not losers.empty else "- losers:  0",
            "",
            "### Top 10 winners",
            "",
        ]
        top = trade_df.sort_values("return_pct", ascending=False).head(10)
        for _, r in top.iterrows():
            lines.append(
                f"- {r['ticker']:<6} {r.get('theme',''):<25} {r['return_pct']*100:+6.1f}%  "
                f"({r['entry_date']} → {r['exit_date']}, {r['holding_period_days']}d, "
                f"exit {r['exit_reason']})"
            )
    body = "\n".join(lines) + "\n"
    (out_dir / "report.md").write_text(body)
    print(body)
    print(f"\n[replay-multi] wrote {out_dir}", file=sys.stderr)
    return {"summary": summary, "out_dir": out_dir}


def run_replay(
    *,
    end_date: pd.Timestamp,
    lookback_days: int,
    source: str = "schwab",
    parquet_path: str | None = None,
    slippage_bps: float = 15.0,
    initial_capital: float = 100_000.0,
    refresh: bool = False,
    progress_every: int = 25,
    exit_rule: str = "ma_50d",
    trailing_pct: float = 25.0,
    dynamic_themes: bool = False,
    universe_top_n: int = 500,
    ignore_themes: bool = False,
    strategy: str = "pathA",
    skew_lookup: dict | None = None,
    max_hold_days: int | None = None,
    signal_decay_z: float | None = None,
    signal_decay_days: int = 2,
    skew_direction: str = "bullish",
    regime_lookup: dict | None = None,
    regime_gate: str = "none",
    run_suffix: str | None = None,
    displacement_enabled: bool = False,
    displacement_min_hold: int = 20,
    displacement_max_return: float = 0.0,
    displacement_z_min: float = 3.0,
) -> dict:
    """End-to-end backtest. Returns a dict of artifacts (also written to disk).

    source ∈ {'schwab', 'massive'}.
        schwab  — pull bars via fetch_daily_bars (~13mo of cached history)
        massive — load bars from local parquet built by massive_ingest.py
                  (5y of history including delisted names)
    Universe = themes.yaml in both cases — that's the conviction system's
    framework, and the discovery test confirmed the curation is doing real
    work. The replay tests the FULL composite over that universe.
    """
    universe = build_universe()
    bars_by_ticker: dict[str, pd.DataFrame] = {}
    metadata_df: pd.DataFrame | None = None

    if source == "schwab":
        fetch_set = list(set(universe + ["$SPX", "SPY", "QQQ"]))
        print(f"[replay] source=schwab  universe={len(universe)} (+SPX, SPY, QQQ); fetching bars...", file=sys.stderr)
        fetch_results = fetch_daily_bars(fetch_set, refresh=refresh)
        for tkr, fr in fetch_results.items():
            if fr.error or fr.bars is None or fr.bars.empty:
                continue
            bars_by_ticker[tkr] = _normalize_bars(fr.bars)
    elif source == "massive":
        if not _MASSIVE_AVAILABLE:
            raise SystemExit("massive_ingest not importable. Build the parquet first via "
                             "run_massive_ingest.sh.")
        from pathlib import Path as _Path
        ppath = _Path(parquet_path) if parquet_path else None
        df = load_parquet(ppath)

        broad_universe = dynamic_themes or ignore_themes
        if broad_universe:
            # Universe = top N by median dollar volume from the full eligible
            # set (CS, non-pharma/biotech, optionable, major exchange).
            # When dynamic_themes: themes are built per-day from SIC codes.
            # When ignore_themes: no themes at all — absolute composite leader.
            if not _DYNAMIC_THEMES_AVAILABLE:
                raise SystemExit("dynamic_themes / massive_reference not importable. "
                                 "Run run_massive_reference.sh first.")
            metadata_df = load_metadata()
            # First load all eligible tickers' bars to compute dollar volume
            from massive_reference import allowed_ticker_set as _allowed
            allowed = _allowed(require_type="CS", exclude_pharma_biotech=True,
                               require_optionable=True)
            allowed.update({"SPY", "QQQ"})
            df = df[df["ticker"].isin(allowed)]
            tmp_bars = to_bars_by_ticker(df)
            for tkr, b in tmp_bars.items():
                tmp_bars[tkr] = _normalize_bars(b)
            top_universe = build_static_universe_top_n(
                tmp_bars, metadata_df, top_n=universe_top_n,
            )
            # Restrict bars dict to top-N + benchmarks
            keep = set(top_universe) | {"SPY", "QQQ"}
            bars_by_ticker = {t: b for t, b in tmp_bars.items() if t in keep}
            print(f"[replay] dynamic themes ON  universe={len(top_universe)} "
                  f"(top {universe_top_n} by median $vol from filtered metadata)",
                  file=sys.stderr)
        else:
            # Static themes.yaml universe
            wanted = set(universe) | {"SPY", "QQQ"}
            df = df[df["ticker"].isin(wanted)]
            bars_by_ticker = to_bars_by_ticker(df)
            for tkr, b in bars_by_ticker.items():
                bars_by_ticker[tkr] = _normalize_bars(b)
            metadata_df = None
            print(f"[replay] source=massive  universe={len(universe)} themes.yaml names; "
                  f"{len(bars_by_ticker)} have bars in parquet", file=sys.stderr)

        # Alias SPY → $SPX so the existing RS factor code keeps working
        if "SPY" in bars_by_ticker and "$SPX" not in bars_by_ticker:
            bars_by_ticker["$SPX"] = bars_by_ticker["SPY"].copy()
    else:
        raise SystemExit(f"unknown source: {source}")

    spx_bars = bars_by_ticker.get("$SPX")
    if spx_bars is None or spx_bars.empty:
        raise RuntimeError("SPX bars unavailable — cannot compute RS factors")

    # ------ Trading day calendar ------
    spx_dates = pd.to_datetime(spx_bars["date"]).dt.normalize().sort_values().unique()
    end_date = pd.Timestamp(end_date).normalize()
    start_date = end_date - pd.Timedelta(days=lookback_days)
    trading_days = [d for d in spx_dates if start_date <= d <= end_date]
    if len(trading_days) < 2:
        raise RuntimeError(f"too few trading days in window: {len(trading_days)}")

    print(f"[replay] window: {trading_days[0].date()} -> {trading_days[-1].date()} "
          f"({len(trading_days)} sessions)", file=sys.stderr)

    # ------ State machine + outputs ------
    state = "CASH"
    holding: str | None = None
    entry_price: float | None = None
    entry_date: pd.Timestamp | None = None
    entry_source: str = ""
    entry_theme: str | None = None
    peak_close: float | None = None  # highest close seen since entry — for trailing-pct rule
    signal_decay_streak: int = 0  # consecutive days where the entry signal has decayed

    held_during_day: list[str | None] = [None] * len(trading_days)
    equity = [initial_capital]
    daily_returns: list[float] = [0.0]  # day 0
    trades: list[dict] = []

    slip = slippage_bps / 10_000.0
    trailing_drawdown_threshold = trailing_pct / 100.0
    if exit_rule not in ("ma_50d", "trailing_pct"):
        raise SystemExit(f"unknown exit_rule: {exit_rule}")
    print(f"[replay] exit_rule={exit_rule}"
          + (f"  trailing_pct={trailing_pct}%" if exit_rule == "trailing_pct" else ""),
          file=sys.stderr)

    for i, today in enumerate(trading_days):
        # 1. Compute equity for `today` based on what was held DURING today.
        #    held_during_day[i] reflects the carry from last close to today's
        #    close (or rotation cost if there was a rotation at today's open).
        if i > 0:
            held_today = held_during_day[i]
            yest = trading_days[i - 1]
            if held_today is None:
                day_ret = 0.0
            else:
                close_today = get_close(bars_by_ticker, held_today, today)
                close_yest = get_close(bars_by_ticker, held_today, yest)
                if close_today is None or close_yest is None or close_yest <= 0:
                    day_ret = 0.0
                else:
                    day_ret = close_today / close_yest - 1.0
                # If we rotated AT today's open (i.e. held_during_day[i-1]
                # differs from held_today), we sold the old at open with
                # slippage and bought the new at open with slippage. Apply
                # the rotation cost on top of the new name's intraday move.
                if held_during_day[i - 1] != held_today:
                    # Effective return for today =
                    #   (sell_old at open vs old close yesterday) * (1-slip)
                    #   replaced by (buy_new at open) * (close/open) * (1-slip)
                    # We capture the slippage cost as a return haircut on top
                    # of the intraday move.
                    intraday_open = get_open(bars_by_ticker, held_today, today)
                    if intraday_open and close_today:
                        intraday = close_today / intraday_open - 1.0
                    else:
                        intraday = day_ret
                    if held_during_day[i - 1] is not None:
                        old_close_yest = get_close(bars_by_ticker, held_during_day[i - 1], yest)
                        old_open_today = get_open(bars_by_ticker, held_during_day[i - 1], today)
                        if old_close_yest and old_open_today:
                            sell_gap = old_open_today / old_close_yest - 1.0
                        else:
                            sell_gap = 0.0
                    else:
                        sell_gap = 0.0
                    # Round-trip slippage: sell side then buy side
                    day_ret = (1.0 + sell_gap) * (1.0 - slip) * (1.0 + intraday) * (1.0 - slip) - 1.0
            equity.append(equity[-1] * (1.0 + day_ret))
            daily_returns.append(day_ret)

        # 2. Generate signal at close of `today` (deciding tomorrow's holding).
        next_day = trading_days[i + 1] if i + 1 < len(trading_days) else None
        day_state = reconstruct_day(
            bars_by_ticker, today, spx_bars, held_ticker=holding,
            metadata_df=metadata_df,
            use_dynamic_themes=dynamic_themes,
            skew_lookup=skew_lookup,
        )

        if state == "CASH":
            # Regime gate: if RISK_OFF today, no new entries.
            today_norm_for_gate = (today.normalize() if isinstance(today, pd.Timestamp)
                                    else pd.Timestamp(today).normalize())
            regime_blocks_entry = (
                regime_lookup is not None
                and regime_lookup.get(today_norm_for_gate, "RISK_ON") == "RISK_OFF"
            )
            if next_day is not None and not regime_blocks_entry:
                target, theme, src = pick_entry_target(day_state, ignore_themes=ignore_themes, strategy=strategy)
                if target:
                    open_px = get_open(bars_by_ticker, target, next_day)
                    if open_px is not None:
                        # Schedule the buy: held_during_day[i+1] = target
                        if i + 1 < len(held_during_day):
                            held_during_day[i + 1] = target
                        entry_price = open_px * (1.0 + slip)
                        entry_date = next_day
                        entry_source = "initial" if not trades else src
                        entry_theme = theme
                        holding = target
                        peak_close = open_px  # initialize trailing-stop tracker
                        signal_decay_streak = 0
                        state = "HOLDING"
        elif state == "HOLDING":
            assert holding is not None
            f = day_state.factors_by_ticker.get(holding)
            today_close = get_close(bars_by_ticker, holding, today)
            # Update peak_close so the trailing stop sees the high-water mark.
            if today_close is not None:
                peak_close = today_close if peak_close is None else max(peak_close, today_close)

            # Regime-gate force-exit (top priority): if today's regime turned
            # RISK_OFF, schedule exit at next open regardless of trail/max-hold.
            should_eject = False
            exit_reason_label = None
            if regime_lookup:
                today_norm = today.normalize() if isinstance(today, pd.Timestamp) else pd.Timestamp(today).normalize()
                regime_state_today = regime_lookup.get(today_norm, "RISK_ON")
                if regime_state_today == "RISK_OFF":
                    should_eject = True
                    exit_reason_label = f"REGIME_OFF_{regime_gate.upper()}"

            # Signal-decay exit: if skew_z has been below the decay threshold
            # (above for bearish) for N consecutive days, exit. Days with no
            # skew_z are neutral (no change to streak).
            if not should_eject and signal_decay_z is not None and day_state.skew_z_by_ticker:
                z_now = day_state.skew_z_by_ticker.get(holding)
                if z_now is not None:
                    if skew_direction == "bullish":
                        decayed = z_now < signal_decay_z
                    else:
                        decayed = z_now > -signal_decay_z
                    if decayed:
                        signal_decay_streak += 1
                    else:
                        signal_decay_streak = 0
                if signal_decay_streak >= signal_decay_days:
                    should_eject = True
                    exit_reason_label = f"SIGNAL_DECAY_{signal_decay_days}D"

            if not should_eject:
                # Hard max-hold ceiling fires before the trailing/MA logic.
                if (max_hold_days is not None and max_hold_days > 0
                        and entry_date is not None
                        and (today - entry_date).days >= max_hold_days):
                    should_eject = True
                    exit_reason_label = f"MAX_HOLD_{max_hold_days}D"
                elif exit_rule == "trailing_pct":
                    # Eject when close drops > trailing_pct% from peak. Don't use
                    # trend_status at all — pure price-based stop.
                    if today_close is None or peak_close is None or peak_close <= 0:
                        should_eject = False
                    else:
                        drawdown = today_close / peak_close - 1.0
                        should_eject = drawdown <= -trailing_drawdown_threshold
                    exit_reason_label = (
                        f"TRAILING_STOP_{int(trailing_pct)}PCT"
                        if should_eject else None
                    )
                else:  # ma_50d (default)
                    should_eject = (f is None) or (f.trend_status in ("WARNING", "BROKEN"))
                    exit_reason_label = None  # use trend_status below

            # Stale-loser displacement: if not already ejecting, check whether
            # the current position is stale (held >= N days) AND under-water
            # (return so far <= R) AND a fresh challenger with z >= Z exists.
            # If so, force an eject — pick_replacement will rotate into the
            # highest-z non-held candidate (which IS our challenger for pathS).
            if (not should_eject and displacement_enabled
                    and entry_date is not None and entry_price is not None
                    and today_close is not None and today_close > 0):
                hold_days_now = (today - entry_date).days
                if hold_days_now >= displacement_min_hold:
                    cur_ret_so_far = today_close / entry_price - 1.0
                    if cur_ret_so_far <= displacement_max_return:
                        # Look for any non-held challenger meeting z threshold today.
                        has_challenger = False
                        if day_state.skew_z_by_ticker:
                            for tkr_c, z_c in day_state.skew_z_by_ticker.items():
                                if tkr_c == holding or z_c is None:
                                    continue
                                if float(z_c) >= displacement_z_min:
                                    has_challenger = True
                                    break
                        if has_challenger:
                            should_eject = True
                            exit_reason_label = "DISPLACE_STALE_LOSER"
            # If we're going to keep holding, schedule it for tomorrow.
            if not should_eject and next_day is not None and i + 1 < len(held_during_day):
                held_during_day[i + 1] = holding
            elif should_eject and next_day is not None:
                # Close trade at next day's open with slippage
                exit_open = get_open(bars_by_ticker, holding, next_day)
                if exit_open is None:
                    # Stuck — keep holding; mark next day as same
                    if i + 1 < len(held_during_day):
                        held_during_day[i + 1] = holding
                else:
                    exit_price = exit_open * (1.0 - slip)
                    if exit_reason_label:
                        exit_reason = exit_reason_label
                    else:
                        exit_reason = (f.trend_status if f else "MISSING_FACTORS") or "UNKNOWN"
                    holding_period = (next_day - entry_date).days if entry_date else 0
                    trades.append({
                        "entry_date": entry_date.strftime("%Y-%m-%d"),
                        "exit_date": next_day.strftime("%Y-%m-%d"),
                        "ticker": holding,
                        "theme": entry_theme or "",
                        "entry_price": round(entry_price, 4),
                        "exit_price": round(exit_price, 4),
                        "holding_period_days": holding_period,
                        "return_pct": round(exit_price / entry_price - 1.0, 5) if entry_price else 0.0,
                        "exit_reason": exit_reason,
                        "entry_source": entry_source,
                    })
                    # Regime gate: if RISK_OFF, do not look for a replacement —
                    # go to cash and wait for regime to flip back.
                    today_norm_for_repl = (today.normalize() if isinstance(today, pd.Timestamp)
                                            else pd.Timestamp(today).normalize())
                    regime_blocks_replacement = (
                        regime_lookup is not None
                        and regime_lookup.get(today_norm_for_repl, "RISK_ON") == "RISK_OFF"
                    )
                    if regime_blocks_replacement:
                        if i + 1 < len(held_during_day):
                            held_during_day[i + 1] = None
                        holding = None
                        peak_close = None
                        signal_decay_streak = 0
                        state = "CASH"
                        repl_tkr = None
                        repl_theme = None
                        repl_src = None
                    else:
                        # Find replacement for next day
                        repl_tkr, repl_theme, repl_src = pick_replacement(holding, day_state, ignore_themes=ignore_themes, strategy=strategy)
                    if repl_tkr:
                        repl_open = get_open(bars_by_ticker, repl_tkr, next_day)
                        if repl_open is None:
                            # No price for replacement — go to cash for now
                            if i + 1 < len(held_during_day):
                                held_during_day[i + 1] = None
                            holding = None
                            peak_close = None
                            state = "CASH"
                        else:
                            if i + 1 < len(held_during_day):
                                held_during_day[i + 1] = repl_tkr
                            entry_price = repl_open * (1.0 + slip)
                            entry_date = next_day
                            entry_source = repl_src
                            entry_theme = repl_theme
                            holding = repl_tkr
                            peak_close = repl_open  # reset trailing-stop tracker
                            signal_decay_streak = 0
                            state = "HOLDING"
                    else:
                        if i + 1 < len(held_during_day):
                            held_during_day[i + 1] = None
                        holding = None
                        peak_close = None
                        state = "CASH"

        if progress_every and (i % progress_every == 0):
            print(f"[replay] {today.date()}  state={state}  hold={holding or '-'}  "
                  f"equity=${equity[-1]:,.0f}", file=sys.stderr)

    # ------ Force-close any position still open at end-of-window ------
    # Without this, the trade log loses the final trade entirely (P1 bug).
    # The activity stats (n_trades, avg holding days, % time invested) and
    # any winners/losers analysis would all be biased by the missing trade.
    if state == "HOLDING" and holding is not None and entry_price is not None:
        last_day = trading_days[-1]
        last_close = get_close(bars_by_ticker, holding, last_day)
        if last_close is not None:
            exit_price = last_close * (1.0 - slip)
            holding_period = (last_day - entry_date).days if entry_date else 0
            trades.append({
                "entry_date": entry_date.strftime("%Y-%m-%d") if entry_date else "",
                "exit_date": last_day.strftime("%Y-%m-%d"),
                "ticker": holding,
                "theme": entry_theme or "",
                "entry_price": round(entry_price, 4),
                "exit_price": round(exit_price, 4),
                "holding_period_days": holding_period,
                "return_pct": round(exit_price / entry_price - 1.0, 5) if entry_price else 0.0,
                "exit_reason": "END_OF_WINDOW",
                "entry_source": entry_source,
            })

    # ------ Outputs ------
    run_name = f"{datetime.now().strftime('%Y-%m-%d')}_replay_{lookback_days}d_{source}"
    if run_suffix:
        run_name = f"{run_name}_{run_suffix}"
    out_dir = RESULTS_DIR / run_name
    out_dir.mkdir(parents=True, exist_ok=True)

    trade_df = pd.DataFrame(trades)
    trade_df.to_csv(out_dir / "trade_log.csv", index=False)

    equity_df = pd.DataFrame({
        "date": [d.strftime("%Y-%m-%d") for d in trading_days[:len(equity)]],
        "active_ticker": held_during_day[:len(equity)],
        "portfolio_value": equity,
        "daily_return": daily_returns,
    })
    equity_df["drawdown"] = equity_df["portfolio_value"] / equity_df["portfolio_value"].cummax() - 1.0
    equity_df.to_csv(out_dir / "daily_equity.csv", index=False)

    # Benchmarks
    def _bench_curve(sym: str) -> dict:
        bars = bars_by_ticker.get(sym)
        if bars is None or bars.empty:
            return {"total_return": None, "cagr": None, "max_drawdown": None}
        sub = bars[(bars["date"] >= trading_days[0]) & (bars["date"] <= trading_days[-1])]
        sub = sub.sort_values("date")
        if sub.empty:
            return {"total_return": None, "cagr": None, "max_drawdown": None}
        first = float(sub["close"].iloc[0])
        last = float(sub["close"].iloc[-1])
        n_days = len(trading_days)
        years = max(1e-9, n_days / 252.0)
        tr = last / first - 1.0
        cagr = (last / first) ** (1.0 / years) - 1.0
        cum = sub["close"] / sub["close"].cummax()
        mdd = float(cum.min() - 1.0)
        return {"total_return": round(tr, 5), "cagr": round(cagr, 5), "max_drawdown": round(mdd, 5)}

    total_return = equity[-1] / equity[0] - 1.0
    n_days = len(trading_days)
    years = max(1e-9, n_days / 252.0)
    cagr = (equity[-1] / equity[0]) ** (1.0 / years) - 1.0
    rets = pd.Series(daily_returns).iloc[1:]  # skip seed zero
    sharpe = float(rets.mean() / rets.std() * np.sqrt(252)) if rets.std() > 0 else None
    pct_invested = float(np.mean([1.0 if h else 0.0 for h in held_during_day]))
    same_theme_rotations = sum(1 for t in trades if t.get("entry_source") == "same_theme")
    cross_theme_rotations = sum(1 for t in trades if t.get("entry_source") == "cross_theme")
    displacement_count = sum(
        1 for t in trades if str(t.get("exit_reason", "")).startswith("DISPLACE_")
    )
    avg_hold = float(np.mean([t["holding_period_days"] for t in trades])) if trades else 0.0

    # Canonical config block — same schema as the single-position branch above.
    canonical_config = {
        "signal": (f"pathS_{PATH_S_CONFIG.get('direction', 'bullish')}_skew_flip"
                   if strategy == "pathS" else strategy),
        "skew_z_min": (PATH_S_CONFIG.get("abs_skew_z_min")
                       if strategy == "pathS" else None),
        "skew_z_persistence_days": 1,
        "universe_top_n": universe_top_n,
        "ignore_themes": ignore_themes,
        "dynamic_themes": dynamic_themes,
        "positions": 1,
        "regime_gate": regime_gate,
        "exit_rule": exit_rule,
        "trailing_pct": trailing_pct,
        "max_hold_days": max_hold_days,
        "signal_decay_z": locals().get("signal_decay_z"),
        "signal_decay_days": locals().get("signal_decay_days", 2),
        "days": lookback_days,
        "window_start": trading_days[0].strftime("%Y-%m-%d"),
        "window_end": trading_days[-1].strftime("%Y-%m-%d"),
        "cost_bps": slippage_bps,
        "initial_capital": initial_capital,
        "earnings_blackout_before": None,
        "earnings_blackout_after": None,
        "earnings_blackout_mode": None,
        "speculative_only": (PATH_S_CONFIG.get("speculative_only", False)
                             if strategy == "pathS" else False),
        "trend_floor_pct_above_200d": (PATH_S_CONFIG.get("min_pct_above_200d")
                                        if strategy == "pathS" else None),
        "trend_floor_min_ret_60d": (PATH_S_CONFIG.get("min_ret_60d")
                                     if strategy == "pathS" else None),
        "displacement_enabled": displacement_enabled,
        "displacement_min_hold": displacement_min_hold if displacement_enabled else None,
        "displacement_max_return": displacement_max_return if displacement_enabled else None,
        "displacement_z_min": displacement_z_min if displacement_enabled else None,
        "displacement_max_swaps_per_day": None,  # not applicable to single-position
    }

    summary = {
        "run_name": run_name,
        "window": {
            "start": trading_days[0].strftime("%Y-%m-%d"),
            "end": trading_days[-1].strftime("%Y-%m-%d"),
            "n_sessions": n_days,
        },
        "config": canonical_config,
        "params": {
            "slippage_bps": slippage_bps,
            "initial_capital": initial_capital,
        },
        "performance": {
            "final_equity": round(float(equity[-1]), 2),
            "total_return": round(float(total_return), 5),
            "cagr": round(float(cagr), 5),
            "max_drawdown": round(float(equity_df["drawdown"].min()), 5),
            "sharpe": round(sharpe, 3) if sharpe is not None else None,
        },
        "activity": {
            "n_trades": len(trades),
            "avg_holding_days": round(avg_hold, 1),
            "pct_time_invested": round(pct_invested, 4),
            "same_theme_rotations": same_theme_rotations,
            "cross_theme_rotations": cross_theme_rotations,
            "displacements": displacement_count,
        },
        "benchmarks": {
            "SPY": _bench_curve("SPY"),
            "QQQ": _bench_curve("QQQ"),
        },
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2, default=str))

    report = _format_report(summary, trade_df)
    (out_dir / "report.md").write_text(report)
    print(report)
    print(f"\n[replay] wrote {out_dir}", file=sys.stderr)
    return {"summary": summary, "trade_df": trade_df, "equity_df": equity_df, "out_dir": out_dir}


def _format_report(summary: dict, trade_df: pd.DataFrame) -> str:
    perf = summary["performance"]
    act = summary["activity"]
    bench = summary["benchmarks"]
    win = summary["window"]
    lines = [
        f"# Backtest report — {summary['run_name']}\n",
        f"Window: {win['start']} → {win['end']}  ({win['n_sessions']} sessions)",
        f"Slippage: {summary['params']['slippage_bps']:.0f} bps each side; capital $"
        f"{summary['params']['initial_capital']:,.0f}",
        "",
        "## Headline",
        "",
        f"- final equity:    ${perf['final_equity']:,.2f}",
        f"- total return:    {perf['total_return']*100:+.2f}%",
        f"- CAGR:            {perf['cagr']*100:+.2f}%",
        f"- max drawdown:    {perf['max_drawdown']*100:+.2f}%",
        f"- Sharpe (252-d):  {perf['sharpe'] if perf['sharpe'] is not None else '—'}",
        "",
        "## Activity",
        "",
        f"- trades:                       {act['n_trades']}",
        f"- avg holding days:             {act['avg_holding_days']}",
        f"- % time invested:              {act['pct_time_invested']*100:.1f}%",
        f"- same-theme rotations:         {act['same_theme_rotations']}",
        f"- cross-theme rotations:        {act['cross_theme_rotations']}",
        f"- displacements:                {act.get('displacements', 0)}",
        "",
        "## Benchmarks (same window)",
        "",
        f"- SPY total return: {bench['SPY']['total_return']*100:+.2f}%  CAGR {bench['SPY']['cagr']*100:+.2f}%  MDD {bench['SPY']['max_drawdown']*100:+.2f}%"
        if bench["SPY"]["total_return"] is not None else "- SPY: data unavailable",
        f"- QQQ total return: {bench['QQQ']['total_return']*100:+.2f}%  CAGR {bench['QQQ']['cagr']*100:+.2f}%  MDD {bench['QQQ']['max_drawdown']*100:+.2f}%"
        if bench["QQQ"]["total_return"] is not None else "- QQQ: data unavailable",
        "",
    ]
    if not trade_df.empty:
        winners = trade_df[trade_df["return_pct"] > 0]
        losers = trade_df[trade_df["return_pct"] <= 0]
        lines += [
            "## Trade summary",
            "",
            f"- winners: {len(winners)}  (avg return {winners['return_pct'].mean()*100:+.2f}%)" if not winners.empty else "- winners: 0",
            f"- losers:  {len(losers)}  (avg return {losers['return_pct'].mean()*100:+.2f}%)" if not losers.empty else "- losers:  0",
            "",
            "### Top 5 winners",
            "",
        ]
        top = trade_df.sort_values("return_pct", ascending=False).head(5)
        for _, r in top.iterrows():
            lines.append(f"- {r['ticker']:<6} {r['theme']:<20}  {r['return_pct']*100:+6.1f}%  "
                         f"({r['entry_date']} → {r['exit_date']}, {r['holding_period_days']}d, exit {r['exit_reason']})")
        lines += ["", "### Bottom 5 losers", ""]
        bot = trade_df.sort_values("return_pct").head(5)
        for _, r in bot.iterrows():
            lines.append(f"- {r['ticker']:<6} {r['theme']:<20}  {r['return_pct']*100:+6.1f}%  "
                         f"({r['entry_date']} → {r['exit_date']}, {r['holding_period_days']}d, exit {r['exit_reason']})")
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Lookahead sanity check
# ---------------------------------------------------------------------------

def lookahead_check(ticker: str, date_str: str) -> int:
    """Compute factors for ticker on (date) two ways:
       (A) the live path: take all bars up through `date` and call
           compute_stability_factors directly
       (B) the replay path: call reconstruct_day with `date` and pull factors
    They must agree exactly. Any divergence = leakage.
    """
    as_of = pd.Timestamp(date_str).normalize()
    universe = build_universe()
    fetch_set = list(set(universe + ["$SPX", ticker.upper()]))
    print(f"[lookahead] fetching bars for {len(fetch_set)} tickers...", file=sys.stderr)
    fetch_results = fetch_daily_bars(fetch_set)
    bars_by_ticker = {}
    for t, fr in fetch_results.items():
        if fr.error or fr.bars is None or fr.bars.empty:
            continue
        df = fr.bars.sort_values("date").reset_index(drop=True)
        df["date"] = pd.to_datetime(df["date"]).dt.normalize()
        bars_by_ticker[t] = df

    spx_bars = bars_by_ticker.get("$SPX")
    bars = bars_by_ticker.get(ticker.upper())
    if bars is None or bars.empty:
        print(f"[lookahead] no bars for {ticker}", file=sys.stderr)
        return 1

    # Path A: live path
    spx_log_a = _spx_log_returns_from(spx_bars[spx_bars["date"] <= as_of] if spx_bars is not None else None)
    bars_a = bars[bars["date"] <= as_of]
    f_a = compute_stability_factors(ticker.upper(), bars_a, spx_log_returns=spx_log_a)

    # Path B: replay path
    day_state = reconstruct_day(bars_by_ticker, as_of, spx_bars, held_ticker=ticker.upper())
    f_b = day_state.factors_by_ticker.get(ticker.upper())

    if f_a is None and f_b is None:
        print(f"[lookahead] both paths returned None — no data on {date_str}")
        return 0
    if f_a is None or f_b is None:
        print(f"[lookahead] DIVERGENCE: live={f_a is not None}, replay={f_b is not None}")
        return 2

    diffs = []
    factor_keys = sorted(set(f_a.factors.keys()) | set(f_b.factors.keys()))
    for k in factor_keys:
        a = f_a.factors.get(k)
        b = f_b.factors.get(k)
        if a is None and b is None:
            continue
        if a is None or b is None:
            diffs.append((k, a, b))
            continue
        if abs(a - b) > 1e-9:
            diffs.append((k, a, b))
    # Compare a few scalars
    for attr in ("last_close", "ret_12m", "calmar", "r_sq_126", "down_capture", "trend_status"):
        a = getattr(f_a, attr)
        b = getattr(f_b, attr)
        if a is None and b is None:
            continue
        if isinstance(a, float) and isinstance(b, float):
            if abs(a - b) > 1e-9:
                diffs.append((attr, a, b))
        elif a != b:
            diffs.append((attr, a, b))

    if not diffs:
        print(f"[lookahead] NO LEAK detected on {ticker.upper()} as of {date_str}")
        return 0

    print(f"[lookahead] DIVERGENCE on {ticker.upper()} as of {date_str}:")
    for k, a, b in diffs:
        print(f"  {k}:  live={a}   replay={b}")
    return 3


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--days", type=int, default=365,
                    help="lookback window in calendar days (default 365)")
    ap.add_argument("--end-date", default=None,
                    help="end of replay window (YYYY-MM-DD); default = today")
    ap.add_argument("--slippage-bps", type=float, default=15.0,
                    help="slippage applied each leg of a rotation (default 15)")
    ap.add_argument("--capital", type=float, default=100_000.0,
                    help="initial capital (default $100k)")
    ap.add_argument("--refresh", action="store_true",
                    help="bust the daily-bar cache before fetching")
    ap.add_argument("--lookahead-check", nargs=2, metavar=("TICKER", "DATE"),
                    help="run the lookahead-sanity check and exit")
    ap.add_argument("--source", choices=["schwab", "massive"], default="schwab",
                    help="bars source: schwab (~13mo cached) or massive (5y parquet)")
    ap.add_argument("--parquet-path", default=None,
                    help="path to massive parquet (default: backtest/data/aggs_daily.parquet)")
    ap.add_argument("--exit-rule", choices=["ma_50d", "trailing_pct"], default="ma_50d",
                    help="ma_50d (default): eject on close below 50d SMA. "
                         "trailing_pct: eject on close >X%% below peak since entry")
    ap.add_argument("--trailing-pct", type=float, default=25.0,
                    help="trailing-stop %% (only used when --exit-rule=trailing_pct)")
    ap.add_argument("--dynamic-themes", action="store_true",
                    help="construct themes from SIC codes per as-of date instead of "
                         "loading themes.yaml. Removes hindsight bias from the universe.")
    ap.add_argument("--universe-top-n", type=int, default=500,
                    help="when --dynamic-themes / --ignore-themes: use top N tickers by "
                         "median $vol from the eligible metadata set (default 500)")
    ap.add_argument("--ignore-themes", action="store_true",
                    help="Path A: skip theme construction entirely. Pick absolute "
                         "composite leader from the eligible universe; no rotation logic.")
    ap.add_argument("--positions", type=int, default=1,
                    help="number of positions held simultaneously (equal-weight). "
                         "1 (default) = single-position legacy code path. "
                         "2+ uses run_replay_multi which tracks shares per position "
                         "with independent trailing stops.")
    ap.add_argument("--strategy", choices=["pathA", "pathP", "pathV", "pathV2", "pathS"], default="pathA",
                    help="entry rule: pathA (absolute composite leader, breakout names), "
                         "pathP (pullback continuation in strong trenders), pathV (vol "
                         "expansion only — broken), pathV2 (Path A eligibility + coil), "
                         "or pathS (options skew-flip — requires data/skew_daily.parquet)")
    ap.add_argument("--skew-direction", choices=["bullish", "bearish"], default="bullish",
                    help="Path S direction: bullish (z > +threshold) or bearish "
                         "(z < -threshold). Cohort test showed bearish has stronger edge.")
    ap.add_argument("--skew-z-min", type=float, default=1.5,
                    help="Path S |z| threshold (default 1.5)")
    ap.add_argument("--skew-z-window", type=int, default=60,
                    help="Path S rolling-z lookback in trading days. Default 60 "
                         "matches the verdict-memo backtests; 252 matches the live "
                         "data_refresh.py pipeline. Sweepable via cloud matrix.")
    ap.add_argument("--skew-z-persistence", type=int, default=1,
                    help="number of consecutive days the skew threshold must be "
                         "satisfied before an entry qualifies. Default 1 (today only). "
                         "Set to 2 to filter one-day skew noise spikes.")
    ap.add_argument("--speculative-only", action="store_true",
                    help="Path S: replace eligibility filter with price $1-$50 + "
                         "vol_60d > 50%% + $5M+ liquidity. Targets cult/speculative tail.")
    ap.add_argument("--max-hold-days", type=int, default=None,
                    help="hard ceiling on per-position holding period. Fires before "
                         "trailing/MA exits. Default: no max hold.")
    ap.add_argument("--signal-decay-z", type=float, default=None,
                    help="Path S only: exit when skew_z drifts below +decay_z (bullish) "
                         "or above -decay_z (bearish) for N consecutive days. "
                         "E.g. --signal-decay-z 0.5 --signal-decay-days 2 means: exit "
                         "if signal stays below threshold 2 days running.")
    ap.add_argument("--signal-decay-days", type=int, default=2,
                    help="number of consecutive decayed days before exit (default 2)")
    ap.add_argument("--earnings-blackout-before", type=int, default=0,
                    help="skip entries within N calendar days BEFORE the next "
                         "earnings press-release (yfinance dates). 0 disables. "
                         "Implementation: pre-fetches earnings dates for the "
                         "trading universe via yfinance and caches to "
                         "data/earnings_dates.json.")
    ap.add_argument("--earnings-blackout-after", type=int, default=0,
                    help="skip entries within N calendar days AFTER an earnings "
                         "release. 0 disables. Combine with --earnings-blackout-before "
                         "to make a window straddling the report (e.g. -7/+1).")
    ap.add_argument("--earnings-cache-path", default=None,
                    help="override cache path (default scripts/conviction/backtest/data/"
                         "earnings_dates.json)")
    ap.add_argument("--earnings-blackout-mode", choices=["replace", "skip"],
                    default="replace",
                    help="behavior when an entry candidate is in the blackout "
                         "window: 'replace' walks down skew ranks for a "
                         "non-blackout substitute; 'skip' leaves the slot "
                         "empty (and stops filling further slots today). "
                         "Default: replace.")
    ap.add_argument("--regime-gate", choices=["none", "spy", "vix", "both"], default="none",
                    help="market regime gate. RISK_OFF forces exit at next open AND "
                         "blocks new entries. spy=SPY>200d trend filter. vix=VIX state "
                         "machine (pause >25, resume <20 with 3-day confirm). "
                         "both=AND of the two.")
    ap.add_argument("--min-pct-above-200d", type=float, default=None,
                    help="Path S only: require close >= SMA-200 by at least this "
                         "fraction (e.g. 0.10 = +10%% above 200d). Default: no floor.")
    ap.add_argument("--min-ret-60d", type=float, default=None,
                    help="Path S only: require trailing 60d return >= this fraction "
                         "(e.g. 0.20 = +20%%). Default: no floor (only the implicit "
                         ">0% requirement applies).")
    ap.add_argument("--run-suffix", default=None,
                    help="appended to the output dir name (results/<run>_<suffix>). "
                         "Use to keep parameter sweeps from overwriting each other.")
    ap.add_argument("--displacement-enabled", action="store_true",
                    help="single-position only: enable stale-loser displacement. "
                         "When holding a position that's been stale (held >= "
                         "--displacement-min-hold) AND under-water (return so "
                         "far <= --displacement-max-return), force a rotation "
                         "into a fresh challenger if any non-held name has "
                         "z >= --displacement-z-min today. The replacement is "
                         "picked by the strategy's normal pick_replacement (for "
                         "pathS that's the highest-z non-held name).")
    ap.add_argument("--displacement-min-hold", type=int, default=20,
                    help="displacement: minimum hold days before displacement "
                         "can fire. Default 20.")
    ap.add_argument("--displacement-max-return", type=float, default=0.0,
                    help="displacement: only fire if current return so far is "
                         "<= this fraction. Default 0.0 (red positions only).")
    ap.add_argument("--displacement-z-min", type=float, default=3.0,
                    help="displacement: only fire if some non-held name has "
                         "z >= this today. Default 3.0.")
    ap.add_argument("--displacement-max-swaps-per-day", type=int, default=1,
                    help="multi-position only: cap on number of displacement "
                         "swaps that can fire on a single day. Default 1. "
                         "Single-position runs ignore this (only one slot).")
    args = ap.parse_args()

    if args.lookahead_check:
        return lookahead_check(args.lookahead_check[0], args.lookahead_check[1])

    end = pd.Timestamp(args.end_date) if args.end_date else pd.Timestamp(datetime.now())

    skew_lookup: dict | None = None
    if args.strategy == "pathS":
        skew_lookup = load_skew_lookup(
            z_window=args.skew_z_window,
            persistence_days=args.skew_z_persistence,
            abs_skew_z_min=args.skew_z_min,
            direction=args.skew_direction,
        )
        if not skew_lookup:
            raise SystemExit(
                "[replay] Path S requires data/skew_daily.parquet — run "
                "iv_compute.py --shard X/N for all shards then --merge-shards"
            )
        # Push CLI knobs onto the module-level Path S config the picker reads.
        PATH_S_CONFIG["direction"] = args.skew_direction
        PATH_S_CONFIG["abs_skew_z_min"] = args.skew_z_min
        PATH_S_CONFIG["speculative_only"] = args.speculative_only
        PATH_S_CONFIG["min_pct_above_200d"] = args.min_pct_above_200d
        PATH_S_CONFIG["min_ret_60d"] = args.min_ret_60d
        print(f"[replay] Path S config: direction={args.skew_direction} "
              f"|z|>={args.skew_z_min} speculative_only={args.speculative_only} "
              f"min_pct_above_200d={args.min_pct_above_200d} "
              f"min_ret_60d={args.min_ret_60d}",
              file=sys.stderr)

    regime_lookup: dict | None = None
    if args.regime_gate != "none":
        from regime_filter import build_regime_lookup
        regime_lookup = build_regime_lookup(args.regime_gate)

    earnings_lookup = None
    if args.earnings_blackout_before > 0 or args.earnings_blackout_after > 0:
        from earnings_calendar import EarningsLookup, DEFAULT_CACHE
        cache_path = (Path(args.earnings_cache_path)
                      if args.earnings_cache_path else DEFAULT_CACHE)
        earnings_lookup = EarningsLookup(cache_path)

    if args.positions >= 2:
        run_replay_multi(
            end_date=end,
            lookback_days=args.days,
            source=args.source,
            parquet_path=args.parquet_path,
            exit_rule=args.exit_rule,
            trailing_pct=args.trailing_pct,
            dynamic_themes=args.dynamic_themes,
            universe_top_n=args.universe_top_n,
            ignore_themes=args.ignore_themes,
            slippage_bps=args.slippage_bps,
            initial_capital=args.capital,
            refresh=args.refresh,
            n_positions=args.positions,
            strategy=args.strategy,
            skew_lookup=skew_lookup,
            max_hold_days=args.max_hold_days,
            signal_decay_z=args.signal_decay_z,
            signal_decay_days=args.signal_decay_days,
            skew_direction=args.skew_direction,
            regime_lookup=regime_lookup,
            regime_gate=args.regime_gate,
            run_suffix=args.run_suffix,
            earnings_lookup=earnings_lookup,
            earnings_blackout_before=args.earnings_blackout_before,
            earnings_blackout_after=args.earnings_blackout_after,
            earnings_blackout_mode=args.earnings_blackout_mode,
            displacement_enabled=args.displacement_enabled,
            displacement_min_hold=args.displacement_min_hold,
            displacement_max_return=args.displacement_max_return,
            displacement_z_min=args.displacement_z_min,
            displacement_max_swaps_per_day=args.displacement_max_swaps_per_day,
        )
        return 0

    run_replay(
        end_date=end,
        lookback_days=args.days,
        source=args.source,
        parquet_path=args.parquet_path,
        exit_rule=args.exit_rule,
        trailing_pct=args.trailing_pct,
        dynamic_themes=args.dynamic_themes,
        universe_top_n=args.universe_top_n,
        ignore_themes=args.ignore_themes,
        strategy=args.strategy,
        slippage_bps=args.slippage_bps,
        initial_capital=args.capital,
        refresh=args.refresh,
        skew_lookup=skew_lookup,
        max_hold_days=args.max_hold_days,
        signal_decay_z=args.signal_decay_z,
        signal_decay_days=args.signal_decay_days,
        skew_direction=args.skew_direction,
        regime_lookup=regime_lookup,
        regime_gate=args.regime_gate,
        run_suffix=args.run_suffix,
        displacement_enabled=args.displacement_enabled,
        displacement_min_hold=args.displacement_min_hold,
        displacement_max_return=args.displacement_max_return,
        displacement_z_min=args.displacement_z_min,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main() or 0)
