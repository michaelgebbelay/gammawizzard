"""Trade simulator for the v2 scanner.

Design principle: separate **trade-path generation** (slow, depends on
universe + candidates + cooldown) from **exit evaluation** (fast,
post-hoc on a precomputed forward OHLC path). This lets a sweep across
many exit rules reuse one set of forward paths.

Pipeline:
    bars + universe + scores
      → candidate_table(rule)            # per-date top-K or pct selection
      → apply_cooldown(days)             # drop repeat events for same symbol
      → entry_paths(bars)                # entry_open + 20d forward OHLC array
      → simulate_exits(rule, slippage)   # one trade ledger per exit config
      → portfolio_aggregate(max_pos)     # daily P&L curve

Every public function returns a DataFrame so the result is composable and
inspectable.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Literal, Optional

import numpy as np
import pandas as pd


# ────────────────────────────────────────────────────────────────────
# 1. Universe + score helpers (light wrappers for the trade pipeline)
# ────────────────────────────────────────────────────────────────────


def apply_liquidity_floor(
    universe: pd.DataFrame,
    *,
    min_price: float,
    min_avg_dollar_volume_20d: float,
) -> pd.DataFrame:
    """Recompute is_tradeable with tighter thresholds without rebuilding bars."""
    df = universe.copy()
    df["is_tradeable"] = (
        df["is_tradeable"]
        & (df["close"] >= min_price)
        & (df["avg_dollar_vol_20d"] >= min_avg_dollar_volume_20d)
    )
    return df


# ────────────────────────────────────────────────────────────────────
# 2. Candidate generation
# ────────────────────────────────────────────────────────────────────


def make_candidates(
    scored: pd.DataFrame,
    *,
    rule: Literal["top_k", "pct", "top_k_with_pct_floor", "top_1_with_score_gap"],
    rule_arg: float | tuple = 50,
    score_col: str = "explosive_score",
) -> pd.DataFrame:
    """Return one row per (symbol, date) candidate.

    rule = "top_k": rule_arg = K (e.g. 50, 100, 1, 2)
    rule = "pct":   rule_arg = percentile cutoff in (0, 100]
    rule = "top_k_with_pct_floor": rule_arg = (K, pct), e.g. (1, 95) means
                    rank-1 only if also above 95th-pct of score by date
    rule = "top_1_with_score_gap": rule_arg = min gap between rank 1 and
                    rank 2 score (in score-points, e.g. 2.0, 5.0, 10.0)
    """
    s = scored.dropna(subset=[score_col]).copy()
    if rule == "top_k":
        k = int(rule_arg)
        cands = (
            s.sort_values(["date", score_col], ascending=[True, False])
            .groupby("date", sort=False, as_index=False)
            .head(k)
        )
    elif rule == "pct":
        thresh = s.groupby("date")[score_col].transform(
            lambda x: x.quantile(rule_arg / 100.0)
        )
        cands = s[s[score_col] >= thresh]
    elif rule == "top_k_with_pct_floor":
        k, pct = rule_arg
        thresh = s.groupby("date")[score_col].transform(
            lambda x: x.quantile(pct / 100.0)
        )
        topk = (
            s.sort_values(["date", score_col], ascending=[True, False])
            .groupby("date", sort=False, as_index=False)
            .head(int(k))
        )
        cands = topk[topk[score_col] >= thresh.loc[topk.index]]
    elif rule == "top_1_with_score_gap":
        gap_thresh = float(rule_arg)
        keep = []
        for d, g in s.sort_values(["date", score_col], ascending=[True, False]).groupby("date", sort=False):
            if len(g) < 2:
                keep.append(g.index[0])
                continue
            g0 = g[score_col].iat[0]
            g1 = g[score_col].iat[1]
            if (g0 - g1) >= gap_thresh:
                keep.append(g.index[0])
        cands = s.loc[keep]
    else:
        raise ValueError(rule)
    return cands.reset_index(drop=True)


# ────────────────────────────────────────────────────────────────────
# 2b. Entry confirmation filters
# ────────────────────────────────────────────────────────────────────


def filter_close_confirmed_breakout(
    cands: pd.DataFrame, *, vol_ratio_min: float = 1.5,
) -> pd.DataFrame:
    """Entry B: require close_t > prior_5d_high AND volume_ratio_20d >= 1.5
    on the signal day. Both columns must be present in cands."""
    needed = {"close", "prior_5d_high", "volume_ratio_20d"}
    missing = needed - set(cands.columns)
    if missing:
        raise KeyError(f"filter_close_confirmed_breakout missing cols: {missing}")
    return cands[
        (cands["close"] > cands["prior_5d_high"])
        & (cands["volume_ratio_20d"] >= vol_ratio_min)
    ].reset_index(drop=True)


def filter_positive_momentum(cands: pd.DataFrame) -> pd.DataFrame:
    """Entry C: require ret_1d > 0 AND close > open on the signal day."""
    needed = {"ret_1d", "open", "close"}
    missing = needed - set(cands.columns)
    if missing:
        raise KeyError(f"filter_positive_momentum missing cols: {missing}")
    return cands[(cands["ret_1d"] > 0) & (cands["close"] > cands["open"])].reset_index(drop=True)


def filter_strong_close(cands: pd.DataFrame) -> pd.DataFrame:
    """Entry D: close >= low + 0.75 * (high - low) AND ret_1d > 0."""
    needed = {"high", "low", "close", "ret_1d"}
    missing = needed - set(cands.columns)
    if missing:
        raise KeyError(f"filter_strong_close missing cols: {missing}")
    rng = cands["high"] - cands["low"]
    closes_in_top_quartile = cands["close"] >= cands["low"] + 0.75 * rng
    return cands[closes_in_top_quartile & (cands["ret_1d"] > 0)].reset_index(drop=True)


def apply_cooldown(candidates: pd.DataFrame, *, days: int) -> pd.DataFrame:
    """Drop a symbol's signals that fall within `days` trading days of an
    earlier signal for the same symbol. Trading-day distance is approximated
    by counting unique business dates in the panel between the two signals.
    """
    if days <= 0:
        return candidates.reset_index(drop=True)
    cands = candidates.sort_values(["symbol", "date"]).reset_index(drop=True)
    keep = np.zeros(len(cands), dtype=bool)
    last_keep_date_by_symbol: dict[str, pd.Timestamp] = {}
    panel_dates = np.array(sorted(cands["date"].unique()))
    date_to_idx = {d: i for i, d in enumerate(panel_dates)}

    for i, (sym, dt) in enumerate(zip(cands["symbol"].to_numpy(), cands["date"].to_numpy())):
        last = last_keep_date_by_symbol.get(sym)
        if last is None:
            keep[i] = True
            last_keep_date_by_symbol[sym] = dt
            continue
        gap = date_to_idx[dt] - date_to_idx[last]
        if gap >= days:
            keep[i] = True
            last_keep_date_by_symbol[sym] = dt

    return cands.loc[keep].reset_index(drop=True)


# ────────────────────────────────────────────────────────────────────
# 3. Entry paths — for each candidate, pre-collect forward OHLC
# ────────────────────────────────────────────────────────────────────


def attach_entry_paths_buystop(
    candidates: pd.DataFrame,
    bars: pd.DataFrame,
    *,
    trigger_col: str = "prior_5d_high",
    trigger_mult: float = 1.001,
    trigger_window: int = 3,
    forward_days: int = 20,
) -> dict:
    """Entry E: buy-stop breakout.

    Place a buy-stop at `cands[trigger_col] * trigger_mult` valid for
    `trigger_window` trading days after the signal. If next-day open ≥
    trigger → enter at open. Else if any day's high ≥ trigger → enter at
    trigger. If trigger never hits in window → drop the candidate.

    Returns the same shape dict as `attach_entry_paths`, but only with
    candidates that triggered. Forward path begins on the entry day's
    NEXT bar (we exclude the entry day from exit simulation since the
    entry order consumed part of the day).
    """
    bars = bars.sort_values(["symbol", "date"]).reset_index(drop=True)
    by_sym = {}
    for sym, g in bars.groupby("symbol", sort=False):
        by_sym[sym] = {
            "date": g["date"].to_numpy(),
            "open": g["open"].to_numpy(),
            "high": g["high"].to_numpy(),
            "low": g["low"].to_numpy(),
            "close": g["close"].to_numpy(),
            "dollar_volume": g["dollar_volume"].to_numpy(),
        }

    keep_idx = []
    entry_dates, entry_prices, avg_dvs = [], [], []
    paths_o, paths_h, paths_l, paths_c, paths_d = [], [], [], [], []

    syms = candidates["symbol"].to_numpy()
    sig_dates = candidates["date"].to_numpy()
    trig_vals = candidates[trigger_col].to_numpy() * trigger_mult
    cand_idx = candidates.index.to_numpy()
    for k in range(len(candidates)):
        sym = syms[k]
        s = by_sym.get(sym)
        if s is None:
            continue
        trigger_px = trig_vals[k]
        if not np.isfinite(trigger_px):
            continue
        sig_date = sig_dates[k]
        idx0 = np.searchsorted(s["date"], sig_date, side="right")
        i = cand_idx[k]
        end0 = min(idx0 + trigger_window, len(s["date"]))
        # Scan trigger_window days for fill
        entry_idx = -1
        entry_px = np.nan
        for j in range(idx0, end0):
            if s["open"][j] >= trigger_px:
                entry_idx = j
                entry_px = s["open"][j]
                break
            if s["high"][j] >= trigger_px:
                entry_idx = j
                entry_px = trigger_px
                break
        if entry_idx < 0:
            continue
        # Forward path starts on the day AFTER entry
        path_start = entry_idx + 1
        path_end = min(path_start + forward_days, len(s["date"]))
        n_avail = path_end - path_start
        if n_avail <= 0:
            continue
        po = np.full(forward_days, np.nan); ph = np.full(forward_days, np.nan)
        pl = np.full(forward_days, np.nan); pc = np.full(forward_days, np.nan)
        pd_dates = np.full(forward_days, np.datetime64("NaT"), dtype="datetime64[ns]")
        po[:n_avail] = s["open"][path_start:path_end]
        ph[:n_avail] = s["high"][path_start:path_end]
        pl[:n_avail] = s["low"][path_start:path_end]
        pc[:n_avail] = s["close"][path_start:path_end]
        pd_dates[:n_avail] = s["date"][path_start:path_end]

        keep_idx.append(i)
        entry_dates.append(s["date"][entry_idx])
        entry_prices.append(entry_px)
        avg_dvs.append(s["dollar_volume"][entry_idx - 1] if entry_idx >= 1 else np.nan)
        paths_o.append(po); paths_h.append(ph); paths_l.append(pl); paths_c.append(pc); paths_d.append(pd_dates)

    if not keep_idx:
        empty_n = 0
        return {
            "candidates": candidates.iloc[:0].copy().assign(
                entry_date=pd.Series(dtype="datetime64[ns]"),
                entry_open=pd.Series(dtype=float),
                entry_dollar_volume=pd.Series(dtype=float),
            ),
            "open": np.zeros((0, forward_days)),
            "high": np.zeros((0, forward_days)),
            "low": np.zeros((0, forward_days)),
            "close": np.zeros((0, forward_days)),
            "dates": np.zeros((0, forward_days), dtype="datetime64[ns]"),
        }

    cands = candidates.loc[keep_idx].copy().reset_index(drop=True)
    cands["entry_date"] = entry_dates
    cands["entry_open"] = entry_prices  # buy-stop fill price
    cands["entry_dollar_volume"] = avg_dvs
    return {
        "candidates": cands,
        "open":  np.array(paths_o),
        "high":  np.array(paths_h),
        "low":   np.array(paths_l),
        "close": np.array(paths_c),
        "dates": np.array(paths_d),
    }


def attach_entry_paths_gap(
    candidates: pd.DataFrame,
    bars: pd.DataFrame,
    *,
    min_gap_pct: float = 0.02,
    forward_days: int = 20,
) -> dict:
    """Entry F: gap-continuation. Require next day open >= signal close *
    (1 + min_gap_pct). If yes, enter at next open. Else drop."""
    paths = attach_entry_paths(candidates, bars, forward_days=forward_days)
    cands = paths["candidates"]
    needed = {"close"}
    if not needed.issubset(cands.columns):
        raise KeyError(f"attach_entry_paths_gap missing cols on candidates: {needed - set(cands.columns)}")
    gap = cands["entry_open"] / cands["close"] - 1.0
    mask = gap >= min_gap_pct
    if mask.all():
        return paths
    keep = mask.to_numpy()
    return {
        "candidates": cands.loc[keep].reset_index(drop=True),
        "open":  paths["open"][keep],
        "high":  paths["high"][keep],
        "low":   paths["low"][keep],
        "close": paths["close"][keep],
        "dates": paths["dates"][keep],
    }


def attach_entry_paths(
    candidates: pd.DataFrame,
    bars: pd.DataFrame,
    *,
    forward_days: int = 20,
) -> dict[str, np.ndarray]:
    """Return a dict of arrays keyed by field, one row per candidate, each
    row of length forward_days holding the forward OHLC sequence starting at
    entry_date (= signal_date + 1 trading day).

    Output keys:
        candidates  — DataFrame with entry_date, entry_open columns added
        path_open, path_high, path_low, path_close — np.ndarray (n_cand, forward_days)
        path_dates  — np.ndarray (n_cand, forward_days)  dtype=datetime64[ns]
    """
    bars = bars.sort_values(["symbol", "date"]).reset_index(drop=True)
    by_sym: dict[str, dict[str, np.ndarray]] = {}
    for sym, g in bars.groupby("symbol", sort=False):
        by_sym[sym] = {
            "date": g["date"].to_numpy(),
            "open": g["open"].to_numpy(),
            "high": g["high"].to_numpy(),
            "low": g["low"].to_numpy(),
            "close": g["close"].to_numpy(),
            "dollar_volume": g["dollar_volume"].to_numpy(),
        }

    n = len(candidates)
    p_open = np.full((n, forward_days), np.nan)
    p_high = np.full((n, forward_days), np.nan)
    p_low = np.full((n, forward_days), np.nan)
    p_close = np.full((n, forward_days), np.nan)
    p_dates = np.full((n, forward_days), np.datetime64("NaT"), dtype="datetime64[ns]")
    entry_open = np.full(n, np.nan)
    entry_date = np.full(n, np.datetime64("NaT"), dtype="datetime64[ns]")
    avg_dv = np.full(n, np.nan)

    for i, (sym, sig_date) in enumerate(zip(candidates["symbol"].to_numpy(), candidates["date"].to_numpy())):
        s = by_sym.get(sym)
        if s is None:
            continue
        idx = np.searchsorted(s["date"], sig_date, side="right")  # first day after signal
        if idx >= len(s["date"]):
            continue
        entry_open[i] = s["open"][idx]
        entry_date[i] = s["date"][idx]
        avg_dv[i] = s["dollar_volume"][idx - 1] if idx >= 1 else np.nan
        end = min(idx + forward_days, len(s["date"]))
        n_avail = end - idx
        if n_avail <= 0:
            continue
        p_open[i, :n_avail] = s["open"][idx:end]
        p_high[i, :n_avail] = s["high"][idx:end]
        p_low[i, :n_avail] = s["low"][idx:end]
        p_close[i, :n_avail] = s["close"][idx:end]
        p_dates[i, :n_avail] = s["date"][idx:end]

    cands = candidates.copy()
    cands["entry_date"] = entry_date
    cands["entry_open"] = entry_open
    cands["entry_dollar_volume"] = avg_dv
    return {
        "candidates": cands,
        "open": p_open,
        "high": p_high,
        "low": p_low,
        "close": p_close,
        "dates": p_dates,
    }


# ────────────────────────────────────────────────────────────────────
# 4. Exit simulators
# ────────────────────────────────────────────────────────────────────


@dataclass
class ExitResult:
    exit_idx: np.ndarray         # integer offset within forward path; -1 if no exit
    exit_price: np.ndarray
    exit_date: np.ndarray
    exit_reason: np.ndarray      # object dtype: "fixed", "target", "stop", "trailing", "max_hold", "no_exit"


def _result_arrays(n: int) -> ExitResult:
    return ExitResult(
        exit_idx=np.full(n, -1, dtype=np.int32),
        exit_price=np.full(n, np.nan),
        exit_date=np.full(n, np.datetime64("NaT"), dtype="datetime64[ns]"),
        exit_reason=np.array(["no_exit"] * n, dtype=object),
    )


def exit_fixed_horizon(paths: dict, *, days: int) -> ExitResult:
    n = paths["close"].shape[0]
    res = _result_arrays(n)
    h = min(days, paths["close"].shape[1]) - 1
    if h < 0:
        return res
    closes = paths["close"][:, h]
    dates = paths["dates"][:, h]
    valid = ~np.isnan(closes)
    res.exit_idx[valid] = h
    res.exit_price[valid] = closes[valid]
    res.exit_date[valid] = dates[valid]
    res.exit_reason[valid] = "fixed"
    return res


def exit_target_stop(
    paths: dict,
    *,
    target_pct: float,
    stop_pct: float,
    max_hold: int,
) -> ExitResult:
    """Profit target / stop loss with intraday high-low simulation.

    Conservative tie rule: if both target and stop are touched same day,
    assume stop hit first. Open-gap logic: if open >= target → exit at open.
    """
    opens = paths["open"]
    highs = paths["high"]
    lows = paths["low"]
    closes = paths["close"]
    dates = paths["dates"]
    n, h = opens.shape
    h = min(h, max_hold)

    n_paths = opens.shape[0]
    res = _result_arrays(n_paths)

    entry = opens[:, 0]
    target_px = entry * (1.0 + target_pct)
    stop_px = entry * (1.0 - stop_pct)

    for i in range(n_paths):
        e = entry[i]
        if not np.isfinite(e):
            continue
        tgt = target_px[i]
        stp = stop_px[i]
        for t in range(h):
            o = opens[i, t]
            if not np.isfinite(o):
                # ran out of forward data
                last_t = t - 1
                if last_t >= 0:
                    res.exit_idx[i] = last_t
                    res.exit_price[i] = closes[i, last_t]
                    res.exit_date[i] = dates[i, last_t]
                    res.exit_reason[i] = "no_exit"
                break
            hi = highs[i, t]
            lo = lows[i, t]

            # On the entry day (t=0), the open IS the entry; intraday from there.
            # Gap-through behavior at session open:
            if t > 0:
                if o <= stp:
                    res.exit_idx[i] = t; res.exit_price[i] = o
                    res.exit_date[i] = dates[i, t]; res.exit_reason[i] = "stop"
                    break
                if o >= tgt:
                    res.exit_idx[i] = t; res.exit_price[i] = o
                    res.exit_date[i] = dates[i, t]; res.exit_reason[i] = "target"
                    break

            # Intraday: stop hit first (conservative)
            if lo <= stp:
                res.exit_idx[i] = t; res.exit_price[i] = stp
                res.exit_date[i] = dates[i, t]; res.exit_reason[i] = "stop"
                break
            if hi >= tgt:
                res.exit_idx[i] = t; res.exit_price[i] = tgt
                res.exit_date[i] = dates[i, t]; res.exit_reason[i] = "target"
                break
        else:
            # Loop completed without break — exit at max_hold close
            last_t = h - 1
            if last_t >= 0 and np.isfinite(closes[i, last_t]):
                res.exit_idx[i] = last_t
                res.exit_price[i] = closes[i, last_t]
                res.exit_date[i] = dates[i, last_t]
                res.exit_reason[i] = "max_hold"
    return res


def exit_target_only(
    paths: dict, *, target_pct: float, max_hold: int,
) -> ExitResult:
    """Target-only exit (no stop). Walk forward; exit at target if hit
    intraday (or open if gapped beyond), else exit at max-hold close."""
    opens = paths["open"]; highs = paths["high"]; closes = paths["close"]; dates = paths["dates"]
    n_paths, fh = closes.shape
    h = min(fh, max_hold)
    res = _result_arrays(n_paths)

    # entry price = day-0 open (consistent with attach_entry_paths). For
    # buy-stop variants, entry price was already substituted into open[0].
    entry = opens[:, 0]
    target_px = entry * (1.0 + target_pct)

    for i in range(n_paths):
        e = entry[i]
        if not np.isfinite(e):
            continue
        tgt = target_px[i]
        for t in range(h):
            o = opens[i, t]; hi = highs[i, t]; c = closes[i, t]
            if not np.isfinite(c):
                last_t = t - 1
                if last_t >= 0:
                    res.exit_idx[i] = last_t
                    res.exit_price[i] = closes[i, last_t]
                    res.exit_date[i] = dates[i, last_t]
                    res.exit_reason[i] = "no_exit"
                break
            if t > 0 and o >= tgt:
                res.exit_idx[i] = t; res.exit_price[i] = o
                res.exit_date[i] = dates[i, t]; res.exit_reason[i] = "target"
                break
            if hi >= tgt:
                res.exit_idx[i] = t; res.exit_price[i] = tgt
                res.exit_date[i] = dates[i, t]; res.exit_reason[i] = "target"
                break
        else:
            last_t = h - 1
            if last_t >= 0 and np.isfinite(closes[i, last_t]):
                res.exit_idx[i] = last_t
                res.exit_price[i] = closes[i, last_t]
                res.exit_date[i] = dates[i, last_t]
                res.exit_reason[i] = "max_hold"
    return res


def exit_target_emergency_stop(
    paths: dict, *, target_pct: float, emergency_stop_pct: float, max_hold: int,
) -> ExitResult:
    """Target hit intraday OR close-based emergency stop. The emergency
    stop checks ONLY the daily close — no intraday whipsaw exits. If
    close <= entry * (1 - emergency_stop_pct), exit at next open."""
    opens = paths["open"]; highs = paths["high"]; closes = paths["close"]; dates = paths["dates"]
    n_paths, fh = closes.shape
    h = min(fh, max_hold)
    res = _result_arrays(n_paths)
    entry = opens[:, 0]
    target_px = entry * (1.0 + target_pct)
    stop_close_px = entry * (1.0 - emergency_stop_pct)

    for i in range(n_paths):
        e = entry[i]
        if not np.isfinite(e):
            continue
        tgt = target_px[i]
        stp = stop_close_px[i]
        triggered_emergency_at = -1
        for t in range(h):
            o = opens[i, t]; hi = highs[i, t]; c = closes[i, t]
            if not np.isfinite(c):
                last_t = t - 1
                if last_t >= 0:
                    res.exit_idx[i] = last_t
                    res.exit_price[i] = closes[i, last_t]
                    res.exit_date[i] = dates[i, last_t]
                    res.exit_reason[i] = "no_exit"
                break

            # If emergency stop already armed (close hit yesterday), exit at today's open
            if triggered_emergency_at >= 0:
                res.exit_idx[i] = t; res.exit_price[i] = o
                res.exit_date[i] = dates[i, t]; res.exit_reason[i] = "emergency_stop"
                break

            # Target check (intraday OR open gap)
            if t > 0 and o >= tgt:
                res.exit_idx[i] = t; res.exit_price[i] = o
                res.exit_date[i] = dates[i, t]; res.exit_reason[i] = "target"
                break
            if hi >= tgt:
                res.exit_idx[i] = t; res.exit_price[i] = tgt
                res.exit_date[i] = dates[i, t]; res.exit_reason[i] = "target"
                break

            # Emergency stop arms if close <= stop level
            if c <= stp:
                triggered_emergency_at = t
        else:
            last_t = h - 1
            if last_t >= 0 and np.isfinite(closes[i, last_t]):
                res.exit_idx[i] = last_t
                res.exit_price[i] = closes[i, last_t]
                res.exit_date[i] = dates[i, last_t]
                res.exit_reason[i] = "max_hold"
    return res


def exit_variant(
    paths: dict,
    *,
    target_pct: float,
    emergency_stop_pct: float,
    max_hold: int,
    # Sprint-6 execution-realism knobs
    allow_entry_day_target: bool = True,
    target_fill_haircut: float = 0.0,
    ambiguous_day_skip_target: bool = False,
    ambiguous_day_close_threshold: float = 0.80,
    adverse_first_intraday: bool = False,
    adverse_intraday_stop_pct: float | None = None,
) -> ExitResult:
    """Configurable exit for Sprint-6 execution variants.

    Variant A (current):  defaults
    Variant B (no same-day target): allow_entry_day_target=False
    Variant C (same-day target only via gap-up open >= tgt): for next-open
        entry this is equivalent to allow_entry_day_target=False (because
        open == entry on day 0). Use the same flag.
    Variant D (target haircut): target_fill_haircut > 0; fills at
        tgt * (1 - haircut) instead of tgt.
    Variant E (ambiguous day penalty): ambiguous_day_skip_target=True;
        on day 0, if high >= tgt AND close <= entry * ambiguous_day_close_threshold,
        treat as no fill (emergency arming logic continues).
    Variant F (worst path): adverse_first_intraday=True;
        if low <= entry * (1 - adverse_intraday_stop_pct) AND high >= tgt
        on the same day, assume the stop hit first and exit at the
        intraday stop level. Requires adverse_intraday_stop_pct (defaults
        to emergency_stop_pct).
    """
    if adverse_intraday_stop_pct is None:
        adverse_intraday_stop_pct = emergency_stop_pct

    opens = paths["open"]; highs = paths["high"]; lows = paths["low"]
    closes = paths["close"]; dates = paths["dates"]
    n_paths, fh = closes.shape
    h = min(fh, max_hold)
    res = _result_arrays(n_paths)
    entry = opens[:, 0]
    target_px = entry * (1.0 + target_pct)
    fill_px = target_px * (1.0 - target_fill_haircut)
    close_stop_px = entry * (1.0 - emergency_stop_pct)
    adverse_intraday_px = entry * (1.0 - adverse_intraday_stop_pct)

    for i in range(n_paths):
        e = entry[i]
        if not np.isfinite(e):
            continue
        tgt = target_px[i]
        fill = fill_px[i]
        cstp = close_stop_px[i]
        adv_px = adverse_intraday_px[i]
        emergency_armed = -1
        for t in range(h):
            o = opens[i, t]; hi = highs[i, t]; lo = lows[i, t]; c = closes[i, t]
            if not np.isfinite(c):
                last_t = t - 1
                if last_t >= 0:
                    res.exit_idx[i] = last_t
                    res.exit_price[i] = closes[i, last_t]
                    res.exit_date[i] = dates[i, last_t]
                    res.exit_reason[i] = "no_exit"
                break

            # Emergency stop already armed → exit at next open
            if emergency_armed >= 0:
                res.exit_idx[i] = t; res.exit_price[i] = o
                res.exit_date[i] = dates[i, t]; res.exit_reason[i] = "emergency_stop"
                break

            # Entry-day target gating
            entry_day = (t == 0)
            target_check_allowed = (not entry_day) or allow_entry_day_target
            ambiguous_day = False
            if entry_day and ambiguous_day_skip_target:
                if hi >= tgt and c <= e * ambiguous_day_close_threshold:
                    ambiguous_day = True
                    target_check_allowed = False  # skip target on this day

            # Day-t > 0 open-gap checks
            if t > 0:
                if o >= tgt and target_check_allowed:
                    res.exit_idx[i] = t; res.exit_price[i] = o * (1.0 - target_fill_haircut)
                    res.exit_date[i] = dates[i, t]; res.exit_reason[i] = "target"
                    break

            # Adverse-first variant (F): if both target and intraday stop touched
            # same day, take the stop. This is independent of close-based emergency.
            if adverse_first_intraday and target_check_allowed:
                if lo <= adv_px and hi >= tgt:
                    res.exit_idx[i] = t; res.exit_price[i] = adv_px
                    res.exit_date[i] = dates[i, t]; res.exit_reason[i] = "adverse_first"
                    break

            # Intraday: target hit (subject to gating)
            if hi >= tgt and target_check_allowed:
                res.exit_idx[i] = t; res.exit_price[i] = fill
                res.exit_date[i] = dates[i, t]; res.exit_reason[i] = "target"
                break

            # Close-based emergency arming
            if c <= cstp:
                emergency_armed = t
        else:
            last_t = h - 1
            if last_t >= 0 and np.isfinite(closes[i, last_t]):
                res.exit_idx[i] = last_t
                res.exit_price[i] = closes[i, last_t]
                res.exit_date[i] = dates[i, last_t]
                res.exit_reason[i] = "max_hold"
    return res


def exit_target_emergency_hard(
    paths: dict,
    *,
    target_pct: float,
    emergency_stop_pct: float,
    hard_stop_pct: float,
    max_hold: int,
) -> ExitResult:
    """Sprint-5 exit: target + close-based emergency stop + intraday HARD stop.

    Hard stop catches blow-out crashes that the close-based emergency stop
    is too late for (Sprint 4 TOP went \$99 → \$16 intraday on day 0; close
    emergency armed, but exit at next open was \$16.68 = -83%; with a 20%
    intraday hard stop, exit at 80\% of entry = -20% gross).

    Tie rule: if target and hard stop both touched same day, **stop hits first**.
    Open-gap rule on day t>0: if open <= hard_stop, exit at open (gap-down).
                              if open >= target, exit at open (gap-up).
    """
    opens = paths["open"]; highs = paths["high"]; lows = paths["low"]
    closes = paths["close"]; dates = paths["dates"]
    n_paths, fh = closes.shape
    h = min(fh, max_hold)
    res = _result_arrays(n_paths)
    entry = opens[:, 0]
    target_px = entry * (1.0 + target_pct)
    hard_stop_px = entry * (1.0 - hard_stop_pct)
    close_stop_px = entry * (1.0 - emergency_stop_pct)

    for i in range(n_paths):
        e = entry[i]
        if not np.isfinite(e):
            continue
        tgt = target_px[i]; hstp = hard_stop_px[i]; cstp = close_stop_px[i]
        emergency_armed = -1
        for t in range(h):
            o = opens[i, t]; hi = highs[i, t]; lo = lows[i, t]; c = closes[i, t]
            if not np.isfinite(c):
                last_t = t - 1
                if last_t >= 0:
                    res.exit_idx[i] = last_t
                    res.exit_price[i] = closes[i, last_t]
                    res.exit_date[i] = dates[i, last_t]
                    res.exit_reason[i] = "no_exit"
                break

            # Emergency stop already armed (close hit yesterday) → exit at today's open
            if emergency_armed >= 0:
                res.exit_idx[i] = t; res.exit_price[i] = o
                res.exit_date[i] = dates[i, t]; res.exit_reason[i] = "emergency_stop"
                break

            # Day t > 0 gap checks (in priority: hard stop first, then target)
            if t > 0:
                if o <= hstp:
                    res.exit_idx[i] = t; res.exit_price[i] = o
                    res.exit_date[i] = dates[i, t]; res.exit_reason[i] = "hard_stop"
                    break
                if o >= tgt:
                    res.exit_idx[i] = t; res.exit_price[i] = o
                    res.exit_date[i] = dates[i, t]; res.exit_reason[i] = "target"
                    break

            # Intraday: stop hits before target (conservative tie rule)
            if lo <= hstp:
                res.exit_idx[i] = t; res.exit_price[i] = hstp
                res.exit_date[i] = dates[i, t]; res.exit_reason[i] = "hard_stop"
                break
            if hi >= tgt:
                res.exit_idx[i] = t; res.exit_price[i] = tgt
                res.exit_date[i] = dates[i, t]; res.exit_reason[i] = "target"
                break

            # Close-based emergency: arms but doesn't fire today
            if c <= cstp:
                emergency_armed = t
        else:
            last_t = h - 1
            if last_t >= 0 and np.isfinite(closes[i, last_t]):
                res.exit_idx[i] = last_t
                res.exit_price[i] = closes[i, last_t]
                res.exit_date[i] = dates[i, last_t]
                res.exit_reason[i] = "max_hold"
    return res


def exit_trailing_stop(
    paths: dict,
    *,
    trailing_pct: float,
    max_hold: int,
) -> ExitResult:
    closes = paths["close"]
    dates = paths["dates"]
    n_paths, fh = closes.shape
    h = min(fh, max_hold)
    res = _result_arrays(n_paths)

    for i in range(n_paths):
        peak = -np.inf
        for t in range(h):
            c = closes[i, t]
            if not np.isfinite(c):
                last_t = t - 1
                if last_t >= 0:
                    res.exit_idx[i] = last_t
                    res.exit_price[i] = closes[i, last_t]
                    res.exit_date[i] = dates[i, last_t]
                    res.exit_reason[i] = "no_exit"
                break
            peak = max(peak, c)
            if t > 0 and c <= peak * (1.0 - trailing_pct):
                res.exit_idx[i] = t; res.exit_price[i] = c
                res.exit_date[i] = dates[i, t]; res.exit_reason[i] = "trailing"
                break
        else:
            last_t = h - 1
            if last_t >= 0 and np.isfinite(closes[i, last_t]):
                res.exit_idx[i] = last_t
                res.exit_price[i] = closes[i, last_t]
                res.exit_date[i] = dates[i, last_t]
                res.exit_reason[i] = "max_hold"
    return res


# ────────────────────────────────────────────────────────────────────
# 5. Build the trade ledger
# ────────────────────────────────────────────────────────────────────


def liquidity_slippage(avg_dollar_volume: float) -> float:
    """Per-side slippage tier based on average dollar volume."""
    if not np.isfinite(avg_dollar_volume):
        return 0.015
    if avg_dollar_volume >= 100_000_000:
        return 0.0025
    if avg_dollar_volume >= 50_000_000:
        return 0.005
    if avg_dollar_volume >= 25_000_000:
        return 0.010
    return 0.015


def build_ledger(
    paths: dict,
    exit_result: ExitResult,
    *,
    slippage_per_side: float | str,
    score_col: str = "explosive_score",
    extra_cols: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    """Convert paths + exit_result into the trade ledger DataFrame."""
    cands = paths["candidates"].reset_index(drop=True)
    n = len(cands)

    # holding_days = exit_idx (0 = same-day, 1 = held one full session, etc.)
    holding_days = exit_result.exit_idx.astype(float)
    holding_days[exit_result.exit_idx < 0] = np.nan

    # max runup / drawdown over the realized holding window
    runup = np.full(n, np.nan)
    drawdown = np.full(n, np.nan)
    for i in range(n):
        ei = exit_result.exit_idx[i]
        if ei < 0:
            continue
        e = paths["open"][i, 0]
        if not np.isfinite(e) or e <= 0:
            continue
        hi = np.nanmax(paths["high"][i, : ei + 1])
        lo = np.nanmin(paths["low"][i, : ei + 1])
        runup[i] = hi / e - 1.0
        drawdown[i] = lo / e - 1.0

    entry_open = cands["entry_open"].to_numpy()
    exit_price = exit_result.exit_price
    gross = (exit_price - entry_open) / entry_open

    if isinstance(slippage_per_side, str):
        if slippage_per_side != "liquidity_adjusted":
            raise ValueError(slippage_per_side)
        slip = np.array([liquidity_slippage(dv) for dv in cands["entry_dollar_volume"].to_numpy()])
    else:
        slip = np.full(n, float(slippage_per_side))
    net = gross - 2.0 * slip

    out = pd.DataFrame({
        "symbol": cands["symbol"].to_numpy(),
        "signal_date": cands["date"].to_numpy(),
        "entry_date": cands["entry_date"].to_numpy(),
        "entry_price": entry_open,
        "exit_date": exit_result.exit_date,
        "exit_price": exit_price,
        "exit_reason": exit_result.exit_reason,
        "gross_return": gross,
        "net_return": net,
        "slippage_per_side": slip,
        "max_runup": runup,
        "max_drawdown": drawdown,
        "holding_days": holding_days,
        score_col: cands[score_col].to_numpy() if score_col in cands.columns else np.nan,
        "entry_dollar_volume": cands["entry_dollar_volume"].to_numpy(),
    })
    if extra_cols:
        for c in extra_cols:
            if c in cands.columns:
                out[c] = cands[c].to_numpy()
    return out.dropna(subset=["entry_date", "exit_date"]).reset_index(drop=True)


# ────────────────────────────────────────────────────────────────────
# 6. Trade-level metrics
# ────────────────────────────────────────────────────────────────────


def trade_metrics(ledger: pd.DataFrame, *, return_col: str = "net_return") -> dict:
    if ledger.empty:
        return {"n": 0}
    r = ledger[return_col].to_numpy()
    wins = r > 0
    avg_win = r[wins].mean() if wins.any() else 0.0
    avg_loss = r[~wins].mean() if (~wins).any() else 0.0
    gross_gain = r[wins].sum() if wins.any() else 0.0
    gross_loss = -r[~wins].sum() if (~wins).any() else 0.0
    profit_factor = gross_gain / gross_loss if gross_loss > 0 else np.inf
    return {
        "n": int(len(r)),
        "avg_return": float(np.nanmean(r)),
        "median_return": float(np.nanmedian(r)),
        "win_rate": float(wins.mean()),
        "avg_winner": float(avg_win),
        "avg_loser": float(avg_loss),
        "profit_factor": float(profit_factor),
        "pct_above_10": float((r >= 0.10).mean()),
        "pct_above_20": float((r >= 0.20).mean()),
        "pct_above_30": float((r >= 0.30).mean()),
        "worst_1pct": float(np.quantile(r, 0.01)),
        "worst_5pct": float(np.quantile(r, 0.05)),
        "max_loss": float(np.nanmin(r)),
        "max_gain": float(np.nanmax(r)),
        "avg_holding_days": float(ledger["holding_days"].mean()),
    }


# ────────────────────────────────────────────────────────────────────
# 7. Portfolio aggregation
# ────────────────────────────────────────────────────────────────────


def aggregate_portfolio(
    ledger: pd.DataFrame,
    bars: pd.DataFrame,
    *,
    max_positions: int = 10,
    return_col: str = "net_return",
) -> pd.DataFrame:
    """Construct an equal-weight, max-N-position daily portfolio from the
    trade ledger. Re-uses ledger-level returns and holding periods rather
    than recomputing intraday — the strategy compounds at the trade level
    when trades close, with cash earning 0 in between.

    Position-selection rule: on each entry_date, sort that day's ledger
    candidates by `score_col` descending; fill open slots up to
    `max_positions`. Symbols already in book are skipped.

    Returns a DataFrame with one row per trading day:
        date, n_positions, daily_return, equity, exposure
    """
    led = ledger.sort_values(["entry_date", "explosive_score"], ascending=[True, False]).reset_index(drop=True)
    cal_dates = pd.Series(np.sort(bars["date"].unique()))

    book: dict[str, dict] = {}  # symbol -> {entry_date, exit_date, net_return}
    rejected = 0
    accepted = 0
    rows = []
    led_by_entry = led.groupby("entry_date", sort=True)
    led_by_exit = led.groupby("exit_date", sort=True)

    for d in cal_dates:
        # Close trades exiting today
        if d in led_by_exit.groups:
            for sym in led_by_exit.get_group(d)["symbol"].unique():
                book.pop(sym, None)
        # Add new entries today (if slots free)
        if d in led_by_entry.groups:
            todays = led_by_entry.get_group(d)
            for _, row in todays.iterrows():
                if row["symbol"] in book:
                    continue
                if len(book) >= max_positions:
                    rejected += 1
                    continue
                book[row["symbol"]] = {
                    "entry_date": row["entry_date"],
                    "exit_date": row["exit_date"],
                    "net_return": row[return_col],
                }
                accepted += 1
        # Daily contribution: trades that EXITED today realize their return,
        # spread across the lifetime of the trade. We allocate equal-weight
        # per-trade and book the entire return on exit_date / hold_days.
        # Simpler equity model: book the trade's net return on exit_date as
        # a 1/max_positions weighted contribution.
        rows.append({
            "date": d,
            "n_positions": len(book),
            "exposure": len(book) / max_positions,
        })

    port = pd.DataFrame(rows)
    # Daily realized P&L: for every trade exiting on day d, contribution = (1/max_positions) * net_return
    if not led.empty:
        contrib = (
            led.groupby("exit_date")[return_col]
            .apply(lambda s: s.sum() / max_positions)
            .rename("realized_today")
        )
        port = port.merge(contrib.to_frame(), left_on="date", right_index=True, how="left")
        port["realized_today"] = port["realized_today"].fillna(0.0)
    else:
        port["realized_today"] = 0.0

    port["equity"] = (1.0 + port["realized_today"]).cumprod()
    port["accepted_trades"] = accepted
    port["rejected_for_capacity"] = rejected
    return port


def portfolio_metrics(port: pd.DataFrame) -> dict:
    if port.empty:
        return {}
    eq = port["equity"]
    days = (port["date"].iloc[-1] - port["date"].iloc[0]).days
    years = max(days / 365.25, 1e-9)
    cagr = eq.iloc[-1] ** (1 / years) - 1
    daily = port["realized_today"]
    sharpe = (daily.mean() / daily.std() * np.sqrt(252)) if daily.std() > 0 else float("nan")
    peak = eq.cummax()
    dd = eq / peak - 1.0
    return {
        "cagr": float(cagr),
        "total_return": float(eq.iloc[-1] - 1.0),
        "vol_ann": float(daily.std() * np.sqrt(252)) if daily.std() > 0 else 0.0,
        "sharpe": float(sharpe),
        "max_drawdown": float(dd.min()),
        "avg_exposure": float(port["exposure"].mean()),
        "avg_positions": float(port["n_positions"].mean()),
        "n_days": int(len(port)),
        "accepted_trades": int(port["accepted_trades"].iloc[-1]) if not port.empty else 0,
        "rejected_for_capacity": int(port["rejected_for_capacity"].iloc[-1]) if not port.empty else 0,
    }
