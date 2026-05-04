#!/usr/bin/env python3
"""
Single-position z-score matrix backtest (research; separate from production).

Decomposes the strategy into four explicit dimensions:
  F : entry filter            (z>=3 / z>=6 / z>=3 OR z<=-3 / z<=-3)
  S : selection rule          (highest |z| / closest to threshold / random)
  X : exit rule               (90d max / 20% trail + 90d / 20% trail + 45d)
  B : between-trade behavior  (cash / SPY)

Train: 2022-05-31 → 2024-12-31. Pick top-5 by Sharpe (Calmar / CAGR / MDD
tie-breakers, n_trades>=8, days_in_stock>=40%). Force-include best F4 and
best F2 cell if absent (preserves the explicit hypotheses tests).
F2 has a low-N exception: n_trades>=5 with low_N=true tag.

Run only those on OOS 2025-01-01 → 2026-04-29, plus matching S3
random-selection noise floors (20 deterministic seeds each).

Execution mirrors replay.py: signal at day D close → fill at D+1 open with
`cost_bps` slippage. Trailing stop tracks daily close vs. peak_close;
triggers fill at next open. End-of-window: force-close at last close
(no slippage; same convention for stock and SPY).

B2 SPY transitions are NOT free: 15bps applied on initial-entry, every
SPY-into-stock rotation (sell SPY + buy stock), and stock-back-to-SPY
rotation (sell stock + buy SPY). No daily SPY rebalance cost while
continuously holding SPY. The summary tracks SPY vs stock contributions
and per-leg cost so a B2 win can't be hidden behind free SPY exposure.

Implementation: parallel, resumable, per-cell timeout, atomic writes,
manifest CSV. Diagnostics only — does NOT modify replay.py.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import signal
import sys
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
DATA_DIR = HERE / "data"
RESULTS_DIR = HERE / "results"
DEFAULT_OUT = RESULTS_DIR / "single_position_zmatrix"


# ---------------------------------------------------------------------------
# Matrix definition
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class FilterDef:
    code: str
    name: str
    qualifies: Callable[[float], bool]
    tail_side: Callable[[float], str]


FILTERS: list[FilterDef] = [
    FilterDef("F1", "z>=3",
              qualifies=lambda z: z >= 3.0,
              tail_side=lambda z: "positive"),
    FilterDef("F2", "z>=6",
              qualifies=lambda z: z >= 6.0,
              tail_side=lambda z: "positive"),
    FilterDef("F3", "z>=3 OR z<=-3",
              qualifies=lambda z: (z >= 3.0) or (z <= -3.0),
              tail_side=lambda z: "positive" if z >= 3.0 else "negative"),
    FilterDef("F4", "z<=-3",
              qualifies=lambda z: z <= -3.0,
              tail_side=lambda z: "negative"),
]
FILTER_BY_CODE = {f.code: f for f in FILTERS}

SELECTION_RULES = ["S1", "S2", "S3"]   # highest |z|, closest to threshold, random

EXITS: dict[str, dict] = {
    "X1": {"trailing_pct": None, "max_hold_days": 90},   # max-hold only
    "X2": {"trailing_pct": 0.20, "max_hold_days": 90},   # 20% trail + 90d
    "X3": {"trailing_pct": 0.20, "max_hold_days": 45},   # 20% trail + 45d
}

IDLE = ["B1", "B2"]  # cash / SPY

INITIAL_CAPITAL = 100_000.0
COOLDOWN_DAYS_DEFAULT = 20
COST_BPS_DEFAULT = 15.0
RANDOM_SEEDS_DEFAULT = 20
MIN_TRADES_DEFAULT = 8
MIN_TRADES_F2 = 5
MIN_STOCK_PCT_DEFAULT = 0.40


# ---------------------------------------------------------------------------
# Loaders (mirror zscore_event_study.py / replay.py conventions)
# ---------------------------------------------------------------------------

def load_skew_z(skew_path: Path) -> pd.DataFrame:
    df = pd.read_parquet(skew_path, columns=["underlying", "date", "skew_5otm"])
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df = df.sort_values(["underlying", "date"]).reset_index(drop=True)
    grp = df.groupby("underlying")["skew_5otm"]
    rmean = grp.transform(lambda s: s.shift(1).rolling(60, min_periods=20).mean())
    rstd = grp.transform(lambda s: s.shift(1).rolling(60, min_periods=20).std())
    df["skew_z"] = (df["skew_5otm"] - rmean) / rstd
    df = df.dropna(subset=["skew_z"]).reset_index(drop=True)
    return df[["underlying", "date", "skew_z"]].rename(columns={"underlying": "ticker"})


def load_bars(stocks_path: Path) -> pd.DataFrame:
    cols = ["ticker", "date", "open", "high", "low", "close", "volume"]
    df = pd.read_parquet(stocks_path, columns=cols)
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)
    return df


def build_universe(bars_df: pd.DataFrame, top_n: int) -> set[str]:
    sys.path.insert(0, str(HERE))
    from massive_reference import allowed_ticker_set  # noqa
    allowed = allowed_ticker_set(require_type="CS",
                                 exclude_pharma_biotech=True,
                                 require_optionable=True)
    elig = bars_df[bars_df["ticker"].isin(allowed)].copy()
    elig["dv"] = elig["close"].astype(float) * elig["volume"].astype(float)
    med = elig.groupby("ticker")["dv"].median().dropna()
    med = med[med > 0]
    counts = bars_df.groupby("ticker").size()
    elig_tkrs = [t for t in med.index if counts.get(t, 0) >= 252]
    ranked = sorted(elig_tkrs, key=lambda t: med[t], reverse=True)
    return set(ranked[:top_n])


def compute_spy_regime_diag(bars_df: pd.DataFrame) -> dict:
    """{date: 'RISK_ON' | 'RISK_OFF'} from SPY 200d state machine.
    Diagnostic only — never gates entries."""
    sys.path.insert(0, str(HERE))
    from regime_filter import compute_spy_regime  # noqa
    spy = bars_df[bars_df["ticker"] == "SPY"][["date", "close"]].copy()
    if spy.empty:
        return {}
    return compute_spy_regime(spy)


def build_fast_bars(bars_lookup: dict[str, pd.DataFrame]) -> dict:
    """{ticker: {date: (open, high, low, close)}} for O(1) lookups."""
    out: dict = {}
    for tkr, b in bars_lookup.items():
        if b is None or b.empty:
            continue
        d = b["date"].dt.normalize().tolist()
        o = b["open"].astype(float).tolist()
        h = b["high"].astype(float).tolist()
        l = b["low"].astype(float).tolist()
        c = b["close"].astype(float).tolist()
        out[tkr] = {dt: (o[i], h[i], l[i], c[i]) for i, dt in enumerate(d)}
    return out


# ---------------------------------------------------------------------------
# Candidate event detection (per filter; cooldown per (ticker, filter))
# ---------------------------------------------------------------------------

def detect_filter_events(skew_df: pd.DataFrame, fdef: FilterDef,
                         cooldown_days: int) -> pd.DataFrame:
    """For one filter, emit one row each time a ticker enters the filter
    state (qualified=True today, qualified=False yesterday). Apply a per-ticker
    cooldown for that filter so a single continuous episode produces one event."""
    df = skew_df.copy()
    df["q"] = df["skew_z"].apply(fdef.qualifies)
    df["q_prev"] = df.groupby("ticker")["q"].shift(1).fillna(False)
    fresh = df[df["q"] & ~df["q_prev"]].copy()
    if fresh.empty:
        return pd.DataFrame(columns=["ticker", "event_date", "z", "tail_side", "filter_code"])

    df["row_idx"] = df.groupby("ticker").cumcount()
    idx_lookup = {(r.ticker, r.date): r.row_idx for r in df.itertuples(index=False)}

    rows = []
    last_idx: dict[str, int] = {}
    for r in fresh.sort_values(["ticker", "date"]).itertuples(index=False):
        cur = idx_lookup[(r.ticker, r.date)]
        prev = last_idx.get(r.ticker)
        if cooldown_days > 0 and prev is not None and (cur - prev) < cooldown_days:
            continue
        last_idx[r.ticker] = cur
        rows.append({
            "ticker": r.ticker,
            "event_date": r.date,
            "z": float(r.skew_z),
            "tail_side": fdef.tail_side(float(r.skew_z)),
            "filter_code": fdef.code,
        })
    return pd.DataFrame(rows)


def candidates_by_date(events: pd.DataFrame) -> dict:
    out: dict = {}
    if events.empty:
        return out
    for d, sub in events.groupby("event_date"):
        out[d] = sub.to_dict("records")
    return out


# ---------------------------------------------------------------------------
# Selection rules
# ---------------------------------------------------------------------------

def select_candidate(cands: list[dict], rule: str, fcode: str,
                     rng: np.random.Generator | None = None) -> dict:
    """Pick exactly one candidate. Caller guarantees `cands` non-empty.

    S1: highest abs(z)
    S2: closest to threshold (smallest margin past it)
        - F1 z>=3:  smallest z among z>=3
        - F2 z>=6:  smallest z among z>=6
        - F4 z<=-3: largest z among z<=-3 (closest to -3)
        - F3 z>=3 OR z<=-3: smallest |z|-3 across both tails
          NOTE: positive- and negative-tail margins are computed
          symmetrically (|z|-3), but cumulative event density is
          asymmetric so F3/S2 will skew toward whichever tail fires
          more often. The `tail_side` field on every trade lets us
          decompose the F3 result post-hoc; we did not add an extra knob.
    S3: random with rng (deterministic by seed)
    """
    if rule == "S1":
        return max(cands, key=lambda c: abs(c["z"]))
    if rule == "S2":
        if fcode == "F1":
            return min(cands, key=lambda c: c["z"])
        if fcode == "F2":
            return min(cands, key=lambda c: c["z"])
        if fcode == "F4":
            return max(cands, key=lambda c: c["z"])
        if fcode == "F3":
            return min(cands, key=lambda c: abs(c["z"]) - 3.0)
        raise ValueError(fcode)
    if rule == "S3":
        cs = sorted(cands, key=lambda c: c["ticker"])
        idx = int(rng.integers(0, len(cs))) if rng is not None else 0
        return cs[idx]
    raise ValueError(rule)


# ---------------------------------------------------------------------------
# Per-cell replay
# ---------------------------------------------------------------------------

@dataclass
class RunConfig:
    cell_id: str
    fcode: str
    scode: str
    xcode: str
    bcode: str
    seed: int | None
    trailing_pct: float | None
    max_hold_days: int
    cost_bps: float
    split: str = "train"  # 'train' or 'oos'


def run_cell(cfg: RunConfig,
             trading_days: list[pd.Timestamp],
             cands_by_date: dict,
             fast_bars: dict,
             spy_bars: pd.DataFrame | None,
             spy_regime: dict) -> dict:
    """Single-position state machine. Returns {summary, trades, equity}."""
    rng = np.random.default_rng(cfg.seed) if cfg.seed is not None else None
    cost = cfg.cost_bps / 10_000.0
    trail = cfg.trailing_pct
    spy_fast = fast_bars.get("SPY", {})

    cash = INITIAL_CAPITAL
    spy_shares = 0.0
    spy_entry_price: float | None = None    # last SPY buy fill (with cost)
    pos: dict | None = None
    pending_entry: dict | None = None
    pending_exit = False
    pending_exit_reason: str | None = None

    equity_path: list[dict] = []
    trades: list[dict] = []

    # Throughput counters
    n_total_candidate_events = 0
    n_candidate_days = 0
    n_events_while_flat = 0
    n_events_blocked = 0
    days_in_stock = 0
    days_in_spy = 0
    days_in_cash = 0

    # B2 cost / contribution accounting
    spy_buy_count = 0
    spy_sell_count = 0
    spy_buy_cost_total = 0.0
    spy_sell_cost_total = 0.0
    spy_realized_pnl = 0.0     # cumulative $ from SPY round-trips
    stock_realized_pnl = 0.0   # cumulative $ from stock round-trips
    stock_buy_cost_total = 0.0
    stock_sell_cost_total = 0.0

    def _bar(tkr, d):
        tb = fast_bars.get(tkr)
        if tb is None: return None
        return tb.get(d)

    def _spy(d):
        return spy_fast.get(d)

    def _do_buy_spy(d):
        nonlocal cash, spy_shares, spy_entry_price, spy_buy_count, spy_buy_cost_total
        sv = _spy(d)
        if sv is None or sv[0] <= 0 or cash <= 0:
            return
        nominal = cash
        cost_paid = nominal * cost / (1.0 + cost)  # cost embedded in fill
        # Equivalent: shares = cash / (open*(1+cost)); cost = nominal - shares*open
        fill = sv[0] * (1.0 + cost)
        shares = cash / fill
        spy_shares = shares
        spy_entry_price = fill
        cash = 0.0
        spy_buy_count += 1
        spy_buy_cost_total += cost_paid

    def _do_sell_spy(d, force_no_cost: bool = False):
        nonlocal cash, spy_shares, spy_entry_price, spy_sell_count, spy_sell_cost_total, spy_realized_pnl
        if spy_shares <= 0:
            return
        sv = _spy(d)
        if sv is None:
            return
        if force_no_cost:
            fill = sv[3]  # close, no slippage
            cost_paid = 0.0
        else:
            fill = sv[0] * (1.0 - cost)
            cost_paid = sv[0] * cost * spy_shares
        proceeds = spy_shares * fill
        if spy_entry_price is not None:
            spy_realized_pnl += (fill - spy_entry_price) * spy_shares
        cash += proceeds
        spy_shares = 0.0
        spy_entry_price = None
        spy_sell_count += 1
        spy_sell_cost_total += cost_paid

    # ------- B2: enter SPY at first day open with cost -------
    if cfg.bcode == "B2" and spy_fast:
        _do_buy_spy(trading_days[0])

    for i, today in enumerate(trading_days):
        is_last = (i == len(trading_days) - 1)

        # ---- 1. Process pending exit from prior day's signal: fill at today's open ----
        if pending_exit and pos is not None:
            row = _bar(pos["ticker"], today)
            if row is not None and row[0] > 0:
                fill = row[0] * (1.0 - cost)
                gross = pos["shares"] * row[0]
                stock_sell_cost_total += gross * cost
                proceeds = pos["shares"] * fill
                stock_realized_pnl += proceeds - pos["entry_price"] * pos["shares"]
                cash += proceeds
                trade_ret = fill / pos["entry_price"] - 1.0
                hold = (today - pos["entry_date"]).days
                trades.append(_make_trade_record(cfg, pos, fill, today, hold,
                                                 pending_exit_reason, trade_ret))
                pos = None
                pending_exit = False
                pending_exit_reason = None
                # Re-enter SPY at today's open (B2)
                if cfg.bcode == "B2":
                    _do_buy_spy(today)

        # ---- 2. Process pending entry: fill at today's open ----
        if pending_entry is not None and pos is None:
            tkr = pending_entry["ticker"]
            row = _bar(tkr, today)
            if row is not None and row[0] > 0:
                # Liquidate SPY first (B2)
                if cfg.bcode == "B2":
                    _do_sell_spy(today)
                fill = row[0] * (1.0 + cost)
                stock_buy_cost_total += cash * cost / (1.0 + cost)
                shares = cash / fill
                cash = 0.0
                pos = {
                    "ticker": tkr,
                    "shares": shares,
                    "entry_price": fill,
                    "entry_date": today,
                    "peak_close": row[3],
                    "last_close": row[3],
                    "entry_z": pending_entry["z"],
                    "tail_side": pending_entry["tail_side"],
                    "n_candidates_same_day": pending_entry["n_candidates_same_day"],
                    "rank_by_abs_z": pending_entry["rank_by_abs_z"],
                    "rank_by_boundary": pending_entry["rank_by_boundary"],
                    "spy_regime_at_entry": spy_regime.get(today, "RISK_ON") == "RISK_ON",
                    "mfe_running": row[3] / fill - 1.0,
                    "mae_running": row[3] / fill - 1.0,
                    "days_to_mfe": 0,
                }
            pending_entry = None

        # ---- 3. Mark equity to today's close + update peak / MFE / MAE ----
        if pos is not None:
            row = _bar(pos["ticker"], today)
            if row is not None:
                c = row[3]
                pos["last_close"] = c
                if c > pos["peak_close"]:
                    pos["peak_close"] = c
                ret = c / pos["entry_price"] - 1.0
                if ret > pos["mfe_running"]:
                    pos["mfe_running"] = ret
                    pos["days_to_mfe"] = (today - pos["entry_date"]).days
                if ret < pos["mae_running"]:
                    pos["mae_running"] = ret
            value = cash + pos["shares"] * pos["last_close"]
            mode = "stock"
            days_in_stock += 1
        elif cfg.bcode == "B2" and spy_shares > 0:
            sv = _spy(today)
            sclose = sv[3] if sv is not None else 0.0
            value = cash + spy_shares * sclose
            mode = "spy"
            days_in_spy += 1
        else:
            value = cash
            mode = "cash"
            days_in_cash += 1
        equity_path.append({"date": today.date().isoformat(), "equity": value, "mode": mode})

        # ---- 4. Exit signals (if pos) → fill next day open ----
        if pos is not None and not pending_exit:
            hd = (today - pos["entry_date"]).days
            reason: str | None = None
            if trail is not None:
                if pos["last_close"] <= pos["peak_close"] * (1.0 - trail):
                    reason = f"TRAILING_STOP_{int(trail*100)}PCT"
            if reason is None and hd >= cfg.max_hold_days:
                reason = f"MAX_HOLD_{cfg.max_hold_days}D"
            if reason is not None:
                pending_exit = True
                pending_exit_reason = reason

        # ---- 5. Candidate signals from today's close (only if flat / no pending action) ----
        cands = cands_by_date.get(today, [])
        if cands:
            n_total_candidate_events += len(cands)
            n_candidate_days += 1
        if cands:
            if pos is None and not pending_exit and pending_entry is None:
                n_events_while_flat += len(cands)
                ranked_abs = sorted(cands, key=lambda c: -abs(c["z"]))

                def boundary_margin(c):
                    if cfg.fcode == "F1":
                        return c["z"] - 3.0
                    if cfg.fcode == "F2":
                        return c["z"] - 6.0
                    if cfg.fcode == "F4":
                        return abs(c["z"]) - 3.0
                    return abs(c["z"]) - 3.0  # F3

                ranked_boundary = sorted(cands, key=boundary_margin)
                pick = select_candidate(cands, cfg.scode, cfg.fcode, rng=rng)
                pending_entry = dict(pick)
                pending_entry["n_candidates_same_day"] = len(cands)
                pending_entry["rank_by_abs_z"] = ranked_abs.index(pick) + 1
                pending_entry["rank_by_boundary"] = ranked_boundary.index(pick) + 1
            else:
                n_events_blocked += len(cands)

        # ---- 6. End-of-window force-close (no slippage) at last close ----
        if is_last and pos is not None:
            fill = pos["last_close"]
            proceeds = pos["shares"] * fill
            stock_realized_pnl += proceeds - pos["entry_price"] * pos["shares"]
            cash += proceeds
            trade_ret = fill / pos["entry_price"] - 1.0
            hold = (today - pos["entry_date"]).days
            trades.append(_make_trade_record(cfg, pos, fill, today, hold,
                                             "END_OF_WINDOW", trade_ret))
            pos = None

    # End of window: liquidate SPY too (B2) at last close (no slippage; same as stock EOW)
    if cfg.bcode == "B2" and spy_shares > 0:
        _do_sell_spy(trading_days[-1], force_no_cost=True)
        if equity_path:
            equity_path[-1]["equity"] = cash

    eq_df = pd.DataFrame(equity_path)
    trades_df = pd.DataFrame(trades)
    summary = compute_summary(
        cfg, eq_df, trades_df, trading_days,
        n_total_candidate_events, n_candidate_days,
        n_events_while_flat, n_events_blocked,
        days_in_stock, days_in_spy, days_in_cash,
        spy_bars,
        spy_buy_count, spy_sell_count,
        spy_buy_cost_total, spy_sell_cost_total,
        spy_realized_pnl, stock_realized_pnl,
        stock_buy_cost_total, stock_sell_cost_total,
    )
    return {"summary": summary, "trades": trades_df, "equity": eq_df}


def _make_trade_record(cfg, pos, fill, today, hold, reason, trade_ret):
    mfe = pos["mfe_running"]
    return {
        "cell_id": cfg.cell_id,
        "ticker": pos["ticker"],
        "entry_date": pos["entry_date"].date().isoformat(),
        "exit_date": today.date().isoformat(),
        "entry_z": pos["entry_z"],
        "tail_side": pos["tail_side"],
        "entry_filter": cfg.fcode,
        "selection_rule": cfg.scode,
        "exit_rule": cfg.xcode,
        "idle_behavior": cfg.bcode,
        "entry_price": round(pos["entry_price"], 4),
        "exit_price": round(fill, 4),
        "trade_return": trade_ret,
        "MFE": mfe,
        "MAE": pos["mae_running"],
        "giveback_ratio": (mfe - trade_ret) / mfe if mfe > 0 else None,
        "days_to_MFE": pos["days_to_mfe"],
        "hold_days": hold,
        "exit_reason": reason,
        "SPY_regime_on_at_entry": pos["spy_regime_at_entry"],
        "n_candidates_same_day": pos["n_candidates_same_day"],
        "selected_candidate_rank_by_abs_z": pos["rank_by_abs_z"],
        "selected_candidate_rank_by_boundary": pos["rank_by_boundary"],
    }


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

def compute_summary(cfg, eq, trades, trading_days,
                    n_total, n_days, n_flat, n_blocked,
                    days_stock, days_spy, days_cash,
                    spy_bars,
                    spy_buy_count, spy_sell_count,
                    spy_buy_cost_total, spy_sell_cost_total,
                    spy_realized_pnl, stock_realized_pnl,
                    stock_buy_cost_total, stock_sell_cost_total) -> dict:
    n_days_total = len(trading_days)
    if eq.empty or n_days_total == 0:
        return {"cell_id": cfg.cell_id, "error": "empty"}
    eq_vals = eq["equity"].astype(float).to_numpy()
    rets = pd.Series(eq_vals).pct_change().fillna(0.0).to_numpy()
    total_return = eq_vals[-1] / eq_vals[0] - 1.0
    years = n_days_total / 252.0
    cagr = (eq_vals[-1] / eq_vals[0]) ** (1.0 / max(years, 1e-9)) - 1.0
    sharpe = (rets.mean() / rets.std() * np.sqrt(252.0)) if rets.std() > 0 else 0.0
    peak = np.maximum.accumulate(eq_vals)
    dd = eq_vals / peak - 1.0
    mdd = float(np.min(dd))
    calmar = cagr / abs(mdd) if mdd < 0 else np.nan

    eq_for_month = eq.copy()
    eq_for_month["date"] = pd.to_datetime(eq_for_month["date"])
    monthly = eq_for_month.set_index("date")["equity"].resample("ME").last().pct_change().dropna()
    worst_m = float(monthly.min()) if not monthly.empty else np.nan
    best_m = float(monthly.max()) if not monthly.empty else np.nan

    spy_cagr = spy_sharpe = spy_mdd = spy_total = np.nan
    active_ret = excess_cagr = active_sharpe = np.nan
    if spy_bars is not None and not spy_bars.empty:
        ss = spy_bars[(spy_bars["date"] >= trading_days[0])
                      & (spy_bars["date"] <= trading_days[-1])].copy()
        if len(ss) >= 2:
            spy_close = ss["close"].astype(float).to_numpy()
            spy_total = spy_close[-1] / spy_close[0] - 1.0
            spy_cagr = (spy_close[-1] / spy_close[0]) ** (1.0 / max(years, 1e-9)) - 1.0
            spy_rets = pd.Series(spy_close).pct_change().fillna(0.0).to_numpy()
            spy_sharpe = (spy_rets.mean() / spy_rets.std() * np.sqrt(252.0)) if spy_rets.std() > 0 else 0.0
            spy_peak = np.maximum.accumulate(spy_close)
            spy_mdd = float(np.min(spy_close / spy_peak - 1.0))
            excess_cagr = cagr - spy_cagr
            active_ret = total_return - spy_total
            # Active Sharpe: align rets to spy_rets length (clip equity to bench length)
            if len(rets) == len(spy_rets):
                excess_daily = rets - spy_rets
                if excess_daily.std() > 0:
                    active_sharpe = excess_daily.mean() / excess_daily.std() * np.sqrt(252.0)

    if trades.empty:
        avg_trade = med_trade = win_rate = avg_w = avg_l = avg_mfe = avg_mae = avg_gb = avg_dtm = med_dtm = np.nan
        avg_hold = med_hold = np.nan
        n_pos_tail = n_neg_tail = 0
        pnl_pos = pnl_neg = avg_ret_pos = avg_ret_neg = np.nan
    else:
        avg_trade = float(trades["trade_return"].mean())
        med_trade = float(trades["trade_return"].median())
        win_rate = float((trades["trade_return"] > 0).mean())
        wins = trades.loc[trades["trade_return"] > 0, "trade_return"]
        losses = trades.loc[trades["trade_return"] <= 0, "trade_return"]
        avg_w = float(wins.mean()) if not wins.empty else np.nan
        avg_l = float(losses.mean()) if not losses.empty else np.nan
        avg_mfe = float(trades["MFE"].mean())
        avg_mae = float(trades["MAE"].mean())
        gb = trades["giveback_ratio"].dropna()
        avg_gb = float(gb.mean()) if not gb.empty else np.nan
        avg_dtm = float(trades["days_to_MFE"].mean())
        med_dtm = float(trades["days_to_MFE"].median())
        avg_hold = float(trades["hold_days"].mean())
        med_hold = float(trades["hold_days"].median())
        pos = trades[trades["tail_side"] == "positive"]
        neg = trades[trades["tail_side"] == "negative"]
        n_pos_tail = int(len(pos))
        n_neg_tail = int(len(neg))
        pnl_pos = float((1.0 + pos["trade_return"]).prod() - 1.0) if not pos.empty else np.nan
        pnl_neg = float((1.0 + neg["trade_return"]).prod() - 1.0) if not neg.empty else np.nan
        avg_ret_pos = float(pos["trade_return"].mean()) if not pos.empty else np.nan
        avg_ret_neg = float(neg["trade_return"].mean()) if not neg.empty else np.nan

    return {
        "cell_id": cfg.cell_id,
        "split": cfg.split,
        "entry_filter": cfg.fcode,
        "selection_rule": cfg.scode,
        "exit_rule": cfg.xcode,
        "idle_behavior": cfg.bcode,
        "seed": cfg.seed,
        "n_trades": int(len(trades)),
        "n_total_candidate_events": int(n_total),
        "n_events_while_flat": int(n_flat),
        "n_events_blocked_while_in_position": int(n_blocked),
        "pct_candidate_events_taken": (len(trades) / n_total) if n_total else np.nan,
        "pct_days_in_stock": days_stock / n_days_total,
        "pct_days_in_cash": days_cash / n_days_total,
        "pct_days_in_spy": days_spy / n_days_total,
        "CAGR": cagr,
        "total_return": total_return,
        "Sharpe": sharpe,
        "max_drawdown": mdd,
        "Calmar": calmar,
        "worst_month": worst_m,
        "best_month": best_m,
        "avg_trade_return": avg_trade,
        "median_trade_return": med_trade,
        "win_rate": win_rate,
        "avg_winner": avg_w,
        "avg_loser": avg_l,
        "avg_trade_MFE": avg_mfe,
        "avg_trade_MAE": avg_mae,
        "avg_giveback_ratio": avg_gb,
        "avg_days_to_MFE": avg_dtm,
        "median_days_to_MFE": med_dtm,
        "avg_hold_days": avg_hold,
        "median_hold_days": med_hold,
        "SPY_total_return_same_period": spy_total,
        "SPY_CAGR_same_period": spy_cagr,
        "SPY_Sharpe_same_period": spy_sharpe,
        "SPY_MDD_same_period": spy_mdd,
        "excess_CAGR_vs_SPY": excess_cagr,
        "active_return_vs_SPY": active_ret,
        "active_Sharpe_vs_SPY": active_sharpe,
        # Tail decomposition (mostly relevant for F3)
        "trades_from_positive_tail": n_pos_tail,
        "trades_from_negative_tail": n_neg_tail,
        "pnl_from_positive_tail": pnl_pos,
        "pnl_from_negative_tail": pnl_neg,
        "avg_return_positive_tail": avg_ret_pos,
        "avg_return_negative_tail": avg_ret_neg,
        # B2 cost / contribution accounting
        "spy_buy_count": int(spy_buy_count),
        "spy_sell_count": int(spy_sell_count),
        "spy_buy_cost_total_$": spy_buy_cost_total,
        "spy_sell_cost_total_$": spy_sell_cost_total,
        "spy_realized_pnl_$": spy_realized_pnl,
        "stock_realized_pnl_$": stock_realized_pnl,
        "stock_buy_cost_total_$": stock_buy_cost_total,
        "stock_sell_cost_total_$": stock_sell_cost_total,
    }


# ---------------------------------------------------------------------------
# Cell list / selection
# ---------------------------------------------------------------------------

def make_cells_logical(split: str = "train") -> list[RunConfig]:
    """All 72 logical cells (S3 has seed=None placeholder; expand for actual runs)."""
    cells = []
    for f in FILTERS:
        for s in SELECTION_RULES:
            for x, exit_cfg in EXITS.items():
                for b in IDLE:
                    cells.append(RunConfig(
                        cell_id=f"{f.code}_{s}_{x}_{b}",
                        fcode=f.code, scode=s, xcode=x, bcode=b,
                        seed=None,
                        trailing_pct=exit_cfg["trailing_pct"],
                        max_hold_days=exit_cfg["max_hold_days"],
                        cost_bps=COST_BPS_DEFAULT,
                        split=split,
                    ))
    return cells


def expand_actual_runs(cells: list[RunConfig], n_seeds: int) -> list[RunConfig]:
    """Expand S3 cells into n_seeds deterministic runs each."""
    out = []
    for c in cells:
        if c.scode == "S3":
            for k in range(n_seeds):
                out.append(RunConfig(
                    cell_id=f"{c.cell_id}_seed{k}",
                    fcode=c.fcode, scode=c.scode, xcode=c.xcode, bcode=c.bcode,
                    seed=k,
                    trailing_pct=c.trailing_pct,
                    max_hold_days=c.max_hold_days,
                    cost_bps=c.cost_bps,
                    split=c.split,
                ))
        else:
            out.append(c)
    return out


def select_top_train(train_summaries: list[dict],
                     min_trades: int = MIN_TRADES_DEFAULT,
                     min_trades_f2: int = MIN_TRADES_F2,
                     min_stock_pct: float = MIN_STOCK_PCT_DEFAULT,
                     top_n: int = 5) -> tuple[list[str], dict]:
    """Returns (selected_cell_ids, eligibility_metadata). Considers S1 and S2
    only for top-N (S3 cells are noise-floor estimators, not strategy
    candidates). Force-includes best F2 and F4 cells if absent.
    """
    df = pd.DataFrame(train_summaries)
    if df.empty:
        return [], {}

    is_s12 = df["selection_rule"].isin(["S1", "S2"])
    is_f2 = df["entry_filter"] == "F2"

    # F2 eligibility: relaxed n_trades floor + low_N tag
    df["min_trades_required"] = np.where(is_f2, min_trades_f2, min_trades)
    df["meets_trades"] = df["n_trades"] >= df["min_trades_required"]
    df["meets_stock_pct"] = df["pct_days_in_stock"] >= min_stock_pct
    df["low_N"] = is_f2 & (df["n_trades"] < min_trades)
    df["eligible"] = is_s12 & df["meets_trades"] & df["meets_stock_pct"]

    eligible = df[df["eligible"]].copy()
    if eligible.empty:
        # Fall back to S1/S2 set ignoring stock_pct floor; never select S3
        eligible = df[is_s12 & df["meets_trades"]].copy()

    sorted_df = eligible.sort_values(
        ["Sharpe", "Calmar", "CAGR", "max_drawdown"],
        ascending=[False, False, False, False],
    )
    selected = sorted_df.head(top_n)["cell_id"].tolist()

    # Force-include best F4 (negative-tail) and best F2 (high-z) cells
    metadata = {"low_N_cells": [], "force_included": []}
    for forced in ("F4", "F2"):
        already = any(c.startswith(f"{forced}_") for c in selected)
        if already:
            continue
        # Prefer eligible candidates first
        forced_eligible = sorted_df[sorted_df["entry_filter"] == forced]
        if not forced_eligible.empty:
            cid = forced_eligible.iloc[0]["cell_id"]
            selected.append(cid)
            metadata["force_included"].append({"filter": forced, "cell_id": cid,
                                               "low_N": bool(forced_eligible.iloc[0].get("low_N", False))})
            continue
        # Fall back to ANY S1/S2 cell of that filter (may be insufficient sample)
        any_forced = df[is_s12 & (df["entry_filter"] == forced)].sort_values(
            ["Sharpe", "Calmar", "CAGR"], ascending=[False, False, False])
        if not any_forced.empty:
            cid = any_forced.iloc[0]["cell_id"]
            selected.append(cid)
            metadata["force_included"].append({"filter": forced, "cell_id": cid,
                                               "low_N": True,
                                               "insufficient_sample": True})

    metadata["low_N_cells"] = df[df["low_N"]]["cell_id"].tolist()
    return selected, metadata


# ---------------------------------------------------------------------------
# Atomic writes / manifest
# ---------------------------------------------------------------------------

def write_atomic(path: Path, content: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content)
    os.replace(tmp, path)


def write_atomic_csv(path: Path, df: pd.DataFrame) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    df.to_csv(tmp, index=False)
    os.replace(tmp, path)


MANIFEST_COLS = [
    "cell_id", "split", "entry_filter", "selection_rule", "exit_rule",
    "idle_behavior", "seed", "status", "started_at", "finished_at",
    "runtime_seconds", "output_dir", "error_message",
]


def load_manifest(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=MANIFEST_COLS)
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def save_manifest(path: Path, df: pd.DataFrame) -> None:
    write_atomic_csv(path, df[MANIFEST_COLS])


def manifest_init_row(cfg: RunConfig, cell_dir: Path) -> dict:
    return {
        "cell_id": cfg.cell_id,
        "split": cfg.split,
        "entry_filter": cfg.fcode,
        "selection_rule": cfg.scode,
        "exit_rule": cfg.xcode,
        "idle_behavior": cfg.bcode,
        "seed": "" if cfg.seed is None else str(cfg.seed),
        "status": "PENDING",
        "started_at": "",
        "finished_at": "",
        "runtime_seconds": "",
        "output_dir": str(cell_dir),
        "error_message": "",
    }


# ---------------------------------------------------------------------------
# Worker pool
# ---------------------------------------------------------------------------

# Globals populated by initializer (fork inherits at no cost; spawn re-runs init).
_W: dict = {}


def _worker_init(skew_path: str, stocks_path: str, universe_top_n: int,
                 cooldown_days: int):
    """Load all data once per worker and stash on module-global."""
    skew_df = load_skew_z(Path(skew_path))
    bars_df = load_bars(Path(stocks_path))
    universe = build_universe(bars_df, universe_top_n)
    keep = universe | {"SPY"}
    bars_df = bars_df[bars_df["ticker"].isin(keep)].reset_index(drop=True)
    skew_df = skew_df[skew_df["ticker"].isin(universe)].reset_index(drop=True)
    bars_lookup = {t: g.reset_index(drop=True) for t, g in bars_df.groupby("ticker")}
    fast_bars = build_fast_bars(bars_lookup)
    spy_bars = bars_lookup.get("SPY")
    spy_regime = compute_spy_regime_diag(bars_df)
    events_by_filter: dict = {}
    for f in FILTERS:
        ev = detect_filter_events(skew_df, f, cooldown_days)
        events_by_filter[f.code] = candidates_by_date(ev)
    spy_dates = sorted(spy_bars["date"].dt.normalize().unique().tolist())
    _W["fast_bars"] = fast_bars
    _W["spy_bars"] = spy_bars
    _W["spy_regime"] = spy_regime
    _W["events_by_filter"] = events_by_filter
    _W["spy_dates"] = spy_dates


def _worker_run_cell(payload: dict) -> dict:
    """Worker entry point. payload: {cfg_dict, cell_dir, train_window, oos_window,
    cell_timeout_seconds, force}.
    """
    cfg_dict = payload["cfg_dict"]
    cfg_dict["seed"] = (None if cfg_dict.get("seed") in (None, "") else int(cfg_dict["seed"]))
    cfg_dict["trailing_pct"] = (None if cfg_dict.get("trailing_pct") is None
                                else float(cfg_dict["trailing_pct"]))
    cfg_dict["max_hold_days"] = int(cfg_dict["max_hold_days"])
    cfg_dict["cost_bps"] = float(cfg_dict["cost_bps"])
    cfg = RunConfig(**cfg_dict)
    cell_dir = Path(payload["cell_dir"])
    summary_path = cell_dir / "summary.json"

    if summary_path.exists() and not payload.get("force"):
        try:
            cached = json.loads(summary_path.read_text())
            return {"status": "DONE_CACHED", "cell_id": cfg.cell_id,
                    "summary": cached, "runtime": 0.0}
        except Exception:
            pass

    timeout = int(payload.get("cell_timeout_seconds", 1800))

    def _alarm_handler(signum, frame):
        raise TimeoutError(f"cell exceeded {timeout}s")

    if timeout > 0:
        try:
            signal.signal(signal.SIGALRM, _alarm_handler)
            signal.alarm(timeout)
        except (AttributeError, ValueError):
            pass

    started = time.time()
    try:
        spy_dates = _W["spy_dates"]
        if cfg.split == "train":
            window = (pd.Timestamp(payload["train_window"][0]),
                      pd.Timestamp(payload["train_window"][1]))
        else:
            window = (pd.Timestamp(payload["oos_window"][0]),
                      pd.Timestamp(payload["oos_window"][1]))
        trading_days = [d for d in spy_dates if window[0] <= d <= window[1]]

        cands = _W["events_by_filter"][cfg.fcode]
        res = run_cell(cfg, trading_days, cands,
                       _W["fast_bars"], _W["spy_bars"], _W["spy_regime"])

        cell_dir.mkdir(parents=True, exist_ok=True)
        write_atomic(summary_path,
                     json.dumps(res["summary"], default=_json_default, indent=2))
        if not res["trades"].empty:
            write_atomic_csv(cell_dir / "trade_log.csv", res["trades"])
        if not res["equity"].empty:
            write_atomic_csv(cell_dir / "daily_equity.csv", res["equity"])
        write_atomic(cell_dir / "config.json",
                     json.dumps(asdict(cfg), default=_json_default, indent=2))

        try:
            signal.alarm(0)
        except Exception:
            pass

        return {"status": "DONE", "cell_id": cfg.cell_id,
                "summary": res["summary"], "runtime": time.time() - started}
    except TimeoutError as e:
        return {"status": "TIMED_OUT", "cell_id": cfg.cell_id,
                "error": str(e), "runtime": time.time() - started}
    except Exception as e:
        cell_dir.mkdir(parents=True, exist_ok=True)
        (cell_dir / "stderr_or_error.txt").write_text(traceback.format_exc())
        return {"status": "FAILED", "cell_id": cfg.cell_id,
                "error": str(e), "runtime": time.time() - started}


def _json_default(o):
    if isinstance(o, (np.integer, np.int64, np.int32)):
        return int(o)
    if isinstance(o, (np.floating, np.float64, np.float32)):
        v = float(o)
        return v if np.isfinite(v) else None
    if isinstance(o, np.ndarray):
        return o.tolist()
    if isinstance(o, pd.Timestamp):
        return o.isoformat()
    return str(o)


# ---------------------------------------------------------------------------
# Heartbeat / orchestration
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def run_batch(cells: list[RunConfig], out_dir: Path,
              train_window: tuple[str, str], oos_window: tuple[str, str],
              workers: int, cell_timeout_seconds: int, heartbeat_seconds: int,
              skew_path: Path, stocks_path: Path, universe_top_n: int,
              cooldown_days: int, force: bool, retry_failed: bool,
              fail_fast: bool, manifest_path: Path,
              partial_csv_path: Path) -> list[dict]:
    """Run cells in parallel with manifest checkpointing. Returns list of summaries."""
    cells_dir = out_dir / "cells"
    cells_dir.mkdir(parents=True, exist_ok=True)

    # Initialize / merge manifest
    manifest = load_manifest(manifest_path)
    seen = set(manifest["cell_id"].tolist()) if not manifest.empty else set()
    rows_new = []
    for c in cells:
        cd = cells_dir / c.split / c.cell_id
        if c.cell_id in seen:
            continue
        rows_new.append(manifest_init_row(c, cd))
    if rows_new:
        manifest = pd.concat([manifest, pd.DataFrame(rows_new)], ignore_index=True)

    # Reset RUNNING (likely from interrupted prior run) → PENDING
    if not manifest.empty:
        running_mask = manifest["status"] == "RUNNING"
        if running_mask.any():
            manifest.loc[running_mask, "status"] = "PENDING"

    # Decide which cells to actually run
    runnable: list[RunConfig] = []
    completed_cached: list[dict] = []
    cell_index = {c.cell_id: c for c in cells}
    for _, row in manifest.iterrows():
        cid = row["cell_id"]
        cfg = cell_index.get(cid)
        if cfg is None:
            continue
        cd = cells_dir / cfg.split / cfg.cell_id
        summary_p = cd / "summary.json"
        status = row["status"]
        if force:
            runnable.append(cfg)
            continue
        if status == "DONE" and summary_p.exists():
            try:
                completed_cached.append(json.loads(summary_p.read_text()))
                continue
            except Exception:
                runnable.append(cfg)
                continue
        if status in ("FAILED", "TIMED_OUT") and not retry_failed:
            continue
        runnable.append(cfg)

    save_manifest(manifest_path, manifest)

    if not runnable:
        print(f"[zmatrix] all {len(cells)} cells already complete (resume).", file=sys.stderr)
        return completed_cached

    print(f"[zmatrix] {len(runnable)} cells to run "
          f"({len(completed_cached)} already cached) | workers={workers} "
          f"| per-cell timeout={cell_timeout_seconds}s", file=sys.stderr)

    # Build payloads
    payloads = []
    for cfg in runnable:
        cd = cells_dir / cfg.split / cfg.cell_id
        payloads.append({
            "cfg_dict": asdict(cfg),
            "cell_dir": str(cd),
            "train_window": list(train_window),
            "oos_window": list(oos_window),
            "cell_timeout_seconds": cell_timeout_seconds,
            "force": force,
        })

    started_overall = time.time()
    results: list[dict] = list(completed_cached)
    last_heartbeat = time.time()
    completed = len(completed_cached)
    failed = 0
    timed_out = 0
    partial_summaries: list[dict] = list(completed_cached)

    def _save_partial():
        if partial_summaries:
            try:
                write_atomic_csv(partial_csv_path, pd.DataFrame(partial_summaries))
            except Exception:
                pass

    def _heartbeat(latest_id: str | None):
        elapsed = int(time.time() - started_overall)
        msg = (f"[zmatrix] heartbeat: completed {completed}/{len(cells)} "
               f"| failed {failed} | timed_out {timed_out} "
               f"| elapsed {elapsed//60}m{elapsed%60:02d}s")
        if latest_id:
            msg += f" | latest {latest_id}"
        print(msg, flush=True, file=sys.stderr)

    # Use 'fork' on Linux to share data cheaply (workers inherit globals)
    import multiprocessing as mp
    ctx = mp.get_context("fork") if sys.platform != "win32" else mp.get_context("spawn")

    with ProcessPoolExecutor(
        max_workers=workers,
        mp_context=ctx,
        initializer=_worker_init,
        initargs=(str(skew_path), str(stocks_path), universe_top_n, cooldown_days),
    ) as ex:
        # Mark as RUNNING in manifest before submission
        for cfg in runnable:
            mask = manifest["cell_id"] == cfg.cell_id
            manifest.loc[mask, "status"] = "RUNNING"
            manifest.loc[mask, "started_at"] = _now_iso()
        save_manifest(manifest_path, manifest)

        futures = {ex.submit(_worker_run_cell, p): p for p in payloads}
        try:
            for fut in as_completed(futures):
                p = futures[fut]
                cid = p["cfg_dict"]["cell_id"]
                try:
                    res = fut.result()
                except Exception as e:
                    res = {"status": "FAILED", "cell_id": cid,
                           "error": f"future raised: {e}", "runtime": 0.0}
                status = res["status"]
                runtime = float(res.get("runtime", 0.0))
                mask = manifest["cell_id"] == cid
                manifest.loc[mask, "status"] = ("DONE" if status in ("DONE", "DONE_CACHED")
                                                else status)
                manifest.loc[mask, "finished_at"] = _now_iso()
                manifest.loc[mask, "runtime_seconds"] = f"{runtime:.2f}"
                manifest.loc[mask, "error_message"] = res.get("error", "")
                if status in ("DONE", "DONE_CACHED"):
                    summary = res.get("summary")
                    if summary:
                        results.append(summary)
                        partial_summaries.append(summary)
                elif status == "FAILED":
                    failed += 1
                elif status == "TIMED_OUT":
                    timed_out += 1
                completed += 1
                save_manifest(manifest_path, manifest)
                _save_partial()

                if time.time() - last_heartbeat >= heartbeat_seconds:
                    _heartbeat(cid)
                    last_heartbeat = time.time()
                else:
                    # Concise per-cell line
                    print(f"[zmatrix] [{completed}/{len(cells)}] {status} "
                          f"{cid} ({runtime:.1f}s)", flush=True, file=sys.stderr)

                if fail_fast and status in ("FAILED", "TIMED_OUT"):
                    print(f"[zmatrix] --fail-fast: aborting after {cid} {status}",
                          file=sys.stderr)
                    for f2 in futures:
                        f2.cancel()
                    break
        except KeyboardInterrupt:
            print("[zmatrix] KeyboardInterrupt — saving manifest and exiting",
                  file=sys.stderr)
            save_manifest(manifest_path, manifest)
            raise

    _heartbeat(None)
    print(f"[zmatrix] batch done in {int(time.time()-started_overall)}s "
          f"(completed={completed} failed={failed} timed_out={timed_out})",
          file=sys.stderr)
    return results


# ---------------------------------------------------------------------------
# Pre-compute event table (single pass, persisted)
# ---------------------------------------------------------------------------

def precompute_events(out_dir: Path, skew_path: Path, stocks_path: Path,
                      universe_top_n: int, cooldown_days: int) -> Path:
    """Build and persist precomputed_events.parquet (diagnostic; the worker
    independently rebuilds the same dict from the same inputs at init time)."""
    events_path = out_dir / "precomputed_events.parquet"
    print(f"[zmatrix] precomputing events → {events_path}", file=sys.stderr)
    skew_df = load_skew_z(skew_path)
    bars_df = load_bars(stocks_path)
    universe = build_universe(bars_df, universe_top_n)
    skew_df = skew_df[skew_df["ticker"].isin(universe)]
    rows = []
    for f in FILTERS:
        ev = detect_filter_events(skew_df, f, cooldown_days)
        rows.append(ev)
    all_events = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    if not all_events.empty:
        all_events.to_parquet(events_path, index=False)
    print(f"[zmatrix] precomputed {len(all_events):,} events across {len(FILTERS)} filters",
          file=sys.stderr)
    return events_path


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

PCT_COLS = (
    "CAGR", "total_return", "max_drawdown", "Calmar", "worst_month", "best_month",
    "avg_trade_return", "median_trade_return", "win_rate",
    "avg_winner", "avg_loser", "avg_trade_MFE", "avg_trade_MAE",
    "avg_giveback_ratio", "pct_candidate_events_taken",
    "pct_days_in_stock", "pct_days_in_cash", "pct_days_in_spy",
    "SPY_total_return_same_period", "SPY_CAGR_same_period",
    "SPY_MDD_same_period", "excess_CAGR_vs_SPY", "active_return_vs_SPY",
    "pnl_from_positive_tail", "pnl_from_negative_tail",
    "avg_return_positive_tail", "avg_return_negative_tail",
)


def fmt_table(df: pd.DataFrame, cols: list[str]) -> str:
    cols = [c for c in cols if c in df.columns]
    if df.empty or not cols:
        return "_(no rows)_"
    d = df[cols].copy()
    for c in d.columns:
        if c in PCT_COLS:
            d[c] = d[c].map(lambda v: f"{v:+.1%}" if pd.notna(v) else "—")
        elif c in ("Sharpe", "SPY_Sharpe_same_period", "active_Sharpe_vs_SPY"):
            d[c] = d[c].map(lambda v: f"{v:+.2f}" if pd.notna(v) else "—")
        elif c in ("avg_days_to_MFE", "median_days_to_MFE",
                   "avg_hold_days", "median_hold_days"):
            d[c] = d[c].map(lambda v: f"{v:.0f}" if pd.notna(v) else "—")
        else:
            d[c] = d[c].map(lambda v: str(v) if pd.notna(v) else "—")
    # Manual GFM-table render so we don't depend on `tabulate`.
    header = "| " + " | ".join(d.columns) + " |"
    sep = "| " + " | ".join("---" for _ in d.columns) + " |"
    rows = ["| " + " | ".join(str(x) for x in row) + " |" for row in d.values.tolist()]
    return "\n".join([header, sep, *rows])


def write_report(out_dir: Path, train_df: pd.DataFrame, top_cells: list[str],
                 oos_df: pd.DataFrame, oos_vs_random: pd.DataFrame,
                 random_seed_dist: pd.DataFrame,
                 train_window: tuple[str, str], oos_window: tuple[str, str],
                 cooldown: int, cost_bps: float, top_n: int,
                 selection_meta: dict,
                 batch_stats: dict) -> None:
    lines: list[str] = []
    lines.append("# Single-Position Z-Score Matrix Backtest\n")
    lines.append("Diagnostic single-position research matrix. Does NOT modify "
                 "production replay.py or the canonical Path S baselines.\n")

    lines.append("## 1. Objective\n")
    lines.append(
        "Test whether the z-score event study can produce a better "
        "single-position strategy by separating entry filter, selection rule, "
        "exit rule, and between-trade behavior, instead of bundling them into "
        "one strategy blob.\n"
    )

    lines.append("## 2. Data window and execution convention\n")
    lines.append(f"- Train window: **{train_window[0]} → {train_window[1]}**")
    lines.append(f"- OOS window:   **{oos_window[0]} → {oos_window[1]}**")
    lines.append(f"- Cooldown:     **{cooldown} trading days** per (ticker, filter)")
    lines.append(f"- Cost:         **{cost_bps:.0f} bps per leg** (slippage)")
    lines.append("- Execution:    signal at day D close → fill at D+1 open (mirrors replay.py).")
    lines.append("- Trailing stop tracks daily close vs. peak_close; fill at next open.")
    lines.append("- End-of-window: force-close at last close (no slippage; same for stock and SPY).\n")
    lines.append("SPY 200d regime is **logged as diagnostic** at entry but does NOT gate "
                 "candidate events. This preserves the F4 negative-tail hypothesis test, "
                 "which is most likely to fire in risk-off conditions a SPY gate would "
                 "filter out.\n")
    lines.append("**B2 SPY transitions are not free.** 15bps applied to the initial SPY "
                 "entry, every SPY-into-stock rotation (sell SPY + buy stock), and every "
                 "stock-back-to-SPY rotation. EOW close is no-slippage for both stock "
                 "and SPY (consistent with B1). The summary breaks out per-leg costs and "
                 "stock vs SPY realized P&L so a B2 win cannot be hidden behind free "
                 "SPY exposure.\n")

    lines.append("## 3. Matrix definition\n")
    lines.append("| Dim | Codes | Meaning |")
    lines.append("|---|---|---|")
    lines.append("| F | F1=z>=3, F2=z>=6, F3=z>=3 OR z<=-3, F4=z<=-3 | entry filter |")
    lines.append("| S | S1=highest \\|z\\|, S2=closest to threshold, S3=random | selection rule |")
    lines.append("| X | X1=90d max only, X2=20% trail + 90d, X3=20% trail + 45d | exit rule |")
    lines.append("| B | B1=cash, B2=SPY | between-trade behavior |\n")
    lines.append("4 × 3 × 3 × 2 = **72 logical cells**. With 20-seed S3 expansion, the "
                 "actual train job count is 24+24+480 = **528 runs**.\n")
    lines.append("**F3/S2 asymmetry:** F3/S2 picks the smallest |z|-3 across both tails. "
                 "Cumulative event density typically differs between tails, so F3/S2 will "
                 "skew toward the more-frequent tail. The `tail_side` field on every trade "
                 "and the `*_positive_tail` / `*_negative_tail` fields in each cell summary "
                 "let the F3 result be decomposed post-hoc; we did not add an extra knob.\n")

    lines.append("## 4. Train ranking method\n")
    lines.append(f"- Eligibility: `n_trades >= {MIN_TRADES_DEFAULT}` AND `pct_days_in_stock >= "
                 f"{int(MIN_STOCK_PCT_DEFAULT*100)}%`. F2 cells use a relaxed "
                 f"`n_trades >= {MIN_TRADES_F2}` floor with `low_N=true` tag (z>=6 is sparse "
                 "by construction).")
    lines.append("- Top-N picked from S1/S2 cells only — S3 random cells exist as "
                 "OOS noise-floor estimators, not strategy candidates.")
    lines.append("- Rank: Sharpe ↓, Calmar ↓ (tie-1), CAGR ↓ (tie-2), MDD desc (tie-3).")
    lines.append(f"- Pick top {top_n}; force-include best **F2** and **F4** cells if absent.\n")
    if selection_meta.get("force_included"):
        lines.append("Force-included diagnostic cells:")
        for fc in selection_meta["force_included"]:
            tag = " (low_N=true)" if fc.get("low_N") else ""
            tag += " (insufficient_sample)" if fc.get("insufficient_sample") else ""
            lines.append(f"- {fc['filter']}: `{fc['cell_id']}`{tag}")
    if selection_meta.get("low_N_cells"):
        lines.append(f"\nlow_N flagged cells: {len(selection_meta['low_N_cells'])} "
                     "(F2 cells with n_trades below normal floor)")

    cols_brief = ["cell_id", "n_trades", "pct_days_in_stock", "CAGR",
                  "Sharpe", "max_drawdown", "Calmar",
                  "SPY_CAGR_same_period", "excess_CAGR_vs_SPY"]

    lines.append(f"\n## 5. Top {top_n} train cells (S1/S2 + force-included)\n")
    if not train_df.empty:
        sel_train = train_df[train_df["cell_id"].isin(top_cells)].copy()
        order = {c: i for i, c in enumerate(top_cells)}
        sel_train["_o"] = sel_train["cell_id"].map(order)
        sel_train = sel_train.sort_values("_o").drop(columns="_o")
        lines.append(fmt_table(sel_train, cols_brief))
    else:
        lines.append("_(no train data)_")

    lines.append("\n## 6. OOS results for selected cells\n")
    if not oos_df.empty:
        non_random = oos_df[oos_df["seed"].isna()]
        lines.append(fmt_table(non_random, cols_brief))
    else:
        lines.append("_(no OOS data)_")

    lines.append("\n## 7. OOS comparison vs SPY\n")
    if not oos_df.empty:
        non_random = oos_df[oos_df["seed"].isna()].copy()
        cols_spy = ["cell_id", "total_return", "CAGR", "Sharpe", "max_drawdown",
                    "SPY_total_return_same_period", "SPY_CAGR_same_period",
                    "SPY_Sharpe_same_period", "SPY_MDD_same_period",
                    "active_return_vs_SPY", "excess_CAGR_vs_SPY",
                    "active_Sharpe_vs_SPY"]
        lines.append(fmt_table(non_random, cols_spy))

    lines.append("\n## 8. OOS comparison vs random S3 noise floor\n")
    if not oos_vs_random.empty:
        cols_rnd = ["cell_id", "n_trades", "CAGR", "Sharpe", "max_drawdown",
                    "S3_mean_CAGR", "S3_median_CAGR", "S3_min_CAGR",
                    "S3_max_CAGR", "S3_std_CAGR",
                    "S3_mean_Sharpe", "S3_median_Sharpe",
                    "beats_S3_median_Sharpe"]
        lines.append(fmt_table(oos_vs_random, cols_rnd))
    else:
        lines.append("_(no S3 noise floor — no S1/S2 OOS cells, or seed runs failed)_")

    f4_oos = oos_df[(oos_df["entry_filter"] == "F4") & (oos_df["seed"].isna())] if not oos_df.empty else pd.DataFrame()
    f2_oos = oos_df[(oos_df["entry_filter"] == "F2") & (oos_df["seed"].isna())] if not oos_df.empty else pd.DataFrame()
    b2_oos = oos_df[(oos_df["idle_behavior"] == "B2") & (oos_df["seed"].isna())] if not oos_df.empty else pd.DataFrame()

    lines.append("\n## 9. F4 negative-tail verdict\n")
    if f4_oos.empty:
        lines.append("_F4 not in selected OOS set._")
    else:
        for _, r in f4_oos.iterrows():
            lines.append(f"- **{r['cell_id']}** OOS: total_return={r['total_return']:+.1%}, "
                         f"Sharpe={r['Sharpe']:+.2f}, MDD={r['max_drawdown']:+.1%}, "
                         f"vs SPY active={r['active_return_vs_SPY']:+.1%}, "
                         f"n_trades={int(r['n_trades'])}.")

    lines.append("\n### F2 high-z verdict\n")
    if f2_oos.empty:
        lines.append("_F2 not in selected OOS set._")
    else:
        for _, r in f2_oos.iterrows():
            lines.append(f"- **{r['cell_id']}** OOS: total_return={r['total_return']:+.1%}, "
                         f"Sharpe={r['Sharpe']:+.2f}, MDD={r['max_drawdown']:+.1%}, "
                         f"vs SPY active={r['active_return_vs_SPY']:+.1%}, "
                         f"n_trades={int(r['n_trades'])}. "
                         "(z>=6 is structurally sparse — interpret with low_N caveat.)")

    lines.append("\n## 10. B1 cash vs B2 SPY-idle verdict\n")
    if b2_oos.empty:
        lines.append("_No B2 cell in selected OOS set._")
    else:
        for _, r in b2_oos.iterrows():
            spy_pnl = r.get("spy_realized_pnl_$", 0.0) or 0.0
            stk_pnl = r.get("stock_realized_pnl_$", 0.0) or 0.0
            spy_cost = (r.get("spy_buy_cost_total_$", 0.0) or 0.0) + (r.get("spy_sell_cost_total_$", 0.0) or 0.0)
            stk_cost = (r.get("stock_buy_cost_total_$", 0.0) or 0.0) + (r.get("stock_sell_cost_total_$", 0.0) or 0.0)
            lines.append(
                f"- **{r['cell_id']}** OOS: total_return={r['total_return']:+.1%}, "
                f"days_in_stock={r['pct_days_in_stock']:.0%} / days_in_spy={r['pct_days_in_spy']:.0%}. "
                f"Stock realized $P&L=${stk_pnl:,.0f}, SPY realized $P&L=${spy_pnl:,.0f}. "
                f"Cost paid: stock ${stk_cost:,.0f} / SPY ${spy_cost:,.0f}. "
                f"SPY same-period total_return={r['SPY_total_return_same_period']:+.1%}, "
                f"active vs SPY={r['active_return_vs_SPY']:+.1%}."
            )
        lines.append("\nA B2 cell that wins only because of SPY exposure is not a "
                     "stock-selection win. The stock $P&L line is what isolates the "
                     "stock-selection contribution.")

    lines.append("\n## 11. Throughput analysis\n")
    if not oos_df.empty:
        non_random = oos_df[oos_df["seed"].isna()].copy()
        cols_thr = ["cell_id", "n_total_candidate_events", "n_events_while_flat",
                    "n_events_blocked_while_in_position", "n_trades",
                    "pct_candidate_events_taken", "pct_days_in_stock",
                    "avg_hold_days", "median_hold_days"]
        lines.append(fmt_table(non_random, cols_thr))

    lines.append("\n## 12. Final research conclusion\n")
    if not oos_df.empty:
        non_random = oos_df[oos_df["seed"].isna()].copy()
        if not non_random.empty:
            best = non_random.sort_values("Sharpe", ascending=False).iloc[0]
            lines.append(f"- Best OOS Sharpe: **{best['cell_id']}** "
                         f"(Sharpe={best['Sharpe']:+.2f}, CAGR={best['CAGR']:+.1%}, "
                         f"MDD={best['max_drawdown']:+.1%}, "
                         f"days_in_stock={best['pct_days_in_stock']:.0%}).")
            spy_share = best.get("SPY_Sharpe_same_period")
            if pd.notna(spy_share):
                if best["Sharpe"] > spy_share:
                    lines.append(f"  - Beats SPY same-period Sharpe ({spy_share:+.2f}).")
                else:
                    lines.append(f"  - Does NOT beat SPY same-period Sharpe ({spy_share:+.2f}).")
            if best["pct_days_in_stock"] < 0.60:
                lines.append("  - **Fails** the 60% days-in-stock criterion. Do not promote.")
            else:
                lines.append("  - Meets 60% days-in-stock criterion.")

    lines.append("\n## 13. What should NOT be concluded\n")
    lines.append(
        "- This test does **not** change the production strategy by itself.\n"
        "- This test does **not** validate top-2 portfolios.\n"
        "- This test does **not** validate stale-loser displacement.\n"
        "- This test does **not** prove high z is or is not an exit.\n"
        "- This test only evaluates single-position entry/selection/exit/idle "
        "behavior under the defined matrix.\n"
        "- A train winner that fails OOS Sharpe-vs-SPY or 60% days-in-stock is "
        "**not** a deployable candidate, no matter how good its train numbers look.\n"
    )

    lines.append("\n## 14. Run statistics\n")
    lines.append(f"- Logical cells: **{batch_stats.get('n_logical', '?')}**")
    lines.append(f"- Actual runs (S3 expanded): **{batch_stats.get('n_actual', '?')}**")
    lines.append(f"- Completed: **{batch_stats.get('completed', '?')}**, "
                 f"failed: **{batch_stats.get('failed', '?')}**, "
                 f"timed_out: **{batch_stats.get('timed_out', '?')}**")
    lines.append(f"- Worker count: **{batch_stats.get('workers', '?')}**, "
                 f"per-cell timeout: **{batch_stats.get('cell_timeout_seconds', '?')}s**")
    lines.append(f"- Total runtime: **{batch_stats.get('total_runtime_seconds', '?')}s**")

    write_atomic(out_dir / "report.md", "\n".join(lines))


# ---------------------------------------------------------------------------
# Diagnostic OOS-only runner (matched-comparator mode)
# ---------------------------------------------------------------------------

def _run_diagnostic_oos(diag_ids: list[str], args, out_dir: Path,
                        train_window: tuple[str, str], oos_window: tuple[str, str],
                        manifest_path: Path, oos_partial: Path) -> int:
    """Run a hand-picked list of cells in OOS only. Skips train + selection.
    Each non-S3 diagnostic cell also gets its matching S3 noise floor (n seeds).
    Writes diagnostic_oos_results.csv and diagnostic_report.md.
    """
    print(f"[zmatrix] === DIAGNOSTIC OOS mode: {len(diag_ids)} cell(s) ===",
          file=sys.stderr)
    train_logical = make_cells_logical(split="oos")
    by_id = {c.cell_id: c for c in train_logical}
    jobs: list[RunConfig] = []
    for cid in diag_ids:
        cfg_t = by_id.get(cid)
        if cfg_t is None:
            print(f"[zmatrix] WARNING: unknown cell_id {cid}, skipping", file=sys.stderr)
            continue
        jobs.append(RunConfig(
            cell_id=cfg_t.cell_id, fcode=cfg_t.fcode, scode=cfg_t.scode,
            xcode=cfg_t.xcode, bcode=cfg_t.bcode, seed=None,
            trailing_pct=cfg_t.trailing_pct, max_hold_days=cfg_t.max_hold_days,
            cost_bps=cfg_t.cost_bps, split="oos",
        ))
        if cfg_t.scode != "S3":
            s3_id = f"{cfg_t.fcode}_S3_{cfg_t.xcode}_{cfg_t.bcode}"
            for k in range(args.random_seeds):
                jobs.append(RunConfig(
                    cell_id=f"{s3_id}_seed{k}",
                    fcode=cfg_t.fcode, scode="S3", xcode=cfg_t.xcode, bcode=cfg_t.bcode,
                    seed=k,
                    trailing_pct=cfg_t.trailing_pct, max_hold_days=cfg_t.max_hold_days,
                    cost_bps=cfg_t.cost_bps, split="oos",
                ))
    print(f"[zmatrix] diagnostic OOS jobs: {len(jobs)} "
          f"(non-S3 + matching S3 noise floors)", file=sys.stderr)

    t0 = time.time()
    summaries = run_batch(
        jobs, out_dir, train_window, oos_window,
        args.workers, args.cell_timeout_seconds, args.heartbeat_seconds,
        args.skew_path, args.stocks_path, args.universe_top_n,
        args.cooldown_days, args.force, args.retry_failed,
        args.fail_fast, manifest_path, oos_partial,
    )
    runtime = time.time() - t0

    df = pd.DataFrame(summaries) if summaries else pd.DataFrame()
    if df.empty:
        print("[zmatrix] no diagnostic results", file=sys.stderr)
        return 1
    non_random = df[df["seed"].isna()].copy()
    s3_runs = df[df["seed"].notna()].copy()
    write_atomic_csv(out_dir / "diagnostic_oos_results.csv", non_random)
    if not s3_runs.empty:
        write_atomic_csv(out_dir / "diagnostic_oos_s3_runs.csv", s3_runs)

    # S3 aggregation
    pair_rows = []
    for _, sel in non_random.iterrows():
        if sel["selection_rule"] == "S3":
            continue
        pair_id = f"{sel['entry_filter']}_S3_{sel['exit_rule']}_{sel['idle_behavior']}"
        grp = s3_runs[s3_runs["cell_id"].str.startswith(f"{pair_id}_seed")] if not s3_runs.empty else pd.DataFrame()
        if grp.empty:
            continue
        cagrs = grp["CAGR"].astype(float)
        sharpes = grp["Sharpe"].astype(float)
        pair_rows.append({
            "cell_id": sel["cell_id"],
            "n_trades": int(sel["n_trades"]),
            "CAGR": float(sel["CAGR"]),
            "Sharpe": float(sel["Sharpe"]),
            "max_drawdown": float(sel["max_drawdown"]),
            "S3_mean_CAGR": float(cagrs.mean()),
            "S3_median_CAGR": float(cagrs.median()),
            "S3_min_CAGR": float(cagrs.min()),
            "S3_max_CAGR": float(cagrs.max()),
            "S3_std_CAGR": float(cagrs.std()),
            "S3_mean_Sharpe": float(sharpes.mean()),
            "S3_median_Sharpe": float(sharpes.median()),
            "beats_S3_median_Sharpe": float(sel["Sharpe"]) > float(sharpes.median()),
        })
    pair_df = pd.DataFrame(pair_rows)
    if not pair_df.empty:
        write_atomic_csv(out_dir / "diagnostic_oos_vs_random.csv", pair_df)

    # Per-tail decomposition from each cell's trade log
    tail_rows = []
    for _, sel in non_random.iterrows():
        cell_dir = out_dir / "cells" / "oos" / sel["cell_id"]
        tl_path = cell_dir / "trade_log.csv"
        if not tl_path.exists():
            continue
        tl = pd.read_csv(tl_path)
        for tail in ("positive", "negative"):
            sub = tl[tl["tail_side"] == tail]
            if sub.empty:
                tail_rows.append({"cell_id": sel["cell_id"], "tail": tail,
                                  "n_trades": 0})
                continue
            compound = float((1.0 + sub["trade_return"]).prod() - 1.0)
            tail_rows.append({
                "cell_id": sel["cell_id"],
                "tail": tail,
                "n_trades": int(len(sub)),
                "compound_return": compound,
                "avg_return": float(sub["trade_return"].mean()),
                "median_return": float(sub["trade_return"].median()),
                "win_rate": float((sub["trade_return"] > 0).mean()),
                "avg_MFE": float(sub["MFE"].mean()),
                "avg_MAE": float(sub["MAE"].mean()),
                "avg_hold_days": float(sub["hold_days"].mean()),
                "tickers": ",".join(sub["ticker"].tolist()),
            })
    tail_df = pd.DataFrame(tail_rows)
    if not tail_df.empty:
        write_atomic_csv(out_dir / "diagnostic_tail_decomposition.csv", tail_df)

    # Overlap matrix: which trades show up across cells (by entry_date+ticker)
    all_trades_map: dict[str, set] = {}
    for _, sel in non_random.iterrows():
        tl_path = out_dir / "cells" / "oos" / sel["cell_id"] / "trade_log.csv"
        if not tl_path.exists():
            continue
        tl = pd.read_csv(tl_path)
        all_trades_map[sel["cell_id"]] = set(
            (r["entry_date"], r["ticker"]) for _, r in tl.iterrows()
        )
    overlap_rows = []
    cell_ids = list(all_trades_map.keys())
    for cid in cell_ids:
        for other in cell_ids:
            if cid == other:
                continue
            shared = all_trades_map[cid] & all_trades_map[other]
            overlap_rows.append({
                "cell_a": cid,
                "cell_b": other,
                "n_a": len(all_trades_map[cid]),
                "n_b": len(all_trades_map[other]),
                "n_shared": len(shared),
                "shared_keys": ";".join(sorted(f"{d}|{t}" for d, t in shared)),
            })
    overlap_df = pd.DataFrame(overlap_rows)
    if not overlap_df.empty:
        write_atomic_csv(out_dir / "diagnostic_trade_overlap.csv", overlap_df)

    # Mini report
    lines = ["# Diagnostic OOS Matched-Comparator Report\n"]
    lines.append(f"_OOS window: {oos_window[0]} → {oos_window[1]}_\n")
    lines.append(f"_Cells run: {', '.join(diag_ids)} + matching S3 noise floors._\n")

    lines.append("## Headline\n")
    cols_brief = ["cell_id", "entry_filter", "selection_rule", "exit_rule",
                  "idle_behavior", "n_trades", "total_return", "CAGR", "Sharpe",
                  "max_drawdown", "SPY_total_return_same_period",
                  "active_return_vs_SPY"]
    lines.append(fmt_table(non_random, cols_brief))

    lines.append("\n## S3 noise-floor comparison\n")
    if not pair_df.empty:
        lines.append(fmt_table(pair_df, [
            "cell_id", "n_trades", "CAGR", "Sharpe", "max_drawdown",
            "S3_median_CAGR", "S3_median_Sharpe", "S3_max_Sharpe",
            "beats_S3_median_Sharpe"]))
    else:
        lines.append("_(no S3 noise floor)_")

    lines.append("\n## Tail decomposition (per cell)\n")
    if not tail_df.empty:
        lines.append(fmt_table(tail_df, [
            "cell_id", "tail", "n_trades", "compound_return", "avg_return",
            "win_rate", "avg_MFE", "avg_MAE", "avg_hold_days", "tickers"]))
    else:
        lines.append("_(no tail decomposition)_")

    lines.append("\n## Trade overlap across cells\n")
    if not overlap_df.empty:
        lines.append(fmt_table(overlap_df, [
            "cell_a", "cell_b", "n_a", "n_b", "n_shared", "shared_keys"]))

    lines.append("\n## Interpretation rules\n")
    lines.append(
        "- If F3 wins beat both matched F1 and matched F4 in OOS → combined "
        "U-shape adds real edge.\n"
        "- If F3 ≈ F1 (matched X/B) → the combined rule is mostly positive-tail "
        "with altered cooldown/selection timing, not a U-shape edge.\n"
        "- If F4_S1 (deep negative pick) wins OOS while F4_S2 (shallow pick) "
        "loses → 'depth of z' matters more than 'tail side', and the F4_S2 "
        "OOS collapse is about pick quality, not the negative tail itself.\n"
        "- If overlap is high between F3 and F1 (or F3 and F4) → most F3 "
        "trades are duplicates of one tail's matched cell, not unique combined "
        "selections.\n"
    )
    lines.append(f"\n_Runtime: {int(runtime)}s_\n")
    write_atomic(out_dir / "diagnostic_report.md", "\n".join(lines))

    print(f"[zmatrix] DIAGNOSTIC OOS DONE → {out_dir}", file=sys.stderr)
    return 0


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--train-start", default="2022-05-31")
    ap.add_argument("--train-end", default="2024-12-31")
    ap.add_argument("--oos-start", default="2025-01-01")
    ap.add_argument("--oos-end", default="2026-04-29")
    ap.add_argument("--cooldown-days", type=int, default=COOLDOWN_DAYS_DEFAULT)
    ap.add_argument("--universe-top-n", type=int, default=2000)
    ap.add_argument("--cost-bps", type=float, default=COST_BPS_DEFAULT)
    ap.add_argument("--random-seeds", type=int, default=RANDOM_SEEDS_DEFAULT)
    ap.add_argument("--top-n", type=int, default=5)
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--skew-path", type=Path, default=DATA_DIR / "skew_daily.parquet")
    ap.add_argument("--stocks-path", type=Path,
                    default=DATA_DIR / "aggs_daily_adjusted.parquet")
    ap.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 2) - 1))
    ap.add_argument("--cell-timeout-seconds", type=int, default=1800)
    ap.add_argument("--heartbeat-seconds", type=int, default=60)
    ap.add_argument("--resume", action="store_true",
                    help="Skip cells with completed summary.json")
    ap.add_argument("--force", action="store_true",
                    help="Re-run cells even if completed")
    ap.add_argument("--retry-failed", action="store_true",
                    help="Re-run FAILED / TIMED_OUT cells")
    ap.add_argument("--fail-fast", action="store_true",
                    help="Abort batch on first FAILED / TIMED_OUT cell")
    ap.add_argument("--limit-cells", type=int, default=0,
                    help="Smoke-test mode: only run first N actual jobs (0 = all)")
    ap.add_argument("--diagnostic-oos-cells", type=str, default="",
                    help="Comma-separated cell IDs to run OOS-only as matched "
                         "comparators (skips train + selection). Each non-S3 "
                         "cell also gets its matching S3 noise-floor (20 seeds).")
    args = ap.parse_args()

    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "cells").mkdir(exist_ok=True)
    manifest_path = out_dir / "manifest.csv"
    train_partial = out_dir / "train_all_cells_partial.csv"
    oos_partial = out_dir / "oos_selected_cells_partial.csv"
    seed_partial = out_dir / "random_s3_seed_distribution_partial.csv"

    # Pre-compute event table on disk (diagnostic; workers also rebuild it).
    precompute_events(out_dir, args.skew_path, args.stocks_path,
                      args.universe_top_n, args.cooldown_days)

    train_window = (args.train_start, args.train_end)
    oos_window = (args.oos_start, args.oos_end)

    # ------ DIAGNOSTIC OOS-ONLY MODE ------
    diag_ids = [s.strip() for s in args.diagnostic_oos_cells.split(",") if s.strip()]
    if diag_ids:
        return _run_diagnostic_oos(
            diag_ids, args, out_dir, train_window, oos_window,
            manifest_path, oos_partial,
        )

    # ------ TRAIN PHASE ------
    print(f"[zmatrix] === TRAIN phase: {train_window[0]} → {train_window[1]} ===",
          file=sys.stderr)
    train_logical = make_cells_logical(split="train")
    train_actual = expand_actual_runs(train_logical, args.random_seeds)
    if args.limit_cells > 0:
        train_actual = train_actual[: args.limit_cells]
        print(f"[zmatrix] SMOKE-TEST: limiting to first {len(train_actual)} train jobs",
              file=sys.stderr)
    print(f"[zmatrix] train: {len(train_logical)} logical → "
          f"{len(train_actual)} actual jobs", file=sys.stderr)

    t0 = time.time()
    train_summaries = run_batch(
        train_actual, out_dir, train_window, oos_window,
        args.workers, args.cell_timeout_seconds, args.heartbeat_seconds,
        args.skew_path, args.stocks_path, args.universe_top_n,
        args.cooldown_days, args.force, args.retry_failed,
        args.fail_fast, manifest_path, train_partial,
    )
    train_runtime = time.time() - t0

    train_df = pd.DataFrame(train_summaries)
    if not train_df.empty:
        write_atomic_csv(out_dir / "train_all_cells.csv", train_df)

    # ------ SELECTION ------
    selected_train_only = train_df[train_df["seed"].isna()] if not train_df.empty else train_df
    sel_summaries = (selected_train_only.to_dict("records")
                     if not selected_train_only.empty else [])
    top_cells, sel_meta = select_top_train(sel_summaries, top_n=args.top_n)
    print(f"[zmatrix] selected for OOS: {top_cells}", file=sys.stderr)
    print(f"[zmatrix] selection meta: {json.dumps(sel_meta, default=str)}", file=sys.stderr)
    if top_cells:
        write_atomic_csv(out_dir / "train_top5_selected.csv",
                         train_df[train_df["cell_id"].isin(top_cells)])
    write_atomic(out_dir / "selection_metadata.json",
                 json.dumps(sel_meta, indent=2, default=str))

    # ------ OOS PHASE: selected non-S3 + matching S3 noise floors (20 seeds each) ------
    print(f"[zmatrix] === OOS phase: {oos_window[0]} → {oos_window[1]} ===",
          file=sys.stderr)
    oos_jobs: list[RunConfig] = []
    train_logical_by_id = {c.cell_id: c for c in train_logical}
    for cid in top_cells:
        cfg_t = train_logical_by_id.get(cid)
        if cfg_t is None:
            print(f"[zmatrix] WARNING: {cid} not in logical set", file=sys.stderr)
            continue
        oos_jobs.append(RunConfig(
            cell_id=cfg_t.cell_id, fcode=cfg_t.fcode, scode=cfg_t.scode,
            xcode=cfg_t.xcode, bcode=cfg_t.bcode, seed=None,
            trailing_pct=cfg_t.trailing_pct, max_hold_days=cfg_t.max_hold_days,
            cost_bps=cfg_t.cost_bps, split="oos",
        ))
        # Matching S3 noise floor
        if cfg_t.scode != "S3":
            s3_id = f"{cfg_t.fcode}_S3_{cfg_t.xcode}_{cfg_t.bcode}"
            for k in range(args.random_seeds):
                oos_jobs.append(RunConfig(
                    cell_id=f"{s3_id}_seed{k}",
                    fcode=cfg_t.fcode, scode="S3", xcode=cfg_t.xcode, bcode=cfg_t.bcode,
                    seed=k,
                    trailing_pct=cfg_t.trailing_pct, max_hold_days=cfg_t.max_hold_days,
                    cost_bps=cfg_t.cost_bps, split="oos",
                ))
    print(f"[zmatrix] oos: {len(oos_jobs)} actual jobs", file=sys.stderr)

    t1 = time.time()
    oos_summaries = run_batch(
        oos_jobs, out_dir, train_window, oos_window,
        args.workers, args.cell_timeout_seconds, args.heartbeat_seconds,
        args.skew_path, args.stocks_path, args.universe_top_n,
        args.cooldown_days, args.force, args.retry_failed,
        args.fail_fast, manifest_path, oos_partial,
    )
    oos_runtime = time.time() - t1

    oos_df = pd.DataFrame(oos_summaries) if oos_summaries else pd.DataFrame()
    if not oos_df.empty:
        # split into non-random (selected) and S3 (noise floor)
        non_random = oos_df[oos_df["seed"].isna()]
        s3_runs = oos_df[oos_df["seed"].notna()]
        if not non_random.empty:
            write_atomic_csv(out_dir / "oos_selected_cells.csv", non_random)
            cols_spy = ["cell_id", "total_return", "CAGR", "Sharpe", "max_drawdown",
                        "SPY_total_return_same_period", "SPY_CAGR_same_period",
                        "SPY_Sharpe_same_period", "SPY_MDD_same_period",
                        "active_return_vs_SPY", "excess_CAGR_vs_SPY",
                        "active_Sharpe_vs_SPY"]
            write_atomic_csv(out_dir / "oos_selected_vs_spy.csv",
                             non_random[[c for c in cols_spy if c in non_random.columns]])
        if not s3_runs.empty:
            write_atomic_csv(out_dir / "random_s3_seed_distribution_raw.csv", s3_runs)

    # ------ S3 noise-floor aggregation ------
    pair_rows = []
    seed_dist_rows = []
    if not oos_df.empty:
        non_random = oos_df[oos_df["seed"].isna()]
        s3_runs = oos_df[oos_df["seed"].notna()].copy()
        if not s3_runs.empty:
            for _, sel in non_random.iterrows():
                if sel["selection_rule"] == "S3":
                    continue
                pair_id = f"{sel['entry_filter']}_S3_{sel['exit_rule']}_{sel['idle_behavior']}"
                grp = s3_runs[s3_runs["cell_id"].str.startswith(f"{pair_id}_seed")]
                if grp.empty:
                    continue
                cagrs = grp["CAGR"].astype(float)
                sharpes = grp["Sharpe"].astype(float)
                seed_dist_rows.append({
                    "paired_with": sel["cell_id"],
                    "S3_n_seeds": int(len(grp)),
                    "S3_mean_CAGR": float(cagrs.mean()),
                    "S3_median_CAGR": float(cagrs.median()),
                    "S3_min_CAGR": float(cagrs.min()),
                    "S3_max_CAGR": float(cagrs.max()),
                    "S3_std_CAGR": float(cagrs.std()),
                    "S3_mean_Sharpe": float(sharpes.mean()),
                    "S3_median_Sharpe": float(sharpes.median()),
                    "S3_min_Sharpe": float(sharpes.min()),
                    "S3_max_Sharpe": float(sharpes.max()),
                    "S3_std_Sharpe": float(sharpes.std()),
                })
                pair_rows.append({
                    "cell_id": sel["cell_id"],
                    "n_trades": int(sel["n_trades"]),
                    "CAGR": float(sel["CAGR"]),
                    "Sharpe": float(sel["Sharpe"]),
                    "max_drawdown": float(sel["max_drawdown"]),
                    "S3_mean_CAGR": float(cagrs.mean()),
                    "S3_median_CAGR": float(cagrs.median()),
                    "S3_min_CAGR": float(cagrs.min()),
                    "S3_max_CAGR": float(cagrs.max()),
                    "S3_std_CAGR": float(cagrs.std()),
                    "S3_mean_Sharpe": float(sharpes.mean()),
                    "S3_median_Sharpe": float(sharpes.median()),
                    "beats_S3_median_Sharpe": float(sel["Sharpe"]) > float(sharpes.median()),
                })
    seed_dist_df = pd.DataFrame(seed_dist_rows)
    pair_df = pd.DataFrame(pair_rows)
    if not seed_dist_df.empty:
        write_atomic_csv(out_dir / "random_s3_seed_distribution.csv", seed_dist_df)
    if not pair_df.empty:
        write_atomic_csv(out_dir / "oos_selected_vs_random.csv", pair_df)

    # ------ Final report + manifest summary ------
    manifest_final = load_manifest(manifest_path)
    completed = (manifest_final["status"] == "DONE").sum() if not manifest_final.empty else 0
    failed = (manifest_final["status"] == "FAILED").sum() if not manifest_final.empty else 0
    timed_out = (manifest_final["status"] == "TIMED_OUT").sum() if not manifest_final.empty else 0
    batch_stats = {
        "n_logical": len(train_logical),
        "n_actual": len(train_actual) + len(oos_jobs),
        "completed": int(completed),
        "failed": int(failed),
        "timed_out": int(timed_out),
        "workers": args.workers,
        "cell_timeout_seconds": args.cell_timeout_seconds,
        "total_runtime_seconds": int(train_runtime + oos_runtime),
    }
    write_report(
        out_dir,
        train_df=train_df,
        top_cells=top_cells,
        oos_df=oos_df,
        oos_vs_random=pair_df,
        random_seed_dist=seed_dist_df,
        train_window=train_window,
        oos_window=oos_window,
        cooldown=args.cooldown_days,
        cost_bps=args.cost_bps,
        top_n=args.top_n,
        selection_meta=sel_meta,
        batch_stats=batch_stats,
    )

    manifest = {
        "train_window": list(train_window),
        "oos_window": list(oos_window),
        "cooldown_days": args.cooldown_days,
        "universe_top_n": args.universe_top_n,
        "cost_bps": args.cost_bps,
        "random_seeds": args.random_seeds,
        "top_n": args.top_n,
        "workers": args.workers,
        "cell_timeout_seconds": args.cell_timeout_seconds,
        "n_logical_cells": len(train_logical),
        "n_train_actual_jobs": len(train_actual),
        "n_oos_actual_jobs": len(oos_jobs),
        "selected_cells": top_cells,
        "selection_metadata": sel_meta,
        "batch_stats": batch_stats,
    }
    write_atomic(out_dir / "manifest.json", json.dumps(manifest, indent=2, default=str))

    print(f"[zmatrix] DONE → {out_dir}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
