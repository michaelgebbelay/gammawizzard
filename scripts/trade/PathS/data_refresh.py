#!/usr/bin/env python3
"""Incremental Path-S signal refresh.

Production data pipeline. Pulls only what's missing — never reprocesses
the full 4-year window. The cold-run scripts in
`scripts/conviction/backtest/` are preserved for backtests.

What it does, in order:

  1. Bars (stocks): download missing days via massive_ingest (already
     incremental on download), apply splits.
  2. Options: download missing daily csv.gz, parse + filter for the
     cleaned universe, APPEND only the new rows to options_daily.parquet.
  3. Skew: for each underlying with new options data, compute skew_5otm
     for the new dates only. APPEND to skew_daily.parquet.
  4. Compute lookahead-safe 252d rolling z on the target date, top-2000
     universe filter, regime gate.
  5. Write `signal_today.json` for downstream consumers.

Usage:

    python scripts/trade/PathS/data_refresh.py
    python scripts/trade/PathS/data_refresh.py --as-of 2026-05-01
    python scripts/trade/PathS/data_refresh.py --no-bars  # skip bars step
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import numpy as np

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
BACKTEST_DIR = REPO / "scripts" / "conviction" / "backtest"
DATA_DIR = BACKTEST_DIR / "data"
OUT_DIR = HERE / "state"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Reuse the cold-run scripts' helpers (no modifications to those files)
sys.path.insert(0, str(BACKTEST_DIR))
sys.path.insert(0, str(REPO / "scripts" / "conviction"))
from massive_options_ingest import (  # noqa: E402
    download_window,
    parse_opra_ticker,
    EXPECTED_COLUMNS,
    PARQUET_PATH as OPTIONS_PARQUET,
    RAW_DIR as OPT_RAW_DIR,
)
from iv_compute import (  # noqa: E402
    compute_skew_for_underlying,
    SKEW_PATH as SKEW_PARQUET,
)
from dynamic_themes import build_static_universe_top_n  # noqa: E402
from massive_reference import allowed_ticker_set  # noqa: E402
from path_s_pit_universe import build_or_load_pit_universe_table  # noqa: E402
from stability import compute_stability_factors  # noqa: E402

STOCKS_PARQUET     = DATA_DIR / "aggs_daily_adjusted.parquet"
RAW_BARS_PARQUET   = DATA_DIR / "aggs_daily.parquet"
SPLITS_PARQUET     = DATA_DIR / "splits.parquet"
META_PARQUET       = DATA_DIR / "ticker_metadata.parquet"
TOP2000_CACHE      = DATA_DIR / "top2000_universe.parquet"
SIGNAL_OUT         = OUT_DIR / "signal_today.json"
STATE_PATH         = OUT_DIR / "path_s_state.json"

# Canonical contract is the single source of truth — lives in the backtest
# dir (tracked) so replay.py and data_refresh.py share one path.
from canonical_config import (  # noqa: E402  (resolved via BACKTEST_DIR sys.path)
    CANONICAL,
    CANONICAL_HASH,
    signal_config_block,
    assert_runtime_match,
)

Z_MIN          = CANONICAL["z_threshold"]
ROLLING_DAYS   = CANONICAL["z_window"]
ROLLING_MIN    = CANONICAL["rolling_min"]
UNIVERSE_TOP_N = CANONICAL["universe_top_n"]

# Aliases retained for verify_parity.py back-compat (it imports these names).
EXPECTED_Z_WINDOW       = ROLLING_DAYS
EXPECTED_ROLLING_MIN    = ROLLING_MIN
EXPECTED_Z_MIN          = Z_MIN
EXPECTED_UNIVERSE_TOP_N = UNIVERSE_TOP_N

UNIVERSE_SELECTOR_CORE = "core_2000"
UNIVERSE_SELECTOR_PIT_60 = "pit_option_liq_1000_60d"
UNIVERSE_SELECTOR_PIT_126 = "pit_option_liq_1000_126d"
UNIVERSE_SELECTOR_CHOICES = [
    UNIVERSE_SELECTOR_CORE,
    UNIVERSE_SELECTOR_PIT_60,
    UNIVERSE_SELECTOR_PIT_126,
]
UNIVERSE_SELECTOR_PIT_PARAMS = {
    UNIVERSE_SELECTOR_PIT_60: {"target_n": 1000, "window_sessions": 60},
    UNIVERSE_SELECTOR_PIT_126: {"target_n": 1000, "window_sessions": 126},
}


def assert_signal_config_canonical() -> None:
    """Fail-closed guard at module-load and main entry. Refuses to compute
    if any constant drifts from the canonical contract."""
    assert_runtime_match(
        z_window=ROLLING_DAYS,
        rolling_min=ROLLING_MIN,
        z_threshold=Z_MIN,
        universe_top_n=UNIVERSE_TOP_N,
    )


def _mtime(p: Path) -> float:
    return p.stat().st_mtime if p.exists() else 0.0


def _normalize_dates(values: list[date] | set[date] | tuple[date, ...]) -> list[date]:
    return sorted({pd.Timestamp(v).date() for v in values})


def _enumerate_weekdays(start: date, end: date) -> list[date]:
    out = []
    cur = start
    while cur <= end:
        if cur.weekday() < 5:
            out.append(cur)
        cur += timedelta(days=1)
    return out


def _load_skew_presence_df() -> pd.DataFrame:
    if not SKEW_PARQUET.exists():
        return pd.DataFrame(columns=["underlying", "date"])
    df = pd.read_parquet(SKEW_PARQUET, columns=["underlying", "date"])
    df["underlying"] = df["underlying"].astype(str).str.upper()
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    return df


def _load_current_position_context() -> dict:
    """Load the current held ticker from local Path S state, if any."""
    if not STATE_PATH.exists():
        return {
            "held_ticker": None,
            "held_qty": 0,
            "held_force_include_reason": None,
            "state_as_of": None,
        }
    try:
        payload = json.loads(STATE_PATH.read_text())
    except (OSError, json.JSONDecodeError):
        return {
            "held_ticker": None,
            "held_qty": 0,
            "held_force_include_reason": None,
            "state_as_of": None,
        }

    pos = payload.get("position") or {}
    ticker = str(pos.get("ticker", "")).upper().strip() or None
    qty = pos.get("qty") or 0
    try:
        qty = int(qty)
    except (TypeError, ValueError):
        qty = 0
    if ticker and qty > 0:
        return {
            "held_ticker": ticker,
            "held_qty": qty,
            "held_force_include_reason": "current_position",
            "state_as_of": payload.get("as_of"),
        }
    return {
        "held_ticker": None,
        "held_qty": 0,
        "held_force_include_reason": None,
        "state_as_of": payload.get("as_of"),
    }


def _load_signal_payload(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None


def _signal_fast_path_status(
    *,
    signal_path: Path,
    state_path: Path,
    selector: str,
    target: date,
) -> dict:
    """Assess whether an existing signal file is safe to reuse.

    Returned keys:
      - eligible: bool
      - reason: short machine-readable reason
      - payload: parsed signal payload or None
    """
    payload = _load_signal_payload(signal_path)
    if payload is None:
        return {"eligible": False, "reason": "missing_or_invalid_signal", "payload": None}
    try:
        sig_as_of = date.fromisoformat(payload["as_of"])
    except (KeyError, ValueError, TypeError):
        return {"eligible": False, "reason": "invalid_signal_as_of", "payload": payload}
    sig_mtime = _mtime(signal_path)
    inputs_unchanged = (
        _mtime(SKEW_PARQUET) <= sig_mtime
        and _mtime(STOCKS_PARQUET) <= sig_mtime
        and _mtime(state_path) <= sig_mtime
    )
    if sig_as_of < target:
        return {"eligible": False, "reason": "stale_signal_date", "payload": payload}
    if payload.get("universe_selector", UNIVERSE_SELECTOR_CORE) != selector:
        return {"eligible": False, "reason": "selector_mismatch", "payload": payload}
    if not inputs_unchanged:
        return {"eligible": False, "reason": "input_mtime_newer", "payload": payload}
    return {"eligible": True, "reason": "fast_path_ok", "payload": payload}


def _selector_action(payload: dict) -> str:
    if payload.get("regime", {}).get("open") and payload.get("n_candidates", 0) > 0:
        return "ENTER"
    return "NONE"


def _build_selector_parity(
    *,
    selector_payload: dict,
    core_payload: dict,
    selector_allowed_tickers: set[str],
) -> dict:
    core_top = core_payload["candidates"][0]["ticker"] if core_payload.get("candidates") else ""
    selector_top = (
        selector_payload["candidates"][0]["ticker"] if selector_payload.get("candidates") else ""
    )
    core_action = _selector_action(core_payload)
    selector_action = _selector_action(selector_payload)
    core_top_in_selector = bool(
        not core_top or core_top in selector_allowed_tickers
    )
    excluded_reason = "" if core_top_in_selector else "not_in_selector_universe"
    warning = None
    if core_action != selector_action:
        warning = f"selector action diverges from core ({selector_action} vs {core_action})"
    elif core_top != selector_top:
        warning = f"selector top candidate diverges from core ({selector_top or 'NONE'} vs {core_top or 'NONE'})"
    elif not core_top_in_selector:
        warning = f"core top candidate {core_top} is excluded from selector universe"
    return {
        "core_selector": UNIVERSE_SELECTOR_CORE,
        "core_top_candidate": core_top,
        "selector_top_candidate": selector_top,
        "same_top_candidate": core_top == selector_top,
        "core_action": core_action,
        "selector_action": selector_action,
        "same_action": core_action == selector_action,
        "core_top_candidate_included_in_selector": core_top_in_selector,
        "core_top_candidate_excluded_reason": excluded_reason,
        "warning": warning,
    }


def _resolve_universe_selector(
    selector: str,
    target_dates: list[date] | set[date] | tuple[date, ...],
    *,
    force_include_ticker: str | None = None,
    force_include_reason: str | None = None,
    rebuild_pit_cache: bool = False,
) -> tuple[dict[date, set[str]], dict]:
    """Resolve a date-specific live universe for the requested selector.

    For core mode the same top-2000 set applies to every date. For PIT modes
    we load the cached day-by-day membership table and slice it at the
    requested dates using only trailing information already available before
    each date.
    """
    dates = _normalize_dates(target_dates)
    if not dates:
        return {}, {
            "selector": selector,
            "base_universe_size": 0,
            "implemented_universe_size": 0,
            "avg_allowed_universe_size": 0.0,
            "selector_timing_seconds": 0.0,
        }

    t0 = time.perf_counter()
    core_tickers = sorted(_load_or_build_top2000_cache())
    if selector == UNIVERSE_SELECTOR_CORE:
        members = {d: set(core_tickers) for d in dates}
        forced_missing_dates: list[str] = []
        if force_include_ticker:
            for d in dates:
                if force_include_ticker not in members[d]:
                    forced_missing_dates.append(d.isoformat())
                members[d].add(force_include_ticker)
        return members, {
            "selector": selector,
            "base_universe_size": len(core_tickers),
            "implemented_universe_size": len({t for tickers in members.values() for t in tickers}),
            "avg_allowed_universe_size": float(np.mean([len(v) for v in members.values()])),
            "selector_timing_seconds": round(time.perf_counter() - t0, 3),
            "held_force_include_ticker": force_include_ticker,
            "held_force_include_reason": force_include_reason,
            "held_force_include_missing_dates": forced_missing_dates,
        }

    if selector not in UNIVERSE_SELECTOR_PIT_PARAMS:
        raise SystemExit(
            f"unknown universe selector {selector!r}; choices: {', '.join(UNIVERSE_SELECTOR_CHOICES)}"
        )

    params = UNIVERSE_SELECTOR_PIT_PARAMS[selector]
    bars = pd.read_parquet(
        STOCKS_PARQUET,
        columns=["ticker", "date", "close", "volume"],
        filters=[("ticker", "in", core_tickers)],
    )
    bars["ticker"] = bars["ticker"].astype(str).str.upper()
    bars["date"] = pd.to_datetime(bars["date"]).dt.normalize()
    bars["dollar_vol"] = bars["close"].astype(float) * bars["volume"].astype(float)
    session_dates = pd.DatetimeIndex(sorted(bars["date"].dropna().unique()))
    skew_df = _load_skew_presence_df()

    table, cache_meta = build_or_load_pit_universe_table(
        core_tickers=core_tickers,
        eligible_df=bars,
        session_dates=session_dates,
        skew_df=skew_df,
        target_n=int(params["target_n"]),
        window_sessions=int(params["window_sessions"]),
        rebuild=rebuild_pit_cache,
    )
    table = table[
        table["date"].isin(pd.to_datetime(pd.Series(dates)).dt.normalize())
    ].copy()
    members: dict[date, set[str]] = {}
    for dt, grp in table.groupby("date"):
        members[pd.Timestamp(dt).date()] = set(
            grp.sort_values("rank")["ticker"].astype(str).str.upper().tolist()
        )
    for d in dates:
        members.setdefault(d, set())
    forced_missing_dates: list[str] = []
    if force_include_ticker:
        for d in dates:
            if force_include_ticker not in members[d]:
                forced_missing_dates.append(d.isoformat())
            members[d].add(force_include_ticker)

    union_tickers = sorted({t for tickers in members.values() for t in tickers})
    avg_allowed = float(np.mean([len(v) for v in members.values()])) if members else 0.0
    return members, {
        "selector": selector,
        "base_universe_size": len(core_tickers),
        "implemented_universe_size": len(union_tickers),
        "avg_allowed_universe_size": avg_allowed,
        "selector_timing_seconds": round(time.perf_counter() - t0, 3),
        "pit_cache_hit": bool(cache_meta.get("cache_hit", False)),
        "pit_cache_rows": int(cache_meta.get("rows", 0)),
        "pit_target_n": int(params["target_n"]),
        "pit_window_sessions": int(params["window_sessions"]),
        "held_force_include_ticker": force_include_ticker,
        "held_force_include_reason": force_include_reason,
        "held_force_include_missing_dates": forced_missing_dates,
    }


# ---------------------------------------------------------------------------
# Step 1: bars
# ---------------------------------------------------------------------------

def refresh_bars(target: date) -> None:
    """Download missing stock day_aggs through `target` and re-apply splits.

    We delegate to the existing massive_ingest.py CLI: --start picks the
    day after the last bar in aggs_daily.parquet, --end is target, and
    --apply-splits-only refreshes aggs_daily_adjusted.parquet.
    """
    import subprocess

    # Find last bar date
    if STOCKS_PARQUET.exists():
        bars = pd.read_parquet(STOCKS_PARQUET, columns=["date"])
        last = pd.to_datetime(bars["date"]).max().date()
    else:
        last = None
    start = (last + timedelta(days=1)) if last else (target - timedelta(days=400))
    if start > target:
        print(f"[bars] up to date (last={last}, target={target})")
        return

    print(f"[bars] downloading {start} → {target}")
    venv_py = REPO / ".venv" / "bin" / "python"
    cmd = [str(venv_py), str(BACKTEST_DIR / "massive_ingest.py"),
           "--start", start.isoformat(), "--end", target.isoformat()]
    t0 = time.time()
    raw_mtime_before = _mtime(RAW_BARS_PARQUET)
    subprocess.run(cmd, check=True, cwd=str(REPO))
    print(f"[bars] download+merge in {time.time()-t0:.1f}s")

    # Skip the 8-min splits rebuild unless an input actually changed.
    # The adjusted parquet is a pure function of (raw bars, splits); if both
    # files are older than the adjusted output, the existing adjustment is
    # still correct.
    raw_changed    = _mtime(RAW_BARS_PARQUET) > raw_mtime_before
    adj_mtime      = _mtime(STOCKS_PARQUET)
    splits_newer   = _mtime(SPLITS_PARQUET) > adj_mtime
    raw_newer_adj  = _mtime(RAW_BARS_PARQUET) > adj_mtime
    if not (raw_changed or raw_newer_adj or splits_newer):
        print(f"[bars] adjusted parquet up-to-date "
              f"(raw mtime <= adj mtime, splits mtime <= adj mtime); skipping rebuild")
        return

    t0 = time.time()
    cmd = [str(venv_py), str(BACKTEST_DIR / "massive_ingest.py"), "--apply-splits-only"]
    subprocess.run(cmd, check=True, cwd=str(REPO))
    print(f"[bars] splits applied in {time.time()-t0:.1f}s")


# ---------------------------------------------------------------------------
# Step 2: options — incremental
# ---------------------------------------------------------------------------

def _parse_one_options_file(
    path: Path,
    allowed_underlyings: set[str],
    *,
    return_metrics: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, dict]:
    """Same logic as massive_options_ingest.merge_to_parquet, applied to one
    daily file. Returns the filtered + normalized rows for that day."""
    df = pd.read_csv(path, compression="gzip")
    raw_rows = int(len(df))
    if not EXPECTED_COLUMNS.issubset(df.columns):
        empty = pd.DataFrame()
        metrics = {
            "raw_rows": raw_rows,
            "parsed_rows": 0,
            "kept_rows": 0,
        }
        return (empty, metrics) if return_metrics else empty
    parsed = df["ticker"].apply(parse_opra_ticker)
    ok = parsed.notna()
    df = df[ok].copy()
    parsed_rows = int(len(df))
    df[["underlying", "expiry", "cp", "strike"]] = pd.DataFrame(
        parsed[ok].tolist(), index=df.index
    )
    df = df[df["underlying"].isin(allowed_underlyings)]
    df["date"] = (
        pd.to_datetime(df["window_start"], unit="ns", utc=True)
          .dt.tz_convert("America/New_York")
          .dt.normalize()
          .dt.tz_localize(None)
    )
    df["expiry"] = pd.to_datetime(df["expiry"])
    cols = ["underlying", "date", "expiry", "cp", "strike",
            "ticker", "open", "high", "low", "close", "volume"]
    if "transactions" in df.columns:
        cols.append("transactions")
    df = df[cols].dropna(subset=["underlying", "date", "expiry", "close"])
    metrics = {
        "raw_rows": raw_rows,
        "parsed_rows": parsed_rows,
        "kept_rows": int(len(df)),
    }
    return (df, metrics) if return_metrics else df


def refresh_options(
    target: date,
    *,
    write_monolith: bool = False,
    allowed_underlyings_by_date: dict[date, set[str]] | None = None,
    return_metrics: bool = False,
) -> tuple[set[date], pd.DataFrame] | tuple[set[date], pd.DataFrame, dict]:
    """Download missing options daily files and parse them. By default we
    return the parsed DataFrame WITHOUT writing back to options_daily.parquet
    — the production signal pipeline reads from skew_daily.parquet, not the
    options monolith, and the 2.6 GB rewrite costs ~3 min/day on this machine.

    Pass `write_monolith=True` to keep options_daily.parquet current (e.g.
    if a backtest cold-run is queued behind this refresh).

    Returns: (new_dates, new_rows_df). new_rows_df is empty when nothing new.
    Source of truth for "are we current" is skew_daily.parquet, not the
    options monolith — the monolith may drift since we stop rewriting it.
    """
    # Use skew as the staleness gate (source of truth). The options monolith
    # may legitimately be stale because we no longer rewrite it daily.
    if SKEW_PARQUET.exists():
        skew_dates_col = pd.read_parquet(SKEW_PARQUET, columns=["date"])
        skew_dates = set(pd.to_datetime(skew_dates_col["date"]).dt.date.unique())
        last = max(skew_dates) if skew_dates else None
        existing_dates = skew_dates
    else:
        existing_dates = set()
        last = None

    if last and last >= target:
        print(f"[opt] up to date (last={last}, target={target})")
        empty = (set(), pd.DataFrame())
        metrics = {
            "download_seconds": 0.0,
            "parse_seconds": 0.0,
            "new_dates": 0,
            "files_parsed": 0,
            "rows_raw": 0,
            "rows_parsed": 0,
            "rows_kept": 0,
            "avg_allowed_underlyings": 0.0,
        }
        return (*empty, metrics) if return_metrics else empty

    start = (last + timedelta(days=1)) if last else (target - timedelta(days=400))
    print(f"[opt] downloading {start} → {target}")
    t0 = time.time()
    download_window(start, target, max_workers=10)
    download_seconds = time.time() - t0
    print(f"[opt] download in {download_seconds:.1f}s")

    # Find the actual new daily files (download_window writes into RAW_DIR).
    # NOTE: pathlib's `.stem` only strips the last suffix, so `2026-05-01.csv.gz`
    # → `2026-05-01.csv`. Use `name[:10]` to extract the date string.
    def _date_from_filename(p: Path) -> date | None:
        try:
            return date.fromisoformat(p.name[:10])
        except ValueError:
            return None

    new_files = []
    for p in OPT_RAW_DIR.glob("*.csv.gz"):
        d = _date_from_filename(p)
        if d is None or d < start or d > target:
            continue
        if d in existing_dates:
            continue
        if p.stat().st_size <= 0:
            continue
        new_files.append(p)
    new_files.sort()
    if not new_files:
        print("[opt] no new files to merge")
        empty = (set(), pd.DataFrame())
        metrics = {
            "download_seconds": round(download_seconds, 3),
            "parse_seconds": 0.0,
            "new_dates": 0,
            "files_parsed": 0,
            "rows_raw": 0,
            "rows_parsed": 0,
            "rows_kept": 0,
            "avg_allowed_underlyings": 0.0,
        }
        return (*empty, metrics) if return_metrics else empty
    print(f"[opt] parsing {len(new_files)} new daily files")

    t0 = time.time()
    if allowed_underlyings_by_date is None:
        base_allowed = allowed_ticker_set(
            require_type="CS", exclude_pharma_biotech=True, require_optionable=True,
        )
        allowed_underlyings_by_date = {
            _date_from_filename(p): set(base_allowed)
            for p in new_files
            if _date_from_filename(p) is not None
        }
    allowed_sizes = [len(v) for v in allowed_underlyings_by_date.values() if v]
    avg_allowed_size = float(np.mean(allowed_sizes)) if allowed_sizes else 0.0
    print(f"[opt] filtering to ~{avg_allowed_size:,.0f} allowed underlyings/day")

    parsed_frames: list[pd.DataFrame] = []
    raw_rows = 0
    parsed_rows = 0
    kept_rows = 0
    for p in new_files:
        d = _date_from_filename(p)
        allowed = allowed_underlyings_by_date.get(d, set()) if d is not None else set()
        df_day, stats = _parse_one_options_file(p, allowed, return_metrics=True)
        raw_rows += int(stats["raw_rows"])
        parsed_rows += int(stats["parsed_rows"])
        kept_rows += int(stats["kept_rows"])
        if not df_day.empty:
            parsed_frames.append(df_day)
    new_df = pd.concat(parsed_frames, ignore_index=True) if parsed_frames else pd.DataFrame()
    parse_seconds = time.time() - t0
    if new_df.empty:
        print("[opt] no new rows produced (holiday window?)")
        empty = (set(), pd.DataFrame())
        metrics = {
            "download_seconds": round(download_seconds, 3),
            "parse_seconds": round(parse_seconds, 3),
            "new_dates": 0,
            "files_parsed": len(new_files),
            "rows_raw": raw_rows,
            "rows_parsed": parsed_rows,
            "rows_kept": kept_rows,
            "avg_allowed_underlyings": round(avg_allowed_size, 2),
        }
        return (*empty, metrics) if return_metrics else empty

    new_dates = set(pd.to_datetime(new_df["date"]).dt.date.unique())
    print(f"[opt] {len(new_df):,} new rows from {len(new_dates)} new dates "
          f"in {parse_seconds:.1f}s")

    if write_monolith and OPTIONS_PARQUET.exists():
        # Cold-run-compat append. Costs ~3 min/day at current size.
        t0 = time.time()
        existing = pd.read_parquet(OPTIONS_PARQUET)
        merged = pd.concat([existing, new_df], ignore_index=True)
        merged = merged.drop_duplicates(subset=["underlying", "date", "expiry", "cp", "strike"])
        merged = merged.sort_values(["underlying", "date"]).reset_index(drop=True)
        merged.to_parquet(OPTIONS_PARQUET, index=False)
        print(f"[opt] appended → {OPTIONS_PARQUET.name} "
              f"({len(merged):,} rows total, write {time.time()-t0:.1f}s)")
    elif write_monolith:
        new_df.to_parquet(OPTIONS_PARQUET, index=False)
        print(f"[opt] wrote first parquet → {OPTIONS_PARQUET.name} ({len(new_df):,} rows)")
    else:
        print(f"[opt] skipping options_daily.parquet rewrite "
              f"(use --write-options-monolith to update for backtests)")

    metrics = {
        "download_seconds": round(download_seconds, 3),
        "parse_seconds": round(parse_seconds, 3),
        "new_dates": len(new_dates),
        "files_parsed": len(new_files),
        "rows_raw": raw_rows,
        "rows_parsed": parsed_rows,
        "rows_kept": kept_rows,
        "avg_allowed_underlyings": round(avg_allowed_size, 2),
    }
    if return_metrics:
        return new_dates, new_df, metrics
    return new_dates, new_df


# ---------------------------------------------------------------------------
# Step 3: skew — incremental
# ---------------------------------------------------------------------------

def _compute_skew_rows_for_options_df(opts_new: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Compute skew rows from an already-filtered options frame, without any
    append/write side effects."""
    if opts_new.empty:
        return pd.DataFrame(), {
            "load_stocks_seconds": 0.0,
            "compute_iv_seconds": 0.0,
            "option_rows_input": 0,
            "stock_rows_loaded": 0,
            "underlyings_to_process": 0,
            "skew_rows_new": 0,
            "iv_pairs_est": 0,
        }

    underlyings_to_process = sorted(opts_new["underlying"].astype(str).str.upper().unique())

    t0 = time.time()
    stocks = pd.read_parquet(
        STOCKS_PARQUET,
        filters=[("ticker", "in", underlyings_to_process)],
    )
    stocks["date"] = pd.to_datetime(stocks["date"])
    stocks = stocks[stocks["ticker"].isin(set(underlyings_to_process))]
    stocks_indexed = stocks.set_index("ticker")
    load_stocks_seconds = time.time() - t0
    print(f"[skew] {len(stocks):,} stock rows for these underlyings")

    t0 = time.time()
    new_rows = []
    for u, g in opts_new.groupby("underlying", sort=False):
        try:
            spot_df = stocks_indexed.loc[u]
            if isinstance(spot_df, pd.Series):
                spot_df = spot_df.to_frame().T
            spot_u = spot_df.set_index("date")["close"]
        except (KeyError, AttributeError):
            continue
        if spot_u.empty:
            continue
        skew = compute_skew_for_underlying(g, spot_u)
        if not skew.empty:
            skew["underlying"] = u
            new_rows.append(skew)

    if not new_rows:
        return pd.DataFrame(), {
            "load_stocks_seconds": round(load_stocks_seconds, 3),
            "compute_iv_seconds": round(time.time() - t0, 3),
            "option_rows_input": int(len(opts_new)),
            "stock_rows_loaded": int(len(stocks)),
            "underlyings_to_process": len(underlyings_to_process),
            "skew_rows_new": 0,
            "iv_pairs_est": 0,
        }

    new_skew = pd.concat(new_rows, ignore_index=True)
    cols = [
        "underlying", "date", "atm_spot", "expiry_used", "days_to_exp",
        "call_strike_5otm", "put_strike_5otm",
        "call_iv_5otm", "put_iv_5otm", "skew_5otm",
        "call_iv_atm", "put_iv_atm",
    ]
    new_skew = new_skew[cols]
    compute_iv_seconds = time.time() - t0
    print(f"[skew] computed {len(new_skew):,} new skew rows in {compute_iv_seconds:.1f}s")
    return new_skew, {
        "load_stocks_seconds": round(load_stocks_seconds, 3),
        "compute_iv_seconds": round(compute_iv_seconds, 3),
        "option_rows_input": int(len(opts_new)),
        "stock_rows_loaded": int(len(stocks)),
        "underlyings_to_process": len(underlyings_to_process),
        "skew_rows_new": int(len(new_skew)),
        "iv_pairs_est": int(2 * len(new_skew)),
    }


def refresh_skew(
    new_dates: set[date],
    new_options_df: pd.DataFrame | None = None,
    *,
    return_metrics: bool = False,
) -> int | tuple[int, dict]:
    """Compute skew_5otm for new dates and APPEND to skew_daily.parquet.

    `new_options_df` (optional) lets the caller hand off the just-parsed
    options rows so we skip re-reading them from options_daily.parquet.
    This is the production path. If None, falls back to reading from the
    options monolith (for one-off invocations).

    Skips (u, d) pairs already in skew_daily — IV computation legitimately
    produces no row when there's no liquid OTM chain, and we never want to
    re-attempt the same pair.
    """
    if not new_dates:
        print("[skew] no new options dates — nothing to compute")
        metrics = {
            "load_options_seconds": 0.0,
            "filter_options_seconds": 0.0,
            "load_stocks_seconds": 0.0,
            "compute_iv_seconds": 0.0,
            "write_outputs_seconds": 0.0,
            "option_rows_after_dedupe": 0,
            "underlyings_to_process": 0,
            "skew_rows_new": 0,
            "iv_pairs_est": 0,
        }
        return (0, metrics) if return_metrics else 0
    if not STOCKS_PARQUET.exists():
        raise SystemExit("[skew] stocks parquet missing — run refresh_bars first")

    new_dates_ts = {pd.Timestamp(d) for d in new_dates}

    # Build the set of (underlying, date) pairs that already have skew rows
    if SKEW_PARQUET.exists():
        existing_skew = pd.read_parquet(SKEW_PARQUET)
        existing_skew["date"] = pd.to_datetime(existing_skew["date"])
        in_window = existing_skew[existing_skew["date"].isin(new_dates_ts)]
        already = set(zip(in_window["underlying"], in_window["date"]))
    else:
        existing_skew = pd.DataFrame()
        already = set()

    # Source the new options data either from the caller or from the parquet
    t0 = time.time()
    if new_options_df is not None and not new_options_df.empty:
        opts_new = new_options_df.copy()
        opts_new["date"]   = pd.to_datetime(opts_new["date"])
        opts_new["expiry"] = pd.to_datetime(opts_new["expiry"])
        print(f"[skew] using {len(opts_new):,} options rows handed off from refresh_options "
              f"({time.time()-t0:.1f}s)")
    else:
        if not OPTIONS_PARQUET.exists():
            raise SystemExit("[skew] options parquet missing and no df handed off")
        opts_new = pd.read_parquet(
            OPTIONS_PARQUET, filters=[("date", "in", list(new_dates_ts))],
        )
        opts_new["date"]   = pd.to_datetime(opts_new["date"])
        opts_new["expiry"] = pd.to_datetime(opts_new["expiry"])
        print(f"[skew] read {len(opts_new):,} option rows from monolith "
              f"({time.time()-t0:.1f}s)")
    load_options_seconds = time.time() - t0

    # Drop pairs we've already attempted
    t0 = time.time()
    pairs = list(zip(opts_new["underlying"], opts_new["date"]))
    keep_mask = pd.Series([p not in already for p in pairs], index=opts_new.index)
    opts_new = opts_new[keep_mask]
    underlyings_to_process = sorted(opts_new["underlying"].unique())
    print(f"[skew] {len(opts_new):,} option rows after dedupe "
          f"({len(underlyings_to_process):,} underlyings to process)")
    filter_options_seconds = time.time() - t0
    if opts_new.empty:
        print("[skew] all (underlying, date) pairs already attempted")
        metrics = {
            "load_options_seconds": round(load_options_seconds, 3),
            "filter_options_seconds": round(filter_options_seconds, 3),
            "load_stocks_seconds": 0.0,
            "compute_iv_seconds": 0.0,
            "write_outputs_seconds": 0.0,
            "option_rows_after_dedupe": 0,
            "underlyings_to_process": 0,
            "skew_rows_new": 0,
            "iv_pairs_est": 0,
        }
        return (0, metrics) if return_metrics else 0

    new_skew, compute_metrics = _compute_skew_rows_for_options_df(opts_new)
    if new_skew.empty:
        print("[skew] no new skew rows produced")
        metrics = {
            "load_options_seconds": round(load_options_seconds, 3),
            "filter_options_seconds": round(filter_options_seconds, 3),
            "load_stocks_seconds": compute_metrics["load_stocks_seconds"],
            "compute_iv_seconds": compute_metrics["compute_iv_seconds"],
            "write_outputs_seconds": 0.0,
            "option_rows_after_dedupe": int(len(opts_new)),
            "underlyings_to_process": int(compute_metrics["underlyings_to_process"]),
            "skew_rows_new": 0,
            "iv_pairs_est": 0,
        }
        return (0, metrics) if return_metrics else 0

    # Append + dedupe
    t0 = time.time()
    if not existing_skew.empty:
        merged = pd.concat([existing_skew, new_skew], ignore_index=True)
        merged = merged.drop_duplicates(subset=["underlying", "date"], keep="last")
        merged = merged.sort_values(["underlying", "date"]).reset_index(drop=True)
    else:
        merged = new_skew.sort_values(["underlying", "date"]).reset_index(drop=True)
    merged.to_parquet(SKEW_PARQUET, index=False)
    write_outputs_seconds = time.time() - t0
    print(f"[skew] wrote {SKEW_PARQUET.name} ({len(merged):,} rows total)")
    metrics = {
        "load_options_seconds": round(load_options_seconds, 3),
        "filter_options_seconds": round(filter_options_seconds, 3),
        "load_stocks_seconds": compute_metrics["load_stocks_seconds"],
        "compute_iv_seconds": compute_metrics["compute_iv_seconds"],
        "write_outputs_seconds": round(write_outputs_seconds, 3),
        "option_rows_after_dedupe": int(len(opts_new)),
        "underlyings_to_process": int(compute_metrics["underlyings_to_process"]),
        "skew_rows_new": int(len(new_skew)),
        "iv_pairs_est": int(compute_metrics["iv_pairs_est"]),
    }
    return (len(new_skew), metrics) if return_metrics else len(new_skew)


# ---------------------------------------------------------------------------
# Step 4: compute today's signal
# ---------------------------------------------------------------------------

def _load_or_build_top2000_cache() -> set[str]:
    """Top-2000 by median dollar volume. The set only changes when new bars
    arrive (or rarely, when ticker metadata changes), so we cache it as a
    1-column parquet and rebuild only when STOCKS_PARQUET is newer.
    """
    if (
        TOP2000_CACHE.exists()
        and _mtime(TOP2000_CACHE) >= _mtime(STOCKS_PARQUET)
        and _mtime(TOP2000_CACHE) >= _mtime(META_PARQUET)
    ):
        cached = pd.read_parquet(TOP2000_CACHE)
        return set(cached["ticker"].tolist())

    t0 = time.time()
    bars = pd.read_parquet(
        STOCKS_PARQUET, columns=["ticker", "close", "volume"]
    )
    bars_by_tkr = {tkr: g for tkr, g in bars.groupby("ticker")}
    meta = pd.read_parquet(META_PARQUET)
    top2000_list = build_static_universe_top_n(
        bars_by_tkr, meta, top_n=UNIVERSE_TOP_N
    )
    pd.DataFrame({"ticker": top2000_list}).to_parquet(TOP2000_CACHE, index=False)
    print(f"[universe] rebuilt top-2000 cache "
          f"({len(top2000_list)} tickers, {time.time()-t0:.1f}s)")
    return set(top2000_list)


def compute_signal(
    target: date,
    *,
    allowed_tickers: set[str] | None = None,
    allowed_tickers_by_date: dict[date, set[str]] | None = None,
    universe_selector: str = UNIVERSE_SELECTOR_CORE,
    universe_meta: dict | None = None,
    held_context: dict | None = None,
    return_metrics: bool = False,
) -> dict | tuple[dict, dict]:
    """Build signal_today payload: regime, top-2000 universe, z>=3 candidates."""
    target_ts = pd.Timestamp(target)
    phase_metrics: dict[str, float | int] = {}
    # Read full history — the rolling(60) stat at any row picks the prior 60
    # ROWS for that underlying, and underlyings with sparse skew can have those
    # rows reach further back than a 60-trading-day calendar window. A trim
    # here causes z-score divergence vs. replay.py.
    t0 = time.time()
    sk = pd.read_parquet(
        SKEW_PARQUET,
        columns=["underlying", "date", "skew_5otm", "atm_spot", "days_to_exp",
                 "expiry_used", "call_strike_5otm", "put_strike_5otm",
                 "call_iv_5otm", "put_iv_5otm"],
    ).sort_values(["underlying", "date"]).reset_index(drop=True)
    sk["date"] = pd.to_datetime(sk["date"])
    phase_metrics["phase_load_skew_seconds"] = round(time.time() - t0, 3)

    # Lookahead-safe rolling z (replay.py logic exactly: shift(1) before rolling)
    t0 = time.time()
    sk["skew_lag"] = sk.groupby("underlying")["skew_5otm"].shift(1)
    sk["mu"] = sk.groupby("underlying")["skew_lag"].rolling(
        ROLLING_DAYS, min_periods=ROLLING_MIN
    ).mean().reset_index(level=0, drop=True)
    sk["sd"] = sk.groupby("underlying")["skew_lag"].rolling(
        ROLLING_DAYS, min_periods=ROLLING_MIN
    ).std().reset_index(level=0, drop=True)
    sk["z"] = (sk["skew_5otm"] - sk["mu"]) / sk["sd"]
    phase_metrics["phase_build_z_seconds"] = round(time.time() - t0, 3)

    todays = sk[sk["date"] == target_ts].copy()
    if todays.empty:
        # Fall back to most recent available
        actual_last = sk["date"].max()
        print(f"[signal] no skew for {target}, using {actual_last.date()}")
        target_ts = actual_last
        todays = sk[sk["date"] == target_ts].copy()

    # Top-2000 or PIT universe membership for the signal date.
    t0 = time.time()
    if allowed_tickers_by_date is not None:
        signal_universe = set(allowed_tickers_by_date.get(target_ts.date(), set()))
    elif allowed_tickers is not None:
        signal_universe = set(allowed_tickers)
    else:
        signal_universe = _load_or_build_top2000_cache()
    phase_metrics["phase_build_universe_seconds"] = round(time.time() - t0, 3)

    in_uni = todays[todays["underlying"].isin(signal_universe)].copy()
    z_passing = in_uni[in_uni["z"] >= Z_MIN].sort_values("z", ascending=False)

    # Trend / liquidity entry filters — same code path as
    # replay.py:_path_s_skew_flip + stability.rank_universe. Without these,
    # data_refresh emits names the backtest engine would never have entered
    # (falling-knife z-flips on broken trends). Only run on the z-passing set
    # (typically <30 tickers), so cost is negligible.
    cand_tickers = list(z_passing["underlying"].unique())
    t0 = time.time()
    if cand_tickers:
        cand_bars = pd.read_parquet(
            STOCKS_PARQUET,
            columns=["ticker", "date", "open", "high", "low", "close", "volume"],
            filters=[("ticker", "in", cand_tickers)],
        )
        cand_bars["date"] = pd.to_datetime(cand_bars["date"])
        cand_bars = cand_bars[cand_bars["date"] <= target_ts]
    else:
        cand_bars = pd.DataFrame()
    phase_metrics["phase_load_trend_inputs_seconds"] = round(time.time() - t0, 3)

    eligible_trend_status = set(CANONICAL["filter_eligible_trend_status_in"])
    min_dvol  = CANONICAL["filter_eligible_min_dollar_vol"]
    min_r12m  = CANONICAL["filter_eligible_min_ret_12m"]
    max_r12m  = CANONICAL["filter_eligible_max_ret_12m"]

    def _filter_one(ticker: str) -> tuple[bool, str | None, dict]:
        """Returns (passes, reject_reason, factor_trace)."""
        g = cand_bars[cand_bars["ticker"] == ticker].sort_values("date").reset_index(drop=True)
        f = compute_stability_factors(ticker, g)
        if f is None:
            return False, "insufficient_bars", {}
        trace = {
            "above_50d_sma":     f.above_50d_sma,
            "recent_60d_ret":    None if f.recent_60d_ret is None else round(float(f.recent_60d_ret), 4),
            "ret_12m":           None if f.ret_12m is None else round(float(f.ret_12m), 4),
            "dollar_vol_20d":    None if f.dollar_vol_20d is None else round(float(f.dollar_vol_20d), 0),
            "trend_status":      f.trend_status,
        }
        if CANONICAL["filter_require_above_50d_sma"] and not f.above_50d_sma:
            return False, "below_50d_sma", trace
        if CANONICAL["filter_require_ret_60d_positive"]:
            if f.recent_60d_ret is None or f.recent_60d_ret <= 0:
                return False, "ret_60d_not_positive", trace
        if CANONICAL["filter_require_eligible_flyer"]:
            if f.dollar_vol_20d is None or f.dollar_vol_20d < min_dvol:
                return False, "dollar_vol_below_floor", trace
            if f.ret_12m is None or f.ret_12m < min_r12m or f.ret_12m > max_r12m:
                return False, "ret_12m_out_of_band", trace
            if f.trend_status not in eligible_trend_status:
                return False, "trend_status_not_eligible", trace
        return True, None, trace

    t0 = time.time()
    qualified_rows = []
    rejected_rows = []
    for _, r in z_passing.iterrows():
        passes, reason, trace = _filter_one(str(r["underlying"]))
        rec = {"row": r, "trace": trace, "reject_reason": reason}
        (qualified_rows if passes else rejected_rows).append(rec)

    qualified = (
        z_passing[z_passing["underlying"].isin([rec["row"]["underlying"] for rec in qualified_rows])]
                  .sort_values("z", ascending=False)
        if qualified_rows else z_passing.iloc[0:0]
    )
    # Index trace by ticker for lookup when building the output dicts
    trace_by_tkr = {rec["row"]["underlying"]: rec["trace"] for rec in qualified_rows + rejected_rows}
    reason_by_tkr = {rec["row"]["underlying"]: rec["reject_reason"] for rec in rejected_rows}
    phase_metrics["phase_filter_candidates_seconds"] = round(time.time() - t0, 3)

    # SPY 200d regime gate (only need SPY's last 200 closes)
    t0 = time.time()
    spy = pd.read_parquet(
        STOCKS_PARQUET,
        columns=["ticker", "date", "close"],
        filters=[("ticker", "==", "SPY")],
    )
    spy["date"] = pd.to_datetime(spy["date"])
    spy = spy[spy["date"] <= target_ts].sort_values("date")
    spy_close = float(spy.iloc[-1]["close"])
    spy_ma200 = float(spy["close"].tail(200).mean())
    regime_open = spy_close > spy_ma200
    phase_metrics["phase_load_regime_seconds"] = round(time.time() - t0, 3)

    skew_max_date = sk["date"].max().date()
    requested_target = target  # before any fallback
    actual_as_of = target_ts.date()
    held_force_include_ticker = None
    held_force_included = False
    held_force_reason = None
    if held_context:
        held_force_include_ticker = held_context.get("held_ticker")
    if universe_meta and held_force_include_ticker:
        missing_dates = set(universe_meta.get("held_force_include_missing_dates", []) or [])
        held_force_included = target_ts.date().isoformat() in missing_dates
        held_force_reason = universe_meta.get("held_force_include_reason")
    t0 = time.time()
    payload = {
        "as_of": str(actual_as_of),
        "signal_config": signal_config_block(),
        "config_hash": CANONICAL_HASH,
        "universe_selector": universe_selector,
        "held_force_include": {
            "held_ticker": held_force_include_ticker,
            "held_force_included": held_force_included,
            "held_force_include_reason": held_force_reason if held_force_included else None,
            "target_universe_has_held_ticker": (
                bool(held_force_include_ticker and held_force_include_ticker in signal_universe)
            ),
        },
        "freshness": {
            "requested_target":   str(requested_target),
            "actual_as_of":       str(actual_as_of),
            "skew_data_max_date": str(skew_max_date),
            "fallback_used":      requested_target != actual_as_of,
        },
        "regime": {
            "spy_close": round(spy_close, 4),
            "spy_200d": round(spy_ma200, 4),
            "open": regime_open,
            "ma_window_trading_days": CANONICAL["regime_ma_window"],
            "evaluation_basis": CANONICAL["regime_evaluation_basis"],
            "symbol": CANONICAL["regime_symbol"],
        },
        "universe_size": len(signal_universe),
        "tickers_with_skew_today": int(len(in_uni)),
        "z_threshold": Z_MIN,
        "z_compare_op": CANONICAL["z_compare_op"],
        "n_z_passing": int(len(z_passing)),
        "n_candidates": int(len(qualified)),
        "candidates": [
            {
                "rank": i + 1,
                "ticker": str(r["underlying"]),
                "z": round(float(r["z"]), 4),
                # Skew measure (full contract identity)
                "skew_5otm": round(float(r["skew_5otm"]), 6),
                "spot": round(float(r["atm_spot"]), 4),
                "expiry": str(pd.Timestamp(r["expiry_used"]).date()),
                "days_to_exp": int(r["days_to_exp"]),
                "call_strike_5otm": (None if pd.isna(r["call_strike_5otm"])
                                     else round(float(r["call_strike_5otm"]), 4)),
                "put_strike_5otm": (None if pd.isna(r["put_strike_5otm"])
                                    else round(float(r["put_strike_5otm"]), 4)),
                "call_iv_5otm": (None if pd.isna(r["call_iv_5otm"])
                                 else round(float(r["call_iv_5otm"]), 6)),
                "put_iv_5otm": (None if pd.isna(r["put_iv_5otm"])
                                else round(float(r["put_iv_5otm"]), 6)),
                "filter_trace": trace_by_tkr.get(r["underlying"], {}),
            }
            for i, (_, r) in enumerate(qualified.iterrows())
        ],
        "candidates_rejected": [
            {
                "ticker": str(r["underlying"]),
                "z": round(float(r["z"]), 4),
                "reject_reason": reason_by_tkr.get(r["underlying"]),
                "filter_trace": trace_by_tkr.get(r["underlying"], {}),
            }
            for _, r in z_passing.iterrows()
            if r["underlying"] in reason_by_tkr
        ],
        "watchlist_below": [
            {
                "ticker": str(r["underlying"]),
                "z": round(float(r["z"]), 4),
            }
            for _, r in in_uni[(in_uni["z"] >= 2.5) & (in_uni["z"] < Z_MIN)]
                .sort_values("z", ascending=False).head(5).iterrows()
        ],
    }
    if universe_meta:
        payload["universe_meta"] = universe_meta
    phase_metrics["phase_build_payload_seconds"] = round(time.time() - t0, 3)
    if return_metrics:
        return payload, phase_metrics
    return payload


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _load_dotenv() -> None:
    env_path = REPO / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--as-of", type=str, default=None,
                    help="target trading date YYYY-MM-DD (default: today)")
    ap.add_argument("--no-bars", action="store_true",
                    help="skip bars refresh (assume already current)")
    ap.add_argument("--no-options", action="store_true",
                    help="skip options refresh")
    ap.add_argument("--no-skew", action="store_true",
                    help="skip skew refresh")
    ap.add_argument("--signal-only", action="store_true",
                    help="only compute signal from existing parquets")
    ap.add_argument("--write-options-monolith", action="store_true",
                    help="rewrite the 2.6GB options_daily.parquet so cold-run "
                         "backtests stay current. Adds ~3 min/day. Skip in "
                         "production — signal compute reads skew, not options.")
    ap.add_argument(
        "--universe-selector",
        choices=UNIVERSE_SELECTOR_CHOICES,
        default=UNIVERSE_SELECTOR_CORE,
        help="signal/data universe to apply during refresh (default: core_2000)",
    )
    ap.add_argument(
        "--rebuild-pit-cache",
        action="store_true",
        help="force PIT universe cache regeneration before using a PIT selector",
    )
    ap.add_argument("--force", action="store_true",
                    help="bypass idempotent fast-path; redownload + recompute "
                         "even if signal_today.json already matches target")
    args = ap.parse_args()

    _load_dotenv()
    held_context = _load_current_position_context()

    target = (date.fromisoformat(args.as_of) if args.as_of
              else datetime.now().date())
    overall_t0 = time.time()
    print(f"=== Path-S data refresh — target {target} ===")

    # Fail-closed BEFORE any expensive work. If the canonical constants
    # have drifted, refuse — better to miss a signal than write a bad one.
    assert_signal_config_canonical()
    print(f"[config] z_window={ROLLING_DAYS}, rolling_min={ROLLING_MIN}, "
          f"z_threshold={Z_MIN}, universe_top_n={UNIVERSE_TOP_N} — canonical ✓")
    print(f"[config] universe_selector={args.universe_selector}")

    # Fast-path: if the existing signal already matches target and downstream
    # parquets haven't changed since it was written, just re-print and exit.
    if not args.force and not args.signal_only and SIGNAL_OUT.exists():
        fast_path = _signal_fast_path_status(
            signal_path=SIGNAL_OUT,
            state_path=STATE_PATH,
            selector=args.universe_selector,
            target=target,
        )
        existing = fast_path.get("payload")
        if fast_path["eligible"] and existing is not None:
            sig_as_of = date.fromisoformat(existing["as_of"])
            print(f"[fast-path] signal_today.json already as_of {sig_as_of} "
                  f"(>= target {target}) and inputs unchanged; skipping refresh.")
            print(f"            pass --force to override.")
            _print_signal(existing)
            print(f"\n=== Total wall time: {time.time()-overall_t0:.1f}s ===")
            return
        if fast_path["reason"] == "selector_mismatch" and existing is not None:
            print(f"[fast-path] signal file is fresh, but selector mismatch "
                  f"({existing.get('universe_selector')} != {args.universe_selector}); recomputing.")

    selector_dates = {target}
    allowed_by_date: dict[date, set[str]] = {}
    selector_meta: dict[str, object] = {
        "selector": args.universe_selector,
    }
    new_dates: set[date] = set()
    new_options_df: pd.DataFrame | None = None
    pipeline_profile: dict[str, object] = {
        "universe_selector": args.universe_selector,
    }
    if not args.signal_only:
        if not args.no_bars:
            t0 = time.time()
            refresh_bars(target)
            print(f"[bars] step total {time.time()-t0:.1f}s\n")
        if SKEW_PARQUET.exists():
            skew_dates_col = pd.read_parquet(SKEW_PARQUET, columns=["date"])
            skew_dates = set(pd.to_datetime(skew_dates_col["date"]).dt.date.unique())
            last_skew = max(skew_dates) if skew_dates else None
        else:
            last_skew = None
        planned_start = (last_skew + timedelta(days=1)) if last_skew else (target - timedelta(days=400))
        selector_dates.update(_enumerate_weekdays(planned_start, target))
    allowed_by_date, selector_meta = _resolve_universe_selector(
        args.universe_selector,
        selector_dates,
        force_include_ticker=held_context.get("held_ticker"),
        force_include_reason=held_context.get("held_force_include_reason"),
        rebuild_pit_cache=args.rebuild_pit_cache,
    )
    pipeline_profile["selector_meta"] = selector_meta
    pipeline_profile["held_context"] = held_context
    if selector_meta.get("implemented_universe_size") is not None:
        print(
            "[universe] "
            f"base={selector_meta.get('base_universe_size', 0):,} "
            f"implemented={selector_meta.get('implemented_universe_size', 0):,} "
            f"avg_allowed={selector_meta.get('avg_allowed_universe_size', 0.0):.1f} "
            f"(selector {selector_meta.get('selector_timing_seconds', 0.0):.1f}s)"
        )
        if held_context.get("held_ticker"):
            missing_dates = selector_meta.get("held_force_include_missing_dates", []) or []
            print(
                f"[universe] held ticker {held_context['held_ticker']} "
                f"force-included on {len(missing_dates)} date(s)"
            )
    if not args.signal_only:
        if not args.no_options:
            t0 = time.time()
            new_dates, new_options_df, opt_metrics = refresh_options(
                target,
                write_monolith=args.write_options_monolith,
                allowed_underlyings_by_date=allowed_by_date,
                return_metrics=True,
            )
            pipeline_profile["options"] = opt_metrics
            print(f"[opt] step total {time.time()-t0:.1f}s\n")
        if not args.no_skew:
            t0 = time.time()
            _, skew_metrics = refresh_skew(new_dates, new_options_df, return_metrics=True)
            pipeline_profile["skew"] = skew_metrics
            print(f"[skew] step total {time.time()-t0:.1f}s\n")

    t0 = time.time()
    payload, signal_metrics = compute_signal(
        target,
        allowed_tickers_by_date=allowed_by_date,
        universe_selector=args.universe_selector,
        universe_meta=selector_meta,
        held_context=held_context,
        return_metrics=True,
    )
    if args.universe_selector != UNIVERSE_SELECTOR_CORE:
        core_allowed_by_date, core_selector_meta = _resolve_universe_selector(
            UNIVERSE_SELECTOR_CORE,
            selector_dates,
            force_include_ticker=held_context.get("held_ticker"),
            force_include_reason=held_context.get("held_force_include_reason"),
            rebuild_pit_cache=False,
        )
        core_payload, _ = compute_signal(
            target,
            allowed_tickers_by_date=core_allowed_by_date,
            universe_selector=UNIVERSE_SELECTOR_CORE,
            universe_meta=core_selector_meta,
            held_context=held_context,
            return_metrics=True,
        )
        payload["selector_parity"] = _build_selector_parity(
            selector_payload=payload,
            core_payload=core_payload,
            selector_allowed_tickers=allowed_by_date.get(pd.Timestamp(payload["as_of"]).date(), set()),
        )
    pipeline_profile["signal"] = signal_metrics
    pipeline_profile["total_wall_time_seconds"] = round(time.time() - overall_t0, 3)
    payload["pipeline_profile"] = pipeline_profile
    SIGNAL_OUT.write_text(json.dumps(payload, indent=2, default=str))
    print(f"[signal] wrote {SIGNAL_OUT} ({time.time()-t0:.1f}s)")
    print()

    _print_signal(payload)
    print(f"\n=== Total wall time: {time.time()-overall_t0:.1f}s ===")


def _print_signal(payload: dict) -> None:
    print(f"=== Signal as of {payload['as_of']} ===")
    cfg = payload.get("signal_config", {})
    # Hash equality is the actual canonical check; assert_signal_config_canonical
    # already raised earlier if any of the 4 scalar constants drifted, so we got
    # this far → config matches. Report directly from the hash on the payload.
    cfg_ok = "✓" if payload.get("config_hash") == cfg.get("config_hash") else "✗ MISMATCH"
    print(f"signal_config: z_window={cfg.get('z_window')}, "
          f"rolling_min={cfg.get('rolling_min')}, "
          f"z_threshold={cfg.get('z_threshold')}, "
          f"universe_top_n={cfg.get('universe_top_n')}  {cfg_ok}")
    print(f"universe_selector: {payload.get('universe_selector', UNIVERSE_SELECTOR_CORE)}")
    held_meta = payload.get("held_force_include") or {}
    if held_meta.get("held_ticker"):
        status = "yes" if held_meta.get("held_force_included") else "no"
        print(
            f"held_force_include: ticker={held_meta.get('held_ticker')} "
            f"applied={status}"
        )
    r = payload["regime"]
    print(f"SPY {r['spy_close']:.2f} vs 200d {r['spy_200d']:.2f}  "
          f"regime: {'OPEN' if r['open'] else 'BLOCKED'}")
    n_z = payload.get("n_z_passing", payload["n_candidates"])
    print(f"universe size {payload['universe_size']}, {payload['tickers_with_skew_today']} with skew today, "
          f"{n_z} z>={payload['z_threshold']} → {payload['n_candidates']} after trend filter")
    parity = payload.get("selector_parity") or {}
    if parity.get("warning"):
        print(f"selector_parity_warning: {parity['warning']}")
    print()
    if payload["candidates"]:
        print(f"{'rank':>4} {'ticker':<8} {'z':>7} {'skew':>9} {'spot':>10} {'DTE':>5}")
        for c in payload["candidates"]:
            print(f"{c['rank']:>4} {c['ticker']:<8} {c['z']:>7.3f} "
                  f"{c['skew_5otm']:>9.4f} {c['spot']:>10.2f} {c['days_to_exp']:>5}")
        print()
        top = payload["candidates"][0]
        print(f">>> Single-position pick: {top['ticker']} (z={top['z']:.2f})")
    else:
        print(">>> No qualifying candidates today — no entry per spec")
    rejected = payload.get("candidates_rejected") or []
    if rejected:
        print()
        print(f"Rejected by trend filter ({len(rejected)}):")
        print(f"  {'ticker':<8} {'z':>5}  {'reason':<28}  {'r60d':>7} {'r12m':>7}  {'trend':<10}")
        for r in rejected:
            tr = r.get("filter_trace", {})
            r60 = tr.get("recent_60d_ret")
            r12 = tr.get("ret_12m")
            ts  = tr.get("trend_status", "?")
            print(f"  {r['ticker']:<8} {r['z']:>5.2f}  {r.get('reject_reason',''):<28}  "
                  f"{(r60 if r60 is not None else 0):>+7.1%} "
                  f"{(r12 if r12 is not None else 0):>+7.1%}  {ts:<10}")


if __name__ == "__main__":
    main()
