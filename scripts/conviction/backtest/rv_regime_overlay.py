#!/usr/bin/env python3
"""Backtest SPX RV regime overlays on top of the canonical Path S replay.

The RV file used here is one EOD row per trading day, so the timing contract
is:

    signal on date D close -> action on date D+1 open

This script:
  1. Loads `data/spx_intraday_rv.csv`
  2. Converts RV from daily decimal vol into annualized percent
  3. Builds RV state machines (hard / hysteresis / RV20-ratio)
  4. Supports:
       fast  -> overlay on an existing canonical Path S daily path
       exact -> rerun Path S with binary RV gates through replay.py
  5. ANDs binary RV gates with the existing Path S SPY>200d gate
  6. Writes a comparison study with overlay-specific diagnostics
"""
from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
DATA_DIR = HERE / "data"
RESULTS_DIR = HERE / "results"

DEFAULT_RV_FILE = DATA_DIR / "spx_intraday_rv.csv"
DEFAULT_BASELINE_RUN = "2026-05-05_replay_1460d_massive_n1_baseline_repro"
DEFAULT_END_DATE = "2026-04-29"
DEFAULT_DAYS = 1460
DEFAULT_CAPITAL = 100_000.0
DEFAULT_SLIPPAGE_BPS = 15.0
ANN_FACTOR = math.sqrt(252.0) * 100.0


@dataclass(frozen=True)
class BinaryVariant:
    name: str
    metric_col: str
    enter_threshold: float
    exit_threshold: float
    description: str


BINARY_VARIANTS: tuple[BinaryVariant, ...] = (
    BinaryVariant(
        name="RV_HARD_10",
        metric_col="rv_ann_pct",
        enter_threshold=10.0,
        exit_threshold=10.0,
        description="Risk ON if annualized RV < 10; OFF otherwise.",
    ),
    BinaryVariant(
        name="RV_HYST_10_12",
        metric_col="rv_ann_pct",
        enter_threshold=10.0,
        exit_threshold=12.0,
        description="Enter below 10, exit above 12, stay in between.",
    ),
    BinaryVariant(
        name="RV_HYST_10_15",
        metric_col="rv_ann_pct",
        enter_threshold=10.0,
        exit_threshold=15.0,
        description="Enter below 10, exit above 15, stay in between.",
    ),
    BinaryVariant(
        name="RV_HYST_12_20",
        metric_col="rv_ann_pct",
        enter_threshold=12.0,
        exit_threshold=20.0,
        description="Enter below 12, exit above 20, stay in between.",
    ),
    BinaryVariant(
        name="RV_RATIO_20",
        metric_col="rv_ratio_20",
        enter_threshold=0.80,
        exit_threshold=1.25,
        description="Enter when RV/RV20 < 0.80, exit when RV/RV20 > 1.25.",
    ),
)

SCALE_VARIANT_NAME = "RV_SCALE_10"


def _slug(text: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else "_" for ch in text).strip("_")


def _calmar(cagr: float | None, max_drawdown: float | None) -> float | None:
    if cagr is None or max_drawdown is None or max_drawdown >= 0:
        return None
    return float(cagr / abs(max_drawdown))


def _worst_20d_drawdown(equity_df: pd.DataFrame) -> float | None:
    if equity_df.empty or "portfolio_value" not in equity_df.columns:
        return None
    pv = equity_df["portfolio_value"].astype(float)
    trailing_high = pv.rolling(20, min_periods=1).max()
    dd20 = pv / trailing_high - 1.0
    if dd20.empty:
        return None
    return float(dd20.min())


def _monthly_returns(equity_df: pd.DataFrame) -> pd.Series:
    if equity_df.empty:
        return pd.Series(dtype=float)
    indexed = equity_df.copy()
    indexed["date"] = pd.to_datetime(indexed["date"]).dt.normalize()
    indexed = indexed.set_index("date")
    monthly = indexed["portfolio_value"].resample("ME").last().pct_change()
    return monthly.dropna()


def _avg_off_spell_days(state_series: pd.Series) -> float:
    lengths: list[int] = []
    streak = 0
    for state in state_series.fillna(1).astype(int):
        if state == 0:
            streak += 1
        elif streak:
            lengths.append(streak)
            streak = 0
    if streak:
        lengths.append(streak)
    return float(np.mean(lengths)) if lengths else 0.0


def _transition_counts(state_series: pd.Series) -> tuple[int, int]:
    series = state_series.fillna(1).astype(int)
    prev = series.shift(1)
    exits = int(((prev == 1) & (series == 0)).sum())
    reentries = int(((prev == 0) & (series == 1)).sum())
    return exits, reentries


def _standardize_active_column(equity_df: pd.DataFrame) -> pd.DataFrame:
    out = equity_df.copy()
    if "active_ticker" not in out.columns and "active_tickers" in out.columns:
        out["active_ticker"] = out["active_tickers"]
    out["active_ticker"] = out.get("active_ticker", "").fillna("")
    return out


def load_rv_daily(rv_path: Path) -> tuple[pd.DataFrame, dict]:
    if not rv_path.exists():
        raise FileNotFoundError(f"RV CSV not found: {rv_path}")

    raw = pd.read_csv(rv_path)
    required = {"Date", "RV", "SPX", "VIX", "RV20", "RV10", "RV5"}
    missing = sorted(required - set(raw.columns))
    if missing:
        raise ValueError(f"RV CSV missing required columns: {missing}")

    df = raw.copy()
    df["date"] = pd.to_datetime(df["Date"], errors="coerce").dt.normalize()
    if df["date"].isna().any():
        bad = int(df["date"].isna().sum())
        raise ValueError(f"RV CSV has {bad} rows with invalid dates")

    numeric_cols = [
        "RV", "SPX", "VIX", "RV20", "RV10", "RV5", "R", "VixOne", "Y", "M",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    rows_per_date = df.groupby("date").size()
    df = df.sort_values("date").groupby("date", as_index=False).last()

    df["rv_ann_pct"] = df["RV"] * ANN_FACTOR
    df["rv20_ann_pct"] = df["RV20"] * ANN_FACTOR
    df["rv10_ann_pct"] = df["RV10"] * ANN_FACTOR
    df["rv5_ann_pct"] = df["RV5"] * ANN_FACTOR
    df["vix_pct"] = np.where(df["VIX"].abs() <= 1.5, df["VIX"] * 100.0, df["VIX"])
    if "VixOne" in df.columns:
        df["vix1d_pct"] = np.where(df["VixOne"].abs() <= 1.5, df["VixOne"] * 100.0, df["VixOne"])
    else:
        df["vix1d_pct"] = np.nan
    df["rv_ratio_20"] = df["RV"] / df["RV20"]
    df["is_row_per_day"] = rows_per_date.reindex(df["date"]).fillna(1).eq(1).to_numpy()

    meta = {
        "source_file": str(rv_path),
        "rows_raw": int(len(raw)),
        "rows_daily": int(len(df)),
        "min_date": df["date"].min().date().isoformat(),
        "max_date": df["date"].max().date().isoformat(),
        "max_rows_per_date": int(rows_per_date.max()),
        "duplicate_dates": int(rows_per_date.gt(1).sum()),
        "one_row_per_day": bool(rows_per_date.max() == 1),
    }
    return df, meta


def add_variant_states(rv_df: pd.DataFrame) -> pd.DataFrame:
    out = rv_df.copy().sort_values("date").reset_index(drop=True)
    for variant in BINARY_VARIANTS:
        states: list[int] = []
        state = 1
        for value in out[variant.metric_col]:
            if pd.isna(value):
                states.append(state)
                continue
            if value >= variant.exit_threshold:
                state = 0
            elif value <= variant.enter_threshold:
                state = 1
            states.append(state)
        out[f"{variant.name.lower()}_state"] = states

    scale = 10.0 / out["rv_ann_pct"].replace(0.0, np.nan)
    scale = scale.clip(upper=1.0)
    out["rv_scale_10_target"] = scale.replace([np.inf, -np.inf], np.nan).fillna(1.0)
    return out


def build_state_calendar(rv_df: pd.DataFrame, spy_lookup: dict[pd.Timestamp, str]) -> pd.DataFrame:
    calendar = pd.DataFrame({"date": sorted(pd.Timestamp(d).normalize() for d in spy_lookup)})
    out = calendar.merge(rv_df, on="date", how="left")
    fill_cols = [
        "RV", "SPX", "VIX", "RV20", "RV10", "RV5", "R", "VixOne",
        "rv_ann_pct", "rv20_ann_pct", "rv10_ann_pct", "rv5_ann_pct",
        "vix_pct", "vix1d_pct", "rv_ratio_20", "rv_scale_10_target",
    ]
    state_cols = [f"{variant.name.lower()}_state" for variant in BINARY_VARIANTS]
    for col in fill_cols:
        if col in out.columns:
            out[col] = out[col].ffill()
    for col in state_cols:
        out[col] = out[col].ffill().fillna(1).astype(int)
    out["rv_scale_10_target"] = out["rv_scale_10_target"].fillna(1.0)
    out["spy_state"] = out["date"].map(
        lambda d: spy_lookup.get(pd.Timestamp(d).normalize(), "RISK_ON")
    )
    for variant in BINARY_VARIANTS:
        state_col = f"{variant.name.lower()}_state"
        combo_col = f"{variant.name.lower()}_combined"
        out[combo_col] = np.where(
            (out["spy_state"] == "RISK_ON") & (out[state_col] == 1),
            "RISK_ON",
            "RISK_OFF",
        )
    out["rv_scale_10_effective_target"] = np.where(
        out["spy_state"] == "RISK_ON",
        out["rv_scale_10_target"],
        0.0,
    )
    return out


def build_state_calendar_from_dates(rv_df: pd.DataFrame, dates: pd.Series | list[pd.Timestamp]) -> pd.DataFrame:
    calendar = pd.DataFrame({"date": pd.to_datetime(pd.Series(dates)).dt.normalize().drop_duplicates().sort_values()})
    out = calendar.merge(rv_df, on="date", how="left")
    fill_cols = [
        "RV", "SPX", "VIX", "RV20", "RV10", "RV5", "R", "VixOne",
        "rv_ann_pct", "rv20_ann_pct", "rv10_ann_pct", "rv5_ann_pct",
        "vix_pct", "vix1d_pct", "rv_ratio_20", "rv_scale_10_target",
    ]
    state_cols = [f"{variant.name.lower()}_state" for variant in BINARY_VARIANTS]
    for col in fill_cols:
        if col in out.columns:
            out[col] = out[col].ffill()
    for col in state_cols:
        out[col] = out[col].ffill().fillna(1).astype(int)
        out[f"{col[:-6]}_combined"] = np.where(out[col] == 1, "RISK_ON", "RISK_OFF")
    out["rv_scale_10_target"] = out["rv_scale_10_target"].fillna(1.0)
    out["rv_scale_10_effective_target"] = out["rv_scale_10_target"]
    return out


def build_regime_lookup_from_calendar(calendar_df: pd.DataFrame, combined_col: str) -> dict[pd.Timestamp, str]:
    subset = calendar_df[["date", combined_col]].copy()
    return {
        pd.Timestamp(row["date"]).normalize(): row[combined_col]
        for _, row in subset.iterrows()
    }


def prepare_path_s_config() -> dict:
    import replay

    replay.PATH_S_CONFIG["direction"] = "bullish"
    replay.PATH_S_CONFIG["abs_skew_z_min"] = 3.0
    replay.PATH_S_CONFIG["speculative_only"] = False
    replay.PATH_S_CONFIG["min_pct_above_200d"] = None
    replay.PATH_S_CONFIG["min_ret_60d"] = None
    return replay.load_skew_lookup(
        z_window=60,
        persistence_days=1,
        abs_skew_z_min=3.0,
        direction="bullish",
    )


def run_path_s_variant(
    *,
    regime_lookup: dict[pd.Timestamp, str],
    regime_gate: str,
    run_suffix: str,
    end_date: str,
    days: int,
    skew_lookup: dict,
) -> dict:
    import replay

    return replay.run_replay(
        end_date=pd.Timestamp(end_date),
        lookback_days=days,
        source="massive",
        parquet_path=None,
        slippage_bps=DEFAULT_SLIPPAGE_BPS,
        initial_capital=DEFAULT_CAPITAL,
        refresh=False,
        progress_every=50,
        exit_rule="trailing_pct",
        trailing_pct=20.0,
        dynamic_themes=False,
        universe_top_n=2000,
        ignore_themes=True,
        strategy="pathS",
        skew_lookup=skew_lookup,
        max_hold_days=90,
        exit_mode="baseline",
        post90_trail_pct=10.0,
        exclude_tickers=None,
        launch_date=None,
        signal_decay_z=None,
        signal_decay_days=2,
        skew_direction="bullish",
        skew_z_window=60,
        skew_z_rolling_min=20,
        regime_lookup=regime_lookup,
        regime_gate=regime_gate,
        run_suffix=run_suffix,
        displacement_enabled=False,
        displacement_min_hold=20,
        displacement_max_return=0.0,
        displacement_z_min=3.0,
    )


def load_run_artifacts(run_name: str) -> tuple[Path, dict, pd.DataFrame, pd.DataFrame]:
    run_dir = RESULTS_DIR / run_name
    summary = json.loads((run_dir / "summary.json").read_text())
    daily = pd.read_csv(run_dir / "daily_equity.csv")
    daily["date"] = pd.to_datetime(daily["date"]).dt.normalize()
    daily = _standardize_active_column(daily)
    trades = pd.read_csv(run_dir / "trade_log.csv")
    return run_dir, summary, daily, trades


def summary_from_daily(
    daily_df: pd.DataFrame,
    *,
    run_name: str,
    pct_time_invested: float | None = None,
) -> dict:
    rets = daily_df["daily_return"].astype(float).iloc[1:]
    total_return = daily_df["portfolio_value"].iloc[-1] / daily_df["portfolio_value"].iloc[0] - 1.0
    years = max(1e-9, (len(daily_df) - 1) / 252.0)
    cagr = daily_df["portfolio_value"].iloc[-1] / daily_df["portfolio_value"].iloc[0]
    cagr = cagr ** (1.0 / years) - 1.0
    sharpe = float(rets.mean() / rets.std() * math.sqrt(252.0)) if rets.std() > 0 else None
    if pct_time_invested is None:
        pct_time_invested = float((daily_df.get("effective_exposure", 0.0) > 0).mean())
    return {
        "run_name": run_name,
        "performance": {
            "final_equity": round(float(daily_df["portfolio_value"].iloc[-1]), 2),
            "total_return": round(float(total_return), 5),
            "cagr": round(float(cagr), 5),
            "max_drawdown": round(float(daily_df["drawdown"].min()), 5),
            "sharpe": round(float(sharpe), 3) if sharpe is not None else None,
        },
        "activity": {
            "n_trades": None,
            "avg_holding_days": None,
            "pct_time_invested": round(float(pct_time_invested), 4),
        },
    }


def simulate_binary_overlay(
    baseline_daily: pd.DataFrame,
    state_calendar: pd.DataFrame,
    variant_name: str,
) -> pd.DataFrame:
    aligned = _standardize_active_column(baseline_daily.copy())
    state_col = f"{variant_name.lower()}_state"
    aligned = aligned.merge(state_calendar[["date", state_col]], on="date", how="left")
    aligned["baseline_exposure"] = np.where(aligned["active_ticker"].astype(str) != "", 1.0, 0.0)
    target_exposure = aligned[state_col].fillna(1).astype(float)
    aligned["effective_exposure"] = np.where(
        aligned["baseline_exposure"] > 0,
        target_exposure.shift(1).fillna(1.0),
        0.0,
    )
    trade_cost = aligned["effective_exposure"].diff().abs().fillna(0.0) * (DEFAULT_SLIPPAGE_BPS / 10_000.0)
    aligned["daily_return"] = aligned["daily_return"].astype(float) * aligned["effective_exposure"] - trade_cost

    equity = [DEFAULT_CAPITAL]
    for daily_ret in aligned["daily_return"].iloc[1:]:
        equity.append(equity[-1] * (1.0 + float(daily_ret)))
    aligned["portfolio_value"] = equity
    aligned["drawdown"] = aligned["portfolio_value"] / aligned["portfolio_value"].cummax() - 1.0
    return aligned


def synthesize_scale_variant(
    baseline_daily: pd.DataFrame,
    state_calendar: pd.DataFrame,
    out_dir: Path,
) -> tuple[pd.DataFrame, dict]:
    aligned = baseline_daily.copy()
    aligned = _standardize_active_column(aligned)
    aligned["baseline_exposure"] = np.where(aligned["active_ticker"].astype(str) != "", 1.0, 0.0)

    scale_cols = ["date", "rv_ann_pct", "rv_scale_10_effective_target"]
    aligned = aligned.merge(state_calendar[scale_cols], on="date", how="left")
    aligned["rv_scale_10_effective_target"] = aligned["rv_scale_10_effective_target"].fillna(0.0)

    effective = aligned["rv_scale_10_effective_target"].shift(1).fillna(0.0)
    aligned["effective_exposure"] = np.where(aligned["baseline_exposure"] > 0, effective, 0.0)
    trade_cost = aligned["effective_exposure"].diff().abs().fillna(0.0) * (DEFAULT_SLIPPAGE_BPS / 10_000.0)
    aligned["variant_daily_return"] = aligned["daily_return"].astype(float) * aligned["effective_exposure"] - trade_cost

    equity = [DEFAULT_CAPITAL]
    for daily_ret in aligned["variant_daily_return"].iloc[1:]:
        equity.append(equity[-1] * (1.0 + float(daily_ret)))
    aligned["portfolio_value"] = equity
    aligned["drawdown"] = aligned["portfolio_value"] / aligned["portfolio_value"].cummax() - 1.0
    aligned["daily_return"] = aligned["variant_daily_return"]
    aligned.to_csv(out_dir / "rv_scale_10_daily_equity.csv", index=False)

    rets = aligned["daily_return"].astype(float).iloc[1:]
    total_return = aligned["portfolio_value"].iloc[-1] / aligned["portfolio_value"].iloc[0] - 1.0
    years = max(1e-9, (len(aligned) - 1) / 252.0)
    cagr = aligned["portfolio_value"].iloc[-1] / aligned["portfolio_value"].iloc[0]
    cagr = cagr ** (1.0 / years) - 1.0
    sharpe = float(rets.mean() / rets.std() * math.sqrt(252.0)) if rets.std() > 0 else None
    summary = {
        "run_name": SCALE_VARIANT_NAME,
        "performance": {
            "final_equity": round(float(aligned["portfolio_value"].iloc[-1]), 2),
            "total_return": round(float(total_return), 5),
            "cagr": round(float(cagr), 5),
            "max_drawdown": round(float(aligned["drawdown"].min()), 5),
            "sharpe": round(float(sharpe), 3) if sharpe is not None else None,
        },
        "activity": {
            "n_trades": None,
            "avg_holding_days": None,
            "pct_time_invested": round(float((aligned["effective_exposure"] > 0).mean()), 4),
            "avg_exposure_when_held": round(
                float(aligned.loc[aligned["baseline_exposure"] > 0, "effective_exposure"].mean()),
                4,
            ) if (aligned["baseline_exposure"] > 0).any() else 0.0,
            "exposure_turnover": round(
                float(aligned["effective_exposure"].diff().abs().fillna(0.0).sum()),
                4,
            ),
        },
    }
    return aligned, summary


def build_variant_row(
    *,
    variant_name: str,
    summary: dict,
    daily_df: pd.DataFrame,
    baseline_summary: dict,
    baseline_daily: pd.DataFrame,
    state_calendar: pd.DataFrame,
) -> dict:
    perf = summary["performance"]
    act = summary.get("activity", {})
    total_return = perf.get("total_return")
    cagr = perf.get("cagr")
    max_drawdown = perf.get("max_drawdown")
    sharpe = perf.get("sharpe")
    calmar = _calmar(cagr, max_drawdown)
    worst_20d = _worst_20d_drawdown(daily_df)

    base_perf = baseline_summary["performance"]
    base_total_return = base_perf.get("total_return")
    base_cagr = base_perf.get("cagr")
    base_mdd = base_perf.get("max_drawdown")
    base_sharpe = base_perf.get("sharpe")
    base_calmar = _calmar(base_cagr, base_mdd)
    base_worst_20d = _worst_20d_drawdown(baseline_daily)

    monthly_corr = None
    monthly_variant = _monthly_returns(daily_df)
    monthly_base = _monthly_returns(baseline_daily)
    if not monthly_variant.empty and not monthly_base.empty:
        joined = pd.concat(
            [monthly_base.rename("baseline"), monthly_variant.rename("variant")],
            axis=1,
            join="inner",
        ).dropna()
        if len(joined) >= 2:
            monthly_corr = float(joined["baseline"].corr(joined["variant"]))

    state_col = f"{variant_name.lower()}_state"
    if variant_name == "PATH_S_ALONE":
        rv_exits = 0
        rv_reentries = 0
        avg_off_days = 0.0
        pct_off_days = 0.0
    elif variant_name == SCALE_VARIANT_NAME:
        rv_exits = 0
        rv_reentries = 0
        avg_off_days = 0.0
        pct_off_days = 0.0
    else:
        rv_state = state_calendar[state_col]
        rv_exits, rv_reentries = _transition_counts(rv_state)
        avg_off_days = _avg_off_spell_days(rv_state)
        pct_off_days = float((rv_state == 0).mean())

    aligned = baseline_daily[["date", "daily_return"]].rename(columns={"daily_return": "baseline_daily_return"})
    aligned = aligned.merge(
        daily_df[["date", "daily_return"]].rename(columns={"daily_return": "variant_daily_return"}),
        on="date",
        how="inner",
    )
    aligned["baseline_daily_return"] = aligned["baseline_daily_return"].astype(float)
    aligned["variant_daily_return"] = aligned["variant_daily_return"].astype(float)
    positive_gap = aligned["baseline_daily_return"] - aligned["variant_daily_return"]
    missed_rebound = positive_gap.where(aligned["baseline_daily_return"] > 0, 0.0).clip(lower=0.0).sum()
    drawdown_avoided = (aligned["variant_daily_return"] - aligned["baseline_daily_return"]).where(
        aligned["baseline_daily_return"] < 0, 0.0
    ).clip(lower=0.0).sum()

    return_retention = None
    if base_total_return is not None and base_total_return > 0 and total_return is not None:
        return_retention = float(total_return / base_total_return)

    mdd_improved_10pct = None
    if base_mdd is not None and max_drawdown is not None:
        mdd_improved_10pct = abs(max_drawdown) <= abs(base_mdd) * 0.90

    row = {
        "variant": variant_name,
        "run_name": summary.get("run_name"),
        "final_equity": perf.get("final_equity"),
        "total_return": total_return,
        "cagr": cagr,
        "sharpe": sharpe,
        "calmar": calmar,
        "max_drawdown": max_drawdown,
        "worst_20d_drawdown": worst_20d,
        "n_trades": act.get("n_trades"),
        "avg_holding_days": act.get("avg_holding_days"),
        "pct_time_invested": act.get("pct_time_invested"),
        "rv_exits": rv_exits,
        "rv_reentries": rv_reentries,
        "avg_days_risk_off": round(avg_off_days, 2),
        "pct_days_risk_off": round(pct_off_days, 4),
        "missed_rebound_cost_pct_pts": round(float(missed_rebound * 100.0), 3),
        "drawdown_avoided_pct_pts": round(float(drawdown_avoided * 100.0), 3),
        "monthly_corr_to_path_s": round(monthly_corr, 4) if monthly_corr is not None else None,
        "delta_total_return": round(total_return - base_total_return, 5) if total_return is not None else None,
        "delta_cagr": round(cagr - base_cagr, 5) if cagr is not None else None,
        "delta_sharpe": round(sharpe - base_sharpe, 3) if sharpe is not None and base_sharpe is not None else None,
        "delta_calmar": round(calmar - base_calmar, 3) if calmar is not None and base_calmar is not None else None,
        "delta_max_drawdown": round(max_drawdown - base_mdd, 5) if max_drawdown is not None else None,
        "delta_worst_20d_drawdown": round(worst_20d - base_worst_20d, 5) if worst_20d is not None and base_worst_20d is not None else None,
        "return_retention_vs_path_s": round(return_retention, 4) if return_retention is not None else None,
        "pass_calmar_improves": bool(calmar is not None and base_calmar is not None and calmar > base_calmar),
        "pass_sharpe_improves": bool(sharpe is not None and base_sharpe is not None and sharpe > base_sharpe),
        "pass_mdd_improves_10pct": mdd_improved_10pct,
        "pass_return_within_90pct": bool(return_retention is not None and return_retention >= 0.90),
        "pass_worst_20d_improves": bool(worst_20d is not None and base_worst_20d is not None and worst_20d > base_worst_20d),
    }
    row["pass_count"] = int(
        sum(
            1
            for key in (
                "pass_calmar_improves",
                "pass_sharpe_improves",
                "pass_mdd_improves_10pct",
                "pass_return_within_90pct",
                "pass_worst_20d_improves",
            )
            if row.get(key) is True
        )
    )
    return row


def write_report(
    *,
    out_dir: Path,
    rv_meta: dict,
    summary_df: pd.DataFrame,
    run_map: dict[str, str],
    mode: str,
    end_date: str,
    days: int,
) -> None:
    best = summary_df[summary_df["variant"] != "PATH_S_ALONE"].sort_values(
        ["pass_count", "delta_calmar", "delta_sharpe"],
        ascending=[False, False, False],
        na_position="last",
    )
    top_row = best.iloc[0] if not best.empty else None

    lines = [
        f"# RV regime overlay study - {datetime.now():%Y-%m-%d}",
        "",
        f"- RV file: `{rv_meta['source_file']}`",
        f"- RV coverage: {rv_meta['min_date']} to {rv_meta['max_date']} "
        f"({rv_meta['rows_daily']} daily rows; one_row_per_day={rv_meta['one_row_per_day']})",
        f"- Mode: `{mode}`",
        f"- Baseline window: {days} trading sessions ending {end_date}",
        (
            "- Timing: EOD RV signal on D, action on D+1 via overlay on the canonical Path S daily path"
            if mode == "fast"
            else "- Timing: binary RV gates rerun through replay.py with D-to-D+1 regime timing; scale remains a daily path overlay"
        ),
        "",
        "## Variant Runs",
        "",
    ]
    for variant, run_name in run_map.items():
        lines.append(f"- {variant}: `{run_name}`")
    lines.extend(["", "## Results", "", "```text", summary_df.to_string(index=False), "```"])
    if top_row is not None:
        lines.extend(
            [
                "",
                "## First Read",
                "",
                f"- Top pass-count variant: `{top_row['variant']}`",
                f"- Pass count: {top_row['pass_count']}",
                f"- Delta Calmar: {top_row['delta_calmar']}",
                f"- Delta Sharpe: {top_row['delta_sharpe']}",
                f"- Delta max drawdown: {top_row['delta_max_drawdown']}",
            ]
        )
    (out_dir / "report.md").write_text("\n".join(lines))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rv-file", type=Path, default=DEFAULT_RV_FILE, help="Saved SPX RV CSV")
    parser.add_argument("--mode", choices=["fast", "exact"], default="fast")
    parser.add_argument(
        "--baseline-run",
        default=DEFAULT_BASELINE_RUN,
        help="Existing canonical Path S run directory name under results/ (fast mode)",
    )
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS, help="Replay lookback in calendar days (exact mode)")
    parser.add_argument("--end-date", default=DEFAULT_END_DATE, help="Replay end date (exact mode)")
    parser.add_argument(
        "--variants",
        default=",".join([variant.name for variant in BINARY_VARIANTS] + [SCALE_VARIANT_NAME]),
        help="Comma-separated variant list",
    )
    parser.add_argument("--study-name", default=None, help="Override comparison output directory suffix")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    requested_variants = [item.strip() for item in args.variants.split(",") if item.strip()]
    binary_requested = [variant for variant in BINARY_VARIANTS if variant.name in requested_variants]
    scale_requested = SCALE_VARIANT_NAME in requested_variants

    rv_df, rv_meta = load_rv_daily(args.rv_file)
    rv_df = add_variant_states(rv_df)

    study_suffix = args.study_name or (
        f"rv_regime_overlay_fast_{args.baseline_run}"
        if args.mode == "fast"
        else f"rv_regime_overlay_exact_{args.days}d_{args.end_date}"
    )
    study_slug = _slug(study_suffix)
    study_dir = RESULTS_DIR / f"{datetime.now():%Y-%m-%d}_{study_suffix}"
    study_dir.mkdir(parents=True, exist_ok=True)

    if args.mode == "fast":
        baseline_run_dir, baseline_summary_json, baseline_daily, _baseline_trades = load_run_artifacts(args.baseline_run)
        state_calendar = build_state_calendar_from_dates(rv_df, baseline_daily["date"])
        run_map: dict[str, str] = {"PATH_S_ALONE": args.baseline_run}
    else:
        from regime_filter import build_regime_lookup

        spy_lookup = build_regime_lookup("spy")
        state_calendar = build_state_calendar(rv_df, spy_lookup)
        skew_lookup = prepare_path_s_config()
        baseline_run = run_path_s_variant(
            regime_lookup=spy_lookup,
            regime_gate="spy",
            run_suffix=f"{study_slug}_baseline",
            end_date=args.end_date,
            days=args.days,
            skew_lookup=skew_lookup,
        )
        baseline_run_dir, baseline_summary_json, baseline_daily, _baseline_trades = load_run_artifacts(
            baseline_run["run_name"]
        )
        baseline_dates = set(baseline_daily["date"])
        state_calendar = state_calendar[state_calendar["date"].isin(baseline_dates)].copy().reset_index(drop=True)
        run_map = {"PATH_S_ALONE": baseline_run["run_name"]}

    state_calendar.to_csv(study_dir / "rv_state_daily.csv", index=False)
    (study_dir / "rv_meta.json").write_text(json.dumps(rv_meta, indent=2))

    summary_rows: list[dict] = []
    summary_rows.append(
        build_variant_row(
            variant_name="PATH_S_ALONE",
            summary=baseline_summary_json,
            daily_df=baseline_daily,
            baseline_summary=baseline_summary_json,
            baseline_daily=baseline_daily,
            state_calendar=state_calendar,
        )
    )

    for variant in binary_requested:
        if args.mode == "fast":
            daily_df = simulate_binary_overlay(baseline_daily, state_calendar, variant.name)
            daily_df.to_csv(study_dir / f"{variant.name.lower()}_daily_equity.csv", index=False)
            summary_json = summary_from_daily(
                daily_df,
                run_name=variant.name,
                pct_time_invested=float((daily_df["effective_exposure"] > 0).mean()),
            )
            run_map[variant.name] = variant.name
        else:
            combined_col = f"{variant.name.lower()}_combined"
            regime_lookup = build_regime_lookup_from_calendar(state_calendar, combined_col)
            run_summary = run_path_s_variant(
                regime_lookup=regime_lookup,
                regime_gate=f"spy_{variant.name.lower()}",
                run_suffix=f"{study_slug}_{variant.name.lower()}",
                end_date=args.end_date,
                days=args.days,
                skew_lookup=skew_lookup,
            )
            run_map[variant.name] = run_summary["run_name"]
            _run_dir, summary_json, daily_df, _trades = load_run_artifacts(run_summary["run_name"])
        summary_rows.append(
            build_variant_row(
                variant_name=variant.name,
                summary=summary_json,
                daily_df=daily_df,
                baseline_summary=baseline_summary_json,
                baseline_daily=baseline_daily,
                state_calendar=state_calendar,
            )
        )

    if scale_requested:
        scale_daily, scale_summary = synthesize_scale_variant(baseline_daily, state_calendar, study_dir)
        run_map[SCALE_VARIANT_NAME] = SCALE_VARIANT_NAME
        summary_rows.append(
            build_variant_row(
                variant_name=SCALE_VARIANT_NAME,
                summary=scale_summary,
                daily_df=scale_daily,
                baseline_summary=baseline_summary_json,
                baseline_daily=baseline_daily,
                state_calendar=state_calendar,
            )
        )

    summary_df = pd.DataFrame(summary_rows)
    summary_df.to_csv(study_dir / "variant_summary.csv", index=False)
    (study_dir / "run_map.json").write_text(json.dumps(run_map, indent=2))
    write_report(
        out_dir=study_dir,
        rv_meta=rv_meta,
        summary_df=summary_df,
        run_map=run_map,
        mode=args.mode,
        end_date=str(baseline_summary_json["window"]["end"]),
        days=int(baseline_summary_json["window"]["n_sessions"]),
    )

    print("=" * 100)
    print("SPX RV REGIME OVERLAY STUDY")
    print("=" * 100)
    print(f"Study dir: {study_dir}")
    print(f"Mode:      {args.mode}")
    print(f"Baseline:  {baseline_run_dir.name}")
    print(f"RV rows:   {rv_meta['rows_daily']} ({rv_meta['min_date']} -> {rv_meta['max_date']})")
    print("")
    print(summary_df.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
