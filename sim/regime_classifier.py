#!/usr/bin/env python3
"""
Variance Risk Premium Regime Classifier
========================================

Multi-layer regime classification for SPX short-premium strategies,
synthesizing findings from three academic sub-literatures:

Layer 1 — Variance Risk Premium (VRP)
  Bollerslev, Tauchen, Zhou (2009): VIX² − RV² is the single most
  informative regime variable, simultaneously predicting equity returns
  and governing short-premium profitability (Carr & Wu, 2009).

Layer 2 — Bekaert-Hoerova Decomposition
  Splits elevated VIX into:
    VP (variance premium) = risk aversion → rich premiums to harvest
    CV (conditional variance) = physical uncertainty → genuine danger
  Key discriminant: VP_share = VP / VIX² determines whether elevated
  VIX signals opportunity or risk.

Layer 3 — Dealer Gamma Exposure (GEX) Regimes
  Endogenous volatility regimes from dealer hedging (Barbon & Buraschi 2021).
  Positive GEX (call-dominated OI) → vol compression → RV < IV
  Negative GEX (put-dominated OI) → vol amplification → RV > IV
  Distinct from return regimes — determines whether realized vol will
  compress or amplify relative to implied, directly affecting P&L.

Layer 5 — 0DTE / Short-Dated VRP
  VIX1D-based intraday variance risk premium.
  Returns dominated by realized skewness (not just VRP level) — mechanical
  selling without regime awareness produces unacceptably wide distributions.

Synthesis — Binary Signal (Nystrup et al.)
  Binary regime-conditioned rules outperform continuous dynamic sizing.
  Primary value: knowing when NOT to trade.
    HARVEST: VRP rich + VP dominant + GEX compressing → full size
    AVOID:   VRP poor OR (CV dominant AND GEX amplifying) → sit out
    NEUTRAL: Mixed signals → default/reduced size

Data: reads from Gamma/sim/cache/{date}/ infrastructure.
"""

import json
import os
import glob
import math
import warnings
from io import StringIO
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests

warnings.filterwarnings("ignore")

# ─── Config ────────────────────────────────────────────────────────────

CACHE_DIR = "/Users/mgebremichael/Documents/Gamma/sim/cache"
VOL_DECOUPLE_PATH = "/Users/mgebremichael/Documents/Gamma/sim/data/vol_decouple_daily.csv"
PANEL_PATH = "/Users/mgebremichael/Documents/Gamma/sim/data/leo_ic_long_pattern_panel.csv"
SPX_MULTIPLIER = 100

# Lookback
LOOKBACK_PCTILE = 252
TRADING_DAYS_PER_YEAR = 252

# Thresholds (starting points — tunable via sweep)
VRP_HARVEST_PCTILE = 0.70
VRP_AVOID_PCTILE = 0.30
VP_SHARE_HARVEST = 0.60
VP_SHARE_DANGER = 0.40
PERSISTENCE_DAYS = 2

# IC_LONG -> RR_SHORT regime switch thresholds.
# These match the validated Leo regime rule used in the dedicated switch study.
IC_LONG_RR_SHORT_VIX_RV10_THRESHOLD = 1.95
IC_LONG_RR_SHORT_RV5_RV20_THRESHOLD = 1.10

# Secondary diagnostics from the pattern-discovery pass. These are explanatory
# only and do not change the base switch rule.
IC_LONG_RR_SHORT_GO_IMBALANCE_BAD_THRESHOLD = 0.004849
IC_LONG_RR_SHORT_LEFT_ASYMMETRY_THRESHOLD_PCT = -0.011505


def _safe_float(value: Any) -> Optional[float]:
    """Return float(value) or None when the value is missing/non-finite."""
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(out):
        return None
    return out


def _coerce_pct_points(value: Any) -> Optional[float]:
    """
    Normalize a percent-like input to percentage points.

    Examples:
      0.155  -> 15.5
      15.5   -> 15.5
      0.079  -> 7.9
      7.9    -> 7.9
    """
    out = _safe_float(value)
    if out is None:
        return None
    if abs(out) <= 1.5:
        return out * 100.0
    return out


def _coerce_daily_vol_decimal(value: Any) -> Optional[float]:
    """
    Normalize a daily realized-vol input to decimal std-dev units.

    Repo data usually stores RV as a daily decimal, e.g. 0.0048. If the caller
    passes daily vol in percentage points, e.g. 0.48, this converts it.
    """
    out = _safe_float(value)
    if out is None:
        return None
    if abs(out) > 0.03:
        return out / 100.0
    return out


def compute_ic_long_rr_short_regime_metrics(
    *,
    vix: Any = None,
    rv10_daily: Any = None,
    rv10_ann_pct: Any = None,
    rv5_daily: Any = None,
    rv5_ann_pct: Any = None,
    rv20_daily: Any = None,
    rv20_ann_pct: Any = None,
    vix_rv10_ratio: Any = None,
    rv5_rv20_ratio: Any = None,
    left_go: Any = None,
    right_go: Any = None,
    go_imbalance: Any = None,
    spot: Any = None,
    inner_put: Any = None,
    inner_call: Any = None,
    center_offset_pct: Any = None,
    vix_rv10_threshold: float = IC_LONG_RR_SHORT_VIX_RV10_THRESHOLD,
    rv5_rv20_threshold: float = IC_LONG_RR_SHORT_RV5_RV20_THRESHOLD,
) -> Dict[str, Optional[float]]:
    """
    Compute the exact auditable regime metrics for the IC_LONG -> RR_SHORT rule.

    Base switch rule:
      switch when VIX/RV10 >= 2.04238 AND RV5/RV20 <= 1.02536

    Inputs can be either:
      - precomputed ratios, or
      - raw VIX and RV values, from which the ratios are derived.

    Returned fields are intended to be inspectable and serializable.
    """
    vix_pct = _coerce_pct_points(vix)

    rv10_ann_pct_val = _coerce_pct_points(rv10_ann_pct)
    if rv10_ann_pct_val is None:
        rv10_daily_val = _coerce_daily_vol_decimal(rv10_daily)
        if rv10_daily_val is not None:
            rv10_ann_pct_val = rv10_daily_val * math.sqrt(TRADING_DAYS_PER_YEAR) * 100.0
    else:
        rv10_daily_val = rv10_ann_pct_val / (100.0 * math.sqrt(TRADING_DAYS_PER_YEAR))

    rv5_ann_pct_val = _coerce_pct_points(rv5_ann_pct)
    if rv5_ann_pct_val is None:
        rv5_daily_val = _coerce_daily_vol_decimal(rv5_daily)
        if rv5_daily_val is not None:
            rv5_ann_pct_val = rv5_daily_val * math.sqrt(TRADING_DAYS_PER_YEAR) * 100.0
    else:
        rv5_daily_val = rv5_ann_pct_val / (100.0 * math.sqrt(TRADING_DAYS_PER_YEAR))

    rv20_ann_pct_val = _coerce_pct_points(rv20_ann_pct)
    if rv20_ann_pct_val is None:
        rv20_daily_val = _coerce_daily_vol_decimal(rv20_daily)
        if rv20_daily_val is not None:
            rv20_ann_pct_val = rv20_daily_val * math.sqrt(TRADING_DAYS_PER_YEAR) * 100.0
    else:
        rv20_daily_val = rv20_ann_pct_val / (100.0 * math.sqrt(TRADING_DAYS_PER_YEAR))

    vix_rv10_ratio_val = _safe_float(vix_rv10_ratio)
    if vix_rv10_ratio_val is None and vix_pct is not None and rv10_ann_pct_val and rv10_ann_pct_val > 0:
        vix_rv10_ratio_val = vix_pct / rv10_ann_pct_val

    rv5_rv20_ratio_val = _safe_float(rv5_rv20_ratio)
    if rv5_rv20_ratio_val is None and rv5_ann_pct_val and rv20_ann_pct_val and rv20_ann_pct_val > 0:
        rv5_rv20_ratio_val = rv5_ann_pct_val / rv20_ann_pct_val

    left_go_val = _safe_float(left_go)
    right_go_val = _safe_float(right_go)
    go_imbalance_val = _safe_float(go_imbalance)
    if go_imbalance_val is None and left_go_val is not None and right_go_val is not None:
        go_imbalance_val = left_go_val - right_go_val

    spot_val = _safe_float(spot)
    inner_put_val = _safe_float(inner_put)
    inner_call_val = _safe_float(inner_call)
    center_offset_pct_val = _safe_float(center_offset_pct)
    if (
        center_offset_pct_val is None
        and spot_val is not None
        and spot_val > 0
        and inner_put_val is not None
        and inner_call_val is not None
    ):
        center = (inner_put_val + inner_call_val) / 2.0
        center_offset_pct_val = ((center - spot_val) / spot_val) * 100.0

    passes_vix_rv10 = bool(
        vix_rv10_ratio_val is not None and vix_rv10_ratio_val >= vix_rv10_threshold
    )
    passes_rv5_rv20 = bool(
        rv5_rv20_ratio_val is not None and rv5_rv20_ratio_val <= rv5_rv20_threshold
    )
    switch_to_rr_short = passes_vix_rv10 and passes_rv5_rv20

    return {
        "vix_pct": round(vix_pct, 6) if vix_pct is not None else None,
        "rv10_daily": round(rv10_daily_val, 8) if rv10_daily_val is not None else None,
        "rv10_ann_pct": round(rv10_ann_pct_val, 6) if rv10_ann_pct_val is not None else None,
        "rv5_daily": round(rv5_daily_val, 8) if rv5_daily_val is not None else None,
        "rv5_ann_pct": round(rv5_ann_pct_val, 6) if rv5_ann_pct_val is not None else None,
        "rv20_daily": round(rv20_daily_val, 8) if rv20_daily_val is not None else None,
        "rv20_ann_pct": round(rv20_ann_pct_val, 6) if rv20_ann_pct_val is not None else None,
        "vix_rv10_threshold": vix_rv10_threshold,
        "rv5_rv20_threshold": rv5_rv20_threshold,
        "vix_rv10_ratio": round(vix_rv10_ratio_val, 6) if vix_rv10_ratio_val is not None else None,
        "rv5_rv20_ratio": round(rv5_rv20_ratio_val, 6) if rv5_rv20_ratio_val is not None else None,
        "vix_rv10_excess": round(vix_rv10_ratio_val - vix_rv10_threshold, 6)
        if vix_rv10_ratio_val is not None
        else None,
        "rv5_rv20_headroom": round(rv5_rv20_threshold - rv5_rv20_ratio_val, 6)
        if rv5_rv20_ratio_val is not None
        else None,
        "passes_vix_rv10_gate": passes_vix_rv10,
        "passes_rv5_rv20_gate": passes_rv5_rv20,
        "switch_to_rr_short": switch_to_rr_short,
        "left_go": round(left_go_val, 6) if left_go_val is not None else None,
        "right_go": round(right_go_val, 6) if right_go_val is not None else None,
        "go_imbalance": round(go_imbalance_val, 6) if go_imbalance_val is not None else None,
        "go_imbalance_bad": bool(
            go_imbalance_val is not None
            and go_imbalance_val >= IC_LONG_RR_SHORT_GO_IMBALANCE_BAD_THRESHOLD
        ),
        "spot": round(spot_val, 6) if spot_val is not None else None,
        "inner_put": round(inner_put_val, 6) if inner_put_val is not None else None,
        "inner_call": round(inner_call_val, 6) if inner_call_val is not None else None,
        "center_offset_pct": round(center_offset_pct_val, 6)
        if center_offset_pct_val is not None
        else None,
        "left_asymmetry_signal": bool(
            center_offset_pct_val is not None
            and center_offset_pct_val <= IC_LONG_RR_SHORT_LEFT_ASYMMETRY_THRESHOLD_PCT
        ),
    }


def classify_ic_long_rr_short_regime(**kwargs: Any) -> Dict[str, Any]:
    """
    Return the switch decision plus a human-readable explanation payload.

    This is the narrow, auditable classifier for the validated Leo switch rule.
    It does not replace the broader VRP/BH/GEX classifier below.
    """
    metrics = compute_ic_long_rr_short_regime_metrics(**kwargs)

    reasons: List[str] = []
    if metrics["passes_vix_rv10_gate"]:
        reasons.append(
            f"VIX/RV10 {metrics['vix_rv10_ratio']:.6f} >= {metrics['vix_rv10_threshold']:.5f}"
        )
    elif metrics["vix_rv10_ratio"] is None:
        reasons.append("VIX/RV10 unavailable")
    else:
        reasons.append(
            f"VIX/RV10 {metrics['vix_rv10_ratio']:.6f} < {metrics['vix_rv10_threshold']:.5f}"
        )

    if metrics["passes_rv5_rv20_gate"]:
        reasons.append(
            f"RV5/RV20 {metrics['rv5_rv20_ratio']:.6f} <= {metrics['rv5_rv20_threshold']:.5f}"
        )
    elif metrics["rv5_rv20_ratio"] is None:
        reasons.append("RV5/RV20 unavailable")
    else:
        reasons.append(
            f"RV5/RV20 {metrics['rv5_rv20_ratio']:.6f} > {metrics['rv5_rv20_threshold']:.5f}"
        )

    if metrics["go_imbalance"] is not None:
        state = "bad" if metrics["go_imbalance_bad"] else "neutral/good"
        reasons.append(f"go_imbalance {metrics['go_imbalance']:.6f} ({state})")

    if metrics["center_offset_pct"] is not None:
        side = "left-shifted" if metrics["left_asymmetry_signal"] else "not left-shifted"
        reasons.append(f"center_offset_pct {metrics['center_offset_pct']:.6f}% ({side})")

    action = "SWITCH_TO_RR_SHORT" if metrics["switch_to_rr_short"] else "KEEP_IC_LONG"
    return {
        "action": action,
        "reasons": reasons,
        **metrics,
    }

# Calm-but-scared overlay thresholds
CALM_SCARED_LOW_RV_PCTILE = 0.30
CALM_SCARED_MID_VIX_LOW = 0.20
CALM_SCARED_MID_VIX_HIGH = 0.80
CALM_SCARED_HIGH_GAP_PCTILE = 0.90
CALM_SCARED_TAIL_PCTILE = 0.50

CBOE_HISTORY_SPECS = {
    "vix_cboe": (
        "https://cdn.cboe.com/api/global/us_indices/daily_prices/"
        "VIX_History.csv",
        "close",
    ),
    "spot_cboe": (
        "https://cdn.cboe.com/api/global/us_indices/daily_prices/"
        "SPX_History.csv",
        "spx",
    ),
    "vix9d": (
        "https://cdn.cboe.com/api/global/us_indices/daily_prices/"
        "VIX9D_History.csv",
        "close",
    ),
    "vix3m": (
        "https://cdn.cboe.com/api/global/us_indices/daily_prices/"
        "VIX3M_History.csv",
        "close",
    ),
    "skew": (
        "https://cdn.cboe.com/api/global/us_indices/daily_prices/"
        "SKEW_History.csv",
        "skew",
    ),
}


def _consecutive_true_counts(series: pd.Series) -> pd.Series:
    """Count consecutive True observations."""
    counts = np.zeros(len(series), dtype=int)
    streak = 0
    for i, flag in enumerate(series.fillna(False).astype(bool).to_numpy()):
        if flag:
            streak += 1
        else:
            streak = 0
        counts[i] = streak
    return pd.Series(counts, index=series.index)


def _forward_annualized_rv(log_returns: pd.Series, window: int) -> pd.Series:
    """Future realized vol over the next window, annualized in index points."""
    vals = log_returns.to_numpy(dtype=float)
    out = np.full(len(vals), np.nan)
    ann = math.sqrt(TRADING_DAYS_PER_YEAR) * 100

    for i in range(len(vals) - window):
        future = vals[i + 1:i + 1 + window]
        if np.isnan(future).any():
            continue
        out[i] = np.std(future, ddof=0) * ann

    return pd.Series(out, index=log_returns.index)


def _load_optional_cboe_history(name: str) -> Optional[pd.DataFrame]:
    """Best-effort fetch of official Cboe daily history for extra context."""
    url, value_col = CBOE_HISTORY_SPECS[name]

    try:
        hist = pd.read_csv(url)
    except Exception:
        try:
            resp = requests.get(url, timeout=20)
            resp.raise_for_status()
            hist = pd.read_csv(StringIO(resp.text))
        except Exception as exc:
            print(f"    Warning: unable to load {name.upper()} history: {exc}")
            return None

    hist.columns = [c.lower() for c in hist.columns]
    if "date" not in hist.columns or value_col not in hist.columns:
        print(f"    Warning: unexpected schema for {name.upper()} history")
        return None

    hist["date"] = pd.to_datetime(hist["date"])
    return hist[["date", value_col]].rename(columns={value_col: name})


# ════════════════════════════════════════════════════════════════════════
# DATA ASSEMBLY
# ════════════════════════════════════════════════════════════════════════

def build_daily_panel() -> pd.DataFrame:
    """
    Assemble daily panel from all cache sources.
    Priority: GW > features > vol_decouple > SPX-derived.
    """
    print("=" * 80)
    print("Building Daily Panel")
    print("=" * 80)

    date_dirs = sorted(glob.glob(os.path.join(CACHE_DIR, "????-??-??")))
    print(f"  Found {len(date_dirs)} date directories in cache")

    rows = []
    for date_dir in date_dirs:
        date_str = os.path.basename(date_dir)
        row = {"date": date_str}

        # ── Source 1: close5.json (VIX, spot) ──
        c5_path = os.path.join(date_dir, "close5.json")
        if os.path.exists(c5_path):
            try:
                with open(c5_path) as f:
                    d = json.load(f)
                if d.get("vix") is not None:
                    row["vix"] = float(d["vix"])
                chain = d.get("chain", {})
                if chain.get("_underlying_price"):
                    row["spot"] = float(chain["_underlying_price"])
            except Exception:
                pass

        # ── Source 2: features file (IV surface, RV, VIX1D) ──
        for feat_name in ["close5_features.json", "features_close5.json"]:
            feat_path = os.path.join(date_dir, feat_name)
            if os.path.exists(feat_path):
                try:
                    with open(feat_path) as f:
                        d = json.load(f)
                    for key in ["spot", "vix_1d", "rv", "rv5", "rv10", "rv20",
                                "risk_reversal_25d", "put_skew_slope", "iv_atm",
                                "gex_total"]:
                        val = d.get(key)
                        if val is not None:
                            row[f"feat_{key}"] = float(val)
                except Exception:
                    pass
                break

        # ── Source 3: gw_close5.json (RV data) ──
        gw_path = os.path.join(date_dir, "gw_close5.json")
        if os.path.exists(gw_path):
            try:
                with open(gw_path) as f:
                    d = json.load(f)
                for key in ["vix", "vix_1d", "rv", "rv5", "rv10", "rv20"]:
                    val = d.get(key)
                    if val is not None:
                        row[f"gw_{key}"] = float(val)
            except Exception:
                pass

        rows.append(row)

    daily = pd.DataFrame(rows)
    daily["date"] = pd.to_datetime(daily["date"])
    daily = daily.sort_values("date").reset_index(drop=True)

    # ── Merge with vol_decouple_daily.csv for historical coverage ──
    if os.path.exists(VOL_DECOUPLE_PATH):
        vd = pd.read_csv(VOL_DECOUPLE_PATH)
        vd["date"] = pd.to_datetime(vd["date"])
        rename_map = {
            "rv_10d_ann_pct": "vd_rv10_ann",
            "rv_5d_ann_pct": "vd_rv5_ann",
            "rv_20d_ann_pct": "vd_rv20_ann",
            "vix": "vd_vix",
            "spot": "vd_spot",
        }
        vd = vd.rename(columns=rename_map)
        vd_cols = ["date"] + [c for c in vd.columns if c.startswith("vd_")]
        vd_cols = [c for c in vd_cols if c in vd.columns]
        daily = daily.merge(vd[vd_cols], on="date", how="left")

    # ── Resolve to canonical columns ──

    # VIX
    if "vix" not in daily.columns:
        daily["vix"] = np.nan
    if "vd_vix" in daily.columns:
        daily["vix"] = daily["vix"].fillna(daily["vd_vix"])

    # Spot
    if "spot" not in daily.columns:
        daily["spot"] = np.nan
    if "feat_spot" in daily.columns:
        daily["spot"] = daily["spot"].fillna(daily["feat_spot"])
    if "vd_spot" in daily.columns:
        daily["spot"] = daily["spot"].fillna(daily["vd_spot"])

    # RV fields — GW/features store as daily decimal (e.g. 0.0075),
    # vol_decouple stores as annualized %
    for rv_key, gw_key, feat_key, vd_key in [
        ("rv_daily",  "gw_rv",   "feat_rv",   None),
        ("rv5_daily", "gw_rv5",  "feat_rv5",  "vd_rv5_ann"),
        ("rv10_daily","gw_rv10", "feat_rv10", "vd_rv10_ann"),
        ("rv20_daily","gw_rv20", "feat_rv20", "vd_rv20_ann"),
    ]:
        daily[rv_key] = np.nan
        if gw_key in daily.columns:
            daily[rv_key] = daily[gw_key]
        if feat_key in daily.columns:
            daily[rv_key] = daily[rv_key].fillna(daily[feat_key])
        if vd_key and vd_key in daily.columns:
            # vol_decouple is annualized % → convert to daily decimal
            vd_daily = daily[vd_key] / 100 / math.sqrt(TRADING_DAYS_PER_YEAR)
            daily[rv_key] = daily[rv_key].fillna(vd_daily)

    # VIX1D — GW/features store as decimal (e.g. 0.2646 = 26.46%)
    daily["vix1d"] = np.nan
    if "gw_vix_1d" in daily.columns:
        daily["vix1d"] = daily["gw_vix_1d"] * 100
    if "feat_vix_1d" in daily.columns:
        daily["vix1d"] = daily["vix1d"].fillna(daily["feat_vix_1d"] * 100)

    # Risk reversal (already in natural units)
    if "feat_risk_reversal_25d" in daily.columns:
        daily["risk_reversal_25d"] = daily["feat_risk_reversal_25d"]
    else:
        daily["risk_reversal_25d"] = np.nan

    # ── Optional official Cboe histories for medium-term term structure,
    # short-dated IV, and tail-risk context ──
    print("  Enriching with optional Cboe histories...")
    for name in ["vix_cboe", "spot_cboe", "vix9d", "vix3m", "skew"]:
        hist = _load_optional_cboe_history(name)
        if hist is not None:
            daily = daily.merge(hist, on="date", how="left")
        else:
            daily[name] = np.nan

    daily["vix"] = daily["vix"].fillna(daily["vix_cboe"])
    daily["spot"] = daily["spot"].fillna(daily["spot_cboe"])

    # Log returns from the final spot series, then fill RV gaps from SPX.
    daily["log_return"] = np.log(daily["spot"] / daily["spot"].shift(1))
    daily["rv20_from_spx"] = daily["log_return"].rolling(20).std()
    daily["rv10_from_spx"] = daily["log_return"].rolling(10).std()
    daily["rv5_from_spx"] = daily["log_return"].rolling(5).std()
    daily["rv_from_spx"] = daily["log_return"].abs()

    for rv_key, spx_key in [
        ("rv20_daily", "rv20_from_spx"),
        ("rv10_daily", "rv10_from_spx"),
        ("rv5_daily",  "rv5_from_spx"),
        ("rv_daily",   "rv_from_spx"),
    ]:
        daily[rv_key] = daily[rv_key].fillna(daily[spx_key])

    # Coverage report
    n = len(daily)
    print(f"\n  Panel: {n} dates ({daily['date'].min().date()} to "
          f"{daily['date'].max().date()})")
    for col in ["vix", "spot", "rv20_daily", "rv10_daily", "vix1d",
                "vix9d", "vix3m", "skew", "risk_reversal_25d"]:
        nn = daily[col].notna().sum()
        print(f"    {col:<25} {nn:>5} ({nn/n*100:.1f}%)")

    return daily


# ════════════════════════════════════════════════════════════════════════
# LAYER 1: VARIANCE RISK PREMIUM
# ════════════════════════════════════════════════════════════════════════

def compute_vrp(daily: pd.DataFrame) -> pd.DataFrame:
    """
    VRP = VIX² − RV² (Bollerslev, Tauchen, Zhou 2009).

    Both expressed as annualized variance (decimal).
    VIX=20 → implied variance = (20/100)² = 0.04
    rv20=0.0075 (daily decimal) → realized var = 0.0075² × 252 = 0.01418
    VRP = 0.04 − 0.01418 = 0.02582

    Positive VRP → options overpriced → short premium favorable.
    """
    # Implied variance from VIX (annualized)
    daily["vix_var"] = (daily["vix"] / 100) ** 2

    # Realized variance from RV20 (annualized)
    # rv stored as daily std decimal → var = rv² × 252
    daily["rv20_var"] = daily["rv20_daily"] ** 2 * TRADING_DAYS_PER_YEAR
    daily["rv10_var"] = daily["rv10_daily"] ** 2 * TRADING_DAYS_PER_YEAR

    # VRP = implied − realized (positive when options overpriced)
    daily["vrp"] = daily["vix_var"] - daily["rv20_var"]

    # Scale to "variance points" (× 10000) for readability
    # VRP_pts of +258 means implied var is 2.58% above realized
    daily["vrp_pts"] = daily["vrp"] * 10000

    # Rolling percentile (trailing 252d)
    daily["vrp_pctile"] = daily["vrp"].rolling(
        LOOKBACK_PCTILE, min_periods=60).rank(pct=True)

    # Z-score for sensitivity analysis
    vrp_mean = daily["vrp"].rolling(LOOKBACK_PCTILE, min_periods=60).mean()
    vrp_std = daily["vrp"].rolling(LOOKBACK_PCTILE, min_periods=60).std()
    daily["vrp_zscore"] = (daily["vrp"] - vrp_mean) / vrp_std

    # Also compute VRP using 10-day RV (faster signal)
    daily["vrp_10d"] = daily["vix_var"] - daily["rv10_var"]
    daily["vrp_10d_pts"] = daily["vrp_10d"] * 10000

    return daily


# ════════════════════════════════════════════════════════════════════════
# LAYER 2: BEKAERT-HOEROVA DECOMPOSITION
# ════════════════════════════════════════════════════════════════════════

def compute_bh_decomposition(daily: pd.DataFrame) -> pd.DataFrame:
    """
    Bekaert-Hoerova (2014) decomposition of VIX² into:
      VP (Variance Premium) = risk aversion component
      CV (Conditional Variance) = physical uncertainty

    VIX² = VP + CV
    VP = VIX_var − CV
    CV ≈ E_P[RV_future] ≈ HAR-RV forecast (Corsi 2009)

    The HAR-RV uses three horizons to capture heterogeneous memory:
      CV = w_d × RV_daily_var + w_w × RV_weekly_var + w_m × RV_monthly_var

    VP_share = VP / VIX_var tells us what fraction of implied variance
    is pure risk compensation vs. justified by physical vol.
    """
    # HAR-RV conditional variance forecast
    # Weights from typical HAR regressions (Corsi 2009)
    w_d, w_w, w_m = 0.20, 0.40, 0.40

    rv_daily_var = daily["rv_daily"] ** 2 * TRADING_DAYS_PER_YEAR
    rv_weekly_var = daily["rv5_daily"] ** 2 * TRADING_DAYS_PER_YEAR
    rv_monthly_var = daily["rv20_var"]

    # CV = expected physical variance
    cv_raw = (w_d * rv_daily_var.fillna(0) +
              w_w * rv_weekly_var.fillna(0) +
              w_m * rv_monthly_var.fillna(0))

    # Where we only have monthly data, fall back to it
    daily["cv"] = cv_raw.where(cv_raw > 0, daily["rv20_var"])

    # VP = VIX_var − CV (risk premium component)
    daily["vp"] = daily["vix_var"] - daily["cv"]

    # VP share = VP / VIX_var
    # High VP_share → VIX driven by risk aversion (harvest opportunity)
    # Low VP_share → VIX driven by physical vol (genuine danger)
    daily["vp_share"] = daily["vp"] / daily["vix_var"]
    daily["vp_share"] = daily["vp_share"].clip(-1, 2)

    # BH signal
    conditions = [
        daily["vp_share"] >= VP_SHARE_HARVEST,
        daily["vp_share"] <= VP_SHARE_DANGER,
    ]
    choices = ["harvest", "danger"]
    daily["bh_signal"] = np.select(conditions, choices, default="ambiguous")

    # Rolling context
    daily["vp_share_pctile"] = daily["vp_share"].rolling(
        LOOKBACK_PCTILE, min_periods=60).rank(pct=True)
    daily["cv_pctile"] = daily["cv"].rolling(
        LOOKBACK_PCTILE, min_periods=60).rank(pct=True)

    return daily


# ════════════════════════════════════════════════════════════════════════
# LAYER 3: DEALER GAMMA EXPOSURE (VOLUME-BASED PROXY)
# ════════════════════════════════════════════════════════════════════════
#
# SPX 0DTE/1DTE options have zero OI in EOD snapshots (expires next day),
# so we use volume-weighted gamma asymmetry as a flow-based proxy.
#
# Theory (Barbon & Buraschi 2021):
#   Positive GEX → dealer hedging suppresses vol (sell rallies, buy dips)
#   Negative GEX → dealer hedging amplifies vol (sell dips, buy rallies)
#
# Proxy construction (3 signals):
#   1. Volume-weighted gamma asymmetry: Σ(call_γ×vol) − Σ(put_γ×vol)
#      Higher put volume × gamma → dealers absorbing more put flow → negative
#   2. Put/call volume ratio: high → more put demand → negative GEX proxy
#   3. VIX term structure: VIX1D > VIX (backwardation) → near-term stress →
#      likely negative GEX; VIX > VIX1D (contango) → stable → positive GEX

def _parse_chain_contracts(chain: dict) -> List[dict]:
    """Extract flat contract list from Schwab or ThetaData format."""
    contracts = chain.get("contracts", [])
    if contracts and len(contracts) > 0:
        return contracts

    result = []
    for pc, map_key in [("C", "callExpDateMap"), ("P", "putExpDateMap")]:
        exp_map = chain.get(map_key, {})
        for exp_str, strike_map in exp_map.items():
            for strike_str, opts in strike_map.items():
                for opt in (opts if isinstance(opts, list) else [opts]):
                    result.append({
                        "strike": opt.get("strikePrice", 0),
                        "option_type": pc,
                        "gamma": opt.get("gamma", 0),
                        "open_interest": opt.get("openInterest", 0),
                        "volume": opt.get("totalVolume", 0),
                        "delta": opt.get("delta", 0),
                        "implied_vol": opt.get("volatility", 0),
                    })
    return result


def _compute_gex_for_chain(chain: dict, spot: float) -> dict:
    """
    Compute volume-weighted gamma asymmetry + put/call metrics.

    Returns dict with gex_vol_asym, call_gamma_vol, put_gamma_vol,
    pc_vol_ratio, total_call_vol, total_put_vol.
    Also computes OI-based GEX when OI is available (rare for 1DTE).
    """
    contracts = _parse_chain_contracts(chain)
    if not contracts:
        return {}

    underlying = chain.get("_underlying_price", spot)
    if not underlying or underlying <= 0:
        underlying = spot

    call_gamma_vol = 0.0
    put_gamma_vol = 0.0
    call_gamma_oi = 0.0
    put_gamma_oi = 0.0
    total_call_vol = 0
    total_put_vol = 0

    for c in contracts:
        strike = c.get("strike", 0) or 0
        gamma = c.get("gamma", 0) or 0
        volume = c.get("volume", 0) or 0
        oi = c.get("open_interest", 0) or 0
        opt_type = c.get("option_type", c.get("putCall", "")).upper()

        if gamma <= 0:
            continue
        if underlying > 0 and abs(strike - underlying) / underlying > 0.15:
            continue

        dollar_gamma_vol = gamma * volume * SPX_MULTIPLIER * underlying
        dollar_gamma_oi = gamma * oi * SPX_MULTIPLIER * underlying

        if opt_type in ("C", "CALL"):
            call_gamma_vol += dollar_gamma_vol
            call_gamma_oi += dollar_gamma_oi
            total_call_vol += volume
        elif opt_type in ("P", "PUT"):
            put_gamma_vol += dollar_gamma_vol
            put_gamma_oi += dollar_gamma_oi
            total_put_vol += volume

    result = {
        "gex_vol_asym": call_gamma_vol - put_gamma_vol,
        "call_gamma_vol": call_gamma_vol,
        "put_gamma_vol": put_gamma_vol,
        "total_call_vol": total_call_vol,
        "total_put_vol": total_put_vol,
        "pc_vol_ratio": (total_put_vol / total_call_vol
                         if total_call_vol > 0 else np.nan),
    }

    # OI-based signed GEX (when available)
    if call_gamma_oi > 0 or put_gamma_oi > 0:
        result["gex_oi_signed"] = call_gamma_oi - put_gamma_oi
    else:
        result["gex_oi_signed"] = np.nan

    return result


def compute_gex_daily(daily: pd.DataFrame) -> pd.DataFrame:
    """
    Compute dealer gamma proxy for each date.

    Primary: volume-weighted gamma asymmetry (flow-based).
    Secondary: put/call volume ratio + term structure.
    Tertiary: OI-based GEX (when OI available, rare for 1DTE).

    Composite GEX regime combines all three into compression/amplification.
    """
    print("  Computing volume-weighted dealer gamma proxy...")

    date_dirs = sorted(glob.glob(os.path.join(CACHE_DIR, "????-??-??")))

    gex_rows = {}
    processed = 0
    for date_dir in date_dirs:
        date_str = os.path.basename(date_dir)
        c5_path = os.path.join(date_dir, "close5.json")
        if not os.path.exists(c5_path):
            continue

        try:
            with open(c5_path) as f:
                data = json.load(f)
            chain = data.get("chain", {})
        except Exception:
            continue

        spot = chain.get("_underlying_price", 0) or 0
        if spot <= 0:
            match = daily[daily["date"] == pd.Timestamp(date_str)]
            if len(match) > 0 and pd.notna(match.iloc[0].get("spot")):
                spot = match.iloc[0]["spot"]
            else:
                continue

        result = _compute_gex_for_chain(chain, spot)
        if result:
            gex_rows[date_str] = result
            processed += 1

    print(f"    GEX proxy computed for {processed} dates")

    if gex_rows:
        gex_df = pd.DataFrame.from_dict(gex_rows, orient="index")
        gex_df.index = pd.to_datetime(gex_df.index)
        gex_df.index.name = "date"
        gex_df = gex_df.reset_index()
        daily = daily.merge(gex_df, on="date", how="left")
    else:
        for col in ["gex_vol_asym", "call_gamma_vol", "put_gamma_vol",
                     "pc_vol_ratio", "gex_oi_signed"]:
            daily[col] = np.nan

    # ── Normalize volume asymmetry for cross-date comparison ──
    # Divide by (spot² × total volume) to remove market-cap and activity scaling
    total_vol = (daily.get("total_call_vol", 0).fillna(0) +
                 daily.get("total_put_vol", 0).fillna(0))
    daily["gex_vol_norm"] = daily["gex_vol_asym"] / (
        daily["spot"] ** 2 * total_vol.replace(0, np.nan))

    # ── Rolling percentiles ──
    daily["gex_vol_asym_pctile"] = daily["gex_vol_asym"].rolling(
        LOOKBACK_PCTILE, min_periods=30).rank(pct=True)
    daily["pc_vol_ratio_pctile"] = daily["pc_vol_ratio"].rolling(
        LOOKBACK_PCTILE, min_periods=30).rank(pct=True)

    # ── Composite GEX regime ──
    # Combine volume asymmetry + P/C ratio + term structure
    # Positive vol asymmetry (call γ×vol > put γ×vol) → compression
    # Low P/C ratio → less put demand → compression
    # Contango (VIX > VIX1D) → stable near-term → compression

    vol_asym_signal = np.where(
        daily["gex_vol_asym_pctile"] >= 0.60, 1,
        np.where(daily["gex_vol_asym_pctile"] <= 0.40, -1, 0))

    pc_signal = np.where(
        daily["pc_vol_ratio_pctile"] <= 0.40, 1,  # low P/C → compression
        np.where(daily["pc_vol_ratio_pctile"] >= 0.60, -1, 0))

    # Term structure signal (VIX > VIX1D = contango = positive)
    term_spread = daily["vix"] - daily["vix1d"]
    term_signal = np.where(
        term_spread > 2, 1,   # meaningful contango
        np.where(term_spread < -2, -1, 0))  # meaningful backwardation

    # Weighted composite (vol asymmetry is primary)
    daily["gex_composite_score"] = (
        0.50 * pd.Series(vol_asym_signal, index=daily.index).fillna(0) +
        0.30 * pd.Series(pc_signal, index=daily.index).fillna(0) +
        0.20 * pd.Series(term_signal, index=daily.index).fillna(0))

    daily["gex_regime"] = np.where(
        daily["gex_composite_score"] > 0.3, "compression",
        np.where(daily["gex_composite_score"] < -0.3, "amplification",
                 "neutral"))

    # Where we have no chain data at all, mark empty
    daily.loc[daily["gex_vol_asym"].isna(), "gex_regime"] = ""

    n_comp = (daily["gex_regime"] == "compression").sum()
    n_amp = (daily["gex_regime"] == "amplification").sum()
    n_neut = (daily["gex_regime"] == "neutral").sum()
    print(f"    Regimes: compression={n_comp}, amplification={n_amp}, "
          f"neutral={n_neut}")

    return daily


# ════════════════════════════════════════════════════════════════════════
# LAYER 4: 0DTE / SHORT-DATED VRP
# ════════════════════════════════════════════════════════════════════════

def compute_0dte_vrp(daily: pd.DataFrame) -> pd.DataFrame:
    """
    Short-dated (1-day) VRP using VIX1D.

    VRP_1d = VIX1D_var − RV_1d_var

    Key insight from 0DTE literature: intraday VRP exists but returns
    are dominated by realized skewness. Mechanical selling without
    regime awareness → unacceptably wide return distributions.
    """
    # VIX1D → 1-day implied variance (VIX1D is in % terms)
    daily["vix1d_var"] = (daily["vix1d"] / 100) ** 2

    # 1-day realized variance (from daily log returns)
    daily["rv1d_var"] = daily["log_return"] ** 2 * TRADING_DAYS_PER_YEAR

    # 1-day VRP
    daily["vrp_1d"] = daily["vix1d_var"] - daily["rv1d_var"]
    daily["vrp_1d_pts"] = daily["vrp_1d"] * 10000

    # Realized skewness (trailing 20d) — key predictor per 0DTE literature
    daily["realized_skew_20d"] = daily["log_return"].rolling(20).apply(
        lambda x: pd.Series(x).skew(), raw=False)

    # Realized kurtosis (tail risk)
    daily["realized_kurt_20d"] = daily["log_return"].rolling(20).apply(
        lambda x: pd.Series(x).kurtosis(), raw=False)

    # 0DTE regime: favorable when VRP_1d positive AND skew not extreme
    daily["dte0_favorable"] = (
        (daily["vrp_1d"] > 0) &
        (daily["realized_skew_20d"] > -1.0)
    )

    return daily


# ════════════════════════════════════════════════════════════════════════
# CALM-BUT-SCARED OVERLAY
# ════════════════════════════════════════════════════════════════════════

def compute_calm_scared_overlay(daily: pd.DataFrame) -> pd.DataFrame:
    """
    Add a separate "calm but scared" regime overlay.

    This deliberately separates:
      1) Live state detection (low trailing RV + normal VIX + high trail gap)
      2) Ex-post validation (matched-horizon implied vs future realized)

    The overlay does not change the existing HARVEST/AVOID classifier.
    It gives a second lens that can later be used as a filter or switch rule.
    """
    ann = math.sqrt(TRADING_DAYS_PER_YEAR) * 100

    # Annualized realized vol in the same units as VIX index points.
    daily["rv10_ann_pct"] = daily["rv10_daily"] * ann
    daily["rv20_ann_pct"] = daily["rv20_daily"] * ann

    # Forward realized vol is for research/validation, not live classification.
    daily["fwd_rv10_ann_pct"] = _forward_annualized_rv(daily["log_return"], 10)
    daily["fwd_rv21_ann_pct"] = _forward_annualized_rv(daily["log_return"], 21)

    valid_gap = (
        daily["vix"].gt(0) &
        daily["rv10_ann_pct"].gt(0)
    )
    daily["trail_gap"] = np.where(
        valid_gap,
        np.log(daily["vix"] / daily["rv10_ann_pct"]),
        np.nan,
    )

    daily["econ_gap_30"] = np.where(
        daily["fwd_rv21_ann_pct"].gt(0),
        daily["vix"] / daily["fwd_rv21_ann_pct"],
        np.nan,
    )
    daily["econ_gap_10"] = np.where(
        daily["vix9d"].gt(0) & daily["fwd_rv10_ann_pct"].gt(0),
        daily["vix9d"] / daily["fwd_rv10_ann_pct"],
        np.nan,
    )

    daily["vix_pctile_cb"] = daily["vix"].rolling(
        LOOKBACK_PCTILE, min_periods=60).rank(pct=True)
    daily["rv10_pctile_cb"] = daily["rv10_ann_pct"].rolling(
        LOOKBACK_PCTILE, min_periods=60).rank(pct=True)
    daily["trail_gap_pctile"] = daily["trail_gap"].rolling(
        LOOKBACK_PCTILE, min_periods=60).rank(pct=True)

    # Tail-risk context: prefer official Cboe SKEW, fall back to -RR25d.
    daily["tail_premium_proxy"] = daily["skew"]
    tail_fallback = -daily["risk_reversal_25d"]
    daily["tail_premium_proxy"] = daily["tail_premium_proxy"].fillna(tail_fallback)
    daily["tail_premium_pctile"] = daily["tail_premium_proxy"].rolling(
        LOOKBACK_PCTILE, min_periods=30).rank(pct=True)

    # Medium-term term structure: prefer VIX3M-VIX; fall back to front-end
    # contango when VIX1D is available.
    daily["term_3m_1m"] = daily["vix3m"] - daily["vix"]
    fallback_term = daily["vix"] - daily["vix1d"]
    daily["term_structure_proxy"] = daily["term_3m_1m"].fillna(fallback_term)
    daily["term_structure_positive"] = daily["term_structure_proxy"] > 0

    # Live detector.
    daily["cb_low_rv"] = daily["rv10_pctile_cb"] <= CALM_SCARED_LOW_RV_PCTILE
    daily["cb_mid_vix"] = (
        daily["vix_pctile_cb"] >= CALM_SCARED_MID_VIX_LOW
    ) & (
        daily["vix_pctile_cb"] <= CALM_SCARED_MID_VIX_HIGH
    )
    daily["cb_high_gap"] = (
        daily["trail_gap_pctile"] >= CALM_SCARED_HIGH_GAP_PCTILE
    )
    daily["cb_tail_rich"] = (
        daily["tail_premium_pctile"] >= CALM_SCARED_TAIL_PCTILE
    )

    daily["calm_scared_core"] = (
        daily["cb_low_rv"] &
        daily["cb_mid_vix"] &
        daily["cb_high_gap"]
    )
    daily["calm_scared_strict"] = (
        daily["calm_scared_core"] &
        daily["cb_tail_rich"] &
        daily["term_structure_positive"].fillna(False)
    )

    # Score ranges from 0 to 1 and is meant for ranking, not thresholding.
    rv_score = np.clip(
        (CALM_SCARED_LOW_RV_PCTILE - daily["rv10_pctile_cb"]) /
        CALM_SCARED_LOW_RV_PCTILE,
        0,
        1,
    )
    gap_score = np.clip(
        (daily["trail_gap_pctile"] - 0.50) /
        (CALM_SCARED_HIGH_GAP_PCTILE - 0.50),
        0,
        1,
    )
    vix_mid_score = daily["cb_mid_vix"].astype(float)
    tail_score = daily["tail_premium_pctile"].fillna(0.50)
    term_score = daily["term_structure_positive"].fillna(False).astype(float)
    daily["calm_scared_score"] = (
        0.35 * gap_score +
        0.25 * rv_score +
        0.15 * vix_mid_score +
        0.15 * tail_score +
        0.10 * term_score
    )

    daily["calm_scared_core_persist"] = _consecutive_true_counts(
        daily["calm_scared_core"])
    daily["calm_scared_strict_persist"] = _consecutive_true_counts(
        daily["calm_scared_strict"])
    daily["calm_scared_signal"] = "OFF"
    daily.loc[
        daily["calm_scared_core_persist"] >= PERSISTENCE_DAYS,
        "calm_scared_signal"
    ] = "CORE"
    daily.loc[
        daily["calm_scared_strict_persist"] >= PERSISTENCE_DAYS,
        "calm_scared_signal"
    ] = "STRICT"

    return daily


# ════════════════════════════════════════════════════════════════════════
# COMPOSITE BINARY SIGNAL
# ════════════════════════════════════════════════════════════════════════

def compute_composite_signal(daily: pd.DataFrame) -> pd.DataFrame:
    """
    Combine layers into binary regime signal (Nystrup et al.).

    Binary regime-conditioned rules outperform continuous dynamic sizing.
    Primary value: knowing when NOT to trade.

    HARVEST: VRP rich AND (VP dominant OR GEX compressing)
    AVOID:   VRP poor OR (CV dominant AND GEX amplifying)
    NEUTRAL: Mixed signals

    GEX as override:
      - Negative GEX can downgrade HARVEST → NEUTRAL
      - Positive GEX can upgrade NEUTRAL → HARVEST (not AVOID → HARVEST)
    """
    n = len(daily)
    signals = ["NEUTRAL"] * n
    scores = [0.5] * n

    for i in range(n):
        row = daily.iloc[i]

        vrp_pctile = row.get("vrp_pctile", np.nan)
        vp_share = row.get("vp_share", np.nan)
        bh_signal = row.get("bh_signal", "")
        gex_regime = row.get("gex_regime", "")

        if pd.isna(vrp_pctile):
            continue

        # ── Score components (0=worst, 1=best for short premium) ──
        vrp_score = vrp_pctile if not pd.isna(vrp_pctile) else 0.5
        vp_score = (min(1.0, max(0.0, vp_share))
                    if not pd.isna(vp_share) else 0.5)
        gex_score = (0.7 if gex_regime == "compression"
                     else (0.3 if gex_regime == "amplification" else 0.5))

        # Weighted composite (VRP primary per Bollerslev et al.)
        score = 0.50 * vrp_score + 0.30 * vp_score + 0.20 * gex_score
        scores[i] = score

        # ── Binary classification ──
        vrp_rich = vrp_pctile >= VRP_HARVEST_PCTILE
        vrp_poor = vrp_pctile <= VRP_AVOID_PCTILE
        vp_dominant = bh_signal == "harvest"
        cv_dominant = bh_signal == "danger"
        gex_pos = gex_regime == "compression"
        gex_neg = gex_regime == "amplification"

        if vrp_rich and (vp_dominant or gex_pos):
            signals[i] = "HARVEST"
        elif vrp_poor or (cv_dominant and gex_neg):
            signals[i] = "AVOID"
        else:
            # GEX override: positive GEX upgrades neutral with decent VRP
            if gex_pos and vrp_pctile >= 0.50:
                signals[i] = "HARVEST"
            # GEX override: negative GEX downgrades when VRP isn't strong
            elif gex_neg and vrp_pctile < VRP_HARVEST_PCTILE:
                signals[i] = "AVOID"
            else:
                signals[i] = "NEUTRAL"

    daily["signal"] = signals
    daily["harvest_score"] = scores

    # ── Persistence (consecutive days in same signal) ──
    daily["signal_persist"] = 0
    count = 0
    prev = ""
    for i in range(n):
        sig = daily.iloc[i]["signal"]
        if sig == prev:
            count += 1
        else:
            count = 1
            prev = sig
        daily.iat[i, daily.columns.get_loc("signal_persist")] = count

    # Confirmed signal (must persist PERSISTENCE_DAYS)
    daily["signal_confirmed"] = daily["signal"]
    daily.loc[
        daily["signal_persist"] < PERSISTENCE_DAYS,
        "signal_confirmed"
    ] = "NEUTRAL"

    return daily


# ════════════════════════════════════════════════════════════════════════
# TRADE INTEGRATION
# ════════════════════════════════════════════════════════════════════════

def merge_with_trades(daily: pd.DataFrame,
                      panel_path: str = PANEL_PATH
                      ) -> Optional[pd.DataFrame]:
    """Merge regime signals with IC_LONG trade panel for backtesting."""
    if not os.path.exists(panel_path):
        print(f"\n  Trade panel not found: {panel_path}")
        return None

    print(f"\n{'=' * 80}")
    print("Merging with Trade Panel")
    print("=" * 80)

    panel = pd.read_csv(panel_path)
    panel["date"] = pd.to_datetime(panel["date"])
    print(f"  Trades: {len(panel)} "
          f"({panel['date'].min().date()} to {panel['date'].max().date()})")

    merge_cols = [
        "date", "vix_var", "rv20_var", "vrp", "vrp_pts", "vrp_pctile",
        "vrp_zscore", "vrp_10d", "vrp_10d_pts",
        "cv", "vp", "vp_share", "bh_signal",
        "gex_vol_asym", "gex_regime", "gex_composite_score",
        "pc_vol_ratio", "gex_vol_norm",
        "vrp_1d", "realized_skew_20d", "realized_kurt_20d", "dte0_favorable",
        "rv10_ann_pct", "fwd_rv10_ann_pct", "fwd_rv21_ann_pct",
        "trail_gap", "trail_gap_pctile", "econ_gap_10", "econ_gap_30",
        "tail_premium_proxy", "tail_premium_pctile",
        "term_3m_1m", "term_structure_proxy", "term_structure_positive",
        "calm_scared_core", "calm_scared_strict",
        "calm_scared_core_persist", "calm_scared_strict_persist",
        "calm_scared_score", "calm_scared_signal",
        "signal", "signal_confirmed", "harvest_score", "signal_persist",
    ]
    available = [c for c in merge_cols if c in daily.columns]
    merged = panel.merge(daily[available], on="date", how="left")

    # Period column for grouping
    def get_period(dt):
        y = dt.year
        if y < 2020:
            return "pre-2020"
        elif y <= 2023:
            return "2020-2023"
        elif y <= 2025:
            return "2024-2025"
        return "2026"

    merged["period"] = merged["date"].apply(get_period)

    has_signal = merged["signal"].notna().sum()
    no_signal = (merged["signal"].isna() | (merged["signal"] == "")).sum()
    print(f"  Trades with regime signal: {has_signal}/{len(merged)}")
    if no_signal > 0:
        print(f"  Trades missing signal: {no_signal} "
              f"(pre-data-coverage dates)")

    return merged


# ════════════════════════════════════════════════════════════════════════
# ANALYSIS & REPORTING
# ════════════════════════════════════════════════════════════════════════

def _stats(df):
    """Compute trade stats for a group."""
    n = len(df)
    if n == 0:
        return {"n": 0, "wr": 0, "avg": 0, "total": 0,
                "avg_s": 0, "total_s": 0, "sharpe": 0}
    pnl = df["orig_pnl_pts"]
    sized = df["orig_pnl_sized_pts"]
    wins = (pnl > 0).sum()
    sharpe = pnl.mean() / pnl.std() * math.sqrt(252) if pnl.std() > 0 else 0
    return {
        "n": n, "wr": wins / n * 100,
        "avg": pnl.mean(), "total": pnl.sum(),
        "avg_s": sized.mean(), "total_s": sized.sum(),
        "sharpe": sharpe,
    }


def _fmt_row(label, s, width=15):
    """Format a stats row for printing."""
    if s["n"] == 0:
        return f"  {label:<{width}} {'—':>5}"
    return (f"  {label:<{width}} {s['n']:>5} {s['wr']:>6.1f}% "
            f"{s['avg']:>+9.4f} {s['total']:>+10.2f} "
            f"{s['avg_s']:>+9.4f} {s['total_s']:>+10.2f} "
            f"{s['sharpe']:>+6.2f}")


def report_daily_stats(daily: pd.DataFrame):
    """Print daily regime statistics."""
    print(f"\n{'=' * 80}")
    print("DAILY REGIME STATISTICS")
    print("=" * 80)

    n = len(daily)
    has_vrp = daily["vrp"].notna().sum()
    has_gex = daily["gex_vol_asym"].notna().sum()

    print(f"\n  Total days: {n}")
    print(f"  Days with VRP: {has_vrp}")
    print(f"  Days with GEX: {has_gex}")

    # VRP distribution
    vrp_pts = daily["vrp_pts"].dropna()
    if len(vrp_pts) > 0:
        print(f"\n  VRP Distribution (variance points × 10000):")
        print(f"    Mean:   {vrp_pts.mean():>+8.1f}")
        print(f"    Median: {vrp_pts.median():>+8.1f}")
        print(f"    Std:    {vrp_pts.std():>8.1f}")
        for p in [10, 25, 50, 75, 90]:
            print(f"    P{p:<2}:    {vrp_pts.quantile(p/100):>+8.1f}")

    # BH decomposition
    vp_share = daily["vp_share"].dropna()
    if len(vp_share) > 0:
        print(f"\n  VP Share Distribution (fraction of VIX² from risk premium):")
        print(f"    Mean:   {vp_share.mean():>7.1%}")
        print(f"    Median: {vp_share.median():>7.1%}")
        n_harvest = (vp_share >= VP_SHARE_HARVEST).sum()
        n_danger = (vp_share <= VP_SHARE_DANGER).sum()
        print(f"    Days harvest (VP≥{VP_SHARE_HARVEST:.0%}): {n_harvest}")
        print(f"    Days danger  (VP≤{VP_SHARE_DANGER:.0%}): {n_danger}")

    # GEX regime
    if has_gex > 0:
        gex_pos = (daily["gex_regime"] == "compression").sum()
        gex_neg = (daily["gex_regime"] == "amplification").sum()
        print(f"\n  GEX Regime Distribution:")
        print(f"    Compression (positive):   {gex_pos} ({gex_pos/has_gex*100:.1f}%)")
        print(f"    Amplification (negative): {gex_neg} ({gex_neg/has_gex*100:.1f}%)")

    # Signal distribution
    print(f"\n  Composite Signal (confirmed):")
    for sig in ["HARVEST", "NEUTRAL", "AVOID"]:
        n_sig = (daily["signal_confirmed"] == sig).sum()
        pct = n_sig / n * 100
        print(f"    {sig:<10}: {n_sig:>5} ({pct:.1f}%)")

    if "calm_scared_signal" in daily.columns:
        print(f"\n  Calm-But-Scared Overlay:")
        for sig in ["STRICT", "CORE", "OFF"]:
            n_sig = (daily["calm_scared_signal"] == sig).sum()
            pct = n_sig / n * 100
            print(f"    {sig:<10}: {n_sig:>5} ({pct:.1f}%)")

        tg = daily["trail_gap"].dropna()
        if len(tg) > 0:
            print(f"    trail_gap mean / median: {tg.mean():+.3f} / "
                  f"{tg.median():+.3f}")

        econ30 = daily["econ_gap_30"].dropna()
        econ10 = daily["econ_gap_10"].dropna()
        if len(econ30) > 0:
            print(f"    econ_gap_30 mean: {econ30.mean():.2f}x")
        if len(econ10) > 0:
            print(f"    econ_gap_10 mean: {econ10.mean():.2f}x")

    # Transition matrix
    signals = daily["signal_confirmed"]
    if len(signals) > 1:
        print(f"\n  Signal Transition Matrix:")
        trans = pd.crosstab(signals.shift(1), signals, normalize="index")
        for from_sig in ["HARVEST", "NEUTRAL", "AVOID"]:
            if from_sig in trans.index:
                probs = [f"{trans.loc[from_sig].get(s, 0):.1%}"
                         for s in ["HARVEST", "NEUTRAL", "AVOID"]]
                print(f"    {from_sig:>10} → H:{probs[0]:>6}  "
                      f"N:{probs[1]:>6}  A:{probs[2]:>6}")


def report_regime_performance(merged: pd.DataFrame):
    """Analyze how regime signals predict IC_LONG trade outcomes."""
    print(f"\n{'=' * 80}")
    print("REGIME SIGNAL vs TRADE PERFORMANCE")
    print("=" * 80)

    all_s = _stats(merged)
    print(f"\n  Baseline: {all_s['n']} trades, WR={all_s['wr']:.1f}%, "
          f"Avg={all_s['avg']:+.4f}, Total={all_s['total']:+.2f}")

    # ── By confirmed signal ──
    hdr = (f"  {'Signal':<12} {'N':>5} {'WR%':>7} {'Avg PnL':>9} "
           f"{'Tot PnL':>10} {'Avg Sized':>9} {'Tot Sized':>10} "
           f"{'Sharpe':>6}")
    print(f"\n{hdr}")
    print(f"  {'─' * 12} {'─' * 5} {'─' * 7} {'─' * 9} "
          f"{'─' * 10} {'─' * 9} {'─' * 10} {'─' * 6}")

    for signal in ["HARVEST", "NEUTRAL", "AVOID"]:
        sub = merged[merged["signal_confirmed"] == signal]
        print(_fmt_row(signal, _stats(sub), 12))

    # ── Separation ──
    h_s = _stats(merged[merged["signal_confirmed"] == "HARVEST"])
    a_s = _stats(merged[merged["signal_confirmed"] == "AVOID"])
    if h_s["n"] > 0 and a_s["n"] > 0:
        sep = h_s["avg"] - a_s["avg"]
        print(f"\n  Separation (HARVEST − AVOID avg): {sep:+.4f} pts")
        print(f"  HARVEST Sharpe: {h_s['sharpe']:+.2f}")
        print(f"  AVOID Sharpe:   {a_s['sharpe']:+.2f}")

    # ── Layer-by-layer analysis ──
    print(f"\n  --- Layer-Level Analysis ---")

    # VRP quartiles
    m_vrp = merged[merged["vrp_pctile"].notna()].copy()
    if len(m_vrp) > 10:
        print(f"\n  VRP Quartile:")
        m_vrp["vrp_q"] = pd.qcut(
            m_vrp["vrp_pctile"], 4,
            labels=["Q1(low)", "Q2", "Q3", "Q4(high)"],
            duplicates="drop")
        for q in ["Q1(low)", "Q2", "Q3", "Q4(high)"]:
            sub = m_vrp[m_vrp["vrp_q"] == q]
            s = _stats(sub)
            if s["n"] > 0:
                print(f"    {q:<12} N={s['n']:>4}, WR={s['wr']:.1f}%, "
                      f"Avg={s['avg']:+.4f}, Tot={s['total']:+.2f}")

    # BH decomposition
    print(f"\n  Bekaert-Hoerova Signal:")
    for sig in ["harvest", "ambiguous", "danger"]:
        sub = merged[merged["bh_signal"] == sig]
        s = _stats(sub)
        if s["n"] > 0:
            print(f"    {sig:<12} N={s['n']:>4}, WR={s['wr']:.1f}%, "
                  f"Avg={s['avg']:+.4f}, Tot={s['total']:+.2f}")

    # GEX regime
    print(f"\n  GEX Regime:")
    for reg in ["compression", "amplification", ""]:
        label = reg if reg else "(no data)"
        sub = merged[merged["gex_regime"] == reg]
        s = _stats(sub)
        if s["n"] > 0:
            print(f"    {label:<15} N={s['n']:>4}, WR={s['wr']:.1f}%, "
                  f"Avg={s['avg']:+.4f}, Tot={s['total']:+.2f}")

    if "calm_scared_signal" in merged.columns:
        print(f"\n  Calm-But-Scared Overlay:")
        for reg in ["STRICT", "CORE", "OFF"]:
            sub = merged[merged["calm_scared_signal"] == reg]
            s = _stats(sub)
            if s["n"] > 0:
                print(f"    {reg:<15} N={s['n']:>4}, WR={s['wr']:.1f}%, "
                      f"Avg={s['avg']:+.4f}, Tot={s['total']:+.2f}")

    # ── Period breakdown ──
    print(f"\n  Period Breakdown:")
    print(f"  {'Period':<12} {'Signal':<10} {'N':>4} {'WR':>7} "
          f"{'Avg PnL':>9} {'Tot PnL':>10}")
    print(f"  {'─' * 12} {'─' * 10} {'─' * 4} {'─' * 7} "
          f"{'─' * 9} {'─' * 10}")
    for period in ["pre-2020", "2020-2023", "2024-2025", "2026"]:
        for signal in ["HARVEST", "NEUTRAL", "AVOID"]:
            sub = merged[
                (merged["period"] == period) &
                (merged["signal_confirmed"] == signal)]
            s = _stats(sub)
            if s["n"] > 0:
                print(f"  {period:<12} {signal:<10} {s['n']:>4} "
                      f"{s['wr']:>6.1f}% {s['avg']:>+9.4f} "
                      f"{s['total']:>+10.2f}")

    # ── Switch analysis ──
    if "rr_short_pnl_sized_pts" in merged.columns:
        avoided = merged[merged["signal_confirmed"] == "AVOID"]
        if len(avoided) > 0:
            ic_pnl = avoided["orig_pnl_sized_pts"].sum()
            rr_pnl = avoided["rr_short_pnl_sized_pts"].sum()
            edge = rr_pnl - ic_pnl
            print(f"\n  Switch Analysis (AVOID → RR_SHORT):")
            print(f"    IC_LONG sized PnL:  {ic_pnl:>+10.2f}")
            print(f"    RR_SHORT sized PnL: {rr_pnl:>+10.2f}")
            print(f"    Switch edge:        {edge:>+10.2f}")

    # ── Comparison to user's existing rule ──
    if "VIX_RV10_ratio" in merged.columns and "rv5_rv20_ratio" in merged.columns:
        merged["user_rule"] = (
            (merged["VIX_RV10_ratio"] >= IC_LONG_RR_SHORT_VIX_RV10_THRESHOLD) &
            (merged["rv5_rv20_ratio"] <= IC_LONG_RR_SHORT_RV5_RV20_THRESHOLD))

        print(f"\n  --- Comparison to Existing User Rule ---")
        new = merged["signal_confirmed"] == "AVOID"
        old = merged["user_rule"] == True
        both = (new & old).sum()
        new_only = (new & ~old).sum()
        old_only = (~new & old).sum()
        neither = (~new & ~old).sum()

        print(f"    Both flag:      {both:>4}")
        print(f"    VRP-only flags: {new_only:>4} (VRP catches, user misses)")
        print(f"    User-only flags:{old_only:>4} (user catches, VRP misses)")
        print(f"    Neither flags:  {neither:>4}")

        # False positive comparison
        for label, mask in [("VRP AVOID", new), ("User Rule", old)]:
            flagged = merged[mask]
            if len(flagged) > 0:
                fp = (flagged["orig_pnl_pts"] > 0).sum()
                fp_rate = fp / len(flagged) * 100
                print(f"    {label} FP rate: {fp}/{len(flagged)} "
                      f"= {fp_rate:.1f}%")


def deep_dive_2026(daily: pd.DataFrame, merged: Optional[pd.DataFrame]):
    """Detailed 2026 analysis."""
    print(f"\n{'=' * 80}")
    print("2026 DEEP DIVE")
    print("=" * 80)

    d26 = daily[daily["date"].dt.year == 2026].copy()
    if len(d26) == 0:
        print("  No 2026 data")
        return

    print(f"\n  {'Date':<12} {'VIX':>6} {'VRP_pts':>8} {'VRP_pct':>7} "
          f"{'VP_shr':>7} {'BH':>8} {'GEX_reg':>12} "
          f"{'Signal':>8} {'Score':>6}")
    print(f"  {'─' * 12} {'─' * 6} {'─' * 8} {'─' * 7} "
          f"{'─' * 7} {'─' * 8} {'─' * 12} "
          f"{'─' * 8} {'─' * 6}")

    for _, r in d26.iterrows():
        dt = r["date"].strftime("%Y-%m-%d")
        vix = f"{r['vix']:.1f}" if pd.notna(r.get("vix")) else "  n/a"
        vrp_p = (f"{r['vrp_pts']:+.1f}"
                 if pd.notna(r.get("vrp_pts")) else "    n/a")
        vrp_pct = (f"{r['vrp_pctile']:.2f}"
                   if pd.notna(r.get("vrp_pctile")) else "  n/a")
        vps = (f"{r['vp_share']:.1%}"
               if pd.notna(r.get("vp_share")) else "  n/a")
        bh = str(r.get("bh_signal", ""))[:8]
        gex = str(r.get("gex_regime", ""))[:12]
        sig = r.get("signal_confirmed", "")
        score = (f"{r['harvest_score']:.2f}"
                 if pd.notna(r.get("harvest_score")) else " n/a")

        print(f"  {dt:<12} {vix:>6} {vrp_p:>8} {vrp_pct:>7} "
              f"{vps:>7} {bh:>8} {gex:>12} "
              f"{sig:>8} {score:>6}")

    if "calm_scared_signal" in d26.columns:
        active = d26[d26["calm_scared_signal"] != "OFF"]
        if len(active) > 0:
            print(f"\n  2026 Calm-But-Scared Days:")
            print(f"  {'Date':<12} {'RV10':>6} {'Gap':>7} {'GapPct':>7} "
                  f"{'Tail':>7} {'Term':>7} {'E30':>6} {'Sig':>8}")
            print(f"  {'─' * 12} {'─' * 6} {'─' * 7} {'─' * 7} "
                  f"{'─' * 7} {'─' * 7} {'─' * 6} {'─' * 8}")

            for _, r in active.iterrows():
                dt = r["date"].strftime("%Y-%m-%d")
                rv10 = (f"{r['rv10_ann_pct']:.1f}"
                        if pd.notna(r.get("rv10_ann_pct")) else "  n/a")
                gap = (f"{r['trail_gap']:+.2f}"
                       if pd.notna(r.get("trail_gap")) else "   n/a")
                gap_pct = (f"{r['trail_gap_pctile']:.2f}"
                           if pd.notna(r.get("trail_gap_pctile")) else "  n/a")
                tail = (f"{r['tail_premium_proxy']:.1f}"
                        if pd.notna(r.get("tail_premium_proxy")) else "  n/a")
                term = (f"{r['term_structure_proxy']:+.2f}"
                        if pd.notna(r.get("term_structure_proxy")) else "   n/a")
                econ30 = (f"{r['econ_gap_30']:.2f}"
                          if pd.notna(r.get("econ_gap_30")) else "  n/a")
                sig = r.get("calm_scared_signal", "")

                print(f"  {dt:<12} {rv10:>6} {gap:>7} {gap_pct:>7} "
                      f"{tail:>7} {term:>7} {econ30:>6} {sig:>8}")

    if merged is not None:
        t26 = merged[merged["period"] == "2026"]
        if len(t26) > 0:
            print(f"\n  2026 Trades:")
            print(f"  {'Date':<12} {'Signal':>8} {'Score':>6} "
                  f"{'VRP_pct':>7} {'PnL':>8} {'Sized':>9} {'W/L':>4}")
            print(f"  {'─' * 12} {'─' * 8} {'─' * 6} "
                  f"{'─' * 7} {'─' * 8} {'─' * 9} {'─' * 4}")

            for _, r in t26.sort_values("date").iterrows():
                dt = r["date"].strftime("%Y-%m-%d")
                sig = r.get("signal_confirmed", "")
                score = (f"{r['harvest_score']:.2f}"
                         if pd.notna(r.get("harvest_score")) else " n/a")
                vpct = (f"{r['vrp_pctile']:.2f}"
                        if pd.notna(r.get("vrp_pctile")) else "  n/a")
                pnl = r["orig_pnl_pts"]
                sized = r["orig_pnl_sized_pts"]
                wl = "W" if pnl > 0 else "L"

                print(f"  {dt:<12} {sig:>8} {score:>6} "
                      f"{vpct:>7} {pnl:>+8.3f} {sized:>+9.2f} {wl:>4}")


# ════════════════════════════════════════════════════════════════════════
# THRESHOLD SWEEP
# ════════════════════════════════════════════════════════════════════════

def sweep_thresholds(daily: pd.DataFrame, merged: pd.DataFrame):
    """
    Sweep VRP and VP_share thresholds to find optimal binary cutoffs.
    Tests separation between HARVEST and AVOID across threshold combos.
    """
    print(f"\n{'=' * 80}")
    print("THRESHOLD SENSITIVITY SWEEP")
    print("=" * 80)

    best_sep = -999
    best_params = {}
    results = []

    for vrp_h in [0.60, 0.65, 0.70, 0.75, 0.80]:
        for vrp_a in [0.20, 0.25, 0.30, 0.35, 0.40]:
            for vp_h in [0.50, 0.55, 0.60, 0.65, 0.70]:
                for vp_d in [0.30, 0.35, 0.40, 0.45]:
                    if vrp_a >= vrp_h or vp_d >= vp_h:
                        continue

                    # Classify trades
                    harvest_mask = (
                        (merged["vrp_pctile"] >= vrp_h) &
                        (merged["vp_share"] >= vp_h))
                    avoid_mask = (
                        (merged["vrp_pctile"] <= vrp_a) |
                        (merged["vp_share"] <= vp_d))

                    h_trades = merged[harvest_mask & merged["vrp_pctile"].notna()]
                    a_trades = merged[avoid_mask & merged["vrp_pctile"].notna()]

                    if len(h_trades) < 5 or len(a_trades) < 5:
                        continue

                    h_avg = h_trades["orig_pnl_pts"].mean()
                    a_avg = a_trades["orig_pnl_pts"].mean()
                    sep = h_avg - a_avg

                    results.append({
                        "vrp_h": vrp_h, "vrp_a": vrp_a,
                        "vp_h": vp_h, "vp_d": vp_d,
                        "h_n": len(h_trades), "a_n": len(a_trades),
                        "h_avg": h_avg, "a_avg": a_avg, "sep": sep,
                    })

                    if sep > best_sep:
                        best_sep = sep
                        best_params = {
                            "vrp_h": vrp_h, "vrp_a": vrp_a,
                            "vp_h": vp_h, "vp_d": vp_d}

    if results:
        res_df = pd.DataFrame(results).sort_values("sep", ascending=False)
        print(f"\n  Top 10 threshold combinations by separation:")
        print(f"  {'VRP_H':>6} {'VRP_A':>6} {'VP_H':>6} {'VP_D':>6} "
              f"{'H_N':>5} {'A_N':>5} {'H_Avg':>9} {'A_Avg':>9} "
              f"{'Sep':>9}")
        print(f"  {'─' * 6} {'─' * 6} {'─' * 6} {'─' * 6} "
              f"{'─' * 5} {'─' * 5} {'─' * 9} {'─' * 9} "
              f"{'─' * 9}")
        for _, r in res_df.head(10).iterrows():
            print(f"  {r['vrp_h']:>6.2f} {r['vrp_a']:>6.2f} "
                  f"{r['vp_h']:>6.2f} {r['vp_d']:>6.2f} "
                  f"{r['h_n']:>5.0f} {r['a_n']:>5.0f} "
                  f"{r['h_avg']:>+9.4f} {r['a_avg']:>+9.4f} "
                  f"{r['sep']:>+9.4f}")

        print(f"\n  Best: VRP_H={best_params['vrp_h']:.2f}, "
              f"VRP_A={best_params['vrp_a']:.2f}, "
              f"VP_H={best_params['vp_h']:.2f}, "
              f"VP_D={best_params['vp_d']:.2f} → "
              f"separation={best_sep:+.4f}")

    return best_params


# ════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════

def main():
    print("\n" + "#" * 80)
    print("#  VARIANCE RISK PREMIUM REGIME CLASSIFIER")
    print("#  Bollerslev/Tauchen/Zhou × Bekaert-Hoerova × Dealer GEX")
    print("#" * 80)

    # ── Step 1: Build daily panel ──
    daily = build_daily_panel()

    # ── Step 2: VRP (Layer 1) ──
    print(f"\n{'=' * 80}")
    print("Layer 1: Variance Risk Premium")
    print("=" * 80)
    daily = compute_vrp(daily)
    print(f"  VRP computed for {daily['vrp'].notna().sum()} days")

    # ── Step 3: Bekaert-Hoerova (Layer 2) ──
    print(f"\n{'=' * 80}")
    print("Layer 2: Bekaert-Hoerova Decomposition")
    print("=" * 80)
    daily = compute_bh_decomposition(daily)
    print(f"  VP/CV decomposition for {daily['vp_share'].notna().sum()} days")

    # ── Step 4: Dealer GEX (Layer 3) ──
    print(f"\n{'=' * 80}")
    print("Layer 3: Dealer Gamma Exposure")
    print("=" * 80)
    daily = compute_gex_daily(daily)

    # ── Step 5: 0DTE VRP (Layer 4) ──
    print(f"\n{'=' * 80}")
    print("Layer 4: 0DTE / Short-Dated VRP")
    print("=" * 80)
    daily = compute_0dte_vrp(daily)
    print(f"  VRP_1d computed for {daily['vrp_1d'].notna().sum()} days")

    # ── Step 6: Calm-but-scared overlay ──
    print(f"\n{'=' * 80}")
    print("Overlay: Calm But Scared")
    print("=" * 80)
    daily = compute_calm_scared_overlay(daily)
    print(f"  Calm-but-scared CORE days: {daily['calm_scared_core'].sum()}")
    print(f"  Calm-but-scared STRICT days: {daily['calm_scared_strict'].sum()}")

    # ── Step 7: Composite signal ──
    print(f"\n{'=' * 80}")
    print("Composite Signal")
    print("=" * 80)
    daily = compute_composite_signal(daily)

    # ── Step 8: Reports ──
    report_daily_stats(daily)

    # ── Step 9: Trade integration ──
    merged = merge_with_trades(daily)
    if merged is not None:
        report_regime_performance(merged)

    # ── Step 10: 2026 deep dive ──
    deep_dive_2026(daily, merged)

    # ── Step 11: Threshold sweep ──
    if merged is not None:
        sweep_thresholds(daily, merged)

    # ── Executive Summary ──
    print(f"\n{'=' * 80}")
    print("EXECUTIVE SUMMARY")
    print("=" * 80)

    print("""
  Architecture:
    L1: VRP = VIX^2 - RV^2_20d    (Bollerslev/Tauchen/Zhou 2009)
    L2: VP/CV = VIX^2 decomposed   (Bekaert-Hoerova 2014)
        VP_share = (VIX^2 - HAR_CV) / VIX^2
    L3: GEX = sum(call_g*OI) - sum(put_g*OI) * 100 * S
        (Barbon & Buraschi 2021)
    L4: VRP_1d = VIX1D^2 - RV^2_1d (0DTE regime awareness)
    Overlay: calm-but-scared = low RV10 + normal VIX + high log(VIX/RV10)
        with tail premium and term structure as context, not proof of mispricing

  Signal: Binary HARVEST/AVOID/NEUTRAL (Nystrup et al.)
    - Primary value: knowing when NOT to trade
    - GEX modifies regime (compression vs amplification)
    - 2-day persistence filter
    """)

    if merged is not None:
        all_s = _stats(merged)
        kept = merged[merged["signal_confirmed"] != "AVOID"]
        k_s = _stats(kept)
        avoided = merged[merged["signal_confirmed"] == "AVOID"]
        a_s = _stats(avoided)

        print(f"  Impact of filtering AVOID-regime trades:")
        print(f"    All trades:   N={all_s['n']}, WR={all_s['wr']:.1f}%, "
              f"Avg={all_s['avg']:+.4f}, Total={all_s['total']:+.2f}")
        print(f"    After filter: N={k_s['n']}, WR={k_s['wr']:.1f}%, "
              f"Avg={k_s['avg']:+.4f}, Total={k_s['total']:+.2f}")
        print(f"    Avoided:      N={a_s['n']}, WR={a_s['wr']:.1f}%, "
              f"Avg={a_s['avg']:+.4f}, Total={a_s['total']:+.2f}")
        if a_s["n"] > 0:
            print(f"    Edge (kept − avoided): "
                  f"{k_s['avg'] - a_s['avg']:+.4f}")

    return daily, merged


if __name__ == "__main__":
    daily, merged = main()
