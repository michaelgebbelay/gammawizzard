#!/usr/bin/env python3
"""Path-S canonical configuration contract — SINGLE SOURCE OF TRUTH.

Every parity-relevant parameter in the system lives here. `data_refresh.py`,
`replay.py`, and `verify_parity.py` all import or validate against this
dict. Any drift between live and backtest produces a different `config_hash`
and the placer must refuse to fire.

Background: from launch through 2026-05-04 the live `data_refresh.py` ran
at z_window=252 while `replay.py` (which produced the +621% verdict) used
z_window=60. The mismatch hid inside two unrelated Python constants. This
module exists so that bug class cannot recur — there is one source of truth,
one hash, and exit-non-zero on disagreement.

Update procedure:

  1. If a parameter changes, update the value in CANONICAL below.
  2. Re-run the canonical sweep (replay.py at the new value, 4y window,
     production single-position config).
  3. Update WALKFORWARD_VERDICT.md with the new sweep result.
  4. The new `config_hash` will not match prior summary.json files — that
     is the intended fail-loud behavior. Don't backfill old artifacts.

DO NOT add knobs here that aren't in the verdict-validated strategy.
This is doctrine, not a settings panel.
"""
from __future__ import annotations

import hashlib
import json


# ---------------------------------------------------------------------------
# THE CONTRACT
# ---------------------------------------------------------------------------

CANONICAL: dict = {
    # --- Strategy identity
    "strategy":                 "path_s_z3_single_bullish_skew_flip",
    "direction":                "bullish",
    "positions":                1,                    # 1 = production candidate, 2 = top-2 alternate

    # --- Universe
    "universe_top_n":           2000,
    "universe_type":            "CS",                 # common stock
    "universe_exclude_pharma":  True,
    "universe_require_optionable": True,
    "universe_require_major_exchange": True,

    # --- Skew measure (must exactly match iv_compute.py)
    "skew_measure":             "call_iv_5otm_minus_put_iv_5otm",
    "otm_pct":                  0.05,                 # call strike target = spot * 1.05; put = 0.95
    "otm_side_filter":          "calls_strike_ge_spot__puts_strike_le_spot",
    "strike_select_rule":       "nearest_absolute_diff",

    # --- Expiry rule (iv_compute._select_expiry)
    "expiry_target_dte":        30,                   # iv_compute.TARGET_DTE
    "expiry_min_dte":           7,                    # iv_compute.MIN_DTE
    "expiry_max_dte":           60,                   # iv_compute.MAX_DTE
    "expiry_grain":             "monthly",            # monthly cycle, weekly NOT used
    "expiry_select_rule":       "closest_to_target_within_min_max",

    # --- IV calc
    "iv_model":                 "black_scholes_brentq",
    "iv_risk_free":             0.045,                # iv_compute.RISK_FREE
    "iv_dividend_yield":        0.0,
    "iv_price_field":           "close",              # daily-aggs last trade
    "iv_min_volume":            1,                    # both legs must have >= 1 contract traded
    "iv_solver_xtol":           1e-4,
    "iv_solver_maxiter":        64,

    # --- Z-score (replay.load_skew_lookup defaults)
    "z_window":                 60,                   # trading days
    "rolling_min":              20,                   # min_periods
    "lookahead_shift":          1,                    # shift(1) before rolling
    "z_persistence_days":       1,                    # 1 = today only

    # --- Threshold + ranking
    "z_threshold":              3.0,
    "z_compare_op":             ">=",                 # not >, not == (rounding-safe)
    "rank_by":                  "z_desc",
    "rank_tiebreak":            "ticker_asc",         # deterministic on ties

    # --- Regime gate
    "regime_gate":              "SPY_gt_200d_prior_close",
    "regime_symbol":            "SPY",
    "regime_ma_window":         200,                  # simple MA, calendar trading days
    "regime_close_basis":       "split_adjusted_close",
    "regime_evaluation_basis":  "prior_close",        # using info available before next-open trade

    # --- Execution
    "exec_signal_basis":        "as_of_close",
    "exec_entry_timing":        "next_open",
    "exec_exit_timing":         "next_open_after_close_trigger",
    "exec_order_type_default":  "market_on_open",     # subject to override at placer level

    # --- Exits
    "exit_trail_pct":           0.20,                 # 20% trailing stop on close
    "exit_trail_basis":         "peak_close_since_entry",
    "exit_max_hold_days":       90,                   # CALENDAR days (matches replay.py)
    "exit_max_hold_basis":      "calendar_days",
    "exit_regime":              "exit_on_first_close_below_spy_200d",
    "exit_intraday_check":      False,                # exits evaluated on close, not intraday

    # --- Sizing
    "size_pct_sleeve":          1.00,                 # 100% in single mode (backtest assumption)
    "size_fractional_shares":   False,
    "size_use_margin":          False,

    # --- Costs (backtest planning assumption — live records actual)
    "cost_bps_planning":        15,                   # 15bps per leg in backtest comparison

    # --- Freshness
    "freshness_max_signal_age_trading_days": 1,       # refuse to trade on stale signal

    # --- Overlays — explicitly NONE in production
    "overlay_persistence":      None,                 # rejected
    "overlay_earnings_blackout":None,                 # rejected (all 6 variants)
    "overlay_trend_floor":      None,                 # rejected
    "overlay_displacement":     None,                 # rejected (full path-dependent replay)
    "overlay_high_z_exhaustion":None,                 # research-only
    "overlay_speculative_only": False,                # rejected as core

    # --- Provenance
    "verdict_run_id":           "GHA 25345947109",    # rolling-z window sweep, 2026-05-04
    "verdict_run_4y_return":    "+621.3%",
    "verdict_run_sharpe":       1.32,
    "verdict_run_mdd":          "-29.6%",
    "verdict_run_trades":       17,
    "verdict_doctrine_anchor":  "scripts/conviction/backtest/WALKFORWARD_VERDICT.md",
}


def canonical_hash() -> str:
    """Stable SHA-256 of the canonical contract.

    We hash the JSON-canonical-form (sorted keys, no whitespace) so
    semantically equivalent dicts produce the same digest. Any field
    change shifts the hash.
    """
    blob = json.dumps(CANONICAL, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(blob).hexdigest()


CANONICAL_HASH = canonical_hash()


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def signal_config_block() -> dict:
    """The block embedded in every signal_today.json. Mirror, plus hash."""
    return {**CANONICAL, "config_hash": CANONICAL_HASH}


def assert_runtime_match(*, z_window: int, rolling_min: int,
                         z_threshold: float, universe_top_n: int) -> None:
    """Fail-closed guard. Used by data_refresh.py at module load.
    Raises RuntimeError on any drift from the canonical contract."""
    bad = []
    if z_window != CANONICAL["z_window"]:
        bad.append(f"z_window={z_window} (canonical {CANONICAL['z_window']})")
    if rolling_min != CANONICAL["rolling_min"]:
        bad.append(f"rolling_min={rolling_min} (canonical {CANONICAL['rolling_min']})")
    if z_threshold != CANONICAL["z_threshold"]:
        bad.append(f"z_threshold={z_threshold} (canonical {CANONICAL['z_threshold']})")
    if universe_top_n != CANONICAL["universe_top_n"]:
        bad.append(f"universe_top_n={universe_top_n} (canonical {CANONICAL['universe_top_n']})")
    if bad:
        raise RuntimeError(
            "Path-S runtime config does not match canonical contract — "
            "refusing to compute signal. Drift: " + "; ".join(bad)
            + ". See scripts/trade/PathS/canonical_config.py."
        )


# ---------------------------------------------------------------------------
# Fail-closed checklist — what the placer MUST check before each order
# ---------------------------------------------------------------------------
# This is a documentation-level enumeration. Each entry maps to an exit
# code in verify_parity.py. If a check fails, the placer's allowed
# response is one and only one: REFUSE_TO_TRADE.

PLACER_PRECONDITIONS: list[dict] = [
    {"id":  1, "check": "signal_today.signal_config.config_hash == canonical_hash"},
    {"id":  2, "check": "signal_today.as_of within freshness_max_signal_age_trading_days"},
    {"id":  3, "check": "verify_parity.py exits 0 against today's signal"},
    {"id":  4, "check": "broker holdings == path_s_state.json holdings"},
    {"id":  5, "check": "no open / unfilled / partial orders against the sleeve"},
    {"id":  6, "check": "schwab_token not expired (run /token-refresh if so)"},
    {"id":  7, "check": "regime info available (SPY close + 200d both present)"},
    {"id":  8, "check": "candidate's 5OTM call_strike + put_strike present (not None)"},
    {"id":  9, "check": "candidate's z built from >= rolling_min observations"},
    {"id": 10, "check": "data pipeline ran cleanly (no [opt-ingest] ERROR lines for target date)"},
]


if __name__ == "__main__":
    # CLI smoke check
    print("CANONICAL config hash:", CANONICAL_HASH)
    print(f"  z_window:        {CANONICAL['z_window']}")
    print(f"  rolling_min:     {CANONICAL['rolling_min']}")
    print(f"  z_threshold:     {CANONICAL['z_threshold']}")
    print(f"  universe_top_n:  {CANONICAL['universe_top_n']}")
    print(f"  expiry rule:     target {CANONICAL['expiry_target_dte']}d, "
          f"window [{CANONICAL['expiry_min_dte']},{CANONICAL['expiry_max_dte']}]")
    print(f"  iv_model:        {CANONICAL['iv_model']} (r={CANONICAL['iv_risk_free']})")
    print(f"  regime gate:     {CANONICAL['regime_gate']}")
