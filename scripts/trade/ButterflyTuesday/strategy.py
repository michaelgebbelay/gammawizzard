#!/usr/bin/env python3
"""Trade selection for the hybrid butterfly strategy with BUY vol filter.

Signal (VIX1D at 4:01 PM ET):
  BUY  if 10 < VIX1D <= 20  →  long call butterfly  (4DTE, D20P wings)
  SELL if VIX1D > 20         →  short call butterfly (3DTE, D35P wings)
  SKIP if VIX1D <= 10

Vol filter (BUY only — skip BUY when EITHER fires):
  - Straddle Efficiency 5d avg > 0.9
  - Straddle Efficiency today > 1.0

Straddle efficiency = |SPX move today| / ATM straddle mid.
"""

from __future__ import annotations

import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional

import boto3


def _add_repo_root() -> None:
    cur = os.path.abspath(os.path.dirname(__file__))
    while True:
        parent = os.path.dirname(cur)
        if os.path.basename(cur) == "Gamma":
            if cur not in sys.path:
                sys.path.insert(0, cur)
            return
        if parent == cur:
            return
        cur = parent


_add_repo_root()

from scripts.schwab_token_keeper import schwab_client

from sim.data.chain_snapshot import ChainSnapshot, OptionContract, parse_schwab_chain
from sim.data.gw_client import fetch_gw_data


BUY_MIN_VIX1D = 10.0
BUY_MAX_VIX1D = 20.0
BUY_TARGET_DTE = 4
SELL_TARGET_DTE = 3

SE_5D_THRESHOLD = 0.9
SE_TODAY_THRESHOLD = 1.0

VOL_HISTORY_S3_KEY = "cadence/bf_straddle_eff_history.json"
VOL_HISTORY_MAX = 10


@dataclass(frozen=True)
class TradeConfig:
    name: str
    side: str
    put_delta_target: float


BUY_20 = TradeConfig(name="BUY_D20P", side="BUY", put_delta_target=0.20)
SELL_35 = TradeConfig(name="SELL_D35P", side="SELL", put_delta_target=0.35)


# ---------- Helpers ----------

def normalize_vix1d(value: object) -> Optional[float]:
    try:
        fv = float(value)
    except (TypeError, ValueError):
        return None
    if fv <= 1.5:
        return fv * 100.0
    return fv


def signal_from_vix1d(vix1d_points: Optional[float]) -> str:
    if vix1d_points is None:
        return "NO_SIGNAL"
    if BUY_MIN_VIX1D < vix1d_points <= BUY_MAX_VIX1D:
        return "BUY"
    if vix1d_points > BUY_MAX_VIX1D:
        return "SELL"
    return "SKIP"


def target_dte_for_signal(signal: str) -> int:
    if signal == "BUY":
        return int(os.environ.get("BF_BUY_TARGET_DTE", str(BUY_TARGET_DTE)))
    if signal == "SELL":
        return int(os.environ.get("BF_SELL_TARGET_DTE", str(SELL_TARGET_DTE)))
    return int(os.environ.get("BF_BUY_TARGET_DTE", str(BUY_TARGET_DTE)))


def to_osi(sym: str) -> str:
    raw = (sym or "").strip().upper().replace(" ", "").lstrip(".").replace("_", "")
    m = (
        re.match(r"^([A-Z.$^]{1,6})(\d{6})([CP])(\d{1,5})(?:\.(\d{1,3}))?$", raw)
        or re.match(r"^([A-Z.$^]{1,6})(\d{6})([CP])(\d{8})$", raw)
    )
    if not m:
        raise ValueError(f"Cannot parse option symbol: {sym}")
    root, ymd, cp, strike, frac = (m.groups() + ("",))[:5]
    if len(strike) < 8:
        mills = int(strike) * 1000 + (int((frac or "0").ljust(3, "0")) if frac else 0)
    else:
        mills = int(strike)
    return f"{root:<6}{ymd}{cp}{int(mills):08d}"


def add_business_days(d: date, n: int) -> date:
    """Add n business days (Mon-Fri) to a date."""
    result = d
    added = 0
    while added < n:
        result += timedelta(days=1)
        if result.weekday() < 5:
            added += 1
    return result


# ---------- Schwab chain ----------

def fetch_vix(c) -> float:
    try:
        resp = c.get_quote("$VIX")
        resp.raise_for_status()
        data = resp.json()
        for val in data.values():
            if isinstance(val, dict):
                q = val.get("quote", val)
                last = q.get("lastPrice") or q.get("last") or q.get("mark")
                if last is not None:
                    return float(last)
    except Exception:
        pass
    return 0.0


def fetch_chain_for_expiry(target_expiry: date, strike_count: int = 120) -> tuple[ChainSnapshot, date]:
    c = schwab_client()
    resp = c.get_option_chain(
        "$SPX",
        contract_type=c.Options.ContractType.ALL,
        strike_count=strike_count,
        include_underlying_quote=True,
        from_date=target_expiry,
        to_date=target_expiry + timedelta(days=1),
        option_type=c.Options.Type.ALL,
    )
    resp.raise_for_status()
    raw = resp.json()
    snapshot = parse_schwab_chain(raw, phase="close", vix=fetch_vix(c))
    if target_expiry not in snapshot.expirations:
        raise RuntimeError(f"Target expiry {target_expiry} not present in Schwab chain")
    return snapshot, target_expiry


def expiry_strikes(chain: ChainSnapshot, exp: date) -> list[float]:
    strikes = {c.strike for c in chain.calls(exp)}
    strikes.update(c.strike for c in chain.puts(exp))
    return sorted(strikes)


def atm_strike_for_exp(chain: ChainSnapshot, exp: date) -> float:
    strikes = expiry_strikes(chain, exp)
    if not strikes:
        raise RuntimeError(f"No strikes available for expiry {exp}")
    return min(strikes, key=lambda strike: (abs(strike - chain.underlying_price), strike))


def nearest_put_delta_contract(
    chain: ChainSnapshot,
    exp: date,
    *,
    target_abs: float,
    center: float,
) -> Optional[OptionContract]:
    strict = [
        contract
        for contract in chain.puts(exp)
        if contract.delta is not None and contract.delta < 0 and contract.strike < center
    ]
    fallback = [
        contract for contract in chain.puts(exp) if contract.delta is not None and contract.delta < 0
    ]
    candidates = strict or fallback
    if not candidates:
        return None
    return min(
        candidates,
        key=lambda contract: (
            abs(abs(contract.delta) - target_abs),
            abs(contract.strike - center),
        ),
    )


def butterfly_nbbo_from_chain(chain: ChainSnapshot, exp: date, lower: float, center: float, upper: float) -> tuple[float, float, float]:
    lower_call = chain.get_contract(lower, "C", exp)
    center_call = chain.get_contract(center, "C", exp)
    upper_call = chain.get_contract(upper, "C", exp)
    if lower_call is None or center_call is None or upper_call is None:
        raise RuntimeError("Missing butterfly call legs in chain")

    bid = lower_call.bid + upper_call.bid - 2.0 * center_call.ask
    ask = lower_call.ask + upper_call.ask - 2.0 * center_call.bid
    bid = round(max(0.0, bid), 2)
    ask = round(max(bid, ask), 2)
    mid = round((bid + ask) / 2.0, 2)
    return bid, ask, mid


# ---------- Vol filter (straddle efficiency) ----------

def _s3_bucket() -> str:
    return (
        os.environ.get("BF_VOL_STATE_S3_BUCKET")
        or os.environ.get("SIM_CACHE_BUCKET", "")
    ).strip()


def _load_vol_history() -> list[dict]:
    bucket = _s3_bucket()
    if not bucket:
        return []
    try:
        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket=bucket, Key=VOL_HISTORY_S3_KEY)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        return data.get("history", [])
    except Exception:
        return []


def _save_vol_history(history: list[dict]) -> None:
    bucket = _s3_bucket()
    if not bucket:
        return
    try:
        s3 = boto3.client("s3")
        s3.put_object(
            Bucket=bucket,
            Key=VOL_HISTORY_S3_KEY,
            Body=json.dumps({"history": history[-VOL_HISTORY_MAX:]}, indent=2),
            ContentType="application/json",
        )
    except Exception as e:
        print(f"BF_VOL_FILTER WARN: cannot save history: {e}")


def compute_straddle_efficiency(chain: ChainSnapshot, exp: date) -> Optional[float]:
    """Compute today's straddle efficiency = |actual move| / ATM straddle mid."""
    spot = chain.underlying_price
    prev_close = chain.spx_prev_close

    if prev_close <= 0 or spot <= 0:
        return None

    actual_move = abs(spot - prev_close)
    center = atm_strike_for_exp(chain, exp)
    atm_call = chain.get_contract(center, "C", exp)
    atm_put = chain.get_contract(center, "P", exp)

    if atm_call is None or atm_put is None:
        return None

    straddle_mid = atm_call.mid + atm_put.mid
    if straddle_mid <= 0:
        return None

    return actual_move / straddle_mid


def vol_filter_decision(
    chain: ChainSnapshot,
    exp: date,
    trade_date: str,
) -> tuple[bool, float | None, float | None, str]:
    """Returns (should_skip_buy, se_today, se_5d_avg, reason).

    should_skip_buy is True if EITHER condition fires:
      - SE 5d avg > 0.9
      - SE today > 1.0
    """
    se_today = compute_straddle_efficiency(chain, exp)

    # Load history and update
    history = _load_vol_history()

    # Don't double-count today
    history = [h for h in history if h.get("date") != trade_date]

    if se_today is not None:
        history.append({"date": trade_date, "se": round(se_today, 4)})
        _save_vol_history(history)

    # Compute 5d trailing avg (last 5 entries, excluding today for avg)
    prior = [h["se"] for h in history if h.get("date") != trade_date and h.get("se") is not None]
    recent = prior[-4:]  # last 4 prior days
    if se_today is not None:
        avg_window = recent + [se_today]
    else:
        avg_window = prior[-5:]

    se_5d_avg = sum(avg_window) / len(avg_window) if avg_window else None

    # Filter logic
    skip = False
    reasons = []

    if se_5d_avg is not None and se_5d_avg > SE_5D_THRESHOLD:
        skip = True
        reasons.append(f"SE5d={se_5d_avg:.3f}>{SE_5D_THRESHOLD}")

    if se_today is not None and se_today > SE_TODAY_THRESHOLD:
        skip = True
        reasons.append(f"SE_today={se_today:.3f}>{SE_TODAY_THRESHOLD}")

    reason = "; ".join(reasons) if reasons else "PASS"
    return skip, se_today, se_5d_avg, reason


# ---------- Main trade plan ----------

def build_trade_plan(trade_day: date) -> dict:
    gw = fetch_gw_data(trade_day.isoformat())
    if not gw:
        return {"status": "ERROR", "reason": "GW_DATA_UNAVAILABLE"}
    if gw.get("date") != trade_day.isoformat():
        return {
            "status": "ERROR",
            "reason": f"GW_DATE_MISMATCH:{gw.get('date') or 'NONE'}",
        }

    vix1d_points = normalize_vix1d(gw.get("vix_1d"))
    signal = signal_from_vix1d(vix1d_points)
    if signal == "NO_SIGNAL":
        return {"status": "ERROR", "reason": "VIX1D_UNAVAILABLE"}
    if signal == "SKIP":
        return {
            "status": "SKIP",
            "reason": "VIX1D_LE_10",
            "signal": signal,
            "vix1d": vix1d_points,
        }

    config = BUY_20 if signal == "BUY" else SELL_35
    target_dte = target_dte_for_signal(signal)
    target_expiry = add_business_days(trade_day, target_dte)
    strike_count = int(os.environ.get("BF_STRIKE_COUNT", "120"))
    max_leg_spread = float(os.environ.get("BF_MAX_LEG_SPREAD", "30"))

    try:
        chain, exp = fetch_chain_for_expiry(target_expiry, strike_count=strike_count)
    except Exception as exc:
        return {"status": "ERROR", "reason": f"CHAIN_FETCH_FAIL:{type(exc).__name__}:{exc}"}

    # Vol filter — only applies to BUY signal
    if signal == "BUY":
        should_skip, se_today, se_5d, vol_reason = vol_filter_decision(
            chain, exp, trade_day.isoformat()
        )
        if should_skip:
            return {
                "status": "SKIP",
                "reason": f"VOL_FILTER:{vol_reason}",
                "signal": signal,
                "vix1d": vix1d_points,
                "se_today": se_today,
                "se_5d_avg": se_5d,
            }
    else:
        # Still compute and save SE for history (even on SELL days)
        se_today = compute_straddle_efficiency(chain, exp)
        history = _load_vol_history()
        history = [h for h in history if h.get("date") != trade_day.isoformat()]
        if se_today is not None:
            history.append({"date": trade_day.isoformat(), "se": round(se_today, 4)})
            _save_vol_history(history)
        se_5d = None

    try:
        center = atm_strike_for_exp(chain, exp)
    except Exception as exc:
        return {"status": "ERROR", "reason": f"ATM_FAIL:{type(exc).__name__}:{exc}"}

    lower_put = nearest_put_delta_contract(
        chain,
        exp,
        target_abs=config.put_delta_target,
        center=center,
    )
    if lower_put is None or lower_put.strike >= center:
        return {"status": "ERROR", "reason": "LOWER_PUT_NOT_FOUND"}

    lower = lower_put.strike
    width = center - lower
    if width <= 0:
        return {"status": "ERROR", "reason": "NON_POSITIVE_WIDTH"}

    upper = center + width
    lower_call = chain.get_contract(lower, "C", exp)
    center_call = chain.get_contract(center, "C", exp)
    upper_call = chain.get_contract(upper, "C", exp)
    if lower_call is None or center_call is None or upper_call is None:
        return {"status": "ERROR", "reason": "CALL_LEG_MISSING"}

    for leg_name, contract in (
        ("lower", lower_call),
        ("center", center_call),
        ("upper", upper_call),
    ):
        if contract.ask - contract.bid > max_leg_spread:
            return {
                "status": "ERROR",
                "reason": f"LEG_SPREAD_TOO_WIDE:{leg_name}:{contract.ask - contract.bid:.2f}",
            }

    try:
        bid, ask, mid = butterfly_nbbo_from_chain(chain, exp, lower, center, upper)
    except Exception as exc:
        return {"status": "ERROR", "reason": f"NBBO_FAIL:{type(exc).__name__}:{exc}"}

    order_side = "DEBIT" if signal == "BUY" else "CREDIT"

    return {
        "status": "OK",
        "signal": signal,
        "config": config.name,
        "trade_date": trade_day.isoformat(),
        "expiry_date": exp.isoformat(),
        "vix": round(chain.vix, 2),
        "vix1d": round(vix1d_points or 0.0, 2),
        "spot": round(chain.underlying_price, 2),
        "target_dte": target_dte,
        "target_put_delta": config.put_delta_target,
        "actual_put_delta": round(abs(lower_put.delta), 4),
        "center_strike": center,
        "lower_strike": lower,
        "upper_strike": upper,
        "width": round(width, 2),
        "order_side": order_side,
        "package_bid": bid,
        "package_ask": ask,
        "package_mid": mid,
        "lower_osi": to_osi(lower_call.symbol),
        "center_osi": to_osi(center_call.symbol),
        "upper_osi": to_osi(upper_call.symbol),
        "se_today": round(se_today, 4) if se_today is not None else None,
        "se_5d_avg": round(se_5d, 4) if se_5d is not None else None,
    }
