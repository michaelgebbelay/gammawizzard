#!/usr/bin/env python3
"""Trade selection for the hybrid butterfly strategy with regime + vol filters.

Signal (VIX1D at 4:01 PM ET):
  BUY  if 10 < VIX1D <= 20  →  long call butterfly  (4DTE, D20P wings)
  SELL if VIX1D > 20         →  short call butterfly (3DTE, D35P wings)
  SKIP if VIX1D <= 10

Regime filter — BUY (skip unless BOTH_DOWN):
  - Require dVIX < 0 AND dVIX1D < 0 (both strictly negative vs prior day)
  - Skip if no prior-day vol reading available

Vol filter — BUY (skip BUY when EITHER fires):
  - Straddle Efficiency 5d avg > 0.9
  - RV5 > 12

Vol filter — SELL (skip when straddle efficiency is too high OR market is choppy):
  - Straddle Efficiency 5d avg > 1.00
  - ER3 < 0.60 (price efficiency ratio over 3 days — low = chop, high = trend)
  - ER3 = |Close[t] - Close[t-3]| / sum(|daily moves|)
  - Sell butterfly needs directional displacement through expiry, not chop

Straddle efficiency = |SPX move today| / ATM straddle mid.
"""

from __future__ import annotations

import json
import math
import os
import re
import statistics
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional

import boto3


def _add_repo_root() -> None:
    cur = os.path.abspath(os.path.dirname(__file__))
    while True:
        parent = os.path.dirname(cur)
        if os.path.basename(cur) == "Gamma" or os.path.isdir(os.path.join(cur, "scripts")):
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

from scripts.trade.ButterflyTuesday.pdv_filter import pdv_filter_decision


BUY_MIN_VIX1D = 10.0
BUY_MAX_VIX1D = 20.0
BUY_TARGET_DTE = 4
SELL_TARGET_DTE = 3

SE_5D_THRESHOLD = 0.9
RV5_THRESHOLD = 12.0
SELL_SE_5D_THRESHOLD = 1.00
SELL_ER3_THRESHOLD = 0.60

VOL_HISTORY_S3_KEY = "cadence/bf_straddle_eff_history.json"
VOL_HISTORY_MAX = 10
VOL_REGIME_S3_KEY = "cadence/bf_vol_regime_history.json"
VOL_REGIME_MAX = 5


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


def _fetch_spx_closes(c, trade_date: date) -> list[float]:
    """Fetch recent SPX daily closes from Schwab, ending at trade_date.

    Shared by fetch_rv5 and fetch_er3 to avoid duplicate API calls.
    """
    end = datetime.combine(trade_date, datetime.max.time())
    start = end - timedelta(days=20)  # buffer for weekends
    r = c.get_price_history_every_day(
        "$SPX",
        start_datetime=start,
        end_datetime=end,
        need_extended_hours_data=False,
    )
    r.raise_for_status()
    candles = sorted(r.json().get("candles", []), key=lambda x: x["datetime"])
    return [candle["close"] for candle in candles if candle.get("close")]


def compute_rv5(closes: list[float]) -> Optional[float]:
    """Compute 5-day annualized realized vol from daily closes."""
    if len(closes) < 6:  # need 5+1 for 5 returns
        return None
    recent = closes[-6:]
    log_rets = [math.log(recent[i] / recent[i - 1]) for i in range(1, len(recent))]
    if len(log_rets) < 5:
        return None
    return float(statistics.stdev(log_rets) * math.sqrt(252) * 100)


def compute_er3(closes: list[float]) -> Optional[float]:
    """Compute 3-day price efficiency ratio from daily closes.

    ER3 = |Close[t] - Close[t-3]| / (|d1| + |d2| + |d3|)
    where d_i are consecutive daily close-to-close moves.

    Low ER3 = choppy/mean-reverting, high ER3 = directional trend.
    """
    if len(closes) < 4:  # need 4 closes for 3 daily moves
        return None
    recent = closes[-4:]
    d1 = recent[1] - recent[0]
    d2 = recent[2] - recent[1]
    d3 = recent[3] - recent[2]
    total_path = abs(d1) + abs(d2) + abs(d3)
    if total_path == 0:
        return 1.0
    net_move = abs(recent[3] - recent[0])
    return float(net_move / total_path)


def fetch_rv5_and_er3(c, trade_date: date) -> tuple[Optional[float], Optional[float]]:
    """Fetch SPX closes once, compute both RV5 and ER3.

    Uses trade_date (respects BF_TRADE_DATE_OVERRIDE) instead of datetime.now().
    """
    try:
        closes = _fetch_spx_closes(c, trade_date)
        return compute_rv5(closes), compute_er3(closes)
    except Exception as e:
        print(f"BF_VOL WARN: SPX price history fetch failed: {e}")
        return None, None


def sell_vol_filter_decision(
    se_5d_avg: Optional[float],
    er3: Optional[float] = None,
) -> tuple[bool, str]:
    """Check if SELL should be skipped due to high SE or low ER3 (choppy market).

    Returns (should_skip, reason).
    Skip when SE 5d avg > 1.00 OR ER3 < 0.60.
    """
    se_threshold = float(os.environ.get(
        "BF_SELL_SE_THRESHOLD",
        str(SELL_SE_5D_THRESHOLD),
    ))
    er3_threshold = float(os.environ.get(
        "BF_SELL_ER3_THRESHOLD",
        str(SELL_ER3_THRESHOLD),
    ))

    # Strict: both SE5d and ER3 must be present — skip if either is None
    if se_5d_avg is None:
        return True, "SE5D=None — data missing, sell skipped"
    if er3 is None:
        return True, "ER3=None — data missing, sell skipped"

    if se_5d_avg > se_threshold:
        return True, f"SE5D={se_5d_avg:.3f}>{se_threshold} — sell skipped"

    if er3 < er3_threshold:
        return True, (
            f"ER3={er3:.3f}<{er3_threshold} — choppy market, sell skipped"
        )

    return False, f"PASS (SE5D={se_5d_avg:.3f}, ER3={er3:.3f})"


# ---------- Regime filter (BOTH_DOWN for buys) ----------

def _load_vol_regime() -> list[dict]:
    """Load prior-day VIX/VIX1D history from S3."""
    bucket = _s3_bucket()
    if not bucket:
        return []
    try:
        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket=bucket, Key=VOL_REGIME_S3_KEY)
        data = json.loads(obj["Body"].read().decode("utf-8"))
        return data.get("history", [])
    except Exception:
        return []


def _save_vol_regime(history: list[dict]) -> None:
    """Persist VIX/VIX1D history to S3 for regime filter."""
    bucket = _s3_bucket()
    if not bucket:
        return
    try:
        s3 = boto3.client("s3")
        s3.put_object(
            Bucket=bucket,
            Key=VOL_REGIME_S3_KEY,
            Body=json.dumps({"history": history[-VOL_REGIME_MAX:]}, indent=2),
            ContentType="application/json",
        )
    except Exception as e:
        print(f"BF_REGIME WARN: cannot save regime history: {e}")


def regime_filter_decision(
    vix: float,
    vix1d: float,
    trade_date: str,
) -> tuple[bool, str]:
    """Check if BUY should be skipped because VIX/VIX1D are not BOTH_DOWN.

    Returns (should_skip, reason).
    Requires dVIX < 0 AND dVIX1D < 0 (strictly negative).
    Always saves today's values for tomorrow's comparison.
    """
    history = _load_vol_regime()

    # Find prior-day entry (most recent entry before today)
    prior = [h for h in history if h.get("date", "") < trade_date]
    prior.sort(key=lambda h: h["date"])

    # Save today's values (deduplicate)
    history = [h for h in history if h.get("date") != trade_date]
    history.append({
        "date": trade_date,
        "vix": round(vix, 2),
        "vix1d": round(vix1d, 2),
    })
    _save_vol_regime(history)

    if not prior:
        return True, "NO_PRIOR_DAY_VOL — skipping buy"

    prev = prior[-1]
    prev_vix = prev.get("vix")
    prev_vix1d = prev.get("vix1d")
    if prev_vix is None or prev_vix1d is None:
        return True, f"PRIOR_DAY_INCOMPLETE (date={prev.get('date')}) — skipping buy"

    d_vix = vix - prev_vix
    d_vix1d = vix1d - prev_vix1d

    if d_vix < 0 and d_vix1d < 0:
        return False, (
            f"BOTH_DOWN — PASS (dVIX={d_vix:+.2f}, dVIX1D={d_vix1d:+.2f}, "
            f"prior={prev.get('date')})"
        )

    parts = []
    if d_vix >= 0:
        parts.append(f"dVIX={d_vix:+.2f}>=0")
    if d_vix1d >= 0:
        parts.append(f"dVIX1D={d_vix1d:+.2f}>=0")
    return True, f"NOT_BOTH_DOWN: {', '.join(parts)} (prior={prev.get('date')}) — skipping buy"


def _check_position_conflicts(
    expiry: date, lower: float, center: float, upper: float
) -> Optional[str]:
    """Check if any butterfly strike already has an open Schwab position.

    Builds the 3 target OSI keys and checks only those against the account
    positions (single API call). Skips positions on other expiries entirely.
    Returns a conflict description if overlap found, None if clear.
    """
    try:
        c = schwab_client()
        resp = c.get_account_numbers()
        resp.raise_for_status()
        arr = resp.json() or []
        if not arr:
            return None
        acct_hash = str(arr[0].get("hashValue") or arr[0].get("hashvalue") or "")
        if not acct_hash:
            return None

        # Build the 3 OSI keys we care about (calls on this expiry)
        exp_ymd = expiry.strftime("%y%m%d")
        target_osis = set()
        for strike in (lower, center, upper):
            mills = int(round(strike * 1000))
            target_osis.add(f"SPXW  {exp_ymd}C{mills:08d}")
            target_osis.add(f"SPXW  {exp_ymd}P{mills:08d}")

        url = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}"
        resp = c.session.get(url, params={"fields": "positions"}, timeout=20)
        if resp.status_code != 200:
            return None
        data = resp.json()
        sa = data[0]["securitiesAccount"] if isinstance(data, list) else (data.get("securitiesAccount") or data)

        for pos in sa.get("positions") or []:
            ins = pos.get("instrument", {}) or {}
            if (ins.get("assetType") or "").upper() != "OPTION":
                continue
            sym = (ins.get("symbol") or "").upper()
            if sym not in target_osis:
                continue
            qty = float(pos.get("longQuantity", 0)) - float(pos.get("shortQuantity", 0))
            if abs(qty) > 0:
                return f"{sym.strip()}:qty={qty:.0f}"
    except Exception as e:
        print(f"BF_GUARD WARN: position check failed: {e}")
    return None


def fetch_spx_prev_close(c) -> float:
    """Fetch SPX previous close from Schwab quote (closePrice)."""
    try:
        resp = c.get_quote("$SPX")
        resp.raise_for_status()
        data = resp.json()
        for val in data.values():
            if isinstance(val, dict):
                q = val.get("quote", val)
                close = q.get("closePrice") or q.get("previousClose")
                if close is not None:
                    return float(close)
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
    snapshot.spx_prev_close = fetch_spx_prev_close(c)
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
    rv5: Optional[float] = None,
) -> tuple[bool, float | None, float | None, str]:
    """Returns (should_skip_buy, se_today, se_5d_avg, reason).

    should_skip_buy is True if EITHER condition fires:
      - SE 5d avg > 0.9
      - RV5 > 12
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

    # Strict: both SE5d and RV5 must be present — skip if either is None
    if se_5d_avg is None:
        return True, se_today, se_5d_avg, "SE5D=None — data missing, buy skipped"
    if rv5 is None:
        return True, se_today, se_5d_avg, "RV5=None — data missing, buy skipped"

    # Filter logic
    skip = False
    reasons = []

    if se_5d_avg > SE_5D_THRESHOLD:
        skip = True
        reasons.append(f"SE5d={se_5d_avg:.3f}>{SE_5D_THRESHOLD}")

    if rv5 > RV5_THRESHOLD:
        skip = True
        reasons.append(f"RV5={rv5:.1f}>{RV5_THRESHOLD}")

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
    gw_vix = gw.get("vix")  # VIX from GW for regime filter
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

    # Use VIX from chain (live Schwab quote) for regime filter;
    # fall back to GW VIX if chain VIX is missing
    vix_for_regime = chain.vix if chain.vix else (float(gw_vix) if gw_vix else None)

    # Always save today's VIX/VIX1D to regime history (needed for tomorrow)
    if vix_for_regime is not None and vix1d_points is not None:
        regime_skip, regime_reason = regime_filter_decision(
            vix_for_regime, vix1d_points, trade_day.isoformat()
        )
    else:
        regime_skip, regime_reason = True, "VIX_OR_VIX1D_UNAVAILABLE"

    # Regime filter — BUY: require BOTH_DOWN (dVIX < 0 AND dVIX1D < 0)
    if signal == "BUY" and regime_skip:
        # Still compute and save SE history even when skipping
        se_today = compute_straddle_efficiency(chain, exp)
        history = _load_vol_history()
        history = [h for h in history if h.get("date") != trade_day.isoformat()]
        if se_today is not None:
            history.append({"date": trade_day.isoformat(), "se": round(se_today, 4)})
            _save_vol_history(history)
        print(f"BF_REGIME: {regime_reason}")
        return {
            "status": "SKIP",
            "reason": f"REGIME_FILTER:{regime_reason}",
            "signal": signal,
            "vix1d": vix1d_points,
            "vix": round(vix_for_regime, 2) if vix_for_regime else None,
            "se_today": round(se_today, 4) if se_today is not None else None,
        }

    # Fetch RV5 + ER3 from a single Schwab price history call
    c_schwab = schwab_client()
    rv5, er3 = fetch_rv5_and_er3(c_schwab, trade_day)

    # Vol filter — BUY: straddle efficiency + RV5
    if signal == "BUY":
        should_skip, se_today, se_5d, vol_reason = vol_filter_decision(
            chain, exp, trade_day.isoformat(), rv5=rv5
        )
        if should_skip:
            return {
                "status": "SKIP",
                "reason": f"VOL_FILTER:{vol_reason}",
                "signal": signal,
                "vix1d": vix1d_points,
                "se_today": se_today,
                "se_5d_avg": se_5d,
                "rv5": round(rv5, 2) if rv5 is not None else None,
            }
    else:
        # SELL path: compute and save SE for history
        se_today = compute_straddle_efficiency(chain, exp)
        history = _load_vol_history()
        history = [h for h in history if h.get("date") != trade_day.isoformat()]
        if se_today is not None:
            history.append({"date": trade_day.isoformat(), "se": round(se_today, 4)})
            _save_vol_history(history)

        # Compute SE 5d avg for sell filter
        prior_se = [h["se"] for h in history if h.get("date") != trade_day.isoformat() and h.get("se") is not None]
        recent_se = prior_se[-4:]
        if se_today is not None:
            avg_window = recent_se + [se_today]
        else:
            avg_window = prior_se[-5:]
        se_5d = sum(avg_window) / len(avg_window) if avg_window else None

    # Vol filter — SELL: skip when SE too high OR market is choppy (ER3 low)
    sell_filter_reason = ""
    if signal == "SELL":
        sell_skip, sell_filter_reason = sell_vol_filter_decision(se_5d, er3=er3)
        print(f"BF_SELL_FILTER: {sell_filter_reason}")
        if sell_skip:
            return {
                "status": "SKIP",
                "reason": f"SELL_VOL_FILTER:{sell_filter_reason}",
                "signal": signal,
                "vix1d": vix1d_points,
                "vix": round(chain.vix, 2),
                "se_today": round(se_today, 4) if se_today is not None else None,
                "se_5d_avg": round(se_5d, 4) if se_5d is not None else None,
                "er3": round(er3, 3) if er3 is not None else None,
            }

    # PDV filter — only applies to BUY signal
    pdv_skip = False
    pdv_info = {}
    if signal == "BUY":
        # Find 20-delta put from chain for PDV comparison
        _put_20d = nearest_put_delta_contract(
            chain, exp, target_abs=0.20, center=chain.underlying_price
        )
        pdv_skip, pdv_info = pdv_filter_decision(
            spot=chain.underlying_price,
            put_20d_strike=_put_20d.strike if _put_20d else None,
            dte=target_dte,
        )
        print(f"BF_PDV: {pdv_info}")
        if pdv_skip:
            return {
                "status": "SKIP",
                "reason": f"PDV_FILTER:{pdv_info.get('pdv_reason', '')}",
                "signal": signal,
                "vix1d": vix1d_points,
                "se_today": se_today if signal == "BUY" else None,
                "se_5d_avg": se_5d if signal == "BUY" else None,
                **{k: v for k, v in pdv_info.items() if k.startswith("pdv_")},
            }

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

    # Position guard: skip if any butterfly strike overlaps an existing position
    # on the same expiry (prevents accidental close of DualSide/CS legs)
    conflict = _check_position_conflicts(exp, lower, center, upper)
    if conflict:
        return {
            "status": "SKIP",
            "reason": f"POSITION_CONFLICT:{conflict}",
            "signal": signal,
            "vix1d": vix1d_points,
        }

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
        "pdv_sigma_ann": pdv_info.get("pdv_sigma_ann"),
        "pdv_put_div": pdv_info.get("put_div"),
        "rv5": round(rv5, 2) if rv5 is not None else None,
        "sell_filter": sell_filter_reason if signal == "SELL" else None,
        "er3": round(er3, 3) if er3 is not None else None,
        "regime_filter": regime_reason,
    }
