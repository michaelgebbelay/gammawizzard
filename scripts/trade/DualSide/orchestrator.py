#!/usr/bin/env python3
"""
DualSide Strategy — SPX vertical orchestrator (Schwab)

Put side: 6DTE 10-wide em0.75 direction switch
  If iv_minus_vix >= -0.0502 AND rr25 <= -0.9976: bull_put_credit
  Else: bear_put_debit

Call side: 5DTE 10-wide 50-delta bull_put_credit (always long direction)
  Uses puts instead of calls to avoid strike overlap with butterfly call legs.

Filters:
  1. VIX1D veto: skip ALL when 10.0 <= VIX1D < 11.5
  2. VIX < 10: skip call side
  3. RV5/RV20 0.70-0.85: skip bullish legs (bull_put_credit + bull_put_credit)
     Bear_put_debit always trades.

Delegates placement to scripts/trade/DualSide/place.py (same as ConstantStable placer).
"""

import math
import os
import sys
import re
import json
import subprocess
import time
import random
from datetime import date, datetime, timedelta
from typing import Any, Dict, Optional, Tuple

import numpy as np
import requests

# ── path setup ──
def _add_scripts_root():
    cur = os.path.abspath(os.path.dirname(__file__))
    while True:
        if os.path.basename(cur) == "scripts":
            if cur not in sys.path:
                sys.path.append(cur)
            return
        parent = os.path.dirname(cur)
        if parent == cur:
            return
        cur = parent

_add_scripts_root()
from schwab_token_keeper import schwab_client

__version__ = "1.0.0"

# ── config ──
CS_UNIT_DOLLARS = float(os.environ.get("DS_UNIT_DOLLARS", os.environ.get("CS_UNIT_DOLLARS", "10000")))
DS_LOG_PATH = os.environ.get("DS_LOG_PATH", os.environ.get("CS_LOG_PATH", "logs/dualside_trades.csv"))
DS_DRY_RUN = (os.environ.get("DS_DRY_RUN", os.environ.get("VERT_DRY_RUN", "false")) or "false").strip().lower() in ("1", "true", "yes")

# Strategy parameters
PUT_WIDTH = 10
CALL_WIDTH = 10
EM_FACTOR = 0.75

# Signal thresholds
IV_MINUS_VIX_THRESH = -0.0502
RR25_THRESH = -0.9976

# Filter thresholds
VIX1D_VETO_LO = 10.0
VIX1D_VETO_HI = 11.5
VIX_CALL_SKIP = 10.0
RV_BAND_LO = 0.70
RV_BAND_HI = 0.85

# Placement
PLACER_SCRIPT = os.path.join(os.path.dirname(__file__), "place.py")

# Guard
DS_GUARD_NO_CLOSE = (os.environ.get("CS_GUARD_NO_CLOSE", "1") or "1").strip().lower() in ("1", "true", "yes")
DS_TOPUP = (os.environ.get("CS_TOPUP", "1") or "1").strip().lower() in ("1", "true", "yes")


# ═══════════════════════════════════════════════════════════════
# Schwab API helpers
# ═══════════════════════════════════════════════════════════════

def _sleep_for_429(resp, attempt: int) -> float:
    ra = resp.headers.get("Retry-After")
    if ra:
        try:
            return max(1.0, float(ra))
        except Exception:
            pass
    return min(8.0, 0.6 * (2 ** attempt)) + random.uniform(0.0, 0.25)


def schwab_get_json(c, url: str, params=None, tries: int = 6, tag: str = ""):
    last = ""
    for i in range(tries):
        try:
            r = c.session.get(url, params=(params or {}), timeout=20)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                time.sleep(_sleep_for_429(r, i))
                continue
            last = f"HTTP_{r.status_code}:{(r.text or '')[:200]}"
        except Exception as e:
            last = f"{type(e).__name__}:{str(e)}"
        time.sleep(min(6.0, 0.5 * (2 ** i)))
    raise RuntimeError(f"SCHWAB_GET_FAIL({tag}) {last or 'unknown'}")


def get_quote(c, symbol: str) -> dict:
    """Fetch a single quote from Schwab."""
    r = c.get_quote(symbol)
    r.raise_for_status()
    data = r.json()
    # Response is {symbol: {quote: {...}, reference: {...}}}
    if symbol in data:
        return data[symbol].get("quote", data[symbol])
    return data


def get_spx_price_history(c, days: int = 30) -> list:
    """Fetch daily SPX closes for RV computation."""
    end = datetime.now()
    start = end - timedelta(days=days + 10)  # buffer for weekends
    r = c.get_price_history_every_day(
        "$SPX",
        start_datetime=start,
        end_datetime=end,
        need_extended_hours_data=False,
    )
    r.raise_for_status()
    data = r.json()
    candles = data.get("candles", [])
    return sorted(candles, key=lambda x: x["datetime"])


def get_option_chain(c, dte: int) -> dict:
    """Fetch SPX option chain for a specific DTE.

    Searches a window of -1 to +2 days around the target to handle weekends
    (e.g., 5DTE on Monday targets Saturday, but we want Friday).
    """
    from schwab.client import Client
    today = date.today()
    target_exp = today + timedelta(days=dte)

    r = c.get_option_chain(
        "$SPX",
        contract_type=Client.Options.ContractType.ALL,
        strike_range=Client.Options.StrikeRange.ALL,
        from_date=target_exp - timedelta(days=1),
        to_date=target_exp + timedelta(days=2),
        include_underlying_quote=True,
    )
    r.raise_for_status()
    return r.json()


# ═══════════════════════════════════════════════════════════════
# Chain parsing — extract what we need from Schwab chain format
# ═══════════════════════════════════════════════════════════════

def parse_chain(chain_json: dict, target_dte: int) -> Optional[dict]:
    """Parse Schwab option chain into our format.

    Returns dict with: spot, contracts (list of dicts with strike, type, bid, ask,
    implied_vol, delta), expiration.
    """
    underlying = chain_json.get("underlying", {}) or {}
    spot = underlying.get("last") or underlying.get("mark") or underlying.get("close")
    if not spot:
        # Try underlyingPrice at top level
        spot = chain_json.get("underlyingPrice")
    if not spot or spot <= 0:
        return None

    contracts = []

    # Schwab returns callExpDateMap and putExpDateMap
    # Each is {exp_date_str: {strike_str: [contract_dict]}}
    for side, opt_type in [("callExpDateMap", "C"), ("putExpDateMap", "P")]:
        exp_map = chain_json.get(side, {})
        # Pick the expiration closest to target_dte
        best_exp = None
        best_diff = 999
        for exp_key, strikes_map in exp_map.items():
            # exp_key format: "2026-03-16:6" (date:dte)
            parts = exp_key.split(":")
            if len(parts) >= 2:
                try:
                    dte_val = int(parts[1])
                except ValueError:
                    continue
                diff = abs(dte_val - target_dte)
                if diff < best_diff:
                    best_diff = diff
                    best_exp = (exp_key, strikes_map)

        if not best_exp or best_diff > 1:
            continue

        exp_key, strikes_map = best_exp
        expiration = exp_key.split(":")[0]

        for strike_str, contracts_list in strikes_map.items():
            if not contracts_list:
                continue
            c = contracts_list[0]  # first contract at this strike
            strike = c.get("strikePrice")
            if not strike:
                continue

            bid = c.get("bid", 0) or 0
            ask = c.get("ask", 0) or 0
            iv = c.get("volatility")  # Schwab returns as percentage (e.g., 16.5)
            delta = c.get("delta")
            osi = c.get("symbol", "")

            contracts.append({
                "strike": float(strike),
                "type": opt_type,
                "bid": float(bid),
                "ask": float(ask),
                "implied_vol": float(iv) / 100 if iv else None,  # convert to decimal
                "delta": float(delta) if delta is not None else None,
                "osi": osi,  # keep spaces — Schwab API requires padded OSI
                "expiration": expiration,
            })

    if not contracts:
        return None

    return {
        "spot": spot,
        "contracts": contracts,
        "expiration": expiration if contracts else None,
    }


# ═══════════════════════════════════════════════════════════════
# Signal computation (same as backtest)
# ═══════════════════════════════════════════════════════════════

def compute_signals(contracts, spot) -> Tuple[Optional[float], Optional[float]]:
    """Compute iv_minus_vix and rr25 from chain contracts.

    Returns (iv_minus_vix_vol_pts, rr25_vol_pts).
    iv_minus_vix uses ATM IV from the chain (not VIX — we subtract VIX separately).
    """
    calls = [c for c in contracts if c["type"] == "C" and c.get("implied_vol")]
    puts = [c for c in contracts if c["type"] == "P" and c.get("implied_vol")]
    if not calls or not puts:
        return None, None

    # ATM: closest to spot
    c_atm = min(calls, key=lambda c: abs(c["strike"] - spot))
    p_atm = min(puts, key=lambda c: abs(c["strike"] - spot))

    iv_atm_pct = ((c_atm["implied_vol"] * 100) + (p_atm["implied_vol"] * 100)) / 2

    # 25-delta: find calls with delta ~0.25 and puts with delta ~-0.25
    calls_with_delta = [c for c in calls if c.get("delta") is not None]
    puts_with_delta = [c for c in puts if c.get("delta") is not None]

    if not calls_with_delta or not puts_with_delta:
        return None, None

    c25 = min(calls_with_delta, key=lambda c: abs(abs(c["delta"]) - 0.25))
    p25 = min(puts_with_delta, key=lambda c: abs(abs(c["delta"]) - 0.25))

    rr25_vol_pts = (c25["implied_vol"] - p25["implied_vol"]) * 100

    return iv_atm_pct, rr25_vol_pts


def compute_em(contracts, spot) -> Optional[float]:
    """Compute expected move = ATM straddle mid."""
    calls = [c for c in contracts if c["type"] == "C"]
    puts = [c for c in contracts if c["type"] == "P"]
    if not calls or not puts:
        return None

    c_atm = min(calls, key=lambda c: abs(c["strike"] - spot))
    p_atm = min(puts, key=lambda c: abs(c["strike"] - spot))

    def mid(c):
        b = c.get("bid", 0) or 0
        a = c.get("ask", 0) or 0
        if b <= 0 and a <= 0:
            return None
        if b <= 0:
            return a / 2
        if a <= 0:
            return b / 2
        return (b + a) / 2

    cm = mid(c_atm)
    pm = mid(p_atm)
    if cm is None or pm is None:
        return None
    return cm + pm


def find_strike_near(contracts, opt_type, target_strike):
    """Find contract closest to target strike."""
    filtered = [c for c in contracts if c["type"] == opt_type]
    if not filtered:
        return None
    return min(filtered, key=lambda c: abs(c["strike"] - target_strike))


def find_by_delta(contracts, opt_type, target_delta):
    """Find contract closest to target delta."""
    filtered = [c for c in contracts if c["type"] == opt_type and c.get("delta") is not None]
    if not filtered:
        return None
    return min(filtered, key=lambda c: abs(abs(c["delta"]) - target_delta))


def compute_rv(candles, window: int) -> Optional[float]:
    """Compute annualized realized vol from daily candles."""
    closes = [c["close"] for c in candles if c.get("close")]
    if len(closes) < window + 1:
        return None
    recent = closes[-(window + 1):]
    log_rets = [math.log(recent[i] / recent[i-1]) for i in range(1, len(recent))]
    if len(log_rets) < window:
        return None
    return float(np.std(log_rets, ddof=1) * math.sqrt(252) * 100)


# ═══════════════════════════════════════════════════════════════
# OSI helpers (from ConstantStable)
# ═══════════════════════════════════════════════════════════════

def to_osi(sym: str) -> str:
    raw = (sym or "").strip().upper().lstrip(".").replace("_", "")
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


def osi_canon(osi: str) -> Tuple[str, str, str]:
    s = (osi or "")
    if len(s) < 21:
        return ("", "", "")
    return (s[6:12], s[12], s[-8:])


def build_osi(expiration: str, opt_type: str, strike: float) -> str:
    """Build OSI symbol from expiration date, type (C/P), and strike."""
    d = date.fromisoformat(expiration)
    ymd = d.strftime("%y%m%d")
    mills = int(round(strike * 1000))
    return f"{'SPXW':<6}{ymd}{opt_type}{mills:08d}"


# ═══════════════════════════════════════════════════════════════
# Position helpers (from ConstantStable)
# ═══════════════════════════════════════════════════════════════

def _osi_from_instrument(ins: Dict[str, Any]) -> Optional[str]:
    sym = (ins.get("symbol") or "").strip()
    if sym:
        try:
            sym_clean = re.sub(r"\s+", "", sym)
            return to_osi(sym_clean)
        except Exception:
            pass
    exp = ins.get("optionExpirationDate") or ins.get("expirationDate") or ""
    pc = (ins.get("putCall") or ins.get("type") or "").upper()
    strike = ins.get("strikePrice") or ins.get("strike")
    try:
        if exp and strike is not None and pc:
            ymd = date.fromisoformat(str(exp)[:10]).strftime("%y%m%d")
            cp = "C" if pc.startswith("C") else "P"
            mills = int(round(float(strike) * 1000))
            return f"{'SPXW':<6}{ymd}{cp}{mills:08d}"
    except Exception:
        return None
    return None


def positions_map(c, acct_hash: str) -> Dict[Tuple[str, str, str], float]:
    url = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}"
    j = schwab_get_json(c, url, params={"fields": "positions"}, tag="POSITIONS")
    sa = j[0]["securitiesAccount"] if isinstance(j, list) else (j.get("securitiesAccount") or j)
    out: Dict[Tuple[str, str, str], float] = {}
    for p in (sa.get("positions") or []):
        ins = p.get("instrument", {}) or {}
        atype = (ins.get("assetType") or ins.get("type") or "").upper()
        if atype != "OPTION":
            continue
        osi = _osi_from_instrument(ins)
        if not osi:
            continue
        try:
            qty = float(p.get("longQuantity", 0)) - float(p.get("shortQuantity", 0))
        except Exception:
            continue
        if abs(qty) < 1e-9:
            continue
        key = osi_canon(osi)
        out[key] = out.get(key, 0.0) + qty
    return out


def get_account_hash(c):
    r = c.get_account_numbers()
    r.raise_for_status()
    arr = r.json() or []
    if not arr:
        return ""
    return str(arr[0].get("hashValue") or arr[0].get("hashvalue") or "")


def opening_cash_for_account(c, prefer_number=None):
    r = c.get_accounts()
    r.raise_for_status()
    data = r.json()
    arr = data if isinstance(data, list) else [data]
    if prefer_number is None:
        try:
            rr = c.get_account_numbers()
            rr.raise_for_status()
            prefer_number = str((rr.json() or [{}])[0].get("accountNumber") or "")
        except Exception:
            prefer_number = None

    def hunt(a):
        acct_id = None; init = {}; curr = {}
        stack = [a]
        while stack:
            x = stack.pop()
            if isinstance(x, dict):
                if acct_id is None and x.get("accountNumber"):
                    acct_id = str(x["accountNumber"])
                if "initialBalances" in x: init = x["initialBalances"]
                if "currentBalances" in x: curr = x["currentBalances"]
                for v in x.values():
                    if isinstance(v, (dict, list)): stack.append(v)
            elif isinstance(x, list):
                stack.extend(x)
        return acct_id, init, curr

    chosen = None; acct_num = ""
    for a in arr:
        aid, init, curr = hunt(a)
        if prefer_number and aid == prefer_number:
            chosen = (init, curr); acct_num = aid; break
        if chosen is None:
            chosen = (init, curr); acct_num = aid
    if not chosen:
        return None, "none", ""
    init, curr = chosen
    keys = ["liquidationValue", "cashAvailableForTrading", "cashBalance"]
    for src in (init, curr):
        for k in keys:
            v = (src or {}).get(k)
            if isinstance(v, (int, float)) and float(v) > 0:
                return float(v), k, acct_num
    return None, "none", acct_num


def open_spreads_for_vertical(v, pos):
    buy_key = osi_canon(v["long_osi"])
    sell_key = osi_canon(v["short_osi"])
    buy_pos = float(pos.get(buy_key, 0.0))
    sell_pos = float(pos.get(sell_key, 0.0))
    b = buy_pos if buy_pos > 0 else 0.0
    s = (-sell_pos) if sell_pos < 0 else 0.0
    return int(min(b, s) + 1e-9)


def would_close_guard(v, pos):
    buy_key = osi_canon(v["long_osi"])
    sell_key = osi_canon(v["short_osi"])
    buy_leg_pos = float(pos.get(buy_key, 0.0))
    sell_leg_pos = float(pos.get(sell_key, 0.0))
    print(f"DS GUARD_CHECK {v['name']}: BUY {v['long_osi']} pos={buy_leg_pos:+g} ; SELL {v['short_osi']} pos={sell_leg_pos:+g}")
    return buy_leg_pos < -1e-9 or sell_leg_pos > 1e-9


# ═══════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════

def main():
    print(f"DS_RUN v{__version__} DRY_RUN={DS_DRY_RUN}")

    # ── Schwab client + account ──
    try:
        c = schwab_client()
        oc_val, oc_src, acct_num = opening_cash_for_account(c)
        acct_hash = get_account_hash(c)
    except Exception as e:
        print(f"DS_RUN SKIP: Schwab init failed: {e}")
        return 1

    print(f"DS_RUN EQUITY: {oc_val} (src={oc_src}, acct={acct_num})")

    # Fixed 1 contract to start
    qty = 1
    print(f"DS_RUN QTY: {qty}")

    # ── Fetch market data ──
    # VIX quote
    try:
        vix_quote = get_quote(c, "$VIX")
        vix = vix_quote.get("lastPrice") or vix_quote.get("mark") or vix_quote.get("closePrice")
        print(f"DS_RUN VIX: {vix}")
    except Exception as e:
        print(f"DS_RUN SKIP: VIX fetch failed: {e}")
        return 1

    # VIX1D quote
    vix1d = None
    try:
        vix1d_quote = get_quote(c, "$VIX1D")
        vix1d = vix1d_quote.get("lastPrice") or vix1d_quote.get("mark") or vix1d_quote.get("closePrice")
        print(f"DS_RUN VIX1D: {vix1d}")
    except Exception as e:
        print(f"DS_RUN WARN: VIX1D fetch failed ({e}), proceeding without veto")

    # ── Filter 1: VIX1D veto ──
    if vix1d is not None and VIX1D_VETO_LO <= vix1d < VIX1D_VETO_HI:
        print(f"DS_RUN SKIP: VIX1D veto ({vix1d:.2f} in [{VIX1D_VETO_LO}, {VIX1D_VETO_HI}))")
        return 0

    # ── RV5/RV20 for filter ──
    rv_ratio = None
    try:
        candles = get_spx_price_history(c, days=40)
        rv5 = compute_rv(candles, 5)
        rv20 = compute_rv(candles, 20)
        if rv5 is not None and rv20 is not None and rv20 > 0:
            rv_ratio = rv5 / rv20
        print(f"DS_RUN RV5={rv5:.2f} RV20={rv20:.2f} ratio={rv_ratio:.3f}" if rv5 and rv20 else f"DS_RUN RV: insufficient data")
    except Exception as e:
        print(f"DS_RUN WARN: RV computation failed ({e}), proceeding without RV filter")

    rv_in_band = rv_ratio is not None and RV_BAND_LO <= rv_ratio < RV_BAND_HI
    if rv_in_band:
        print(f"DS_RUN RV_BAND: {rv_ratio:.3f} in [{RV_BAND_LO}, {RV_BAND_HI}) — will skip bullish legs")

    # ── Fetch 6DTE chain (put side) ──
    print("\nDS_RUN FETCHING 6DTE chain...")
    try:
        chain6_raw = get_option_chain(c, 6)
        chain6 = parse_chain(chain6_raw, 6)
    except Exception as e:
        print(f"DS_RUN SKIP: 6DTE chain fetch failed: {e}")
        return 1

    if not chain6:
        print("DS_RUN SKIP: 6DTE chain empty or no underlying price")
        return 1

    spot = chain6["spot"]
    exp6 = chain6["expiration"]
    contracts6 = chain6["contracts"]
    print(f"DS_RUN 6DTE: spot={spot:.2f} exp={exp6} contracts={len(contracts6)}")

    # ── Compute signals from 6DTE chain ──
    iv_atm_pct, rr25_vol_pts = compute_signals(contracts6, spot)
    em = compute_em(contracts6, spot)

    if iv_atm_pct is None or rr25_vol_pts is None or em is None:
        print(f"DS_RUN SKIP: could not compute signals (iv_atm={iv_atm_pct}, rr25={rr25_vol_pts}, em={em})")
        return 1

    # iv_minus_vix in vol points
    iv_minus_vix = iv_atm_pct - vix
    em075 = em * EM_FACTOR
    target_put_strike = spot - em075

    is_bull = (iv_minus_vix >= IV_MINUS_VIX_THRESH and rr25_vol_pts <= RR25_THRESH)
    direction = "BULL_PUT_CREDIT" if is_bull else "BEAR_PUT_DEBIT"

    print(f"DS_RUN SIGNALS: iv_atm={iv_atm_pct:.4f} iv_minus_vix={iv_minus_vix:.4f} rr25={rr25_vol_pts:.4f}")
    print(f"DS_RUN EM: {em:.2f} em075={em075:.2f} target_put_strike={target_put_strike:.0f}")
    print(f"DS_RUN DIRECTION: {direction} (is_bull={is_bull})")

    # ── Build put vertical ──
    v_put = None
    put_skip_reason = None

    if is_bull and rv_in_band:
        put_skip_reason = "RV_BAND (bull_put_credit skipped)"
    else:
        put_c = find_strike_near(contracts6, "P", target_put_strike)
        if put_c:
            if is_bull:
                # Bull put credit: sell higher put, buy lower put
                short_strike = put_c["strike"]
                long_strike = short_strike - PUT_WIDTH
                short_osi = put_c.get("osi") or build_osi(exp6, "P", short_strike)
                # Find the long leg contract for its OSI
                long_c = find_strike_near(contracts6, "P", long_strike)
                long_osi = long_c.get("osi", build_osi(exp6, "P", long_strike)) if long_c else build_osi(exp6, "P", long_strike)
                v_put = {
                    "name": "PUT_CREDIT", "kind": "PUT", "side": "CREDIT", "direction": "SHORT",
                    "short_osi": short_osi, "long_osi": long_osi,
                    "short_strike": short_strike, "long_strike": long_strike,
                    "target_qty": qty, "strength": 1.0,
                }
            else:
                # Bear put debit: buy higher put, sell lower put
                long_strike = put_c["strike"]
                short_strike = long_strike - PUT_WIDTH
                long_osi = put_c.get("osi") or build_osi(exp6, "P", long_strike)
                short_c = find_strike_near(contracts6, "P", short_strike)
                short_osi = short_c.get("osi", build_osi(exp6, "P", short_strike)) if short_c else build_osi(exp6, "P", short_strike)
                v_put = {
                    "name": "PUT_DEBIT", "kind": "PUT", "side": "DEBIT", "direction": "LONG",
                    "short_osi": short_osi, "long_osi": long_osi,
                    "short_strike": short_strike, "long_strike": long_strike,
                    "target_qty": qty, "strength": 1.0,
                }
        else:
            put_skip_reason = "no put contract near target strike"

    if put_skip_reason:
        print(f"DS_RUN PUT SKIP: {put_skip_reason}")
    else:
        print(f"DS_RUN PUT: {v_put['name']} strikes={v_put['long_strike']}/{v_put['short_strike']} "
              f"short={v_put['short_osi']} long={v_put['long_osi']}")

    # ── Fetch 5DTE chain (call side) ──
    v_call = None
    call_skip_reason = None

    # Filter 2: VIX < 10 skips call
    if vix is not None and vix < VIX_CALL_SKIP:
        call_skip_reason = f"VIX<{VIX_CALL_SKIP} ({vix:.2f})"
    elif rv_in_band:
        call_skip_reason = "RV_BAND (bull_put_credit skipped)"
    else:
        print("\nDS_RUN FETCHING 5DTE chain...")
        try:
            chain5_raw = get_option_chain(c, 5)
            chain5 = parse_chain(chain5_raw, 5)
        except Exception as e:
            call_skip_reason = f"5DTE chain fetch failed: {e}"
            chain5 = None

        if chain5:
            spot5 = chain5["spot"]
            exp5 = chain5["expiration"]
            contracts5 = chain5["contracts"]
            print(f"DS_RUN 5DTE: spot={spot5:.2f} exp={exp5} contracts={len(contracts5)}")

            # 50-delta bull put credit (same strikes/payoff as old bull call debit,
            # but uses puts to avoid call-type overlap with butterfly legs)
            call_p = find_by_delta(contracts5, "P", 0.50)
            if call_p:
                short_strike = call_p["strike"]
                long_strike = short_strike - CALL_WIDTH
                short_osi = call_p.get("osi") or build_osi(exp5, "P", short_strike)
                long_c = find_strike_near(contracts5, "P", long_strike)
                long_osi = long_c.get("osi", build_osi(exp5, "P", long_strike)) if long_c else build_osi(exp5, "P", long_strike)
                v_call = {
                    "name": "BULL_PUT_CREDIT", "kind": "PUT", "side": "CREDIT", "direction": "LONG",
                    "short_osi": short_osi, "long_osi": long_osi,
                    "short_strike": short_strike, "long_strike": long_strike,
                    "target_qty": qty, "strength": 1.0,
                }
            else:
                call_skip_reason = "no 50-delta put found"
        elif not call_skip_reason:
            call_skip_reason = "5DTE chain empty"

    if call_skip_reason:
        print(f"DS_RUN CALL SKIP: {call_skip_reason}")
    else:
        print(f"DS_RUN CALL: {v_call['name']} strikes={v_call['long_strike']}/{v_call['short_strike']} "
              f"short={v_call['short_osi']} long={v_call['long_osi']}")

    # ── Nothing to trade? ──
    if not v_put and not v_call:
        print("DS_RUN SKIP: no verticals to trade after filters.")
        return 0

    # ── Position guard + topup ──
    pos = None
    if DS_GUARD_NO_CLOSE or DS_TOPUP:
        try:
            pos = positions_map(c, acct_hash)
            print(f"DS_RUN POSITIONS: loaded {len(pos)} legs")
        except Exception as e:
            print(f"DS_RUN POSITIONS SKIP: fetch failed ({e}) — skipping ALL trades.")
            return 0

    def finalize(v):
        if not v:
            return None
        target = int(v["target_qty"])
        open_qty = 0
        if DS_TOPUP and pos is not None:
            open_qty = open_spreads_for_vertical(v, pos)
        rem = max(0, target - open_qty) if DS_TOPUP else target
        if DS_TOPUP:
            print(f"DS_RUN TOPUP {v['name']}: target={target} open={open_qty} rem={rem}")
        if rem <= 0:
            print(f"DS_RUN SKIP {v['name']}: AT_OR_ABOVE_TARGET")
            return None
        if DS_GUARD_NO_CLOSE and pos is not None:
            if would_close_guard(v, pos):
                print(f"DS_RUN GUARD_SKIP {v['name']}: WOULD_CLOSE")
                return None
        v2 = dict(v)
        v2["send_qty"] = int(rem)
        return v2

    v_put_f = finalize(v_put)
    v_call_f = finalize(v_call)

    if not v_put_f and not v_call_f:
        print("DS_RUN SKIP: nothing to place after TOPUP/GUARD.")
        return 0

    # ── Place orders ──
    def env_for_vertical(v):
        e = dict(os.environ)
        e.update({
            "VERT_SIDE":         v["side"],
            "VERT_KIND":         v["kind"],
            "VERT_NAME":         v["name"],
            "VERT_DIRECTION":    v["direction"],
            "VERT_SHORT_OSI":    v["short_osi"],
            "VERT_LONG_OSI":     v["long_osi"],
            "VERT_QTY":          str(v["send_qty"]),
            "VERT_GO":           "",
            "VERT_STRENGTH":     "1.000",
            "VERT_TRADE_DATE":   date.today().isoformat(),
            "VERT_TDATE":        v.get("expiration", ""),
            "VERT_GW_PRICE":     "",
            "VERT_UNIT_DOLLARS": str(CS_UNIT_DOLLARS),
            "VERT_OC":           str(oc_val or 0),
            "VERT_UNITS":        "1",
            "VERT_VOL_FIELD":    "VIX",
            "VERT_VOL_USED":     "VIX",
            "VERT_VOL_VALUE":    str(vix or ""),
            "VERT_VOL_BUCKET":   "1",
            "VERT_VOL_MULT":     "1",
            "VERT_QTY_RULE":     "FIXED",
            "SCHWAB_APP_KEY":    os.environ["SCHWAB_APP_KEY"],
            "SCHWAB_APP_SECRET": os.environ["SCHWAB_APP_SECRET"],
            "SCHWAB_TOKEN_JSON": os.environ.get("SCHWAB_TOKEN_JSON", ""),
            "SCHWAB_ACCT_HASH":  acct_hash,
            "CS_LOG_PATH":       DS_LOG_PATH,
        })
        if DS_DRY_RUN:
            e["VERT_DRY_RUN"] = "true"
        return e

    for v in (v_put_f, v_call_f):
        if not v:
            continue
        print(
            f"\nDS_RUN PLACING {v['name']}: side={v['side']} kind={v['kind']} "
            f"short={v['short_osi']} long={v['long_osi']} qty={v['send_qty']}"
        )
        env = env_for_vertical(v)
        rc = subprocess.call([sys.executable, PLACER_SCRIPT], env=env)
        if rc != 0:
            print(f"DS_RUN {v['name']}: placer rc={rc}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
