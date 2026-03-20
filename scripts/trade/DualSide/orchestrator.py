#!/usr/bin/env python3
"""
DualSide Strategy — SPX vertical orchestrator (Schwab)

v1.1.0 — 2026-03-19 regime-follow + 5-wide

Changes from v1.0.0:
  - Width reduced from $10 to $5 on both sides (halves max loss per trade)
  - 50-delta (call side) now follows the regime switch instead of always bullish
    When bearish: bear_put_debit at 50-delta (buy higher put, sell 5 lower)
    When bullish: bull_put_credit at 50-delta (sell higher put, buy 5 lower)

Put side (25-delta): 6DTE 5-wide em0.75 direction switch
  If iv_minus_vix >= -0.0502 AND rr25 <= -0.9976: bull_put_credit
  Else: bear_put_debit

Call side (50-delta): 5DTE 5-wide 50-delta, follows same regime switch
  When bullish: bull_put_credit (sell higher put, buy 5 lower)
  When bearish: bear_put_debit (buy higher put, sell 5 lower)

Filters:
  1. VIX1D veto: skip ALL when 10.0 <= VIX1D < 11.5
  2. VIX < 10: skip call side
  3. RV5/RV20 0.70-0.85: skip bullish legs (bull_put_credit on either side)
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
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import statistics

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

# ── Event reporting (best-effort, never blocks trading) ──
_ew = None
try:
    _repo_root = str(Path(__file__).resolve().parent.parent.parent.parent)
    if _repo_root not in sys.path:
        sys.path.insert(0, _repo_root)
    from reporting.events import EventWriter
    _EVENTS_AVAILABLE = True
except ImportError:
    _EVENTS_AVAILABLE = False


def _init_events(today: date):
    global _ew
    if not _EVENTS_AVAILABLE:
        return None
    try:
        _ew = EventWriter(strategy="dualside", account="schwab", trade_date=today)
        return _ew
    except Exception as e:
        print(f"DS_RUN WARN: EventWriter init failed: {e}")
        return None


def _emit(method: str, **kwargs):
    """Best-effort event emission. Never raises."""
    if _ew is None:
        return
    try:
        getattr(_ew, method)(**kwargs)
    except Exception as e:
        print(f"DS_RUN WARN: event emit failed ({method}): {e}")

__version__ = "1.1.0"

# ── config ──
CS_UNIT_DOLLARS = float(os.environ.get("DS_UNIT_DOLLARS", os.environ.get("CS_UNIT_DOLLARS", "10000")))
DS_LOG_PATH = os.environ.get("DS_LOG_PATH", "/tmp/logs/dualside_trades.csv")
DS_DRY_RUN = (os.environ.get("DS_DRY_RUN", os.environ.get("VERT_DRY_RUN", "false")) or "false").strip().lower() in ("1", "true", "yes")

# Strategy parameters
PUT_WIDTH = 5
CALL_WIDTH = 5
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

# Bull put credit position cap: skip new BPC if >= MAX_OPEN_BPC already open
MAX_OPEN_BPC = 3


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


def get_option_chain_wide(c) -> dict:
    """Fetch SPX option chain covering 1-14 calendar days.

    Returns the raw Schwab JSON with all expirations in the range.
    Wide enough to capture the 6th SPX expiration from any weekday.
    """
    from schwab.client import Client
    today = date.today()

    r = c.get_option_chain(
        "$SPX",
        contract_type=Client.Options.ContractType.ALL,
        strike_range=Client.Options.StrikeRange.ALL,
        from_date=today + timedelta(days=1),
        to_date=today + timedelta(days=14),
        include_underlying_quote=True,
    )
    r.raise_for_status()
    return r.json()


def available_expirations(chain_json: dict) -> list:
    """Extract all unique expiration dates from a Schwab chain, sorted ascending.

    Returns list of (date_str, dte_int) tuples.
    DTE here is actual calendar days to expiration as reported by Schwab.
    """
    seen = {}
    for side in ("callExpDateMap", "putExpDateMap"):
        for exp_key in chain_json.get(side, {}):
            parts = exp_key.split(":")
            if len(parts) >= 2:
                try:
                    seen[parts[0]] = int(parts[1])
                except ValueError:
                    pass
    return sorted(seen.items(), key=lambda x: x[0])


# ═══════════════════════════════════════════════════════════════
# Chain parsing — extract what we need from Schwab chain format
# ═══════════════════════════════════════════════════════════════

def parse_chain_for_expiration(chain_json: dict, target_exp_date: str) -> Optional[dict]:
    """Parse Schwab option chain, extracting only contracts for a specific expiration.

    Args:
        chain_json: raw Schwab chain JSON
        target_exp_date: expiration date string like "2026-03-14"

    Returns dict with: spot, contracts, expiration.
    """
    underlying = chain_json.get("underlying", {}) or {}
    spot = underlying.get("last") or underlying.get("mark") or underlying.get("close")
    if not spot:
        spot = chain_json.get("underlyingPrice")
    if not spot or spot <= 0:
        return None

    contracts = []

    for side, opt_type in [("callExpDateMap", "C"), ("putExpDateMap", "P")]:
        exp_map = chain_json.get(side, {})
        # Find the key matching our target date
        matched_strikes = None
        for exp_key, strikes_map in exp_map.items():
            if exp_key.startswith(target_exp_date):
                matched_strikes = strikes_map
                break

        if not matched_strikes:
            continue

        for strike_str, contracts_list in matched_strikes.items():
            if not contracts_list:
                continue
            c = contracts_list[0]
            strike = c.get("strikePrice")
            if not strike:
                continue

            # Skip AM-settled SPX options (root "SPX" not "SPXW").
            # On 3rd-Friday expirations, Schwab returns both AM and PM contracts.
            osi = c.get("symbol", "")
            osi_root = osi.strip()[:4] if osi else ""
            if osi and osi_root != "SPXW":
                continue

            bid = c.get("bid", 0) or 0
            ask = c.get("ask", 0) or 0
            iv = c.get("volatility")
            delta = c.get("delta")

            contracts.append({
                "strike": float(strike),
                "type": opt_type,
                "bid": float(bid),
                "ask": float(ask),
                "implied_vol": float(iv) / 100 if iv else None,
                "delta": float(delta) if delta is not None else None,
                "osi": osi,
                "expiration": target_exp_date,
            })

    if not contracts:
        return None

    return {
        "spot": spot,
        "contracts": contracts,
        "expiration": target_exp_date,
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
    return float(statistics.stdev(log_rets) * math.sqrt(252) * 100)


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


def count_open_bpc(pos):
    """Count open bull_put_credit spreads from broker positions.

    A bull_put_credit is a short put with a matching long put WIDTH points lower.
    We count distinct short-put legs that have a paired long put.
    """
    # Group by (expiration, put_call) → key is (ymd, cp, strike_padded)
    short_puts = []  # (exp, strike) where qty < 0 and type=P
    long_puts = set()  # (exp, strike) where qty > 0 and type=P
    for (ymd, cp, strike_str), qty in pos.items():
        if cp != "P":
            continue
        try:
            strike = int(strike_str) / 1000.0
        except (ValueError, TypeError):
            continue
        if qty < -1e-9:
            short_puts.append((ymd, strike, abs(qty)))
        elif qty > 1e-9:
            long_puts.add((ymd, strike))

    count = 0
    for ymd, strike, qty in short_puts:
        # Check for a matching long put at current or legacy width
        if (ymd, strike - PUT_WIDTH) in long_puts or (ymd, strike - 10) in long_puts:
            count += int(qty + 0.5)
    return count


# ═══════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════

def main():
    print(f"DS_RUN v{__version__} DRY_RUN={DS_DRY_RUN}")

    today = date.today()
    ew = _init_events(today)

    # ── Schwab client + account ──
    try:
        c = schwab_client()
        oc_val, oc_src, acct_num = opening_cash_for_account(c)
        acct_hash = get_account_hash(c)
    except Exception as e:
        _emit("error", message=f"Schwab init failed: {e}", stage="init")
        if ew:
            try: ew.close()
            except Exception: pass
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
        _emit("strategy_run", signal="SKIP", config="",
              reason=f"VIX1D veto ({vix1d:.2f})", vix=float(vix or 0), vix1d=float(vix1d))
        _emit("skip", reason=f"VIX1D veto ({vix1d:.2f})", signal="SKIP")
        if ew:
            try: ew.close()
            except Exception: pass
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

    # ── Fetch wide chain and resolve expirations by counting SPX expirations ──
    print("\nDS_RUN FETCHING wide chain (1-14 cal days)...")
    try:
        wide_chain = get_option_chain_wide(c)
    except Exception as e:
        print(f"DS_RUN SKIP: chain fetch failed: {e}")
        return 1

    expirations = available_expirations(wide_chain)
    print(f"DS_RUN EXPIRATIONS: {expirations}")

    # 5DTE = 5th SPX expiration from today, 6DTE = 6th
    # Index 4 and 5 (0-based) since expirations are sorted ascending
    if len(expirations) < 6:
        print(f"DS_RUN SKIP: need at least 6 expirations, found {len(expirations)}")
        return 1

    exp5_date = expirations[4][0]  # 5th expiration = call side helper
    exp6_date = expirations[5][0]  # 6th expiration = put side directional
    print(f"DS_RUN 5DTE={exp5_date} (dte={expirations[4][1]})  6DTE={exp6_date} (dte={expirations[5][1]})")

    # Parse 6DTE chain
    chain6 = parse_chain_for_expiration(wide_chain, exp6_date)
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

    # ── Fetch 5DTE chain (call side — follows regime switch) ──
    v_call = None
    call_skip_reason = None

    # Filter 2: VIX < 10 skips call
    if vix is not None and vix < VIX_CALL_SKIP:
        call_skip_reason = f"VIX<{VIX_CALL_SKIP} ({vix:.2f})"
    elif is_bull and rv_in_band:
        call_skip_reason = "RV_BAND (bull_put_credit skipped)"
    else:
        print(f"\nDS_RUN PARSING 5DTE chain (exp={exp5_date})...")
        chain5 = parse_chain_for_expiration(wide_chain, exp5_date)

        if chain5:
            spot5 = chain5["spot"]
            exp5 = chain5["expiration"]
            contracts5 = chain5["contracts"]
            print(f"DS_RUN 5DTE: spot={spot5:.2f} exp={exp5} contracts={len(contracts5)}")

            # 50-delta — follows regime switch (v1.1.0)
            call_p = find_by_delta(contracts5, "P", 0.50)
            if call_p:
                if is_bull:
                    # Bull put credit: sell higher put, buy CALL_WIDTH lower
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
                    # Bear put debit: buy higher put, sell CALL_WIDTH lower
                    long_strike = call_p["strike"]
                    short_strike = long_strike - CALL_WIDTH
                    long_osi = call_p.get("osi") or build_osi(exp5, "P", long_strike)
                    short_c = find_strike_near(contracts5, "P", short_strike)
                    short_osi = short_c.get("osi", build_osi(exp5, "P", short_strike)) if short_c else build_osi(exp5, "P", short_strike)
                    v_call = {
                        "name": "BEAR_PUT_DEBIT", "kind": "PUT", "side": "DEBIT", "direction": "LONG",
                        "short_osi": short_osi, "long_osi": long_osi,
                        "short_strike": short_strike, "long_strike": long_strike,
                        "target_qty": qty, "strength": 1.0,
                    }
            else:
                call_skip_reason = "no 50-delta put found"
        elif not call_skip_reason:
            call_skip_reason = f"5DTE chain empty (exp={exp5_date})"

    if call_skip_reason:
        print(f"DS_RUN CALL SKIP: {call_skip_reason}")
    else:
        print(f"DS_RUN CALL: {v_call['name']} strikes={v_call['long_strike']}/{v_call['short_strike']} "
              f"short={v_call['short_osi']} long={v_call['long_osi']}")

    # ── Nothing to trade? ──
    if not v_put and not v_call:
        _emit("strategy_run", signal="SKIP", config="",
              reason="no verticals after filters", spot=float(spot),
              vix=float(vix or 0), vix1d=float(vix1d or 0),
              filters={"put_skip": put_skip_reason, "call_skip": call_skip_reason,
                        "rv_in_band": rv_in_band, "iv_minus_vix": iv_minus_vix})
        _emit("skip", reason="no verticals to trade after filters")
        if ew:
            try: ew.close()
            except Exception: pass
        print("DS_RUN SKIP: no verticals to trade after filters.")
        return 0

    # ── Position guard + topup ──
    pos = None
    open_bpc_count = 0
    try:
        pos = positions_map(c, acct_hash)
        open_bpc_count = count_open_bpc(pos)
        print(f"DS_RUN POSITIONS: loaded {len(pos)} legs, open BPC={open_bpc_count}")
    except Exception as e:
        print(f"DS_RUN POSITIONS SKIP: fetch failed ({e}) — skipping ALL trades.")
        return 0

    def finalize(v):
        if not v:
            return None
        # BPC cap: skip any new bull_put_credit (put-side or call-side helper) if at cap
        if v["side"] == "CREDIT" and v["kind"] == "PUT" and open_bpc_count >= MAX_OPEN_BPC:
            print(f"DS_RUN BPC_CAP_SKIP {v['name']}: open={open_bpc_count} >= cap={MAX_OPEN_BPC}")
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
        _emit("strategy_run", signal=direction, config="",
              reason="nothing after TOPUP/GUARD", spot=float(spot),
              vix=float(vix or 0), vix1d=float(vix1d or 0))
        _emit("skip", reason="nothing to place after TOPUP/GUARD")
        if ew:
            try: ew.close()
            except Exception: pass
        print("DS_RUN SKIP: nothing to place after TOPUP/GUARD.")
        return 0

    # Emit strategy_run event for actual trade
    _emit("strategy_run", signal=direction, config=f"PUT:{v_put_f['name'] if v_put_f else 'SKIP'}_CALL:{v_call_f['name'] if v_call_f else 'SKIP'}",
          reason="OK", spot=float(spot), vix=float(vix or 0), vix1d=float(vix1d or 0),
          filters={"iv_minus_vix": iv_minus_vix, "rr25": rr25_vol_pts, "rv_ratio": rv_ratio,
                    "rv_in_band": rv_in_band, "put_skip": put_skip_reason, "call_skip": call_skip_reason},
          extra={"em": em, "em075": em075, "exp5": exp5_date, "exp6": exp6_date})

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

        # Each vertical gets its own trade_group_id
        if _ew is not None:
            _ew.new_trade_group()

        _emit("trade_intent", side=v["side"], direction=v["direction"],
              legs=[
                  {"osi": v["short_osi"], "strike": v.get("short_strike", 0),
                   "option_type": v["kind"], "action": "SELL_TO_OPEN", "qty": v["send_qty"]},
                  {"osi": v["long_osi"], "strike": v.get("long_strike", 0),
                   "option_type": v["kind"], "action": "BUY_TO_OPEN", "qty": v["send_qty"]},
              ],
              target_qty=v["send_qty"],
              extra={"name": v["name"], "expiration": v.get("expiration", "")})

        print(
            f"\nDS_RUN PLACING {v['name']}: side={v['side']} kind={v['kind']} "
            f"short={v['short_osi']} long={v['long_osi']} qty={v['send_qty']}"
        )
        env = env_for_vertical(v)
        rc = subprocess.call([sys.executable, PLACER_SCRIPT], env=env)
        if rc != 0:
            _emit("error", message=f"placer rc={rc}", stage=f"placement_{v['name']}")
            print(f"DS_RUN {v['name']}: placer rc={rc}")
        else:
            # Read back placement result from CSV log (last line)
            try:
                import csv as _csv
                with open(DS_LOG_PATH) as _lf:
                    rows = list(_csv.DictReader(_lf))
                if rows:
                    last = rows[-1]
                    oids = [x for x in (last.get("order_ids") or "").split(",") if x]
                    filled = int(last.get("qty_filled") or 0)
                    last_price = float(last.get("last_price") or 0) if last.get("last_price") else 0
                    requested = v["send_qty"]
                    for oid in oids:
                        _emit("order_submitted", order_id=oid,
                              legs=[
                                  {"osi": v["short_osi"], "strike": v.get("short_strike", 0),
                                   "option_type": v["kind"], "action": "SELL_TO_OPEN", "qty": requested},
                                  {"osi": v["long_osi"], "strike": v.get("long_strike", 0),
                                   "option_type": v["kind"], "action": "BUY_TO_OPEN", "qty": requested},
                              ],
                              limit_price=last_price)
                    if filled > 0:
                        _emit("fill", order_id=oids[0] if oids else "",
                              fill_qty=filled, fill_price=last_price,
                              legs=[
                                  {"osi": v["short_osi"], "strike": v.get("short_strike", 0),
                                   "option_type": v["kind"], "qty": filled},
                                  {"osi": v["long_osi"], "strike": v.get("long_strike", 0),
                                   "option_type": v["kind"], "qty": filled},
                              ])
            except Exception as _e:
                print(f"DS_RUN WARN: could not read placement result: {_e}")

    if ew:
        try: ew.close()
        except Exception: pass

    return 0


if __name__ == "__main__":
    sys.exit(main())
