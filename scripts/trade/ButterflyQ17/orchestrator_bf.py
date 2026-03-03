#!/usr/bin/env python3
# BUTTERFLY Q17 — SPX 2DTE call butterfly orchestrator
#
# What it does:
# - Fetches VIX1D (VixOne) from Leo's GammaWizard API.
# - Fetches VIX quote + 2DTE SPX option chain from Schwab.
# - Classifies VIX1D into Q17 quantile bucket -> SELL / BUY / SKIP.
# - VIX > 23 cap: SELL -> SKIP.
# - Computes ATM strike, expected move (EM), and EM-scaled width.
#   - SELL execution: width = round(EM * 1.25 / 5) * 5
#   - BUY execution:  width = round(EM * 0.85 / 5) * 5
# - Builds 3-leg ATM call butterfly: BUY lower + SELL 2x center + BUY upper.
# - Delegates placement to scripts/trade/ButterflyQ17/place_butterfly.py via BF_* envs.
#
# Strategy trained on 2018-2026 backtest, Q17 quantile VIX1D buckets (2023+ training).
# 825 trades, $206k total, 1.45 PF, zero losses > $5k.

import os
import sys
import re
import time
import random
import subprocess
from datetime import date, timedelta
from typing import Any, Dict, Optional, Tuple

import requests


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

# ---------- Strategy Config ----------

VIX_CAP = 23.0          # SELL -> SKIP when VIX > this
SELL_EM_MULT = 1.25     # width = EM * 1.25 for SELL
BUY_EM_MULT = 0.85      # width = EM * 0.85 for BUY
MIN_WIDTH = 15
MAX_WIDTH = 200
STRIKE_STEP = 5          # SPX strikes are $5 apart
BF_UNIT_DOLLARS = 30_000 # $30k equity per 1 butterfly contract

BF_LOG_PATH = os.environ.get("BF_LOG_PATH", "logs/butterfly_q17_trades.csv")

GW_BASE = os.environ.get("GW_BASE", "https://gandalf.gammawizard.com").rstrip("/")
GW_ENDPOINT = os.environ.get("GW_ENDPOINT", "rapi/GetUltraPureConstantStable").lstrip("/")

# Q17 bucket edges (VIX1D raw decimal values, NOT x100)
Q17_EDGES = [
    0.0, 0.064, 0.078, 0.095, 0.106, 0.113, 0.120, 0.127, 0.134,
    0.142, 0.154, 0.164, 0.174, 0.185, 0.200, 0.218, 0.265, 0.854,
]

# Q17 bucket actions: index = bucket number
Q17_ACTIONS = [
    "SKIP",   # B0:  < 0.064
    "SELL",   # B1:  0.064-0.078
    "SELL",   # B2:  0.078-0.095
    "BUY",    # B3:  0.095-0.106
    "BUY",    # B4:  0.106-0.113
    "SELL",   # B5:  0.113-0.120
    "SELL",   # B6:  0.120-0.127
    "BUY",    # B7:  0.127-0.134
    "SKIP",   # B8:  0.134-0.142
    "BUY",    # B9:  0.142-0.154
    "SELL",   # B10: 0.154-0.164
    "SELL",   # B11: 0.164-0.174
    "SELL",   # B12: 0.174-0.185
    "SKIP",   # B13: 0.185-0.200
    "SKIP",   # B14: 0.200-0.218
    "BUY",    # B15: 0.218-0.265
    "SELL",   # B16: 0.265+
]

# --- TOPUP config (prevent duplicate entries if already positioned) ---
BF_TOPUP = (os.environ.get("BF_TOPUP", "1") or "1").strip().lower() in ("1", "true", "yes", "y")
BF_TOPUP_FAIL_ACTION = (os.environ.get("BF_TOPUP_FAIL_ACTION", "SKIP_ALL") or "SKIP_ALL").strip().upper()

# --- NO-CLOSE guard config (prevent closing existing legs) ---
BF_GUARD_NO_CLOSE = (os.environ.get("BF_GUARD_NO_CLOSE", "1") or "1").strip().lower() in ("1", "true", "yes", "y")
BF_GUARD_FAIL_ACTION = (os.environ.get("BF_GUARD_FAIL_ACTION", "SKIP_ALL") or "SKIP_ALL").strip().upper()


# ---------- Utility helpers ----------

def _add_business_days(d, n):
    """Add n business days to date d (ignores market holidays)."""
    current = d
    added = 0
    while added < n:
        current += timedelta(days=1)
        if current.weekday() < 5:  # Mon-Fri
            added += 1
    return current


def build_osi(root: str, exp_date: date, cp: str, strike: int) -> str:
    """Build OSI symbol: ROOT(6) + YYMMDD(6) + C/P(1) + STRIKE_MILLS(8)."""
    exp6 = f"{exp_date:%y%m%d}"
    mills = strike * 1000
    return f"{root:<6}{exp6}{cp}{mills:08d}"


def round_to_strike(value: float) -> int:
    """Round to nearest strike step ($5)."""
    return int(round(value / STRIKE_STEP) * STRIKE_STEP)


def compute_width(em: float, mult: float) -> int:
    """Compute butterfly wing width from expected move and multiplier."""
    raw = em * mult
    w = round_to_strike(raw)
    return max(MIN_WIDTH, min(MAX_WIDTH, w))


def classify_vix1d(vix1d: float) -> Tuple[int, str]:
    """Returns (bucket_index, action) for a given VIX1D value."""
    for i in range(len(Q17_EDGES) - 1):
        if vix1d < Q17_EDGES[i + 1]:
            return (i, Q17_ACTIONS[i])
    return (len(Q17_ACTIONS) - 1, Q17_ACTIONS[-1])


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
    """
    Canonical key for an OSI-like string:
      (YYMMDD, 'C'/'P', strike_8digits)
    Assumes OSI formatted like: ROOT(6) + YYMMDD(6) + C/P(1) + STRIKE(8).
    """
    s = (osi or "")
    if len(s) < 21:
        return ("", "", "")
    return (s[6:12], s[12], s[-8:])


def fnum(x):
    try:
        return float(x)
    except Exception:
        return None


# ---------- GammaWizard ----------

def _sanitize_token(t: str) -> str:
    t = (t or "").strip().strip('"').strip("'")
    return t.split(None, 1)[1] if t.lower().startswith("bearer ") else t


def gw_fetch():
    base = GW_BASE
    endpoint = GW_ENDPOINT
    tok = _sanitize_token(os.environ.get("GW_TOKEN", "") or "")

    url = f"{base}/{endpoint}"
    print("BF_Q17 GW URL:", url)

    def hit(tkn):
        h = {"Accept": "application/json"}
        if tkn:
            h["Authorization"] = f"Bearer {_sanitize_token(tkn)}"
        return requests.get(url, headers=h, timeout=30)

    r = hit(tok) if tok else None
    if (r is None) or (r.status_code in (401, 403)):
        email = os.environ.get("GW_EMAIL", "")
        pwd = os.environ.get("GW_PASSWORD", "")
        if not (email and pwd):
            raise RuntimeError("GW_AUTH_REQUIRED")
        rr = requests.post(
            f"{base}/goauth/authenticateFireUser",
            data={"email": email, "password": pwd},
            timeout=30,
        )
        rr.raise_for_status()
        t = rr.json().get("token") or ""
        r = hit(t)

    r.raise_for_status()
    return r.json()


def extract_vix1d(j) -> Optional[float]:
    """Extract VIX1D (VixOne) from GW API response. Returns raw decimal or None."""
    if isinstance(j, dict):
        if "Trade" in j:
            tr = j["Trade"]
            if isinstance(tr, list) and tr:
                tr = tr[-1]
            if isinstance(tr, dict):
                v = fnum(tr.get("VixOne"))
                if v is not None:
                    return v
        v = fnum(j.get("VixOne"))
        if v is not None:
            return v
        for val in j.values():
            if isinstance(val, (dict, list)):
                r = extract_vix1d(val)
                if r is not None:
                    return r
    if isinstance(j, list):
        for it in reversed(j):
            r = extract_vix1d(it)
            if r is not None:
                return r
    return None


# ---------- Schwab helpers ----------

def opening_cash_for_account(c, prefer_number=None):
    """
    Returns (value_or_None, source_key, acct_number).
    Ignores 0/negative values; prefers liquidationValue first.
    """
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
        acct_id = None
        init = {}
        curr = {}
        stack = [a]
        while stack:
            x = stack.pop()
            if isinstance(x, dict):
                if acct_id is None and x.get("accountNumber"):
                    acct_id = str(x["accountNumber"])
                if "initialBalances" in x and isinstance(x["initialBalances"], dict):
                    init = x["initialBalances"]
                if "currentBalances" in x and isinstance(x["currentBalances"], dict):
                    curr = x["currentBalances"]
                for v in x.values():
                    if isinstance(v, (dict, list)):
                        stack.append(v)
            elif isinstance(x, list):
                stack.extend(x)
        return acct_id, init, curr

    chosen = None
    acct_num = ""
    for a in arr:
        aid, init, curr = hunt(a)
        if prefer_number and aid == prefer_number:
            chosen = (init, curr)
            acct_num = aid
            break
        if chosen is None:
            chosen = (init, curr)
            acct_num = aid

    if not chosen:
        return None, "none", ""

    init, curr = chosen

    keys = [
        "liquidationValue",
        "cashAvailableForTrading",
        "cashBalance",
        "availableFundsNonMarginableTrade",
        "buyingPowerNonMarginableTrade",
        "optionBuyingPower",
        "buyingPower",
    ]

    def pick(src):
        for k in keys:
            v = (src or {}).get(k)
            if isinstance(v, (int, float)):
                fv = float(v)
                if fv > 0:
                    return fv, k
        return None

    for src in (init, curr):
        got = pick(src)
        if got:
            return got[0], got[1], acct_num

    return None, "none", acct_num


def get_account_hash(c):
    r = c.get_account_numbers()
    r.raise_for_status()
    arr = r.json() or []
    if not arr:
        return ""
    info = arr[0]
    return str(info.get("hashValue") or info.get("hashvalue") or "")


# ----- Positions map (for NO-CLOSE guard + TOPUP) -----

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


def _osi_from_instrument(ins: Dict[str, Any]) -> str | None:
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
    """
    Returns dict: canon -> net_qty (positive=long, negative=short)
    """
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


def open_butterflies_count(bf: Dict[str, Any], pos: Dict[Tuple[str, str, str], float]) -> int:
    """
    Count open butterfly spreads from existing positions.
    Butterfly: BUY 1 lower + SELL 2 center + BUY 1 upper.
    Open count = min(lower_long, center_short // 2, upper_long).
    """
    lower_key = osi_canon(bf["lower_osi"])
    center_key = osi_canon(bf["center_osi"])
    upper_key = osi_canon(bf["upper_osi"])

    lower_pos = float(pos.get(lower_key, 0.0))
    center_pos = float(pos.get(center_key, 0.0))
    upper_pos = float(pos.get(upper_key, 0.0))

    lo = lower_pos if lower_pos > 0 else 0.0
    mid = (-center_pos / 2.0) if center_pos < 0 else 0.0
    hi = upper_pos if upper_pos > 0 else 0.0
    return int(min(lo, mid, hi) + 1e-9)


def would_close_guard_bf(bf: Dict[str, Any], pos: Dict[Tuple[str, str, str], float]) -> bool:
    """
    Returns True if opening this butterfly would close an existing position.
    - BUY_TO_OPEN lower:  unsafe if currently short that strike
    - SELL_TO_OPEN center: unsafe if currently long that strike
    - BUY_TO_OPEN upper:  unsafe if currently short that strike
    """
    lower_key = osi_canon(bf["lower_osi"])
    center_key = osi_canon(bf["center_osi"])
    upper_key = osi_canon(bf["upper_osi"])

    lower_pos = float(pos.get(lower_key, 0.0))
    center_pos = float(pos.get(center_key, 0.0))
    upper_pos = float(pos.get(upper_key, 0.0))

    print(
        f"BF_Q17 GUARD_CHECK: "
        f"BUY lower {bf['lower_osi']} pos={lower_pos:+g} ; "
        f"SELL center {bf['center_osi']} pos={center_pos:+g} ; "
        f"BUY upper {bf['upper_osi']} pos={upper_pos:+g}"
    )

    if lower_pos < -1e-9:     # short lower -> BUY would close
        return True
    if center_pos > 1e-9:     # long center -> SELL would close
        return True
    if upper_pos < -1e-9:     # short upper -> BUY would close
        return True
    return False


# ---------- Schwab market data ----------

def fetch_vix(c) -> float:
    """Fetch current VIX from Schwab quotes API."""
    j = schwab_get_json(
        c, "https://api.schwabapi.com/marketdata/v1/quotes",
        params={"symbols": "$VIX", "fields": "quote"}, tag="VIX",
    )
    q = (j.get("$VIX") or {}).get("quote") or {}
    vix = fnum(q.get("lastPrice")) or fnum(q.get("closePrice"))
    if vix is None:
        raise RuntimeError("BF_Q17 FAIL: cannot read VIX from Schwab")
    return vix


def fetch_spx_chain(c, target_exp: date) -> dict:
    """Fetch SPX CALL option chain for a specific expiration date."""
    return schwab_get_json(
        c, "https://api.schwabapi.com/marketdata/v1/chains",
        params={
            "symbol": "$SPX",
            "contractType": "CALL",
            "fromDate": target_exp.isoformat(),
            "toDate": target_exp.isoformat(),
            "strikeCount": 80,
        }, tag="CHAIN",
    )


def parse_chain(raw: dict, target_exp: date) -> Dict[str, Any]:
    """
    Parse Schwab chain response -> dict with:
      spot, atm_strike, em, atm_call_mid, strikes{strike->{bid,ask,mid}}
    """
    underlying = raw.get("underlying") or {}
    spot = (
        fnum(underlying.get("last"))
        or fnum(underlying.get("close"))
        or fnum(underlying.get("mark"))
        or fnum(underlying.get("lastPrice"))
        or fnum(underlying.get("closePrice"))
    )
    if not spot or spot <= 0:
        print(f"BF_Q17 DEBUG: underlying keys={list(underlying.keys())[:15]} values(last={underlying.get('last')}, close={underlying.get('close')}, mark={underlying.get('mark')})")
        raise RuntimeError("BF_Q17 FAIL: no underlying spot in chain")

    call_map = raw.get("callExpDateMap") or {}
    exp_key = None
    for k in call_map:
        if k.startswith(target_exp.isoformat()):
            exp_key = k
            break
    if not exp_key:
        raise RuntimeError(f"BF_Q17 FAIL: no chain for exp={target_exp}")

    strikes_raw = call_map[exp_key]
    strikes = {}
    for sk, contracts in strikes_raw.items():
        strike_val = fnum(sk)
        if strike_val is None or not contracts:
            continue
        c0 = contracts[0] if isinstance(contracts, list) else contracts
        bid = fnum(c0.get("bid")) or 0.0
        ask = fnum(c0.get("ask")) or 0.0
        mid = (bid + ask) / 2.0 if (bid > 0 and ask > 0) else 0.0
        strikes[int(round(strike_val))] = {"bid": bid, "ask": ask, "mid": mid}

    if not strikes:
        raise RuntimeError("BF_Q17 FAIL: no strikes parsed from chain")

    atm_strike = min(strikes.keys(), key=lambda s: abs(s - spot))
    atm_data = strikes.get(atm_strike, {})
    atm_call_mid = atm_data.get("mid", 0.0)
    em = atm_call_mid * 2.0  # straddle ~ 2 x ATM call at ATM

    return {
        "spot": spot,
        "atm_strike": atm_strike,
        "em": em,
        "atm_call_mid": atm_call_mid,
        "strikes": strikes,
    }


# ---------- main ----------

def main():
    # --- Schwab init + equity-based sizing ---
    try:
        c = schwab_client()
        oc_val, oc_src, acct_num = opening_cash_for_account(c)
        acct_hash = get_account_hash(c)
    except Exception as e:
        print(f"BF_Q17 SKIP: Schwab init failed: {e}")
        return 1

    ov_raw = (os.environ.get("SIZING_DOLLARS_OVERRIDE", "") or "").strip()
    if ov_raw:
        try:
            oc_val = float(ov_raw)
            oc_src = "SIZING_DOLLARS_OVERRIDE"
            print(f"BF_Q17 INFO: using SIZING_DOLLARS_OVERRIDE={oc_val}")
        except Exception:
            print("BF_Q17 WARN: bad SIZING_DOLLARS_OVERRIDE, ignoring.")

    dry_run = (os.environ.get("BF_DRY_RUN", "") or "").strip().lower() in ("1", "true", "yes", "y")

    print(f"BF_Q17 EQUITY: {oc_val} (src={oc_src}, acct={acct_num})")

    if oc_val is None or oc_val < BF_UNIT_DOLLARS:
        if dry_run:
            units = 1
            print(f"BF_Q17 DRY_RUN: equity {oc_val} below ${BF_UNIT_DOLLARS:,} — using {units} unit(s) for paper trade")
        else:
            print(f"BF_Q17 SKIP: equity {oc_val} below minimum ${BF_UNIT_DOLLARS:,} — not trading")
            return 0
    else:
        units = int(oc_val // BF_UNIT_DOLLARS)

    print(f"BF_Q17 UNITS: {units} (BF_UNIT_DOLLARS={BF_UNIT_DOLLARS}, equity={oc_val})")

    # --- Fetch VIX1D from Leo's GW API ---
    try:
        api = gw_fetch()
        vix1d = extract_vix1d(api)
    except Exception as e:
        print(f"BF_Q17 SKIP: GW fetch failed: {e}")
        return 0

    if vix1d is None:
        print("BF_Q17 SKIP: VixOne not found in GW response")
        return 0

    bucket, action = classify_vix1d(vix1d)
    print(f"BF_Q17 VIX1D: {vix1d:.4f} -> bucket={bucket} action={action}")

    if action == "SKIP":
        print(f"BF_Q17 SKIP: Q17 bucket {bucket} -> SKIP")
        return 0

    # --- Fetch VIX from Schwab, apply VIX > 23 cap ---
    try:
        vix = fetch_vix(c)
    except Exception as e:
        print(f"BF_Q17 SKIP: VIX fetch failed: {e}")
        return 0

    print(f"BF_Q17 VIX: {vix:.2f}")

    if action == "SELL" and vix > VIX_CAP:
        print(f"BF_Q17 SKIP: VIX {vix:.2f} > cap {VIX_CAP} — SELL capped to SKIP")
        return 0

    # --- Compute target expiration (2 business days from today) ---
    today = date.today()
    target_exp = _add_business_days(today, 2)
    print(f"BF_Q17 EXPIRATION: today={today} target_exp={target_exp}")

    # --- Fetch 2DTE chain from Schwab ---
    try:
        raw_chain = fetch_spx_chain(c, target_exp)
        chain = parse_chain(raw_chain, target_exp)
    except Exception as e:
        print(f"BF_Q17 SKIP: chain fetch/parse failed: {e}")
        return 0

    spot = chain["spot"]
    atm_strike = chain["atm_strike"]
    em = chain["em"]
    atm_call_mid = chain["atm_call_mid"]
    print(f"BF_Q17 CHAIN: spot={spot:.2f} ATM={atm_strike} EM={em:.2f} ATM_call_mid={atm_call_mid:.2f}")

    if em <= 0:
        print("BF_Q17 SKIP: EM <= 0")
        return 0

    # --- Compute width and strikes ---
    mult = SELL_EM_MULT if action == "SELL" else BUY_EM_MULT
    width = compute_width(em, mult)
    center = atm_strike
    lower = center - width
    upper = center + width

    lower_osi = build_osi("SPXW", target_exp, "C", lower)
    center_osi = build_osi("SPXW", target_exp, "C", center)
    upper_osi = build_osi("SPXW", target_exp, "C", upper)

    direction = action  # "SELL" or "BUY"

    print(f"BF_Q17 BUTTERFLY: action={action} width={width} mult={mult:.2f}")
    print(f"  lower={lower} ({lower_osi})")
    print(f"  center={center} ({center_osi}) x2")
    print(f"  upper={upper} ({upper_osi})")
    print(f"  direction={direction} qty={units}")

    bf = {
        "lower_osi": lower_osi,
        "center_osi": center_osi,
        "upper_osi": upper_osi,
        "target_qty": units,
    }

    # --- TOPUP + GUARD ---
    need_positions = BF_GUARD_NO_CLOSE or BF_TOPUP
    pos = None
    if need_positions:
        try:
            pos = positions_map(c, acct_hash)
            print(
                f"BF_Q17 POSITIONS: loaded count={len(pos)} "
                f"(guard={'on' if BF_GUARD_NO_CLOSE else 'off'}, "
                f"topup={'on' if BF_TOPUP else 'off'})"
            )
        except Exception as e:
            msg = str(e)[:220]
            guard_ok = (not BF_GUARD_NO_CLOSE) or (BF_GUARD_FAIL_ACTION == "CONTINUE")
            topup_ok = (not BF_TOPUP) or (BF_TOPUP_FAIL_ACTION == "CONTINUE")
            if guard_ok and topup_ok:
                print(f"BF_Q17 POSITIONS WARN: fetch failed ({msg}) — continuing WITHOUT positions.")
                pos = None
            else:
                print(f"BF_Q17 POSITIONS SKIP: fetch failed ({msg}) — skipping.")
                return 0

    send_qty = units

    if BF_TOPUP:
        if pos is None:
            if BF_TOPUP_FAIL_ACTION != "CONTINUE":
                print("BF_Q17 SKIP: TOPUP enabled but positions unavailable")
                return 0
        else:
            open_qty = open_butterflies_count(bf, pos)
            send_qty = max(0, units - open_qty)
            print(f"BF_Q17 TOPUP: target={units} open={open_qty} rem={send_qty}")

    if send_qty <= 0:
        print("BF_Q17 SKIP: AT_OR_ABOVE_TARGET")
        return 0

    if BF_GUARD_NO_CLOSE:
        if pos is None:
            if BF_GUARD_FAIL_ACTION != "CONTINUE":
                print("BF_Q17 SKIP: GUARD enabled but positions unavailable")
                return 0
        else:
            if would_close_guard_bf(bf, pos):
                print("BF_Q17 GUARD_SKIP: would close existing position")
                return 0

    # --- Delegate to placer ---
    env = dict(os.environ)
    env.update({
        "BF_DIRECTION":      direction,
        "BF_LOWER_OSI":      lower_osi,
        "BF_CENTER_OSI":     center_osi,
        "BF_UPPER_OSI":      upper_osi,
        "BF_QTY":            str(send_qty),
        "BF_WIDTH":          str(width),
        "BF_ATM_STRIKE":     str(center),
        "BF_EM":             f"{em:.2f}",
        "BF_EM_MULT":        f"{mult:.2f}",
        "BF_SPOT":           f"{spot:.2f}",
        "BF_VIX":            f"{vix:.2f}",
        "BF_VIX1D":          f"{vix1d:.4f}",
        "BF_BUCKET":         str(bucket),
        "BF_ACTION":         action,
        "BF_EXPIRATION":     target_exp.isoformat(),
        "BF_UNIT_DOLLARS_V": str(BF_UNIT_DOLLARS),
        "BF_EQUITY":         str(oc_val or 0),
        "BF_UNITS":          str(units),

        # needed by placer
        "SCHWAB_APP_KEY":    os.environ["SCHWAB_APP_KEY"],
        "SCHWAB_APP_SECRET": os.environ["SCHWAB_APP_SECRET"],
        "SCHWAB_TOKEN_JSON": os.environ["SCHWAB_TOKEN_JSON"],
        "SCHWAB_ACCT_HASH":  acct_hash,
        "BF_LOG_PATH":       BF_LOG_PATH,
    })

    print(f"BF_Q17 PLACING: {direction} {send_qty}x {width}w butterfly @ {center}")
    rc = subprocess.call(
        [sys.executable, "scripts/trade/ButterflyQ17/place_butterfly.py"], env=env,
    )
    if rc != 0:
        print(f"BF_Q17 PLACER: rc={rc}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
