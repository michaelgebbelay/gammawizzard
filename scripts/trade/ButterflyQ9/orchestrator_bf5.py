#!/usr/bin/env python3
# BUTTERFLY Q9 — SPX 5DTE call butterfly orchestrator
#
# What it does:
# - Fetches VIX1D (VixOne) from Leo's GammaWizard API.
# - Fetches VIX quote + 5DTE SPX option chain from Schwab.
# - Classifies VIX1D into Q9 quantile bucket -> SELL / BUY / SKIP.
# - VIX > 23 cap: SELL -> SKIP.
# - Applies adaptive cadence gate (FAST_V1) by equity + drawdown tiers.
# - Computes ATM strike and expected move (EM).
# - Selects wings by side-specific put-delta profiles:
#   - BUY execution: 20P anchor, symmetric call wing from put width
#   - SELL execution: 35P anchor, symmetric call wing from put width
# - Falls back to EM-scaled symmetric width only if delta strikes are unavailable.
# - Builds 3-leg ATM call butterfly: BUY lower + SELL 2x center + BUY upper.
# - Delegates placement to scripts/trade/ButterflyQ17/place_butterfly.py via BF_* envs.
#
# Strategy trained on 2023-2026 backtest, Q9 quantile VIX1D buckets (BASE).
# Walk-forward OOS: $96k, PF 1.32, 219 trades.

import os
import sys
import re
import time
import random
import json
import subprocess
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, Set

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

SELL_EM_MULT = 1.25     # width = EM * 1.25 for SELL
BUY_EM_MULT = 0.85      # width = EM * 0.85 for BUY
MIN_WIDTH = 15
MAX_WIDTH = 200
STRIKE_STEP = 5          # SPX strikes are $5 apart
BF_UNIT_DOLLARS = 30_000 # $30k equity per 1 butterfly contract
DTE_BUSINESS_DAYS = 5    # 5 business days to expiration
BF_PRICE_TICK = 0.05

# Static weekday execution gate fallback; default Mon/Tue/Wed (0,1,2).
ALLOWED_ENTRY_WEEKDAYS = {
    int(x.strip())
    for x in (os.environ.get("BF_ENTRY_WEEKDAYS", "0,1,2") or "0,1,2").split(",")
    if x.strip().isdigit() and 0 <= int(x.strip()) <= 6
}
if not ALLOWED_ENTRY_WEEKDAYS:
    ALLOWED_ENTRY_WEEKDAYS = {0, 1, 2}

BF_LOG_PATH = os.environ.get("BF_LOG_PATH", "logs/butterfly_q9_trades.csv")

# Adaptive cadence (FAST_V1) enabled by default.
BF_USE_ADAPTIVE_CADENCE = (os.environ.get("BF_USE_ADAPTIVE_CADENCE", "1") or "1").strip().lower() in ("1", "true", "yes", "y")
BF_CADENCE_STATE_PATH = os.environ.get(
    "BF_CADENCE_STATE_PATH",
    str(Path(BF_LOG_PATH).with_name("butterfly_q9_cadence_state.json")),
)

# FAST_V1 thresholds:
# 1) DD >= 30% -> Wed only
# 2) DD >= 20% -> Tue/Thu
# 3) Balance < 23k -> Tue/Thu
# 4) 23k-32k -> Mon/Tue/Wed
# 5) 32k-40k -> Mon/Tue/Wed/Thu
# 6) >= 40k -> Mon-Fri
FAST_V1_DD_W_ONLY = 0.30
FAST_V1_DD_TTH = 0.20
FAST_V1_BAL_TTH = 23_000.0
FAST_V1_BAL_MTW = 32_000.0
FAST_V1_BAL_MTWTH = 40_000.0

GW_BASE = os.environ.get("GW_BASE", "https://gandalf.gammawizard.com").rstrip("/")
GW_ENDPOINT = os.environ.get("GW_ENDPOINT", "rapi/GetUltraPureConstantStable").lstrip("/")

# Q9 bucket edges (VIX1D raw decimal values, NOT x100)
# Trained on 2023+ data, 9 quantile buckets with SKIP on extremes (0 and 8)
Q9_EDGES = [
    0.015, 0.076, 0.099, 0.112, 0.124,
    0.135, 0.155, 0.173, 0.199, 0.519,
]

# Q9 bucket actions: index = bucket number (BASE variant — all buckets active)
Q9_ACTIONS = [
    "SELL",   # B0:  < 0.076  (VIX1D < 7.6%)
    "BUY",    # B1:  0.076-0.099
    "BUY",    # B2:  0.099-0.112
    "BUY",    # B3:  0.112-0.124
    "SELL",   # B4:  0.124-0.135
    "SELL",   # B5:  0.135-0.155
    "SELL",   # B6:  0.155-0.173
    "BUY",    # B7:  0.173-0.199
    "SELL",   # B8:  >= 0.199  (VIX1D > 19.9%)
]

# Side profiles (absolute delta targets).
BUY_PUT_DELTA = 0.20
BUY_CALL_DELTA = 0.10
SELL_PUT_DELTA = 0.35
SELL_CALL_DELTA = 0.25

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


def _leg_mid_from_chain(q: Dict[str, Any]) -> float:
    m = fnum((q or {}).get("mid"))
    if m is not None and m > 0:
        return float(m)
    b = fnum((q or {}).get("bid"))
    a = fnum((q or {}).get("ask"))
    if b is not None and b > 0 and a is not None and a > 0:
        return float((b + a) / 2.0)
    return 0.0


def _round_price_tick(v: float) -> float:
    return round(round(float(v) / BF_PRICE_TICK) * BF_PRICE_TICK + 1e-12, 2)


def side_delta_targets(action: str) -> Tuple[float, float, str]:
    """Return (put_delta_abs, call_delta_abs, profile_tag) for BUY/SELL side."""
    if action == "SELL":
        return SELL_PUT_DELTA, SELL_CALL_DELTA, "SELL_P35_ANCHORED"
    return BUY_PUT_DELTA, BUY_CALL_DELTA, "BUY_P20_ANCHORED"


def nearest_delta_strike(
    deltas: Dict[int, float],
    target_abs: float,
    center: int,
    side: str,
) -> Optional[int]:
    """
    Find strike nearest to target absolute delta.
    side="PUT": prefer strikes below center with negative deltas.
    side="CALL": prefer strikes above center with positive deltas.
    """
    side = side.upper().strip()
    if side not in ("PUT", "CALL"):
        return None

    def valid_for_side(strike: int, delta: float) -> bool:
        if side == "PUT":
            return strike < center and delta < 0
        return strike > center and delta > 0

    candidates = [
        (strike, delta)
        for strike, delta in deltas.items()
        if delta is not None and valid_for_side(strike, delta)
    ]

    # Fallback if strict side filtering leaves nothing (data gaps / sparse greeks).
    if not candidates:
        if side == "PUT":
            candidates = [(s, d) for s, d in deltas.items() if d is not None and d < 0]
        else:
            candidates = [(s, d) for s, d in deltas.items() if d is not None and d > 0]

    if not candidates:
        return None

    best = min(
        candidates,
        key=lambda x: (abs(abs(x[1]) - target_abs), abs(x[0] - center)),
    )
    return int(best[0])


def classify_vix1d(vix1d: float) -> Tuple[int, str]:
    """Returns (bucket_index, action) for a given VIX1D value."""
    for i in range(len(Q9_EDGES) - 1):
        if vix1d < Q9_EDGES[i + 1]:
            return (i, Q9_ACTIONS[i])
    return (len(Q9_ACTIONS) - 1, Q9_ACTIONS[-1])


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


def fnum(x):
    try:
        return float(x)
    except Exception:
        return None


def _load_cadence_peak_equity(path: str) -> float:
    try:
        p = Path(path)
        if not p.exists():
            return 0.0
        j = json.loads(p.read_text(encoding="utf-8"))
        v = fnum((j or {}).get("peak_equity"))
        return max(0.0, float(v or 0.0))
    except Exception:
        return 0.0


def _save_cadence_peak_equity(path: str, peak_equity: float) -> None:
    try:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "peak_equity": float(max(0.0, peak_equity)),
            "updated_utc_epoch": int(time.time()),
        }
        p.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    except Exception:
        # Non-fatal: cadence can run without persisted peak.
        pass


def _weekday_names(days: Set[int]) -> str:
    names = {
        0: "Mon",
        1: "Tue",
        2: "Wed",
        3: "Thu",
        4: "Fri",
        5: "Sat",
        6: "Sun",
    }
    return ",".join(names.get(d, str(d)) for d in sorted(days))


def cadence_fast_v1_allowed_weekdays(equity: float, peak_equity: float) -> Tuple[Set[int], str, float]:
    """
    Return (allowed_weekdays, tier_name, drawdown_pct) for FAST_V1 adaptive cadence.
    """
    eq = max(0.0, float(equity))
    peak = max(eq, float(peak_equity))
    dd = ((peak - eq) / peak) if peak > 0 else 0.0

    if dd >= FAST_V1_DD_W_ONLY:
        return {2}, "W_ONLY_DD30", dd
    if dd >= FAST_V1_DD_TTH:
        return {1, 3}, "TTH_DD20", dd
    if eq < FAST_V1_BAL_TTH:
        return {1, 3}, "TTH_LT23K", dd
    if eq < FAST_V1_BAL_MTW:
        return {0, 1, 2}, "MTW_23K_32K", dd
    if eq < FAST_V1_BAL_MTWTH:
        return {0, 1, 2, 3}, "MTWTH_32K_40K", dd
    return {0, 1, 2, 3, 4}, "ALL_GTE40K", dd


# ---------- GammaWizard ----------

def _sanitize_token(t: str) -> str:
    t = (t or "").strip().strip('"').strip("'")
    return t.split(None, 1)[1] if t.lower().startswith("bearer ") else t


def gw_fetch():
    base = GW_BASE
    endpoint = GW_ENDPOINT
    tok = _sanitize_token(os.environ.get("GW_TOKEN", "") or "")

    url = f"{base}/{endpoint}"
    print("BF_Q9 GW URL:", url)

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
    lower_key = osi_canon(bf["lower_osi"])
    center_key = osi_canon(bf["center_osi"])
    upper_key = osi_canon(bf["upper_osi"])

    lower_pos = float(pos.get(lower_key, 0.0))
    center_pos = float(pos.get(center_key, 0.0))
    upper_pos = float(pos.get(upper_key, 0.0))

    print(
        f"BF_Q9 GUARD_CHECK: "
        f"BUY lower {bf['lower_osi']} pos={lower_pos:+g} ; "
        f"SELL center {bf['center_osi']} pos={center_pos:+g} ; "
        f"BUY upper {bf['upper_osi']} pos={upper_pos:+g}"
    )

    if lower_pos < -1e-9:
        return True
    if center_pos > 1e-9:
        return True
    if upper_pos < -1e-9:
        return True
    return False


# ---------- Schwab market data ----------

def fetch_vix(c) -> float:
    j = schwab_get_json(
        c, "https://api.schwabapi.com/marketdata/v1/quotes",
        params={"symbols": "$VIX", "fields": "quote"}, tag="VIX",
    )
    q = (j.get("$VIX") or {}).get("quote") or {}
    vix = fnum(q.get("lastPrice")) or fnum(q.get("closePrice"))
    if vix is None:
        raise RuntimeError("BF_Q9 FAIL: cannot read VIX from Schwab")
    return vix


def fetch_spx_chain(c, target_exp: date) -> dict:
    return schwab_get_json(
        c, "https://api.schwabapi.com/marketdata/v1/chains",
        params={
            "symbol": "$SPX",
            "contractType": "ALL",
            "fromDate": target_exp.isoformat(),
            "toDate": target_exp.isoformat(),
            "strikeCount": 80,
        }, tag="CHAIN",
    )


def parse_chain(raw: dict, target_exp: date) -> Dict[str, Any]:
    underlying = raw.get("underlying") or {}
    spot = (
        fnum(underlying.get("last"))
        or fnum(underlying.get("close"))
        or fnum(underlying.get("mark"))
        or fnum(raw.get("underlyingPrice"))
    )
    if not spot or spot <= 0:
        raise RuntimeError("BF_Q9 FAIL: no underlying spot in chain")

    call_map = raw.get("callExpDateMap") or {}
    exp_key = None
    for k in call_map:
        if k.startswith(target_exp.isoformat()):
            exp_key = k
            break
    if not exp_key:
        raise RuntimeError(f"BF_Q9 FAIL: no chain for exp={target_exp}")

    strikes_raw = call_map[exp_key]
    strikes = {}
    call_deltas: Dict[int, float] = {}
    for sk, contracts in strikes_raw.items():
        strike_val = fnum(sk)
        if strike_val is None or not contracts:
            continue
        c0 = contracts[0] if isinstance(contracts, list) else contracts
        bid = fnum(c0.get("bid")) or 0.0
        ask = fnum(c0.get("ask")) or 0.0
        mid = (bid + ask) / 2.0 if (bid > 0 and ask > 0) else 0.0
        strike_i = int(round(strike_val))
        strikes[strike_i] = {"bid": bid, "ask": ask, "mid": mid}
        d = fnum(c0.get("delta"))
        if d is not None:
            call_deltas[strike_i] = d

    put_deltas: Dict[int, float] = {}
    put_map = raw.get("putExpDateMap") or {}
    exp_key_put = None
    for k in put_map:
        if k.startswith(target_exp.isoformat()):
            exp_key_put = k
            break
    if exp_key_put:
        for sk, contracts in (put_map.get(exp_key_put) or {}).items():
            strike_val = fnum(sk)
            if strike_val is None or not contracts:
                continue
            c0 = contracts[0] if isinstance(contracts, list) else contracts
            strike_i = int(round(strike_val))
            d = fnum(c0.get("delta"))
            if d is not None:
                put_deltas[strike_i] = d

    if not strikes:
        raise RuntimeError("BF_Q9 FAIL: no strikes parsed from chain")

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
        "call_deltas": call_deltas,
        "put_deltas": put_deltas,
    }


def estimate_butterfly_quote_from_chain(
    chain: Dict[str, Any],
    lower: int,
    center: int,
    upper: int,
) -> Tuple[float, float, float]:
    """
    Estimate butterfly (1:-2:1) quote from parsed chain marks.
    Returns (bid, ask, mid), each >= 0.0. Values may be 0 if unavailable.
    """
    strikes = chain.get("strikes", {}) or {}
    lq = strikes.get(int(lower), {}) or {}
    cq = strikes.get(int(center), {}) or {}
    uq = strikes.get(int(upper), {}) or {}

    lb = fnum(lq.get("bid"))
    la = fnum(lq.get("ask"))
    cb = fnum(cq.get("bid"))
    ca = fnum(cq.get("ask"))
    ub = fnum(uq.get("bid"))
    ua = fnum(uq.get("ask"))

    bid = 0.0
    ask = 0.0
    if None not in (lb, la, cb, ca, ub, ua) and min(lb, la, cb, ca, ub, ua) > 0:
        bid = max(0.0, _round_price_tick(lb + ub - 2.0 * ca))
        ask = max(bid, _round_price_tick(la + ua - 2.0 * cb))

    lm = _leg_mid_from_chain(lq)
    cm = _leg_mid_from_chain(cq)
    um = _leg_mid_from_chain(uq)
    mid = max(0.0, _round_price_tick(lm + um - 2.0 * cm)) if min(lm, cm, um) > 0 else 0.0

    if mid <= 0 and bid > 0 and ask > 0:
        mid = _round_price_tick((bid + ask) / 2.0)
    if bid <= 0 and ask > 0 and mid > 0:
        bid = max(0.0, _round_price_tick(min(mid, ask - BF_PRICE_TICK)))
    if ask <= 0 and mid > 0:
        ask = max(mid, _round_price_tick(mid + BF_PRICE_TICK))

    if ask < bid:
        ask = bid
    return (float(bid), float(ask), float(mid))


# ---------- main ----------

def main():
    # --- Schwab init + equity-based sizing ---
    try:
        c = schwab_client()
        oc_val, oc_src, acct_num = opening_cash_for_account(c)
        acct_hash = get_account_hash(c)
    except Exception as e:
        print(f"BF_Q9 SKIP: Schwab init failed: {e}")
        return 1

    ov_raw = (os.environ.get("SIZING_DOLLARS_OVERRIDE", "") or "").strip()
    if ov_raw:
        try:
            oc_val = float(ov_raw)
            oc_src = "SIZING_DOLLARS_OVERRIDE"
            print(f"BF_Q9 INFO: using SIZING_DOLLARS_OVERRIDE={oc_val}")
        except Exception:
            print("BF_Q9 WARN: bad SIZING_DOLLARS_OVERRIDE, ignoring.")

    dry_run = (os.environ.get("BF_DRY_RUN", "") or "").strip().lower() in ("1", "true", "yes", "y")

    print(f"BF_Q9 EQUITY: {oc_val} (src={oc_src}, acct={acct_num})")

    force_paper_only = False
    if oc_val is None or oc_val < BF_UNIT_DOLLARS:
        # Keep recording would-have-traded rows, but never send live orders.
        units = 1
        force_paper_only = True
        print(
            f"BF_Q9 PAPER_ONLY: equity {oc_val} below minimum ${BF_UNIT_DOLLARS:,} "
            f"— no live order, logging hypothetical trade"
        )
    else:
        units = int(oc_val // BF_UNIT_DOLLARS)

    effective_dry_run = dry_run or force_paper_only
    if effective_dry_run:
        print(f"BF_Q9 DRY_RUN effective={effective_dry_run} (user_dry_run={dry_run}, force_paper_only={force_paper_only})")

    print(f"BF_Q9 UNITS: {units} (BF_UNIT_DOLLARS={BF_UNIT_DOLLARS}, equity={oc_val})")

    today = date.today()
    cadence_mode = "STATIC"
    cadence_tier = "STATIC_BF_ENTRY_WEEKDAYS"
    cadence_dd = 0.0

    if BF_USE_ADAPTIVE_CADENCE:
        # Persisted peak equity keeps drawdown tiers stable across runs/days.
        peak_equity_prev = _load_cadence_peak_equity(BF_CADENCE_STATE_PATH)
        peak_equity_now = max(float(oc_val or 0.0), peak_equity_prev)
        if not ov_raw:
            _save_cadence_peak_equity(BF_CADENCE_STATE_PATH, peak_equity_now)

        allowed_days, cadence_tier, cadence_dd = cadence_fast_v1_allowed_weekdays(
            float(oc_val or 0.0),
            peak_equity_now,
        )
        cadence_mode = "FAST_V1_ADAPTIVE"
        allowed_names = _weekday_names(allowed_days)
        print(
            "BF_Q9 CADENCE: "
            f"mode={cadence_mode} tier={cadence_tier} "
            f"equity={float(oc_val or 0.0):.2f} peak={peak_equity_now:.2f} "
            f"dd={cadence_dd*100:.2f}% allowed={allowed_names}"
        )
    else:
        allowed_days = set(ALLOWED_ENTRY_WEEKDAYS)
        allowed_names = _weekday_names(allowed_days)
        print(
            f"BF_Q9 CADENCE: mode={cadence_mode} "
            f"allowed={allowed_names}"
        )

    if today.weekday() not in allowed_days:
        print(
            f"BF_Q9 SKIP: weekday gate today={today} weekday={today.weekday()} "
            f"allowed={allowed_names} tier={cadence_tier}"
        )
        return 0

    # --- Fetch VIX1D from Leo's GW API ---
    try:
        api = gw_fetch()
        vix1d = extract_vix1d(api)
    except Exception as e:
        print(f"BF_Q9 SKIP: GW fetch failed: {e}")
        return 0

    if vix1d is None:
        print("BF_Q9 SKIP: VixOne not found in GW response")
        return 0

    bucket, action = classify_vix1d(vix1d)
    print(f"BF_Q9 VIX1D: {vix1d:.4f} -> bucket={bucket} action={action}")

    if action == "SKIP":
        print(f"BF_Q9 SKIP: Q9 bucket {bucket} -> SKIP")
        return 0

    # --- Fetch VIX from Schwab (informational, no cap) ---
    try:
        vix = fetch_vix(c)
    except Exception as e:
        print(f"BF_Q9 WARN: VIX fetch failed (non-fatal): {e}")
        vix = 0.0

    print(f"BF_Q9 VIX: {vix:.2f}")

    # --- Compute target expiration (5 business days from today) ---
    target_exp = _add_business_days(today, DTE_BUSINESS_DAYS)
    print(f"BF_Q9 EXPIRATION: today={today} target_exp={target_exp} (DTE={DTE_BUSINESS_DAYS})")

    # --- Fetch 5DTE chain from Schwab ---
    try:
        raw_chain = fetch_spx_chain(c, target_exp)
        chain = parse_chain(raw_chain, target_exp)
    except Exception as e:
        print(f"BF_Q9 SKIP: chain fetch/parse failed: {e}")
        return 0

    spot = chain["spot"]
    atm_strike = chain["atm_strike"]
    em = chain["em"]
    atm_call_mid = chain["atm_call_mid"]
    print(f"BF_Q9 CHAIN: spot={spot:.2f} ATM={atm_strike} EM={em:.2f} ATM_call_mid={atm_call_mid:.2f}")

    if em <= 0:
        print("BF_Q9 SKIP: EM <= 0")
        return 0

    # --- Compute strikes ---
    mult = SELL_EM_MULT if action == "SELL" else BUY_EM_MULT
    center = atm_strike
    lower = None
    upper = None
    width = ""
    width_mode = "EM_WIDTH"
    put_delta_target = ""
    call_delta_target = ""

    # Side-specific delta targeting.
    # New rule: put side anchors width; call side mirrors put width.
    put_tgt, call_tgt, profile_tag = side_delta_targets(action)
    put_delta_target = f"{put_tgt:.2f}"
    call_delta_target = f"{call_tgt:.2f}"

    lower_cand = nearest_delta_strike(
        chain.get("put_deltas", {}),
        put_tgt,
        center,
        side="PUT",
    )
    # Keep call delta lookup for diagnostics only; strike selection is put-anchored.
    upper_cand = nearest_delta_strike(
        chain.get("call_deltas", {}),
        call_tgt,
        center,
        side="CALL",
    )

    if lower_cand is not None and lower_cand < center:
        put_width = int(center - lower_cand)
        upper_sym = int(center + put_width)

        # Ensure symmetric upper strike exists in parsed call chain.
        if upper_sym in chain.get("strikes", {}):
            lower = int(lower_cand)
            upper = upper_sym
            width = str(put_width)
            width_mode = f"{profile_tag}_PUT_ANCHORED"
            diag_call = f"{upper_cand}" if upper_cand is not None else "n/a"
            print(
                f"BF_Q9 DELTA_PROFILE: {width_mode} "
                f"(put_target={put_tgt:.2f}, call_target={call_tgt:.2f}, call_diag={diag_call}) "
                f"-> lower={lower} upper={upper} width={width}"
            )
        else:
            print(
                "BF_Q9 DELTA_PROFILE WARN: symmetric upper strike missing in chain "
                f"(center={center}, put_width={put_width}, upper_sym={upper_sym}) "
                "-- fallback to EM width."
            )
    if lower is None or upper is None:
        print(
            "BF_Q9 DELTA_PROFILE WARN: could not resolve delta strikes "
            f"(profile={profile_tag}, put_target={put_tgt:.2f}, call_target={call_tgt:.2f}) "
            "-- fallback to EM width."
        )
        width_pts = compute_width(em, mult)
        lower = center - width_pts
        upper = center + width_pts
        width = str(width_pts)

    lower_osi = build_osi("SPXW", target_exp, "C", lower)
    center_osi = build_osi("SPXW", target_exp, "C", center)
    upper_osi = build_osi("SPXW", target_exp, "C", upper)

    direction = action  # "SELL" or "BUY"

    print(f"BF_Q9 BUTTERFLY: action={action} width={width} mode={width_mode} mult={mult:.2f}")
    print(f"  lower={lower} ({lower_osi})")
    print(f"  center={center} ({center_osi}) x2")
    print(f"  upper={upper} ({upper_osi})")
    print(f"  direction={direction} qty={units}")

    chain_bid, chain_ask, chain_mid = estimate_butterfly_quote_from_chain(
        chain,
        lower,
        center,
        upper,
    )
    print(
        "BF_Q9 CHAIN_BFLY_QUOTE: "
        f"bid={chain_bid:.2f} ask={chain_ask:.2f} mid={chain_mid:.2f}"
    )

    bf = {
        "lower_osi": lower_osi,
        "center_osi": center_osi,
        "upper_osi": upper_osi,
        "target_qty": units,
    }

    # --- TOPUP + GUARD ---
    # If we're below minimum equity and paper-only, always log the signal
    # regardless of currently open account positions.
    apply_position_controls = not force_paper_only
    need_positions = apply_position_controls and (BF_GUARD_NO_CLOSE or BF_TOPUP)
    pos = None
    if need_positions:
        try:
            pos = positions_map(c, acct_hash)
            print(
                f"BF_Q9 POSITIONS: loaded count={len(pos)} "
                f"(guard={'on' if BF_GUARD_NO_CLOSE else 'off'}, "
                f"topup={'on' if BF_TOPUP else 'off'})"
            )
        except Exception as e:
            msg = str(e)[:220]
            guard_ok = (not BF_GUARD_NO_CLOSE) or (BF_GUARD_FAIL_ACTION == "CONTINUE")
            topup_ok = (not BF_TOPUP) or (BF_TOPUP_FAIL_ACTION == "CONTINUE")
            if guard_ok and topup_ok:
                print(f"BF_Q9 POSITIONS WARN: fetch failed ({msg}) — continuing WITHOUT positions.")
                pos = None
            else:
                print(f"BF_Q9 POSITIONS SKIP: fetch failed ({msg}) — skipping.")
                return 0

    send_qty = units

    if apply_position_controls and BF_TOPUP:
        if pos is None:
            if BF_TOPUP_FAIL_ACTION != "CONTINUE":
                print("BF_Q9 SKIP: TOPUP enabled but positions unavailable")
                return 0
        else:
            open_qty = open_butterflies_count(bf, pos)
            send_qty = max(0, units - open_qty)
            print(f"BF_Q9 TOPUP: target={units} open={open_qty} rem={send_qty}")

    if send_qty <= 0:
        print("BF_Q9 SKIP: AT_OR_ABOVE_TARGET")
        return 0

    if apply_position_controls and BF_GUARD_NO_CLOSE:
        if pos is None:
            if BF_GUARD_FAIL_ACTION != "CONTINUE":
                print("BF_Q9 SKIP: GUARD enabled but positions unavailable")
                return 0
        else:
            if would_close_guard_bf(bf, pos):
                print("BF_Q9 GUARD_SKIP: would close existing position")
                return 0

    # --- Delegate to placer (shared with Q17) ---
    env = dict(os.environ)
    env.update({
        "BF_DIRECTION":      direction,
        "BF_LOWER_OSI":      lower_osi,
        "BF_CENTER_OSI":     center_osi,
        "BF_UPPER_OSI":      upper_osi,
        "BF_QTY":            str(send_qty),
        "BF_WIDTH":          str(width),
        "BF_WIDTH_MODE":     width_mode,
        "BF_LOWER_STRIKE":   str(lower),
        "BF_UPPER_STRIKE":   str(upper),
        "BF_PUT_DELTA_TGT":  put_delta_target,
        "BF_CALL_DELTA_TGT": call_delta_target,
        "BF_ATM_STRIKE":     str(center),
        "BF_EM":             f"{em:.2f}",
        "BF_EM_MULT":        f"{mult:.2f}",
        "BF_SPOT":           f"{spot:.2f}",
        "BF_VIX":            f"{vix:.2f}",
        "BF_VIX1D":          f"{vix1d:.4f}",
        "BF_BUCKET":         str(bucket),
        "BF_ACTION":         action,
        "BF_EXPIRATION":     target_exp.isoformat(),
        "BF_CHAIN_BID":      f"{chain_bid:.2f}" if chain_bid > 0 else "",
        "BF_CHAIN_ASK":      f"{chain_ask:.2f}" if chain_ask > 0 else "",
        "BF_CHAIN_MID":      f"{chain_mid:.2f}" if chain_mid > 0 else "",
        "BF_UNIT_DOLLARS_V": str(BF_UNIT_DOLLARS),
        "BF_EQUITY":         str(oc_val or 0),
        "BF_UNITS":          str(units),
        "BF_CADENCE_MODE":   cadence_mode,
        "BF_CADENCE_TIER":   cadence_tier,
        "BF_CADENCE_DD_PCT": f"{cadence_dd*100:.4f}",

        # needed by placer
        "SCHWAB_APP_KEY":    os.environ["SCHWAB_APP_KEY"],
        "SCHWAB_APP_SECRET": os.environ["SCHWAB_APP_SECRET"],
        "SCHWAB_TOKEN_JSON": os.environ["SCHWAB_TOKEN_JSON"],
        "SCHWAB_ACCT_HASH":  acct_hash,
        "BF_LOG_PATH":       BF_LOG_PATH,
        "BF_DRY_RUN":        "true" if effective_dry_run else "false",
        "BF_DRY_RUN_REASON": "DRY_RUN_EQUITY_LT_30K" if force_paper_only else "DRY_RUN",
    })

    print(f"BF_Q9 PLACING: {direction} {send_qty}x {width}w butterfly @ {center}")
    rc = subprocess.call(
        [sys.executable, "scripts/trade/ButterflyQ17/place_butterfly.py"], env=env,
    )
    if rc != 0:
        print(f"BF_Q9 PLACER: rc={rc}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
