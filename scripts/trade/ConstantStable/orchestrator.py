#!/usr/bin/env python3
# CONSTANT STABLE — vertical orchestrator (with NO-CLOSE guard, TOPUP, and optional 4-leg bundling)
#
# What it does:
# - Fetches ConstantStable payload from GammaWizard (GetUltraPureConstantStable).
# - Builds 5-wide SPX verticals around Limit / CLimit.
# - PUT leg:
#     LeftGo < 0  -> short put vertical (credit)
#     LeftGo > 0  -> long  put vertical (debit)
# - CALL leg:
#     RightGo < 0 -> short call vertical (credit)
#     RightGo > 0 -> long  call vertical (debit)
#
# Sizing:
# - units = floor(account_value / CS_UNIT_DOLLARS)   (min 1)
# - Vol bucket multiplier from CS_VIX_MULTS (len = len(CS_VIX_BREAKS)+1).
# - target_qty = max(1, units * vix_mult)  (if vix_mult == 0 -> skip)
#
# TOPUP (default ON):
# - If you already have open spreads for today's strikes, only top up to target_qty.
#   (Avoids "adding more just because".)
#
# Guard (NO-CLOSE, default ON):
# - If an opening vertical would net/close an existing option position leg, SKIP that vertical.
#   (Adding to existing same-direction is OK.)
#
# 4-leg bundling (default ON):
# - If both PUT and CALL legs are eligible AND have the same send_qty, place as ONE 4-leg order.
# - If asymmetric, fallback to placing each vertical separately.
#
# Delegates placement + logging to scripts/trade/ConstantStable/place.py via VERT_* envs.

import os
import sys
import re
import time
import random
import subprocess
from datetime import date
from typing import Any, Dict, Tuple

import requests
from schwab.auth import client_from_token_file

__version__ = "2.3.0"

GW_BASE = os.environ.get("GW_BASE", "https://gandalf.gammawizard.com").rstrip("/")
GW_ENDPOINT = os.environ.get("GW_ENDPOINT", "rapi/GetUltraPureConstantStable").lstrip("/")

CS_UNIT_DOLLARS = float(os.environ.get("CS_UNIT_DOLLARS", "10000"))
CS_LOG_PATH = os.environ.get("CS_LOG_PATH", "logs/constantstable_vertical_trades.csv")

# Vol bucket config
CS_VOL_FIELD = (os.environ.get("CS_VOL_FIELD", "VixOne") or "VixOne").strip()  # default changed to VixOne
CS_VIX_BREAKS = os.environ.get("CS_VIX_BREAKS", "0.089,0.111,0.131,0.158,0.192,0.253")
CS_VIX_MULTS = os.environ.get("CS_VIX_MULTS", "1,1,1,2,3,4,6")

# --- NO-CLOSE guard config ---
CS_GUARD_NO_CLOSE = (os.environ.get("CS_GUARD_NO_CLOSE", "1") or "1").strip().lower() in ("1", "true", "yes", "y")
CS_GUARD_FAIL_ACTION = (os.environ.get("CS_GUARD_FAIL_ACTION", "SKIP_ALL") or "SKIP_ALL").strip().upper()
#   SKIP_ALL  -> safest: if we cannot load positions, skip everything
#   CONTINUE  -> proceed without guard (not recommended)

# --- TOPUP config ---
CS_TOPUP = (os.environ.get("CS_TOPUP", "1") or "1").strip().lower() in ("1", "true", "yes", "y")
CS_TOPUP_FAIL_ACTION = (os.environ.get("CS_TOPUP_FAIL_ACTION", "SKIP_ALL") or "SKIP_ALL").strip().upper()
#   SKIP_ALL  -> safest: if we cannot load positions, skip everything
#   CONTINUE  -> proceed without topup

# --- 4-leg bundle config ---
CS_BUNDLE_4LEG = (os.environ.get("CS_BUNDLE_4LEG", "1") or "1").strip().lower() in ("1", "true", "yes", "y")
CS_BUNDLE_REQUIRE_EQUAL_QTY = (os.environ.get("CS_BUNDLE_REQUIRE_EQUAL_QTY", "1") or "1").strip().lower() in ("1", "true", "yes", "y")


# ---------- Utility helpers ----------

def yymmdd(iso: str) -> str:
    d = date.fromisoformat((iso or "")[:10])
    return f"{d:%y%m%d}"


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


def parse_csv_floats(s: str):
    out = []
    for part in (s or "").split(","):
        part = part.strip()
        if not part:
            continue
        out.append(float(part))
    return out


def get_case_insensitive(d: Dict[str, Any], key: str):
    if key in d:
        return d.get(key)
    lk = key.lower()
    for k, v in d.items():
        if str(k).lower() == lk:
            return v
    return None


def pick_vol_value(tr: Dict[str, Any], vol_field: str) -> Tuple[str, float | None]:
    """
    Returns (field_used, value_or_None).
    vol_field supports:
      - "VIX" / "VixOne" / etc
      - "AUTO": try common candidates
    """
    if (vol_field or "").strip().upper() == "AUTO":
        candidates = ["VixOne", "VIXONE", "Vix1", "VIX1", "VIX", "Vix"]
    else:
        candidates = [vol_field]

    for k in candidates:
        v = get_case_insensitive(tr, k)
        fv = fnum(v)
        if fv is not None:
            return (k, fv)
    return (vol_field, None)


def vix_bucket_and_mult(vix_val: float | None, breaks_csv: str, mults_csv: str) -> Tuple[int, int]:
    """
    N buckets => (N-1) breaks and N multipliers.
    Example (7 buckets):
      breaks = 0.089,0.111,0.131,0.158,0.192,0.253
      mults  = 1,1,1,2,3,4,6
    Buckets are 1..N.
    """
    breaks = parse_csv_floats(breaks_csv)
    mults = parse_csv_floats(mults_csv)

    if len(breaks) < 1:
        raise ValueError("CS_VIX_BREAKS must contain at least 1 cutoff")
    if len(mults) != len(breaks) + 1:
        raise ValueError("CS_VIX_MULTS must have exactly len(CS_VIX_BREAKS)+1 values")

    if vix_val is None:
        mid = len(mults) // 2
        return (mid + 1, int(mults[mid]))

    for i, b in enumerate(breaks):
        if vix_val < b:
            return (i + 1, int(mults[i]))
    return (len(mults), int(mults[-1]))


# ---------- GammaWizard ----------

def _sanitize_token(t: str) -> str:
    t = (t or "").strip().strip('"').strip("'")
    return t.split(None, 1)[1] if t.lower().startswith("bearer ") else t


def gw_fetch():
    base = GW_BASE
    endpoint = GW_ENDPOINT
    tok = _sanitize_token(os.environ.get("GW_TOKEN", "") or "")

    url = f"{base}/{endpoint}"
    print("CS_VERT_RUN GW URL:", url)

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


def extract_trade(j):
    if isinstance(j, dict):
        if "Trade" in j:
            tr = j["Trade"]
            return tr[-1] if isinstance(tr, list) and tr else tr if isinstance(tr, dict) else {}
        keys = ("Date", "TDate", "Limit", "CLimit", "LeftGo", "RightGo", "LImp", "RImp", "VIX", "VixOne")
        if any(k in j for k in keys):
            return j
        for v in j.values():
            if isinstance(v, (dict, list)):
                t = extract_trade(v)
                if t:
                    return t
    if isinstance(j, list):
        for it in reversed(j):
            t = extract_trade(it)
            if t:
                return t
    return {}


# ---------- Schwab helpers ----------

def schwab_client():
    app_key = os.environ["SCHWAB_APP_KEY"]
    app_secret = os.environ["SCHWAB_APP_SECRET"]
    token_json = os.environ["SCHWAB_TOKEN_JSON"]
    with open("schwab_token.json", "w") as f:
        f.write(token_json)
    c = client_from_token_file(
        api_key=app_key,
        app_secret=app_secret,
        token_path="schwab_token.json",
    )
    return c


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


def open_spreads_for_vertical(v: Dict[str, Any], pos: Dict[Tuple[str, str, str], float]) -> int:
    """
    Computes open spread count for a vertical from option leg net positions.
    For a properly opened spread:
      BUY leg should be > 0
      SELL leg should be < 0
    Spread count ~= min(buy_qty, abs(sell_qty))
    """
    buy_key = osi_canon(v["long_osi"])   # BUY_TO_OPEN
    sell_key = osi_canon(v["short_osi"]) # SELL_TO_OPEN

    buy_pos = float(pos.get(buy_key, 0.0))
    sell_pos = float(pos.get(sell_key, 0.0))

    b = buy_pos if buy_pos > 0 else 0.0
    s = (-sell_pos) if sell_pos < 0 else 0.0
    return int(min(b, s) + 1e-9)


def would_close_guard(v: Dict[str, Any], pos: Dict[Tuple[str, str, str], float]) -> bool:
    """
    Returns True if this opening order would net/close an existing position (unsafe).
    """
    buy_key = osi_canon(v["long_osi"])   # BUY_TO_OPEN
    sell_key = osi_canon(v["short_osi"]) # SELL_TO_OPEN

    buy_leg_pos = float(pos.get(buy_key, 0.0))
    sell_leg_pos = float(pos.get(sell_key, 0.0))

    print(
        f"CS_VERT_RUN GUARD_CHECK {v['name']}: "
        f"BUY_TO_OPEN {v['long_osi']} pos={buy_leg_pos:+g} ; "
        f"SELL_TO_OPEN {v['short_osi']} pos={sell_leg_pos:+g}"
    )

    if buy_leg_pos < -1e-9:
        return True
    if sell_leg_pos > 1e-9:
        return True
    return False


# ---------- main ----------

def main():
    # --- Schwab + equity (with override & fallback) ---
    try:
        c = schwab_client()
        oc_val, oc_src, acct_num = opening_cash_for_account(c)
        acct_hash = get_account_hash(c)
    except Exception as e:
        print(f"CS_VERT_RUN SKIP: Schwab init failed: {e}")
        return 0

    ov_raw = (os.environ.get("SIZING_DOLLARS_OVERRIDE", "") or "").strip()
    if ov_raw:
        try:
            oc_val = float(ov_raw)
            oc_src = "SIZING_DOLLARS_OVERRIDE"
            print(f"CS_VERT_RUN INFO: using SIZING_DOLLARS_OVERRIDE={oc_val}")
        except Exception:
            print("CS_VERT_RUN WARN: bad SIZING_DOLLARS_OVERRIDE, ignoring override.")

    print(f"CS_VERT_RUN EQUITY_RAW: {oc_val} (src={oc_src}, acct={acct_num})")

    if CS_UNIT_DOLLARS <= 0:
        print("CS_VERT_RUN FATAL: CS_UNIT_DOLLARS must be > 0")
        return 1

    if oc_val is None or oc_val <= 0:
        print("CS_VERT_RUN WARN: equity unavailable/<=0 — defaulting to CS_UNIT_DOLLARS for sizing")
        oc_val = CS_UNIT_DOLLARS
        units = 1
    else:
        units = max(1, int(oc_val // CS_UNIT_DOLLARS))

    print(f"CS_VERT_RUN UNITS: {units} (CS_UNIT_DOLLARS={CS_UNIT_DOLLARS}, oc_val={oc_val})")

    # --- ConstantStable payload from GammaWizard ---
    try:
        api = gw_fetch()
        tr = extract_trade(api)
    except Exception as e:
        print(f"CS_VERT_RUN SKIP: GW fetch failed: {e}")
        return 0

    if not tr:
        print("CS_VERT_RUN SKIP: NO_TRADE_PAYLOAD")
        return 0

    trade_date = str(tr.get("Date", ""))
    tdate_iso = str(tr.get("TDate", ""))

    exp6 = yymmdd(tdate_iso)
    inner_put = int(float(tr.get("Limit")))
    inner_call = int(float(tr.get("CLimit")))
    width = 5

    # strikes
    p_low = inner_put - width
    p_high = inner_put
    c_low = inner_call
    c_high = inner_call + width

    put_low_osi = to_osi(f".SPXW{exp6}P{p_low}")
    put_high_osi = to_osi(f".SPXW{exp6}P{p_high}")
    call_low_osi = to_osi(f".SPXW{exp6}C{c_low}")
    call_high_osi = to_osi(f".SPXW{exp6}C{c_high}")

    left_go = fnum(tr.get("LeftGo"))
    right_go = fnum(tr.get("RightGo"))
    left_imp = fnum(tr.get("LImp"))
    right_imp = fnum(tr.get("RImp"))

    put_strength = left_imp if left_imp is not None else (abs(left_go) if left_go is not None else 0.0)
    call_strength = right_imp if right_imp is not None else (abs(right_go) if right_go is not None else 0.0)

    # Vol bucket sizing
    field_used, vol_val = pick_vol_value(tr, CS_VOL_FIELD)
    bucket, vix_mult = vix_bucket_and_mult(vol_val, CS_VIX_BREAKS, CS_VIX_MULTS)
    print(f"CS_VERT_RUN VOL: field={CS_VOL_FIELD} used={field_used} value={vol_val} bucket={bucket} mult={vix_mult}")
    print(f"CS_VERT_RUN VIX_BREAKS={CS_VIX_BREAKS} VIX_MULTS={CS_VIX_MULTS}")

    print(f"CS_VERT_RUN TRADE: Date={trade_date} TDate={tdate_iso}")
    print(f"  PUT strikes : {p_low} / {p_high}  OSI=({put_low_osi},{put_high_osi})  LeftGo={left_go} LImp={left_imp}")
    print(f"  CALL strikes: {c_low} / {c_high}  OSI=({call_low_osi},{call_high_osi})  RightGo={right_go} RImp={right_imp}")
    print(f"CS_VERT_RUN RAW_STRENGTH: put_strength={put_strength:.3f} call_strength={call_strength:.3f}")

    # Build candidate verticals
    v_put = None
    v_call = None

    if left_go is not None and left_go != 0.0 and vix_mult != 0:
        target = max(1, units * int(vix_mult))
        if left_go < 0:
            v_put = {
                "name": "PUT_SHORT", "kind": "PUT", "side": "CREDIT", "direction": "SHORT",
                "short_osi": put_high_osi, "long_osi": put_low_osi,
                "go": left_go, "strength": put_strength, "target_qty": target,
            }
        else:
            v_put = {
                "name": "PUT_LONG", "kind": "PUT", "side": "DEBIT", "direction": "LONG",
                "short_osi": put_low_osi, "long_osi": put_high_osi,
                "go": left_go, "strength": put_strength, "target_qty": target,
            }

    if right_go is not None and right_go != 0.0 and vix_mult != 0:
        target = max(1, units * int(vix_mult))
        if right_go < 0:
            v_call = {
                "name": "CALL_SHORT", "kind": "CALL", "side": "CREDIT", "direction": "SHORT",
                "short_osi": call_low_osi, "long_osi": call_high_osi,
                "go": right_go, "strength": call_strength, "target_qty": target,
            }
        else:
            v_call = {
                "name": "CALL_LONG", "kind": "CALL", "side": "DEBIT", "direction": "LONG",
                "short_osi": call_high_osi, "long_osi": call_low_osi,
                "go": right_go, "strength": call_strength, "target_qty": target,
            }

    if not v_put and not v_call:
        print("CS_VERT_RUN SKIP: no verticals to trade (LeftGo/RightGo zero or vix_mult=0).")
        return 0

    # Load positions once if needed (guard and/or topup)
    need_positions = CS_GUARD_NO_CLOSE or CS_TOPUP
    pos = None
    if need_positions:
        try:
            pos = positions_map(c, acct_hash)
            print(
                f"CS_VERT_RUN POSITIONS: loaded count={len(pos)} "
                f"(guard={'on' if CS_GUARD_NO_CLOSE else 'off'}, topup={'on' if CS_TOPUP else 'off'})"
            )
        except Exception as e:
            msg = str(e)[:220]
            # decide fail action
            guard_ok = (not CS_GUARD_NO_CLOSE) or (CS_GUARD_FAIL_ACTION == "CONTINUE")
            topup_ok = (not CS_TOPUP) or (CS_TOPUP_FAIL_ACTION == "CONTINUE")
            if guard_ok and topup_ok:
                print(f"CS_VERT_RUN POSITIONS WARN: fetch failed ({msg}) — continuing WITHOUT positions (guard/topup degraded).")
                pos = None
            else:
                print(f"CS_VERT_RUN POSITIONS SKIP: fetch failed ({msg}) — skipping ALL trades.")
                return 0

    # Apply TOPUP + GUARD to determine send_qty
    def finalize(v: Dict[str, Any]) -> Dict[str, Any] | None:
        if not v:
            return None

        target_qty = int(v["target_qty"])
        open_qty = 0

        if CS_TOPUP:
            if pos is None:
                # If topup is enabled but we couldn't load positions, safest is to skip
                if CS_TOPUP_FAIL_ACTION != "CONTINUE":
                    print(f"CS_VERT_RUN SKIP {v['name']}: TOPUP_NEEDS_POSITIONS")
                    return None
            else:
                open_qty = open_spreads_for_vertical(v, pos)

        rem = max(0, target_qty - open_qty) if CS_TOPUP else target_qty
        if CS_TOPUP:
            print(f"CS_VERT_RUN TOPUP {v['name']}: target={target_qty} open={open_qty} rem={rem}")

        if rem <= 0:
            print(f"CS_VERT_RUN SKIP {v['name']}: AT_OR_ABOVE_TARGET")
            return None

        # Guard check
        if CS_GUARD_NO_CLOSE:
            if pos is None:
                if CS_GUARD_FAIL_ACTION != "CONTINUE":
                    print(f"CS_VERT_RUN SKIP {v['name']}: GUARD_NEEDS_POSITIONS")
                    return None
            else:
                if would_close_guard(v, pos):
                    print(f"CS_VERT_RUN GUARD_SKIP {v['name']}: WOULD_CLOSE")
                    return None

        v2 = dict(v)
        v2["send_qty"] = int(rem)
        return v2

    v_put_f = finalize(v_put) if v_put else None
    v_call_f = finalize(v_call) if v_call else None

    if not v_put_f and not v_call_f:
        print("CS_VERT_RUN SKIP: nothing to place after TOPUP/GUARD.")
        return 0

    qty_rule = "VIX_BUCKET_TOPUP" if CS_TOPUP else "VIX_BUCKET"

    def env_for_vertical(v: Dict[str, Any]) -> Dict[str, str]:
        strength_s = f"{float(v['strength']):.3f}"
        e = dict(os.environ)
        e.update({
            "VERT_SIDE":         v["side"],
            "VERT_KIND":         v["kind"],
            "VERT_NAME":         v["name"],
            "VERT_DIRECTION":    v["direction"],
            "VERT_SHORT_OSI":    v["short_osi"],
            "VERT_LONG_OSI":     v["long_osi"],
            "VERT_QTY":          str(v["send_qty"]),
            "VERT_GO":           "" if v.get("go") is None else str(v["go"]),
            "VERT_STRENGTH":     strength_s,
            "VERT_TRADE_DATE":   trade_date,
            "VERT_TDATE":        tdate_iso,

            # sizing context
            "VERT_UNIT_DOLLARS": str(CS_UNIT_DOLLARS),
            "VERT_OC":           str(oc_val),
            "VERT_UNITS":        str(units),

            # vol context
            "VERT_VOL_FIELD":    CS_VOL_FIELD,
            "VERT_VOL_USED":     field_used,
            "VERT_VOL_VALUE":    "" if vol_val is None else str(vol_val),
            "VERT_VOL_BUCKET":   str(bucket),
            "VERT_VOL_MULT":     str(vix_mult),
            "VERT_QTY_RULE":     qty_rule,

            # needed by placer
            "SCHWAB_APP_KEY":     os.environ["SCHWAB_APP_KEY"],
            "SCHWAB_APP_SECRET":  os.environ["SCHWAB_APP_SECRET"],
            "SCHWAB_TOKEN_JSON":  os.environ["SCHWAB_TOKEN_JSON"],
            "SCHWAB_ACCT_HASH":   acct_hash,
            "CS_LOG_PATH":        CS_LOG_PATH,
        })
        return e

    # ----- Place bundled 4-leg order if possible -----
    if CS_BUNDLE_4LEG and v_put_f and v_call_f:
        q_put = int(v_put_f["send_qty"])
        q_call = int(v_call_f["send_qty"])

        can_bundle = (q_put == q_call) if CS_BUNDLE_REQUIRE_EQUAL_QTY else True
        if can_bundle and q_put > 0 and q_call > 0:
            q = min(q_put, q_call) if not CS_BUNDLE_REQUIRE_EQUAL_QTY else q_put

            print(
                f"CS_VERT_RUN BUNDLE4: qty={q} "
                f"PUT={v_put_f['name']}({v_put_f['short_osi']}|{v_put_f['long_osi']}) "
                f"CALL={v_call_f['name']}({v_call_f['short_osi']}|{v_call_f['long_osi']})"
            )

            env = env_for_vertical({**v_put_f, "send_qty": q})
            # add second vertical into env
            env.update({
                "VERT_BUNDLE": "true",
                "VERT2_SIDE":       v_call_f["side"],
                "VERT2_KIND":       v_call_f["kind"],
                "VERT2_NAME":       v_call_f["name"],
                "VERT2_DIRECTION":  v_call_f["direction"],
                "VERT2_SHORT_OSI":  v_call_f["short_osi"],
                "VERT2_LONG_OSI":   v_call_f["long_osi"],
                "VERT2_QTY":        str(q),
                "VERT2_GO":         "" if v_call_f.get("go") is None else str(v_call_f["go"]),
                "VERT2_STRENGTH":   f"{float(v_call_f['strength']):.3f}",
            })

            rc = subprocess.call([sys.executable, "scripts/trade/ConstantStable/place.py"], env=env)
            if rc != 0:
                print(f"CS_VERT_RUN BUNDLE4: placer rc={rc}")
            return 0  # done (we deliberately do not chase leftovers in asymmetric scenarios)

        print(f"CS_VERT_RUN BUNDLE4 SKIP: qty mismatch (put={q_put} call={q_call}) — placing separately")

    # ----- Fallback: place separately -----
    for v in (v_put_f, v_call_f):
        if not v:
            continue
        print(
            f"CS_VERT_RUN {v['name']}: side={v['side']} kind={v['kind']} "
            f"short={v['short_osi']} long={v['long_osi']} "
            f"go={v['go']} target_qty={v['target_qty']} send_qty={v['send_qty']} "
            f"(units={units} vix_mult={vix_mult} bucket={bucket})"
        )
        env = env_for_vertical(v)
        rc = subprocess.call([sys.executable, "scripts/trade/ConstantStable/place.py"], env=env)
        if rc != 0:
            print(f"CS_VERT_RUN {v['name']}: placer rc={rc}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
