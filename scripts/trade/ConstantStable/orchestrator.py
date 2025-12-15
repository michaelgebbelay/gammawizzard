#!/usr/bin/env python3
# CONSTANT STABLE — vertical orchestrator
#
# Features:
# - Fetches ConstantStable payload from GammaWizard.
# - Builds 5-wide SPX verticals around Limit / CLimit.
# - Direction:
#     LeftGo  < 0  → PUT_SHORT  (credit)
#     LeftGo  > 0  → PUT_LONG   (debit)
#     RightGo < 0  → CALL_SHORT (credit)
#     RightGo > 0  → CALL_LONG  (debit)
#
# - Sizing:
#     units = floor(account_value / CS_UNIT_DOLLARS) (min 1; if equity unavailable => 1 unit)
#     qty   = units * vix_mult
#
# - VIX bucket sizing (default 1/1/2/4/6):
#     CS_VOL_FIELD = VIX (or VixOne, etc if present in payload)
#     CS_VIX_BREAKS (or VIX_BREAKS) = "0.14,0.16,0.18,0.22"
#     CS_VIX_MULTS  (or VIX_MULTS)  = "1,1,2,4,6"
#
# - NO-CLOSE GUARD (IMPORTANT):
#     If the intended opening order would CLOSE an existing position in either leg, SKIP that leg.
#     (Adding to existing aligned positions is allowed.)
#
# Env:
#   SCHWAB_APP_KEY, SCHWAB_APP_SECRET, SCHWAB_TOKEN_JSON
#   GW_EMAIL, GW_PASSWORD (or GW_TOKEN)
#   GW_BASE, GW_ENDPOINT
#   CS_UNIT_DOLLARS, CS_LOG_PATH
#   CS_VOL_FIELD, CS_VIX_BREAKS, CS_VIX_MULTS
#   CS_GUARD_NO_CLOSE=true|false (default true)
#   CS_DRY_RUN=true|false (passed through to place.py)

import os
import sys
import re
import subprocess
import time
import random
from datetime import date
import requests
from schwab.auth import client_from_token_file

__version__ = "1.5.0"

# ---------- Config ----------
GW_BASE = os.environ.get("GW_BASE", "https://gandalf.gammawizard.com").rstrip("/")
GW_ENDPOINT = os.environ.get("GW_ENDPOINT", "/rapi/GetUltraPureConstantStable").lstrip("/")

CS_UNIT_DOLLARS = float(os.environ.get("CS_UNIT_DOLLARS", "10000"))
CS_LOG_PATH = os.environ.get("CS_LOG_PATH", "logs/constantstable_vertical_trades.csv")

CS_VOL_FIELD = (os.environ.get("CS_VOL_FIELD", os.environ.get("VOL_FIELD", "VIX")) or "VIX").strip()

VIX_BREAKS_STR = (os.environ.get("CS_VIX_BREAKS", os.environ.get("VIX_BREAKS", "0.14,0.16,0.18,0.22")) or "").strip()
VIX_MULTS_STR  = (os.environ.get("CS_VIX_MULTS",  os.environ.get("VIX_MULTS",  "1,1,2,4,6")) or "").strip()

CS_GUARD_NO_CLOSE = (os.environ.get("CS_GUARD_NO_CLOSE", "true") or "").strip().lower() in ("1", "true", "yes", "y", "on")
CS_DRY_RUN = (os.environ.get("CS_DRY_RUN", "false") or "").strip().lower() in ("1", "true", "yes", "y", "on")


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

def osi_canon(osi: str):
    # (exp6, put/call, strike8)
    return (osi[6:12], osi[12], osi[-8:])

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

def parse_csv_ints(s: str):
    out = []
    for part in (s or "").split(","):
        part = part.strip()
        if not part:
            continue
        out.append(int(float(part)))
    return out


# ---------- GammaWizard ----------
def _sanitize_token(t: str) -> str:
    t = (t or "").strip().strip('"').strip("'")
    return t.split(None, 1)[1] if t.lower().startswith("bearer ") else t

def gw_fetch():
    tok = _sanitize_token(os.environ.get("GW_TOKEN", "") or "")
    url = f"{GW_BASE}/{GW_ENDPOINT}"
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
            f"{GW_BASE}/goauth/authenticateFireUser",
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
        keys = ("Date", "TDate", "Limit", "CLimit", "LeftGo", "RightGo", "VIX", "VixOne", "VIX1")
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
    return client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")

def _sleep_for_429(r, attempt):
    ra = r.headers.get("Retry-After")
    if ra:
        try:
            return max(1.0, float(ra))
        except Exception:
            pass
    return min(10.0, 0.5 * (2 ** attempt)) + random.uniform(0.0, 0.25)

def schwab_get_json(c, url, params=None, tries=6, tag=""):
    last = ""
    for i in range(tries):
        try:
            r = c.session.get(url, params=(params or {}), timeout=20)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                time.sleep(_sleep_for_429(r, i))
                continue
            last = f"HTTP_{r.status_code}:{(r.text or '')[:160]}"
        except Exception as e:
            last = f"{type(e).__name__}:{str(e)}"
        time.sleep(0.5 * (2 ** i))
    raise RuntimeError(f"SCHWAB_GET_FAIL({tag}) {last}")

def get_account_numbers(c):
    r = c.get_account_numbers()
    r.raise_for_status()
    arr = r.json() or []
    if not arr:
        return ("", "")
    info = arr[0]
    acct_hash = str(info.get("hashValue") or info.get("hashvalue") or "")
    acct_num = str(info.get("accountNumber") or info.get("account_number") or "")
    return acct_hash, acct_num

def opening_cash_for_account(c, prefer_number=None):
    r = c.get_accounts()
    r.raise_for_status()
    data = r.json()
    arr = data if isinstance(data, list) else [data]

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
        return None, "none", acct_num

    init, curr = chosen
    keys = [
        "cashAvailableForTrading",
        "cashBalance",
        "availableFundsNonMarginableTrade",
        "buyingPowerNonMarginableTrade",
        "buyingPower",
        "optionBuyingPower",
        "liquidationValue",
    ]

    def pick(src):
        for k in keys:
            v = (src or {}).get(k)
            if isinstance(v, (int, float)):
                return float(v), k
        return None

    for src in (init, curr):
        got = pick(src)
        if got:
            return got[0], got[1], acct_num

    return None, "none", acct_num

def _osi_from_instrument(ins: dict):
    sym = (ins.get("symbol") or "")
    try:
        return to_osi(sym)
    except Exception:
        pass
    exp = ins.get("optionExpirationDate") or ins.get("expirationDate") or ""
    pc = (ins.get("putCall") or ins.get("type") or "").upper()
    strike = ins.get("strikePrice") or ins.get("strike")
    try:
        if exp and pc in ("CALL", "PUT") and strike is not None:
            ymd = date.fromisoformat(str(exp)[:10]).strftime("%y%m%d")
            cp = "C" if pc.startswith("C") else "P"
            mills = int(round(float(strike) * 1000))
            return "{:<6s}{}{}{:08d}".format("SPXW", ymd, cp, mills)
    except Exception:
        pass
    return None

def positions_map(c, acct_hash: str):
    url = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}"
    j = schwab_get_json(c, url, params={"fields": "positions"}, tag="POSITIONS")
    sa = j[0]["securitiesAccount"] if isinstance(j, list) else (j.get("securitiesAccount") or j)

    out = {}
    for p in (sa.get("positions") or []):
        ins = p.get("instrument", {}) or {}
        atype = (ins.get("assetType") or ins.get("type") or "").upper()
        if atype != "OPTION":
            continue
        osi = _osi_from_instrument(ins)
        if not osi:
            continue
        qty = float(p.get("longQuantity", 0)) - float(p.get("shortQuantity", 0))
        if abs(qty) < 1e-9:
            continue
        out[osi_canon(osi)] = out.get(osi_canon(osi), 0.0) + qty
    return out


# ---------- VIX bucketing ----------
def pick_field_case_insensitive(d: dict, desired: str):
    if not isinstance(d, dict):
        return (None, None)
    m = {str(k).lower(): k for k in d.keys()}
    dk = desired.strip().lower()
    if dk in m:
        k = m[dk]
        return d.get(k), str(k)
    return (None, None)

def normalize_vol_and_breaks(vol_val: float, breaks: list[float]):
    # Make vol and breaks comparable if one is percent-scale and the other is decimal.
    if vol_val is None:
        return (None, breaks)

    bmax = max(breaks) if breaks else 0.0
    v = float(vol_val)

    # If vol looks like 15.7 and breaks look like 0.14 => convert vol to 0.157
    if v > 1.0 and bmax <= 1.0:
        v = v / 100.0

    # If vol looks like 0.157 and breaks look like 14/16/18 => convert breaks to decimal
    if v <= 1.0 and bmax > 1.0:
        breaks = [b / 100.0 for b in breaks]

    return (v, breaks)

def bucket_and_mult(vol_value: float, breaks: list[float], mults: list[int]):
    if vol_value is None:
        return (1, mults[0] if mults else 1)

    bucket = 1
    for b in breaks:
        if vol_value >= b:
            bucket += 1
    if not mults:
        return (bucket, 1)
    idx = min(bucket - 1, len(mults) - 1)
    return (bucket, int(mults[idx]))


# ---------- Guard ----------
def would_close_existing(pos_map: dict, long_osi: str, short_osi: str) -> tuple[bool, float, float]:
    """
    Our vertical order is always:
      BUY_TO_OPEN  long_osi  (desired +qty)
      SELL_TO_OPEN short_osi (desired -qty)

    If we are currently short the buy-leg OR long the sell-leg, then an order would
    reduce/flip existing position → treat as WOULD_CLOSE and SKIP.
    """
    cur_long = float(pos_map.get(osi_canon(long_osi), 0.0))
    cur_short = float(pos_map.get(osi_canon(short_osi), 0.0))

    # BUY_TO_OPEN would close if currently short
    if cur_long < -1e-9:
        return (True, cur_long, cur_short)

    # SELL_TO_OPEN would close if currently long
    if cur_short > 1e-9:
        return (True, cur_long, cur_short)

    return (False, cur_long, cur_short)


# ---------- main ----------
def main():
    # --- Schwab init + equity ---
    try:
        c = schwab_client()
        acct_hash, acct_num = get_account_numbers(c)
        oc_val, oc_src, acct_num2 = opening_cash_for_account(c, prefer_number=acct_num)
        acct_num = acct_num2 or acct_num
    except Exception as e:
        print(f"CS_VERT_RUN SKIP: Schwab init failed: {e}")
        return 0

    # Optional manual override for sizing dollars
    ov_raw = (os.environ.get("SIZING_DOLLARS_OVERRIDE", "") or "").strip()
    if ov_raw:
        try:
            oc_val = float(ov_raw)
            oc_src = "SIZING_DOLLARS_OVERRIDE"
            print(f"CS_VERT_RUN INFO: using SIZING_DOLLARS_OVERRIDE={oc_val}")
        except Exception:
            print("CS_VERT_RUN WARN: bad SIZING_DOLLARS_OVERRIDE, ignoring override.")

    print(f"CS_VERT_RUN EQUITY_RAW: {oc_val} (src={oc_src}, acct={acct_num})")

    if oc_val is None or oc_val <= 0:
        print("CS_VERT_RUN WARN: equity unavailable/<=0 — defaulting to CS_UNIT_DOLLARS for sizing")
        oc_val = CS_UNIT_DOLLARS
        units = 1
    else:
        if CS_UNIT_DOLLARS <= 0:
            print("CS_VERT_RUN FATAL: CS_UNIT_DOLLARS must be > 0")
            return 1
        units = max(1, int(float(oc_val) // float(CS_UNIT_DOLLARS)))

    print(f"CS_VERT_RUN UNITS: {units} (CS_UNIT_DOLLARS={CS_UNIT_DOLLARS}, oc_val={oc_val})")

    # --- Fetch GW trade ---
    try:
        api = gw_fetch()
        tr = extract_trade(api)
    except Exception as e:
        print(f"CS_VERT_RUN SKIP: GW fetch failed: {e}")
        return 0

    if not tr:
        print("CS_VERT_RUN SKIP: NO_TRADE_PAYLOAD")
        return 0

    # --- Vol bucketing ---
    breaks = parse_csv_floats(VIX_BREAKS_STR)
    mults = parse_csv_ints(VIX_MULTS_STR)

    raw_vol, used_key = pick_field_case_insensitive(tr, CS_VOL_FIELD)
    vol_value = fnum(raw_vol)
    vol_value, breaks = normalize_vol_and_breaks(vol_value, breaks)

    bucket, vix_mult = bucket_and_mult(vol_value, breaks, mults)
    used_key = used_key or CS_VOL_FIELD

    print(f"CS_VERT_RUN VOL: field={CS_VOL_FIELD} used={used_key} value={vol_value} bucket={bucket} mult={vix_mult}")
    print(f"CS_VERT_RUN VIX_BREAKS={','.join(str(x) for x in breaks)} VIX_MULTS={VIX_MULTS_STR}")

    base_qty = int(units) * int(vix_mult)

    # Allow mult=0 to skip trades in low-vol regimes (optional behavior)
    if base_qty <= 0:
        print(f"CS_VERT_RUN SKIP: base_qty={base_qty} (units={units} vix_mult={vix_mult})")
        return 0

    # --- Positions map for no-close guard ---
    pos_map = None
    if CS_GUARD_NO_CLOSE:
        try:
            pos_map = positions_map(c, acct_hash)
        except Exception as e:
            # Fail closed: if we can't verify we won't close, we skip.
            print(f"CS_VERT_RUN SKIP: guard positions fetch failed: {e}")
            return 0

    # --- Build strikes + OSIs ---
    trade_date = str(tr.get("Date", ""))
    tdate_iso = str(tr.get("TDate", ""))

    exp6 = yymmdd(tdate_iso)
    inner_put = int(float(tr.get("Limit")))
    inner_call = int(float(tr.get("CLimit")))
    width = 5

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

    # keep as informational only (not used for sizing now)
    put_strength = left_imp if left_imp is not None else (abs(left_go) if left_go is not None else 0.0)
    call_strength = right_imp if right_imp is not None else (abs(right_go) if right_go is not None else 0.0)

    print(f"CS_VERT_RUN TRADE: Date={trade_date} TDate={tdate_iso}")
    print(f"  PUT strikes : {p_low} / {p_high}  OSI=({put_low_osi},{put_high_osi})  LeftGo={left_go} LImp={left_imp}")
    print(f"  CALL strikes: {c_low} / {c_high}  OSI=({call_low_osi},{call_high_osi})  RightGo={right_go} RImp={right_imp}")
    print(f"CS_VERT_RUN RAW_STRENGTH: put_strength={put_strength:.3f} call_strength={call_strength:.3f}")

    verts = []

    # PUT
    if left_go is not None and left_go != 0.0:
        if left_go < 0:
            # short put vertical (credit): sell higher strike, buy lower strike
            verts.append({
                "name": "PUT_SHORT",
                "kind": "PUT",
                "side": "CREDIT",
                "direction": "SHORT",
                "short_osi": put_high_osi,
                "long_osi": put_low_osi,
                "go": left_go,
                "strength": put_strength,
            })
        else:
            # long put vertical (debit): buy higher strike, sell lower strike
            verts.append({
                "name": "PUT_LONG",
                "kind": "PUT",
                "side": "DEBIT",
                "direction": "LONG",
                "short_osi": put_low_osi,
                "long_osi": put_high_osi,
                "go": left_go,
                "strength": put_strength,
            })

    # CALL
    if right_go is not None and right_go != 0.0:
        if right_go < 0:
            # short call vertical (credit): sell lower strike, buy higher strike
            verts.append({
                "name": "CALL_SHORT",
                "kind": "CALL",
                "side": "CREDIT",
                "direction": "SHORT",
                "short_osi": call_low_osi,
                "long_osi": call_high_osi,
                "go": right_go,
                "strength": call_strength,
            })
        else:
            # long call vertical (debit): buy lower strike, sell higher strike
            verts.append({
                "name": "CALL_LONG",
                "kind": "CALL",
                "side": "DEBIT",
                "direction": "LONG",
                "short_osi": call_high_osi,
                "long_osi": call_low_osi,
                "go": right_go,
                "strength": call_strength,
            })

    if not verts:
        print("CS_VERT_RUN SKIP: no nonzero LeftGo/RightGo — no verticals to trade.")
        return 0

    # --- Place each vertical (with guard) ---
    for v in verts:
        qty = int(base_qty)

        # Guard: skip if this would close an existing position
        if CS_GUARD_NO_CLOSE and pos_map is not None:
            bad, cur_long, cur_short = would_close_existing(pos_map, v["long_osi"], v["short_osi"])
            if bad:
                print(
                    f"CS_VERT_RUN GUARD_SKIP {v['name']}: WOULD_CLOSE "
                    f"(buy_leg={v['long_osi']} pos={cur_long:+g}; sell_leg={v['short_osi']} pos={cur_short:+g})"
                )
                continue

        print(
            f"CS_VERT_RUN {v['name']}: side={v['side']} kind={v['kind']} "
            f"short={v['short_osi']} long={v['long_osi']} go={v['go']} "
            f"qty={qty} (units={units} vix_mult={vix_mult} bucket={bucket})"
        )

        env = dict(os.environ)
        env.update({
            "VERT_SIDE": v["side"],
            "VERT_KIND": v["kind"],
            "VERT_NAME": v["name"],
            "VERT_DIRECTION": v["direction"],
            "VERT_SHORT_OSI": v["short_osi"],
            "VERT_LONG_OSI": v["long_osi"],
            "VERT_QTY": str(qty),
            "VERT_GO": "" if v["go"] is None else str(v["go"]),
            "VERT_STRENGTH": f"{float(v['strength']):.3f}",
            "VERT_TRADE_DATE": trade_date,
            "VERT_TDATE": tdate_iso,
            "VERT_UNIT_DOLLARS": str(CS_UNIT_DOLLARS),
            "VERT_OC": str(oc_val),
            "VERT_UNITS": str(units),

            # useful context
            "VERT_VOL_FIELD": CS_VOL_FIELD,
            "VERT_VOL_VALUE": "" if vol_value is None else str(vol_value),
            "VERT_VOL_BUCKET": str(bucket),
            "VERT_VOL_MULT": str(vix_mult),

            "CS_LOG_PATH": CS_LOG_PATH,
            "SCHWAB_ACCT_HASH": acct_hash,
            "VERT_DRY_RUN": "true" if CS_DRY_RUN else "false",
        })

        rc = subprocess.call([sys.executable, "scripts/trade/ConstantStable/place.py"], env=env)
        if rc != 0:
            print(f"CS_VERT_RUN {v['name']}: placer rc={rc}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
