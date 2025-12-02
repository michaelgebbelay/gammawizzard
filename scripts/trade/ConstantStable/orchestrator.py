#!/usr/bin/env python3
# CONSTANT STABLE — vertical orchestrator (10k sizing)
#
# - Fetches ConstantStable payload from GammaWizard (UltraPureConstantStable).
# - Builds 5-wide SPX verticals around Limit / CLimit.
# - Per side:
#     LeftGo < 0  → short put vertical (credit)
#     LeftGo > 0  → long  put vertical (debit)
#     RightGo < 0 → short call vertical (credit)
#     RightGo > 0 → long  call vertical (debit)
#
# - Strength per leg:
#     PUT strength  = LImp  if present, else abs(LeftGo)
#     CALL strength = RImp  if present, else abs(RightGo)
#     strong if strength >= CS_STRONG_THRESHOLD (default 0.66)
#     strong → qty = 2 * units; weak → qty = 1 * units
#
# - Sizing:
#     CS_UNIT_DOLLARS (default 10,000) → units = floor(equity / CS_UNIT_DOLLARS)
#     If Schwab equity is unavailable / <=0 → pretend equity = CS_UNIT_DOLLARS and units = 1.
#
# - Delegates placement + logging to ConstantStable/place.py via VERT_* envs.

import os
import sys
import re
import subprocess
from datetime import date
import requests
from schwab.auth import client_from_token_file

__version__ = "1.2.0"

GW_BASE = os.environ.get("GW_BASE", "https://gandalf.gammawizard.com").rstrip("/")
GW_ENDPOINT = os.environ.get("GW_ENDPOINT", "/rapi/GetUltraPureConstantStable").lstrip("/")

CS_UNIT_DOLLARS     = float(os.environ.get("CS_UNIT_DOLLARS", "10000"))
CS_STRONG_THRESHOLD = float(os.environ.get("CS_STRONG_THRESHOLD", "0.66"))
CS_LOG_PATH         = os.environ.get("CS_LOG_PATH", "logs/constantstable_vertical_trades.csv")


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


def fnum(x):
    try:
        return float(x)
    except Exception:
        return None


# ---------- GammaWizard (ConstantStable) ----------

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
        keys = ("Date", "TDate", "Limit", "CLimit", "Cat1", "Cat2", "LeftGo", "RightGo", "LImp", "RImp")
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
    Try to find a reasonable "equity / cash available" number.

    Returns (value_or_None, source_key, acct_number).
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


def get_account_hash(c):
    r = c.get_account_numbers()
    r.raise_for_status()
    arr = r.json() or []
    if not arr:
        return ""
    info = arr[0]
    return str(info.get("hashValue") or info.get("hashvalue") or "")


def ensure_log_dir():
    d = os.path.dirname(CS_LOG_PATH)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


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

    # Optional manual override for sizing
    ov_raw = (os.environ.get("SIZING_DOLLARS_OVERRIDE", "") or "").strip()
    if ov_raw:
        try:
            oc_val = float(ov_raw)
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

    left_go   = fnum(tr.get("LeftGo"))
    right_go  = fnum(tr.get("RightGo"))
    left_imp  = fnum(tr.get("LImp"))
    right_imp = fnum(tr.get("RImp"))

    # Strength per leg: prefer LImp/RImp, fallback to abs(Go)
    put_strength  = left_imp  if left_imp  is not None else (abs(left_go)  if left_go  is not None else 0.0)
    call_strength = right_imp if right_imp is not None else (abs(right_go) if right_go is not None else 0.0)

    print(f"CS_VERT_RUN TRADE: Date={trade_date} TDate={tdate_iso}")
    print(f"  PUT strikes : {p_low} / {p_high}  OSI=({put_low_osi},{put_high_osi})  LeftGo={left_go} LImp={left_imp}")
    print(f"  CALL strikes: {c_low} / {c_high}  OSI=({call_low_osi},{call_high_osi})  RightGo={right_go} RImp={right_imp}")
    print(
        "CS_VERT_RUN RAW_STRENGTH:",
        f"put_strength={put_strength:.3f} call_strength={call_strength:.3f}",
        f"(threshold={CS_STRONG_THRESHOLD:.3f})",
    )
    print(f"CS_VERT_RUN STRONG_THRESHOLD={CS_STRONG_THRESHOLD:.3f} LOG_PATH={CS_LOG_PATH}")

    ensure_log_dir()

    verts = []

    # ----- PUT side: one vertical only -----
    if left_go is not None and left_go != 0.0:
        strength = put_strength
        is_strong = strength >= CS_STRONG_THRESHOLD
        mult = 2 if is_strong else 1
        qty = max(1, units * mult)

        if left_go < 0:
            # Short put vertical (credit): short higher strike, long lower
            name = "PUT_SHORT"
            side = "CREDIT"
            direction = "SHORT"
            short_osi = put_high_osi
            long_osi = put_low_osi
        else:
            # Long put vertical (debit): buy higher, sell lower
            name = "PUT_LONG"
            side = "DEBIT"
            direction = "LONG"
            short_osi = put_low_osi
            long_osi = put_high_osi

        verts.append({
            "name": name,
            "kind": "PUT",
            "side": side,
            "direction": direction,
            "short_osi": short_osi,
            "long_osi": long_osi,
            "go": left_go,
            "strength": strength,
            "is_strong": is_strong,
            "qty": qty,
        })

    # ----- CALL side: one vertical only -----
    if right_go is not None and right_go != 0.0:
        strength = call_strength
        is_strong = strength >= CS_STRONG_THRESHOLD
        mult = 2 if is_strong else 1
        qty = max(1, units * mult)

        if right_go < 0:
            # Short call vertical (credit): short LOWER strike, long HIGHER strike
            name = "CALL_SHORT"
            side = "CREDIT"
            direction = "SHORT"
            short_osi = call_low_osi
            long_osi = call_high_osi
        else:
            # Long call vertical (debit): buy LOWER strike, sell HIGHER strike
            name = "CALL_LONG"
            side = "DEBIT"
            direction = "LONG"
            short_osi = call_high_osi
            long_osi = call_low_osi

        verts.append({
            "name": name,
            "kind": "CALL",
            "side": side,
            "direction": direction,
            "short_osi": short_osi,
            "long_osi": long_osi,
            "go": right_go,
            "strength": strength,
            "is_strong": is_strong,
            "qty": qty,
        })

    if not verts:
        print("CS_VERT_RUN SKIP: no nonzero LeftGo/RightGo — no verticals to trade.")
        return 0

    # ----- Spawn placer per vertical -----
    for v in verts:
        strength_s = f"{v['strength']:.3f}"
        print(
            f"CS_VERT_RUN {v['name']}: side={v['side']} kind={v['kind']} "
            f"short={v['short_osi']} long={v['long_osi']} "
            f"go={v['go']} strength={strength_s} is_strong={v['is_strong']} qty={v['qty']}"
        )

        env = dict(os.environ)
        env.update({
            "VERT_SIDE":        v["side"],
            "VERT_KIND":        v["kind"],
            "VERT_NAME":        v["name"],
            "VERT_DIRECTION":   v["direction"],
            "VERT_SHORT_OSI":   v["short_osi"],
            "VERT_LONG_OSI":    v["long_osi"],
            "VERT_QTY":         str(v["qty"]),
            "VERT_GO":          "" if v["go"] is None else str(v["go"]),
            "VERT_STRENGTH":    strength_s,
            "VERT_IS_STRONG":   "true" if v["is_strong"] else "false",
            "VERT_TRADE_DATE":  trade_date,
            "VERT_TDATE":       tdate_iso,
            "VERT_UNIT_DOLLARS": str(CS_UNIT_DOLLARS),
            "VERT_OC":          str(oc_val),
            "VERT_UNITS":       str(units),
            "SCHWAB_APP_KEY":     os.environ["SCHWAB_APP_KEY"],
            "SCHWAB_APP_SECRET":  os.environ["SCHWAB_APP_SECRET"],
            "SCHWAB_TOKEN_JSON":  os.environ["SCHWAB_TOKEN_JSON"],
            "SCHWAB_ACCT_HASH":   acct_hash,
            "CS_LOG_PATH":        CS_LOG_PATH,
        })

        rc = subprocess.call(
            [sys.executable, "scripts/trade/ConstantStable/place.py"],
            env=env,
        )
        if rc != 0:
            print(f"CS_VERT_RUN {v['name']}: placer rc={rc}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
