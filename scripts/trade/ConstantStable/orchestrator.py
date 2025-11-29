#!/usr/bin/env python3
# ORCHESTRATOR — ConstantStable verticals, 10k sizing, per-leg 1x/2x based on Go strength.
#
# Flow:
#   1. Fetch ConstantStable trade from GW (/rapi/GetLeoCross).
#   2. Build 5-wide put & call vertical strikes around Limit / CLimit.
#   3. Get Schwab equity; units = floor(equity / ACCOUNT_UNIT_DOLLARS).
#   4. Per leg:
#        strong = |Go| >= GO_STRONG_THRESHOLD  → size multiplier = 2
#                 else                          multiplier = 1
#        qty_leg = units * multiplier
#        side_leg = CREDIT if Go<0 else DEBIT
#   5. Export env and spawn scripts/trade/ConstantStable/place.py.

import os, sys, math, re
from datetime import date
import requests
from schwab.auth import client_from_token_file

ACCOUNT_UNIT_DOLLARS = float(os.environ.get("ACCOUNT_UNIT_DOLLARS", "10000"))
GO_STRONG_THRESHOLD  = float(os.environ.get("GO_STRONG_THRESHOLD", "0.66"))

def yymmdd(iso: str) -> str:
    d = date.fromisoformat((iso or "")[:10])
    return f"{d:%y%m%d}"

def to_osi(sym: str) -> str:
    raw = (sym or "").strip().upper().lstrip(".").replace("_","")
    m = re.match(r'^([A-Z.$^]{1,6})(\d{6})([CP])(\d{1,5})(?:\.(\d{1,3}))?$', raw) or \
        re.match(r'^([A-Z.$^]{1,6})(\d{6})([CP])(\d{8})$', raw)
    if not m:
        raise ValueError(f"Cannot parse option symbol: {sym}")
    root, ymd, cp, strike, frac = (m.groups()+("",))[:5]
    if len(strike) < 8:
        mills = int(strike) * 1000 + (int((frac or "0").ljust(3,'0')) if frac else 0)
    else:
        mills = int(strike)
    return f"{root:<6}{ymd}{cp}{mills:08d}"

def strike_from_osi(osi: str) -> float:
    return int(osi[-8:]) / 1000.0

def _sanitize_token(t: str) -> str:
    t = (t or "").strip().strip('"').strip("'")
    return t.split(None,1)[1] if t.lower().startswith("bearer ") else t

def gw_fetch_constantstable():
    base = os.environ.get("GW_BASE","https://gandalf.gammawizard.com").rstrip("/")
    endpoint = os.environ.get("GW_ENDPOINT","/rapi/GetLeoCross")
    tok = _sanitize_token(os.environ.get("GW_TOKEN","") or "")
    def hit(t):
        h = {"Accept":"application/json"}
        if t:
            h["Authorization"] = f"Bearer {_sanitize_token(t)}"
        return requests.get(f"{base}/{endpoint.lstrip('/')}", headers=h, timeout=30)
    r = hit(tok) if tok else None
    if (r is None) or (r.status_code in (401,403)):
        email = os.environ.get("GW_EMAIL","")
        pwd   = os.environ.get("GW_PASSWORD","")
        if not (email and pwd):
            raise RuntimeError("GW_AUTH_REQUIRED")
        rr = requests.post(f"{base}/goauth/authenticateFireUser",
                           data={"email":email,"password":pwd}, timeout=30)
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
        keys = ("Date","TDate","Limit","CLimit","Cat1","Cat2","LeftGo","RightGo")
        if any(k in j for k in keys):
            return j
        for v in j.values():
            if isinstance(v,(dict,list)):
                t = extract_trade(v)
                if t:
                    return t
    if isinstance(j, list):
        for it in reversed(j):
            t = extract_trade(it)
            if t:
                return t
    return {}

def schwab_client():
    app_key    = os.environ["SCHWAB_APP_KEY"]
    app_secret = os.environ["SCHWAB_APP_SECRET"]
    token_json = os.environ["SCHWAB_TOKEN_JSON"]
    with open("schwab_token.json","w") as f:
        f.write(token_json)
    return client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")

def opening_cash_for_account(c, prefer_number=None):
    """
    Returns (value, source_key, acct_number).
    Prefers cashAvailableForTrading, then cashBalance, then liquidationValue, etc.
    """
    r = c.get_accounts(); r.raise_for_status()
    data = r.json()
    arr = data if isinstance(data,list) else [data]

    if prefer_number is None:
        try:
            rr = c.get_account_numbers(); rr.raise_for_status()
            prefer_number = str((rr.json() or [{}])[0].get("accountNumber") or "")
        except Exception:
            prefer_number = None

    def hunt(a):
        acct_id=None; init={}; curr={}
        stack=[a]
        while stack:
            x=stack.pop()
            if isinstance(x,dict):
                if acct_id is None and x.get("accountNumber"):
                    acct_id=str(x["accountNumber"])
                if "initialBalances" in x and isinstance(x["initialBalances"], dict):
                    init=x["initialBalances"]
                if "currentBalances" in x and isinstance(x["currentBalances"], dict):
                    curr=x["currentBalances"]
                for v in x.values():
                    if isinstance(v,(dict,list)):
                        stack.append(v)
            elif isinstance(x,list):
                stack.extend(x)
        return acct_id, init, curr

    chosen=None
    for a in arr:
        aid, init, curr = hunt(a)
        if prefer_number and aid == prefer_number:
            chosen=(aid,init,curr); break
        if chosen is None:
            chosen=(aid,init,curr)

    if not chosen:
        return 0.0, "none", ""
    aid, init, curr = chosen

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
            v = src.get(k)
            if isinstance(v,(int,float)):
                return float(v), k
        return None

    for src in (init, curr):
        got = pick(src)
        if got:
            return got[0], got[1], aid

    return 0.0, "none", aid

def fnum(x):
    try:
        return float(x)
    except Exception:
        return None

def is_strong(go_val):
    return (go_val is not None) and (abs(go_val) >= GO_STRONG_THRESHOLD)

def build_vertical_strikes(exp6: str, inner_put: int, inner_call: int, width: int = 5):
    """
    Returns 4 OSIs:
        put_low, put_high, call_low, call_high
    with low < high in terms of strikes.
    """
    p_low  = inner_put  - width
    p_high = inner_put
    c_low  = inner_call
    c_high = inner_call + width

    put_low  = to_osi(f".SPXW{exp6}P{p_low}")
    put_high = to_osi(f".SPXW{exp6}P{p_high}")
    call_low  = to_osi(f".SPXW{exp6}C{c_low}")
    call_high = to_osi(f".SPXW{exp6}C{c_high}")

    if strike_from_osi(put_low) > strike_from_osi(put_high):
        put_low, put_high = put_high, put_low
    if strike_from_osi(call_low) > strike_from_osi(call_high):
        call_low, call_high = call_high, call_low

    return put_low, put_high, call_low, call_high

def main():
    MODE = (os.environ.get("PLACER_MODE","NOW") or "NOW").upper()

    # 1) Fetch ConstantStable trade
    try:
        api = gw_fetch_constantstable()
        tr  = extract_trade(api)
    except Exception as e:
        print("ORCH: GW fetch failed:", e)
        return 1

    if not tr:
        print("ORCH: NO_TRADE_PAYLOAD — skip")
        return 0

    sig_date = str(tr.get("Date",""))
    exp_iso  = str(tr.get("TDate",""))
    exp6     = yymmdd(exp_iso)

    inner_put  = int(float(tr.get("Limit")))
    inner_call = int(float(tr.get("CLimit")))

    left_go  = fnum(tr.get("LeftGo"))
    right_go = fnum(tr.get("RightGo"))

    put_low, put_high, call_low, call_high = build_vertical_strikes(exp6, inner_put, inner_call, width=5)

    print(f"ORCH ConstantStable: Date={sig_date} TDate={exp_iso} "
          f"Limit={inner_put} CLimit={inner_call}")
    print(f"  PUT vertical  : {strike_from_osi(put_low)}-{strike_from_osi(put_high)} ({put_low},{put_high})")
    print(f"  CALL vertical : {strike_from_osi(call_low)}-{strike_from_osi(call_high)} ({call_low},{call_high})")
    print(f"  LeftGo={left_go} RightGo={right_go}")

    # 2) Schwab equity → units
    try:
        c = schwab_client()
        # c here is actually the client; for consistency with opening_cash_for_account we don't need acct_hash
        oc_val, oc_src, acct_num = opening_cash_for_account(c)
    except Exception as e:
        print("ORCH: Schwab client/balance failed:", e)
        return 1

    equity = oc_val
    print(f"ORCH EQUITY: {equity:.2f} (src={oc_src}, acct={acct_num})")

    if ACCOUNT_UNIT_DOLLARS <= 0:
        print("ORCH FATAL: ACCOUNT_UNIT_DOLLARS must be > 0")
        return 1

    units = int(equity // ACCOUNT_UNIT_DOLLARS)
    if units <= 0:
        print("ORCH SKIP: equity below one 10k unit")
        return 0

    base_qty = units
    print(f"ORCH UNITS: {units} (1 unit = ${ACCOUNT_UNIT_DOLLARS:.0f})")

    # 3) Per-leg sizing & side

    put_side = ""
    put_qty  = 0
    strong_put = False

    if left_go is not None and left_go != 0.0:
        strong_put = is_strong(left_go)
        mult_put   = 2 if strong_put else 1
        put_qty    = max(1, base_qty * mult_put)
        put_side   = "CREDIT" if left_go < 0 else "DEBIT"

    call_side = ""
    call_qty  = 0
    strong_call = False

    if right_go is not None and right_go != 0.0:
        strong_call = is_strong(right_go)
        mult_call   = 2 if strong_call else 1
        call_qty    = max(1, base_qty * mult_call)
        call_side   = "CREDIT" if right_go < 0 else "DEBIT"

    if not put_side and not call_side:
        print("ORCH SKIP: both LeftGo and RightGo are missing/zero.")
        return 0

    print(f"ORCH LEG CONFIG (MODE={MODE}):")
    print(f"  PUT : side={put_side or 'NONE'} qty={put_qty} strong={strong_put}")
    print(f"  CALL: side={call_side or 'NONE'} qty={call_qty} strong={strong_call}")

    # 4) Pass env to ConstantStable/place.py
    env = dict(os.environ)
    env.update({
        "CS_SIDE_PUT":   put_side,
        "CS_SIDE_CALL":  call_side,
        "CS_QTY_PUT":    str(put_qty),
        "CS_QTY_CALL":   str(call_qty),
        "CS_PUT_LOW_OSI":  put_low,
        "CS_PUT_HIGH_OSI": put_high,
        "CS_CALL_LOW_OSI":  call_low,
        "CS_CALL_HIGH_OSI": call_high,
        "CS_STRONG_PUT":   "true" if strong_put else "false",
        "CS_STRONG_CALL":  "true" if strong_call else "false",
        "CS_GO_PUT":   "" if left_go  is None else str(left_go),
        "CS_GO_CALL":  "" if right_go is None else str(right_go),
        "CS_SIGNAL_DATE":  sig_date,
        "CS_EXPIRY_ISO":   exp_iso,
        "CS_ACCOUNT_EQUITY": f"{equity:.2f}",
    })

    script_path = "scripts/trade/ConstantStable/place.py"
    print(f"ORCH → spawning {script_path}")
    rc = os.spawnve(os.P_WAIT, sys.executable, [sys.executable, script_path], env)
    return rc

if __name__=="__main__":
    sys.exit(main())
