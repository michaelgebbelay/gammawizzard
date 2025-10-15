#!/usr/bin/env python3
# ORCHESTRATOR — same‑shorts CREDIT only (width from picker). Minimal, no push‑out.
# Calls placer with legs + qty. Mode NOW (no gates) unless you add them.

import os, sys, json, re, math, time
from datetime import date
import requests
from decimal import Decimal, ROUND_HALF_UP
from schwab.auth import client_from_token_file

# ====== Utils ======
def yymmdd(iso: str) -> str:
    d = date.fromisoformat((iso or "")[:10]); return f"{d:%y%m%d}"

def to_osi(sym: str) -> str:
    raw = (sym or "").strip().upper().lstrip(".").replace("_","")
    m = re.match(r'^([A-Z.$^]{1,6})(\d{6})([CP])(\d{1,5})(?:\.(\d{1,3}))?$', raw) or \
        re.match(r'^([A-Z.$^]{1,6})(\d{6})([CP])(\d{8})$', raw)
    if not m: raise ValueError(f"Cannot parse option symbol: {sym}")
    root, ymd, cp, strike, frac = (m.groups()+("",))[:5]
    mills = int(strike)*1000 + (int((frac or "0").ljust(3,'0')) if frac else 0) if len(strike)<8 else int(strike)
    return f"{root:<6}{ymd}{cp}{mills:08d}"

def strike_from_osi(osi: str) -> float:
    return int(osi[-8:]) / 1000.0

def orient_credit(bp,sp,sc,bc):
    bpS=strike_from_osi(bp); spS=strike_from_osi(sp)
    scS=strike_from_osi(sc); bcS=strike_from_osi(bc)
    if bpS>spS: bp,sp=sp,bp
    if scS>bcS: sc,bc=bc,sc
    return [bp,sp,sc,bc]

def build_legs_same_shorts(exp6: str, inner_put: int, inner_call: int, width: int):
    p_low, p_high = inner_put - width, inner_put
    c_low, c_high = inner_call, inner_call + width
    return orient_credit(
        to_osi(f".SPXW{exp6}P{p_low}"),
        to_osi(f".SPXW{exp6}P{p_high}"),
        to_osi(f".SPXW{exp6}C{c_low}"),
        to_osi(f".SPXW{exp6}C{c_high}")
    )

def _round_half_up(x: float) -> int:
    return int(Decimal(x).quantize(Decimal('1'), rounding=ROUND_HALF_UP))

# ====== Schwab ======
def schwab_client():
    app_key=os.environ["SCHWAB_APP_KEY"]; app_secret=os.environ["SCHWAB_APP_SECRET"]; token_json=os.environ["SCHWAB_TOKEN_JSON"]
    with open("schwab_token.json","w") as f: f.write(token_json)
    return client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")

def opening_cash_for_account(c):
    r=c.get_accounts(); r.raise_for_status()
    j=r.json()
    arr = j if isinstance(j,list) else [j]
    def pick(d,*ks):
        for k in ks:
            v=(d or {}).get(k)
            if isinstance(v,(int,float)): return float(v)
    # pick first acct
    a=arr[0]
    init=a.get("initialBalances",{}) if isinstance(a,dict) else {}
    curr=a.get("currentBalances",{}) if isinstance(a,dict) else {}
    oc = pick(init,"cashBalance","cashAvailableForTrading","liquidationValue")
    if oc is None: oc = pick(curr,"cashBalance","cashAvailableForTrading","liquidationValue")
    return float(oc or 0.0)

# ====== GammaWizard ======
def _sanitize_token(t: str) -> str:
    t=(t or "").strip().strip('"').strip("'");  return t.split(None,1)[1] if t.lower().startswith("bearer ") else t

def gw_fetch():
    base = os.environ.get("GW_BASE","https://gandalf.gammawizard.com").rstrip("/")
    endpoint = os.environ.get("GW_ENDPOINT","/rapi/GetLeoCross")
    tok=_sanitize_token(os.environ.get("GW_TOKEN","") or "")
    def hit(t):
        h={"Accept":"application/json"}
        if t: h["Authorization"]=f"Bearer {_sanitize_token(t)}"
        return requests.get(f"{base}/{endpoint.lstrip('/')}", headers=h, timeout=30)
    r=hit(tok) if tok else None
    if (r is None) or (r.status_code in (401,403)):
        email=os.environ.get("GW_EMAIL",""); pwd=os.environ.get("GW_PASSWORD","")
        rr=requests.post(f"{base}/goauth/authenticateFireUser", data={"email":email,"password":pwd}, timeout=30)
        rr.raise_for_status()
        t=rr.json().get("token") or ""
        r=hit(t)
    r.raise_for_status()
    return r.json()

def extract_trade(j):
    if isinstance(j,dict):
        if "Trade" in j:
            tr=j["Trade"];  return tr[-1] if isinstance(tr,list) and tr else tr if isinstance(tr,dict) else {}
        for v in j.values():
            if isinstance(v,(dict,list)):
                t=extract_trade(v)
                if t: return t
    if isinstance(j,list):
        for it in reversed(j):
            t=extract_trade(it)
            if t: return t
    return {}

# ====== Main ======
def main():
    MODE = (os.environ.get("PLACER_MODE","NOW") or "NOW").upper()
    width_env = os.environ.get("CREDIT_SPREAD_WIDTH","20").strip()
    try: width = int(width_env)
    except: width = 20
    width = max(5, int(math.ceil(width/5.0)*5))

    # GW
    tr = extract_trade(gw_fetch())
    exp6 = yymmdd(str(tr.get("TDate","")))
    inner_put  = int(float(tr.get("Limit")))
    inner_call = int(float(tr.get("CLimit")))

    # Schwab + sizing
    c = schwab_client()
    oc = opening_cash_for_account(c)
    # $4k per 5‑wide; scale with width; round half‑up; min 1
    denom = 4000.0 * (width/5.0)
    qty = max(1, _round_half_up((float(oc) if oc is not None else 0.0) / denom))
    # Allow override
    qov = os.environ.get("QTY_OVERRIDE","").strip()
    if qov:
        try: qty = max(1, int(qov))
        except: pass

    # Legs (same‑shorts; CREDIT)
    legs = build_legs_same_shorts(exp6, inner_put, inner_call, width)

    print(f"ORCH GATE disabled (MODE={MODE})")
    print(f"ORCH CONFIG: side=CREDIT, width={width}, mode={MODE}")
    print("ORCH SNAPSHOT:")
    labels=[("BUY_PUT",legs[0],-1),("SELL_PUT",legs[1],+1),("SELL_CALL",legs[2],+1),("BUY_CALL",legs[3],-1)]
    for name, osi, sign in labels:
        print(f"  {name:10s} {osi}  acct_qty=+0 sign={sign:+d}")  # minimal snapshot

    print(f"ORCH SIZE: qty={qty} open_cash={oc:.2f}")

    # Pass to placer via env
    env = dict(os.environ)
    env["SIDE"] = "CREDIT"
    env["WIDTH"] = str(width)
    env["QTY"]   = str(qty)
    env["OCC_BUY_PUT"]  = legs[0]
    env["OCC_SELL_PUT"] = legs[1]
    env["OCC_SELL_CALL"]= legs[2]
    env["OCC_BUY_CALL"] = legs[3]

    # Reuse Schwab token file
    app_key=os.environ["SCHWAB_APP_KEY"]; app_secret=os.environ["SCHWAB_APP_SECRET"]; token_json=os.environ["SCHWAB_TOKEN_JSON"]
    with open("schwab_token.json","w") as f: f.write(token_json)
    env["SCHWAB_APP_KEY"]=app_key
    env["SCHWAB_APP_SECRET"]=app_secret
    env["SCHWAB_TOKEN_JSON"]=token_json

    # Run placer
    print("ORCH → PLACER")
    rc = os.spawnve(os.P_WAIT, sys.executable, [sys.executable, "scripts/trade/leocross_place_simple.py"], env)
    return rc

if __name__=="__main__":
    sys.exit(main())
