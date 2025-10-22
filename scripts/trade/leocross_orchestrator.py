#!/usr/bin/env python3
# ORCHESTRATOR — CREDIT/DEBIT. CREDIT supports split wings + optional CALL_MULT.
# If CALL_MULT==1 and qtys match → one unbalanced 4-leg condor.
# If CALL_MULT>1 → two verticals (puts, calls) with ratio.

import os, sys, re, math
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
import requests
from schwab.auth import client_from_token_file

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

def build_legs_split(exp6: str, inner_put: int, inner_call: int, Wp: int, Wc: int):
    p_low, p_high = inner_put - Wp, inner_put
    c_low, c_high = inner_call, inner_call + Wc
    bp = to_osi(f".SPXW{exp6}P{p_low}")
    sp = to_osi(f".SPXW{exp6}P{p_high}")
    sc = to_osi(f".SPXW{exp6}C{c_low}")
    bc = to_osi(f".SPXW{exp6}C{c_high}")
    return orient_credit(bp,sp,sc,bc)

def _round_half_up(x: float) -> int:
    return int(Decimal(x).quantize(Decimal('1'), rounding=ROUND_HALF_UP))

# -------- Schwab ----------
def schwab_client():
    app_key=os.environ["SCHWAB_APP_KEY"]; app_secret=os.environ["SCHWAB_APP_SECRET"]; token_json=os.environ["SCHWAB_TOKEN_JSON"]
    with open("schwab_token.json","w") as f: f.write(token_json)
    return client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")

def opening_cash_for_account(c):
    r=c.get_accounts(); r.raise_for_status()
    j=r.json(); arr = j if isinstance(j,list) else [j]
    def pick(d,*ks):
        for k in ks:
            v=(d or {}).get(k)
            if isinstance(v,(int,float)): return float(v)
    a=arr[0] if arr else {}
    init=a.get("initialBalances",{}) if isinstance(a,dict) else {}
    curr=a.get("currentBalances",{}) if isinstance(a,dict) else {}
    oc = pick(init,"cashBalance","cashAvailableForTrading","liquidationValue")
    if oc is None: oc = pick(curr,"cashBalance","cashAvailableForTrading","liquidationValue")
    return float(oc or 0.0)

# -------- GW (LeoCross) ----------
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

# -------- main ----------
def main():
    MODE = (os.environ.get("PLACER_MODE","NOW") or "NOW").upper()

    # Leo → shorts & side
    tr = extract_trade(gw_fetch())
    exp6 = yymmdd(str(tr.get("TDate","")))
    inner_put  = int(float(tr.get("Limit")))
    inner_call = int(float(tr.get("CLimit")))
    # Side from Cat1/Cat2 with overrides
    def fnum(x):
        try: return float(x)
        except: return None
    cat1=fnum(tr.get("Cat1")); cat2=fnum(tr.get("Cat2"))
    is_credit = True if (cat2 is None or cat1 is None or cat2>=cat1) else False
    SIDE_OVERRIDE = (os.environ.get("SIDE_OVERRIDE","AUTO") or "AUTO").upper()
    if SIDE_OVERRIDE == "CREDIT": is_credit=True
    elif SIDE_OVERRIDE == "DEBIT": is_credit=False

    c = schwab_client()
    oc = opening_cash_for_account(c)

    # Sizing: $4k per 5-wide, scale by max wing width; round half-up; min 1
    def base_qty_for(maxw):
        denom = 4000.0 * (maxw/5.0)
        return max(1, _round_half_up((float(oc) if oc is not None else 0.0) / denom))

    # Picker outputs (optional)
    Wp_env = os.environ.get("PICKED_PUT_WIDTH","").strip()
    Wc_env = os.environ.get("PICKED_CALL_WIDTH","").strip()
    Cm_env = os.environ.get("CALL_MULT","").strip()
    Wdef   = os.environ.get("CREDIT_SPREAD_WIDTH","20").strip()
    try: Wdef = int(Wdef)
    except: Wdef = 20
    try: Wp = int(Wp_env) if Wp_env else Wdef
    except: Wp = Wdef
    try: Wc = int(Wc_env) if Wc_env else Wdef
    except: Wc = Wdef
    Wp = max(5, int(math.ceil(Wp/5.0)*5))
    Wc = max(5, int(math.ceil(Wc/5.0)*5))
    try: call_mult = max(1, int(Cm_env)) if Cm_env else 1
    except: call_mult = 1

    if not is_credit:
        # Long IC (always 5-wide)
        Wp=Wc=5
        qty = max(1, int(math.floor((float(oc) if oc else 0.0)/4000.0)))
        bp,sp,sc,bc = build_legs_split(exp6, inner_put, inner_call, Wp, Wc)
        env = dict(os.environ)
        env.update({
            "SIDE":"DEBIT","STRUCTURE":"CONDOR",
            "QTY":str(qty),"QTY_PUT":str(qty),"QTY_CALL":str(qty),
            "OCC_BUY_PUT":bp, "OCC_SELL_PUT":sp, "OCC_SELL_CALL":sc, "OCC_BUY_CALL":bc,
            "WIDTH_PUT":str(Wp), "WIDTH_CALL":str(Wc)
        })
        print(f"ORCH GATE disabled (MODE={MODE})")
        print(f"ORCH CONFIG: side=DEBIT, width=5, mode={MODE}")
        print("ORCH → PLACER")
        rc = os.spawnve(os.P_WAIT, sys.executable, [sys.executable, "scripts/trade/leocross_place_simple.py"], env)
        return rc

    # CREDIT path (skew-aware)
    bp,sp,sc,bc = build_legs_split(exp6, inner_put, inner_call, Wp, Wc)
    qty_base = base_qty_for(max(Wp,Wc))
    qov = os.environ.get("QTY_OVERRIDE","").strip()
    if qov:
        try: qty_base = max(1, int(qov))
        except: pass

    qty_put  = qty_base
    qty_call = qty_base * call_mult

    # structure: single condor if qtys equal; otherwise split (two verticals)
    structure = "CONDOR" if qty_put == qty_call else "SPLIT"

    print(f"ORCH GATE disabled (MODE={MODE})")
    print(f"ORCH CONFIG: side=CREDIT, Wp={Wp}, Wc={Wc}, call_mult={call_mult}, mode={MODE}")
    print(f"ORCH SIZE: base_qty={qty_base} oc={oc:.2f} structure={structure}")

    env = dict(os.environ)
    env.update({
        "SIDE":"CREDIT",
        "STRUCTURE": structure,
        "WIDTH_PUT":  str(Wp),
        "WIDTH_CALL": str(Wc),
        "QTY":        str(qty_base),
        "QTY_PUT":    str(qty_put),
        "QTY_CALL":   str(qty_call),
        "OCC_BUY_PUT":bp, "OCC_SELL_PUT":sp, "OCC_SELL_CALL":sc, "OCC_BUY_CALL":bc
    })

    # reuse Schwab token file for placer
    app_key=os.environ["SCHWAB_APP_KEY"]; app_secret=os.environ["SCHWAB_APP_SECRET"]; token_json=os.environ["SCHWAB_TOKEN_JSON"]
    with open("schwab_token.json","w") as f: f.write(token_json)
    env["SCHWAB_APP_KEY"]=app_key; env["SCHWAB_APP_SECRET"]=app_secret; env["SCHWAB_TOKEN_JSON"]=token_json

    print("ORCH → PLACER")
    rc = os.spawnve(os.P_WAIT, sys.executable, [sys.executable, "scripts/trade/leocross_place_simple.py"], env)
    return rc

if __name__=="__main__":
    sys.exit(main())
