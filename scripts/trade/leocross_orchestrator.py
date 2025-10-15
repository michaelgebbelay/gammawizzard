#!/usr/bin/env python3
# ORCHESTRATOR — Same‑shorts only. No push‑out. No long sleep on NOW mode.
# Builds legs from Leo, sizes quantity, runs basic guard, then calls placer.

import os, re, json, math, time
from datetime import datetime, date, timezone, timedelta
from zoneinfo import ZoneInfo
import requests
from schwab.auth import client_from_token_file

ET = ZoneInfo("America/New_York")
GW_BASE = "https://gandalf.gammawizard.com"
GW_ENDPOINT = "/rapi/GetLeoCross"

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

def build_legs_same_shorts(exp6: str, inner_put: int, inner_call: int, width: int, side_credit=True):
    p_low, p_high = inner_put - width, inner_put
    c_low, c_high = inner_call, inner_call + width
    bp = to_osi(f".SPXW{exp6}P{p_low}")
    sp = to_osi(f".SPXW{exp6}P{p_high}")
    sc = to_osi(f".SPXW{exp6}C{c_low}")
    bc = to_osi(f".SPXW{exp6}C{c_high}")
    if side_credit:
        return orient_credit(bp,sp,sc,bc)
    else:
        # for long IC we want BUY wings beyond shorts, but order legs are still BUY wings / SELL shorts
        # use same orientation; the placer will set NET_DEBIT
        return orient_credit(bp,sp,sc,bc)

def _sanitize_token(t: str) -> str:
    t=(t or "").strip().strip('"').strip("'")
    return t.split(None,1)[1] if t.lower().startswith("bearer ") else t

def gw_fetch():
    tok=_sanitize_token(os.environ.get("GW_TOKEN","") or "")
    def hit(t):
        h={"Accept":"application/json"}
        if t: h["Authorization"]=f"Bearer {_sanitize_token(t)}"
        return requests.get(f"{GW_BASE.rstrip('/')}/{GW_ENDPOINT.lstrip('/')}", headers=h, timeout=30)
    r=hit(tok) if tok else None
    if (r is None) or (r.status_code in (401,403)):
        email=os.environ.get("GW_EMAIL",""); pwd=os.environ.get("GW_PASSWORD","")
        if not (email and pwd): raise RuntimeError("GW_AUTH_REQUIRED")
        rr=requests.post(f"{GW_BASE}/goauth/authenticateFireUser", data={"email":email,"password":pwd}, timeout=30)
        rr.raise_for_status()
        t=rr.json().get("token") or ""
        r=hit(t)
    r.raise_for_status()
    return r.json()

def extract_trade(j):
    if isinstance(j,dict):
        if "Trade" in j:
            tr=j["Trade"]
            return tr[-1] if isinstance(tr,list) and tr else tr if isinstance(tr,dict) else {}
        keys=("Date","TDate","Limit","CLimit","Cat1","Cat2")
        if any(k in j for k in keys): return j
        for v in j.values():
            if isinstance(v,(dict,list)):
                t=extract_trade(v)
                if t: return t
    if isinstance(j,list):
        for it in reversed(j):
            t=extract_trade(it)
            if t: return t
    return {}

def schwab_client():
    app_key=os.environ["SCHWAB_APP_KEY"]; app_secret=os.environ["SCHWAB_APP_SECRET"]; token_json=os.environ["SCHWAB_TOKEN_JSON"]
    with open("schwab_token.json","w") as f: f.write(token_json)
    c=client_from_token_file(api_key=app_key, api_secret=app_secret, token_path="schwab_token.json")  # backward compat
    # newer arg name
    try:
        c=client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")
    except Exception:
        pass
    r=c.get_account_numbers(); r.raise_for_status()
    acct_info=r.json()[0]
    return c, str(acct_info.get("accountNumber")), str(acct_info.get("hashValue"))

def opening_cash_for_account(c, acct_number: str):
    r=c.get_accounts(); r.raise_for_status()
    data=r.json()
    accs = data if isinstance(data, list) else [data]
    def pick(d,*ks):
        for k in ks:
            v=(d or {}).get(k)
            if isinstance(v,(int,float)): return float(v)
    def hunt(a):
        acct_id=None; initial={}; current={}
        stack=[a]
        while stack:
            x=stack.pop()
            if isinstance(x,dict):
                if acct_id is None and x.get("accountNumber"): acct_id=str(x["accountNumber"])
                if "initialBalances" in x and isinstance(x["initialBalances"], dict): initial=x["initialBalances"]
                if "currentBalances" in x and isinstance(x["currentBalances"], dict): current=x["currentBalances"]
                for v in x.values():
                    if isinstance(v,(dict,list)): stack.append(v)
            elif isinstance(x,list): stack.extend(x)
        return acct_id, initial, current
    chosen=None
    for a in accs:
        aid, init, curr = hunt(a)
        if acct_number and aid==acct_number: chosen=(init,curr); break
        if chosen is None: chosen=(init,curr)
    if not chosen: return None
    init, curr = chosen
    oc = pick(init,"cashBalance","cashAvailableForTrading","liquidationValue")
    if oc is None: oc = pick(curr,"cashBalance","cashAvailableForTrading","liquidationValue")
    return oc

def positions_map(c, acct_hash: str):
    url=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}"
    r=c.session.get(url, params={"fields":"positions"}, timeout=20)
    r.raise_for_status()
    j=r.json()
    sa=j[0]["securitiesAccount"] if isinstance(j,list) else (j.get("securitiesAccount") or j)
    out={}
    for p in (sa.get("positions") or []):
        ins=p.get("instrument",{}) or {}
        if (ins.get("assetType") or ins.get("type") or "").upper() != "OPTION": continue
        sym = ins.get("symbol") or ""
        try:
            m = re.match(r'^([A-Z.$^]{1,6})(\d{6})([CP])(\d{8})$', sym)
            if not m: continue
            canon = (sym[6:12], sym[12], sym[-8:])
            qty=float(p.get("longQuantity",0))-float(p.get("shortQuantity",0))
            if abs(qty)>1e-9:
                out[canon] = out.get(canon, 0.0) + qty
        except Exception:
            continue
    return out

def main():
    MODE=(os.environ.get("PLACER_MODE","NOW") or "NOW").upper()
    HARD_CUTOFF_HHMM=(os.environ.get("HARD_CUTOFF_HHMM","16:15") or "16:15").strip()
    BYPASS_GUARD=str(os.environ.get("BYPASS_GUARD","")).strip().lower() in {"1","true","yes","y","on"}
    BYPASS_QTY=(os.environ.get("BYPASS_QTY","") or "").strip()
    SIDE_OVERRIDE=(os.environ.get("SIDE_OVERRIDE","AUTO") or "AUTO").upper()
    CREDIT_SPREAD_WIDTH=int(os.environ.get("CREDIT_SPREAD_WIDTH","20"))

    # Schwab + Leo
    c, acct_num, acct_hash = schwab_client()
    api=gw_fetch(); tr=extract_trade(api)
    if not tr:
        print("ORCH ABORT: NO_TRADE_PAYLOAD")
        return 1

    sig_date=str(tr.get("Date",""))
    exp_iso=str(tr.get("TDate","")); exp6=yymmdd(exp_iso)
    inner_put=int(float(tr.get("Limit"))); inner_call=int(float(tr.get("CLimit")))
    def fnum(x):
        try: return float(x)
        except: return None
    cat1=fnum(tr.get("Cat1")); cat2=fnum(tr.get("Cat2"))
    is_credit = True if (cat2 is None or cat1 is None or cat2>=cat1) else False
    if SIDE_OVERRIDE=="CREDIT": is_credit=True
    elif SIDE_OVERRIDE=="DEBIT": is_credit=False

    width = (CREDIT_SPREAD_WIDTH if is_credit else 5)

    # NOW mode: never sleep. SCHEDULED mode: short hold to 16:13 + FAST_HOLD.
    if MODE=="SCHEDULED":
        try:
            FAST_HOLD=int(os.environ.get("FAST_HOLD_SECONDS","30"))
        except Exception:
            FAST_HOLD=30
        now=datetime.now(ET)
        gate = now.replace(hour=16, minute=13, second=0, microsecond=0) + timedelta(seconds=FAST_HOLD)
        if now < gate:
            wait_s=int((gate-now).total_seconds())
            print(f"ORCH GATE sleep {wait_s}s (FAST_HOLD_SECONDS={FAST_HOLD})")
            time.sleep(max(0,gate.timestamp()-now.timestamp()))
        else:
            print("ORCH GATE immediate (already past hold window)")
    else:
        print("ORCH GATE disabled (MODE=NOW)")

    # Guard: skip if would close or partial overlap (unless BYPASS_GUARD)
    legs = build_legs_same_shorts(exp6, inner_put, inner_call, width, side_credit=is_credit)

    pos = positions_map(c, acct_hash)
    labels=[("BUY_PUT",legs[0],-1),("SELL_PUT",legs[1],+1),("SELL_CALL",legs[2],+1),("BUY_CALL",legs[3],-1)]
    print(f"ORCH CONFIG: side={'CREDIT' if is_credit else 'DEBIT'}, width={width}, mode={MODE}")
    print("ORCH SNAPSHOT:")
    for name, osi, sign in labels:
        can=(osi[6:12],osi[12],osi[-8:])
        cur=pos.get(can,0.0)
        print(f"  {name:10s} {osi}  acct_qty={cur:+g} sign={sign:+d}")

    if not BYPASS_GUARD:
        any_opposite=False; nonzero=0
        for _, osi, sign in labels:
            can=(osi[6:12],osi[12],osi[-8:]); cur=pos.get(can,0.0)
            if abs(cur)>1e-9: nonzero+=1
            if (sign<0 and cur<0) or (sign>0 and cur>0):
                any_opposite=True
        if any_opposite:
            print("ORCH SKIP: WOULD_CLOSE")
            return 0
        if 0 < nonzero < 4:
            print("ORCH SKIP: PARTIAL_OVERLAP")
            return 0

    # Sizing
    oc_override_raw=(os.environ.get("SIZING_DOLLARS_OVERRIDE","") or "").strip()
    oc_real=opening_cash_for_account(c, acct_num)
    oc = float(oc_override_raw) if oc_override_raw else (oc_real if oc_real is not None else 0.0)

    def round_half_up(x): return int(math.floor(x+0.5))
    if is_credit:
        qty = round_half_up( oc / (4000.0 * (width/5.0)) )
    else:
        qty = max(1, int(math.floor(oc / 4000.0)))
    if BYPASS_QTY:
        try: qty = max(1, int(BYPASS_QTY))
        except: pass
    qty = max(1, qty)
    print(f"ORCH SIZE: qty={qty} open_cash={oc:.2f}")

    # Call placer
    env=dict(os.environ)
    env["PLACER_SIDE"]   = "CREDIT" if is_credit else "DEBIT"
    env["PLACER_WIDTH"]  = str(width)
    env["QTY_OVERRIDE"]  = str(qty)
    env["PLACER_MODE"]   = MODE
    os.execve(os.environ.get("PYTHON","/usr/bin/python3"), [os.environ.get("PYTHON","/usr/bin/python3"), "scripts/trade/leocross_place_simple.py"], env)

if __name__=="__main__":
    raise SystemExit(main())
