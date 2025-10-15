#!/usr/bin/env python3
# PLACER — Same‑shorts only. No push‑out. Two cycles, 4 rungs each. Full error text.

import os, re, sys, time, math, requests
from datetime import date
from zoneinfo import ZoneInfo
from schwab.auth import client_from_token_file

ET = ZoneInfo("America/New_York")
TICK = 0.05

def clamp_tick(x: float) -> float:
    return round(round(float(x) / TICK) * TICK + 1e-12, 2)

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

def strike_from_osi(osi: str) -> float: return int(osi[-8:]) / 1000.0

def orient_credit(bp,sp,sc,bc):
    bpS=strike_from_osi(bp); spS=strike_from_osi(sp)
    scS=strike_from_osi(sc); bcS=strike_from_osi(bc)
    if bpS>spS: bp,sp=sp,bp
    if scS>bcS: sc,bc=bc,sc
    return [bp,sp,sc,bc]

def build_legs(exp6: str, inner_put: int, inner_call: int, width: int):
    p_low, p_high = inner_put - width, inner_put
    c_low, c_high = inner_call, inner_call + width
    bp = to_osi(f".SPXW{exp6}P{p_low}")
    sp = to_osi(f".SPXW{exp6}P{p_high}")
    sc = to_osi(f".SPXW{exp6}C{c_low}")
    bc = to_osi(f".SPXW{exp6}C{c_high}")
    return orient_credit(bp,sp,sc,bc)

def _sanitize_token(t: str) -> str:
    t=(t or "").strip().strip('"').strip("'")
    return t.split(None,1)[1] if t.lower().startswith("bearer ") else t

def gw_fetch():
    base = os.environ.get("GW_BASE","https://gandalf.gammawizard.com")
    endpoint = os.environ.get("GW_ENDPOINT","/rapi/GetLeoCross")
    tok=_sanitize_token(os.environ.get("GW_TOKEN","") or "")
    def hit(t):
        h={"Accept":"application/json"}
        if t: h["Authorization"]=f"Bearer {_sanitize_token(t)}"
        return requests.get(f"{base.rstrip('/')}/{endpoint.lstrip('/')}", headers=h, timeout=30)
    r=hit(tok) if tok else None
    if (r is None) or (r.status_code in (401,403)):
        email=os.environ.get("GW_EMAIL",""); pwd=os.environ.get("GW_PASSWORD","")
        if not (email and pwd): raise RuntimeError("GW_AUTH_REQUIRED")
        rr=requests.post(f"{base}/goauth/authenticateFireUser", data={"email":email,"password":pwd}, timeout=30)
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
    c=client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")
    r=c.get_account_numbers(); r.raise_for_status()
    acct_info=r.json()[0]
    return c, str(acct_info.get("hashValue"))

def fetch_bid_ask(c, osi: str):
    r=c.get_quote(osi)
    if r.status_code!=200: return (None,None)
    d=list(r.json().values())[0] if isinstance(r.json(), dict) else {}
    q=d.get("quote", d)
    b=q.get("bidPrice") or q.get("bid") or q.get("bidPriceInDouble")
    a=q.get("askPrice") or q.get("ask") or q.get("askPriceInDouble")
    return (float(b) if b is not None else None, float(a) if a is not None else None)

def condor_nbbo_credit(c, legs):
    bp, sp, sc, bc = legs
    bp_b, bp_a = fetch_bid_ask(c, bp); sp_b, sp_a = fetch_bid_ask(c, sp)
    sc_b, sc_a = fetch_bid_ask(c, sc); bc_b, bc_a = fetch_bid_ask(c, bc)
    if None in (bp_b, bp_a, sp_b, sp_a, sc_b, sc_a, bc_b, bc_a):
        return (None, None, None)
    credit_bid = (sp_b + sc_b) - (bp_a + bc_a)
    credit_ask = (sp_a + sc_a) - (bp_b + bc_b)
    credit_mid = (credit_bid + credit_ask) / 2.0
    return (clamp_tick(credit_bid), clamp_tick(credit_ask), clamp_tick(credit_mid))

def order_payload_credit(legs, qty:int, price:float):
    # orderType must be NET_CREDIT for complex combo at Schwab
    return {
        "orderType": "NET_CREDIT",
        "session": "NORMAL",
        "price": f"{clamp_tick(price):.2f}",
        "duration": "DAY",
        "orderStrategyType": "SINGLE",
        "complexOrderStrategyType": "IRON_CONDOR",
        "orderLegCollection":[
            {"instruction":"BUY_TO_OPEN",  "positionEffect":"OPENING","quantity":qty,"instrument":{"symbol":legs[0],"assetType":"OPTION"}},
            {"instruction":"SELL_TO_OPEN", "positionEffect":"OPENING","quantity":qty,"instrument":{"symbol":legs[1],"assetType":"OPTION"}},
            {"instruction":"SELL_TO_OPEN", "positionEffect":"OPENING","quantity":qty,"instrument":{"symbol":legs[2],"assetType":"OPTION"}},
            {"instruction":"BUY_TO_OPEN",  "positionEffect":"OPENING","quantity":qty,"instrument":{"symbol":legs[3],"assetType":"OPTION"}},
        ]
    }

def place_order(c, acct_hash: str, payload: dict):
    url=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders"
    r = c.session.post(url, json=payload, timeout=20)
    ok = r.status_code in (200,201,202)
    if not ok:
        try: body = r.text
        except Exception: body = ""
        print(f"WARN: place failed — HTTP_{r.status_code} {body[:240]}")
        return None
    # Try to read orderId or Location
    try:
        j=r.json()
        oid = str(j.get("orderId") or "") if isinstance(j,dict) else ""
    except Exception:
        oid=""
    if not oid:
        loc=r.headers.get("Location","")
        oid = loc.rstrip("/").split("/")[-1] if loc else ""
    return oid or None

def cancel_order(c, acct_hash: str, order_id: str):
    if not order_id: return
    url=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{order_id}"
    try:
        r=c.session.delete(url, timeout=20)
        print(f"CANCEL {order_id} → HTTP_{r.status_code}")
    except Exception as e:
        print(f"WARN: cancel failed {order_id}: {e}")

def main():
    side=(os.environ.get("PLACER_SIDE","CREDIT") or "CREDIT").upper()
    width=int(os.environ.get("PLACER_WIDTH","20"))
    qty=max(1,int(os.environ.get("QTY_OVERRIDE","1") or "1"))
    mode=(os.environ.get("PLACER_MODE","NOW") or "NOW").upper()

    if side!="CREDIT":
        print("ABORT: This simple placer only supports CREDIT in this build.")
        return 1

    # Leo
    j=gw_fetch(); tr=extract_trade(j)
    if not tr:
        print("ABORT: NO_TRADE_PAYLOAD"); return 1
    exp6 = yymmdd(str(tr.get("TDate","")))
    inner_put  = int(float(tr.get("Limit")))
    inner_call = int(float(tr.get("CLimit")))
    legs = build_legs(exp6, inner_put, inner_call, width)

    # Schwab
    c, acct_hash = schwab_client()

    print(f"PLACER START side=CREDIT width={width} qty={qty}")
    # NBBO + ladder rungs
    def nbbo():
        b,a,m = condor_nbbo_credit(c, legs)
        if m is None:
            print("NBBO unavailable"); return None
        print(f"CREDIT NBBO: bid={b} ask={a} mid={m}")
        ladder = [x for x in [a, m, clamp_tick(m-0.05), clamp_tick(m-0.10)] if x is not None]
        # remove dups while preserving order
        out=[]; seen=set()
        for p in ladder:
            if p not in seen:
                out.append(p); seen.add(p)
        print(f"CYCLE ladder: {out}")
        return out

    cycles=2
    for cyc in range(cycles):
        ladder = nbbo()
        if not ladder:
            break
        active_id=None
        for px in ladder:
            print(f"RUNG → price={px:.2f} to_place={qty}")
            oid = place_order(c, acct_hash, order_payload_credit(legs, qty, px))
            if not oid:
                continue
            active_id = oid
            # brief wait; we do not poll status here (keep simple)
            time.sleep(2.0)
            # cancel and move to next rung unless it filled instantly
            cancel_order(c, acct_hash, active_id)
            active_id=None
        # refresh Leo between passes (keeps same‑shorts; if inner strikes moved, rebuild accordingly)
        try:
            j2=gw_fetch(); tr2=extract_trade(j2)
            exp6b = yymmdd(str(tr2.get("TDate","")))
            put2  = int(float(tr2.get("Limit"))); call2=int(float(tr2.get("CLimit")))
            if exp6b!=exp6 or put2!=inner_put or call2!=inner_call:
                exp6, inner_put, inner_call = exp6b, put2, call2
                legs = build_legs(exp6, inner_put, inner_call, width)
                print(f"REFRESH_FROM_LEO: legs={legs}")
        except Exception as e:
            print(f"REFRESH_FROM_LEO failed: {e}")

    print("PLACER DONE")
    return 0

if __name__=="__main__":
    sys.exit(main())
