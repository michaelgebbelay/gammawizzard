#!/usr/bin/env python3
# PLACER — Same‑shorts only. No push‑out. Two‑cycle ladder. Refresh Leo between cycles.

import os, re, json, time, math, random
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
import requests
from schwab.auth import client_from_token_file

ET = ZoneInfo("America/New_York")
GW_BASE = "https://gandalf.gammawizard.com"
GW_ENDPOINT = "/rapi/GetLeoCross"
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
    bp = to_osi(f".SPXW{exp6}P{p_low}")
    sp = to_osi(f".SPXW{exp6}P{p_high}")
    sc = to_osi(f".SPXW{exp6}C{c_low}")
    bc = to_osi(f".SPXW{exp6}C{c_high}")
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
    c=client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")
    r=c.get_account_numbers(); r.raise_for_status()
    acct_info=r.json()[0]
    return c, str(acct_info.get("accountNumber")), str(acct_info.get("hashValue"))

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

def condor_nbbo_debit(c, legs):
    bp, sp, sc, bc = legs
    bp_b, bp_a = fetch_bid_ask(c, bp); sp_b, sp_a = fetch_bid_ask(c, sp)
    sc_b, sc_a = fetch_bid_ask(c, sc); bc_b, bc_a = fetch_bid_ask(c, bc)
    if None in (bp_b, bp_a, sp_b, sp_a, sc_b, sc_a, bc_b, bc_a):
        return (None, None, None)
    debit_bid = (bp_b + bc_b) - (sp_a + sc_a)
    debit_ask = (bp_a + bc_a) - (sp_b + sc_b)
    debit_mid = (debit_bid + debit_ask) / 2.0
    return (clamp_tick(debit_bid), clamp_tick(debit_ask), clamp_tick(debit_mid))

def list_recent_orders(c, acct_hash: str):
    now=datetime.now(ET)
    start=now.replace(hour=0, minute=0, second=0, microsecond=0)
    url=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders"
    try:
        r=c.session.get(url, params={"fromEnteredTime": start.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                                     "toEnteredTime":   now.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                                     "maxResults": 200}, timeout=20)
        r.raise_for_status()
        return r.json() or []
    except Exception:
        return []

def _legs_canon_from_order(o):
    got=set()
    for leg in (o.get("orderLegCollection") or []):
        ins=(leg.get("instrument",{}) or {})
        sym=ins.get("symbol") or ""
        m=re.match(r'^([A-Z.$^]{1,6})(\d{6})([CP])(\d{8})$', sym)
        if m:
            got.add((sym[6:12], sym[12], sym[-8:]))
    return got

def pick_active_and_overlaps(c, acct_hash: str, canon_set):
    exact_id=None; active_status=""; overlaps=[]
    for o in list_recent_orders(c, acct_hash):
        st=str(o.get("status") or "").upper()
        if st not in {"WORKING","QUEUED","OPEN","PENDING_ACTIVATION","ACCEPTED","RECEIVED",
                      "PENDING_REPLACE","PENDING_CANCEL","CANCEL_REQUESTED"}: continue
        got=_legs_canon_from_order(o)
        if not got: continue
        if got==canon_set and exact_id is None:
            exact_id=str(o.get("orderId") or ""); active_status=st
        elif got & canon_set:
            oid=str(o.get("orderId") or "")
            if oid: overlaps.append(oid)
    return exact_id, active_status, overlaps

def main():
    # ENV
    SIDE=(os.environ.get("PLACER_SIDE","CREDIT") or "CREDIT").upper()
    WIDTH=int(os.environ.get("PLACER_WIDTH","20"))
    QTY=max(1, int(float(os.environ.get("QTY_OVERRIDE","1") or "1")))
    STEP_WAIT_CREDIT=float(os.environ.get("STEP_WAIT_CREDIT","5")); STEP_WAIT_DEBIT=float(os.environ.get("STEP_WAIT_DEBIT","5"))
    MIN_RUNG_WAIT = float(os.environ.get("MIN_RUNG_WAIT","5"))
    MAX_LADDER_CYCLES=int(os.environ.get("MAX_LADDER_CYCLES","2"))
    MAX_RUNTIME_SECS=float(os.environ.get("MAX_RUNTIME_SECS","115"))
    CANCEL_SETTLE_SECS=float(os.environ.get("CANCEL_SETTLE_SECS","0.8"))
    HARD_CUTOFF_HHMM=(os.environ.get("HARD_CUTOFF_HHMM","16:15") or "16:15").strip()

    # Schwab + Leo (fresh)
    c, acct_num, acct_hash = schwab_client()
    j=gw_fetch(); tr=extract_trade(j)
    exp6=yymmdd(str(tr.get("TDate","")))
    inner_put=int(float(tr.get("Limit"))); inner_call=int(float(tr.get("CLimit")))

    def legs_now():
        return build_legs_same_shorts(exp6, inner_put, inner_call, WIDTH)

    legs=legs_now()
    canon = {(x[6:12],x[12],x[-8:]) for x in legs}
    print(f"PLACER START side={SIDE} width={WIDTH} qty={QTY}")

    # Cancel overlaps
    def cancel_order(oid):
        try:
            url=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{oid}"
            r=c.session.delete(url, timeout=20); r.raise_for_status(); return True
        except Exception:
            return False

    ex, st, ovs = pick_active_and_overlaps(c, acct_hash, canon)
    for oid in ([ex] if ex else []) + ovs:
        if cancel_order(oid):
            time.sleep(CANCEL_SETTLE_SECS)

    # NBBO helpers
    def nbbo():
        return (condor_nbbo_credit(c, legs) if SIDE=="CREDIT" else condor_nbbo_debit(c, legs))

    # Order helpers
    def parse_order_id(r):
        try: j=r.json(); oid=str(j.get("orderId") or j.get("order_id") or "")
        except Exception: oid=""
        if not oid:
            loc=r.headers.get("Location","")
            if loc: oid=loc.rstrip("/").split("/")[-1]
        return oid

    def order_payload(price: float, qty: int):
        order_type = ("NET_CREDIT" if SIDE=="CREDIT" else "NET_DEBIT")
        return {
            "orderType": order_type, "session":"NORMAL", "price": f"{clamp_tick(price):.2f}",
            "duration":"DAY","orderStrategyType":"SINGLE","complexOrderStrategyType":"IRON_CONDOR",
            "orderLegCollection":[
                {"instruction":"BUY_TO_OPEN", "positionEffect":"OPENING","quantity":qty,"instrument":{"symbol":legs[0],"assetType":"OPTION"}},
                {"instruction":"SELL_TO_OPEN","positionEffect":"OPENING","quantity":qty,"instrument":{"symbol":legs[1],"assetType":"OPTION"}},
                {"instruction":"SELL_TO_OPEN","positionEffect":"OPENING","quantity":qty,"instrument":{"symbol":legs[2],"assetType":"OPTION"}},
                {"instruction":"BUY_TO_OPEN", "positionEffect":"OPENING","quantity":qty,"instrument":{"symbol":legs[3],"assetType":"OPTION"}},
            ]
        }

    def place(price: float, qty: int):
        url=f"https://api/schwabapi.com/trader/v1/accounts/{acct_hash}/orders"
        for attempt in range(6):
            try:
                r=c.session.post(url, json=order_payload(price, qty), timeout=20)
                if r.status_code in (200,201,202):
                    return parse_order_id(r)
                if r.status_code==429:
                    time.sleep( min(10.0, 0.5*(2**attempt)) ); continue
                time.sleep(0.3)
            except Exception:
                time.sleep( min(10.0, 0.5*(2**attempt)) )
        return ""

    def get_status(oid: str):
        try:
            url=f"https://api/schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{oid}"
            r=c.session.get(url, timeout=20); r.raise_for_status(); return r.json() or {}
        except Exception:
            return {}

    def wait_until_closed(oid: str, max_wait: float = CANCEL_SETTLE_SECS):
        t_end=time.time()+max_wait
        while time.time() < t_end:
            st=get_status(oid)
            status=str(st.get("status") or st.get("orderStatus") or "").upper()
            if (not status) or status in {"CANCELED","FILLED","REJECTED","EXPIRED"}:
                return True
            if status not in {"WORKING","QUEUED","OPEN","PENDING_ACTIVATION","ACCEPTED","RECEIVED",
                              "PENDING_REPLACE","PENDING_CANCEL","CANCEL_REQUESTED"}:
                return True
            time.sleep(0.25)
        return False

    def cutoff_reached():
        now=datetime.now(ET)
        try: hh,mm=[int(x) for x in HARD_CUTOFF_HHMM.split(":")]
        except Exception: hh,mm=16,15
        cut=now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        return now >= cut

    # Ladder builders
    def credit_ladder(bid, ask, mid):
        r=[]
        if ask is not None: r.append(ask)
        if mid is not None:
            r += [mid, clamp_tick(mid-0.05), clamp_tick(mid-0.10)]
            if bid is not None:
                r[-2] = max(r[-2], bid)
                r[-1] = max(r[-1], bid)
        seen=set(); out=[]
        for p in r:
            p=clamp_tick(p)
            if p not in seen:
                seen.add(p); out.append(p)
        return out

    def debit_ladder(bid, ask, mid):
        r=[]
        if bid is not None: r.append(bid)
        if mid is not None:
            p3=clamp_tick(mid+0.05); p4=clamp_tick(mid+0.10)
            if ask is not None:
                p3=min(p3, ask); p4=min(p4, ask)
            r += [mid, p3, p4]
        seen=set(); out=[]
        for p in r:
            p=clamp_tick(max(0.05,p))
            if p not in seen:
                seen.add(p); out.append(p)
        return out

    # Run
    START_TS=time.time()
    filled=0; active_oid=""; steps=[]; canceled=0

    def rung(price, secs):
        nonlocal active_oid, filled
        to_place=max(0, QTY - filled)
        if to_place==0: return "FILLED"
        print(f"RUNG → price={clamp_tick(price):.2f} to_place={to_place}")
        # cancel active first
        if active_oid:
            cancel_order(active_oid); wait_until_closed(active_oid); active_oid=""
        active_oid = place(price, to_place)
        if not active_oid:
            print("WARN: place failed"); return "WORKING"
        # wait dwell
        t_end=time.time()+max(secs, MIN_RUNG_WAIT)
        while time.time()<t_end:
            if cutoff_reached() or (time.time()-START_TS>MAX_RUNTIME_SECS): break
            st=get_status(active_oid)
            status=str(st.get("status") or st.get("orderStatus") or "").upper()
            fq=int(round(float(st.get("filledQuantity") or st.get("filled_quantity") or 0)))
            if fq>filled: filled=fq
            if status=="FILLED" or filled>=QTY:
                break
            time.sleep(0.25)
        steps.append(f"{clamp_tick(price):.2f}@{to_place}")
        return "FILLED" if filled>=QTY else ("TIMEOUT" if cutoff_reached() or (time.time()-START_TS>MAX_RUNTIME_SECS) else "WORKING")

    cycles=0
    while cycles<MAX_LADDER_CYCLES and filled<QTY and not cutoff_reached():
        bid,ask,mid = nbbo()
        print(f"{SIDE} NBBO: bid={bid} ask={ask} mid={mid}")
        ladder = credit_ladder(bid,ask,mid) if SIDE=="CREDIT" else debit_ladder(bid,ask,mid)
        print(f"CYCLE {cycles+1}/{MAX_LADDER_CYCLES} ladder: {ladder}")
        secs = (STEP_WAIT_CREDIT if SIDE=="CREDIT" else STEP_WAIT_DEBIT)
        for p in ladder:
            st=rung(p, secs)
            if st in ("FILLED","TIMEOUT"): break
        if filled>=QTY or cutoff_reached(): break
        # cancel active before next cycle
        if active_oid:
            cancel_order(active_oid); wait_until_closed(active_oid); active_oid=""; canceled+=1
        # refresh Leo & legs between cycles
        try:
            j2=gw_fetch(); tr2=extract_trade(j2)
            if tr2:
                exp6=yymmdd(str(tr2.get("TDate","")))
                inner_put=int(float(tr2.get("Limit"))); inner_call=int(float(tr2.get("CLimit")))
                legs=build_legs_same_shorts(exp6, inner_put, inner_call, WIDTH)
                canon={(x[6:12],x[12],x[-8:]) for x in legs}
                print(f"REFRESH_FROM_LEO: legs={legs}")
        except Exception as e:
            print(f"REFRESH_FROM_LEO failed: {e}")
        cycles+=1

    # cleanup
    if active_oid:
        cancel_order(active_oid); wait_until_closed(active_oid); active_oid=""; canceled+=1
    # final sweep
    ex, st, ovs = pick_active_and_overlaps(c, acct_hash, canon)
    for oid in ([ex] if ex else []) + ovs:
        cancel_order(oid); wait_until_closed(oid)

    used_price = steps[-1].split("@",1)[0] if steps else ""
    status_txt = ("FILLED" if filled>=QTY else "TIMEOUT")
    print(f"FINAL {status_txt} STEPS {'→'.join(steps) if steps else ''} | FILLED {filled}/{QTY} | CANCELED {canceled} | width={WIDTH} PRICE_USED={used_price or 'NA'}")

if __name__=="__main__":
    main()
