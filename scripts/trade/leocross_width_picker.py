#!/usr/bin/env python3
# WIDTH PICKER — Credit widths only, same‑shorts, no push‑out.
# Picks the width with the highest EV and only switches from DEFAULT_CREDIT_WIDTH
# if ΔEV >= EV_MIN_ADVANTAGE. Prints a table and emits GitHub Actions outputs.

import os, re, json, math, time
from datetime import date
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

def orient(bp,sp,sc,bc):
    bpS=strike_from_osi(bp); spS=strike_from_osi(sp)
    scS=strike_from_osi(sc); bcS=strike_from_osi(bc)
    if bpS>spS: bp,sp = sp,bp
    if scS>bcS: sc,bc = bc,sc
    return [bp,sp,sc,bc]

def build_legs_same_shorts(exp6: str, inner_put: int, inner_call: int, width: int):
    p_low, p_high = inner_put - width, inner_put
    c_low, c_high = inner_call, inner_call + width
    bp = to_osi(f".SPXW{exp6}P{p_low}")
    sp = to_osi(f".SPXW{exp6}P{p_high}")
    sc = to_osi(f".SPXW{exp6}C{c_low}")
    bc = to_osi(f".SPXW{exp6}C{c_high}")
    return orient(bp,sp,sc,bc)

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
        if not (email and pwd):
            raise RuntimeError("GW_AUTH_REQUIRED")
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

def emit(outputs: dict, table_lines=None):
    path=os.environ.get("GITHUB_OUTPUT","")
    if path:
        with open(path,"a") as fh:
            for k,v in outputs.items():
                fh.write(f"{k}={v}\n")
    if table_lines:
        for line in table_lines:
            print(line)

def main():
    # ---------- env ----------
    DEFAULT_W  = int(os.environ.get("DEFAULT_CREDIT_WIDTH","20"))
    EV_GATE    = float(os.environ.get("EV_MIN_ADVANTAGE","0.10"))
    CANDS = [int(x) for x in (os.environ.get("CANDIDATE_CREDIT_WIDTHS","15,20,25,30,40,50").split(","))]
    WINP_STD = json.loads(os.environ.get("WINP_STD_JSON",'{"5":0.714,"15":0.791,"20":0.816,"25":0.840,"30":0.863,"40":0.889,"50":0.916}'))
    RATIO_STD= json.loads(os.environ.get("RATIO_STD_JSON",'{"5":1.0,"15":2.55,"20":3.175,"25":3.6625,"30":4.15,"40":4.85,"50":5.375}'))

    # ---------- Schwab ----------
    app_key=os.environ["SCHWAB_APP_KEY"]; app_secret=os.environ["SCHWAB_APP_SECRET"]; token_json=os.environ["SCHWAB_TOKEN_JSON"]
    with open("schwab_token.json","w") as f: f.write(token_json)
    c=client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")

    # ---------- Leo ----------
    try:
        j=gw_fetch()
    except Exception as e:
        print(f"WIDTH_PICKER: GW fetch failed: {e} — using default {DEFAULT_W}")
        emit({"picked_width":str(DEFAULT_W),"picked_ref":"0.00","picked_metric":"EV","picked_ev":"0.0000",
              "base_ev":"0.0000","delta_ev":"0.0000","five_mid":"0.00"})
        return 0
    tr=extract_trade(j)
    if not tr:
        print(f"WIDTH_PICKER: NO_TRADE — default {DEFAULT_W}")
        emit({"picked_width":str(DEFAULT_W),"picked_ref":"0.00","picked_metric":"EV","picked_ev":"0.0000",
              "base_ev":"0.0000","delta_ev":"0.0000","five_mid":"0.00"})
        return 0

    exp6=yymmdd(str(tr.get("TDate","")))
    inner_put=int(float(tr.get("Limit"))); inner_call=int(float(tr.get("CLimit")))
    legs5=build_legs_same_shorts(exp6, inner_put, inner_call, 5)
    _, _, five_mid = condor_nbbo_credit(c, legs5)

    rows=[]
    best=None
    ev_map={}
    mid_map={}
    for W in CANDS:
        legs=build_legs_same_shorts(exp6, inner_put, inner_call, W)
        _,_,mid=condor_nbbo_credit(c, legs)
        if not mid or mid<=0:
            rows.append((W, None, None, None, None, None))
            continue
        pwin = float(WINP_STD.get(str(W), 0.80))
        ev = pwin*mid - (1.0-pwin)*(W - mid)
        base = (float(RATIO_STD.get(str(W), 0.0)) * float(five_mid or 0.0)) if five_mid else 0.0
        edge = (mid - base) if base>0 else 0.0
        rows.append((W, mid, base, edge, pwin, ev))
        ev_map[W]=ev; mid_map[W]=mid
        if best is None or ev > best[1] + 1e-12:
            best=(W, ev)

    base_ev = ev_map.get(DEFAULT_W, 0.0)
    pickW, pickEV = (best if best else (DEFAULT_W, base_ev))
    delta = pickEV - base_ev
    if delta < EV_GATE:
        pickW = DEFAULT_W
        pickEV = base_ev

    # ---------- print table ----------
    print(f"WIDTH_PICKER (same_shorts, EV_gate={EV_GATE:.2f}, baseW={DEFAULT_W})")
    print("|Width|Mid|Baseline|Edge|Win%|EV|Credit/Width|")
    print("|---:|---:|---:|---:|---:|---:|---:|")
    for W, mid, base, edge, pwin, ev in rows:
        if mid is None:
            print(f"|{W}|NA|NA|NA|NA|NA|NA|")
        else:
            cw = (mid/float(W)) if W else 0.0
            print(f"|{W}|{mid:.2f}|{(base or 0):.2f}|{(edge or 0):+0.2f}|{(pwin or 0):.2%}|{(ev or 0):.2f}|{cw:.5f}|")
    pref = mid_map.get(pickW, 0.0)
    be   = base_ev
    de   = pickEV - be
    print(f"**Picked:** {pickW}-wide @ {pref:.2f} (metric=EV), base_ev={be:.2f}, delta_ev={de:+.2f}")

    # ---------- outputs ----------
    emit({
        "picked_width": str(pickW),
        "picked_ref":   f"{pref:.2f}",
        "picked_metric":"EV",
        "picked_ev":    f"{pickEV:.4f}",
        "base_ev":      f"{base_ev:.4f}",
        "delta_ev":     f"{de:.4f}",
        "five_mid":     f"{(five_mid or 0.0):.2f}",
    })
    return 0

if __name__=="__main__":
    raise SystemExit(main())
