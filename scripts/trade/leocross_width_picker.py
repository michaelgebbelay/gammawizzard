#!/usr/bin/env python3
# WIDTH PICKER — CREDIT only shape: Call side = (Put width / 2) with 2x quantity
# Emits GHA outputs:
#   picked_put_width, picked_call_width, call_mult,
#   picked_width(=Wp for back-compat), picked_ref(mid), picked_metric, picked_ev, base_ev, delta_ev, five_mid

import os, re, json, math, sys
from datetime import date
import requests
from schwab.auth import client_from_token_file

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

def build_legs(exp6: str, inner_put: int, inner_call: int, Wp: int, Wc: int):
    p_low, p_high = inner_put - Wp, inner_put
    c_low, c_high = inner_call, inner_call + Wc
    bp = to_osi(f".SPXW{exp6}P{p_low}")
    sp = to_osi(f".SPXW{exp6}P{p_high}")
    sc = to_osi(f".SPXW{exp6}C{c_low}")
    bc = to_osi(f".SPXW{exp6}C{c_high}")
    return orient_credit(bp,sp,sc,bc)

# --- Schwab + GW helpers ---
def schwab_client():
    app_key=os.environ["SCHWAB_APP_KEY"]; app_secret=os.environ["SCHWAB_APP_SECRET"]; token_json=os.environ["SCHWAB_TOKEN_JSON"]
    with open("schwab_token.json","w") as f: f.write(token_json)
    return client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")

def fetch_bid_ask(c, osi: str):
    r=c.get_quote(osi)
    if r.status_code!=200: return (None,None)
    d=list(r.json().values())[0] if isinstance(r.json(), dict) else {}
    q=d.get("quote", d)
    b=q.get("bidPrice") or q.get("bid") or q.get("bidPriceInDouble")
    a=q.get("askPrice") or q.get("ask") or q.get("askPriceInDouble")
    return (float(b) if b is not None else None, float(a) if a is not None else None)

def condor_nbbo_credit_ratio(c, legs, m: int):
    # legs = [bp, sp, sc, bc]; call legs scaled by m
    bp, sp, sc, bc = legs
    bp_b, bp_a = fetch_bid_ask(c, bp); sp_b, sp_a = fetch_bid_ask(c, sp)
    sc_b, sc_a = fetch_bid_ask(c, sc); bc_b, bc_a = fetch_bid_ask(c, bc)
    if None in (bp_b, bp_a, sp_b, sp_a, sc_b, sc_a, bc_b, bc_a):
        return (None, None, None, None, None)
    # Per-package NBBO:
    credit_bid = (sp_b + m*sc_b) - (bp_a + m*bc_a)
    credit_ask = (sp_a + m*sc_a) - (bp_b + m*bc_b)
    credit_mid = (credit_bid + credit_ask) / 2.0
    # Simple leg mids for visibility (not used in pricing)
    put_mid  = (sp_b + sp_a)/2.0 - (bp_b + bp_a)/2.0
    call_mid = (sc_b + sc_a)/2.0 - (bc_b + bc_a)/2.0
    return (clamp_tick(credit_bid), clamp_tick(credit_ask), clamp_tick(credit_mid),
            clamp_tick(put_mid), clamp_tick(call_mid))

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

# --- GHA outputs ---
def emit_output(pW:int, cW:int, m:int, mid:float, ev:float, base_ev:float, five_mid:float):
    out_path = os.environ.get("GITHUB_OUTPUT","")
    def w(k,v):
        if not out_path: return
        with open(out_path,"a") as fh: fh.write(f"{k}={v}\n")
    w("picked_put_width", str(pW))
    w("picked_call_width", str(cW))
    w("call_mult", str(m))
    w("picked_width", str(pW))                 # back-compat (old readers used one width)
    w("picked_ref", f"{mid:.2f}" if mid else "")
    w("picked_metric","EV")
    w("picked_ev", f"{ev:.4f}")
    w("base_ev",   f"{base_ev:.4f}")
    w("delta_ev",  f"{(ev-base_ev):.4f}")
    w("five_mid",  f"{five_mid:.2f}" if five_mid else "")
    print(f"::notice title=WidthPicker::Wp={pW} Wc={cW} m={m} base_ev={base_ev:.2f} ΔEV={(ev-base_ev):+.2f} five_mid={(f'{five_mid:.2f}' if five_mid else 'NA')}")

def grid_half_up_to5(x):
    # ceil to nearest 5 (keeps call width >= exact half)
    return int(math.ceil(x/5.0)*5)

def main():
    # Knobs
    DEFAULT_PUT_WIDTH = int(os.environ.get("DEFAULT_CREDIT_WIDTH","20"))
    EV_MIN_ADV = float(os.environ.get("EV_MIN_ADVANTAGE","0.10"))
    CAND_P = [int(x) for x in (os.environ.get("CANDIDATE_CREDIT_WIDTHS","15,20,25,30,40,50").split(","))]
    CALL_MULT = 2  # fixed by design

    # Historical win% (per put width; ratio condor keeps max-loss = Wp - credit)
    WINP = json.loads(os.environ.get("WINP_STD_JSON",
        '{"5":0.714,"15":0.791,"20":0.816,"25":0.840,"30":0.863,"40":0.889,"50":0.916}'))

    # Leo (limits)
    j=gw_fetch(); tr=extract_trade(j)
    if not tr:
        print("WIDTH_PICKER: NO_TRADE_PAYLOAD — using default")
        emit_output(DEFAULT_PUT_WIDTH, grid_half_up_to5(DEFAULT_PUT_WIDTH/2.0), CALL_MULT, 0.0, 0.0, 0.0, 0.0)
        return 0
    exp6 = yymmdd(str(tr.get("TDate","")))
    inner_put  = int(float(tr.get("Limit")))
    inner_call = int(float(tr.get("CLimit")))

    # Schwab client
    c = schwab_client()

    # 5‑wide symmetric ref (visibility only)
    legs5 = build_legs(exp6, inner_put, inner_call, 5, 5)
    _,_,five_mid,_,_ = condor_nbbo_credit_ratio(c, legs5, m=1)

    rows=[]
    best=None
    base_ev=None

    for Wp in CAND_P:
        Wc = grid_half_up_to5(Wp/2.0)
        legs = build_legs(exp6, inner_put, inner_call, Wp, Wc)
        bid, ask, mid, put_mid, call_mid = condor_nbbo_credit_ratio(c, legs, m=CALL_MULT)
        if mid is None or mid <= 0:
            rows.append((Wp, Wc, None, None, None, None)); continue
        p = float(WINP.get(str(Wp), WINP.get(Wp, 0.80)))
        # With m=2 and Wc≈Wp/2, max loss per package is (Wp - mid) on either side.
        ev = p*mid - (1.0-p)*(Wp - mid)
        rows.append((Wp, Wc, put_mid, call_mid, mid, ev))
        if Wp == DEFAULT_PUT_WIDTH and base_ev is None: base_ev = ev
        if best is None or ev > best[5] + 1e-12: best = (Wp, Wc, put_mid, call_mid, mid, ev)

    # Table
    print("|Wp|Wc|m|PutMid|CallMid|TotMid|EV|")
    print("|--:|--:|--:|-----:|------:|-----:|--:|")
    for Wp, Wc, pMid, cMid, mid, ev in rows:
        if mid is None:
            print(f"|{Wp}|{Wc}|{CALL_MULT}|NA|NA|NA|NA|")
        else:
            print(f"|{Wp}|{Wc}|{CALL_MULT}|{pMid:.2f}|{cMid:.2f}|{mid:.2f}|{ev:.2f}|")

    if best is None:
        print("WIDTH_PICKER: No quotes — using default")
        emit_output(DEFAULT_PUT_WIDTH, grid_half_up_to5(DEFAULT_PUT_WIDTH/2.0), CALL_MULT, 0.0, 0.0, (base_ev or 0.0), (five_mid or 0.0))
        return 0

    if base_ev is None: base_ev = next((ev for (Wp,Wc,_,_,_,ev) in rows if Wp==DEFAULT_PUT_WIDTH and ev is not None), 0.0)
    Wp_sel, Wc_sel, pMidSel, cMidSel, midSel, evSel = best
    delta = evSel - (base_ev or 0.0)

    # EV gate
    outWp = Wp_sel if (delta > EV_MIN_ADV) else DEFAULT_PUT_WIDTH
    outWc = grid_half_up_to5(outWp/2.0)

    switch_txt = "→ Switch" if delta > EV_MIN_ADV else "→ Stay"
    print(f"**Picked:** Wp={outWp}, Wc={outWc}, m={CALL_MULT}, cond_mid={(midSel or 0.0):.2f} (metric=EV) base_ev={(base_ev or 0.0):.2f} ΔEV={(delta):+.2f} {switch_txt}")

    emit_output(outWp, outWc, CALL_MULT, (midSel or 0.0), (evSel or 0.0), (base_ev or 0.0), (five_mid or 0.0))
    return 0

if __name__=="__main__":
    sys.exit(main())
