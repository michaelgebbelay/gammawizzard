#!/usr/bin/env python3
# WIDTH PICKER — Same‑shorts, skew‑aware. Chooses Wp, Wc and optional CALL_MULT.
# Emits GHA outputs:
#  picked_width (back‑compat = max(Wp,Wc)), picked_ref (condor mid),
#  picked_put_width, picked_call_width, call_mult,
#  picked_metric=EV, picked_ev, base_ev, delta_ev, five_mid

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

def build_legs_split(exp6: str, inner_put: int, inner_call: int, Wp: int, Wc: int):
    p_low, p_high = inner_put - Wp, inner_put
    c_low, c_high = inner_call, inner_call + Wc
    bp = to_osi(f".SPXW{exp6}P{p_low}")
    sp = to_osi(f".SPXW{exp6}P{p_high}")
    sc = to_osi(f".SPXW{exp6}C{c_low}")
    bc = to_osi(f".SPXW{exp6}C{c_high}")
    return orient_credit(bp,sp,sc,bc)

def _sanitize_token(t: str) -> str:
    t=(t or "").strip().strip('"').strip("'")
    return t.split(None,1)[1] if t.lower().startswith("bearer ") else t

# ---------- GammaWizard ----------
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

# ---------- Schwab quotes ----------
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

def vertical_nbbo_credit(c, short_osi, long_osi):
    sb,sa = fetch_bid_ask(c, short_osi)
    lb,la = fetch_bid_ask(c, long_osi)
    if None in (sb,sa,lb,la): return (None,None,None)
    bid = sb - la
    ask = sa - lb
    mid = (bid + ask) / 2.0
    return (clamp_tick(bid), clamp_tick(ask), clamp_tick(mid))

def condor_mid_from_verticals(c, bp, sp, sc, bc):
    _,_,put_mid  = vertical_nbbo_credit(c, sp, bp)   # short put – long put
    _,_,call_mid = vertical_nbbo_credit(c, sc, bc)   # short call – long call
    if put_mid is None or call_mid is None: return (None, None, None)
    return (clamp_tick(put_mid), clamp_tick(call_mid), clamp_tick(put_mid + call_mid))

# ---------- outputs ----------
def emit_output(d: dict):
    out_path = os.environ.get("GITHUB_OUTPUT","")
    def w(k,v):
        if not out_path: return
        with open(out_path,"a") as fh: fh.write(f"{k}={v}\n")
    for k,v in d.items(): w(k, v)

# ---------- main ----------
def main():
    # Knobs
    DEFAULT_WIDTH = int(os.environ.get("DEFAULT_CREDIT_WIDTH","20"))
    EV_MIN_ADV    = float(os.environ.get("EV_MIN_ADVANTAGE","0.10"))
    CANDS         = [int(x) for x in (os.environ.get("CANDIDATE_CREDIT_WIDTHS","15,20,25,30,40,50").split(","))]
    MIN_CALL_CR   = float(os.environ.get("MIN_CALL_CREDIT","4.00"))
    CALL_RATIO_MIN= float(os.environ.get("CALL_TO_PUT_RATIO_MIN","0.50"))  # call credit >= 50% of put credit
    MAX_CALL_MULT = int(os.environ.get("MAX_CALL_MULT","2"))

    # Historical win% proxy keyed by width (fallback 0.8)
    WINP = json.loads(os.environ.get("WINP_STD_JSON",
        '{"5":0.714,"15":0.791,"20":0.816,"25":0.840,"30":0.863,"40":0.889,"50":0.916}'))

    # Leo
    tr=extract_trade(gw_fetch())
    if not tr:
        print("WIDTH_PICKER: NO_TRADE_PAYLOAD — using default")
        emit_output({"picked_width":str(DEFAULT_WIDTH),"picked_ref":"","picked_metric":"EV",
                     "picked_ev":"0.0000","base_ev":"0.0000","delta_ev":"0.0000",
                     "five_mid":"","picked_put_width":str(DEFAULT_WIDTH),"picked_call_width":str(DEFAULT_WIDTH),
                     "call_mult":"1"})
        return 0
    exp6 = yymmdd(str(tr.get("TDate","")))
    inner_put  = int(float(tr.get("Limit")))
    inner_call = int(float(tr.get("CLimit")))

    # Schwab
    c = schwab_client()

    # 5‑wide ref (visibility)
    bp5,sp5,sc5,bc5 = build_legs_split(exp6, inner_put, inner_call, 5, 5)
    _,_,five_mid = condor_mid_from_verticals(c, bp5,sp5,sc5,bc5)

    # grid search Wp, Wc
    rows=[]
    best=None
    base_ev=None
    baseW=DEFAULT_WIDTH

    for Wp in CANDS:
        for Wc in CANDS:
            bp,sp,sc,bc = build_legs_split(exp6, inner_put, inner_call, Wp, Wc)
            put_mid, call_mid, cond_mid = condor_mid_from_verticals(c, bp,sp,sc,bc)
            if cond_mid is None or cond_mid <= 0:
                rows.append((Wp,Wc,None,None,None)); continue
            p = float(WINP.get(str(max(Wp,Wc)), 0.80))
            ev = p*cond_mid - (1.0-p)*(max(Wp,Wc) - cond_mid)
            rows.append((Wp,Wc,put_mid,call_mid,ev))
            if (Wp==baseW and Wc==baseW):
                base_ev = ev
                if best is None: best = (Wp,Wc,put_mid,call_mid,ev)
            if best is None or ev > best[4] + 1e-12:
                best = (Wp,Wc,put_mid,call_mid,ev)

    # print compact table
    print("|Wp|Wc|PutMid|CallMid|EV|")
    print("|--:|--:|-----:|------:|--:|")
    for Wp,Wc,pm,cm,ev in rows:
        if pm is None:
            print(f"|{Wp}|{Wc}|NA|NA|NA|"); continue
        print(f"|{Wp}|{Wc}|{pm:.2f}|{cm:.2f}|{ev:.2f}|")

    if best is None:
        print("WIDTH_PICKER: No quotes — using default")
        emit_output({"picked_width":str(DEFAULT_WIDTH),"picked_ref":"","picked_metric":"EV",
                     "picked_ev":"0.0000","base_ev":"0.0000","delta_ev":"0.0000",
                     "five_mid":(f"{five_mid:.2f}" if five_mid else ""),
                     "picked_put_width":str(DEFAULT_WIDTH),"picked_call_width":str(DEFAULT_WIDTH),
                     "call_mult":"1"})
        return 0

    if base_ev is None:
        base_ev = next((ev for (Wp,Wc,pm,cm,ev) in rows if Wp==baseW and Wc==baseW and ev is not None), 0.0)

    Wp_b,Wc_b,pm_b,cm_b,ev_b = best
    delta = ev_b - (base_ev or 0.0)

    # EV gate still controls switching vs base width
    use_Wp = Wp_b if (delta > EV_MIN_ADV) else baseW
    use_Wc = Wc_b if (delta > EV_MIN_ADV) else baseW

    # recompute combined mid for display at chosen Wp/Wc
    bp,sp,sc,bc = build_legs_split(exp6, inner_put, inner_call, use_Wp, use_Wc)
    pm_use, cm_use, cond_mid_use = condor_mid_from_verticals(c, bp,sp,sc,bc)

    # enforce call-credit sanity at the chosen widths
    call_target = max(MIN_CALL_CR, CALL_RATIO_MIN * (pm_use or 0.0))
    call_mult = 1
    if cm_use and cm_use > 0 and cm_use < call_target:
        call_mult = min(MAX_CALL_MULT, int(math.ceil(call_target / cm_use)))

    picked = {
        "picked_width": str(max(use_Wp,use_Wc)),
        "picked_ref":   f"{(cond_mid_use or 0.0):.2f}",
        "picked_metric":"EV",
        "picked_ev":    f"{(ev_b or 0.0):.4f}",
        "base_ev":      f"{(base_ev or 0.0):.4f}",
        "delta_ev":     f"{(delta or 0.0):.4f}",
        "five_mid":     (f"{five_mid:.2f}" if five_mid else ""),
        "picked_put_width":  str(use_Wp),
        "picked_call_width": str(use_Wc),
        "call_mult":    str(call_mult)
    }
    print(f"**Picked:** Wp={use_Wp}, Wc={use_Wc}, cond_mid={(cond_mid_use or 0.0):.2f} "
          f"(metric=EV) base_ev={(base_ev or 0.0):.2f} ΔEV={(delta or 0.0):+.2f} "
          f"→ {'Switch' if delta>EV_MIN_ADV else 'Stay'}; CALL_MULT={call_mult}")
    emit_output(picked)
    return 0

if __name__=="__main__":
    sys.exit(main())
