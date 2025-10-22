#!/usr/bin/env python3
# WIDTH PICKER — CREDIT-ONLY. Skew-aware (split call width; optional call multiplicity).
# Emits GHA outputs ONLY for CREDIT days:
#   picked_put_width, picked_call_width, call_mult,
#   picked_width (legacy = picked_put_width),
#   picked_ref (total mid), picked_metric, picked_ev, base_ev, delta_ev, five_mid

import os, re, json, math, sys
from datetime import date
import requests
from schwab.auth import client_from_token_file

TICK = 0.05

# ---------- small utils ----------
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

def build_put_legs(exp6: str, short_put: int, width_p: int):
    p_low, p_high = short_put - width_p, short_put
    return orient_credit(
        to_osi(f".SPXW{exp6}P{p_low}"),
        to_osi(f".SPXW{exp6}P{p_high}"),
        "SPXW000000C00000000",  # placeholder to keep tuple shape (ignored)
        "SPXW000000C00000000",
    )[:2]

def build_call_legs(exp6: str, short_call: int, width_c: int):
    c_low, c_high = short_call, short_call + width_c
    return orient_credit(
        "SPXW000000P00000000",
        "SPXW000000P00000000",
        to_osi(f".SPXW{exp6}C{c_low}"),
        to_osi(f".SPXW{exp6}C{c_high}")
    )[2:]

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

# --- Schwab quote helpers ---
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

def vertical_credit_mid(c, short_osi: str, long_osi: str):
    sb, sa = fetch_bid_ask(c, short_osi)
    lb, la = fetch_bid_ask(c, long_osi)
    if None in (sb, sa, lb, la): return None
    bid = sb - la
    ask = sa - lb
    return clamp_tick((bid + ask) / 2.0)

# ---------- outputs ----------
def emit_output(picked_put:int|None, picked_call:int|None, call_mult:int|None,
                picked_ref:float, picked_metric:str,
                picked_ev:float, base_ev:float, delta_ev:float,
                five_mid:float|None, is_credit_flag:bool):
    out_path = os.environ.get("GITHUB_OUTPUT","")
    def w(k,v):
        if not out_path: return
        with open(out_path,"a") as fh: fh.write(f"{k}={v}\n")
    if is_credit_flag:
        w("picked_put_width", str(picked_put or ""))
        w("picked_call_width", str(picked_call or ""))
        w("call_mult",        str(call_mult or "1"))
        w("picked_width",     str(picked_put or ""))  # legacy
        w("picked_ref",       f"{picked_ref:.2f}" if picked_ref else "")
        w("picked_metric",    picked_metric)
        w("picked_ev",        f"{picked_ev:.4f}")
        w("base_ev",          f"{base_ev:.4f}")
        w("delta_ev",         f"{delta_ev:.4f}")
        w("five_mid",         f"{(five_mid or 0.0):.2f}")
    else:
        # blank everything for debit days
        for k in ("picked_put_width","picked_call_width","call_mult",
                  "picked_width","picked_ref","picked_metric","picked_ev","base_ev","delta_ev","five_mid"):
            w(k, "")
    print(f"::notice title=WidthPicker::{('CREDIT' if is_credit_flag else 'DEBIT')} day; outputs {'emitted' if is_credit_flag else 'suppressed'}")

def fnum(x):
    try: return float(x)
    except: return None

def is_credit_signal(tr: dict) -> bool:
    c1=fnum(tr.get("Cat1")); c2=fnum(tr.get("Cat2"))
    if (c1 is not None) and (c2 is not None):
        return c2 >= c1
    # Unknown? Do NOT apply picker.
    return False

# ---------- main ----------
def main():
    # Knobs
    DEFAULT_WIDTH = int(os.environ.get("DEFAULT_CREDIT_WIDTH","20"))
    EV_MIN_ADV = float(os.environ.get("EV_MIN_ADVANTAGE","0.10"))
    PUT_CANDS  = [int(x) for x in (os.environ.get("CANDIDATE_PUT_WIDTHS","15,20,25,30,40,50").split(","))]
    CALL_CANDS = [int(x) for x in (os.environ.get("CANDIDATE_CALL_WIDTHS","15,20,25,30,40,50").split(","))]
    CALL_MULTS = [int(x) for x in (os.environ.get("CALL_MULT_OPTIONS","1,2").split(","))]

    # Win% table by width
    WINP = json.loads(os.environ.get("WINP_STD_JSON",
        '{"5":0.714,"15":0.791,"20":0.816,"25":0.840,"30":0.863,"40":0.889,"50":0.916}'))

    # GW
    j=gw_fetch(); tr=extract_trade(j)
    if not tr:
        emit_output(None,None,None,0.0,"EV",0.0,0.0,0.0,0.0,False); return 0

    # CREDIT-ONLY gate
    credit = is_credit_signal(tr)
    exp6 = yymmdd(str(tr.get("TDate","")))
    short_put  = int(float(tr.get("Limit")))
    short_call = int(float(tr.get("CLimit")))

    # Schwab client
    c = schwab_client()

    # 5‑wide reference condor mid (info only)
    try:
        p5 = vertical_credit_mid(c, *build_put_legs(exp6, short_put, 5))
        c5 = vertical_credit_mid(c, *build_call_legs(exp6, short_call, 5))
        five_mid = clamp_tick((p5 or 0.0) + (c5 or 0.0))
    except Exception:
        five_mid = 0.0

    if not credit:
        print("WIDTH_PICKER: DEBIT signal → bypass (no outputs).")
        emit_output(None,None,None,0.0,"NA",0.0,0.0,0.0,five_mid,False)
        return 0

    # Baseline (symmetric DEFAULT_WIDTH, m=1)
    base_put_mid  = vertical_credit_mid(c, *build_put_legs(exp6, short_put, DEFAULT_WIDTH))
    base_call_mid = vertical_credit_mid(c, *build_call_legs(exp6, short_call, DEFAULT_WIDTH))
    base_tot_mid  = clamp_tick((base_put_mid or 0.0) + (base_call_mid or 0.0))
    p_base = float(WINP.get(str(DEFAULT_WIDTH), 0.80))
    ev_base = p_base*(base_put_mid or 0.0) - (1-p_base)*(DEFAULT_WIDTH - (base_put_mid or 0.0)) \
            + p_base*(base_call_mid or 0.0) - (1-p_base)*(DEFAULT_WIDTH - (base_call_mid or 0.0))

    # Grid
    print("|Wp|Wc|m|PutMid|CallMid|TotMid|EV|")
    print("|--:|--:|--:|-----:|------:|-----:|--:|")

    best=None
    for Wp in PUT_CANDS:
        p_mid = vertical_credit_mid(c, *build_put_legs(exp6, short_put, Wp))
        if p_mid is None: 
            for Wc in CALL_CANDS:
                for m in CALL_MULTS:
                    print(f"|{Wp}|{Wc}|{m}|NA|NA|NA|NA|")
            continue
        p_win = float(WINP.get(str(Wp), 0.80))
        for Wc in CALL_CANDS:
            c_mid = vertical_credit_mid(c, *build_call_legs(exp6, short_call, Wc))
            if c_mid is None:
                for m in CALL_MULTS:
                    print(f"|{Wp}|{Wc}|{m}|{p_mid:.2f}|NA|NA|NA|")
                continue
            c_win = float(WINP.get(str(Wc), 0.80))
            for m in CALL_MULTS:
                tot_mid = clamp_tick(p_mid + m*c_mid)
                ev = (p_win*p_mid - (1-p_win)*(Wp - p_mid)) \
                   + m*(c_win*c_mid - (1-c_win)*(Wc - c_mid))
                print(f"|{Wp}|{Wc}|{m}|{p_mid:.2f}|{c_mid:.2f}|{tot_mid:.2f}|{ev:.2f}|")
                if (best is None) or (ev > best[5] + 1e-12):
                    best=(Wp,Wc,m,p_mid,c_mid,ev,tot_mid)

    if best is None:
        emit_output(None,None,None,0.0,"EV",0.0,ev_base,0.0,five_mid,True)
        return 0

    Wp_best, Wc_best, m_best, p_mid_b, c_mid_b, ev_best, tot_mid_b = best
    delta = ev_best - ev_base

    # EV gate vs baseline (DEFAULT_WIDTH, m=1)
    if delta <= EV_MIN_ADV:
        Wp_sel, Wc_sel, m_sel, tot_mid_sel, ev_sel = DEFAULT_WIDTH, DEFAULT_WIDTH, 1, base_tot_mid, ev_base
        switch_txt = "→ Stay"
    else:
        Wp_sel, Wc_sel, m_sel, tot_mid_sel, ev_sel = Wp_best, Wc_best, m_best, tot_mid_b, ev_best
        switch_txt = "→ Switch"

    print(f"**Picked:** Wp={Wp_sel}, Wc={Wc_sel}, m={m_sel}, cond_mid={tot_mid_sel:.2f} (metric=EV) base_ev={ev_base:.2f} ΔEV={delta:+.2f} {switch_txt}")

    # Emit (CREDIT only)
    emit_output(Wp_sel, Wc_sel, m_sel, tot_mid_sel, "EV", ev_sel, ev_base, delta, five_mid, True)
    return 0

if __name__=="__main__":
    sys.exit(main())
