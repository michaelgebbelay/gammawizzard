#!/usr/bin/env python3
# WIDTH PICKER — CREDIT ONLY
# - Same-shorts core (Leo inner strikes) with asymmetric call-side multiplier.
# - Skips entirely on DEBIT signals (emits blank outputs).
# - Outputs (for GHA): picked_put_width, picked_call_width, call_mult,
#   picked_width (alias = put width), picked_ref (condor mid), picked_metric,
#   picked_ev, base_ev, delta_ev, five_mid

import os, re, json, math, sys, time
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

def _sanitize_token(t: str) -> str:
    t=(t or "").strip().strip('"').strip("'")
    return t.split(None,1)[1] if t.lower().startswith("bearer ") else t

# ---------- GammaWizard ----------
def gw_fetch():
    base = os.environ.get("GW_BASE","https://gandalf.gammawizard.com").rstrip("/")
    endpoint = os.environ.get("GW_ENDPOINT","/rapi/GetLeoCross")
    tok=_sanitize_token(os.environ.get("GW_TOKEN","") or "")
    def hit(t):
        h={"Accept":"application/json"}
        if t: h["Authorization"]=f"Bearer {_sanitize_token(t)}"
        return requests.get(f"{base}/{endpoint.lstrip('/')}", headers=h, timeout=30)
    r = hit(tok) if tok else None
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

def vertical_credit_nbbo_mid(c, short_osi: str, long_osi: str):
    sb, sa = fetch_bid_ask(c, short_osi); lb, la = fetch_bid_ask(c, long_osi)
    if None in (sb, sa, lb, la): return None
    bid = sb - la
    ask = sa - lb
    return clamp_tick((bid + ask)/2.0)

def condor_nbbo_mid(c, bp, sp, sc, bc):
    bp_b, bp_a = fetch_bid_ask(c, bp); sp_b, sp_a = fetch_bid_ask(c, sp)
    sc_b, sc_a = fetch_bid_ask(c, sc); bc_b, bc_a = fetch_bid_ask(c, bc)
    if None in (bp_b, bp_a, sp_b, sp_a, sc_b, sc_a, bc_b, bc_a): return None
    credit_bid = (sp_b + sc_b) - (bp_a + bc_a)
    credit_ask = (sp_a + sc_a) - (bp_b + bc_b)
    return clamp_tick((credit_bid + credit_ask)/2.0)

# ---------- GHA outputs ----------
def gha_write(k,v):
    out_path = os.environ.get("GITHUB_OUTPUT","")
    if not out_path: return
    with open(out_path,"a") as fh: fh.write(f"{k}={v}\n")

def emit_outputs_credit(Wp, Wc, m, cond_mid, metric, picked_ev, base_ev, delta_ev, five_mid):
    gha_write("picked_put_width", str(Wp))
    gha_write("picked_call_width", str(Wc))
    gha_write("call_mult", str(m))
    gha_write("picked_width", str(Wp))                 # backward compat (alias)
    gha_write("picked_ref", f"{cond_mid:.2f}")
    gha_write("picked_metric", metric)
    gha_write("picked_ev", f"{picked_ev:.4f}")
    gha_write("base_ev", f"{base_ev:.4f}")
    gha_write("delta_ev", f"{delta_ev:.4f}")
    gha_write("five_mid", f"{five_mid:.2f}" if five_mid is not None else "")

def emit_outputs_skip(five_mid=None):
    # Blank key outputs so workflow fallbacks kick in (and debit logic ignores picker)
    for k in ["picked_put_width","picked_call_width","call_mult","picked_width","picked_ref","picked_metric","picked_ev","base_ev","delta_ev"]:
        gha_write(k, "")
    gha_write("five_mid", f"{five_mid:.2f}" if five_mid is not None else "")

# ---------- main ----------
def main():
    # Knobs
    DEFAULT_PUT_WIDTH  = int(os.environ.get("DEFAULT_CREDIT_WIDTH","20"))
    DEFAULT_CALL_WIDTH = int(os.environ.get("DEFAULT_CALL_WIDTH", str(DEFAULT_PUT_WIDTH)))
    EV_MIN_ADV         = float(os.environ.get("EV_MIN_ADVANTAGE","0.10"))
    PUT_CANDS  = [int(x) for x in (os.environ.get("CANDIDATE_PUT_WIDTHS","15,20,25,30,40,50").split(","))]
    CALL_CANDS = [int(x) for x in (os.environ.get("CANDIDATE_CALL_WIDTHS","15,20,25,30,40,50").split(","))]
    CALL_MULTS = [int(x) for x in (os.environ.get("CALL_MULT_CANDS","1,2").split(","))]

    # Historical win% table keyed by max risk width
    WINP = json.loads(os.environ.get("WINP_STD_JSON",
        '{"5":0.714,"15":0.791,"20":0.816,"25":0.840,"30":0.863,"40":0.889,"50":0.916}'))

    # Leo
    j=gw_fetch(); tr=extract_trade(j)
    if not tr:
        print("WIDTH_PICKER: NO_TRADE_PAYLOAD — skipping.")
        emit_outputs_skip()
        return 0

    exp6 = yymmdd(str(tr.get("TDate","")))
    inner_put  = int(float(tr.get("Limit")))
    inner_call = int(float(tr.get("CLimit")))

    # CREDIT vs DEBIT (credit when Cat2 >= Cat1, or Cat* missing → treat as credit-ish)
    def fnum(x):
        try: return float(x)
        except: return None
    cat1=fnum(tr.get("Cat1")); cat2=fnum(tr.get("Cat2"))
    is_credit = True if (cat2 is None or cat1 is None or cat2 >= cat1) else False

    # Schwab
    c = schwab_client()

    # 5-wide reference condor mid (info only)
    bp5 = to_osi(f".SPXW{exp6}P{inner_put-5}")
    sp5 = to_osi(f".SPXW{exp6}P{inner_put}")
    sc5 = to_osi(f".SPXW{exp6}C{inner_call}")
    bc5 = to_osi(f".SPXW{exp6}C{inner_call+5}")
    five_mid = condor_nbbo_mid(c, bp5, sp5, sc5, bc5)

    # --- CREDIT-ONLY GATE ---
    if not is_credit:
        print("WIDTH_PICKER CREDIT_ONLY: Leo indicates DEBIT — skipping width selection.")
        emit_outputs_skip(five_mid=five_mid)
        return 0

    # Build tables over (Wp, Wc, m)
    rows=[]  # (Wp, Wc, m, put_mid, call_mid, tot_mid, ratio, ev)
    best=None
    base_ev=None

    # Prebuild quote function closures
    def put_vert_mid(Wp):
        bp = to_osi(f".SPXW{exp6}P{inner_put-Wp}")
        sp = to_osi(f".SPXW{exp6}P{inner_put}")
        return vertical_credit_nbbo_mid(c, sp, bp)

    def call_vert_mid(Wc):
        sc = to_osi(f".SPXW{exp6}C{inner_call}")
        bc = to_osi(f".SPXW{exp6}C{inner_call+Wc}")
        return vertical_credit_nbbo_mid(c, sc, bc)

    cache_put = {}
    cache_call = {}

    for Wp in PUT_CANDS:
        pm = cache_put.get(Wp); 
        if pm is None:
            pm = put_vert_mid(Wp)
            cache_put[Wp] = pm
        for Wc in CALL_CANDS:
            cm_one = cache_call.get(Wc)
            if cm_one is None:
                cm_one = call_vert_mid(Wc)
                cache_call[Wc] = cm_one
            for m in CALL_MULTS:
                if pm is None or cm_one is None: 
                    continue
                tot_mid = clamp_tick(pm + m*cm_one)
                max_risk = float(max(Wp, m*Wc))
                p = float(WINP.get(str(int(max_risk)), WINP.get(int(max_risk), 0.80)))
                ev = p*tot_mid - (1.0-p)*(max_risk - tot_mid)
                ratio = (tot_mid / max_risk) if max_risk>0 else 0.0
                rows.append((Wp, Wc, m, pm, cm_one*m, tot_mid, ratio, ev))
                if (Wp==DEFAULT_PUT_WIDTH and Wc==DEFAULT_CALL_WIDTH and m==1):
                    base_ev = ev
                    if best is None: best=(Wp,Wc,m,tot_mid,ev)
                if best is None or ev > best[4] + 1e-12:
                    best=(Wp,Wc,m,tot_mid,ev)

    # Print table
    print("|Wp|Wc|m|PutMid|CallMid|TotMid|Ratio|EV|Credit/Risk|")
    print("|--:|--:|--:|-----:|------:|-----:|----:|--:|---------:|")
    for Wp,Wc,m,pm,cm,tm,ratio,ev in rows:
        print(f"|{Wp}|{Wc}|{m}|{pm:.2f}|{cm:.2f}|{tm:.2f}|{ratio:.2f}|{ev:.2f}|{tm/max(1, max(Wp, m*Wc)):.4f}|")

    if best is None:
        print("WIDTH_PICKER: No valid quotes — skipping.")
        emit_outputs_skip(five_mid=five_mid)
        return 0

    if base_ev is None:
        # fallback: base=DEFAULT widths if present; else 0
        base_ev = 0.0
        for Wp,Wc,m,pm,cm,tm,ratio,ev in rows:
            if Wp==DEFAULT_PUT_WIDTH and Wc==DEFAULT_CALL_WIDTH and m==1:
                base_ev = ev
                break

    pickedWp, pickedWc, pickedM, cond_mid, pickedEV = best
    delta = pickedEV - base_ev

    width_out_wp = pickedWp if (delta > EV_MIN_ADV) else DEFAULT_PUT_WIDTH
    width_out_wc = pickedWc if (delta > EV_MIN_ADV) else DEFAULT_CALL_WIDTH
    mult_out     = pickedM   if (delta > EV_MIN_ADV) else 1

    switch_txt = "→ Switch" if delta > EV_MIN_ADV else "→ Stay"
    print(f"**Picked:** Wp={width_out_wp}, Wc={width_out_wc}, m={mult_out}, cond_mid={cond_mid:.2f} (metric=EV) base_ev={base_ev:.2f} ΔEV={delta:+.2f} {switch_txt}")

    emit_outputs_credit(width_out_wp, width_out_wc, mult_out, cond_mid, "EV", pickedEV, base_ev, delta, five_mid)
    return 0

if __name__=="__main__":
    sys.exit(main())
