#!/usr/bin/env python3
# WIDTH PICKER — Same‑shorts only. No push‑out. Uses EV to choose width.
# Emits GHA outputs: picked_width, picked_ref, picked_metric, picked_ev, base_ev, delta_ev, five_mid

import os, re, json, math, sys
from datetime import date
import requests

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

def _truthy(s: str) -> bool:
    return str(s or "").strip().lower() in {"1","true","t","yes","y","on"}

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

# --- Schwab quote helpers (using schwab-py client session) ---
from schwab.auth import client_from_token_file
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

def emit_output(picked_width:int, picked_ref:float, picked_metric:str,
                picked_ev:float, base_ev:float, delta_ev:float, five_mid:float):
    out_path = os.environ.get("GITHUB_OUTPUT","")
    def w(k,v):
        if not out_path: return
        with open(out_path,"a") as fh: fh.write(f"{k}={v}\n")
    w("picked_width", str(picked_width))
    w("picked_ref",   f"{picked_ref:.2f}" if picked_ref else "")
    w("picked_metric", picked_metric)
    w("picked_ev",    f"{picked_ev:.4f}")
    w("base_ev",      f"{base_ev:.4f}")
    w("delta_ev",     f"{delta_ev:.4f}")
    w("five_mid",     f"{five_mid:.2f}" if five_mid else "")
    print(f"::notice title=WidthPicker::picked_width={picked_width} metric={picked_metric} base_ev={base_ev:.2f} delta_ev={delta_ev:+.2f} five_mid={(f'{five_mid:.2f}' if five_mid else 'NA')}")

def main():
    # Knobs
    DEFAULT_WIDTH = int(os.environ.get("DEFAULT_CREDIT_WIDTH","20"))
    EV_MIN_ADV = float(os.environ.get("EV_MIN_ADVANTAGE","0.10"))
    CANDS = [int(x) for x in (os.environ.get("CANDIDATE_CREDIT_WIDTHS","15,20,25,30,40,50").split(","))]

    # Historical win% table (same‑shorts). You can override via env.
    WINP = json.loads(os.environ.get("WINP_STD_JSON",
        '{"5":0.714,"15":0.791,"20":0.816,"25":0.840,"30":0.863,"40":0.889,"50":0.916}'))

    # Leo
    j=gw_fetch(); tr=extract_trade(j)
    if not tr:
        print("WIDTH_PICKER: NO_TRADE_PAYLOAD — using default")
        emit_output(DEFAULT_WIDTH, 0.0, "EV", 0.0, 0.0, 0.0, 0.0)
        return 0
    exp6 = yymmdd(str(tr.get("TDate","")))
    inner_put  = int(float(tr.get("Limit")))
    inner_call = int(float(tr.get("CLimit")))

    # Schwab
    c = schwab_client()

    # 5‑wide ref (for operator visibility; not used in EV math)
    legs5 = build_legs(exp6, inner_put, inner_call, 5)
    _,_,five_mid = condor_nbbo_credit(c, legs5)

    rows=[]
    best=None
    base_ev=None
    for W in CANDS:
        legsW = build_legs(exp6, inner_put, inner_call, W)
        b,a,m = condor_nbbo_credit(c, legsW)
        if m is None or m <= 0:
            rows.append((W, None, None, None))
            continue
        p = float(WINP.get(str(W), WINP.get(W, 0.80)))
        ev = p*m - (1.0-p)*(W - m)   # EV per condor (same‑shorts)
        rows.append((W, m, p, ev))
        if W == DEFAULT_WIDTH:
            base_ev = ev
            if best is None: best = (W, m, p, ev)
        # pick by EV
        if best is None or ev > best[3] + 1e-12:
            best = (W, m, p, ev)

    # Print table
    print("|Width|Mid|Win%|EV|Credit/Width|")
    print("|---:|---:|---:|---:|---:|")
    for W, m, p, ev in rows:
        if m is None:
            print(f"|{W}|NA|NA|NA|NA|"); continue
        print(f"|{W}|{m:.2f}|{p*100:.2f}%|{ev:.2f}|{(m/float(W)):.5f}|")

    if best is None:
        print("WIDTH_PICKER: No quotes — using default")
        emit_output(DEFAULT_WIDTH, 0.0, "EV", 0.0, 0.0, 0.0, (five_mid or 0.0))
        return 0

    if base_ev is None:
        base_ev = next((ev for (W,m,p,ev) in rows if W==DEFAULT_WIDTH and ev is not None), 0.0)
    delta = best[3] - (base_ev or 0.0)
    pickedW, pickedMid, pickedP, pickedEV = best

    # EV gate
    width_out = pickedW if (delta > EV_MIN_ADV) else DEFAULT_WIDTH
    print(f"**Picked:** {width_out}-wide @ {pickedMid:.2f if pickedMid else 0.0} (metric=EV), base_ev={(base_ev or 0.0):.2f}, delta_ev={delta:+.2f} {'→ Switch' if delta>EV_MIN_ADV else '→ Stay'}")

    emit_output(width_out, (pickedMid or 0.0), "EV", (pickedEV or 0.0), (base_ev or 0.0), (delta or 0.0), (five_mid or 0.0))
    return 0

if __name__=="__main__":
    sys.exit(main())
