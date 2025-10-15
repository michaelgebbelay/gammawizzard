#!/usr/bin/env python3
# WIDTH PICKER — SAME‑SHORTS ONLY (no push‑out)
# Emits GitHub outputs: picked_width, picked_ref, picked_metric=EV,
# picked_ev, base_ev, delta_ev, five_mid, picker_diag
#
# Metrics:
#   - EV per condor using historical win% by width (same‑shorts)
#   - Baseline from 5‑wide mid × ratios (same‑shorts only)
#   - Gate: switch from base width only if ΔEV >= EV_GATE
#
# ENV:
#   SCHWAB_APP_KEY / SCHWAB_APP_SECRET / SCHWAB_TOKEN_JSON  (required)
#   GW_TOKEN or (GW_EMAIL + GW_PASSWORD)                    (required)
#   DEFAULT_CREDIT_WIDTH   default 20
#   CANDIDATE_CREDIT_WIDTHS "15,20,25,30,40,50"
#   EV_GATE                 default 0.10
#   SELECTOR_USE            MID | ASK (default MID)
#   RATIO_STD_JSON          JSON of same‑shorts ratios vs 5‑wide
#   HIST_WIN_JSON           JSON of historical win% by width (same‑shorts)
#
# NOTE: 5‑wide shorts are never pushed (same‑shorts template for all W).

import os, re, json, math, requests
from datetime import date
from zoneinfo import ZoneInfo
from schwab.auth import client_from_token_file

ET = ZoneInfo("America/New_York")
TICK = 0.05

# ---------- helpers ----------
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

def osi_canon(osi: str): return (osi[6:12], osi[12], osi[-8:])
def strike_from_osi(osi: str) -> float: return int(osi[-8:]) / 1000.0

def orient(bp,sp,sc,bc):
    # SELL PUT spread -> sell higher strike (sp), buy lower strike (bp)
    # SELL CALL spread -> sell lower strike (sc), buy higher strike (bc)
    bpS=strike_from_osi(bp); spS=strike_from_osi(sp)
    scS=strike_from_osi(sc); bcS=strike_from_osi(bc)
    if bpS > spS: bp, sp = sp, bp
    if scS > bcS: sc, bc = bc, sc
    return [bp,sp,sc,bc]

def build_legs_same_shorts(width:int, exp6:str, inner_put:int, inner_call:int):
    # SAME‑SHORTS for credit IC
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

def _truthy(s:str)->bool:
    return str(s or "").strip().lower() in {"1","true","t","yes","y","on"}

def _sanitize_token(t: str) -> str:
    t=(t or "").strip().strip('"').strip("'")
    return t.split(None,1)[1] if t.lower().startswith("bearer ") else t

def gw_login_token(base: str, email: str, pwd: str):
    r=requests.post(f"{base}/goauth/authenticateFireUser", data={"email":email,"password":pwd}, timeout=30)
    r.raise_for_status(); j=r.json(); t=j.get("token")
    if not t: raise RuntimeError("GW_LOGIN_NO_TOKEN")
    return t

def gw_get_leocross(base: str, endpoint: str, token: str|None, email: str|None, pwd: str|None):
    tok=_sanitize_token(token or "")
    def hit(t):
        h={"Accept":"application/json","Authorization":f"Bearer {_sanitize_token(t)}","User-Agent":"width-picker/ss-1.0"}
        return requests.get(f"{base.rstrip('/')}/{endpoint.lstrip('/')}", headers=h, timeout=30)
    r=hit(tok) if tok else None
    if (r is None) or (r.status_code in (401,403)):
        if not (email and pwd):
            raise RuntimeError("GW_403_AND_NO_CREDS")
        t = gw_login_token(base, email, pwd)
        r=hit(t)
    if r.status_code!=200: raise RuntimeError(f"GW_HTTP_{r.status_code}:{(r.text or '')[:180]}")
    return r.json()

def extract_trade(j):
    if isinstance(j,dict):
        if "Trade" in j:
            tr=j["Trade"]; 
            return tr[-1] if isinstance(tr,list) and tr else tr if isinstance(tr,dict) else {}
        for v in j.values():
            if isinstance(v,(dict,list)):
                t=extract_trade(v)
                if t: return t
    if isinstance(j,list):
        for it in reversed(j):
            t=extract_trade(it)
            if t: return t
    return {}

# ---------- main ----------
def main():
    # ENV
    GW_BASE = os.environ.get("GW_BASE","https://gandalf.gammawizard.com")
    GW_ENDPOINT = os.environ.get("GW_ENDPOINT","/rapi/GetLeoCross")
    GW_TOKEN = os.environ.get("GW_TOKEN","")
    GW_EMAIL = os.environ.get("GW_EMAIL","")
    GW_PASSWORD = os.environ.get("GW_PASSWORD","")

    DEFAULT_WIDTH  = int(os.environ.get("DEFAULT_CREDIT_WIDTH","20"))
    CANDS          = [int(x) for x in (os.environ.get("CANDIDATE_CREDIT_WIDTHS","15,20,25,30,40,50").split(","))]
    EV_GATE        = float(os.environ.get("EV_GATE","0.10"))
    SELECTOR       = (os.environ.get("SELECTOR_USE","MID") or "MID").upper()

    # Same‑shorts ratios only
    RATIO_STD = json.loads(os.environ.get(
        "RATIO_STD_JSON",
        '{"5":1.0,"15":2.55,"20":3.175,"25":3.6625,"30":4.15,"40":4.85,"50":5.375}'
    ))

    # Historical win% (same‑shorts) — use your calibrated numbers
    HIST_WIN = json.loads(os.environ.get(
        "HIST_WIN_JSON",
        '{"5":0.7141,"15":0.7906,"20":0.8156,"25":0.8400,"30":0.8625,"40":0.8891,"50":0.9156}'
    ))

    # Schwab
    app_key=os.environ["SCHWAB_APP_KEY"]; app_secret=os.environ["SCHWAB_APP_SECRET"]; token_json=os.environ["SCHWAB_TOKEN_JSON"]
    with open("schwab_token.json","w") as f: f.write(token_json)
    c=client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")

    # Leo
    api = gw_get_leocross(GW_BASE, GW_ENDPOINT, GW_TOKEN, GW_EMAIL, GW_PASSWORD)
    tr  = extract_trade(api)
    if not tr:
        print("WIDTH_PICKER: NO_TRADE_PAYLOAD — using default")
        return emit(DEFAULT_WIDTH, 0.0, "EV", 0.0, 0.0, 0.0, "NO_TRADE")

    exp6 = yymmdd(str(tr.get("TDate","")))
    inner_put  = int(float(tr.get("Limit")))
    inner_call = int(float(tr.get("CLimit")))

    # 5‑wide same‑shorts reference
    legs5 = build_legs_same_shorts(5, exp6, inner_put, inner_call)
    bid5, ask5, mid5 = condor_nbbo_credit(c, legs5)
    five_ref = (mid5 if SELECTOR=="MID" else ask5)
    if not five_ref or five_ref<=0:
        print("WIDTH_PICKER: No 5‑wide mid/ask — using default")
        return emit(DEFAULT_WIDTH, 0.0, "EV", 0.0, 0.0, 0.0, "NO_5W_QUOTE")

    # Candidate scan (same‑shorts)
    rows=[]
    best=None
    baseW = DEFAULT_WIDTH
    base_ev = None

    for W in CANDS:
        legsW = build_legs_same_shorts(W, exp6, inner_put, inner_call)
        b,a,m = condor_nbbo_credit(c, legsW)
        ref = (m if SELECTOR=="MID" else a)
        if ref is None or ref<=0:
            rows.append((W, None, None, None, None, None))
            continue

        # Baseline credit by ratio
        mult = float(RATIO_STD.get(str(W), 0.0))
        baseline = mult * float(five_ref)

        # Win%
        p = float(HIST_WIN.get(str(W), 0.0))
        q = 1.0 - p
        loss = float(W) - ref
        ev = p*ref - q*loss  # $ per condor

        edge = ref - baseline
        rows.append((W, ref, baseline, edge, p, ev))

        if W == baseW:
            base_ev = ev

        cand = (W, ev)
        if (best is None) or (ev > best[1] + 1e-12):
            best = cand

    # Choose
    if best is None or base_ev is None:
        pickedW, picked_ref, picked_ev = baseW, 0.0, 0.0
        delta_ev = 0.0
    else:
        pickedW, _ev = best
        picked_ref = next((r[1] for r in rows if r[0]==pickedW), 0.0)
        picked_ev  = next((r[5] for r in rows if r[0]==pickedW), 0.0)
        delta_ev   = (picked_ev - base_ev)

    # EV gate — stick with baseW unless gain >= EV_GATE
    if delta_ev < EV_GATE:
        pickedW = baseW
        picked_ref = next((r[1] for r in rows if r[0]==pickedW), picked_ref)
        picked_ev  = base_ev
        delta_ev   = 0.0

    # Nice console table
    print(f"WIDTH_PICKER (same‑shorts only, EV_gate={EV_GATE:.2f}, baseW={baseW})")
    print("|Width|Mid|Baseline|Edge|Win%|EV|Credit/Width|")
    print("|---:|---:|---:|---:|---:|---:|---:|")
    for (W, ref, base, edge, p, ev) in rows:
        if ref is None:
            print(f"|{W}|NA|NA|NA|NA|NA|NA|")
            continue
        cw = (ref / float(W)) if W>0 else 0.0
        print(f"|{W}|{ref:.2f}|{base:.2f}|{edge:+.2f}|{p*100:.2f}%|{ev:.2f}|{cw:.5f}|")
    print(f"**Picked:** {pickedW}-wide @ {picked_ref:.2f} (metric=EV), base_ev={(base_ev or 0.0):.2f}, delta_ev={delta_ev:+.2f}")
    print(f"5‑wide mid reference = {five_ref:.2f}")

    diag = " ; ".join([f"W{r[0]}:EV={(r[5] if r[5] is not None else 'NA')}" for r in rows])
    return emit(pickedW, picked_ref, "EV", picked_ev, (base_ev or 0.0), delta_ev, diag, five_ref)

def emit(width:int, ref:float, metric:str, picked_ev:float, base_ev:float, delta_ev:float, diag:str, five_mid:float=0.0):
    out_path = os.environ.get("GITHUB_OUTPUT","")
    def w(k,v):
        if not out_path: return
        with open(out_path,"a") as fh:
            fh.write(f"{k}={v}\n")
    w("picked_width", str(width))
    w("picked_ref",   f"{(ref or 0.0):.2f}")
    w("picked_metric", metric)
    w("picked_ev",    f"{picked_ev:.4f}")
    w("base_ev",      f"{base_ev:.4f}")
    w("delta_ev",     f"{delta_ev:.4f}")
    w("five_mid",     f"{(five_mid or 0.0):.2f}")
    w("picker_diag",  diag[:900])
    print(f"::notice title=WidthPicker::picked_width={width} ref={(ref or 0.0):.2f} metric={metric} ev={picked_ev:.4f} base_ev={base_ev:.4f} delta_ev={delta_ev:+.4f} five_mid={(five_mid or 0.0):.2f}")
    return 0

if __name__=="__main__":
    raise SystemExit(main())
