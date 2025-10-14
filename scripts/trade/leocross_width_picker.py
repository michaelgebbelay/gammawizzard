#!/usr/bin/env python3
# WIDTH PICKER — EV mode
# Chooses the best credit width using: Score(W) = (C/W) - (1-p(W)), where
# C is live credit (mid or ask) and p(W) is the historical win rate for that width.
# Emits GitHub Actions outputs: picked_width, picked_ref, picked_p, picked_edge, picked_score, pushed_out, picker_diag
#
# Env knobs
# ----------
# Auth: SCHWAB_APP_KEY, SCHWAB_APP_SECRET, SCHWAB_TOKEN_JSON
# GW:   GW_TOKEN (optional), GW_EMAIL, GW_PASSWORD, GW_BASE (opt), GW_ENDPOINT (opt)
#
# Behavior:
#   PICKER_MODE: EV | RATIO      (EV is recommended)
#   PICKER_FORCE_CREDIT: "true"  (to run on long-signal days for testing)
#   PUSH_OUT_SHORTS: "true"      (use pushed ±5 shorts for W>5; 5-wide stays same-shorts)
#   DEFAULT_CREDIT_WIDTH: "20"
#   CANDIDATE_CREDIT_WIDTHS: "15,20,25,30,40,50"
#   SELECTOR_USE: MID | ASK
#   SELECTOR_TICK_TOL: "0.10"    (tie band in approx $/trade terms)
#
# EV mode tables (JSON maps of width -> win% as decimals):
#   WIN_TABLE_STD_JSON:  e.g. {"5":0.7141,"15":0.7906,"20":0.8156,"30":0.8625,"40":0.8891,"50":0.9156}
#   WIN_TABLE_PUSH_JSON: e.g. {"5":0.8125,"15":0.8625,"20":0.8844,"30":0.9078,"40":0.9250,"50":0.9313}
#
# Ratio mode (fallback/legacy): RATIO_STD_JSON, RATIO_PUSH_JSON (not used in EV mode)

import os, sys, re, json, math
from datetime import date
from zoneinfo import ZoneInfo
from schwab.auth import client_from_token_file
import requests

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

def osi_canon(osi: str): return (osi[6:12], osi[12], osi[-8:])
def strike_from_osi(osi: str) -> float: return int(osi[-8:]) / 1000.0

def orient(bp,sp,sc,bc, is_credit=True):
    bpS=strike_from_osi(bp); spS=strike_from_osi(sp)
    scS=strike_from_osi(sc); bcS=strike_from_osi(bc)
    if is_credit:
        if bpS>spS: bp,sp = sp,bp
        if scS>bcS: sc,bc = bc,sc
    else:
        if bpS<spS: bp,sp = sp,bp
        if bcS>scS: sc,bc = bc,sc
    return [bp,sp,sc,bc]

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

def gw_login_token(base: str):
    email=os.environ.get("GW_EMAIL",""); pwd=os.environ.get("GW_PASSWORD","")
    if not (email and pwd): raise RuntimeError("Missing GW_EMAIL/GW_PASSWORD")
    r=requests.post(f"{base}/goauth/authenticateFireUser", data={"email":email,"password":pwd}, timeout=30)
    r.raise_for_status(); j=r.json(); t=j.get("token")
    if not t: raise RuntimeError("GW_LOGIN_NO_TOKEN")
    return t

def gw_get_leocross(base: str, endpoint: str):
    tok=_sanitize_token(os.environ.get("GW_TOKEN","") or "")
    def hit(t):
        h={"Accept":"application/json","Authorization":f"Bearer {_sanitize_token(t)}","User-Agent":"width-picker/ev/1.0"}
        return requests.get(f"{base.rstrip('/')}/{endpoint.lstrip('/')}", headers=h, timeout=30)
    r=hit(tok) if tok else None
    if (r is None) or (r.status_code in (401,403)): r=hit(gw_login_token(base))
    if r.status_code!=200: raise RuntimeError(f"GW_HTTP_{r.status_code}:{(r.text or '')[:180]}")
    return r.json()

def extract_trade(j):
    if isinstance(j,dict):
        if "Trade" in j:
            tr=j["Trade"]; 
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

def build_legs(width:int, is_credit:bool, exp6:str, inner_put:int, inner_call:int, pushed:bool):
    # NOTE: 5-wide is never "pushed"; we keep same shorts so its credit matches the signal.
    if is_credit:
        if pushed and width != 5:
            sell_put  = inner_put  - 5
            buy_put   = sell_put   - width
            sell_call = inner_call + 5
            buy_call  = sell_call  + width
            p_low, p_high = buy_put, sell_put
            c_low, c_high = sell_call, buy_call
        else:
            p_low, p_high = inner_put - width, inner_put
            c_low, c_high = inner_call, inner_call + width
    else:
        p_low, p_high = inner_put - width, inner_put
        c_low, c_high = inner_call, inner_call + width
    bp = to_osi(f".SPXW{exp6}P{p_low}")
    sp = to_osi(f".SPXW{exp6}P{p_high}")
    sc = to_osi(f".SPXW{exp6}C{c_low}")
    bc = to_osi(f".SPXW{exp6}C{c_high}")
    return orient(bp,sp,sc,bc, is_credit=True)

def _truthy(s:str)->bool:
    return str(s or "").strip().lower() in {"1","true","t","yes","y","on"}

def _parse_widths(s: str):
    arr=[int(x) for x in re.split(r'[,\s]+', (s or "").strip()) if x]
    arr=sorted(set(w for w in arr if w>=5 and w%5==0))
    return arr or [15,20,25,30,40,50]

def _load_json_env(name: str, default: str) -> dict:
    raw=os.environ.get(name, default)
    try:
        d=json.loads(raw or "{}")
        return {int(k): float(v) for (k,v) in d.items()}
    except Exception:
        return {}

def _interp_win(pmap: dict, w: int) -> float:
    """Return win% for width w from map; linear interpolate if needed; fallback to nearest."""
    if not pmap: return None
    if w in pmap: return float(pmap[w])
    keys=sorted(pmap.keys())
    lo = max([k for k in keys if k<=w], default=None)
    hi = min([k for k in keys if k>=w], default=None)
    if lo is None and hi is None: return None
    if lo is None: return float(pmap[hi])
    if hi is None: return float(pmap[lo])
    if lo==hi: return float(pmap[lo])
    # linear interpolation
    p_lo=float(pmap[lo]); p_hi=float(pmap[hi])
    return p_lo + (p_hi - p_lo) * ( (w - lo) / float(hi - lo) )

def main():
    # ---- Knobs
    GW_BASE     = os.environ.get("GW_BASE","https://gandalf.gammawizard.com")
    GW_ENDPOINT = os.environ.get("GW_ENDPOINT","/rapi/GetLeoCross")
    DEFAULT_W   = int(os.environ.get("DEFAULT_CREDIT_WIDTH","20"))
    CANDS       = _parse_widths(os.environ.get("CANDIDATE_CREDIT_WIDTHS","15,20,25,30,40,50"))
    SELECTOR    = (os.environ.get("SELECTOR_USE","MID") or "MID").upper()
    TIE_TOL     = float(os.environ.get("SELECTOR_TICK_TOL","0.10"))
    MODE        = (os.environ.get("PICKER_MODE","EV") or "EV").upper()
    PUSHED      = _truthy(os.environ.get("PUSH_OUT_SHORTS","false"))
    FORCE_CREDIT= _truthy(os.environ.get("PICKER_FORCE_CREDIT","false"))

    # Win-rate tables (EV mode)
    WIN_STD = _load_json_env("WIN_TABLE_STD_JSON",
                             '{"5":0.7141,"15":0.7906,"20":0.8156,"30":0.8625,"40":0.8891,"50":0.9156}')
    WIN_PUSH= _load_json_env("WIN_TABLE_PUSH_JSON",
                             '{"5":0.8125,"15":0.8625,"20":0.8844,"30":0.9078,"40":0.9250,"50":0.9313}')

    # Legacy ratios (only used if MODE!='EV')
    RAT_STD = _load_json_env("RATIO_STD_JSON", '{"5":1.0,"15":2.55,"20":3.175,"25":3.6625,"30":4.15,"40":4.85,"50":5.375}')
    RAT_PUSH= _load_json_env("RATIO_PUSH_JSON",'{"5":1.0,"15":2.205128,"20":2.743590,"25":3.179487,"30":3.615385,"40":4.256410,"50":4.717949}')

    # ---- Schwab auth
    app_key=os.environ["SCHWAB_APP_KEY"]; app_secret=os.environ["SCHWAB_APP_SECRET"]; token_json=os.environ["SCHWAB_TOKEN_JSON"]
    with open("schwab_token.json","w") as f: f.write(token_json)
    c=client_from_token_file(api_key=app_key, api_secret=app_secret, token_path="schwab_token.json")

    # ---- Leo signal
    api = gw_get_leocross(GW_BASE, GW_ENDPOINT); tr = extract_trade(api)
    if not tr:
        print("WIDTH_PICKER: NO_TRADE_PAYLOAD — using default")
        return emit(DEFAULT_W, 0.0, 0.0, 0.0, 0.0, PUSHED)

    exp6 = yymmdd(str(tr.get("TDate","")))
    inner_put  = int(float(tr.get("Limit"))); 
    inner_call = int(float(tr.get("CLimit")))
    def fnum(x): 
        try: return float(x)
        except: return None
    cat1=fnum(tr.get("Cat1")); cat2=fnum(tr.get("Cat2"))
    is_credit = True if (cat2 is None or cat1 is None or cat2>=cat1) else False

    if not is_credit and not FORCE_CREDIT:
        print("WIDTH_PICKER: Non‑credit day — set PICKER_FORCE_CREDIT=true to test the picker.")
        return emit(DEFAULT_W, 0.0, 0.0, 0.0, 0.0, PUSHED)

    if not is_credit and FORCE_CREDIT:
        print("WIDTH_PICKER: Forced‑credit test on a long‑signal day.")

    # ---- 5w reference (for logging only; EV mode does not use this as baseline)
    legs5 = build_legs(5, True, exp6, inner_put, inner_call, False)
    bid5, ask5, mid5 = condor_nbbo_credit(c, legs5)
    five_ref = (mid5 if SELECTOR=="MID" else ask5)

    # ---- Evaluate candidates
    diag=[]
    best=None  # (W, refC, pW, edge$, score, loss_prob)

    for W in CANDS:
        legsW = build_legs(W, True, exp6, inner_put, inner_call, PUSHED)
        b,a,m = condor_nbbo_credit(c, legsW)
        ref = (m if SELECTOR=="MID" else a)
        if ref is None or ref <= 0:
            diag.append(f"W{W}: NOQUOTE")
            continue

        if MODE == "EV":
            pW = _interp_win(WIN_PUSH if PUSHED else WIN_STD, W)
            if pW is None:
                diag.append(f"W{W}: NOVALID_P"); continue
            loss_prob = 1.0 - float(pW)
            edge_dollars = ref - loss_prob * W     # EV per contract in $ (binary settle)
            score = edge_dollars / float(W)        # EV per contract per width (∝ EV per trade @ constant risk)
        else:
            # Legacy ratio baseline (kept for backwards compatibility)
            mult = (RAT_PUSH if PUSHED else RAT_STD).get(W) or (RAT_PUSH if PUSHED else RAT_STD).get(int(W))
            baseline = (mult * five_ref) if (mult and five_ref and five_ref>0) else 0.0
            pW = None
            loss_prob = None
            edge_dollars = ref - baseline
            score = ref/float(W)

        diag.append("W{}: ref={:.2f}{}{} edge={:+.2f} score={:.5f}".format(
            W, ref,
            (f" p={pW:.4f}" if pW is not None else ""),
            (f" lossP={loss_prob:.4f}" if loss_prob is not None else ""),
            edge_dollars, score
        ))

        cand=(W, ref, (float(pW) if pW is not None else 0.0), edge_dollars, score, (float(loss_prob) if loss_prob is not None else 0.0))
        if best is None:
            best=cand
        else:
            # Primary: higher score
            if cand[4] > best[4] + 1e-12:
                best = cand
            else:
                # Tie band: if scores nearly equal, prefer larger dollar edge (more cushion)
                # Convert score diff into approximate $/trade diff using the larger width.
                per_trade_diff = abs(cand[4] - best[4]) * float(max(cand[0], best[0]))
                if per_trade_diff <= TIE_TOL and cand[3] > best[3] + 1e-12:
                    best = cand

    if best is None:
        print("WIDTH_PICKER: No candidates quoted — using default")
        return emit(DEFAULT_W, 0.0, 0.0, 0.0, (five_ref or 0.0), PUSHED, diag)

    Wsel, refsel, psel, edgesel, scoresel, losssel = best
    print("WIDTH_PICKER[{}]: five_ref={} ({}), pushed_out={}, pick=W{}, ref={}, p={:.4f}, edge={:+.2f}, score={:.5f}".format(
        MODE, (f\"{five_ref:.2f}\" if five_ref else "NA"), SELECTOR, PUSHED, Wsel, f\"{refsel:.2f}\", psel, edgesel, scoresel))
    print("CANDIDATES → " + " | ".join(diag))
    return emit(Wsel, refsel, psel, edgesel, (five_ref or 0.0), PUSHED, diag, score=scoresel)

def emit(width:int, ref:float, p:float, edge:float, five_mid:float, pushed:bool, diag=None, score:float=0.0):
    out_path = os.environ.get("GITHUB_OUTPUT","")
    def w(k,v):
        if not out_path: return
        with open(out_path,"a") as fh:
            fh.write(f"{k}={v}\n")
    w("picked_width", str(width))
    w("picked_ref",   f"{ref:.2f}")
    w("picked_p",     f"{p:.6f}")
    w("picked_edge",  f"{edge:+.2f}")
    w("picked_score", f"{score:.6f}")
    w("five_mid",     f"{five_mid:.2f}")
    w("pushed_out",   "true" if pushed else "false")
    if diag:
        w("picker_diag", " / ".join(diag)[:900])
    print(f"::notice title=WidthPicker::{('EV' if os.environ.get('PICKER_MODE','EV').upper()=='EV' else 'RATIO')}  pick=W{width} ref={ref:.2f} p={p:.4f} edge={edge:+.2f} score={score:.5f} pushed_out={pushed}")
    return 0

if __name__=="__main__":
    sys.exit(main())
