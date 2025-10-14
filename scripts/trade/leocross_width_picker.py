#!/usr/bin/env python3
# WIDTH PICKER — chooses the best credit width for today from live quotes
# Emits GitHub Actions outputs: picked_width, picked_ref, picked_score, picked_edge, five_mid, pushed_out
# Credit side only; long days are ignored (outputs default width).

import os, sys, re, json, math, time
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
    email=os.environ["GW_EMAIL"]; pwd=os.environ["GW_PASSWORD"]
    r=requests.post(f"{base}/goauth/authenticateFireUser", data={"email":email,"password":pwd}, timeout=30)
    r.raise_for_status(); j=r.json(); t=j.get("token")
    if not t: raise RuntimeError("GW_LOGIN_NO_TOKEN")
    return t

def gw_get_leocross(base: str, endpoint: str):
    tok=_sanitize_token(os.environ.get("GW_TOKEN","") or "")
    def hit(t):
        h={"Accept":"application/json","Authorization":f"Bearer {_sanitize_token(t)}","User-Agent":"width-picker/1.0"}
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

def main():
    # ---- env knobs
    GW_BASE = os.environ.get("GW_BASE","https://gandalf.gammawizard.com")
    GW_ENDPOINT = os.environ.get("GW_ENDPOINT","/rapi/GetLeoCross")
    PUSH_OUT_SHORTS   = _truthy(os.environ.get("PUSH_OUT_SHORTS","false"))
    DEFAULT_WIDTH     = int(os.environ.get("DEFAULT_CREDIT_WIDTH","20"))
    CANDS = [int(x) for x in (os.environ.get("CANDIDATE_CREDIT_WIDTHS","15,20,25,30,40,50").split(","))]
    SELECTOR_USE      = (os.environ.get("SELECTOR_USE","MID") or "MID").upper()  # MID or ASK
    SELECTOR_TICK_TOL = float(os.environ.get("SELECTOR_TICK_TOL","0.10"))
    RATIO_STD = json.loads(os.environ.get("RATIO_STD_JSON",'{"5":1.0, "15":2.55, "20":3.175, "25":3.6625, "30":4.15, "40":4.85, "50":5.375}'))
    RATIO_PUSH= json.loads(os.environ.get("RATIO_PUSH_JSON",'{"5":1.0, "15":2.205128, "20":2.743590, "25":3.179487, "30":3.615385, "40":4.256410, "50":4.717949}'))

    # ---- Schwab auth
    app_key=os.environ["SCHWAB_APP_KEY"]; app_secret=os.environ["SCHWAB_APP_SECRET"]; token_json=os.environ["SCHWAB_TOKEN_JSON"]
    with open("schwab_token.json","w") as f: f.write(token_json)
    c=client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")

    # ---- Leo
    api = gw_get_leocross(GW_BASE, GW_ENDPOINT); tr = extract_trade(api)
    if not tr:
        print("WIDTH_PICKER: NO_TRADE_PAYLOAD — using default")
        return emit(DEFAULT_WIDTH, 0.0, 0.0, 0.0, 0.0, PUSH_OUT_SHORTS)

    exp6 = yymmdd(str(tr.get("TDate","")))
    inner_put  = int(float(tr.get("Limit"))); 
    inner_call = int(float(tr.get("CLimit")))
    def fnum(x): 
        try: return float(x)
        except: return None
    cat1=fnum(tr.get("Cat1")); cat2=fnum(tr.get("Cat2"))
    is_credit = True if (cat2 is None or cat1 is None or cat2>=cat1) else False
    if not is_credit:
        print("WIDTH_PICKER: Non‑credit day — returning default width for completeness")
        return emit(DEFAULT_WIDTH, 0.0, 0.0, 0.0, 0.0, PUSH_OUT_SHORTS)

    # ---- 5‑wide at current shorts (or pushed for 5? keep 5 unchanged)
    legs5 = build_legs(5, True, exp6, inner_put, inner_call, False)  # 5-wide never pushed
    bid5, ask5, mid5 = condor_nbbo_credit(c, legs5)
    five_ref = (mid5 if SELECTOR_USE=="MID" else ask5)

    ratios = RATIO_PUSH if PUSH_OUT_SHORTS else RATIO_STD
    diag = []
    best = None  # (W, ref, score, edge, baseline)

    for W in CANDS:
        legsW = build_legs(W, True, exp6, inner_put, inner_call, PUSH_OUT_SHORTS)
        b,a,m = condor_nbbo_credit(c, legsW)
        ref = (m if SELECTOR_USE=="MID" else a)
        if ref is None or ref <= 0:
            diag.append(f"W{W}: NOQUOTE")
            continue
        # baseline from ratio * five_mid (if we have a five_ref); else 0
        mult = ratios.get(str(W)) if isinstance(ratios, dict) else None
        if mult is None:
            try: mult = ratios.get(W)  # int key fallback
            except: mult = None
        baseline = (mult * five_ref) if (mult is not None and five_ref and five_ref>0) else 0.0
        edge = ref - baseline
        score = ref / float(W)
        diag.append(f"W{W}: ref={ref:.2f} score={score:.5f} edge={edge:+.2f} base={baseline:.2f}")
        cand = (W, ref, score, edge, baseline)
        if best is None:
            best = cand
        else:
            # primary: higher score
            if cand[2] > best[2] + 1e-12:
                best = cand
            else:
                # tie band: within SELECTOR_TICK_TOL on total‑credit equivalence
                # Convert score diff into credit diff at a "typical" width (use max(W,bestW))
                credit_diff = abs(cand[2] - best[2]) * float(max(W, best[0]))
                if credit_diff <= SELECTOR_TICK_TOL and cand[3] > best[3] + 1e-12:
                    best = cand

    if best is None:
        print("WIDTH_PICKER: No candidates quoted — using default")
        return emit(DEFAULT_WIDTH, 0.0, 0.0, 0.0, (five_ref or 0.0), PUSH_OUT_SHORTS, diag)

    Wsel, refsel, scoresel, edgesel, basesel = best
    print("WIDTH_PICKER: five_ref={} ({}), pushed_out={}, pick=W{}, ref={}, score={:.5f}, edge={:+.2f}".format(
        f"{five_ref:.2f}" if five_ref else "NA", SELECTOR_USE, PUSH_OUT_SHORTS, Wsel, f"{refsel:.2f}", scoresel, edgesel))
    print("CANDIDATES → " + " | ".join(diag))
    return emit(Wsel, refsel, scoresel, edgesel, (five_ref or 0.0), PUSH_OUT_SHORTS, diag)

def emit(width:int, ref:float, score:float, edge:float, five_mid:float, pushed:bool, diag=None):
    out_path = os.environ.get("GITHUB_OUTPUT","")
    def w(k,v):
        if not out_path: return
        with open(out_path,"a") as fh:
            fh.write(f"{k}={v}\n")
    w("picked_width", str(width))
    w("picked_ref",   f"{ref:.2f}")
    w("picked_score", f"{score:.6f}")
    w("picked_edge",  f"{edge:+.2f}")
    w("five_mid",     f"{five_mid:.2f}")
    w("pushed_out",   "true" if pushed else "false")
    if diag:
        # truncate to keep logs tidy
        w("picker_diag", " / ".join(diag)[:900])
    print(f"::notice title=WidthPicker::picked_width={width} ref={ref:.2f} score={score:.6f} edge={edge:+.2f} five_mid={five_mid:.2f} pushed_out={pushed}")
    return 0

if __name__=="__main__":
    sys.exit(main())
