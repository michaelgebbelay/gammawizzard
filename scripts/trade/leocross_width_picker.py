#!/usr/bin/env python3
# WIDTH PICKER — CREDIT only. Unbalanced IC: Calls = m * (Wc), with m=2, Wc = floor_half(Wp) to 5-pt grid.
# Emits outputs when CREDIT; emits blanks when DEBIT.

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
    # Ensure SELLs are closer to spot than BUYs for a CREDIT
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

def spread_credit_mid_from_nbbo(sell_sym, buy_sym, c):
    sb, sa = fetch_bid_ask(c, sell_sym)
    bb, ba = fetch_bid_ask(c, buy_sym)
    if None in (sb, sa, bb, ba): return None, None, None
    bid = clamp_tick(sb - ba)
    ask = clamp_tick(sa - bb)
    mid = clamp_tick((bid + ask) / 2.0)
    return bid, ask, mid

def emit_output(**kw):
    out_path = os.environ.get("GITHUB_OUTPUT","")
    def w(k,v):
        if not out_path: return
        with open(out_path,"a") as fh: fh.write(f"{k}={v}\n")
    for k,v in kw.items(): w(k, v if isinstance(v,str) else str(v))

def floor5(x: int) -> int:
    return int(math.floor(x/5.0)*5)

def main():
    DEFAULT_WP = int(os.environ.get("DEFAULT_CREDIT_WIDTH","20"))  # compare for EV gate
    EV_MIN_ADV = float(os.environ.get("EV_MIN_ADVANTAGE","0.10"))
    CANDS = [int(x) for x in (os.environ.get("CANDIDATE_CREDIT_WIDTHS","15,20,25,30,40,50").split(","))]
    CALL_MULT = 2  # fixed by spec
    # Win% keyed by put width (risk driver)
    WINP = json.loads(os.environ.get("WINP_STD_JSON",
        '{"5":0.714,"15":0.791,"20":0.816,"25":0.840,"30":0.863,"40":0.889,"50":0.916}'))

    # Leo
    tr = extract_trade(gw_fetch())
    if not tr:
        print("WIDTH_PICKER: NO_TRADE_PAYLOAD — emit blanks")
        emit_output(picked_put_width="", picked_call_width="", call_mult="", picked_width="", picked_ref="",
                    picked_metric="", picked_ev="", base_ev="", delta_ev="", five_mid="")
        return 0

    exp6 = yymmdd(str(tr.get("TDate","")))
    inner_put  = int(float(tr.get("Limit")))
    inner_call = int(float(tr.get("CLimit")))

    # side (credit only)
    def fnum(x): 
        try: return float(x)
        except: return None
    cat1=fnum(tr.get("Cat1")); cat2=fnum(tr.get("Cat2"))
    is_credit = True if (cat2 is None or cat1 is None or cat2>=cat1) else False

    c = schwab_client()

    # 5‑wide ref mid (visibility only)
    bp5, sp5, sc5, bc5 = build_legs(exp6, inner_put, inner_call, 5, 5)
    _,_,five_mid = spread_credit_mid_from_nbbo(sp5, bp5, c)  # 5‑wide ref as PUT spread mid
    # (We report this just like before for continuity.)

    if not is_credit:
        print("WIDTH_PICKER: DEBIT day — bypass (no outputs).")
        emit_output(picked_put_width="", picked_call_width="", call_mult="", picked_width="", picked_ref="",
                    picked_metric="", picked_ev="", base_ev="", delta_ev="", five_mid=(f"{five_mid:.2f}" if five_mid else ""))
        return 0

    rows=[]
    best=None
    base_ev=None

    print("|Wp|Wc|m|PutMid|CallMid|TotMid|EV|")
    print("|--:|--:|--:|-----:|------:|-----:|--:|")

    for Wp in CANDS:
        Wc = max(5, floor5(Wp/2.0))  # half the put width, floored to 5‑pt grid
        bp, sp, sc, bc = build_legs(exp6, inner_put, inner_call, Wp, Wc)

        # mid credits
        _,_,put_mid  = spread_credit_mid_from_nbbo(sp, bp, c)
        _,_,call_mid = spread_credit_mid_from_nbbo(sc, bc, c)

        if put_mid is None or call_mid is None:
            rows.append((Wp, Wc, CALL_MULT, None, None, None, None))
            print(f"|{Wp}|{Wc}|{CALL_MULT}|NA|NA|NA|NA|")
            continue

        tot_mid = clamp_tick(put_mid + CALL_MULT*call_mid)
        p = float(WINP.get(str(Wp), WINP.get(Wp, 0.80)))
        ev = p*tot_mid - (1.0-p)*(Wp - tot_mid)   # risk driver = Wp
        rows.append((Wp, Wc, CALL_MULT, put_mid, call_mid, tot_mid, ev))
        print(f"|{Wp}|{Wc}|{CALL_MULT}|{put_mid:.2f}|{call_mid:.2f}|{tot_mid:.2f}|{ev:.2f}|")

        if Wp == DEFAULT_WP:
            base_ev = ev
            if best is None: best = (Wp, Wc, CALL_MULT, put_mid, call_mid, tot_mid, ev)
        if best is None or ev > best[6] + 1e-12:
            best = (Wp, Wc, CALL_MULT, put_mid, call_mid, tot_mid, ev)

    if best is None:
        print("WIDTH_PICKER: No quotes — emit blanks")
        emit_output(picked_put_width="", picked_call_width="", call_mult="", picked_width="", picked_ref="",
                    picked_metric="", picked_ev="", base_ev="", delta_ev="", five_mid=(f"{five_mid:.2f}" if five_mid else ""))
        return 0

    if base_ev is None:
        base_ev = next((ev for (Wp,Wc,m,pm,cm,tm,ev) in rows if Wp==DEFAULT_WP and ev is not None), 0.0)

    Wp_sel, Wc_sel, m_sel, pm_sel, cm_sel, tm_sel, ev_sel = best
    delta = ev_sel - (base_ev or 0.0)
    width_out = Wp_sel if (delta > EV_MIN_ADV) else DEFAULT_WP
    # If staying with default, recompute the tuple to be consistent
    if width_out != Wp_sel:
        for r in rows:
            if r[0]==DEFAULT_WP:
                Wp_sel, Wc_sel, m_sel, pm_sel, cm_sel, tm_sel, ev_sel = r
                delta = ev_sel - (base_ev or 0.0)
                break

    print(f"**Picked:** Wp={Wp_sel}, Wc={Wc_sel}, m={m_sel}, cond_mid={tm_sel:.2f} (metric=EV) base_ev={base_ev:.2f} ΔEV={delta:+.2f} → {'Switch' if delta>EV_MIN_ADV else 'Stay'}")
    emit_output(
        picked_put_width=str(Wp_sel),
        picked_call_width=str(Wc_sel),
        call_mult=str(m_sel),
        picked_width=str(Wp_sel),      # for backward compat
        picked_ref=f"{tm_sel:.2f}",
        picked_metric="EV",
        picked_ev=f"{ev_sel:.4f}",
        base_ev=f"{base_ev:.4f}",
        delta_ev=f"{delta:.4f}",
        five_mid=(f"{five_mid:.2f}" if five_mid else "")
    )
    return 0

if __name__=="__main__":
    sys.exit(main())
