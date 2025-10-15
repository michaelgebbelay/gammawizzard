#!/usr/bin/env python3
# WIDTH PICKER (credit only)
# - Lock to a specific width with PICKER_LOCK_TO
# - Choose by EV if WINP_JSON provided; else by credit/width
# - Print full candidate table; emit GH outputs

import os, re, json
from datetime import date
from zoneinfo import ZoneInfo
from schwab.auth import client_from_token_file

ET = ZoneInfo("America/New_York")
TICK = 0.05

def clamp(x): return round(round(float(x)/TICK)*TICK + 1e-12, 2)
def yymmdd(iso): return f"{date.fromisoformat((iso or '')[:10]):%y%m%d}"

def to_osi(sym):
    raw=(sym or '').strip().upper().lstrip('.').replace('_','')
    m=re.match(r'^([A-Z.$^]{1,6})(\d{6})([CP])(\d{1,5})(?:\.(\d{1,3}))?$', raw) or \
      re.match(r'^([A-Z.$^]{1,6})(\d{6})([CP])(\d{8})$', raw)
    if not m: raise ValueError(f"Cannot parse: {sym}")
    root,ymd,cp,strike,frac=(m.groups()+('',))[:5]
    mills=int(strike)*1000 + (int((frac or '0').ljust(3,'0')) if frac else 0) if len(strike)<8 else int(strike)
    return f"{root:<6}{ymd}{cp}{mills:08d}"

def osi_canon(osi): return (osi[6:12], osi[12], osi[-8:])
def strike_from_osi(osi): return int(osi[-8:])/1000.0
def orient(bp,sp,sc,bc, credit=True):
    bpS,spS=strike_from_osi(bp),strike_from_osi(sp)
    scS,bcS=strike_from_osi(sc),strike_from_osi(bc)
    if credit:
        if bpS>spS: bp,sp=sp,bp
        if scS>bcS: sc,bc=bc,sc
    else:
        if bpS<spS: bp,sp=sp,bp
        if bcS>scS: sc,bc=bc,sc
    return [bp,sp,sc,bc]

def fetch_bid_ask(c, osi):
    r=c.get_quote(osi)
    if r.status_code!=200: return (None,None)
    d=list(r.json().values())[0] if isinstance(r.json(),dict) else {}
    q=d.get('quote', d)
    b=q.get('bidPrice') or q.get('bid') or q.get('bidPriceInDouble')
    a=q.get('askPrice') or q.get('ask') or q.get('askPriceInDouble')
    return (float(b) if b is not None else None, float(a) if a is not None else None)

def nbbo_credit(c, legs):
    bp,sp,sc,bc=legs
    bp_b,bp_a=fetch_bid_ask(c,bp); sp_b,sp_a=fetch_bid_ask(c,sp)
    sc_b,sc_a=fetch_bid_ask(c,sc); bc_b,bc_a=fetch_bid_ask(c,bc)
    if None in (bp_b,bp_a,sp_b,sp_a,sc_b,sc_a,bc_b,bc_a): return (None,None,None)
    bid=(sp_b+sc_b)-(bp_a+bc_a); ask=(sp_a+sc_a)-(bp_b+bc_b); mid=(bid+ask)/2.0
    return (clamp(bid), clamp(ask), clamp(mid))

def _truthy(s): return str(s or "").strip().lower() in {"1","true","t","yes","y","on"}

def build_legs(exp6, inner_put, inner_call, width, pushed=False):
    # 5-wide is never pushed
    if pushed and width!=5:
        sell_put  = inner_put  - 5
        buy_put   = sell_put   - width
        sell_call = inner_call + 5
        buy_call  = sell_call  + width
        p_low,p_high = buy_put, sell_put
        c_low,c_high = sell_call, buy_call
    else:
        p_low,p_high = inner_put - width, inner_put
        c_low,c_high = inner_call, inner_call + width
    return orient(to_osi(f".SPXW{exp6}P{p_low}"),
                  to_osi(f".SPXW{exp6}P{p_high}"),
                  to_osi(f".SPXW{exp6}C{c_low}"),
                  to_osi(f".SPXW{exp6}C{c_high}"),
                  True)

def main():
    # ---- env knobs ----
    pushed   = _truthy(os.environ.get("PUSH_OUT_SHORTS","false"))
    lock_to  = os.environ.get("PICKER_LOCK_TO","" ).strip()
    lock_to  = int(lock_to) if (lock_to and lock_to.isdigit()) else None
    cands    = [int(x) for x in (os.environ.get("CANDIDATE_CREDIT_WIDTHS","15,20,25,30,40,50").split(","))]
    selector = (os.environ.get("SELECTOR_METRIC","EV") or "EV").upper()  # EV | SCORE | EDGE
    ratios   = json.loads(os.environ.get(
        "RATIO_STD_JSON",
        '{"5":1.0,"15":2.55,"20":3.175,"25":3.6625,"30":4.15,"40":4.85,"50":5.375}'
    ))
    # Optional win% (standard)
    WINP = json.loads(os.environ.get(
        "WINP_JSON",
        '{"5":0.714,"15":0.791,"20":0.816,"25":0.840,"30":0.863,"40":0.889,"50":0.916}'
    ))

    # ---- Schwab auth ----
    app_key=os.environ["SCHWAB_APP_KEY"]; app_secret=os.environ["SCHWAB_APP_SECRET"]; token_json=os.environ["SCHWAB_TOKEN_JSON"]
    with open("schwab_token.json","w") as f: f.write(token_json)
    c=client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")

    # ---- Leo ----
    import requests
    def _sanitize(t): 
        t=(t or "").strip().strip('"').strip("'")
        return t.split(None,1)[1] if t.lower().startswith("bearer ") else t
    def gw_login():
        r=requests.post("https://gandalf.gammawizard.com/goauth/authenticateFireUser",
                        data={"email":os.environ["GW_EMAIL"],"password":os.environ["GW_PASSWORD"]}, timeout=30)
        r.raise_for_status(); j=r.json(); 
        if not j.get("token"): raise RuntimeError("GW login failed")
        return j["token"]
    tok=_sanitize(os.environ.get("GW_TOKEN","") or "") or gw_login()
    h={"Authorization":f"Bearer {_sanitize(tok)}","Accept":"application/json"}
    r=requests.get("https://gandalf.gammawizard.com/rapi/GetLeoCross", headers=h, timeout=30); r.raise_for_status()
    j=r.json()
    tr = j["Trade"][-1] if isinstance(j.get("Trade"), list) else j
    exp6 = yymmdd(str(tr.get("TDate","")))
    inner_put  = int(float(tr.get("Limit"))); 
    inner_call = int(float(tr.get("CLimit")))
    # 5-wide baseline (never pushed) for ratio baseline
    bid5,ask5,mid5 = nbbo_credit(c, build_legs(exp6, inner_put, inner_call, 5, pushed=False))
    five_ref = mid5

    rows=[]
    best=None
    for W in cands:
        legs = build_legs(exp6, inner_put, inner_call, W, pushed=pushed)
        b,a,m = nbbo_credit(c, legs)
        ref = m
        if not ref:
            rows.append((W, None, None, None, None, None, None)); continue
        base_mult = ratios.get(str(W), None)
        baseline  = (base_mult * five_ref) if (base_mult and five_ref) else None
        edge      = (ref - baseline) if baseline else None
        winp      = WINP.get(str(W)) or WINP.get(W)  # optional
        breach    = (1.0 - winp) if winp else None
        ev        = (ref - breach*W) if (breach is not None) else None
        score     = ref/float(W)

        # choose metric
        metric = (ev if (selector=="EV" and ev is not None) 
                    else (score if selector=="SCORE" else (edge if edge is not None else score)))

        rows.append((W, ref, baseline, edge, winp, ev, score))
        cand = (W, ref, metric)
        if best is None or (cand[2] > best[2] + 1e-12):
            best = cand

    # Lock, if requested and quoted
    if lock_to and any(r[0]==lock_to and r[1] for r in rows):
        for r in rows:
            if r[0]==lock_to and r[1]:
                best = (r[0], r[1], (r[5] if selector=="EV" else (r[6] if selector=="SCORE" else r[3])))
                break

    if best is None:
        print("WIDTH_PICKER: no quotes; defaulting to 20")
        emit(20, 0.0, "NONE", rows, five_ref, pushed); 
        return

    Wsel, refsel, metric_sel = best
    emit(Wsel, refsel, selector, rows, five_ref, pushed)

def emit(width, ref, selector, rows, five_ref, pushed):
    # Markdown table
    hdr = "|Width|Mid|Baseline|Edge|Win%|EV|Credit/Width|\n|---:|---:|---:|---:|---:|---:|---:|"
    lines=[hdr]
    for W,mid,base,edge,winp,ev,score in rows:
        lines.append(f"|{W}|{'' if mid is None else f'{mid:.2f}'}|"
                     f"{'' if base is None else f'{base:.2f}'}|"
                     f"{'' if edge is None else f'{edge:+.2f}'}|"
                     f"{'' if winp is None else f'{winp*100:.2f}%'}|"
                     f"{'' if ev   is None else f'{ev:.2f}'}|"
                     f"{'' if score is None else f'{score:.5f}'}|")
    table_md = "\n".join(lines)
    five_ref_str = "NA" if five_ref is None else f"{five_ref:.2f}"
    pick_line = f"**Picked:** {width}-wide @ {ref:.2f} ({selector}), five_ref={five_ref_str}, pushed_out={pushed}"
    print("WIDTH_PICKER\n" + table_md + "\n" + pick_line)

    out_path=os.environ.get("GITHUB_OUTPUT","")
    def w(k,v):
        if not out_path: return
        with open(out_path,"a") as fh: fh.write(f"{k}={v}\n")
    w("picked_width", str(width))
    w("picked_ref",   f"{ref:.2f}")
    w("picked_metric", selector)
    # also expose a compact diag
    diag = "; ".join([f"W{r[0]}:{'NA' if r[1] is None else f'{r[1]:.2f}'}"
                      for r in rows])
    w("picker_diag", diag)

if __name__ == "__main__":
    main()
