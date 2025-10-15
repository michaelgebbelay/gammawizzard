#!/usr/bin/env python3
# WIDTH PICKER — CREDIT SIDE ONLY
#
# Picks the best credit width from live quotes with an EV gate:
#   Only deviate from the base width if EV(best) - EV(base) > EV_MIN_ADVANTAGE (default $0.10)
#
# ENV knobs:
#   SCHWAB_APP_KEY / SCHWAB_APP_SECRET / SCHWAB_TOKEN_JSON  (auth)
#   GW_TOKEN or (GW_EMAIL + GW_PASSWORD)                    (LeoCross)
#
#   DEFAULT_CREDIT_WIDTH   e.g. "20"   ← base width the gate compares against
#   CANDIDATE_CREDIT_WIDTHS e.g. "15,20,25,30,40,50"
#   PUSH_OUT_SHORTS        "true"|"false" (never pushes 5‑wide)
#
#   EV_MIN_ADVANTAGE       numeric string, default "0.10"
#
#   WINP_STD_JSON          JSON of standard (same‑shorts) win% by width (0..1)
#   WINP_PUSH_JSON         JSON of pushed‑shorts win% by width (0..1)
#   RATIO_STD_JSON         JSON of baseline ratios for "edge" column
#   RATIO_PUSH_JSON        (optional) ratios when PUSH_OUT_SHORTS=true
#
# Outputs (GitHub Actions):
#   picked_width, picked_ref, picked_metric="EV", picked_ev, base_ev, delta_ev,
#   five_mid, pushed_out, picker_diag  (and a full markdown table in logs)

import os, re, json
from datetime import date
from zoneinfo import ZoneInfo
import requests
from schwab.auth import client_from_token_file

ET   = ZoneInfo("America/New_York")
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
    # 5‑wide is never pushed
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

def gw_login():
    r=requests.post("https://gandalf.gammawizard.com/goauth/authenticateFireUser",
                    data={"email":os.environ["GW_EMAIL"],"password":os.environ["GW_PASSWORD"]}, timeout=30)
    r.raise_for_status()
    j=r.json(); t=j.get("token")
    if not t: raise RuntimeError("GW login failed")
    return t

def gw_fetch():
    tok=(os.environ.get("GW_TOKEN","") or "").strip()
    if tok.lower().startswith("bearer "): tok=tok.split(None,1)[1]
    if not tok: tok=gw_login()
    h={"Authorization":f"Bearer {tok}","Accept":"application/json"}
    r=requests.get("https://gandalf.gammawizard.com/rapi/GetLeoCross", headers=h, timeout=30)
    r.raise_for_status()
    return r.json()

def main():
    # ===== env =====
    baseW   = int(os.environ.get("DEFAULT_CREDIT_WIDTH","20") or 20)
    pushed  = _truthy(os.environ.get("PUSH_OUT_SHORTS","false"))
    ev_gate = float(os.environ.get("EV_MIN_ADVANTAGE","0.10") or "0.10")

    # candidates (ensure base is included)
    CANDS = [int(x) for x in (os.environ.get("CANDIDATE_CREDIT_WIDTHS","15,20,25,30,40,50").split(","))]
    if baseW not in CANDS: CANDS.append(baseW)
    CANDS = sorted(set(CANDS))

    # win% tables
    WINP_STD = json.loads(os.environ.get(
        "WINP_STD_JSON",
        '{"5":0.714,"15":0.791,"20":0.816,"25":0.840,"30":0.863,"40":0.889,"50":0.916}'
    ))
    WINP_PUSH = json.loads(os.environ.get(
        "WINP_PUSH_JSON",
        '{"5":0.812,"15":0.868,"20":0.885,"25":0.900,"30":0.914,"40":0.929,"50":0.943}'
    ))
    WINP = WINP_PUSH if pushed else WINP_STD

    # ratios just for the "edge vs baseline" column (not used by gate)
    RATIO_STD = json.loads(os.environ.get(
        "RATIO_STD_JSON",
        '{"5":1.0,"15":2.55,"20":3.175,"25":3.6625,"30":4.15,"40":4.85,"50":5.375}'
    ))
    RATIO_PUSH = json.loads(os.environ.get(
        "RATIO_PUSH_JSON",
        '{"5":1.0,"15":2.205128,"20":2.743590,"25":3.179487,"30":3.615385,"40":4.256410,"50":4.717949}'
    ))
    RATIOS = RATIO_PUSH if pushed else RATIO_STD

    # ===== Schwab auth =====
    app_key=os.environ["SCHWAB_APP_KEY"]; app_secret=os.environ["SCHWAB_APP_SECRET"]; token_json=os.environ["SCHWAB_TOKEN_JSON"]
    with open("schwab_token.json","w") as f: f.write(token_json)
    c=client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")

    # ===== Leo =====
    j=gw_fetch()
    tr = j["Trade"][-1] if isinstance(j.get("Trade"), list) else j
    exp6 = yymmdd(str(tr.get("TDate","")))
    inner_put  = int(float(tr.get("Limit")))
    inner_call = int(float(tr.get("CLimit")))

    # 5‑wide reference (never pushed) for the baseline/edge column
    _,_,five_mid = nbbo_credit(c, build_legs(exp6, inner_put, inner_call, 5, pushed=False))

    # collect candidates
    rows=[]  # (W, mid, baseline, edge, winp, ev, score)
    for W in CANDS:
        legs = build_legs(exp6, inner_put, inner_call, W, pushed=pushed)
        _,_,mid = nbbo_credit(c, legs)
        if not mid:
            rows.append((W, None, None, None, None, None, None)); continue
        mult = RATIOS.get(str(W))
        baseline = (mult * five_mid) if (mult and five_mid) else None
        edge     = (mid - baseline) if baseline else None
        winp     = WINP.get(str(W)) or WINP.get(W)
        breach   = (1.0 - winp) if winp is not None else None
        ev       = (mid - breach*W) if breach is not None else None
        score    = mid/float(W)
        rows.append((W, mid, baseline, edge, winp, ev, score))

    # find base row & best EV row
    base_row = next((r for r in rows if r[0]==baseW and r[5] is not None), None)
    best_row = None
    for r in rows:
        if r[5] is None:   # EV missing
            continue
        if (best_row is None) or (r[5] > best_row[5] + 1e-12):
            best_row = r

    # decision with EV gate
    if base_row is None:
        # Cannot compute EV(base) — be conservative and stick to base if quoted; otherwise pick best mid
        picked = next((r for r in rows if r[0]==baseW and r[1] is not None), None) or \
                 (best_row if best_row is not None else max(rows, key=lambda x: (x[1] or 0.0)))
        gate_note = "EV(base) unavailable → fallback"
    else:
        base_ev = base_row[5]
        if (best_row is None) or (best_row[0]==baseW):
            picked = base_row; gate_note = "Best==Base"
        else:
            delta_ev = best_row[5] - base_ev
            if delta_ev > ev_gate + 1e-12:
                picked = best_row; gate_note = f"Switch (ΔEV={delta_ev:.2f} > {ev_gate:.2f})"
            else:
                picked = base_row; gate_note = f"Stick (ΔEV={delta_ev:.2f} ≤ {ev_gate:.2f})"

    # print nice table
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
    Wsel, mid_sel = picked[0], picked[1] or 0.0
    base_ev_out  = (base_row[5] if base_row else 0.0)
    delta_ev_out = ((picked[5] - base_ev_out) if (base_row and picked[5] is not None) else 0.0)

    print("WIDTH_PICKER (pushed_out={}, EV_gate={:.2f}, baseW={})"
          .format(pushed, ev_gate, baseW))
    print(table_md)
    print(f"**Picked:** {Wsel}-wide @ {mid_sel:.2f} (metric=EV), "
          f"base_ev={base_ev_out:.2f}, delta_ev={delta_ev_out:+.2f} → {gate_note}")
    if five_mid:
        print(f"5‑wide mid reference = {five_mid:.2f}")

    # emit GH outputs
    out_path=os.environ.get("GITHUB_OUTPUT","")
    def w(k,v):
        if not out_path: return
        with open(out_path,"a") as fh: fh.write(f"{k}={v}\n")
    w("picked_width", str(Wsel))
    w("picked_ref",   f"{mid_sel:.2f}")
    w("picked_metric","EV")
    w("picked_ev",    f"{(picked[5] if picked[5] is not None else 0.0):.4f}")
    w("base_ev",      f"{base_ev_out:.4f}")
    w("delta_ev",     f"{delta_ev_out:.4f}")
    w("five_mid",     f"{(five_mid or 0.0):.2f}")
    w("pushed_out",   "true" if pushed else "false")
    diag = "; ".join([f"W{r[0]}:{'NA' if r[5] is None else f'EV={r[5]:.2f}'}" for r in rows])
    w("picker_diag", diag)

if __name__ == "__main__":
    main()
