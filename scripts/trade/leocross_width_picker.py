#!/usr/bin/env python3
# WIDTH PICKER — Skew-aware. Supports asymmetric wings and ratio condors.
# Outputs:
#   picked_put_width, picked_call_width, call_mult,
#   picked_width (alias of put width for backward compat),
#   picked_ref (condor mid), picked_metric=EV,
#   picked_ev, base_ev, delta_ev, five_mid

import os, re, json, math, sys, time
from datetime import date
import requests
from schwab.auth import client_from_token_file

TICK = 0.05

# ---------- small utils ----------
def clamp_tick(x: float | None) -> float | None:
    if x is None: return None
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

def build_legs(exp6: str, inner_put: int, inner_call: int, Wp: int, Wc: int):
    """Same‑shorts condor with possibly different widths on each wing."""
    p_low, p_high = inner_put - Wp, inner_put
    c_low, c_high = inner_call, inner_call + Wc
    return orient_credit(
        to_osi(f".SPXW{exp6}P{p_low}"),
        to_osi(f".SPXW{exp6}P{p_high}"),
        to_osi(f".SPXW{exp6}C{c_low}"),
        to_osi(f".SPXW{exp6}C{c_high}")
    )

# ---------- GW auth/fetch ----------
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

def legs_quote_pack(c, legs):
    bp, sp, sc, bc = legs
    bp_b, bp_a = fetch_bid_ask(c, bp); sp_b, sp_a = fetch_bid_ask(c, sp)
    sc_b, sc_a = fetch_bid_ask(c, sc); bc_b, bc_a = fetch_bid_ask(c, bc)
    return (bp_b, bp_a, sp_b, sp_a, sc_b, sc_a, bc_b, bc_a)

def vertical_mid(short_b, short_a, long_b, long_a):
    if None in (short_b, short_a, long_b, long_a): return None
    bid_spread = short_b - long_a
    ask_spread = short_a - long_b
    return clamp_tick((bid_spread + ask_spread) / 2.0)

def condor_nbbo_credit_weighted(quotes, call_mult:int):
    bp_b, bp_a, sp_b, sp_a, sc_b, sc_a, bc_b, bc_a = quotes
    if None in quotes: return (None,None,None)
    # Bid = what we can *sell* the package for now:
    credit_bid = (sp_b - bp_a) + call_mult * (sc_b - bc_a)
    # Ask = what we must *pay* to close immediately (for reference):
    credit_ask = (sp_a - bp_b) + call_mult * (sc_a - bc_b)
    mid = (credit_bid + credit_ask) / 2.0
    return (clamp_tick(credit_bid), clamp_tick(credit_ask), clamp_tick(mid))

# ---------- GHA outputs ----------
def emit_output(**kv):
    out_path = os.environ.get("GITHUB_OUTPUT","")
    if not out_path: return
    with open(out_path,"a") as fh:
        for k,v in kv.items():
            fh.write(f"{k}={v}\n")

# ---------- main ----------
def main():
    # Knobs
    DEFAULT_WP = int(os.environ.get("DEFAULT_CREDIT_WIDTH","20"))
    EV_MIN_ADV = float(os.environ.get("EV_MIN_ADVANTAGE","0.10"))
    PUT_CANDS = [int(x) for x in (os.environ.get("CANDIDATE_PUT_WIDTHS","15,20,25,30,40,50").split(","))]
    CALL_CANDS = [int(x) for x in (os.environ.get("CANDIDATE_CALL_WIDTHS","15,20,25,30,40,50").split(","))]
    CALL_MIN_RATIO = float(os.environ.get("CALL_MIN_RATIO","0.80"))  # require m*CallMid >= ratio*PutMid
    HALF_MODE = (os.environ.get("CALL_HALF_MODE","false").strip().lower() == "true")

    # Historical win% proxy by max wing width (fallback 0.80 if missing)
    WINP = json.loads(os.environ.get("WINP_STD_JSON",
        '{"5":0.714,"15":0.791,"20":0.816,"25":0.840,"30":0.863,"40":0.889,"50":0.916}'))

    # GW
    tr = extract_trade(gw_fetch())
    if not tr:
        print("WIDTH_PICKER: NO_TRADE_PAYLOAD — using defaults")
        emit_output(picked_put_width=str(DEFAULT_WP),
                    picked_call_width=str(DEFAULT_WP),
                    call_mult="1",
                    picked_width=str(DEFAULT_WP),
                    picked_ref="0.00", picked_metric="EV",
                    picked_ev="0.0000", base_ev="0.0000", delta_ev="0.0000",
                    five_mid="")
        return 0

    exp6 = yymmdd(str(tr.get("TDate","")))
    inner_put  = int(float(tr.get("Limit")))   # LeoCross short put strike (inner)  [oai_citation:2‡rapi_GetUltraSVJ_20251016_131354.json](sediment://file_00000000f8b061f7b9cfe792c5761598)
    inner_call = int(float(tr.get("CLimit")))  # LeoCross short call strike (inner)  [oai_citation:3‡rapi_GetLeoCross_20251016_131353.json](sediment://file_0000000010ac61f7af1c82f94118bf11)

    # Schwab quotes
    c = schwab_client()

    # 5‑wide ref (optional visibility)
    legs5 = build_legs(exp6, inner_put, inner_call, 5, 5)
    q5 = legs_quote_pack(c, legs5)
    _,_,five_mid = condor_nbbo_credit_weighted(q5, call_mult=1)

    # Build candidate grid
    rows=[]  # (Wp, Wc, m, put_mid, call_mid, tot_mid, ratio, EV, CPUR)
    best=None
    base_ev=None

    for Wp in PUT_CANDS:
        # half‑mode restricts call choices to (Wc=Wp/2, m=2). Otherwise explore Wc + m∈{1,2}
        pairs = []
        if HALF_MODE:
            Wc = max(5, int(round(Wp/2.0/5.0))*5)
            pairs.append((Wc, 2))
        else:
            for Wc in CALL_CANDS:
                pairs.append((Wc, 1))
                # also test 2x calls when Wc<=Wp
                if Wc*2 <= max(CALL_CANDS + [Wp*2]):  # guard
                    pairs.append((Wc, 2))

        for (Wc, m) in pairs:
            legs = build_legs(exp6, inner_put, inner_call, Wp, Wc)
            q = legs_quote_pack(c, legs)
            if any(v is None for v in q):
                continue

            # per‑wing mids
            bp_b, bp_a, sp_b, sp_a, sc_b, sc_a, bc_b, bc_a = q
            put_mid  = vertical_mid(sp_b, sp_a, bp_b, bp_a)    # short put - long put
            call_mid = vertical_mid(sc_b, sc_a, bc_b, bc_a)    # short call - long call
            if put_mid is None or call_mid is None: continue

            tot_bid, tot_ask, tot_mid = condor_nbbo_credit_weighted(q, call_mult=m)
            if tot_mid is None or tot_mid <= 0: continue

            # balance requirement
            ratio = (m*call_mid) / put_mid if put_mid>0 else 0.0
            if ratio < CALL_MIN_RATIO:
                # keep row for table but skip in rank unless nothing qualifies
                pass

            # Risk proxy = max wing width
            Wmax = float(max(Wp, m*Wc))
            p = float(WINP.get(str(int(Wmax)), WINP.get(int(Wmax), 0.80)))
            EV = p*tot_mid - (1.0-p)*(Wmax - tot_mid)
            CPUR = tot_mid / Wmax

            rows.append((Wp, Wc, m, put_mid, call_mid, tot_mid, ratio, EV, CPUR))

    # print table
    if rows:
        print("|Wp|Wc|m|PutMid|CallMid|TotMid|Ratio|EV|Credit/Risk|")
        print("|--:|--:|--:|-----:|------:|-----:|----:|--:|---------:|")
        for Wp,Wc,m,pm,cm,tm,ra,ev,cpur in rows:
            print(f"|{Wp}|{Wc}|{m}|{pm:.2f}|{cm:.2f}|{tm:.2f}|{ra:.2f}|{ev:.2f}|{cpur:.4f}|")
    else:
        print("No quoteable candidates.")
        emit_output(picked_put_width=str(DEFAULT_WP),
                    picked_call_width=str(DEFAULT_WP),
                    call_mult="1",
                    picked_width=str(DEFAULT_WP),
                    picked_ref="0.00", picked_metric="EV",
                    picked_ev="0.0000", base_ev="0.0000", delta_ev="0.0000",
                    five_mid=(f"{five_mid:.2f}" if five_mid else ""))
        return 0

    # baseline (DEFAULT symmetric, m=1)
    base = [r for r in rows if r[0]==DEFAULT_WP and r[1]==DEFAULT_WP and r[2]==1]
    if base:
        base_ev = base[0][7]
    else:
        # fallback: any with Wp==DEFAULT_WP as baseline
        base_ev = next((r[7] for r in rows if r[0]==DEFAULT_WP), rows[0][7])

    # choose best by EV, then by Credit/Risk, but require ratio gate
    qualified = [r for r in rows if r[6] >= CALL_MIN_RATIO]
    pool = qualified if qualified else rows
    best = max(pool, key=lambda r: (r[7], r[8], r[5]))  # EV, then CPUR, then TotMid

    Wp_sel, Wc_sel, m_sel, pm_sel, cm_sel, tm_sel, ra_sel, ev_sel, cpur_sel = best
    delta = ev_sel - (base_ev or 0.0)
    switch = (delta > EV_MIN_ADV)

    # If not pass EV gate, revert to baseline symmetric widths
    if not switch:
        Wp_sel, Wc_sel, m_sel = DEFAULT_WP, DEFAULT_WP, 1
        # recompute tm at baseline if available
        for r in rows:
            if r[0]==Wp_sel and r[1]==Wc_sel and r[2]==m_sel:
                tm_sel, ev_sel = r[5], r[7]
                delta = ev_sel - (base_ev or 0.0)
                break

    print(f"**Picked:** Wp={Wp_sel}, Wc={Wc_sel}, m={m_sel}, cond_mid={tm_sel:.2f} (metric=EV) base_ev={(base_ev or 0.0):.2f} ΔEV={delta:+.2f} {'→ Switch' if switch else '→ Stay'}")

    # outputs (and legacy aliases)
    emit_output(
        picked_put_width=str(Wp_sel),
        picked_call_width=str(Wc_sel),
        call_mult=str(m_sel),
        picked_width=str(Wp_sel),                   # legacy
        picked_ref=f"{tm_sel:.2f}",
        picked_metric="EV",
        picked_ev=f"{ev_sel:.4f}",
        base_ev=f"{(base_ev or 0.0):.4f}",
        delta_ev=f"{delta:.4f}",
        five_mid=(f"{five_mid:.2f}" if five_mid else "")
    )
    return 0

if __name__=="__main__":
    sys.exit(main())
