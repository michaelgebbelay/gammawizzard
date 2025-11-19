#!/usr/bin/env python3
# ORCHESTRATOR — CREDIT: unbalanced IC (Wp, Wc, m=2). DEBIT: symmetric 5‑wide IC.
# Sizing: CREDIT uses $SIZING_PER_5WIDE per 5‑wide of *put* width; DEBIT uses $SIZING_PER_LONG per 5‑wide long IC.

import os, sys, re, math, json
from datetime import date
import requests
from decimal import Decimal, ROUND_HALF_UP
from schwab.auth import client_from_token_file

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

def build_legs_credit(exp6: str, inner_put: int, inner_call: int, Wp: int, Wc: int):
    p_low, p_high = inner_put - Wp, inner_put
    c_low, c_high = inner_call, inner_call + Wc
    return orient_credit(
        to_osi(f".SPXW{exp6}P{p_low}"),
        to_osi(f".SPXW{exp6}P{p_high}"),
        to_osi(f".SPXW{exp6}C{c_low}"),
        to_osi(f".SPXW{exp6}C{c_high}")
    )

def build_legs_debit(exp6: str, inner_put: int, inner_call: int, W: int):
    # Long IC (DEBIT) 5‑wide, oriented for DEBIT
    p_low, p_high = inner_put - W, inner_put
    c_low, c_high = inner_call, inner_call + W
    bp = to_osi(f".SPXW{exp6}P{p_high}")   # buy higher put
    sp = to_osi(f".SPXW{exp6}P{p_low}")    # sell lower put
    sc = to_osi(f".SPXW{exp6}C{c_high}")   # sell higher call
    bc = to_osi(f".SPXW{exp6}C{c_low}")    # buy lower call
    return [bp,sp,sc,bc]

def _round_half_up(x: float) -> int:
    return int(Decimal(x).quantize(Decimal('1'), rounding=ROUND_HALF_UP))

# ---- GW
def _sanitize_token(t: str) -> str:
    t=(t or "").strip().strip('"').strip("'");  return t.split(None,1)[1] if t.lower().startswith("bearer ") else t

def gw_fetch():
    base = os.environ.get("GW_BASE","https://gandalf.gammawizard.com").rstrip("/")
    endpoint = os.environ.get("GW_ENDPOINT","/rapi/GetLeoProfit")
    tok=_sanitize_token(os.environ.get("GW_TOKEN","") or "")
    def hit(t):
        h={"Accept":"application/json"}
        if t: h["Authorization"]=f"Bearer {_sanitize_token(t)}"
        return requests.get(f"{base}/{endpoint.lstrip('/')}", headers=h, timeout=30)
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
        for v in j.values():
            if isinstance(v,(dict,list)):
                t=extract_trade(v)
                if t: return t
    if isinstance(j,list):
        for it in reversed(j):
            t=extract_trade(it)
            if t: return t
    return {}

# ---- Schwab
def schwab_client():
    app_key=os.environ["SCHWAB_APP_KEY"]; app_secret=os.environ["SCHWAB_APP_SECRET"]; token_json=os.environ["SCHWAB_TOKEN_JSON"]
    with open("schwab_token.json","w") as f: f.write(token_json)
    return client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")

def opening_cash_for_account(c, prefer_number=None):
    """
    Robust balance hunt. Returns (value, source_key, acct_number).
    Prefers cashAvailableForTrading, then cashBalance, then liquidationValue, with several fallbacks.
    """
    r=c.get_accounts(); r.raise_for_status()
    data = r.json()
    arr = data if isinstance(data,list) else [data]

    # Pick the right account if multiple
    if prefer_number is None:
        try:
            rr = c.get_account_numbers(); rr.raise_for_status()
            prefer_number = str((rr.json() or [{}])[0].get("accountNumber") or "")
        except Exception:
            prefer_number = None

    def hunt(a):
        acct_id=None; init={}; curr={}
        stack=[a]
        while stack:
            x=stack.pop()
            if isinstance(x,dict):
                if acct_id is None and x.get("accountNumber"): acct_id=str(x["accountNumber"])
                if "initialBalances" in x and isinstance(x["initialBalances"], dict): init=x["initialBalances"]
                if "currentBalances" in x and isinstance(x["currentBalances"], dict): curr=x["currentBalances"]
                for v in x.values():
                    if isinstance(v,(dict,list)): stack.append(v)
            elif isinstance(x,list):
                stack.extend(x)
        return acct_id, init, curr

    chosen = None
    for a in arr:
        aid, init, curr = hunt(a)
        if prefer_number and aid == prefer_number:
            chosen=(aid,init,curr); break
        if chosen is None:
            chosen=(aid,init,curr)

    if not chosen: return 0.0, "none", ""
    aid, init, curr = chosen

    # Priority order
    keys = [
        "cashAvailableForTrading",
        "cashBalance",
        "availableFundsNonMarginableTrade",
        "buyingPowerNonMarginableTrade",
        "buyingPower",
        "optionBuyingPower",
        "liquidationValue",
    ]

    def pick(src):
        for k in keys:
            v = src.get(k)
            if isinstance(v,(int,float)): return float(v), k
        return None

    for src in (init, curr):
        got = pick(src)
        if got: return got[0], got[1], aid

    # Last resort
    return 0.0, "none", aid

def floor5(x: float) -> int:
    return int(math.floor(float(x)/5.0)*5)

def main():
    MODE = (os.environ.get("PLACER_MODE","NOW") or "NOW").upper()

    # LeoProfit payload (for exp/shorts + side)
    tr = extract_trade(gw_fetch())
    exp6 = yymmdd(str(tr.get("TDate","")))
    inner_put  = int(float(tr.get("Limit")))
    inner_call = int(float(tr.get("CLimit")))

    def fnum(x): 
        try: return float(x)
        except: return None
    cat1=fnum(tr.get("Cat1")); cat2=fnum(tr.get("Cat2"))
    is_credit = True if (cat2 is None or cat1 is None or cat2>=cat1) else False

    SIDE_OVERRIDE = (os.environ.get("SIDE_OVERRIDE","AUTO") or "AUTO").upper()
    if SIDE_OVERRIDE == "CREDIT": is_credit=True
    elif SIDE_OVERRIDE == "DEBIT": is_credit=False

    # Widths / ratio for CREDIT (from picker) — default to Wc=floor(Wp/2), m=2
    Wp_env = (os.environ.get("CREDIT_PUT_WIDTH","") or "").strip()
    Wc_env = (os.environ.get("CREDIT_CALL_WIDTH","") or "").strip()
    m_env  = (os.environ.get("CALL_MULT","") or "").strip()

    if is_credit:
        Wp = int(Wp_env) if Wp_env.isdigit() else int(os.environ.get("CREDIT_SPREAD_WIDTH","20"))
        Wp = max(5, int(math.ceil(Wp/5.0)*5))
        Wc = int(Wc_env) if Wc_env.isdigit() else max(5, floor5(Wp/2.0))
        m  = int(m_env) if (m_env.isdigit() and int(m_env)>=1) else 2
        legs = build_legs_credit(exp6, inner_put, inner_call, Wp, Wc)
    else:
        Wp, Wc, m = 5, 5, 1
        legs = build_legs_debit(exp6, inner_put, inner_call, 5)

    # Schwab + sizing
    c = schwab_client()
    oc_val, oc_src, acct_num = opening_cash_for_account(c)

    # Allow override via workflow input
    ov_raw = (os.environ.get("SIZING_DOLLARS_OVERRIDE","") or "").strip()
    if ov_raw:
        try: oc_val = float(ov_raw)
        except: pass

    SIZING_PER_5WIDE = float(os.environ.get("SIZING_PER_5WIDE","6000"))
    SIZING_PER_LONG  = float(os.environ.get("SIZING_PER_LONG","6000"))

    if is_credit:
        risk_w = float(Wp)              # risk driver = put width
        denom  = SIZING_PER_5WIDE * (risk_w/5.0)
    else:
        risk_w = 5.0                    # long IC is always 5‑wide in this flow
        denom  = SIZING_PER_LONG        # 1 long IC per $SIZING_PER_LONG

    qty = max(1, _round_half_up((float(oc_val) if oc_val is not None else 0.0) / max(1e-9, denom)))

    # Manual qty override if provided (BYPASS_QTY in workflow)
    qov = (os.environ.get("BYPASS_QTY","") or "").strip()
    if qov:
        try: qty = max(1, int(qov))
        except: pass

    side_txt = "CREDIT" if is_credit else "DEBIT"
    struct = "CONDOR_RATIO" if (is_credit and m>1) else "CONDOR"
    print(f"ORCH GATE disabled (MODE={MODE})")
    print(f"ORCH CONFIG: side={side_txt}, Wp={Wp}, Wc={Wc}, call_mult={m}, mode={MODE}")
    print(f"ORCH SIZE: base_qty={qty} oc={oc_val:.2f} (src={oc_src}, acct={acct_num}) denom={denom:.2f} structure={struct}")

    # Pass to placer via env
    env = dict(os.environ)
    env.update({
        "SIDE": side_txt,
        "STRUCTURE": struct,
        "QTY": str(qty),
        "PUT_WIDTH": str(Wp),
        "CALL_WIDTH": str(Wc),
        "CALL_MULT": str(m),
        "OCC_BUY_PUT":  legs[0],
        "OCC_SELL_PUT": legs[1],
        "OCC_SELL_CALL":legs[2],
        "OCC_BUY_CALL":  legs[3],
        # reuse Schwab token
        "SCHWAB_APP_KEY": os.environ["SCHWAB_APP_KEY"],
        "SCHWAB_APP_SECRET": os.environ["SCHWAB_APP_SECRET"],
        "SCHWAB_TOKEN_JSON": os.environ["SCHWAB_TOKEN_JSON"],
    })

    rc = os.spawnve(os.P_WAIT, sys.executable, [sys.executable, "scripts/trade/leoprofit/leoprofit_place_simple.py"], env)
    return rc

if __name__=="__main__":
    sys.exit(main())
