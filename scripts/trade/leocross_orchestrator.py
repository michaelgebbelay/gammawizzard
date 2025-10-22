#!/usr/bin/env python3
# ORCHESTRATOR — handles CREDIT (short IC) and DEBIT (long IC). Same‑shorts only.
# Sizing:
#   CREDIT: round‑half‑up( opening_cash / (CREDIT_UNIT_DOLLARS * width/5) ), min 1
#   DEBIT:  floor(       opening_cash /  LONG_UNIT_DOLLARS ),               min 1
#
# Width:
#   CREDIT: from env CREDIT_SPREAD_WIDTH (usually the picker output)
#   DEBIT:  fixed 5‑wide
#
# Env honored:
#   SIDE_OVERRIDE = AUTO|CREDIT|DEBIT
#   CREDIT_SPREAD_WIDTH (e.g., "20", or picker output)
#   QTY_OVERRIDE (optional hard override)
#   SIZING_DOLLARS_OVERRIDE (optional – pretend cash for sizing)
#   CREDIT_UNIT_DOLLARS (default 4000)
#   LONG_UNIT_DOLLARS   (default 4000)

import os, sys, re, json, math
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
import requests
from schwab.auth import client_from_token_file

# ---------- small utils ----------
def _round_half_up(x: float) -> int:
    return int(Decimal(x).quantize(Decimal('1'), rounding=ROUND_HALF_UP))

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
    # make sure long wings are farther and shorts are inside for CREDIT condor
    bpS=strike_from_osi(bp); spS=strike_from_osi(sp)
    scS=strike_from_osi(sc); bcS=strike_from_osi(bc)
    if bpS>spS: bp,sp=sp,bp
    if scS>bcS: sc,bc=bc,sc
    return [bp,sp,sc,bc]

def orient_debit(bp,sp,sc,bc):
    # long IC (DEBIT): buy the inner strikes, sell the outer (reverse orientation vs credit)
    # We still emit in standard [BUY_PUT, SELL_PUT, SELL_CALL, BUY_CALL] order.
    bpS=strike_from_osi(bp); spS=strike_from_osi(sp)
    scS=strike_from_osi(sc); bcS=strike_from_osi(bc)
    # Put wing: inner (higher strike) is BUY, outer (lower) is SELL for long IC
    if bpS > spS:   # bp is higher strike than sp
        buy_put, sell_put = sp, bp
    else:
        buy_put, sell_put = bp, sp
    # Call wing: inner (lower strike) is BUY, outer (higher) is SELL for long IC
    if scS < bcS:   # sc is lower than bc
        buy_call, sell_call = sc, bc
    else:
        buy_call, sell_call = bc, sc
    # Return in standard order: BUY_PUT, SELL_PUT, SELL_CALL, BUY_CALL
    return [to_osi(buy_put), to_osi(sell_put), to_osi(sell_call), to_osi(buy_call)]

def build_legs_same_shorts(exp6: str, inner_put: int, inner_call: int, width: int, *, is_credit: bool):
    p_low, p_high = inner_put - width, inner_put
    c_low, c_high = inner_call, inner_call + width
    bp = to_osi(f".SPXW{exp6}P{p_low}")
    sp = to_osi(f".SPXW{exp6}P{p_high}")
    sc = to_osi(f".SPXW{exp6}C{c_low}")
    bc = to_osi(f".SPXW{exp6}C{c_high}")
    return orient_credit(bp,sp,sc,bc) if is_credit else orient_debit(bp,sp,sc,bc)

# ---------- Schwab ----------
def schwab_client_and_acct():
    app_key=os.environ["SCHWAB_APP_KEY"]; app_secret=os.environ["SCHWAB_APP_SECRET"]; token_json=os.environ["SCHWAB_TOKEN_JSON"]
    with open("schwab_token.json","w") as f: f.write(token_json)
    c=client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")
    r=c.get_account_numbers(); r.raise_for_status()
    acct_info=r.json()[0]
    acct_hash=str(acct_info["hashValue"])
    acct_num =str(acct_info.get("accountNumber") or acct_info.get("account_number") or "")
    return c, acct_hash, acct_num

def opening_cash_for_account(c, acct_number: str|None):
    r = c.get_accounts(); r.raise_for_status()
    data = r.json()
    accs = data if isinstance(data, list) else [data]

    def pick(d,*ks):
        for k in ks:
            v = (d or {}).get(k)
            if isinstance(v,(int,float)): return float(v)

    def hunt(a):
        acct_id=None; initial={}; current={}
        stack=[a]
        while stack:
            x=stack.pop()
            if isinstance(x,dict):
                if acct_id is None and x.get("accountNumber"): acct_id=str(x["accountNumber"])
                if "initialBalances" in x and isinstance(x["initialBalances"], dict): initial=x["initialBalances"]
                if "currentBalances" in x and isinstance(x["currentBalances"], dict): current=x["currentBalances"]
                for v in x.values():
                    if isinstance(v,(dict,list)): stack.append(v)
            elif isinstance(x,list):
                stack.extend(x)
        return acct_id, initial, current

    chosen=None
    for a in accs:
        aid, init, curr = hunt(a)
        if acct_number and aid == acct_number:
            chosen=(init,curr); break
        if chosen is None:
            chosen=(init,curr)  # fallback to first seen

    if not chosen: return 0.0
    init, curr = chosen
    oc = pick(init,"cashBalance","cashAvailableForTrading","liquidationValue")
    if oc is None:
        oc = pick(curr,"cashBalance","cashAvailableForTrading","liquidationValue")
    try:
        return float(oc or 0.0)
    except:
        return 0.0

# ---------- GammaWizard ----------
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
    # GW
    tr = extract_trade(gw_fetch())
    if not tr:
        print("ORCH ABORT: no trade payload from GW")
        return 1
    exp6 = yymmdd(str(tr.get("TDate","")))
    inner_put  = int(float(tr.get("Limit")))
    inner_call = int(float(tr.get("CLimit")))

    # Side (AUTO from Cat1/Cat2, with SIDE_OVERRIDE support)
    def fnum(x):
        try: return float(x)
        except: return None
    cat1=fnum(tr.get("Cat1")); cat2=fnum(tr.get("Cat2"))
    is_credit_auto = True if (cat2 is None or cat1 is None or cat2>=cat1) else False
    side_override = (os.environ.get("SIDE_OVERRIDE","AUTO") or "AUTO").upper()
    if side_override == "CREDIT": is_credit = True
    elif side_override == "DEBIT": is_credit = False
    else: is_credit = is_credit_auto

    # Widths
    if is_credit:
        w_env = os.environ.get("CREDIT_SPREAD_WIDTH","20").strip()
        try: width = int(w_env)
        except: width = 20
        width = max(5, int(math.ceil(width/5.0)*5))
    else:
        width = 5  # long IC is always 5‑wide in this workflow

    # Schwab + opening cash
    c, acct_hash, acct_num = schwab_client_and_acct()

    oc_override_raw = os.environ.get("SIZING_DOLLARS_OVERRIDE","").strip()
    oc_override = None
    if oc_override_raw:
        try: oc_override = float(oc_override_raw)
        except: oc_override = None

    oc_real = opening_cash_for_account(c, acct_num)
    oc = oc_override if (oc_override is not None and oc_override > 0) else oc_real

    # Sizing
    CREDIT_UNIT_DOLLARS = float(os.environ.get("CREDIT_UNIT_DOLLARS","4000"))
    LONG_UNIT_DOLLARS   = float(os.environ.get("LONG_UNIT_DOLLARS","4000"))
    if is_credit:
        denom = CREDIT_UNIT_DOLLARS * (width/5.0)
        qty = max(1, _round_half_up((float(oc) if oc is not None else 0.0) / denom))
    else:
        denom = LONG_UNIT_DOLLARS
        qty = max(1, int((float(oc) if oc is not None else 0.0) // denom))

    qov = os.environ.get("QTY_OVERRIDE","").strip()
    if qov:
        try: qty = max(1, int(qov))
        except: pass

    # Legs
    legs = build_legs_same_shorts(exp6, inner_put, inner_call, width, is_credit=is_credit)

    mode = (os.environ.get("PLACER_MODE","NOW") or "NOW").upper()
    print(f"ORCH GATE disabled (MODE={mode})")
    print(f"ORCH CONFIG: side={'CREDIT' if is_credit else 'DEBIT'}, width={width}, mode={mode}")
    print(f"ORCH SIZE: qty={qty} oc={oc:.2f} acct={acct_num or 'NA'} denom={denom:.2f}")
    print("ORCH → PLACER")

    # Pass to placer
    env = dict(os.environ)
    env["SIDE"] = "CREDIT" if is_credit else "DEBIT"
    env["WIDTH"] = str(width)
    env["QTY"]   = str(qty)
    env["OCC_BUY_PUT"]   = legs[0]
    env["OCC_SELL_PUT"]  = legs[1]
    env["OCC_SELL_CALL"] = legs[2]
    env["OCC_BUY_CALL"]  = legs[3]

    # Reuse Schwab token
    env["SCHWAB_APP_KEY"]    = os.environ["SCHWAB_APP_KEY"]
    env["SCHWAB_APP_SECRET"] = os.environ["SCHWAB_APP_SECRET"]
    env["SCHWAB_TOKEN_JSON"] = os.environ["SCHWAB_TOKEN_JSON"]

    rc = os.spawnve(os.P_WAIT, sys.executable, [sys.executable, "scripts/trade/leocross_place_simple.py"], env)
    return rc

if __name__=="__main__":
    sys.exit(main())
