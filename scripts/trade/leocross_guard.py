#!/usr/bin/env python3
# VERSION: 2025-10-05 ShortIC fixed-width + contracts-per-equity sizing
__version__ = "3.3.0"

# LeoCross GUARD (stateless): derive 4 legs from GW, inspect Schwab positions & open orders,
# and emit a single decision for the orchestrator.

import os, sys, json, re, time, math, random
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime, date, timezone
from zoneinfo import ZoneInfo
import requests
from schwab.auth import client_from_token_file

# ----- Config knobs (credit spreads) -----
CREDIT_DOLLARS_PER_CONTRACT = float(os.environ.get("CREDIT_DOLLARS_PER_CONTRACT", "12000"))
# Default to 20-wide; still overridable via env.
CREDIT_SPREAD_WIDTH         = int(os.environ.get("CREDIT_SPREAD_WIDTH", "20"))
CREDIT_MIN_WIDTH            = 5  # SPX strikes trade in 5-point increments

ET = ZoneInfo("America/New_York")
GW_BASE = "https://gandalf.gammawizard.com"
GW_ENDPOINT = "/rapi/GetLeoCross"

QTY_TARGET = int(os.environ.get("QTY_TARGET","4") or "4")

# ------------- small utils -------------
def goutput(name: str, val: str):
    path = os.environ.get("GITHUB_OUTPUT")
    if path:
        with open(path, "a") as fh:
            fh.write(f"{name}={val}\n")

def yymmdd(iso: str) -> str:
    d = date.fromisoformat((iso or "")[:10])
    return "{:%y%m%d}".format(d)

def to_osi(sym: str) -> str:
    raw = (sym or "").upper()
    raw = re.sub(r'\s+', '', raw).lstrip('.')
    raw = re.sub(r'[^A-Z0-9.$^]', '', raw)
    m = re.match(r'^([A-Z.$^]{1,6})(\d{6})([CP])(\d{1,5})(?:\.(\d{1,3}))?$', raw) \
        or re.match(r'^([A-Z.$^]{1,6})(\d{6})([CP])(\d{8})$', raw)
    if not m: raise ValueError("Cannot parse option symbol: " + sym)
    root, ymd, cp, strike, frac = (m.groups()+("",))[:5]
    if len(strike)==8 and not frac:
        mills = int(strike)
    else:
        mills = int(strike)*1000 + (int((frac or "0").ljust(3,'0')) if frac else 0)
    return "{:<6s}{}{}{:08d}".format(root, ymd, cp, mills)

def osi_canon(osi: str):
    return (osi[6:12], osi[12], osi[-8:])  # (yymmdd, C/P, strike8)

def strike_from_osi(osi: str) -> float:
    return int(osi[-8:]) / 1000.0

def iso_z(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _credit_width() -> int:
    width = max(CREDIT_MIN_WIDTH, int(CREDIT_SPREAD_WIDTH))
    # round to nearest 5 upward to stay on SPX grid
    return int(math.ceil(width / 5.0) * 5)

def _round_half_up(x: float) -> int:
    return int(Decimal(x).quantize(Decimal('1'), rounding=ROUND_HALF_UP))

def calc_short_ic_width(opening_cash: float | int) -> int:
    """Retained for compatibility: ignores cash and returns configured credit width."""
    return _credit_width()

def calc_short_ic_contracts(opening_cash: float | int) -> int:
    """Short-IC sizing: $4,000 per 5-wide, scaled by width, half-up rounding, min 1."""
    try:
        oc = float(opening_cash)
    except Exception:
        oc = 0.0
    width = _credit_width()
    denom = 4000.0 * (width / 5.0)
    units = _round_half_up(oc / denom)
    return max(1, int(units))

# ------------- Schwab helpers -------------
def schwab_client():
    app_key=os.environ["SCHWAB_APP_KEY"]; app_secret=os.environ["SCHWAB_APP_SECRET"]; token_json=os.environ["SCHWAB_TOKEN_JSON"]
    with open("schwab_token.json","w") as f: f.write(token_json)
    c=client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")
    r=c.get_account_numbers(); r.raise_for_status()
    acct_info=r.json()[0]
    acct_hash=str(acct_info["hashValue"])
    acct_num =str(acct_info.get("accountNumber") or acct_info.get("account_number") or "")
    return c, acct_hash, acct_num

def _sleep_for_429(r, attempt):
    ra = r.headers.get("Retry-After")
    if ra:
        try:
            return max(1.0, float(ra))
        except Exception:
            pass
    # exponential backoff + small jitter
    return min(10.0, 0.5 * (2 ** attempt)) + random.uniform(0.0, 0.25)


def schwab_get_json(c, url, params=None, tries=6, tag=""):
    last = ""
    for i in range(tries):
        try:
            r = c.session.get(url, params=(params or {}), timeout=20)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                time.sleep(_sleep_for_429(r, i))
                continue
            last = f"HTTP_{r.status_code}:{(r.text or '')[:160]}"
        except Exception as e:
            last = f"{type(e).__name__}:{str(e)}"
        time.sleep(0.5 * (2 ** i))
    raise RuntimeError(f"SCHWAB_GET_FAIL({tag}) {last}")

def _osi_from_instrument(ins: dict):
    sym = (ins.get("symbol") or "")
    try:
        return to_osi(sym)
    except Exception:
        pass
    exp = ins.get("optionExpirationDate") or ins.get("expirationDate") or ""
    pc  = (ins.get("putCall") or ins.get("type") or "").upper()
    strike = ins.get("strikePrice") or ins.get("strike")
    try:
        if exp and pc in ("CALL","PUT") and strike is not None:
            ymd = date.fromisoformat(str(exp)[:10]).strftime("%y%m%d")
            cp = "C" if pc.startswith("C") else "P"
            mills = int(round(float(strike)*1000))
            return "{:<6s}{}{}{:08d}".format("SPXW", ymd, cp, mills)
    except Exception:
        pass
    return None

def positions_map(c, acct_hash: str):
    url=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}"
    j=schwab_get_json(c,url,params={"fields":"positions"},tag="POSITIONS")
    sa=j[0]["securitiesAccount"] if isinstance(j,list) else (j.get("securitiesAccount") or j)
    out={}
    for p in (sa.get("positions") or []):
        ins=p.get("instrument",{}) or {}
        atype = (ins.get("assetType") or ins.get("type") or "").upper()
        if atype != "OPTION": continue
        osi = _osi_from_instrument(ins)
        if not osi: continue
        qty=float(p.get("longQuantity",0))-float(p.get("shortQuantity",0))
        if abs(qty)<1e-9: continue
        out[osi_canon(osi)] = out.get(osi_canon(osi), 0.0) + qty
    return out

def list_matching_open_ids(c, acct_hash: str, canon_set):
    now_et = datetime.now(ET)
    start_et = now_et.replace(hour=0, minute=0, second=0, microsecond=0)
    url=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders"
    params = {"fromEnteredTime": iso_z(start_et), "toEnteredTime": iso_z(now_et), "maxResults": 200}
    try:
        arr = schwab_get_json(c, url, params=params, tag="ORDERS") or []
    except Exception:
        return []
    out=[]
    for o in arr or []:
        st=str(o.get("status") or "").upper()
        if st not in ("WORKING","QUEUED","PENDING_ACTIVATION","OPEN"): continue
        got=set()
        for leg in (o.get("orderLegCollection") or []):
            ins=(leg.get("instrument",{}) or {})
            sym=(ins.get("symbol") or "")
            osi=None
            try: osi=to_osi(sym)
            except: osi=_osi_from_instrument(ins)
            if osi:
                got.add(osi_canon(osi))
        if got==canon_set:
            oid=str(o.get("orderId") or "")
            if oid: out.append(oid)
    return out

# ------------- opening cash (for width) -------------
def opening_cash_for_account(c, acct_number: str):
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
            chosen=(init,curr)

    if not chosen: return None
    init, curr = chosen
    oc = pick(init,"cashBalance","cashAvailableForTrading","liquidationValue")
    if oc is None:
        oc = pick(curr,"cashBalance","cashAvailableForTrading","liquidationValue")
    return oc

# ------------- GammaWizard -------------
def _sanitize_token(t: str) -> str:
    t = (t or "").strip().strip('"').strip("'")
    return t.split(None,1)[1] if t.lower().startswith("bearer ") else t

def _gw_timeout():
    try: return int(os.environ.get("GW_TIMEOUT","30"))
    except: return 30

def gw_login_token():
    email=os.environ.get("GW_EMAIL",""); pwd=os.environ.get("GW_PASSWORD","")
    if not (email and pwd): raise RuntimeError("GW_LOGIN_MISSING_CREDS")
    r=requests.post(f"{GW_BASE}/goauth/authenticateFireUser", data={"email":email,"password":pwd}, timeout=_gw_timeout())
    if r.status_code!=200: raise RuntimeError(f"GW_LOGIN_HTTP_{r.status_code}:{(r.text or '')[:180]}")
    j=r.json(); t=j.get("token")
    if not t: raise RuntimeError("GW_LOGIN_NO_TOKEN")
    return t

def gw_get_leocross():
    tok=_sanitize_token(os.environ.get("GW_TOKEN","") or "")
    def hit(t):
        h={"Accept":"application/json","Authorization":f"Bearer {_sanitize_token(t)}","User-Agent":"gw-guard/1.1"}
        return requests.get(f"{GW_BASE.rstrip('/')}/{GW_ENDPOINT.lstrip('/')}", headers=h, timeout=_gw_timeout())
    r=hit(tok) if tok else None
    if (r is None) or (r.status_code in (401,403)): r=hit(gw_login_token())
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

# ------------- logic -------------
def main():
    # Schwab client
    try:
        c, acct_hash, acct_num = schwab_client()
    except Exception as e:
        msg=str(e)
        reason="SCHWAB_OAUTH_REFRESH_FAILED — rotate SCHWAB_TOKEN_JSON" if ("unsupported_token_type" in msg or "refresh_token_authentication_error" in msg) else ("SCHWAB_CLIENT_INIT_FAILED — " + msg[:200])
        print("GUARD SKIP:", reason)
        for k,v in {"action":"SKIP","reason":reason,"rem_qty":"","legs_json":"[]",
                    "canon_key":"","is_credit":"", "open_order_ids":"","signal_date":""}.items():
            goutput(k,v)
        return 0

    # GW → trade
    try:
        api=gw_get_leocross(); tr=extract_trade(api)
        if not tr:
            print("GUARD SKIP: NO_TRADE_PAYLOAD")
            for k,v in {"action":"SKIP","reason":"NO_TRADE_PAYLOAD","rem_qty":"","legs_json":"[]",
                        "canon_key":"","is_credit":"", "open_order_ids":"","signal_date":""}.items():
                goutput(k,v)
            return 0
    except Exception as e:
        reason=f"GW_FETCH_FAILED — {str(e)[:200]}"
        print("GUARD SKIP:", reason)
        for k,v in {"action":"SKIP","reason":reason,"rem_qty":"","legs_json":"[]",
                    "canon_key":"","is_credit":"", "open_order_ids":"","signal_date":""}.items():
            goutput(k,v)
        return 0

    sig_date=str(tr.get("Date",""))
    exp_iso=str(tr.get("TDate","")); exp6=yymmdd(exp_iso)
    inner_put=int(float(tr.get("Limit"))); inner_call=int(float(tr.get("CLimit")))

    def fnum(x):
        try: return float(x)
        except: return None
    cat1=fnum(tr.get("Cat1")); cat2=fnum(tr.get("Cat2"))
    is_credit = True if (cat2 is None or cat1 is None or cat2>=cat1) else False

    # determine width
    oc = opening_cash_for_account(c, acct_num)
    if is_credit and oc is None:
        reason="OPENING_CASH_UNAVAILABLE — cannot compute Short IC width."
        print("GUARD SKIP:", reason)
        for k,v in {"action":"SKIP","reason":reason,"rem_qty":"","legs_json":"[]",
                    "canon_key":"","is_credit":"true","open_order_ids":"","signal_date":sig_date}.items():
            goutput(k,v)
        return 0

    width = calc_short_ic_width(oc) if is_credit else 5

    # legs (unoriented base strikes)
    if is_credit:
        # PUSHED-OUT shorts by 5; wings ±width from those shorts
        sell_put  = inner_put  - 5
        buy_put   = sell_put   - width
        sell_call = inner_call + 5
        buy_call  = sell_call  + width
        p_low, p_high = buy_put, sell_put
        c_low, c_high = sell_call, buy_call
    else:
        # LONG path unchanged (same-shorts)
        p_low, p_high = inner_put - width, inner_put
        c_low, c_high = inner_call, inner_call + width
    bp = to_osi(f".SPXW{exp6}P{p_low}")
    sp = to_osi(f".SPXW{exp6}P{p_high}")
    sc = to_osi(f".SPXW{exp6}C{c_low}")
    bc = to_osi(f".SPXW{exp6}C{c_high}")

    # orient so BUY legs are protective wings for credit; reverse for debit
    def orient(bp,sp,sc,bc):
        bpS=strike_from_osi(bp); spS=strike_from_osi(sp)
        scS=strike_from_osi(sc); bcS=strike_from_osi(bc)
        if is_credit:
            if bpS>spS: bp,sp = sp,bp
            if scS>bcS: sc,bc = bc,sc
        else:
            if bpS<spS: bp,sp = sp,bp
            if bcS>scS: sc,bc = bc,sc
        return [bp,sp,sc,bc]
    legs = orient(bp,sp,sc,bc)
    canon = {osi_canon(x) for x in legs}

    # positions
    try:
        pos = positions_map(c, acct_hash)
    except Exception as e:
        reason=f"POSITIONS_FAILED — {str(e)[:200]}"
        print("GUARD SKIP:", reason)
        for k,v in {"action":"SKIP","reason":reason,"rem_qty":"","legs_json":json.dumps(legs),
                    "canon_key":"","is_credit":("true" if is_credit else "false"),"open_order_ids":"","signal_date":sig_date}.items():
            goutput(k,v)
        return 0

    # snapshot
    labels=[("BUY_PUT",legs[0],-1),("SELL_PUT",legs[1],+1),("SELL_CALL",legs[2],+1),("BUY_CALL",legs[3],-1)]
    print(f"GUARD SNAPSHOT ({'CREDIT' if is_credit else 'DEBIT'} width={width}):")
    for name, osi, sign in labels:
        cur = pos.get(osi_canon(osi), 0.0)
        print(f"  {name:10s} {osi}  acct_qty={cur:+g}  sign={sign:+d}")

    # NO‑CLOSE rule + overlap classification
    any_opposite=False; nonzero_count=0; aligned_count=0
    for _, osi, sign in labels:
        cur = pos.get(osi_canon(osi), 0.0)
        if abs(cur)>1e-9: nonzero_count+=1
        if (sign<0 and cur<0) or (sign>0 and cur>0): any_opposite=True
        if (sign<0 and cur>=0) or (sign>0 and cur<=0):
            if abs(cur)>1e-9: aligned_count+=1

    open_ids = list_matching_open_ids(c, acct_hash, canon) or []
    action=""; rem_qty=""; reason=""

    # units open (only meaningful when all 4 aligned)
    def condor_units_open(pos_map, legs):
        b1 = max(0.0,  pos_map.get(osi_canon(legs[0]), 0.0))
        b2 = max(0.0,  pos_map.get(osi_canon(legs[3]), 0.0))
        s1 = max(0.0, -pos_map.get(osi_canon(legs[1]), 0.0))
        s2 = max(0.0, -pos_map.get(osi_canon(legs[2]), 0.0))
        return int(min(b1, b2, s1, s2))

    target_qty = calc_short_ic_contracts(oc) if is_credit else max(0, QTY_TARGET)

    if any_opposite:
        action="SKIP"; reason="WOULD_CLOSE"
    elif (nonzero_count==0) and not open_ids:
        action="NEW"; rem_qty=str(target_qty)
    elif (nonzero_count==4 and aligned_count==4) and not open_ids:
        uo = condor_units_open(pos, legs)
        if uo >= target_qty:
            action="SKIP"; reason="AT_OR_ABOVE_TARGET"
        else:
            action="NEW"; rem_qty=str(target_qty - uo)
    else:
        action="REPRICE_EXISTING"
        reason = "PARTIAL_OVERLAP or WORKING_ORDER"

    canon_key = f"{legs[0][6:12]}:P{legs[0][-8:]}-{legs[1][-8:]}:C{legs[2][-8:]}-{legs[3][-8:]}"

    # Emit outputs
    outs = {
        "action": action,
        "reason": reason,
        "rem_qty": rem_qty,
        "legs_json": json.dumps(legs),
        "canon_key": canon_key,
        "is_credit": "true" if is_credit else "false",
        "open_order_ids": ",".join(open_ids),
        "signal_date": sig_date,
    }
    for k,v in outs.items(): goutput(k, v)

    print(f"GUARD DECISION: action={action} rem_qty={rem_qty or 'NA'} width={width} open_order_ids={outs['open_order_ids']}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
