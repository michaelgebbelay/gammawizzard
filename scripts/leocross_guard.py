# LeoCross GUARD: fetch Leo signal, compute legs, check Schwab for overlap/conflict.
# If any overlap or conflict is detected (positions or working orders), set proceed=false.
# Otherwise proceed=true so the placer can run.
#
# Outputs for GitHub Actions (written to $GITHUB_OUTPUT):
#   proceed=true|false
#   reason=<text>

import os, sys, json, time, re
from datetime import datetime, date, timezone
from zoneinfo import ZoneInfo
import requests
from schwab.auth import client_from_token_file

ET = ZoneInfo("America/New_York")
GW_BASE = "https://gandalf.gammawizard.com"
GW_ENDPOINT = "/rapi/GetLeoCross"

# ---------- tiny utils ----------
def _sanitize_token(t: str) -> str:
    t = (t or "").strip().strip('"').strip("'")
    return t.split(None,1)[1] if t.lower().startswith("bearer ") else t

def yymmdd(iso: str) -> str:
    d = date.fromisoformat((iso or "")[:10])
    return "{:%y%m%d}".format(d)

def to_osi(sym: str) -> str:
    raw = (sym or "").strip().upper().lstrip(".").replace("_","")
    m = re.match(r'^([A-Z.$^]{1,6})(\d{6})([CP])(\d{1,5})(?:\.(\d{1,3}))?$', raw) or \
        re.match(r'^([A-Z.$^]{1,6})(\d{6})([CP])(\d{8})$', raw)
    if not m: raise ValueError("Cannot parse option symbol: " + sym)
    root, ymd, cp, strike, frac = (m.groups()+("",))[:5]
    mills = int(strike)*1000 + (int((frac or "0").ljust(3,'0')) if frac else 0) if len(strike)<8 else int(strike)
    return "{:<6s}{}{}{:08d}".format(root, ymd, cp, mills)

def osi_canon(osi: str):
    return (osi[6:12], osi[12], osi[-8:])  # (yymmdd, C/P, strike8)

def strike_from_osi(osi: str) -> float:
    return int(osi[-8:]) / 1000.0

def iso_z(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def goutput(name: str, val: str):
    path = os.environ.get("GITHUB_OUTPUT")
    if path:
        with open(path, "a") as fh:
            fh.write("{}={}\n".format(name, val))

# ---------- GW auth & fetch ----------
def _gw_timeout():
    try: return int(os.environ.get("GW_TIMEOUT","30"))
    except: return 30

def gw_login_token():
    email=os.environ.get("GW_EMAIL",""); pwd=os.environ.get("GW_PASSWORD","")
    if not (email and pwd): raise RuntimeError("GW_LOGIN_MISSING_CREDS")
    r=requests.post("{}/goauth/authenticateFireUser".format(GW_BASE), data={"email":email,"password":pwd}, timeout=_gw_timeout())
    if r.status_code!=200: raise RuntimeError("GW_LOGIN_HTTP_{}:{}".format(r.status_code, (r.text or "")[:180]))
    j=r.json(); t=j.get("token")
    if not t: raise RuntimeError("GW_LOGIN_NO_TOKEN")
    return t

def gw_get_leocross():
    tok=_sanitize_token(os.environ.get("GW_TOKEN","") or "")
    def hit(t):
        h={"Accept":"application/json","Authorization":"Bearer {}".format(_sanitize_token(t)),"User-Agent":"gw-guard/1.0"}
        return requests.get("{}/{}".format(GW_BASE.rstrip("/"), GW_ENDPOINT.lstrip("/")), headers=h, timeout=_gw_timeout())
    r=hit(tok) if tok else None
    if (r is None) or (r.status_code in (401,403)): r=hit(gw_login_token())
    if r.status_code!=200: raise RuntimeError("GW_HTTP_{}:{}".format(r.status_code, (r.text or "")[:180]))
    return r.json()

# ---------- Schwab helpers ----------
def _backoff(i): return 0.6*(2**i)

def schwab_get_json(c, url, params=None, tries=6, tag=""):
    last=""
    for i in range(tries):
        try:
            r=c.session.get(url, params=(params or {}), timeout=20)
            if r.status_code==200: return r.json()
            last="HTTP_{:d}:{:s}".format(r.status_code, (r.text or "")[:160])
        except Exception as e:
            last="{}:{}".format(type(e).__name__, str(e))
        time.sleep(_backoff(i))
    raise RuntimeError("SCHWAB_GET_FAIL({}) {}".format(tag, last))

def positions_map(c, acct_hash: str):
    url="https://api.schwabapi.com/trader/v1/accounts/{}".format(acct_hash)
    j=schwab_get_json(c,url,params={"fields":"positions"},tag="POSITIONS")
    sa=j[0]["securitiesAccount"] if isinstance(j,list) else (j.get("securitiesAccount") or j)
    out={}
    for p in (sa.get("positions") or []):
        ins=p.get("instrument",{}) or {}
        if (ins.get("assetType") or "").upper()!="OPTION": continue
        sym=ins.get("symbol","")
        try: key=osi_canon(to_osi(sym))
        except: continue
        qty=float(p.get("longQuantity",0))-float(p.get("shortQuantity",0))
        if abs(qty)<1e-9: continue
        out[key]=out.get(key,0.0)+qty
    return out

def list_matching_open_ids(c, acct_hash: str, canon_set):
    now_et = datetime.now(ET)
    start_et = now_et.replace(hour=0, minute=0, second=0, microsecond=0)
    url="https://api.schwabapi.com/trader/v1/accounts/{}/orders".format(acct_hash)
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
            sym=(leg.get("instrument",{}) or {}).get("symbol","")
            if not sym: continue
            try: got.add(osi_canon(to_osi(sym)))
            except: pass
        if got==canon_set:
            oid=str(o.get("orderId") or "")
            if oid: out.append(oid)
    return out

# ---------- core ----------
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

def main():
    # Schwab client
    app_key=os.environ["SCHWAB_APP_KEY"]; app_secret=os.environ["SCHWAB_APP_SECRET"]; token_json=os.environ["SCHWAB_TOKEN_JSON"]
    with open("schwab_token.json","w") as f: f.write(token_json)
    c=client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")
    r=c.get_account_numbers(); r.raise_for_status()
    acct_hash=r.json()[0]["hashValue"]

    # GW
    api=gw_get_leocross()
    tr=extract_trade(api)
    if not tr:
        goutput("proceed","false"); goutput("reason","NO_TRADE_PAYLOAD"); print("GUARD SKIP: no trade payload")
        return

    sig_date=str(tr.get("Date","")); exp_iso=str(tr.get("TDate","")); exp6=yymmdd(exp_iso)
    inner_put=int(float(tr.get("Limit"))); inner_call=int(float(tr.get("CLimit")))
    def fnum(x):
        try: return float(x)
        except: return None
    cat1=fnum(tr.get("Cat1")); cat2=fnum(tr.get("Cat2"))
    is_credit = True if (cat2 is None or cat1 is None or cat2>=cat1) else False

    # legs
    p_low,p_high = inner_put-5, inner_put
    c_low,c_high = inner_call, inner_call+5
    bp = to_osi(".SPXW{}P{}".format(exp6, p_low));  sp = to_osi(".SPXW{}P{}".format(exp6, p_high))
    sc = to_osi(".SPXW{}C{}".format(exp6, c_low));  bc = to_osi(".SPXW{}C{}".format(exp6, c_high))

    # orient so BUY legs are protective wings, SELL legs are inner strikes for credit; inverted for debit (long IC)
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

    # positions snapshot
    try:
        pos = positions_map(c, acct_hash)
    except Exception as e:
        goutput("proceed","false"); goutput("reason","GUARD_FAIL_POSITIONS"); print("GUARD SKIP: positions error: {}".format(e))
        return

    # --- overlap rules ---
    # 1) NO-CLOSE rule: placing BUY_TO_OPEN on a leg where you're short (<0), or SELL_TO_OPEN where you're long (>0) would "close" — skip.
    checks=[("BUY",legs[0],-1),("SELL",legs[1],+1),("SELL",legs[2],+1),("BUY",legs[3],-1)]
    for _, osi, sign in checks:
        cur = pos.get(osi_canon(osi), 0.0)
        if sign<0 and cur<0:
            goutput("proceed","false"); goutput("reason","WOULD_CLOSE_{}".format(osi)); print("GUARD SKIP: would close {}".format(osi)); return
        if sign>0 and cur>0:
            goutput("proceed","false"); goutput("reason","WOULD_CLOSE_{}".format(osi)); print("GUARD SKIP: would close {}".format(osi)); return

    # 2) Any same-direction overlap on any leg (already holding part of this condor) — treat as overlap, skip.
    overlap=False
    if pos.get(osi_canon(legs[0]),0.0)>0: overlap=True  # already long buy-put
    if pos.get(osi_canon(legs[3]),0.0)>0: overlap=True  # already long buy-call
    if pos.get(osi_canon(legs[1]),0.0)<0: overlap=True  # already short sell-put
    if pos.get(osi_canon(legs[2]),0.0)<0: overlap=True  # already short sell-call
    if overlap:
        goutput("proceed","false"); goutput("reason","POSITION_OVERLAP"); print("GUARD SKIP: position overlap with at least one leg")
        return

    # 3) Any working order with the same 4 legs — skip to avoid duplicates.
    try:
        open_ids = list_matching_open_ids(c, acct_hash, canon)
    except Exception:
        open_ids=[]
    if open_ids:
        goutput("proceed","false"); goutput("reason","WORKING_ORDER_{}".format(",".join(open_ids)))
        print("GUARD SKIP: working matching order(s): {}".format(",".join(open_ids)))
        return

    # If we get here, it's safe to proceed
    goutput("proceed","true"); goutput("reason","OK")
    print("GUARD OK: no overlap, proceeding with placement")

if __name__=="__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:
        goutput("proceed","false")
        goutput("reason","GUARD_UNHANDLED:{}".format(str(e)[:140]))
        print("GUARD SKIP (unhandled): {}".format(e))
        sys.exit(0)
