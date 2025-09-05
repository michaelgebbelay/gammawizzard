# LeoCross ORCHESTRATOR: guard → remaining size → run placer when safe.
# Blocks ONLY if the intended order would "close" any existing leg.
# If partial units already exist, runs placer for REMAINDER (QTY_TARGET - open_units).
#
# Manual daily target:
QTY_TARGET = 4  # <<< change this to your target size

import os, sys, json, time, re, subprocess
from datetime import datetime, date, timezone
from zoneinfo import ZoneInfo
import requests
from schwab.auth import client_from_token_file

ET = ZoneInfo("America/New_York")
GW_BASE = "https://gandalf.gammawizard.com"
GW_ENDPOINT = "/rapi/GetLeoCross"

# ---------- utils ----------
def yymmdd(iso: str) -> str:
    d = date.fromisoformat((iso or "")[:10])
    return "{:%y%m%d}".format(d)

def to_osi(sym: str) -> str:
    """Robust OSI normalizer for Schwab/TOS symbols."""
    raw = (sym or "").upper()
    raw = re.sub(r'\s+', '', raw)      # strip spaces
    raw = raw.lstrip('.')              # remove leading dot
    raw = re.sub(r'[^A-Z0-9.$^]', '', raw)

    m = re.match(r'^([A-Z.$^]{1,6})(\d{6})([CP])(\d{1,5})(?:\.(\d{1,3}))?$', raw) \
        or re.match(r'^([A-Z.$^]{1,6})(\d{6})([CP])(\d{8})$', raw)
    if not m: raise ValueError("Cannot parse option symbol: " + sym)
    root, ymd, cp, strike, frac = (m.groups()+("",))[:5]

    if len(strike) == 8 and not frac:
        mills = int(strike)
    else:
        mills = int(strike)*1000 + (int((frac or "0").ljust(3, '0')) if frac else 0)

    return "{:<6s}{}{}{:08d}".format(root, ymd, cp, mills)

def osi_canon(osi: str):
    return (osi[6:12], osi[12], osi[-8:])  # ignore root differences

def strike_from_osi(osi: str) -> float:
    return int(osi[-8:]) / 1000.0

def iso_z(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _backoff(i): return 0.6*(2**i)

# ---------- Schwab helpers ----------
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

# ---------- GW ----------
def _gw_timeout():
    try: return int(os.environ.get("GW_TIMEOUT","30"))
    except: return 30

def _sanitize_token(t: str) -> str:
    t = (t or "").strip().strip('"').strip("'")
    return t.split(None,1)[1] if t.lower().startswith("bearer ") else t

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
        h={"Accept":"application/json","Authorization":"Bearer {}".format(_sanitize_token(t)),"User-Agent":"gw-orchestrator/1.1"}
        return requests.get("{}/{}".format(GW_BASE.rstrip("/"), GW_ENDPOINT.lstrip("/")), headers=h, timeout=_gw_timeout())
    r=hit(tok) if tok else None
    if (r is None) or (r.status_code in (401,403)): r=hit(gw_login_token())
    if r.status_code!=200: raise RuntimeError("GW_HTTP_{}:{}".format(r.status_code, (r.text or "")[:180]))
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

# ---------- condor helpers ----------
def condor_units_open(pos_map, legs):
    b1 = max(0.0,  pos_map.get(osi_canon(legs[0]), 0.0))  # long wing put
    b2 = max(0.0,  pos_map.get(osi_canon(legs[3]), 0.0))  # long wing call
    s1 = max(0.0, -pos_map.get(osi_canon(legs[1]), 0.0))  # short inner put
    s2 = max(0.0, -pos_map.get(osi_canon(legs[2]), 0.0))  # short inner call
    return int(min(b1, b2, s1, s2))

# ---------- main ----------
def main():
    # ---- Schwab auth (catch refresh failures cleanly) ----
    try:
        app_key=os.environ["SCHWAB_APP_KEY"]; app_secret=os.environ["SCHWAB_APP_SECRET"]; token_json=os.environ["SCHWAB_TOKEN_JSON"]
        with open("schwab_token.json","w") as f: f.write(token_json)
        c=client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")
        r=c.get_account_numbers(); r.raise_for_status()
        acct_hash=r.json()[0]["hashValue"]
    except Exception as e:
        msg=str(e)
        if ("unsupported_token_type" in msg) or ("refresh_token_authentication_error" in msg):
            print("ORCH ABORT: SCHWAB_OAUTH_REFRESH_FAILED — rotate SCHWAB_TOKEN_JSON secret.")
        else:
            print("ORCH ABORT: SCHWAB_CLIENT_INIT_FAILED — {}".format(msg[:200]))
        return 1

    # ---- Leo signal → legs ----
    try:
        api=gw_get_leocross()
        tr=extract_trade(api)
        if not tr:
            print("ORCH SKIP: NO_TRADE_PAYLOAD"); return 0
    except Exception as e:
        print("ORCH ABORT: GW_FETCH_FAILED — {}".format(str(e)[:200])); return 1

    sig_date=str(tr.get("Date","")); exp_iso=str(tr.get("TDate","")); exp6=yymmdd(exp_iso)
    inner_put=int(float(tr.get("Limit"))); inner_call=int(float(tr.get("CLimit")))
    def fnum(x):
        try: return float(x)
        except: return None
    cat1=fnum(tr.get("Cat1")); cat2=fnum(tr.get("Cat2"))
    is_credit = True if (cat2 is None or cat1 is None or cat2>=cat1) else False

    p_low,p_high = inner_put-5, inner_put
    c_low,c_high = inner_call, inner_call+5
    bp = to_osi(".SPXW{}P{}".format(exp6, p_low));  sp = to_osi(".SPXW{}P{}".format(exp6, p_high))
    sc = to_osi(".SPXW{}C{}".format(exp6, c_low));  bc = to_osi(".SPXW{}C{}".format(exp6, c_high))

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

    # ---- positions → NO‑CLOSE guard ----
    try:
        pos = positions_map(c, acct_hash)
    except Exception as e:
        print("ORCH ABORT: POSITIONS_FAILED — {}".format(str(e)[:200])); return 1

    checks=[("BUY",legs[0],-1),("SELL",legs[1],+1),("SELL",legs[2],+1),("BUY",legs[3],-1)]
    for _, osi, sign in checks:
        cur = pos.get(osi_canon(osi), 0.0)
        if (sign<0 and cur<0) or (sign>0 and cur>0):
            print("ORCH SKIP: WOULD_CLOSE {} qty={}".format(osi, cur))
            return 0

    # ---- remaining qty (partial aware) ----
    units_open = condor_units_open(pos, legs)
    rem_qty = max(0, QTY_TARGET - units_open)
    print("ORCH DECISION: target={} open_units={} rem_qty={}".format(QTY_TARGET, units_open, rem_qty))
    if rem_qty == 0:
        print("ORCH SKIP: Already at or above target.")
        return 0

    # ---- call placer with override ----
    env = dict(os.environ)
    env["QTY_OVERRIDE"] = str(rem_qty)
    rc = subprocess.call([sys.executable, "scripts/leocross_place_simple.py"], env=env)
    return rc

if __name__ == "__main__":
    sys.exit(main())
