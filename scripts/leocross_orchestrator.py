# LeoCross ORCHESTRATOR (STRICT OVERLAP GUARD + Google Sheet logging)
# - Fetch LeoCross trade
# - Build intended 4 legs (SPXW)
# - STRICT GUARD:
#     * Block if ANY leg is present but not ALL 4 in the correct direction (partial overlap).
#     * Block if any leg would be "closed" (opposite sign).
#     * Allow topping up only if ALL 4 legs are already present in the same direction; size = target - open_units.
# - Log every decision to Sheets tab "guard".
# - If allowed, run placer with QTY_OVERRIDE.

QTY_TARGET = 4  # daily target units (condors)

import os, sys, json, time, re
from datetime import datetime, date, timezone
from zoneinfo import ZoneInfo
import requests
from schwab.auth import client_from_token_file

# Sheets
from google.oauth2 import service_account
from googleapiclient.discovery import build as gbuild

ET = ZoneInfo("America/New_York")
GW_BASE = "https://gandalf.gammawizard.com"
GW_ENDPOINT = "/rapi/GetLeoCross"

# Set GUARD_ONLY=1 to just print/log the snapshot/decision and exit (no placing)
GUARD_ONLY = (os.environ.get("GUARD_ONLY","0").strip().lower() in ("1","true","yes","y"))

# ===== Sheets helpers =====
GUARD_TAB = "guard"
GUARD_HEADERS = [
    "ts","source","symbol","signal_date","decision","detail","open_units","rem_qty",
    "occ_buy_put","occ_sell_put","occ_sell_call","occ_buy_call",
    "acct_qty_bp","acct_qty_sp","acct_qty_sc","acct_qty_bc"
]

def ensure_header_and_get_sheetid(svc, spreadsheet_id: str, tab: str, header: list):
    meta = svc.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheet_id_num = None
    for sh in meta["sheets"]:
        if sh["properties"]["title"] == tab:
            sheet_id_num = sh["properties"]["sheetId"]; break
    if sheet_id_num is None:
        svc.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id,
            body={"requests":[{"addSheet":{"properties":{"title":tab}}}]}).execute()
        meta = svc.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheet_id_num = next(sh["properties"]["sheetId"] for sh in meta["sheets"] if sh["properties"]["title"]==tab)
    got = svc.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=f"{tab}!1:1").execute().get("values",[])
    if not got or got[0] != header:
        svc.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range=f"{tab}!1:1",
            valueInputOption="USER_ENTERED", body={"values":[header]}).execute()
    return sheet_id_num

def top_insert(svc, spreadsheet_id: str, sheet_id_num: int):
    svc.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id,
        body={"requests":[{"insertDimension":{"range":{"sheetId":sheet_id_num,"dimension":"ROWS","startIndex":1,"endIndex":2},
                                               "inheritFromBefore": False}}]}).execute()

def guard_log(svc, sheet_id_num, spreadsheet_id: str, row_vals: list):
    top_insert(svc, spreadsheet_id, sheet_id_num)
    svc.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range=f"{GUARD_TAB}!A2",
        valueInputOption="USER_ENTERED", body={"values":[row_vals]}).execute()

# ===== utils =====
def yymmdd(iso: str) -> str:
    d = date.fromisoformat((iso or "")[:10])
    return "{:%y%m%d}".format(d)

def to_osi(sym: str) -> str:
    raw = (sym or "").upper()
    raw = re.sub(r'\s+', '', raw)
    raw = raw.lstrip('.')
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

def _backoff(i): return 0.6*(2**i)

# ===== Schwab helpers =====
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

def _osi_from_instrument(ins: dict) -> str | None:
    """Robust OSI from Schwab instrument; fall back to structured fields when needed."""
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
    """Return {canon: net_qty} for all options. Positive=long, negative=short."""
    url="https://api.schwabapi.com/trader/v1/accounts/{}".format(acct_hash)
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

# ===== GW =====
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
        h={"Accept":"application/json","Authorization":"Bearer {}".format(_sanitize_token(t)),"User-Agent":"gw-orchestrator/1.4"}
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

# ===== condor math =====
def condor_units_open(pos_map, legs):
    # legs: [bp, sp, sc, bc] where bp/bc are long wings; sp/sc are short inners
    b1 = max(0.0,  pos_map.get(osi_canon(legs[0]), 0.0))  # long put wing
    b2 = max(0.0,  pos_map.get(osi_canon(legs[3]), 0.0))  # long call wing
    s1 = max(0.0, -pos_map.get(osi_canon(legs[1]), 0.0))  # short put inner
    s2 = max(0.0, -pos_map.get(osi_canon(legs[2]), 0.0))  # short call inner
    return int(min(b1, b2, s1, s2))

def print_guard_snapshot(pos, legs, is_credit):
    labels = [("BUY_PUT",legs[0],-1),("SELL_PUT",legs[1],+1),("SELL_CALL",legs[2],+1),("BUY_CALL",legs[3],-1)]
    print("ORCH GUARD SNAPSHOT ({}):".format("CREDIT" if is_credit else "DEBIT"))
    for name, osi, sign in labels:
        can = osi_canon(osi); cur = pos.get(can, 0.0)
        print("  {:10s} {}  acct_qty={:+g}  sign={:+d}".format(name, osi, cur, sign))

# ===== main =====
def main():
    # Sheets client
    try:
        sheet_id=os.environ["GSHEET_ID"]
        sa_json=os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
        creds=service_account.Credentials.from_service_account_info(json.loads(sa_json),
            scopes=["https://www.googleapis.com/auth/spreadsheets"])
        svc=gbuild("sheets","v4",credentials=creds)
        guard_sheet_id = ensure_header_and_get_sheetid(svc, sheet_id, GUARD_TAB, GUARD_HEADERS)
    except Exception as e:
        print("ORCH WARN: Sheets init failed — {}".format(str(e)[:200]))
        svc = None; sheet_id = None; guard_sheet_id = None

    def log(decision, detail, legs=None, acct_qty=(0,0,0,0), open_units="", rem_qty=""):
        if not svc: return
        bp,sp,sc,bc = (legs or ("","","",""))
        bpq,spq,scq,bcq = acct_qty
        row=[datetime.utcnow().isoformat()+"Z","ORCH","SPX",sig_date if 'sig_date' in locals() else "",
             decision, detail, open_units, rem_qty,
             bp,sp,sc,bc, bpq,spq,scq,bcq]
        try:
            guard_log(svc, guard_sheet_id, sheet_id, row)
        except Exception as e:
            print("ORCH WARN: guard log failed — {}".format(str(e)[:200]))

    # ---- Schwab auth ----
    try:
        app_key=os.environ["SCHWAB_APP_KEY"]; app_secret=os.environ["SCHWAB_APP_SECRET"]; token_json=os.environ["SCHWAB_TOKEN_JSON"]
        with open("schwab_token.json","w") as f: f.write(token_json)
        c=client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")
        r=c.get_account_numbers(); r.raise_for_status()
        acct_hash=r.json()[0]["hashValue"]
    except Exception as e:
        msg=str(e)
        reason="SCHWAB_OAUTH_REFRESH_FAILED — rotate SCHWAB_TOKEN_JSON secret." if ("unsupported_token_type" in msg or "refresh_token_authentication_error" in msg) else ("SCHWAB_CLIENT_INIT_FAILED — " + msg[:200])
        print("ORCH ABORT:", reason)
        log("ABORT", reason)
        return 1

    # ---- Leo signal → legs ----
    try:
        api=gw_get_leocross()
        tr=extract_trade(api)
        if not tr:
            print("ORCH SKIP: NO_TRADE_PAYLOAD")
            log("SKIP","NO_TRADE_PAYLOAD")
            return 0
    except Exception as e:
        reason="GW_FETCH_FAILED — {}".format(str(e)[:200])
        print("ORCH ABORT:", reason); log("ABORT", reason)
        return 1

    sig_date=str(tr.get("Date","")); exp_iso=str(tr.get("TDate","")); exp6=yymmdd(exp_iso)
    inner_put=int(float(tr.get("Limit"))); inner_call=int(float(tr.get("CLimit")))
    def fnum(x):
        try: return float(x)
        except: return None
    cat1=fnum(tr.get("Cat1")); cat2=fnum(tr.get("Cat2"))
    is_credit = True if (cat2 is None or cat1 is None or cat2>=cat1) else False

    # Build planned legs (width 5)
    p_low,p_high = inner_put-5, inner_put
    c_low,c_high = inner_call, inner_call+5
    bp = to_osi(".SPXW{}P{}".format(exp6, p_low))   # BUY_TO_OPEN
    sp = to_osi(".SPXW{}P{}".format(exp6, p_high))  # SELL_TO_OPEN
    sc = to_osi(".SPXW{}C{}".format(exp6, c_low))   # SELL_TO_OPEN
    bc = to_osi(".SPXW{}C{}".format(exp6, c_high))  # BUY_TO_OPEN

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

    # ---- positions ----
    try:
        pos = positions_map(c, acct_hash)
    except Exception as e:
        reason="POSITIONS_FAILED — {}".format(str(e)[:200])
        print("ORCH ABORT:", reason); log("ABORT", reason, legs)
        return 1

    print_guard_snapshot(pos, legs, is_credit)

    # snapshot quantities in account (for logging)
    bpq = pos.get(osi_canon(legs[0]), 0.0)
    spq = pos.get(osi_canon(legs[1]), 0.0)
    scq = pos.get(osi_canon(legs[2]), 0.0)
    bcq = pos.get(osi_canon(legs[3]), 0.0)
    acct_snapshot=(bpq,spq,scq,bcq)

    # ---- STRICT OVERLAP GUARD ----
    checks=[("BUY",legs[0],-1),("SELL",legs[1],+1),("SELL",legs[2],+1),("BUY",legs[3],-1)]

    any_opposite=False
    nonzero_count=0
    aligned_count=0
    present_legs=[]
    opposite_legs=[]

    for label, osi, sign in checks:
        cur = pos.get(osi_canon(osi), 0.0)
        if abs(cur) > 1e-9:
            nonzero_count += 1
            present_legs.append((label, osi, cur))
        if (sign < 0 and cur < 0) or (sign > 0 and cur > 0):
            any_opposite = True
            opposite_legs.append((label, osi, cur))
        if (sign < 0 and cur >= 0) or (sign > 0 and cur <= 0):
            if abs(cur) > 1e-9:
                aligned_count += 1

    if any_opposite:
        details="; ".join(["{} {} acct_qty={:+g}".format(l, o, q) for (l,o,q) in opposite_legs])
        print("ORCH SKIP: WOULD_CLOSE — {}".format(details))
        log("SKIP","WOULD_CLOSE — {}".format(details), legs, acct_snapshot, "", "")
        return 0

    if nonzero_count == 0:
        rem_qty = QTY_TARGET
        print("ORCH DECISION: target={} open_units=0 rem_qty={}".format(QTY_TARGET, rem_qty))
        log("ALLOW","CLEAN_ACCOUNT", legs, acct_snapshot, 0, rem_qty)
    elif nonzero_count == 4 and aligned_count == 4:
        units_open = condor_units_open(pos, legs)
        rem_qty = max(0, QTY_TARGET - units_open)
        print("ORCH DECISION: target={} open_units={} rem_qty={}".format(QTY_TARGET, units_open, rem_qty))
        if rem_qty == 0:
            print("ORCH SKIP: Already at/above target for these strikes.")
            log("SKIP","AT_OR_ABOVE_TARGET", legs, acct_snapshot, units_open, 0)
            return 0
        log("ALLOW","TOP_UP", legs, acct_snapshot, units_open, rem_qty)
    else:
        details="; ".join(["{} {} acct_qty={:+g}".format(l, o, q) for (l,o,q) in present_legs])
        print("ORCH SKIP: PARTIAL_OVERLAP — {}".format(details))
        log("SKIP","PARTIAL_OVERLAP — {}".format(details), legs, acct_snapshot, "", "")
        return 0

    if GUARD_ONLY:
        print("ORCH GUARD_ONLY=1 — allowed, NOT invoking placer.")
        return 0

    # ---- run placer with override ----
    env = dict(os.environ)
    env["QTY_OVERRIDE"] = str(rem_qty)
    rc = os.spawnve(os.P_WAIT, sys.executable, [sys.executable, "scripts/leocross_place_simple.py"], env)
    return rc

if __name__ == "__main__":
    sys.exit(main())
