# LIVE LeoCross → Schwab placer (cancel-then-place ladder, one order only)
# MODE: PLACER_MODE = NOW | SCHEDULED (default SCHEDULED: 16:08–16:14 ET)
# Sizing: fixed manual quantity at top. No buying-power checks. Partial-fill safe. Never closes legs.

import os, sys, json, time, re
from datetime import datetime, date, timezone
from zoneinfo import ZoneInfo
import requests
from schwab.auth import client_from_token_file
from google.oauth2 import service_account
from googleapiclient.discovery import build as gbuild

# ======== MANUAL SETTING ========
QTY = 4   # <<< change this when you want a different contract count

# ======== CONSTANTS ========
TICK = 0.05
WIDTH = 5
ET = ZoneInfo("America/New_York")

STEP_WAIT = 30
CANCEL_GRACE = 12

CREDIT_START = 2.10
CREDIT_FLOOR = 1.90
DEBIT_START  = 1.90
DEBIT_CEIL   = 2.10

GW_BASE = "https://gandalf.gammawizard.com"
GW_ENDPOINT = "/rapi/GetLeoCross"

SHEET_TAB = "schwab"
HEADERS = [
    "ts","source","symbol","last_price",
    "signal_date","order_mode","side","qty_exec","order_type","limit_price",
    "occ_buy_put","occ_sell_put","occ_sell_call","occ_buy_call",
    "order_id","status"
]

# ======== UTILS ========
def clamp_tick(x: float) -> float:
    return round(round(x / TICK) * TICK + 1e-12, 2)

def _sanitize_token(t: str) -> str:
    t = (t or "").strip().strip('"').strip("'")
    return t.split(None,1)[1] if t.lower().startswith("bearer ") else t

def yymmdd(iso: str) -> str:
    d = date.fromisoformat((iso or "")[:10])
    return f"{d:%y%m%d}"

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

# ======== SHEETS ========
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
    got = svc.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range="{}!1:1".format(tab)).execute().get("values",[])
    if not got or got[0] != header:
        svc.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range="{}!1:1".format(tab),
            valueInputOption="USER_ENTERED", body={"values":[header]}).execute()
    return sheet_id_num

def top_insert(svc, spreadsheet_id: str, sheet_id_num: int):
    svc.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id,
        body={"requests":[{"insertDimension":{"range":{"sheetId":sheet_id_num,"dimension":"ROWS","startIndex":1,"endIndex":2},
                                               "inheritFromBefore": False}}]}).execute()

def one_log(svc, sheet_id_num, spreadsheet_id: str, tab: str, row_vals: list):
    top_insert(svc, spreadsheet_id, sheet_id_num)
    svc.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range="{}!A2".format(tab),
        valueInputOption="USER_ENTERED", body={"values":[row_vals]}).execute()

# ======== SCHWAB HTTP HELPERS (loop retries) ========
def _backoff(i): return 0.6*(2**i)

def schwab_get_json(c, url, params=None, tries=8, tag=""):
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

def schwab_post_json(c, url, payload, tries=8, tag=""):
    last=""
    for i in range(tries):
        try:
            r=c.session.post(url, json=payload, timeout=20)
            if r.status_code in (200,201,202): return r
            last="HTTP_{:d}:{:s}".format(r.status_code, (r.text or "")[:160])
        except Exception as e:
            last="{}:{}".format(type(e).__name__, str(e))
        time.sleep(_backoff(i))
    raise RuntimeError("SCHWAB_POST_FAIL({}) {}".format(tag, last))

def schwab_delete(c, url, tries=6, tag=""):
    last=""
    for i in range(tries):
        try:
            r=c.session.delete(url, timeout=20)
            if r.status_code in (200,201,202,204): return r
            last="HTTP_{:d}:{:s}".format(r.status_code, (r.text or "")[:160])
        except Exception as e:
            last="{}:{}".format(type(e).__name__, str(e))
        time.sleep(_backoff(i))
    raise RuntimeError("SCHWAB_DELETE_FAIL({}) {}".format(tag, last))

# ======== GW + QUOTES ========
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
        h={"Accept":"application/json","Authorization":"Bearer {}".format(_sanitize_token(t)),"User-Agent":"gw-placer/1.0"}
        return requests.get("{}/{}".format(GW_BASE.rstrip("/"), GW_ENDPOINT.lstrip("/")), headers=h, timeout=_gw_timeout())
    r=hit(tok) if tok else None
    if (r is None) or (r.status_code in (401,403)): r=hit(gw_login_token())
    if r.status_code!=200: raise RuntimeError("GW_HTTP_{}:{}".format(r.status_code, (r.text or "")[:180]))
    return r.json()

def fetch_bid_ask(c, osi: str):
    r=c.get_quote(osi)
    if r.status_code!=200: return (None,None)
    d=list(r.json().values())[0] if isinstance(r.json(), dict) else {}
    q=d.get("quote", d)
    b=q.get("bidPrice") or q.get("bid") or q.get("bidPriceInDouble")
    a=q.get("askPrice") or q.get("ask") or q.get("askPriceInDouble")
    return (float(b) if b is not None else None, float(a) if a is not None else None)

def mid_condor(c, legs):
    bp,sp,sc,bc=legs
    bp_b,bp_a=fetch_bid_ask(c,bp); sp_b,sp_a=fetch_bid_ask(c,sp)
    sc_b,sc_a=fetch_bid_ask(c,sc); bc_b,bc_a=fetch_bid_ask(c,bc)
    if None in (bp_b,bp_a,sp_b,sp_a,sc_b,sc_a,bc_b,bc_a): return None
    net_bid=(sp_b+sc_b)-(bp_a+bc_a); net_ask=(sp_a+sc_a)-(bp_b+bc_b)
    return (net_bid+net_ask)/2.0

# ======== POSITIONS / ORDERS / GUARDS ========
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
    """Schwab /orders requires fromEnteredTime/toEnteredTime. Query today 00:00 ET → now ET (UTC Z)."""
    now_et = datetime.now(ET)
    start_et = now_et.replace(hour=0, minute=0, second=0, microsecond=0)
    url="https://api.schwabapi.com/trader/v1/accounts/{}/orders".format(acct_hash)
    params = {"fromEnteredTime": iso_z(start_et), "toEnteredTime": iso_z(now_et), "maxResults": 200}
    try:
        arr = schwab_get_json(c, url, params=params, tag="ORDERS") or []
    except Exception as e:
        print("WARN orders lookup failed: {} (continuing without cancel)".format(e))
        return []  # degrade gracefully — risk: parallel working orders
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

def cancel_all_and_wait(c, acct_hash: str, canon_set, grace=CANCEL_GRACE):
    t_end=time.time()+grace
    while True:
        try:
            ids=list_matching_open_ids(c,acct_hash,canon_set)
        except Exception as e:
            print("WARN cancel phase lookup: {} (skipping cancel)".format(e))
            return
        if not ids: return
        for oid in ids:
            url="https://api.schwabapi.com/trader/v1/accounts/{}/orders/{}".format(acct_hash, oid)
            try: schwab_delete(c,url,tag="CANCEL:{}".format(oid))
            except Exception as e: print("WARN cancel {}: {}".format(oid, e))
        if time.time()>=t_end:
            try:
                ids2=list_matching_open_ids(c,acct_hash,canon_set)
                if ids2: print("WARN lingering orders: {}".format(",".join(ids2)))
            except Exception: pass
            return
        time.sleep(0.5)

def condor_units_open(pos_map, legs):
    """How many full condors are already open with these exact legs."""
    b1 = max(0.0,  pos_map.get(osi_canon(legs[0]), 0.0))  # BUY legs long qty
    b2 = max(0.0,  pos_map.get(osi_canon(legs[3]), 0.0))
    s1 = max(0.0, -pos_map.get(osi_canon(legs[1]), 0.0))  # SELL legs short qty
    s2 = max(0.0, -pos_map.get(osi_canon(legs[2]), 0.0))
    return int(min(b1, b2, s1, s2))

# ======== MAIN ========
def main():
    MODE=(os.environ.get("PLACER_MODE","SCHEDULED") or "SCHEDULED").upper()
    source="SIMPLE_" + MODE

    # secrets/env
    sheet_id=os.environ["GSHEET_ID"]; sa_json=os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
    app_key=os.environ["SCHWAB_APP_KEY"]; app_secret=os.environ["SCHWAB_APP_SECRET"]; token_json=os.environ["SCHWAB_TOKEN_JSON"]

    with open("schwab_token.json","w") as f: f.write(token_json)
    c=client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")

    r=c.get_account_numbers(); r.raise_for_status()
    acct_hash=r.json()[0]["hashValue"]

    # last SPX (for log)
    def spx_last():
        for sym in ["$SPX.X","SPX","SPX.X","$SPX"]:
            try:
                q=c.get_quote(sym)
                if q.status_code==200 and sym in q.json():
                    last=q.json()[sym].get("quote",{}).get("lastPrice")
                    if last is not None: return last
            except: pass
        return ""
    last_px=spx_last()

    # Sheets client
    creds=service_account.Credentials.from_service_account_info(json.loads(sa_json),
        scopes=["https://www.googleapis.com/auth/spreadsheets"])
    svc=gbuild("sheets","v4",credentials=creds)
    sheet_id_num=ensure_header_and_get_sheetid(svc, sheet_id, SHEET_TAB, HEADERS)

    # schedule gate
    if MODE=="SCHEDULED":
        now=datetime.now(ET)
        if now.weekday()>=5 or not (now.hour==16 and 8<=now.minute<=14):
            row=[datetime.utcnow().isoformat()+"Z", source, "SPX", last_px, "", "SKIP","","","","",
                 "","","","", "", "SKIPPED_TIME_WINDOW"]
            one_log(svc, sheet_id_num, sheet_id, SHEET_TAB, row); print("skip window"); sys.exit(0)

    # ---- GW → trade payload ----
    try:
        api=gw_get_leocross()
    except Exception as e:
        row=[datetime.utcnow().isoformat()+"Z", source, "SPX", last_px, "", "ABORT","","","","",
             "","","","", "", "ABORT_GW:{}".format(str(e)[:150])]
        one_log(svc, sheet_id_num, sheet_id, SHEET_TAB, row); print(e); sys.exit(0)

    # extract minimal fields from GW
    def extract(j):
        if isinstance(j,dict):
            if "Trade" in j:
                tr=j["Trade"]
                return tr[-1] if isinstance(tr,list) and tr else tr if isinstance(tr,dict) else {}
            keys=("Date","TDate","Limit","CLimit","Cat1","Cat2")
            if any(k in j for k in keys): return j
            for v in j.values():
                if isinstance(v,(dict,list)):
                    t=extract(v)
                    if t: return t
        if isinstance(j,list):
            for it in reversed(j):
                t=extract(it)
                if t: return t
        return {}
    tr=extract(api)
    if not tr:
        row=[datetime.utcnow().isoformat()+"Z", source, "SPX", last_px, "", "ABORT","","","","",
             "","","","", "", "NO_TRADE_PAYLOAD"]
        one_log(svc, sheet_id_num, sheet_id, SHEET_TAB, row); sys.exit(0)

    sig_date=str(tr.get("Date","")); exp_iso=str(tr.get("TDate","")); exp6=yymmdd(exp_iso)
    inner_put=int(float(tr.get("Limit"))); inner_call=int(float(tr.get("CLimit")))
    def fnum(x):
        try: return float(x)
        except: return None
    cat1=fnum(tr.get("Cat1")); cat2=fnum(tr.get("Cat2"))
    is_credit = True if (cat2 is None or cat1 is None or cat2>=cat1) else False

    # legs (width 5), orient so legs[0],legs[3] are BUY_TO_OPEN; legs[1],legs[2] are SELL_TO_OPEN
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
    canon = {osi_canon(x) for x in legs}

    # NO-CLOSE guard (never flip an existing leg)
    try:
        pos = positions_map(c, acct_hash)
    except Exception as e:
        row=[datetime.utcnow().isoformat()+"Z", source, "SPX", last_px, sig_date, "ABORT",
             ("SHORT_IRON_CONDOR" if is_credit else "LONG_IRON_CONDOR"), "", ("NET_CREDIT" if is_credit else "NET_DEBIT"), "",
             legs[0],legs[1],legs[2],legs[3], "", "ABORT_NET_POS:{}".format(str(e)[:140])]
        one_log(svc, sheet_id_num, sheet_id, SHEET_TAB, row); print(e); sys.exit(0)

    def would_close():
        checks=[("BUY",legs[0],-1),("SELL",legs[1],+1),("SELL",legs[2],+1),("BUY",legs[3],-1)]
        for _, osi, sign in checks:
            cur = pos.get(osi_canon(osi), 0.0)
            if sign<0 and cur<0: return (False, "LEG_WOULD_CLOSE {}".format(osi))
            if sign>0 and cur>0: return (False, "LEG_WOULD_CLOSE {}".format(osi))
        return (True,"")
    ok,reason = would_close()
    if not ok:
        row=[datetime.utcnow().isoformat()+"Z", source, "SPX", last_px, sig_date, "ABORT",
             ("SHORT_IRON_CONDOR" if is_credit else "LONG_IRON_CONDOR"), "", ("NET_CREDIT" if is_credit else "NET_DEBIT"), "",
             legs[0],legs[1],legs[2],legs[3], "", "ABORT_{}".format(reason)]
        one_log(svc, sheet_id_num, sheet_id, SHEET_TAB, row); print(reason); sys.exit(0)

    # compute remaining size (handles partial fills gracefully)
    already_units = condor_units_open(pos, legs)
    rem_qty = max(0, QTY - already_units)
    if rem_qty == 0:
        row=[datetime.utcnow().isoformat()+"Z", source, "SPX", last_px, sig_date, "SKIP",
             ("SHORT_IRON_CONDOR" if is_credit else "LONG_IRON_CONDOR"), already_units,
             ("NET_CREDIT" if is_credit else "NET_DEBIT"), "",
             legs[0],legs[1],legs[2],legs[3], "",
             "ALREADY_AT_TARGET QTY_TARGET={} OPEN_UNITS={}".format(QTY, already_units)]
        one_log(svc, sheet_id_num, sheet_id, SHEET_TAB, row)
        print("Already at target qty; nothing to place."); sys.exit(0)

    side_name  = "SHORT_IRON_CONDOR" if is_credit else "LONG_IRON_CONDOR"
    order_type = "NET_CREDIT" if is_credit else "NET_DEBIT"

    # --- build payload ---
    def order_payload(price: float):
        return {
            "orderType": order_type,
            "session": "NORMAL",
            "price": "{:.2f}".format(price),
            "duration": "DAY",
            "orderStrategyType": "SINGLE",
            "complexOrderStrategyType": "IRON_CONDOR",
            "orderLegCollection":[
                {"instruction":"BUY_TO_OPEN", "positionEffect":"OPENING","quantity":rem_qty,"instrument":{"symbol":legs[0],"assetType":"OPTION"}},
                {"instruction":"SELL_TO_OPEN","positionEffect":"OPENING","quantity":rem_qty,"instrument":{"symbol":legs[1],"assetType":"OPTION"}},
                {"instruction":"SELL_TO_OPEN","positionEffect":"OPENING","quantity":rem_qty,"instrument":{"symbol":legs[2],"assetType":"OPTION"}},
                {"instruction":"BUY_TO_OPEN", "positionEffect":"OPENING","quantity":rem_qty,"instrument":{"symbol":legs[3],"assetType":"OPTION"}},
            ]
        }

    def place(price: float) -> str:
        cancel_all_and_wait(c, acct_hash, canon)  # cancel/replace ladder
        url="https://api.schwabapi.com/trader/v1/accounts/{}/orders".format(acct_hash)
        r=schwab_post_json(c,url,order_payload(price),tag="PLACE@{:.2f}".format(price))
        try:
            j=r.json(); return str(j.get("orderId") or j.get("order_id") or "")
        except:
            loc=r.headers.get("Location",""); return loc.rstrip("/").split("/")[-1] if loc else ""

    def order_status(oid: str) -> str:
        url="https://api.schwabapi.com/trader/v1/accounts/{}/orders/{}".format(acct_hash, oid)
        try:
            j=schwab_get_json(c,url,tag="STATUS:{}".format(oid))
            return str(j.get("status") or j.get("orderStatus") or "")
        except Exception:
            return ""

    def wait_or_filled(secs: int, oid: str):
        end=time.time()+secs; last=""
        while time.time()<end:
            last=(order_status(oid) or "").upper()
            if last=="FILLED": return True,last
            time.sleep(1)
        return False,last

    # --- ladder sequence ---
    steps=[]; used_price=None; oid=""; filled=False; st=""

    # Step 0: start @ 2.10 credit (or 1.90 debit)
    used_price = clamp_tick(CREDIT_START if is_credit else DEBIT_START)
    oid = place(used_price); steps.append("{:.2f}".format(used_price))
    filled, st = wait_or_filled(STEP_WAIT, oid)

    if not filled:
        # Step 1: mid
        cancel_all_and_wait(c, acct_hash, canon)
        m1 = mid_condor(c, legs)
        used_price = clamp_tick(m1 if m1 is not None else used_price)
        oid = place(used_price); steps.append("{:.2f}".format(used_price))
        filled, st = wait_or_filled(STEP_WAIT, oid)

    if not filled:
        # Step 2: new mid
        cancel_all_and_wait(c, acct_hash, canon)
        m2 = mid_condor(c, legs)
        used_price = clamp_tick(m2 if m2 is not None else used_price)
        oid = place(used_price); steps.append("{:.2f}".format(used_price))
        filled, st = wait_or_filled(STEP_WAIT, oid)

    if not filled:
        # Step 3: prev mid ±0.05, bounded to floor/ceil
        cancel_all_and_wait(c, acct_hash, canon)
        pm = m2 if ('m2' in locals() and m2 is not None) else (m1 if ('m1' in locals()) else None)
        step_px = clamp_tick((pm + (-0.05 if is_credit else +0.05)) if pm is not None else used_price)
        if is_credit: step_px = max(CREDIT_FLOOR, min(CREDIT_START, step_px))
        else:         step_px = max(DEBIT_START,  min(DEBIT_CEIL,   step_px))
        used_price = step_px
        oid = place(used_price); steps.append("{:.2f}".format(used_price))
        filled, st = wait_or_filled(STEP_WAIT, oid)

    if not filled:
        # Step 4: bound, then cancel
        cancel_all_and_wait(c, acct_hash, canon)
        used_price = clamp_tick(CREDIT_FLOOR if is_credit else DEBIT_CEIL)
        oid = place(used_price); steps.append("{:.2f}".format(used_price))
        filled, st = wait_or_filled(STEP_WAIT, oid)
        cancel_all_and_wait(c, acct_hash, canon)
        steps.append("CXL")

    # ---- final log ----
    trace = "STEPS " + "→".join(steps)
    status_txt = (("FILLED " + trace) if filled else ((st or "WORKING") + " " + trace)) \
                 + " | TARGET={} OPEN_UNITS={} REM={}".format(QTY, already_units, rem_qty)
    row = [datetime.utcnow().isoformat()+"Z", source, "SPX", last_px,
           sig_date, "PLACE", side_name,
           rem_qty, ("NET_CREDIT" if is_credit else "NET_DEBIT"),
           ("" if used_price is None else "{:.2f}".format(used_price)),
           legs[0],legs[1],legs[2],legs[3],
           oid, status_txt]
    one_log(svc, sheet_id_num, sheet_id, SHEET_TAB, row)
    print("FINAL {} OID={} PRICE_USED={}".format(status_txt, oid, ("{:.2f}".format(used_price) if used_price is not None else "NA")))

# ---- entry (schedule gate) ----
def time_gate_ok():
    now=datetime.now(ZoneInfo("America/New_York"))
    return (now.weekday()<5 and now.hour==16 and 8<=now.minute<=14)

if __name__=="__main__":
    MODE=(os.environ.get("PLACER_MODE","SCHEDULED") or "SCHEDULED").upper()
    if MODE=="SCHEDULED" and not time_gate_ok():
        print("Scheduled window not met; exit."); sys.exit(0)
    main()
