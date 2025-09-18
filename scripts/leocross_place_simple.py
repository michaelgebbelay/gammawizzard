#!/usr/bin/env python3
# VERSION: 2025-10-05 v2.6.0 — Short IC sizing = 15-wide & 12k-per-contract ladder:
#   start = max($4.80, $2 * width/5), step = -$0.05 every 10s, floor $4.80.
#   Long IC (debit) ladder unchanged.

__version__ = "2.6.0"

# LeoCross PLACER — laddered IRON_CONDOR with true Cancel/Replace (PUT) semantics.
# - Short IC (credit): fixed-width (default 15) & qty = ceil(open_cash/12k) unless QTY_OVERRIDE.
#   Ladder start/step/floor as above.
# - Long IC (debit): legacy 5‑wide; qty from QTY_OVERRIDE or QTY_FIXED.
# - Logs ladder/price/filled/replaced/canceled to Google Sheet tab "schwab".
# - Maintains ONE active working order: replace its price/size each rung.

# ======= MANUAL SIZE (edit this, or override via QTY_OVERRIDE env) =======
QTY_FIXED = 4  # used only for Long IC unless QTY_OVERRIDE provided

import os, sys, json, time, re, math, random
from datetime import datetime, date, timezone
from zoneinfo import ZoneInfo
import requests
from schwab.auth import client_from_token_file
from google.oauth2 import service_account
from googleapiclient.discovery import build as gbuild

# ===== constants =====
TICK = 0.05
ET = ZoneInfo("America/New_York")

# Waits: Short IC faster per your spec
STEP_WAIT_CREDIT = int(os.environ.get("STEP_WAIT_CREDIT", "10"))  # seconds between rungs (credit)
STEP_WAIT_DEBIT  = int(os.environ.get("STEP_WAIT_DEBIT",  "30"))  # unchanged (debit)
FINAL_CANCEL = True  # if still not filled after last rung, cancel working ticket
WINDOW_STATUSES = {"WORKING","QUEUED","OPEN","PENDING_ACTIVATION"}

# ===== Debit ladders (unchanged) =====
DEBIT_START  = float(os.environ.get("DEBIT_START", "1.90"))
DEBIT_CEIL   = float(os.environ.get("DEBIT_CEIL",  "2.10"))
DEBIT_STEP   = float(os.environ.get("DEBIT_STEP",  "0.05"))

# ===== Short IC sizing knobs =====
CREDIT_DOLLARS_PER_CONTRACT = float(os.environ.get("CREDIT_DOLLARS_PER_CONTRACT", "12000"))
CREDIT_SPREAD_WIDTH         = int(os.environ.get("CREDIT_SPREAD_WIDTH", "15"))
CREDIT_MIN_WIDTH            = 5

# ===== Short IC ladder knobs =====
CREDIT_PER5_START = float(os.environ.get("CREDIT_PER5_START", "2.00"))  # $ per 5pt width
CREDIT_STEP       = float(os.environ.get("CREDIT_STEP",       "0.05"))  # down 5¢ each rung
CREDIT_FLOOR      = float(os.environ.get("CREDIT_FLOOR",      "4.80"))  # never go below this

GW_BASE = "https://gandalf.gammawizard.com"
GW_ENDPOINT = "/rapi/GetLeoCross"

SHEET_TAB = "schwab"
HEADERS = [
    "ts","source","symbol","last_price",
    "signal_date","order_mode","side","qty_exec","order_type","limit_price",
    "occ_buy_put","occ_sell_put","occ_sell_call","occ_buy_call",
    "order_id","status"
]

# ===== tiny utils =====
def clamp_tick(x: float) -> float:
    return round(round(x / 0.05) * 0.05 + 1e-12, 2)

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
    if not m: raise ValueError(f"Cannot parse option symbol: {sym}")
    root, ymd, cp, strike, frac = (m.groups()+("",))[:5]
    mills = int(strike)*1000 + (int((frac or "0").ljust(3,'0')) if frac else 0) if len(strike)<8 else int(strike)
    return f"{root:<6}{ymd}{cp}{mills:08d}"

def osi_canon(osi: str):
    return (osi[6:12], osi[12], osi[-8:])

def strike_from_osi(osi: str) -> float:
    return int(osi[-8:]) / 1000.0

def iso_z(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# ===== Sheets =====
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

def one_log(svc, sheet_id_num, spreadsheet_id: str, tab: str, row_vals: list):
    top_insert(svc, spreadsheet_id, sheet_id_num)
    svc.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range=f"{tab}!A2",
        valueInputOption="USER_ENTERED", body={"values":[row_vals]}).execute()

# ===== Schwab hardened HTTP =====
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


def schwab_post_json(c, url, payload, tries=6, tag=""):
    last = ""
    for i in range(tries):
        try:
            r = c.session.post(url, json=payload, timeout=20)
            if r.status_code in (200, 201, 202):
                return r
            if r.status_code == 429:
                time.sleep(_sleep_for_429(r, i))
                continue
            last = f"HTTP_{r.status_code}:{(r.text or '')[:160]}"
        except Exception as e:
            last = f"{type(e).__name__}:{str(e)}"
        time.sleep(0.5 * (2 ** i))
    raise RuntimeError(f"SCHWAB_POST_FAIL({tag}) {last}")


def schwab_put_json(c, url, payload, tries=6, tag=""):
    last = ""
    for i in range(tries):
        try:
            r = c.session.put(url, json=payload, timeout=20)
            if r.status_code in (200, 201, 202):
                return r
            if r.status_code == 429:
                time.sleep(_sleep_for_429(r, i))
                continue
            last = f"HTTP_{r.status_code}:{(r.text or '')[:160]}"
        except Exception as e:
            last = f"{type(e).__name__}:{str(e)}"
        time.sleep(0.5 * (2 ** i))
    raise RuntimeError(f"SCHWAB_PUT_FAIL({tag}) {last}")


def schwab_delete(c, url, tries=6, tag=""):
    last = ""
    for i in range(tries):
        try:
            r = c.session.delete(url, timeout=20)
            if r.status_code in (200, 201, 202, 204):
                return r
            if r.status_code == 429:
                time.sleep(_sleep_for_429(r, i))
                continue
            last = f"HTTP_{r.status_code}:{(r.text or '')[:160]}"
        except Exception as e:
            last = f"{type(e).__name__}:{str(e)}"
        time.sleep(0.5 * (2 ** i))
    raise RuntimeError(f"SCHWAB_DELETE_FAIL({tag}) {last}")

# ===== Account helpers =====
def get_primary_acct(c):
    r=c.get_account_numbers(); r.raise_for_status()
    first=r.json()[0]
    return str(first.get("accountNumber")), str(first.get("hashValue"))

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

def calc_short_ic_width(opening_cash: float | int) -> int:
    """Backwards-compatible helper; width no longer depends on cash."""
    return _credit_width()

# ===== GW + quotes (mid not used now, but kept for completeness) =====
def _gw_timeout():
    try: return int(os.environ.get("GW_TIMEOUT","30"))
    except: return 30

def gw_login_token():
    email=os.environ.get("GW_EMAIL",""); pwd=os.environ.get("GW_PASSWORD","")
    if not (email and pwd): raise RuntimeError("GW_LOGIN_MISSING_CREDS")
    r=requests.post(f"{GW_BASE}/goauth/authenticateFireUser", data={"email":email,"password":pwd}, timeout=_gw_timeout())
    if r.status_code!=200: raise RuntimeError(f"GW_LOGIN_HTTP_{r.status_code}:{(r.text or '')[:180]}")
    j=r.json(); t=j.get("token"); 
    if not t: raise RuntimeError("GW_LOGIN_NO_TOKEN")
    return t

def gw_get_leocross():
    tok=_sanitize_token(os.environ.get("GW_TOKEN","") or "")
    def hit(t):
        h={"Accept":"application/json","Authorization":f"Bearer {_sanitize_token(t)}","User-Agent":"gw-placer/1.3"}
        return requests.get(f"{GW_BASE.rstrip('/')}/{GW_ENDPOINT.lstrip('/')}", headers=h, timeout=_gw_timeout())
    r=hit(tok) if tok else None
    if (r is None) or (r.status_code in (401,403)): r=hit(gw_login_token())
    if r.status_code!=200: raise RuntimeError(f"GW_HTTP_{r.status_code}:{(r.text or '')[:180]}")
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

def _osi_from_instrument(ins: dict) -> str | None:
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
        if (ins.get("assetType") or "").upper()!="OPTION": continue
        osi = _osi_from_instrument(ins)
        if not osi: continue
        qty=float(p.get("longQuantity",0))-float(p.get("shortQuantity",0))
        if abs(qty)<1e-9: continue
        out[osi_canon(osi)]=out.get(osi_canon(osi),0.0)+qty
    return out

def list_recent_orders(c, acct_hash: str):
    now_et = datetime.now(ZoneInfo("America/New_York"))
    start_et = now_et.replace(hour=0, minute=0, second=0, microsecond=0)
    url=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders"
    params = {"fromEnteredTime": iso_z(start_et), "toEnteredTime": iso_z(now_et), "maxResults": 200}
    try:
        return schwab_get_json(c, url, params=params, tag="ORDERS") or []
    except Exception:
        return []

def _legs_canon_from_order(o):
    got=set()
    for leg in (o.get("orderLegCollection") or []):
        ins=(leg.get("instrument",{}) or {})
        osi=_osi_from_instrument(ins)
        if osi: got.add(osi_canon(osi))
    return got

def pick_active_and_overlaps(c, acct_hash: str, canon_set):
    exact_id=None; active_status=""; overlaps=[]
    for o in list_recent_orders(c, acct_hash):
        st=str(o.get("status") or "").upper()
        if st not in {"WORKING","QUEUED","OPEN","PENDING_ACTIVATION"}: continue
        got=_legs_canon_from_order(o)
        if not got: continue
        if got==canon_set and exact_id is None:
            exact_id=str(o.get("orderId") or ""); active_status=st
        elif got & canon_set:
            oid=str(o.get("orderId") or "")
            if oid: overlaps.append(oid)
    return exact_id, active_status, overlaps

def _credit_width() -> int:
    width = max(CREDIT_MIN_WIDTH, int(CREDIT_SPREAD_WIDTH))
    return int(math.ceil(width / 5.0) * 5)

def calc_width_for_side(is_credit: bool, opening_cash: float) -> int:
    return _credit_width() if is_credit else 5

def calc_credit_contracts(opening_cash: float | int) -> int:
    try:
        oc = float(opening_cash)
    except Exception:
        oc = 0.0
    denom = CREDIT_DOLLARS_PER_CONTRACT if CREDIT_DOLLARS_PER_CONTRACT > 0 else 1.0
    units = math.ceil(max(0.0, oc) / denom)
    return max(1, int(units))

def get_qty(is_credit: bool, qty_override: str, opening_cash: float | None) -> int:
    if qty_override:
        try: return max(0, int(qty_override))
        except: return 0
    if is_credit:
        return calc_credit_contracts(opening_cash if opening_cash is not None else 0)
    return QTY_FIXED

def main():
    MODE=(os.environ.get("PLACER_MODE","SCHEDULED") or "SCHEDULED").upper()
    source=f"SIMPLE_{MODE}"

    sheet_id=os.environ["GSHEET_ID"]; sa_json=os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
    app_key=os.environ["SCHWAB_APP_KEY"]; app_secret=os.environ["SCHWAB_APP_SECRET"]; token_json=os.environ["SCHWAB_TOKEN_JSON"]

    with open("schwab_token.json","w") as f: f.write(token_json)
    c=client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")

    acct_num, acct_hash = get_primary_acct(c)
    oc = opening_cash_for_account(c, acct_num)

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

    creds=service_account.Credentials.from_service_account_info(json.loads(sa_json),
        scopes=["https://www.googleapis.com/auth/spreadsheets"])
    svc=gbuild("sheets","v4",credentials=creds)
    sheet_id_num=ensure_header_and_get_sheetid(svc, sheet_id, "schwab", HEADERS)

    if MODE=="SCHEDULED":
        now=datetime.now(ZoneInfo("America/New_York"))
        if now.weekday()>=5 or not (now.hour==16 and 8<=now.minute<=14):
            row=[datetime.utcnow().isoformat()+"Z", source, "SPX", last_px, "", "SKIP","","","","",
                 "","","","", "", "SKIPPED_TIME_WINDOW"]
            one_log(svc, sheet_id_num, sheet_id, "schwab", row); print("skip window"); sys.exit(0)

    try:
        api=gw_get_leocross()
    except Exception as e:
        row=[datetime.utcnow().isoformat()+"Z", source, "SPX", last_px, "", "ABORT","","","","",
             "","","","", "", f"ABORT_GW:{str(e)[:150]}"]
        one_log(svc, sheet_id_num, sheet_id, "schwab", row); print(e); sys.exit(0)

    def extract(j):
        if isinstance(j,dict):
            if "Trade" in j:
                tr=j["Trade"]; 
                return tr[-1] if isinstance(tr,list) and tr else tr if isinstance(tr,dict) else {}
            keys=("Date","TDate","Limit","CLimit","Cat1","Cat2")
            if any(k in j for k in keys): return j
            for v in j.values():
                if isinstance(v,(dict,list)):
                    t=extract(v); 
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
        one_log(svc, sheet_id_num, sheet_id, "schwab", row); sys.exit(0)

    sig_date=str(tr.get("Date","")); exp_iso=str(tr.get("TDate","")); exp6=yymmdd(exp_iso)
    inner_put=int(float(tr.get("Limit"))); inner_call=int(float(tr.get("CLimit")))
    def fnum(x): 
        try: return float(x)
        except: return None
    cat1=fnum(tr.get("Cat1")); cat2=fnum(tr.get("Cat2"))
    is_credit = True if (cat2 is None or cat1 is None or cat2>=cat1) else False

    # ---- build legs with proper width ----
    width = calc_width_for_side(is_credit, oc if oc is not None else 0)
    p_low,p_high = inner_put-width, inner_put
    c_low,c_high = inner_call, inner_call+width
    bp = to_osi(f".SPXW{exp6}P{p_low}"); sp = to_osi(f".SPXW{exp6}P{p_high}")
    sc = to_osi(f".SPXW{exp6}C{c_low}"); bc = to_osi(f".SPXW{exp6}C{c_high}")

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

    side_name  = "SHORT_IRON_CONDOR" if is_credit else "LONG_IRON_CONDOR"
    order_type = "NET_CREDIT" if is_credit else "NET_DEBIT"

    qty = get_qty(is_credit, os.environ.get("QTY_OVERRIDE","").strip(), oc)
    if qty < 1:
        row=[datetime.utcnow().isoformat()+"Z", source, "SPX", last_px, sig_date, "ABORT",
             side_name, "", order_type, "",
             legs[0],legs[1],legs[2],legs[3], "", "ABORT_QTY_LT_1"]
        one_log(svc, sheet_id_num, sheet_id, "schwab", row); print("qty<1"); sys.exit(0)

    def order_payload(price: float, q: int):
        return {
            "orderType": order_type,
            "session": "NORMAL",
            "price": f"{clamp_tick(price):.2f}",
            "duration": "DAY",
            "orderStrategyType": "SINGLE",
            "complexOrderStrategyType": "IRON_CONDOR",
            "orderLegCollection":[
                {"instruction":"BUY_TO_OPEN", "positionEffect":"OPENING","quantity":q,"instrument":{"symbol":legs[0],"assetType":"OPTION"}},
                {"instruction":"SELL_TO_OPEN","positionEffect":"OPENING","quantity":q,"instrument":{"symbol":legs[1],"assetType":"OPTION"}},
                {"instruction":"SELL_TO_OPEN","positionEffect":"OPENING","quantity":q,"instrument":{"symbol":legs[2],"assetType":"OPTION"}},
                {"instruction":"BUY_TO_OPEN", "positionEffect":"OPENING","quantity":q,"instrument":{"symbol":legs[3],"assetType":"OPTION"}},
            ]
        }

    # One active working order (replace). Cancel any overlapping partial matches first.
    active_oid, active_status, overlaps = pick_active_and_overlaps(c, acct_hash, canon)
    for oid in overlaps:
        try:
            url=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{oid}"
            schwab_delete(c,url,tag=f"CANCEL_OVERLAP:{oid}")
        except Exception as e:
            print(f"WARN cancel overlap {oid}: {e}")

    def get_status(oid: str) -> dict:
        url=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{oid}"
        try:
            return schwab_get_json(c,url,tag=f"STATUS:{oid}") or {}
        except Exception:
            return {}

    def parse_order_id_from_response(r):
        try:
            j=r.json()
            if isinstance(j,dict):
                oid = j.get("orderId") or j.get("order_id")
                if oid: return str(oid)
        except Exception:
            pass
        loc=r.headers.get("Location","")
        return loc.rstrip("/").split("/")[-1] if loc else ""

    replacements = 0
    canceled = 0
    steps=[]
    filled_total = 0

    def ensure_active(price: float, to_place: int):
        nonlocal active_oid, replacements, canceled
        px = clamp_tick(price)
        if active_oid:
            try:
                url=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{active_oid}"
                r=schwab_put_json(c,url,order_payload(px,to_place),tag=f"REPLACE@{px:.2f}x{to_place}")
                new_id = parse_order_id_from_response(r) or active_oid
                if new_id != active_oid:
                    replacements += 1
                    active_oid = new_id
                else:
                    replacements += 1
                return active_oid
            except Exception as e:
                try:
                    url=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{active_oid}"
                    schwab_delete(c,url,tag=f"CANCEL_FALLBACK:{active_oid}")
                    canceled += 1
                except Exception:
                    pass
                active_oid = None

        url=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders"
        r=schwab_post_json(c,url,order_payload(px,to_place),tag=f"PLACE@{px:.2f}x{to_place}")
        active_oid = parse_order_id_from_response(r)
        return active_oid

    def wait_loop(secs: int):
        nonlocal filled_total
        t_end=time.time()+secs
        while time.time()<t_end:
            if not active_oid: break
            st=get_status(active_oid)
            status = str(st.get("status") or st.get("orderStatus") or "").upper()
            fq = int(round(float(st.get("filledQuantity") or st.get("filled_quantity") or 0)))
            if fq > filled_total: filled_total = fq
            if status == "FILLED" or filled_total >= qty:
                return "FILLED"
            time.sleep(1)
        return "WORKING"

    def rung(px, secs):
        nonlocal filled_total
        to_place = max(0, qty - filled_total)
        if to_place==0: return "FILLED"
        ensure_active(px, to_place)
        steps.append(f"{clamp_tick(px):.2f}@{to_place}")
        return wait_loop(secs)

    # ===== Build ladder =====
    status = "WORKING"
    if is_credit:
        units = max(1.0, width / 5.0)
        start = clamp_tick(max(CREDIT_FLOOR, units * CREDIT_PER5_START))
        stop  = clamp_tick(min(start, max(CREDIT_FLOOR, 0.05)))
        step  = CREDIT_STEP
        secs  = STEP_WAIT_CREDIT

        px = start
        ladder = []
        # Include both start and stop; descending
        while px >= stop - 1e-9:
            ladder.append(clamp_tick(px))
            px = px - step
        for price in ladder:
            status = rung(price, secs)
            if status == "FILLED":
                break
    else:
        # Debit ladder unchanged
        start = clamp_tick(DEBIT_START); stop = clamp_tick(DEBIT_CEIL); step = DEBIT_STEP; secs = STEP_WAIT_DEBIT
        px = start
        ladder = []
        while px <= stop + 1e-9:
            ladder.append(clamp_tick(px))
            px = px + step
        for price in ladder:
            status = rung(price, secs)
            if status == "FILLED":
                break

    if status != "FILLED" and FINAL_CANCEL and active_oid:
        try:
            url=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{active_oid}"
            schwab_delete(c,url,tag=f"CANCEL_FINAL:{active_oid}")
            canceled += 1
        except Exception:
            pass

    side = "SHORT_IRON_CONDOR" if is_credit else "LONG_IRON_CONDOR"
    otype = "NET_CREDIT" if is_credit else "NET_DEBIT"
    trace = "STEPS " + "→".join(steps) if steps else "STEPS"
    filled_str = f"FILLED {filled_total}/{qty}"
    repl_str   = f"REPLACED {replacements}"
    canceled_str = f"CANCELED {canceled}"
    used_price = steps[-1].split("@",1)[0] if steps else ""
    oid_for_log = active_oid or ""
    if is_credit:
        ladder_note = ("credit_ladder start={:.2f} step={:.2f} floor={:.2f} width={} oc={}"
                       .format(start, CREDIT_STEP, CREDIT_FLOOR,
                               width, oc if oc is not None else 'NA'))
    else:
        ladder_note = f"debit_ladder {DEBIT_START:.2f}→{DEBIT_CEIL:.2f} step={DEBIT_STEP:.2f} width={width}"

    status_txt = (("FILLED " + trace) if (filled_total >= qty) else (status + " " + trace)) + \
                 f" | {filled_str} | {repl_str} | {canceled_str} | {ladder_note}"

    row = [datetime.utcnow().isoformat()+"Z", source, "SPX", last_px,
           sig_date, "PLACE", side, qty, otype,
           used_price,
           legs[0],legs[1],legs[2],legs[3],
           oid_for_log, status_txt]
    one_log(svc, sheet_id_num, sheet_id, "schwab", row)
    print(f"FINAL {status_txt} OID={oid_for_log} PRICE_USED={used_price if used_price else 'NA'}")

def time_gate_ok():
    now=datetime.now(ZoneInfo("America/New_York"))
    return (now.weekday()<5 and now.hour==16 and 8<=now.minute<=14)

if __name__=="__main__":
    MODE=(os.environ.get("PLACER_MODE","SCHEDULED") or "SCHEDULED").upper()
    if MODE=="SCHEDULED" and not time_gate_ok():
        print("Scheduled window not met; exit."); sys.exit(0)
    main()
