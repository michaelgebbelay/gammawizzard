#!/usr/bin/env python3
# VERSION: 2025-10-14 v3.1 — Placer
# - Side override (AUTO|CREDIT|DEBIT)
# - Push-out toggle honored (5-wide never pushed)
# - Debit ladder: BID → MID → MID+0.05 → MID+0.10 (capped to ASK)
# - Credit ladder: ASK → MID → MID-0.05 → MID-0.10 (capped to BID)
# - CANCEL_REPLACE with dwell per rung; two passes; hard cutoff
# - Logs to Sheets "schwab" tab

import os, sys, json, time, re, math, random
from datetime import datetime, date, timezone, timedelta
from zoneinfo import ZoneInfo
from decimal import Decimal, ROUND_HALF_UP
import requests
from typing import List
from schwab.auth import client_from_token_file
from google.oauth2 import service_account
from googleapiclient.discovery import build as gbuild

TICK = 0.05
ET   = ZoneInfo("America/New_York")

def _as_float(env_name: str, default: str) -> float:
    raw = os.environ.get(env_name, default)
    try: return float(raw)
    except: return float(default)

def _as_int(env_name: str, default: str) -> int:
    raw = os.environ.get(env_name, default)
    try: return int(raw)
    except: return int(default)

def clamp_tick(x: float) -> float:
    return round(round(x / 0.05) * 0.05 + 1e-12, 2)

def _truthy(s:str)->bool: return str(s or "").strip().lower() in {"1","true","t","yes","y","on"}

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

def osi_canon(osi: str): return (osi[6:12], osi[12], osi[-8:])
def strike_from_osi(osi: str) -> float: return int(osi[-8:]) / 1000.0
def iso_z(dt): return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# ===== Config from env =====
REPLACE_MODE        = (os.environ.get("REPLACE_MODE", "CANCEL_REPLACE") or "CANCEL_REPLACE").upper()
STEP_WAIT_CREDIT    = _as_float("STEP_WAIT_CREDIT", "5")
STEP_WAIT_DEBIT     = _as_float("STEP_WAIT_DEBIT",  "5")
MIN_RUNG_WAIT       = _as_float("MIN_RUNG_WAIT",    "5")
MAX_LADDER_CYCLES   = _as_int("MAX_LADDER_CYCLES",  "2")
MAX_RUNTIME_SECS    = _as_float("MAX_RUNTIME_SECS", "115")
CANCEL_SETTLE_SECS  = _as_float("CANCEL_SETTLE_SECS","0.8")
HARD_CUTOFF_HHMM    = os.environ.get("HARD_CUTOFF_HHMM","16:15").strip()

CREDIT_SPREAD_WIDTH = int(os.environ.get("CREDIT_SPREAD_WIDTH","20"))
CREDIT_MIN_WIDTH    = 5
PUSH_OUT_SHORTS     = _truthy(os.environ.get("PUSH_OUT_SHORTS","false"))
VERBOSE             = _truthy(os.environ.get("VERBOSE","1"))

WINDOW_STATUSES = {"WORKING","QUEUED","OPEN","PENDING_ACTIVATION","ACCEPTED","RECEIVED"}
ACTIVE_STATUSES = WINDOW_STATUSES | {"PENDING_CANCEL","CANCEL_REQUESTED","PENDING_REPLACE"}

SHEET_TAB = "schwab"
HEADERS = [
    "ts","source","symbol","last_price",
    "signal_date","order_mode","side","qty_exec","order_type","limit_price",
    "occ_buy_put","occ_sell_put","occ_sell_call","occ_buy_call",
    "order_id","status"
]

def vprint(*args, **kwargs):
    if VERBOSE: print(*args, **kwargs)

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
        try: return max(1.0, float(ra))
        except: pass
    return min(10.0, 0.5 * (2 ** attempt)) + random.uniform(0.0, 0.25)

def schwab_get_json(c, url, params=None, tries=6, tag=""):
    last = ""
    for i in range(tries):
        try:
            r = c.session.get(url, params=(params or {}), timeout=20)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                time.sleep(_sleep_for_429(r, i)); continue
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
                time.sleep(_sleep_for_429(r, i)); continue
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
            if r.status_code in (200, 201, 202, 204):
                return r
            if r.status_code == 429:
                time.sleep(_sleep_for_429(r, i)); continue
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
                time.sleep(_sleep_for_429(r, i)); continue
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
    data = r.json(); accs = data if isinstance(data, list) else [data]
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
            elif isinstance(x,list): stack.extend(x)
        return acct_id, initial, current
    chosen=None
    for a in accs:
        aid, init, curr = hunt(a)
        if acct_number and aid == acct_number: chosen=(init,curr); break
        if chosen is None: chosen=(init,curr)
    if not chosen: return None
    init, curr = chosen
    oc = pick(init,"cashBalance","cashAvailableForTrading","liquidationValue")
    if oc is None: oc = pick(curr,"cashBalance","cashAvailableForTrading","liquidationValue")
    return oc

def _credit_width() -> int:
    width = max(CREDIT_MIN_WIDTH, int(CREDIT_SPREAD_WIDTH))
    return int(math.ceil(width / 5.0) * 5)

def _round_half_up(x: float) -> int:
    return int(Decimal(x).quantize(Decimal('1'), rounding=ROUND_HALF_UP))

def calc_width_for_side(is_credit: bool, opening_cash: float | int | None = None) -> int:
    return _credit_width() if is_credit else 5

def calc_credit_contracts(opening_cash: float | int) -> int:
    try: oc=float(opening_cash)
    except: oc=0.0
    width=_credit_width()
    denom=4000.0*(width/5.0)
    units=_round_half_up(oc/denom)
    return max(1,int(units))

def calc_debit_contracts(opening_cash: float | int) -> int:
    try: oc=float(opening_cash)
    except: oc=0.0
    return max(1,int(math.floor(oc/4000.0)))

# ===== Quotes & NBBO =====
def fetch_bid_ask(c, osi: str):
    r=c.get_quote(osi)
    if r.status_code!=200: return (None,None)
    d=list(r.json().values())[0] if isinstance(r.json(), dict) else {}
    q=d.get("quote", d)
    b=q.get("bidPrice") or q.get("bid") or q.get("bidPriceInDouble")
    a=q.get("askPrice") or q.get("ask") or q.get("askPriceInDouble")
    return (float(b) if b is not None else None, float(a) if a is not None else None)

def condor_nbbo_credit(c, legs):
    bp, sp, sc, bc = legs
    bp_b, bp_a = fetch_bid_ask(c, bp); sp_b, sp_a = fetch_bid_ask(c, sp)
    sc_b, sc_a = fetch_bid_ask(c, sc); bc_b, bc_a = fetch_bid_ask(c, bc)
    if None in (bp_b, bp_a, sp_b, sp_a, sc_b, sc_a, bc_b, bc_a):
        return (None, None, None)
    credit_bid = (sp_b + sc_b) - (bp_a + bc_a)
    credit_ask = (sp_a + sc_a) - (bp_b + bc_b)
    credit_mid = (credit_bid + credit_ask) / 2.0
    return (clamp_tick(credit_bid), clamp_tick(credit_ask), clamp_tick(credit_mid))

def condor_nbbo_debit(c, legs):
    bp, sp, sc, bc = legs
    bp_b, bp_a = fetch_bid_ask(c, bp); sp_b, sp_a = fetch_bid_ask(c, sp)
    sc_b, sc_a = fetch_bid_ask(c, sc); bc_b, bc_a = fetch_bid_ask(c, bc)
    if None in (bp_b, bp_a, sp_b, sp_a, sc_b, sc_a, bc_b, bc_a):
        return (None, None, None)
    debit_bid = (bp_b + bc_b) - (sp_a + sc_a)   # lowest you might pay
    debit_ask = (bp_a + bc_a) - (sp_b + sc_b)   # highest you might pay
    debit_mid = (debit_bid + debit_ask) / 2.0
    return (clamp_tick(debit_bid), clamp_tick(debit_ask), clamp_tick(debit_mid))

# ===== Orders helpers =====
def list_recent_orders(c, acct_hash: str):
    now_et = datetime.now(ET)
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
        sym = ins.get("symbol") or ""
        try:
            osi = to_osi(sym)
        except Exception:
            exp = ins.get("optionExpirationDate") or ins.get("expirationDate") or ""
            pc  = (ins.get("putCall") or ins.get("type") or "").upper()
            strike = ins.get("strikePrice") or ins.get("strike")
            if exp and pc in ("CALL","PUT") and strike is not None:
                ymd = date.fromisoformat(str(exp)[:10]).strftime("%y%m%d")
                cp = "C" if pc.startswith("C") else "P"
                mills = int(round(float(strike)*1000))
                osi = "{:<6s}{}{}{:08d}".format("SPXW", ymd, cp, mills)
            else:
                osi = None
        if osi: got.add(osi_canon(osi))
    return got

def pick_active_and_overlaps(c, acct_hash: str, canon_set):
    exact_id=None; active_status=""; overlaps=[]
    for o in list_recent_orders(c, acct_hash):
        st=str(o.get("status") or "").upper()
        if st not in ACTIVE_STATUSES: continue
        got=_legs_canon_from_order(o)
        if not got: continue
        if got==canon_set and exact_id is None:
            exact_id=str(o.get("orderId") or ""); active_status=st
        elif got & canon_set:
            oid=str(o.get("orderId") or "")
            if oid: overlaps.append(oid)
    return exact_id, active_status, overlaps

# ===== GW helpers =====
def _sanitize_token(t: str) -> str:
    t=(t or "").strip().strip('"').strip("'")
    return t.split(None,1)[1] if t.lower().startswith("bearer ") else t

def _gw_timeout():
    try: return int(os.environ.get("GW_TIMEOUT","30"))
    except: return 30

def gw_login_token():
    email=os.environ.get("GW_EMAIL",""); pwd=os.environ.get("GW_PASSWORD","")
    if not (email and pwd): raise RuntimeError("GW_LOGIN_MISSING_CREDS")
    r=requests.post(f"https://gandalf.gammawizard.com/goauth/authenticateFireUser", data={"email":email,"password":pwd}, timeout=_gw_timeout())
    if r.status_code!=200: raise RuntimeError(f"GW_LOGIN_HTTP_{r.status_code}:{(r.text or '')[:180]}")
    j=r.json(); t=j.get("token"); 
    if not t: raise RuntimeError("GW_LOGIN_NO_TOKEN")
    return t

def gw_get_leocross():
    tok=_sanitize_token(os.environ.get("GW_TOKEN","") or "")
    def hit(t):
        h={"Accept":"application/json","Authorization":f"Bearer {_sanitize_token(t)}","User-Agent":"gw-placer/1.4"}
        return requests.get("https://gandalf.gammawizard.com/rapi/GetLeoCross", headers=h, timeout=_gw_timeout())
    r=hit(tok) if tok else None
    if (r is None) or (r.status_code in (401,403)): r=hit(gw_login_token())
    if r.status_code!=200: raise RuntimeError(f"GW_HTTP_{r.status_code}:{(r.text or '')[:180]}")
    return r.json()

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

# ===== main =====
def main():
    MODE=(os.environ.get("PLACER_MODE","MANUAL") or "MANUAL").upper()
    source=f"SIMPLE_{MODE}"
    sheet_id=os.environ.get("GSHEET_ID","")
    sa_json=os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON","")

    app_key=os.environ["SCHWAB_APP_KEY"]; app_secret=os.environ["SCHWAB_APP_SECRET"]; token_json=os.environ["SCHWAB_TOKEN_JSON"]
    with open("schwab_token.json","w") as f: f.write(token_json)
    c=client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")
    acct_num, acct_hash = get_primary_acct(c)

    # Optional $ override just for qty calc
    oc_override_raw = os.environ.get("SIZING_DOLLARS_OVERRIDE","").strip()
    oc_override=None
    if oc_override_raw:
        try: oc_override=float(oc_override_raw)
        except: oc_override=None
    oc_real = opening_cash_for_account(c, acct_num)
    oc = oc_override if (oc_override is not None and oc_override > 0) else oc_real

    LOCK_SIDE        = (os.environ.get("LOCK_SIDE") or os.environ.get("SIDE_OVERRIDE") or "AUTO").strip().upper()
    LOCK_WIDTH_RAW   = (os.environ.get("LOCK_WIDTH") or "").strip()
    LOCK_LEGS_JSON   = (os.environ.get("LOCK_LEGS_JSON") or "").strip()
    ALLOW_QTY_OVER_MAX = str(os.environ.get("ALLOW_QTY_OVER_MAX","false")).lower() in {"1","true","t","yes","y","on"}

    # SPX last (for logging)
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

    # Sheets
    svc=None; sheet_id_num=None
    if sheet_id and sa_json:
        try:
            creds=service_account.Credentials.from_service_account_info(json.loads(sa_json),
                scopes=["https://www.googleapis.com/auth/spreadsheets"])
            svc=gbuild("sheets","v4",credentials=creds)
            sheet_id_num=ensure_header_and_get_sheetid(svc, sheet_id, SHEET_TAB, HEADERS)
        except Exception as e:
            vprint("Sheets init failed:", e)

    def log_row(status_txt, side_name, otype, legs, qty, used_price, oid_for_log, sig_date):
        if not svc: 
            vprint("LOG:", status_txt)
            return
        row = [datetime.utcnow().isoformat()+"Z", source, "SPX", last_px,
               sig_date, "PLACE", side_name, qty, otype,
               used_price, legs[0],legs[1],legs[2],legs[3], oid_for_log, status_txt]
        one_log(svc, sheet_id_num, sheet_id, SHEET_TAB, row)

    # Time gate for SCHEDULED mode only
    def time_gate_ok():
        now=datetime.now(ET)
        return (now.weekday()<5 and now.hour==16 and 8<=now.minute<=14)
    if MODE=="SCHEDULED" and not time_gate_ok():
        log_row("SKIPPED_TIME_WINDOW","", "", ["","","",""], 0, "", "", "")
        print("skip window"); sys.exit(0)

    # ---- Leo / Locks
    tr = {}
    sig_date = ""
    exp_iso = ""

    # Determine side (credit/debit) and legs/width
    is_credit = None
    legs = None
    width = None

    # 1) If we were given exact legs, use them and deduce width if not provided.
    if LOCK_LEGS_JSON:
        try:
            legs = json.loads(LOCK_LEGS_JSON)
            if not (isinstance(legs, list) and len(legs) == 4):
                legs = None
        except Exception:
            legs = None

    if LOCK_SIDE in {"CREDIT","DEBIT"}:
        is_credit = (LOCK_SIDE == "CREDIT")

    if LOCK_WIDTH_RAW:
        try:
            width = int(LOCK_WIDTH_RAW)
        except Exception:
            width = None

    # 2) If legs are locked, we don't need Leo to rebuild anything.
    if legs and (is_credit is not None) and (width is not None):
        pass  # fully locked path
    else:
        # FALLBACK: fetch Leo once (only if locks were not complete)
        try:
            api = gw_get_leocross()
            tr = extract(api)
        except Exception as e:
            log_row(f"ABORT_GW:{str(e)[:150]}", "", "", ["","","",""], 0, "", "", "")
            print(e); sys.exit(0)

        if not tr:
            log_row("NO_TRADE_PAYLOAD", "", "", ["","","",""], 0, "", "", ""); sys.exit(0)

        sig_date = str(tr.get("Date",""))
        exp_iso = str(tr.get("TDate",""))

        if is_credit is None:
            def fnum(x):
                try: return float(x)
                except: return None
            cat1=fnum(tr.get("Cat1")); cat2=fnum(tr.get("Cat2"))
            is_credit = True if (cat2 is None or cat1 is None or cat2>=cat1) else False

        if legs is None or width is None:
            exp6 = yymmdd(exp_iso)
            inner_put = int(float(tr.get("Limit")))
            inner_call = int(float(tr.get("CLimit")))
            if width is None:
                width = calc_width_for_side(is_credit, oc if oc is not None else 0)
            if is_credit:
                if PUSH_OUT_SHORTS and width != 5:
                    sell_put  = inner_put  - 5
                    buy_put   = sell_put   - width
                    sell_call = inner_call + 5
                    buy_call  = sell_call  + width
                    p_low, p_high = buy_put, sell_put
                    c_low, c_high = sell_call, buy_call
                else:
                    p_low, p_high = inner_put - width, inner_put
                    c_low, c_high = inner_call, inner_call + width
            else:
                p_low, p_high = inner_put - width, inner_put
                c_low, c_high = inner_call, inner_call + width
            bp = to_osi(f".SPXW{exp6}P{p_low}"); sp = to_osi(f".SPXW{exp6}P{p_high}")
            sc = to_osi(f".SPXW{exp6}C{c_low}"); bc = to_osi(f".SPXW{exp6}C{c_high}")
            legs = [bp, sp, sc, bc]

    # Safety: if width is still None, compute from legs
    if width is None and legs:
        width = int(round(abs(strike_from_osi(legs[1]) - strike_from_osi(legs[0]))))

    def orient(bp, sp, sc, bc):
        bpS=strike_from_osi(bp); spS=strike_from_osi(sp)
        scS=strike_from_osi(sc); bcS=strike_from_osi(bc)
        if is_credit:
            if bpS>spS: bp,sp = sp,bp
            if scS>bcS: sc,bc = bc,sc
        else:
            if bpS<spS: bp,sp = sp,bp
            if bcS>scS: sc,bc = bc,sc
        return [bp, sp, sc, bc]

    legs = orient(*legs)
    bp, sp, sc, bc = legs
    canon = {osi_canon(x) for x in legs}

    side_name  = "SHORT_IRON_CONDOR" if is_credit else "LONG_IRON_CONDOR"
    order_type = "NET_CREDIT"        if is_credit else "NET_DEBIT"

    # ---- Quantity (with clamp) ----
    qty_override_raw = os.environ.get("QTY_OVERRIDE","" ).strip()
    qty_req = 0
    if qty_override_raw:
        try: qty_req = int(qty_override_raw)
        except: qty_req = 0

    auto_max = calc_credit_contracts(oc) if is_credit else calc_debit_contracts(oc)
    qty = (qty_req if qty_req>0 else auto_max)
    if not ALLOW_QTY_OVER_MAX:
        qty = max(1, min(qty, auto_max))
    else:
        qty = max(1, qty)

    if qty < 1:
        log_row("ABORT_QTY_LT_1", side_name, order_type, legs, 0, "", "", sig_date); print("qty<1"); sys.exit(0)

    # Banner
    secs = STEP_WAIT_CREDIT if is_credit else STEP_WAIT_DEBIT
    vprint(f"PLACER START side={'CREDIT' if is_credit else 'DEBIT'} "
           f"{'(LOCKED)' if LOCK_SIDE in {'CREDIT','DEBIT'} else '(AUTO)'} "
           f"width={width} qty={qty} (req={qty_req}, auto_max={auto_max}) oc={oc}")
    vprint(f"LADDER wait={secs:.1f}s (min {MIN_RUNG_WAIT:.2f}s), cycles={MAX_LADDER_CYCLES} replace={REPLACE_MODE} cutoff={HARD_CUTOFF_HHMM} ET")

    # ---- status helpers ----
    def get_status(oid: str) -> dict:
        url=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{oid}"
        try:
            return schwab_get_json(c,url,tag=f"STATUS:{oid}") or {}
        except Exception:
            return {}

    def wait_until_closed(oid: str, max_wait: float = CANCEL_SETTLE_SECS) -> bool:
        t_end = time.time() + max_wait
        while time.time() < t_end:
            st = get_status(oid)
            status = str(st.get("status") or st.get("orderStatus") or "").upper()
            if (not status) or status in {"CANCELED","FILLED","REJECTED","EXPIRED"}:
                return True
            if status not in ACTIVE_STATUSES:
                return True
            time.sleep(0.2)
        return False

    # One active working order (replace). Cancel overlapping partial matches first.
    active_oid, active_status, overlaps = pick_active_and_overlaps(c, acct_hash, canon)
    for oid in overlaps:
        try:
            url=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{oid}"
            schwab_delete(c, url, tag=f"CANCEL_OVERLAP:{oid}")
            vprint(f"CANCEL_OVERLAP OID={oid}")
            wait_until_closed(oid)
        except Exception as e:
            print(f"WARN cancel overlap {oid}: {e}")

    # Best-effort cleanup on termination
    import signal
    def _on_term(signum, frame):
        try:
            if active_oid:
                url=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{active_oid}"
                schwab_delete(c, url, tag=f"SIG_CANCEL:{active_oid}")
                vprint(f"SIG_CANCEL OID={active_oid}")
        except Exception as e:
            print(f"WARN SIG cleanup failed: {e}")
        finally:
            sys.exit(128+signum)
    signal.signal(signal.SIGTERM, _on_term)
    signal.signal(signal.SIGINT,  _on_term)

    def parse_order_id_from_response(r):
        try:
            j=r.json()
            if isinstance(j,dict):
                oid = j.get("orderId") or j.get("order_id")
                if oid: return str(oid)
        except Exception: pass
        loc=r.headers.get("Location","")
        return loc.rstrip("/").split("/")[-1] if loc else ""

    replacements = 0
    canceled = 0
    steps=[]
    filled_total = 0
    START_TS = time.time()

    # ---- order payload ----
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

    # ---- (cancel-)replace or place ----
    def ensure_active(price: float, to_place: int):
        nonlocal active_oid, replacements, canceled
        px = clamp_tick(price)
        for attempt in range(6):
            try:
                if active_oid and REPLACE_MODE == "REPLACE":
                    url=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{active_oid}"
                    try:
                        r=schwab_put_json(c, url, order_payload(px,to_place), tag=f"REPLACE@{px:.2f}x{to_place}")
                        new_id = parse_order_id_from_response(r) or active_oid
                        replacements += 1
                        active_oid = new_id
                        vprint(f"REPLACE → OID={active_oid} @ {px:.2f} x{to_place}")
                        return active_oid
                    except Exception as e:
                        vprint(f"REPLACE failed ({e}); falling back to CANCEL_REPLACE")
                        try:
                            schwab_delete(c, url, tag=f"CANCEL_STEP:{active_oid}")
                            canceled += 1
                        except Exception: pass
                        active_oid = None

                if active_oid and REPLACE_MODE == "CANCEL_REPLACE":
                    try:
                        url=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{active_oid}"
                        schwab_delete(c, url, tag=f"CANCEL_STEP:{active_oid}")
                        canceled += 1
                        vprint(f"CANCEL_STEP OID={active_oid}")
                        wait_until_closed(active_oid)
                    except Exception: pass
                    active_oid = None
                    # double-check: no residual overlaps before we place
                    ex, st, ovs = pick_active_and_overlaps(c, acct_hash, canon)
                    for oid in ([ex] if ex else []) + ovs:
                        try:
                            url=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{oid}"
                            schwab_delete(c, url, tag=f"CANCEL_RESIDUAL:{oid}")
                            vprint(f"CANCEL_RESIDUAL OID={oid}")
                            wait_until_closed(oid)
                        except Exception: pass

                url=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders"
                r=schwab_post_json(c, url, order_payload(px,to_place), tag=f"PLACE@{px:.2f}x{to_place}")
                active_oid = parse_order_id_from_response(r)
                vprint(f"PLACE → OID={active_oid} @ {px:.2f} x{to_place}")
                return active_oid

            except Exception as e:
                vprint(f"ensure_active retry {attempt+1}/6: {e}")
                time.sleep(min(10.0, 0.5*(2**attempt)))
                continue

        vprint("ensure_active exhausted retries; proceeding without active OID")
        return active_oid

    # ---- cutoff check ----
    def cutoff_reached() -> bool:
        now = datetime.now(ET)
        try:
            hh, mm = [int(x) for x in HARD_CUTOFF_HHMM.split(":")]
        except Exception:
            hh, mm = 16, 15
        cut = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        return now >= cut

    # ---- wait loop with dwell & cutoff ----
    def wait_loop(secs: float):
        nonlocal filled_total
        secs_eff = max(float(secs), MIN_RUNG_WAIT)
        t_end=time.time()+secs_eff
        while time.time()<t_end:
            if cutoff_reached() or (time.time() - START_TS > MAX_RUNTIME_SECS):
                return "TIMEOUT"
            if not active_oid:
                time.sleep(0.25); continue
            st=get_status(active_oid)
            status = str(st.get("status") or st.get("orderStatus") or "").upper()
            fq = int(round(float(st.get("filledQuantity") or st.get("filled_quantity") or 0)))
            if fq > filled_total: filled_total = fq
            if status == "FILLED" or filled_total >= qty:
                return "FILLED"
            time.sleep(0.25)
        return "WORKING"

    # ---- rung ----
    def rung(px, secs):
        nonlocal filled_total
        to_place = max(0, qty - filled_total)
        if to_place==0: return "FILLED"
        vprint(f"RUNG → price={clamp_tick(px):.2f} to_place={to_place} wait≈{max(secs, MIN_RUNG_WAIT):.2f}s")
        ensure_active(px, to_place)
        st = wait_loop(secs)
        steps.append(f"{clamp_tick(px):.2f}@{to_place}")
        vprint(f"RUNG_DONE status={st} filled={filled_total}/{qty} active_oid={active_oid or 'NA'}")
        return st

    # ---- ladder builders (from Schwab NBBO) ----
    def credit_ladder_from_nbbo(credit_bid, credit_ask, credit_mid):
        """ASK → MID → MID-0.05 → MID-0.10, never below credit_bid."""
        if credit_ask is None and credit_mid is None:
            return []
        rungs = []
        if credit_ask is not None:
            rungs.append(credit_ask)
        if credit_mid is not None:
            rungs += [credit_mid,
                      clamp_tick(credit_mid - 0.05),
                      clamp_tick(credit_mid - 0.10)]
            if credit_bid is not None:
                rungs[-2] = max(rungs[-2], credit_bid)
                rungs[-1] = max(rungs[-1], credit_bid)
        seen=set(); out=[]
        for p in rungs:
            p = clamp_tick(p)
            if p not in seen:
                seen.add(p); out.append(p)
        return out

    def debit_ladder_from_nbbo(debit_bid, debit_ask, debit_mid):
        """BID → MID → MID+0.05 → MID+0.10, never above debit_ask; floor at 0.05."""
        if debit_bid is None and debit_mid is None:
            return []
        rungs = []
        if debit_bid is not None:
            rungs.append(debit_bid)
        if debit_mid is not None:
            p3 = clamp_tick(debit_mid + 0.05)
            p4 = clamp_tick(debit_mid + 0.10)
            if debit_ask is not None:
                p3 = min(p3, debit_ask)
                p4 = min(p4, debit_ask)
            rungs += [debit_mid, p3, p4]
        seen=set(); out=[]
        for p in rungs:
            p = clamp_tick(max(0.05, p))
            if p not in seen:
                seen.add(p); out.append(p)
        return out

    # ===== Cycle loop =====
    status = "WORKING"
    cycles = 0

    while cycles < MAX_LADDER_CYCLES and filled_total < qty and not cutoff_reached():
        secs = STEP_WAIT_CREDIT if is_credit else STEP_WAIT_DEBIT

        if is_credit:
            nbbo_bid, nbbo_ask, nbbo_mid = condor_nbbo_credit(c, legs)
        else:
            nbbo_bid, nbbo_ask, nbbo_mid = condor_nbbo_debit(c, legs)
        if nbbo_bid is None and nbbo_mid is None:
            vprint("NBBO unavailable — skipping cycle")
            break

        vprint(f"{'CREDIT' if is_credit else 'DEBIT'} NBBO: bid={nbbo_bid} ask={nbbo_ask} mid={nbbo_mid}")
        ladder = (credit_ladder_from_nbbo(nbbo_bid, nbbo_ask, nbbo_mid)
                  if is_credit else
                  debit_ladder_from_nbbo(nbbo_bid, nbbo_ask, nbbo_mid))
        vprint(f"CYCLE {cycles+1}/{MAX_LADDER_CYCLES} ladder: {ladder}")

        for price in ladder:
            status = rung(price, secs)
            if status in ("FILLED","TIMEOUT"):
                break

        if filled_total >= qty or status == "TIMEOUT" or cutoff_reached():
            break

        # End of cycle — cancel any working order before next cycle
        if active_oid:
            try:
                url=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{active_oid}"
                schwab_delete(c, url, tag=f"CANCEL_CYCLE:{active_oid}")
                canceled += 1
                vprint(f"CANCEL_CYCLE OID={active_oid}")
                wait_until_closed(active_oid)
            except Exception: pass
            active_oid = None

        # quick Leo refresh between passes
        try:
            api2 = gw_get_leocross()
            tr2  = extract(api2)
            if tr2:
                # (re)build legs if strikes/expiry changed
                exp6_new = yymmdd(str(tr2.get("TDate","")))
                put_new  = int(float(tr2.get("Limit")))
                call_new = int(float(tr2.get("CLimit")))

                # Read the current value safely without tripping Python's local
                # scoping rules when we assign later in this block.
                exp6_cur = locals().get("exp6")

                changed = (exp6_cur is None) or \
                          (exp6_new != exp6_cur) or \
                          (put_new  != inner_put) or \
                          (call_new != inner_call)
                if changed:
                    # Update working strikes/expiry
                    exp6      = exp6_new
                    inner_put = put_new
                    inner_call= call_new

                    if is_credit:
                        sell_put  = inner_put  - 5
                        buy_put   = sell_put   - width
                        sell_call = inner_call + 5
                        buy_call  = sell_call  + width
                        p_low, p_high = buy_put, sell_put
                        c_low, c_high = sell_call, buy_call
                    else:
                        p_low, p_high = inner_put - width, inner_put
                        c_low, c_high = inner_call, inner_call + width

                    legs = orient(
                        to_osi(f".SPXW{exp6}P{p_low}"),
                        to_osi(f".SPXW{exp6}P{p_high}"),
                        to_osi(f".SPXW{exp6}C{c_low}"),
                        to_osi(f".SPXW{exp6}C{c_high}")
                    )
                    canon = {osi_canon(x) for x in legs}
                    vprint(f"REFRESH_FROM_LEO: width={width} legs={legs}")
        except Exception as e:
            vprint(f"REFRESH_FROM_LEO failed: {e} — continuing")

        cycles += 1

    # final cleanup if still working or cutoff
    if active_oid:
        try:
            url=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{active_oid}"
            schwab_delete(c, url, tag=f"CANCEL_FINAL:{active_oid}")
            canceled += 1
            vprint(f"CANCEL_FINAL OID={active_oid}")
            wait_until_closed(active_oid)
        except Exception: pass

    # Final sweep to guarantee no stray working/accepted orders for these legs
    try:
        ex, st, ovs = pick_active_and_overlaps(c, acct_hash, canon)
        for oid in ([ex] if ex else []) + ovs:
            url = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{oid}"
            try:
                schwab_delete(c, url, tag=f"CANCEL_SWEEP:{oid}")
                vprint(f"CANCEL_SWEEP OID={oid}")
                wait_until_closed(oid, max_wait=1.5)
            except Exception: pass
    except Exception: pass

    used_price = steps[-1].split("@",1)[0] if steps else ""
    oid_for_log = ""
    filled_str = f"FILLED {filled_total}/{qty}"
    repl_str   = f"REPLACED {replacements}"
    canceled_str = f"CANCELED {canceled}"
    trace = "STEPS " + "→".join(steps) if steps else "STEPS"
    status_txt = (("FILLED " + trace) if (filled_total >= qty) else (status + " " + trace)) + \
                 f" | {filled_str} | {repl_str} | {canceled_str} | width={width}"

    print(f"FINAL {status_txt} PRICE_USED={used_price if used_price else 'NA'}")
    log_row(status_txt, side_name, order_type, legs, qty, used_price, oid_for_log, sig_date)

if __name__=="__main__":
    main()
