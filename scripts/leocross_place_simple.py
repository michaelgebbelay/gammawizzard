# LIVE LeoCross → Schwab placer (minimal, reliable)
# MODE: PLACER_MODE = NOW | SCHEDULED
#
# What it does (no fluff):
# - OBP sizing: qty = floor(optionBuyingPower / 5000)
# - Build 5-wide SPX iron condor (from LeoCross Limit/CLimit)
# - NEVER CLOSE: if any leg would offset an existing position → skip & log
# - Exactly ONE working order: every step = cancel any matching open orders, then place fresh
# - Ladder (30s per step, polls 1s) — CREDIT: 2.10 → mid → new mid → prev mid - 0.05 → 1.90 → wait 30s → CANCEL → restart
#                                             DEBIT:  1.90 → mid → new mid → prev mid + 0.05 → 2.10 → wait 30s → CANCEL → restart
# - Timebox ≈ 180s. Stops on fill or timebox.
# - One write to Google Sheet at the end (includes step trace + sizing)
#
# Required env (secrets):
#   SCHWAB_APP_KEY, SCHWAB_APP_SECRET, SCHWAB_TOKEN_JSON
#   GSHEET_ID, GOOGLE_SERVICE_ACCOUNT_JSON
#   GW_EMAIL, GW_PASSWORD  (GW_TOKEN optional; login fallback is automatic)
#
# Only control:
#   PLACER_MODE = NOW | SCHEDULED    (default SCHEDULED: only 16:08–16:14 ET, weekdays)

import os, sys, json, time, re
from datetime import datetime, date
from zoneinfo import ZoneInfo
import requests
from schwab.auth import client_from_token_file
from google.oauth2 import service_account
from googleapiclient.discovery import build as gbuild

# ---------- fixed constants per your spec ----------
TICK = 0.05
WIDTH = 5
ET = ZoneInfo("America/New_York")
STEP_WAIT = 30           # seconds between steps
TIMEBOX_SEC = 180        # ~2.5–3.0 minutes total
CREDIT_START = 2.10
CREDIT_FLOOR = 1.90
DEBIT_START  = 1.90
DEBIT_CEIL   = 2.10
PER_UNIT = 5000          # 1 contract per $5k OBP

GW_BASE = "https://gandalf.gammawizard.com"
GW_ENDPOINT = "/rapi/GetLeoCross"

SHEET_TAB = "schwab"     # log tab (one row)

# ---------- little utils ----------
def _to_int(x, d):  # safe cast
    try: return int(x)
    except: return d

def _to_float(x, d):
    try: return float(x)
    except: return d

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
    m = re.match(r'^([A-Z.$^]{1,6})(\d{6})([CP])(\d{1,5})(?:\.(\d{1,3}))?$', raw)
    if not m:
        m = re.match(r'^([A-Z.$^]{1,6})(\d{6})([CP])(\d{8})$', raw)
    if not m:
        raise ValueError(f"Cannot parse option symbol: {sym}")
    root, ymd, cp, strike, frac = (m.groups()+("",))[:5]
    mills = int(strike) * 1000 + (int((frac or "0").ljust(3,'0')) if frac else 0) if len(strike) < 8 else int(strike)
    return f"{root:<6}{ymd}{cp}{mills:08d}"

def osi_canon(osi: str):
    """Canonical key ignoring root: (yymmdd, 'C'/'P', strike_mills)."""
    ymd = osi[6:12]
    cp = osi[12]
    strike8 = osi[-8:]
    return (ymd, cp, strike8)

def strike_from_osi(osi: str) -> float:
    return int(osi[-8:]) / 1000.0

# ---------- Sheets ----------
SCHWAB_HEADERS = [
    "ts","source","symbol","last_price",
    "signal_date","order_mode","side","qty_exec","order_type","limit_price",
    "occ_buy_put","occ_sell_put","occ_sell_call","occ_buy_call",
    "order_id","status"
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

def one_log(svc, sheet_id_num, spreadsheet_id: str, tab: str, row_vals: list):
    top_insert(svc, spreadsheet_id, sheet_id_num)
    svc.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range=f"{tab}!A2",
        valueInputOption="USER_ENTERED", body={"values":[row_vals]}).execute()

# ---------- GammaWizard (resilient fetch) ----------
def _gw_timeout(): return _to_int(os.environ.get("GW_TIMEOUT", "30"), 30)

def gw_login_token():
    email = os.environ.get("GW_EMAIL",""); password = os.environ.get("GW_PASSWORD","")
    if not (email and password): raise RuntimeError("GW_LOGIN_MISSING_CREDS")
    r = requests.post(f"{GW_BASE}/goauth/authenticateFireUser", data={"email":email,"password":password}, timeout=_gw_timeout())
    if r.status_code!=200: raise RuntimeError(f"GW_LOGIN_HTTP_{r.status_code}: {r.text[:180]}")
    j=r.json(); t=j.get("token"); 
    if not t: raise RuntimeError(f"GW_LOGIN_NO_TOKEN: {str(j)[:180]}")
    return t

def gw_get_leocross():
    tok = _sanitize_token(os.environ.get("GW_TOKEN","") or "")
    def hit(t):
        h={"Accept":"application/json","Authorization":f"Bearer {_sanitize_token(t)}","User-Agent":"gw-placer/1.0"}
        return requests.get(f"{GW_BASE.rstrip('/')}/{GW_ENDPOINT.lstrip('/')}", headers=h, timeout=_gw_timeout())
    r = hit(tok) if tok else None
    if (r is None) or (r.status_code in (401,403)):
        r = hit(gw_login_token())
    if r.status_code!=200: raise RuntimeError(f"GW_HTTP_{r.status_code}: {r.text[:180]}")
    return r.json()

# ---------- Schwab helpers ----------
def fetch_bid_ask(c, sym_osi: str):
    r=c.get_quote(sym_osi)
    if r.status_code!=200: return (None,None)
    d=list(r.json().values())[0] if isinstance(r.json(), dict) else {}
    q=d.get("quote", d)
    b = q.get("bidPrice") or q.get("bid") or q.get("bidPriceInDouble")
    a = q.get("askPrice") or q.get("ask") or q.get("askPriceInDouble")
    return (float(b) if b is not None else None, float(a) if a is not None else None)

def compute_mid_condor(c, legs):
    bp, sp, sc, bc = legs
    bp_b,bp_a = fetch_bid_ask(c, bp); sp_b,sp_a = fetch_bid_ask(c, sp)
    sc_b,sc_a = fetch_bid_ask(c, sc); bc_b,bc_a = fetch_bid_ask(c, bc)
    if None in (bp_b,bp_a,sp_b,sp_a,sc_b,sc_a,bc_b,bc_a): return None
    net_bid=(sp_b+sc_b)-(bp_a+bc_a)
    net_ask=(sp_a+sc_a)-(bp_b+bc_b)
    return (net_bid+net_ask)/2.0

def get_obp(c, acct_hash: str) -> float:
    url=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}"
    r=c.session.get(url, params={"fields":"positions"})
    if r.status_code!=200: return 0.0
    j=r.json(); 
    if isinstance(j,list): j=j[0]
    sa=j.get("securitiesAccount") or j
    vals=[]
    for sec in ("currentBalances","projectedBalances","initialBalances","balances"):
        b=sa.get(sec,{}) or {}
        for k in ("optionBuyingPower","optionsBuyingPower"):
            v=b.get(k); 
            if isinstance(v,(int,float)): vals.append(float(v))
    for k in ("optionBuyingPower","optionsBuyingPower"):
        v=sa.get(k); 
        if isinstance(v,(int,float)): vals.append(float(v))
    return max(vals) if vals else 0.0

def positions_map_canon(c, acct_hash: str):
    """Return {(ymd,cp,strike8): qty} across SPX/SPXW options."""
    out={}
    url=f"https://api/schwabapi.com/trader/v1/accounts/{acct_hash}"
    r=c.session.get(url, params={"fields":"positions"})
    if r.status_code!=200: return out
    j=r.json(); 
    if isinstance(j,list): j=j[0]
    sa=j.get("securitiesAccount") or j
    for p in (sa.get("positions") or []):
        instr=p.get("instrument",{}) or {}
        if (instr.get("assetType") or "").upper()!="OPTION": continue
        sym=instr.get("symbol","")
        try: osi=to_osi(sym)
        except: continue
        key=osi_canon(osi)
        qty=float(p.get("longQuantity",0)) - float(p.get("shortQuantity",0))
        if abs(qty) < 1e-9: continue
        out[key]=out.get(key,0.0)+qty
    return out

def would_close_guard(legs, pos_map_canon):
    """Return (ok_bool, reason_or_empty)."""
    intended = [
        ("BUY_TO_OPEN",  legs[0]),
        ("SELL_TO_OPEN", legs[1]),
        ("SELL_TO_OPEN", legs[2]),
        ("BUY_TO_OPEN",  legs[3]),
    ]
    for instr, osi in intended:
        key = osi_canon(osi)
        cur = pos_map_canon.get(key, 0.0)
        if instr=="BUY_TO_OPEN"  and cur < 0:  # buying against an open short
            return (False, f"LEG_WOULD_CLOSE {key} cur={cur}")
        if instr=="SELL_TO_OPEN" and cur > 0:  # selling against an open long
            return (False, f"LEG_WOULD_CLOSE {key} cur={cur}")
    return (True, "")

def open_orders_for_legs(c, acct_hash: str, legs_set):
    url=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders"
    r=c.session.get(url)
    if r.status_code!=200: return []
    out=[]
    for o in (r.json() or []):
        st=str(o.get("status") or "").upper()
        if st not in ("WORKING","QUEUED","PENDING_ACTIVATION","OPEN"): 
            continue
        got=set()
        for leg in (o.get("orderLegCollection") or []):
            sym=(leg.get("instrument",{}) or {}).get("symbol","")
            if not sym: continue
            try: got.add(to_osi(sym))
            except: pass
        if got and got==legs_set:
            out.append(str(o.get("orderId") or ""))
    return out

def cancel_order(c, acct_hash: str, oid: str):
    url = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{oid}"
    r = c.session.delete(url)
    print(f"CANCEL_HTTP {r.status_code} OID={oid}")

def cancel_all_matching(c, acct_hash: str, legs_set):
    for oid in open_orders_for_legs(c, acct_hash, legs_set):
        cancel_order(c, acct_hash, oid)

# ---------- time gate ----------
def time_gate_ok() -> bool:
    now = datetime.now(ET)
    if now.weekday()>=5: return False
    return (now.hour==16 and 8 <= now.minute <= 14)

# ---------- main ----------
def main():
    MODE = (os.environ.get("PLACER_MODE","SCHEDULED") or "SCHEDULED").upper()
    source = f"SIMPLE_{MODE}"

    # --- Secrets
    sheet_id = os.environ["GSHEET_ID"]
    sa_json  = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]

    app_key    = os.environ["SCHWAB_APP_KEY"]
    app_secret = os.environ["SCHWAB_APP_SECRET"]
    token_json = os.environ["SCHWAB_TOKEN_JSON"]

    with open("schwab_token.json","w") as f: f.write(token_json)
    c = client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")

    # Account hash
    r=c.get_account_numbers(); r.raise_for_status()
    acct_hash = r.json()[0]["hashValue"]

    # Last SPX for log
    def spx_last():
        for sym in ["$SPX.X","SPX","SPX.X","$SPX"]:
            try:
                q=c.get_quote(sym)
                if q.status_code==200 and sym in q.json():
                    last=q.json()[sym].get("quote",{}).get("lastPrice")
                    if last is not None: return last
            except: pass
        return ""
    last_px = spx_last()

    # Sheets client
    creds = service_account.Credentials.from_service_account_info(json.loads(sa_json),
            scopes=["https://www.googleapis.com/auth/spreadsheets"])
    svc = gbuild("sheets","v4",credentials=creds)
    sheet_id_num = ensure_header_and_get_sheetid(svc, sheet_id, SHEET_TAB, SCHWAB_HEADERS)

    # Mode gate
    if MODE=="SCHEDULED" and not time_gate_ok():
        row = [datetime.utcnow().isoformat()+"Z", source, "SPX", last_px,
               "", "SKIP", "", "", "", "",
               "","","","", "", "SKIPPED_TIME_WINDOW"]
        one_log(svc, sheet_id_num, sheet_id, SHEET_TAB, row)
        print("Scheduled window not met → skipped"); sys.exit(0)

    # LeoCross
    try:
        api = gw_get_leocross()
    except Exception as e:
        row = [datetime.utcnow().isoformat()+"Z", source, "SPX", last_px,
               "", "ABORT", "", "", "", "",
               "","","","", "", f"ABORT_GW: {str(e)[:180]}"]
        one_log(svc, sheet_id_num, sheet_id, SHEET_TAB, row); print(str(e)); sys.exit(0)

    # Extract trade
    def extract(j):
        if isinstance(j, dict):
            if "Trade" in j:
                tr=j["Trade"]
                return tr[-1] if isinstance(tr, list) and tr else tr if isinstance(tr, dict) else {}
            keys=("Date","TDate","Limit","CLimit","Cat1","Cat2")
            if any(k in j for k in keys): return j
            for v in j.values():
                if isinstance(v,(dict,list)):
                    t=extract(v); 
                    if t: return t
        if isinstance(j, list):
            for item in reversed(j):
                t=extract(item)
                if t: return t
        return {}
    trade = extract(api)
    if not trade:
        row = [datetime.utcnow().isoformat()+"Z", source, "SPX", last_px,
               "", "ABORT", "", "", "", "", "","","","", "", "NO_TRADE_PAYLOAD"]
        one_log(svc, sheet_id_num, sheet_id, SHEET_TAB, row); sys.exit(0)

    sig_date = str(trade.get("Date",""))
    exp_iso  = str(trade.get("TDate",""))
    exp6 = yymmdd(exp_iso)
    inner_put  = _to_int(float(trade.get("Limit")), 0)
    inner_call = _to_int(float(trade.get("CLimit")), 0)

    # Build legs (width=5), orient for side by Cat rule (tie→credit)
    def fnum(x):
        try: return float(x)
        except: return None
    cat1=fnum(trade.get("Cat1")); cat2=fnum(trade.get("Cat2"))
    is_credit = True if (cat2 is None or cat1 is None or cat2 >= cat1) else False

    p_low, p_high = inner_put - WIDTH, inner_put
    c_low, c_high = inner_call, inner_call + WIDTH
    bp = to_osi(f".SPXW{exp6}P{p_low}")
    sp = to_osi(f".SPXW{exp6}P{p_high}")
    sc = to_osi(f".SPXW{exp6}C{c_low}")
    bc = to_osi(f".SPXW{exp6}C{c_high}")

    # Orient
    def orient(bp,sp,sc,bc):
        bpS=strike_from_osi(bp); spS=strike_from_osi(sp)
        scS=strike_from_osi(sc); bcS=strike_from_osi(bc)
        if is_credit:
            if bpS > spS: bp,sp = sp,bp
            if scS > bcS: sc,bc = bc,sc
        else:
            if bpS < spS: bp,sp = sp,bp
            if bcS > scS: sc,bc = bc,sc
        return [bp,sp,sc,bc]
    legs = orient(bp,sp,sc,bc)
    legs_set = set(legs)

    # NO CLOSE guard (root-agnostic)
    pos_map = positions_map_canon(c, acct_hash)
    ok, reason = would_close_guard(legs, pos_map)
    if not ok:
        row = [datetime.utcnow().isoformat()+"Z", source, "SPX", last_px,
               sig_date, "ABORT", ("SHORT_IRON_CONDOR" if is_credit else "LONG_IRON_CONDOR"),
               "", ("NET_CREDIT" if is_credit else "NET_DEBIT"), "",
               legs[0], legs[1], legs[2], legs[3],
               "", f"ABORT_{reason}"]
        one_log(svc, sheet_id_num, sheet_id, SHEET_TAB, row); print(reason); sys.exit(0)

    # Qty (OBP)
    obp = get_obp(c, acct_hash)
    qty = max(1, obp // PER_UNIT)
    qty = int(qty)

    side_name  = "SHORT_IRON_CONDOR" if is_credit else "LONG_IRON_CONDOR"
    order_type = "NET_CREDIT" if is_credit else "NET_DEBIT"

    # Order builders
    def build_order(price: float):
        return {
            "orderType": order_type,
            "session": "NORMAL",
            "price": f"{price:.2f}",
            "duration": "DAY",
            "orderStrategyType": "SINGLE",
            "complexOrderStrategyType": "IRON_CONDOR",
            "orderLegCollection":[
                {"instruction":"BUY_TO_OPEN","quantity":qty,"instrument":{"symbol":legs[0],"assetType":"OPTION"}},
                {"instruction":"SELL_TO_OPEN","quantity":qty,"instrument":{"symbol":legs[1],"assetType":"OPTION"}},
                {"instruction":"SELL_TO_OPEN","quantity":qty,"instrument":{"symbol":legs[2],"assetType":"OPTION"}},
                {"instruction":"BUY_TO_OPEN","quantity":qty,"instrument":{"symbol":legs[3],"assetType":"OPTION"}},
            ]
        }

    def place(price):
        cancel_all_matching(c, acct_hash, legs_set)    # hard ensure single order
        r = c.place_order(acct_hash, build_order(price))
        oid = ""
        try:
            j=r.json(); oid = str(j.get("orderId") or j.get("order_id") or "")
        except:
            oid = r.headers.get("Location","").rstrip("/").split("/")[-1]
        print(f"PLACE {order_type} @ {price:.2f}  OID={oid} HTTP={r.status_code}")
        return oid

    def cancel_wait_all():
        ids = open_orders_for_legs(c, acct_hash, legs_set)
        for oid in ids: cancel_order(c, acct_hash, oid)

    def status_of(oid: str):
        url=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{oid}"
        r=c.session.get(url)
        try: j=r.json() if r.status_code==200 else {}
        except: j={}
        return str(j.get("status") or j.get("orderStatus") or "")

    def wait_or_filled(seconds: int, oid_local: str):
        end = time.time() + seconds
        last=""
        while time.time() < end:
            last = status_of(oid_local).upper()
            if last=="FILLED": return True,last
            time.sleep(1)
        return False,last

    # Ladder loop
    path=[]; used_price=None; oid=""; filled=False; st=""
    start_ts = time.time()
    deadline = start_ts + TIMEBOX_SEC
    start_price = clamp_tick(CREDIT_START if is_credit else DEBIT_START)
    bound_price = CREDIT_FLOOR if is_credit else DEBIT_CEIL
    step_sign = -0.05 if is_credit else +0.05

    while time.time() < deadline and not filled:
        # Step 0: start
        used_price = start_price
        oid = place(used_price); path.append(f"{used_price:.2f}")
        filled, st = wait_or_filled(STEP_WAIT, oid)
        if filled or time.time() >= deadline: break

        # Step 1: mid
        cancel_wait_all()
        mid1 = compute_mid_condor(c, legs)
        used_price = clamp_tick(mid1 if mid1 is not None else start_price)
        oid = place(used_price); path.append(f"{used_price:.2f}")
        filled, st = wait_or_filled(STEP_WAIT, oid)
        if filled or time.time() >= deadline: break

        # Step 2: new mid
        cancel_wait_all()
        mid2 = compute_mid_condor(c, legs)
        used_price = clamp_tick(mid2 if mid2 is not None else used_price)
        oid = place(used_price); path.append(f"{used_price:.2f}")
        filled, st = wait_or_filled(STEP_WAIT, oid)
        if filled or time.time() >= deadline: break

        # Step 3: previous mid ± 0.05
        cancel_wait_all()
        prev_mid = mid2 if mid2 is not None else mid1
        step_px = clamp_tick((prev_mid + step_sign) if prev_mid is not None else used_price + step_sign)
        if is_credit:
            step_px = max(CREDIT_FLOOR, min(CREDIT_START, step_px))
        else:
            step_px = max(DEBIT_START, min(DEBIT_CEIL, step_px))
        used_price = step_px
        oid = place(used_price); path.append(f"{used_price:.2f}")
        filled, st = wait_or_filled(STEP_WAIT, oid)
        if filled or time.time() >= deadline: break

        # Step 4: bound
        cancel_wait_all()
        used_price = clamp_tick(bound_price)
        oid = place(used_price); path.append(f"{used_price:.2f}")
        filled, st = wait_or_filled(STEP_WAIT, oid)
        if filled or time.time() >= deadline: break

        # after bound wait → CANCEL and restart
        cancel_wait_all()
        path.append("CXL")

    # Final one-row log
    trace = "STEPS " + "→".join(path)
    status_txt = ("FILLED " + trace) if filled else ((st or "WORKING") + " " + trace)
    row = [datetime.utcnow().isoformat()+"Z", source, "SPX", last_px,
           sig_date, "PLACE", ("SHORT_IRON_CONDOR" if is_credit else "LONG_IRON_CONDOR"),
           qty, ("NET_CREDIT" if is_credit else "NET_DEBIT"),
           ("" if used_price is None else f"{used_price:.2f}"),
           legs[0], legs[1], legs[2], legs[3],
           oid, status_txt + f" | OBP={get_obp(c, acct_hash):.2f} PER={PER_UNIT} Q={qty}"]
    one_log(svc, sheet_id_num, sheet_id, SHEET_TAB, row)
    print(f"FINAL {status_txt} OID={oid} PRICE_USED={used_price if used_price is not None else 'NA'}")

if __name__ == "__main__":
    main()
