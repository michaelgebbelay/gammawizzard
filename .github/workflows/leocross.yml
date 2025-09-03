# Simple LeoCross → Schwab placer (LIVE ONLY)
# Modes: PLACER_MODE = NOW | SCHEDULED | OFF
# - NOW        → place immediately (ignores time window)
# - SCHEDULED  → only place in 16:08–16:14 ET window (Mon–Fri)
# - OFF        → skip cleanly (still logs one row)
#
# Behavior:
# - Pulls LeoCross from GammaWizard (resilient: token or login; retries on 401/403).
# - Builds 5‑wide SPXW iron condor (inner strikes from Limit/CLimit).
# - Credit: 2.10 → mid → mid-0.05    Debit: 1.90 → mid → mid+0.05
# - Qty: 1 contract per $5k option BP (override with QTY_OVERRIDE).
# - No‑close guard: if any leg would offset an existing SPX/SPXW position → ABORT (no order).
# - Single Google Sheet write per run (top-insert append).
#
# Required env (secrets):
#   SCHWAB_APP_KEY, SCHWAB_APP_SECRET, SCHWAB_TOKEN_JSON
#   GSHEET_ID, GOOGLE_SERVICE_ACCOUNT_JSON
#   (GammaWizard) GW_EMAIL, GW_PASSWORD  (optional GW_TOKEN too)
#
# Optional env:
#   PLACER_MODE=NOW|SCHEDULED|OFF   (default OFF)
#   QTY_OVERRIDE=1                  (force lot size)
#   OPT_BP_OVERRIDE=5000            (simulate BP for sizing, still live)
#   GW_TOKEN=...                    (used first unless GW_FORCE_LOGIN=1)
#   GW_FORCE_LOGIN=1                (ignore GW_TOKEN, always login)
#   GW_BASE=https://gandalf.gammawizard.com
#   GW_ENDPOINT=/rapi/GetLeoCross
#   GW_TIMEOUT=30                   (seconds)

import os, sys, json, time, math, re
from datetime import datetime, date
from zoneinfo import ZoneInfo
import requests
from schwab.auth import client_from_token_file
from google.oauth2 import service_account
from googleapiclient.discovery import build as gbuild

# ---- constants / defaults
TICK = 0.05
WIDTH = 5                # 5-point wings
CREDIT_START = 2.10
DEBIT_START  = 1.90
STAGE_SEC = 12           # ~36s total ladder
MAX_QTY = 500
ET = ZoneInfo("America/New_York")

GW_BASE = os.environ.get("GW_BASE", "https://gandalf.gammawizard.com")
GW_ENDPOINT = os.environ.get("GW_ENDPOINT", "/rapi/GetLeoCross")

SHEET_ID  = os.environ["GSHEET_ID"]
SA_JSON   = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
SHEET_TAB = os.environ.get("SHEET_TAB", "schwab")  # matches your tab

# LIVE‑order sizing helpers
def _to_int(s, d=0):
    try: return int(s)
    except: return d
def _to_float(s, d=-1.0):
    try: return float(s)
    except: return d

QTY_OVERRIDE    = _to_int(os.environ.get("QTY_OVERRIDE", "0"), 0)
OPT_BP_OVERRIDE = _to_float(os.environ.get("OPT_BP_OVERRIDE", "-1"), -1.0)

# Header matches your 'schwab' tab format
SCHWAB_HEADERS = [
    "ts","source","symbol","last_price",
    "signal_date","order_mode","side","qty_exec","order_type","limit_price",
    "occ_buy_put","occ_sell_put","occ_sell_call","occ_buy_call",
    "order_id","status"
]

# ---- small utils
def clamp_tick(x: float) -> float:
    return round(round(x / TICK) * TICK + 1e-12, 2)

def _sanitize_token(t: str) -> str:
    t = (t or "").strip().strip('"').strip("'")
    return t.split(None,1)[1] if t.lower().startswith("bearer ") else t

def yymmdd(iso: str) -> str:
    d = date.fromisoformat((iso or "")[:10])
    return f"{d:%y%m%d}"

def to_schwab_opt(sym: str) -> str:
    raw = (sym or "").strip().upper()
    if raw.startswith("."): raw=raw[1:]
    raw = raw.replace("_","")
    m = re.match(r'^([A-Z.$^]{1,6})(\d{6})([CP])(\d{1,5})(?:\.(\d{1,3}))?$', raw)
    if not m:
        m = re.match(r'^([A-Z.$^]{1,6})(\d{6})([CP])(\d{8})$', raw)
    if not m:
        raise ValueError(f"Cannot parse option symbol: {sym}")
    root, ymd, cp, strike, frac = (m.groups()+("",))[:5]
    if len(strike)==8 and not frac:
        mills=int(strike)
    else:
        mills = int(strike)*1000 + (int((frac or "0").ljust(3,'0')) if frac else 0)
    return f"{root:<6}{ymd}{cp}{mills:08d}"

def strike_from_osi(osi: str) -> float:
    return int(osi[-8:]) / 1000.0

# ---- Google Sheets (single log row)
def ensure_header_and_get_sheetid(svc, spreadsheet_id: str, tab: str, header: list):
    meta = svc.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheet_id_num = None
    for sh in meta["sheets"]:
        if sh["properties"]["title"] == tab:
            sheet_id_num = sh["properties"]["sheetId"]
            break
    if sheet_id_num is None:
        svc.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests":[{"addSheet":{"properties":{"title":tab}}}]}
        ).execute()
        meta = svc.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheet_id_num = next(sh["properties"]["sheetId"] for sh in meta["sheets"] if sh["properties"]["title"]==tab)
        svc.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id, range=f"{tab}!1:1",
            valueInputOption="USER_ENTERED", body={"values":[header]}
        ).execute()
    else:
        got = svc.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=f"{tab}!1:1").execute().get("values",[])
        if not got:
            svc.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id, range=f"{tab}!1:1",
                valueInputOption="USER_ENTERED", body={"values":[header]}
            ).execute()
    return sheet_id_num

def top_insert(svc, spreadsheet_id: str, sheet_id_num: int):
    svc.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests":[{"insertDimension":{
            "range":{"sheetId":sheet_id_num, "dimension":"ROWS", "startIndex":1, "endIndex":2},
            "inheritFromBefore": False
        }}]}
    ).execute()

def one_log(svc, sheet_id_num, row_vals: list):
    top_insert(svc, SHEET_ID, sheet_id_num)
    svc.spreadsheets().values().update(
        spreadsheetId=SHEET_ID, range=f"{SHEET_TAB}!A2",
        valueInputOption="USER_ENTERED", body={"values":[row_vals]}
    ).execute()

# ---- GammaWizard (resilient)
def _gw_timeout():
    try: return int(os.environ.get("GW_TIMEOUT", "30"))
    except: return 30

def gw_login_token() -> str:
    email = os.environ.get("GW_EMAIL", "")
    password = os.environ.get("GW_PASSWORD", "")
    if not (email and password):
        raise RuntimeError("GW_LOGIN_MISSING_CREDS")
    r = requests.post(f"{GW_BASE}/goauth/authenticateFireUser",
                      data={"email":email,"password":password}, timeout=_gw_timeout())
    if r.status_code != 200:
        raise RuntimeError(f"GW_LOGIN_HTTP_{r.status_code}: {r.text[:180]}")
    j=r.json(); t=j.get("token")
    if not t:
        raise RuntimeError(f"GW_LOGIN_NO_TOKEN: {str(j)[:180]}")
    return t

def gw_get_leocross_resilient() -> dict:
    # Prefer GW_TOKEN unless GW_FORCE_LOGIN=1
    use_login_first = os.environ.get("GW_FORCE_LOGIN","").lower() in ("1","true","yes")
    token = None
    if not use_login_first:
        token = _sanitize_token(os.environ.get("GW_TOKEN","") or "")
        if not token:
            use_login_first = True
    if use_login_first:
        token = gw_login_token()

    def _hit(tok: str):
        hdr = {
            "Accept":"application/json",
            "Authorization": f"Bearer {_sanitize_token(tok)}",
            "User-Agent": "gw-placer/1.0"
        }
        url = f"{GW_BASE.rstrip('/')}/{GW_ENDPOINT.lstrip('/')}"
        return requests.get(url, headers=hdr, timeout=_gw_timeout())

    r = _hit(token)
    if r.status_code in (401, 403):
        # fresh login once, then retry
        fresh = gw_login_token()
        r = _hit(fresh)
    if r.status_code != 200:
        raise RuntimeError(f"GW_HTTP_{r.status_code}: {r.text[:180]}")
    return r.json()

# ---- Schwab helpers
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

def buying_power_usd(c, acct_hash: str) -> float:
    url=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}"
    r=c.session.get(url, params={"fields":"positions"})
    if r.status_code!=200: return 0.0
    j=r.json()
    if isinstance(j,list): j=j[0]
    sa=j.get("securitiesAccount") or j
    cands=[]
    for section in ("currentBalances","projectedBalances","initialBalances","balances"):
        b=sa.get(section,{})
        for k in ("optionBuyingPower","optionsBuyingPower","buyingPower",
                  "availableFunds","cashAvailableForTrading",
                  "cashAvailableForTradingSettled"):
            v=b.get(k)
            if isinstance(v,(int,float)): cands.append(float(v))
    for k in ("optionBuyingPower","optionsBuyingPower","buyingPower"):
        v=sa.get(k)
        if isinstance(v,(int,float)): cands.append(float(v))
    return max(cands) if cands else 0.0

def qty_from_bp(bp: float) -> int:
    q = max(1, int(bp // 5000))
    return min(q, MAX_QTY)

def list_spx_option_positions(c, acct_hash: str):
    out={}
    try:
        url=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}"
        r=c.session.get(url, params={"fields":"positions"})
        if r.status_code!=200: return out
        j=r.json()
        if isinstance(j, list): j=j[0]
        sa=j.get("securitiesAccount") or j
        positions=sa.get("positions",[]) or []
        for p in positions:
            instr=p.get("instrument",{}) or {}
            if (instr.get("assetType") or "").upper()!="OPTION": continue
            sym=instr.get("symbol","")
            try:
                osi=to_schwab_opt(sym)
            except:
                continue
            if not osi.startswith("SPX"):  # SPX + SPXW
                continue
            qty=float(p.get("longQuantity",0)) - float(p.get("shortQuantity",0))
            if abs(qty) < 1e-9: continue
            out[osi]=out.get(osi,0.0)+qty
    except:
        pass
    return out

# ---- time gate (for SCHEDULED)
def time_gate_ok() -> bool:
    now = datetime.now(ET)
    if now.weekday() >= 5:   # Sat/Sun
        return False
    # Accept 16:08–16:14 ET window
    return (now.hour == 16 and 8 <= now.minute <= 14)

# ---- main
def main():
    # Mode: NOW | SCHEDULED | OFF
    MODE = os.environ.get("PLACER_MODE","OFF").strip().upper()
    if MODE not in ("NOW","SCHEDULED","OFF"):
        MODE = "OFF"
    source = f"SIMPLE_{MODE}"

    # Schwab auth
    app_key    = os.environ["SCHWAB_APP_KEY"]
    app_secret = os.environ["SCHWAB_APP_SECRET"]
    token_json = os.environ["SCHWAB_TOKEN_JSON"]
    with open("schwab_token.json","w") as f: f.write(token_json)
    c = client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")

    # Account hash
    r=c.get_account_numbers(); r.raise_for_status()
    acct_hash = r.json()[0]["hashValue"]

    # For log: last SPX
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
    creds = service_account.Credentials.from_service_account_info(json.loads(SA_JSON),
            scopes=["https://www.googleapis.com/auth/spreadsheets"])
    svc = gbuild("sheets","v4",credentials=creds)
    sheet_id_num = ensure_header_and_get_sheetid(svc, SHEET_ID, SHEET_TAB, SCHWAB_HEADERS)

    # Mode gating (log once and exit when OFF or outside window)
    if MODE == "OFF":
        row = [datetime.utcnow().isoformat()+"Z", source, "SPX", last_px,
               "", "SKIP", "", "", "", "",
               "","","","", "", "SKIP_OFF"]
        one_log(svc, sheet_id_num, row); print("Mode=OFF → skipped"); sys.exit(0)

    if MODE == "SCHEDULED" and not time_gate_ok():
        row = [datetime.utcnow().isoformat()+"Z", source, "SPX", last_px,
               "", "SKIP", "", "", "", "",
               "","","","", "", "SKIPPED_TIME_WINDOW"]
        one_log(svc, sheet_id_num, row); print("Scheduled window not met → skipped"); sys.exit(0)

    # LeoCross fetch (resilient; clean abort on failure)
    try:
        api = gw_get_leocross_resilient()
    except Exception as e:
        row = [datetime.utcnow().isoformat()+"Z", source, "SPX", last_px,
               "", "ABORT", "", "", "", "",
               "","","","", "", f"ABORT_GW: {str(e)[:180]}"]
        one_log(svc, sheet_id_num, row)
        print(f"ABORT_GW: {e}")
        sys.exit(0)

    # Extract trade flexibly
    def extract(j):
        if isinstance(j, dict):
            if "Trade" in j:
                tr=j["Trade"]
                return tr[-1] if isinstance(tr, list) and tr else tr if isinstance(tr, dict) else {}
            keys=("Date","TDate","Limit","CLimit","Cat1","Cat2","Put","Call")
            if any(k in j for k in keys): return j
            for v in j.values():
                if isinstance(v,(dict,list)):
                    t=extract(v)
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
        one_log(svc, sheet_id_num, row); print("NO_TRADE_PAYLOAD"); sys.exit(0)

    def fnum(x):
        try: return float(x)
        except: return None

    sig_date = str(trade.get("Date",""))
    exp_iso  = str(trade.get("TDate",""))
    exp6     = yymmdd(exp_iso)
    inner_put  = int(float(trade.get("Limit")))
    inner_call = int(float(trade.get("CLimit")))
    cat1 = fnum(trade.get("Cat1")); cat2 = fnum(trade.get("Cat2"))

    # Decide side (tie → credit)
    is_credit = True
    try:
        if cat1 is not None and cat2 is not None:
            is_credit = (cat2 >= cat1)
    except: pass

    # Build legs (width=5), then orient for side
    p_low, p_high = inner_put - WIDTH, inner_put
    c_low, c_high = inner_call, inner_call + WIDTH
    bp = to_schwab_opt(f".SPXW{exp6}P{p_low}")
    sp = to_schwab_opt(f".SPXW{exp6}P{p_high}")
    sc = to_schwab_opt(f".SPXW{exp6}C{c_low}")
    bc = to_schwab_opt(f".SPXW{exp6}C{c_high}")

    def orient(bp,sp,sc,bc):
        bpS=strike_from_osi(bp); spS=strike_from_osi(sp)
        scS=strike_from_osi(sc); bcS=strike_from_osi(bc)
        if is_credit:
            if bpS > spS: bp,sp = sp,bp
            if scS > bcS: sc,bc = bc,sc
        else:
            if bpS < spS: bp,sp = sp,bp
            if bcS > scS: sc,bc = bc,sc
        return bp,sp,sc,bc
    legs = orient(bp,sp,sc,bc)

    # NO‑CLOSE GUARD: abort if any leg would offset existing position
    pos_map = list_spx_option_positions(c, acct_hash)
    intended = [
        ("BUY_TO_OPEN",  legs[0]),
        ("SELL_TO_OPEN", legs[1]),
        ("SELL_TO_OPEN", legs[2]),
        ("BUY_TO_OPEN",  legs[3]),
    ]
    for instr, osi in intended:
        cur = pos_map.get(osi, 0.0)
        if (instr=="BUY_TO_OPEN"  and cur < 0) or (instr=="SELL_TO_OPEN" and cur > 0):
            row = [datetime.utcnow().isoformat()+"Z", source, "SPX", last_px,
                   sig_date, "ABORT", ("SHORT_IRON_CONDOR" if is_credit else "LONG_IRON_CONDOR"),
                   "", ("NET_CREDIT" if is_credit else "NET_DEBIT"), "",
                   legs[0], legs[1], legs[2], legs[3],
                   "", f"ABORT_WOULD_CLOSE {osi} cur={cur}"]
            one_log(svc, sheet_id_num, row)
            print(f"ABORT_WOULD_CLOSE {osi} cur={cur}"); sys.exit(0)

    # Qty = 1 per $5k BP (option to override for small live tests)
    bp_usd = OPT_BP_OVERRIDE if OPT_BP_OVERRIDE > 0 else buying_power_usd(c, acct_hash)
    qty = QTY_OVERRIDE if QTY_OVERRIDE > 0 else qty_from_bp(bp_usd)
    order_type = "NET_CREDIT" if is_credit else "NET_DEBIT"

    # Ladder prices (snap to tick)
    mid = compute_mid_condor(c, legs)
    if is_credit:
        p1 = clamp_tick(CREDIT_START)
        p2 = clamp_tick(mid if mid is not None else CREDIT_START)
        p3 = clamp_tick((mid - 0.05) if mid is not None else CREDIT_START - 0.05)
    else:
        p1 = clamp_tick(DEBIT_START)
        p2 = clamp_tick(mid if mid is not None else DEBIT_START)
        p3 = clamp_tick((mid + 0.05) if mid is not None else DEBIT_START + 0.05)

    # Build/Place/Status
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
        r = c.place_order(acct_hash, build_order(price))
        try:
            j=r.json(); oid = str(j.get("orderId") or j.get("order_id") or "")
        except:
            oid = r.headers.get("Location","").rstrip("/").split("/")[-1]
        print(f"PLACE {order_type} @ {price:.2f}  OID={oid} HTTP={r.status_code}")
        return oid

    def status(oid: str):
        url=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{oid}"
        r=c.session.get(url)
        try:
            j=r.json() if r.status_code==200 else {}
        except:
            j={}
        return str(j.get("status") or j.get("orderStatus") or "")

    # Stage 1
    oid = place(p1)
    end = time.time() + STAGE_SEC
    while time.time() < end:
        if status(oid).upper()=="FILLED":
            row = [datetime.utcnow().isoformat()+"Z", source, "SPX", last_px,
                   sig_date, "PLACE", ("SHORT_IRON_CONDOR" if is_credit else "LONG_IRON_CONDOR"),
                   qty, order_type, f"{p1:.2f}",
                   legs[0], legs[1], legs[2], legs[3],
                   oid, "FILLED_S1"]
            one_log(svc, sheet_id_num, row); print("FILLED @ stage 1"); return
        time.sleep(1)

    # Stage 2
    r = c.session.put(f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{oid}",
                      json=build_order(p2))
    print(f"REPLACE → {p2:.2f} HTTP={r.status_code}")
    end = time.time() + STAGE_SEC
    while time.time() < end:
        if status(oid).upper()=="FILLED":
            row = [datetime.utcnow().isoformat()+"Z", source, "SPX", last_px,
                   sig_date, "PLACE", ("SHORT_IRON_CONDOR" if is_credit else "LONG_IRON_CONDOR"),
                   qty, order_type, f"{p2:.2f}",
                   legs[0], legs[1], legs[2], legs[3],
                   oid, "FILLED_S2"]
            one_log(svc, sheet_id_num, row); print("FILLED @ stage 2"); return
        time.sleep(1)

    # Stage 3
    r = c.session.put(f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{oid}",
                      json=build_order(p3))
    print(f"REPLACE → {p3:.2f} HTTP={r.status_code}")
    end = time.time() + STAGE_SEC
    st = ""
    while time.time() < end:
        st = status(oid).upper()
        if st=="FILLED":
            row = [datetime.utcnow().isoformat()+"Z", source, "SPX", last_px,
                   sig_date, "PLACE", ("SHORT_IRON_CONDOR" if is_credit else "LONG_IRON_CONDOR"),
                   qty, order_type, f"{p3:.2f}",
                   legs[0], legs[1], legs[2], legs[3],
                   oid, "FILLED_S3"]
            one_log(svc, sheet_id_num, row); print("FILLED @ stage 3"); return
        time.sleep(1)

    # Not filled yet → final snapshot
    row = [datetime.utcnow().isoformat()+"Z", source, "SPX", last_px,
           sig_date, "PLACE", ("SHORT_IRON_CONDOR" if is_credit else "LONG_IRON_CONDOR"),
           qty, order_type, f"{p3:.2f}",
           legs[0], legs[1], legs[2], legs[3],
           oid, (st or "WORKING")]
    one_log(svc, sheet_id_num, row)
    print(f"DONE (status={st or 'WORKING'}) OID={oid}")

if __name__ == "__main__":
    main()
