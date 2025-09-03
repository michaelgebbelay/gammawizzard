# Simple LeoCross → Schwab placer (LIVE ONLY) with mid-tracking ladder & OBP sizing
# Modes: PLACER_MODE = NOW | SCHEDULED | OFF
# - NOW        → place immediately (ignores time window)
# - SCHEDULED  → only place in 16:08–16:14 ET window (Mon–Fri)
# - OFF        → skip cleanly (still logs one row)
#
# Ladder logic (30s steps; poll 1s):
#   CREDIT: start(2.10) → mid → new_mid → (prev_mid - 0.05) → 1.90 → wait 30s → CANCEL → restart sequence
#   DEBIT:  start(1.90) → mid → new_mid → (prev_mid + 0.05) → 2.10 → wait 30s → CANCEL → restart sequence
# Timebox: default 180s (≈ 2.5–3.0 min). Stops when FILLED or timebox hit.
#
# Sizing: **Option Buying Power** only (optionBuyingPower/optionsBuyingPower), qty = floor(OBP / PER_UNIT).
# Defaults: PER_UNIT=5000, SIZE_SOURCE=OBP. (This yields 1 lot per $5k OBP.)
#
# One Google Sheet write at the end with a step trace and sizing info in the status field.
#
# Required env (secrets):
#   SCHWAB_APP_KEY, SCHWAB_APP_SECRET, SCHWAB_TOKEN_JSON
#   GSHEET_ID, GOOGLE_SERVICE_ACCOUNT_JSON
#   (GammaWizard) GW_EMAIL, GW_PASSWORD  (optional GW_TOKEN too)
#
# Key options (env):
#   PLACER_MODE=NOW|SCHEDULED|OFF        (default OFF)
#   SIZE_SOURCE=OBP|CASH|BP_MIN|BP_MAX   (default OBP; OBP is recommended per your rule)
#   PER_UNIT=5000                        (dollars per contract)
#   CASH_RESERVE=0                       (ignored for OBP but available if you switch to CASH)
#   QTY_OVERRIDE=0                       (force exact qty if >0)
#   OPT_BP_OVERRIDE=-1                   (testing: pretend base dollars; still LIVE otherwise)
#   LADDER_SEC=30, TIMEBOX_SEC=180
#   CREDIT_START=2.10, CREDIT_FLOOR=1.90
#   DEBIT_START=1.90,  DEBIT_CEIL=2.10
#   GW_TOKEN=..., GW_FORCE_LOGIN=1, GW_BASE, GW_ENDPOINT, GW_TIMEOUT
import os, sys, json, time, math, re
from datetime import datetime, date
from zoneinfo import ZoneInfo
import requests
from schwab.auth import client_from_token_file
from google.oauth2 import service_account
from googleapiclient.discovery import build as gbuild

# ---- helpers for safe casts
def _to_int(s, d):  # int with default
    try: return int(s)
    except: return d
def _to_float(s, d):  # float with default
    try: return float(s)
    except: return d

# ---- constants / defaults
TICK = 0.05
WIDTH = 5
ET = ZoneInfo("America/New_York")
MAX_QTY = 500

# Starts / bounds
CREDIT_START = _to_float(os.environ.get("CREDIT_START", "2.10"), 2.10)
DEBIT_START  = _to_float(os.environ.get("DEBIT_START",  "1.90"), 1.90)
CREDIT_FLOOR = _to_float(os.environ.get("CREDIT_FLOOR", "1.90"), 1.90)
DEBIT_CEIL   = _to_float(os.environ.get("DEBIT_CEIL",   "2.10"), 2.10)

# Ladder pacing / timebox
LADDER_SEC  = _to_int(os.environ.get("LADDER_SEC",  "30"), 30)
TIMEBOX_SEC = _to_int(os.environ.get("TIMEBOX_SEC", "180"), 180)  # ~2.5–3.0 min

GW_BASE = os.environ.get("GW_BASE", "https://gandalf.gammawizard.com")
GW_ENDPOINT = os.environ.get("GW_ENDPOINT", "/rapi/GetLeoCross")

SHEET_ID  = os.environ["GSHEET_ID"]
SA_JSON   = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
SHEET_TAB = os.environ.get("SHEET_TAB", "schwab")  # matches your tab

# Sizing options (default OBP per your rule)
SIZE_SOURCE     = (os.environ.get("SIZE_SOURCE", "OBP") or "OBP").upper()  # OBP|CASH|BP_MIN|BP_MAX
PER_UNIT        = _to_int(os.environ.get("PER_UNIT", "5000"), 5000)
CASH_RESERVE    = _to_float(os.environ.get("CASH_RESERVE", "0"), 0.0)
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
    return _to_int(os.environ.get("GW_TIMEOUT", "30"), 30)

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

def sizing_base_usd(c, acct_hash: str, src: str) -> float:
    """Return dollars to size against. OBP uses ONLY optionBuyingPower/optionsBuyingPower."""
    url=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}"
    r=c.session.get(url, params={"fields":"positions"})
    if r.status_code!=200: return 0.0
    j=r.json()
    if isinstance(j,list): j=j[0]
    sa=j.get("securitiesAccount") or j

    sections=("currentBalances","projectedBalances","initialBalances","balances")

    def gather(keys):
        vals=[]
        for sec in sections:
            b = sa.get(sec, {}) or {}
            for k in keys:
                v = b.get(k)
                if isinstance(v,(int,float)): vals.append(float(v))
        # ALSO check top-level mirrors
        for k in keys:
            v = sa.get(k)
            if isinstance(v,(int,float)): vals.append(float(v))
        return vals

    src = (src or "OBP").upper()
    if src == "OBP":
        vals = gather(("optionBuyingPower","optionsBuyingPower"))
        return max(vals) if vals else 0.0
    if src == "CASH":
        vals = gather(("cashAvailableForTradingSettled","cashAvailableForTrading","availableFunds"))
        return max(vals) if vals else 0.0
    if src == "BP_MIN":
        vals = gather(("cashAvailableForTradingSettled","cashAvailableForTrading","availableFunds","optionBuyingPower","optionsBuyingPower"))
        return min(vals) if vals else 0.0
    if src == "BP_MAX":
        vals = gather(("cashAvailableForTradingSettled","cashAvailableForTrading","availableFunds","optionBuyingPower","optionsBuyingPower"))
        return max(vals) if vals else 0.0
    # default OBP
    vals = gather(("optionBuyingPower","optionsBuyingPower"))
    return max(vals) if vals else 0.0

def qty_from_base(base: float) -> int:
    # reserve only applies for CASH sizing; kept generic for completeness
    base_eff = max(0.0, base - max(0.0, CASH_RESERVE))
    q = max(1, int(base_eff // max(1, PER_UNIT)))
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

    # Qty = floor( OBP / PER_UNIT ); override takes precedence
    if OPT_BP_OVERRIDE > 0:
        base_dollars = OPT_BP_OVERRIDE
        base_src = "OVERRIDE"
    else:
        base_dollars = sizing_base_usd(c, acct_hash, SIZE_SOURCE)
        base_src = SIZE_SOURCE
    qty = QTY_OVERRIDE if QTY_OVERRIDE > 0 else qty_from_base(base_dollars)
    qty = max(1, qty)
    order_type = "NET_CREDIT" if is_credit else "NET_DEBIT"
    side_name  = "SHORT_IRON_CONDOR" if is_credit else "LONG_IRON_CONDOR"

    # Helpers to talk to Orders API
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
        oid = ""
        try:
            j=r.json(); oid = str(j.get("orderId") or j.get("order_id") or "")
        except:
            oid = r.headers.get("Location","").rstrip("/").split("/")[-1]
        print(f"PLACE {order_type} @ {price:.2f}  OID={oid} HTTP={r.status_code}")
        return oid

    def replace_order(oid: str, price: float):
        url = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{oid}"
        r = c.session.put(url, json=build_order(price))
        new_id = oid
        try:
            j=r.json(); new_id=str(j.get("orderId") or j.get("order_id") or oid)
        except:
            loc=r.headers.get("Location","")
            if loc:
                new_id = loc.rstrip("/").split("/")[-1] or oid
        print(f"REPLACE → {price:.2f} HTTP={r.status_code} NEW_ID={new_id}")
        return new_id

    def cancel_order(oid: str):
        url = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{oid}"
        r = c.session.delete(url)
        print(f"CANCEL_HTTP {r.status_code} OID={oid}")
        return r.status_code

    def status(oid: str):
        url=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{oid}"
        r=c.session.get(url)
        try:
            j=r.json() if r.status_code==200 else {}
        except:
            j={}
        return str(j.get("status") or j.get("orderStatus") or "")

    # Ladder sequence per your spec (timeboxed)
    start_price = clamp_tick(CREDIT_START if is_credit else DEBIT_START)
    bound_price = CREDIT_FLOOR if is_credit else DEBIT_CEIL
    step_sign   = -0.05 if is_credit else +0.05

    start_ts = time.time()
    deadline = start_ts + TIMEBOX_SEC
    path = []  # step trace across cancels
    used_price = None
    oid = None
    filled = False
    st = ""

    def wait_poll(seconds: int, oid_local: str) -> tuple[bool,str]:
        end = time.time() + seconds
        last = ""
        while time.time() < end:
            last = status(oid_local).upper()
            if last == "FILLED":
                return True, last
            time.sleep(1)
        return False, last

    while time.time() < deadline and not filled:
        # Step 0: start price
        cur_mid = compute_mid_condor(c, legs)
        used_price = start_price
        oid = place(used_price); path.append(f"{used_price:.2f}")
        filled, st = wait_poll(LADDER_SEC, oid)
        if filled or time.time() >= deadline: break

        # Step 1: mid
        cur_mid = compute_mid_condor(c, legs)
        if cur_mid is not None:
            used_price = clamp_tick(cur_mid)
            oid = replace_order(oid, used_price); path.append(f"{used_price:.2f}")
            filled, st = wait_poll(LADDER_SEC, oid)
            if filled or time.time() >= deadline: break

        # Step 2: new mid
        cur_mid2 = compute_mid_condor(c, legs)
        if cur_mid2 is not None:
            used_price = clamp_tick(cur_mid2)
            oid = replace_order(oid, used_price); path.append(f"{used_price:.2f}")
            filled, st = wait_poll(LADDER_SEC, oid)
            if filled or time.time() >= deadline: break

        # Step 3: previous mid ± 0.05
        prev_mid = cur_mid2 if cur_mid2 is not None else cur_mid
        if prev_mid is not None:
            used_price = clamp_tick(prev_mid + step_sign)
            # bound toward floor/ceil
            if is_credit:
                used_price = max(CREDIT_FLOOR, min(CREDIT_START, used_price))
            else:
                used_price = max(DEBIT_START, min(DEBIT_CEIL, used_price))
            oid = replace_order(oid, used_price); path.append(f"{used_price:.2f}")
            filled, st = wait_poll(LADDER_SEC, oid)
            if filled or time.time() >= deadline: break

        # Step 4: final bound (1.90 for credit, 2.10 for debit)
        used_price = clamp_tick(bound_price)
        oid = replace_order(oid, used_price); path.append(f"{used_price:.2f}")
        filled, st = wait_poll(LADDER_SEC, oid)
        if filled or time.time() >= deadline: break

        # Not filled after bound step → CANCEL and restart (if time remains)
        cancel_order(oid)
        path.append("CXL")
        # loop restarts; new OID will be placed at step 0

    # Final log (single write)
    trace = "STEPS " + "→".join(path)
    size_note = f" | SIZE {base_src}={base_dollars:.2f} PER={PER_UNIT} Q={qty}"
    final_status = (("FILLED " + trace + size_note) if filled
                    else ((st or "WORKING") + " " + trace + size_note))
    row = [datetime.utcnow().isoformat()+"Z", source, "SPX", last_px,
           sig_date, "PLACE", side_name,
           qty, order_type, ("" if used_price is None else f"{used_price:.2f}"),
           legs[0], legs[1], legs[2], legs[3],
           (oid or ""), final_status]
    one_log(svc, sheet_id_num, row)
    print(f"FINAL {final_status} OID={oid} PRICE_USED={used_price if used_price is not None else 'NA'}")

if __name__ == "__main__":
    main()
