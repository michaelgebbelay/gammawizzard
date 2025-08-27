# scripts/schwab_place_order.py
import os, re, json, sys, time, math, inspect
from datetime import datetime, timezone, date, timedelta

from google.oauth2 import service_account
from googleapiclient.discovery import build

# schwab-py
from schwab.auth import client_from_token_file

# market clock (for expiry check / optional emergency)
import pytz
import pandas_market_calendars as mcal

__VERSION__ = "2025-08-27r"

LEO_TAB    = "leocross"
SCHWAB_TAB = "schwab"

SCHWAB_HEADERS = [
    "ts","source","symbol","last_price",
    "signal_date","order_mode","side","qty_exec","order_type","limit_price",
    "occ_buy_put","occ_sell_put","occ_sell_call","occ_buy_call",
    "order_id","status"
]

# ---------- env helpers ----------
def env_or_die(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        print(f"Missing required env: {name}", file=sys.stderr); sys.exit(1)
    return v
def env_str(name: str, default: str = "") -> str:
    v = os.environ.get(name)
    if v is None or str(v).strip()=="":
        return default
    return str(v).strip()
def env_float(name: str, default: float) -> float:
    s = os.environ.get(name)
    if s is None or str(s).strip()=="":
        return default
    try: return float(s)
    except: return default
def env_int(name: str, default: int) -> int:
    s = os.environ.get(name)
    if s is None or str(s).strip()=="":
        return default
    try: return int(float(s))
    except: return default
def env_bool(name: str, default: bool) -> bool:
    s = os.environ.get(name)
    if s is None: return default
    return str(s).strip().lower() in ("1","true","yes","y","on")

# ---------- time helpers ----------
_ET = pytz.timezone("America/New_York")
def now_et() -> datetime:
    return datetime.now(_ET)

def todays_equity_close_et() -> datetime | None:
    cal = mcal.get_calendar('XNYS')
    d = now_et().date()
    sched = cal.schedule(start_date=d, end_date=d, tz='America/New_York')
    if sched.empty: return None
    return sched['market_close'].iloc[0].to_pydatetime()

def minutes_to_option_close() -> float:
    """Used only for optional emergency rung and logging."""
    close = todays_equity_close_et()
    if close is None: return 1e9
    opt_close = close + timedelta(minutes=15)
    return (opt_close - now_et()).total_seconds() / 60.0

# ---------- symbol/quotes ----------
def to_schwab_opt(sym: str) -> str:
    raw = (sym or "").strip().upper()
    if raw.startswith("."): raw = raw[1:]
    raw = raw.replace("_","")
    m = re.match(r'^([A-Z.$^]{1,6})(\d{6})([CP])(\d{8})$', raw)
    if m:
        root, ymd, cp, strike8 = m.groups()
        return f"{root:<6}{ymd}{cp}{strike8}"
    m = re.match(r'^([A-Z.$^]{1,6})(\d{6})([CP])(\d{1,5})(?:\.(\d{1,3}))?$', raw)
    if m:
        root, ymd, cp, i, frac = m.groups()
        mills = int(i) * 1000 + (int(frac.ljust(3,'0')) if frac else 0)
        return f"{root:<6}{ymd}{cp}{mills:08d}"
    m = re.match(r'^(.{6})(\d{6})([CP])(\d{8})$', raw)
    if m:
        root6, ymd, cp, strike8 = m.groups()
        return f"{root6}{ymd}{cp}{strike8}"
    raise ValueError(f"Cannot parse option symbol: {sym}")

def strike_from_osi(osi: str) -> float:
    return int(osi[-8:]) / 1000.0

def parse_bid_ask(qobj: dict):
    if not qobj: return (None,None)
    d = qobj.get("quote", qobj)
    for bk in ("bidPrice","bid","bidPriceInDouble"):
        for ak in ("askPrice","ask","askPriceInDouble"):
            b=d.get(bk); a=d.get(ak)
            if isinstance(b,(int,float)) and isinstance(a,(int,float)):
                return (float(b), float(a))
    return (None,None)

def fetch_bid_ask(c, symbol: str):
    r=c.get_quote(symbol)
    if r.status_code!=200: return (None,None)
    j=r.json(); d=j.get(symbol) or next(iter(j.values()),{})
    return parse_bid_ask(d)

def compute_mid_condor(c, legs_osi: list):
    (bp, sp, sc, bc) = legs_osi
    bp_bid, bp_ask = fetch_bid_ask(c, bp)
    sp_bid, sp_ask = fetch_bid_ask(c, sp)
    sc_bid, sc_ask = fetch_bid_ask(c, sc)
    bc_bid, bc_ask = fetch_bid_ask(c, bc)
    if None in (bp_bid,bp_ask,sp_bid,sp_ask,sc_bid,sc_ask,bc_bid,bc_ask):
        return (None,None,None,(None,)*8)
    net_bid=(sp_bid+sc_bid)-(bp_ask+bc_ask)
    net_ask=(sp_ask+sc_ask)-(bp_bid+bc_bid)
    mid=(net_bid+net_ask)/2.0
    return (mid, net_bid, net_ask, (bp_bid,bp_ask,sp_bid,sp_ask,sc_bid,sc_ask,bc_bid,bc_ask))

# ---------- sheets ----------
def ensure_header_and_get_sheetid(svc, spreadsheet_id: str, tab: str, header: list):
    got = svc.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=f"{tab}!1:1").execute().get("values",[])
    if not got or got[0] != header:
        svc.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id, range=f"{tab}!1:1",
            valueInputOption="USER_ENTERED", body={"values":[header]}
        ).execute()
    meta = svc.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    return next(sh["properties"]["sheetId"] for sh in meta["sheets"] if sh["properties"]["title"]==tab)

def top_insert(svc, spreadsheet_id: str, sheet_id_num: int):
    svc.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests":[{"insertDimension":{
            "range":{"sheetId":sheet_id_num,"dimension":"ROWS","startIndex":1,"endIndex":2},
            "inheritFromBefore": False
        }}]}
    ).execute()

def parse_iso(ts_str: str):
    try: return datetime.fromisoformat(ts_str.replace("Z","+00:00"))
    except: return None

def next_trading_day(d: date) -> date:
    cal = mcal.get_calendar('XNYS')
    sched = cal.schedule(start_date=d, end_date=d + timedelta(days=7), tz='America/New_York')
    for ts in sched.index:
        if ts.date() > d:
            return ts.date()
    nd = d + timedelta(days=1)
    while nd.weekday() >= 5:
        nd += timedelta(days=1)
    return nd

def ceil_to_tick(x: float, tick: float)->float:
    if x is None: return None
    return round(math.ceil((x + 1e-12)/tick)*tick, 2)
def clamp_and_snap(x: float, lo: float, hi: float, tick: float)->float:
    if x is None: return None
    y = max(lo, min(hi, x))
    y = ceil_to_tick(y, tick)
    return max(lo, min(hi, y))

# ---------- broker open‑order guard ----------
def has_open_order_for_legs(c, acct_hash: str, legs_osi: list) -> bool:
    try:
        url=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders"
        r=c.session.get(url)
        if r.status_code!=200: return False
        arr=r.json()
        want=set(legs_osi)
        for o in arr or []:
            st=str(o.get("status") or "").upper()
            if st not in ("WORKING","QUEUED","PENDING_ACTIVATION","OPEN"):
                continue
            legs=[]
            for leg in (o.get("orderLegCollection") or []):
                sym=(leg.get("instrument",{}) or {}).get("symbol","")
                if sym:
                    try: legs.append(to_schwab_opt(sym))
                    except: pass
            if legs and set(legs)==want:
                return True
        return False
    except:
        return False

# ---------- Schwab client ----------
def make_client(app_key: str, app_secret: str, token_json: str):
    with open("schwab_token.json","w") as f: f.write(token_json)
    try:
        sig = str(inspect.signature(client_from_token_file)); print("client_from_token_file signature:", sig)
    except Exception as e:
        print("SIG_INTROSPECT_ERR", e)
    try:
        return client_from_token_file(token_path="schwab_token.json", api_key=app_key, app_secret=app_secret)
    except TypeError as e1:
        print("INIT_TRY1 failed:", e1)
    try:
        return client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")
    except TypeError as e2:
        print("INIT_TRY2 failed:", e2)
    try:
        return client_from_token_file("schwab_token.json", app_key, app_secret)
    except TypeError as e3:
        print("INIT_TRY3 failed:", e3)
    try:
        return client_from_token_file(token_path="schwab_token.json", api_key=app_key, api_secret=app_secret)  # noqa
    except TypeError as e4:
        print("INIT_TRY4 failed:", e4); raise

# ---------- Buying power ----------
def discover_bp_option_only(sa: dict) -> float | None:
    cands = []
    for section in ("currentBalances","projectedBalances","initialBalances","balances"):
        b = sa.get(section) or {}
        for k in ("optionBuyingPower","optionsBuyingPower","availableFundsForOptionsTrading"):
            v = b.get(k)
            if isinstance(v,(int,float)): cands.append(float(v))
    for k in ("optionBuyingPower","optionsBuyingPower","availableFundsForOptionsTrading"):
        v = sa.get(k)
        if isinstance(v,(int,float)): cands.append(float(v))
    return max(cands) if cands else None

def discover_bp_any(sa: dict) -> float | None:
    cands = []
    for section in ("currentBalances","projectedBalances","initialBalances","balances"):
        b = sa.get(section) or {}
        for k in ("optionBuyingPower","optionsBuyingPower","availableFundsForOptionsTrading",
                  "buyingPower","cashAvailableForTrading","availableFunds","cashAvailableForTradingSettled"):
            v = b.get(k)
            if isinstance(v,(int,float)): cands.append(float(v))
    for k in ("optionBuyingPower","optionsBuyingPower","availableFundsForOptionsTrading","buyingPower"):
        v = sa.get(k)
        if isinstance(v,(int,float)): cands.append(float(v))
    return max(cands) if cands else None

# ---------- main ----------
def main():
    print(f"schwab_place_order.py version {__VERSION__}")

    if (env_str("SCHWAB_PLACE") or env_str("SCHWAB_PLACE_VAR") or env_str("SCHWAB_PLACE_SEC")).lower()!="place":
        print("SCHWAB_PLACE not 'place' → skipping."); sys.exit(0)

    # required secrets
    app_key    = env_or_die("SCHWAB_APP_KEY")
    app_secret = env_or_die("SCHWAB_APP_SECRET")
    token_json = env_or_die("SCHWAB_TOKEN_JSON")
    sheet_id   = env_or_die("GSHEET_ID")
    sa_json    = env_or_die("GOOGLE_SERVICE_ACCOUNT_JSON")

    # timing + ladder
    STAGE_SEC = env_int("STAGE_SEC", 45)     # give each rung real time to work
    TICK      = env_float("TICK", 0.05)
    EMERGENCY_MIN_TO_CLOSE = env_int("EMERGENCY_MIN_TO_CLOSE", 2)  # optional; leave as-is
    CANCEL_IF_UNFILLED = env_bool("CANCEL_IF_UNFILLED", True)

    # sizing (unchanged)
    SIZE_MODE        = env_str("SIZE_MODE","STATIC").upper()
    CREDIT_ALLOC_PCT = env_float("CREDIT_ALLOC_PCT", 0.06)
    DEBIT_ALLOC_PCT  = env_float("DEBIT_ALLOC_PCT",  0.02)
    RISK_PER_CONTRACT_CREDIT = env_float("RISK_PER_CONTRACT_CREDIT", 300.0)
    RISK_PER_CONTRACT_DEBIT  = env_float("RISK_PER_CONTRACT_DEBIT",  200.0)
    MAX_QTY          = env_int("MAX_QTY", 10)
    MIN_QTY          = env_int("MIN_QTY", 1)
    OPT_BP_OVERRIDE  = env_float("OPT_BP_OVERRIDE", -1.0)
    BP_SOURCE        = env_str("BP_SOURCE","OPTION").upper()
    MAX_RISK_PER_TRADE = env_float("MAX_RISK_PER_TRADE", -1.0)
    HARD_QTY_CUTOFF    = env_int("HARD_QTY_CUTOFF", 20)

    # Schwab
    c = make_client(app_key, app_secret, token_json)
    acct_hash = env_str("SCHWAB_ACCT_HASH", "")
    if not acct_hash:
        r=c.get_account_numbers(); r.raise_for_status()
        acct=r.json()[0]; acct_hash=acct["hashValue"]

    # last price (best effort)
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
    creds=service_account.Credentials.from_service_account_info(json.loads(sa_json), scopes=["https://www.googleapis.com/auth/spreadsheets"])
    s=build("sheets","v4",credentials=creds)
    schwab_sheet_id=ensure_header_and_get_sheetid(s, sheet_id, SCHWAB_TAB, SCHWAB_HEADERS)

    # read A2
    two=s.spreadsheets().values().get(spreadsheetId=sheet_id, range=f"{LEO_TAB}!A1:AG2").execute().get("values",[])
    if len(two)<2:
        print("No leocross row 2; nothing to place."); sys.exit(0)
    h,r2=two[0],two[1]; idx={n:i for i,n in enumerate(h)}
    def g(col): j=idx.get(col,-1); return r2[j] if 0<=j<len(r2) else ""

    def log_top(src: str, msg: str, qty_val: int, oid: str = "", price: str = ""):
        top_insert(s, sheet_id, schwab_sheet_id)
        s.spreadsheets().values().update(
            spreadsheetId=sheet_id, range=f"{SCHWAB_TAB}!A2",
            valueInputOption="USER_ENTERED",
            body={"values":[[
                datetime.now(timezone.utc).isoformat(),
                src,"SPX",last_px,
                g("signal_date"),"PLACE",g("side"),qty_val,
                ("NET_CREDIT" if (g('credit_or_debit') or '').lower()=='credit' else "NET_DEBIT"),
                price,
                g("occ_buy_put"),g("occ_sell_put"),g("occ_sell_call"),g("occ_buy_call"),
                oid,msg
            ]]}
        ).execute()

    def log_and_exit(src: str, msg: str, qty_val: int = 0):
        log_top(src, msg, qty_val, "", ""); print(msg); sys.exit(0)

    # recency & dates (keep safety)
    ts = parse_iso(g("ts")); age_min=(datetime.now(timezone.utc)-ts).total_seconds()/60.0 if ts else None
    try:
        sig_d=date.fromisoformat(g("signal_date")) if g("signal_date") else None
        exp_d=date.fromisoformat(g("expiry")) if g("expiry") else None
    except:
        sig_d=exp_d=None
    if age_min is None or age_min>env_int("FRESH_MIN", 120):
        log_and_exit("SCHWAB_ERROR", f"STALE_OR_NO_TS age_min={age_min}")
    if env_bool("ENFORCE_EXPIRY_TOMORROW", True) and sig_d and exp_d:
        expected = next_trading_day(sig_d)
        if exp_d != expected:
            log_and_exit("SCHWAB_ERROR", f"DATE_MISMATCH sig={g('signal_date')} exp={g('expiry')} (expected {expected})")

    # legs
    raw_legs=[g("occ_buy_put"), g("occ_sell_put"), g("occ_sell_call"), g("occ_buy_call")]
    if not all(raw_legs): log_and_exit("SCHWAB_ERROR","MISSING_LEGS")
    try:
        leg_syms = [to_schwab_opt(x) for x in raw_legs]
    except Exception as e:
        log_and_exit("SCHWAB_ERROR", f"SYMBOL_ERR: {str(e)[:180]}")

    # side
    def side_is_credit():
        cod=(g("credit_or_debit") or "").lower()
        if cod in ("credit","debit"): return (cod=="credit")
        try:
            c1=float(g("cat1")); c2=float(g("cat2"))
            return (c1 < c2)
        except: return (g("side").upper().startswith("SHORT"))
    is_credit=side_is_credit()
    order_type="NET_CREDIT" if is_credit else "NET_DEBIT"

    # orientation
    def fix_orientation(legs):
        bp, sp, sc, bc = legs
        bpS = strike_from_osi(bp); spS = strike_from_osi(sp)
        scS = strike_from_osi(sc); bcS = strike_from_osi(bc)
        if is_credit:
            if bpS > spS: bp, sp = sp, bp
            if scS > bcS: sc, bc = bc, sc
        else:
            if bpS < spS: bp, sp = sp, bp
            if bcS > scS: sc, bc = bc, sc
        return [bp, sp, sc, bc]
    leg_syms = fix_orientation(leg_syms)

    # widths → clamp bounds
    put_w  = abs(strike_from_osi(leg_syms[1]) - strike_from_osi(leg_syms[0]))
    call_w = abs(strike_from_osi(leg_syms[3]) - strike_from_osi(leg_syms[2]))
    width  = round(min(put_w, call_w), 3)

    # Leo recs (for pricing only)
    def fnum(x):
        try: return float(x)
        except: return None
    rec_put=fnum(g("rec_put")); rec_call=fnum(g("rec_call"))
    rec_condor=fnum(g("rec_condor")) if g("rec_condor")!="" else (None if (rec_put is None or rec_call is None) else (rec_put+rec_call))

    # mid (best-effort; no liquidity guard)
    mid, _, _, _ = compute_mid_condor(client_from_token_file, leg_syms) if False else (None,None,None,None)  # placeholder
    try:
        mid, _, _, _ = compute_mid_condor(make_client(app_key, app_secret, token_json), leg_syms)
    except Exception:
        mid = None

    # ---------- sizing ----------
    qty_sheet = int((g("qty_exec") or "1"))
    qty_exec  = qty_sheet
    if SIZE_MODE=="PCT_BP":
        bp = OPT_BP_OVERRIDE if OPT_BP_OVERRIDE and OPT_BP_OVERRIDE>0 else None
        if bp is None:
            try:
                url=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}"
                rr=c.session.get(url)
                if rr.status_code==200:
                    j=rr.json()
                    if isinstance(j,list): j=j[0]
                    sa=j.get("securitiesAccount") or j
                    if BP_SOURCE in ("OPTION","OPTIONS"): bp = discover_bp_option_only(sa)
                    else: bp = discover_bp_any(sa)
            except: bp=None
        if bp is not None:
            alloc = CREDIT_ALLOC_PCT if is_credit else DEBIT_ALLOC_PCT
            budget = max(bp*alloc, 0.0)
            if MAX_RISK_PER_TRADE and MAX_RISK_PER_TRADE>0:
                budget = min(budget, MAX_RISK_PER_TRADE)
            risk_per_ct = RISK_PER_CONTRACT_CREDIT if is_credit else RISK_PER_CONTRACT_DEBIT
            risk_per_ct = max(risk_per_ct, 1.0)
            qty_exec = max(1, int(budget // risk_per_ct))
            print(f"SIZING PCT_BP bp={bp:.2f} alloc={alloc:.4f} budget={budget:.2f} "
                  f"risk_per_ct={risk_per_ct:.2f} -> qty={qty_exec}")
        else:
            print("WARN: could not fetch option buying power; using sheet qty.")

    if qty_exec < MIN_QTY: qty_exec = MIN_QTY
    if MAX_QTY>0 and qty_exec > MAX_QTY: qty_exec = MAX_QTY
    if HARD_QTY_CUTOFF>0 and qty_exec > HARD_QTY_CUTOFF:
        log_and_exit("SCHWAB_ABORT", f"QTY_ABOVE_HARD_CUTOFF computed={qty_exec} cutoff={HARD_QTY_CUTOFF}", qty_exec)

    # ---------- fixed ladder (your spec) ----------
    # Helpers
    def snap_credit(x):
        if x is None: return None
        # Credit cannot exceed width - tick; cannot be negative
        hi = max(TICK, width - TICK)
        return clamp_and_snap(x, TICK, hi, TICK)
    def snap_debit(x):
        if x is None: return None
        # Debit cannot exceed width - tick either
        hi = max(TICK, width - TICK)
        return clamp_and_snap(x, TICK, hi, TICK)

    prices = []
    if is_credit:
        # Stage order: Leo rec → 2.20 → mid → mid - 0.05
        candidates = [
            rec_condor,
            2.20,
            mid,
            (None if mid is None else (mid - 0.05)),
        ]
        for p in candidates:
            p = snap_credit(p)
            if p is not None and p not in prices:
                prices.append(p)
    else:
        # Stage order: 1.80 → Leo rec → mid → mid + 0.05
        candidates = [
            1.80,
            rec_condor,
            mid,
            (None if mid is None else (mid + 0.05)),
        ]
        for p in candidates:
            p = snap_debit(p)
            if p is not None and p not in prices:
                prices.append(p)

    if not prices:
        log_and_exit("SCHWAB_ERROR", "NO_VALID_PRICES_AFTER_SNAP")

    # ---------- pre-claim guard ----------
    if has_open_order_for_legs(c, acct_hash, leg_syms):
        log_and_exit("SCHWAB_SKIP", "OPEN_ORDER_EXISTS (pre-claim)", qty_exec)

    # ---------- claim ----------
    fingerprint = "|".join([ (g("signal_date") or "").upper(),
                              ("SHORT" if is_credit else "LONG"),
                              raw_legs[0].upper(), raw_legs[1].upper(), raw_legs[2].upper(), raw_legs[3].upper() ])
    run_id = os.environ.get("GITHUB_RUN_ID","")

    top_insert(s, sheet_id, schwab_sheet_id)
    s.spreadsheets().values().update(
        spreadsheetId=sheet_id, range=f"{SCHWAB_TAB}!A2",
        valueInputOption="USER_ENTERED",
        body={"values":[[
            datetime.now(timezone.utc).isoformat(),
            "SCHWAB_CLAIM","SPX", last_px,
            g("signal_date"), "CLAIM",
            ("SHORT_IRON_CONDOR" if is_credit else "LONG_IRON_CONDOR"),
            qty_exec, ("NET_CREDIT" if is_credit else "NET_DEBIT"), "",
            raw_legs[0], raw_legs[1], raw_legs[2], raw_legs[3],
            run_id, fingerprint
        ]]}
    ).execute()

    # ---------- build / place / replace ----------
    def build_order(price: float, q: int):
        return {
            "orderType": ("NET_CREDIT" if is_credit else "NET_DEBIT"),
            "session":"NORMAL", "price": f"{price:.2f}",
            "duration":"DAY", "orderStrategyType":"SINGLE",
            "complexOrderStrategyType":"IRON_CONDOR",
            "orderLegCollection":[
                {"instruction":"BUY_TO_OPEN","quantity":q,"instrument":{"symbol":leg_syms[0],"assetType":"OPTION"}},
                {"instruction":"SELL_TO_OPEN","quantity":q,"instrument":{"symbol":leg_syms[1],"assetType":"OPTION"}},
                {"instruction":"SELL_TO_OPEN","quantity":q,"instrument":{"symbol":leg_syms[2],"assetType":"OPTION"}},
                {"instruction":"BUY_TO_OPEN","quantity":q,"instrument":{"symbol":leg_syms[3],"assetType":"OPTION"}},
            ]
        }

    def place(order_price: float):
        order = build_order(order_price, qty_exec)
        r = c.place_order(acct_hash, order)
        oid = ""
        try:
            j=r.json(); oid = str(j.get("orderId") or j.get("order_id") or "")
        except:
            oid = r.headers.get("Location","").rstrip("/").split("/")[-1]
        print("PLACE_HTTP", r.status_code, "ORDER_ID", oid, "PRICE", order["price"])
        return (r.status_code, oid, order["price"])

    def replace(oid: str, new_price: float):
        order = build_order(new_price, qty_exec)
        url = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{oid}"
        r = c.session.put(url, json=order)
        new_id = oid
        try:
            j=r.json(); new_id=str(j.get("orderId") or j.get("order_id") or oid)
        except:
            loc=r.headers.get("Location","")
            if loc: new_id=loc.rstrip("/").split("/")[-1] or oid
        print("REPLACE_HTTP", r.status_code, "ORDER_ID", new_id, "PRICE", f"{new_price:.2f}")
        return (r.status_code, new_id, f"{new_price:.2f}")

    def cancel(oid: str):
        try:
            url=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{oid}"
            r=c.session.delete(url)
            print("CANCEL_HTTP", r.status_code, "ORDER_ID", oid)
            return r.status_code
        except Exception as e:
            print("CANCEL_ERR", str(e))
            return None

    def status(oid: str):
        try:
            url=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{oid}"
            r=c.session.get(url)
            if r.status_code!=200: return (r.status_code,None)
            j=r.json(); return (r.status_code, (j.get("status") or j.get("orderStatus")))
        except: return (None,None)

    # stage helpers (also log each rung to the sheet)
    def log_rung(tag: str, oid: str, price_used: str):
        log_top(tag, "WORKING", qty_exec, oid, price_used)

    # Stage 1
    sc, oid, price_used = place(prices[0]); log_rung("SCHWAB_WORKING_S1", oid, price_used)
    filled=False
    end = time.time() + STAGE_SEC
    while time.time() < end:
        _, st = status(oid)
        if st and str(st).upper()=="FILLED": filled=True; break
        time.sleep(1)

    # Remaining stages
    stage_idx = 2
    for p in prices[1:]:
        if filled: break
        rc, oid, price_used = replace(oid, p); log_rung(f"SCHWAB_REPLACE_S{stage_idx}", oid, price_used)
        end = time.time() + STAGE_SEC
        while time.time() < end:
            _, st = status(oid)
            if st and str(st).upper()=="FILLED": filled=True; break
            time.sleep(1)
        stage_idx += 1

    # Optional emergency push at the very end of the session (off-hours this will just queue)
    if not filled and EMERGENCY_MIN_TO_CLOSE>0 and minutes_to_option_close() <= EMERGENCY_MIN_TO_CLOSE:
        emerg_price = prices[-1]  # last rung already most aggressive per your sequence
        rc, oid, price_used = replace(oid, emerg_price); log_rung("SCHWAB_REPLACE_EMERGENCY", oid, price_used)
        end = time.time() + STAGE_SEC
        while time.time() < end:
            _, st = status(oid)
            if st and str(st).upper()=="FILLED": filled=True; break
            time.sleep(1)

    rc, st = status(oid)
    final_status = st or sc

    if not filled and CANCEL_IF_UNFILLED and oid:
        cancel(oid)
        rc, st = status(oid)
        final_status = st or final_status

    # Final log
    log_top("SCHWAB_PLACED" if filled else "SCHWAB_ORDER", str(final_status), qty_exec, oid, price_used)
    print("FINAL_STATUS", final_status, "ORDER_ID", oid, "PRICE_USED", price_used)

if __name__ == "__main__":
    main()
