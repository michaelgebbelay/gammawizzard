# scripts/schwab_place_order.py
import os, re, json, sys, time, math, inspect
from datetime import datetime, timezone, date, timedelta

from google.oauth2 import service_account
from googleapiclient.discovery import build

# schwab-py
from schwab.auth import client_from_token_file

# market clock (early closes)
import pytz
import pandas_market_calendars as mcal

__VERSION__ = "2025-08-27c"

LEO_TAB    = "leocross"
SCHWAB_TAB = "schwab"

SCHWAB_HEADERS = [
    "ts","source","symbol","last_price",
    "signal_date","order_mode","side","qty_exec","order_type","limit_price",
    "occ_buy_put","occ_sell_put","occ_sell_call","occ_buy_call",
    "order_id","status"
]

# -------------------- config helpers --------------------
def _load_cfg():
    try: return json.loads(os.environ.get("CONFIG_JSON","") or "{}")
    except: return {}
CFG = _load_cfg()
def cfg(path, default=None):
    cur=CFG
    for p in path.split("."):
        if isinstance(cur, dict) and p in cur: cur=cur[p]
        else: return default
    return cur

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
_MIN_OVERRIDE = env_float("MINUTES_TO_CLOSE_OVERRIDE", float(cfg("sim.minutes_to_close", -1)))
def now_et() -> datetime:
    return datetime.now(_ET)
def todays_equity_close_et() -> datetime | None:
    cal = mcal.get_calendar('XNYS')
    d = now_et().date()
    sched = cal.schedule(start_date=d, end_date=d, tz='America/New_York')
    if sched.empty: return None
    return sched['market_close'].iloc[0].to_pydatetime()
def minutes_to_option_close() -> float:
    if _MIN_OVERRIDE is not None and _MIN_OVERRIDE >= 0:
        return _MIN_OVERRIDE
    close = todays_equity_close_et()
    if close is None: return 1e9
    opt_close = close + timedelta(minutes=15)
    return (opt_close - now_et()).total_seconds() / 60.0

# ---------- symbol & quotes ----------
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

# ---------- Schwab client factory ----------
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

# ---------- Buying power discovery ----------
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

    app_key    = env_or_die("SCHWAB_APP_KEY")
    app_secret = env_or_die("SCHWAB_APP_SECRET")
    token_json = env_or_die("SCHWAB_TOKEN_JSON")
    sheet_id   = env_or_die("GSHEET_ID")
    sa_json    = env_or_die("GOOGLE_SERVICE_ACCOUNT_JSON")

    # ---- config (env takes precedence over CONFIG_JSON defaults) ----
    DRY_RUN   = env_bool("DRY_RUN", bool(cfg("sim.dry_run", False)))
    SIM_QUOTES= env_bool("SIM_QUOTES", bool(cfg("sim.quotes", False)))
    SIM_MID   = env_float("SIM_MID", float(cfg("sim.mid", float("nan"))))
    SIM_SPREAD= env_float("SIM_NET_SPREAD", float(cfg("sim.spread", float("nan"))))
    SIM_FILL_ON_STAGE = env_int("SIM_FILL_ON_STAGE", int(cfg("sim.fill_on_stage", 2)))

    STAGE_SEC = env_int("STAGE_SEC", int(cfg("ladder.stage_sec", 45)))
    TICK      = env_float("TICK", float(cfg("pricing.tick", 0.05)))

    CREDIT_START_PER = env_float("CREDIT_START_PERLEG", float(cfg("pricing.credit_start_perleg", 1.10)))
    CREDIT_FLOOR_PER = env_float("CREDIT_FLOOR_PERLEG", float(cfg("pricing.credit_floor_perleg", 0.95)))
    DEBIT_START_PER  = env_float("DEBIT_START_PERLEG",  float(cfg("pricing.debit_start_perleg",  0.90)))
    DEBIT_CEIL_PER   = env_float("DEBIT_CEIL_PERLEG",   float(cfg("pricing.debit_ceil_perleg",   1.05)))
    OFFSET_PER_CRED  = env_float("OFFSET_PERLEG_CREDIT", float(cfg("pricing.offset_perleg_credit",-0.05)))
    OFFSET_PER_DEB   = env_float("OFFSET_PERLEG_DEBIT",  float(cfg("pricing.offset_perleg_debit", 0.05)))

    CREDIT_START = 2*CREDIT_START_PER
    CREDIT_FLOOR = 2*CREDIT_FLOOR_PER
    DEBIT_START  = 2*DEBIT_START_PER
    DEBIT_CEIL   = 2*DEBIT_CEIL_PER
    OFFSET_CREDIT = 2*OFFSET_PER_CRED
    OFFSET_DEBIT  = 2*OFFSET_PER_DEB

    SIZE_MODE        = env_str("SIZE_MODE", str(cfg("size.mode", "STATIC"))).upper()
    CREDIT_ALLOC_PCT = env_float("CREDIT_ALLOC_PCT", float(cfg("size.credit_alloc_pct", 0.06)))
    DEBIT_ALLOC_PCT  = env_float("DEBIT_ALLOC_PCT",  float(cfg("size.debit_alloc_pct",  0.02)))
    RISK_PER_CONTRACT_CREDIT = env_float("RISK_PER_CONTRACT_CREDIT", float(cfg("size.risk_per_contract_credit", 300.0)))
    RISK_PER_CONTRACT_DEBIT  = env_float("RISK_PER_CONTRACT_DEBIT",  float(cfg("size.risk_per_contract_debit",  200.0)))
    MAX_QTY          = env_int("MAX_QTY", int(cfg("size.max_qty", 10)))
    MIN_QTY          = env_int("MIN_QTY", int(cfg("size.min_qty", 1)))
    OPT_BP_OVERRIDE  = env_float("OPT_BP_OVERRIDE", float(cfg("size.opt_bp_override", -1.0)))
    BP_SOURCE        = env_str("BP_SOURCE", str(cfg("size.bp_source", "OPTION"))).upper()
    MAX_RISK_PER_TRADE = env_float("MAX_RISK_PER_TRADE", float(cfg("size.max_risk_per_trade", -1.0)))
    HARD_QTY_CUTOFF    = env_int("HARD_QTY_CUTOFF", int(cfg("size.hard_qty_cutoff", 20)))

    MAX_LEG_SPREAD_BASE = env_float("MAX_LEG_SPREAD", float(cfg("liquidity.base.leg", 0.30)))
    MAX_NET_SPREAD_BASE = env_float("MAX_NET_SPREAD", float(cfg("liquidity.base.net", 0.30)))
    MAX_NET_SPREAD_NEAR5 = env_float("MAX_NET_SPREAD_NEAR5", float(cfg("liquidity.near5.net", 0.60)))
    MAX_NET_SPREAD_NEAR2 = env_float("MAX_NET_SPREAD_NEAR2", float(cfg("liquidity.near2.net", 0.80)))
    MAX_NET_SPREAD_NEAR1 = env_float("MAX_NET_SPREAD_NEAR1", float(cfg("liquidity.near1.net", 1.00)))
    MAX_LEG_SPREAD_NEAR5 = env_float("MAX_LEG_SPREAD_NEAR5", float(cfg("liquidity.near5.leg", 0.45)))
    MAX_LEG_SPREAD_NEAR2 = env_float("MAX_LEG_SPREAD_NEAR2", float(cfg("liquidity.near2.leg", 0.60)))
    MAX_LEG_SPREAD_NEAR1 = env_float("MAX_LEG_SPREAD_NEAR1", float(cfg("liquidity.near1.leg", 0.70)))
    SPREAD_RETRIES  = env_int("SPREAD_RETRIES", int(cfg("liquidity.retries", 3)))
    SPREAD_WAIT_MS  = env_int("SPREAD_WAIT_MS", int(cfg("liquidity.wait_ms", 250)))
    BYPASS_WIDE_SPREAD_NEAR_MIN = env_int("BYPASS_WIDE_SPREAD_NEAR_MIN", int(cfg("liquidity.bypass_wide_spread_near_min", 2)))

    EMERGENCY_MIN_TO_CLOSE = env_int("EMERGENCY_MIN_TO_CLOSE", int(cfg("ladder.emergency_min_to_close", 2)))
    CANCEL_IF_UNFILLED = env_bool("CANCEL_IF_UNFILLED", bool(cfg("ladder.cancel_if_unfilled", True)))

    FRESH_MIN = env_int("FRESH_MIN", int(cfg("policy.fresh_min", 120)))
    ENFORCE_EXPIRY_TOMORROW = env_bool("ENFORCE_EXPIRY_TOMORROW", bool(cfg("policy.enforce_expiry_tomorrow", True)))

    # --- Schwab client (skip network in DRY_RUN) ---
    c = None
    acct_hash = ""
    if not DRY_RUN:
        c = make_client(app_key, app_secret, token_json)
        acct_hash = env_str("SCHWAB_ACCT_HASH", "")
        if not acct_hash:
            r=c.get_account_numbers(); r.raise_for_status()
            acct=r.json()[0]; acct_hash=acct["hashValue"]

    # SPX last (skip network in DRY_RUN)
    def spx_last():
        if DRY_RUN: return "SIM"
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
    creds=service_account.Credentials.from_service_account_info(json.loads(sa_json), scopes=["https://www.googleapis.com/auth/spreadsheets"])
    s=build("sheets","v4",credentials=creds)
    schwab_sheet_id=ensure_header_and_get_sheetid(s, sheet_id, SCHWAB_TAB, SCHWAB_HEADERS)

    # Hard kill too-early scheduled runs (prod only)
    if not DRY_RUN and minutes_to_option_close() > 10:
        def _log_skip(msg):
            top_insert(s, sheet_id, schwab_sheet_id)
            s.spreadsheets().values().update(
                spreadsheetId=sheet_id, range=f"{SCHWAB_TAB}!A2",
                valueInputOption="USER_ENTERED",
                body={"values":[[
                    datetime.now(timezone.utc).isoformat(),
                    "SCHWAB_SKIP_EARLY","SPX",last_px,"","SKIP","","","","",
                    "","","","", "", msg
                ]]}
            ).execute()
        _log_skip(f"TOO_EARLY mins_to_close={minutes_to_option_close():.2f}")
        sys.exit(0)

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

    # --- recency & dates ---
    ts = parse_iso(g("ts")); age_min=(datetime.now(timezone.utc)-ts).total_seconds()/60.0 if ts else None
    try:
        sig_d=date.fromisoformat(g("signal_date")) if g("signal_date") else None
        exp_d=date.fromisoformat(g("expiry")) if g("expiry") else None
    except:
        sig_d=exp_d=None
    if age_min is None or age_min>FRESH_MIN: log_and_exit("SCHWAB_ERROR", f"STALE_OR_NO_TS age_min={age_min}")
    if ENFORCE_EXPIRY_TOMORROW and sig_d and exp_d:
        expected = next_trading_day(sig_d)
        if exp_d != expected: log_and_exit("SCHWAB_ERROR", f"DATE_MISMATCH sig={g('signal_date')} exp={g('expiry')} (expected {expected})")

    # --- legs ---
    raw_legs=[g("occ_buy_put"), g("occ_sell_put"), g("occ_sell_call"), g("occ_buy_call")]
    if not all(raw_legs): log_and_exit("SCHWAB_ERROR","MISSING_LEGS")
    try:
        leg_syms = [to_schwab_opt(x) for x in raw_legs]
    except Exception as e:
        log_and_exit("SCHWAB_ERROR", f"SYMBOL_ERR: {str(e)[:180]}")

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

    put_w  = abs(strike_from_osi(leg_syms[1]) - strike_from_osi(leg_syms[0]))
    call_w = abs(strike_from_osi(leg_syms[3]) - strike_from_osi(leg_syms[2]))
    width  = round(min(put_w, call_w), 3)

    # Leo recs (for pricing, not sizing)
    def fnum(x):
        try: return float(x)
        except: return None
    rec_put=fnum(g("rec_put")); rec_call=fnum(g("rec_call"))
    rec_condor=fnum(g("rec_condor")) if g("rec_condor")!="" else (None if (rec_put is None or rec_call is None) else (rec_put+rec_call))

    # ---------- qty sizing ----------
    qty_sheet = int((g("qty_exec") or "1"))
    qty_exec  = qty_sheet

    if SIZE_MODE=="PCT_BP":
        bp = OPT_BP_OVERRIDE if OPT_BP_OVERRIDE and OPT_BP_OVERRIDE>0 else None
        if (bp is None) and (not DRY_RUN):
            try:
                url=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}"
                rr=(c.session.get(url) if c else None)
                if rr and rr.status_code==200:
                    j=rr.json()
                    if isinstance(j,list): j=j[0]
                    sa=j.get("securitiesAccount") or j
                    if BP_SOURCE in ("OPTION","OPTIONS"):
                        bp = discover_bp_option_only(sa)
                    else:
                        bp = discover_bp_any(sa)
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

    # ---------- quotes / liquidity with dynamic limits ----------
    def dynamic_limits() -> tuple[float|None,float|None,float]:
        mins = max(0.0, minutes_to_option_close())
        net = MAX_NET_SPREAD_BASE
        leg = MAX_LEG_SPREAD_BASE
        if mins <= 1.0:
            net = max(net, MAX_NET_SPREAD_NEAR1)
            if leg is not None: leg = max(leg, MAX_LEG_SPREAD_NEAR1)
        elif mins <= 2.0:
            net = max(net, MAX_NET_SPREAD_NEAR2)
            if leg is not None: leg = max(leg, MAX_LEG_SPREAD_NEAR2)
        elif mins <= 5.0:
            net = max(net, MAX_NET_SPREAD_NEAR5)
            if leg is not None: leg = max(leg, MAX_LEG_SPREAD_NEAR5)
        return net, leg, mins

    # quotes (real or simulated)
    legs_ba = (None,)*8
    if SIM_QUOTES:
        mid = SIM_MID if (not math.isnan(SIM_MID)) else (rec_condor if rec_condor is not None else (CREDIT_START if is_credit else DEBIT_START))
        spread = SIM_SPREAD if (not math.isnan(SIM_SPREAD)) else 0.30
        net_bid = max(0.01, mid - spread/2.0)
        net_ask = max(net_bid + 0.01, mid + spread/2.0)
    else:
        mid, net_bid, net_ask, legs_ba = compute_mid_condor(c, leg_syms)

    def passes_liquidity(mid, net_bid, net_ask, legs_ba) -> tuple[bool,str,float,float]:
        net_th, leg_th, mins = dynamic_limits()
        if mid is None or net_bid is None or net_ask is None:
            return (False, "INCOMPLETE_QUOTES", net_th, mins)
        net_spread = net_ask - net_bid
        if net_th is not None and net_spread > net_th:
            return (False, f"NET_SPREAD_EXCEEDS {net_spread:.2f}>{net_th:.2f} (mins_to_close={mins:.2f})", net_th, mins)
        if leg_th is not None and None not in legs_ba:
            bp_bid,bp_ask,sp_bid,sp_ask,sc_bid,sc_ask,bc_bid,bc_ask = legs_ba
            worst = max(bp_ask-bp_bid, sp_ask-sp_bid, sc_ask-sc_bid, bc_ask-bc_bid)
            if worst > leg_th:
                return (False, f"LEG_SPREAD_EXCEEDS {worst:.2f}>{leg_th:.2f} (mins_to_close={mins:.2f})", net_th, mins)
        return (True, "OK", net_th, mins)

    # retries
    ok=False; info=""; net_th=MAX_NET_SPREAD_BASE; mins_left=999.0
    for i in range(max(1, SPREAD_RETRIES+1)):
        ok, info, net_th, mins_left = passes_liquidity(mid, net_bid, net_ask, legs_ba)
        if ok: break
        if i < SPREAD_RETRIES and not SIM_QUOTES:
            time.sleep(max(1, SPREAD_WAIT_MS)/1000.0)
            mid, net_bid, net_ask, legs_ba = compute_mid_condor(c, leg_syms)
    if not ok:
        if BYPASS_WIDE_SPREAD_NEAR_MIN>0 and mins_left <= BYPASS_WIDE_SPREAD_NEAR_MIN:
            print("WIDE_SPREAD_BYPASS", info)
        else:
            log_and_exit("SCHWAB_SKIP", info)

    # rung prices
    def clamp_credit(x): return clamp_and_snap(x, CREDIT_FLOOR, CREDIT_START, TICK)
    def clamp_debit(x):  return clamp_and_snap(x, 0.01, DEBIT_CEIL, TICK)
    if is_credit:
        start_price = clamp_credit(CREDIT_START)
        rec_price   = clamp_credit(rec_condor if rec_condor is not None else mid)
        stage3_base = None if mid is None else (mid + OFFSET_CREDIT)
        stage3_price= clamp_credit(stage3_base)
    else:
        start_price = clamp_debit(DEBIT_START)
        rec_price   = clamp_debit(rec_condor if rec_condor is not None else mid)
        stage3_base = None if mid is None else (mid + OFFSET_DEBIT)
        stage3_price= clamp_debit(stage3_base)

    # order plan artifact for visibility
    order_plan = {
        "is_credit": is_credit,
        "width": width,
        "qty": qty_exec,
        "minutes_to_close": minutes_to_option_close(),
        "dynamic_limits": {"net": net_th},
        "quotes": {"net_bid": net_bid, "net_ask": net_ask, "mid": mid, "sim": SIM_QUOTES},
        "rungs": [
            {"stage":1, "price": start_price},
            {"stage":2, "price": rec_price},
            {"stage":3, "price": stage3_price},
            {"stage":"emergency", "price": (CREDIT_FLOOR if is_credit else DEBIT_CEIL)}
        ]
    }
    try:
        with open("order_plan.json","w") as f: json.dump(order_plan, f, indent=2)
    except: pass

    # ---------- guards ----------
    if not DRY_RUN and has_open_order_for_legs(c, acct_hash, leg_syms):
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
            qty_exec, order_type, "",
            raw_legs[0], raw_legs[1], raw_legs[2], raw_legs[3],
            run_id, fingerprint
        ]]}
    ).execute()

    if not DRY_RUN:
        ok_claim=False
        for _ in range(10):
            head=s.spreadsheets().values().get(spreadsheetId=sheet_id, range=f"{SCHWAB_TAB}!A2:P12").execute().get("values",[])
            if head:
                r=head[0]
                if len(r)>=16 and r[1].upper()=="SCHWAB_CLAIM" and r[14]==run_id and r[15].upper()==fingerprint.upper():
                    ok_claim=True; break
            time.sleep(0.2)
        if not ok_claim:
            print("CLAIM_RACE: another run holds the lock → exiting."); sys.exit(0)
        time.sleep(0.6)
        if has_open_order_for_legs(c, acct_hash, leg_syms):
            log_and_exit("SCHWAB_SKIP", "OPEN_ORDER_EXISTS (post-claim)", qty_exec)

    # ---------- ladder (real or simulated) ----------
    sim = {"stage":0, "filled":False, "oid":"SIM-0"}
    def build_order(price: float, q: int):
        return {
            "orderType": order_type, "session":"NORMAL", "price": f"{price:.2f}",
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
        if DRY_RUN:
            sim["stage"]=1; sim["oid"]="SIM-1"; sim["filled"]=(SIM_FILL_ON_STAGE==1)
            return (200, sim["oid"], f"{order_price:.2f}")
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
        if DRY_RUN:
            sim["stage"] += 1
            sim["oid"] = f"SIM-{sim['stage']}"
            if SIM_FILL_ON_STAGE == sim["stage"]: sim["filled"]=True
            return (200, sim["oid"], f"{new_price:.2f}")
        lo = CREDIT_FLOOR if is_credit else 0.01
        hi = CREDIT_START if is_credit else DEBIT_CEIL
        new_price = clamp_and_snap(new_price, lo, hi, TICK)
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
        if DRY_RUN:
            return 200
        try:
            url=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{oid}"
            r=c.session.delete(url)
            print("CANCEL_HTTP", r.status_code, "ORDER_ID", oid)
            return r.status_code
        except Exception as e:
            print("CANCEL_ERR", str(e))
            return None

    def status(oid: str):
        if DRY_RUN:
            return (200, "FILLED" if sim["filled"] else "WORKING")
        try:
            url=f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{oid}"
            r=c.session.get(url)
            if r.status_code!=200: return (r.status_code,None)
            j=r.json(); return (r.status_code, (j.get("status") or j.get("orderStatus")))
        except: return (None,None)

    sc, oid, price_used = place(start_price)
    log_top("SCHWAB_WORKING", "WORKING", qty_exec, oid, price_used)

    filled=False
    end1 = time.time() + STAGE_SEC
    while time.time() < end1:
        rc, st = status(oid)
        if st and str(st).upper()=="FILLED": filled=True; break
        time.sleep(1)

    if not filled:
        rc, oid, price_used = replace(oid, rec_price)
        end2 = time.time() + STAGE_SEC
        while time.time() < end2:
            rc, st = status(oid)
            if st and str(st).upper()=="FILLED": filled=True; break
            time.sleep(1)

    if not filled:
        rc, oid, price_used = replace(oid, stage3_price)
        end3 = time.time() + STAGE_SEC
        while time.time() < end3:
            rc, st = status(oid)
            if st and str(st).upper()=="FILLED": filled=True; break
            time.sleep(1)

    if not filled and EMERGENCY_MIN_TO_CLOSE>0 and minutes_to_option_close() <= EMERGENCY_MIN_TO_CLOSE:
        emerg_price = (CREDIT_FLOOR if is_credit else DEBIT_CEIL)
        rc, oid, price_used = replace(oid, emerg_price)
        rem = max(0, minutes_to_option_close()*60 - 10)
        end4 = time.time() + min(STAGE_SEC, rem)
        while time.time() < end4:
            rc, st = status(oid)
            if st and str(st).upper()=="FILLED": filled=True; break
            time.sleep(1)

    final_status = "FILLED" if filled else "WORKING"
    if not DRY_RUN:
        rc, st = status(oid)
        final_status = st or sc

    if not filled and CANCEL_IF_UNFILLED and oid:
        cancel(oid)
        if not DRY_RUN:
            rc, st = status(oid)
            final_status = st or final_status
        else:
            final_status = "CANCELED"

    log_top("SCHWAB_PLACED" if filled else "SCHWAB_ORDER", str(final_status), qty_exec, oid, price_used)
    print("FINAL_STATUS", final_status, "ORDER_ID", oid, "PRICE_USED", price_used)
    print("ORDER_PLAN_PATH", "order_plan.json")

if __name__ == "__main__":
    main()
