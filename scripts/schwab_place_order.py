import os, re, json, sys, time
from datetime import datetime, timezone, date, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build
from schwab.auth import client_from_token_file

LEO_TAB = "leocross"
SCHWAB_TAB = "schwab"

SCHWAB_HEADERS = [
    "ts","source","account_hash","account_last4","symbol","last_price",
    "signal_date","order_mode","side","qty_exec","order_type","limit_price",
    "occ_buy_put","occ_sell_put","occ_sell_call","occ_buy_call","order_id","status"
]

# ---------- ENV helpers ----------
def env_or_die(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        print(f"Missing required env: {name}", file=sys.stderr)
        sys.exit(1)
    return v

def env_str(name: str, default: str = "") -> str:
    v = os.environ.get(name, None)
    if v is None: return default
    v = str(v).strip()
    return v if v != "" else default

def env_float(name: str, default: float) -> float:
    s = os.environ.get(name, None)
    if s is None: return default
    try:
        s = str(s).strip()
        if s == "": return default
        return float(s)
    except Exception:
        return default

def env_int(name: str, default: int) -> int:
    s = os.environ.get(name, None)
    if s is None: return default
    try:
        s = str(s).strip()
        if s == "": return default
        return int(float(s))
    except Exception:
        return default

# ---------- symbol conversion ----------
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
        mills = int(i)*1000 + (int(frac.ljust(3,'0')) if frac else 0)
        strike8 = f"{mills:08d}"
        return f"{root:<6}{ymd}{cp}{strike8}"
    m = re.match(r'^(.{6})(\d{6})([CP])(\d{8})$', sym or "")
    if m:
        root6, ymd, cp, strike8 = m.groups()
        return f"{root6}{ymd}{cp}{strike8}"
    raise ValueError(f"Cannot parse option symbol: {sym}")

# ---------- quotes / mids ----------
def parse_bid_ask(qobj: dict):
    if not qobj: return (None, None)
    d = qobj.get("quote", qobj)
    for bk in ("bidPrice","bid","bidPriceInDouble"):
        for ak in ("askPrice","ask","askPriceInDouble"):
            b = d.get(bk); a = d.get(ak)
            if isinstance(b,(int,float)) and isinstance(a,(int,float)):
                return (float(b), float(a))
    return (None, None)

def fetch_bid_ask(c, symbol: str):
    r = c.get_quote(symbol)
    if r.status_code != 200:
        return (None, None)
    j = r.json()
    d = j.get(symbol) or next(iter(j.values()), {})
    return parse_bid_ask(d)

def round_to_tick(x: float, tick: float) -> float:
    if tick <= 0: return x
    # avoid FP artifacts
    r = round(round(x / tick) * tick + 1e-9, 2)
    return r

def compute_mid(c, legs_osi: list):
    (bp, sp, sc, bc) = legs_osi
    bp_bid, bp_ask = fetch_bid_ask(c, bp)
    sp_bid, sp_ask = fetch_bid_ask(c, sp)
    sc_bid, sc_ask = fetch_bid_ask(c, sc)
    bc_bid, bc_ask = fetch_bid_ask(c, bc)
    if None in (bp_bid, bp_ask, sp_bid, sp_ask, sc_bid, sc_ask, bc_bid, bc_ask):
        return (None, None, None)
    net_bid = (sp_bid + sc_bid) - (bp_ask + bc_ask)
    net_ask = (sp_ask + sc_ask) - (bp_bid + bc_bid)
    mid = (net_bid + net_ask) / 2.0
    return (mid, net_bid, net_ask)

# ---------- Sheets helpers ----------
def ensure_header_and_get_sheetid(svc, spreadsheet_id: str, tab: str, header: list):
    got = svc.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=f"{tab}!1:1"
    ).execute().get("values", [])
    if not got or got[0] != header:
        svc.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id, range=f"{tab}!1:1",
            valueInputOption="USER_ENTERED", body={"values":[header]}
        ).execute()
    meta = svc.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheet_id_num = next(sh["properties"]["sheetId"]
                        for sh in meta["sheets"]
                        if sh["properties"]["title"] == tab)
    return sheet_id_num

def top_insert(svc, spreadsheet_id: str, sheet_id_num: int):
    svc.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests":[{"insertDimension":{
            "range":{"sheetId":sheet_id_num,"dimension":"ROWS","startIndex":1,"endIndex":2},
            "inheritFromBefore": False
        }}]}
    ).execute()

def parse_iso(ts_str: str):
    try:
        return datetime.fromisoformat(ts_str.replace("Z","+00:00"))
    except Exception:
        return None

# ---------- main ----------
def main():
    # placement toggle
    place_toggle = (env_str("SCHWAB_PLACE") or env_str("SCHWAB_PLACE_VAR") or env_str("SCHWAB_PLACE_SEC")).lower()
    if place_toggle != "place":
        print("SCHWAB_PLACE not 'place' → skipping order placement.")
        sys.exit(0)

    # required env
    app_key     = env_or_die("SCHWAB_APP_KEY")
    app_secret  = env_or_die("SCHWAB_APP_SECRET")
    token_json  = env_or_die("SCHWAB_TOKEN_JSON")
    sheet_id    = env_or_die("GSHEET_ID")
    sa_json     = env_or_die("GOOGLE_SERVICE_ACCOUNT_JSON")

    # ladder knobs
    TICK                = env_float("TICK", 0.05)
    DELAY_SEC           = env_int("REPRICE_DELAY_SEC", 30)
    MAX_STEPS           = env_int("MAX_REPRICE_STEPS", 8)
    STICKY_MID_STEPS    = env_int("STICKY_MID_STEPS", 2)
    OFFSET_CREDIT       = env_float("OFFSET_CREDIT", 0.05)
    OFFSET_DEBIT        = env_float("OFFSET_DEBIT",  0.05)
    MIN_CREDIT_START    = env_float("MIN_CREDIT_START", 2.00)
    MAX_DEBIT_START     = env_float("MAX_DEBIT_START",  1.90)
    CANCEL_REMAINING    = env_str("CANCEL_REMAINING", "true").lower() == "true"
    FALLBACK_NET_PRICE  = env_float("NET_PRICE", 0.05)

    # freshness & date guards
    FRESH_MIN           = env_int("FRESH_MIN", 120)                 # row must be <= 120 min old
    ENFORCE_TODAY       = env_str("ENFORCE_SIGNAL_TODAY", "true").lower() == "true"
    ENFORCE_TOMORROW    = env_str("ENFORCE_EXPIRY_TOMORROW","true").lower() == "true"

    # Schwab client
    with open("schwab_token.json","w") as f: f.write(token_json)
    c = client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")

    # account
    r = c.get_account_numbers(); r.raise_for_status()
    acct = r.json()[0]
    acct_hash  = acct["hashValue"]
    acct_last4 = acct["accountNumber"][-4:]

    # optional: SPX last for logging
    def spx_last():
        for sym in ["$SPX.X","SPX","SPX.X","$SPX"]:
            try:
                q = c.get_quote(sym)
                if q.status_code == 200 and sym in q.json():
                    last = q.json()[sym].get("quote",{}).get("lastPrice")
                    if last is not None: return last
            except Exception:
                pass
        return ""
    last_px = spx_last()

    # Sheets
    sa_info = json.loads(sa_json)
    creds = service_account.Credentials.from_service_account_info(sa_info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    s = build("sheets","v4",credentials=creds)
    sheet_id_num = ensure_header_and_get_sheetid(s, sheet_id, SCHWAB_TAB, SCHWAB_HEADERS)

    # read leocross A1:Z2
    two = s.spreadsheets().values().get(
        spreadsheetId=sheet_id, range=f"{LEO_TAB}!A1:Z2"
    ).execute().get("values", [])
    if len(two) < 2:
        print("No leocross row 2; nothing to place."); sys.exit(0)
    h, r2 = two[0], two[1]
    idx = {n:i for i,n in enumerate(h)}
    def g(col):
        j = idx.get(col, -1); return r2[j] if 0 <= j < len(r2) else ""

    # freshness & date checks (PT)
    now_pt = datetime.now(timezone.utc).astimezone().astimezone()  # runner local → we only need the date below
    # Convert ISO ts (UTC) to aware and compare age
    ts = parse_iso(g("ts"))
    age_min = None
    if ts:
        age_min = (datetime.now(timezone.utc) - ts).total_seconds()/60.0
    sig = g("signal_date")
    exp = g("expiry")

    # compute PT "today" and "tomorrow" by using America/Los_Angeles offset via the runner's TZ? GitHub runners default UTC,
    # but for dates we can accept the provided API dates which are calendar-based. Enforce equality by string match with today's date in PT.
    # To be safe without pytz, derive PT by offsetting from UTC using US Pacific offset guess via clock time:
    # Simpler: trust the API dates and ensure exp == sig+1 day.
    try:
        sig_d = date.fromisoformat(sig) if sig else None
        exp_d = date.fromisoformat(exp) if exp else None
    except Exception:
        sig_d = exp_d = None

    def bail(msg):
        top_insert(s, sheet_id, sheet_id_num)
        s.spreadsheets().values().update(
            spreadsheetId=sheet_id, range=f"{SCHWAB_TAB}!A2",
            valueInputOption="USER_ENTERED",
            body={"values":[[
                datetime.now(timezone.utc).isoformat(),
                "SCHWAB_ERROR",
                acct_hash, acct_last4, "", last_px,
                sig, "PLACE", g("side"), g("qty_exec"), "", "",
                g("occ_buy_put"), g("occ_sell_put"), g("occ_sell_call"), g("occ_buy_call"),
                "", msg
            ]]}
        ).execute()
        print(msg)
        sys.exit(0)

    if age_min is None or age_min > FRESH_MIN:
        bail(f"STALE_OR_NO_TS age_min={age_min}")

    if ENFORCE_TODAY and sig_d and exp_d:
        # Ensure exp == sig + 1 day
        if exp_d != sig_d + timedelta(days=1):
            bail(f"DATE_MISMATCH sig={sig} exp={exp} (exp should be sig+1d)")
    # (Optionally add a strict PT calendar check if needed; above prevents “yesterday’s row”)

    # legs (execute exactly what's on the sheet)
    raw_legs = [g("occ_buy_put"), g("occ_sell_put"), g("occ_sell_call"), g("occ_buy_call")]
    if not all(raw_legs):
        bail("MISSING_LEGS")

    # De-dupe by same raw legs already logged as placed/order
    all_rows = s.spreadsheets().values().get(
        spreadsheetId=sheet_id, range=f"{SCHWAB_TAB}!A1:Z100000"
    ).execute().get("values", [])
    if all_rows:
        hh, data = all_rows[0], all_rows[1:]
        hidx = {n:i for i,n in enumerate(hh)}
        def cell(r,c): j = hidx.get(c,-1); return (r[j] if 0<=j<len(r) else "").upper()
        sig_legs = [x.upper() for x in raw_legs]
        for r in data:
            if cell(r,"source") in ("SCHWAB_PLACED","SCHWAB_ORDER"):
                if [cell(r,"occ_buy_put"),cell(r,"occ_sell_put"),cell(r,"occ_sell_call"),cell(r,"occ_buy_call")] == sig_legs:
                    print("Duplicate legs already logged; skip placing.")
                    sys.exit(0)

    # convert to OCC
    try:
        leg_syms = [to_schwab_opt(x) for x in raw_legs]
    except Exception as e:
        bail(f"SYMBOL_ERR: {str(e)[:180]}")

    qty_exec = int((g("qty_exec") or "1"))
    side = (g("side") or "").upper()
    credit_or_debit = (g("credit_or_debit") or "").lower()
    is_credit = (credit_or_debit == "credit") or side.startswith("SHORT")
    order_type = "NET_CREDIT" if is_credit else "NET_DEBIT"

    # ladder knobs & pricing
    def build_order(price: float):
        return {
            "orderType": order_type,
            "session": "NORMAL",
            "price": f"{price:.2f}",
            "duration": "DAY",
            "orderStrategyType": "SINGLE",
            "complexOrderStrategyType": "IRON_CONDOR",
            "orderLegCollection": [
                {"instruction":"BUY_TO_OPEN","quantity":qty_exec,"instrument":{"symbol":leg_syms[0],"assetType":"OPTION"}},
                {"instruction":"SELL_TO_OPEN","quantity":qty_exec,"instrument":{"symbol":leg_syms[1],"assetType":"OPTION"}},
                {"instruction":"SELL_TO_OPEN","quantity":qty_exec,"instrument":{"symbol":leg_syms[2],"assetType":"OPTION"}},
                {"instruction":"BUY_TO_OPEN","quantity":qty_exec,"instrument":{"symbol":leg_syms[3],"assetType":"OPTION"}}
            ]
        }

    mid, net_bid, net_ask = compute_mid(c, leg_syms)
    start_price = FALLBACK_NET_PRICE if mid is None else mid
    if is_credit:
        start_price = max(start_price, MIN_CREDIT_START)
    else:
        start_price = min(start_price, MAX_DEBIT_START)
    start_price = round_to_tick(start_price, TICK)

    # place
    order = build_order(start_price)
    order_id = ""; status_code = None
    try:
        ok = c.place_order(acct_hash, order)
        status_code = ok.status_code
        try:
            j = ok.json()
            order_id = str(j.get("orderId") or j.get("order_id") or "")
        except Exception:
            order_id = ok.headers.get("Location","").rstrip("/").split("/")[-1]
        print("PLACE_HTTP", status_code, "ORDER_ID", order_id, "PRICE", order["price"])
    except Exception as e:
        bail(f"ORDER_ERROR: {e}")

    if not (isinstance(status_code,int) and 200 <= status_code < 300):
        bail(f"PLACE_HTTP_{status_code}")

    # helpers for monitoring & replace
    def get_order_status(oid: str):
        try:
            url = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{oid}"
            r = c.session.get(url)
            if r.status_code != 200:
                return (r.status_code, None)
            j = r.json()
            return (r.status_code, (j.get("status") or j.get("orderStatus")) )
        except Exception:
            return (None, None)

    def replace_price(oid: str, new_price: float):
        new_price = round_to_tick(new_price, TICK)
        new_order = build_order(new_price)
        url = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{oid}"
        r = c.session.put(url, json=new_order)
        new_id = oid
        try:
            j = r.json()
            new_id = str(j.get("orderId") or j.get("order_id") or oid)
        except Exception:
            loc = r.headers.get("Location","")
            if loc:
                new_id = loc.rstrip("/").split("/")[-1] or oid
        print("REPLACE_HTTP", r.status_code, "ORDER_ID", new_id, "PRICE", f"{new_price:.2f}")
        return (r.status_code, new_id, f"{new_price:.2f}")

    # ladder
    filled = False
    current_id = order_id
    price_used = order["price"]

    for step in range(MAX_STEPS):
        time.sleep(DELAY_SEC)
        sc, st = get_order_status(current_id)
        if st and str(st).upper() == "FILLED":
            filled = True
            print("ORDER_STATUS FILLED")
            break
        if st and str(st).upper() in ("CANCELED","REJECTED"):
            print(f"ORDER_STATUS {st}")
            break
        mid, net_bid, net_ask = compute_mid(c, leg_syms)
        if mid is None:
            print("QUOTE_MISS step", step+1)
            continue
        if step < 2:
            target = mid
        else:
            target = mid - OFFSET_CREDIT if is_credit else mid + OFFSET_DEBIT
        target = round_to_tick(target, TICK)
        if target != float(price_used):
            rh, new_id, new_price = replace_price(current_id, target)
            if isinstance(rh,int) and 200 <= rh < 400:
                current_id = new_id
                price_used = new_price

    # cancel if still not filled
    final_status = None
    if not filled and env_str("CANCEL_REMAINING","true").lower()=="true" and current_id:
        try:
            url = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders/{current_id}"
            r = c.session.delete(url)
            print("CANCEL_HTTP", r.status_code)
            final_status = f"CANCELED_AFTER_{MAX_STEPS}"
        except Exception as e:
            print("CANCEL_ERROR", e)
            final_status = "CANCEL_ERROR"
    else:
        sc, st = get_order_status(current_id)
        final_status = st or status_code

    # log final row
    try:
        root_label = (raw_legs[0].lstrip(".").replace("_","")[:6]).rstrip()
        root_label = re.match(r'^[A-Z.$^]+', root_label).group(0)
    except Exception:
        root_label = "SPXW"
    top_insert(s, sheet_id, sheet_id_num)
    s.spreadsheets().values().update(
        spreadsheetId=sheet_id, range=f"{SCHWAB_TAB}!A2",
        valueInputOption="USER_ENTERED",
        body={"values":[[
            datetime.now(timezone.utc).isoformat(),
            ("SCHWAB_PLACED" if filled else "SCHWAB_ERROR"),
            acct_hash, acct_last4, root_label, last_px,
            sig, "PLACE", side, qty_exec, ("NET_CREDIT" if is_credit else "NET_DEBIT"), price_used,
            raw_legs[0], raw_legs[1], raw_legs[2], raw_legs[3],
            current_id, str(final_status)
        ]]}
    ).execute()

    print("LEGS_FORMATTED", [to_schwab_opt(x) for x in raw_legs])
    print("FINAL_STATUS", final_status, "ORDER_ID", current_id, "PRICE_USED", price_used)

if __name__ == "__main__":
    main()

