# Schwab → Google Sheets exporter
# - Pulls recent orders and execution fills
# - Writes to 2 tabs: "orders" (one row per order), "fills" (one row per execution event)
# - Idempotent: skips orders already present in the Sheet
#
# Secrets (env):
#   SCHWAB_APP_KEY, SCHWAB_APP_SECRET, SCHWAB_TOKEN_JSON
#   GSHEET_ID, GOOGLE_SERVICE_ACCOUNT_JSON
# Optional (env):
#   DAYS_BACK (default 10), SYMBOL_FILTER (default "SPX")
#
# Sheet schemas:
#   orders: ts_entered, ts_last, order_id, status, qty, filled_qty, side, order_type, limit_price,
#           complex, buy_put, sell_put, sell_call, buy_call, fills_count, source
#   fills : ts_fill, order_id, qty_this_fill, net_est, legs_json

import os, sys, json, time, re
from datetime import datetime, timedelta, date, timezone
from zoneinfo import ZoneInfo
import requests

from schwab.auth import client_from_token_file
from google.oauth2 import service_account
from googleapiclient.discovery import build as gbuild

ET = ZoneInfo("America/New_York")

ORDERS_TAB = "orders"
FILLS_TAB  = "fills"

ORDERS_HEADERS = [
    "ts_entered","ts_last","order_id","status","qty","filled_qty",
    "side","order_type","limit_price","complex",
    "buy_put","sell_put","sell_call","buy_call",
    "fills_count","source"
]

FILLS_HEADERS = [
    "ts_fill","order_id","qty_this_fill","net_est","legs_json"
]

# ---------- small utils ----------
def iso_z(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _b64_or_raw_to_file(secret: str, path: str):
    s = secret or ""
    try:
        import base64
        dec = base64.b64decode(s).decode("utf-8")
        if dec.strip().startswith("{"):
            s = dec
    except Exception:
        pass
    open(path,"w").write(s)

def _sanitize_token(t: str) -> str:
    t = (t or "").strip().strip('"').strip("'")
    return t.split(None,1)[1] if t.lower().startswith("bearer ") else t

def _backoff(i): return 0.6*(2**i)

def yymmdd(iso: str) -> str:
    d = date.fromisoformat((iso or "")[:10])
    return "{:%y%m%d}".format(d)

def to_osi(sym: str) -> str:
    raw = (sym or "").upper()
    raw = re.sub(r'\s+', '', raw).lstrip('.')
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

def _osi_from_instrument(ins: dict) -> str | None:
    """Build robust OSI from Schwab instrument; fallback to structured fields."""
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

# ---------- Sheets helpers ----------
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

def append_top(svc, sheet_id_num, spreadsheet_id: str, tab: str, row_vals: list):
    top_insert(svc, spreadsheet_id, sheet_id_num)
    svc.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range=f"{tab}!A2",
        valueInputOption="USER_ENTERED", body={"values":[row_vals]}).execute()

def get_existing_order_ids(svc, spreadsheet_id: str, tab: str) -> set:
    try:
        # Column C is order_id per our schema (1-based index)
        resp = svc.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=f"{tab}!C2:C100000").execute()
        vals = resp.get("values", [])
        return { (r[0] if r else "").strip() for r in vals if r }
    except Exception:
        return set()

# ---------- Schwab HTTP helpers ----------
def schwab_get_json(c, url, params=None, tries=5, tag=""):
    last=""
    for i in range(tries):
        try:
            r=c.session.get(url, params=(params or {}), timeout=20)
            if r.status_code==200:
                return r.json()
            last = f"HTTP_{r.status_code}:{(r.text or '')[:200]}"
        except Exception as e:
            last = f"{type(e).__name__}:{str(e)}"
        time.sleep(_backoff(i))
    raise RuntimeError(f"SCHWAB_GET_FAIL({tag}) {last}")

# ---------- main ----------
def main():
    # Inputs
    days_back = int(os.environ.get("DAYS_BACK","10") or "10")
    symbol_filter = (os.environ.get("SYMBOL_FILTER","SPX") or "SPX").upper().replace("$","").replace("/","")
    sheet_id = os.environ["GSHEET_ID"]
    sa_json  = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]

    # Sheets client
    creds=service_account.Credentials.from_service_account_info(json.loads(sa_json),
        scopes=["https://www.googleapis.com/auth/spreadsheets"])
    svc=gbuild("sheets","v4",credentials=creds)
    orders_sid = ensure_header_and_get_sheetid(svc, sheet_id, ORDERS_TAB, ORDERS_HEADERS)
    fills_sid  = ensure_header_and_get_sheetid(svc, sheet_id, FILLS_TAB,  FILLS_HEADERS)

    existing = get_existing_order_ids(svc, sheet_id, ORDERS_TAB)

    # Schwab client
    token_path = "schwab_token.json"
    _b64_or_raw_to_file(os.environ.get("SCHWAB_TOKEN_JSON",""), token_path)
    c = client_from_token_file(api_key=os.environ["SCHWAB_APP_KEY"], app_secret=os.environ["SCHWAB_APP_SECRET"], token_path=token_path)

    r=c.get_account_numbers(); r.raise_for_status()
    acct_hash=r.json()[0]["hashValue"]

    # Window
    now = datetime.now(ET)
    start = now - timedelta(days=days_back)

    # Fetch orders
    url = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders"
    params = {"fromEnteredTime": iso_z(start), "toEnteredTime": iso_z(now), "maxResults": 500}
    orders = schwab_get_json(c, url, params=params, tag="ORDERS") or []

    count_orders=0; count_fills=0

    for o in orders:
        # Filter: option complex orders only; and by symbol root
        legs = (o.get("orderLegCollection") or [])
        if not legs: continue
        if not any(((leg.get("instrument") or {}).get("assetType","").upper() == "OPTION") for leg in legs):
            continue
        # Root filter (cheap)
        if symbol_filter:
            ok_any=False
            for leg in legs:
                ins=(leg.get("instrument") or {})
                sym=(ins.get("symbol") or "")
                if symbol_filter in sym.upper():
                    ok_any=True; break
            if not ok_any:
                continue

        order_id = str(o.get("orderId") or "")
        if not order_id: continue

        if order_id in existing:
            # already logged
            pass
        else:
            # build legs OSI in canonical order: buy_put, sell_put, sell_call, buy_call
            # collect all leg OSIs + instructions
            leg_objs=[]
            for leg in legs:
                ins=(leg.get("instrument") or {})
                osi=_osi_from_instrument(ins)
                if not osi: continue
                instr=(leg.get("instruction") or "").upper()
                leg_objs.append((osi, instr))
            # separate P / C
            puts = [x for x in leg_objs if x[0][12]=="P"]
            calls= [x for x in leg_objs if x[0][12]=="C"]
            # order by strike
            puts.sort(key=lambda x: strike_from_osi(x[0]))
            calls.sort(key=lambda x: strike_from_osi(x[0]))
            # infer buy/sell per instruction
            def pick_puts():
                if len(puts)>=2:
                    # lower strike is wing; for credit, wing is BUY
                    # but we just place by instruction labels
                    bp = next((p[0] for p in puts if "BUY"  in p[1]), puts[0][0])
                    sp = next((p[0] for p in puts if "SELL" in p[1]), puts[-1][0])
                    return bp, sp
                elif len(puts)==1:
                    return puts[0][0], ""
                return "",""
            def pick_calls():
                if len(calls)>=2:
                    bc = next((c[0] for c in calls if "BUY"  in c[1]), calls[-1][0])
                    sc = next((c[0] for c in calls if "SELL" in c[1]), calls[0][0])
                    return sc, bc   # return (sell_call, buy_call) for readability below
                elif len(calls)==1:
                    return calls[0][0], ""
                return "",""
            bp, sp = pick_puts()
            sc, bc = pick_calls()

            status = (o.get("status") or "").upper()
            qty    = int(round(float(o.get("quantity") or 0)))
            fqty   = int(round(float(o.get("filledQuantity") or 0)))
            side   = (o.get("complexOrderStrategyType") or o.get("orderStrategyType") or "").upper()
            order_type = (o.get("orderType") or "").upper()     # NET_CREDIT / NET_DEBIT / LIMIT etc
            price  = o.get("price") if isinstance(o.get("price"), (int,float,str)) else ""
            complex_name = (o.get("complexOrderStrategyType") or "").upper()

            # timestamps (best-effort in API)
            ts_entered = o.get("enteredTime") or o.get("enteredTimeUTC") or o.get("orderEnteredTime") or ""
            ts_last    = o.get("closeTime") or o.get("lastUpdateTime") or ts_entered or ""

            row = [ts_entered, ts_last, order_id, status, qty, fqty,
                   side, order_type, ("" if price=="" else f"{float(price):.2f}" if isinstance(price,(int,float)) else str(price)),
                   complex_name,
                   bp, sp, sc, bc,
                   len(o.get("orderActivityCollection") or []),
                   "SCHWAB_API"]
            append_top(svc, orders_sid, sheet_id, ORDERS_TAB, row)
            existing.add(order_id)
            count_orders += 1

        # Fills: one row per execution event
        acts = (o.get("orderActivityCollection") or [])
        if not acts: continue

        # map legId -> instruction to compute net_est
        leg_instr = {}
        for idx, leg in enumerate(legs):
            lid = leg.get("legId")
            if lid is None: lid = idx+1
            leg_instr[int(lid)] = (leg.get("instruction") or "").upper()

        for a in acts:
            et = (a.get("executionLegs") or [])
            if not et: continue
            # qty in this event (min across legs)
            try:
                qty_this = int(min([int(round(float(x.get("quantity") or 0))) for x in et if (x.get("quantity") is not None)]))
            except Exception:
                qty_this = 0
            # net estimate: sum(sell leg prices) - sum(buy leg prices)
            net = 0.0; have_any=False
            for x in et:
                px = x.get("price")
                if px is None: continue
                try:
                    px = float(px)
                except Exception:
                    continue
                lid = int(x.get("legId") or 0)
                instr = leg_instr.get(lid,"")
                if "SELL" in instr:
                    net += px; have_any=True
                elif "BUY" in instr:
                    net -= px; have_any=True
            net_str = ("" if not have_any else f"{net:.2f}")
            # fill ts (prefer explicit time on execution leg)
            ts = (et[0].get("time") or a.get("time") or a.get("activityTs") or "")
            legs_json = json.dumps(et)[:950]  # avoid cell bloat
            rowf=[ts, str(o.get("orderId") or ""), qty_this, net_str, legs_json]
            append_top(svc, fills_sid, sheet_id, FILLS_TAB, rowf)
            count_fills += 1

    print(f"EXPORT DONE: inserted {count_orders} orders, {count_fills} fills")

if __name__=="__main__":
    sys.exit(main())
