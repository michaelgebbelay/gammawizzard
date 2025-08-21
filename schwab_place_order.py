# scripts/schwab_place_order.py
import os, re, json, sys
from datetime import datetime, timezone
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

def env_or_die(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        print(f"Missing required env: {name}", file=sys.stderr)
        sys.exit(1)
    return val

def to_schwab_opt(sym: str) -> str:
    """Convert UI/OCC variations to Schwab 21-char OCC: ROOT(6) + YYMMDD + C/P + STRIKE(8)"""
    raw = (sym or "").strip().upper()
    if raw.startswith("."): raw = raw[1:]      # handle UI dot prefix
    raw = raw.replace("_","")
    # Already OCC shape without padding root
    m = re.match(r'^([A-Z.$^]{1,6})(\d{6})([CP])(\d{8})$', raw)
    if m:
        root, ymd, cp, strike8 = m.groups()
        if root != "SPXW": raise ValueError(f"Non-SPXW root: {root}")
        return f"{root:<6}{ymd}{cp}{strike8}"
    # UI shape like SPXW250821P6365(.ddd)
    m = re.match(r'^([A-Z.$^]{1,6})(\d{6})([CP])(\d{1,5})(?:\.(\d{1,3}))?$', raw)
    if m:
        root, ymd, cp, i, frac = m.groups()
        if root != "SPXW": raise ValueError(f"Non-SPXW root: {root}")
        mills = int(i)*1000 + (int(frac.ljust(3,'0')) if frac else 0)
        strike8 = f"{mills:08d}"
        return f"{root:<6}{ymd}{cp}{strike8}"
    # Already left-padded to 6 root chars
    m = re.match(r'^(.{6})(\d{6})([CP])(\d{8})$', sym or "")
    if m:
        root6, ymd, cp, strike8 = m.groups()
        if not root6.strip().upper().startswith("SPXW"): raise ValueError(f"Non-SPXW root: {root6}")
        return f"{root6}{ymd}{cp}{strike8}"
    raise ValueError(f"Cannot parse option symbol: {sym}")

def main():
    # Toggle must be 'place' to actually submit
    place_toggle = (os.environ.get("SCHWAB_PLACE","") or os.environ.get("SCHWAB_PLACE_VAR","") or os.environ.get("SCHWAB_PLACE_SEC","")).lower()
    if place_toggle != "place":
        print("SCHWAB_PLACE not 'place' → skipping order placement.")
        sys.exit(0)

    app_key    = env_or_die("SCHWAB_APP_KEY")
    app_secret = env_or_die("SCHWAB_APP_SECRET")
    token_json = env_or_die("SCHWAB_TOKEN_JSON")
    sheet_id   = env_or_die("GSHEET_ID")
    sa_json    = env_or_die("GOOGLE_SERVICE_ACCOUNT_JSON")
    limit_price = str(os.environ.get("NET_PRICE") or "0.05")

    # Rehydrate Schwab token
    with open("schwab_token.json","w") as f: f.write(token_json)
    c = client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")

    # Account info
    r = c.get_account_numbers(); r.raise_for_status()
    acct = r.json()[0]
    acct_hash  = acct["hashValue"]
    acct_last4 = acct["accountNumber"][-4:]

    # Sheets client
    sa_info = json.loads(sa_json)
    creds = service_account.Credentials.from_service_account_info(sa_info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    s = build("sheets","v4",credentials=creds)

    # Ensure SCHWAB header
    got = s.spreadsheets().values().get(spreadsheetId=sheet_id, range=f"{SCHWAB_TAB}!1:1").execute().get("values", [])
    if not got or got[0] != SCHWAB_HEADERS:
        s.spreadsheets().values().update(
            spreadsheetId=sheet_id, range=f"{SCHWAB_TAB}!1:1",
            valueInputOption="USER_ENTERED", body={"values":[SCHWAB_HEADERS]}
        ).execute()

    # sheetId for top insert
    meta = s.spreadsheets().get(spreadsheetId=sheet_id).execute()
    sheet_id_num = next(sh["properties"]["sheetId"] for sh in meta["sheets"] if sh["properties"]["title"] == SCHWAB_TAB)

    def top_insert():
        s.spreadsheets().batchUpdate(
            spreadsheetId=sheet_id,
            body={"requests":[{"insertDimension":{
                "range":{"sheetId":sheet_id_num,"dimension":"ROWS","startIndex":1,"endIndex":2},
                "inheritFromBefore": False
            }}]}
        ).execute()

    # Read leocross header + row 2 (latest)
    two = s.spreadsheets().values().get(spreadsheetId=sheet_id, range=f"{LEO_TAB}!A1:Z2").execute().get("values", [])
    if len(two) < 2:
        print("No leocross row 2; nothing to place."); sys.exit(0)
    leo_header, leo_row2 = two[0], two[1]
    idx = {n:i for i,n in enumerate(leo_header)}
    def g(col):
        j = idx.get(col, -1)
        return leo_row2[j] if 0 <= j < len(leo_row2) else ""

    # Legs (must be SPXW-only)
    raw_legs = [g("occ_buy_put"), g("occ_sell_put"), g("occ_sell_call"), g("occ_buy_call")]
    if not all(raw_legs):
        print("Row 2 missing one or more leg symbols; nothing to place."); sys.exit(0)
    if not all((x or "").upper().startswith("SPXW") for x in raw_legs):
        print("Non-SPXW leg detected; enforcing SPXW-only → skip."); sys.exit(0)

    # De-dupe: if these raw legs already logged as placed, skip
    all_rows = s.spreadsheets().values().get(spreadsheetId=sheet_id, range=f"{SCHWAB_TAB}!A1:Z100000").execute().get("values", [])
    if all_rows:
        h, data = all_rows[0], all_rows[1:]
        hidx = {n:i for i,n in enumerate(h)}
        def cell(r,c):
            j = hidx.get(c,-1)
            return (r[j] if 0<=j<len(r) else "").upper()
        sig = [x.upper() for x in raw_legs]
        for r in data:
            if cell(r,"source") in ("SCHWAB_PLACED","SCHWAB_ORDER"):
                if [cell(r,"occ_buy_put"),cell(r,"occ_sell_put"),cell(r,"occ_sell_call"),cell(r,"occ_buy_call")] == sig:
                    print("Duplicate legs already logged; skip placing."); sys.exit(0)

    # Convert symbols to 21-char OCC
    try:
        leg_syms = [to_schwab_opt(x) for x in raw_legs]
    except Exception as e:
        print(f"Symbol conversion error: {e}", file=sys.stderr); sys.exit(1)

    qty_exec = int((g("qty_exec") or "1"))
    side = (g("side") or "").upper()
    credit_or_debit = (g("credit_or_debit") or "").lower()
    is_credit = (credit_or_debit == "credit") or side.startswith("SHORT")
    order_type = "NET_CREDIT" if is_credit else "NET_DEBIT"

    order = {
        "orderType": order_type,
        "session": "NORMAL",
        "price": str(limit_price),              # Schwab accepts string for price
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

    place_http = None; order_id = ""
    try:
        ok = c.place_order(acct_hash, order)
        place_http = ok.status_code
        try:
            j = ok.json()
            order_id = str(j.get("orderId") or j.get("order_id") or "")
        except Exception:
            order_id = ""
        print("PLACE_HTTP", place_http)
    except Exception as e:
        print("ORDER_ERROR", e)

    # Log the placed order row at top for dedupe
    top_insert()
    s.spreadsheets().values().update(
        spreadsheetId=sheet_id, range=f"{SCHWAB_TAB}!A2",
        valueInputOption="USER_ENTERED",
        body={"values":[[
            datetime.now(timezone.utc).isoformat(),
            "SCHWAB_PLACED",
            acct_hash, acct_last4, "SPXW", "",
            g("signal_date"), "PLACE", side, qty_exec, order_type, str(limit_price),
            raw_legs[0], raw_legs[1], raw_legs[2], raw_legs[3],
            order_id, (place_http if place_http is not None else "")
        ]]}
    ).execute()

    # Summary to stdout (Actions will capture)
    print("ORDER_JSON", json.dumps(order))
    print("LEGS_FORMATTED", leg_syms)
    print("ORDER_ID", order_id)

if __name__ == "__main__":
    main()
