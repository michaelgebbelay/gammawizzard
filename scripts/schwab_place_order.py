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

# --------- helpers ---------
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

def to_schwab_opt(sym: str) -> str:
    """
    Convert various UI/OCC variants to Schwab 21-char OCC/OSI:
      ROOT padded to 6 chars + YYMMDD + C/P + STRIKE(8, mills)
    Accepts:
      .SPXW250821P6365
      SPXW_250821P06365000
      'SPXW  250821P06365000'
      SPX250821P6365
      SPX  250821P06365000
    """
    raw = (sym or "").strip().upper()
    if raw.startswith("."): raw = raw[1:]       # strip UI dot
    raw = raw.replace("_","")

    # Already OCC-like without padded root (ROOT<=6 + YYMMDD + C/P + 8-digit)
    m = re.match(r'^([A-Z.$^]{1,6})(\d{6})([CP])(\d{8})$', raw)
    if m:
        root, ymd, cp, strike8 = m.groups()
        return f"{root:<6}{ymd}{cp}{strike8}"

    # UI-like with non-padded strike (e.g., SPXW250821P6365 or SPX250821C6425.5)
    m = re.match(r'^([A-Z.$^]{1,6})(\d{6})([CP])(\d{1,5})(?:\.(\d{1,3}))?$', raw)
    if m:
        root, ymd, cp, i, frac = m.groups()
        mills = int(i)*1000 + (int(frac.ljust(3,'0')) if frac else 0)
        strike8 = f"{mills:08d}"
        return f"{root:<6}{ymd}{cp}{strike8}"

    # Already padded 6-char root
    m = re.match(r'^(.{6})(\d{6})([CP])(\d{8})$', sym or "")
    if m:
        root6, ymd, cp, strike8 = m.groups()
        return f"{root6}{ymd}{cp}{strike8}"

    raise ValueError(f"Cannot parse option symbol: {sym}")

def main():
    # Must be 'place' (var/secret) to submit
    place_toggle = (env_str("SCHWAB_PLACE") or env_str("SCHWAB_PLACE_VAR") or env_str("SCHWAB_PLACE_SEC")).lower()
    if place_toggle != "place":
        print("SCHWAB_PLACE not 'place' → skipping order placement.")
        sys.exit(0)

    app_key     = env_or_die("SCHWAB_APP_KEY")
    app_secret  = env_or_die("SCHWAB_APP_SECRET")
    token_json  = env_or_die("SCHWAB_TOKEN_JSON")
    sheet_id    = env_or_die("GSHEET_ID")
    sa_json     = env_or_die("GOOGLE_SERVICE_ACCOUNT_JSON")
    limit_price = env_str("NET_PRICE", "0.05")  # default 0.05

    # Schwab client
    with open("schwab_token.json","w") as f: f.write(token_json)
    c = client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")

    # Account
    r = c.get_account_numbers(); r.raise_for_status()
    acct = r.json()[0]
    acct_hash  = acct["hashValue"]
    acct_last4 = acct["accountNumber"][-4:]

    # Optional: SPX index last for logging
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

    # Read leocross header + row 2
    two = s.spreadsheets().values().get(spreadsheetId=sheet_id, range=f"{LEO_TAB}!A1:Z2").execute().get("values", [])
    if len(two) < 2:
        print("No leocross row 2; nothing to place."); sys.exit(0)
    leo_header, leo_row2 = two[0], two[1]
    idx = {n:i for i,n in enumerate(leo_header)}
    def g(col):
        j = idx.get(col, -1)
        return leo_row2[j] if 0 <= j < len(leo_row2) else ""

    # Legs — execute exactly what's on the sheet
    raw_legs = [g("occ_buy_put"), g("occ_sell_put"), g("occ_sell_call"), g("occ_buy_call")]
    if not all(raw_legs):
        # still log a row so you can see why it skipped
        top_insert()
        s.spreadsheets().values().update(
            spreadsheetId=sheet_id, range=f"{SCHWAB_TAB}!A2",
            valueInputOption="USER_ENTERED",
            body={"values":[[
                datetime.now(timezone.utc).isoformat(),
                "SCHWAB_ERROR",
                acct_hash, acct_last4, "", last_px,
                g("signal_date"), "PLACE", g("side"), g("qty_exec"), "", limit_price,
                raw_legs[0] or "", raw_legs[1] or "", raw_legs[2] or "", raw_legs[3] or "",
                "", "MISSING_LEGS"
            ]]}
        ).execute()
        sys.exit(0)

    # Dedupe: skip if already logged for same raw legs (case-insensitive)
    all_rows = s.spreadsheets().values().get(spreadsheetId=sheet_id, range=f"{SCHWAB_TAB}!A1:Z100000").execute().get("values", [])
    if all_rows:
        h, data = all_rows[0], all_rows[1:]
        hidx = {n:i for i,n in enumerate(h)}
        def cell(r,c):
            j = hidx.get(c,-1); return (r[j] if 0<=j<len(r) else "").upper()
        sig = [x.upper() for x in raw_legs]
        for r in data:
            if cell(r,"source") in ("SCHWAB_PLACED","SCHWAB_ORDER"):
                if [cell(r,"occ_buy_put"),cell(r,"occ_sell_put"),cell(r,"occ_sell_call"),cell(r,"occ_buy_call")] == sig:
                    print("Duplicate legs already logged; skip placing.")
                    sys.exit(0)

    # Convert to Schwab 21-char OCC
    try:
        leg_syms = [to_schwab_opt(x) for x in raw_legs]
    except Exception as e:
        # Log conversion error row, then stop
        top_insert()
        s.spreadsheets().values().update(
            spreadsheetId=sheet_id, range=f"{SCHWAB_TAB}!A2",
            valueInputOption="USER_ENTERED",
            body={"values":[[
                datetime.now(timezone.utc).isoformat(),
                "SCHWAB_ERROR",
                acct_hash, acct_last4, "", last_px,
                g("signal_date"), "PLACE", g("side"), g("qty_exec"), "", limit_price,
                raw_legs[0], raw_legs[1], raw_legs[2], raw_legs[3],
                "", f"SYMBOL_ERR: {str(e)[:180]}"
            ]]}
        ).execute()
        sys.exit(1)

    # Build order from sheet fields
    qty_exec = int((g("qty_exec") or "1"))
    side = (g("side") or "").upper()
    credit_or_debit = (g("credit_or_debit") or "").lower()
    is_credit = (credit_or_debit == "credit") or side.startswith("SHORT")
    order_type = "NET_CREDIT" if is_credit else "NET_DEBIT"

    order = {
        "orderType": order_type,
        "session": "NORMAL",
        "price": str(limit_price),
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

    # Place ONLY
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
        # network or API exception
        place_http = ""
        print("ORDER_ERROR", e)

    # Log result at top for dedupe (2xx = PLACED; else ERROR)
    src = "SCHWAB_PLACED" if (place_http and isinstance(place_http,int) and 200 <= place_http < 300) else "SCHWAB_ERROR"
    # Derive a human symbol label from first leg's root (purely cosmetic)
    try:
        root_label = (raw_legs[0].lstrip(".").replace("_","")[:6]).rstrip()
        # strip digits off
        root_label = re.match(r'^[A-Z.$^]+', root_label).group(0)
    except Exception:
        root_label = ""
    top_insert()
    s.spreadsheets().values().update(
        spreadsheetId=sheet_id, range=f"{SCHWAB_TAB}!A2",
        valueInputOption="USER_ENTERED",
        body={"values":[[
            datetime.now(timezone.utc).isoformat(),
            src,
            acct_hash, acct_last4, root_label, last_px,
            g("signal_date"), "PLACE", side, qty_exec, order_type, str(limit_price),
            raw_legs[0], raw_legs[1], raw_legs[2], raw_legs[3],
            order_id, (place_http if place_http != "" else "EXC")
        ]]}
    ).execute()

    # Also print useful bits to logs
    print("LEGS_FORMATTED", leg_syms)
    print("ORDER_ID", order_id)

if __name__ == "__main__":
    main()
