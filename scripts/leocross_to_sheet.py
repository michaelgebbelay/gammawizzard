# scripts/leocross_to_sheet.py
import os, json, sys
from datetime import datetime, timezone, date
import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build

LEO_TAB = "leocross"

HEADERS = [
    "ts","signal_date","expiry","side","credit_or_debit","orig_qty","qty_exec","width",
    "put_spread","call_spread",
    "occ_buy_put","occ_sell_put","occ_sell_call","occ_buy_call",
    "cat1","cat2","worst_day_loss",
    "spx","vix","rv20","rv10","rv5","dte",
    "summary"
]

# ---------- robust env helpers ----------
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

def env_int(name: str, default: int) -> int:
    s = os.environ.get(name, None)
    if s is None: return default
    s = str(s).strip()
    if s == "": return default
    try:
        return int(float(s))
    except Exception:
        return default

# ---------- utils ----------
def yymmdd(iso: str) -> str:
    # iso like '2025-08-21' -> '250821'
    try:
        d = date.fromisoformat(iso)
        return f"{d:%y%m%d}"
    except Exception:
        return ""

def to_int(x):
    try:
        return int(round(float(x)))
    except Exception:
        return None

def main():
    gw_token   = env_or_die("GW_TOKEN")
    sheet_id   = env_or_die("GSHEET_ID")
    sa_json    = env_or_die("GOOGLE_SERVICE_ACCOUNT_JSON")

    # Defaults you asked for
    width       = env_int("LEO_WIDTH", 5)         # strike width; default 5
    size_credit = env_int("LEO_SIZE_CREDIT", 3)   # 3 for credit
    size_debit  = env_int("LEO_SIZE_DEBIT", 1)    # 1 for debit

    # 1) Fetch LeoCross JSON
    url = "https://gandalf.gammawizard.com/rapi/GetLeoCross"
    r = requests.get(url, headers={"Authorization": f"Bearer {gw_token}"}, timeout=30)
    if r.status_code != 200:
        print(f"LeoCross GET failed: {r.status_code} {r.text[:400]}", file=sys.stderr)
        sys.exit(1)
    api = r.json()

    # 2) Pull the latest Trade block
    trade = {}
    if isinstance(api.get("Trade"), list) and api["Trade"]:
        trade = api["Trade"][-1]
    elif isinstance(api.get("Trade"), dict):
        trade = api["Trade"]
    if not trade:
        print("No Trade block in API response", file=sys.stderr)
        sys.exit(1)

    signal_date = str(trade.get("Date",""))
    expiry      = str(trade.get("TDate",""))
    side        = "SHORT_IRON_CONDOR"       # LeoCross generates this structure
    credit      = True                      # SHORT_* => credit
    credit_or_debit = "credit" if credit else "debit"
    qty_exec    = size_credit if credit else size_debit

    # Inner strikes from API
    inner_put  = to_int(trade.get("Limit", ""))   # e.g., 6370
    inner_call = to_int(trade.get("CLimit",""))   # e.g., 6425
    if inner_put is None or inner_call is None:
        print("Missing Limit/CLimit in Trade; cannot build strikes.", file=sys.stderr)
        # Write a row that will cause Schwab step to skip
        inner_put = inner_call = 0

    # Build spreads using width
    p1 = inner_put - width; p2 = inner_put
    c1 = inner_call;        c2 = inner_call + width
    put_spread  = f"{p1}/{p2}" if inner_put else ""
    call_spread = f"{c1}/{c2}" if inner_call else ""

    # Make SPXW UI leg symbols that Schwab converter already understands
    # .SPXWYYMMDDP6365 / .SPXWYYMMDDP6370 / .SPXWYYMMDDC6425 / .SPXWYYMMDDC6430
    exp6 = yymmdd(expiry)
    occ_buy_put   = f".SPXW{exp6}P{p1}" if exp6 and p1 else ""
    occ_sell_put  = f".SPXW{exp6}P{p2}" if exp6 and p2 else ""
    occ_sell_call = f".SPXW{exp6}C{c1}" if exp6 and c1 else ""
    occ_buy_call  = f".SPXW{exp6}C{c2}" if exp6 and c2 else ""

    # Stats
    spx  = str(trade.get("SPX",""))
    vix  = str(trade.get("VIX",""))
    rv5  = str(trade.get("RV5",""))
    rv10 = str(trade.get("RV10",""))
    rv20 = str(trade.get("RV20",""))
    dte  = str(trade.get("M",""))
    cat1 = str(trade.get("Cat1",""))
    cat2 = str(trade.get("Cat2",""))

    # Worst-day loss (approx; width * 100 * qty_exec). If you want blank, set LEO_WDL=blank
    worst_day_loss = str(width * 100 * qty_exec)

    summary = f"{signal_date} → {expiry} : {side} qty={qty_exec} width={width} (API-derived)"

    # 3) Sheets write at A2
    sa_info = json.loads(sa_json)
    creds = service_account.Credentials.from_service_account_info(sa_info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    svc = build("sheets","v4",credentials=creds)

    # Ensure header
    got = svc.spreadsheets().values().get(spreadsheetId=sheet_id, range=f"{LEO_TAB}!1:1").execute().get("values", [])
    if not got or got[0] != HEADERS:
        svc.spreadsheets().values().update(
            spreadsheetId=sheet_id, range=f"{LEO_TAB}!1:1",
            valueInputOption="USER_ENTERED", body={"values":[HEADERS]}
        ).execute()

    # Get sheetId to insert at row 2
    meta = svc.spreadsheets().get(spreadsheetId=sheet_id).execute()
    sheet_id_num = next(sh["properties"]["sheetId"] for sh in meta["sheets"] if sh["properties"]["title"] == LEO_TAB)

    svc.spreadsheets().batchUpdate(
        spreadsheetId=sheet_id,
        body={"requests":[{"insertDimension":{
            "range":{"sheetId":sheet_id_num,"dimension":"ROWS","startIndex":1,"endIndex":2},
            "inheritFromBefore": False
        }}]}
    ).execute()

    row = [
        datetime.now(timezone.utc).isoformat(),
        signal_date, expiry, side, credit_or_debit, "", qty_exec, str(width),
        put_spread, call_spread,
        occ_buy_put, occ_sell_put, occ_sell_call, occ_buy_call,
        cat1, cat2, worst_day_loss,
        spx, vix, rv20, rv10, rv5, dte,
        summary
    ]
    svc.spreadsheets().values().update(
        spreadsheetId=sheet_id, range=f"{LEO_TAB}!A2",
        valueInputOption="USER_ENTERED", body={"values":[row]}
    ).execute()

    print(f"leocross: inserted at A2 (qty_exec={qty_exec}, {credit_or_debit})")
    print("legs", occ_buy_put, occ_sell_put, occ_sell_call, occ_buy_call)

if __name__ == "__main__":
    main()
