# scripts/leocross_to_sheet.py
import os, json, re, subprocess, sys
from datetime import datetime, timezone
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

def env_int(name: str, default: int) -> int:
    s = os.environ.get(name, None)
    if s is None:
        return default
    s = str(s).strip()
    if s == "":
        return default
    try:
        return int(float(s))
    except Exception:
        return default

def env_or_die(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        print(f"Missing required env: {name}", file=sys.stderr)
        sys.exit(1)
    return val

def find_ticket_script() -> str:
    # Look for leocross_ticket.py in repo
    for root, _, files in os.walk(".", topdown=True):
        if "leocross_ticket.py" in files:
            return os.path.join(root, "leocross_ticket.py")
    print("leocross_ticket.py not found in repo", file=sys.stderr)
    sys.exit(1)

def to_int(x):
    try:
        return int(float(str(x).strip()))
    except Exception:
        return None

def main():
    gw_token   = env_or_die("GW_TOKEN")
    sheet_id   = env_or_die("GSHEET_ID")
    sa_json    = env_or_die("GOOGLE_SERVICE_ACCOUNT_JSON")
    size_credit = env_int("LEO_SIZE_CREDIT", 3)
    size_debit  = env_int("LEO_SIZE_DEBIT", 1)

    # 1) Fetch LeoCross JSON
    url = "https://gandalf.gammawizard.com/rapi/GetLeoCross"
    r = requests.get(url, headers={"Authorization": f"Bearer {gw_token}"}, timeout=30)
    if r.status_code != 200:
        print(f"LeoCross GET failed: {r.status_code} {r.text[:400]}", file=sys.stderr)
        sys.exit(1)
    api = r.json()

    # 2) Run your ticket generator
    script = find_ticket_script()
    cp = subprocess.run([sys.executable, script], capture_output=True, text=True)
    if cp.returncode != 0:
        print(cp.stdout)
        print(cp.stderr, file=sys.stderr)
        print("leocross_ticket.py failed", file=sys.stderr)
        sys.exit(1)
    txt = cp.stdout

    # 3) Parse fields from the script output
    m1 = re.search(r'(\d{4}-\d{2}-\d{2})\s*(?:->|\u2192)\s*(\d{4}-\d{2}-\d{2})\s*:\s*([A-Z_]+)\s*qty\s*=\s*(\d+)\s*width\s*=\s*([0-9.]+)', txt)
    signal_date = m1.group(1) if m1 else ""
    expiry      = m1.group(2) if m1 else ""
    side        = (m1.group(3) if m1 else "") or ""
    orig_qty    = to_int(m1.group(4)) if m1 else None
    width       = m1.group(5) if m1 else ""

    mS = re.search(r'Strikes?\s+P\s+(\d+(?:\.\d+)?)\s*/\s*(\d+(?:\.\d+)?)\s+C\s+(\d+(?:\.\d+)?)\s*/\s*(\d+(?:\.\d+)?)', txt, re.I)
    p_low  = mS.group(1) if mS else ""; p_high = mS.group(2) if mS else ""
    c_low  = mS.group(3) if mS else ""; c_high = mS.group(4) if mS else ""
    put_spread  = f"{p_low}/{p_high}" if p_low and p_high else ""
    call_spread = f"{c_low}/{c_high}" if c_low and c_high else ""

    mP = re.search(r'Probs.*?Cat1\s*=\s*([0-9.]+)\s+Cat2\s*=\s*([0-9.]+)', txt, re.I)
    cat1 = mP.group(1) if mP else ""
    cat2 = mP.group(2) if mP else ""
    mW = re.search(r'Worst[^A-Za-z0-9]+case(?:\s+day)?\s+loss:\s*\$?([0-9,.-]+)', txt, re.I)
    worst_day_loss = (mW.group(1).replace(',','') if mW else "")

    legs = re.findall(r'\b(BUY|SELL)\s+([A-Z0-9_.]+[CP]\d+)', txt, re.I)
    def pick(leg_list, instr, pc):
        for instr_, sym in leg_list:
            if instr_.upper()==instr and re.search(pc, sym, re.I): return sym
        return ""
    occ_buy_put   = pick(legs,"BUY", r'P')
    occ_sell_put  = pick(legs,"SELL",r'P')
    occ_sell_call = pick(legs,"SELL",r'C')
    occ_buy_call  = pick(legs,"BUY", r'C')

    # Trade snapshot (from API)
    trade = {}
    if isinstance(api.get("Trade"), list) and api["Trade"]:
        trade = api["Trade"][-1]
    elif isinstance(api.get("Trade"), dict):
        trade = api["Trade"]

    spx  = str(trade.get("SPX",""))
    vix  = str(trade.get("VIX",""))
    rv5  = str(trade.get("RV5","")); rv10 = str(trade.get("RV10","")); rv20 = str(trade.get("RV20",""))
    dte  = str(trade.get("M",""))
    summary = next((ln.strip() for ln in txt.splitlines() if ln.strip()), "")

    # Sizing: 3 credit, 1 debit (override via env)
    s_up = side.upper()
    credit = (s_up.startswith("SHORT") or "CREDIT" in s_up)
    qty_exec = size_credit if credit else size_debit
    credit_or_debit = "credit" if credit else "debit"

    # 4) Sheets: ensure header, insert at row 2, write row
    sa_info = json.loads(sa_json)
    creds = service_account.Credentials.from_service_account_info(sa_info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    svc = build("sheets","v4",credentials=creds)

    got = svc.spreadsheets().values().get(spreadsheetId=sheet_id, range=f"{LEO_TAB}!1:1").execute().get("values", [])
    if not got or got[0] != HEADERS:
        svc.spreadsheets().values().update(
            spreadsheetId=sheet_id, range=f"{LEO_TAB}!1:1",
            valueInputOption="USER_ENTERED", body={"values":[HEADERS]}
        ).execute()

    meta = svc.spreadsheets().get(spreadsheetId=sheet_id).execute()
    sheet_id_num = None
    for sh in meta.get("sheets", []):
        if sh["properties"]["title"] == LEO_TAB:
            sheet_id_num = sh["properties"]["sheetId"]; break
    if sheet_id_num is None:
        print(f"Tab '{LEO_TAB}' not found.", file=sys.stderr); sys.exit(1)

    # Insert a blank row at row 2
    svc.spreadsheets().batchUpdate(
        spreadsheetId=sheet_id,
        body={"requests":[{"insertDimension":{
            "range":{"sheetId":sheet_id_num,"dimension":"ROWS","startIndex":1,"endIndex":2},
            "inheritFromBefore": False
        }}]}
    ).execute()

    row = [
        datetime.now(timezone.utc).isoformat(),
        signal_date, expiry, side, credit_or_debit, (orig_qty if orig_qty is not None else ""), qty_exec, width,
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

if __name__ == "__main__":
    main()
