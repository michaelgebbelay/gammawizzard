# scripts/leocross_to_sheet.py
import os, json, sys, re, subprocess
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

def yymmdd(iso: str) -> str:
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

def find_ticket_script():
    for root, _, files in os.walk(".", topdown=True):
        if "leocross_ticket.py" in files:
            return os.path.join(root, "leocross_ticket.py")
    return None

def parse_ticket_output(txt: str):
    # returns dict or None
    m1 = re.search(r'(\d{4}-\d{2}-\d{2})\s*(?:->|\u2192)\s*(\d{4}-\d{2}-\d{2})\s*:\s*([A-Z_]+)\s*qty\s*=\s*(\d+)\s*width\s*=\s*([0-9.]+)', txt)
    if not m1: return None
    signal_date = m1.group(1); expiry = m1.group(2); side = m1.group(3)
    width = m1.group(5); orig_qty = m1.group(4)
    mS = re.search(r'Strikes?\s+P\s+(\d+(?:\.\d+)?)\s*/\s*(\d+(?:\.\d+)?)\s+C\s+(\d+(?:\.\d+)?)\s*/\s*(\d+(?:\.\d+)?)', txt, re.I)
    p1 = mS.group(1) if mS else ""; p2 = mS.group(2) if mS else ""
    c1 = mS.group(3) if mS else ""; c2 = mS.group(4) if mS else ""
    occ = re.findall(r'\b(BUY|SELL)\s+([A-Z0-9_.]+[CP]\d+)', txt, re.I)
    def pick(legs, instr, pc):
        for ins, sym in occ:
            if ins.upper()==instr and re.search(pc, sym, re.I): return sym
        return ""
    occ_buy_put   = pick(occ,"BUY", r'P')
    occ_sell_put  = pick(occ,"SELL",r'P')
    occ_sell_call = pick(occ,"SELL",r'C')
    occ_buy_call  = pick(occ,"BUY", r'C')
    summary = next((ln.strip() for ln in txt.splitlines() if ln.strip()), "")
    return {
        "signal_date": signal_date, "expiry": expiry, "side": side, "width": width, "orig_qty": orig_qty,
        "put_spread": f"{p1}/{p2}" if p1 and p2 else "",
        "call_spread": f"{c1}/{c2}" if c1 and c2 else "",
        "occ_buy_put": occ_buy_put, "occ_sell_put": occ_sell_put, "occ_sell_call": occ_sell_call, "occ_buy_call": occ_buy_call,
        "summary": summary
    }

def main():
    gw_token   = env_or_die("GW_TOKEN")
    sheet_id   = env_or_die("GSHEET_ID")
    sa_json    = env_or_die("GOOGLE_SERVICE_ACCOUNT_JSON")

    # New sizing defaults per your request
    size_credit = env_int("LEO_SIZE_CREDIT", 4)
    size_debit  = env_int("LEO_SIZE_DEBIT",  2)
    width_env   = env_int("LEO_WIDTH", 5)
    force_side  = env_str("LEO_FORCE_SIDE", "").upper()  # "CREDIT" or "DEBIT" to override

    # 1) API fetch
    url = "https://gandalf.gammawizard.com/rapi/GetLeoCross"
    r = requests.get(url, headers={"Authorization": f"Bearer {gw_token}"}, timeout=30)
    if r.status_code != 200:
        print(f"LeoCross GET failed: {r.status_code} {r.text[:400]}", file=sys.stderr)
        sys.exit(1)
    api = r.json()

    # Trade block
    trade = {}
    if isinstance(api.get("Trade"), list) and api["Trade"]:
        trade = api["Trade"][-1]
    elif isinstance(api.get("Trade"), dict):
        trade = api["Trade"]
    if not trade:
        print("No Trade block", file=sys.stderr); sys.exit(1)

    signal_date = str(trade.get("Date",""))
    expiry      = str(trade.get("TDate",""))
    spx  = str(trade.get("SPX","")); vix = str(trade.get("VIX",""))
    rv5  = str(trade.get("RV5","")); rv10 = str(trade.get("RV10","")); rv20 = str(trade.get("RV20",""))
    dte  = str(trade.get("M",""))
    cat1 = str(trade.get("Cat1","")); cat2 = str(trade.get("Cat2",""))

    # 2) Prefer your ticket script if present (authoritative side)
    side = None; summary = ""
    put_spread = call_spread = ""
    occ_buy_put = occ_sell_put = occ_sell_call = occ_buy_call = ""
    width = str(width_env); orig_qty = ""

    ticket = find_ticket_script()
    if ticket:
        cp = subprocess.run([sys.executable, ticket], capture_output=True, text=True)
        if cp.returncode == 0:
            parsed = parse_ticket_output(cp.stdout)
            if parsed:
                side        = parsed["side"]
                summary     = parsed["summary"]
                put_spread  = parsed["put_spread"]
                call_spread = parsed["call_spread"]
                occ_buy_put   = parsed["occ_buy_put"]
                occ_sell_put  = parsed["occ_sell_put"]
                occ_sell_call = parsed["occ_sell_call"]
                occ_buy_call  = parsed["occ_buy_call"]
                width       = parsed["width"] or width
                orig_qty    = parsed["orig_qty"] or ""
    # 3) If no ticket or incomplete, derive legs from API (Limit/CLimit + width)
    if not (occ_buy_put and occ_sell_put and occ_sell_call and occ_buy_call):
        inner_put  = to_int(trade.get("Limit",""))
        inner_call = to_int(trade.get("CLimit",""))
        w = to_int(width) or width_env
        if inner_put is None or inner_call is None:
            print("Missing Limit/CLimit to derive legs", file=sys.stderr); sys.exit(1)
        p1, p2 = inner_put - w, inner_put
        c1, c2 = inner_call, inner_call + w
        put_spread, call_spread = f"{p1}/{p2}", f"{c1}/{c2}"
        exp6 = None
        try: exp6 = date.fromisoformat(expiry).strftime("%y%m%d")
        except Exception: exp6 = ""
        occ_buy_put   = f".SPXW{exp6}P{p1}"
        occ_sell_put  = f".SPXW{exp6}P{p2}"
        occ_sell_call = f".SPXW{exp6}C{c1}"
        occ_buy_call  = f".SPXW{exp6}C{c2}"
        summary = summary or f"{signal_date} → {expiry} : AUTO legs width={w}"

    # 4) Decide credit vs debit
    if force_side in ("CREDIT","DEBIT"):
        credit = (force_side == "CREDIT")
        side = side or ("SHORT_IRON_CONDOR" if credit else "LONG_IRON_CONDOR")
    else:
        # If ticket gave us side, use it; otherwise default to CREDIT
        if side:
            credit = side.upper().startswith("SHORT") or "CREDIT" in side.upper()
        else:
            credit = True
            side = "SHORT_IRON_CONDOR"

    qty_exec = size_credit if credit else size_debit
    credit_or_debit = "credit" if credit else "debit"

    # Worst-day loss approx
    w = to_int(width) or width_env
    worst_day_loss = str(w * 100 * qty_exec)

    # 5) Append at A2
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
        signal_date, expiry, side, credit_or_debit, str(orig_qty or ""),
        qty_exec, str(w),
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
