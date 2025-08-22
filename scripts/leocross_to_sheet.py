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
        print(f"Missing required env: {name}", file=sys.stderr); sys.exit(1)
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
    try: return int(float(s))
    except: return default

def yymmdd(iso: str) -> str:
    try:
        d = date.fromisoformat(iso)
        return f"{d:%y%m%d}"
    except: return ""

def to_int(x):
    try: return int(round(float(x)))
    except: return None

def extract_strike(sym: str):
    """Robust strike parser for '.SPXW250822P6315' or 'SPXW_250822P06315000'."""
    if not sym: return None
    t = sym.strip().upper().lstrip(".").replace("_","")
    m = re.search(r'[PC](\d{6,8})$', t)  # OSI mills
    if m: return int(m.group(1)) / 1000.0
    m = re.search(r'[PC](\d+(?:\.\d+)?)$', t)  # plain
    if m: return float(m.group(1))
    return None

def parse_ticket_output(txt: str):
    m1 = re.search(r'(\d{4}-\d{2}-\d{2})\s*(?:->|\u2192)\s*(\d{4}-\d{2}-\d{2})\s*:\s*([A-Z_]+)\s*qty\s*=\s*(\d+)\s*width\s*=\s*([0-9.]+)', txt)
    if not m1: return None
    signal_date = m1.group(1); expiry = m1.group(2); side = m1.group(3)
    width = m1.group(5); orig_qty = m1.group(4)
    mS = re.search(r'Strikes?\s+P\s+(\d+(?:\.\d+)?)\s*/\s*(\d+(?:\.\d+)?)\s+C\s+(\d+(?:\.\d+)?)\s*/\s*(\d+(?:\.\d+)?)', txt, re.I)
    p1 = mS.group(1) if mS else ""; p2 = mS.group(2) if mS else ""
    c1 = mS.group(3) if mS else ""; c2 = mS.group(4) if mS else ""
    occ = re.findall(r'\b(BUY|SELL)\s+([A-Z0-9_.]+[CP]\d+)', txt, re.I)
    def pick(instr, pc):
        for ins, sym in occ:
            if ins.upper()==instr and re.search(pc, sym, re.I): return sym
        return ""
    occ_buy_put   = pick("BUY",  r'P')
    occ_sell_put  = pick("SELL", r'P')
    occ_sell_call = pick("SELL", r'C')
    occ_buy_call  = pick("BUY",  r'C')
    summary = next((ln.strip() for ln in txt.splitlines() if ln.strip()), "")
    return {
        "signal_date": signal_date, "expiry": expiry, "side": side, "width": width, "orig_qty": orig_qty,
        "p1": p1, "p2": p2, "c1": c1, "c2": c2,
        "occ_buy_put": occ_buy_put, "occ_sell_put": occ_sell_put,
        "occ_sell_call": occ_sell_call, "occ_buy_call": occ_buy_call,
        "summary": summary
    }

def orient_pairs_for_side(bp, sp, sc, bc, is_credit):
    """Given four leg symbols (buyP,sellP,sellC,buyC), ensure the pair
    orientation matches credit/debit rules by swapping within each pair if needed."""
    # Extract strikes (None-safe)
    bps = extract_strike(bp); sps = extract_strike(sp); scs = extract_strike(sc); bcs = extract_strike(bc)
    # Puts
    if bps is not None and sps is not None:
        if is_credit and bps > sps:    bp, sp = sp, bp   # want buy lower < sell higher
        if not is_credit and bps < sps: bp, sp = sp, bp  # want buy higher > sell lower
    # Calls
    if scs is not None and bcs is not None:
        if is_credit and scs > bcs:    sc, bc = bc, sc   # want sell lower < buy higher
        if not is_credit and bcs > scs: sc, bc = bc, sc  # want buy lower < sell higher
    return bp, sp, sc, bc

def main():
    gw_token   = env_or_die("GW_TOKEN")
    sheet_id   = env_or_die("GSHEET_ID")
    sa_json    = env_or_die("GOOGLE_SERVICE_ACCOUNT_JSON")

    # sizing defaults used for display only (Schwab can override via %BP)
    size_credit = env_int("LEO_SIZE_CREDIT", 4)
    size_debit  = env_int("LEO_SIZE_DEBIT",  2)
    width_env   = env_int("LEO_WIDTH", 5)

    force_side  = env_str("LEO_FORCE_SIDE", "").upper()      # "CREDIT"/"DEBIT"
    tie_side    = env_str("LEO_TIE_SIDE", "CREDIT").upper()  # when Cat1 == Cat2

    # 1) Fetch API
    url = "https://gandalf.gammawizard.com/rapi/GetLeoCross"
    r = requests.get(url, headers={"Authorization": f"Bearer {gw_token}"}, timeout=30)
    if r.status_code != 200:
        print(f"LeoCross GET failed: {r.status_code} {r.text[:400]}", file=sys.stderr); sys.exit(1)
    api = r.json()

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

    # 2) Ticket parse (optional)
    put_spread = call_spread = ""
    occ_buy_put = occ_sell_put = occ_sell_call = occ_buy_call = ""
    width = str(width_env); orig_qty = ""; summary = ""

    ticket = None
    for root, _, files in os.walk(".", topdown=True):
        if "leocross_ticket.py" in files:
            ticket = os.path.join(root, "leocross_ticket.py")
            break
    if ticket:
        cp = subprocess.run([sys.executable, ticket], capture_output=True, text=True)
        if cp.returncode == 0:
            parsed = parse_ticket_output(cp.stdout)
            if parsed:
                summary   = parsed["summary"] or summary
                width     = parsed["width"] or width
                orig_qty  = parsed["orig_qty"] or orig_qty
                # spreads if present
                if parsed["p1"] and parsed["p2"]:
                    put_spread  = f'{min(parsed["p1"],parsed["p2"])}/{max(parsed["p1"],parsed["p2"])}'
                if parsed["c1"] and parsed["c2"]:
                    call_spread = f'{min(parsed["c1"],parsed["c2"])}/{max(parsed["c1"],parsed["c2"])}'
                occ_buy_put   = parsed["occ_buy_put"]   or occ_buy_put
                occ_sell_put  = parsed["occ_sell_put"]  or occ_sell_put
                occ_sell_call = parsed["occ_sell_call"] or occ_sell_call
                occ_buy_call  = parsed["occ_buy_call"]  or occ_buy_call

    # 3) If legs missing, derive from API inner (Limit/CLimit) + width
    if not (occ_buy_put and occ_sell_put and occ_sell_call and occ_buy_call):
        w = to_int(width) or width_env
        inner_put  = to_int(trade.get("Limit",""))
        inner_call = to_int(trade.get("CLimit",""))
        if inner_put is None or inner_call is None:
            print("Missing Limit/CLimit for leg derivation.", file=sys.stderr); sys.exit(1)
        p_low, p_high = inner_put - w, inner_put
        c_low, c_high = inner_call, inner_call + w
        put_spread  = f"{p_low}/{p_high}"
        call_spread = f"{c_low}/{c_high}"
        exp6 = yymmdd(expiry)
        # provisional credit-style assignment; we’ll re-orient after Cat decision
        occ_buy_put   = f".SPXW{exp6}P{p_low}"
        occ_sell_put  = f".SPXW{exp6}P{p_high}"
        occ_sell_call = f".SPXW{exp6}C{c_low}"
        occ_buy_call  = f".SPXW{exp6}C{c_high}"
        width = str(w)
        summary = summary or f"{signal_date} → {expiry} : AUTO legs width={w}"

    # 4) Decide CREDIT/DEBIT from Cat-rule (+ override)
    credit = True
    try:
        c1, c2 = float(cat1), float(cat2)
        if c1 > c2: credit = False   # DEBIT
        elif c1 < c2: credit = True  # CREDIT
        else: credit = (tie_side == "CREDIT")
    except:
        credit = True
    if env_str("LEO_FORCE_SIDE","").upper() in ("CREDIT","DEBIT"):
        credit = (env_str("LEO_FORCE_SIDE").upper()=="CREDIT")

    # 5) Re-orient pairs to match side rules
    occ_buy_put, occ_sell_put, occ_sell_call, occ_buy_call = orient_pairs_for_side(
        occ_buy_put, occ_sell_put, occ_sell_call, occ_buy_call, credit
    )

    side = "SHORT_IRON_CONDOR" if credit else "LONG_IRON_CONDOR"
    credit_or_debit = "credit" if credit else "debit"
    qty_exec = size_credit if credit else size_debit

    # Worst-case per condor (approx) for display
    try:
        w = int(float(width))
        worst_day_loss = str(w * 100 * qty_exec) if credit else ""  # debit risk is the debit paid
    except:
        worst_day_loss = ""

    # 6) Write to Sheets (headers, top insert, then A2)
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
        qty_exec, str(width),
        put_spread, call_spread,
        occ_buy_put, occ_sell_put, occ_sell_call, occ_buy_call,
        cat1, cat2, worst_day_loss,
        spx, vix, rv20, rv10, rv5, dte,
        (summary or f"Cat-rule → {credit_or_debit.upper()}")
    ]
    svc.spreadsheets().values().update(
        spreadsheetId=sheet_id, range=f"{LEO_TAB}!A2",
        valueInputOption="USER_ENTERED", body={"values":[row]}
    ).execute()

    print(f"leocross: inserted at A2 (qty_exec={qty_exec}, {credit_or_debit})")
    print("legs", occ_buy_put, occ_sell_put, occ_sell_call, occ_buy_call)

if __name__ == "__main__":
    main()
