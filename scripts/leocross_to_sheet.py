# scripts/leocross_to_sheet.py
import os, json, sys, re, subprocess
from datetime import datetime, timezone, date
import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build

__VERSION__ = "2025-08-27c"

LEO_TAB = "leocross"
HEADERS = [
    "ts","signal_date","expiry","side","credit_or_debit","qty_exec",
    "put_spread","call_spread",
    "occ_buy_put","occ_sell_put","occ_sell_call","occ_buy_call",
    "cat1","cat2",
    "spx","vix","rv20","rv10","rv5",
    "rec_put","rec_call","rec_condor",
    "summary"
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

# -------------------- tiny utils --------------------
def yymmdd(iso: str) -> str:
    try:
        d = date.fromisoformat((iso or "")[:10])
        return f"{d:%y%m%d}"
    except: return ""
def to_int(x):
    try: return int(round(float(x)))
    except: return None
def extract_strike(sym: str):
    if not sym: return None
    t = sym.strip().upper().lstrip(".").replace("_","")
    m = re.search(r'[PC](\d{6,8})$', t);  # OSI mills
    if m: return int(m.group(1)) / 1000.0
    m = re.search(r'[PC](\d+(?:\.\d+)?)$', t)
    if m: return float(m.group(1))
    return None

# -------------------- optional ticket parser --------------------
def parse_ticket_output(txt: str):
    m1 = re.search(r'(\d{4}-\d{2}-\d{2})\s*(?:->|\u2192)\s*(\d{4}-\d{2}-\d{2})\s*:\s*([A-Z_]+)\s*qty\s*=\s*(\d+)\s*width\s*=\s*([0-9.]+)', txt)
    if not m1: return None
    signal_date = m1.group(1); expiry = m1.group(2); side = m1.group(3)
    mS = re.search(r'Strikes?\s+P\s+(\d+(?:\.\d+)?)\s*/\s*(\d+(?:\.\d+)?)\s+C\s+(\d+(?:\.\d+)?)\s*/\s*(\d+(?:\.\d+)?)', txt, re.I)
    p1 = mS.group(1) if mS else ""; p2 = mS.group(2) if mS else ""
    c1 = mS.group(3) if mS else ""; c2 = mS.group(4) if mS else ""
    occ = re.findall(r'\b(BUY|SELL)\s+([A-Z0-9_.]+[CP]\d+)', txt, re.I)
    def pick(instr, pc):
        for ins, sym in occ:
            if ins.upper()==instr and re.search(pc, sym, re.I): return sym
        return ""
    return {
        "signal_date": signal_date, "expiry": expiry, "side": side,
        "p1": p1, "p2": p2, "c1": c1, "c2": c2,
        "occ_buy_put": pick("BUY","P"), "occ_sell_put": pick("SELL","P"),
        "occ_sell_call": pick("SELL","C"), "occ_buy_call": pick("BUY","C"),
        "summary": next((ln.strip() for ln in txt.splitlines() if ln.strip()), "")
    }

def orient_pairs_for_side(bp, sp, sc, bc, is_credit):
    bps = extract_strike(bp); sps = extract_strike(sp); scs = extract_strike(sc); bcs = extract_strike(bc)
    if bps is not None and sps is not None:
        if is_credit and bps > sps:    bp, sp = sp, bp
        if not is_credit and bps < sps: bp, sp = sp, bp
    if scs is not None and bcs is not None:
        if is_credit and scs > bcs:    sc, bc = bc, sc
        if not is_credit and bcs > scs: sc, bc = bc, sc
    return bp, sp, sc, bc

# -------------------- flexible "Trade" extractor --------------------
def _lower_dict(d): return {str(k).lower(): v for k,v in d.items()}
def _pick(m, *keys):
    for k in keys:
        v = m.get(k.lower())
        if v not in (None,""): return v
    return None
def _normalize_trade_like(d):
    m = _lower_dict(d)
    t = {
        "Date": _pick(m,"date","signal_date","sigdate"),
        "TDate": _pick(m,"tdate","expiry","expiration","exp","expiry_date"),
        "SPX": _pick(m,"spx","underlying","spot"),
        "VIX": _pick(m,"vix"),
        "RV5": _pick(m,"rv5"), "RV10": _pick(m,"rv10"), "RV20": _pick(m,"rv20"),
        "Cat1": _pick(m,"cat1"), "Cat2": _pick(m,"cat2"),
        "Put": _pick(m,"put","rec_put","put_rec"),
        "Call": _pick(m,"call","rec_call","call_rec"),
        "Limit": _pick(m,"limit","limit_put","put_limit","p_limit","putstrike"),
        "CLimit": _pick(m,"climit","limit_call","call_limit","c_limit","callstrike"),
    }
    if not any(t.values()): return None
    return t

def extract_trade_from_any(j):
    if isinstance(j, dict) and "Trade" in j:
        tr = j["Trade"]
        if isinstance(tr, list) and tr: return tr[-1]
        if isinstance(tr, dict): return tr
    if isinstance(j, dict):
        t = _normalize_trade_like(j)
        if t: return t
        for v in j.values():
            if isinstance(v,(dict,list)):
                t = extract_trade_from_any(v)
                if t: return t
    if isinstance(j, list):
        for item in reversed(j):
            t = extract_trade_from_any(item)
            if t: return t
    return {}

# -------------------- GammaWizard auth + fetch --------------------
def _sanitize_token(t: str) -> str:
    t = (t or "").strip().strip('"').strip("'")
    if t.lower().startswith("bearer "):
        t = t.split(None, 1)[1].strip()
    return t

def gw_authenticate(base: str, email: str, password: str) -> str:
    url = base.rstrip("/") + "/goauth/authenticateFireUser"
    r = requests.post(url, data={"email": email, "password": password}, timeout=30)
    if r.status_code != 200:
        print(f"AUTH_FAIL HTTP {r.status_code} body={r.text[:200]}", file=sys.stderr); sys.exit(1)
    j = r.json(); tok = (j or {}).get("token")
    if not tok:
        print(f"AUTH_NO_TOKEN payload={str(j)[:200]}", file=sys.stderr); sys.exit(1)
    return _sanitize_token(tok)

def gw_get_json(base: str, endpoint: str, token: str):
    url = base.rstrip("/") + "/" + endpoint.lstrip("/")
    hdr = {"Accept":"application/json", "Authorization": f"Bearer {token}"}
    r = requests.get(url, headers=hdr, timeout=30)
    if r.status_code != 200:
        print(f"GET_FAIL {endpoint} HTTP {r.status_code} body={r.text[:200]}", file=sys.stderr); sys.exit(1)
    return r.json()

# -------------------- main --------------------
def main():
    sheet_id   = env_or_die("GSHEET_ID")
    sa_json    = env_or_die("GOOGLE_SERVICE_ACCOUNT_JSON")

    # Auth config
    auth_mode  = env_str("GW_AUTH_MODE","AUTO").upper()    # AUTO | TOKEN | LOGIN
    base       = env_str("GW_BASE","https://gandalf.gammawizard.com")
    endpoint   = env_str("GW_ENDPOINT","/rapi/GetLeoCross")
    static_tok = _sanitize_token(env_str("GW_TOKEN",""))
    email      = env_str("GW_AUTH_EMAIL","")
    password   = env_str("GW_AUTH_PASSWORD","")

    if auth_mode == "TOKEN":
        if not static_tok:
            print("GW_AUTH_MODE=TOKEN but GW_TOKEN is empty.", file=sys.stderr); sys.exit(1)
        token = static_tok
    elif auth_mode == "LOGIN":
        if not (email and password):
            print("GW_AUTH_MODE=LOGIN but GW_AUTH_EMAIL/PASSWORD missing.", file=sys.stderr); sys.exit(1)
        token = gw_authenticate(base, email, password)
    else:
        token = static_tok if static_tok else gw_authenticate(base, email, password)

    api = gw_get_json(base, endpoint, token)
    trade = extract_trade_from_any(api)
    if not trade:
        print("No Trade-like data in response.", file=sys.stderr); sys.exit(1)

    def s(x): return "" if x is None else str(x)
    signal_date = s(trade.get("Date",""))
    expiry      = s(trade.get("TDate",""))
    spx  = s(trade.get("SPX","")); vix = s(trade.get("VIX",""))
    rv5  = s(trade.get("RV5","")); rv10 = s(trade.get("RV10","")); rv20 = s(trade.get("RV20",""))
    cat1 = s(trade.get("Cat1","")); cat2 = s(trade.get("Cat2",""))

    def fnum(x):
        try: return float(x)
        except: return None
    rec_put  = fnum(trade.get("Put",  None))
    rec_call = fnum(trade.get("Call", None))
    rec_condor = (rec_put + rec_call) if (rec_put is not None and rec_call is not None) else None

    # Optional ticket legs
    put_spread = call_spread = ""
    occ_buy_put = occ_sell_put = occ_sell_call = occ_buy_call = ""
    summary = ""

    ticket = None
    for root, _, files in os.walk(".", topdown=True):
        if "leocross_ticket.py" in files:
            ticket = os.path.join(root, "leocross_ticket.py"); break
    if ticket:
        cp = subprocess.run([sys.executable, ticket], capture_output=True, text=True)
        if cp.returncode == 0:
            parsed = parse_ticket_output(cp.stdout)
            if parsed:
                summary   = parsed["summary"] or summary
                if parsed["p1"] and parsed["p2"]:
                    put_spread  = f'{min(parsed["p1"],parsed["p2"])}/{max(parsed["p1"],parsed["p2"])}'
                if parsed["c1"] and parsed["c2"]:
                    call_spread = f'{min(parsed["c1"],parsed["c2"])}/{max(parsed["c1"],parsed["c2"])}'
                occ_buy_put   = parsed["occ_buy_put"]   or occ_buy_put
                occ_sell_put  = parsed["occ_sell_put"]  or occ_sell_put
                occ_sell_call = parsed["occ_sell_call"] or occ_sell_call
                occ_buy_call  = parsed["occ_buy_call"]  or occ_buy_call

    # Derive legs if missing, from Limit/CLimit + width
    width_env = int(cfg("policy.leo_width", env_int("LEO_WIDTH", 5)))
    if not (occ_buy_put and occ_sell_put and occ_sell_call and occ_buy_call):
        w = width_env
        inner_put  = to_int(trade.get("Limit",""))
        inner_call = to_int(trade.get("CLimit",""))
        if inner_put is None or inner_call is None:
            print("Missing Limit/CLimit for leg derivation.", file=sys.stderr); sys.exit(1)
        p_low, p_high = inner_put - w, inner_put
        c_low, c_high = inner_call, inner_call + w
        put_spread  = f"{p_low}/{p_high}"
        call_spread = f"{c_low}/{c_high}"
        exp6 = yymmdd(expiry)
        occ_buy_put   = f".SPXW{exp6}P{p_low}"
        occ_sell_put  = f".SPXW{exp6}P{p_high}"
        occ_sell_call = f".SPXW{exp6}C{c_low}"
        occ_buy_call  = f".SPXW{exp6}C{c_high}"
        summary = summary or f"{signal_date} → {expiry} : AUTO legs width={w}"

    # Decide CREDIT/DEBIT (Cat rule + tie)
    tie_side = str(cfg("policy.leo_tie_side", env_str("LEO_TIE_SIDE","CREDIT"))).upper()
    credit = True
    try:
        c1, c2 = float(cat1), float(cat2)
        if c1 > c2: credit = False
        elif c1 < c2: credit = True
        else: credit = (tie_side == "CREDIT")
    except: credit = True

    # Orient to side
    occ_buy_put, occ_sell_put, occ_sell_call, occ_buy_call = orient_pairs_for_side(
        occ_buy_put, occ_sell_put, occ_sell_call, occ_buy_call, credit
    )

    side = "SHORT_IRON_CONDOR" if credit else "LONG_IRON_CONDOR"
    credit_or_debit = "credit" if credit else "debit"
    qty_default = int(cfg("policy.leo_size_credit", 4) if credit else cfg("policy.leo_size_debit", 2))
    qty_exec    = env_int("LEO_SIZE_CREDIT" if credit else "LEO_SIZE_DEBIT", qty_default)

    # Write Google Sheet
    creds = service_account.Credentials.from_service_account_info(json.loads(sa_json), scopes=["https://www.googleapis.com/auth/spreadsheets"])
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
        signal_date, expiry, side, credit_or_debit, qty_exec,
        put_spread, call_spread,
        occ_buy_put, occ_sell_put, occ_sell_call, occ_buy_call,
        cat1, cat2,
        spx, vix, rv20, rv10, rv5,
        ("" if rec_put  is None else rec_put),
        ("" if rec_call is None else rec_call),
        ("" if rec_condor is None else rec_condor),
        (summary or f"Cat-rule → {credit_or_debit.upper()}")
    ]
    svc.spreadsheets().values().update(
        spreadsheetId=sheet_id, range=f"{LEO_TAB}!A2",
        valueInputOption="USER_ENTERED", body={"values":[row]}
    ).execute()

    print(f"leocross v{__VERSION__}: inserted at A2 (qty_exec={qty_exec}, {credit_or_debit})")

if __name__ == "__main__":
    main()
