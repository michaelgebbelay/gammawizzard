#!/usr/bin/env python3
"""
Schwab → Sheets raw activity dump with order/exec enrichment, strict API hygiene.

Key changes to stop Schwab "Transaction not found" spam:
- We DO NOT call Get Transaction anymore.
- We enrich only from Orders:
    * get_order(order_id, account) when orderId is present
    * get_orders_for_account(window) cached per ET day when orderId is missing
- txn_id column is still populated (transactionId if present, else orderId) for your sheet,
  but we never assume an orderId is a transactionId for API calls.

Also keeps:
- Non-overlapping ET chunking (transactions list)
- Accept 204 empty
- RAW write + enforced column formats (no price-as-date)
- Computed amount = qty * price * multiplier (options=100 else 1)
"""

import os, sys, json, base64, re, time
from datetime import datetime, timedelta, timezone, date
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from schwab.auth import client_from_token_file
from google.oauth2 import service_account
from googleapiclient.discovery import build as gbuild

ET = ZoneInfo("America/New_York")
RAW_TAB = "sw_txn_raw"
HEADERS = [
    "ts","txn_id","type","sub_type","description",
    "symbol","underlying","exp_primary","strike","put_call",
    "quantity","price","amount","net_amount","commissions","fees_other",
    "source"
]

# columns (0-based)
TEXT_COLS    = [0,1,2,3,4,5,6,7,9,16]
NUMERIC_COLS = [8,10,11,12,13,14,15]

# simple call counters (printed at end for audit)
CALLS = {
    "get_transactions_list": 0,
    "get_order_by_id": 0,
    "get_orders_for_account": 0,
}

# ---------- Sheets helpers ----------
def sheets_client():
    sid = os.environ["GSHEET_ID"]
    sa_json = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
    try:
        dec = base64.b64decode(sa_json).decode("utf-8")
        if dec.strip().startswith("{"):
            sa_json = dec
    except Exception:
        pass
    creds = service_account.Credentials.from_service_account_info(
        json.loads(sa_json),
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    svc = gbuild("sheets","v4",credentials=creds)
    return svc, sid

def get_sheet_meta(svc, sid: str):
    meta = svc.spreadsheets().get(spreadsheetId=sid).execute()
    sheets = meta.get("sheets", [])
    by_title = {s["properties"]["title"]: s for s in sheets}
    return by_title

def ensure_tab_with_header(svc, sid: str, tab: str, headers: List[str]) -> int:
    by_title = get_sheet_meta(svc, sid)
    if tab not in by_title:
        svc.spreadsheets().batchUpdate(
            spreadsheetId=sid,
            body={"requests":[{"addSheet":{"properties":{"title":tab}}}]}
        ).execute()
        by_title = get_sheet_meta(svc, sid)
    sheet = by_title[tab]
    sheet_id = sheet["properties"]["sheetId"]

    got = svc.spreadsheets().values().get(
        spreadsheetId=sid, range=f"{tab}!1:1"
    ).execute().get("values", [])
    if not got or got[0] != headers:
        svc.spreadsheets().values().update(
            spreadsheetId=sid, range=f"{tab}!A1",
            valueInputOption="RAW",
            body={"values":[headers]}
        ).execute()
    return sheet_id

def format_tab_columns(svc, sid: str, sheet_id: int, tab: str) -> None:
    meta = svc.spreadsheets().get(spreadsheetId=sid).execute()
    row_count = 100000
    for s in meta.get("sheets", []):
        if s["properties"]["sheetId"] == sheet_id:
            row_count = max(row_count, s["properties"].get("gridProperties",{}).get("rowCount", 1000))
            break

    reqs = []
    for ci in TEXT_COLS:
        reqs.append({
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": row_count, "startColumnIndex": ci, "endColumnIndex": ci+1},
                "cell": {"userEnteredFormat": {"numberFormat": {"type":"TEXT"}}},
                "fields": "userEnteredFormat.numberFormat"
            }
        })
    for ci in NUMERIC_COLS:
        reqs.append({
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": row_count, "startColumnIndex": ci, "endColumnIndex": ci+1},
                "cell": {"userEnteredFormat": {"numberFormat": {"type":"NUMBER","pattern":"0.########"}}},
                "fields": "userEnteredFormat.numberFormat"
            }
        })
    if reqs:
        svc.spreadsheets().batchUpdate(spreadsheetId=sid, body={"requests": reqs}).execute()

def clear_tab(svc, sid: str, tab: str) -> None:
    svc.spreadsheets().values().clear(spreadsheetId=sid, range=tab).execute()

def write_rows_raw(svc, sid: str, tab: str, headers: List[str], rows: List[List[Any]]) -> None:
    svc.spreadsheets().values().update(
        spreadsheetId=sid,
        range=f"{tab}!A1",
        valueInputOption="RAW",
        body={"values":[headers] + rows}
    ).execute()

# ---------- Schwab auth ----------
def decode_token_to_path() -> str:
    token_env = os.environ.get("SCHWAB_TOKEN_JSON","") or ""
    token_path = "schwab_token.json"
    if token_env:
        try:
            dec = base64.b64decode(token_env).decode("utf-8")
            if dec.strip().startswith("{"): token_env = dec
        except Exception:
            pass
        with open(token_path,"w") as f:
            f.write(token_env)
    return token_path

def schwab_client():
    token_path = decode_token_to_path()
    app_key = os.environ["SCHWAB_APP_KEY"]
    app_secret = os.environ["SCHWAB_APP_SECRET"]
    c = client_from_token_file(token_path, app_key, app_secret)
    r = c.get_account_numbers(); r.raise_for_status()
    acct_hash = r.json()[0]["hashValue"]
    return c, acct_hash

# ---------- time/format helpers ----------
def start_of_day(dt: datetime, tz=ET) -> datetime:
    return dt.astimezone(tz).replace(hour=0, minute=0, second=0, microsecond=0)
def end_of_day(dt: datetime, tz=ET) -> datetime:
    return dt.astimezone(tz).replace(hour=23, minute=59, second=59, microsecond=0)
def fmt_et_with_colon(dt: datetime) -> str:
    s = dt.astimezone(ET).strftime("%Y-%m-%dT%H:%M:%S%z"); return f"{s[:-2]}:{s[-2:]}"
def fmt_et_no_colon(dt: datetime) -> str:
    return dt.astimezone(ET).strftime("%Y-%m-%dT%H:%M:%S%z")
def fmt_utc_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def parse_ts(s: str) -> Optional[datetime]:
    if not s: return None
    x = s.strip()
    if x.endswith("Z"): x = x[:-1] + "+00:00"
    if re.match(r".*[+-]\d{4}$", x): x = x[:-5] + x[-5:-2] + ":" + x[-2:]
    try: return datetime.fromisoformat(x)
    except Exception: return None

# ---------- parsing helpers ----------
def safe_float(x) -> Optional[float]:
    try: return float(x)
    except Exception: return None

def to_underlying(sym: str) -> Optional[str]:
    if not sym: return None
    s = sym.strip().upper().lstrip(".").replace("  "," ")
    if s.startswith("SPXW") or s.startswith("SPX"): return "SPX"
    m = re.match(r"([A-Z.$^]{1,6})", s)
    return m.group(1) if m else None

def parse_exp_from_symbol(sym: str) -> Optional[str]:
    if not sym: return None
    s = sym.strip().upper().replace(" ","")
    m = re.search(r"\D(\d{6})[CP]\d", s)
    if m:
        try: return datetime.strptime(m.group(1), "%y%m%d").date().isoformat()
        except Exception: return None
    m = re.search(r"(20\d{2}-\d{2}-\d{2})", s)
    return m.group(1) if m else None

def parse_pc_from_symbol(sym: str) -> Optional[str]:
    if not sym: return None
    s = sym.upper().replace(" ","")
    m = re.search(r"\d{6}([CP])\d{8}$", s)
    if m: return "CALL" if m.group(1) == "C" else "PUT"
    return None

def parse_strike_from_occ(sym: str) -> Optional[float]:
    if not sym: return None
    s = sym.upper().replace(" ","")
    m = re.search(r"[CP](\d{8})$", s)
    if not m: return None
    try: return int(m.group(1)) / 1000.0
    except Exception: return None

def occ_strike_code(strike: float) -> str:
    v = int(round(float(strike) * 1000)); return f"{v:08d}"

def occ_from_parts(root: str, exp_iso: str, put_call: str, strike: float) -> str:
    y = exp_iso[2:4]; m = exp_iso[5:7]; d = exp_iso[8:10]
    pc = "C" if str(put_call).upper().startswith("C") else "P"
    return f"{root:<6}{y}{m}{d}{pc}{occ_strike_code(strike)}"

def parse_from_description(desc: str) -> Tuple[Optional[str], Optional[str], Optional[float], Optional[str]]:
    if not desc: return None, None, None, None
    d = desc.upper()
    und = "SPX" if ("S & P 500 INDEX" in d or "S&P 500 INDEX" in d) else None
    m = re.search(r"\b(PUT|CALL)\b.*?\$(\d+(?:\.\d+)?)\s+EXP\s+(\d{2}/\d{2}/\d{2})", d)
    if not m: return und, None, None, None
    pc = m.group(1); strike = safe_float(m.group(2))
    mm, dd, yy = m.group(3).split("/"); exp_iso = f"20{yy}-{mm}-{dd}"
    return und, exp_iso, strike, pc

def is_occ_option(sym: str) -> bool:
    if not sym: return False
    return bool(re.search(r"\d{6}[CP]\d{8}$", sym.upper().replace(" ","")))

def contract_multiplier(symbol: str, underlying: str) -> int:
    s = (symbol or "").upper(); u = (underlying or "").upper()
    if is_occ_option(s): return 100
    if u in {"SPX","SPXW","NDX","RUT","VIX","XSP"}: return 100
    return 1

def compute_amount(qty: Optional[float], price: Optional[float], symbol: str, underlying: str) -> Optional[float]:
    if qty is None or price is None: return None
    mult = contract_multiplier(symbol, underlying)
    return round(qty * price * mult, 2)

# ---------- API calls ----------
def get_txns_chunk(c, acct_hash: str, dt0: datetime, dt1: datetime) -> List[Dict[str, Any]]:
    url = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/transactions"
    shapes = [
        ("ET_COLON", fmt_et_with_colon(start_of_day(dt0)), fmt_et_with_colon(end_of_day(dt1))),
        ("ET_NOCOL", fmt_et_no_colon(start_of_day(dt0)),   fmt_et_no_colon(end_of_day(dt1))),
        ("UTC_Z",    fmt_utc_z(start_of_day(dt0)),         fmt_utc_z(end_of_day(dt1))),
    ]
    last_err = ""
    for tag, s0, s1 in shapes:
        params = {"startDate": s0, "endDate": s1}
        try:
            r = c.session.get(url, params=params, timeout=30)
            CALLS["get_transactions_list"] += 1
            if r.status_code == 200:
                j = r.json()
                if isinstance(j, list): return j
                if isinstance(j, dict) and "transactions" in j: return j.get("transactions") or []
                return []
            if r.status_code == 204:
                return []
            last_err = f"{r.status_code}:{(r.text or '')[:160]}"
            print(f"NOTE: txns shape={tag} → {last_err}")
        except Exception as e:
            last_err = f"EXC:{e}"; print(f"NOTE: txns shape={tag} exception: {e}")
        time.sleep(0.2)
    raise RuntimeError(f"transactions fetch failed for chunk — {last_err}")

def get_txns_resilient(c, acct_hash: str, start_dt: datetime, end_dt: datetime) -> List[Dict[str, Any]]:
    txns: List[Dict[str, Any]] = []
    cur_day = start_dt.astimezone(ET).date()
    end_day = end_dt.astimezone(ET).date()
    while cur_day <= end_day:
        chunk_end_day = min(cur_day + timedelta(days=29), end_day)
        dt0 = datetime.combine(cur_day, datetime.min.time(), tzinfo=timezone.utc)
        dt1 = datetime.combine(chunk_end_day, datetime.min.time(), tzinfo=timezone.utc)
        print(f"Pulling {cur_day} → {chunk_end_day} (ET)…")
        txns += get_txns_chunk(c, acct_hash, dt0, dt1)
        cur_day = chunk_end_day + timedelta(days=1)
    return txns

def get_order_by_id(c, acct_hash: str, order_id: str) -> Optional[Dict[str, Any]]:
    if not order_id: return None
    try:
        r = c.get_order(order_id, acct_hash)
        CALLS["get_order_by_id"] += 1
        if getattr(r, "status_code", None) == 200:
            return r.json()
    except Exception as e:
        print(f"NOTE: get_order {order_id} failed: {e}")
    return None

def list_orders_window(c, acct_hash: str, t0: datetime, t1: datetime) -> List[Dict[str, Any]]:
    try:
        r = c.get_orders_for_account(acct_hash, from_entered_datetime=t0, to_entered_datetime=t1)
        CALLS["get_orders_for_account"] += 1
        if getattr(r, "status_code", None) == 200:
            j = r.json()
            if isinstance(j, list): return j
    except Exception as e:
        print(f"NOTE: get_orders_for_account window failed: {e}")
    return []

# ---------- order/exec enrichment ----------
def parse_order_datetime(order: Dict[str, Any]) -> Optional[datetime]:
    for f in ("closeTime","enteredTime"):
        v = order.get(f)
        if isinstance(v, str):
            s = v
            if s.endswith("Z"): s = s[:-1] + "+00:00"
            if re.match(r".*[+-]\d{4}$", s): s = s[:-5] + s[-5:-2] + ":" + s[-2:]
            try: return datetime.fromisoformat(s)
            except Exception: pass
    return None

_order_cache_by_id: Dict[str, Dict[str, Any]] = {}
_orders_cache_by_day: Dict[str, List[Dict[str, Any]]] = {}

def best_order_for_txn(c, acct_hash: str, order_id: str, ts: Optional[datetime], desc: str) -> Optional[Dict[str, Any]]:
    # 1) direct by order_id
    if order_id:
        if order_id in _order_cache_by_id:
            return _order_cache_by_id[order_id]
        o = get_order_by_id(c, acct_hash, order_id)
        if o:
            _order_cache_by_id[order_id] = o
            return o
    # 2) nearby window by timestamp (±1 day)
    if not ts: return None
    day_key = ts.astimezone(ET).strftime("%Y-%m-%d")
    if day_key not in _orders_cache_by_day:
        t0 = ts - timedelta(days=1)
        t1 = ts + timedelta(days=1)
        _orders_cache_by_day[day_key] = list_orders_window(c, acct_hash, t0, t1)
    candidates = _orders_cache_by_day.get(day_key, [])
    if not candidates: return None

    und_hint, exp_hint, strike_hint, pc_hint = parse_from_description(desc)

    def score(order: Dict[str, Any]) -> Tuple[int, float]:
        odt = parse_order_datetime(order)
        dt_sec = abs((odt - ts).total_seconds()) if (odt and ts) else 1e12
        match = 0
        for lg in (order.get("orderLegCollection") or []):
            inst = lg.get("instrument") or {}
            sym = str(inst.get("symbol") or "")
            und = inst.get("underlyingSymbol") or to_underlying(sym) or ""
            exp = parse_exp_from_symbol(sym) or ""
            pc  = parse_pc_from_symbol(sym) or ""
            st  = parse_strike_from_occ(sym)
            if und_hint and und_hint == to_underlying(und): match += 1
            if exp_hint and exp == exp_hint: match += 1
            if pc_hint and pc and pc[0] == pc_hint[0]: match += 1
            if strike_hint and st and abs(st - strike_hint) < 1e-6: match += 1
        status = (order.get("status") or "").upper()
        if status == "FILLED": match += 1
        return (-match, dt_sec)

    best = sorted(candidates, key=score)[0]
    acts = best.get("orderActivityCollection") or []
    has_execs = any((a.get("executionLegs") for a in acts))
    return best if has_execs else None

def rows_from_order(order: Dict[str, Any], txn_meta: Dict[str, Any]) -> List[List[Any]]:
    ts    = txn_meta.get("ts")
    txn_id= txn_meta.get("txn_id","")  # for your sheet
    ttype = txn_meta.get("ttype","")
    subtype = txn_meta.get("subtype","")
    desc  = txn_meta.get("desc","")
    net_amount = txn_meta.get("net_amount")

    legs = order.get("orderLegCollection") or []
    leg_by_id = { int(lg.get("legId")): lg for lg in legs if lg.get("legId") is not None }

    rows: List[List[Any]] = []
    for act in (order.get("orderActivityCollection") or []):
        for el in (act.get("executionLegs") or []):
            leg = leg_by_id.get(int(el.get("legId", -1)), {})
            inst = leg.get("instrument") or {}
            sym = str(inst.get("symbol") or "")
            underlying = inst.get("underlyingSymbol") or to_underlying(sym) or ""
            if underlying.upper().startswith("SPX"): underlying = "SPX"
            exp_primary = parse_exp_from_symbol(sym) or ""
            strike = inst.get("strikePrice") or parse_strike_from_occ(sym)
            pc = inst.get("putCall") or parse_pc_from_symbol(sym) or ""
            pc = "CALL" if str(pc).upper().startswith("C") else ("PUT" if str(pc).upper().startswith("P") else "")

            qty = safe_float(el.get("quantity"))
            price = safe_float(el.get("price"))

            instr = str(leg.get("instruction") or "").upper()
            if instr.startswith("SELL") and qty is not None: qty = -abs(qty)
            elif instr.startswith("BUY") and qty is not None: qty =  abs(qty)

            amt = compute_amount(qty, price, sym, underlying)

            row = [ts, txn_id, ttype, subtype, desc,
                   sym, underlying, (exp_primary or ""), (strike if strike is not None else ""), pc,
                   (qty if qty is not None else ""), (price if price is not None else ""), (amt if amt is not None else ""),
                   (net_amount if net_amount is not None else ""), "", "", "schwab_txn"]
            rows.append(row)
    return rows

# ---------- transaction flattening ----------
def explode_txn_from_items(txn: Dict[str, Any]) -> Tuple[List[List[Any]], Dict[str, Any]]:
    rows: List[List[Any]] = []
    ts = (txn.get("transactionDate") or txn.get("time") or txn.get("date") or "")
    transaction_id = str(txn.get("transactionId") or "")  # REAL transaction id (if any)
    order_id = str(txn.get("orderId") or "")             # order id (different endpoint)
    txn_id_for_sheet = transaction_id or order_id        # preserve single column for your sheet
    ttype = str(txn.get("type") or txn.get("transactionType") or "")
    subtype = str(txn.get("subType") or "")
    desc = str(txn.get("description") or "")
    net_amount = safe_float(txn.get("netAmount"))

    # fees
    fees_total = 0.0; comm_total = 0.0
    if isinstance(txn.get("fees"), dict):
        for k, v in (txn["fees"] or {}).items():
            val = safe_float(v) or 0.0
            if "comm" in k.lower(): comm_total += val
            else: fees_total += val
    elif isinstance(txn.get("fees"), list):
        for f in txn["fees"]:
            val = safe_float(f.get("amount")) or 0.0
            name = str(f.get("feeType") or f.get("type") or "")
            if "comm" in name.lower(): comm_total += val
            else: fees_total += val

    items = txn.get("transactionItems") or txn.get("transactionItem") or []
    if isinstance(items, dict): items = [items]

    if not items:
        und_hint, exp_hint, strike_hint, pc_hint = parse_from_description(desc)
        sym = ""
        if und_hint and exp_hint and strike_hint and pc_hint:
            sym = occ_from_parts("SPXW", exp_hint, pc_hint, strike_hint)
        row = [ts, txn_id_for_sheet, ttype, subtype, desc,
               sym, (und_hint or ""), (exp_hint or ""), (strike_hint if strike_hint is not None else ""), (pc_hint or ""),
               "", "", "", (net_amount if net_amount is not None else ""), round(comm_total,2) or "", round(fees_total,2) or "",
               "schwab_txn"]
        rows.append(row)
        meta = {"ts": ts, "transaction_id": transaction_id, "order_id": order_id, "ttype": ttype, "subtype": subtype, "desc": desc, "net_amount": net_amount}
        return rows, meta

    for it in items:
        qty = safe_float(it.get("quantity"))
        price = safe_float(it.get("price"))
        amount = safe_float(it.get("cost")) or safe_float(it.get("amount"))

        symbol = str((it.get("instrument") or {}).get("symbol") or it.get("symbol") or "")
        underlying = (it.get("instrument") or {}).get("underlyingSymbol") or to_underlying(symbol) or ""
        if underlying.upper().startswith("SPX"): underlying = "SPX"

        pc = ((it.get("instrument") or {}).get("putCall") or it.get("putCall") or "")
        pc = "CALL" if str(pc).upper().startswith("C") else ("PUT" if str(pc).upper().startswith("P") else "")
        strike = safe_float((it.get("instrument") or {}).get("strikePrice") or it.get("strike"))
        exp_primary = None
        exp = (it.get("instrument") or {}).get("optionExpirationDate")
        if exp:
            try: exp_primary = str(date.fromisoformat(str(exp)[:10]))
            except Exception: exp_primary = None
        if not exp_primary:
            exp_primary = parse_exp_from_symbol(symbol)
        if strike is None:
            strike = parse_strike_from_occ(symbol)
        if not pc:
            pc = parse_pc_from_symbol(symbol) or ""

        instr = str((it.get("instruction") or it.get("orderAction") or "")).upper()
        if instr.startswith("SELL") and qty is not None: qty = -abs(qty)
        elif instr.startswith("BUY") and qty is not None: qty =  abs(qty)

        if amount is None:
            amount = compute_amount(qty, price, symbol, underlying)

        row = [ts, txn_id_for_sheet, ttype, subtype, desc,
               symbol, underlying, (exp_primary or ""), (strike if strike is not None else ""), pc,
               (qty if qty is not None else ""), (price if price is not None else ""), (amount if amount is not None else ""),
               (net_amount if net_amount is not None else ""), round(comm_total,2) or "", round(fees_total,2) or "",
               "schwab_txn"]
        rows.append(row)

    meta = {"ts": ts, "transaction_id": transaction_id, "order_id": order_id, "ttype": ttype, "subtype": subtype, "desc": desc, "net_amount": net_amount}
    return rows, meta

def explode_txn(c, acct_hash: str, txn: Dict[str, Any]) -> List[List[Any]]:
    rows, meta = explode_txn_from_items(txn)

    # If TRADE rows still lack qty/price, enrich from Orders ONLY (never Get Transaction)
    need_enrich = any((r[2] == "TRADE") and (r[10] in ("", None) or r[11] in ("", None)) for r in rows)
    if not need_enrich:
        return rows

    ts_dt = parse_ts(meta.get("ts"))
    order = best_order_for_txn(c, acct_hash, meta.get("order_id",""), ts_dt, meta.get("desc",""))
    if order:
        exec_rows = rows_from_order(order, {
            "ts": meta.get("ts"),
            "txn_id": meta.get("transaction_id") or meta.get("order_id") or "",
            "ttype": meta.get("ttype",""),
            "subtype": meta.get("subtype",""),
            "desc": meta.get("desc",""),
            "net_amount": meta.get("net_amount")
        })
        if exec_rows:
            return exec_rows

    return rows

# ---------- type coercion for RAW write ----------
def _to_number(x):
    if x in ("", None): return ""
    try: return float(x)
    except Exception: return x

def prepare_rows_for_sheet(rows: List[List[Any]]) -> List[List[Any]]:
    prepped = []
    for r in rows:
        rr = list(r)
        for ci in TEXT_COLS:
            if ci < len(rr): rr[ci] = "" if rr[ci] in ("", None) else str(rr[ci])
        for ci in NUMERIC_COLS:
            if ci < len(rr): rr[ci] = _to_number(rr[ci])
        prepped.append(rr)
    return prepped

# ---------- main ----------
def main() -> int:
    # Sheets
    try:
        svc, sid = sheets_client()
        sheet_id = ensure_tab_with_header(svc, sid, RAW_TAB, HEADERS)
    except Exception as e:
        print(f"ABORT: Sheets init failed — {e}")
        return 1

    # Schwab
    try:
        c, acct_hash = schwab_client()
    except Exception as e:
        msg = str(e)
        if ("unsupported_token_type" in msg) or ("refresh_token_authentication_error" in msg):
            print("ABORT: Schwab OAuth refresh failed — rotate SCHWAB_TOKEN_JSON secret.")
        else:
            print(f"ABORT: Schwab client init failed — {msg[:200]}")
        return 1

    # Window
    try:
        days_back = int((os.environ.get("DAYS_BACK") or "60").strip())
    except Exception:
        days_back = 60
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=days_back)

    # Fetch + flatten
    try:
        txns = get_txns_resilient(c, acct_hash, start_dt, end_dt)
    except Exception as e:
        print(f"ABORT: transactions fetch failed — {e}")
        return 1

    rows: List[List[Any]] = []
    for t in txns:
        rows.extend(explode_txn(c, acct_hash, t))

    rows = prepare_rows_for_sheet(rows)
    clear_tab(svc, sid, RAW_TAB)
    format_tab_columns(svc, sid, sheet_id, RAW_TAB)
    write_rows_raw(svc, sid, RAW_TAB, HEADERS, rows)

    print(f"OK: wrote {len(rows)} rows to {RAW_TAB}.")
    print(f"API call counts: {CALLS}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
