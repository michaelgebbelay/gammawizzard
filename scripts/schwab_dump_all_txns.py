#!/usr/bin/env python3
"""
Schwab → Sheets raw activity dump with order/exec enrichment and no duplicates.

Key points
- Emits executions for an order once per orderId (guarded).
- Each execution row carries the ledger's fees (commissions + other), so the
  summarizer can allocate fees correctly.
- Execution timestamp is the leg's time, not the ledger time.
- Amount = qty * price * multiplier (100 for index/option contracts).
"""

import os, sys, json, base64, re, time
from datetime import datetime, timedelta, timezone, date
from typing import Any, Dict, List, Optional, Tuple, Set
from zoneinfo import ZoneInfo

from schwab.auth import client_from_token_file
from google.oauth2 import service_account
from googleapiclient.discovery import build as gbuild

ET = ZoneInfo("America/New_York")

RAW_TAB = "sw_txn_raw"
RAW_HEADERS = [
    "ts","txn_id","type","sub_type","description",
    "symbol","underlying","exp_primary","strike","put_call",
    "quantity","price","amount","net_amount","commissions","fees_other",
    "source"
]

# ---------- Sheets ----------
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

def ensure_tab_with_header(svc, sid: str, tab: str, headers: List[str]) -> None:
    meta = svc.spreadsheets().get(spreadsheetId=sid).execute()
    titles = {s["properties"]["title"] for s in meta.get("sheets",[])}
    if tab not in titles:
        svc.spreadsheets().batchUpdate(
            spreadsheetId=sid,
            body={"requests":[{"addSheet":{"properties":{"title":tab}}}]}
        ).execute()
    got = svc.spreadsheets().values().get(spreadsheetId=sid, range=f"{tab}!1:1").execute().get("values",[])
    if not got or got[0] != headers:
        svc.spreadsheets().values().update(
            spreadsheetId=sid, range=f"{tab}!A1",
            valueInputOption="RAW",
            body={"values":[headers]}
        ).execute()

def overwrite_rows(svc, sid: str, tab: str, headers: List[str], rows: List[List[Any]]) -> None:
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

# ---------- time helpers ----------
def start_of_day(dt: datetime, tz=ET) -> datetime:
    return dt.astimezone(tz).replace(hour=0, minute=0, second=0, microsecond=0)
def end_of_day(dt: datetime, tz=ET) -> datetime:
    return dt.astimezone(tz).replace(hour=23, minute=59, second=59, microsecond=0)

def _iso_fix(s: str) -> str:
    x = s.strip()
    if x.endswith("Z"):
        return x[:-1] + "+00:00"
    if re.search(r"[+-]\d{4}$", x):
        return x[:-5] + x[-5:-2] + ":" + x[-2:]
    return x

def fmt_et_with_colon(dt: datetime) -> str:
    s = dt.astimezone(ET).strftime("%Y-%m-%dT%H:%M:%S%z"); return f"{s[:-2]}:{s[-2:]}"
def fmt_et_no_colon(dt: datetime) -> str:
    return dt.astimezone(ET).strftime("%Y-%m-%dT%H:%M:%S%z")
def fmt_utc_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def parse_ts(s: Optional[str]) -> Optional[datetime]:
    if not s: return None
    try: return datetime.fromisoformat(_iso_fix(s))
    except Exception: return None

def exec_ts(el: Dict[str,Any], fallback: Optional[str]) -> str:
    cand = el.get("time") or el.get("executionTime") or fallback or ""
    dt = parse_ts(str(cand)) or datetime.now(timezone.utc)
    s = dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    return f"{s[:-2]}:{s[-2:]}"

# ---------- parsing ----------
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

def contract_multiplier(symbol: str, underlying: str) -> int:
    s = (symbol or "").upper(); u = (underlying or "").upper()
    if re.search(r"\d{6}[CP]\d{8}$", s): return 100
    if u in {"SPX","SPXW","NDX","RUT","VIX","XSP"}: return 100
    return 1

def compute_amount(qty: Optional[float], price: Optional[float], symbol: str, underlying: str) -> Optional[float]:
    if qty is None or price is None: return None
    mult = contract_multiplier(symbol, underlying)
    return round(qty * price * mult, 2)

# ---------- API pulls ----------
CALLS = {"get_transactions_list":0, "get_order_by_id":0, "get_orders_for_account":0}

def get_txns_chunk(c, acct_hash: str, dt0: datetime, dt1: datetime) -> List[Dict[str, Any]]:
    url = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/transactions"
    shapes = [
        ("ET_COLON", fmt_et_with_colon(start_of_day(dt0)), fmt_et_with_colon(end_of_day(dt1))),
        ("ET_NOCOL", fmt_et_no_colon(start_of_day(dt0)),   fmt_et_no_colon(end_of_day(dt1))),
        ("UTC_Z",    fmt_utc_z(start_of_day(dt0)),         fmt_utc_z(end_of_day(dt1))),
    ]
    last_err = ""
    for tag, s0, s1 in shapes:
        try:
            r = c.session.get(url, params={"startDate":s0,"endDate":s1}, timeout=30)
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
def parse_exp_from_instr_symbol(sym: str, inst: Dict[str,Any]) -> Optional[str]:
    exp = inst.get("optionExpirationDate")
    if exp:
        try: return str(date.fromisoformat(str(exp)[:10]))
        except Exception: pass
    return parse_exp_from_symbol(sym)

_orders_emitted: Set[str] = set()  # emit each order's exec legs once

def rows_from_order(order: Dict[str, Any], meta: Dict[str, Any]) -> List[List[Any]]:
    """Build rows from all execution legs; ts is the leg time; includes fees/commissions."""
    ttype = meta.get("ttype",""); subtype = meta.get("subtype",""); desc = meta.get("desc","")
    net_amount = meta.get("net_amount")
    comm_total = meta.get("comm_total") or 0.0
    fees_total = meta.get("fees_total") or 0.0
    order_id_for_sheet = meta.get("order_id") or meta.get("transaction_id") or meta.get("txn_id") or ""

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
            exp_primary = parse_exp_from_instr_symbol(sym, inst) or ""
            strike = inst.get("strikePrice") or parse_strike_from_occ(sym)
            pc = inst.get("putCall") or parse_pc_from_symbol(sym) or ""
            pc = "CALL" if str(pc).upper().startswith("C") else ("PUT" if str(pc).upper().startswith("P") else "")

            ts = exec_ts(el, order.get("closeTime") or order.get("enteredTime"))

            qty = safe_float(el.get("quantity"))
            price = safe_float(el.get("price"))
            instr = str(leg.get("instruction") or "").upper()
            if instr.startswith("SELL") and qty is not None: qty = -abs(qty)
            elif instr.startswith("BUY") and qty is not None: qty =  abs(qty)

            amt = compute_amount(qty, price, sym, underlying)

            rows.append([
                ts, order_id_for_sheet, ttype, subtype, desc,
                sym, underlying, (exp_primary or ""), (strike if strike is not None else ""), pc,
                (qty if qty is not None else ""), (price if price is not None else ""), (amt if amt is not None else ""),
                (net_amount if net_amount is not None else ""), round(comm_total,2) or "", round(fees_total,2) or "",
                "schwab_txn"
            ])
    return rows

def best_order_for_txn(c, acct_hash: str, order_id: str, ts_dt: Optional[datetime], desc: str) -> Optional[Dict[str, Any]]:
    # Only return an order when we have an explicit orderId.
    if not order_id:
        return None
    o = get_order_by_id(c, acct_hash, order_id)
    return o

# ---------- transaction flattening ----------
def explode_txn_from_items(txn: Dict[str, Any]) -> Tuple[List[List[Any]], Dict[str, Any]]:
    rows: List[List[Any]] = []

    ts = (txn.get("transactionDate") or txn.get("time") or txn.get("date") or "")
    transaction_id = str(txn.get("transactionId") or "")
    order_id = str(txn.get("orderId") or "")
    # Prefer orderId so all partial fills of a single order share the same key
    txn_id_for_sheet = order_id or transaction_id
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
        rows.append([ts, txn_id_for_sheet, ttype, subtype, desc,
                     "", "", "", "", "",
                     "", "", "", (net_amount if net_amount is not None else ""), round(comm_total,2) or "", round(fees_total,2) or "",
                     "schwab_txn"])
        meta = {
            "ts": ts, "transaction_id": transaction_id, "order_id": order_id,
            "ttype": ttype, "subtype": subtype, "desc": desc, "net_amount": net_amount,
            "comm_total": round(comm_total,2), "fees_total": round(fees_total,2)
        }
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

        row = [ts, txn_id_for_sheet, ttype, subtype, desc,
               symbol, underlying, (exp_primary or ""), (strike if strike is not None else ""), pc,
               (qty if qty is not None else ""), (price if price is not None else ""), (amount if amount is not None else ""),
               (net_amount if net_amount is not None else ""), round(comm_total,2) or "", round(fees_total,2) or "",
               "schwab_txn"]
        rows.append(row)

    meta = {
        "ts": ts, "transaction_id": transaction_id, "order_id": order_id,
        "ttype": ttype, "subtype": subtype, "desc": desc, "net_amount": net_amount,
        "comm_total": round(comm_total,2), "fees_total": round(fees_total,2)
    }
    return rows, meta

def explode_txn(c, acct_hash: str, txn: Dict[str, Any]) -> List[List[Any]]:
    rows, meta = explode_txn_from_items(txn)
    ttype = (meta.get("ttype") or "").upper()
    oid_hint = (meta.get("order_id") or "").strip()

    if ttype == "TRADE":
        # Only enrich when we truly know the order id; no guessing.
        if oid_hint:
            if oid_hint in _orders_emitted:
                return []
            order = best_order_for_txn(
                c, acct_hash, oid_hint,
                parse_ts(meta.get("ts")), meta.get("desc","")
            )
            if order:
                effective_oid = str(order.get("orderId") or oid_hint)
                if effective_oid in _orders_emitted:
                    return []
                _orders_emitted.add(effective_oid)
                exec_rows = rows_from_order(order, meta)
                if exec_rows:
                    return exec_rows
    return rows

# ---------- main ----------
def main() -> int:
    try:
        svc, sid = sheets_client()
        ensure_tab_with_header(svc, sid, RAW_TAB, RAW_HEADERS)
    except Exception as e:
        print(f"ABORT: Sheets init failed — {e}")
        return 1

    try:
        c, acct_hash = schwab_client()
    except Exception as e:
        msg = str(e)
        if ("unsupported_token_type" in msg) or ("refresh_token_authentication_error" in msg):
            print("ABORT: Schwab OAuth refresh failed — rotate SCHWAB_TOKEN_JSON secret.")
        else:
            print(f"ABORT: Schwab client init failed — {msg[:200]}")
        return 1

    try:
        days_back = int((os.environ.get("DAYS_BACK") or "60").strip())
    except Exception:
        days_back = 60
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=days_back)

    try:
        txns = get_txns_resilient(c, acct_hash, start_dt, end_dt)
    except Exception as e:
        print(f"ABORT: transactions fetch failed — {e}")
        return 1

    rows: List[List[Any]] = []
    for t in txns:
        rows.extend(explode_txn(c, acct_hash, t))

    overwrite_rows(svc, sid, RAW_TAB, RAW_HEADERS, rows)
    print(f"OK: wrote {len(rows)} rows to {RAW_TAB}. API calls: {CALLS}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
