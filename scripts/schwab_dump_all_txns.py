#!/usr/bin/env python3
"""
Schwab → Sheets RAW loader (orders-first, fees from ledger).

Design:
- Build execution rows from FILLED orders only (one row per execution leg).
- Collect per-order fees from the ledger (transactions) and attach totals to
  each order's rows. The summarizer will allocate fees across expiries by |amount|.
- Overwrites the sheet tab sw_txn_raw each run (no duplicates).

Env:
  GSHEET_ID, GOOGLE_SERVICE_ACCOUNT_JSON
  SCHWAB_APP_KEY, SCHWAB_APP_SECRET, SCHWAB_TOKEN_JSON
  DAYS_BACK (default 60)

Output tab (same header you already use):
  sw_txn_raw with headers = RAW_HEADERS
"""

import os, sys, json, base64, re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
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

# ---------------- Sheets helpers ----------------
def sheets_client():
    sid = os.environ["GSHEET_ID"]
    sa_json = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
    # GitHub Secrets may be base64—support both
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
    return gbuild("sheets","v4",credentials=creds), sid

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

# ---------------- Schwab auth ----------------
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

# ---------------- parsing helpers ----------------
def safe_float(x) -> Optional[float]:
    try: return float(x)
    except Exception: return None

def normalize_underlying(u: str) -> str:
    u = (u or "").upper()
    return "SPX" if u.startswith("SPX") else u

def parse_pc(pc: Any) -> str:
    s = str(pc or "").upper()
    if s.startswith("C"): return "CALL"
    if s.startswith("P"): return "PUT"
    return ""

def parse_exp_from_symbol(sym: str) -> Optional[str]:
    if not sym: return None
    s = sym.strip().upper().replace(" ","")
    m = re.search(r"\D(\d{6})[CP]\d", s)
    if m:
        try:
            return datetime.strptime(m.group(1), "%y%m%d").date().isoformat()
        except Exception:
            return None
    m = re.search(r"(20\d{2}-\d{2}-\d{2})", s)
    return m.group(1) if m else None

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
    return round(qty * price * contract_multiplier(symbol, underlying), 2)

def fmt_ts(ts_any: Any) -> str:
    s = str(ts_any or "")
    try:
        from dateutil import parser
        dt = parser.parse(s)
        z = dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        return f"{z[:-2]}:{z[-2:]}" if z else s
    except Exception:
        return s

# ---------------- data pulls ----------------
def list_orders_window(c, acct_hash: str, t0: datetime, t1: datetime) -> List[Dict[str, Any]]:
    """Return a flat list of orders in [t0,t1], regardless of Schwab shape."""
    r = c.get_orders_for_account(acct_hash, from_entered_datetime=t0, to_entered_datetime=t1)
    r.raise_for_status()
    j = r.json()

    # Flatten arbitrary shapes into a list of orders
    out: List[Dict[str, Any]] = []
    def scan(o):
        if isinstance(o, list):
            for x in o: scan(x)
        elif isinstance(o, dict):
            if "orderId" in o and "orderLegCollection" in o: out.append(o)
            else:
                for v in o.values(): scan(v)
    scan(j)
    return out

def list_transactions_window(c, acct_hash: str, t0: datetime, t1: datetime) -> List[Dict[str, Any]]:
    """Ledger pull used ONLY to compute per-order fees."""
    r = c.get_transactions(acct_hash, start_date=t0, end_date=t1)
    # When no content, Schwab returns 204 No Content
    if getattr(r, "status_code", None) == 204: return []
    r.raise_for_status()
    j = r.json()
    return j if isinstance(j, list) else []

# ---------------- fees from ledger ----------------
def fees_by_order_id(txns: List[Dict[str, Any]]) -> Dict[str, Tuple[float,float]]:
    """Return {orderId: (commission_total, fees_other_total)}."""
    out: Dict[str, Tuple[float,float]] = {}
    for t in txns:
        ttype = (t.get("type") or t.get("transactionType") or "").upper()
        if ttype != "TRADE": 
            continue
        oid = str(t.get("orderId") or "").strip()
        if not oid:
            continue
        comm, other = out.get(oid, (0.0, 0.0))
        for ti in (t.get("transferItems") or []):
            fee_type = str(ti.get("feeType") or "").upper()
            amt = abs(safe_float(ti.get("amount")) or 0.0)
            if "COMM" in fee_type:
                comm += amt
            elif "FEE" in fee_type:
                other += amt
        out[oid] = (round(comm,2), round(other,2))
    return out

# ---------------- order → rows ----------------
def rows_from_filled_order(order: Dict[str, Any], comm_total: float, fees_total: float) -> List[List[Any]]:
    """One row per execution leg (FILLs only). Attach per-order fee totals."""
    if (order.get("status") or "").upper() != "FILLED":
        return []
    legs = order.get("orderLegCollection") or []
    leg_by_id = { int(lg.get("legId")): lg for lg in legs if lg.get("legId") is not None }

    rows: List[List[Any]] = []
    for act in (order.get("orderActivityCollection") or []):
        if (act.get("executionType") or "").upper() != "FILL":
            continue
        for el in (act.get("executionLegs") or []):
            leg = leg_by_id.get(int(el.get("legId", -1)), {})
            inst = leg.get("instrument") or {}
            sym = str(inst.get("symbol") or "")
            und = normalize_underlying(inst.get("underlyingSymbol") or "")
            exp = inst.get("optionExpirationDate")
            exp_primary = str(exp)[:10] if exp else (parse_exp_from_symbol(sym) or "")
            strike = inst.get("strikePrice") or parse_strike_from_occ(sym)
            pc = parse_pc(inst.get("putCall"))

            instr = str(leg.get("instruction") or "").upper()
            qty = safe_float(el.get("quantity")) or 0.0
            price = safe_float(el.get("price")) or 0.0
            if instr.startswith("SELL"): qty = -abs(qty)
            else: qty = abs(qty)

            amt = compute_amount(qty, price, sym, und)
            ts  = fmt_ts(el.get("time") or el.get("executionTime") or order.get("closeTime") or order.get("enteredTime"))

            rows.append([
                ts, str(order.get("orderId") or ""), "TRADE", "", "",
                sym, und, (exp_primary or ""), (strike if strike is not None else ""), pc,
                qty, price, (amt if amt is not None else ""), "",
                round(comm_total,2) or "", round(fees_total,2) or "",
                "schwab_order"
            ])
    return rows

# ---------------- main ----------------
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

    # Pull orders once for the window (drives RAW)
    try:
        orders = list_orders_window(c, acct_hash, start_dt, end_dt)
    except Exception as e:
        print(f"ABORT: orders fetch failed — {e}")
        return 1

    # Pull transactions once for the window (only to collect per-order fees)
    try:
        txns = list_transactions_window(c, acct_hash, start_dt, end_dt)
    except Exception as e:
        print(f"NOTE: transactions fetch failed (fees will be zero) — {e}")
        txns = []

    fees_map = fees_by_order_id(txns)

    # Build rows from FILLED orders
    all_rows: List[List[Any]] = []
    for o in orders:
        oid = str(o.get("orderId") or "")
        comm, other = fees_map.get(oid, (0.0, 0.0))
        all_rows.extend(rows_from_filled_order(o, comm, other))

    overwrite_rows(svc, sid, RAW_TAB, RAW_HEADERS, all_rows)
    n_orders = sum(1 for o in orders if (o.get("status") or "").upper() == "FILLED")
    print(f"OK: wrote {len(all_rows)} rows from {n_orders} filled orders to {RAW_TAB}.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
