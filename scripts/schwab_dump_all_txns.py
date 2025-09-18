#!/usr/bin/env python3
"""
Schwab → Sheets RAW loader (ledger-first, per-fill fees).

Design:
- Parse option fills directly from the account ledger (transferItems/fees) so each
  row carries the exact commissions/fees from the fill source of truth.
- Fall back to order executions only when the ledger entry lacks option rows,
  deduping by activity/leg to avoid duplicate fills.
- Overwrites the sheet tab sw_txn_raw each run (no duplicates).

Env:
  GSHEET_ID, GOOGLE_SERVICE_ACCOUNT_JSON
  SCHWAB_APP_KEY, SCHWAB_APP_SECRET, SCHWAB_TOKEN_JSON
  DAYS_BACK (default 60)

Output tab (same header you already use):
  sw_txn_raw with headers = RAW_HEADERS
"""

import os, sys, json, base64, re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from schwab.auth import client_from_token_file
from google.oauth2 import service_account
from googleapiclient.discovery import build as gbuild

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

def to_underlying(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    if not s:
        return ""
    parts = s.split()
    if parts:
        return "SPX" if parts[0].startswith("SPX") else parts[0]
    m = re.match(r"([A-Z]+)\d{6}[CP]\d{8}$", s)
    if m:
        u = m.group(1)
        return "SPX" if u.startswith("SPX") else u
    return "SPX" if s.startswith("SPX") else s

def parse_pc_from_symbol(sym: str) -> str:
    s = (sym or "").upper().replace(" ", "")
    m = re.search(r"\d{6}([CP])", s)
    if not m:
        return ""
    return "CALL" if m.group(1) == "C" else "PUT"

def parse_exp_from_instr_symbol(sym: str, inst: Dict[str, Any]) -> Optional[str]:
    exp = inst.get("expirationDate") or inst.get("optionExpirationDate")
    if exp:
        try:
            return str(date.fromisoformat(str(exp)[:10]))
        except Exception:
            pass
    return parse_exp_from_symbol(sym)

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

def exec_ts(exec_leg: Dict[str, Any], fallback: Any) -> str:
    return fmt_ts(exec_leg.get("time") or exec_leg.get("executionTime") or fallback)

_seen_activity_ids: Set[str] = set()
_orders_emitted: Set[str] = set()

# ---------------- data pulls ----------------
def list_transactions_window(c, acct_hash: str, t0: datetime, t1: datetime) -> List[Dict[str, Any]]:
    """Ledger pull used to drive row generation (and fee totals)."""
    r = c.get_transactions(acct_hash, start_date=t0, end_date=t1)
    # When no content, Schwab returns 204 No Content
    if getattr(r, "status_code", None) == 204: return []
    r.raise_for_status()
    j = r.json()
    return j if isinstance(j, list) else []

def get_order_by_id(c, acct_hash: str, order_id: str) -> Optional[Dict[str, Any]]:
    if not order_id:
        return None
    attempts = [
        (order_id, acct_hash),
        (acct_hash, order_id),
    ]
    for args in attempts:
        try:
            r = c.get_order(*args)
        except TypeError:
            continue
        except Exception:
            return None
        if getattr(r, "status_code", None) == 404:
            return None
        try:
            r.raise_for_status()
        except Exception:
            return None
        data = r.json()
        return data if isinstance(data, dict) else None
    return None

# ---------- transaction flattening (fixed) ----------
def explode_txn_from_items(txn: Dict[str, Any]) -> Tuple[List[List[Any]], Dict[str, Any]]:
    rows: List[List[Any]] = []

    ts = (txn.get("transactionDate") or txn.get("time") or txn.get("date") or "")
    transaction_id = str(txn.get("transactionId") or "")
    order_id = str(txn.get("orderId") or "")
    txn_id_for_sheet = order_id or transaction_id
    ttype = str(txn.get("type") or txn.get("transactionType") or "")
    subtype = str(txn.get("subType") or "")
    desc = str(txn.get("description") or "")
    net_amount = safe_float(txn.get("netAmount"))

    # fees can live under txn["fees"] *or* per-item as CURRENCY transferItems with feeType
    comm_total = 0.0; fees_total = 0.0
    # legacy places:
    if isinstance(txn.get("fees"), dict):
        for k, v in (txn["fees"] or {}).items():
            val = abs(safe_float(v) or 0.0)
            if "comm" in k.lower(): comm_total += val
            else: fees_total += val
    elif isinstance(txn.get("fees"), list):
        for f in (txn["fees"] or []):
            val = abs(safe_float(f.get("amount")) or 0.0)
            name = str(f.get("feeType") or f.get("type") or "")
            if "comm" in name.lower(): comm_total += val
            else: fees_total += val

    # new place (Schwab ledger): transferItems
    for ti in (txn.get("transferItems") or []):
        ft = str(ti.get("feeType") or "")
        if ft:
            val = abs(safe_float(ti.get("cost")) or safe_float(ti.get("amount")) or 0.0)
            if "comm" in ft.lower(): comm_total += val
            else: fees_total += val

    # Items to emit (prefer transferItems)
    items = (txn.get("transactionItems")
             or txn.get("transactionItem")
             or txn.get("transferItems")
             or [])
    if isinstance(items, dict): items = [items]

    for it in items:
        inst = it.get("instrument") or {}
        if (inst.get("assetType") or "").upper() != "OPTION":
            continue  # only option fills

        symbol = str(inst.get("symbol") or "")
        underlying = (inst.get("underlyingSymbol") or to_underlying(symbol) or "").upper()
        if underlying.startswith("SPX"): underlying = "SPX"

        # expiration / strike / putCall
        exp_primary = None
        exp = inst.get("expirationDate") or inst.get("optionExpirationDate")
        if exp:
            try: exp_primary = str(date.fromisoformat(str(exp)[:10]))
            except Exception: pass
        if not exp_primary:
            exp_primary = parse_exp_from_symbol(symbol)
        strike = safe_float(inst.get("strikePrice") or parse_strike_from_occ(symbol))
        pc = (inst.get("putCall") or parse_pc_from_symbol(symbol) or "")
        pc = "CALL" if str(pc).upper().startswith("C") else ("PUT" if str(pc).upper().startswith("P") else "")

        # quantity & price: in transferItems, "amount" is +/- contracts; price is positive
        qty = safe_float(it.get("quantity"))
        if qty is None:
            qty = safe_float(it.get("amount"))  # +/-1 per single-contract fill
        price = safe_float(it.get("price") or inst.get("price"))

        amt = compute_amount(qty, price, symbol, underlying)

        rows.append([
            ts, txn_id_for_sheet, ttype, subtype, desc,
            symbol, underlying, (exp_primary or ""), (strike if strike is not None else ""), pc,
            (qty if qty is not None else ""), (price if price is not None else ""), (amt if amt is not None else ""),
            (net_amount if net_amount is not None else ""), round(comm_total,2) or "", round(fees_total,2) or "",
            "schwab_txn"
        ])

    meta = {
        "ts": ts, "transaction_id": transaction_id, "order_id": order_id,
        "ttype": ttype, "subtype": subtype, "desc": desc, "net_amount": net_amount,
        "comm_total": round(comm_total,2), "fees_total": round(fees_total,2)
    }
    return rows, meta

def rows_from_order(order: Dict[str, Any], meta: Dict[str, Any]) -> List[List[Any]]:
    """Emit one row per filled execution leg; filter out non-fills; dedupe by (activityId, legId)."""
    ttype = meta.get("ttype",""); subtype = meta.get("subtype",""); desc = meta.get("desc","")
    net_amount = meta.get("net_amount")
    comm_total = meta.get("comm_total") or 0.0
    fees_total = meta.get("fees_total") or 0.0
    order_id_for_sheet = str(order.get("orderId") or meta.get("order_id") or meta.get("transaction_id") or "")

    legs = order.get("orderLegCollection") or []
    leg_by_id = { int(lg.get("legId")): lg for lg in legs if lg.get("legId") is not None }

    seen: Set[Tuple[int,int]] = set()  # (activityId, legId)
    rows: List[List[Any]] = []

    for act in (order.get("orderActivityCollection") or []):
        if str(act.get("activityType")).upper() != "EXECUTION": continue
        if str(act.get("executionType") or "").upper() != "FILL": continue
        aid = int(act.get("activityId") or -1)
        for el in (act.get("executionLegs") or []):
            lid = int(el.get("legId", -1))
            if (aid, lid) in seen:
                continue
            seen.add((aid, lid))

            leg = leg_by_id.get(lid, {})
            inst = leg.get("instrument") or {}
            sym = str(inst.get("symbol") or "")
            underlying = (inst.get("underlyingSymbol") or to_underlying(sym) or "").upper()
            if underlying.startswith("SPX"): underlying = "SPX"
            exp_primary = parse_exp_from_instr_symbol(sym, inst) or ""
            strike = safe_float(inst.get("strikePrice") or parse_strike_from_occ(sym))
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
                "schwab_order"
            ])

    return rows

def explode_txn(c, acct_hash: str, txn: Dict[str, Any]) -> List[List[Any]]:
    # dedupe per-ledger activity
    aid = str(txn.get("activityId") or "")
    if aid:
        if aid in _seen_activity_ids:
            return []
        _seen_activity_ids.add(aid)

    rows, meta = explode_txn_from_items(txn)
    ttype = (meta.get("ttype") or "").upper()
    oid_hint = (meta.get("order_id") or "").strip()

    # If we got valid option rows from the ledger, return them (best source of truth, carries per-fill fees).
    if rows:
        return rows

    # Fallback: if it's a trade with a real order id, emit the order's filled execution legs once.
    if ttype == "TRADE" and oid_hint:
        if oid_hint in _orders_emitted:
            return []
        order = get_order_by_id(c, acct_hash, oid_hint)
        if order:
            effective_oid = str(order.get("orderId") or oid_hint)
            if effective_oid in _orders_emitted:
                return []
            _orders_emitted.add(effective_oid)
            exec_rows = rows_from_order(order, meta)
            if exec_rows:
                return exec_rows

    # Last resort: keep the single summary row (rare)
    single = [meta.get("ts"), (oid_hint or meta.get("transaction_id") or ""), meta.get("ttype"), meta.get("subtype"), meta.get("desc"),
              "", "", "", "", "", "", "", "", meta.get("net_amount"), meta.get("comm_total"), meta.get("fees_total"),
              "schwab_txn"]
    return [single]

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

    # Pull transactions once for the window (drives RAW rows)
    try:
        txns = list_transactions_window(c, acct_hash, start_dt, end_dt)
    except Exception as e:
        print(f"ABORT: transactions fetch failed — {e}")
        return 1

    _seen_activity_ids.clear()
    _orders_emitted.clear()

    all_rows: List[List[Any]] = []
    errors = 0
    for txn in txns:
        try:
            rows = explode_txn(c, acct_hash, txn)
        except Exception as exc:
            errors += 1
            tid = txn.get("transactionId") or txn.get("orderId") or "?"
            print(f"NOTE: failed to parse txn {tid}: {exc}")
            continue
        all_rows.extend(rows)

    overwrite_rows(svc, sid, RAW_TAB, RAW_HEADERS, all_rows)
    print(f"OK: wrote {len(all_rows)} rows from {len(txns)} ledger activities to {RAW_TAB}." + (f" ({errors} errors)" if errors else ""))
    return 0

if __name__ == "__main__":
    sys.exit(main())
