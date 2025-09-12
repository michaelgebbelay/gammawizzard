#!/usr/bin/env python3
# Schwab → Sheets (Expiry-based P&L Summary; no estimates)
# - Uses schwab-py "transactions" endpoint correctly (transaction_types=[TRADE])
# - Joins orders in the same window to extract OSI legs and EXPIRATION DATE
# - Writes two tabs via batch updates (low write-rate):
#     1) tos_txn_raw   – normalized trade transactions (no estimates)
#     2) expiry_summary – aggregated by expiration date (+ underlying)
#
# Env:
#   GSHEET_ID, GOOGLE_SERVICE_ACCOUNT_JSON
#   SCHWAB_APP_KEY, SCHWAB_APP_SECRET, SCHWAB_TOKEN_JSON
#   DAYS_BACK (optional, default 14)
#   SYMBOL_FILTER (optional, default "SPX" to include SPX/SPXW; set "" for all)
#   SHEET_TXN_TAB (optional, default "tos_txn_raw")
#   SHEET_SUM_TAB (optional, default "expiry_summary")

import os, json, base64, re, sys
from datetime import datetime, timedelta, timezone, date
from zoneinfo import ZoneInfo

from schwab.auth import client_from_token_file
from schwab.client import Client

from google.oauth2 import service_account
from googleapiclient.discovery import build as gbuild


# ---------- config ----------
ET = ZoneInfo("America/New_York")

SHEET_TXN_TAB = os.environ.get("SHEET_TXN_TAB", "tos_txn_raw").strip()
SHEET_SUM_TAB = os.environ.get("SHEET_SUM_TAB", "expiry_summary").strip()

DEFAULT_DAYS_BACK = 14
try:
    DAYS_BACK = int(os.environ.get("DAYS_BACK", str(DEFAULT_DAYS_BACK)))
    DAYS_BACK = max(1, min(DAYS_BACK, 60))  # Schwab window limit
except Exception:
    DAYS_BACK = DEFAULT_DAYS_BACK

SYMBOL_FILTER = os.environ.get("SYMBOL_FILTER", "SPX").strip().upper()  # "", "SPX", etc.


# ---------- tiny helpers ----------
def _decode_token_to_path(token_env: str) -> str:
    """Support both raw JSON and base64 JSON."""
    path = "/tmp/schwab_token.json"
    if token_env:
        s = token_env
        try:
            dec = base64.b64decode(token_env).decode("utf-8")
            if dec.strip().startswith("{"):
                s = dec
        except Exception:
            pass
        with open(path, "w") as f:
            f.write(s)
    return path

def utc_now():
    return datetime.now(timezone.utc)

def dt_floor_day_et(dt_utc: datetime) -> datetime:
    dt_et = dt_utc.astimezone(ET)
    floor = dt_et.replace(hour=0, minute=0, second=0, microsecond=0)
    return floor.astimezone(timezone.utc)

def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def to_osi(sym: str) -> str:
    """
    Normalize an OCC/OSI-like symbol (as Schwab often returns with spaces):
    'SPXW  250910C06540000' -> 'SPXW  250910C06540000' (already OSI, but
    ensure it's parseable and uppercase and spacing trimmed)
    """
    raw = (sym or "").upper()
    raw = re.sub(r"\s+", " ", raw).strip()
    # If it already looks like OSI 'ROOT<spaces>YYMMDDC/P<strike8>', keep as is.
    # We'll parse expiration from it below.
    return raw

def osi_to_expiry(osi: str) -> date | None:
    """
    Extract expiry date from OSI-like string:
    Root (<=6) + spaces + YYMMDD + C/P + strike(8)
    Example: 'SPXW  250910C06540000'
    """
    s = (osi or "").replace(".", "").upper()
    s = re.sub(r"\s+", " ", s)
    m = re.search(r"\b(\d{6})([CP])(\d{8})\b", s)
    if not m:
        return None
    yymmdd = m.group(1)
    yy = int(yymmdd[0:2]); mm = int(yymmdd[2:4]); dd = int(yymmdd[4:6])
    yyyy = 2000 + yy if yy < 70 else 1900 + yy
    try:
        return date(yyyy, mm, dd)
    except Exception:
        return None

def normalize_underlying(sym: str) -> str:
    """
    For OSI-like SPX / SPXW legs, return 'SPX'.
    Otherwise, return uppercase root (best-effort).
    """
    s = (sym or "").upper()
    if "SPXW" in s or " SPX " in f" {s} " or s.startswith("SPXW"):
        return "SPX"
    # crude root extraction before YYMMDD or delimiter
    m = re.match(r"^([A-Z.$^]{1,6})\b", s.replace(" ", ""))
    return (m.group(1) if m else "UNK")

def sum_fees_blob(txn: dict) -> float:
    fees = txn.get("fees") or {}
    total = 0.0
    for k, v in fees.items():
        try:
            total += float(v or 0.0)
        except Exception:
            pass
    return total

def first_nonempty(*vals):
    for v in vals:
        if v not in (None, "", [], {}):
            return v
    return ""


# ---------- Sheets helpers (batch write) ----------
def sheets_client():
    creds = service_account.Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gbuild("sheets", "v4", credentials=creds)

def ensure_tab_and_header(svc, spreadsheet_id: str, tab: str, headers: list):
    meta = svc.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sid = None
    for sh in meta.get("sheets", []):
        if sh["properties"]["title"] == tab:
            sid = sh["properties"]["sheetId"]
            break
    if sid is None:
        svc.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": tab}}}]},
        ).execute()
        meta = svc.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sid = next(sh["properties"]["sheetId"] for sh in meta["sheets"] if sh["properties"]["title"] == tab)
    # Header
    got = svc.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=f"{tab}!1:1").execute().get("values", [])
    if not got or got[0] != headers:
        svc.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{tab}!1:1",
            valueInputOption="RAW",
            body={"values": [headers]},
        ).execute()
    return sid

def replace_rows(svc, spreadsheet_id: str, tab: str, rows: list):
    """
    Replace all rows below header (row 1) with provided rows in one call.
    """
    if not rows:
        # Just clear everything below the header
        svc.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range=f"{tab}!2:999999",
            body={}
        ).execute()
        return

    # Clear old body
    svc.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=f"{tab}!2:999999",
        body={}
    ).execute()

    # Bulk write
    svc.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"{tab}!A2",
        valueInputOption="RAW",
        body={"values": rows},
    ).execute()


# ---------- Schwab fetchers ----------
def schwab_client():
    token_env = os.environ.get("SCHWAB_TOKEN_JSON", "")
    token_path = _decode_token_to_path(token_env)
    c = client_from_token_file(
        token_path=token_path,
        api_key=os.environ["SCHWAB_APP_KEY"],
        app_secret=os.environ["SCHWAB_APP_SECRET"],
    )
    return c

def get_account_hash(c: Client) -> str:
    r = c.get_account_numbers()
    r.raise_for_status()
    arr = r.json()
    return arr[0]["hashValue"]

def fetch_transactions(c: Client, acct_hash: str, start: datetime, end: datetime):
    # Correct usage per schwab-py docs: transaction_types=[Client.Transactions.TransactionType.TRADE]
    r = c.get_transactions(
        acct_hash,
        start_date=start,
        end_date=end,
        transaction_types=[Client.Transactions.TransactionType.TRADE],
    )
    r.raise_for_status()
    return r.json() or []

def fetch_orders_map(c: Client, acct_hash: str, start: datetime, end: datetime):
    """
    Return orderId -> list of (leg_symbol, expiry_date) using OSI parse.
    We query a broad set of statuses within the date window.
    """
    statuses = [
        Client.Order.Status.FILLED,
        Client.Order.Status.CANCELED,
        Client.Order.Status.REPLACED,
        Client.Order.Status.EXPIRED,
        Client.Order.Status.WORKING,
        Client.Order.Status.QUEUED,
        Client.Order.Status.PENDING_ACTIVATION,
    ]
    r = c.get_orders_for_account(
        acct_hash,
        from_entered_datetime=start,
        to_entered_datetime=end,
        statuses=statuses
    )
    r.raise_for_status()
    data = r.json() or []
    omap = {}
    for o in (data if isinstance(data, list) else [data]):
        oid = str(o.get("orderId", ""))
        if not oid:
            continue
        legs = []
        for leg in (o.get("orderLegCollection") or []):
            ins = (leg.get("instrument") or {})
            sym = ins.get("symbol") or ""
            if not sym:
                # try constructing OSI from structured fields if present
                exp = ins.get("optionExpirationDate") or ins.get("expirationDate") or ""
                pc = (ins.get("putCall") or ins.get("type") or "").upper()
                strike = ins.get("strikePrice") or ins.get("strike")
                if exp and pc and strike is not None:
                    try:
                        ymd = date.fromisoformat(str(exp)[:10]).strftime("%y%m%d")
                        cp = "C" if pc.startswith("C") else "P"
                        mills = int(round(float(strike) * 1000))
                        sym = f"SPXW  {ymd}{cp}{mills:08d}"
                    except Exception:
                        pass
            if sym:
                osi = to_osi(sym)
                expd = osi_to_expiry(osi)
                legs.append((osi, expd))
        if legs:
            omap[oid] = legs
    return omap


# ---------- main logic ----------
def main():
    # Time window (UTC)
    now = utc_now()
    start = now - timedelta(days=DAYS_BACK)

    # Schwab client
    c = schwab_client()
    acct_hash = get_account_hash(c)

    # Pull transactions and orders within same window
    txns = fetch_transactions(c, acct_hash, start=start, end=now)
    orders_map = fetch_orders_map(c, acct_hash, start=start, end=now)

    # Build normalized rows
    txn_headers = [
        "ts", "order_id", "transaction_id", "action", "qty",
        "symbol_raw", "underlying", "expiry", "cp", "strike",
        "price", "net_amount", "fees_total", "description"
    ]
    txn_rows = []

    # Aggregation by (expiry, underlying)
    agg = {}  # key=(expiry, underlying) -> dict

    def add_to_agg(expd: date | None, und: str, order_id: str, qty: float, net_amount: float, fees: float):
        key = (expd.isoformat() if isinstance(expd, date) else "", und)
        d = agg.get(key) or {"contracts": 0.0, "net_amount": 0.0, "fees": 0.0, "orders": set()}
        d["contracts"] += float(qty or 0.0)
        d["net_amount"] += float(net_amount or 0.0)
        d["fees"] += float(fees or 0.0)
        if order_id:
            d["orders"].add(order_id)
        agg[key] = d

    # Walk transactions
    for t in (txns if isinstance(txns, list) else [txns]):
        # Timestamps / ids
        ts = first_nonempty(t.get("transactionDate"), t.get("time"), t.get("date"))
        order_id = str(first_nonempty(t.get("orderId"), t.get("order_id"), "")) or ""
        transaction_id = str(first_nonempty(t.get("transactionId"), t.get("transaction_id"), "")) or ""

        # Amounts (as reported by Schwab)
        net_amount = None
        for k in ("netAmount", "amount", "netamount"):
            if t.get(k) is not None:
                try:
                    net_amount = float(t[k])
                    break
                except Exception:
                    pass
        fees_total = sum_fees_blob(t)

        # Transaction item payload (one leg or summary)
        ti = t.get("transactionItem") or {}
        action = first_nonempty(
            ti.get("instruction"),
            ti.get("positionEffect"),
            t.get("type"),
            t.get("description")
        )
        qty = first_nonempty(ti.get("amount"), ti.get("quantity"), t.get("quantity"))
        try:
            qty = float(qty) if qty is not None else None
        except Exception:
            qty = None

        price = ti.get("price") if isinstance(ti, dict) else None
        try:
            price = float(price) if price is not None else None
        except Exception:
            price = None

        # Instrument symbol + derived fields
        ins = ti.get("instrument") or {}
        sym_raw = first_nonempty(ins.get("symbol"), ins.get("optionSymbol"), "")
        osi = to_osi(sym_raw) if sym_raw else ""

        expd = osi_to_expiry(osi)
        cp = ""
        strike = ""
        if osi:
            m = re.search(r"\b(\d{6})([CP])(\d{8})\b", osi.replace(".", ""))
            if m:
                cp = m.group(2)
                strike = str(int(m.group(3)) / 1000.0)

        # If missing expiry from txn leg, try order legs
        if (not expd) and order_id and order_id in orders_map:
            # pick first leg expiry (for grouping); this is fine for spreads whose legs share same expiry
            legs = orders_map[order_id]
            expd = next((e for (_, e) in legs if e), None)
            if not sym_raw and legs:
                sym_raw = legs[0][0]
                osi = legs[0][0]
                # extract cp/strike from this leg for display
                m2 = re.search(r"\b(\d{6})([CP])(\d{8})\b", osi.replace(".", ""))
                if m2:
                    cp = m2.group(2)
                    strike = str(int(m2.group(3)) / 1000.0)

        underlying = normalize_underlying(osi or sym_raw)

        # Symbol filter (default SPX/SPXW only)
        if SYMBOL_FILTER and SYMBOL_FILTER not in (underlying or ""):
            continue

        # Collect raw row
        txn_rows.append([
            ts, order_id, transaction_id, action, qty,
            sym_raw, underlying, (expd.isoformat() if isinstance(expd, date) else ""),
            cp, strike,
            price, net_amount, fees_total, first_nonempty(t.get("description"), "")
        ])

        # Aggregate (by expiry & underlying)
        add_to_agg(expd, underlying, order_id, (qty or 0.0), (net_amount or 0.0), (fees_total or 0.0))

    # Build summary rows
    sum_headers = ["expiry", "underlying", "contracts", "distinct_orders", "net_amount", "fees_total"]
    sum_rows = []
    for (exp_str, und), d in sorted(agg.items(), key=lambda kv: (kv[0][0], kv[0][1])):
        sum_rows.append([
            exp_str, und, d["contracts"], len(d["orders"]), round(d["net_amount"], 2), round(d["fees"], 2)
        ])

    # Write to Sheets (two batch updates)
    svc = sheets_client()
    spreadsheet_id = os.environ["GSHEET_ID"]

    ensure_tab_and_header(svc, spreadsheet_id, SHEET_TXN_TAB, txn_headers)
    ensure_tab_and_header(svc, spreadsheet_id, SHEET_SUM_TAB, sum_headers)

    replace_rows(svc, spreadsheet_id, SHEET_TXN_TAB, txn_rows)
    replace_rows(svc, spreadsheet_id, SHEET_SUM_TAB, sum_rows)

    print(f"Wrote {len(txn_rows)} rows to {SHEET_TXN_TAB} and {len(sum_rows)} rows to {SHEET_SUM_TAB}.")


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print("ABORT:", e)
        sys.exit(1)
