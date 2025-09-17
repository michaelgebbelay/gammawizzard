#!/usr/bin/env python3
"""
Schwab → Sheets raw transaction dump (no filters), robust to Schwab ZonedDateTime requirements.

- Uses startDate/endDate as ZonedDateTime (tries ET with colon, ET w/o colon, and UTC Z).
- Chunks the pull window by ET calendar days with NO overlap.
- Accepts 204 (empty) without failing.
- Flattens to columns (no JSON blobs) to keep 'text-to-columns' style.
- Clears and overwrites 'sw_txn_raw' in one write to avoid Sheets rate limits.
- Accepts GOOGLE_SERVICE_ACCOUNT_JSON and SCHWAB_TOKEN_JSON as raw JSON or base64(JSON).

Env:
  GSHEET_ID, GOOGLE_SERVICE_ACCOUNT_JSON
  SCHWAB_APP_KEY, SCHWAB_APP_SECRET, SCHWAB_TOKEN_JSON (JSON or base64(JSON))
  DAYS_BACK (optional, default 60)
"""

import os, sys, json, base64, re, time
from datetime import datetime, timedelta, timezone, date
from typing import Any, Dict, List, Optional

from zoneinfo import ZoneInfo
from schwab.auth import client_from_token_file
from google.oauth2 import service_account
from googleapiclient.discovery import build as gbuild

RAW_TAB = "sw_txn_raw"

RAW_HEADERS = [
    # identity
    "ts", "txn_id", "type", "sub_type", "description",
    # instrument
    "symbol", "underlying", "exp_primary", "strike", "put_call",
    # economics (Schwab fields only; no estimates)
    "quantity", "price", "amount", "net_amount", "commissions", "fees_other",
    # misc
    "source"
]

ET = ZoneInfo("America/New_York")


# ---------- Sheets helpers ----------
def sheets_client():
    sid = os.environ["GSHEET_ID"]
    sa_json = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]

    # Accept raw JSON or base64(JSON)
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
    svc = gbuild("sheets", "v4", credentials=creds)
    return svc, sid


def ensure_tab_with_header(svc, sid: str, tab: str, headers: List[str]) -> None:
    meta = svc.spreadsheets().get(spreadsheetId=sid).execute()
    tabs = [s["properties"]["title"] for s in meta.get("sheets", [])]
    if tab not in tabs:
        svc.spreadsheets().batchUpdate(
            spreadsheetId=sid,
            body={"requests": [{"addSheet": {"properties": {"title": tab}}}]}
        ).execute()
    got = svc.spreadsheets().values().get(spreadsheetId=sid, range=f"{tab}!1:1").execute().get("values", [])
    if not got or got[0] != headers:
        svc.spreadsheets().values().update(
            spreadsheetId=sid,
            range=f"{tab}!A1",
            valueInputOption="USER_ENTERED",
            body={"values": [headers]}
        ).execute()


def overwrite_rows(svc, sid: str, tab: str, headers: List[str], rows: List[List[Any]]) -> None:
    # Clear entire tab to avoid stale tails from previous runs
    svc.spreadsheets().values().clear(spreadsheetId=sid, range=tab).execute()
    svc.spreadsheets().values().update(
        spreadsheetId=sid,
        range=f"{tab}!A1",
        valueInputOption="USER_ENTERED",
        body={"values": [headers] + rows}
    ).execute()


# ---------- Schwab auth ----------
def decode_token_to_path() -> str:
    token_env = os.environ.get("SCHWAB_TOKEN_JSON", "") or ""
    token_path = "schwab_token.json"
    if token_env:
        try:
            dec = base64.b64decode(token_env).decode("utf-8")
            if dec.strip().startswith("{"):
                token_env = dec
        except Exception:
            pass
        with open(token_path, "w") as f:
            f.write(token_env)
    return token_path


def schwab_client():
    token_path = decode_token_to_path()
    app_key = os.environ["SCHWAB_APP_KEY"]
    app_secret = os.environ["SCHWAB_APP_SECRET"]
    c = client_from_token_file(token_path, app_key, app_secret)
    r = c.get_account_numbers()
    r.raise_for_status()
    acct_hash = r.json()[0]["hashValue"]
    return c, acct_hash


# ---------- time/format helpers ----------
def start_of_day(dt: datetime, tz=ET) -> datetime:
    return dt.astimezone(tz).replace(hour=0, minute=0, second=0, microsecond=0)


def end_of_day(dt: datetime, tz=ET) -> datetime:
    return dt.astimezone(tz).replace(hour=23, minute=59, second=59, microsecond=0)


def fmt_et_with_colon(dt: datetime) -> str:
    # 2025-09-12T00:00:00-04:00
    local = dt.astimezone(ET)
    s = local.strftime("%Y-%m-%dT%H:%M:%S%z")  # -0400
    return f"{s[:-2]}:{s[-2:]}"  # -> -04:00


def fmt_et_no_colon(dt: datetime) -> str:
    # 2025-09-12T00:00:00-0400
    return dt.astimezone(ET).strftime("%Y-%m-%dT%H:%M:%S%z")


def fmt_utc_z(dt: datetime) -> str:
    # 2025-09-12T00:00:00Z
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------- transaction fetch (robust param shapes + chunking) ----------
def get_txns_chunk(c, acct_hash: str, dt0: datetime, dt1: datetime) -> List[Dict[str, Any]]:
    """
    Try the 3 most common ZonedDateTime shapes Schwab accepts:
      1) ET with colon offset:  2025-09-12T00:00:00-04:00
      2) ET without colon:      2025-09-12T00:00:00-0400
      3) UTC Z:                 2025-09-12T00:00:00Z
    Accept 204 as "no transactions".
    """
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
            if r.status_code == 200:
                j = r.json()
                if isinstance(j, list):
                    return j
                if isinstance(j, dict) and "transactions" in j:
                    return j.get("transactions") or []
                return []
            if r.status_code == 204:
                return []  # valid empty
            last_err = f"{r.status_code}:{(r.text or '')[:160]}"
            print(f"NOTE: txns shape={tag} → {last_err}")
        except Exception as e:
            last_err = f"EXC:{e}"
            print(f"NOTE: txns shape={tag} exception: {e}")
        time.sleep(0.4)
    raise RuntimeError(f"transactions fetch failed for chunk — {last_err}")


def get_txns_resilient(c, acct_hash: str, start_dt: datetime, end_dt: datetime) -> List[Dict[str, Any]]:
    """
    Pull in non-overlapping 30-day blocks using ET calendar days.
    """
    txns: List[Dict[str, Any]] = []
    cur_day = start_dt.astimezone(ET).date()
    end_day = end_dt.astimezone(ET).date()

    while cur_day <= end_day:
        # inclusive block: cur_day .. cur_day+29 (or end_day)
        chunk_end_day = min(cur_day + timedelta(days=29), end_day)

        # any tz-aware placeholders; get_txns_chunk normalizes to ET day bounds
        dt0 = datetime.combine(cur_day, datetime.min.time(), tzinfo=timezone.utc)
        dt1 = datetime.combine(chunk_end_day, datetime.min.time(), tzinfo=timezone.utc)

        print(f"Pulling {cur_day} → {chunk_end_day} (ET)…")
        txns += get_txns_chunk(c, acct_hash, dt0, dt1)

        cur_day = chunk_end_day + timedelta(days=1)  # advance to next day (no overlap)

    return txns


# ---------- parsing/flattening ----------
def safe_float(x) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None


def parse_exp_from_symbol(sym: str) -> Optional[str]:
    if not sym:
        return None
    s = sym.strip().upper()
    m = re.search(r"(20\d{2}-\d{2}-\d{2})", s)
    if m:
        return m.group(1)
    m = re.search(r"\D(\d{6})[CP]\d", s)  # yymmdd
    if m:
        try:
            return datetime.strptime(m.group(1), "%y%m%d").date().isoformat()
        except Exception:
            return None
    return None


def to_underlying(sym: str) -> Optional[str]:
    if not sym:
        return None
    s = sym.strip().upper().lstrip(".").replace("  ", " ")
    if s.startswith("SPXW") or s.startswith("SPX"):
        return "SPX"
    m = re.match(r"([A-Z.$^]{1,6})", s)
    return m.group(1) if m else None


def explode_txn(txn: Dict[str, Any]) -> List[List[Any]]:
    out: List[List[Any]] = []

    ts = (txn.get("transactionDate") or txn.get("time") or txn.get("date") or "")
    txn_id = str(txn.get("transactionId") or txn.get("orderId") or txn.get("id") or "")
    ttype = str(txn.get("type") or txn.get("transactionType") or "")
    subtype = str(txn.get("subType") or "")
    desc = str(txn.get("description") or "")

    # commissions & other fees
    fees_total = 0.0
    comm_total = 0.0
    if isinstance(txn.get("fees"), dict):
        for k, v in (txn["fees"] or {}).items():
            val = safe_float(v) or 0.0
            if "comm" in k.lower():
                comm_total += val
            else:
                fees_total += val
    elif isinstance(txn.get("fees"), list):
        for f in txn["fees"]:
            val = safe_float(f.get("amount")) or 0.0
            name = str(f.get("feeType") or f.get("type") or "")
            if "comm" in name.lower():
                comm_total += val
            else:
                fees_total += val

    items = txn.get("transactionItems") or txn.get("transactionItem") or []
    if isinstance(items, dict):
        items = [items]

    if not items:
        net_amount = safe_float(txn.get("netAmount"))
        row = [ts, txn_id, ttype, subtype, desc,
               "", "", "", "", "",
               "", "", "", (net_amount if net_amount is not None else ""), round(comm_total, 2) or "", round(fees_total, 2) or "",
               "schwab_txn"]
        out.append(row)
        return out

    for it in items:
        qty = safe_float(it.get("amount")) or safe_float(it.get("quantity"))
        price = safe_float(it.get("price"))
        amount = safe_float(it.get("cost")) or safe_float(it.get("amount"))
        symbol = str((it.get("instrument") or {}).get("symbol") or it.get("symbol") or "")
        pc = (it.get("instruction") or it.get("putCall") or "")
        pc = "CALL" if str(pc).upper().startswith("C") else ("PUT" if str(pc).upper().startswith("P") else "")
        strike = safe_float((it.get("instrument") or {}).get("strikePrice") or it.get("strike"))
        exp = (it.get("instrument") or {}).get("optionExpirationDate")
        exp_primary = None
        if exp:
            try:
                exp_primary = str(date.fromisoformat(str(exp)[:10]))
            except Exception:
                exp_primary = None
        if not exp_primary:
            exp_primary = parse_exp_from_symbol(symbol)
        underlying = to_underlying(symbol)
        net_amount = safe_float(txn.get("netAmount"))

        row = [ts, txn_id, ttype, subtype, desc,
               symbol, underlying or "", exp_primary or "", (strike if strike is not None else ""), pc,
               (qty if qty is not None else ""), (price if price is not None else ""), (amount if amount is not None else ""),
               (net_amount if net_amount is not None else ""), round(comm_total, 2) or "", round(fees_total, 2) or "",
               "schwab_txn"]
        out.append(row)

    return out


# ---------- main ----------
def main() -> int:
    # Sheets
    try:
        svc, sid = sheets_client()
        ensure_tab_with_header(svc, sid, RAW_TAB, RAW_HEADERS)
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
        rows.extend(explode_txn(t))

    overwrite_rows(svc, sid, RAW_TAB, RAW_HEADERS, rows)
    print(f"OK: wrote {len(rows)} rows to {RAW_TAB}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
