#!/usr/bin/env python3
"""
Aggregate ConstantStable trades CSV into a daily tracking summary on Google Sheets.

Reads the same trade CSV at CS_LOG_PATH, pivots PUT + CALL rows for the same
(trade_date, tdate) into one combined row per account per day, and upserts to
a "CS_Tracking" tab.

NON-BLOCKING BY DEFAULT (same pattern as cs_trades_to_gsheet.py).

Env:
  GSHEET_ID                    - spreadsheet ID
  GOOGLE_SERVICE_ACCOUNT_JSON  - full JSON string for service account
  CS_LOG_PATH                  - path to trade CSV (default /tmp/cs_trades.csv)
  CS_TRACKING_TAB              - sheet tab name (default "CS_Tracking")
  CS_ACCOUNT_LABEL             - account identifier (schwab, tt-ira, tt-individual)
  CS_COST_PER_CONTRACT         - cost per contract in dollars (e.g. 0.65)
  CS_GSHEET_STRICT             - "1" to fail hard on errors
"""

import os
import sys
import json
import csv
from collections import defaultdict

# --- optional imports (skip if missing) ---
_IMPORT_ERR = None
try:
    from googleapiclient.discovery import build
    from google.oauth2 import service_account
except Exception as e:
    build = None
    service_account = None
    _IMPORT_ERR = e

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

TRACKING_HEADER = [
    "date", "expiry", "account",
    "put_go", "call_go", "put_strikes", "call_strikes",
    "gw_put_price", "gw_call_price",
    "put_spread_price", "call_spread_price",
    "vix_value", "vol_bucket", "vix_mult", "units",
    "put_side", "put_target", "call_side", "call_target",
    "put_filled", "put_fill_price", "put_status",
    "call_filled", "call_fill_price", "call_status",
    "put_improvement", "call_improvement",
    "cost_per_contract", "put_cost", "call_cost", "total_cost",
]

UPSERT_KEYS = ["date", "expiry", "account"]

TAG = "CS_TRACKING"


# ---------------------------------------------------------------------------
# Helpers (same patterns as cs_trades_to_gsheet.py)
# ---------------------------------------------------------------------------

def strict_enabled() -> bool:
    return (os.environ.get("CS_GSHEET_STRICT", "0") or "0").strip().lower() in ("1", "true", "yes", "y")


def log(msg: str):
    print(f"{TAG}: {msg}")


def skip(msg: str) -> int:
    log(f"SKIP — {msg}")
    return 0


def fail(msg: str, code: int = 2) -> int:
    print(f"{TAG}: ERROR — {msg}", file=sys.stderr)
    return code


def creds_from_env():
    raw = (os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip()
    if not raw:
        return None
    info = json.loads(raw)
    return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)


def col_letter(idx: int) -> str:
    n = idx + 1
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def ensure_sheet_tab(svc, sid: str, title: str) -> int:
    meta = svc.spreadsheets().get(spreadsheetId=sid, fields="sheets.properties").execute()
    for s in (meta.get("sheets") or []):
        p = s.get("properties") or {}
        if (p.get("title") or "") == title:
            return int(p.get("sheetId"))
    req = {"requests": [{"addSheet": {"properties": {"title": title}}}]}
    r = svc.spreadsheets().batchUpdate(spreadsheetId=sid, body=req).execute()
    return int(r["replies"][0]["addSheet"]["properties"]["sheetId"])


def read_sheet_all(svc, sid: str, title: str):
    r = svc.spreadsheets().values().get(spreadsheetId=sid, range=f"{title}!A1:ZZ").execute()
    return r.get("values") or []


def upsert_rows(svc, sid: str, title: str, rows, header):
    existing = read_sheet_all(svc, sid, title)

    last_col = col_letter(len(header) - 1)
    if not existing:
        svc.spreadsheets().values().update(
            spreadsheetId=sid,
            range=f"{title}!A1:{last_col}1",
            valueInputOption="RAW",
            body={"values": [header]},
        ).execute()
        existing = [header]
    else:
        if existing[0] != header:
            svc.spreadsheets().values().update(
                spreadsheetId=sid,
                range=f"{title}!A1:{last_col}1",
                valueInputOption="RAW",
                body={"values": [header]},
            ).execute()
            existing = [header] + existing[1:]

    def key_from_dict(d):
        return tuple(str(d.get(k, "")) for k in UPSERT_KEYS)

    existing_map = {}
    for rnum, row in enumerate(existing[1:], start=2):
        d = {header[i]: (row[i] if i < len(row) else "") for i in range(len(header))}
        existing_map[key_from_dict(d)] = rnum

    # De-dupe by key (keep last)
    last_by_key = {}
    for d in rows:
        last_by_key[key_from_dict(d)] = d
    rows = list(last_by_key.values())

    updates = []
    appends = []
    for d in rows:
        key = key_from_dict(d)
        values = [str(d.get(h, "")) for h in header]
        if key in existing_map:
            rnum = existing_map[key]
            rng = f"{title}!A{rnum}:{last_col}{rnum}"
            updates.append((rng, values))
        else:
            appends.append(values)

    if updates:
        data = [{"range": rng, "values": [vals]} for (rng, vals) in updates]
        svc.spreadsheets().values().batchUpdate(
            spreadsheetId=sid,
            body={"valueInputOption": "RAW", "data": data},
        ).execute()

    if appends:
        svc.spreadsheets().values().append(
            spreadsheetId=sid,
            range=f"{title}!A1",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": appends},
        ).execute()

    return {"updated": len(updates), "appended": len(appends), "dedup_rows": len(rows)}


# ---------------------------------------------------------------------------
# Tracking-specific logic
# ---------------------------------------------------------------------------

def extract_strike(osi: str) -> str:
    """Extract strike from OSI symbol: 'SPXW  260211P06900000' -> '6900'."""
    s = (osi or "").strip()
    if len(s) < 8:
        return ""
    digits = s[-8:]
    if not digits.isdigit():
        return ""
    return str(int(digits) // 1000)


def spread_price(row: dict) -> str:
    """Get spread mid-price from nbbo_mid, falling back to first ladder price."""
    mid = (row.get("nbbo_mid") or "").strip()
    if mid:
        return mid
    ladder = (row.get("ladder_prices") or "").strip()
    if ladder:
        parts = [p.strip() for p in ladder.strip("[]").split(",") if p.strip()]
        if parts:
            return parts[0]
    return ""


def aggregate_rows(csv_rows, account_label: str, cost_per_contract: float):
    """Pivot PUT + CALL CSV rows into combined tracking rows."""
    groups = defaultdict(lambda: {"put": None, "call": None})

    for row in csv_rows:
        kind = (row.get("kind") or "").strip().upper()
        trade_date = (row.get("trade_date") or "").strip()
        tdate = (row.get("tdate") or "").strip()
        if kind not in ("PUT", "CALL"):
            continue
        key = (trade_date, tdate)
        # Keep last occurrence per kind (handles bundle fallback)
        groups[key][kind.lower()] = row

    result = []
    for (trade_date, tdate), sides in sorted(groups.items()):
        pr = sides["put"]
        cr = sides["call"]

        def val(row, field):
            return (row.get(field) or "") if row else ""

        # Strikes: show as "low/high"
        def strikes_str(row):
            if not row:
                return ""
            s1 = extract_strike(row.get("short_osi", ""))
            s2 = extract_strike(row.get("long_osi", ""))
            if s1 and s2:
                lo, hi = sorted([int(s1), int(s2)])
                return f"{lo}/{hi}"
            return ""

        # Fill quality: improvement vs NBBO mid (positive = favorable)
        # CREDIT: got more credit than mid -> fill_price - mid
        # DEBIT:  paid less than mid -> mid - fill_price
        def calc_improvement(row, side_str: str) -> str:
            if not row:
                return ""
            fill_s = (row.get("last_price") or "").strip()
            mid_s = (row.get("nbbo_mid") or "").strip()
            if not fill_s or not mid_s:
                return ""
            try:
                fill = float(fill_s)
                mid = float(mid_s)
                side = (side_str or "").upper()
                if side == "CREDIT":
                    return f"{fill - mid:.2f}"
                else:
                    return f"{mid - fill:.2f}"
            except (ValueError, TypeError):
                return ""

        # Cost: cost_per_contract * filled * 2 legs
        def calc_cost(filled_str: str) -> str:
            try:
                filled = int(filled_str)
                if filled > 0:
                    return f"{cost_per_contract * filled * 2:.2f}"
            except (ValueError, TypeError):
                pass
            return "0.00"

        put_filled = val(pr, "qty_filled")
        call_filled = val(cr, "qty_filled")
        put_cost = calc_cost(put_filled)
        call_cost = calc_cost(call_filled)
        try:
            total = f"{float(put_cost) + float(call_cost):.2f}"
        except (ValueError, TypeError):
            total = "0.00"

        ref = pr or cr
        result.append({
            "date": trade_date,
            "expiry": tdate,
            "account": account_label,
            "put_go": val(pr, "go"),
            "call_go": val(cr, "go"),
            "put_strikes": strikes_str(pr),
            "call_strikes": strikes_str(cr),
            "gw_put_price": val(pr, "gw_price"),
            "gw_call_price": val(cr, "gw_price"),
            "put_spread_price": spread_price(pr) if pr else "",
            "call_spread_price": spread_price(cr) if cr else "",
            "vix_value": val(ref, "vol_value"),
            "vol_bucket": val(ref, "vol_bucket"),
            "vix_mult": val(ref, "vol_mult"),
            "units": val(ref, "units"),
            "put_side": val(pr, "side"),
            "put_target": val(pr, "qty_requested"),
            "call_side": val(cr, "side"),
            "call_target": val(cr, "qty_requested"),
            "put_filled": put_filled,
            "put_fill_price": val(pr, "last_price"),
            "put_status": val(pr, "reason"),
            "call_filled": call_filled,
            "call_fill_price": val(cr, "last_price"),
            "call_status": val(cr, "reason"),
            "put_improvement": calc_improvement(pr, val(pr, "side")),
            "call_improvement": calc_improvement(cr, val(cr, "side")),
            "cost_per_contract": f"{cost_per_contract:.2f}",
            "put_cost": put_cost,
            "call_cost": call_cost,
            "total_cost": total,
        })

    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    strict = strict_enabled()

    if build is None or service_account is None:
        msg = f"google sheets libs not installed ({_IMPORT_ERR})"
        return fail(msg, 2) if strict else skip(msg)

    spreadsheet_id = (os.environ.get("GSHEET_ID") or "").strip()
    if not spreadsheet_id:
        return fail("GSHEET_ID missing", 2) if strict else skip("GSHEET_ID missing")

    raw_sa = (os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip()
    if not raw_sa:
        return fail("SA creds missing", 2) if strict else skip("SA creds missing")

    tab = (os.environ.get("CS_TRACKING_TAB") or "CS_Tracking").strip()
    path = (os.environ.get("CS_LOG_PATH") or "logs/constantstable_vertical_trades.csv").strip()
    account_label = (os.environ.get("CS_ACCOUNT_LABEL") or "unknown").strip()

    try:
        cost_per_contract = float(os.environ.get("CS_COST_PER_CONTRACT") or "0.00")
    except (ValueError, TypeError):
        cost_per_contract = 0.0

    if not os.path.exists(path):
        return skip(f"{path} missing")

    with open(path, "r", newline="") as f:
        csv_rows = list(csv.DictReader(f))

    if not csv_rows:
        return skip("no data rows in CSV")

    tracking_rows = aggregate_rows(csv_rows, account_label, cost_per_contract)
    if not tracking_rows:
        return skip("no tracking rows after aggregation")

    log(f"{len(tracking_rows)} row(s) for account={account_label}")

    try:
        creds = creds_from_env()
        if creds is None:
            return fail("SA creds empty", 2) if strict else skip("SA creds empty")
        svc = build("sheets", "v4", credentials=creds, cache_discovery=False)

        ensure_sheet_tab(svc, spreadsheet_id, tab)
        res = upsert_rows(svc, spreadsheet_id, tab, tracking_rows, TRACKING_HEADER)

        log(f"{path} -> {tab}  appended={res['appended']} updated={res['updated']}")
        return 0

    except Exception as e:
        msg = f"Sheets push failed: {type(e).__name__}: {e}"
        return fail(msg, 2) if strict else skip(msg)


if __name__ == "__main__":
    sys.exit(main())
