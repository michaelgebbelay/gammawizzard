#!/usr/bin/env python3
"""
Check TT Individual close order status and write to CS_TT_Close tab.

For TT Individual positions with profit-taking close orders, this script
checks which orders have filled and records the close prices. This data
is consumed by cs_summary_to_gsheet.py for accurate P&L calculation.

For positions that closed early: P&L uses the close price
For positions that expired at settlement: P&L uses settlement price (default)

Runs as a Lambda post-step for tt-individual only.

Env:
  GSHEET_ID                    - spreadsheet ID
  GOOGLE_SERVICE_ACCOUNT_JSON  - SA creds
  TT_ACCOUNT_NUMBER            - TT account number
  CS_TT_CLOSE_TAB              - tab name (default "CS_TT_Close")
  CS_GSHEET_STRICT             - "1" to fail hard
"""

import os
import re
import sys
import time
import random
from datetime import date, timedelta


# --- path setup ---
def _add_paths():
    """Add scripts/ and TT/Script/ to sys.path for shared lib + TT client."""
    cur = os.path.abspath(os.path.dirname(__file__))
    while True:
        if os.path.basename(cur) == "scripts":
            if cur not in sys.path:
                sys.path.append(cur)
            repo_root = os.path.dirname(cur)
            tt_script = os.path.join(repo_root, "TT", "Script")
            if os.path.isdir(tt_script) and tt_script not in sys.path:
                sys.path.append(tt_script)
            return
        parent = os.path.dirname(cur)
        if parent == cur:
            return
        cur = parent


_IMPORT_ERR = None
_TT_ERR = None
try:
    _add_paths()
    from lib.sheets import sheets_client, col_letter, ensure_sheet_tab, get_values
except Exception as e:
    sheets_client = None
    _IMPORT_ERR = e

try:
    from tt_client import request as tt_request
except Exception as e:
    tt_request = None
    _TT_ERR = e


TAG = "CS_TT_CLOSE"
CLOSE_TAB = "CS_TT_Close"
CLOSE_HEADERS = ["expiry", "status", "close_net"]


# ---------------------------------------------------------------------------
# Helpers
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


# ---------------------------------------------------------------------------
# TT API
# ---------------------------------------------------------------------------

def _tt_get_json(url, params=None, tries=4, tag=""):
    import requests as req_lib
    last = ""
    for i in range(tries):
        try:
            r = tt_request("GET", url, params=(params or {}))
            return r.json()
        except req_lib.HTTPError as e:
            resp = e.response
            if resp is not None and resp.status_code == 429:
                ra = resp.headers.get("Retry-After")
                wait = max(1.0, float(ra)) if ra else min(6.0, 0.6 * (2 ** i))
                time.sleep(wait + random.uniform(0, 0.25))
                continue
            last = f"HTTP_{resp.status_code}" if resp is not None else "HTTP_unknown"
        except Exception as e:
            last = f"{type(e).__name__}:{e}"
        time.sleep(min(4.0, 0.5 * (2 ** i)))
    raise RuntimeError(f"TT_GET_FAIL({tag}) {last}")


def _osi_expiry_iso(sym: str):
    """Extract expiry date from an option symbol.

    OSI: 'SPXW  260213P06900000' → '2026-02-13'
    TT:  'SPXW 260213P6900'     → '2026-02-13'
    """
    s = re.sub(r"\s+", "", (sym or "").strip().upper())
    m = re.search(r"(\d{6})[CP]\d+", s)
    if m:
        ymd = m.group(1)
        try:
            dt = date(2000 + int(ymd[:2]), int(ymd[2:4]), int(ymd[4:6]))
            return dt.isoformat()
        except ValueError:
            pass
    return None


def fetch_filled_close_orders(acct_num: str, days_back: int = 7) -> dict:
    """Fetch filled close orders from TT for the last N days.

    Returns: {expiry_iso: close_net}
      close_net: positive = credit received, negative = debit paid (SPX points)
    """
    start = (date.today() - timedelta(days=days_back)).isoformat()
    j = _tt_get_json(
        f"/accounts/{acct_num}/orders",
        params={"start-date": start, "per-page": "250"},
        tag="ORDERS",
    )
    data = j.get("data") if isinstance(j, dict) else {}
    items = data.get("items") or []

    result = {}
    for order in items:
        status = str(order.get("status") or "").lower()
        if status != "filled":
            continue

        legs = order.get("legs") or []
        is_close = any("Close" in str(leg.get("action") or "") for leg in legs)
        if not is_close:
            continue

        price_str = str(order.get("price") or "0")
        price_effect = str(order.get("price-effect") or "").strip().lower()
        try:
            price = float(price_str)
        except (ValueError, TypeError):
            continue

        # close_net: positive = credit received, negative = debit paid
        close_net = price if price_effect == "credit" else -price

        # Extract expiry from leg symbols
        expiry_iso = None
        for leg in legs:
            sym = (leg.get("symbol") or "").strip()
            if sym:
                exp = _osi_expiry_iso(sym)
                if exp:
                    expiry_iso = exp
                    break

        if expiry_iso and expiry_iso not in result:
            result[expiry_iso] = close_net
            log(f"closed {expiry_iso}: net={close_net:+.2f} ({price_effect})")

    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    strict = strict_enabled()

    if sheets_client is None:
        msg = f"google sheets libs not installed ({_IMPORT_ERR})"
        return fail(msg, 2) if strict else skip(msg)

    if tt_request is None:
        msg = f"TT client not available ({_TT_ERR})"
        return fail(msg, 2) if strict else skip(msg)

    if not (os.environ.get("GSHEET_ID") or "").strip():
        return fail("GSHEET_ID missing", 2) if strict else skip("GSHEET_ID missing")

    if not (os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip():
        return fail("SA creds missing", 2) if strict else skip("SA creds missing")

    acct_num = (os.environ.get("TT_ACCOUNT_NUMBER") or "").strip()
    if not acct_num:
        return skip("TT_ACCOUNT_NUMBER missing")

    close_tab = (os.environ.get("CS_TT_CLOSE_TAB") or CLOSE_TAB).strip()

    try:
        filled = fetch_filled_close_orders(acct_num)
        log(f"found {len(filled)} filled close order(s)")

        if not filled:
            return skip("no filled close orders")

        svc, sid = sheets_client()
        ensure_sheet_tab(svc, sid, close_tab)

        existing = get_values(svc, sid, f"{close_tab}!A1:C")
        last_col = col_letter(len(CLOSE_HEADERS) - 1)

        # Ensure header
        if not existing or existing[0] != CLOSE_HEADERS:
            svc.spreadsheets().values().update(
                spreadsheetId=sid,
                range=f"{close_tab}!A1:{last_col}1",
                valueInputOption="RAW",
                body={"values": [CLOSE_HEADERS]},
            ).execute()
            existing = [CLOSE_HEADERS] + (existing[1:] if existing else [])

        # Build existing map
        existing_expiries = {}
        for rnum, row in enumerate(existing[1:], start=2):
            if len(row) > 0 and row[0]:
                existing_expiries[row[0].strip()] = rnum

        # Upsert rows
        updates = []
        appends = []
        for expiry, close_net in filled.items():
            values = [expiry, "closed", str(round(close_net, 4))]
            if expiry in existing_expiries:
                rnum = existing_expiries[expiry]
                updates.append({
                    "range": f"{close_tab}!A{rnum}:{last_col}{rnum}",
                    "values": [values],
                })
            else:
                appends.append(values)

        if updates:
            svc.spreadsheets().values().batchUpdate(
                spreadsheetId=sid,
                body={"valueInputOption": "RAW", "data": updates},
            ).execute()

        if appends:
            svc.spreadsheets().values().append(
                spreadsheetId=sid,
                range=f"{close_tab}!A1",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": appends},
            ).execute()

        log(f"updated={len(updates)} appended={len(appends)} to {close_tab}")
        return 0

    except Exception as e:
        msg = f"Close status failed: {type(e).__name__}: {e}"
        return fail(msg, 2) if strict else skip(msg)


if __name__ == "__main__":
    sys.exit(main())
