#!/usr/bin/env python3
"""EOD butterfly price tracking — fills eod_3dte/2dte/1dte columns in BF_Trades.

Runs as a post-step after the butterfly orchestrator. For each OK row whose
expiry is still in the future, calculates trading DTE and fetches the current
butterfly package mid from Schwab to fill the matching column.

Env:
  GSHEET_ID                    - spreadsheet ID
  GOOGLE_SERVICE_ACCOUNT_JSON  - service account JSON
  BF_GSHEET_TAB                - tab name (default "BF_Trades")
"""

import os
import sys
from datetime import date, datetime
from zoneinfo import ZoneInfo


def _add_repo_root():
    cur = os.path.abspath(os.path.dirname(__file__))
    while True:
        parent = os.path.dirname(cur)
        if os.path.basename(cur) == "Gamma" or os.path.isdir(os.path.join(cur, "scripts")):
            if cur not in sys.path:
                sys.path.insert(0, cur)
            return
        if parent == cur:
            return
        cur = parent


def _add_scripts_root():
    cur = os.path.abspath(os.path.dirname(__file__))
    while True:
        if os.path.basename(cur) == "scripts":
            if cur not in sys.path:
                sys.path.append(cur)
            return
        parent = os.path.dirname(cur)
        if parent == cur:
            return
        cur = parent


_IMPORT_ERR = None
try:
    _add_repo_root()
    _add_scripts_root()
    from lib.sheets import sheets_client, col_letter
    from scripts.schwab_token_keeper import schwab_client
except Exception as e:
    _IMPORT_ERR = e

ET = ZoneInfo("America/New_York")

# Column indices matching bf_trades_to_gsheet.py HEADERS
COL_STATUS = 2       # C
COL_EXPIRY = 8       # I
COL_LOWER = 12       # M
COL_CENTER = 13      # N
COL_UPPER = 14       # O
COL_EOD_3DTE = 25    # Z
COL_EOD_2DTE = 26    # AA
COL_EOD_1DTE = 27    # AB

DTE_COL_MAP = {3: COL_EOD_3DTE, 2: COL_EOD_2DTE, 1: COL_EOD_1DTE}


def trading_days_between(start: date, end: date) -> int:
    """Count business days from start (exclusive) to end (inclusive)."""
    count = 0
    d = start
    while d < end:
        d = d.fromordinal(d.toordinal() + 1)
        if d.weekday() < 5:
            count += 1
    return count


def make_osi(expiry: date, strike: float) -> str:
    """Build Schwab OSI symbol for an SPXW call."""
    ymd = expiry.strftime("%y%m%d")
    mills = int(round(strike * 1000))
    return f"{'SPXW':<6}{ymd}C{mills:08d}"


def fetch_butterfly_mid(c, expiry: date, lower: float, center: float, upper: float):
    """Fetch live butterfly package mid from Schwab quotes."""
    lower_osi = make_osi(expiry, lower)
    center_osi = make_osi(expiry, center)
    upper_osi = make_osi(expiry, upper)

    bids_asks = []
    for osi in (lower_osi, center_osi, upper_osi):
        try:
            resp = c.get_quote(osi)
            if resp.status_code != 200:
                print(f"  BF_EOD quote fail {osi}: HTTP {resp.status_code}")
                return None
            data = list(resp.json().values())[0] if isinstance(resp.json(), dict) else {}
            q = data.get("quote", data)
            bid = q.get("bidPrice") or q.get("bid") or q.get("bidPriceInDouble")
            ask = q.get("askPrice") or q.get("ask") or q.get("askPriceInDouble")
            if bid is None or ask is None:
                print(f"  BF_EOD no bid/ask for {osi}")
                return None
            bids_asks.append((float(bid), float(ask)))
        except Exception as e:
            print(f"  BF_EOD quote error {osi}: {e}")
            return None

    (lb, la), (cb, ca), (ub, ua) = bids_asks
    pkg_bid = max(0.0, lb + ub - 2.0 * ca)
    pkg_ask = max(pkg_bid, la + ua - 2.0 * cb)
    mid = round((pkg_bid + pkg_ask) / 2.0, 2)
    return mid


def main():
    if _IMPORT_ERR:
        print(f"BF_EOD FAIL: import error: {_IMPORT_ERR}")
        return 1

    tab = os.environ.get("BF_GSHEET_TAB", "BF_Trades")
    today = date.today()

    # Override for testing
    override = os.environ.get("BF_TRADE_DATE_OVERRIDE", "").strip()
    if override:
        today = date.fromisoformat(override)

    if today.weekday() >= 5:
        print(f"BF_EOD SKIP: weekend ({today})")
        return 2

    try:
        svc, sid = sheets_client()
    except Exception as e:
        print(f"BF_EOD FAIL: sheets_client failed: {e}")
        return 1

    # Read all data rows
    vals = (
        svc.spreadsheets()
        .values()
        .get(spreadsheetId=sid, range=f"{tab}!A2:AB")
        .execute()
        .get("values", [])
    )

    if not vals:
        print("BF_EOD SKIP: no rows")
        return 0

    # Find rows that need EOD tracking
    updates = []
    c = None  # lazy init Schwab client

    for row_idx, row in enumerate(vals):
        # Pad row to full width
        row += [""] * (COL_EOD_1DTE + 1 - len(row))

        status = row[COL_STATUS] if len(row) > COL_STATUS else ""
        expiry_str = row[COL_EXPIRY] if len(row) > COL_EXPIRY else ""

        if status != "OK" or not expiry_str:
            continue

        try:
            expiry = date.fromisoformat(expiry_str)
        except ValueError:
            continue

        if expiry <= today:
            continue

        # Skip rows entered today — entry price is already in package_mid
        trade_date_str = row[1] if len(row) > 1 else ""
        if trade_date_str == today.isoformat():
            continue

        trading_dte = trading_days_between(today, expiry)
        if trading_dte not in DTE_COL_MAP:
            continue

        col_idx = DTE_COL_MAP[trading_dte]

        # Skip if already filled
        if row[col_idx].strip():
            continue

        try:
            lower = float(row[COL_LOWER])
            center = float(row[COL_CENTER])
            upper = float(row[COL_UPPER])
        except (ValueError, IndexError):
            continue

        # Lazy init Schwab client
        if c is None:
            try:
                c = schwab_client()
            except Exception as e:
                print(f"BF_EOD SKIP: schwab_client failed: {e}")
                return 0

        mid = fetch_butterfly_mid(c, expiry, lower, center, upper)
        if mid is None:
            print(f"BF_EOD WARN: could not fetch mid for row {row_idx + 2} expiry={expiry_str}")
            continue

        # Sheet row is row_idx + 2 (1-indexed, header is row 1)
        sheet_row = row_idx + 2
        col = col_letter(col_idx)
        cell = f"{tab}!{col}{sheet_row}"
        updates.append((cell, str(mid), trading_dte, expiry_str))

    if not updates:
        print(f"BF_EOD SKIP: no rows to update today={today}")
        return 2

    for cell, val, dte, exp in updates:
        svc.spreadsheets().values().update(
            spreadsheetId=sid,
            range=cell,
            valueInputOption="RAW",
            body={"values": [[val]]},
        ).execute()
        print(f"BF_EOD OK: {cell}={val} (eod_{dte}dte, expiry={exp})")

    return 0


if __name__ == "__main__":
    sys.exit(main())
