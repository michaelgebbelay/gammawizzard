#!/usr/bin/env python3
"""Butterfly trade tracker — logs entries and settles expired trades in Google Sheets.

Runs as a post_step for butterfly Lambda accounts.
Handles two tabs:
  - BF_Q17_2DTE : 2DTE Q17 butterfly trades
  - BF_Q9_5DTE  : 5DTE Q9 butterfly trades

On each run:
  1. Reads the current invocation's CSV trade log (if any new trades).
  2. Appends new trade rows to the appropriate Google Sheet tab.
  3. Checks both tabs for OPEN trades whose expiration has passed.
  4. Fetches SPX close from Schwab and computes settlement P&L.
"""

import csv
import os
import sys
from datetime import date

# ---------- path setup ----------

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
    _add_scripts_root()
    from lib.sheets import sheets_client, ensure_tab, read_existing, col_letter
except Exception as e:
    sheets_client = None
    _IMPORT_ERR = e

TAG = "BF_TRACKER"

# ---------- config ----------

# Tab configs: each entry maps a CSV log to a Google Sheet tab
TAB_CONFIGS = [
    {
        "tab": "BF_Q17_2DTE",
        "csv_env": "BF_Q17_LOG_PATH",
        "csv_default": "/tmp/bf_trades.csv",
    },
    {
        "tab": "BF_Q9_5DTE",
        "csv_env": "BF_Q9_LOG_PATH",
        "csv_default": "/tmp/bf5_trades.csv",
    },
]

HEADERS = [
    "trade_date",       # 0
    "expiration",       # 1
    "direction",        # 2
    "bucket",           # 3
    "vix1d",            # 4
    "vix",              # 5
    "spot",             # 6
    "atm_strike",       # 7
    "width",            # 8
    "em",               # 9
    "em_mult",          # 10
    "qty_req",          # 11
    "qty_filled",       # 12
    "fill_price",       # 13
    "nbbo_mid",         # 14
    "status",           # 15
    "spot_settle",      # 16
    "settle_value",     # 17
    "pnl",              # 18
]

IDX_TRADE_DATE = 0
IDX_EXPIRATION = 1
IDX_DIRECTION = 2
IDX_ATM_STRIKE = 7
IDX_WIDTH = 8
IDX_FILL_PRICE = 13
IDX_STATUS = 15
IDX_SPOT_SETTLE = 16
IDX_SETTLE_VALUE = 17
IDX_PNL = 18
IDX_QTY_FILLED = 12


# ---------- helpers ----------

def strict_enabled() -> bool:
    return (os.environ.get("CS_GSHEET_STRICT", "0") or "0").strip().lower() in ("1", "true", "yes", "y")


def log(msg):
    print(f"{TAG}: {msg}")


def skip(msg):
    log(f"SKIP -- {msg}")
    return 0


def fail(msg, code=2):
    print(f"{TAG}: ERROR -- {msg}", file=sys.stderr)
    return code


def _fnum(x):
    try:
        return float(x)
    except Exception:
        return None


def parse_csv_log(csv_path):
    """Parse the butterfly trade CSV log into sheet-ready rows."""
    if not os.path.exists(csv_path):
        return []

    rows = []
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            # Skip entries with no fill
            filled = r.get("qty_filled", "0")
            if filled in ("0", ""):
                continue

            # VIX1D: convert from decimal to percentage for display
            vix1d_raw = _fnum(r.get("vix1d", ""))
            vix1d_pct = f"{vix1d_raw * 100:.1f}" if vix1d_raw is not None else ""

            row = [""] * len(HEADERS)
            row[IDX_TRADE_DATE] = r.get("trade_date", "")
            row[IDX_EXPIRATION] = r.get("expiration", "")
            row[IDX_DIRECTION] = r.get("direction", "")
            row[3] = r.get("bucket", "")
            row[4] = vix1d_pct
            row[5] = r.get("vix", "")
            row[6] = r.get("spot", "")
            row[IDX_ATM_STRIKE] = r.get("atm_strike", "")
            row[IDX_WIDTH] = r.get("width", "")
            row[9] = r.get("em", "")
            row[10] = r.get("em_mult", "")
            row[11] = r.get("qty_requested", "")
            row[IDX_QTY_FILLED] = filled
            row[IDX_FILL_PRICE] = r.get("last_price", "")
            row[14] = r.get("nbbo_mid", "")
            row[IDX_STATUS] = "OPEN"
            rows.append(row)

    return rows


def settle_butterfly_call(lower, center, upper, spot):
    """Compute call butterfly settlement value per contract (in points)."""
    lo = max(0.0, spot - lower)
    ce = max(0.0, spot - center)
    hi = max(0.0, spot - upper)
    return lo - 2 * ce + hi


def fetch_spx_price():
    """Fetch current SPX price from Schwab quotes API."""
    try:
        from schwab_token_keeper import schwab_client
        c = schwab_client()
        r = c.session.get(
            "https://api.schwabapi.com/marketdata/v1/quotes",
            params={"symbols": "$SPX", "fields": "quote"},
            timeout=20,
        )
        if r.status_code == 200:
            j = r.json()
            q = (j.get("$SPX") or {}).get("quote") or {}
            price = _fnum(q.get("lastPrice")) or _fnum(q.get("closePrice"))
            if price and price > 0:
                return price
    except Exception as e:
        log(f"SPX fetch failed: {e}")
    return None


# ---------- main ----------

def main() -> int:
    strict = strict_enabled()

    if sheets_client is None:
        msg = f"google sheets libs not installed ({_IMPORT_ERR})"
        return fail(msg, 2) if strict else skip(msg)

    # Use BF_GSHEET_ID if set, otherwise fall back to GSHEET_ID
    gsheet_id = (os.environ.get("BF_GSHEET_ID") or os.environ.get("GSHEET_ID") or "").strip()
    if not gsheet_id:
        return fail("BF_GSHEET_ID/GSHEET_ID missing", 2) if strict else skip("BF_GSHEET_ID missing")

    if not (os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip():
        return fail("SA creds missing", 2) if strict else skip("SA creds missing")

    try:
        svc, _ = sheets_client()
        sid = gsheet_id  # override with butterfly sheet ID
    except Exception as e:
        msg = f"sheets_client failed: {type(e).__name__}: {e}"
        return fail(msg, 2) if strict else skip(msg)

    today_str = date.today().isoformat()
    spx_price = None  # lazy-fetched once needed

    for cfg in TAB_CONFIGS:
        tab = cfg["tab"]
        csv_path = os.environ.get(cfg["csv_env"], cfg["csv_default"])

        try:
            # Ensure tab exists with headers
            ensure_tab(svc, sid, tab, HEADERS)

            # Read existing rows
            existing = read_existing(svc, sid, tab, HEADERS)
            existing_dates = {row[IDX_TRADE_DATE] for row in existing if row[IDX_TRADE_DATE]}

            # --- 1. Add new trades from CSV log ---
            new_rows = parse_csv_log(csv_path)
            appends = []
            for row in new_rows:
                if row[IDX_TRADE_DATE] and row[IDX_TRADE_DATE] not in existing_dates:
                    appends.append(row)
                    existing.append(row)
                    existing_dates.add(row[IDX_TRADE_DATE])

            if appends:
                svc.spreadsheets().values().append(
                    spreadsheetId=sid,
                    range=f"{tab}!A1",
                    valueInputOption="RAW",
                    insertDataOption="INSERT_ROWS",
                    body={"values": appends},
                ).execute()
                log(f"{tab}: appended {len(appends)} new trade(s)")

            # --- 2. Settle expired trades ---
            updates = []
            for i, row in enumerate(existing):
                if row[IDX_STATUS] != "OPEN":
                    continue

                exp_date = row[IDX_EXPIRATION]
                if not exp_date or exp_date > today_str:
                    continue

                # Trade has expired — compute settlement
                if spx_price is None:
                    spx_price = fetch_spx_price()
                    if spx_price:
                        log(f"SPX price for settlement: {spx_price:.2f}")

                if spx_price is None:
                    log("Cannot settle: SPX price unavailable")
                    break

                atm = _fnum(row[IDX_ATM_STRIKE])
                width = _fnum(row[IDX_WIDTH])
                fill = _fnum(row[IDX_FILL_PRICE])
                direction = row[IDX_DIRECTION]
                qty = _fnum(row[IDX_QTY_FILLED]) or 1.0

                if atm is None or width is None:
                    continue

                lower = atm - width
                upper = atm + width
                settle_val = settle_butterfly_call(lower, atm, upper, spx_price)

                if fill is not None:
                    if direction == "SELL":
                        pnl = (fill - settle_val) * 100 * qty
                    else:
                        pnl = (settle_val - fill) * 100 * qty
                else:
                    pnl = None

                # Build update: columns P (status) through S (pnl) = indices 15-18
                # Sheet row = i + 2 (1-indexed, header is row 1)
                sheet_row = i + 2
                status_col = col_letter(IDX_STATUS)
                pnl_col = col_letter(IDX_PNL)
                range_str = f"{tab}!{status_col}{sheet_row}:{pnl_col}{sheet_row}"

                values = [
                    "SETTLED",
                    f"{spx_price:.2f}",
                    f"{settle_val:.2f}",
                    f"{pnl:.0f}" if pnl is not None else "",
                ]
                updates.append({"range": range_str, "values": [values]})

            if updates:
                svc.spreadsheets().values().batchUpdate(
                    spreadsheetId=sid,
                    body={"valueInputOption": "RAW", "data": updates},
                ).execute()
                log(f"{tab}: settled {len(updates)} trade(s)")

        except Exception as e:
            log(f"{tab}: error: {type(e).__name__}: {e}")
            if strict:
                return 2

    return 0


if __name__ == "__main__":
    sys.exit(main())
