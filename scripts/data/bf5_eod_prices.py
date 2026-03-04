#!/usr/bin/env python3
"""BF5 EOD price updater — daily mark-to-market for open butterfly positions.

Runs independently of the trade orchestrator (e.g., daily at 4:05 PM ET).
For every OPEN row in the BF_Q9_5DTE sheet:
  1. Backfills entry_mid if missing (from current NBBO).
  2. Writes the butterfly mid into the correct DTE column (d4, d3, d2, d1).
  3. Settles expired positions (DTE <= 0) with intrinsic P&L.
"""

import os
import sys
import time
import random
from datetime import date, timedelta

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
_SCHWAB_ERR = None
try:
    _add_scripts_root()
    from lib.sheets import sheets_client, ensure_tab, read_existing, col_letter
except Exception as e:
    sheets_client = None
    _IMPORT_ERR = e

try:
    from schwab_token_keeper import schwab_client
except Exception as e:
    schwab_client = None
    _SCHWAB_ERR = e

TAG = "BF5_EOD"

# ---------- sheet layout (must match bf_tracker_to_gsheet.py) ----------

HEADERS = [
    "trade_date",     # 0
    "expiration",     # 1
    "direction",      # 2
    "bucket",         # 3
    "vix1d",          # 4
    "vix",            # 5
    "spot",           # 6
    "atm_strike",     # 7
    "width",          # 8
    "entry_mid",      # 9
    "d4_mid",         # 10
    "d3_mid",         # 11
    "d2_mid",         # 12
    "d1_mid",         # 13
    "status",         # 14
    "spot_settle",    # 15
    "settle_value",   # 16
    "pnl",            # 17
]

C_TRADE_DATE = 0
C_EXPIRATION = 1
C_DIRECTION = 2
C_ATM_STRIKE = 7
C_WIDTH = 8
C_ENTRY_MID = 9
C_D4 = 10
C_D3 = 11
C_D2 = 12
C_D1 = 13
C_STATUS = 14
C_SPOT_SETTLE = 15
C_SETTLE_VALUE = 16
C_PNL = 17

DTE_COL = {4: C_D4, 3: C_D3, 2: C_D2, 1: C_D1}


# ---------- helpers ----------

def log(msg):
    print(f"{TAG}: {msg}")


def _fnum(x):
    try:
        return float(x)
    except Exception:
        return None


def _parse_width_pair(raw):
    s = str(raw or "").strip()
    if not s:
        return (None, None)
    if "/" in s:
        left, right = s.split("/", 1)
        return (_fnum(left), _fnum(right))
    w = _fnum(s)
    return (w, w)


def _business_days_between(d1, d2):
    count = 0
    d = d1
    while d < d2:
        d += timedelta(days=1)
        if d.weekday() < 5:
            count += 1
    return count


def build_osi(exp_str, strike):
    exp_date = date.fromisoformat(exp_str)
    exp6 = f"{exp_date:%y%m%d}"
    mills = int(round(float(strike))) * 1000
    return f"{'SPXW':<6}{exp6}C{mills:08d}"


# ---------- Schwab helpers ----------

def _sleep_for_429(resp, attempt):
    ra = resp.headers.get("Retry-After")
    if ra:
        try:
            return max(1.0, float(ra))
        except Exception:
            pass
    return min(6.0, 0.5 * (2 ** attempt)) + random.uniform(0.0, 0.25)


def fetch_quotes_batch(c, symbols):
    for attempt in range(4):
        try:
            r = c.session.get(
                "https://api.schwabapi.com/marketdata/v1/quotes",
                params={"symbols": ",".join(symbols), "fields": "quote"},
                timeout=20,
            )
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                time.sleep(_sleep_for_429(r, attempt))
                continue
        except Exception:
            pass
        time.sleep(min(3.0, 0.4 * (2 ** attempt)))
    return {}


def _leg_mid(quotes, osi):
    """Extract mid price for a single option leg from quotes response.

    Tries bid/ask first (live market), then mark, then closePrice (after-hours).
    """
    data = quotes.get(osi, {})
    q = data.get("quote", data) if isinstance(data, dict) else {}

    bid = _fnum(q.get("bidPrice") or q.get("bid"))
    ask = _fnum(q.get("askPrice") or q.get("ask"))
    if bid is not None and ask is not None and bid > 0 and ask > 0:
        return (bid + ask) / 2.0

    # After-hours fallback: mark or closePrice
    mark = _fnum(q.get("mark"))
    if mark is not None and mark > 0:
        return mark

    close = _fnum(q.get("closePrice") or q.get("lastPrice"))
    if close is not None and close > 0:
        return close

    return None


def butterfly_mid_from_quotes(quotes, lower_osi, center_osi, upper_osi):
    lo = _leg_mid(quotes, lower_osi)
    ce = _leg_mid(quotes, center_osi)
    hi = _leg_mid(quotes, upper_osi)
    if lo is None or ce is None or hi is None:
        return None
    val = lo - 2 * ce + hi
    return val if val > 0 else None


def fetch_spx_price(c):
    try:
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


def settle_butterfly_call(lower, center, upper, spot):
    lo = max(0.0, spot - lower)
    ce = max(0.0, spot - center)
    hi = max(0.0, spot - upper)
    return lo - 2 * ce + hi


# ---------- main ----------

def main() -> int:
    if sheets_client is None:
        log(f"SKIP: sheets libs not available ({_IMPORT_ERR})")
        return 0

    if schwab_client is None:
        log(f"SKIP: schwab libs not available ({_SCHWAB_ERR})")
        return 0

    gsheet_id = (os.environ.get("BF_GSHEET_ID") or os.environ.get("GSHEET_ID") or "").strip()
    if not gsheet_id:
        log("SKIP: BF_GSHEET_ID missing")
        return 0

    if not (os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip():
        log("SKIP: SA creds missing")
        return 0

    tab = (os.environ.get("BF_TRACKER_TAB") or "BF_Q9_5DTE").strip()

    try:
        svc, _ = sheets_client()
    except Exception as e:
        log(f"SKIP: sheets_client failed: {e}")
        return 0

    try:
        schwab_c = schwab_client()
    except Exception as e:
        log(f"SKIP: Schwab client failed: {e}")
        return 0

    today = date.today()
    log(f"date={today} tab={tab}")

    try:
        ensure_tab(svc, gsheet_id, tab, HEADERS)
        existing = read_existing(svc, gsheet_id, tab, HEADERS)
    except Exception as e:
        log(f"ERROR reading sheet: {e}")
        return 1

    # Find all OPEN rows
    open_trades = []
    osi_symbols = set()

    for i, row in enumerate(existing):
        if row[C_STATUS] != "OPEN":
            continue

        exp_str = row[C_EXPIRATION]
        atm = _fnum(row[C_ATM_STRIKE])
        width_dn, width_up = _parse_width_pair(row[C_WIDTH])
        if not exp_str or atm is None or width_dn is None or width_up is None:
            continue

        try:
            exp_date = date.fromisoformat(exp_str)
        except ValueError:
            continue

        dte = _business_days_between(today, exp_date)

        lower_osi = build_osi(exp_str, atm - width_dn)
        center_osi = build_osi(exp_str, atm)
        upper_osi = build_osi(exp_str, atm + width_up)

        open_trades.append({
            "row_idx": i,
            "dte": dte,
            "exp_str": exp_str,
            "exp_date": exp_date,
            "atm": atm,
            "width_dn": width_dn,
            "width_up": width_up,
            "direction": row[C_DIRECTION],
            "entry_mid": _fnum(row[C_ENTRY_MID]),
            "has_entry_mid": (_fnum(row[C_ENTRY_MID]) or 0) > 0 if C_ENTRY_MID < len(row) else False,
            "lower_osi": lower_osi,
            "center_osi": center_osi,
            "upper_osi": upper_osi,
        })

        if dte > 0:
            osi_symbols.update([lower_osi, center_osi, upper_osi])

    if not open_trades:
        log("No open trades to update")
        return 0

    log(f"Found {len(open_trades)} OPEN trade(s)")

    # Batch-fetch all option quotes
    quotes = {}
    if osi_symbols:
        sym_list = list(osi_symbols)
        for j in range(0, len(sym_list), 30):
            batch = sym_list[j:j + 30]
            batch_quotes = fetch_quotes_batch(schwab_c, batch)
            quotes.update(batch_quotes)
            if j + 30 < len(sym_list):
                time.sleep(0.5)
        log(f"Fetched quotes for {len(quotes)} option symbols")

    spx_price = None
    updates = []

    for trade in open_trades:
        sheet_row = trade["row_idx"] + 2  # 1-indexed, header = row 1
        dte = trade["dte"]

        log(f"  row={sheet_row} exp={trade['exp_str']} dte={dte} "
            f"entry_mid={'YES' if trade['has_entry_mid'] else 'MISSING'} "
            f"dir={trade['direction']} lower={trade['lower_osi']}")

        bf_mid = butterfly_mid_from_quotes(
            quotes,
            trade["lower_osi"],
            trade["center_osi"],
            trade["upper_osi"],
        ) if dte > 0 else None

        log(f"    bf_mid={bf_mid}")

        # --- Backfill entry_mid if missing ---
        if not trade["has_entry_mid"] and bf_mid is not None and bf_mid > 0:
            entry_col = col_letter(C_ENTRY_MID)
            updates.append({
                "range": f"{tab}!{entry_col}{sheet_row}",
                "values": [[f"{bf_mid:.2f}"]],
            })
            log(f"    BACKFILL entry_mid: {bf_mid:.2f}")

        # --- DTE column update ---
        if dte > 0 and dte in DTE_COL:
            col_idx = DTE_COL[dte]

            # Skip if already filled
            row_data = existing[trade["row_idx"]]
            current_val = (row_data[col_idx] if col_idx < len(row_data) else "").strip()
            if current_val:
                log(f"    d{dte}_mid ALREADY={current_val}")
            elif bf_mid is not None and bf_mid > 0:
                col_ltr = col_letter(col_idx)
                updates.append({
                    "range": f"{tab}!{col_ltr}{sheet_row}",
                    "values": [[f"{bf_mid:.2f}"]],
                })
                log(f"    d{dte}_mid: {bf_mid:.2f}")
            else:
                log(f"    d{dte}_mid: SKIP (no valid quote)")
        elif dte > 4:
            log(f"    dte={dte} > 4, no column to update yet")

        elif dte <= 0:
            # --- Settle expired ---
            if spx_price is None:
                spx_price = fetch_spx_price(schwab_c)
                if spx_price:
                    log(f"SPX for settlement: {spx_price:.2f}")

            if spx_price is None:
                log("Cannot settle: SPX unavailable")
                continue

            lower = trade["atm"] - trade["width_dn"]
            upper = trade["atm"] + trade["width_up"]
            settle_val = settle_butterfly_call(lower, trade["atm"], upper, spx_price)

            pnl = None
            if trade["entry_mid"] is not None:
                if trade["direction"] == "SELL":
                    pnl = (trade["entry_mid"] - settle_val) * 100
                else:
                    pnl = (settle_val - trade["entry_mid"]) * 100

            status_col = col_letter(C_STATUS)
            pnl_col = col_letter(C_PNL)
            updates.append({
                "range": f"{tab}!{status_col}{sheet_row}:{pnl_col}{sheet_row}",
                "values": [[
                    "SETTLED",
                    f"{spx_price:.2f}",
                    f"{settle_val:.2f}",
                    f"{pnl:.0f}" if pnl is not None else "",
                ]],
            })
            pnl_str = f" pnl=${pnl:.0f}" if pnl is not None else ""
            log(f"  SETTLED {trade['exp_str']}: settle={settle_val:.2f}{pnl_str}")

    if updates:
        svc.spreadsheets().values().batchUpdate(
            spreadsheetId=gsheet_id,
            body={"valueInputOption": "RAW", "data": updates},
        ).execute()
        log(f"Updated {len(updates)} cell(s)")
    else:
        log("No updates needed")

    return 0


if __name__ == "__main__":
    sys.exit(main())
