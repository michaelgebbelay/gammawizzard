#!/usr/bin/env python3
"""Butterfly trade tracker — paper trade logging with daily price updates.

Runs as a post_step for butterfly Lambda accounts.
Each invocation handles ONE tab (set via BF_TRACKER_TAB env var).

On each run:
  1. Reads today's trade from the CSV log (if any, including DRY_RUN).
  2. Appends new trade row to the Google Sheet tab.
  3. For OPEN trades: fetches current butterfly NBBO and fills the
     appropriate intermediate column (d4, d3, d2, d1 based on DTE).
  4. For expired trades (DTE=0): computes settlement P&L.
"""

import csv
import os
import re
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

TAG = "BF_TRACKER"

# ---------- config ----------

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
    "risk_pts",       # 10
    "d4_mid",         # 11
    "d3_mid",         # 12
    "d2_mid",         # 13
    "d1_mid",         # 14
    "status",         # 15
    "spot_settle",    # 16
    "settle_value",   # 17
    "pnl",            # 18
]

# Column indices
C_TRADE_DATE = 0
C_EXPIRATION = 1
C_DIRECTION = 2
C_ATM_STRIKE = 7
C_WIDTH = 8
C_ENTRY_MID = 9
C_RISK_PTS = 10
C_D4 = 11
C_D3 = 12
C_D2 = 13
C_D1 = 14
C_STATUS = 15
C_SPOT_SETTLE = 16
C_SETTLE_VALUE = 17
C_PNL = 18

# Map DTE (business days to expiration) -> column index
DTE_COL = {4: C_D4, 3: C_D3, 2: C_D2, 1: C_D1}


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


def _parse_width_pair(raw):
    """
    Parse width field into (down_width, up_width).
    Accepts:
      - symmetric: "65", 65 -> (65, 65)
      - asymmetric: "175/150" -> (175, 150)
    """
    s = str(raw or "").strip()
    if not s:
        return (None, None)
    if "/" in s:
        left, right = s.split("/", 1)
        w_dn = _fnum(left)
        w_up = _fnum(right)
        return (w_dn, w_up)
    w = _fnum(s)
    return (w, w)


def _business_days_between(d1, d2):
    """Count business days from d1 to d2 (exclusive of d1, inclusive of d2)."""
    count = 0
    d = d1
    while d < d2:
        d += timedelta(days=1)
        if d.weekday() < 5:
            count += 1
    return count


def build_osi(exp_str, strike):
    """Build OSI symbol from expiration string and strike."""
    exp_date = date.fromisoformat(exp_str)
    exp6 = f"{exp_date:%y%m%d}"
    mills = int(round(float(strike))) * 1000
    return f"{'SPXW':<6}{exp6}C{mills:08d}"


# ---------- CSV log parsing ----------

def parse_csv_log(csv_path):
    """Parse butterfly trade CSV log into sheet-ready rows."""
    if not os.path.exists(csv_path):
        return []

    rows = []
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            # Accept both filled trades and DRY_RUN entries
            reason = (r.get("reason", "") or "").strip()
            is_dry_run = reason.upper().startswith("DRY_RUN")
            filled = r.get("qty_filled", "0")
            if not is_dry_run and filled in ("0", ""):
                continue

            # VIX1D: convert from decimal to percentage for display
            vix1d_raw = _fnum(r.get("vix1d", ""))
            vix1d_pct = f"{vix1d_raw * 100:.1f}" if vix1d_raw is not None else ""

            row = [""] * len(HEADERS)
            row[C_TRADE_DATE] = r.get("trade_date", "")
            row[C_EXPIRATION] = r.get("expiration", "")
            direction = r.get("direction", "")
            row[C_DIRECTION] = direction
            row[3] = r.get("bucket", "")
            row[4] = vix1d_pct
            row[5] = r.get("vix", "")
            row[6] = r.get("spot", "")
            row[C_ATM_STRIKE] = r.get("atm_strike", "")
            row[C_WIDTH] = r.get("width", "")
            # Entry mid = NBBO mid at time of entry
            entry_mid_str = r.get("nbbo_mid", "") or r.get("last_price", "")
            row[C_ENTRY_MID] = entry_mid_str
            # Risk in SPX points: SELL = width - entry_mid, BUY = entry_mid
            entry_mid_val = _fnum(entry_mid_str)
            width_dn, width_up = _parse_width_pair(row[C_WIDTH])
            if entry_mid_val and entry_mid_val > 0 and width_dn is not None:
                wing = min(width_dn, width_up) if width_up is not None else width_dn
                if direction == "SELL":
                    risk = wing - entry_mid_val
                else:
                    risk = entry_mid_val
                row[C_RISK_PTS] = f"{risk:.1f}" if risk > 0 else ""
            row[C_STATUS] = "SKIP" if direction == "SKIP" else "OPEN"
            rows.append(row)

    return rows


# ---------- Schwab option quote helpers ----------

def _sleep_for_429(resp, attempt):
    ra = resp.headers.get("Retry-After")
    if ra:
        try:
            return max(1.0, float(ra))
        except Exception:
            pass
    return min(6.0, 0.5 * (2 ** attempt)) + random.uniform(0.0, 0.25)


def fetch_quotes_batch(c, symbols):
    """Fetch quotes for multiple option symbols in one Schwab API call."""
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


def butterfly_mid_from_quotes(quotes, lower_osi, center_osi, upper_osi):
    """Compute butterfly spread mid price from batch quotes response."""
    def get_mid(osi):
        data = quotes.get(osi, {})
        q = data.get("quote", data) if isinstance(data, dict) else {}
        bid = _fnum(q.get("bidPrice") or q.get("bid"))
        ask = _fnum(q.get("askPrice") or q.get("ask"))
        if bid is not None and ask is not None and bid >= 0 and ask >= 0:
            return (bid + ask) / 2.0
        return None

    lo_mid = get_mid(lower_osi)
    ce_mid = get_mid(center_osi)
    hi_mid = get_mid(upper_osi)

    if lo_mid is None or ce_mid is None or hi_mid is None:
        return None

    # Long butterfly spread value: +1 lower, -2 center, +1 upper
    return lo_mid - 2 * ce_mid + hi_mid


def fetch_spx_price(c):
    """Fetch current SPX price from Schwab."""
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
    """Compute call butterfly settlement value per contract (in points)."""
    lo = max(0.0, spot - lower)
    ce = max(0.0, spot - center)
    hi = max(0.0, spot - upper)
    return lo - 2 * ce + hi


# ---------- main ----------

def main() -> int:
    strict = strict_enabled()

    if sheets_client is None:
        msg = f"google sheets libs not installed ({_IMPORT_ERR})"
        return fail(msg, 2) if strict else skip(msg)

    gsheet_id = (os.environ.get("BF_GSHEET_ID") or os.environ.get("GSHEET_ID") or "").strip()
    if not gsheet_id:
        return fail("BF_GSHEET_ID missing", 2) if strict else skip("BF_GSHEET_ID missing")

    if not (os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip():
        return fail("SA creds missing", 2) if strict else skip("SA creds missing")

    tab = (os.environ.get("BF_TRACKER_TAB") or "BF_Q9_5DTE").strip()
    csv_path = (os.environ.get("BF_LOG_PATH") or "/tmp/bf5_trades.csv").strip()

    try:
        svc, _ = sheets_client()
        sid = gsheet_id
    except Exception as e:
        msg = f"sheets_client failed: {type(e).__name__}: {e}"
        return fail(msg, 2) if strict else skip(msg)

    # Schwab client for price updates (best-effort)
    schwab_c = None
    if schwab_client is not None:
        try:
            schwab_c = schwab_client()
        except Exception as e:
            log(f"Schwab client init failed (non-fatal): {e}")

    today = date.today()
    today_str = today.isoformat()

    try:
        # --- 1. Ensure tab exists ---
        ensure_tab(svc, sid, tab, HEADERS)

        # --- 2. Read existing rows ---
        existing = read_existing(svc, sid, tab, HEADERS)
        existing_dates = {row[C_TRADE_DATE] for row in existing if row[C_TRADE_DATE]}

        # --- 3. Add new trades from CSV log ---
        new_rows = parse_csv_log(csv_path)
        appends = []
        for row in new_rows:
            if row[C_TRADE_DATE] and row[C_TRADE_DATE] not in existing_dates:
                appends.append(row)
                existing.append(row)
                existing_dates.add(row[C_TRADE_DATE])

        if appends:
            svc.spreadsheets().values().append(
                spreadsheetId=sid,
                range=f"{tab}!A1",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": appends},
            ).execute()
            log(f"appended {len(appends)} new entry(ies)")

        # --- 4. Update intermediate prices + settle expired trades ---
        if schwab_c is None:
            log("No Schwab client — skipping price updates")
            return 0

        # Collect all open trades that need price updates
        updates = []
        osi_symbols = set()
        open_trades = []

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
                "lower_osi": lower_osi,
                "center_osi": center_osi,
                "upper_osi": upper_osi,
            })

            # Collect OSI symbols for batch quote (only for non-expired)
            if dte > 0:
                osi_symbols.update([lower_osi, center_osi, upper_osi])

        if not open_trades:
            log("No open trades to update")
            return 0

        # Fetch all option quotes in one batch
        quotes = {}
        if osi_symbols:
            # Batch in groups of 30 symbols (10 trades × 3 legs)
            sym_list = list(osi_symbols)
            for j in range(0, len(sym_list), 30):
                batch = sym_list[j:j + 30]
                batch_quotes = fetch_quotes_batch(schwab_c, batch)
                quotes.update(batch_quotes)
                if j + 30 < len(sym_list):
                    time.sleep(0.5)

            log(f"Fetched quotes for {len(quotes)} option symbols")

        # Fetch SPX for settlement (lazy)
        spx_price = None

        for trade in open_trades:
            sheet_row = trade["row_idx"] + 2  # 1-indexed, header is row 1
            dte = trade["dte"]

            if dte > 0 and dte in DTE_COL:
                # Intermediate price update
                col_idx = DTE_COL[dte]

                # Skip if already filled
                current_val = existing[trade["row_idx"]][col_idx] if col_idx < len(existing[trade["row_idx"]]) else ""
                if current_val:
                    continue

                bf_mid = butterfly_mid_from_quotes(
                    quotes,
                    trade["lower_osi"],
                    trade["center_osi"],
                    trade["upper_osi"],
                )
                if bf_mid is not None:
                    col_ltr = col_letter(col_idx)
                    updates.append({
                        "range": f"{tab}!{col_ltr}{sheet_row}",
                        "values": [[f"{bf_mid:.2f}"]],
                    })
                    log(f"  {trade['exp_str']} DTE={dte}: bf_mid={bf_mid:.2f}")

            elif dte <= 0:
                # Expired — compute settlement
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

                # Update status through pnl (columns C_STATUS to C_PNL)
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
                log(f"  SETTLED {trade['exp_str']}: settle={settle_val:.2f} pnl=${pnl:.0f}" if pnl else f"  SETTLED {trade['exp_str']}")

        if updates:
            svc.spreadsheets().values().batchUpdate(
                spreadsheetId=sid,
                body={"valueInputOption": "RAW", "data": updates},
            ).execute()
            log(f"Updated {len(updates)} cell(s)")

    except Exception as e:
        msg = f"error: {type(e).__name__}: {e}"
        log(msg)
        if strict:
            return 2

    return 0


if __name__ == "__main__":
    sys.exit(main())
