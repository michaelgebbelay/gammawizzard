#!/usr/bin/env python3
"""
Write monthly performance report to CS_Performance tab.

Reads daily P&L from CS_Summary, aggregates into monthly dollar P&L and
rolling trade statistics (last 10/20 trades) for all 4 accounts:
  Leo (GW Signal), Schwab, TT IRA, TT Individual.

Layout: stacked vertical sections, one per account.  Each section has a
monthly P&L table (years as columns, months as rows) followed by rolling
stats (Total P&L, Edge, Max Drawdown, Factor, Win Rate).

NON-BLOCKING BY DEFAULT (same pattern as other gsheet scripts).

Env:
  GSHEET_ID                    - spreadsheet ID
  GOOGLE_SERVICE_ACCOUNT_JSON  - full JSON string for service account
  CS_SUMMARY_TAB               - source tab (default "CS_Summary")
  CS_PERFORMANCE_TAB           - target tab (default "CS_Performance")
  CS_GSHEET_STRICT             - "1" to fail hard on errors
"""

import os
import sys
from collections import defaultdict

# --- path setup ---
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
    from lib.sheets import sheets_client, ensure_sheet_tab, get_values, col_letter
    from lib.parsing import safe_float
except Exception as e:
    sheets_client = None
    _IMPORT_ERR = e

TAG = "CS_PERFORMANCE"

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SUMMARY_DATA_START_ROW = 11  # CS_Summary row 11 = first data row

# Account definitions: name + P&L column index (0-based) in CS_Summary
# CS_Summary: A=Date, B-E=GW, F-I=Schwab, J-M=TT IRA, N-Q=TT IND
# P&L is the 4th column in each group: E=4, I=8, M=12, Q=16
ACCOUNTS = [
    {"name": "Leo (GW Signal)", "pnl_col": 4},
    {"name": "Schwab",          "pnl_col": 8},
    {"name": "TT IRA",          "pnl_col": 12},
    {"name": "TT Individual",   "pnl_col": 16},
]

SPX_MULTIPLIER = 100  # 1 SPX point = $100

MONTH_NAMES = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

SECTION_ROWS = 22  # rows per account section

# Colors matching CS_Summary (RGB 0-1 scale)
COLOR_GW   = {"red": 0.851, "green": 0.918, "blue": 0.827}
COLOR_SCHW = {"red": 0.788, "green": 0.855, "blue": 0.973}
COLOR_IRA  = {"red": 0.851, "green": 0.918, "blue": 0.827}
COLOR_IND  = {"red": 1.0,   "green": 0.949, "blue": 0.800}
ACCOUNT_COLORS = [COLOR_GW, COLOR_SCHW, COLOR_IRA, COLOR_IND]


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
# Data reading
# ---------------------------------------------------------------------------

def read_summary_data(svc, spreadsheet_id: str, summary_tab: str) -> list:
    """Read CS_Summary data rows (row 11+).

    Returns list of dicts: [{"date": str, "pnl": {acct_idx: float|None}}, ...]
    P&L values are in SPX points (None if empty/unsettled).
    Data comes newest-first from CS_Summary.
    """
    all_rows = get_values(svc, spreadsheet_id,
                          f"{summary_tab}!A{SUMMARY_DATA_START_ROW}:Q")
    result = []
    for row in all_rows:
        if not row or not (row[0] if row else "").strip():
            continue
        date_str = row[0].strip()
        pnl_by_acct = {}
        for i, acct in enumerate(ACCOUNTS):
            col = acct["pnl_col"]
            raw = row[col].strip() if col < len(row) and row[col] else ""
            pnl_by_acct[i] = safe_float(raw)
        result.append({"date": date_str, "pnl": pnl_by_acct})
    return result


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

def aggregate_monthly(data: list, acct_idx: int) -> dict:
    """Aggregate daily P&L into {(year, month): total_dollars}.

    Skips entries where P&L is None (unsettled).
    """
    monthly = defaultdict(float)
    for row in data:
        pnl_points = row["pnl"].get(acct_idx)
        if pnl_points is None:
            continue
        date_str = row["date"]
        try:
            year = int(date_str[:4])
            month = int(date_str[5:7])
        except (ValueError, IndexError):
            continue
        monthly[(year, month)] += pnl_points * SPX_MULTIPLIER
    return monthly


def compute_rolling_stats(data: list, acct_idx: int, n: int) -> dict:
    """Compute rolling stats for the last N settled trades.

    Data is newest-first; take first N non-None P&L entries.
    Returns dict with total_pnl, edge, max_dd, factor, win_rate
    or None if no settled trades.
    """
    trades = []
    for row in data:
        pnl_points = row["pnl"].get(acct_idx)
        if pnl_points is not None:
            trades.append(pnl_points * SPX_MULTIPLIER)
            if len(trades) >= n:
                break

    if not trades:
        return None

    # Reverse to chronological order for drawdown calculation
    trades_chrono = list(reversed(trades))
    num = len(trades_chrono)
    total_pnl = sum(trades_chrono)

    # Edge = avg SPX points per trade
    edge = total_pnl / num / SPX_MULTIPLIER

    # Max drawdown: peak-to-trough in cumulative dollar P&L
    cum = 0.0
    peak = 0.0
    max_dd = 0.0
    for pnl in trades_chrono:
        cum += pnl
        if cum > peak:
            peak = cum
        dd = peak - cum
        if dd > max_dd:
            max_dd = dd

    factor = (total_pnl / max_dd) if max_dd > 0.01 else 0.0
    wins = sum(1 for p in trades_chrono if p > 0)
    win_rate = wins / num

    return {
        "total_pnl": round(total_pnl, 2),
        "edge": round(edge, 2),
        "max_dd": round(max_dd, 2),
        "factor": round(factor, 2),
        "win_rate": round(win_rate, 4),
    }


# ---------------------------------------------------------------------------
# Grid builder
# ---------------------------------------------------------------------------

def build_performance_grid(data: list) -> tuple:
    """Build the complete cell grid for CS_Performance tab.

    Returns (grid, years) where grid is list[list] of cell values
    and years is the sorted list of years found in the data.
    """
    grid = []

    # Title
    grid.append(["CS Performance"])
    grid.append([])

    # Determine year range
    years = set()
    for row in data:
        try:
            years.add(int(row["date"][:4]))
        except (ValueError, IndexError):
            pass
    years = sorted(years) if years else [2026]

    for acct_idx, acct in enumerate(ACCOUNTS):
        monthly = aggregate_monthly(data, acct_idx)
        stats_10 = compute_rolling_stats(data, acct_idx, 10)
        stats_20 = compute_rolling_stats(data, acct_idx, 20)

        # +0: Account name
        grid.append([acct["name"]])

        # +1: Year headers
        grid.append([""] + [str(y) for y in years])

        # +2 to +13: Monthly data (Jan-Dec)
        for m_idx, m_name in enumerate(MONTH_NAMES):
            row = [m_name]
            for y in years:
                val = monthly.get((y, m_idx + 1))
                row.append(round(val, 0) if val is not None else "")
            grid.append(row)

        # +14: blank
        grid.append([])

        # +15: Stats header
        grid.append(["", "Last 10", "Last 20"])

        # +16 to +20: Stats rows
        stat_rows = [
            ("Total P&L", "total_pnl"),
            ("Edge", "edge"),
            ("Max Drawdown", "max_dd"),
            ("Factor", "factor"),
            ("Win Rate", "win_rate"),
        ]
        for label, key in stat_rows:
            val_10 = stats_10[key] if stats_10 else ""
            val_20 = stats_20[key] if stats_20 else ""
            if key == "win_rate":
                val_10 = f"{val_10:.0%}" if isinstance(val_10, float) else ""
                val_20 = f"{val_20:.0%}" if isinstance(val_20, float) else ""
            grid.append([label, val_10, val_20])

        # +21: blank separator
        grid.append([])

    return grid, years


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------

def apply_formatting(svc, spreadsheet_id: str, sheet_id: int,
                     years: list, num_accounts: int = 4):
    """Apply background colors, bold text, and currency formats."""
    requests = []
    num_years = len(years)
    max_col = max(num_years + 1, 3)

    # Title: bold 12pt
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 0, "endRowIndex": 1,
                "startColumnIndex": 0, "endColumnIndex": 1,
            },
            "cell": {"userEnteredFormat": {
                "textFormat": {"bold": True, "fontSize": 12}
            }},
            "fields": "userEnteredFormat.textFormat",
        }
    })

    for acct_idx in range(num_accounts):
        color = ACCOUNT_COLORS[acct_idx]
        base = 2 + acct_idx * SECTION_ROWS  # 0-indexed start of section

        # Account name: bold + background color
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": base, "endRowIndex": base + 1,
                    "startColumnIndex": 0, "endColumnIndex": max_col,
                },
                "cell": {"userEnteredFormat": {
                    "backgroundColor": color,
                    "textFormat": {"bold": True},
                }},
                "fields": "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat",
            }
        })

        # Year header row: background color + bold
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": base + 1, "endRowIndex": base + 2,
                    "startColumnIndex": 0, "endColumnIndex": max_col,
                },
                "cell": {"userEnteredFormat": {
                    "backgroundColor": color,
                    "textFormat": {"bold": True},
                }},
                "fields": "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat",
            }
        })

        # Monthly P&L cells: currency format (rows +2 to +13, cols 1..num_years)
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": base + 2, "endRowIndex": base + 14,
                    "startColumnIndex": 1, "endColumnIndex": num_years + 1,
                },
                "cell": {"userEnteredFormat": {
                    "numberFormat": {"type": "CURRENCY", "pattern": "$#,##0"}
                }},
                "fields": "userEnteredFormat.numberFormat",
            }
        })

        # Stats header row: bold + background color
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": base + 15, "endRowIndex": base + 16,
                    "startColumnIndex": 0, "endColumnIndex": 3,
                },
                "cell": {"userEnteredFormat": {
                    "backgroundColor": color,
                    "textFormat": {"bold": True},
                }},
                "fields": "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat",
            }
        })

        # Currency format for Total P&L and Max Drawdown stats
        for offset in [16, 18]:  # Total P&L = +16, Max Drawdown = +18
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": base + offset,
                        "endRowIndex": base + offset + 1,
                        "startColumnIndex": 1, "endColumnIndex": 3,
                    },
                    "cell": {"userEnteredFormat": {
                        "numberFormat": {"type": "CURRENCY", "pattern": "$#,##0"}
                    }},
                    "fields": "userEnteredFormat.numberFormat",
                }
            })

    if requests:
        svc.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests},
        ).execute()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    strict = strict_enabled()

    if sheets_client is None:
        msg = f"google sheets libs not installed ({_IMPORT_ERR})"
        return fail(msg, 2) if strict else skip(msg)

    if not (os.environ.get("GSHEET_ID") or "").strip():
        return fail("GSHEET_ID missing", 2) if strict else skip("GSHEET_ID missing")

    if not (os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip():
        return fail("SA creds missing", 2) if strict else skip("SA creds missing")

    summary_tab = (os.environ.get("CS_SUMMARY_TAB") or "CS_Summary").strip()
    perf_tab = (os.environ.get("CS_PERFORMANCE_TAB") or "CS_Performance").strip()

    try:
        svc, spreadsheet_id = sheets_client()

        data = read_summary_data(svc, spreadsheet_id, summary_tab)
        if not data:
            return skip("no summary data yet")

        log(f"read {len(data)} summary rows")

        grid, years = build_performance_grid(data)

        sheet_id = ensure_sheet_tab(svc, spreadsheet_id, perf_tab)

        # Clear existing data
        clear_to = max(len(grid) + 50, 200)
        svc.spreadsheets().values().batchClear(
            spreadsheetId=spreadsheet_id,
            body={"ranges": [f"{perf_tab}!A1:Z{clear_to}"]},
        ).execute()

        # Write all data
        svc.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{perf_tab}!A1",
            valueInputOption="RAW",
            body={"values": grid},
        ).execute()

        apply_formatting(svc, spreadsheet_id, sheet_id, years)

        # Log settled trade counts
        for i, acct in enumerate(ACCOUNTS):
            settled = sum(1 for row in data if row["pnl"].get(i) is not None)
            log(f"{acct['name']}: {settled} settled trades")

        log(f"wrote {len(grid)} rows to {perf_tab}")
        return 0

    except Exception as e:
        msg = f"Performance failed: {type(e).__name__}: {e}"
        return fail(msg, 2) if strict else skip(msg)


if __name__ == "__main__":
    sys.exit(main())
