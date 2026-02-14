#!/usr/bin/env python3
"""
Write monthly performance report to CS_Performance tab.

Reads daily P&L from CS_Summary, aggregates into monthly dollar P&L and
rolling trade statistics (last 10/20 trades) for all 4 accounts:
  Leo (GW Signal), Schwab, TT IRA, TT Individual.

Layout:
  Row 1:    "CS Performance"
  Row 2:    (blank)
  Row 3:    "", year1, year2, ...  (year sub-header, merged concept)
  Row 4:    "CS Performance", "Leo (GW Signal)", "Schwab", "TT IRA", "TT Individual", "Total"
  Row 5-16: Jan-Dec monthly P&L (all accounts side by side + Total)
  Row 17:   (blank)
  Row 18:   Leo stats header + Schwab stats header (side by side)
  Row 19-23: Stats rows (Leo cols A-C, blank D, Schwab cols E-G)
  Row 24-25: (blank)
  Row 26:   TT IRA stats header + TT Individual stats header
  Row 27-31: Stats rows (TT IRA cols A-C, blank D, TT Individual cols E-G)

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
ACCOUNTS = [
    {"name": "Leo (GW Signal)", "pnl_col": 4},
    {"name": "Schwab",          "pnl_col": 8},
    {"name": "TT IRA",          "pnl_col": 12},
    {"name": "TT Individual",   "pnl_col": 16},
]

SPX_MULTIPLIER = 100  # 1 SPX point = $100

MONTH_NAMES = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

# Colors matching CS_Summary (RGB 0-1 scale)
COLOR_GW   = {"red": 0.851, "green": 0.918, "blue": 0.827}
COLOR_SCHW = {"red": 0.788, "green": 0.855, "blue": 0.973}
COLOR_IRA  = {"red": 0.851, "green": 0.918, "blue": 0.827}
COLOR_IND  = {"red": 1.0,   "green": 0.949, "blue": 0.800}
ACCOUNT_COLORS = [COLOR_GW, COLOR_SCHW, COLOR_IRA, COLOR_IND]

# Layout row offsets (0-indexed)
YEAR_ROW = 2       # Row 3: year sub-header
HEADER_ROW = 3     # Row 4: column headers
DATA_START = 4     # Row 5: first month (Jan)
DATA_END = 16      # Row 16: last month (Dec) — DATA_START + 12
STATS1_HEADER = 17 # Row 18: Leo + Schwab stats header
STATS1_DATA = 18   # Row 19-22: stats data
STATS2_HEADER = 25 # Row 26: TT IRA + TT Individual stats header
STATS2_DATA = 26   # Row 27-30: stats data


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
# Stats block helper
# ---------------------------------------------------------------------------

def _stats_block(name_left, stats_left_10, stats_left_20,
                 name_right, stats_right_10, stats_right_20) -> list:
    """Build a paired stats block (2 accounts side by side).

    Returns list of 7 rows:
      header: [name_left, "Last 10", "Last 20", "", name_right, "Last 10", "Last 20"]
      5 stat rows: [label, val, val, "", label, val, val]
      blank separator row
    """
    rows = []
    rows.append([name_left, "Last 10", "Last 20", "", name_right, "Last 10", "Last 20"])

    stat_defs = [
        ("Total P&L", "total_pnl"),
        ("Edge", "edge"),
        ("Max Drawdown", "max_dd"),
        ("Factor", "factor"),
        ("Win Rate", "win_rate"),
    ]
    for label, key in stat_defs:
        lv10 = stats_left_10[key] if stats_left_10 else ""
        lv20 = stats_left_20[key] if stats_left_20 else ""
        rv10 = stats_right_10[key] if stats_right_10 else ""
        rv20 = stats_right_20[key] if stats_right_20 else ""
        if key == "win_rate":
            lv10 = f"{lv10:.0%}" if isinstance(lv10, float) else ""
            lv20 = f"{lv20:.0%}" if isinstance(lv20, float) else ""
            rv10 = f"{rv10:.0%}" if isinstance(rv10, float) else ""
            rv20 = f"{rv20:.0%}" if isinstance(rv20, float) else ""
        rows.append([label, lv10, lv20, "", label, rv10, rv20])

    rows.append([])  # blank separator
    return rows


# ---------------------------------------------------------------------------
# Grid builder
# ---------------------------------------------------------------------------

def build_performance_grid(data: list) -> tuple:
    """Build the complete cell grid for CS_Performance tab.

    Returns (grid, years) where grid is list[list] of cell values.
    """
    # Determine year range
    years = set()
    for row in data:
        try:
            years.add(int(row["date"][:4]))
        except (ValueError, IndexError):
            pass
    years = sorted(years) if years else [2026]

    # Aggregate monthly for all accounts
    monthlies = [aggregate_monthly(data, i) for i in range(len(ACCOUNTS))]

    # Compute rolling stats for all accounts
    all_stats = []
    for i in range(len(ACCOUNTS)):
        all_stats.append((
            compute_rolling_stats(data, i, 10),
            compute_rolling_stats(data, i, 20),
        ))

    grid = []

    # Row 1: Title
    grid.append(["CS Performance"])
    # Row 2: blank
    grid.append([])

    # Row 3: Year sub-header (one row per year block)
    # For each year, span across account columns
    year_row = [""]
    for y in years:
        year_row.append(str(y))
    grid.append(year_row)

    # Row 4: Column headers
    # "CS Performance", account names..., "Total"
    header = ["CS Performance"]
    for acct in ACCOUNTS:
        header.append(acct["name"])
    header.append("Total")
    grid.append(header)

    # Rows 5-16: Monthly data (Jan-Dec)
    for m_idx, m_name in enumerate(MONTH_NAMES):
        row = [m_name]
        month_total = 0.0
        has_any = False
        for acct_idx in range(len(ACCOUNTS)):
            val = monthlies[acct_idx].get((years[0] if len(years) == 1 else None, m_idx + 1))
            # For multi-year: sum across years for this month
            if len(years) == 1:
                val = monthlies[acct_idx].get((years[0], m_idx + 1))
            else:
                val = sum(monthlies[acct_idx].get((y, m_idx + 1), 0)
                          for y in years) or None
            if val is not None:
                row.append(round(val, 0))
                month_total += val
                has_any = True
            else:
                row.append("")
        row.append(round(month_total, 0) if has_any else "")
        grid.append(row)

    # Row 17: blank
    grid.append([])

    # Rows 18-23: Leo + Schwab stats (side by side)
    grid.extend(_stats_block(
        ACCOUNTS[0]["name"], all_stats[0][0], all_stats[0][1],
        ACCOUNTS[1]["name"], all_stats[1][0], all_stats[1][1],
    ))

    # Row 24-25: blank (the _stats_block already ends with a blank row)
    grid.append([])

    # Rows 26-31: TT IRA + TT Individual stats (side by side)
    grid.extend(_stats_block(
        ACCOUNTS[2]["name"], all_stats[2][0], all_stats[2][1],
        ACCOUNTS[3]["name"], all_stats[3][0], all_stats[3][1],
    ))

    return grid, years


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------

def apply_formatting(svc, spreadsheet_id: str, sheet_id: int, years: list):
    """Apply background colors, bold text, and currency formats."""
    requests = []
    num_accts = len(ACCOUNTS)
    total_col = num_accts + 2  # A + 4 accounts + Total = 6 cols (0..5), end=6

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

    # Header row (row 4, 0-indexed=3): bold
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": HEADER_ROW, "endRowIndex": HEADER_ROW + 1,
                "startColumnIndex": 0, "endColumnIndex": total_col,
            },
            "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
            "fields": "userEnteredFormat.textFormat",
        }
    })

    # Account column colors in monthly table (rows 4-16, 0-indexed 3-16)
    for acct_idx, color in enumerate(ACCOUNT_COLORS):
        col = acct_idx + 1  # col B=1, C=2, D=3, E=4
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": HEADER_ROW, "endRowIndex": DATA_END,
                    "startColumnIndex": col, "endColumnIndex": col + 1,
                },
                "cell": {"userEnteredFormat": {"backgroundColor": color}},
                "fields": "userEnteredFormat.backgroundColor",
            }
        })

    # Currency format for monthly P&L data (rows 5-16, cols B-F)
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": DATA_START, "endRowIndex": DATA_END,
                "startColumnIndex": 1, "endColumnIndex": total_col,
            },
            "cell": {"userEnteredFormat": {
                "numberFormat": {"type": "CURRENCY", "pattern": "$#,##0"}
            }},
            "fields": "userEnteredFormat.numberFormat",
        }
    })

    # Stats formatting (two pairs)
    for header_row, color_left, color_right in [
        (STATS1_HEADER, COLOR_GW, COLOR_SCHW),
        (STATS2_HEADER, COLOR_IRA, COLOR_IND),
    ]:
        # Left stats header: bold + color (cols A-C)
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": header_row, "endRowIndex": header_row + 1,
                    "startColumnIndex": 0, "endColumnIndex": 3,
                },
                "cell": {"userEnteredFormat": {
                    "backgroundColor": color_left,
                    "textFormat": {"bold": True},
                }},
                "fields": "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat",
            }
        })
        # Right stats header: bold + color (cols E-G)
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": header_row, "endRowIndex": header_row + 1,
                    "startColumnIndex": 4, "endColumnIndex": 7,
                },
                "cell": {"userEnteredFormat": {
                    "backgroundColor": color_right,
                    "textFormat": {"bold": True},
                }},
                "fields": "userEnteredFormat.backgroundColor,userEnteredFormat.textFormat",
            }
        })

        # Currency format for Total P&L and Max Drawdown (offsets +1 and +3)
        data_row = header_row + 1
        for offset in [0, 2]:  # Total P&L = +0, Max Drawdown = +2
            for start_c, end_c in [(1, 3), (5, 7)]:  # left B-C, right F-G
                requests.append({
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": data_row + offset,
                            "endRowIndex": data_row + offset + 1,
                            "startColumnIndex": start_c, "endColumnIndex": end_c,
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
