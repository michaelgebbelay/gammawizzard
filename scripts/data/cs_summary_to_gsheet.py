#!/usr/bin/env python3
"""
Compute aggregate summary from CS_Tracking tab and write to CS_Summary tab.

Reads all rows from CS_Tracking, computes per-account stats, and writes
a clean summary to CS_Summary. Runs as a post-step after cs_tracking_to_gsheet.py.

NON-BLOCKING BY DEFAULT (same pattern as other gsheet scripts).

Env:
  GSHEET_ID                    - spreadsheet ID
  GOOGLE_SERVICE_ACCOUNT_JSON  - full JSON string for service account
  CS_TRACKING_TAB              - source tab (default "CS_Tracking")
  CS_SUMMARY_TAB               - target tab (default "CS_Summary")
  CS_GSHEET_STRICT             - "1" to fail hard on errors
"""

import os
import sys
import json
from collections import defaultdict
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

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

TAG = "CS_SUMMARY"
ET = ZoneInfo("America/New_York")


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


def safe_int(s, default=0):
    try:
        return int(s)
    except (ValueError, TypeError):
        return default


def safe_float(s, default=None):
    try:
        return float(s)
    except (ValueError, TypeError):
        return default


# ---------------------------------------------------------------------------
# Summary computation
# ---------------------------------------------------------------------------

def compute_summary(rows: list[dict]) -> list[list[str]]:
    """Compute per-account summary stats from tracking rows."""

    # Accumulate per account
    accounts = defaultdict(lambda: {
        "days": set(),
        "put_target": 0, "put_filled": 0,
        "call_target": 0, "call_filled": 0,
        "put_improvements": [], "call_improvements": [],
        "total_cost": 0.0,
    })

    for row in rows:
        acct = (row.get("account") or "").strip()
        if not acct:
            continue
        a = accounts[acct]
        a["days"].add(row.get("date", ""))
        a["put_target"] += safe_int(row.get("put_target"))
        a["put_filled"] += safe_int(row.get("put_filled"))
        a["call_target"] += safe_int(row.get("call_target"))
        a["call_filled"] += safe_int(row.get("call_filled"))

        pi = safe_float(row.get("put_improvement"))
        if pi is not None:
            a["put_improvements"].append(pi)
        ci = safe_float(row.get("call_improvement"))
        if ci is not None:
            a["call_improvements"].append(ci)

        tc = safe_float(row.get("total_cost"))
        if tc is not None:
            a["total_cost"] += tc

    # Build summary rows
    ACCOUNT_ORDER = ["schwab", "tt-ira", "tt-individual"]
    sorted_accounts = sorted(accounts.keys(), key=lambda x: ACCOUNT_ORDER.index(x) if x in ACCOUNT_ORDER else 99)

    result = []
    for acct in sorted_accounts:
        a = accounts[acct]
        days = len(a["days"])
        pt, pf = a["put_target"], a["put_filled"]
        ct, cf = a["call_target"], a["call_filled"]
        put_rate = f"{pf / pt * 100:.0f}%" if pt > 0 else "—"
        call_rate = f"{cf / ct * 100:.0f}%" if ct > 0 else "—"
        avg_pi = f"{sum(a['put_improvements']) / len(a['put_improvements']):.3f}" if a["put_improvements"] else "—"
        avg_ci = f"{sum(a['call_improvements']) / len(a['call_improvements']):.3f}" if a["call_improvements"] else "—"
        total_spreads = pf + cf
        cost_per_spread = f"{a['total_cost'] / total_spreads:.2f}" if total_spreads > 0 else "—"

        result.append([
            acct, str(days),
            str(pt), str(pf), put_rate,
            str(ct), str(cf), call_rate,
            avg_pi, avg_ci,
            f"{a['total_cost']:.2f}", cost_per_spread,
        ])

    return result


def compute_daily_detail(rows: list[dict]) -> list[list[str]]:
    """Build a daily pivot: one row per date with accounts side by side."""

    # Group by date
    by_date = defaultdict(dict)
    for row in rows:
        dt = (row.get("date") or "").strip()
        acct = (row.get("account") or "").strip()
        if dt and acct:
            by_date[dt][acct] = row

    ACCOUNT_ORDER = ["schwab", "tt-ira", "tt-individual"]
    result = []
    for dt in sorted(by_date.keys(), reverse=True):
        accts = by_date[dt]
        row_out = [dt]
        for acct in ACCOUNT_ORDER:
            r = accts.get(acct)
            if r:
                pf = r.get("put_filled", "0")
                cf = r.get("call_filled", "0")
                fp = r.get("put_fill_price", "")
                fc = r.get("call_fill_price", "")
                pi = r.get("put_improvement", "")
                ci = r.get("call_improvement", "")
                tc = r.get("total_cost", "")
                row_out.extend([pf, cf, fp, fc, pi, ci, tc])
            else:
                row_out.extend(["", "", "", "", "", "", ""])
        result.append(row_out)

    return result


SUMMARY_HEADER = [
    "account", "trading_days",
    "put_target", "put_filled", "put_fill_rate",
    "call_target", "call_filled", "call_fill_rate",
    "avg_put_improve", "avg_call_improve",
    "total_cost", "cost_per_spread",
]

DAILY_HEADER = [
    "date",
    "schw_put", "schw_call", "schw_put_px", "schw_call_px", "schw_put_imp", "schw_call_imp", "schw_cost",
    "ira_put", "ira_call", "ira_put_px", "ira_call_px", "ira_put_imp", "ira_call_imp", "ira_cost",
    "ind_put", "ind_call", "ind_put_px", "ind_call_px", "ind_put_imp", "ind_call_imp", "ind_cost",
]


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

    tracking_tab = (os.environ.get("CS_TRACKING_TAB") or "CS_Tracking").strip()
    summary_tab = (os.environ.get("CS_SUMMARY_TAB") or "CS_Summary").strip()

    try:
        creds = creds_from_env()
        if creds is None:
            return fail("SA creds empty", 2) if strict else skip("SA creds empty")
        svc = build("sheets", "v4", credentials=creds, cache_discovery=False)

        # Read tracking data
        resp = svc.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=f"{tracking_tab}!A1:ZZ"
        ).execute()
        all_rows = resp.get("values") or []
        if len(all_rows) < 2:
            return skip("no tracking data yet")

        header = all_rows[0]
        rows = []
        for vals in all_rows[1:]:
            d = {header[i]: (vals[i] if i < len(vals) else "") for i in range(len(header))}
            rows.append(d)

        log(f"read {len(rows)} tracking rows")

        # Compute summaries
        summary_rows = compute_summary(rows)
        daily_rows = compute_daily_detail(rows)

        # Write to summary tab
        ensure_sheet_tab(svc, spreadsheet_id, summary_tab)

        now_et = datetime.now(timezone.utc).astimezone(ET)
        timestamp = now_et.strftime("%Y-%m-%d %I:%M %p ET")

        # Build output: title + summary + gap + daily detail
        output = []
        output.append(["Account Performance Summary", "", "", "", "", "", "", "", "", "", "", f"Updated: {timestamp}"])
        output.append([])  # blank row
        output.append(SUMMARY_HEADER)
        output.extend(summary_rows)
        output.append([])  # blank row
        output.append([])  # blank row
        output.append(["Daily Detail (newest first)"])
        output.append(DAILY_HEADER)
        output.extend(daily_rows)

        last_col = col_letter(max(len(SUMMARY_HEADER), len(DAILY_HEADER)) - 1)
        last_row = len(output)

        # Clear and write
        svc.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range=f"{summary_tab}!A1:ZZ",
        ).execute()

        svc.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{summary_tab}!A1:{last_col}{last_row}",
            valueInputOption="RAW",
            body={"values": output},
        ).execute()

        log(f"wrote summary ({len(summary_rows)} accounts, {len(daily_rows)} daily rows) to {summary_tab}")
        return 0

    except Exception as e:
        msg = f"Summary failed: {type(e).__name__}: {e}"
        return fail(msg, 2) if strict else skip(msg)


if __name__ == "__main__":
    sys.exit(main())
