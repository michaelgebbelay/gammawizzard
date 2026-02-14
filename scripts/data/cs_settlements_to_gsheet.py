#!/usr/bin/env python3
"""
Fetch SPX settlement prices from GammaWizard API and upsert to sw_settlements tab.

NON-BLOCKING BY DEFAULT (same pattern as other CS gsheet scripts).

The sw_settlements tab is also consumed by sw_3way_summary.py for Schwab
performance analysis.

Env:
  GSHEET_ID                    - spreadsheet ID
  GOOGLE_SERVICE_ACCOUNT_JSON  - full JSON string for service account
  GW_BASE                      - GW API base URL (default https://gandalf.gammawizard.com)
  GW_EMAIL / GW_PASSWORD       - GW credentials
  GW_TOKEN                     - bearer token (optional, falls back to email/password)
  GW_SETTLE_ENDPOINT           - API endpoint for settlements (default rapi/GetSettlements)
  SETTLE_BACKFILL_DAYS         - days of history to keep (default 90)
  CS_GSHEET_STRICT             - "1" to fail hard on errors
"""

import os
import sys

import requests

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
    from lib.sheets import sheets_client, col_letter, ensure_sheet_tab, get_values
except Exception as e:
    sheets_client = None
    _IMPORT_ERR = e

TAG = "CS_SETTLEMENTS"
SETTLE_TAB = "sw_settlements"
SETTLE_HEADERS = ["exp_primary", "settle"]


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
# GW API
# ---------------------------------------------------------------------------

def _sanitize_token(t: str) -> str:
    t = (t or "").strip().strip('"').strip("'")
    return t.split(None, 1)[1] if t.lower().startswith("bearer ") else t


def gw_fetch_settlements():
    base = (os.environ.get("GW_BASE", "https://gandalf.gammawizard.com") or "").rstrip("/")
    endpoint = (os.environ.get("GW_SETTLE_ENDPOINT", "rapi/GetSettlements") or "").lstrip("/")
    url = f"{base}/{endpoint}"

    def hit(tok):
        h = {"Accept": "application/json"}
        if tok:
            h["Authorization"] = f"Bearer {_sanitize_token(tok)}"
        return requests.get(url, headers=h, timeout=30)

    tok = _sanitize_token(os.environ.get("GW_TOKEN", "") or "")
    r = hit(tok) if tok else None

    if (r is None) or (r.status_code in (401, 403)):
        email = os.environ.get("GW_EMAIL", "")
        pwd = os.environ.get("GW_PASSWORD", "")
        if not (email and pwd):
            raise RuntimeError("GW_AUTH_REQUIRED")
        rr = requests.post(
            f"{base}/goauth/authenticateFireUser",
            data={"email": email, "password": pwd},
            timeout=30,
        )
        rr.raise_for_status()
        t = rr.json().get("token") or ""
        r = hit(t)

    r.raise_for_status()
    return r.json()


def extract_settlements(j) -> list[dict]:
    """Extract settlement rows from GW API response.

    Expected format: list of {Date: "2026-02-11", Settlement: 6032.58, ...}
    or nested inside a dict key.
    """
    items = []
    if isinstance(j, list):
        items = j
    elif isinstance(j, dict):
        # Try common keys
        for key in ("Settlements", "Settlement", "Data", "Trade", "data"):
            v = j.get(key)
            if isinstance(v, list):
                items = v
                break
        if not items:
            # Single dict with Date key
            if "Date" in j:
                items = [j]

    result = []
    for item in items:
        if not isinstance(item, dict):
            continue
        # Try various field names for date and settlement price
        dt = item.get("Date") or item.get("date") or item.get("TDate") or ""
        settle = (
            item.get("Settlement") or item.get("settlement")
            or item.get("Settle") or item.get("settle")
            or item.get("SPXSettle") or item.get("Close") or item.get("close")
        )
        if dt and settle is not None:
            try:
                result.append({"exp_primary": str(dt).strip(), "settle": float(settle)})
            except (ValueError, TypeError):
                pass

    return result


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

    try:
        # Fetch settlement data from GW
        api = gw_fetch_settlements()
        rows = extract_settlements(api)
        if not rows:
            return skip("no settlement data from GW")

        log(f"fetched {len(rows)} settlement row(s) from GW")

        # Connect to Sheets
        svc, sid = sheets_client()
        ensure_sheet_tab(svc, sid, SETTLE_TAB)

        # Read existing data
        existing = get_values(svc, sid, f"{SETTLE_TAB}!A1:ZZ")

        last_col = col_letter(len(SETTLE_HEADERS) - 1)

        # Ensure header
        if not existing or existing[0] != SETTLE_HEADERS:
            svc.spreadsheets().values().update(
                spreadsheetId=sid,
                range=f"{SETTLE_TAB}!A1:{last_col}1",
                valueInputOption="RAW",
                body={"values": [SETTLE_HEADERS]},
            ).execute()
            if existing:
                existing = [SETTLE_HEADERS] + existing[1:]
            else:
                existing = [SETTLE_HEADERS]

        # Build existing date map
        existing_dates = {}
        for rnum, row in enumerate(existing[1:], start=2):
            dt = row[0] if len(row) > 0 else ""
            if dt:
                existing_dates[dt.strip()] = rnum

        # Upsert rows
        updates = []
        appends = []
        for d in rows:
            exp = d["exp_primary"]
            values = [exp, str(d["settle"])]
            if exp in existing_dates:
                rnum = existing_dates[exp]
                updates.append({
                    "range": f"{SETTLE_TAB}!A{rnum}:{last_col}{rnum}",
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
                range=f"{SETTLE_TAB}!A1",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": appends},
            ).execute()

        log(f"updated={len(updates)} appended={len(appends)} to {SETTLE_TAB}")
        return 0

    except Exception as e:
        msg = f"Settlements failed: {type(e).__name__}: {e}"
        return fail(msg, 2) if strict else skip(msg)


if __name__ == "__main__":
    sys.exit(main())
