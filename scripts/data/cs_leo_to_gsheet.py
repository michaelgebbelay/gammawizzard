#!/usr/bin/env python3
"""
Fetch Leo trade signals from GammaWizard API and upsert to sw_leo_orders tab.

The sw_leo_orders tab is consumed by sw_3way_summary.py for Schwab performance
analysis (Leo vs Standard vs Adjusted P&L comparison).

NON-BLOCKING BY DEFAULT (same pattern as other CS gsheet scripts).

Env:
  GSHEET_ID                    - spreadsheet ID
  GOOGLE_SERVICE_ACCOUNT_JSON  - full JSON string for service account
  GW_BASE                      - GW API base URL (default https://gandalf.gammawizard.com)
  GW_EMAIL / GW_PASSWORD       - GW credentials
  GW_TOKEN                     - bearer token (optional, falls back to email/password)
  LEO_SPREAD_WIDTH             - spread width in points (default 5)
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
    from lib.parsing import safe_float
except Exception as e:
    sheets_client = None
    _IMPORT_ERR = e

TAG = "CS_LEO"
LEO_TAB = "sw_leo_orders"
LEO_HEADERS = ["exp_primary", "side", "short_put", "long_put",
               "short_call", "long_call", "price"]


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


def gw_fetch_leo():
    """Fetch the GetLeoCross endpoint from GammaWizard."""
    base = (os.environ.get("GW_BASE", "https://gandalf.gammawizard.com") or "").rstrip("/")
    endpoint = "rapi/GetLeoCross"
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


def extract_trade(j):
    """Extract trade dict from GW API response (same logic as leocross_orchestrator)."""
    if isinstance(j, dict):
        if "Trade" in j:
            tr = j["Trade"]
            return tr[-1] if isinstance(tr, list) and tr else tr if isinstance(tr, dict) else {}
        for v in j.values():
            if isinstance(v, (dict, list)):
                t = extract_trade(v)
                if t:
                    return t
    if isinstance(j, list):
        for it in reversed(j):
            t = extract_trade(it)
            if t:
                return t
    return {}


def extract_leo_rows(j) -> list[dict]:
    """Extract Leo order rows from GW API response.

    Expected trade fields: TDate, Limit, CLimit, Cat1, Cat2, Price/Mid.
    Returns list of dicts matching LEO_HEADERS.
    """
    width = int(os.environ.get("LEO_SPREAD_WIDTH", "5") or "5")

    # Try to get multiple trades (historical)
    trades = []

    # Check if response is a list of trades
    if isinstance(j, list):
        for item in j:
            tr = extract_trade(item) if isinstance(item, dict) else {}
            if tr:
                trades.append(tr)
    elif isinstance(j, dict):
        # Check for array-like keys
        for key in ("Trade", "Trades", "Data", "data"):
            v = j.get(key)
            if isinstance(v, list):
                for item in v:
                    tr = extract_trade(item) if isinstance(item, dict) else item
                    if isinstance(tr, dict) and tr:
                        trades.append(tr)
                break

        # If no list found, try single trade extraction
        if not trades:
            tr = extract_trade(j)
            if tr:
                trades.append(tr)

    result = []
    seen_dates = set()
    for tr in trades:
        # Expiry date
        exp = str(tr.get("TDate") or tr.get("tdate") or tr.get("Date") or "").strip()
        if not exp or len(exp) < 8:
            continue

        # Keep only the date portion (first 10 chars of ISO)
        exp = exp[:10]
        if exp in seen_dates:
            continue
        seen_dates.add(exp)

        # Strikes
        inner_put = safe_float(tr.get("Limit") or tr.get("limit"))
        inner_call = safe_float(tr.get("CLimit") or tr.get("climit"))
        if inner_put is None or inner_call is None:
            continue

        short_put = int(inner_put)
        short_call = int(inner_call)
        long_put = short_put - width
        long_call = short_call + width

        # Side: Credit if Cat2 >= Cat1 (same as orchestrator)
        cat1 = safe_float(tr.get("Cat1") or tr.get("cat1"))
        cat2 = safe_float(tr.get("Cat2") or tr.get("cat2"))
        if cat2 is None or cat1 is None or cat2 >= cat1:
            side = "short"  # credit IC
        else:
            side = "long"   # debit IC

        # Price: try common field names
        price = (
            safe_float(tr.get("Price") or tr.get("price"))
            or safe_float(tr.get("Mid") or tr.get("mid"))
            or safe_float(tr.get("NetPrice") or tr.get("net_price"))
            or safe_float(tr.get("Theo") or tr.get("theo"))
        )
        price_str = f"{price:.2f}" if price is not None else ""

        result.append({
            "exp_primary": exp,
            "side": side,
            "short_put": str(short_put),
            "long_put": str(long_put),
            "short_call": str(short_call),
            "long_call": str(long_call),
            "price": price_str,
        })

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
        api = gw_fetch_leo()
        rows = extract_leo_rows(api)
        if not rows:
            return skip("no Leo trade data from GW")

        log(f"fetched {len(rows)} Leo order row(s) from GW")

        svc, sid = sheets_client()
        ensure_sheet_tab(svc, sid, LEO_TAB)

        existing = get_values(svc, sid, f"{LEO_TAB}!A1:ZZ")
        last_col = col_letter(len(LEO_HEADERS) - 1)

        # Ensure header
        if not existing or existing[0] != LEO_HEADERS:
            svc.spreadsheets().values().update(
                spreadsheetId=sid,
                range=f"{LEO_TAB}!A1:{last_col}1",
                valueInputOption="RAW",
                body={"values": [LEO_HEADERS]},
            ).execute()
            if existing:
                existing = [LEO_HEADERS] + existing[1:]
            else:
                existing = [LEO_HEADERS]

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
            values = [d[h] for h in LEO_HEADERS]
            if exp in existing_dates:
                rnum = existing_dates[exp]
                updates.append({
                    "range": f"{LEO_TAB}!A{rnum}:{last_col}{rnum}",
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
                range=f"{LEO_TAB}!A1",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": appends},
            ).execute()

        log(f"updated={len(updates)} appended={len(appends)} to {LEO_TAB}")
        return 0

    except Exception as e:
        msg = f"Leo orders failed: {type(e).__name__}: {e}"
        return fail(msg, 2) if strict else skip(msg)


if __name__ == "__main__":
    sys.exit(main())
