#!/usr/bin/env python3
"""
Check close order status across all accounts and write to CS_TT_Close tab.

For positions with profit-taking close orders, this script checks which
orders have filled and records the close prices. This data is consumed by
cs_summary_to_gsheet.py for accurate P&L calculation.

For positions that closed early: P&L uses the close price
For positions that expired at settlement: P&L uses settlement price (default)

Supports TT IRA, TT Individual, and Schwab accounts.

Env:
  GSHEET_ID                    - spreadsheet ID
  GOOGLE_SERVICE_ACCOUNT_JSON  - SA creds
  TT_ACCOUNT_NUMBERS           - "acct:label,..." (e.g. "5WT09219:tt-individual,5WT20360:tt-ira")
  TT_ACCOUNT_NUMBER            - fallback: single TT account number
  CS_TT_CLOSE_TAB              - tab name (default "CS_TT_Close")
  CS_GSHEET_STRICT             - "1" to fail hard
"""

import os
import re
import sys
import time
import random
from datetime import date, timedelta, datetime, timezone


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
    from lib.parsing import safe_float
except Exception as e:
    sheets_client = None
    safe_float = None
    _IMPORT_ERR = e

try:
    from tt_client import request as tt_request
except Exception as e:
    tt_request = None
    _TT_ERR = e

_SW_ERR = None
try:
    # schwab_token_keeper lives in scripts/ (already on sys.path)
    from schwab_token_keeper import schwab_client as _schwab_client_fn
except Exception as e:
    _schwab_client_fn = None
    _SW_ERR = e


TAG = "CS_TT_CLOSE"
CLOSE_TAB = "CS_TT_Close"
CLOSE_HEADERS = ["expiry", "account", "status", "close_net"]

TT_ACCOUNT_LABELS = {
    "5WT09219": "tt-individual",
    "5WT20360": "tt-ira",
}


def _derive_label(acct_num: str) -> str:
    return TT_ACCOUNT_LABELS.get(acct_num, f"tt-{acct_num}")


def _parse_tt_accounts() -> list:
    """Parse TT_ACCOUNT_NUMBERS or fall back to TT_ACCOUNT_NUMBER.

    Returns list of (acct_num, label) tuples.
    """
    raw = (os.environ.get("TT_ACCOUNT_NUMBERS") or "").strip()
    if not raw:
        single = (os.environ.get("TT_ACCOUNT_NUMBER") or "").strip()
        if not single:
            return []
        return [(single, _derive_label(single))]

    entries = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        if ":" in part:
            num, label = part.split(":", 1)
            entries.append((num.strip(), label.strip()))
        else:
            entries.append((part, _derive_label(part)))
    return entries


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
        params={"start-date": start, "per-page": "100"},
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
# Schwab close detection
# ---------------------------------------------------------------------------

def fetch_schwab_close_fills(days_back: int = 7) -> dict:
    """Fetch filled close orders from Schwab. Returns {expiry_iso: close_net}."""
    if _schwab_client_fn is None:
        return {}

    c = _schwab_client_fn()
    r = c.get_account_numbers()
    r.raise_for_status()
    acct_hash = r.json()[0]["hashValue"]

    now = datetime.now(timezone.utc)
    from_time = now - timedelta(days=days_back)

    url = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders"
    params = {
        "fromEnteredTime": from_time.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "toEnteredTime": now.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
    }

    resp = c.session.get(url, params=params, timeout=20)
    if resp.status_code != 200:
        log(f"WARN — schwab orders HTTP {resp.status_code}")
        return {}

    orders = resp.json() or []
    result = {}

    for order in orders:
        status = str(order.get("status") or "").upper()
        if status != "FILLED":
            continue

        legs = order.get("orderLegCollection") or []
        is_close = any(
            str(leg.get("instruction") or "").upper() in
            ("SELL_TO_CLOSE", "BUY_TO_CLOSE")
            for leg in legs
        )
        if not is_close:
            continue

        price = safe_float(str(order.get("price") or "0")) if safe_float else None
        if price is None:
            continue

        # Net credit/debit from order strategy type
        strategy = str(order.get("orderStrategyType") or "").upper()
        order_type = str(order.get("orderType") or "").upper()
        price_str = str(order.get("price") or "0")
        # Schwab: for complex orders, check complexOrderStrategyType
        complex_type = str(order.get("complexOrderStrategyType") or "").upper()

        # Determine sign: check if this is a credit or debit order
        # For vertical spreads, NET_CREDIT means we received credit
        if "CREDIT" in order_type or "CREDIT" in complex_type:
            close_net = price
        else:
            close_net = -price

        # Extract expiry from leg symbols
        expiry_iso = None
        for leg in legs:
            sym = (leg.get("instrument", {}).get("symbol") or "").strip()
            if sym:
                exp = _osi_expiry_iso(sym)
                if exp:
                    expiry_iso = exp
                    break

        if expiry_iso and expiry_iso not in result:
            result[expiry_iso] = close_net
            log(f"schwab closed {expiry_iso}: net={close_net:+.2f}")

    return result


# ---------------------------------------------------------------------------
# Migration
# ---------------------------------------------------------------------------

OLD_HEADERS = ["expiry", "status", "close_net"]


def _migrate_old_format(svc, sid, close_tab, existing):
    """Migrate 3-column format to 4-column format with account column."""
    log("migrating CS_TT_Close: adding account column")
    last_col = col_letter(len(CLOSE_HEADERS) - 1)
    migrated = [CLOSE_HEADERS]
    for row in existing[1:]:
        if len(row) >= 3:
            migrated.append([row[0], "tt-individual", row[1], row[2]])

    svc.spreadsheets().values().clear(
        spreadsheetId=sid, range=close_tab
    ).execute()
    svc.spreadsheets().values().update(
        spreadsheetId=sid,
        range=f"{close_tab}!A1:{last_col}{len(migrated)}",
        valueInputOption="RAW",
        body={"values": migrated},
    ).execute()
    return migrated


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

    tt_accounts = _parse_tt_accounts()
    has_tt = tt_request is not None and len(tt_accounts) > 0
    has_schwab = _schwab_client_fn is not None

    if not has_tt and not has_schwab:
        return skip("no TT accounts and no Schwab client available")

    close_tab = (os.environ.get("CS_TT_CLOSE_TAB") or CLOSE_TAB).strip()

    try:
        # --- Fetch close fills from all accounts ---
        all_filled = {}  # {(expiry, account): close_net}

        # TT accounts
        if has_tt:
            for acct_num, label in tt_accounts:
                try:
                    filled = fetch_filled_close_orders(acct_num)
                    for expiry, close_net in filled.items():
                        all_filled[(expiry, label)] = close_net
                    log(f"{label} ({acct_num}): {len(filled)} close(s)")
                except Exception as e:
                    log(f"WARN — {label} close fetch: {e}")

        # Schwab
        if has_schwab:
            try:
                sw_filled = fetch_schwab_close_fills()
                for expiry, close_net in sw_filled.items():
                    all_filled[(expiry, "schwab")] = close_net
                log(f"schwab: {len(sw_filled)} close(s)")
            except Exception as e:
                log(f"WARN — schwab close fetch: {e}")

        log(f"total: {len(all_filled)} filled close order(s) across all accounts")

        if not all_filled:
            return skip("no filled close orders across any account")

        # --- Write to sheet ---
        svc, sid = sheets_client()
        ensure_sheet_tab(svc, sid, close_tab)

        existing = get_values(svc, sid, f"{close_tab}!A1:D")
        last_col = col_letter(len(CLOSE_HEADERS) - 1)

        # Migrate old 3-column format if needed
        if existing and existing[0] == OLD_HEADERS:
            existing = _migrate_old_format(svc, sid, close_tab, existing)

        # Ensure header
        if not existing or existing[0] != CLOSE_HEADERS:
            svc.spreadsheets().values().update(
                spreadsheetId=sid,
                range=f"{close_tab}!A1:{last_col}1",
                valueInputOption="RAW",
                body={"values": [CLOSE_HEADERS]},
            ).execute()
            existing = [CLOSE_HEADERS] + (existing[1:] if existing else [])

        # Build existing map keyed by (expiry, account)
        existing_keys = {}
        for rnum, row in enumerate(existing[1:], start=2):
            if len(row) >= 2:
                key = (row[0].strip(), row[1].strip())
                existing_keys[key] = rnum

        # Upsert rows
        updates = []
        appends = []
        for (expiry, account), close_net in all_filled.items():
            values = [expiry, account, "closed", str(round(close_net, 4))]
            key = (expiry, account)
            if key in existing_keys:
                rnum = existing_keys[key]
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
