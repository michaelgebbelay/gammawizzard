#!/usr/bin/env python3
"""
Recover CS_Tracking rows from broker APIs when CSV-based tracking is missing.

Pulls filled OPEN orders from all 3 accounts (Schwab, TT IRA, TT Individual),
joins with GW_Signal for signal metadata, and upserts to CS_Tracking.

Smart merge: only fills gaps — does not overwrite existing CSV-sourced data.

Env:
  GSHEET_ID                    - spreadsheet ID
  GOOGLE_SERVICE_ACCOUNT_JSON  - SA creds
  CS_TRACKING_TAB              - tracking tab (default "CS_Tracking")
  CS_GW_SIGNAL_TAB             - GW signal tab (default "GW_Signal")
  CS_API_DAYS_BACK             - days to look back (default 7)
  CS_GSHEET_STRICT             - "1" to fail hard
  TT_ACCOUNT_NUMBERS           - "acct:label,..." or TT_ACCOUNT_NUMBER
  SCHWAB_APP_KEY / SCHWAB_APP_SECRET / SCHWAB_TOKEN_PATH
"""

import os
import re
import sys
import time
import random
from datetime import date, timedelta, datetime, timezone
from zoneinfo import ZoneInfo


# --- path setup ---
def _add_paths():
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
_SW_ERR = None

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

try:
    from schwab_token_keeper import schwab_client as _schwab_client_fn
except Exception as e:
    _schwab_client_fn = None
    _SW_ERR = e


TAG = "CS_API_TRACK"
ET = ZoneInfo("America/New_York")

TRACKING_HEADER = [
    "expiry", "account",
    "put_go", "call_go", "put_strikes", "call_strikes",
    "gw_put_price", "gw_call_price",
    "put_spread_price", "call_spread_price",
    "vix_value", "vol_bucket", "vix_mult", "units",
    "put_side", "put_target", "call_side", "call_target",
    "put_filled", "put_fill_price", "put_status",
    "call_filled", "call_fill_price", "call_status",
    "put_improvement", "call_improvement",
    "cost_per_contract", "put_cost", "call_cost", "total_cost",
]

UPSERT_KEYS = ["expiry", "account"]

COST_PER_CONTRACT = {
    "schwab": 0.97,
    "tt-ira": 1.72,
    "tt-individual": 1.72,
}

TT_ACCOUNT_LABELS = {
    "5WT09219": "tt-individual",
    "5WT20360": "tt-ira",
}


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


def _derive_label(acct_num: str) -> str:
    return TT_ACCOUNT_LABELS.get(acct_num, f"tt-{acct_num}")


def _parse_tt_accounts() -> list:
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
# OSI symbol parsing
# ---------------------------------------------------------------------------

def _osi_expiry_iso(sym: str):
    """Extract expiry date from an option symbol (OSI or TT format)."""
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


def _osi_put_call(sym: str):
    """Extract P or C from option symbol."""
    s = re.sub(r"\s+", "", (sym or "").strip().upper())
    m = re.search(r"\d{6}([CP])\d+", s)
    return m.group(1) if m else None


def _osi_strike(sym: str) -> str:
    """Extract strike from OSI symbol: last 8 digits / 1000."""
    s = (sym or "").strip()
    if len(s) < 8:
        return ""
    digits = s[-8:]
    if not digits.isdigit():
        # TT format: shorter, e.g. 'SPXW 260213P6900'
        m = re.search(r"[CP](\d+)$", re.sub(r"\s+", "", s))
        if m:
            return m.group(1)
        return ""
    return str(int(digits) // 1000)


def _strikes_pair(short_sym: str, long_sym: str) -> str:
    """Build 'low/high' strike string from two leg symbols."""
    s1 = _osi_strike(short_sym)
    s2 = _osi_strike(long_sym)
    if s1 and s2:
        lo, hi = sorted([int(s1), int(s2)])
        return f"{lo}/{hi}"
    return s1 or s2 or ""


# ---------------------------------------------------------------------------
# TT API helpers
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


def fetch_tt_open_fills(acct_num: str, label: str, days_back: int) -> list:
    """Fetch filled OPEN orders from TT. Returns list of tracking-row dicts."""
    start = (date.today() - timedelta(days=days_back)).isoformat()
    j = _tt_get_json(
        f"/accounts/{acct_num}/orders",
        params={"start-date": start, "per-page": "100"},
        tag=f"ORDERS_{label}",
    )
    data = j.get("data") if isinstance(j, dict) else {}
    items = data.get("items") or []

    # Group fills by expiry to combine PUT and CALL legs
    by_expiry = {}

    for order in items:
        status = str(order.get("status") or "").lower()
        if status != "filled":
            continue

        legs = order.get("legs") or []
        is_open = any("Open" in str(leg.get("action") or "") for leg in legs)
        if not is_open:
            continue

        # Only SPXW options
        is_spxw = any("SPXW" in str(leg.get("symbol") or "").upper()
                       for leg in legs)
        if not is_spxw:
            continue

        # Order-level price and effect
        price = safe_float(str(order.get("price") or "0"))
        price_effect = str(order.get("price-effect") or "").strip().lower()
        side = "CREDIT" if price_effect == "credit" else "DEBIT"

        # Determine total filled qty (order level)
        order_size = int(float(order.get("size", 0) or 0))
        remaining = int(float(order.get("remaining-quantity", 0) or 0))
        filled_qty = order_size - remaining if order_size > remaining else order_size

        # Parse legs to determine put/call and extract symbols
        for leg in legs:
            sym = (leg.get("symbol") or "").strip()
            action = (leg.get("action") or "").strip()
            if "Open" not in action:
                continue

            pc = _osi_put_call(sym)
            expiry = _osi_expiry_iso(sym)
            if not pc or not expiry:
                continue

            if expiry not in by_expiry:
                by_expiry[expiry] = {
                    "expiry": expiry,
                    "side": side,
                }

            entry = by_expiry[expiry]
            leg_qty = int(float(leg.get("quantity", 0) or 0))

            if pc == "P":
                entry.setdefault("put_syms", []).append(sym)
                entry["put_filled"] = max(entry.get("put_filled", 0), leg_qty or filled_qty)
                entry["put_side"] = side
            else:
                entry.setdefault("call_syms", []).append(sym)
                entry["call_filled"] = max(entry.get("call_filled", 0), leg_qty or filled_qty)
                entry["call_side"] = side

        # Assign order-level fill price to the correct leg (PUT or CALL)
        # TT typically places each vertical spread as a separate order,
        # but manual IC orders may combine all 4 legs in one order.
        has_put = any(_osi_put_call(leg.get("symbol", "")) == "P"
                      for leg in legs if "Open" in str(leg.get("action", "")))
        has_call = any(_osi_put_call(leg.get("symbol", "")) == "C"
                       for leg in legs if "Open" in str(leg.get("action", "")))
        for leg in legs:
            sym = (leg.get("symbol") or "").strip()
            expiry = _osi_expiry_iso(sym)
            if expiry and expiry in by_expiry and price is not None:
                if has_put and not has_call:
                    by_expiry[expiry]["put_fill_price"] = str(price)
                elif has_call and not has_put:
                    by_expiry[expiry]["call_fill_price"] = str(price)
                elif has_put and has_call:
                    # 4-leg IC order: split price evenly between sides
                    half = round(price / 2, 2)
                    by_expiry[expiry].setdefault("put_fill_price", str(half))
                    by_expiry[expiry].setdefault("call_fill_price", str(half))
                break

    # Build tracking rows
    cost = COST_PER_CONTRACT.get(label, 1.72)
    rows = []
    for expiry, entry in by_expiry.items():
        put_syms = entry.get("put_syms", [])
        call_syms = entry.get("call_syms", [])
        put_filled = entry.get("put_filled", 0)
        call_filled = entry.get("call_filled", 0)

        # Build strikes from symbols
        put_strikes = ""
        call_strikes = ""
        if len(put_syms) >= 2:
            put_strikes = _strikes_pair(put_syms[0], put_syms[1])
        if len(call_syms) >= 2:
            call_strikes = _strikes_pair(call_syms[0], call_syms[1])

        put_cost = f"{cost * put_filled * 2:.2f}" if put_filled else "0.00"
        call_cost = f"{cost * call_filled * 2:.2f}" if call_filled else "0.00"
        total_cost = f"{float(put_cost) + float(call_cost):.2f}"

        rows.append({
            "expiry": expiry,
            "account": label,
            "put_strikes": put_strikes,
            "call_strikes": call_strikes,
            "put_side": entry.get("put_side", ""),
            "call_side": entry.get("call_side", ""),
            "put_filled": str(put_filled) if put_filled else "",
            "put_fill_price": entry.get("put_fill_price", ""),
            "put_status": "filled" if put_filled else "",
            "call_filled": str(call_filled) if call_filled else "",
            "call_fill_price": entry.get("call_fill_price", ""),
            "call_status": "filled" if call_filled else "",
            "cost_per_contract": f"{cost:.2f}",
            "put_cost": put_cost,
            "call_cost": call_cost,
            "total_cost": total_cost,
        })

    return rows


# ---------------------------------------------------------------------------
# Schwab API helpers
# ---------------------------------------------------------------------------

def fetch_schwab_open_fills(days_back: int) -> list:
    """Fetch filled OPEN orders from Schwab. Returns list of tracking-row dicts."""
    if _schwab_client_fn is None:
        return []

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
        return []

    orders = resp.json() or []
    by_expiry = {}

    for order in orders:
        status = str(order.get("status") or "").upper()
        if status != "FILLED":
            continue

        legs = order.get("orderLegCollection") or []
        is_open = any(
            str(leg.get("instruction") or "").upper() in ("BUY_TO_OPEN", "SELL_TO_OPEN")
            for leg in legs
        )
        if not is_open:
            continue

        # Only SPXW options
        is_spxw = any(
            "SPXW" in str(leg.get("instrument", {}).get("symbol") or "").upper()
            for leg in legs
        )
        if not is_spxw:
            continue

        # Price info
        price = safe_float(order.get("price"))
        order_type = str(order.get("orderType") or "").upper()
        side = "CREDIT" if "CREDIT" in order_type else "DEBIT"

        # Filled qty from order
        filled_qty = int(float(order.get("filledQuantity", 0) or 0))
        if filled_qty == 0:
            filled_qty = int(float(order.get("quantity", 0) or 0))

        # Parse legs
        for leg in legs:
            sym = (leg.get("instrument", {}).get("symbol") or "").strip()
            instruction = str(leg.get("instruction") or "").upper()
            if "OPEN" not in instruction:
                continue

            pc = _osi_put_call(sym)
            expiry = _osi_expiry_iso(sym)
            if not pc or not expiry:
                continue

            if expiry not in by_expiry:
                by_expiry[expiry] = {
                    "expiry": expiry,
                    "side": side,
                }

            entry = by_expiry[expiry]
            leg_qty = int(float(leg.get("quantity", 0) or 0))

            if pc == "P":
                entry.setdefault("put_syms", []).append(sym)
                entry["put_filled"] = max(entry.get("put_filled", 0), leg_qty or filled_qty)
                entry["put_side"] = side
            else:
                entry.setdefault("call_syms", []).append(sym)
                entry["call_filled"] = max(entry.get("call_filled", 0), leg_qty or filled_qty)
                entry["call_side"] = side

        # Assign order-level fill price to the correct leg (PUT or CALL)
        has_put = any(_osi_put_call(leg.get("instrument", {}).get("symbol", "")) == "P"
                      for leg in legs if "OPEN" in str(leg.get("instruction", "")).upper())
        has_call = any(_osi_put_call(leg.get("instrument", {}).get("symbol", "")) == "C"
                       for leg in legs if "OPEN" in str(leg.get("instruction", "")).upper())
        for leg in legs:
            sym = (leg.get("instrument", {}).get("symbol") or "").strip()
            expiry = _osi_expiry_iso(sym)
            if expiry and expiry in by_expiry and price is not None:
                if has_put and not has_call:
                    by_expiry[expiry]["put_fill_price"] = str(price)
                elif has_call and not has_put:
                    by_expiry[expiry]["call_fill_price"] = str(price)
                elif has_put and has_call:
                    half = round(price / 2, 2)
                    by_expiry[expiry].setdefault("put_fill_price", str(half))
                    by_expiry[expiry].setdefault("call_fill_price", str(half))
                break

    # Build tracking rows
    cost = COST_PER_CONTRACT.get("schwab", 0.97)
    rows = []
    for expiry, entry in by_expiry.items():
        put_syms = entry.get("put_syms", [])
        call_syms = entry.get("call_syms", [])
        put_filled = entry.get("put_filled", 0)
        call_filled = entry.get("call_filled", 0)

        put_strikes = ""
        call_strikes = ""
        if len(put_syms) >= 2:
            put_strikes = _strikes_pair(put_syms[0], put_syms[1])
        if len(call_syms) >= 2:
            call_strikes = _strikes_pair(call_syms[0], call_syms[1])

        put_cost = f"{cost * put_filled * 2:.2f}" if put_filled else "0.00"
        call_cost = f"{cost * call_filled * 2:.2f}" if call_filled else "0.00"
        total_cost = f"{float(put_cost) + float(call_cost):.2f}"

        rows.append({
            "expiry": expiry,
            "account": "schwab",
            "put_strikes": put_strikes,
            "call_strikes": call_strikes,
            "put_side": entry.get("put_side", ""),
            "call_side": entry.get("call_side", ""),
            "put_filled": str(put_filled) if put_filled else "",
            "put_fill_price": entry.get("put_fill_price", ""),
            "put_status": "filled" if put_filled else "",
            "call_filled": str(call_filled) if call_filled else "",
            "call_fill_price": entry.get("call_fill_price", ""),
            "call_status": "filled" if call_filled else "",
            "cost_per_contract": f"{cost:.2f}",
            "put_cost": put_cost,
            "call_cost": call_cost,
            "total_cost": total_cost,
        })

    return rows


# ---------------------------------------------------------------------------
# Timestamp parsing
# ---------------------------------------------------------------------------

def _parse_iso_to_et_date(ts_str) -> str:
    """Parse a timestamp and return YYYY-MM-DD in Eastern Time.

    Handles ISO-8601 strings, Unix epoch (seconds or milliseconds), and date strings.
    """
    if not ts_str:
        return ""
    ts_str = str(ts_str).strip()

    # Try Unix epoch (integer or float — milliseconds if > 10 billion)
    try:
        num = float(ts_str)
        if num > 1e12:
            num /= 1000.0  # milliseconds to seconds
        dt = datetime.fromtimestamp(num, tz=timezone.utc)
        return dt.astimezone(ET).strftime("%Y-%m-%d")
    except (ValueError, OSError):
        pass

    # Try ISO-8601
    try:
        ts = ts_str.replace("Z", "+00:00")
        if re.search(r"[+-]\d{4}$", ts):
            ts = ts[:-5] + ts[-5:-2] + ":" + ts[-2:]
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(ET).strftime("%Y-%m-%d")
    except Exception:
        pass

    # Try just extracting the date part
    m = re.match(r"(\d{4}-\d{2}-\d{2})", ts_str)
    return m.group(1) if m else ""


# ---------------------------------------------------------------------------
# GW Signal reader
# ---------------------------------------------------------------------------

def read_gw_signal(svc, spreadsheet_id: str, tab: str) -> dict:
    """Read GW_Signal tab. Returns {date: {put_price, call_price, left_go, right_go, vix, ...}}."""
    try:
        all_rows = get_values(svc, spreadsheet_id, f"{tab}!A1:ZZ")
        if len(all_rows) < 2:
            return {}
        header = all_rows[0]
        result = {}
        for vals in all_rows[1:]:
            d = {header[i]: (vals[i] if i < len(vals) else "") for i in range(len(header))}
            dt = (d.get("date") or "").strip()
            if dt:
                result[dt] = d
        log(f"read {len(result)} GW signal rows")
        return result
    except Exception as e:
        log(f"WARN — could not read GW_Signal: {e}")
        return {}


# ---------------------------------------------------------------------------
# Existing tracking reader
# ---------------------------------------------------------------------------

def read_existing_keys(svc, sid, tab) -> set:
    """Read CS_Tracking and return set of (expiry, account) keys with COMPLETE fill data.

    A row is "complete" only if it has fill qty AND fill price for at least one side.
    Rows with fills but no prices are treated as gaps so the API can re-fetch them.
    """
    existing = get_values(svc, sid, f"{tab}!A1:ZZ")
    if len(existing) < 2:
        return set()
    header = existing[0]
    keys = set()
    for row in existing[1:]:
        d = {header[i]: (row[i] if i < len(row) else "") for i in range(len(header))}
        expiry = (d.get("expiry") or "").strip()
        account = (d.get("account") or "").strip()
        put_filled = (d.get("put_filled") or "").strip()
        call_filled = (d.get("call_filled") or "").strip()
        put_price = (d.get("put_fill_price") or "").strip()
        call_price = (d.get("call_fill_price") or "").strip()
        # Complete = has fill qty AND fill price for at least one side
        has_put = put_filled and put_price
        has_call = call_filled and call_price
        if expiry and account and (has_put or has_call):
            keys.add((expiry, account))
    return keys


# ---------------------------------------------------------------------------
# Enrichment from GW Signal
# ---------------------------------------------------------------------------

def enrich_from_gw_signal(rows: list, gw_signal: dict) -> list:
    """Fill in GW signal metadata for each tracking row."""
    for row in rows:
        # GW_Signal is keyed by date; the signal for expiry E is sent on E-1,
        # but the GW_Signal tab stores it by the signal date, not expiry.
        # Try both the expiry date and the day before as lookup keys.
        expiry = row.get("expiry", "")
        gw = gw_signal.get(expiry, {})
        if not gw and expiry:
            try:
                exp_date = date.fromisoformat(expiry)
                prev_date = (exp_date - timedelta(days=1)).isoformat()
                gw = gw_signal.get(prev_date, {})
            except ValueError:
                pass
        if not gw:
            continue
        # Signal prices
        row.setdefault("gw_put_price", (gw.get("put_price") or "").strip())
        row.setdefault("gw_call_price", (gw.get("call_price") or "").strip())
        # Go values
        row.setdefault("put_go", (gw.get("left_go") or "").strip())
        row.setdefault("call_go", (gw.get("right_go") or "").strip())
        # VIX
        vix = (gw.get("vix_one") or gw.get("vix") or "").strip()
        row.setdefault("vix_value", vix)
    return rows


# ---------------------------------------------------------------------------
# Upsert (reuse pattern from cs_tracking_to_gsheet.py)
# ---------------------------------------------------------------------------

_DATE_PAT = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_ACCT_NAMES = {"schwab", "tt-ira", "tt-individual"}


def _is_old_format_row(row):
    """Check if a data row has the old format (date, expiry, account, ...).

    Old format: col A = trade date, col B = expiry date, col C = account name
    New format: col A = expiry date, col B = account name
    """
    if len(row) < 3:
        return False
    a, b, c = (row[0] or "").strip(), (row[1] or "").strip(), (row[2] or "").strip()
    return (_DATE_PAT.match(a) and _DATE_PAT.match(b)
            and c.lower() in _ACCT_NAMES)


def _migrate_tracking_data(svc, sid, tab, header):
    """Detect and migrate old-format CS_Tracking data (strip date column).

    Old format:  date | expiry | account | put_go | ...
    New format:  expiry | account | put_go | ...

    Returns migrated rows (including header) or None if no migration needed.
    """
    existing = get_values(svc, sid, f"{tab}!A1:ZZ")
    if len(existing) < 2:
        return None

    old_header = existing[0]

    # Detect old format: header starts with "date"
    needs_migration = (old_header[0] == "date" and len(old_header) > 1
                       and old_header[1] == "expiry")

    # Or: header was already updated but data rows still have old format
    if not needs_migration:
        for row in existing[1:]:
            if _is_old_format_row(row):
                needs_migration = True
                break

    if not needs_migration:
        return None

    log("migrating CS_Tracking: stripping old 'date' column")
    last_col = col_letter(len(header) - 1)

    # Build migrated data: header + data rows with date column removed
    migrated = [header]
    seen_keys = {}  # (expiry, account) -> row index for dedup
    for row in existing[1:]:
        if _is_old_format_row(row):
            new_row = row[1:]  # strip old date column
        else:
            new_row = row

        # Pad or trim to match header length
        new_row = list(new_row[:len(header)])
        while len(new_row) < len(header):
            new_row.append("")

        key = (new_row[0].strip(), new_row[1].strip())
        if key in seen_keys:
            # Keep the row with more non-empty fields (better data wins)
            old_idx = seen_keys[key]
            old_row = migrated[old_idx]
            old_filled = sum(1 for v in old_row if (v or "").strip()
                             and (v or "").strip() != "0")
            new_filled = sum(1 for v in new_row if (v or "").strip()
                             and (v or "").strip() != "0")
            if new_filled > old_filled:
                migrated[old_idx] = new_row
        else:
            seen_keys[key] = len(migrated)
            migrated.append(new_row)

    # Clear entire sheet and rewrite
    old_last_col = col_letter(max(len(header), len(old_header)) + 1)
    svc.spreadsheets().values().clear(
        spreadsheetId=sid,
        range=f"{tab}!A1:{old_last_col}5000",
    ).execute()

    # Write migrated data
    svc.spreadsheets().values().update(
        spreadsheetId=sid,
        range=f"{tab}!A1:{last_col}{len(migrated)}",
        valueInputOption="RAW",
        body={"values": migrated},
    ).execute()

    log(f"migration complete: {len(migrated) - 1} rows (deduped from {len(existing) - 1})")
    return migrated


def upsert_rows(svc, sid, tab, rows, header):
    existing = get_values(svc, sid, f"{tab}!A1:ZZ")
    last_col = col_letter(len(header) - 1)

    if not existing:
        svc.spreadsheets().values().update(
            spreadsheetId=sid,
            range=f"{tab}!A1:{last_col}1",
            valueInputOption="RAW",
            body={"values": [header]},
        ).execute()
        existing = [header]
    elif existing[0] != header:
        svc.spreadsheets().values().update(
            spreadsheetId=sid,
            range=f"{tab}!A1:{last_col}1",
            valueInputOption="RAW",
            body={"values": [header]},
        ).execute()
        existing = [header] + existing[1:]

    def key_from_dict(d):
        return tuple(str(d.get(k, "")) for k in UPSERT_KEYS)

    existing_map = {}    # key -> row number
    existing_data = {}   # key -> existing row values list
    for rnum, row in enumerate(existing[1:], start=2):
        d = {header[i]: (row[i] if i < len(row) else "") for i in range(len(header))}
        k = key_from_dict(d)
        existing_map[k] = rnum
        existing_data[k] = row

    # De-dupe by key (keep last)
    last_by_key = {}
    for d in rows:
        last_by_key[key_from_dict(d)] = d
    rows = list(last_by_key.values())

    updates = []
    appends = []
    for d in rows:
        key = key_from_dict(d)
        values = [str(d.get(h, "")) for h in header]
        if key in existing_map:
            rnum = existing_map[key]
            rng = f"{tab}!A{rnum}:{last_col}{rnum}"
            # Merge: keep existing non-empty values, fill in blanks from API
            old_row = existing_data[key]
            merged = []
            for i, h in enumerate(header):
                new_val = values[i]
                old_val = (old_row[i] if i < len(old_row) else "").strip()
                # Keep old value if it's non-empty; use new if old is empty
                merged.append(old_val if old_val else new_val)
            updates.append((rng, merged))
        else:
            appends.append(values)

    if updates:
        data = [{"range": rng, "values": [vals]} for (rng, vals) in updates]
        svc.spreadsheets().values().batchUpdate(
            spreadsheetId=sid,
            body={"valueInputOption": "RAW", "data": data},
        ).execute()

    if appends:
        svc.spreadsheets().values().append(
            spreadsheetId=sid,
            range=f"{tab}!A1",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": appends},
        ).execute()

    return {"updated": len(updates), "appended": len(appends)}


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

    tracking_tab = (os.environ.get("CS_TRACKING_TAB") or "CS_Tracking").strip()
    gw_signal_tab = (os.environ.get("CS_GW_SIGNAL_TAB") or "GW_Signal").strip()
    days_back = int(os.environ.get("CS_API_DAYS_BACK", "14"))

    tt_accounts = _parse_tt_accounts()
    has_tt = tt_request is not None and len(tt_accounts) > 0
    has_schwab = _schwab_client_fn is not None

    if not has_tt and not has_schwab:
        return skip("no TT accounts and no Schwab client available")

    try:
        svc, sid = sheets_client()
        ensure_sheet_tab(svc, sid, tracking_tab)

        # Migrate old-format data (one-time: strips date column, dedupes)
        _migrate_tracking_data(svc, sid, tracking_tab, TRACKING_HEADER)

        # Read existing tracking to find gaps
        filled_keys = read_existing_keys(svc, sid, tracking_tab)
        log(f"existing tracking has {len(filled_keys)} filled entries")

        # Read GW signal for enrichment
        gw_signal = read_gw_signal(svc, sid, gw_signal_tab)

        # Fetch from all accounts
        all_rows = []

        # TT accounts
        if has_tt:
            for acct_num, label in tt_accounts:
                try:
                    rows = fetch_tt_open_fills(acct_num, label, days_back)
                    all_rows.extend(rows)
                    log(f"{label}: {len(rows)} filled open order(s)")
                except Exception as e:
                    log(f"WARN — {label}: {e}")

        # Schwab
        if has_schwab:
            try:
                rows = fetch_schwab_open_fills(days_back)
                all_rows.extend(rows)
                log(f"schwab: {len(rows)} filled open order(s)")
            except Exception as e:
                log(f"WARN — schwab: {e}")

        if not all_rows:
            return skip("no filled open orders from APIs")

        # Enrich with GW signal
        all_rows = enrich_from_gw_signal(all_rows, gw_signal)

        # Filter: only rows that are MISSING from existing tracking
        new_rows = []
        for row in all_rows:
            key = (row.get("expiry", ""), row.get("account", ""))
            if key not in filled_keys:
                new_rows.append(row)

        if not new_rows:
            log(f"all {len(all_rows)} API rows already in tracking — no gaps")
            return 0

        log(f"filling {len(new_rows)} gap(s) in {tracking_tab}")

        ensure_sheet_tab(svc, sid, tracking_tab)
        res = upsert_rows(svc, sid, tracking_tab, new_rows, TRACKING_HEADER)
        log(f"appended={res['appended']} updated={res['updated']}")
        return 0

    except Exception as e:
        msg = f"API tracking failed: {type(e).__name__}: {e}"
        return fail(msg, 2) if strict else skip(msg)


if __name__ == "__main__":
    sys.exit(main())
