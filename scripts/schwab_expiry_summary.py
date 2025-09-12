#!/usr/bin/env python3
# Schwab → Google Sheets: TRADE transactions grouped by option EXPIRATION DATE.
# - Only uses TOS/Schwab data (no estimates). Uses transaction["netAmount"] (fees included).
# - Calls Schwab transactions endpoint with ONLY start/end; we filter locally (avoids 400 "Unexpected parameter").
# - Produces two tabs:
#     sw_txn_raw         : one row per Schwab transaction (TRADE)
#     sw_expiry_summary  : totals by option expiration (YYYY-MM-DD)
#
# Env:
#   GSHEET_ID, GOOGLE_SERVICE_ACCOUNT_JSON
#   SCHWAB_APP_KEY, SCHWAB_APP_SECRET, SCHWAB_TOKEN_JSON
#   DAYS_BACK (e.g. "14"), SYMBOL_FILTER (e.g. "SPX" or "")

import os, sys, json, base64, math, time
from datetime import datetime, timedelta, timezone, date
from typing import Any, Dict, List, Tuple, Optional

from schwab.auth import client_from_token_file
from schwab.client import Client

from google.oauth2 import service_account
from googleapiclient.discovery import build as gbuild

UTC = timezone.utc

# ------------- Google Sheets helpers -------------
RAW_TAB = "sw_txn_raw"
SUM_TAB = "sw_expiry_summary"

RAW_HEADERS = [
    "ts_trade_utc","order_id","net_amount","commissions","fees_other",
    "legs_count","exp_ymd","underlyings","leg_instructions","account_id"
]
SUM_HEADERS = [
    "exp_ymd","txn_count","total_net_amount","total_commissions","total_fees_other",
    "underlyings_set","notes"
]

def sheets_client() -> Tuple[Any, str]:
    sheet_id = os.environ["GSHEET_ID"]
    sa_json = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
    creds = service_account.Credentials.from_service_account_info(
        json.loads(sa_json),
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    svc = gbuild("sheets", "v4", credentials=creds)
    return svc, sheet_id

def write_tab_overwrite(svc, spreadsheet_id: str, tab_name: str, header: List[str], rows: List[List[Any]]):
    # Write entire tab in one request. If the sheet doesn't exist, create it.
    # Then write header+rows starting at A1.
    try:
        meta = svc.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        titles = [s["properties"]["title"] for s in meta.get("sheets",[])]
        if tab_name not in titles:
            svc.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests":[{"addSheet":{"properties":{"title":tab_name}}}]}
            ).execute()
    except Exception as e:
        print(f"WARNING: Sheets get/add failed: {e}")

    body = {"values": [header] + rows}
    svc.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"{tab_name}!A1",
        valueInputOption="USER_ENTERED",
        body=body
    ).execute()

# ------------- Schwab client helpers -------------
def _token_to_file_from_env() -> str:
    token_env = os.environ.get("SCHWAB_TOKEN_JSON","") or ""
    path = "schwab_token.json"
    if token_env:
        # allow base64-encoded or raw JSON
        try:
            dec = base64.b64decode(token_env).decode("utf-8")
            if dec.strip().startswith("{"):
                token_env = dec
        except Exception:
            pass
        with open(path, "w") as f:
            f.write(token_env)
    return path

def schwab_client() -> Tuple[Client, str]:
    token_path = _token_to_file_from_env()
    c = client_from_token_file(
        api_key=os.environ["SCHWAB_APP_KEY"],
        api_secret=os.environ["SCHWAB_APP_SECRET"],
        token_path=token_path
    )
    r = c.get_account_numbers(); r.raise_for_status()
    acct_hash = r.json()[0]["hashValue"]
    return c, acct_hash

# ------------- Transactions fetch -------------
def clamp_days_back(n: int) -> int:
    # Schwab "transactions" allows up to 60-day windows. Keep simple: cap to 60.
    return max(1, min(60, n))

def fetch_trades(c: Client, acct_hash: str, days_back: int) -> List[Dict[str,Any]]:
    end_dt = datetime.now(UTC)
    start_dt = end_dt - timedelta(days=clamp_days_back(days_back))

    # IMPORTANT: only pass start/end. No symbol, no types filter => avoids "Unexpected parameter".
    # We will filter to type == TRADE locally.
    r = c.get_transactions(
        acct_hash,
        start_date=start_dt,
        end_date=end_dt
        # DO NOT pass transaction_types or symbol; many accounts get HTTP_400 with those.
    )
    r.raise_for_status()
    data = r.json()
    # Schwab can return either list or object with "transactions" key depending on endpoint;
    # be robust:
    if isinstance(data, dict) and "transactions" in data:
        arr = data["transactions"]
    elif isinstance(data, list):
        arr = data
    else:
        arr = []
    # Keep only TRADE
    out = [x for x in arr if str(x.get("type","")).upper() == "TRADE"]
    return out

# ------------- Option helpers -------------
def _first(items: List[Any]) -> Any:
    return items[0] if items else None

def to_date_ymd(d: Any) -> Optional[str]:
    if not d: return None
    s = str(d)
    try:
        # permit "2025-09-10T00:00:00+0000" or "2025-09-10"
        y, m, dd = s[:10].split("-")
        return f"{int(y):04d}-{int(m):02d}-{int(dd):02d}"
    except Exception:
        return None

def extract_items(tx: Dict[str,Any]) -> List[Dict[str,Any]]:
    # Schwab uses "transactionItems" or "transactionItem"
    items = tx.get("transactionItems")
    if isinstance(items, list): return items
    it = tx.get("transactionItem")
    return it if isinstance(it, list) else ([] if it is None else [it])

def is_option_item(item: Dict[str,Any]) -> bool:
    ins = item.get("instrument") or {}
    at = (ins.get("assetType") or ins.get("type") or "").upper()
    return at == "OPTION"

def item_expiry_ymd(item: Dict[str,Any]) -> Optional[str]:
    ins = item.get("instrument") or {}
    # preferred field
    exp = ins.get("optionExpirationDate") or ins.get("expirationDate")
    ymd = to_date_ymd(exp)
    if ymd: return ymd
    # fallback parse from symbol if present, e.g. "SPXW  250910P06500000"
    sym = ins.get("symbol") or ""
    sym = "".join(sym.split())
    # try to locate YYMMDD after root (6..12 chars)
    try:
        # search for 6-digit date in the string
        import re
        m = re.search(r'(\d{6})[CP]\d{5,8}', sym)
        if m:
            s = m.group(1)
            y = 2000 + int(s[0:2])
            mth = int(s[2:4])
            dd = int(s[4:6])
            return f"{y:04d}-{mth:02d}-{dd:02d}"
    except Exception:
        pass
    return None

def item_underlying(item: Dict[str,Any]) -> str:
    ins = item.get("instrument") or {}
    # many returns have "underlyingSymbol" or we can infer from symbol prefix
    u = ins.get("underlyingSymbol")
    if u: return str(u)
    sym = (ins.get("symbol") or "").strip()
    if not sym: return ""
    # take leading letters until first digit as a rough root
    i = 0
    while i < len(sym) and not sym[i].isdigit():
        i += 1
    return sym[:i].strip()

def item_instruction(item: Dict[str,Any]) -> str:
    # BUY/SELL from item; fallback to quantity sign if available
    instr = (item.get("instruction") or "").upper()
    if instr: return instr
    qty = item.get("amount") or item.get("quantity")
    try:
        q = float(qty)
        return "SELL" if q < 0 else "BUY"
    except Exception:
        return ""

# ------------- Transform & aggregate -------------
def transform_raw_rows(trades: List[Dict[str,Any]], symbol_contains: str) -> Tuple[List[List[Any]], List[List[Any]]]:
    """
    Returns (raw_rows, summary_rows).
    - raw_rows: one per transaction
    - summary_rows: one per expiration date
    """
    raw_rows: List[List[Any]] = []
    by_exp: Dict[str, Dict[str,Any]] = {}

    want = (symbol_contains or "").strip().upper()

    for tx in trades:
        items = [i for i in extract_items(tx) if is_option_item(i)]
        if not items:
            # not options → skip (you can include equities if desired)
            continue

        # optional client-side symbol filter: if none of the legs contains want, skip
        if want:
            leg_syms = [ (i.get("instrument") or {}).get("symbol","") for i in items ]
            if not any(want in (s or "").upper() for s in leg_syms):
                continue

        # Expiry: require all option legs share the same date; otherwise label mixed
        exps = { item_expiry_ymd(i) for i in items }
        exps.discard(None)
        if len(exps) == 1:
            exp_ymd = _first(list(exps))
            mixed_flag = ""
        else:
            exp_ymd = "MIXED_OR_UNKNOWN"
            mixed_flag = "MIXED_EXPIRY"

        # Underlyings set (unique)
        unds = sorted({ item_underlying(i) for i in items if item_underlying(i) })
        unds_txt = ",".join(unds) if unds else ""

        # Instructions list for visibility (BUY/SELL PUT/CALL detection not strictly needed)
        leg_instr = []
        for i in items:
            ins = (i.get("instrument") or {})
            pc = (ins.get("putCall") or ins.get("type") or "")
            strike = ins.get("strikePrice") or ins.get("strike")
            instr = item_instruction(i)
            leg_instr.append(f"{instr} {pc} {strike}")

        # Monetary fields from Schwab (authoritative; includes fees)
        net = float(tx.get("netAmount") or 0.0)

        # fees come as list entries; we’ll split "commissions" vs "other" for readability
        fees_list = tx.get("fees") or []
        commissions = 0.0
        other_fees = 0.0
        for f in fees_list:
            amt = float(f.get("amount") or 0.0)
            code = (f.get("code") or "").lower()
            if "commission" in code:
                commissions += amt
            else:
                other_fees += amt

        # Misc IDs/timestamps
        order_id = tx.get("orderId") or tx.get("orderNumber") or tx.get("transactionId") or ""
        ts = tx.get("transactionDate") or tx.get("transactionDateTime") or tx.get("time") or tx.get("closeDate")
        ts = str(ts or "")
        # Try to normalize to ISO if possible
        ts_norm = ts
        try:
            # sample form: "2025-09-10T14:13:52+0000"
            ts_norm = datetime.fromisoformat(ts.replace("+0000","+00:00")).astimezone(UTC).isoformat()
        except Exception:
            pass

        # Build raw row
        raw_rows.append([
            ts_norm, order_id, net, commissions, other_fees,
            len(items), exp_ymd, unds_txt, "; ".join(leg_instr), str(tx.get("accountId") or tx.get("account") or "")
        ])

        # Aggregate by expiration date
        bucket = by_exp.setdefault(exp_ymd, {
            "txn_count": 0,
            "total_net": 0.0,
            "comm": 0.0,
            "fees": 0.0,
            "underlyings": set(),
            "notes": set()
        })
        bucket["txn_count"] += 1
        bucket["total_net"] += net
        bucket["comm"] += commissions
        bucket["fees"] += other_fees
        for u in unds:
            bucket["underlyings"].add(u)
        if mixed_flag:
            bucket["notes"].add(mixed_flag)

    # Build summary rows
    # Sort expirations so MOS: unknown/mixed at bottom
    def key_exp(k: str):
        if k and k[0].isdigit():
            return (0, k)
        return (1, k)
    summary_rows: List[List[Any]] = []
    for exp in sorted(by_exp.keys(), key=key_exp):
        b = by_exp[exp]
        summary_rows.append([
            exp,
            b["txn_count"],
            round(b["total_net"], 2),
            round(b["comm"], 2),
            round(b["fees"], 2),
            ",".join(sorted(b["underlyings"])) if b["underlyings"] else "",
            "; ".join(sorted(b["notes"])) if b["notes"] else ""
        ])

    return raw_rows, summary_rows

# ------------- main -------------
def main() -> int:
    days_back = 14
    try:
        days_back = int(os.environ.get("DAYS_BACK","14").strip() or "14")
    except Exception:
        pass
    symbol_filter = os.environ.get("SYMBOL_FILTER","").strip()

    # Schwab client
    try:
        c, acct_hash = schwab_client()
    except Exception as e:
        print(f"ABORT: Schwab client init failed — {e}")
        return 1

    # Fetch trades
    try:
        trades = fetch_trades(c, acct_hash, days_back)
    except Exception as e:
        print(f"ABORT: transactions fetch failed — {e}")
        return 1

    # Transform
    raw_rows, sum_rows = transform_raw_rows(trades, symbol_filter)

    # Sheets write
    try:
        svc, sheet_id = sheets_client()
        write_tab_overwrite(svc, sheet_id, RAW_TAB, RAW_HEADERS, raw_rows)
        write_tab_overwrite(svc, sheet_id, SUM_TAB, SUM_HEADERS, sum_rows)
        print(f"OK: wrote {len(raw_rows)} txns, {len(sum_rows)} expiry rows.")
    except Exception as e:
        print(f"ABORT: Sheets write failed — {e}")
        return 1

    return 0

if __name__ == "__main__":
    sys.exit(main())
