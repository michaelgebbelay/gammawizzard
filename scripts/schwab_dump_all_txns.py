#!/usr/bin/env python3
# Dump ALL Schwab transactions (no filtering) into Google Sheets tab: sw_txn_raw
# - Handles 60-day Schwab transactions window by iterating windows if START_DATE is provided
# - Flattens up to MAX_LEGS legs into text-to-columns fields (leg1_..., leg2_... etc.)
# - Adds has_option, underlyings_set, expirations_set, exp_primary
# - Writes in a SINGLE update (header + rows) to avoid Sheets rate throttling

import os, sys, json, base64, collections, math, re
from datetime import datetime, timedelta, timezone, date
from typing import Any, Dict, List, Tuple, Optional

UTC = timezone.utc
MAX_LEGS = 8
DEBUG = (os.environ.get("DEBUG","0").strip().lower() in ("1","true","yes","y"))

# ---- Google Sheets ----
from google.oauth2 import service_account
from googleapiclient.discovery import build as gbuild

RAW_TAB = "sw_txn_raw"
RAW_HEADERS_BASE = [
    "ts_trade_utc","account_hash","txn_id","type","description",
    "net_amount","commissions","fees_other","fees_total",
    "has_option","underlyings_set","expirations_set","exp_primary"
]
LEG_HEADERS = []
for i in range(1, MAX_LEGS+1):
    LEG_HEADERS += [
        f"leg{i}_assetType", f"leg{i}_instruction", f"leg{i}_putCall",
        f"leg{i}_strike", f"leg{i}_qty", f"leg{i}_price", f"leg{i}_symbol"
    ]
RAW_HEADERS = RAW_HEADERS_BASE + LEG_HEADERS + ["legs_json"]

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
    # Create sheet if missing, then write header+rows in a single call
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

# ---- Schwab client ----
from schwab.auth import client_from_token_file
try:
    from schwab.client import Client  # type: ignore
except Exception:
    Client = Any  # type: ignore

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

def schwab_client():
    token_path = _token_to_file_from_env()
    # Try modern kw signature
    try:
        c = client_from_token_file(
            api_key=os.environ["SCHWAB_APP_KEY"],
            app_secret=os.environ["SCHWAB_APP_SECRET"],
            token_path=token_path
        )
    except TypeError:
        # Fallback to legacy positional
        c = client_from_token_file(token_path, os.environ["SCHWAB_APP_KEY"], os.environ["SCHWAB_APP_SECRET"])

    r = c.get_account_numbers(); r.raise_for_status()
    arr = r.json()
    acct_hashes = [x.get("hashValue") for x in (arr if isinstance(arr,list) else [arr]) if x.get("hashValue")]
    if not acct_hashes:
        raise RuntimeError("No Schwab accounts returned for this token.")
    return c, acct_hashes

# ---- Transactions fetch (no filters beyond dates) ----
def clamp_days_back(n: int) -> int:
    return max(1, min(60, n))  # Schwab supports ~60-day window for transactions endpoint

def parse_start_end() -> Tuple[datetime, datetime, bool]:
    # If START_DATE provided, we will do windowed fetch from start to today (multi-requests)
    s = (os.environ.get("START_DATE","") or "").strip()
    if s:
        y,m,d = map(int, s.split("-"))
        start = datetime(y,m,d, tzinfo=UTC)
        end   = datetime.now(UTC)
        return start, end, True
    # else days_back
    try:
        days = int((os.environ.get("DAYS_BACK","60") or "60").strip())
    except Exception:
        days = 60
    end = datetime.now(UTC)
    start = end - timedelta(days=clamp_days_back(days))
    return start, end, False

def fetch_trades_for_acct(c, acct_hash: str, start_dt: datetime, end_dt: datetime) -> List[Dict[str,Any]]:
    # Only pass dates — NO symbol/type filters => avoids 400 "Unexpected parameter"
    try:
        r = c.get_transactions(acct_hash, start_date=start_dt, end_date=end_dt)
    except TypeError:
        r = c.get_transactions(acct_hash, start_date=start_dt.date(), end_date=end_dt.date())
    r.raise_for_status()
    data = r.json()
    if isinstance(data, dict) and "transactions" in data:
        arr = data["transactions"]
    elif isinstance(data, list):
        arr = data
    else:
        arr = []
    return arr

def fetch_all(c, acct_hashes: List[str], start: datetime, end: datetime, is_windowed: bool) -> List[Dict[str,Any]]:
    out: List[Dict[str,Any]] = []
    if is_windowed:
        cur = start
        step = timedelta(days=60)  # 60-day slices
        while cur < end:
            win_end = min(end, cur + step)
            for h in acct_hashes:
                tx = fetch_trades_for_acct(c, h, cur, win_end)
                for t in tx: t["_acctHash"] = h
                out.extend(tx)
            if DEBUG:
                print(f"DEBUG: fetched {len(out)} up to {win_end.date()}")
            cur = win_end + timedelta(seconds=1)
    else:
        for h in acct_hashes:
            tx = fetch_trades_for_acct(c, h, start, end)
            for t in tx: t["_acctHash"] = h
            out.extend(tx)
    if DEBUG:
        typs = collections.Counter([str(x.get("type","")).upper() for x in out])
        print(f"DEBUG: window {start.isoformat()} → {end.isoformat()}  raw_count={len(out)} type_hist={dict(typs)}")
    return out

# ---- Flatten helpers ----
def iter_dicts(obj):
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from iter_dicts(v)
    elif isinstance(obj, list):
        for it in obj:
            yield from iter_dicts(it)

def norm_ts_to_utc_iso(ts: Any) -> str:
    s = str(ts or "")
    if not s: return ""
    try:
        # Support "...+0000" or proper ISO
        return datetime.fromisoformat(s.replace("+0000","+00:00")).astimezone(UTC).isoformat()
    except Exception:
        try:
            return datetime.fromisoformat(s).astimezone(UTC).isoformat()
        except Exception:
            return s

def to_float(x) -> float:
    try: return float(x)
    except: return 0.0

def item_is_option(d: Dict[str,Any]) -> bool:
    ins = d.get("instrument")
    if not isinstance(ins, dict): return False
    at = (ins.get("assetType") or ins.get("type") or "").upper()
    return at == "OPTION"

def item_expiry_ymd(item: Dict[str,Any]) -> Optional[str]:
    ins = item.get("instrument") or {}
    exp = ins.get("optionExpirationDate") or ins.get("expirationDate")
    if exp:
        try:
            y,m,dd = str(exp)[:10].split("-")
            return f"{int(y):04d}-{int(m):02d}-{int(dd):02d}"
        except Exception:
            pass
    sym = (ins.get("symbol") or "").replace(" ", "")
    m = re.search(r'(\d{6})[CP]\d{5,8}', sym)
    if m:
        s = m.group(1); y = 2000 + int(s[:2]); mo = int(s[2:4]); dd = int(s[4:6])
        return f"{y:04d}-{mo:02d}-{dd:02d}"
    return None

def item_underlying(item: Dict[str,Any]) -> str:
    ins = item.get("instrument") or {}
    u = ins.get("underlyingSymbol")
    if u: return str(u)
    sym = (ins.get("symbol") or "").strip()
    if not sym: return ""
    i = 0
    while i < len(sym) and not sym[i].isdigit():
        i += 1
    return sym[:i].strip()

def item_instruction(item: Dict[str,Any]) -> str:
    instr = (item.get("instruction") or "").upper()
    if instr: return instr
    qty = item.get("amount") or item.get("quantity")
    try:
        q = float(qty)
        return "SELL" if q < 0 else "BUY"
    except Exception:
        return ""

def extract_legs(tx: Dict[str,Any]) -> List[Dict[str,Any]]:
    legs: List[Dict[str,Any]] = []
    for d in iter_dicts(tx):
        if not isinstance(d, dict): continue
        ins = d.get("instrument")
        if not isinstance(ins, dict): continue
        # Collect ANY leg (option, equity, etc.) so the dump is universal
        at = (ins.get("assetType") or ins.get("type") or "").upper()
        if at in ("OPTION","EQUITY","MUTUAL_FUND","FOREX","FUTURE","INDEX","ETF","BOND"):
            legs.append(d)
    return legs

def flatten_tx(tx: Dict[str,Any]) -> List[Any]:
    ts = tx.get("transactionDate") or tx.get("transactionDateTime") or tx.get("time") or tx.get("closeDate")
    ts_iso = norm_ts_to_utc_iso(ts)
    acct = str(tx.get("_acctHash") or tx.get("accountId") or tx.get("account") or "")
    txn_id = tx.get("orderId") or tx.get("orderNumber") or tx.get("transactionId") or ""
    ttype = str(tx.get("type","")).upper()
    desc  = str(tx.get("description") or tx.get("subAccount") or tx.get("subType") or "")
    net   = to_float(tx.get("netAmount"))

    commissions = 0.0
    fees_other  = 0.0
    for f in (tx.get("fees") or []):
        amt = to_float(f.get("amount"))
        code = (f.get("code") or "").lower()
        descf= (f.get("description") or "").lower()
        if ("commission" in code) or ("commission" in descf):
            commissions += amt
        else:
            fees_other += amt
    fees_total = commissions + fees_other

    legs = extract_legs(tx)
    has_option = 1 if any(item_is_option(i) for i in legs) else 0

    unds = sorted({ item_underlying(i) for i in legs if item_underlying(i) })
    exps = sorted({ e for e in (item_expiry_ymd(i) for i in legs if item_is_option(i)) if e })
    if len(exps) == 1:
        exp_primary = exps[0]
    elif len(exps) == 0:
        exp_primary = ""
    else:
        exp_primary = "MIXED_OR_UNKNOWN"

    base = [
        ts_iso, acct, str(txn_id), ttype, desc,
        round(net, 2), round(commissions, 2), round(fees_other, 2), round(fees_total, 2),
        has_option,
        ",".join(unds) if unds else "",
        ",".join(exps) if exps else "",
        exp_primary
    ]

    # Legs into fixed columns
    cols: List[Any] = []
    for i in range(MAX_LEGS):
        if i < len(legs):
            it = legs[i]; ins = it.get("instrument") or {}
            cols += [
                (ins.get("assetType") or ins.get("type") or "").upper(),
                item_instruction(it),
                (ins.get("putCall") or ins.get("type") or "").upper(),
                ins.get("strikePrice") or ins.get("strike") or "",
                it.get("quantity") or it.get("amount") or "",
                it.get("price") or "",
                ins.get("symbol") or ""
            ]
        else:
            cols += ["","","","","","",""]

    legs_json = json.dumps(legs, separators=(",",":")) if legs else ""
    return base + cols + [legs_json]

# ---- main ----
def main() -> int:
    # Schwab client
    try:
        c, acct_hashes = schwab_client()
    except Exception as e:
        print(f"ABORT: Schwab client init failed — {e}")
        return 1

    start, end, is_windowed = parse_start_end()
    try:
        all_tx = fetch_all(c, acct_hashes, start, end, is_windowed)
    except Exception as e:
        print(f"ABORT: transactions fetch failed — {e}")
        return 1

    rows = [flatten_tx(t) for t in all_tx]

    try:
        svc, sheet_id = sheets_client()
        write_tab_overwrite(svc, sheet_id, RAW_TAB, RAW_HEADERS, rows)
        print(f"OK: wrote {len(rows)} txns into {RAW_TAB}.")
    except Exception as e:
        print(f"ABORT: Sheets write failed — {e}")
        return 1

    return 0

if __name__ == "__main__":
    sys.exit(main())
