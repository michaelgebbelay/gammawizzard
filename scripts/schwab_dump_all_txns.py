#!/usr/bin/env python3
"""
Schwab → Sheets raw transaction dump (no filters).
- Auth to Schwab using your token JSON secret.
- Fetch transactions for DAYS_BACK with resilient parameter shapes.
- Flatten to clean columns (no JSON blobs).
- Single write to tab 'sw_txn_raw' to avoid Sheets rate limits.
"""

import os, sys, json, base64, re
from datetime import datetime, timedelta, timezone, date
from typing import Any, Dict, List, Optional

import requests
from schwab.auth import client_from_token_file
from google.oauth2 import service_account
from googleapiclient.discovery import build as gbuild

RAW_TAB = "sw_txn_raw"

RAW_HEADERS = [
    # identity
    "ts", "txn_id", "type", "sub_type", "description",
    # instrument
    "symbol", "underlying", "exp_primary", "strike", "put_call",
    # economics (from Schwab only; no estimates)
    "quantity", "price", "amount", "net_amount", "commissions", "fees_other",
    # misc
    "source"
]

def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def sheets_client():
    sid = os.environ["GSHEET_ID"]
    sa_json = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
    creds = service_account.Credentials.from_service_account_info(
        json.loads(sa_json),
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    svc = gbuild("sheets","v4",credentials=creds)
    return svc, sid

def ensure_tab_with_header(svc, sid: str, tab: str, headers: List[str]) -> None:
    meta = svc.spreadsheets().get(spreadsheetId=sid).execute()
    tabs = [s["properties"]["title"] for s in meta.get("sheets",[])]
    if tab not in tabs:
        svc.spreadsheets().batchUpdate(
            spreadsheetId=sid,
            body={"requests":[{"addSheet":{"properties":{"title":tab}}}]}
        ).execute()
    got = svc.spreadsheets().values().get(spreadsheetId=sid, range=f"{tab}!1:1").execute().get("values",[])
    if not got or got[0] != headers:
        svc.spreadsheets().values().update(
            spreadsheetId=sid,
            range=f"{tab}!A1",
            valueInputOption="USER_ENTERED",
            body={"values":[headers]}
        ).execute()

def overwrite_rows(svc, sid: str, tab: str, headers: List[str], rows: List[List[Any]]) -> None:
    body = {"values":[headers] + rows}
    svc.spreadsheets().values().update(
        spreadsheetId=sid,
        range=f"{tab}!A1",
        valueInputOption="USER_ENTERED",
        body=body
    ).execute()

def decode_token_to_path() -> str:
    token_env = os.environ.get("SCHWAB_TOKEN_JSON","") or ""
    token_path = "schwab_token.json"
    if token_env:
        try:
            dec = base64.b64decode(token_env).decode("utf-8")
            if dec.strip().startswith("{"): token_env = dec
        except Exception:
            pass
        with open(token_path,"w") as f:
            f.write(token_env)
    return token_path

def schwab_client():
    token_path = decode_token_to_path()
    app_key = os.environ["SCHWAB_APP_KEY"]
    app_secret = os.environ["SCHWAB_APP_SECRET"]
    # Positional args to avoid package keyword drift
    c = client_from_token_file(token_path, app_key, app_secret)
    r = c.get_account_numbers(); r.raise_for_status()
    acct_hash = r.json()[0]["hashValue"]
    return c, acct_hash

def safe_float(x) -> Optional[float]:
    try: return float(x)
    except: return None

def parse_exp_from_symbol(sym: str) -> Optional[str]:
    # Accept OSI-ish symbols like 'SPXW  250910P06500000'
    if not sym: return None
    s = sym.strip().upper()
    m = re.search(r"(20\d{2}-\d{2}-\d{2})", s)
    if m: return m.group(1)
    m = re.search(r"\D(\d{6})[CP]\d", s)  # yymmdd
    if m:
        try: return datetime.strptime(m.group(1), "%y%m%d").date().isoformat()
        except: return None
    return None

def to_underlying(sym: str) -> Optional[str]:
    if not sym: return None
    s = sym.strip().upper().lstrip(".").replace("  "," ")
    if s.startswith("SPXW") or s.startswith("SPX"): return "SPX"
    m = re.match(r"([A-Z.$^]{1,6})", s)
    return m.group(1) if m else None

def explode_txn(txn: Dict[str, Any]) -> List[List[Any]]:
    out: List[List[Any]] = []

    ts = (txn.get("transactionDate") or txn.get("time") or txn.get("date") or "")
    txn_id = str(txn.get("transactionId") or txn.get("orderId") or txn.get("id") or "")
    ttype = str(txn.get("type") or txn.get("transactionType") or "")
    subtype = str(txn.get("subType") or "")
    desc = str(txn.get("description") or "")

    fees_total = 0.0
    comm_total = 0.0
    if isinstance(txn.get("fees"), dict):
        for k,v in (txn["fees"] or {}).items():
            val = safe_float(v) or 0.0
            if "comm" in k.lower(): comm_total += val
            else:                   fees_total += val
    elif isinstance(txn.get("fees"), list):
        for f in txn["fees"]:
            val = safe_float(f.get("amount")) or 0.0
            name = str(f.get("feeType") or f.get("type") or "")
            if "comm" in name.lower(): comm_total += val
            else:                      fees_total += val

    items = txn.get("transactionItems") or txn.get("transactionItem") or []
    if isinstance(items, dict): items = [items]

    if not items:
        net_amount = safe_float(txn.get("netAmount"))
        row = [ts, txn_id, ttype, subtype, desc,
               "", "", "", "", "",
               "", "", "", (net_amount if net_amount is not None else ""), round(comm_total,2) or "", round(fees_total,2) or "",
               "schwab_txn"]
        out.append(row)
        return out

    for it in items:
        qty = safe_float(it.get("amount")) or safe_float(it.get("quantity"))
        price = safe_float(it.get("price"))
        amount = safe_float(it.get("cost")) or safe_float(it.get("amount"))
        symbol = str((it.get("instrument") or {}).get("symbol") or it.get("symbol") or "")
        pc = (it.get("instruction") or it.get("putCall") or "")
        pc = "CALL" if str(pc).upper().startswith("C") else ("PUT" if str(pc).upper().startswith("P") else "")
        strike = safe_float((it.get("instrument") or {}).get("strikePrice") or it.get("strike"))
        exp = (it.get("instrument") or {}).get("optionExpirationDate")
        exp_primary = None
        if exp:
            try: exp_primary = str(date.fromisoformat(str(exp)[:10]))
            except: exp_primary = None
        if not exp_primary:
            exp_primary = parse_exp_from_symbol(symbol)
        underlying = to_underlying(symbol)
        net_amount = safe_float(txn.get("netAmount"))

        row = [ts, txn_id, ttype, subtype, desc,
               symbol, underlying or "", exp_primary or "", (strike if strike is not None else ""), pc,
               (qty if qty is not None else ""), (price if price is not None else ""), (amount if amount is not None else ""),
               (net_amount if net_amount is not None else ""), round(comm_total,2) or "", round(fees_total,2) or "",
               "schwab_txn"]
        out.append(row)

    return out

def get_txns_resilient(c, acct_hash: str, start_dt: datetime, end_dt: datetime) -> List[Dict[str, Any]]:
    base = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/transactions"

    def try_get(params: Optional[Dict[str, str]]):
        try:
            r = c.session.get(base, params=(params or {}), timeout=25)
            if r.status_code == 200:
                j = r.json()
                if isinstance(j, list): return j
                if isinstance(j, dict) and "transactions" in j: return j.get("transactions") or []
                return []
            print(f"NOTE: txns params={params} → {r.status_code}:{(r.text or '')[:160]}")
            return None
        except Exception as e:
            print(f"NOTE: txns exception params={params}: {e}")
            return None

    # Try shapes commonly accepted across tenants
    for params in (
        {"fromEnteredTime": iso_z(start_dt), "toEnteredTime": iso_z(end_dt)},
        {"startDate": start_dt.date().isoformat(), "endDate": end_dt.date().isoformat()},
        None,
    ):
        j = try_get(params)
        if j is not None:
            if params is None:
                # client-side window filter if needed
                out=[]
                for t in j:
                    ts = str(t.get("transactionDate") or t.get("time") or t.get("date") or "")
                    try:
                        dt = datetime.fromisoformat(ts.replace("Z","+00:00"))
                        if start_dt <= dt <= end_dt:
                            out.append(t)
                    except Exception:
                        out.append(t)
                return out
            return j

    raise RuntimeError("transactions fetch failed (all parameter shapes rejected)")

def main() -> int:
    # Sheets
    try:
        svc, sid = sheets_client()
        ensure_tab_with_header(svc, sid, RAW_TAB, RAW_HEADERS)
    except Exception as e:
        print(f"ABORT: Sheets init failed — {e}")
        return 1

    # Schwab
    try:
        c, acct_hash = schwab_client()
    except Exception as e:
        msg = str(e)
        if ("unsupported_token_type" in msg) or ("refresh_token_authentication_error" in msg):
            print("ABORT: Schwab OAuth refresh failed — rotate SCHWAB_TOKEN_JSON secret.")
        else:
            print(f"ABORT: Schwab client init failed — {msg[:200]}")
        return 1

    # Window
    try:
        days_back = int((os.environ.get("DAYS_BACK") or "60").strip())
    except:
        days_back = 60
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=days_back)

    # Fetch + flatten
    try:
        txns = get_txns_resilient(c, acct_hash, start_dt, end_dt)
    except Exception as e:
        print(f"ABORT: transactions fetch failed — {e}")
        return 1

    rows: List[List[Any]] = []
    for t in txns:
        rows.extend(explode_txn(t))

    overwrite_rows(svc, sid, RAW_TAB, RAW_HEADERS, rows)
    print(f"OK: wrote {len(rows)} rows to {RAW_TAB}.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
