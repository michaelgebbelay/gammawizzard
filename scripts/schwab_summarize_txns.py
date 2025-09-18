#!/usr/bin/env python3
"""
Summarize P/L by expiration — ledger-first (correct cash), no re-allocation.

What this does
--------------
- Uses only rows sourced from Schwab's ledger (`source == "schwab_txn"`) and `type == "TRADE"`.
- premium_net  = Σ net_amount                (already includes per-fill commissions & fees)
- fees_alloc   = Σ (commissions + fees_other)
- premium_gross= premium_net + fees_alloc     (since net = gross - fees)
- legs         = distinct symbols per (exp, underlying)
- num_orders   = distinct txn_id per (exp, underlying)
- contracts_abs= Σ |quantity| (ledger is one row per contract fill)
- contracts_net= Σ quantity

Tabs
----
RAW: sw_txn_raw            (input)
ACC: sw_txn_accum          (filtered ledger-only trades; for audit)
OUT: sw_summary_by_expiry  (rebuilt each run)
"""

import os, json, base64
from typing import Any, Dict, List, Tuple, Optional
from collections import defaultdict

from google.oauth2 import service_account
from googleapiclient.discovery import build as gbuild

RAW_TAB = "sw_txn_raw"
ACC_TAB = "sw_txn_accum"
OUT_TAB = "sw_summary_by_expiry"

OUT_HEADERS = [
    "exp_primary","underlying","num_orders","legs","contracts_net","contracts_abs",
    "premium_gross","fees_alloc","premium_net"
]

# ---------- Sheets ----------
def sheets_client():
    sid = os.environ["GSHEET_ID"]
    sa_json = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
    try:
        dec = base64.b64decode(sa_json).decode("utf-8")
        if dec.strip().startswith("{"):
            sa_json = dec
    except Exception:
        pass
    creds = service_account.Credentials.from_service_account_info(
        json.loads(sa_json),
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return gbuild("sheets","v4",credentials=creds), sid

def read_tab(svc, sid: str, tab: str):
    res = svc.spreadsheets().values().get(spreadsheetId=sid, range=f"{tab}!A1:ZZ").execute()
    vals = res.get("values", [])
    return (vals[0], vals[1:]) if vals else ([], [])

def ensure_tab_with_header(svc, sid: str, tab: str, headers: List[str]) -> int:
    meta = svc.spreadsheets().get(spreadsheetId=sid).execute()
    ids = {s["properties"]["title"]: s["properties"]["sheetId"] for s in meta.get("sheets", [])}
    if tab not in ids:
        svc.spreadsheets().batchUpdate(spreadsheetId=sid, body={"requests":[{"addSheet":{"properties":{"title":tab}}}]}).execute()
        meta = svc.spreadsheets().get(spreadsheetId=sid).execute()
        ids = {s["properties"]["title"]: s["properties"]["sheetId"] for s in meta.get("sheets", [])}
    sheet_id = ids[tab]
    got = svc.spreadsheets().values().get(spreadsheetId=sid, range=f"{tab}!1:1").execute().get("values",[])
    if not got or got[0] != headers:
        svc.spreadsheets().values().update(
            spreadsheetId=sid, range=f"{tab}!A1",
            valueInputOption="RAW",
            body={"values":[headers]}
        ).execute()
    return sheet_id

def clear_tab(svc, sid: str, tab: str) -> None:
    svc.spreadsheets().values().clear(spreadsheetId=sid, range=tab).execute()

def write_tab(svc, sid: str, tab: str, headers: List[str], rows: List[List[Any]]) -> None:
    svc.spreadsheets().values().update(
        spreadsheetId=sid, range=f"{tab}!A1",
        valueInputOption="RAW",
        body={"values":[headers] + rows}
    ).execute()

def format_out_columns(svc, sid: str, sheet_id: int) -> None:
    numeric = [2,3,4,5,6,7,8]
    reqs = []
    for ci in [0,1]:
        reqs.append({"repeatCell":{"range":{"sheetId":sheet_id,"startRowIndex":1,"startColumnIndex":ci,"endColumnIndex":ci+1},
                                   "cell":{"userEnteredFormat":{"numberFormat":{"type":"TEXT"}}},
                                   "fields":"userEnteredFormat.numberFormat"}})
    for ci in numeric:
        reqs.append({"repeatCell":{"range":{"sheetId":sheet_id,"startRowIndex":1,"startColumnIndex":ci,"endColumnIndex":ci+1},
                                   "cell":{"userEnteredFormat":{"numberFormat":{"type":"NUMBER","pattern":"0.00"}}},
                                   "fields":"userEnteredFormat.numberFormat"}})
    if reqs:
        svc.spreadsheets().batchUpdate(spreadsheetId=sid, body={"requests":reqs}).execute()

# ---------- helpers ----------
def idxmap(header: List[str]) -> Dict[str,int]:
    return {name:i for i,name in enumerate(header)}

def to_float(x) -> Optional[float]:
    try:
        s = str(x).strip()
        if s == "": return None
        return float(s.replace(",",""))
    except Exception:
        return None

def norm(x) -> str:
    return "" if x is None else str(x).strip()

def get(row: List[str], idx: Dict[str,int], name: str, default: str="") -> str:
    i = idx.get(name, -1)
    return row[i] if 0 <= i < len(row) else default

# ---------- main ----------
def main() -> int:
    svc, sid = sheets_client()

    raw_header, raw_rows = read_tab(svc, sid, RAW_TAB)
    if not raw_header or not raw_rows:
        print("ABORT: sw_txn_raw empty or missing.")
        return 1

    need = ["ts","txn_id","type","description","symbol","underlying","exp_primary",
            "strike","put_call","quantity","price","amount","net_amount","commissions","fees_other","source"]
    missing = [c for c in need if c not in raw_header]
    if missing:
        print(f"ABORT: missing in {RAW_TAB}: {missing}")
        return 1

    idx = idxmap(raw_header)

    # ----- filter to clean ledger-only trade rows -----
    clean: List[List[str]] = []
    for r in raw_rows:
        if norm(get(r, idx, "source")).lower() != "schwab_txn":
            continue
        if norm(get(r, idx, "type")).upper() != "TRADE":
            continue
        # keep only rows that have an expiration and an option symbol
        if not norm(get(r, idx, "exp_primary")) or not norm(get(r, idx, "symbol")):
            continue
        clean.append(r)

    # Write ACC as an auditable filtered view
    ensure_tab_with_header(svc, sid, ACC_TAB, raw_header)
    write_tab(svc, sid, ACC_TAB, raw_header, clean)

    # ----- aggregate by (exp_primary, underlying) -----
    buckets: Dict[Tuple[str,str], Dict[str,Any]] = defaultdict(lambda: {
        "orders": set(),
        "symbols": set(),
        "contracts_net": 0.0,
        "contracts_abs": 0.0,
        "premium_net": 0.0,
        "fees_alloc": 0.0
    })

    for r in clean:
        exp = norm(get(r, idx, "exp_primary"))
        und = norm(get(r, idx, "underlying")).upper()
        if und.startswith("SPX"): und = "SPX"
        b = buckets[(exp, und)]

        b["orders"].add(norm(get(r, idx, "txn_id")))
        b["symbols"].add(norm(get(r, idx, "symbol")).upper())

        q = to_float(get(r, idx, "quantity")) or 0.0
        b["contracts_net"] += q
        b["contracts_abs"] += abs(q)

        b["premium_net"] += to_float(get(r, idx, "net_amount")) or 0.0
        c = to_float(get(r, idx, "commissions")) or 0.0
        f = to_float(get(r, idx, "fees_other")) or 0.0
        b["fees_alloc"] += (c + f)

    out_rows: List[List[Any]] = []
    for (exp, und), s in sorted(buckets.items(), key=lambda kv: kv[0], reverse=True):
        premium_net = round(s["premium_net"], 2)
        fees_alloc  = round(s["fees_alloc"], 2)
        premium_gross = round(premium_net + fees_alloc, 2)
        out_rows.append([
            exp, und,
            len(s["orders"]),
            len(s["symbols"]),
            round(s["contracts_net"], 6),
            round(s["contracts_abs"], 2),
            premium_gross,
            fees_alloc,
            premium_net
        ])

    # write OUT
    out_sheet_id = ensure_tab_with_header(svc, sid, OUT_TAB, OUT_HEADERS)
    clear_tab(svc, sid, OUT_TAB)
    write_tab(svc, sid, OUT_TAB, OUT_HEADERS, out_rows)
    format_out_columns(svc, sid, out_sheet_id)

    print(f"OK: wrote {len(out_rows)} summary rows from {len(clean)} ledger trade rows.")
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())
