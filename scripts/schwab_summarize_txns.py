#!/usr/bin/env python3
"""
Summarize sw_txn_raw → sw_summary_by_expiry

What it does
- Pulls all rows from 'sw_txn_raw'
- Dedupes identical legs emitted multiple times for the same order (key: txn_id+symbol+qty+price; ts used when txn_id is blank)
- Groups by (exp_primary, underlying)
- Sums:
    * legs (unique legs after dedupe)
    * contracts_net (sum of qty)
    * premium_gross (sum of leg 'amount')
    * fees_alloc (commissions+fees allocated per order across its expiries by |amount|)
    * premium_net = premium_gross - fees_alloc
- Writes compact table to 'sw_summary_by_expiry' and formats columns as numbers (no date auto-munge)

Env:
  GSHEET_ID, GOOGLE_SERVICE_ACCOUNT_JSON
"""

import os, json, base64, re
from collections import defaultdict
from typing import Any, Dict, List, Tuple, Optional

from google.oauth2 import service_account
from googleapiclient.discovery import build as gbuild

RAW_TAB = "sw_txn_raw"
OUT_TAB = "sw_summary_by_expiry"

SUMMARY_HEADERS = [
    "exp_primary","underlying","num_orders","legs","contracts_net",
    "premium_gross","fees_alloc","premium_net"
]

# -------- Sheets helpers --------
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

def read_raw(svc, sid: str) -> Tuple[List[str], List[List[str]]]:
    # Columns A..Q (17 columns)
    res = svc.spreadsheets().values().get(
        spreadsheetId=sid, range=f"{RAW_TAB}!A1:Q"
    ).execute()
    values = res.get("values", [])
    if not values:
        return [], []
    header = values[0]
    rows = values[1:]
    return header, rows

def ensure_tab_with_header(svc, sid: str, tab: str, headers: List[str]) -> int:
    meta = svc.spreadsheets().get(spreadsheetId=sid).execute()
    sheet_id = None
    titles = {}
    for s in meta.get("sheets", []):
        title = s["properties"]["title"]
        titles[title] = s["properties"]["sheetId"]
    if tab not in titles:
        svc.spreadsheets().batchUpdate(
            spreadsheetId=sid,
            body={"requests":[{"addSheet":{"properties":{"title":tab}}}]}
        ).execute()
        meta = svc.spreadsheets().get(spreadsheetId=sid).execute()
        for s in meta.get("sheets", []):
            if s["properties"]["title"] == tab:
                sheet_id = s["properties"]["sheetId"]
                break
    else:
        sheet_id = titles[tab]

    got = svc.spreadsheets().values().get(spreadsheetId=sid, range=f"{tab}!1:1").execute().get("values", [])
    if not got or got[0] != headers:
        svc.spreadsheets().values().update(
            spreadsheetId=sid, range=f"{tab}!A1",
            valueInputOption="RAW",
            body={"values":[headers]}
        ).execute()
    return sheet_id

def clear_and_write(svc, sid: str, tab: str, headers: List[str], rows: List[List[Any]]) -> None:
    svc.spreadsheets().values().clear(spreadsheetId=sid, range=tab).execute()
    svc.spreadsheets().values().update(
        spreadsheetId=sid, range=f"{tab}!A1",
        valueInputOption="RAW",
        body={"values":[headers] + rows}
    ).execute()

def format_out_columns(svc, sid: str, sheet_id: int) -> None:
    # number formats for numeric columns
    # headers: 0 exp, 1 und, 2 num_orders, 3 legs, 4 contracts_net, 5 premium_gross, 6 fees_alloc, 7 premium_net
    numeric_cols = [2,3,4,5,6,7]
    requests = []
    for ci in [0,1]:
        requests.append({
            "repeatCell":{
                "range":{"sheetId":sheet_id,"startRowIndex":1,"startColumnIndex":ci,"endColumnIndex":ci+1},
                "cell":{"userEnteredFormat":{"numberFormat":{"type":"TEXT"}}},
                "fields":"userEnteredFormat.numberFormat"
            }
        })
    for ci in numeric_cols:
        requests.append({
            "repeatCell":{
                "range":{"sheetId":sheet_id,"startRowIndex":1,"startColumnIndex":ci,"endColumnIndex":ci+1},
                "cell":{"userEnteredFormat":{"numberFormat":{"type":"NUMBER","pattern":"0.00"}}},
                "fields":"userEnteredFormat.numberFormat"
            }
        })
    if requests:
        svc.spreadsheets().batchUpdate(spreadsheetId=sid, body={"requests":requests}).execute()

# -------- parse helpers --------
def to_float(x) -> Optional[float]:
    if x is None: return None
    s = str(x).strip()
    if s == "": return None
    try:
        return float(s.replace(",",""))
    except Exception:
        return None

def norm_str(x) -> str:
    return "" if x is None else str(x).strip()

# -------- summarizer --------
def main() -> int:
    svc, sid = sheets_client()

    header, rows = read_raw(svc, sid)
    if not header or not rows:
        print("ABORT: sw_txn_raw empty or missing.")
        return 1

    # map columns
    idx = {name: i for i, name in enumerate(header)}
    need = ["ts","txn_id","type","description","symbol","underlying","exp_primary",
            "strike","put_call","quantity","price","amount","commissions","fees_other"]
    for n in need:
        if n not in idx:
            print(f"ABORT: missing column in sw_txn_raw: {n}")
            return 1

    # --- Deduplicate legs: same order leg repeated multiple times ---
    seen = set()
    dedup_rows: List[List[str]] = []

    for r in rows:
        # safe access
        def g(name: str) -> str:
            i = idx[name]
            return r[i] if i < len(r) else ""

        txn_id = norm_str(g("txn_id"))
        ts     = norm_str(g("ts"))
        symbol = norm_str(g("symbol")).upper()
        qty    = to_float(g("quantity")) or 0.0
        price  = to_float(g("price")) or 0.0

        if txn_id:
            k = ("ID", txn_id, symbol, round(qty,6), round(price,6))
        else:
            # for rows without txn_id (e.g. expirations), key on ts+symbol+qty+price
            k = ("TS", ts, symbol, round(qty,6), round(price,6))

        if k in seen:
            continue
        seen.add(k)
        dedup_rows.append(r)

    # --- Aggregate by (exp_primary, underlying) ---
    agg = {}  # (exp, und) -> dict
    # track per-order fees once & allocate to expiries pro-rata by |amount|
    fees_by_txn: Dict[str, float] = defaultdict(float)
    exp_weight_by_txn: Dict[str, Dict[Tuple[str,str], float]] = defaultdict(lambda: defaultdict(float))
    orders_by_bucket: Dict[Tuple[str,str], set] = defaultdict(set)

    for r in dedup_rows:
        def g(name: str) -> str:
            i = idx[name]
            return r[i] if i < len(r) else ""

        exp  = norm_str(g("exp_primary"))
        und  = norm_str(g("underlying")).upper() or "UNK"
        if und.startswith("SPX"): und = "SPX"  # normalize SPXW→SPX
        if not exp:
            # ignore non-option rows for expiry summary
            continue

        txn_id = norm_str(g("txn_id"))
        qty    = to_float(g("quantity")) or 0.0
        amt    = to_float(g("amount")) or 0.0
        comm   = to_float(g("commissions")) or 0.0
        fees   = to_float(g("fees_other")) or 0.0

        bucket = (exp, und)
        if bucket not in agg:
            agg[bucket] = {
                "legs":0,
                "qty_net":0.0,
                "premium_gross":0.0,
                "fees_alloc":0.0
            }

        # aggregate legs/premium/qty
        agg[bucket]["legs"] += 1
        agg[bucket]["qty_net"] += qty
        agg[bucket]["premium_gross"] += amt

        # track distinct orders per bucket
        if txn_id:
            orders_by_bucket[bucket].add(txn_id)

        # record order-level fees once: take the max seen for this order (rows often repeat the same fees)
        if txn_id:
            cur = fees_by_txn.get(txn_id, 0.0)
            total_fees_txn = max(cur, (comm + fees))
            fees_by_txn[txn_id] = total_fees_txn

            # build weights per (exp,und) using |amount|
            w = abs(amt)
            exp_weight_by_txn[txn_id][bucket] += w

    # --- Allocate fees per order across its expiries ---
    for txn_id, fee_total in fees_by_txn.items():
        if fee_total <= 0.0: 
            continue
        weights = exp_weight_by_txn.get(txn_id, {})
        if not weights:
            continue
        denom = sum(weights.values())
        # if the order had zero |amount| (edge), split evenly
        if denom <= 0.0:
            equal = fee_total / max(1, len(weights))
            for bucket in weights.keys():
                if bucket not in agg: continue
                agg[bucket]["fees_alloc"] += equal
        else:
            for bucket, w in weights.items():
                if bucket not in agg: continue
                alloc = fee_total * (w / denom)
                agg[bucket]["fees_alloc"] += alloc

    # --- Build summary rows ---
    out_rows: List[List[Any]] = []
    for (exp, und), vals in sorted(agg.items(), key=lambda kv: kv[0]):
        num_orders = len(orders_by_bucket.get((exp, und), set()))
        legs = vals["legs"]
        qty_net = round(vals["qty_net"], 6)
        prem = round(vals["premium_gross"], 2)
        fees_alloc = round(vals["fees_alloc"], 2)
        prem_net = round(prem - fees_alloc, 2)
        out_rows.append([exp, und, num_orders, legs, qty_net, prem, fees_alloc, prem_net])

    # ensure tab and write
    sheet_id = ensure_tab_with_header(svc, sid, OUT_TAB, SUMMARY_HEADERS)
    # clear then write
    svc.spreadsheets().values().clear(spreadsheetId=sid, range=OUT_TAB).execute()
    svc.spreadsheets().values().update(
        spreadsheetId=sid, range=f"{OUT_TAB}!A1",
        valueInputOption="RAW",
        body={"values":[SUMMARY_HEADERS] + out_rows}
    ).execute()
    # format columns
    format_out_columns(svc, sid, sheet_id)

    print(f"OK: wrote {len(out_rows)} rows to {OUT_TAB}.")
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())
