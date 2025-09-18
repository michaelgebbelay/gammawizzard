#!/usr/bin/env python3
"""
Summarize sw_txn_raw → sw_summary_by_expiry with a persistent accumulator.

Flow per run
- Read today's 'sw_txn_raw'
- Deduplicate legs inside the window (kills repeated ledger echoes)
- Append only NEW unique legs to 'sw_txn_accum' (persistent across runs)
- Recompute 'sw_summary_by_expiry' from the full accumulator
- Sort summary by exp_primary DESC so newest expiries are on top

Uniqueness key (leg):
- If txn_id present: ("ID", txn_id, symbol, round(qty,6), round(price,6), exp_primary, put_call, strike)
- Else (e.g., expiration entries): ("TS", ts, symbol, round(qty,6), round(price,6), exp_primary, put_call, strike)

Fees:
- We allocate per-order (txn_id) total fees = max(commissions+fees_other seen for that order)
- Allocation pro‑rata across that order's expiries by |amount| from the accumulator
- premium_net = premium_gross - fees_alloc

Env:
  GSHEET_ID, GOOGLE_SERVICE_ACCOUNT_JSON
  ACC_RESET (optional: "1"/"true" to clear accumulator before appending)
Tabs:
  RAW:  sw_txn_raw           (input; ephemeral)
  ACC:  sw_txn_accum         (persistent, deduped)
  OUT:  sw_summary_by_expiry (summary, rebuilt each run)
"""

import os, json, base64, re
from collections import defaultdict
from typing import Any, Dict, List, Tuple, Optional

from google.oauth2 import service_account
from googleapiclient.discovery import build as gbuild

RAW_TAB = "sw_txn_raw"
ACC_TAB = "sw_txn_accum"
OUT_TAB = "sw_summary_by_expiry"

OUT_HEADERS = [
    "exp_primary","underlying","num_orders","legs","contracts_net",
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

def get_sheet_meta(svc, sid: str):
    return svc.spreadsheets().get(spreadsheetId=sid).execute()

def ensure_tab_with_header(svc, sid: str, tab: str, headers: List[str]) -> int:
    meta = get_sheet_meta(svc, sid)
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
        meta = get_sheet_meta(svc, sid)
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

def read_tab(svc, sid: str, tab: str) -> Tuple[List[str], List[List[str]]]:
    res = svc.spreadsheets().values().get(spreadsheetId=sid, range=f"{tab}!A1:ZZ").execute()
    vals = res.get("values", [])
    if not vals: return [], []
    return vals[0], vals[1:]

def append_rows(svc, sid: str, tab: str, rows: List[List[Any]]) -> None:
    if not rows: return
    svc.spreadsheets().values().append(
        spreadsheetId=sid,
        range=f"{tab}!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": rows}
    ).execute()

def clear_tab(svc, sid: str, tab: str) -> None:
    svc.spreadsheets().values().clear(spreadsheetId=sid, range=tab).execute()

def format_out_columns(svc, sid: str, sheet_id: int) -> None:
    # headers: 0 exp, 1 und, 2 num_orders, 3 legs, 4 contracts_net, 5 premium_gross, 6 fees_alloc, 7 premium_net
    numeric_cols = [2,3,4,5,6,7]
    reqs = []
    for ci in [0,1]:
        reqs.append({
            "repeatCell":{
                "range":{"sheetId":sheet_id,"startRowIndex":1,"startColumnIndex":ci,"endColumnIndex":ci+1},
                "cell":{"userEnteredFormat":{"numberFormat":{"type":"TEXT"}}},
                "fields":"userEnteredFormat.numberFormat"
            }
        })
    for ci in numeric_cols:
        reqs.append({
            "repeatCell":{
                "range":{"sheetId":sheet_id,"startRowIndex":1,"startColumnIndex":ci,"endColumnIndex":ci+1},
                "cell":{"userEnteredFormat":{"numberFormat":{"type":"NUMBER","pattern":"0.00"}}},
                "fields":"userEnteredFormat.numberFormat"
            }
        })
    if reqs:
        svc.spreadsheets().batchUpdate(spreadsheetId=sid, body={"requests":reqs}).execute()

# ---------- Helpers ----------
def to_float(x) -> Optional[float]:
    if x is None: return None
    s = str(x).strip()
    if s == "": return None
    try: return float(s.replace(",",""))
    except Exception: return None

def norm_str(x) -> str:
    return "" if x is None else str(x).strip()

def normalize_underlying(u: str) -> str:
    u = (u or "").upper()
    return "SPX" if u.startswith("SPX") else u or "UNK"

def leg_key(idx: Dict[str,int], row: List[str]) -> Tuple:
    """Stable uniqueness key for a leg row."""
    # Safe getters
    def g(name: str) -> str:
        i = idx.get(name, -1)
        return row[i] if i >= 0 and i < len(row) else ""
    txn_id = norm_str(g("txn_id"))
    ts     = norm_str(g("ts"))
    symbol = norm_str(g("symbol")).upper()
    qty    = to_float(g("quantity")) or 0.0
    price  = to_float(g("price")) or 0.0
    exp    = norm_str(g("exp_primary"))
    pc     = norm_str(g("put_call")).upper()
    strike = norm_str(g("strike"))
    if txn_id:
        return ("ID", txn_id, symbol, round(qty,6), round(price,6), exp, pc, strike)
    else:
        return ("TS", ts, symbol, round(qty,6), round(price,6), exp, pc, strike)

# ---------- Main ----------
def main() -> int:
    svc, sid = sheets_client()

    # 1) Read RAW and ensure it has expected headers
    raw_header, raw_rows = read_tab(svc, sid, RAW_TAB)
    if not raw_header or not raw_rows:
        print("ABORT: sw_txn_raw empty or missing.")
        return 1

    # Required columns
    needed = ["ts","txn_id","type","description","symbol","underlying","exp_primary",
              "strike","put_call","quantity","price","amount","commissions","fees_other","net_amount","source"]
    miss = [c for c in needed if c not in raw_header]
    if miss:
        print(f"ABORT: missing columns in {RAW_TAB}: {miss}")
        return 1

    # 2) Ensure ACC tab exists (same header as RAW). Allow reset if requested.
    acc_reset = (os.environ.get("ACC_RESET","").lower() in ("1","true","yes"))
    acc_sheet_id = ensure_tab_with_header(svc, sid, ACC_TAB, raw_header)
    if acc_reset:
        clear_tab(svc, sid, ACC_TAB)
        ensure_tab_with_header(svc, sid, ACC_TAB, raw_header)

    acc_header, acc_rows = read_tab(svc, sid, ACC_TAB)
    if not acc_header:
        # brand new; seed header
        ensure_tab_with_header(svc, sid, ACC_TAB, raw_header)
        acc_header, acc_rows = raw_header, []

    # 3) Build "seen" set from ACC, then dedupe RAW and append only new legs
    idx_acc = {name:i for i,name in enumerate(acc_header)}
    seen = set()
    for r in acc_rows:
        seen.add(leg_key(idx_acc, r))

    idx_raw = {name:i for i,name in enumerate(raw_header)}

    # In-window dedupe first (kills 3–4x echoes in sw_txn_raw)
    tmp_seen = set()
    dedup_rows = []
    for r in raw_rows:
        k = leg_key(idx_raw, r)
        if k in tmp_seen:  # intra-window duplicate
            continue
        tmp_seen.add(k)
        dedup_rows.append(r)

    # Now append only if not seen in accumulator
    new_rows = []
    for r in dedup_rows:
        k = leg_key(idx_raw, r)
        if k in seen:
            continue
        new_rows.append(r)
        seen.add(k)

    if new_rows:
        append_rows(svc, sid, ACC_TAB, new_rows)
        # refresh acc_rows to include new additions for summary
        acc_header, acc_rows = read_tab(svc, sid, ACC_TAB)

    # 4) Aggregate from full ACC
    idx = {name:i for i,name in enumerate(acc_header)}
    def g(row, name): 
        i = idx[name]; return row[i] if i < len(row) else ""

    # Normalize & collect
    agg: Dict[Tuple[str,str], Dict[str,Any]] = {}
    orders_by_bucket: Dict[Tuple[str,str], set] = defaultdict(set)
    fees_by_txn: Dict[str, float] = defaultdict(float)
    exp_weight_by_txn: Dict[str, Dict[Tuple[str,str], float]] = defaultdict(lambda: defaultdict(float))

    for r in acc_rows:
        exp  = norm_str(g(r, "exp_primary"))
        und  = normalize_underlying(g(r, "underlying"))
        if not exp:
            continue  # skip non-option rows in expiry summary

        txn_id = norm_str(g(r, "txn_id"))
        qty    = to_float(g(r, "quantity")) or 0.0
        amt    = to_float(g(r, "amount")) or 0.0
        comm   = to_float(g(r, "commissions")) or 0.0
        fees   = to_float(g(r, "fees_other")) or 0.0

        bucket = (exp, und)
        if bucket not in agg:
            agg[bucket] = {"legs":0,"qty_net":0.0,"premium_gross":0.0,"fees_alloc":0.0}

        agg[bucket]["legs"] += 1
        agg[bucket]["qty_net"] += qty
        agg[bucket]["premium_gross"] += amt

        if txn_id:
            orders_by_bucket[bucket].add(txn_id)
            # record max total fees per order (comm+fees can repeat across echoes)
            fees_by_txn[txn_id] = max(fees_by_txn[txn_id], (comm + fees))
            exp_weight_by_txn[txn_id][bucket] += abs(amt)

    # Allocate order fees pro‑rata by |amount|
    for txn_id, fee_total in fees_by_txn.items():
        if fee_total <= 0: 
            continue
        weights = exp_weight_by_txn.get(txn_id, {})
        denom = sum(weights.values())
        if denom <= 0:  # split evenly across touched buckets
            if not weights: 
                continue
            share = fee_total / len(weights)
            for bucket in weights.keys():
                if bucket in agg: agg[bucket]["fees_alloc"] += share
        else:
            for bucket, w in weights.items():
                if bucket in agg:
                    agg[bucket]["fees_alloc"] += fee_total * (w / denom)

    # 5) Build OUT rows (newest expiries first)
    out_rows: List[List[Any]] = []
    # sort by exp desc, then underlying asc
    for (exp, und) in sorted(agg.keys(), key=lambda b: (b[0], b[1]), reverse=True):
        vals = agg[(exp, und)]
        num_orders = len(orders_by_bucket.get((exp,und), set()))
        legs = vals["legs"]
        qty_net = round(vals["qty_net"], 6)
        prem = round(vals["premium_gross"], 2)
        fees_alloc = round(vals["fees_alloc"], 2)
        prem_net = round(prem - fees_alloc, 2)
        out_rows.append([exp, und, num_orders, legs, qty_net, prem, fees_alloc, prem_net])

    # 6) Write OUT
    out_sheet_id = ensure_tab_with_header(svc, sid, OUT_TAB, OUT_HEADERS)
    clear_tab(svc, sid, OUT_TAB)
    svc.spreadsheets().values().update(
        spreadsheetId=sid, range=f"{OUT_TAB}!A1",
        valueInputOption="RAW",
        body={"values":[OUT_HEADERS] + out_rows}
    ).execute()
    format_out_columns(svc, sid, out_sheet_id)

    print(f"OK: accumulator += {len(new_rows)} new legs; wrote {len(out_rows)} summary rows.")
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())
