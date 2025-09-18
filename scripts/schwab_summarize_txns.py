#!/usr/bin/env python3
"""
Summarize trades by expiry with a persistent accumulator.

- TRADE-only.
- Dedup key includes execution timestamp to preserve distinct fills.
- premium_gross = Σ(amount)  (credits positive, debits negative)
- fees_alloc allocated per order across expiries by |amount|
- premium_net = premium_gross - fees_alloc
- contracts_abs = Σ|quantity| (so 6 ICs → 24)

Tabs:
  RAW:  sw_txn_raw
  ACC:  sw_txn_accum
  OUT:  sw_summary_by_expiry
"""

import os, json, base64
from collections import defaultdict
from typing import Any, Dict, List, Tuple, Optional

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

def ensure_tab_with_header(svc, sid: str, tab: str, headers: List[str]) -> int:
    meta = svc.spreadsheets().get(spreadsheetId=sid).execute()
    ids = {s["properties"]["title"]: s["properties"]["sheetId"] for s in meta.get("sheets", [])}
    if tab not in ids:
        svc.spreadsheets().batchUpdate(spreadsheetId=sid, body={"requests":[{"addSheet":{"properties":{"title":tab}}}]}).execute()
        meta = svc.spreadsheets().get(spreadsheetId=sid).execute()
        ids = {s["properties"]["title"]: s["properties"]["sheetId"] for s in meta.get("sheets", [])}
    sheet_id = ids[tab]
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
        spreadsheetId=sid, range=f"{tab}!A1",
        valueInputOption="RAW", insertDataOption="INSERT_ROWS",
        body={"values": rows}
    ).execute()

def clear_tab(svc, sid: str, tab: str) -> None:
    svc.spreadsheets().values().clear(spreadsheetId=sid, range=tab).execute()

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
    return "SPX" if u.startswith("SPX") else (u or "UNK")

def leg_key(idx: Dict[str,int], row: List[str]) -> Tuple:
    """Include ts so identical price/size fills at the same level remain separate."""
    def g(name: str) -> str:
        i = idx.get(name, -1)
        return row[i] if 0 <= i < len(row) else ""
    txn_id = norm_str(g("txn_id"))
    ts     = norm_str(g("ts"))
    symbol = norm_str(g("symbol")).upper()
    qty    = to_float(g("quantity")) or 0.0
    price  = to_float(g("price")) or 0.0
    exp    = norm_str(g("exp_primary"))
    pc     = norm_str(g("put_call")).upper()
    strike = norm_str(g("strike"))
    if txn_id:
        return ("ID", txn_id, ts, symbol, round(qty,6), round(price,6), exp, pc, strike)
    else:
        return ("TS", ts, symbol, round(qty,6), round(price,6), exp, pc, strike)

# ---------- main ----------
def main() -> int:
    svc, sid = sheets_client()

    raw_header, raw_rows = read_tab(svc, sid, RAW_TAB)
    if not raw_header or not raw_rows:
        print("ABORT: sw_txn_raw empty or missing.")
        return 1

    need = ["ts","txn_id","type","description","symbol","underlying","exp_primary",
            "strike","put_call","quantity","price","amount","commissions","fees_other","net_amount","source"]
    miss = [c for c in need if c not in raw_header]
    if miss:
        print(f"ABORT: missing columns in {RAW_TAB}: {miss}")
        return 1
    idx_raw = {name:i for i,name in enumerate(raw_header)}

    # TRADE-only from RAW
    def is_trade(r: List[str]) -> bool:
        t = norm_str(r[idx_raw["type"]]).upper()
        return t == "TRADE"
    raw_trade = [r for r in raw_rows if is_trade(r)]

    # Ensure/optionally reset accumulator (same header as RAW)
    acc_reset = (os.environ.get("ACC_RESET","").lower() in ("1","true","yes"))
    acc_sheet_id = ensure_tab_with_header(svc, sid, ACC_TAB, raw_header)
    if acc_reset:
        clear_tab(svc, sid, ACC_TAB)
        ensure_tab_with_header(svc, sid, ACC_TAB, raw_header)

    acc_header, acc_rows_all = read_tab(svc, sid, ACC_TAB)
    if not acc_header:
        ensure_tab_with_header(svc, sid, ACC_TAB, raw_header)
        acc_header, acc_rows_all = raw_header, []
    idx_acc = {name:i for i,name in enumerate(acc_header)}

    def acc_is_trade(r: List[str]) -> bool:
        t = norm_str(r[idx_acc["type"]]).upper() if "type" in idx_acc else "TRADE"
        return t == "TRADE"
    acc_rows = [r for r in acc_rows_all if acc_is_trade(r)]

    # Build seen set from ACC
    seen = set(leg_key(idx_acc, r) for r in acc_rows)

    # Dedupe RAW in-window and append only new legs
    tmp_seen = set()
    dedup_rows = []
    for r in raw_trade:
        k = leg_key(idx_raw, r)
        if k in tmp_seen: 
            continue
        tmp_seen.add(k)
        if k in seen:
            continue
        dedup_rows.append(r)
        seen.add(k)

    if dedup_rows:
        append_rows(svc, sid, ACC_TAB, dedup_rows)
        acc_header, acc_rows_all = read_tab(svc, sid, ACC_TAB)
        idx_acc = {name:i for i,name in enumerate(acc_header)}
        acc_rows = [r for r in acc_rows_all if acc_is_trade(r)]

    # Aggregate from full ACC
    idx = {name:i for i,name in enumerate(acc_header)}
    def g(row, name): 
        i = idx[name]; return row[i] if i < len(row) else ""

    agg: Dict[Tuple[str,str], Dict[str,Any]] = {}
    orders_by_bucket: Dict[Tuple[str,str], set] = defaultdict(set)
    fees_by_txn: Dict[str, float] = defaultdict(float)
    exp_weight_by_txn: Dict[str, Dict[Tuple[str,str], float]] = defaultdict(lambda: defaultdict(float))

    for r in acc_rows:
        exp  = norm_str(g(r, "exp_primary"))
        und  = normalize_underlying(g(r, "underlying"))
        if not exp: 
            continue

        txn_id = norm_str(g(r, "txn_id"))
        qty    = to_float(g(r, "quantity")) or 0.0
        amt    = to_float(g(r, "amount")) or 0.0
        comm   = to_float(g(r, "commissions")) or 0.0
        fees   = to_float(g(r, "fees_other")) or 0.0

        bucket = (exp, und)
        if bucket not in agg:
            agg[bucket] = {"legs":0,"qty_net":0.0,"qty_abs":0.0,"premium_amount_sum":0.0,"fees_alloc":0.0}

        agg[bucket]["legs"] += 1
        agg[bucket]["qty_net"] += qty
        agg[bucket]["qty_abs"] += abs(qty)
        agg[bucket]["premium_amount_sum"] += amt

        if txn_id:
            orders_by_bucket[bucket].add(txn_id)
            fees_by_txn[txn_id] = max(fees_by_txn[txn_id], (comm + fees))
            exp_weight_by_txn[txn_id][bucket] += abs(amt)

    # Allocate order fees
    for txn_id, fee_total in fees_by_txn.items():
        if fee_total <= 0: 
            continue
        weights = exp_weight_by_txn.get(txn_id, {})
        denom = sum(weights.values())
        if denom <= 0:
            if not weights: continue
            share = fee_total / len(weights)
            for bucket in weights.keys():
                if bucket in agg: agg[bucket]["fees_alloc"] += share
        else:
            for bucket, w in weights.items():
                if bucket in agg: agg[bucket]["fees_alloc"] += fee_total * (w/denom)

    # Build OUT rows (newest expiries first)
    out_rows: List[List[Any]] = []
    for (exp, und) in sorted(agg.keys(), key=lambda b: (b[0], b[1]), reverse=True):
        vals = agg[(exp, und)]
        num_orders = len(orders_by_bucket.get((exp,und), set()))
        legs = vals["legs"]
        qty_net = round(vals["qty_net"], 6)
        qty_abs = round(vals["qty_abs"], 2)
        premium_gross = round(vals["premium_amount_sum"], 2)  # credits positive
        fees_alloc = round(vals["fees_alloc"], 2)
        premium_net = round(premium_gross - fees_alloc, 2)
        out_rows.append([exp, und, num_orders, legs, qty_net, qty_abs, premium_gross, fees_alloc, premium_net])

    out_sheet_id = ensure_tab_with_header(svc, sid, OUT_TAB, OUT_HEADERS)
    clear_tab(svc, sid, OUT_TAB)
    svc.spreadsheets().values().update(
        spreadsheetId=sid, range=f"{OUT_TAB}!A1",
        valueInputOption="RAW",
        body={"values":[OUT_HEADERS] + out_rows}
    ).execute()
    format_out_columns(svc, sid, out_sheet_id)

    print(f"OK: accumulator += {len(dedup_rows)} new TRADE legs; wrote {len(out_rows)} summary rows.")
    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())
