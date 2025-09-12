#!/usr/bin/env python3
# Read sw_txn_raw and produce sw_summary_by_expiry (option txns grouped by exp_primary)
# - No Schwab API calls here. Sheets → Sheets.
# - Uses only Schwab-provided net_amount & fees (no estimates)
# - Skips rows where has_option != 1 or exp_primary is blank / MIXED_OR_UNKNOWN

import os, sys, json
from typing import Any, List, Dict
from google.oauth2 import service_account
from googleapiclient.discovery import build as gbuild

RAW_TAB = "sw_txn_raw"
SUM_TAB = "sw_summary_by_expiry"

SUM_HEADERS = [
    "exp_ymd","txn_count","total_net_amount","total_commissions","total_fees_other","net_after_fees"
]

def sheets_client():
    sheet_id = os.environ["GSHEET_ID"]
    sa_json = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
    creds = service_account.Credentials.from_service_account_info(
        json.loads(sa_json),
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    svc = gbuild("sheets","v4",credentials=creds)
    return svc, sheet_id

def read_tab(svc, sheet_id: str, tab: str) -> List[List[Any]]:
    resp = svc.spreadsheets().values().get(spreadsheetId=sheet_id, range=f"{tab}!A1:ZZ").execute()
    return resp.get("values", []) or []

def write_tab_overwrite(svc, sheet_id: str, tab: str, header: List[str], rows: List[List[Any]]):
    # ensure sheet
    try:
        meta = svc.spreadsheets().get(spreadsheetId=sheet_id).execute()
        titles = [s["properties"]["title"] for s in meta.get("sheets",[])]
        if tab not in titles:
            svc.spreadsheets().batchUpdate(
                spreadsheetId=sheet_id,
                body={"requests":[{"addSheet":{"properties":{"title":tab}}}]}
            ).execute()
    except Exception as e:
        print(f"WARNING: Sheets get/add failed: {e}")
    body = {"values": [header] + rows}
    svc.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=f"{tab}!A1",
        valueInputOption="USER_ENTERED",
        body=body
    ).execute()

def to_float(x):
    try: return float(x)
    except: return 0.0

def main() -> int:
    try:
        svc, sheet_id = sheets_client()
    except Exception as e:
        print(f"ABORT: Sheets init failed — {e}")
        return 1

    data = read_tab(svc, sheet_id, RAW_TAB)
    if not data or len(data) < 2:
        write_tab_overwrite(svc, sheet_id, SUM_TAB, SUM_HEADERS, [])
        print("OK: sw_txn_raw empty; wrote 0 summary rows.")
        return 0

    header = data[0]
    rows   = data[1:]

    # locate needed columns
    def idx(col):
        try: return header.index(col)
        except: return -1

    i_hasopt = idx("has_option")
    i_exp    = idx("exp_primary")
    i_net    = idx("net_amount")
    i_comm   = idx("commissions")
    i_fees   = idx("fees_other")

    if min(i_hasopt, i_exp, i_net, i_comm, i_fees) < 0:
        print("ABORT: sw_txn_raw is missing required columns.")
        return 1

    buckets: Dict[str, Dict[str, float]] = {}

    skipped = 0
    for r in rows:
        if i_hasopt >= len(r) or i_exp >= len(r): 
            skipped += 1
            continue
        try:
            hasopt = int(str(r[i_hasopt]).strip() or "0")
        except:
            hasopt = 0
        exp = str(r[i_exp]).strip() if i_exp < len(r) else ""
        if hasopt != 1: 
            continue
        if not exp or exp == "MIXED_OR_UNKNOWN":
            # we only want single-expiry rows here
            continue

        net  = to_float(r[i_net]  if i_net  < len(r) else 0)
        comm = to_float(r[i_comm] if i_comm < len(r) else 0)
        fees = to_float(r[i_fees] if i_fees < len(r) else 0)

        b = buckets.setdefault(exp, {"n":0,"net":0.0,"comm":0.0,"fees":0.0})
        b["n"]    += 1
        b["net"]  += net
        b["comm"] += comm
        b["fees"] += fees

    out_rows: List[List[Any]] = []
    for exp in sorted(buckets.keys()):
        b = buckets[exp]
        net_after = b["net"] - b["comm"] - b["fees"]
        out_rows.append([
            exp,
            b["n"],
            round(b["net"], 2),
            round(b["comm"], 2),
            round(b["fees"], 2),
            round(net_after, 2)
        ])

    write_tab_overwrite(svc, sheet_id, SUM_TAB, SUM_HEADERS, out_rows)
    print(f"OK: wrote {len(out_rows)} expiry rows into {SUM_TAB}.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
