#!/usr/bin/env python3
# Sheets summarizer:
# - Reads sw_txn_raw (dumped by schwab_dump_all_txns.py).
# - If tab missing or empty → writes empty sw_summary_by_expiry and exits OK.
# - Only summarizes rows with has_option==1 AND a single clear exp_primary (YYYY-MM-DD).
# - Uses Schwab-provided net_amount/commissions/fees_other exactly (no estimates).
# - Writes summary in ONE update (avoids rate limits).

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
    sid = os.environ["GSHEET_ID"]
    sa_json = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
    creds = service_account.Credentials.from_service_account_info(
        json.loads(sa_json),
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    svc = gbuild("sheets","v4",credentials=creds)
    return svc, sid

def list_tabs(svc, sid) -> List[str]:
    meta = svc.spreadsheets().get(spreadsheetId=sid).execute()
    return [s["properties"]["title"] for s in meta.get("sheets",[])]

def ensure_tab_exists(svc, sid, tab: str):
    tabs = list_tabs(svc, sid)
    if tab not in tabs:
        svc.spreadsheets().batchUpdate(
            spreadsheetId=sid,
            body={"requests":[{"addSheet":{"properties":{"title":tab}}}]}
        ).execute()

def read_tab(svc, sid: str, tab: str) -> List[List[Any]]:
    # Caller ensures tab exists
    resp = svc.spreadsheets().values().get(
        spreadsheetId=sid, range=f"{tab}!A1:ZZ"
    ).execute()
    return resp.get("values", []) or []

def write_tab_overwrite(svc, sid: str, tab: str, header: List[str], rows: List[List[Any]]):
    ensure_tab_exists(svc, sid, tab)
    body = {"values": [header] + rows}
    svc.spreadsheets().values().update(
        spreadsheetId=sid,
        range=f"{tab}!A1",
        valueInputOption="USER_ENTERED",
        body=body
    ).execute()

def to_float(x):
    try: return float(x)
    except: return 0.0

def idx(header: List[str], name: str) -> int:
    try: return header.index(name)
    except ValueError: return -1

def main() -> int:
    try:
        svc, sid = sheets_client()
    except Exception as e:
        print(f"ABORT: Sheets init failed — {e}")
        return 1

    # If raw tab missing → create empty raw, write empty summary, exit cleanly
    tabs = list_tabs(svc, sid)
    if RAW_TAB not in tabs:
        print(f"INFO: {RAW_TAB} not found; creating it and writing empty summary.")
        ensure_tab_exists(svc, sid, RAW_TAB)
        write_tab_overwrite(svc, sid, SUM_TAB, SUM_HEADERS, [])
        print("OK: wrote 0 summary rows (no raw data).")
        return 0

    data = read_tab(svc, sid, RAW_TAB)
    if not data or len(data) < 2:
        write_tab_overwrite(svc, sid, SUM_TAB, SUM_HEADERS, [])
        print("OK: sw_txn_raw empty; wrote 0 summary rows.")
        return 0

    header = data[0]
    rows   = data[1:]

    # Column positions expected from schwab_dump_all_txns.py
    i_hasopt = idx(header, "has_option")
    i_exp    = idx(header, "exp_primary")
    i_net    = idx(header, "net_amount")
    i_comm   = idx(header, "commissions")
    i_fees   = idx(header, "fees_other")

    # If the dump hasn't been run yet (or old columns), don't blow up.
    if min(i_hasopt, i_exp, i_net, i_comm, i_fees) < 0:
        print("WARN: sw_txn_raw missing required columns; writing empty summary.")
        write_tab_overwrite(svc, sid, SUM_TAB, SUM_HEADERS, [])
        print("OK: wrote 0 summary rows.")
        return 0

    buckets: Dict[str, Dict[str, float]] = {}

    for r in rows:
        # bounds checks
        if max(i_hasopt, i_exp, i_net, i_comm, i_fees) >= len(r):
            continue
        try:
            hasopt = int(str(r[i_hasopt]).strip() or "0")
        except:
            hasopt = 0
        exp = str(r[i_exp]).strip()
        if hasopt != 1:
            continue
        if not exp or exp == "MIXED_OR_UNKNOWN":
            continue

        net  = to_float(r[i_net])
        comm = to_float(r[i_comm])
        fees = to_float(r[i_fees])

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
            int(b["n"]),
            round(b["net"], 2),
            round(b["comm"], 2),
            round(b["fees"], 2),
            round(net_after, 2)
        ])

    write_tab_overwrite(svc, sid, SUM_TAB, SUM_HEADERS, out_rows)
    print(f"OK: wrote {len(out_rows)} expiry rows into {SUM_TAB}.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
