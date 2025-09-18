#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Build sw_summary directly from sw_txn_raw (order-level truth).

- Reads the sheet tab sw_txn_raw (the rows your RAW loader writes).
- Dedupes identical legs (same order, symbol, strike, pc, qty, price, source).
- Collapses to per-order:
    gross  = sum(amount) across legs (your RAW sign convention)
    fees   = -(gross + sum(ledger net_amounts)) if ledger nets exist for the order
             else one snapshot of (commissions + fees_other) for the order
    net    = sum(ledger net_amounts) if present; otherwise: -gross - fees
    legs, contracts_abs, contracts_net as simple counts/sums
- Rolls up by (exp_primary, underlying) and writes sw_summary.
"""

import os, json, base64
from collections import defaultdict
from typing import Any, Dict, List

from google.oauth2 import service_account
from googleapiclient.discovery import build as gbuild

RAW_TAB = "sw_txn_raw"
SUMMARY_TAB = "sw_summary"
SUMMARY_HEADERS = [
    "exp_primary","underlying",
    "num_orders","legs","contracts_net","contracts_abs",
    "premium_gross","fees_alloc","premium_net"
]

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

def ensure_tab_with_header(svc, sid: str, tab: str, headers: List[str]) -> None:
    meta = svc.spreadsheets().get(spreadsheetId=sid).execute()
    titles = {s["properties"]["title"] for s in meta.get("sheets",[])}
    if tab not in titles:
        svc.spreadsheets().batchUpdate(
            spreadsheetId=sid,
            body={"requests":[{"addSheet":{"properties":{"title":tab}}}]}
        ).execute()
    got = svc.spreadsheets().values().get(spreadsheetId=sid, range=f"{tab}!1:1").execute().get("values",[])
    if not got or got[0] != headers:
        svc.spreadsheets().values().update(
            spreadsheetId=sid, range=f"{tab}!A1",
            valueInputOption="RAW",
            body={"values":[headers]}
        ).execute()

def overwrite_rows(svc, sid: str, tab: str, headers: List[str], rows: List[List[Any]]) -> None:
    svc.spreadsheets().values().update(
        spreadsheetId=sid,
        range=f"{tab}!A1",
        valueInputOption="RAW",
        body={"values":[headers] + rows}
    ).execute()

def read_raw(svc, sid: str) -> (List[str], List[List[Any]]):
    resp = svc.spreadsheets().values().get(spreadsheetId=sid, range=f"{RAW_TAB}!A1:Q").execute()
    vals = resp.get("values", [])
    if not vals:
        return [], []
    headers = vals[0]
    rows = vals[1:]
    return headers, rows

def idxmap(headers: List[str]) -> Dict[str,int]:
    return {h:i for i,h in enumerate(headers)}

def f2(x):
    try:
        if isinstance(x, str): x = x.replace(",", "")
        return float(x)
    except Exception:
        return None

def dedupe_rows(rows, H):
    I = idxmap(H)
    seen = set()
    out = []
    for r in rows:
        # pad row if short
        if len(r) < len(H):
            r = r + [""]*(len(H)-len(r))
        key = (
            str(r[I["txn_id"]]).strip(),
            str(r[I["symbol"]]).strip().upper(),
            str(r[I["exp_primary"]]).strip(),
            round(f2(r[I["strike"]]) or 0.0, 3),
            str(r[I["put_call"]]).strip().upper(),
            round(f2(r[I["quantity"]]) or 0.0, 6),
            round(f2(r[I["price"]]) or 0.0, 6),
            str(r[I["source"]]).strip()
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out

def build_summary_from_raw(rows, H):
    I = idxmap(H)
    # Per-order accumulator
    orders: Dict[str, Dict[str, Any]] = {}

    for r in rows:
        oid = str(r[I["txn_id"]] or "").strip()
        if not oid:
            # synthesize to avoid losing rows with missing id
            oid = f"AUTO-{r[I['ts']]}-{r[I['symbol']]}-{r[I['price']]}"

        exp = str(r[I["exp_primary"]] or "").strip()
        und = str(r[I["underlying"]] or "").strip().upper() or "SPX"

        qty = f2(r[I["quantity"]]) or 0.0
        amt = f2(r[I["amount"]])
        if amt is None:
            # reconstruct if needed
            price = f2(r[I["price"]]) or 0.0
            # assume index options (×100) unless proven otherwise
            mult = 100 if und in {"SPX","SPXW","NDX","RUT","VIX","XSP"} or "SPX" in str(r[I["symbol"]]).upper() else 1
            amt = round(qty * price * mult, 2)

        net_amt = f2(r[I.get("net_amount", -1)])  # ledger ‘net’ if present
        comm = f2(r[I.get("commissions", -1)]) or 0.0
        fees = f2(r[I.get("fees_other", -1)]) or 0.0

        d = orders.get(oid)
        if not d:
            d = orders[oid] = {
                "exp": exp, "und": und,
                "legs": 0,
                "abs": 0.0, "net_qty": 0.0,
                "gross": 0.0,
                "ledger_net_sum": 0.0,
                "has_ledger_net": False,
                "fee_snaps": []
            }
        if exp: d["exp"] = exp
        if und: d["und"] = und
        d["legs"] += 1
        d["abs"] += abs(qty)
        d["net_qty"] += qty
        d["gross"] += (amt or 0.0)
        if net_amt is not None:
            d["ledger_net_sum"] += net_amt
            d["has_ledger_net"] = True
        # keep one or more snapshots; we will *not* sum them to avoid double counting
        if (comm or fees):
            d["fee_snaps"].append(comm + fees)

    # Finalize per-order and roll up by (exp, und)
    by_day: Dict[tuple, Dict[str, Any]] = {}

    for d in orders.values():
        exp = d["exp"] or ""
        und = d["und"] or ""
        gross = round(d["gross"], 2)

        if d["has_ledger_net"]:
            net = round(d["ledger_net_sum"], 2)
            fees_total = round(-(gross + net), 2)  # net = -gross - fees ⇒ fees = -(gross + net)
        else:
            # take a single plausible order-level fee snapshot (max avoids per-leg duplicates)
            fees_total = round(max(d["fee_snaps"]) if d["fee_snaps"] else 0.0, 2)
            net = round(-gross - fees_total, 2)

        k = (exp, und)
        agg = by_day.get(k)
        if not agg:
            agg = by_day[k] = {
                "num_orders": 0, "legs": 0,
                "contracts_net": 0.0, "contracts_abs": 0.0,
                "gross": 0.0, "fees": 0.0, "net": 0.0
            }
        agg["num_orders"] += 1
        agg["legs"] += d["legs"]
        agg["contracts_net"] += d["net_qty"]
        agg["contracts_abs"] += d["abs"]
        agg["gross"] += gross
        agg["fees"] += fees_total
        agg["net"] += net

    # Format for sheet
    out_rows: List[List[str]] = []
    for (exp, und) in sorted(by_day.keys()):
        s = by_day[(exp, und)]
        out_rows.append([
            exp, und,
            str(int(s["num_orders"])),
            str(int(s["legs"])),
            f'{s["contracts_net"]:.2f}',
            f'{s["contracts_abs"]:.2f}',
            f'{s["gross"]:.2f}',
            f'{s["fees"]:.2f}',
            f'{s["net"]:.2f}',
        ])
    return out_rows

def main():
    svc, sid = sheets_client()
    headers, rows = read_raw(svc, sid)
    if not rows:
        ensure_tab_with_header(svc, sid, SUMMARY_TAB, SUMMARY_HEADERS)
        overwrite_rows(svc, sid, SUMMARY_TAB, SUMMARY_HEADERS, [])
        print("SUMMARY: no RAW rows found; wrote header only.")
        return 0

    rows = dedupe_rows(rows, headers)
    ensure_tab_with_header(svc, sid, SUMMARY_TAB, SUMMARY_HEADERS)
    summary_rows = build_summary_from_raw(rows, headers)
    overwrite_rows(svc, sid, SUMMARY_TAB, SUMMARY_HEADERS, summary_rows)
    print(f"SUMMARY: wrote {len(summary_rows)} rows.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
