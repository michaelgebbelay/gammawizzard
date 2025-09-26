#!/usr/bin/env python3
# PURPOSE: Build sw_orig_orders only (Standard "orig" order per expiry) from sw_txn_raw.
# NOTHING ELSE. No settles, no Leo, no 3-way. Minimal, robust.
# Window: business day D-1 of expiry, between ORIG_ET_START..ORIG_ET_END (ET).

import os, json, base64, re
from datetime import datetime, date, time, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from google.oauth2 import service_account
from googleapiclient.discovery import build as gbuild

ET = ZoneInfo("America/New_York")
RAW_TAB = "sw_txn_raw"
OUT_TAB = "sw_orig_orders"
OUT_HEADERS = ["exp_primary","side","short_put","long_put","short_call","long_call","price","contracts"]

# -------- Sheets ----------
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
            spreadsheetId=sid,
            range=f"{tab}!A1",
            valueInputOption="RAW",
            body={"values":[headers]}
        ).execute()

def overwrite_rows(svc, sid: str, tab: str, headers: List[str], rows: List[List[Any]]) -> None:
    svc.spreadsheets().values().clear(spreadsheetId=sid, range=tab).execute()
    svc.spreadsheets().values().update(
        spreadsheetId=sid,
        range=f"{tab}!A1",
        valueInputOption="RAW",
        body={"values":[headers] + rows}
    ).execute()

# -------- helpers ----------
def parse_dt_et(s: str) -> Optional[datetime]:
    if not s: return None
    raw = str(s).strip()
    if not raw: return None
    # normalize "Z" to +00:00
    if raw.endswith("Z"): raw = raw[:-1] + "+00:00"
    # add colon to offset if missing
    if re.search(r"[+-]\d{4}$", raw):
        raw = raw[:-5] + raw[-5:-2] + ":" + raw[-2:]
    try:
        dt = datetime.fromisoformat(raw)
        return dt.astimezone(ET)
    except Exception:
        return None

def prev_business_day(d: date, n: int) -> date:
    # step back n business days (Mon-Fri)
    step = 0
    cur = d
    while step < n:
        cur = cur - timedelta(days=1)
        if cur.weekday() < 5:  # 0..4 Mon..Fri
            step += 1
    return cur

def as_int_strike(x) -> Optional[int]:
    try:
        f=float(x)
        # SPX strikes are 5-point grid; cast safely
        return int(round(f))
    except Exception:
        return None

def safe_float(x) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None

# -------- core ----------
def build_orig_from_raw(svc, sid: str) -> None:
    # read raw values
    resp = svc.spreadsheets().values().get(
        spreadsheetId=sid, range=f"{RAW_TAB}!A1:R"
    ).execute()
    values = resp.get("values", [])
    if not values or values[0][:3] != ["ts","txn_id","type"]:
        # header sanity
        overwrite_rows(svc, sid, OUT_TAB, OUT_HEADERS, [])
        print("ORIG: raw missing or header mismatch; wrote empty.")
        return

    hdr = values[0]
    def idx(col: str) -> int:
        try: return hdr.index(col)
        except ValueError: return -1

    i_ts = idx("ts")
    i_ledger = idx("ledger_id")
    i_exp = idx("exp_primary")
    i_pc = idx("put_call")
    i_strike = idx("strike")
    i_qty = idx("quantity")
    i_amt = idx("amount")
    i_net = idx("net_amount")      # used only if needed
    i_comm = idx("commissions")
    i_fee = idx("fees_other")

    # env window
    et_start = os.environ.get("ORIG_ET_START","16:00").strip()
    et_end   = os.environ.get("ORIG_ET_END","16:20").strip()
    def _hhmm(hm: str) -> time:
        try:
            h,m = [int(x) for x in hm.split(":")]
            return time(hour=h, minute=m, tzinfo=ET)
        except Exception:
            return time(hour=16, minute=0, tzinfo=ET)
    T0 = _hhmm(et_start); T1 = _hhmm(et_end)
    days_before = int(os.environ.get("ORIG_DAYS_BEFORE","1") or "1")

    # Build map: expiry -> [rows]
    rows = [r + [""]*(len(hdr)-len(r)) for r in values[1:]]
    exps = sorted({ (r[i_exp] or "").strip() for r in rows if i_exp>=0 and r[i_exp] }, key=lambda s:s)
    out_rows: List[List[Any]] = []

    for e in exps:
        # compute the "orig day" (business day D-1 by default)
        try:
            exp_d = date.fromisoformat(e[:10])
        except Exception:
            continue
        orig_d = prev_business_day(exp_d, days_before)
        win_start = datetime.combine(orig_d, T0).astimezone(ET)
        win_end   = datetime.combine(orig_d, T1).astimezone(ET)

        # filter rows for this expiry + time window + have ledger_id
        per_ledger: Dict[str, List[List[Any]]] = {}
        for r in rows:
            if i_ledger < 0 or i_exp < 0 or i_ts < 0: continue
            exp_val = (r[i_exp] or "").strip()
            if exp_val != e: continue
            lid = (r[i_ledger] or "").strip()
            if not lid: continue
            dt = parse_dt_et(r[i_ts])
            if not dt: continue
            if not (win_start <= dt <= win_end): continue
            per_ledger.setdefault(lid, []).append(r)

        if not per_ledger:
            # nothing in window → skip expiry
            continue

        # score ledgers and pick best
        cand_entries: List[Tuple[int,int,datetime,str]] = []  # (score, units, latest_ts, ledger_id)
        ledger_calc: Dict[str, Dict[str, Any]] = {}

        for lid, legs in per_ledger.items():
            # dedupe legs within ledger by (pc,strike,qty,price) to avoid row dupes
            seen = set()
            legs_clean = []
            latest_ts = None
            for r in legs:
                pc = (r[i_pc] or "").strip().upper() if i_pc>=0 else ""
                strike = safe_float(r[i_strike]) if i_strike>=0 else None
                qty = safe_float(r[i_qty]) if i_qty>=0 else None
                amt = safe_float(r[i_amt]) if i_amt>=0 else None
                key = (pc, strike, qty, amt)
                if key in seen: continue
                seen.add(key)
                legs_clean.append((pc,strike,qty,amt))
                dt = parse_dt_et(r[i_ts])
                if dt and (latest_ts is None or dt > latest_ts):
                    latest_ts = dt
            # Validate condor: need 2 puts + 2 calls with strikes
            puts = [(s,q,a) for (pc,s,q,a) in legs_clean if pc=="PUT" and s is not None and q is not None]
            calls= [(s,q,a) for (pc,s,q,a) in legs_clean if pc=="CALL" and s is not None and q is not None]
            if len(puts) < 2 or len(calls) < 2:
                continue
            # choose the two most distinct strikes per side
            def pick2(pair_list):
                uniq = {}
                for s,q,a in pair_list:
                    uniq.setdefault(s, []).append((q,a))
                # pick top two distinct strikes by total abs qty, then by strike
                scored = []
                for s, qa in uniq.items():
                    qsum = sum(abs(q or 0) for q,_ in qa)
                    scored.append((qsum, s))
                scored.sort(reverse=True)
                strikes = [s for _,s in scored[:2]]
                # build back the (s, total_qty, total_amt)
                out=[]
                for s in strikes:
                    qq = sum(q for q,_ in uniq[s] if q is not None)
                    aa = sum(a for _,a in uniq[s] if a is not None)
                    out.append((s, qq, aa))
                # ensure exactly two
                if len(out)==1:
                    out.append(out[0])
                return out[:2]
            p2 = pick2(puts)
            c2 = pick2(calls)

            # units = min abs qty across the 4 legs (rounded to int)
            qtys = [abs(q or 0) for (_,q,_) in p2 + c2]
            if not qtys or min(qtys) <= 0:
                continue
            units = int(round(min(qtys)))

            # total leg amount (sum over all legs used) — exclude fees/commissions
            # amount is qty * price * 100 with sign; sum amounts can be +/-.
            amt_sum = 0.0
            valid_amt = True
            for (_,q,a) in p2 + c2:
                if a is None:
                    valid_amt = False
                    break
                amt_sum += a
            if not valid_amt or units <= 0:
                # fallback to ledger net - fees - comm to approximate premium
                # (only if first rows carry net/comm/fee)
                net = None; comm=0.0; fee=0.0
                for r in legs:
                    if net is None and i_net>=0 and r[i_net] not in ("",None):
                        net = safe_float(r[i_net])
                    if i_comm>=0 and r[i_comm] not in ("",None):
                        comm = max(comm, safe_float(r[i_comm]) or 0.0)
                    if i_fee>=0 and r[i_fee] not in ("",None):
                        fee = max(fee, safe_float(r[i_fee]) or 0.0)
                if net is None or units<=0:
                    continue
                # net includes fees; get gross premium
                amt_sum = (net + comm + fee) * 1.0 * 100.0  # scale to leg-amount basis
            # per-unit price (positive)
            price = round(abs(amt_sum) / (units * 100.0), 2)

            # determine side from net premium sign:
            side = "short" if (amt_sum < 0) else "long"  # sums of leg amounts: sells negative; credit → amt_sum < 0

            # identify short/long legs by sign of qty
            def split_short_long(two):
                # two = [(strike, qty, amt), (strike, qty, amt)]
                s_short = None; s_long = None
                # prefer sign: short if qty < 0, long if qty > 0
                neg = [s for (s,q,_) in two if (q or 0) < 0]
                pos = [s for (s,q,_) in two if (q or 0) > 0]
                if neg: s_short = neg[0]
                if pos: s_long = pos[0]
                # fallback by strike ordering if ambiguous
                if s_short is None or s_long is None:
                    strikes = sorted([s for (s,_,_) in two])
                    if side == "short":  # typical credit IC: short put is higher of the two puts; short call is lower of the two calls
                        s_short = s_short if s_short is not None else strikes[-1]
                        s_long  = s_long  if s_long  is not None else strikes[0]
                    else:  # debit IC (long): reverse
                        s_short = s_short if s_short is not None else strikes[0]
                        s_long  = s_long  if s_long  is not None else strikes[-1]
                return (as_int_strike(s_short), as_int_strike(s_long))

            sp, lp = split_short_long(p2)  # put side
            sc, lc = split_short_long(c2)  # call side

            # score: prefer complete (4 legs) and higher units, then latest ts
            score = 1  # we only consider 2 puts + 2 calls candidates
            ledger_calc[lid] = {
                "side": side, "sp": sp, "lp": lp, "sc": sc, "lc": lc,
                "price": price, "units": units
            }
            cand_entries.append((score, units, latest_ts or datetime.min.replace(tzinfo=ET), lid))

        if not cand_entries:
            continue

        cand_entries.sort(key=lambda x: (x[0], x[1], x[2]))  # score, units, ts
        _, _, _, best_lid = cand_entries[-1]
        best = ledger_calc.get(best_lid)
        if not best: 
            continue

        out_rows.append([
            e,
            best["side"],
            best["sp"] if best["sp"] is not None else "",
            best["lp"] if best["lp"] is not None else "",
            best["sc"] if best["sc"] is not None else "",
            best["lc"] if best["lc"] is not None else "",
            "{:.2f}".format(best["price"]),
            str(int(best["units"]))
        ])

    # sort by expiry ascending
    out_rows.sort(key=lambda r: r[0])
    ensure_tab_with_header(svc, sid, OUT_TAB, OUT_HEADERS)
    overwrite_rows(svc, sid, OUT_TAB, OUT_HEADERS, out_rows)
    print(f"ORIG: wrote {len(out_rows)} rows to {OUT_TAB}.")

def main() -> int:
    svc, sid = sheets_client()
    build_orig_from_raw(svc, sid)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
