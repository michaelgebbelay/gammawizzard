#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Schwab → Sheets RAW dump (ledger-only, idempotent upsert, tiny fetch windows).

One-time: BACKFILL_YTD=1   → pulls from Jan 1 this year (ET) to now
Daily:    DAYS_BACK=4       → pulls last 4 days only (default)

Tabs:
  - sw_txn_raw   (upserted/merged; header preserved)
  - sw_dump_log  (append-only audit)

Env:
  GSHEET_ID, GOOGLE_SERVICE_ACCOUNT_JSON
  SCHWAB_APP_KEY, SCHWAB_APP_SECRET, SCHWAB_TOKEN_JSON
  BACKFILL_YTD (0/1), DAYS_BACK (default 4)
"""
import base64, json, os, re, math, sys
from datetime import datetime, timedelta, timezone, date
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from schwab.auth import client_from_token_file
from google.oauth2 import service_account
from googleapiclient.discovery import build as gbuild

ET = ZoneInfo("America/New_York")
RAW_TAB = "sw_txn_raw"
LOG_TAB = "sw_dump_log"

RAW_HEADERS = [
    "ts","txn_id","type","sub_type","description",
    "symbol","underlying","exp_primary","strike","put_call",
    "quantity","price","amount","net_amount","commissions","fees_other",
    "source","ledger_id"
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
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gbuild("sheets","v4",credentials=creds), sid

def ensure_tab_with_header(svc, sid: str, tab: str, headers: List[str]) -> None:
    meta = svc.spreadsheets().get(spreadsheetId=sid).execute()
    titles = {s["properties"]["title"] for s in meta.get("sheets",[])}
    if tab not in titles:
        svc.spreadsheets().batchUpdate(spreadsheetId=sid, body={"requests":[{"addSheet":{"properties":{"title":tab}}}]}).execute()
    cur = svc.spreadsheets().values().get(spreadsheetId=sid, range=f"{tab}!1:1").execute().get("values",[])
    if not cur or cur[0] != headers:
        svc.spreadsheets().values().update(spreadsheetId=sid, range=f"{tab}!A1",
            valueInputOption="RAW", body={"values":[headers]}).execute()

def ensure_tab_exists(svc, sid: str, tab: str) -> None:
    meta = svc.spreadsheets().get(spreadsheetId=sid).execute()
    titles = {s["properties"]["title"] for s in meta.get("sheets",[])}
    if tab not in titles:
        svc.spreadsheets().batchUpdate(spreadsheetId=sid, body={"requests":[{"addSheet":{"properties":{"title":tab}}}]}).execute()

def read_rows(svc, sid: str, tab: str, headers: List[str]) -> List[List[Any]]:
    resp = svc.spreadsheets().values().get(spreadsheetId=sid, range=f"{tab}!A2:{chr(ord('A')+len(headers)-1)}").execute()
    vals = resp.get("values", [])
    out=[]
    for r in vals:
        rr=list(r)
        if len(rr)<len(headers): rr += [""]*(len(headers)-len(rr))
        out.append(rr[:len(headers)])
    return out

def write_all_rows(svc, sid: str, tab: str, headers: List[str], rows: List[List[Any]]) -> None:
    # rewrite whole tab deterministically (after merge), but only with merged history
    svc.spreadsheets().values().clear(spreadsheetId=sid, range=tab).execute()
    svc.spreadsheets().values().update(spreadsheetId=sid, range=f"{tab}!A1",
        valueInputOption="RAW", body={"values":[headers]+rows}).execute()

def log_run(svc, sid: str, record: List[Any]) -> None:
    ensure_tab_exists(svc, sid, LOG_TAB)
    svc.spreadsheets().values().append(spreadsheetId=sid, range=f"{LOG_TAB}!A1",
        valueInputOption="RAW", insertDataOption="INSERT_ROWS", body={"values":[record]}).execute()

# ---------- Schwab ----------
def decode_token_to_path() -> str:
    token_env = os.environ.get("SCHWAB_TOKEN_JSON","") or ""
    token_path = "schwab_token.json"
    if token_env:
        try:
            dec = base64.b64decode(token_env).decode("utf-8")
            if dec.strip().startswith("{"): token_env = dec
        except Exception:
            pass
        with open(token_path,"w") as f: f.write(token_env)
    return token_path

def schwab_client():
    token_path = decode_token_to_path()
    c = client_from_token_file(token_path, os.environ["SCHWAB_APP_KEY"], os.environ["SCHWAB_APP_SECRET"])
    r = c.get_account_numbers(); r.raise_for_status()
    return c, r.json()[0]["hashValue"]

def to_et_iso(ts: str) -> str:
    s = ts.strip()
    if s.endswith("Z"): s = s[:-1]+"+00:00"
    if re.search(r"[+-]\d{4}$", s): s = s[:-5]+s[-5:-2]+":"+s[-2:]
    try:
        dt = datetime.fromisoformat(s).astimezone(ET)
        z = dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        return f"{z[:-2]}:{z[-2:]}"
    except Exception:
        return ts

def safe_float(x) -> Optional[float]:
    try: return float(x)
    except Exception: return None

def parse_exp(sym: str) -> Optional[str]:
    if not sym: return None
    s = sym.strip().upper().replace(" ","")
    m = re.search(r"\D(\d{6})[CP]\d", s)
    if m:
        try: return datetime.strptime(m.group(1), "%y%m%d").date().isoformat()
        except Exception: pass
    m = re.search(r"(20\d{2}-\d{2}-\d{2})", s)
    return m.group(1) if m else None

def to_underlying(sym: str, hint: str="") -> str:
    u = (hint or "").upper()
    if u: return "SPX" if u.startswith("SPX") else u
    s = (sym or "").split()[0].upper()
    return "SPX" if s.startswith("SPX") else s

def parse_pc(sym: str) -> Optional[str]:
    if not sym: return None
    s = sym.upper().replace(" ","")
    m = re.search(r"\d{6}([CP])\d{8}$", s)
    return "CALL" if (m and m.group(1)=="C") else ("PUT" if m else None)

def parse_strike(sym: str) -> Optional[float]:
    if not sym: return None
    s = sym.upper().replace(" ","")
    m = re.search(r"[CP](\d{8})$", s)
    try: return int(m.group(1))/1000.0 if m else None
    except Exception: return None

def multiplier(symbol: str, underlying: str) -> int:
    s = (symbol or "").upper(); u = (underlying or "").upper()
    if re.search(r"\d{6}[CP]\d{8}$", s): return 100
    if u in {"SPX","SPXW","NDX","RUT","VIX","XSP"}: return 100
    return 1

def calc_amount(qty: Optional[float], price: Optional[float], symbol: str, underlying: str) -> Optional[float]:
    if qty is None or price is None: return None
    return round(qty*price*multiplier(symbol,underlying), 2)

def rows_from_ledger(txn: Dict[str, Any]) -> List[List[Any]]:
    """One row per OPTION leg in transferItems. Attach net/fees ONCE per ledger."""
    ts = to_et_iso(str(txn.get("time") or txn.get("transactionDate") or txn.get("date") or ""))
    ttype = str(txn.get("type") or txn.get("transactionType") or "")
    subtype = str(txn.get("subType") or "")
    desc = str(txn.get("description") or "")
    order_id = str(txn.get("orderId") or "")
    transaction_id = str(txn.get("transactionId") or "")
    txn_id_for_sheet = order_id or transaction_id
    ledger_id = str(txn.get("activityId") or "") or transaction_id or ts

    # fees from transferItems
    comm_total=0.0; fees_total=0.0
    for ti in (txn.get("transferItems") or []):
        ft = str(ti.get("feeType") or "")
        if not ft: continue
        v = safe_float(ti.get("cost")) or safe_float(ti.get("amount")) or 0.0
        if "comm" in ft.lower(): comm_total += abs(v)
        else: fees_total += abs(v)
    comm_total = round(comm_total,2); fees_total = round(fees_total,2)

    rows=[]; seen=set(); attached=False
    for it in (txn.get("transferItems") or []):
        inst = it.get("instrument") or {}
        if str(inst.get("assetType") or "").upper() != "OPTION": continue

        symbol = str(inst.get("symbol") or "")
        underlying = to_underlying(symbol, inst.get("underlyingSymbol") or "")
        if underlying.startswith("SPX"): underlying = "SPX"
        exp_primary = parse_exp(symbol) or ""
        strike = inst.get("strikePrice") or parse_strike(symbol)
        pc = inst.get("putCall") or parse_pc(symbol) or ""
        pc = "CALL" if str(pc).upper().startswith("C") else ("PUT" if str(pc).upper().startswith("P") else "")

        raw_qty = safe_float(it.get("amount"))
        if raw_qty is not None and raw_qty != 0:
            qty = raw_qty
        else:
            qty = safe_float(it.get("quantity")) or 0.0
            instr = str(it.get("instruction") or "").upper()
            qty = -abs(qty) if instr.startswith("SELL") else abs(qty)

        price = safe_float(it.get("price"))
        amt = calc_amount(qty, price, symbol, underlying)

        leg_key = (symbol, exp_primary, pc, strike, round(qty or 0.0,6), round(price or 0.0,6))
        if leg_key in seen: continue
        seen.add(leg_key)

        # attach net/fees ONCE on first leg
        if not attached:
            net_for_row = txn.get("netAmount") if txn.get("netAmount") is not None else ""
            comm_for_row = comm_total or ""
            fees_for_row = fees_total or ""
            attached = True
        else:
            net_for_row = ""; comm_for_row = ""; fees_for_row = ""

        rows.append([
            ts, txn_id_for_sheet, ttype, subtype, desc,
            symbol, underlying, (exp_primary or ""), (strike if strike is not None else ""), pc,
            (qty if qty is not None else ""), (price if price is not None else ""), (amt if amt is not None else ""),
            net_for_row, comm_for_row, fees_for_row,
            "schwab_ledger", ledger_id
        ])
    return rows

def list_transactions(c, acct_hash: str, start_dt: datetime, end_dt: datetime) -> List[Dict[str, Any]]:
    r = c.get_transactions(acct_hash, start_date=start_dt, end_date=end_dt)
    if getattr(r,"status_code",None)==204: return []
    r.raise_for_status()
    j = r.json()
    return j if isinstance(j, list) else []

def merge_rows(existing: List[List[Any]], new: List[List[Any]], headers: List[str]) -> List[List[Any]]:
    """Idempotent merge keyed primarily by ledger_id (falls back to a leg fingerprint)."""
    def idx(col: str) -> int: return headers.index(col)
    i_ledger, i_ts = idx("ledger_id"), idx("ts")
    def key(r: List[Any]) -> tuple:
        # Prefer ledger_id if present; else the whole row
        ledger = (r[i_ledger].strip() if i_ledger < len(r) and isinstance(r[i_ledger], str) else "")
        return ("ledger", ledger) if ledger else tuple(r)
    merged={}
    for r in existing: merged[key(r)] = r
    for r in new:       merged[key(r)] = r

    def sort_key(r: List[Any]):
        try:
            s = str(r[i_ts]).strip()
            if s.endswith("Z"): s = s[:-1]+"+00:00"
            dt = datetime.fromisoformat(s)
        except Exception:
            dt = datetime.min.replace(tzinfo=timezone.utc)
        return dt
    out = list(merged.values())
    out.sort(key=sort_key, reverse=True)
    return out

def main() -> int:
    svc, sid = sheets_client()
    ensure_tab_with_header(svc, sid, RAW_TAB, RAW_HEADERS)
    ensure_tab_exists(svc, sid, LOG_TAB)

    # window selection
    backfill = (os.environ.get("BACKFILL_YTD","0").strip() in {"1","true","yes","on"})
    if backfill:
        now = datetime.now(ET)
        start_et = datetime(now.year, 1, 1, 0, 0, 0, tzinfo=ET)
        start_dt = start_et.astimezone(timezone.utc)
        end_dt = datetime.now(timezone.utc)
        mode = f"YTD {start_et.date()}→{end_dt.date()}"
    else:
        days_back = int((os.environ.get("DAYS_BACK") or "4").strip())
        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(days=days_back)
        mode = f"last {days_back} days"

    # Schwab auth
    try:
        c, acct_hash = schwab_client()
    except Exception as e:
        print(f"ABORT: Schwab auth failed — {e}")
        return 1

    # fetch + parse
    txns = list_transactions(c, acct_hash, start_dt, end_dt)
    new_rows=[]
    for t in txns:
        try:
            new_rows.extend(rows_from_ledger(t))
        except Exception as exc:
            tid = t.get("transactionId") or t.get("orderId") or "<?>"
            print(f"NOTE: failed to parse ledger {tid}: {exc}")

    existing = read_rows(svc, sid, RAW_TAB, RAW_HEADERS)
    before = len(existing)
    merged = merge_rows(existing, new_rows, RAW_HEADERS)
    after = len(merged)
    write_all_rows(svc, sid, RAW_TAB, RAW_HEADERS, merged)

    # audit log
    log_run(svc, sid, [
        datetime.now(ET).strftime("%Y-%m-%d %H:%M:%S"),
        mode, len(txns), len(new_rows), before, after, (after-before)
    ])
    print(f"OK: {mode} — fetched {len(txns)} ledger activities → {len(new_rows)} rows; merged {before}→{after}.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
