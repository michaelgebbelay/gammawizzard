#!/usr/bin/env python3
# Dump Schwab account ledger → Sheets: sw_txn_raw (per-leg, per-fill, de-duped), no summary here.

import os, json, base64, re, math
from datetime import datetime, timedelta, timezone, date
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo
from schwab.auth import client_from_token_file
from google.oauth2 import service_account
from googleapiclient.discovery import build as gbuild

ET = ZoneInfo("America/New_York")
RAW_TAB = "sw_txn_raw"
RAW_HEADERS = [
    "ts","txn_id","type","sub_type","description",
    "symbol","underlying","exp_primary","strike","put_call",
    "quantity","price","amount","net_amount","commissions","fees_other",
    "source","ledger_id"
]

def _sa():
    sj=os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
    try:
        dec=base64.b64decode(sj).decode("utf-8")
        if dec.strip().startswith("{"): sj=dec
    except Exception: pass
    creds=service_account.Credentials.from_service_account_info(json.loads(sj),
        scopes=["https://www.googleapis.com/auth/spreadsheets"])
    svc=gbuild("sheets","v4",credentials=creds)
    sid=os.environ["GSHEET_ID"]
    return svc, sid

def ensure_tab_with_header(svc,sid,tab,headers):
    meta=svc.spreadsheets().get(spreadsheetId=sid).execute()
    names={s["properties"]["title"] for s in meta.get("sheets",[])}
    if tab not in names:
        svc.spreadsheets().batchUpdate(spreadsheetId=sid, body={"requests":[{"addSheet":{"properties":{"title":tab}}}]}).execute()
    got=svc.spreadsheets().values().get(spreadsheetId=sid, range=f"{tab}!1:1").execute().get("values",[])
    if not got or got[0]!=headers:
        svc.spreadsheets().values().update(spreadsheetId=sid, range=f"{tab}!A1",
            valueInputOption="RAW", body={"values":[headers]}).execute()

def read_existing(svc,sid,tab,headers):
    last_col=chr(ord("A")+len(headers)-1)
    vals=svc.spreadsheets().values().get(spreadsheetId=sid, range=f"{tab}!A2:{last_col}").execute().get("values",[])
    out=[]
    for r in vals:
        row=list(r)+[""]*(len(headers)-len(r))
        out.append(row[:len(headers)])
    return out

def overwrite_rows(svc,sid,tab,headers,rows):
    svc.spreadsheets().values().clear(spreadsheetId=sid, range=tab).execute()
    svc.spreadsheets().values().update(spreadsheetId=sid, range=f"{tab}!A1",
        valueInputOption="RAW", body={"values":[headers]+rows}).execute()

def merge_rows(existing,new,headers):
    def idx(c): return headers.index(c) if c in headers else -1
    i_ledger=idx("ledger_id"); i_symbol=idx("symbol"); i_qty=idx("quantity"); i_price=idx("price"); i_amt=idx("amount"); i_ts=idx("ts")
    def key(r):
        ledger = (r[i_ledger].strip() if 0<=i_ledger<len(r) else "")
        if ledger:
            return ("ledger", ledger, r[i_symbol], r[i_qty], r[i_price], r[i_amt])
        return tuple(r)
    merged={}
    for r in existing: merged[key(r)]=r
    for r in new:      merged[key(r)]=r
    def ts_key(r):
        try:
            return datetime.fromisoformat(str(r[i_ts]).replace("Z","+00:00"))
        except Exception:
            return datetime.min.replace(tzinfo=timezone.utc)
    out=list(merged.values()); out.sort(key=ts_key, reverse=True)
    return out

def _token_to_file():
    tj=os.environ["SCHWAB_TOKEN_JSON"]
    try:
        dec=base64.b64decode(tj).decode("utf-8")
        if dec.strip().startswith("{"): tj=dec
    except Exception: pass
    with open("schwab_token.json","w") as f: f.write(tj)
    return "schwab_token.json"

def schwab_client():
    c=client_from_token_file(api_key=os.environ["SCHWAB_APP_KEY"],
                             app_secret=os.environ["SCHWAB_APP_SECRET"],
                             token_path=_token_to_file())
    r=c.get_account_numbers(); r.raise_for_status()
    acct_hash=r.json()[0]["hashValue"]
    return c, acct_hash

def _iso_fix(s: str) -> str:
    x=str(s).strip()
    if x.endswith("Z"): return x[:-1]+"+00:00"
    m=re.search(r"[+-]\d{4}$", x)
    if m: return x[:-5]+x[-5:-2]+":"+x[-2:]
    return x

def fmt_ts_et(s: str) -> str:
    try:
        dt=datetime.fromisoformat(_iso_fix(s))
    except Exception:
        return s
    dt=dt.astimezone(ET)
    z=dt.strftime("%Y-%m-%dT%H:%M:%S%z"); return f"{z[:-2]}:{z[-2:]}"

def safe_float(x):
    try: return float(x)
    except Exception: return None

def parse_exp(sym: str) -> Optional[str]:
    if not sym: return None
    s=sym.upper().replace(" ","")
    m=re.search(r"\D(\d{6})[CP]\d", s)
    if m:
        try: return datetime.strptime(m.group(1), "%y%m%d").date().isoformat()
        except Exception: return None
    m=re.search(r"(20\d{2}-\d{2}-\d{2})", s)
    return m.group(1) if m else None

def parse_pc(sym: str) -> Optional[str]:
    if not sym: return None
    s=sym.upper().replace(" ",""); m=re.search(r"\d{6}([CP])\d{8}$", s)
    if m: return "CALL" if m.group(1)=="C" else "PUT"
    return None

def parse_strike(sym: str) -> Optional[float]:
    if not sym: return None
    s=sym.upper().replace(" ",""); m=re.search(r"[CP](\d{8})$", s)
    if not m: return None
    try: return int(m.group(1))/1000.0
    except Exception: return None

def to_underlying(sym: str, hint: str="") -> str:
    u=(hint or "").upper(); 
    if u: return "SPX" if u.startswith("SPX") else u
    s=(sym or "").strip().upper(); 
    if not s: return ""
    p0=s.split()[0]
    return "SPX" if p0.startswith("SPX") else p0

def contract_multiplier(symbol: str, underlying: str) -> int:
    s=(symbol or "").upper(); u=(underlying or "").upper()
    if re.search(r"\d{6}[CP]\d{8}$", s): return 100
    if u in {"SPX","SPXW","NDX","RUT","VIX","XSP"}: return 100
    return 1

def amount_from(qty: Optional[float], price: Optional[float], symbol: str, underlying: str):
    if qty is None or price is None: return None
    return round(qty*price*contract_multiplier(symbol, underlying), 2)

def list_transactions(c, acct_hash, t0: datetime, t1: datetime):
    r=c.get_transactions(acct_hash, start_date=t0, end_date=t1)
    if getattr(r,"status_code",None)==204: return []
    r.raise_for_status(); j=r.json()
    return j if isinstance(j,list) else []

def rows_from_ledger(txn: Dict[str,Any]) -> List[List[Any]]:
    ttype=str(txn.get("type") or txn.get("transactionType") or "")
    subtype=str(txn.get("subType") or "")
    desc=str(txn.get("description") or "")
    ts=fmt_ts_et(str(txn.get("time") or txn.get("transactionDate") or txn.get("date") or ""))
    order_id=str(txn.get("orderId") or "")
    transaction_id=str(txn.get("transactionId") or "")
    txn_id_for_sheet = order_id or transaction_id
    ledger_id = str(txn.get("activityId") or "") or transaction_id or ts

    comm_total=0.0; fees_total=0.0
    for ti in (txn.get("transferItems") or []):
        ft=str(ti.get("feeType") or "")
        if not ft: continue
        val=safe_float(ti.get("cost")) or safe_float(ti.get("amount")) or 0.0
        if "comm" in ft.lower(): comm_total += abs(val)
        else: fees_total += abs(val)
    comm_total=round(comm_total,2); fees_total=round(fees_total,2)

    out=[]; seen=set()
    for it in (txn.get("transferItems") or []):
        ins=it.get("instrument") or {}
        if (str(ins.get("assetType") or "").upper()!="OPTION"): 
            continue
        symbol=str(ins.get("symbol") or "")
        underlying=to_underlying(symbol, ins.get("underlyingSymbol") or "")
        if underlying.upper().startswith("SPX"): underlying="SPX"
        exp_primary = parse_exp(symbol) or ""
        strike = ins.get("strikePrice") or parse_strike(symbol)
        pc = ins.get("putCall") or parse_pc(symbol) or ""
        pc = "CALL" if str(pc).upper().startswith("C") else ("PUT" if str(pc).upper().startswith("P") else "")
        raw_qty=safe_float(it.get("amount"))
        if raw_qty is not None and raw_qty!=0:
            qty=raw_qty
        else:
            qty=safe_float(it.get("quantity")) or 0.0
            instr=str(it.get("instruction") or "").upper()
            qty = -abs(qty) if instr.startswith("SELL") else abs(qty)
        price=safe_float(it.get("price"))
        amt=amount_from(qty, price, symbol, underlying)

        leg_key=(symbol, exp_primary, pc, strike, round(qty or 0.0,6), round(price or 0.0,6))
        if leg_key in seen: 
            continue
        seen.add(leg_key)
        out.append([
            ts, txn_id_for_sheet, ttype, subtype, desc,
            symbol, underlying, (exp_primary or ""), (strike if strike is not None else ""), pc,
            (qty if qty is not None else ""), (price if price is not None else ""), (amt if amt is not None else ""),
            (txn.get("netAmount") if txn.get("netAmount") is not None else ""), comm_total or "", fees_total or "",
            "schwab_ledger", ledger_id
        ])
    return out

def main():
    svc,sid=_sa()
    ensure_tab_with_header(svc,sid,RAW_TAB,RAW_HEADERS)

    c, acct_hash = schwab_client()

    backfill = str(os.environ.get("BACKFILL_YTD","0")).strip().lower() in {"1","true","yes","on"}
    if backfill:
        now_et = datetime.now(ET)
        start_dt = now_et.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc)
        end_dt   = now_et.astimezone(timezone.utc)
    else:
        try:
            days_back=int((os.environ.get("DAYS_BACK") or "4").strip())
        except Exception:
            days_back=4
        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(days=days_back)

    txns=list_transactions(c, acct_hash, start_dt, end_dt)

    rows=[]
    for t in txns:
        try:
            rows.extend(rows_from_ledger(t))
        except Exception as e:
            tid=t.get("transactionId") or t.get("orderId") or "<?>"
            print(f"WARN skip ledger {tid}: {e}")

    existing=read_existing(svc,sid,RAW_TAB,RAW_HEADERS)
    merged=merge_rows(existing, rows, RAW_HEADERS)
    overwrite_rows(svc,sid,RAW_TAB,RAW_HEADERS,merged)
    print(f"OK: wrote {len(merged)} rows to {RAW_TAB} from {len(txns)} ledger activities.")
    return 0

if __name__=="__main__":
    raise SystemExit(main())
