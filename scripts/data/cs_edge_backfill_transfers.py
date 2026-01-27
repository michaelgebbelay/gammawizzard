#!/usr/bin/env python3
"""
Backfill ConstantStableState with transfer-adjusted equity metrics.

Required env:
  GSHEET_ID
  GOOGLE_SERVICE_ACCOUNT_JSON
  SCHWAB_APP_KEY
  SCHWAB_APP_SECRET
  SCHWAB_TOKEN_JSON

Optional:
  CS_STATE_TAB          default ConstantStableState
  BACKFILL_START_DATE   default 2025-12-01 (YYYY-MM-DD)
  CS_EDGE_RECOMPUTE_LLR default false
  CS_EDGE_MU0, CS_EDGE_MU1, CS_EDGE_SIGMA, CS_EDGE_ALPHA, CS_EDGE_BETA, CS_EDGE_MIN_SAMPLES, CS_EDGE_PAUSE_DAYS
"""

import os
import sys
import json
import math
from datetime import datetime, timedelta, timezone, date
from zoneinfo import ZoneInfo


def _add_scripts_root():
    cur = os.path.abspath(os.path.dirname(__file__))
    while True:
        if os.path.basename(cur) == "scripts":
            if cur not in sys.path:
                sys.path.append(cur)
            return
        parent = os.path.dirname(cur)
        if parent == cur:
            return
        cur = parent


_add_scripts_root()
from schwab_token_keeper import schwab_client

ET = ZoneInfo("America/New_York")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def truthy(s: str) -> bool:
    return str(s or "").strip().lower() in ("1", "true", "yes", "y", "on")


def safe_float(x, default=None):
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def ffloat_from(v, default=None):
    try:
        s = (v or "").strip()
        if s == "":
            return default
        return float(s)
    except Exception:
        return default


def fint_from(v, default=None):
    try:
        s = (v or "").strip()
        if s == "":
            return default
        return int(float(s))
    except Exception:
        return default


def parse_iso_dt(s: str) -> datetime | None:
    if not s:
        return None
    raw = s.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(raw)
    except Exception:
        return None


def creds_from_env():
    from google.oauth2 import service_account
    raw = (os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip()
    info = json.loads(raw)
    return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)


def build_svc():
    from googleapiclient.discovery import build
    return build("sheets", "v4", credentials=creds_from_env(), cache_discovery=False)


def read_all(svc, sid: str, title: str):
    r = svc.spreadsheets().values().get(spreadsheetId=sid, range=f"{title}!A1:ZZ").execute()
    return r.get("values") or []


def write_all(svc, sid: str, title: str, values: list[list[str]]):
    svc.spreadsheets().values().update(
        spreadsheetId=sid,
        range=f"{title}!A1",
        valueInputOption="RAW",
        body={"values": values},
    ).execute()


def get_account_hash(c):
    r = c.get_account_numbers()
    r.raise_for_status()
    arr = r.json() or []
    info = arr[0] if arr else {}
    return str(info.get("hashValue") or info.get("hashvalue") or "")


def list_transactions(c, acct_hash: str, t0: datetime, t1: datetime):
    r = c.get_transactions(acct_hash, start_date=t0, end_date=t1)
    if getattr(r, "status_code", None) == 204:
        return []
    r.raise_for_status()
    j = r.json()
    return j if isinstance(j, list) else []


def is_transfer_txn(txn: dict) -> bool:
    ttype = str(txn.get("type") or txn.get("transactionType") or "").upper()
    subtype = str(txn.get("subType") or "").upper()
    desc = str(txn.get("description") or "").upper()
    text = " ".join([ttype, subtype, desc])

    exclude = ("DIVIDEND", "INTEREST", "TRADE", "OPTION", "BUY", "SELL", "EXERCISE", "ASSIGNMENT")
    if any(x in text for x in exclude):
        return False

    include = ("TRANSFER", "WIRE", "ACH", "EFT", "DEPOSIT", "WITHDRAW", "JOURNAL", "CONTRIBUTION", "DISTRIBUTION")
    if any(x in text for x in include):
        return True

    if txn.get("orderId"):
        return False

    items = txn.get("transferItems") or []
    if items:
        for it in items:
            ins = it.get("instrument") or {}
            asset = str(ins.get("assetType") or "").upper()
            if asset == "OPTION":
                return False
        return True

    return False


def parse_txn_time(txn: dict) -> datetime | None:
    for k in ("time", "transactionDate", "date"):
        v = txn.get(k)
        if v:
            dt = parse_iso_dt(str(v))
            if dt:
                return dt.astimezone(timezone.utc)
    return None


def llr_increment_normal(x: float, mu0: float, mu1: float, sigma: float) -> float:
    return ((mu0 * mu0 - mu1 * mu1) + 2.0 * x * (mu1 - mu0)) / (2.0 * sigma * sigma)


def main():
    sid = (os.environ.get("GSHEET_ID") or "").strip()
    tab = (os.environ.get("CS_STATE_TAB") or "ConstantStableState").strip()
    start_raw = (os.environ.get("BACKFILL_START_DATE") or "2025-12-01").strip()
    start_date = date.fromisoformat(start_raw[:10])
    recompute_llr = truthy(os.environ.get("CS_EDGE_RECOMPUTE_LLR", "false"))

    svc = build_svc()
    data = read_all(svc, sid, tab)
    if not data or len(data) < 2:
        print("No data to backfill.")
        return 0

    header = list(data[0])
    required = [
        "transfer_net", "transfer_cum", "equity_adj", "prev_equity_adj",
    ]
    for col in required:
        if col not in header:
            header.append(col)

    idx = {h: i for i, h in enumerate(header)}

    def get(row, key, default=""):
        if row is None:
            return default
        j = idx.get(key)
        return row[j] if j is not None and j < len(row) else default

    def setv(row, key, val):
        j = idx.get(key)
        if j is None:
            return
        while len(row) <= j:
            row.append("")
        row[j] = val

    # Build row metadata
    rows = []
    for i, row in enumerate(data[1:], start=2):
        if not row:
            continue
        run_date = get(row, "run_date", "").strip()
        ts_utc = get(row, "ts_utc", "").strip()
        d = None
        if run_date:
            try:
                d = date.fromisoformat(run_date[:10])
            except Exception:
                d = None
        if d is None and ts_utc:
            dt = parse_iso_dt(ts_utc)
            d = dt.date() if dt else None
        rows.append({"rownum": i, "row": row, "run_date": d, "ts_utc": ts_utc})

    # Determine range to backfill
    start_idx = None
    for i, r in enumerate(rows):
        if r["run_date"] and r["run_date"] >= start_date:
            start_idx = i
            break
    if start_idx is None:
        print("No rows on/after start date.")
        return 0

    # Fetch transfers once for the full time range
    t_start = None
    t_end = None
    for r in rows[start_idx:]:
        dt = parse_iso_dt(r["ts_utc"])
        if not dt:
            continue
        dt = dt.astimezone(timezone.utc)
        if t_start is None or dt < t_start:
            t_start = dt
        if t_end is None or dt > t_end:
            t_end = dt
    if t_start is None or t_end is None:
        print("No valid timestamps to backfill.")
        return 0

    c = schwab_client()
    acct_hash = get_account_hash(c)

    txns = list_transactions(c, acct_hash, t_start, t_end + timedelta(minutes=1))
    transfers = []
    for t in txns:
        if not is_transfer_txn(t):
            continue
        tdt = parse_txn_time(t)
        if not tdt:
            continue
        amt = safe_float(t.get("netAmount"))
        if amt is None:
            continue
        transfers.append((tdt, float(amt)))
    transfers.sort(key=lambda x: x[0])

    def transfers_in_window(t0: datetime, t1: datetime) -> float:
        total = 0.0
        for dt, amt in transfers:
            if dt <= t0:
                continue
            if dt > t1:
                break
            total += amt
        return round(total, 2)

    # Seed from prior row (if it has transfer_cum)
    prev_row = rows[start_idx - 1]["row"] if start_idx > 0 else None
    prev_cum = ffloat_from(get(prev_row, "transfer_cum", "0"), default=0.0) if prev_row else 0.0
    prev_adj = ffloat_from(get(prev_row, "equity_adj", ""), default=None)
    prev_peak = ffloat_from(get(prev_row, "peak_equity", ""), default=None)

    # Edge test params (optional)
    mu0 = safe_float(os.environ.get("CS_EDGE_MU0"), 0.0)
    mu1 = safe_float(os.environ.get("CS_EDGE_MU1"))
    sigma = safe_float(os.environ.get("CS_EDGE_SIGMA"))
    alpha = safe_float(os.environ.get("CS_EDGE_ALPHA"), 0.01)
    beta = safe_float(os.environ.get("CS_EDGE_BETA"), 0.05)
    min_n = int(safe_float(os.environ.get("CS_EDGE_MIN_SAMPLES"), 30) or 30)
    pause_days = int(safe_float(os.environ.get("CS_EDGE_PAUSE_DAYS"), 5) or 5)

    prev_llr = ffloat_from(get(prev_row, "llr", ""), default=0.0) if prev_row else 0.0
    prev_n = fint_from(get(prev_row, "n", ""), default=0) if prev_row else 0
    pause_until = get(prev_row, "pause_until", "") if prev_row else ""

    # Apply backfill
    for i in range(start_idx, len(rows)):
        r = rows[i]
        row = r["row"]
        dt = parse_iso_dt(r["ts_utc"])
        if not dt:
            continue
        dt = dt.astimezone(timezone.utc)

        # prev timestamp in window (use prior row timestamp if available)
        prev_dt = None
        if i > 0:
            prev_dt = parse_iso_dt(rows[i - 1]["ts_utc"])
        if prev_dt:
            prev_dt = prev_dt.astimezone(timezone.utc)
        else:
            prev_dt = dt - timedelta(days=1)

        transfer_net = transfers_in_window(prev_dt, dt)
        prev_cum = round(prev_cum + transfer_net, 2)

        eq = ffloat_from(get(row, "equity", ""), default=None)
        equity_adj = (eq - prev_cum) if (eq is not None) else None
        prev_equity_adj = prev_adj

        pnl = ""
        ret = ""
        winloss = ""
        if prev_equity_adj and prev_equity_adj > 0 and equity_adj is not None:
            pnl_v = equity_adj - prev_equity_adj
            ret_v = pnl_v / prev_equity_adj
            pnl = f"{pnl_v:.2f}"
            ret = f"{ret_v:.8f}"
            if pnl_v > 0:
                winloss = "WIN"
            elif pnl_v < 0:
                winloss = "LOSS"
            else:
                winloss = "FLAT"

        peak = equity_adj if (prev_peak is None or prev_peak <= 0) else max(prev_peak, equity_adj or prev_peak)
        dd = max(0.0, (peak or 0.0) - (equity_adj or 0.0))
        dd_pct = (dd / peak) if peak and peak > 0 else 0.0

        setv(row, "transfer_net", f"{transfer_net:.2f}")
        setv(row, "transfer_cum", f"{prev_cum:.2f}")
        setv(row, "equity_adj", f"{equity_adj:.2f}" if equity_adj is not None else "")
        setv(row, "prev_equity_adj", f"{prev_equity_adj:.2f}" if prev_equity_adj else "")
        setv(row, "pnl", pnl)
        setv(row, "ret", ret)
        setv(row, "winloss", winloss)
        setv(row, "peak_equity", f"{peak:.2f}" if peak is not None else "")
        setv(row, "dd", f"{dd:.2f}")
        setv(row, "dd_pct", f"{dd_pct:.6f}")

        if recompute_llr and mu1 is not None and sigma is not None and ret:
            x = float(ret)
            if sigma > 0:
                new_llr = prev_llr + llr_increment_normal(x, mu0, mu1, sigma)
                new_n = prev_n + 1
                A = math.log((1.0 - beta) / alpha)
                B = math.log(beta / (1.0 - alpha))
                decision = "OK"
                if new_n >= min_n and new_llr <= B:
                    pause_until = (dt.astimezone(ET).date() + timedelta(days=int(pause_days))).isoformat()
                    decision = "PAUSE_EDGE"
                    new_llr = 0.0
                    new_n = 0
                elif new_llr >= A:
                    decision = "EDGE_OK_RESET"
                    new_llr = 0.0
                    new_n = 0
                setv(row, "llr", f"{new_llr:.6f}")
                setv(row, "n", str(new_n))
                setv(row, "decision", decision)
                setv(row, "pause_until", pause_until)
                prev_llr, prev_n = new_llr, new_n

        prev_adj = equity_adj
        prev_peak = peak

    # Rebuild table
    out = [header]
    for r in rows:
        row = r["row"]
        # ensure row has full length
        if len(row) < len(header):
            row = row + [""] * (len(header) - len(row))
        out.append(row)

    write_all(svc, sid, tab, out)
    print(f"Backfill complete: {tab} from {start_date.isoformat()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
