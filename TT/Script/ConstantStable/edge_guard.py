#!/usr/bin/env python3
"""
ConstantStable - Equity logger + edge guard (Google Sheets + GitHub Actions outputs)

Writes one row per run into a Google Sheet tab (default: ConstantStableState).
Computes daily/run P&L based on Schwab liquidationValue and adjusts for transfers.
Optionally pauses trading when edge is likely gone (sequential test on returns).

Required env (for Schwab):
  SCHWAB_APP_KEY
  SCHWAB_APP_SECRET
  SCHWAB_TOKEN_JSON

Optional env (for Google Sheets logging):
  GSHEET_ID
  GOOGLE_SERVICE_ACCOUNT_JSON   (full JSON string)
  CS_STATE_TAB                  default ConstantStableState

Edge test env (optional; if missing -> monitor only, never blocks trading):
  CS_EDGE_MU1                   expected mean return per run (e.g. 0.00035)
  CS_EDGE_SIGMA                 std dev of return per run (e.g. 0.0040)
  CS_EDGE_MU0                   default 0.0 (no edge)
  CS_EDGE_ALPHA                 default 0.01
  CS_EDGE_BETA                  default 0.05
  CS_EDGE_MIN_SAMPLES           default 30
  CS_EDGE_PAUSE_DAYS            default 5

Failure behavior:
  CS_EDGE_FAIL_OPEN             default true  (if sheets unavailable, allow trading)

Outputs (GITHUB_OUTPUT):
  can_trade = 1/0
  reason    = string
"""

import os, sys, json, math
from datetime import datetime, timedelta, timezone, date
from zoneinfo import ZoneInfo


def _add_scripts_root():
    cur = os.path.abspath(os.path.dirname(__file__))
    while True:
        if os.path.basename(cur) in ("scripts", "Script"):
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

# ---------------- utils ----------------
def goutput(name: str, val: str):
    p = os.environ.get("GITHUB_OUTPUT")
    if p:
        with open(p, "a") as fh:
            fh.write(f"{name}={val}\n")

def truthy(s: str) -> bool:
    return str(s or "").strip().lower() in ("1","true","yes","y","on")

def ffloat(key: str, default=None):
    raw = os.environ.get(key, "")
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return float(raw)
    except Exception:
        return default

def fint(key: str, default=None):
    raw = os.environ.get(key, "")
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return int(float(raw))
    except Exception:
        return default

# ---------------- Schwab equity ----------------
def opening_cash_for_account(c):
    """
    Returns (equity_value, source_key, acct_number) preferring liquidationValue and ignoring 0/neg.
    """
    r = c.get_accounts()
    r.raise_for_status()
    data = r.json()
    arr = data if isinstance(data, list) else [data]

    acct_num = ""
    try:
        rr = c.get_account_numbers()
        rr.raise_for_status()
        acct_num = str((rr.json() or [{}])[0].get("accountNumber") or "")
    except Exception:
        pass

    def hunt(a):
        acct_id=None; init={}; curr={}
        stack=[a]
        while stack:
            x=stack.pop()
            if isinstance(x,dict):
                if acct_id is None and x.get("accountNumber"):
                    acct_id=str(x["accountNumber"])
                if "initialBalances" in x and isinstance(x["initialBalances"], dict):
                    init=x["initialBalances"]
                if "currentBalances" in x and isinstance(x["currentBalances"], dict):
                    curr=x["currentBalances"]
                for v in x.values():
                    if isinstance(v,(dict,list)):
                        stack.append(v)
            elif isinstance(x,list):
                stack.extend(x)
        return acct_id, init, curr

    chosen=None
    chosen_num=""
    for a in arr:
        aid, init, curr = hunt(a)
        if acct_num and aid == acct_num:
            chosen=(init,curr); chosen_num=aid; break
        if chosen is None:
            chosen=(init,curr); chosen_num=aid or ""

    if not chosen:
        return None, "none", chosen_num

    init, curr = chosen
    keys = [
        "liquidationValue",
        "cashAvailableForTrading",
        "cashBalance",
        "availableFundsNonMarginableTrade",
        "buyingPowerNonMarginableTrade",
        "optionBuyingPower",
        "buyingPower",
    ]
    def pick(src):
        for k in keys:
            v = (src or {}).get(k)
            if isinstance(v,(int,float)):
                fv = float(v)
                if fv > 0:
                    return fv, k
        return None

    for src in (init, curr):
        got = pick(src)
        if got:
            return got[0], got[1], chosen_num
    return None, "none", chosen_num

def get_account_hash(c):
    r = c.get_account_numbers()
    r.raise_for_status()
    arr = r.json() or []
    info = arr[0] if arr else {}
    return str(info.get("hashValue") or info.get("hashvalue") or "")

def safe_float(x, default=None):
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default

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

def net_transfers_between(c, acct_hash: str, t0: datetime, t1: datetime) -> float:
    txns = list_transactions(c, acct_hash, t0, t1)
    total = 0.0
    for t in txns:
        if not is_transfer_txn(t):
            continue
        amt = safe_float(t.get("netAmount"))
        if amt is None:
            continue
        total += amt
    return round(total, 2)

# ---------------- Google Sheets ----------------
def sheets_available():
    return bool((os.environ.get("GSHEET_ID") or "").strip()) and bool((os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip())

def creds_from_env():
    from google.oauth2 import service_account
    raw = (os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip()
    info = json.loads(raw)
    return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)

def build_svc():
    from googleapiclient.discovery import build
    return build("sheets", "v4", credentials=creds_from_env(), cache_discovery=False)

def ensure_tab(svc, sid: str, title: str):
    meta = svc.spreadsheets().get(spreadsheetId=sid, fields="sheets.properties").execute()
    for s in (meta.get("sheets") or []):
        p = s.get("properties") or {}
        if (p.get("title") or "") == title:
            return
    req = {"requests":[{"addSheet":{"properties":{"title": title}}}]}
    svc.spreadsheets().batchUpdate(spreadsheetId=sid, body=req).execute()

def read_all(svc, sid: str, title: str):
    r = svc.spreadsheets().values().get(spreadsheetId=sid, range=f"{title}!A1:ZZ").execute()
    return r.get("values") or []

def append_row(svc, sid: str, title: str, header: list[str], row: dict):
    # Ensure header
    existing = read_all(svc, sid, title)
    if not existing:
        svc.spreadsheets().values().update(
            spreadsheetId=sid,
            range=f"{title}!A1",
            valueInputOption="RAW",
            body={"values":[header]},
        ).execute()
    else:
        if existing[0] != header:
            svc.spreadsheets().values().update(
                spreadsheetId=sid,
                range=f"{title}!A1",
                valueInputOption="RAW",
                body={"values":[header]},
            ).execute()

    values = [str(row.get(h,"")) for h in header]
    svc.spreadsheets().values().append(
        spreadsheetId=sid,
        range=f"{title}!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values":[values]},
    ).execute()

def last_state(existing: list[list[str]], header: list[str]) -> dict:
    if not existing or len(existing) < 2:
        return {}
    cur_header = existing[0]
    idx = {h:i for i,h in enumerate(cur_header)}
    last = existing[-1]
    out={}
    for h in header:
        j = idx.get(h)
        out[h] = (last[j] if (j is not None and j < len(last)) else "")
    return out

# ---------------- Edge test (sequential) ----------------
def llr_increment_normal(x: float, mu0: float, mu1: float, sigma: float) -> float:
    # log( f1(x) / f0(x) ) for Normal with same sigma
    # = ((mu0^2 - mu1^2) + 2*x*(mu1 - mu0)) / (2*sigma^2)
    return ((mu0*mu0 - mu1*mu1) + 2.0*x*(mu1 - mu0)) / (2.0*sigma*sigma)

def main():
    # defaults: fail-open to never block trading if sheets broken
    fail_open = truthy(os.environ.get("CS_EDGE_FAIL_OPEN", "true"))

    # 1) Get equity
    try:
        c = schwab_client()
        eq, src, acct = opening_cash_for_account(c)
        acct_hash = get_account_hash(c)
    except Exception as e:
        # If Schwab fails, trading probably fails too; be explicit
        print(f"CS_EDGE_GUARD ERROR: Schwab init/equity failed: {e}")
        goutput("can_trade","0")
        goutput("reason","SCHWAB_FAIL")
        return 1

    if eq is None or eq <= 0:
        print(f"CS_EDGE_GUARD WARN: equity unavailable (eq={eq}) - allowing trade")
        goutput("can_trade","1")
        goutput("reason","NO_EQUITY")
        return 0

    ts_utc = datetime.now(timezone.utc)
    ts_et = ts_utc.astimezone(ET)
    run_date = ts_et.date().isoformat()

    # 2) If sheets not configured, just output allow + log to stdout
    if not sheets_available():
        print(f"CS_EDGE_GUARD: sheets not configured; equity={eq:.2f} src={src} acct={acct}")
        goutput("can_trade","1")
        goutput("reason","NO_SHEETS")
        return 0

    sid = (os.environ.get("GSHEET_ID") or "").strip()
    tab = (os.environ.get("CS_STATE_TAB") or "ConstantStableState").strip()

    header = [
        "ts_utc","ts_et","run_date",
        "acct","equity","equity_src",
        "prev_equity","transfer_net","transfer_cum","equity_adj","prev_equity_adj","pnl","ret",
        "peak_equity","dd","dd_pct",
        "winloss",
        "llr","n",
        "pause_until","decision"
    ]

    try:
        svc = build_svc()
        ensure_tab(svc, sid, tab)
        existing = read_all(svc, sid, tab)
    except Exception as e:
        print(f"CS_EDGE_GUARD WARN: sheets read/init failed: {str(e)[:200]}")
        if fail_open:
            goutput("can_trade","1")
            goutput("reason","SHEETS_FAIL_OPEN")
            return 0
        goutput("can_trade","0")
        goutput("reason","SHEETS_FAIL_CLOSED")
        return 0

    prev = last_state(existing, header)
    prev_eq = ffloat_from(prev.get("equity",""), default=None)
    prev_adj = ffloat_from(prev.get("equity_adj",""), default=None)
    prev_trans_cum = ffloat_from(prev.get("transfer_cum",""), default=0.0) or 0.0
    prev_peak = ffloat_from(prev.get("peak_equity",""), default=None)
    prev_llr = ffloat_from(prev.get("llr",""), default=0.0) or 0.0
    prev_n   = fint_from(prev.get("n",""), default=0) or 0
    pause_until = (prev.get("pause_until","") or "").strip()

    # pause check
    if pause_until:
        try:
            pu = date.fromisoformat(pause_until[:10])
            if ts_et.date() <= pu:
                # still paused
                row = {
                    "ts_utc": ts_utc.isoformat(),
                    "ts_et": ts_et.isoformat(),
                    "run_date": run_date,
                    "acct": acct,
                    "equity": f"{eq:.2f}",
                    "equity_src": src,
                    "prev_equity": prev.get("equity",""),
                    "transfer_net": "",
                    "transfer_cum": prev.get("transfer_cum",""),
                    "equity_adj": prev.get("equity_adj",""),
                    "prev_equity_adj": prev.get("prev_equity_adj",""),
                    "pnl": "",
                    "ret": "",
                    "peak_equity": prev.get("peak_equity",""),
                    "dd": prev.get("dd",""),
                    "dd_pct": prev.get("dd_pct",""),
                    "winloss": "",
                    "llr": prev.get("llr",""),
                    "n": prev.get("n",""),
                    "pause_until": pause_until,
                    "decision": "PAUSED",
                }
                append_row(svc, sid, tab, header, row)
                goutput("can_trade","0")
                goutput("reason","PAUSED")
                print(f"CS_EDGE_GUARD: PAUSED until {pause_until} (equity={eq:.2f})")
                return 0
        except Exception:
            pass

    # transfer adjustment window: from prev run to now (fallback 1 day)
    if prev.get("ts_utc"):
        try:
            t0 = datetime.fromisoformat(prev.get("ts_utc")).astimezone(timezone.utc)
        except Exception:
            t0 = ts_utc - timedelta(days=1)
    else:
        t0 = ts_utc - timedelta(days=1)
    t1 = ts_utc

    transfer_net = 0.0
    if acct_hash:
        try:
            transfer_net = net_transfers_between(c, acct_hash, t0, t1)
        except Exception as e:
            print(f"CS_EDGE_GUARD WARN: transfer lookup failed: {str(e)[:200]}")

    transfer_cum = prev_trans_cum + transfer_net

    equity_adj = eq - transfer_cum
    if prev_adj is None:
        prev_adj = prev_eq if prev_eq is not None else None

    pnl = ""
    ret = ""
    winloss = ""
    if prev_adj and prev_adj > 0:
        pnl_v = equity_adj - prev_adj
        ret_v = pnl_v / prev_adj
        pnl = f"{pnl_v:.2f}"
        ret = f"{ret_v:.8f}"
        if pnl_v > 0: winloss = "WIN"
        elif pnl_v < 0: winloss = "LOSS"
        else: winloss = "FLAT"

    peak = max(prev_peak or 0.0, equity_adj) if (prev_peak and prev_peak > 0) else equity_adj
    dd = max(0.0, peak - equity_adj)
    dd_pct = (dd / peak) if peak > 0 else 0.0

    # Edge-test parameters (returns, not dollars)
    mu0   = ffloat("CS_EDGE_MU0", 0.0)
    mu1   = ffloat("CS_EDGE_MU1", None)
    sigma = ffloat("CS_EDGE_SIGMA", None)
    alpha = ffloat("CS_EDGE_ALPHA", 0.01)
    beta  = ffloat("CS_EDGE_BETA", 0.05)
    min_n = fint("CS_EDGE_MIN_SAMPLES", 30)
    pause_days = fint("CS_EDGE_PAUSE_DAYS", 5)

    decision = "OK"
    new_llr = prev_llr
    new_n = prev_n

    can_trade = True
    reason = "OK"

    # Only run test if we have params AND we have a new return datapoint
    if (mu1 is not None) and (sigma is not None) and (prev_adj and prev_adj > 0):
        x = float(ret)  # return per run
        if sigma > 0:
            new_llr = prev_llr + llr_increment_normal(x, mu0, mu1, sigma)
            new_n = prev_n + 1

            A = math.log((1.0 - beta) / alpha)
            B = math.log(beta / (1.0 - alpha))

            if new_n >= min_n and new_llr <= B:
                # evidence for H0: edge <= 0
                pu = (ts_et.date() + timedelta(days=int(pause_days))).isoformat()
                pause_until = pu
                decision = "PAUSE_EDGE"
                can_trade = False
                reason = f"EDGE_PAUSE(llr={new_llr:.2f},n={new_n})"
                # reset stats after triggering, so after pause we start fresh
                new_llr = 0.0
                new_n = 0
            elif new_llr >= A:
                # strong evidence edge is present -> reset to avoid runaway number
                decision = "EDGE_OK_RESET"
                new_llr = 0.0
                new_n = 0
        else:
            decision = "NO_SIGMA"
    else:
        decision = "MONITOR_ONLY"

    row = {
        "ts_utc": ts_utc.isoformat(),
        "ts_et": ts_et.isoformat(),
        "run_date": run_date,
        "acct": acct,
        "equity": f"{eq:.2f}",
        "equity_src": src,
        "prev_equity": f"{prev_eq:.2f}" if prev_eq else "",
        "transfer_net": f"{transfer_net:.2f}",
        "transfer_cum": f"{transfer_cum:.2f}",
        "equity_adj": f"{equity_adj:.2f}",
        "prev_equity_adj": f"{prev_adj:.2f}" if prev_adj else "",
        "pnl": pnl,
        "ret": ret,
        "peak_equity": f"{peak:.2f}",
        "dd": f"{dd:.2f}",
        "dd_pct": f"{dd_pct:.6f}",
        "winloss": winloss,
        "llr": f"{new_llr:.6f}",
        "n": str(new_n),
        "pause_until": pause_until,
        "decision": decision,
    }

    try:
        append_row(svc, sid, tab, header, row)
    except Exception as e:
        print(f"CS_EDGE_GUARD WARN: sheets append failed: {str(e)[:200]}")
        if not fail_open:
            can_trade = False
            reason = "SHEETS_APPEND_FAIL_CLOSED"

    goutput("can_trade","1" if can_trade else "0")
    goutput("reason", reason)
    print(f"CS_EDGE_GUARD: can_trade={can_trade} reason={reason} equity={eq:.2f} dd_pct={dd_pct:.3%} decision={decision}")
    return 0

def ffloat_from(v, default=None):
    try:
        s = (v or "").strip()
        if s == "": return default
        return float(s)
    except Exception:
        return default

def fint_from(v, default=None):
    try:
        s = (v or "").strip()
        if s == "": return default
        return int(float(s))
    except Exception:
        return default

if __name__ == "__main__":
    sys.exit(main())
