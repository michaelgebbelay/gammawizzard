#!/usr/bin/env python3
"""
Schwab → Sheets RAW loader (LEDGER-ONLY, per-fill fees, no duplicates).

**What changed vs your current file**
- Uses ONLY the account ledger (`transferItems`) for option fills. No order fallback.
- Quantity sign comes from `amount` when present (Schwab uses ±contracts there). `instruction`
  is used only when `amount` is missing. This fixes the SELL/BUY sign flip you saw.
- Emits one row per ledger option line (per-fill, per-leg). No artificial row explosion.
- Carries the exact commissions/fees for each ledger activity (once).
- Overwrites sw_txn_raw on each run. Header gained a new, harmless column: `ledger_id`.

Env:
  GSHEET_ID, GOOGLE_SERVICE_ACCOUNT_JSON
  SCHWAB_APP_KEY, SCHWAB_APP_SECRET, SCHWAB_TOKEN_JSON
  DAYS_BACK (default 5)
    • Safe to run an initial catch-up (e.g. DAYS_BACK=120) and then smaller windows.
      Existing rows are merged/deduped by ledger leg when writing to Sheets.

Output tab: sw_txn_raw with RAW_HEADERS below
"""

import calendar
import os, sys, json, base64, re, time
from datetime import datetime, timedelta, timezone, date
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from schwab.auth import client_from_token_file
from google.oauth2 import service_account
from googleapiclient.discovery import build as gbuild

ET = ZoneInfo("America/New_York")

RAW_TAB = "sw_txn_raw"
PERF_TAB = "sw_performance_summary"
EDGE_RISK_UNIT = 100.0

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
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    svc = gbuild("sheets","v4",credentials=creds)
    return svc, sid

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


def ensure_tab_exists(svc, sid: str, tab: str) -> None:
    meta = svc.spreadsheets().get(spreadsheetId=sid).execute()
    titles = {s["properties"]["title"] for s in meta.get("sheets", [])}
    if tab not in titles:
        svc.spreadsheets().batchUpdate(
            spreadsheetId=sid,
            body={"requests": [{"addSheet": {"properties": {"title": tab}}}]},
        ).execute()

def overwrite_rows(svc, sid: str, tab: str, headers: List[str], rows: List[List[Any]]) -> None:
    svc.spreadsheets().values().clear(
        spreadsheetId=sid,
        range=tab,
    ).execute()
    svc.spreadsheets().values().update(
        spreadsheetId=sid,
        range=f"{tab}!A1",
        valueInputOption="RAW",
        body={"values":[headers] + rows}
    ).execute()


def read_existing_rows(
    svc, sid: str, tab: str, headers: List[str]
) -> List[List[Any]]:
    """Return existing rows (without header), normalised to header length."""
    resp = svc.spreadsheets().values().get(
        spreadsheetId=sid,
        range=f"{tab}!A2:{chr(ord('A') + len(headers) - 1)}",
    ).execute()
    rows = resp.get("values", [])
    norm: List[List[Any]] = []
    for r in rows:
        cur = list(r)
        if len(cur) < len(headers):
            cur.extend([""] * (len(headers) - len(cur)))
        else:
            cur = cur[: len(headers)]
        norm.append(cur)
    return norm


def merge_rows(existing: List[List[Any]], new: List[List[Any]], headers: List[str]) -> List[List[Any]]:
    """Merge ``existing`` with ``new`` rows, using ledger + leg fields as key."""

    def idx(col: str) -> int:
        try:
            return headers.index(col)
        except ValueError:
            return -1

    i_ledger = idx("ledger_id")
    i_symbol = idx("symbol")
    i_qty = idx("quantity")
    i_price = idx("price")
    i_amt = idx("amount")
    i_ts = idx("ts")

    def key_for(row: List[Any]) -> tuple:
        ledger = (row[i_ledger].strip() if i_ledger >= 0 and i_ledger < len(row) else "")
        symbol = row[i_symbol] if i_symbol >= 0 and i_symbol < len(row) else ""
        qty = row[i_qty] if i_qty >= 0 and i_qty < len(row) else ""
        price = row[i_price] if i_price >= 0 and i_price < len(row) else ""
        amt = row[i_amt] if i_amt >= 0 and i_amt < len(row) else ""
        if ledger:
            return ("ledger", ledger, symbol, qty, price, amt)
        return tuple(row)

    merged = {}
    for r in existing:
        merged[key_for(r)] = r
    for r in new:
        merged[key_for(r)] = r

    def ts_sort_key(row: List[Any]):
        if i_ts < 0 or i_ts >= len(row):
            return datetime.min
        raw = str(row[i_ts]).strip()
        if not raw:
            return datetime.min
        try:
            return datetime.fromisoformat(raw)
        except Exception:
            return datetime.min

    merged_rows = list(merged.values())
    merged_rows.sort(key=ts_sort_key, reverse=True)
    return merged_rows

# ---------- Schwab auth ----------
def decode_token_to_path() -> str:
    token_env = os.environ.get("SCHWAB_TOKEN_JSON","") or ""
    token_path = "schwab_token.json"
    if token_env:
        try:
            dec = base64.b64decode(token_env).decode("utf-8")
            if dec.strip().startswith("{"): token_env = dec
        except Exception:
            pass
        with open(token_path,"w") as f:
            f.write(token_env)
    return token_path

def schwab_client():
    token_path = decode_token_to_path()
    app_key = os.environ["SCHWAB_APP_KEY"]
    app_secret = os.environ["SCHWAB_APP_SECRET"]
    c = client_from_token_file(token_path, app_key, app_secret)
    r = c.get_account_numbers(); r.raise_for_status()
    acct_hash = r.json()[0]["hashValue"]
    return c, acct_hash

# ---------- time / parsing helpers ----------
def _iso_fix(s: str) -> str:
    x = s.strip()
    if x.endswith("Z"):
        return x[:-1] + "+00:00"
    if re.search(r"[+-]\d{4}$", x):
        return x[:-5] + x[-5:-2] + ":" + x[-2:]
    return x

def fmt_ts_utc_to_et(s: str) -> str:
    try:
        dt = datetime.fromisoformat(_iso_fix(s))
    except Exception:
        return s
    dt = dt.astimezone(ET)
    z = dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    return f"{z[:-2]}:{z[-2:]}"

def safe_float(x) -> Optional[float]:
    try: return float(x)
    except Exception: return None


def parse_sheet_datetime(value: str) -> Optional[datetime]:
    if not value:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(_iso_fix(raw))
    except Exception:
        return None


def max_drawdown(pnls: List[float]) -> float:
    running = 0.0
    peak = 0.0
    max_dd = 0.0
    for pnl in pnls:
        running += pnl
        if running > peak:
            peak = running
        drawdown = peak - running
        if drawdown > max_dd:
            max_dd = drawdown
    return round(max_dd, 2)


def compute_stats(pnls: List[float]) -> Dict[str, Any]:
    count = len(pnls)
    total = round(sum(pnls), 2)
    wins = sum(1 for v in pnls if v > 0)
    win_rate = (wins / count * 100.0) if count else 0.0
    edge = (total / (count * EDGE_RISK_UNIT)) if count else 0.0
    dd = max_drawdown(pnls) if count else 0.0
    factor = (total / dd) if dd else 0.0
    return {
        "count": count,
        "total": total,
        "wins": wins,
        "win_rate": win_rate,
        "edge": edge,
        "max_drawdown": round(dd, 2),
        "factor": round(factor, 2) if factor else 0.0,
    }


def fmt_number(val: Optional[float]) -> str:
    if val is None:
        return "NA"
    try:
        fval = float(val)
    except Exception:
        return str(val)
    if abs(fval) < 1e-9:
        fval = 0.0
    if abs(fval - round(fval)) < 1e-9:
        return str(int(round(fval)))
    return f"{fval:.2f}"


def fmt_edge(val: float) -> str:
    text = f"{val:.7f}"
    text = text.rstrip("0").rstrip(".")
    return text or "0"


def fmt_percent(val: float) -> str:
    text = f"{val:.2f}"
    text = text.rstrip("0").rstrip(".")
    return f"{text}%"


def build_performance_summary_rows(values: List[List[Any]]) -> List[List[Any]]:
    if len(values) <= 1:
        return [["Monthly performance"], ["No trade data available"]]

    header = values[0]
    try:
        i_ts = header.index("ts")
        i_net = header.index("net_amount")
    except ValueError:
        return [["Monthly performance"], ["Missing expected columns"]]

    i_ledger = header.index("ledger_id") if "ledger_id" in header else -1
    i_txn = header.index("txn_id") if "txn_id" in header else -1

    trades: Dict[str, Dict[str, Any]] = {}
    for row in values[1:]:
        if max(i_ts, i_net, i_ledger, i_txn) >= len(row):
            row = list(row) + [""] * (max(i_ts, i_net, i_ledger, i_txn) + 1 - len(row))
        ledger = ""
        if 0 <= i_ledger < len(row):
            ledger = str(row[i_ledger]).strip()
        if not ledger and 0 <= i_txn < len(row):
            ledger = str(row[i_txn]).strip()
        if not ledger:
            continue

        net = safe_float(row[i_net]) if 0 <= i_net < len(row) else None
        if net is None:
            continue

        ts_raw = row[i_ts] if 0 <= i_ts < len(row) else ""
        dt = parse_sheet_datetime(ts_raw)

        entry = trades.setdefault(ledger, {"net": None, "ts": None})
        entry["net"] = net
        if dt is not None:
            if entry["ts"] is None or dt < entry["ts"]:
                entry["ts"] = dt

    trade_list = [
        {
            "ledger": k,
            "net": v["net"],
            "ts": v["ts"],
        }
        for k, v in trades.items()
        if v["net"] is not None
    ]
    if not trade_list:
        return [["Monthly performance"], ["No trade data available"]]

    trade_list.sort(key=lambda item: (item["ts"] or datetime.min, item["ledger"]))

    pnls = [float(t["net"]) for t in trade_list]

    monthly: Dict[Tuple[int, int], float] = {}
    years: List[int] = []
    for t in trade_list:
        ts = t.get("ts")
        if not isinstance(ts, datetime):
            continue
        year = ts.year
        month = ts.month
        monthly[(year, month)] = round(monthly.get((year, month), 0.0) + float(t["net"]), 2)
        if year not in years:
            years.append(year)

    years.sort()
    if not years:
        years = []
    years_to_show = years[-2:] if len(years) > 2 else years

    rows: List[List[Any]] = [["Monthly performance"]]
    if years_to_show:
        header_row = ["Month"] + [str(y) for y in years_to_show]
        rows.append(header_row)
        for m in range(1, 13):
            row = [calendar.month_abbr[m]]
            for y in years_to_show:
                val = monthly.get((y, m))
                row.append(val if val is not None else "NA")
            rows.append(row)
    else:
        rows.append(["Month"])
        rows.append(["(no dated trades)"])

    rows.append([])

    stats_all = compute_stats(pnls)
    stats_last10 = compute_stats(pnls[-10:])
    stats_last20 = compute_stats(pnls[-20:])

    now_et = datetime.now(ET)
    pnls_ytd = [
        float(t["net"])
        for t in trade_list
        if isinstance(t.get("ts"), datetime) and t["ts"].year == now_et.year
    ]
    stats_ytd = compute_stats(pnls_ytd)

    def stats_block(label: str, stats: Dict[str, Any]) -> List[List[str]]:
        if not stats.get("count"):
            return [[f"Total profit {label} = N/A (no trades)"]]
        total = fmt_number(stats["total"])
        edge = fmt_edge(float(stats["edge"]))
        dd = fmt_number(stats["max_drawdown"])
        factor = fmt_number(stats["factor"])
        win = fmt_percent(float(stats["win_rate"]))
        return [
            [f"Total profit {label} = {total} Edge= {edge} Max Drawdown = {dd}"],
            [f"Factor = {factor} Win rate = {win}"],
        ]

    rows.extend(stats_block("last 10 trades", stats_last10))
    rows.extend(stats_block("last 20 trades", stats_last20))
    rows.extend(stats_block("Year To Date", stats_ytd))

    if stats_all.get("count"):
        rows.append([])
        hist_win = fmt_percent(float(stats_all["win_rate"]))
        hist_win_display = hist_win[:-1] if hist_win.endswith("%") else hist_win
        rows.append([f"Historical Win rate: {hist_win_display}"])
        total_all = fmt_number(stats_all["total"])
        total_dd = fmt_number(stats_all["max_drawdown"])
        rows.append([f"Total Profit: {total_all} Total Max DD = {total_dd}"])
    else:
        rows.append(["No historical trade data available."])

    return rows


def write_performance_summary_from_raw(svc, sid: str, values: List[List[Any]], tab: str = PERF_TAB):
    ensure_tab_exists(svc, sid, tab)
    rows = build_performance_summary_rows(values)
    svc.spreadsheets().values().clear(
        spreadsheetId=sid,
        range=tab,
    ).execute()
    svc.spreadsheets().values().update(
        spreadsheetId=sid,
        range=f"{tab}!A1",
        valueInputOption="USER_ENTERED",
        body={"values": rows},
    ).execute()

def to_underlying(sym: str, underlying_hint: str = "") -> str:
    u = (underlying_hint or "").upper()
    if u:
        return "SPX" if u.startswith("SPX") else u
    s = (sym or "").strip().upper()
    if not s: return ""
    p0 = s.split()[0]
    return "SPX" if p0.startswith("SPX") else p0

def parse_exp_from_symbol(sym: str) -> Optional[str]:
    if not sym: return None
    s = sym.strip().upper().replace(" ","")
    m = re.search(r"\D(\d{6})[CP]\d", s)
    if m:
        try: return datetime.strptime(m.group(1), "%y%m%d").date().isoformat()
        except Exception: return None
    m = re.search(r"(20\d{2}-\d{2}-\d{2})", s)
    return m.group(1) if m else None

def parse_pc_from_symbol(sym: str) -> Optional[str]:
    if not sym: return None
    s = sym.upper().replace(" ","")
    m = re.search(r"\d{6}([CP])\d{8}$", s)
    if m: return "CALL" if m.group(1) == "C" else "PUT"
    return None

def parse_strike_from_occ(sym: str) -> Optional[float]:
    if not sym: return None
    s = sym.upper().replace(" ","")
    m = re.search(r"[CP](\d{8})$", s)
    if not m: return None
    try: return int(m.group(1)) / 1000.0
    except Exception: return None

def contract_multiplier(symbol: str, underlying: str) -> int:
    s = (symbol or "").upper(); u = (underlying or "").upper()
    if re.search(r"\d{6}[CP]\d{8}$", s): return 100
    if u in {"SPX","SPXW","NDX","RUT","VIX","XSP"}: return 100
    return 1

def compute_amount(qty: Optional[float], price: Optional[float], symbol: str, underlying: str) -> Optional[float]:
    if qty is None or price is None: return None
    return round(qty * price * contract_multiplier(symbol, underlying), 2)

# ---------- Data pull: LEDGER ONLY ----------
def list_transactions_window(c, acct_hash: str, t0: datetime, t1: datetime) -> List[Dict[str, Any]]:
    r = c.get_transactions(acct_hash, start_date=t0, end_date=t1)
    if getattr(r, "status_code", None) == 204: return []
    r.raise_for_status()
    j = r.json()
    return j if isinstance(j, list) else []

# ---------- Row builder (LEDGER ONLY) ----------
def rows_from_ledger_txn(txn: Dict[str, Any]) -> List[List[Any]]:
    """Emit one row per OPTION leg present in transferItems. Attach fees once per ledger txn."""
    ttype = str(txn.get("type") or txn.get("transactionType") or "")
    subtype = str(txn.get("subType") or "")
    desc = str(txn.get("description") or "")
    ts = str(txn.get("time") or txn.get("transactionDate") or txn.get("date") or "")
    ts = fmt_ts_utc_to_et(ts)

    order_id = str(txn.get("orderId") or "")
    transaction_id = str(txn.get("transactionId") or "")
    txn_id_for_sheet = order_id or transaction_id  # prefer order id

    # ledger identifier to help fee de-dup in the summarizer
    ledger_id = str(txn.get("activityId") or "") or transaction_id or ts

    # fees (from transferItems)
    comm_total = 0.0; fees_total = 0.0
    for ti in (txn.get("transferItems") or []):
        ft = str(ti.get("feeType") or "")
        if not ft: continue
        val = safe_float(ti.get("cost")) or safe_float(ti.get("amount")) or 0.0
        if "comm" in ft.lower(): comm_total += abs(val)
        else: fees_total += abs(val)
    comm_total = round(comm_total, 2)
    fees_total = round(fees_total, 2)

    rows: List[List[Any]] = []
    seen_leg = set()  # prevent duplicates within the same ledger activity

    for it in (txn.get("transferItems") or []):
        inst = it.get("instrument") or {}
        if (str(inst.get("assetType") or "").upper() != "OPTION"):
            continue

        symbol = str(inst.get("symbol") or "")
        underlying = to_underlying(symbol, inst.get("underlyingSymbol") or "")
        if underlying.upper().startswith("SPX"): underlying = "SPX"
        exp_primary = parse_exp_from_symbol(symbol) or ""
        strike = inst.get("strikePrice") or parse_strike_from_occ(symbol)
        pc = inst.get("putCall") or parse_pc_from_symbol(symbol) or ""
        pc = "CALL" if str(pc).upper().startswith("C") else ("PUT" if str(pc).upper().startswith("P") else "")

        # Quantity sign: prefer signed 'amount' (contracts), else use 'quantity' with instruction
        raw_qty = safe_float(it.get("amount"))
        if raw_qty is not None and raw_qty != 0:
            qty = raw_qty
        else:
            qty = safe_float(it.get("quantity")) or 0.0
            instr = str(it.get("instruction") or "").upper()
            if instr.startswith("SELL"): qty = -abs(qty)
            else: qty = abs(qty)

        price = safe_float(it.get("price"))
        amt = compute_amount(qty, price, symbol, underlying)

        # Intra-txn dedupe key
        leg_key = (symbol, exp_primary, pc, strike, round(qty or 0.0, 6), round(price or 0.0, 6))
        if leg_key in seen_leg:
            continue
        seen_leg.add(leg_key)

        rows.append([
            ts, txn_id_for_sheet, ttype, subtype, desc,
            symbol, underlying, (exp_primary or ""), (strike if strike is not None else ""), pc,
            (qty if qty is not None else ""), (price if price is not None else ""), (amt if amt is not None else ""),
            (txn.get("netAmount") if txn.get("netAmount") is not None else ""), comm_total or "", fees_total or "",
            "schwab_ledger", ledger_id
        ])

    return rows


def write_simple_summary_from_raw(svc, sid, src_tab=RAW_TAB, out_tab="sw_txn_summary"):
    # Pull raw values
    resp = svc.spreadsheets().values().get(
        spreadsheetId=sid, range=f"{src_tab}!A1:Q"
    ).execute()
    values = resp.get("values", [])
    # Ensure tab + header even if empty
    headers = ["exp_primary", "net_amount_sum"]
    ensure_tab_with_header(svc, sid, out_tab, headers)
    write_performance_summary_from_raw(svc, sid, values)
    if len(values) <= 1:
        overwrite_rows(svc, sid, out_tab, headers, [])
        return

    header = values[0]
    # robust lookup by name
    try:
        i_exp = header.index("exp_primary")
        i_net = header.index("net_amount")
    except ValueError:
        # If headers are off, just leave summary empty rather than crashing
        overwrite_rows(svc, sid, out_tab, headers, [])
        return

    # Group sum(net_amount) by exp_primary
    sums = {}
    for r in values[1:]:
        # pad row so indexing is safe
        if len(r) < max(i_exp, i_net) + 1:
            continue
        exp = (r[i_exp] or "").strip()
        if not exp:
            continue
        net = safe_float(r[i_net]) or 0.0
        sums[exp] = round(sums.get(exp, 0.0) + net, 2)

    summary_rows = [[k, v] for k, v in sums.items()]

    def _to_dt(x: str):
        try:
            return datetime.fromisoformat(str(x).strip())
        except Exception:
            # Push blanks / bad values to the bottom
            return datetime.min

    summary_rows.sort(key=lambda r: _to_dt(r[0]), reverse=True)

    overwrite_rows(svc, sid, out_tab, headers, summary_rows)


# ---------- main ----------
def main() -> int:
    try:
        svc, sid = sheets_client()
        ensure_tab_with_header(svc, sid, RAW_TAB, RAW_HEADERS)
    except Exception as e:
        print(f"ABORT: Sheets init failed — {e}")
        return 1

    try:
        c, acct_hash = schwab_client()
    except Exception as e:
        msg = str(e)
        if ("unsupported_token_type" in msg) or ("refresh_token_authentication_error" in msg):
            print("ABORT: Schwab OAuth refresh failed — rotate SCHWAB_TOKEN_JSON secret.")
        else:
            print(f"ABORT: Schwab client init failed — {msg[:200]}")
        return 1

    try:
        days_back = int((os.environ.get("DAYS_BACK") or "5").strip())
    except Exception:
        days_back = 5
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=days_back)

    # Pull ledger for window
    try:
        txns = list_transactions_window(c, acct_hash, start_dt, end_dt)
    except Exception as e:
        print(f"ABORT: transactions fetch failed — {e}")
        return 1

    all_rows: List[List[Any]] = []
    for t in txns:
        try:
            all_rows.extend(rows_from_ledger_txn(t))
        except Exception as exc:
            tid = t.get("transactionId") or t.get("orderId") or "<?>"
            print(f"NOTE: failed to parse ledger {tid}: {exc}")

    existing_rows = read_existing_rows(svc, sid, RAW_TAB, RAW_HEADERS)
    merged_rows = merge_rows(existing_rows, all_rows, RAW_HEADERS)
    overwrite_rows(svc, sid, RAW_TAB, RAW_HEADERS, merged_rows)
    write_simple_summary_from_raw(svc, sid)
    print(
        f"OK: merged {len(all_rows)} rows from {len(txns)} ledger activities into {RAW_TAB}."
    )
    return 0

if __name__ == "__main__":
    sys.exit(main())
