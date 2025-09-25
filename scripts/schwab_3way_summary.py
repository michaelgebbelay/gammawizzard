#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Three-way expiry summary: Leo vs Standard vs Adjusted (from Schwab), with Last-10/20 stats.

Inputs (Sheets tabs):
  - sw_txn_raw (already produced by schwab_dump_all_txns.py)
  - sw_leo_orders: exp_primary, side, short_put, long_put, short_call, long_call, price
  - sw_orig_orders: exp_primary, side, short_put, long_put, short_call, long_call, price, contracts
  - sw_settlements: exp_primary, settle

Outputs:
  - sw_3way_by_expiry
  - sw_3way_perf

Env:
  GSHEET_ID, GOOGLE_SERVICE_ACCOUNT_JSON
  UNIT_RISK (default 4500)   # dollars of max loss to normalize to
"""

import base64, json, os, math
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

from google.oauth2 import service_account
from googleapiclient.discovery import build as gbuild

ET = ZoneInfo("America/New_York")
UNIT_RISK = float(os.environ.get("UNIT_RISK", "4500"))

RAW_TAB = "sw_txn_raw"
LEO_TAB = "sw_leo_orders"
ORIG_TAB = "sw_orig_orders"
SETTLE_TAB = "sw_settlements"
OUT_TAB = "sw_3way_by_expiry"
PERF_TAB = "sw_3way_perf"

RAW_HEADERS = [
    "ts","txn_id","type","sub_type","description",
    "symbol","underlying","exp_primary","strike","put_call",
    "quantity","price","amount","net_amount","commissions","fees_other",
    "source","ledger_id"
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
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gbuild("sheets","v4",credentials=creds), sid

def get_values(svc, sid, rng: str) -> List[List[Any]]:
    return svc.spreadsheets().values().get(spreadsheetId=sid, range=rng).execute().get("values", [])

def write_rows(svc, sid, tab: str, headers: List[str], rows: List[List[Any]]):
    svc.spreadsheets().values().clear(spreadsheetId=sid, range=tab).execute()
    svc.spreadsheets().values().update(
        spreadsheetId=sid, range=f"{tab}!A1",
        valueInputOption="RAW",
        body={"values":[headers] + rows}
    ).execute()

def ensure_tab(svc, sid: str, tab: str):
    meta = svc.spreadsheets().get(spreadsheetId=sid).execute()
    titles = {s["properties"]["title"] for s in meta.get("sheets", [])}
    if tab not in titles:
        svc.spreadsheets().batchUpdate(
            spreadsheetId=sid,
            body={"requests":[{"addSheet":{"properties":{"title":tab}}}]}
        ).execute()

def parse_date(x) -> Optional[date]:
    if x is None: return None
    s = str(x).strip()
    if not s: return None
    # allow 'YYYY-MM-DD' and full timestamps
    try: return date.fromisoformat(s[:10])
    except Exception:
        try:
            z = s.replace("Z","+00:00")
            return datetime.fromisoformat(z).astimezone(ET).date()
        except Exception:
            return None

def f2(x: Optional[float]) -> str:
    if x is None or not math.isfinite(x): return ""
    sign = "-" if x < 0 else ""
    return f"{sign}$ {abs(x):,.2f}"

def d4(x: Optional[float]) -> str:
    if x is None or not math.isfinite(x): return ""
    s = f"{x:.4f}".rstrip("0").rstrip(".")
    return s or "0"

def max_dd(pnls: List[float]) -> float:
    run = 0.0; peak = 0.0; mdd = 0.0
    for p in pnls:
        run += p
        peak = max(peak, run)
        mdd = max(mdd, peak - run)
    return round(mdd, 2)

def stats(pnls: List[float]) -> Dict[str, Any]:
    n = len(pnls)
    tot = round(sum(pnls), 2)
    wr = (sum(1 for p in pnls if p > 0) / n * 100.0) if n else 0.0
    exp = (tot / n) if n else 0.0
    edge = exp / 100.0  # per $100 risk unit; we’ll present separately as $/unit and edge
    dd = max_dd(pnls) if n else 0.0
    rec = (tot / dd) if dd else 0.0
    sw = sum(p for p in pnls if p > 0)
    sl = sum(p for p in pnls if p < 0)
    pf = (sw / abs(sl)) if sl < 0 else None
    shp = None
    if n >= 2:
        m = tot / n
        var = sum((p - m)**2 for p in pnls) / (n - 1)
        sd = math.sqrt(var) if var > 0 else 0.0
        if sd > 0: shp = m / sd
    return {
        "count": n, "total": tot, "win_rate": wr, "expectancy": exp,
        "edge": exp / 100.0, "max_dd": dd, "recovery": rec,
        "profit_factor": pf, "sharpe": shp
    }

# ---- payoff helpers for iron condors ----
def _flt(x) -> Optional[float]:
    try:
        if x is None or str(x).strip()=="":
            return None
        return float(x)
    except Exception:
        return None

def ic_widths(sp, lp, sc, lc) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    wp = None if (sp is None or lp is None) else (sp - lp)
    wc = None if (lc is None or sc is None) else (lc - sc)
    w = None
    if wp is not None and wc is not None: w = max(wp, wc)
    else: w = wp if wp is not None else wc
    return wp, wc, w

def pnl_iron_condor(side: str, sp: float, lp: float, sc: float, lc: float, price: float, settle: float) -> float:
    """Return P&L in dollars per 1 condor at expiry (100 multiplier). side: 'short' (credit) or 'long' (debit)."""
    wp, wc, _ = ic_widths(sp, lp, sc, lc)
    if wp is None or wc is None:
        return 0.0
    # intrinsic of each short spread, capped by its width
    put_loss = max(0.0, min(wp, (sp - settle))) * 100.0   # only if S < short_put
    call_loss = max(0.0, min(wc, (settle - sc))) * 100.0  # only if S > short_call
    if side.lower().startswith("short"):
        return price * 100.0 - (put_loss + call_loss)
    else:
        # long IC: max gain is spread widths minus debit; losses occur inside
        return (put_loss + call_loss) - price * 100.0

def max_loss(side: str, width: float, price: float) -> float:
    """Maximum loss per 1 condor (dollars)."""
    if side.lower().startswith("short"):
        return max(0.0, width * 100.0 - price * 100.0)
    else:
        return price * 100.0  # debit paid

# ---- core aggregation ----
def build_three_way(svc, sid):
    # Read raw (A:R to include ledger_id)
    last_col = chr(ord("A") + len(RAW_HEADERS) - 1)
    raw = get_values(svc, sid, f"{RAW_TAB}!A1:{last_col}")
    # helper tabs → dicts
    leo = get_values(svc, sid, f"{LEO_TAB}!A1:Z")
    orig = get_values(svc, sid, f"{ORIG_TAB}!A1:Z")
    settle = get_values(svc, sid, f"{SETTLE_TAB}!A1:Z")

    # map settlements
    st_map: Dict[date, float] = {}
    if settle:
        head = [h.strip() for h in settle[0]]
        i_exp = head.index("exp_primary")
        i_set = head.index("settle")
        for r in settle[1:]:
            if i_exp >= len(r) or i_set >= len(r): continue
            d = parse_date(r[i_exp]); s = _flt(r[i_set])
            if d and s is not None: st_map[d] = s

    # leo map
    leo_map: Dict[date, Dict[str, Any]] = {}
    if leo:
        h = [x.strip() for x in leo[0]]
        ix = {k:i for i,k in enumerate(h)}
        need = {"exp_primary","side","short_put","long_put","short_call","long_call","price"}
        if need.issubset(ix.keys()):
            for r in leo[1:]:
                d = parse_date(r[ix["exp_primary"]])
                if not d: continue
                leo_map[d] = {
                    "side": (r[ix["side"]] or "short"),
                    "sp": _flt(r[ix["short_put"]]), "lp": _flt(r[ix["long_put"]]),
                    "sc": _flt(r[ix["short_call"]]), "lc": _flt(r[ix["long_call"]]),
                    "price": _flt(r[ix["price"]]),
                }

    # original order map
    orig_map: Dict[date, Dict[str, Any]] = {}
    if orig:
        h = [x.strip() for x in orig[0]]
        ix = {k:i for i,k in enumerate(h)}
        need = {"exp_primary","side","short_put","long_put","short_call","long_call","price","contracts"}
        if need.issubset(ix.keys()):
            for r in orig[1:]:
                d = parse_date(r[ix["exp_primary"]])
                if not d: continue
                orig_map[d] = {
                    "side": (r[ix["side"]] or "short"),
                    "sp": _flt(r[ix["short_put"]]), "lp": _flt(r[ix["long_put"]]),
                    "sc": _flt(r[ix["short_call"]]), "lc": _flt(r[ix["long_call"]]),
                    "price": _flt(r[ix["price"]]),
                    "contracts": int(float(r[ix["contracts"]])) if ix["contracts"] < len(r) and str(r[ix["contracts"]]).strip() else 1
                }

    # realized P&L by expiry from sw_txn_raw (dedupe by ledger_id, <= yesterday)
    adj_by_exp: Dict[date, float] = {}
    cutoff = (datetime.now(ET).date() - timedelta(days=1))
    if raw and len(raw[0]) >= len(RAW_HEADERS):
        head = raw[0]
        i_exp = head.index("exp_primary")
        i_net = head.index("net_amount")
        i_ledger = head.index("ledger_id")
        seen_ledgers = set()
        for r in raw[1:]:
            if max(i_exp,i_net,i_ledger) >= len(r): continue
            led = str(r[i_ledger]).strip()
            if not led or led in seen_ledgers: 
                continue
            seen_ledgers.add(led)
            d = parse_date(r[i_exp])
            if not d or d > cutoff: 
                continue
            net = _flt(r[i_net])
            if net is None: 
                continue
            adj_by_exp[d] = round(adj_by_exp.get(d, 0.0) + float(net), 2)

    # Build per-expiry rows
    all_dates = sorted(set(st_map.keys()) | set(leo_map.keys()) | set(orig_map.keys()) | set(adj_by_exp.keys()), reverse=True)
    rows: List[List[Any]] = []
    series_leo: List[Tuple[date, float]] = []
    series_std: List[Tuple[date, float]] = []
    series_adjN: List[Tuple[date, float]] = []
    series_val: List[Tuple[date, float]] = []

    for d in all_dates:
        if d > cutoff: 
            continue
        settle_px = st_map.get(d)
        # Leo baseline (normalized to UNIT_RISK)
        leo_pnl_norm = None; leo_w = None; leo_price = None
        if settle_px is not None and d in leo_map:
            L = leo_map[d]
            sp,lp,sc,lc = L["sp"],L["lp"],L["sc"],L["lc"]
            side = L["side"]; price = L["price"] or 0.0
            _, _, w = ic_widths(sp, lp, sc, lc); leo_w = w; leo_price = price
            if w is not None and price is not None:
                per = pnl_iron_condor(side, sp, lp, sc, lc, price, settle_px)
                maxL = max_loss(side, w, price)
                if maxL > 0:
                    leo_pnl_norm = per * (UNIT_RISK / maxL)
                    series_leo.append((d, round(leo_pnl_norm,2)))

        # Standard original (normalized)
        std_pnl_norm = None; std_w=None; std_price=None; std_contracts=None; std_risk_total=None
        if settle_px is not None and d in orig_map:
            O = orig_map[d]
            sp,lp,sc,lc = O["sp"],O["lp"],O["sc"],O["lc"]
            side = O["side"]; price = O["price"] or 0.0
            c = O["contracts"] or 1
            _,_,w = ic_widths(sp,lp,sc,lc); std_w=w; std_price=price; std_contracts=c
            if w is not None and price is not None:
                per = pnl_iron_condor(side, sp, lp, sc, lc, price, settle_px)
                maxL = max_loss(side, w, price)
                std_risk_total = maxL * c
                if std_risk_total > 0:
                    std_pnl_norm = (per * c) * (UNIT_RISK / std_risk_total)
                    series_std.append((d, round(std_pnl_norm,2)))

        # Adjusted realized: dollars and normalized (using original risk if available)
        adj_nom = adj_by_exp.get(d)
        adj_norm = None
        if adj_nom is not None and std_risk_total and std_risk_total > 0:
            adj_norm = adj_nom * (UNIT_RISK / std_risk_total)
            series_adjN.append((d, round(adj_norm,2)))

        # Value add vs Standard (normalized)
        val = None
        if adj_norm is not None and std_pnl_norm is not None:
            val = adj_norm - std_pnl_norm
            series_val.append((d, round(val,2)))

        rows.append([
            d.isoformat(), settle_px if settle_px is not None else "",
            # Leo
            leo_w if leo_w is not None else "", leo_price if leo_price is not None else "", f2(leo_pnl_norm),
            # Standard
            std_w if std_w is not None else "", std_price if std_price is not None else "", std_contracts if std_contracts is not None else "",
            f2(std_pnl_norm),
            # Adjusted
            f2(adj_nom), f2(adj_norm),
            # Value add
            f2(val)
        ])

    headers = [
        "exp_primary","settle",
        "leo_width","leo_price","leo_pnl_norm",
        "std_width","std_price","std_contracts","std_pnl_norm",
        "adjusted_realized","adjusted_pnl_norm",
        "value_add_vs_std"
    ]
    ensure_tab(svc, sid, OUT_TAB)
    write_rows(svc, sid, OUT_TAB, headers, rows)

    # ---- performance blocks (Last 10/20 by expiry) ----
    def lastN(series: List[Tuple[date,float]], n: int) -> List[float]:
        # series is sorted desc by build order; ensure desc then take N then reverse to chrono
        series_sorted = sorted(series, key=lambda t: t[0], reverse=True)
        take = [v for _,v in series_sorted[:n]]
        return list(reversed(take))

    cats = [
        ("Leo (normalized)", series_leo),
        ("Standard (normalized)", series_std),
        ("Adjusted (normalized)", series_adjN),
        ("Value add (Adj-Std)", series_val),
    ]

    # build a compact table: Category | metric | Last10 | Last20
    perf_rows = [["Category","Metric","Last 10","Last 20"]]
    for name, series in cats:
        p10 = lastN(series, 10); p20 = lastN(series, 20)
        s10 = stats(p10); s20 = stats(p20)
        block = [
            [name,"Total profit", f2(s10["total"]), f2(s20["total"])],
            ["","Win rate", f"{s10['win_rate']:.0f}%", f"{s20['win_rate']:.0f}%"],
            ["","Profit factor", d4(s10["profit_factor"]) if s10["profit_factor"] is not None else "", d4(s20["profit_factor"]) if s20["profit_factor"] is not None else ""],
            ["","Edge / Expectancy per trade", f"{d4(s10['edge'])} / {f2(s10['expectancy'])}", f"{d4(s20['edge'])} / {f2(s20['expectancy'])}"],
            ["","Max drawdown", f2(s10["max_dd"]), f2(s20["max_dd"])],
            ["","Recovery factor", d4(s10["recovery"]), d4(s20["recovery"])],
            ["","Sharpe per trade", d4(s10["sharpe"]) if s10["sharpe"] is not None else "", d4(s20["sharpe"]) if s20["sharpe"] is not None else ""],
            ["","","",""],
        ]
        perf_rows.extend(block)

    ensure_tab(svc, sid, PERF_TAB)
    write_rows(svc, sid, PERF_TAB, perf_rows[0], perf_rows[1:])

def main() -> int:
    svc, sid = sheets_client()
    build_three_way(svc, sid)
    print("OK: built sw_3way_by_expiry and sw_3way_perf.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
