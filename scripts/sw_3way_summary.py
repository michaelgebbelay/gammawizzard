#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Three-way expiry summary (numeric outputs, with alerts).

Inputs:
  sw_txn_raw (A:R), sw_leo_orders, sw_orig_orders, sw_settlements

Outputs:
  sw_3way_by_expiry
  sw_3way_perf
  sw_3way_alerts

Env:
  GSHEET_ID, GOOGLE_SERVICE_ACCOUNT_JSON
  UNIT_RISK (default 4500)   # dollars; used to normalize P&L
"""
import base64, json, os, math
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, date, timedelta, timezone
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
ALERTS_TAB = "sw_3way_alerts"

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

def ensure_tab(svc, sid: str, tab: str, headers: List[str]):
    meta = svc.spreadsheets().get(spreadsheetId=sid).execute()
    titles = {s["properties"]["title"] for s in meta.get("sheets", [])}
    if tab not in titles:
        svc.spreadsheets().batchUpdate(spreadsheetId=sid, body={"requests":[{"addSheet":{"properties":{"title":tab}}}]}).execute()
    got = svc.spreadsheets().values().get(spreadsheetId=sid, range=f"{tab}!1:1").execute().get("values", [])
    if not got or got[0] != headers:
        svc.spreadsheets().values().update(spreadsheetId=sid, range=f"{tab}!A1",
            valueInputOption="RAW", body={"values":[headers]}).execute()

def write_rows(svc, sid, tab: str, headers: List[str], rows: List[List[Any]]):
    svc.spreadsheets().values().clear(spreadsheetId=sid, range=tab).execute()
    svc.spreadsheets().values().update(spreadsheetId=sid, range=f"{tab}!A1",
        valueInputOption="RAW", body={"values":[headers]+rows}).execute()

def parse_date(x) -> Optional[date]:
    if x is None: return None
    s = str(x).strip()
    if not s: return None
    try:
        return date.fromisoformat(s[:10])
    except Exception:
        try:
            z = s.replace("Z","+00:00")
            return datetime.fromisoformat(z).astimezone(ET).date()
        except Exception:
            return None

def _f(x) -> Optional[float]:
    try:
        if x is None or str(x).strip()=="":
            return None
        return float(x)
    except Exception:
        return None

# ---- payoff helpers ----
def ic_widths(sp, lp, sc, lc) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    wp = None if (sp is None or lp is None) else (sp - lp)
    wc = None if (lc is None or sc is None) else (lc - sc)
    if wp is None and wc is None: return None, None, None
    if wp is None: return None, wc, wc
    if wc is None: return wp, None, wp
    return wp, wc, max(wp, wc)

def pnl_iron_condor(side: str, sp: float, lp: float, sc: float, lc: float, price: float, settle: float) -> float:
    """P&L per 1 condor at expiry, dollars (multiplier 100)."""
    wp, wc, _ = ic_widths(sp, lp, sc, lc)
    if wp is None or wc is None or price is None:
        return 0.0
    put_val  = max(0.0, min(wp,  sp - settle)) * 100.0
    call_val = max(0.0, min(wc,  settle - sc)) * 100.0
    if str(side).lower().startswith("short"):
        return price * 100.0 - (put_val + call_val)
    else:
        return (put_val + call_val) - price * 100.0

def max_loss(side: str, width: float, price: float) -> float:
    if str(side).lower().startswith("short"):
        return max(0.0, width*100.0 - price*100.0)
    else:
        return price*100.0

# ---- stats ----
def max_dd(pnls: List[float]) -> float:
    run=0.0; peak=0.0; mdd=0.0
    for p in pnls:
        run += p
        peak = max(peak, run)
        mdd = max(mdd, peak-run)
    return round(mdd,2)

def stats(pnls: List[float]) -> Dict[str, Optional[float]]:
    n = len(pnls)
    tot = round(sum(pnls),2)
    wr  = (sum(1 for p in pnls if p>0)/n*100.0) if n else None
    exp = (tot/n) if n else None
    dd  = max_dd(pnls) if n else None
    rec = (tot/dd) if (dd and dd>0) else None
    sw  = sum(p for p in pnls if p>0)
    sl  = sum(p for p in pnls if p<0)
    pf  = (sw/abs(sl)) if sl<0 else None
    shp = None
    if n>=2:
        m = tot/n
        var = sum((p-m)**2 for p in pnls)/(n-1)
        sd = math.sqrt(var) if var>0 else 0.0
        if sd>0: shp = m/sd
    return {"count": n, "total": tot, "win_rate": wr, "expectancy": exp, "max_dd": dd, "recovery": rec, "profit_factor": pf, "sharpe": shp}

# ---- core build ----
def build_three_way(svc, sid):
    # Read raw
    last_col = chr(ord("A")+len(RAW_HEADERS)-1)  # "R"
    raw = get_values(svc, sid, f"{RAW_TAB}!A1:{last_col}")
    leo = get_values(svc, sid, f"{LEO_TAB}!A1:Z")
    orig = get_values(svc, sid, f"{ORIG_TAB}!A1:Z")
    settle = get_values(svc, sid, f"{SETTLE_TAB}!A1:Z")

    alerts: List[List[Any]] = []

    # Map settlements
    st_map: Dict[date, float] = {}
    if settle:
        h = [c.strip() for c in settle[0]]
        i_exp = h.index("exp_primary"); i_set = h.index("settle")
        for r in settle[1:]:
            if i_exp>=len(r) or i_set>=len(r): continue
            d = parse_date(r[i_exp]); s = _f(r[i_set])
            if d and s is not None: st_map[d]=s

    # Leo ideas
    leo_map: Dict[date, Dict[str, Any]] = {}
    if leo:
        h=[c.strip() for c in leo[0]]
        ix={k:i for i,k in enumerate(h)}
        for need in ["exp_primary","side","short_put","long_put","short_call","long_call","price"]:
            if need not in ix: 
                alerts.append(["config","leo_orders_missing_col",need,""])
                ix[need]=None
        for r in leo[1:]:
            d = parse_date(r[ix["exp_primary"]]) if ix["exp_primary"] is not None else None
            if not d: continue
            if d in leo_map:
                alerts.append(["config","leo_duplicate_expiry",d.isoformat(),""])
            leo_map[d]={
                "side": (r[ix["side"]] if ix["side"] is not None else "short"),
                "sp": _f(r[ix["short_put"]]) if ix["short_put"] is not None else None,
                "lp": _f(r[ix["long_put"]]) if ix["long_put"] is not None else None,
                "sc": _f(r[ix["short_call"]]) if ix["short_call"] is not None else None,
                "lc": _f(r[ix["long_call"]]) if ix["long_call"] is not None else None,
                "price": _f(r[ix["price"]]) if ix["price"] is not None else None,
            }

    # Your original orders
    orig_map: Dict[date, Dict[str, Any]] = {}
    if orig:
        h=[c.strip() for c in orig[0]]
        ix={k:i for i,k in enumerate(h)}
        for need in ["exp_primary","side","short_put","long_put","short_call","long_call","price","contracts"]:
            if need not in ix: 
                alerts.append(["config","orig_orders_missing_col",need,""])
                ix[need]=None
        for r in orig[1:]:
            d = parse_date(r[ix["exp_primary"]]) if ix["exp_primary"] is not None else None
            if not d: continue
            if d in orig_map:
                alerts.append(["config","orig_duplicate_expiry",d.isoformat(),""])
            contracts = 1
            if ix["contracts"] is not None and ix["contracts"] < len(r) and str(r[ix["contracts"]]).strip():
                try:
                    contracts = int(float(r[ix["contracts"]]))
                except Exception:
                    alerts.append(["data","contracts_parse_fail",d.isoformat(),str(r[ix["contracts"]])])
                    contracts = 1
            orig_map[d]={
                "side": (r[ix["side"]] if ix["side"] is not None else "short"),
                "sp": _f(r[ix["short_put"]]) if ix["short_put"] is not None else None,
                "lp": _f(r[ix["long_put"]]) if ix["long_put"] is not None else None,
                "sc": _f(r[ix["short_call"]]) if ix["short_call"] is not None else None,
                "lc": _f(r[ix["long_call"]]) if ix["long_call"] is not None else None,
                "price": _f(r[ix["price"]]) if ix["price"] is not None else None,
                "contracts": contracts
            }

    # Realized adjusted from sw_txn_raw (per-ledger net, per-expiry), with roll detection
    adj_by_exp: Dict[date, float] = {}
    cutoff = (datetime.now(ET).date() - timedelta(days=1))
    if raw and len(raw[0]) >= len(RAW_HEADERS):
        head = raw[0]
        i_exp = head.index("exp_primary")
        i_net = head.index("net_amount")
        i_ledger = head.index("ledger_id")
        i_ts = head.index("ts")
        # ledger -> {expiries}, first_net, ts
        lg_exps: Dict[str, set] = {}
        lg_ts: Dict[str, Optional[date]] = {}
        lg_nets: Dict[str, float] = {}
        lg_net_rows: Dict[str, int] = {}
        for r in raw[1:]:
            need = max(i_exp, i_net, i_ledger, i_ts)+1
            if len(r) < need: 
                continue
            led = str(r[i_ledger]).strip()
            if not led:
                continue
            expd = parse_date(r[i_exp])
            if led not in lg_exps: lg_exps[led]=set()
            if expd: lg_exps[led].add(expd)
            # timestamp for ordering (unused here but might be useful later)
            if led not in lg_ts:
                try:
                    z=str(r[i_ts]).replace("Z","+00:00")
                    lg_ts[led] = datetime.fromisoformat(z).astimezone(ET).date()
                except Exception:
                    lg_ts[led] = None
            net = _f(r[i_net])
            if net is not None:
                lg_nets[led] = lg_nets.get(led, 0.0) + float(net)
                lg_net_rows[led] = lg_net_rows.get(led, 0)+1

        for led, net_total in lg_nets.items():
            exps = [e for e in (lg_exps.get(led) or []) if isinstance(e, date)]
            if not exps:
                alerts.append(["raw","ledger_missing_exp",led, net_total])
                continue
            if len(exps) > 1:
                alerts.append(["raw","ledger_multi_expiry_roll_skip", led, ", ".join(sorted(d.isoformat() for d in exps))])
                continue  # skip allocation; manual review
            d = exps[0]
            if d > cutoff:
                continue
            if lg_net_rows.get(led,0) > 1:
                alerts.append(["raw","ledger_multiple_net_rows", led, lg_net_rows[led]])
            adj_by_exp[d] = round(adj_by_exp.get(d, 0.0) + net_total, 2)

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

        # Leo normalized P&L
        leo_pnl_norm=None; leo_w=None; leo_price=None
        if d in leo_map and settle_px is not None:
            L = leo_map[d]
            sp,lp,sc,lc = L["sp"],L["lp"],L["sc"],L["lc"]
            side = L["side"]; price = L["price"]
            wp,wc,w = ic_widths(sp,lp,sc,lc)
            leo_w = w; leo_price = price
            if w is not None and price is not None:
                per = pnl_iron_condor(side, sp, lp, sc, lc, price, settle_px)
                ml = max_loss(side, w, price)
                if ml>0:
                    leo_pnl_norm = per * (UNIT_RISK/ml)
                    series_leo.append((d, round(leo_pnl_norm,2)))

        # Standard normalized P&L
        std_pnl_norm=None; std_w=None; std_price=None; std_contracts=None; std_risk_total=None
        if d in orig_map and settle_px is not None:
            O = orig_map[d]
            sp,lp,sc,lc = O["sp"],O["lp"],O["sc"],O["lc"]
            side = O["side"]; price = O["price"]; c = O["contracts"] or 1
            wp,wc,w = ic_widths(sp,lp,sc,lc)
            std_w=w; std_price=price; std_contracts=c
            if w is not None and price is not None:
                per = pnl_iron_condor(side, sp, lp, sc, lc, price, settle_px)
                ml = max_loss(side, w, price)
                std_risk_total = ml * c
                if std_risk_total>0:
                    std_pnl_norm = (per*c) * (UNIT_RISK/std_risk_total)
                    series_std.append((d, round(std_pnl_norm,2)))

        # Adjusted: realized dollars (from Schwab) and normalized (using std risk)
        adj_nom = adj_by_exp.get(d)
        adj_norm = None
        if adj_nom is not None and std_risk_total and std_risk_total>0:
            adj_norm = adj_nom * (UNIT_RISK/std_risk_total)
            series_adjN.append((d, round(adj_norm,2)))
        if adj_nom is not None and (std_risk_total is None or std_risk_total<=0):
            alerts.append(["calc","no_std_risk_for_norm", d.isoformat(), adj_nom])

        # Value-add vs Standard (normalized)
        val = None
        if adj_norm is not None and std_pnl_norm is not None:
            val = adj_norm - std_pnl_norm
            series_val.append((d, round(val,2)))

        # Alert: missing settle for a date we’re trying to evaluate Leo/Standard
        if settle_px is None and (d in leo_map or d in orig_map):
            alerts.append(["config","missing_settle", d.isoformat(), ""])

        rows.append([
            d.isoformat(),                             # exp_primary
            settle_px if settle_px is not None else "",# settle
            # Leo
            leo_w if leo_w is not None else "",
            leo_price if leo_price is not None else "",
            round(leo_pnl_norm,2) if leo_pnl_norm is not None else "",
            # Standard
            std_w if std_w is not None else "",
            std_price if std_price is not None else "",
            std_contracts if std_contracts is not None else "",
            round(std_risk_total,2) if std_risk_total is not None else "",
            round(std_pnl_norm,2) if std_pnl_norm is not None else "",
            # Adjusted
            round(adj_nom,2) if adj_nom is not None else "",
            round(adj_norm,2) if adj_norm is not None else "",
            # Value add
            round(val,2) if val is not None else "",
        ])

    # Write by-expiry
    by_headers = [
        "exp_primary","settle",
        "leo_width","leo_price","leo_pnl_norm",
        "std_width","std_price","std_contracts","std_risk_total","std_pnl_norm",
        "adjusted_realized","adjusted_pnl_norm",
        "value_add_vs_std"
    ]
    ensure_tab(svc, sid, OUT_TAB, by_headers)
    write_rows(svc, sid, OUT_TAB, by_headers, rows)

    # Build perf (Last-10/20 expiry dates) for each stream
    def lastN(series: List[Tuple[date,float]], n: int) -> List[float]:
        ser = sorted(series, key=lambda t:t[0], reverse=True)[:n]
        ser = list(reversed([v for _,v in ser]))
        return ser

    cats = [
        ("Leo (norm)", series_leo),
        ("Standard (norm)", series_std),
        ("Adjusted (norm)", series_adjN),
        ("Value add (Adj-Std, norm)", series_val),
    ]
    perf_rows = [["Category","Metric","Last10","Last20"]]
    for name, ser in cats:
        p10=lastN(ser,10); p20=lastN(ser,20)
        s10=stats(p10); s20=stats(p20)
        def n(x): return "" if x is None else round(x,4) if isinstance(x,float) else x
        perf_rows += [
            [name,"Count",           s10["count"],     s20["count"]],
            ["","Total",             n(s10["total"]),  n(s20["total"])],
            ["","Win_rate_pct",      n(s10["win_rate"]), n(s20["win_rate"])],
            ["","Profit_factor",     n(s10["profit_factor"]), n(s20["profit_factor"])],
            ["","Expectancy",        n(s10["expectancy"]), n(s20["expectancy"])],
            ["","Max_drawdown",      n(s10["max_dd"]), n(s20["max_dd"])],
            ["","Recovery_factor",   n(s10["recovery"]), n(s20["recovery"])],
            ["","Sharpe_per_trade",  n(s10["sharpe"]), n(s20["sharpe"])],
            ["","","",""],
        ]
    ensure_tab(svc, sid, PERF_TAB, ["Category","Metric","Last10","Last20"])
    write_rows(svc, sid, PERF_TAB, ["Category","Metric","Last10","Last20"], perf_rows[1:])

    # Alerts
    ensure_tab(svc, sid, ALERTS_TAB, ["scope","issue","key","detail"])
    write_rows(svc, sid, ALERTS_TAB, ["scope","issue","key","detail"], alerts)

def main() -> int:
    svc, sid = sheets_client()
    build_three_way(svc, sid)
    print("OK: built sw_3way_by_expiry, sw_3way_perf, sw_3way_alerts.")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
