#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Three-way expiry summary (numeric outputs, with alerts).

Inputs:
  sw_txn_raw (A:R), sw_leo_orders, sw_settlements

Outputs:
  sw_3way_by_expiry
  sw_3way_perf
  sw_3way_alerts

Env:
  GSHEET_ID, GOOGLE_SERVICE_ACCOUNT_JSON
  UNIT_RISK (default 4500)   # dollars; used to normalize P&L
"""
import base64, json, os, math, re
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, date, timedelta, timezone
from zoneinfo import ZoneInfo
from google.oauth2 import service_account
from googleapiclient.discovery import build as gbuild

ET = ZoneInfo("America/New_York")
UNIT_RISK = float(os.environ.get("UNIT_RISK", "4500"))

RAW_TAB = "sw_txn_raw"
LEO_TAB = "sw_leo_orders"
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

# ---- ENV knobs for Standard detection (your pattern) ----
ORIG_ET_START = os.environ.get("ORIG_ET_START", "16:00")
ORIG_ET_END   = os.environ.get("ORIG_ET_END",   "16:20")
ORIG_DAYS_BEFORE = int(os.environ.get("ORIG_DAYS_BEFORE", "1"))
# Allocate multi-expiry ledger net across expiries by gross leg flow (avoid dropping roll P&L)
ALLOCATE_MULTI_EXP = os.environ.get("ALLOCATE_MULTI_EXP", "1").strip() in {"1","true","yes","on","y"}

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

def parse_sheet_datetime(x) -> Optional[datetime]:
    if isinstance(x, datetime):
        if x.tzinfo:
            return x
        return x.replace(tzinfo=timezone.utc)
    if isinstance(x, date):
        return datetime(x.year, x.month, x.day, tzinfo=timezone.utc)
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None

def parse_sheet_date(x) -> Optional[date]:
    if isinstance(x, date) and not isinstance(x, datetime):
        return x
    dt = parse_sheet_datetime(x)
    if isinstance(dt, datetime):
        return dt.astimezone(ET).date()
    return parse_date(x)

def _to_minutes(hhmm: str) -> int:
    try:
        hh, mm = [int(x) for x in hhmm.split(":")]
        return hh*60 + mm
    except Exception:
        return 16*60  # 16:00 fallback

def _et_date(dt: Optional[datetime]) -> Optional[date]:
    if not isinstance(dt, datetime): return None
    d = dt.astimezone(ET) if dt.tzinfo else dt.replace(tzinfo=ET)
    return d.date()

def _et_minutes(dt: Optional[datetime]) -> Optional[int]:
    if not isinstance(dt, datetime): return None
    d = dt.astimezone(ET) if dt.tzinfo else dt.replace(tzinfo=ET)
    return d.hour*60 + d.minute

def _safe_float(x) -> Optional[float]:
    try:
        if x is None or str(x).strip()=="":
            return None
        return float(x)
    except Exception:
        return None

def _multiplier(underlying: str, symbol: str) -> int:
    u = (underlying or "").upper()
    s = (symbol or "").upper()
    # OCC-coded options & index options → 100; else 1
    if re.search(r"\d{6}[CP]\d{8}$", s): return 100
    if u in {"SPX","SPXW","NDX","RUT","VIX","XSP"}: return 100
    return 1

def _is_ic_open(legs: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Classify a 4-leg iron condor OPEN (2P/2C, one +qty & one -qty per wing). Return dict or None."""
    puts  = [L for L in legs if (L.get("put_call")=="PUT")]
    calls = [L for L in legs if (L.get("put_call")=="CALL")]
    if len(puts)!=2 or len(calls)!=2:
        return None
    def split_sign(arr):
        pos=[L for L in arr if (_safe_float(L.get("quantity")) or 0)>0]
        neg=[L for L in arr if (_safe_float(L.get("quantity")) or 0)<0]
        return pos, neg
    p_pos, p_neg = split_sign(puts)
    c_pos, c_neg = split_sign(calls)
    if len(p_pos)!=1 or len(p_neg)!=1 or len(c_pos)!=1 or len(c_neg)!=1:
        return None
    sp = _safe_float(p_neg[0].get("strike"));  lp = _safe_float(p_pos[0].get("strike"))
    sc = _safe_float(c_neg[0].get("strike"));  lc = _safe_float(c_pos[0].get("strike"))
    if None in (sp,lp,sc,lc): return None
    contracts = int(round(min(abs(_safe_float(p_neg[0].get("quantity")) or 0.0),
                               abs(_safe_float(c_neg[0].get("quantity")) or 0.0))))
    if contracts < 1:
        return None
    wp = abs(sp-lp); wc = abs(lc-sc)
    width = max(wp, wc)
    # Attach signs orientation
    return {
        "short_put": sp, "long_put": lp, "short_call": sc, "long_call": lc,
        "width": width, "contracts": contracts
    }

def derive_standard_from_raw(raw: List[List[Any]], alerts: List[List[Any]]) -> Dict[date, Dict[str, Any]]:
    """Return per-expiry STANDARD from sw_txn_raw using 16:00–16:20 ET on expiry-1.
       Returns {exp: {side, width, price, contracts, risk_total, tickets:[{side,sp,lp,sc,lc,price,contracts}]}}"""
    if not raw or raw[0] != RAW_HEADERS:
        return {}
    head = raw[0]
    i_ts = head.index("ts"); i_exp = head.index("exp_primary"); i_pc = head.index("put_call")
    i_strk = head.index("strike"); i_qty = head.index("quantity"); i_amt = head.index("amount")
    i_net = head.index("net_amount"); i_und = head.index("underlying"); i_sym = head.index("symbol")
    i_ledger = head.index("ledger_id")

    # ledger aggregation
    led = {}  # ledger_id -> {ts:dt, exp_set:set[date], legs:[...], net:float}
    for r in raw[1:]:
        if len(r) < len(RAW_HEADERS):
            r = r + [""]*(len(RAW_HEADERS)-len(r))
        ledger = (r[i_ledger] or "").strip()
        if not ledger:
            continue
        und = (r[i_und] or "").strip().upper()
        if und not in {"SPX","SPXW","XSP"}:
            continue
        # parse
        dt = parse_sheet_datetime(r[i_ts])
        exp = parse_sheet_date(r[i_exp])
        pc  = (r[i_pc] or "").strip().upper() if r[i_pc] else ""
        strike = _safe_float(r[i_strk])
        qty    = _safe_float(r[i_qty]) or 0.0
        sym    = (r[i_sym] or "").strip()
        amt    = _safe_float(r[i_amt])
        if amt is None:
            # back-compute if missing
            price = _safe_float(r[raw[0].index("price")]) if "price" in head else None
            mult = _multiplier(und, sym)
            amt = (qty * price * mult) if (price is not None) else 0.0
        net = _safe_float(r[i_net])
        bucket = led.get(ledger)
        if not bucket:
            bucket = {"ts": dt, "exp_set": set(), "legs": [], "net": 0.0, "net_seen": False}
            led[ledger] = bucket
        if isinstance(exp, date):
            bucket["exp_set"].add(exp)
        if isinstance(dt, datetime):
            if (bucket["ts"] is None) or (dt < bucket["ts"]):
                bucket["ts"] = dt
        bucket["legs"].append({"put_call": pc, "strike": strike, "quantity": qty})
        if (net is not None) and (not bucket["net_seen"]):
            bucket["net"] += float(net); bucket["net_seen"] = True

    # candidates → aggregate by expiry in window
    start_min = _to_minutes(ORIG_ET_START); end_min = _to_minutes(ORIG_ET_END)
    std: Dict[date, Dict[str, Any]] = {}  # exp -> aggregate

    def add_component(exp: date, side: str, width: float, contracts: int, net: float, strikes=None):
        # net<0 => credit (short). price is positive number per condor
        price = abs(net) / (contracts * 100.0) if contracts>0 else 0.0
        risk = (width*100.0 - price*100.0) if side=="short" else (price*100.0)
        agg = std.setdefault(exp, {"side": side, "contracts": 0, "widths": {}, "price_sum": 0.0, "price_w": 0, "risk_total": 0.0, "tickets":[]})
        if side != agg["side"]:
            alerts.append(["std","mixed_side_for_expiry", exp.isoformat(), f"{agg['side']} vs {side}"])
        agg["contracts"] += contracts
        agg["widths"][round(width,3)] = agg["widths"].get(round(width,3), 0) + contracts
        agg["price_sum"] += price * contracts
        agg["price_w"]   += contracts
        agg["risk_total"] += risk * contracts
        if strikes:
            sp,lp,sc,lc = strikes
            agg["tickets"].append({"side":side,"sp":sp,"lp":lp,"sc":sc,"lc":lc,"price":price,"contracts":contracts})

    for lg, b in led.items():
        if len(b["exp_set"]) != 1:
            # not a single-expiry open; leave for adjusted allocation
            continue
        exp = next(iter(b["exp_set"]))
        tsd = _et_date(b["ts"])
        mins = _et_minutes(b["ts"])
        if not isinstance(tsd, date) or mins is None:
            continue
        # window check: ts date == exp - ORIG_DAYS_BEFORE and time in [start,end]
        if tsd != (exp - timedelta(days=ORIG_DAYS_BEFORE)) or not (start_min <= mins <= end_min):
            continue
        cls = _is_ic_open(b["legs"])
        if not cls:
            continue
        side = ("short" if (b["net"] or 0.0) < 0 else "long")
        strikes = (cls["short_put"], cls["long_put"], cls["short_call"], cls["long_call"])
        add_component(exp, side, cls["width"], cls["contracts"], (b["net"] or 0.0), strikes)

    # Fallback: if no window match for an expiry that clearly had an IC open, take earliest IC-open on exp-1 any time
    have_by_exp = set(std.keys())
    fallback_candidates: Dict[date, List[Tuple[datetime, str, float, int, float]]] = {}
    for lg, b in led.items():
        if len(b["exp_set"]) != 1:
            continue
        exp = next(iter(b["exp_set"]))
        if exp in have_by_exp:
            continue
        tsd = _et_date(b["ts"])
        if tsd != (exp - timedelta(days=ORIG_DAYS_BEFORE)):
            continue
        cls = _is_ic_open(b["legs"])
        if not cls:
            continue
        side = ("short" if (b["net"] or 0.0) < 0 else "long")
        fallback_candidates.setdefault(exp, []).append((b["ts"], side, cls["width"], cls["contracts"], (b["net"] or 0.0)))
    for exp, arr in fallback_candidates.items():
        arr.sort(key=lambda x: x[0])  # earliest first
        for _, side, width, contracts, net in arr:
            # No per-ledger strikes in fallback path; leave strikes=None. P&L will still be approx via width/price if needed.
            add_component(exp, side, width, contracts, net, strikes=None)
        alerts.append(["std","used_fallback_no_time_window", exp.isoformat(), f"{len(arr)} ticket(s)"])

    # finalize: compute representative width (mode) & VWAP price
    for exp, agg in std.items():
        if not agg["widths"]:
            continue
        width_mode = max(agg["widths"].items(), key=lambda kv: kv[1])[0]
        if len(agg["widths"]) > 1:
            widths_txt = ";".join(f"{w}:{c}" for w,c in sorted(agg["widths"].items()))
            alerts.append(["std","mixed_widths_combined", exp.isoformat(), widths_txt])
        agg["width"] = width_mode
        agg["price"] = round((agg["price_sum"]/agg["price_w"]), 2) if agg["price_w"] else None
    return std

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

    # STANDARD: derive directly from sw_txn_raw using 16:00–16:20 ET on expiry-1
    std_map: Dict[date, Dict[str, Any]] = derive_standard_from_raw(raw, alerts)

    # Realized adjusted from sw_txn_raw (per-ledger net, per-expiry), with roll detection
    adj_by_exp: Dict[date, float] = {}
    cutoff = (datetime.now(ET).date() - timedelta(days=1))
    if raw and len(raw[0]) >= len(RAW_HEADERS):
        head = raw[0]
        i_exp = head.index("exp_primary")
        i_net = head.index("net_amount")
        i_ledger = head.index("ledger_id")
        i_ts = head.index("ts")
        i_amt = head.index("amount")
        # ledger -> {expiries}, first_net, ts
        lg_exps: Dict[str, set] = {}
        lg_ts: Dict[str, Optional[date]] = {}
        lg_nets: Dict[str, float] = {}
        lg_net_rows: Dict[str, int] = {}
        lg_abs_by_exp: Dict[str, Dict[date,float]] = {}
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
            # gross leg flow by expiry (for allocation)
            amt = _f(r[i_amt])
            if amt is None:
                # try recompute from price*qty*mult (rarely needed)
                try:
                    price = _f(r[head.index("price")])
                    qty = _f(r[head.index("quantity")]) or 0.0
                    mult = 100.0  # SPX options; safe default here
                    amt = qty*price*mult if (price is not None) else 0.0
                except Exception:
                    amt = 0.0
            if isinstance(expd, date):
                lg_abs_by_exp.setdefault(led, {})
                lg_abs_by_exp[led][expd] = round(lg_abs_by_exp[led].get(expd, 0.0) + abs(float(amt)), 2)

        for led, net_total in lg_nets.items():
            exps = [e for e in (lg_exps.get(led) or []) if isinstance(e, date)]
            if not exps:
                alerts.append(["raw","ledger_missing_exp",led, net_total])
                continue
            if len(exps) == 1 or not ALLOCATE_MULTI_EXP:
                d = exps[0]
                if d > cutoff:
                    continue
                if lg_net_rows.get(led,0) > 1:
                    alerts.append(["raw","ledger_multiple_net_rows", led, lg_net_rows[led]])
                adj_by_exp[d] = round(adj_by_exp.get(d, 0.0) + net_total, 2)
            else:
                # allocate net_total by gross leg flow weight per expiry
                weights = lg_abs_by_exp.get(led, {})
                tot = sum(weights.values())
                if tot <= 0:
                    alerts.append(["raw","ledger_multi_expiry_no_weights", led, "skip"])
                    continue
                for d, w in weights.items():
                    if d > cutoff:
                        continue
                    share = (w / tot)
                    adj_by_exp[d] = round(adj_by_exp.get(d, 0.0) + net_total*share, 2)
                alerts.append(["raw","ledger_multi_expiry_allocated", led,
                               "; ".join(f"{dd.isoformat()}:{weights[dd]:.2f}" for dd in sorted(weights.keys()))])

    # Build per-expiry rows
    all_dates = sorted(set(st_map.keys()) | set(leo_map.keys()) | set(std_map.keys()) | set(adj_by_exp.keys()), reverse=True)
    rows: List[List[Any]] = []
    series_leo: List[Tuple[date, float]] = []
    series_std: List[Tuple[date, float]] = []
    series_adjN: List[Tuple[date, float]] = []
    series_val: List[Tuple[date, float]] = []

    for d in all_dates:
        if d > cutoff:
            continue
        settle_px = st_map.get(d)

        # Standard (from raw) — compute P&L when settle exists
        std_pnl_norm=None; std_w=None; std_price=None; std_contracts=None; std_risk_total=None
        if d in std_map:
            S = std_map[d]
            std_w = S.get("width"); std_price = S.get("price"); std_contracts = S.get("contracts")
            std_risk_total = S.get("risk_total")
            if settle_px is not None and std_risk_total and std_risk_total>0:
                pnl_sum = 0.0
                tickets = S.get("tickets") or []
                if tickets:
                    # precise per-ticket payoff
                    for t in tickets:
                        sp,lp,sc,lc = t["sp"],t["lp"],t["sc"],t["lc"]
                        per = pnl_iron_condor(t["side"], sp, lp, sc, lc, t["price"], settle_px)
                        pnl_sum += per * (t["contracts"] or 1)
                else:
                    # approximate using width/price if strikes are missing
                    w = std_w; px = std_price; side = S.get("side","short")
                    if w is not None and px is not None:
                        # Build proxy symmetric wings around settle (ok for PM 0DTE stats)
                        sp = settle_px - w/2.0; lp = sp - w
                        sc = settle_px + w/2.0; lc = sc + w
                        per = pnl_iron_condor(side, sp, lp, sc, lc, px, settle_px)
                        pnl_sum += per * (std_contracts or 1)
                        alerts.append(["std","approx_pnl_used_no_strikes", d.isoformat(), f"w={w} px={px}"])
                std_pnl_norm = pnl_sum * (UNIT_RISK / std_risk_total)
                series_std.append((d, round(std_pnl_norm,2)))

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
        if settle_px is None and (d in leo_map or d in std_map):
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
