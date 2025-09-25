#!/usr/bin/env python3
# 3-way summary by expiry: Leo vs Standard vs Adjusted (from raw), perf + alerts.

import os, json, re, math
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo
from google.oauth2 import service_account
from googleapiclient.discovery import build as gbuild
from googleapiclient.errors import HttpError

ET=ZoneInfo("America/New_York")

RAW_TAB="sw_txn_raw"
LEO_TAB="sw_leo_orders"
SETTLE_TAB="sw_settlements"
OUT_TAB="sw_3way_by_expiry"
PERF_TAB="sw_3way_perf"
ALERTS_TAB="sw_3way_alerts"

RAW_HEADERS=[
    "ts","txn_id","type","sub_type","description",
    "symbol","underlying","exp_primary","strike","put_call",
    "quantity","price","amount","net_amount","commissions","fees_other",
    "source","ledger_id"
]

UNIT_RISK=float(os.environ.get("UNIT_RISK","4500"))
ORIG_ET_START=os.environ.get("ORIG_ET_START","16:00")
ORIG_ET_END=os.environ.get("ORIG_ET_END","16:20")
ORIG_DAYS_BEFORE=int(os.environ.get("ORIG_DAYS_BEFORE","1"))
ALLOCATE_MULTI_EXP=str(os.environ.get("ALLOCATE_MULTI_EXP","1")).strip().lower() in {"1","true","yes","on","y"}

def sheets_client():
    creds=service_account.Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]),
        scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return gbuild("sheets","v4",credentials=creds), os.environ["GSHEET_ID"]

def get_values(svc,sid,rng):
    try:
        return svc.spreadsheets().values().get(spreadsheetId=sid, range=rng).execute().get("values",[])
    except HttpError as e:
        if e.resp.status==400 and "Unable to parse range" in str(e): return []
        raise

def ensure_tab(svc,sid,tab,headers):
    meta=svc.spreadsheets().get(spreadsheetId=sid).execute()
    names={s["properties"]["title"] for s in meta.get("sheets",[])}
    if tab not in names:
        svc.spreadsheets().batchUpdate(spreadsheetId=sid, body={"requests":[{"addSheet":{"properties":{"title":tab}}}]}).execute()
    got=svc.spreadsheets().values().get(spreadsheetId=sid, range=f"{tab}!1:1").execute().get("values",[])
    if not got or got[0]!=headers:
        svc.spreadsheets().values().update(spreadsheetId=sid, range=f"{tab}!A1",
            valueInputOption="RAW", body={"values":[headers]}).execute()

def write_rows(svc,sid,tab,headers,rows):
    svc.spreadsheets().values().clear(spreadsheetId=sid, range=tab).execute()
    svc.spreadsheets().values().update(spreadsheetId=sid, range=f"{tab}!A1",
        valueInputOption="RAW", body={"values":[headers]+rows}).execute()

def append_rows(svc,sid,tab,rows):
    if not rows: return
    svc.spreadsheets().values().append(spreadsheetId=sid, range=f"{tab}!A1",
        valueInputOption="RAW", insertDataOption="INSERT_ROWS", body={"values":rows}).execute()

def parse_sheet_datetime(value: str) -> Optional[datetime]:
    if not value: return None
    raw=str(value).strip()
    try:
        if raw.endswith("Z"): raw=raw[:-1]+"+00:00"
        if re.search(r"[+-]\d{4}$", raw): raw=raw[:-5]+raw[-5:-2]+":"+raw[-2:]
        return datetime.fromisoformat(raw)
    except Exception:
        return None

def parse_sheet_date(value) -> Optional[date]:
    if value is None: return None
    if isinstance(value, datetime):
        dt=value
    else:
        raw=str(value).strip()
        if not raw: return None
        try:
            dt=datetime.fromisoformat(raw)
        except Exception:
            try: return date.fromisoformat(raw)
            except Exception: return None
    if dt.tzinfo is not None: dt=dt.astimezone(ET)
    return dt.date()

def _to_minutes(hhmm: str)->int:
    try:
        hh,mm=[int(x) for x in hhmm.split(":")]
        return hh*60+mm
    except Exception: return 16*60

def _et_date(dt: Optional[datetime]) -> Optional[date]:
    if not isinstance(dt,datetime): return None
    d=dt.astimezone(ET) if dt.tzinfo else dt.replace(tzinfo=ET)
    return d.date()

def _et_minutes(dt: Optional[datetime]) -> Optional[int]:
    if not isinstance(dt,datetime): return None
    d=dt.astimezone(ET) if dt.tzinfo else dt.replace(tzinfo=ET)
    return d.hour*60+d.minute

def _f(x):
    try:
        if x is None or str(x).strip()=="":
            return None
        return float(x)
    except Exception:
        return None

# -------- payoff & stats helpers --------
def ic_widths(sp,lp,sc,lc):
    if None in (sp,lp,sc,lc): return (None,None,None)
    wp=abs(float(sp)-float(lp)); wc=abs(float(lc)-float(sc)); return (wp,wc,max(wp,wc))

def max_loss(side, w, price):
    if w is None or price is None: return None
    if side=="short": return (w - float(price))*100.0
    else: return float(price)*100.0

def pnl_iron_condor(side, sp, lp, sc, lc, price, settle):
    # price is positive credit (short) or debit (long)
    pnl = (float(price) if side=="short" else -float(price))
    pnl += -max(0.0, float(sp) - settle) + max(0.0, float(lp) - settle)  # puts
    pnl += -max(0.0, settle - float(sc)) + max(0.0, settle - float(lc))  # calls
    return pnl*100.0

def max_drawdown(pnls: List[float])->float:
    running=0.0; peak=0.0; max_dd=0.0
    for p in pnls:
        running+=p
        if running>peak: peak=running
        dd=peak-running
        if dd>max_dd: max_dd=dd
    return round(max_dd,2)

def stats_block(pnls: List[float])->Dict[str,Any]:
    xs=[float(x) for x in pnls if x is not None]
    n=len(xs); total=round(sum(xs),2)
    wins=sum(1 for v in xs if v>0); win_rate=(wins/n*100.0) if n else 0.0
    expectancy=(total/n) if n else 0.0
    dd=max_drawdown(xs) if n else 0.0
    sum_w=sum(v for v in xs if v>0); sum_l=sum(v for v in xs if v<0)
    pf=(sum_w/abs(sum_l)) if sum_l<0 else None
    rec=(total/dd) if dd else None
    sharpe=None
    if n>=2:
        m=total/n
        var=sum((v-m)**2 for v in xs)/(n-1)
        sd=math.sqrt(var) if var>0 else 0.0
        if sd>0: sharpe=m/sd
    return {"count":n,"total":total,"win_rate":win_rate,"profit_factor":pf,"expectancy":expectancy,"max_drawdown":dd,"recovery_factor":rec,"sharpe":sharpe}

# -------- Standard detection from raw --------
def _is_ic_open(legs: List[Dict[str,Any]])->Optional[Dict[str,Any]]:
    puts=[L for L in legs if L.get("put_call")=="PUT"]
    calls=[L for L in legs if L.get("put_call")=="CALL"]
    if len(puts)!=2 or len(calls)!=2: return None
    def split(arr):
        pos=[L for L in arr if (_f(L.get("quantity")) or 0)>0]
        neg=[L for L in arr if (_f(L.get("quantity")) or 0)<0]
        return pos,neg
    p_pos,p_neg=split(puts); c_pos,c_neg=split(calls)
    if len(p_pos)!=1 or len(p_neg)!=1 or len(c_pos)!=1 or len(c_neg)!=1: return None
    sp=_f(p_neg[0]["strike"]); lp=_f(p_pos[0]["strike"])
    sc=_f(c_neg[0]["strike"]); lc=_f(c_pos[0]["strike"])
    if None in (sp,lp,sc,lc): return None
    cp=abs(_f(p_neg[0]["quantity"]) or 0.0); cc=abs(_f(c_neg[0]["quantity"]) or 0.0)
    contracts=int(round(min(cp,cc)))
    if contracts<1: return None
    wp=abs(sp-lp); wc=abs(lc-sc); width=max(wp,wc)
    return {"short_put":sp,"long_put":lp,"short_call":sc,"long_call":lc,"width":width,"contracts":contracts}

def derive_standard_from_raw(raw: List[List[Any]], alerts: List[List[Any]]) -> Dict[date, Dict[str, Any]]:
    if not raw or raw[0]!=RAW_HEADERS: return {}
    head=raw[0]
    i_ts=head.index("ts"); i_exp=head.index("exp_primary"); i_pc=head.index("put_call")
    i_strk=head.index("strike"); i_qty=head.index("quantity"); i_amt=head.index("amount")
    i_net=head.index("net_amount"); i_und=head.index("underlying"); i_sym=head.index("symbol")
    i_ledger=head.index("ledger_id")
    led={}
    for r in raw[1:]:
        r=(r+[""]*len(RAW_HEADERS))[:len(RAW_HEADERS)]
        ledger=(r[i_ledger] or "").strip()
        if not ledger: continue
        und=(r[i_und] or "").strip().upper()
        if und not in {"SPX","SPXW","XSP"}: continue
        dt=parse_sheet_datetime(r[i_ts])
        exp=parse_sheet_date(r[i_exp])
        pc=(r[i_pc] or "").strip().upper()
        strike=_f(r[i_strk]); qty=_f(r[i_qty]) or 0.0
        sym=(r[i_sym] or "").strip()
        amt=_f(r[i_amt])
        if amt is None:
            price=_f(r[head.index("price")]) if "price" in head else None
            mult=100.0
            amt = qty*price*mult if (price is not None) else 0.0
        net=_f(r[i_net])
        b=led.get(ledger)
        if not b:
            b={"ts":dt,"exp_set":set(),"legs":[],"net":0.0,"net_seen":False}
            led[ledger]=b
        if isinstance(exp,date): b["exp_set"].add(exp)
        if isinstance(dt,datetime):
            if b["ts"] is None or dt<b["ts"]: b["ts"]=dt
        b["legs"].append({"put_call":pc,"strike":strike,"quantity":qty})
        if (net is not None) and (not b["net_seen"]):
            b["net"]+=float(net); b["net_seen"]=True

    start_min=_to_minutes(ORIG_ET_START); end_min=_to_minutes(ORIG_ET_END)
    std: Dict[date, Dict[str, Any]] = {}
    used=set()

    def add(exp: date, side: str, width: float, contracts: int, net: float, strikes=None):
        price=abs(net)/(contracts*100.0) if contracts>0 else 0.0
        risk=(width*100.0 - price*100.0) if side=="short" else (price*100.0)
        agg=std.setdefault(exp, {"side":side,"contracts":0,"widths":{},"price_sum":0.0,"price_w":0,"risk_total":0.0,"tickets":[]})
        if side!=agg["side"]:
            alerts.append(["std","mixed_side_for_expiry",exp.isoformat(),f"{agg['side']} vs {side}"])
        agg["contracts"]+=contracts
        agg["widths"][round(width,3)]=agg["widths"].get(round(width,3),0)+contracts
        agg["price_sum"]+=price*contracts
        agg["price_w"]+=contracts
        agg["risk_total"]+=risk*contracts
        if strikes:
            sp,lp,sc,lc=strikes
            agg["tickets"].append({"side":side,"sp":sp,"lp":lp,"sc":sc,"lc":lc,"price":price,"contracts":contracts})

    # window first
    for lg,b in led.items():
        if len(b["exp_set"])!=1: continue
        exp=next(iter(b["exp_set"]))
        tsd=_et_date(b["ts"]); mins=_et_minutes(b["ts"])
        if not isinstance(tsd,date) or mins is None: continue
        if tsd!=(exp - timedelta(days=ORIG_DAYS_BEFORE)) or not (start_min<=mins<=end_min): continue
        cls=_is_ic_open(b["legs"])
        if not cls: continue
        side = "short" if (b["net"] or 0.0)<0 else "long"
        strikes=(cls["short_put"], cls["long_put"], cls["short_call"], cls["long_call"])
        add(exp, side, cls["width"], cls["contracts"], (b["net"] or 0.0), strikes)
        used.add(lg)

    # fallback earliest on exp-1
    have=set(std.keys())
    fallback={}
    for lg,b in led.items():
        if len(b["exp_set"])!=1: continue
        exp=next(iter(b["exp_set"]))
        if exp in have: continue
        tsd=_et_date(b["ts"])
        if tsd!=(exp - timedelta(days=ORIG_DAYS_BEFORE)): continue
        cls=_is_ic_open(b["legs"])
        if not cls: continue
        side="short" if (b["net"] or 0.0)<0 else "long"
        fallback.setdefault(exp, []).append((b["ts"], side, cls["width"], cls["contracts"], (b["net"] or 0.0)))
    for exp, arr in fallback.items():
        arr.sort(key=lambda x: x[0])
        for _,side,width,contracts,net in arr:
            add(exp, side, width, contracts, net, strikes=None)
        alerts.append(["std","used_fallback_no_time_window",exp.isoformat(),f"{len(arr)} ticket(s)"])

    for exp,agg in std.items():
        if agg["widths"]:
            width_mode=max(agg["widths"].items(), key=lambda kv: kv[1])[0]
            if len(agg["widths"])>1:
                txt=";".join(f"{w}:{c}" for w,c in sorted(agg["widths"].items()))
                alerts.append(["std","mixed_widths_combined",exp.isoformat(),txt])
            agg["width"]=width_mode
            agg["price"]=round(agg["price_sum"]/agg["price_w"],2) if agg["price_w"] else None
    return std

# -------- Adjusted (realized) with roll allocation --------
def adjusted_from_raw(raw: List[List[Any]], alerts: List[List[Any]], cutoff: date)->Dict[date,float]:
    out={}
    if not raw or raw[0]!=RAW_HEADERS: return out
    head=raw[0]
    i_exp=head.index("exp_primary"); i_net=head.index("net_amount")
    i_ledger=head.index("ledger_id"); i_ts=head.index("ts")
    i_amt=head.index("amount"); i_pc=head.index("put_call")
    lg_exps={}; lg_ts={}; lg_nets={}; lg_net_rows={}; lg_abs_by_exp={}
    for r in raw[1:]:
        need=max(i_exp,i_net,i_ledger,i_ts)+1
        if len(r)<need: continue
        led=str(r[i_ledger]).strip()
        if not led: continue
        expd=parse_sheet_date(r[i_exp])
        lg_exps.setdefault(led,set())
        if expd: lg_exps[led].add(expd)
        if led not in lg_ts:
            try:
                z=str(r[i_ts]).replace("Z","+00:00")
                lg_ts[led]=datetime.fromisoformat(z).astimezone(ET).date()
            except Exception:
                lg_ts[led]=None
        net=_f(r[i_net])
        if net is not None:
            lg_nets[led]=lg_nets.get(led,0.0)+float(net)
            lg_net_rows[led]=lg_net_rows.get(led,0)+1
        amt=_f(r[i_amt]) or 0.0
        if isinstance(expd,date):
            lg_abs_by_exp.setdefault(led,{})
            lg_abs_by_exp[led][expd]=round(lg_abs_by_exp[led].get(expd,0.0)+abs(amt),2)

    for led, net_total in lg_nets.items():
        exps=[e for e in (lg_exps.get(led) or []) if isinstance(e,date)]
        if not exps:
            alerts.append(["raw","ledger_missing_exp",led,net_total]); continue
        if len(exps)==1 or not ALLOCATE_MULTI_EXP:
            d=exps[0]
            if d>cutoff: continue
            if lg_net_rows.get(led,0)>1:
                alerts.append(["raw","ledger_multiple_net_rows",led,lg_net_rows[led]])
            out[d]=round(out.get(d,0.0)+net_total,2)
        else:
            weights=lg_abs_by_exp.get(led,{})
            tot=sum(weights.values())
            if tot<=0:
                alerts.append(["raw","ledger_multi_expiry_no_weights",led,"skip"]); continue
            for d,w in weights.items():
                if d>cutoff: continue
                share=w/tot
                out[d]=round(out.get(d,0.0)+net_total*share,2)
            alerts.append(["raw","ledger_multi_expiry_allocated",led,
                           "; ".join(f"{dd.isoformat()}:{weights[dd]:.2f}" for dd in sorted(weights.keys()))])
    return out

# -------- main build --------
def build():
    svc,sid=sheets_client()
    ensure_tab(svc,sid,OUT_TAB,["exp_primary","settle","leo_width","leo_price","leo_pnl_norm",
                                "std_width","std_price","std_contracts","std_risk_total","std_pnl_norm",
                                "adjusted_realized","adjusted_pnl_norm","value_add_vs_std"])
    ensure_tab(svc,sid,PERF_TAB,["Category","Metric","Last10","Last20"])
    ensure_tab(svc,sid,ALERTS_TAB,["scope","issue","key","detail"])

    last_col=chr(ord("A")+len(RAW_HEADERS)-1)
    raw=get_values(svc,sid,f"{RAW_TAB}!A1:{last_col}")
    leo=get_values(svc,sid,f"{LEO_TAB}!A1:Z")
    settle=get_values(svc,sid,f"{SETTLE_TAB}!A1:Z")

    alerts=[]

    # settlements map
    st_map={}
    if settle:
        hh=[c.strip() for c in settle[0]]
        ix={k:i for i,k in enumerate(hh)}
        if "exp_primary" in ix and "settle" in ix:
            for r in settle[1:]:
                d=parse_sheet_date(r[ix["exp_primary"]]); v=_f(r[ix["settle"]])
                if isinstance(d,date) and v is not None: st_map[d]=float(v)

    # Leo map
    leo_map={}
    if leo:
        h=[c.strip() for c in leo[0]]
        ix={k:i for i,k in enumerate(h)}
        need=["exp_primary","side","short_put","long_put","short_call","long_call","price"]
        if all(k in ix for k in need):
            for r in leo[1:]:
                d=parse_sheet_date(r[ix["exp_primary"]])
                if not d: continue
                rec={"side":str(r[ix["side"]]).strip().lower() or "short",
                     "sp":_f(r[ix["short_put"]]),"lp":_f(r[ix["long_put"]]),
                     "sc":_f(r[ix["short_call"]]),"lc":_f(r[ix["long_call"]]),
                     "price":_f(r[ix["price"]])}
                leo_map[d]=rec

    cutoff=(datetime.now(ET).date()-timedelta(days=1))
    std_map=derive_standard_from_raw(raw, alerts)
    adj_by_exp=adjusted_from_raw(raw, alerts, cutoff)

    # all expiries we know about
    all_dates=sorted(set(st_map.keys())|set(leo_map.keys())|set(std_map.keys())|set(adj_by_exp.keys()), reverse=True)

    rows=[]
    series_leo=[]; series_std=[]; series_adj=[]
    for d in all_dates:
        if d>cutoff: continue
        settle_px=st_map.get(d)

        # Leo
        leo_pnl_norm=None; leo_w=None; leo_price=None
        if d in leo_map and settle_px is not None:
            L=leo_map[d]; sp,lp,sc,lc=L["sp"],L["lp"],L["sc"],L["lc"]
            side=L["side"]; price=L["price"]
            wp,wc,w=ic_widths(sp,lp,sc,lc); leo_w=w; leo_price=price
            if w is not None and price is not None:
                per=pnl_iron_condor(side, sp, lp, sc, lc, price, settle_px)
                ml=max_loss(side, w, price)
                if ml and ml>0:
                    leo_pnl_norm=per*(UNIT_RISK/ml); series_leo.append((d,round(leo_pnl_norm,2)))

        # Standard
        std_pnl_norm=None; std_w=None; std_price=None; std_contracts=None; std_risk_total=None
        if d in std_map:
            S=std_map[d]
            std_w=S.get("width"); std_price=S.get("price"); std_contracts=S.get("contracts")
            std_risk_total=S.get("risk_total")
            if settle_px is not None and std_risk_total and std_risk_total>0:
                pnl_sum=0.0
                tickets=S.get("tickets") or []
                if tickets:
                    for t in tickets:
                        per=pnl_iron_condor(t["side"], t["sp"], t["lp"], t["sc"], t["lc"], t["price"], settle_px)
                        pnl_sum += per * (t["contracts"] or 1)
                else:
                    # approximate using width/price if strikes missing
                    w=std_w; px=std_price; side=S.get("side","short")
                    if w is not None and px is not None:
                        sp=settle_px - w/2.0; lp=sp - w
                        sc=settle_px + w/2.0; lc=sc + w
                        per=pnl_iron_condor(side, sp, lp, sc, lc, px, settle_px)
                        pnl_sum += per * (std_contracts or 1)
                        alerts.append(["std","approx_pnl_used_no_strikes", d.isoformat(), f"w={w} px={px}"])
                std_pnl_norm=pnl_sum*(UNIT_RISK/std_risk_total); series_std.append((d,round(std_pnl_norm,2)))

        # Adjusted
        adj_nom=adj_by_exp.get(d); adj_norm=None
        if adj_nom is not None and std_risk_total and std_risk_total>0:
            adj_norm=adj_nom*(UNIT_RISK/std_risk_total); series_adj.append((d,round(adj_norm,2)))
        if adj_nom is not None and (not std_risk_total or std_risk_total<=0):
            alerts.append(["calc","no_std_risk_for_norm", d.isoformat(), adj_nom])

        val=None
        if adj_norm is not None and std_pnl_norm is not None: val=adj_norm-std_pnl_norm

        rows.append([
            d.isoformat(),
            settle_px if settle_px is not None else "",
            leo_w if leo_w is not None else "",
            leo_price if leo_price is not None else "",
            round(leo_pnl_norm,2) if leo_pnl_norm is not None else "",
            std_w if std_w is not None else "",
            std_price if std_price is not None else "",
            std_contracts if std_contracts is not None else "",
            round(std_risk_total,2) if std_risk_total is not None else "",
            round(std_pnl_norm,2) if std_pnl_norm is not None else "",
            round(adj_nom,2) if adj_nom is not None else "",
            round(adj_norm,2) if adj_norm is not None else "",
            round(val,2) if val is not None else "",
        ])

    # write main table
    write_rows(svc,sid,OUT_TAB,
        ["exp_primary","settle","leo_width","leo_price","leo_pnl_norm","std_width","std_price","std_contracts","std_risk_total","std_pnl_norm","adjusted_realized","adjusted_pnl_norm","value_add_vs_std"],
        rows)

    # perf
    def series_vals(series): 
        xs=[v for _,v in sorted(series)]  # ascending by date
        return xs[-10:], xs[-20:]
    def perf_block(name, xs10, xs20):
        s10=stats_block(xs10); s20=stats_block(xs20)
        return [
            [name,"Count", len(xs10), len(xs20)],
            ["","Total", round(sum(xs10),2), round(sum(xs20),2)],
            ["","Win_rate_pct", (s10["win_rate"] if xs10 else ""), (s20["win_rate"] if xs20 else "")],
            ["","Profit_factor", (round(s10["profit_factor"],2) if s10["profit_factor"] is not None else ""), (round(s20["profit_factor"],2) if s20["profit_factor"] is not None else "")],
            ["","Expectancy", (round(sum(xs10)/len(xs10),2) if xs10 else ""), (round(sum(xs20)/len(xs20),2) if xs20 else "")],
            ["","Max_drawdown", (s10["max_drawdown"] if xs10 else ""), (s20["max_drawdown"] if xs20 else "")],
            ["","Recovery_factor", (round(s10["recovery_factor"],2) if s10["recovery_factor"] else ""), (round(s20["recovery_factor"],2) if s20["recovery_factor"] else "")],
            ["","Sharpe_per_trade", (round(s10["sharpe"],2) if s10["sharpe"] else ""), (round(s20["sharpe"],2) if s20["sharpe"] else "")]
        ]
    xs10,xs20=series_vals(series_leo); block_leo=perf_block("Leo (norm)", xs10, xs20)
    xs10,xs20=series_vals(series_std); block_std=perf_block("Standard (norm)", xs10, xs20)
    xs10,xs20=series_vals(series_adj); block_adj=perf_block("Adjusted (norm)", xs10, xs20)
    perf_rows = block_leo + [["","","",""]] + block_std + [["","","",""]] + block_adj
    write_rows(svc,sid,PERF_TAB,["Category","Metric","Last10","Last20"], perf_rows)

    # alerts
    if alerts:
        append_rows(svc,sid,ALERTS_TAB,alerts)
    print(f"SUMMARY rows={len(rows)} perf_lines={len(perf_rows)} alerts_added={len(alerts)}")
    return 0

def main():
    return build()

if __name__=="__main__":
    raise SystemExit(main())
