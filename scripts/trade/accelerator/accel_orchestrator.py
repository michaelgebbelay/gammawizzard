#!/usr/bin/env python3
# ACCELERATOR — SPX 0DTE, ±3 Acceleration, 10m cadence (paper logging: MID-0.10)
from __future__ import annotations
import os, json, time
import pandas as pd

from accel_guard import now_pt, is_rth, is_first_tick, is_10m_gate, is_ignored
from accel_signal import compute_acceleration_10m, edge_on_last_bar, last_edge_anytime
from accel_strikes import Leg, pick_short_leg, mate_long_leg, vertical_nbbo_mid
from accel_gsheet import append_rows

from schwab.auth import client_from_token_file

STATE_PATH = os.environ.get("ACCEL_STATE_PATH", "scripts/trade/accelerator_state.json")
GSHEET_ID  = os.environ.get("ACCEL_SHEET_ID")            # <-- set this
GSHEET_TAB = os.environ.get("ACCEL_SHEET_TAB","accelerator_paper")
GSA_JSON   = os.environ.get("ACCEL_GSA_JSON")            # path to service account JSON

ACCEL_THR      = float(os.environ.get("ACCEL_THR","3.0"))
LENGTH         = int(os.environ.get("ACCEL_LENGTH","26"))
EMA_SPAN       = int(os.environ.get("ACCEL_EMA","9"))
WIDTH_POINTS   = 5
DELTA_FLOOR    = 0.45
DELTA_CEIL     = 0.50
SELL_OFFSET    = float(os.environ.get("ACCEL_SELL_OFFSET","0.10"))  # MID - 0.10
MIN_CREDIT     = float(os.environ.get("ACCEL_MIN_CREDIT","0.40"))
QTY            = int(os.environ.get("ACCEL_QTY","1"))

# ---------- state ----------
def load_state() -> dict:
    if os.path.exists(STATE_PATH):
        try:
            return json.load(open(STATE_PATH))
        except Exception:
            pass
    return {"bias": 0, "count": 0, "last_trade_id": None}

def save_state(st: dict):
    os.makedirs(os.path.dirname(STATE_PATH) or ".", exist_ok=True)
    with open(STATE_PATH,"w") as f: json.dump(st, f)

# ---------- schwab helpers ----------
def schwab_client():
    app_key=os.environ["SCHWAB_APP_KEY"]
    app_secret=os.environ["SCHWAB_APP_SECRET"]
    token_json=os.environ["SCHWAB_TOKEN_JSON"]
    with open("schwab_token.json","w") as f: f.write(token_json)
    return client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")

def fetch_spx_1m_bars(c, minutes=600) -> pd.Series:
    """
    Return a pandas Series of SPX 1m closes (UTC timestamps index).
    Implement using your existing fetch util if you prefer; this placeholder expects you to
    replace with the same call you use today in scripts/data/*.
    """
    # ---- PLACEHOLDER ----
    # Raise so you remember to hook your existing data fetch.
    raise NotImplementedError("Wire to your existing 1m SPX fetch (scripts/data/*).")

def aggregate_10m(series_1m: pd.Series) -> pd.Series:
    # Resample to 10-minute closes
    s = series_1m.copy()
    s.index = pd.to_datetime(s.index)
    s = s.sort_index().resample("10T").last().dropna()
    return s

def fetch_spxw_chain_today(c) -> dict:
    """
    Return {"expiry":"YYYY-MM-DD","legs":[{occ,right,strike,delta,bid,ask},...]}
    Implement via your existing chain util (scripts/data/gw_api.py) or direct Schwab endpoint.
    """
    # ---- PLACEHOLDER ----
    raise NotImplementedError("Wire to your existing SPXW 0DTE chain fetch.")

# ---------- gsheet row builder ----------
def gsheet_row(event: str, side: str, dir_sign: int, expiry: str,
               short: Leg, long: Leg, mid: float, underlying: float,
               a1: float, a2: float, trade_id: str, note: str="") -> list:
    assumed = max(MIN_CREDIT, round(mid - SELL_OFFSET, 2))
    return [
        now_pt().strftime("%Y-%m-%d %H:%M"),
        event,
        trade_id,
        side, dir_sign, expiry,
        float(short.strike), float(long.strike), WIDTH_POINTS, QTY,
        abs(float(short.delta)), float(mid), float(assumed), SELL_OFFSET,
        float(underlying) if underlying is not None else "",
        float(a1) if a1 == a1 else "", float(a2) if a2 == a2 else "",  # NaN-safe
        note
    ]

def make_trade_id(ts_str: str, side: str, short_k: float, long_k: float) -> str:
    return f"{ts_str}_{side}_{int(short_k)}x{int(long_k)}"

# ---------- main ----------
def main():
    ts = now_pt()
    if not is_rth(ts) or is_ignored(ts, last_minutes=30):
        return "idle"

    first_tick = is_first_tick(ts, "06:41")
    gate = first_tick or is_10m_gate(ts)
    if not gate:
        return "idle"

    st = load_state()

    c = schwab_client()

    # 1m -> 10m Acceleration
    closes_1m = fetch_spx_1m_bars(c, minutes=600)  # pandas Series
    closes_10m = aggregate_10m(closes_1m)
    accel = compute_acceleration_10m(closes_10m, LENGTH, EMA_SPAN)

    # Edge logic
    edge_dir = last_edge_anytime(accel, ACCEL_THR) if first_tick else edge_on_last_bar(accel, ACCEL_THR)
    if edge_dir == 0 and first_tick:
        # no historical edge; do nothing at 06:41
        return "no_edge_at_0641"
    if edge_dir == 0:
        return "no_new_edge"

    # Chain + strikes
    chain = fetch_spxw_chain_today(c)  # must return {"expiry":..., "legs":[Leg(...), ...]}
    legs = [Leg(**l) if isinstance(l, dict) else l for l in chain["legs"]]
    side = "PUT" if edge_dir > 0 else "CALL"

    short_leg = pick_short_leg(legs, side, DELTA_FLOOR, DELTA_CEIL)
    if not short_leg:
        return "no_leg_45_50_delta"
    long_leg = mate_long_leg(short_leg, legs, WIDTH_POINTS)
    if not long_leg:
        return "no_exact_5wide"

    mid = vertical_nbbo_mid(short_leg, long_leg)
    if mid < MIN_CREDIT:
        # Still log it so we can review how often low-credit would have triggered
        pass

    # Underlying from the latest 1m bar
    underlying = float(closes_1m.iloc[-1])

    # Accel audit values
    a1 = float(accel.iloc[-2]) if len(accel) >= 2 else float("nan")
    a2 = float(accel.iloc[-3]) if len(accel) >= 3 else float("nan")

    # FSM: add once / ignore third / flip & close all
    bias = int(st.get("bias", 0))
    count = int(st.get("count", 0))
    ts_str = ts.strftime("%Y%m%d_%H%M")

    rows = []
    if first_tick and count == 0:
        # First trade of the day: open based on most recent edge
        trade_id = make_trade_id(ts_str, side, short_leg.strike, long_leg.strike)
        rows.append(gsheet_row("OPEN_0641", side, edge_dir, chain["expiry"], short_leg, long_leg, mid, underlying, a1, a2, trade_id))
        st.update({"bias": edge_dir, "count": 1, "last_trade_id": trade_id})

    elif edge_dir == bias:
        if count == 1:
            trade_id = make_trade_id(ts_str, side, short_leg.strike, long_leg.strike)
            rows.append(gsheet_row("ADD", side, edge_dir, chain["expiry"], short_leg, long_leg, mid, underlying, a1, a2, trade_id))
            st.update({"count": 2, "last_trade_id": trade_id})
        else:
            return "ignored_third"

    elif bias != 0 and edge_dir == -bias:
        # Flip: log a CLOSE_ALL row (paper) then a FLIP_OPEN
        # CLOSE_ALL: one line summary; P&L calc will reference previous opens by date later
        rows.append([
            now_pt().strftime("%Y-%m-%d %H:%M"), "CLOSE_ALL", "", "", "", chain["expiry"],
            "", "", "", "", "", "", "", "",
            underlying, a1, a2, f"from_bias={bias}"
        ])
        side_new = "PUT" if edge_dir > 0 else "CALL"
        trade_id = make_trade_id(ts_str, side_new, short_leg.strike, long_leg.strike)
        rows.append(gsheet_row("FLIP_OPEN", side_new, edge_dir, chain["expiry"], short_leg, long_leg, mid, underlying, a1, a2, trade_id))
        st.update({"bias": edge_dir, "count": 1, "last_trade_id": trade_id})

    else:
        # bias == 0 after 06:41 and we got an edge ⇒ open fresh
        trade_id = make_trade_id(ts_str, side, short_leg.strike, long_leg.strike)
        rows.append(gsheet_row("OPEN_POST", side, edge_dir, chain["expiry"], short_leg, long_leg, mid, underlying, a1, a2, trade_id))
        st.update({"bias": edge_dir, "count": 1, "last_trade_id": trade_id})

    # Push rows
    if rows and GSHEET_ID and GSA_JSON:
        append_rows(GSA_JSON, GSHEET_ID, GSHEET_TAB, rows)

    save_state(st)
    return "logged"

if __name__ == "__main__":
    print(main())
