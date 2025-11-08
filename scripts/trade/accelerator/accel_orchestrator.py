#!/usr/bin/env python3
# ACCELERATOR — SPX 0DTE, ±3 Acceleration on 10m bars, :41 cadence (paper logging to Google Sheets)
from __future__ import annotations
import os, json
import pandas as pd

from accel_guard import now_pt, is_rth, is_first_tick, is_10m_gate, is_ignored
from accel_signal import compute_acceleration_10m, edge_on_last_bar, last_edge_anytime
from accel_strikes import Leg, pick_short_leg, mate_long_leg, vertical_nbbo_mid
from accel_gsheet import append_rows

from schwab.auth import client_from_token_file  # reuse your existing auth

STATE_PATH = os.environ.get("ACCEL_STATE_PATH", "scripts/trade/accelerator_state.json")
GSHEET_ID  = os.environ.get("ACCEL_SHEET_ID")
GSHEET_TAB = os.environ.get("ACCEL_SHEET_TAB","accelerator_paper")
GSA_JSON   = os.environ.get("ACCEL_GSA_JSON")

ACCEL_THR      = float(os.environ.get("ACCEL_THR","3.0"))
LENGTH         = int(os.environ.get("ACCEL_LENGTH","26"))
EMA_SPAN       = int(os.environ.get("ACCEL_EMA","9"))
WIDTH_POINTS   = 5
DELTA_FLOOR    = 0.45
DELTA_CEIL     = 0.50
SELL_OFFSET    = float(os.environ.get("ACCEL_SELL_OFFSET","0.10"))  # paper sell = MID - 0.10
MIN_CREDIT     = float(os.environ.get("ACCEL_MIN_CREDIT","0.40"))
QTY            = int(os.environ.get("ACCEL_QTY","1"))

# ---------- small state machine ----------
def _today_key() -> str:
    return now_pt().strftime("%Y-%m-%d")

def load_state() -> dict:
    st = {"bias": 0, "count": 0, "open_keys": [], "asof": _today_key(), "last_trade_id": None}
    if os.path.exists(STATE_PATH):
        try:
            disk = json.load(open(STATE_PATH))
            st.update(disk or {})
        except Exception:
            pass
    # daily rollover: clear positions at the start of each day
    if st.get("asof") != _today_key():
        st.update({"bias": 0, "count": 0, "open_keys": [], "asof": _today_key(), "last_trade_id": None})
    return st

def save_state(st: dict):
    os.makedirs(os.path.dirname(STATE_PATH) or ".", exist_ok=True)
    with open(STATE_PATH,"w") as f: json.dump(st, f)

def key_of(side: str, short_k: float, long_k: float) -> str:
    return f"{side}:{int(short_k)}-{int(long_k)}"

# ---------- schwab adapters (reuse your code) ----------
def schwab_client():
    app_key=os.environ["SCHWAB_APP_KEY"]
    app_secret=os.environ["SCHWAB_APP_SECRET"]
    token_json=os.environ["SCHWAB_TOKEN_JSON"]
    with open("schwab_token.json","w") as f: f.write(token_json)
    return client_from_token_file(api_key=app_key, app_secret=app_secret, token_path="schwab_token.json")

def fetch_spx_1m_bars(c, minutes=600) -> pd.Series:
    """
    Return a pandas Series of 1m SPX closes indexed by timestamp.
    >>> Replace this with your existing data fetch (scripts/data/*).
    """
    raise NotImplementedError("Wire to your existing 1m SPX fetch.")

def aggregate_10m(series_1m: pd.Series) -> pd.Series:
    s = series_1m.copy()
    s.index = pd.to_datetime(s.index)
    return s.sort_index().resample("10T").last().dropna()

def fetch_spxw_chain_today(c) -> dict:
    """
    Return {"expiry":"YYYY-MM-DD","legs":[{occ,right,strike,delta,bid,ask},...]} for today's SPXW.
    >>> Replace with your existing chain getter.
    """
    raise NotImplementedError("Wire to your existing SPXW 0DTE chain fetch.")

# ---------- rows ----------
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
        float(a1) if a1 == a1 else "", float(a2) if a2 == a2 else "",
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
    if not (first_tick or is_10m_gate(ts)):
        return "idle"

    st = load_state()
    c = schwab_client()

    # 1m -> 10m bars -> Acceleration
    closes_1m = fetch_spx_1m_bars(c, minutes=600)          # IMPLEMENT with your fetcher
    closes_10m = aggregate_10m(closes_1m)
    accel = compute_acceleration_10m(closes_10m, LENGTH, EMA_SPAN)

    # Edge logic
    edge_dir = last_edge_anytime(accel, ACCEL_THR) if first_tick else edge_on_last_bar(accel, ACCEL_THR)
    if edge_dir == 0 and first_tick:
        return "no_edge_at_0641"
    if edge_dir == 0:
        return "no_new_edge"

    chain = fetch_spxw_chain_today(c)                      # IMPLEMENT with your chain getter
    legs = [Leg(**l) if isinstance(l, dict) else l for l in chain["legs"]]
    side = "PUT" if edge_dir > 0 else "CALL"

    short_leg = pick_short_leg(legs, side, DELTA_FLOOR, DELTA_CEIL)
    if not short_leg:
        return "no_leg_45_50_delta"
    long_leg = mate_long_leg(short_leg, legs, WIDTH_POINTS)
    if not long_leg:
        return "no_exact_5wide"

    mid = vertical_nbbo_mid(short_leg, long_leg)
    underlying = float(closes_1m.iloc[-1])
    a1 = float(accel.iloc[-2]) if len(accel) >= 2 else float("nan")
    a2 = float(accel.iloc[-3]) if len(accel) >= 3 else float("nan")
    ts_str = ts.strftime("%Y%m%d_%H%M")

    # ----- SKIP if opening would "close" (duplicate strikes already open) -----
    new_key = key_of(side, short_leg.strike, long_leg.strike)
    open_keys = set(st.get("open_keys", []))
    if new_key in open_keys:
        # In live, this same-legs order could be interpreted as closing; skip.
        return "skip_would_close_or_duplicate"

    rows = []
    bias = int(st.get("bias", 0))
    count = int(st.get("count", 0))

    if first_tick and count == 0:
        trade_id = make_trade_id(ts_str, side, short_leg.strike, long_leg.strike)
        rows.append(gsheet_row("OPEN_0641", side, edge_dir, chain["expiry"], short_leg, long_leg, mid, underlying, a1, a2, trade_id))
        open_keys.add(new_key)
        st.update({"bias": edge_dir, "count": 1, "last_trade_id": trade_id, "open_keys": sorted(open_keys)})

    elif edge_dir == bias:
        if count == 1:
            trade_id = make_trade_id(ts_str, side, short_leg.strike, long_leg.strike)
            rows.append(gsheet_row("ADD", side, edge_dir, chain["expiry"], short_leg, long_leg, mid, underlying, a1, a2, trade_id))
            open_keys.add(new_key)
            st.update({"count": 2, "last_trade_id": trade_id, "open_keys": sorted(open_keys)})
        else:
            return "ignored_third"

    elif bias != 0 and edge_dir == -bias:
        # Close all (paper) + flip open
        rows.append([now_pt().strftime("%Y-%m-%d %H:%M"), "CLOSE_ALL", "", "", "", chain["expiry"],
                     "", "", "", "", "", "", "", "", underlying, a1, a2, f"from_bias={bias}"])
        # Reset open set before adding new one
        open_keys.clear()
        side_new = "PUT" if edge_dir > 0 else "CALL"
        trade_id = make_trade_id(ts_str, side_new, short_leg.strike, long_leg.strike)
        rows.append(gsheet_row("FLIP_OPEN", side_new, edge_dir, chain["expiry"], short_leg, long_leg, mid, underlying, a1, a2, trade_id))
        open_keys.add(key_of(side_new, short_leg.strike, long_leg.strike))
        st.update({"bias": edge_dir, "count": 1, "last_trade_id": trade_id, "open_keys": sorted(open_keys)})

    else:
        # bias == 0 after 06:41 -> open post
        trade_id = make_trade_id(ts_str, side, short_leg.strike, long_leg.strike)
        rows.append(gsheet_row("OPEN_POST", side, edge_dir, chain["expiry"], short_leg, long_leg, mid, underlying, a1, a2, trade_id))
        open_keys.add(new_key)
        st.update({"bias": edge_dir, "count": 1, "last_trade_id": trade_id, "open_keys": sorted(open_keys)})

    if rows and GSHEET_ID and GSA_JSON:
        append_rows(GSA_JSON, GSHEET_ID, GSHEET_TAB, rows)

    save_state(st)
    return "logged"

if __name__ == "__main__":
    print(main())
