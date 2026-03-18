#!/usr/bin/env python3
"""IC_LONG regime filter — pre-check that runs before trade execution.

Fetches VIX (GammaWizard with Schwab fallback), computes realized-vol
ratios from SPX closes, and writes a go/no-go decision to S3 for the
orchestrators to read.

Live rule (L1 regime switch — flip IC_LONG to RR_SHORT):
  VIX/RV10 >= 1.95  AND  RV5/RV20 <= 1.10

All skip filters (trail/ER3/SE) are disabled — backtest showed they
destroy value.

Scheduled at 4:01 PM ET (before 4:13 trade execution).
"""

__version__ = "2.0.0"

import json
import math
import os
import statistics
import sys
import time
from datetime import date, datetime, timedelta

import boto3
import requests


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

S3_BUCKET = (
    os.environ.get("CS_MOVE_STATE_S3_BUCKET")
    or os.environ.get("SIM_CACHE_BUCKET", "")
).strip()
S3_MOVE_KEY = os.environ.get("CS_MOVE_STATE_S3_KEY", "cadence/cs_spx_move_state.json")
S3_DECISION_KEY = os.environ.get("CS_IC_DECISION_S3_KEY", "cadence/cs_ic_long_decision.json")
TRAIL_DAYS = int(os.environ.get("CS_IC_LONG_TRAIL_DAYS", "5"))

GW_BASE = os.environ.get("GW_BASE", "https://gandalf.gammawizard.com").rstrip("/")
GW_ENDPOINT = os.environ.get("GW_ENDPOINT", "rapi/GetUltraPureConstantStable").lstrip("/")
GW_PROFIT_ENDPOINT = os.environ.get("GW_PROFIT_ENDPOINT", "rapi/GetLeoProfit").lstrip("/")


def fnum(x):
    try:
        return float(x)
    except Exception:
        return None


# ---------- S3 helpers ----------

def s3_get_json(key):
    if not S3_BUCKET:
        return None
    try:
        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception:
        return None


def s3_put_json(key, data):
    if not S3_BUCKET:
        print("IC_FILTER WARN: no S3 bucket configured")
        return
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=S3_BUCKET, Key=key,
        Body=json.dumps(data, indent=2).encode("utf-8"),
        ContentType="application/json",
    )


# ---------- SPX quote ----------

def fetch_spx_quote(c):
    r = c.session.get(
        "https://api.schwabapi.com/marketdata/v1/quotes",
        params={"symbols": "$SPX", "fields": "quote"},
        timeout=20,
    )
    r.raise_for_status()
    q = (r.json().get("$SPX") or {}).get("quote") or {}
    spot = fnum(q.get("lastPrice"))
    prev_close = fnum(q.get("closePrice"))
    return spot, prev_close


def fetch_vix_quote(c):
    """Fetch VIX from Schwab as fallback when GW doesn't provide it."""
    try:
        r = c.session.get(
            "https://api.schwabapi.com/marketdata/v1/quotes",
            params={"symbols": "$VIX", "fields": "quote"},
            timeout=20,
        )
        r.raise_for_status()
        q = (r.json().get("$VIX") or {}).get("quote") or {}
        return fnum(q.get("lastPrice"))
    except Exception as e:
        print(f"IC_FILTER WARN: VIX quote fetch failed: {e}")
        return None


# ---------- GammaWizard ----------

def gw_fetch_signal(endpoint=None):
    endpoint = endpoint or GW_ENDPOINT
    tok = (os.environ.get("GW_TOKEN", "") or "").strip().strip('"').strip("'")

    def hit(t):
        h = {"Accept": "application/json"}
        if t:
            clean = t.split(None, 1)[1] if t.lower().startswith("bearer ") else t
            h["Authorization"] = f"Bearer {clean}"
        return requests.get(f"{GW_BASE}/{endpoint}", headers=h, timeout=30)

    r = hit(tok) if tok else None
    if r is None or r.status_code in (401, 403):
        email = os.environ.get("GW_EMAIL", "")
        pwd = os.environ.get("GW_PASSWORD", "")
        if not (email and pwd):
            raise RuntimeError("GW_AUTH_REQUIRED")
        rr = requests.post(
            f"{GW_BASE}/goauth/authenticateFireUser",
            data={"email": email, "password": pwd}, timeout=30,
        )
        rr.raise_for_status()
        t = rr.json().get("token") or ""
        r = hit(t)

    r.raise_for_status()
    return extract_trade(r.json())


def extract_trade(j):
    if isinstance(j, dict):
        if "Trade" in j:
            tr = j["Trade"]
            return tr[-1] if isinstance(tr, list) and tr else tr if isinstance(tr, dict) else {}
        keys = ("Date", "TDate", "Limit", "CLimit", "LeftGo", "RightGo")
        if any(k in j for k in keys):
            return j
        for v in j.values():
            if isinstance(v, (dict, list)):
                t = extract_trade(v)
                if t:
                    return t
    if isinstance(j, list):
        for it in reversed(j):
            t = extract_trade(it)
            if t:
                return t
    return {}


# ---------- Chop filter (shared with butterfly SELL) ----------

IC_LONG_ER3_THRESHOLD = float(os.environ.get("CS_IC_LONG_ER3_THRESHOLD", "0.60"))
IC_LONG_SE5D_THRESHOLD = float(os.environ.get("CS_IC_LONG_SE5D_THRESHOLD", "1.00"))
SE_HISTORY_S3_KEY = "cadence/bf_straddle_eff_history.json"


# --- IV/RV Regime Rule: IC_LONG → RR_SHORT switch ---
# Backtested rule: +$5,557 edge over 315 IC_LONG trades (2016-2026).
# When VIX overprices fear (high VIX/RV10) and vol term structure is flat or
# contracting (low RV5/RV20), IC_LONG loses because SPX stays range-bound.
# Switching to RR_SHORT (buy put spread + sell call spread) is profitable.
IC_LONG_RR_SHORT_VIX_RV10_THRESHOLD = 1.95
IC_LONG_RR_SHORT_RV5_RV20_THRESHOLD = 1.10


def fetch_spx_closes(c, lookback_days=40):
    """Fetch recent SPX daily close prices from Schwab for RV computation."""
    try:
        end = datetime.now()
        start = end - timedelta(days=lookback_days)
        r = c.get_price_history_every_day(
            "$SPX", start_datetime=start, end_datetime=end,
            need_extended_hours_data=False,
        )
        r.raise_for_status()
        candles = sorted(r.json().get("candles", []), key=lambda x: x["datetime"])
        return [candle["close"] for candle in candles if candle.get("close")]
    except Exception as e:
        print(f"IC_FILTER WARN: SPX closes fetch failed: {e}")
        return []


def compute_realized_vol(closes, n):
    """Compute n-day annualized realized vol (decimal) from daily closes."""
    if len(closes) < n + 1:
        return None
    recent = closes[-(n + 1):]
    log_rets = [math.log(recent[i + 1] / recent[i]) for i in range(n)]
    if len(log_rets) < 2:
        return None
    return statistics.stdev(log_rets) * math.sqrt(252)


def regime_rule_check(vix_decimal, rv5, rv10, rv20):
    """Check VIX/RV10 + RV5/RV20 regime rule for IC_LONG → RR_SHORT switch.

    Returns (switch_to_rr_short, vix_rv10_ratio, rv5_rv20_ratio, reason).
    """
    if rv10 is None or rv10 <= 0 or rv5 is None or rv20 is None or rv20 <= 0:
        return False, None, None, "RV data unavailable — KEEP IC_LONG"

    vix_rv10 = vix_decimal / rv10
    rv5_rv20 = rv5 / rv20

    switch = (vix_rv10 >= IC_LONG_RR_SHORT_VIX_RV10_THRESHOLD
              and rv5_rv20 <= IC_LONG_RR_SHORT_RV5_RV20_THRESHOLD)

    tag = "SWITCH_TO_RR_SHORT" if switch else "KEEP_IC_LONG"
    reason = (f"{tag}: VIX/RV10={vix_rv10:.4f} (thr={IC_LONG_RR_SHORT_VIX_RV10_THRESHOLD}) "
              f"RV5/RV20={rv5_rv20:.5f} (thr={IC_LONG_RR_SHORT_RV5_RV20_THRESHOLD})")
    return switch, vix_rv10, rv5_rv20, reason


def compute_er3(c):
    """Compute 3-day price efficiency ratio from Schwab SPX candles.

    ER3 = |Close[t] - Close[t-3]| / (|d1| + |d2| + |d3|)
    Low = choppy/mean-reverting, high = directional trend.
    """
    try:
        end = datetime.now()
        start = end - timedelta(days=15)
        r = c.get_price_history_every_day(
            "$SPX", start_datetime=start, end_datetime=end,
            need_extended_hours_data=False,
        )
        r.raise_for_status()
        candles = sorted(r.json().get("candles", []), key=lambda x: x["datetime"])
        closes = [candle["close"] for candle in candles if candle.get("close")]
        if len(closes) < 4:
            return None
        recent = closes[-4:]
        d1, d2, d3 = recent[1] - recent[0], recent[2] - recent[1], recent[3] - recent[2]
        total_path = abs(d1) + abs(d2) + abs(d3)
        if total_path == 0:
            return 1.0
        return abs(recent[3] - recent[0]) / total_path
    except Exception as e:
        print(f"IC_FILTER WARN: ER3 compute failed: {e}")
        return None


def read_se_5d_avg():
    """Read SE 5d avg from butterfly's persisted SE history in S3."""
    data = s3_get_json(SE_HISTORY_S3_KEY)
    if not data:
        return None
    history = data.get("history", [])
    recent = [h["se"] for h in history[-5:] if h.get("se") is not None]
    if not recent:
        return None
    return sum(recent) / len(recent)


def chop_filter_check(c):
    """Check ER3 + SE5d chop filter (same rule as butterfly SELL).

    Returns (should_skip, er3, se_5d, reason).
    """
    er3 = compute_er3(c)
    se_5d = read_se_5d_avg()

    if er3 is None:
        return True, er3, se_5d, "ER3=None — data missing, IC_LONG skipped"
    if se_5d is None:
        return True, er3, se_5d, "SE5D=None — data missing, IC_LONG skipped"

    if se_5d > IC_LONG_SE5D_THRESHOLD:
        return True, er3, se_5d, f"SE5D={se_5d:.3f}>{IC_LONG_SE5D_THRESHOLD} — IC_LONG skipped"
    if er3 < IC_LONG_ER3_THRESHOLD:
        return True, er3, se_5d, f"ER3={er3:.3f}<{IC_LONG_ER3_THRESHOLD} — choppy market, IC_LONG skipped"

    return False, er3, se_5d, f"PASS (SE5D={se_5d:.3f}, ER3={er3:.3f})"


# ---------- Core logic ----------

def main():
    today_str = date.today().isoformat()
    print(f"IC_FILTER v{__version__} | date={today_str}")

    # 1. Schwab client + SPX quote
    try:
        c = schwab_client()
    except Exception as e:
        print(f"IC_FILTER SKIP: Schwab init failed: {e}")
        return 0

    spot, prev_close = fetch_spx_quote(c)
    if not spot or not prev_close or prev_close <= 0:
        print("IC_FILTER SKIP: SPX quote unavailable — writing ALLOW")
        s3_put_json(S3_DECISION_KEY, {
            "date": today_str, "ic_long_skip": False,
            "reason": "SPX quote unavailable", "updated_utc_epoch": int(time.time()),
        })
        return 0

    move_pct = abs(spot - prev_close) / prev_close * 100.0
    print(f"IC_FILTER SPX: spot={spot:.2f} prev_close={prev_close:.2f} move={abs(spot-prev_close):.2f}pts ({move_pct:.3f}%)")

    # 2. Update move history in S3
    state = s3_get_json(S3_MOVE_KEY) or {}
    history = state.get("move_history", [])

    if not history or history[-1].get("date") != today_str:
        history.append({"date": today_str, "move_pct": round(move_pct, 5)})
    else:
        history[-1]["move_pct"] = round(move_pct, 5)

    s3_put_json(S3_MOVE_KEY, {
        "move_history": history[-20:],
        "updated_utc_epoch": int(time.time()),
    })

    # 3. Compute trailing average
    recent = [e["move_pct"] for e in history[-TRAIL_DAYS:] if e.get("move_pct") is not None]
    if len(recent) < min(3, TRAIL_DAYS):
        print(f"IC_FILTER: insufficient history ({len(recent)} days) — writing ALLOW")
        s3_put_json(S3_DECISION_KEY, {
            "date": today_str, "ic_long_skip": False,
            "reason": f"insufficient history ({len(recent)} days)",
            "trail_move_pct": None, "anchor_pct": None,
            "updated_utc_epoch": int(time.time()),
        })
        return 0

    trail = sum(recent) / len(recent)

    # 3b. Chop filter (ER3 + SE5d — same rule as butterfly SELL)
    chop_skip, er3_val, se5d_val, chop_reason = chop_filter_check(c)
    print(f"IC_FILTER CHOP: {chop_reason}")

    # 4. Fetch GW signal for anchor strikes
    try:
        tr = gw_fetch_signal()
    except Exception as e:
        print(f"IC_FILTER SKIP: GW fetch failed: {e} — writing ALLOW")
        s3_put_json(S3_DECISION_KEY, {
            "date": today_str, "ic_long_skip": False,
            "reason": f"GW fetch failed: {e}",
            "trail_move_pct": round(trail, 5), "anchor_pct": None,
            "updated_utc_epoch": int(time.time()),
        })
        return 0

    if not tr or not tr.get("Limit") or not tr.get("CLimit"):
        print("IC_FILTER SKIP: no trade payload — writing ALLOW")
        s3_put_json(S3_DECISION_KEY, {
            "date": today_str, "ic_long_skip": False,
            "reason": "no trade payload",
            "trail_move_pct": round(trail, 5), "anchor_pct": None,
            "updated_utc_epoch": int(time.time()),
        })
        return 0

    inner_put = int(float(tr["Limit"]))
    inner_call = int(float(tr["CLimit"]))
    left_go = fnum(tr.get("LeftGo"))
    right_go = fnum(tr.get("RightGo"))

    # Determine structure
    is_ic_long = (left_go is not None and left_go > 0
                  and right_go is not None and right_go > 0)

    # Use signal SPX or midpoint of anchors
    spx = fnum(tr.get("SPX")) or ((inner_put + inner_call) / 2.0)
    dist_to_put = spx - inner_put
    dist_to_call = inner_call - spx
    min_dist = min(dist_to_put, dist_to_call)
    anchor_pct = (min_dist / spx * 100.0) if spx > 0 else 0.0

    structure = "IC_LONG" if is_ic_long else "OTHER"

    # 5. Regime rule: always compute RV + ratios (data ready before structure is known)
    #    The switch decision is only applied when structure is IC_LONG.
    switch_to_rr_short = False
    vix_rv10_ratio = None
    rv5_rv20_ratio = None
    regime_reason = ""
    regime_fires = False

    # Try GW VIX first, fall back to Schwab quote
    vix_gw = fnum(tr.get("VIX"))
    vix_source = "GW"
    if vix_gw is not None and vix_gw > 0:
        vix_dec = vix_gw / 100.0 if vix_gw > 1 else vix_gw
    else:
        vix_schwab = fetch_vix_quote(c)
        if vix_schwab is not None and vix_schwab > 0:
            vix_dec = vix_schwab / 100.0 if vix_schwab > 1 else vix_schwab
            vix_source = "Schwab"
            print(f"IC_FILTER VIX: GW unavailable, using Schwab VIX={vix_schwab:.2f}")
        else:
            vix_dec = None

    if vix_dec is not None and vix_dec > 0:
        closes = fetch_spx_closes(c, lookback_days=40)
        if len(closes) >= 21:
            rv5 = compute_realized_vol(closes, 5)
            rv10 = compute_realized_vol(closes, 10)
            rv20 = compute_realized_vol(closes, 20)
            if all(v is not None for v in (rv5, rv10, rv20)):
                print(f"IC_FILTER REGIME_RV: vix={vix_dec:.5f} (src={vix_source}) rv5={rv5:.5f} rv10={rv10:.5f} rv20={rv20:.5f}")
            else:
                print("IC_FILTER REGIME_RV: insufficient return data for RV")
            regime_fires, vix_rv10_ratio, rv5_rv20_ratio, regime_reason = \
                regime_rule_check(vix_dec, rv5, rv10, rv20)
        else:
            regime_reason = f"insufficient closes ({len(closes)}) for RV computation"
    else:
        regime_reason = "VIX unavailable from both GW and Schwab"
    print(f"IC_FILTER REGIME: {regime_reason}")

    # Set switch flag when regime fires — the orchestrator will check structure.
    # At 4:01 PM GW may not have LeftGo/RightGo yet (structure=OTHER),
    # but the orchestrator at 4:13 PM gets the signal and checks is_ic_long itself.
    if regime_fires:
        switch_to_rr_short = True

    # All skip filters disabled — backtest showed trail/chop/ER3 all destroy value.
    # Live rule is pure L1: switch IC_LONG → RR_SHORT when regime fires, keep otherwise.
    skip = False

    # Switch to RR_SHORT supersedes skip (the switch IS the action for this regime)
    if switch_to_rr_short:
        skip = False

    reason_parts = [
        f"structure={structure}",
        f"trail{TRAIL_DAYS}={trail:.3f}%",
        f"anchor={anchor_pct:.3f}%",
        f"ER3={er3_val:.3f}" if er3_val is not None else "ER3=N/A",
        f"SE5D={se5d_val:.3f}" if se5d_val is not None else "SE5D=N/A",
    ]
    reason = "ALLOW"

    # 5. If skipping IC_LONG, fetch Profit signal as substitute (Strategy C)
    profit_signal = None
    if skip:
        try:
            profit_tr = gw_fetch_signal(endpoint=GW_PROFIT_ENDPOINT)
            if profit_tr and profit_tr.get("Limit") and profit_tr.get("CLimit"):
                cat1 = fnum(profit_tr.get("Cat1"))
                cat2 = fnum(profit_tr.get("Cat2"))
                # Cat2 < Cat1 means Profit is calling IC_LONG (debit)
                profit_is_ic_long = (cat1 is not None and cat2 is not None
                                     and cat2 < cat1)
                profit_signal = {
                    "Limit": int(float(profit_tr["Limit"])),
                    "CLimit": int(float(profit_tr["CLimit"])),
                    "TDate": str(profit_tr.get("TDate", "")),
                    "Cat1": cat1,
                    "Cat2": cat2,
                    "is_ic_long": profit_is_ic_long,
                }
                side_tag = "IC_LONG" if profit_is_ic_long else "IC_SHORT"
                print(
                    f"IC_FILTER PROFIT: {side_tag} | "
                    f"Limit={profit_signal['Limit']} CLimit={profit_signal['CLimit']} "
                    f"Cat1={cat1} Cat2={cat2}"
                )
            else:
                print("IC_FILTER PROFIT: no valid payload from Profit endpoint")
        except Exception as e:
            print(f"IC_FILTER PROFIT: fetch failed ({e}) — skip only, no substitute")

    decision = {
        "date": today_str,
        "ic_long_skip": skip,
        "switch_to_rr_short": switch_to_rr_short,
        "reason": reason,
        "regime_reason": regime_reason,
        "structure": structure,
        "trail_move_pct": round(trail, 5),
        "anchor_pct": round(anchor_pct, 5),
        "er3": round(er3_val, 3) if er3_val is not None else None,
        "se_5d_avg": round(se5d_val, 3) if se5d_val is not None else None,
        "chop_skip": chop_skip,
        "vix_rv10_ratio": round(vix_rv10_ratio, 5) if vix_rv10_ratio is not None else None,
        "rv5_rv20_ratio": round(rv5_rv20_ratio, 5) if rv5_rv20_ratio is not None else None,
        "spot": round(spot, 2),
        "inner_put": inner_put,
        "inner_call": inner_call,
        "left_go": left_go,
        "right_go": right_go,
        "today_move_pct": round(move_pct, 5),
        "profit_signal": profit_signal,
        "updated_utc_epoch": int(time.time()),
    }

    s3_put_json(S3_DECISION_KEY, decision)

    tag = "SWITCH_RR_SHORT" if switch_to_rr_short else ("SKIP" if skip else "ALLOW")
    profit_tag = ""
    if skip and profit_signal:
        profit_tag = f" | profit_sub={'IC_LONG' if profit_signal['is_ic_long'] else 'IC_SHORT'}"
    regime_tag = ""
    if vix_rv10_ratio is not None:
        regime_tag = f" | VIX/RV10={vix_rv10_ratio:.4f} RV5/RV20={rv5_rv20_ratio:.5f}"
    print(
        f"IC_FILTER DECISION: {tag} | {' | '.join(reason_parts)}{regime_tag} | "
        f"put_anchor={inner_put} call_anchor={inner_call} spx={spx:.0f}{profit_tag}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
