#!/usr/bin/env python3
"""IC_LONG regime filter — pre-check that runs before trade execution.

Fetches today's SPX move from Schwab, updates trailing move history in S3,
fetches Leo's signal from GammaWizard to get anchor strikes, and writes
a go/no-go decision to S3 for the orchestrators to read.

Rule: Skip IC_LONG when trailing 5-day avg |SPX move|% < nearest anchor
distance%. Backtested over 10 years: IC_LONG in this regime is 0 EV
(40% WR, -$1/trade over 169 trades). All other structures unaffected.

Scheduled at 4:01 PM ET (before 4:13 trade execution).
"""

__version__ = "2.0.0"

import json
import os
import sys
import time
from datetime import date

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
    skip = is_ic_long and trail < anchor_pct

    reason_parts = [
        f"structure={structure}",
        f"trail{TRAIL_DAYS}={trail:.3f}%",
        f"anchor={anchor_pct:.3f}%",
    ]
    if skip:
        reason = f"IC_LONG_SKIP: trail{TRAIL_DAYS}_move={trail:.3f}% < anchor_dist={anchor_pct:.3f}%"
    else:
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
        "reason": reason,
        "structure": structure,
        "trail_move_pct": round(trail, 5),
        "anchor_pct": round(anchor_pct, 5),
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

    tag = "SKIP" if skip else "ALLOW"
    profit_tag = ""
    if skip and profit_signal:
        profit_tag = f" | profit_sub={'IC_LONG' if profit_signal['is_ic_long'] else 'IC_SHORT'}"
    print(
        f"IC_FILTER DECISION: {tag} | {' | '.join(reason_parts)} | "
        f"put_anchor={inner_put} call_anchor={inner_call} spx={spx:.0f}{profit_tag}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
