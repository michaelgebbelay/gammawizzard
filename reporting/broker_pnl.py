"""Broker-first P&L: classify Schwab orders by strategy, match opens/closes, compute P&L.

Uses actual Schwab filled orders as the source of truth. Classifies trades by:
  - API tag (automated vs manual)
  - Fill time (ET window: CS=16:08-16:45, DS=16:00-16:15, BF=15:55-16:25, morning=09:30-10:00)
  - Spread structure (width, leg count, option type)

P&L computed two ways:
  - Closed early: open price vs close price (from matched close order)
  - Expired: open price vs SPX settlement (from sim cache or Leo signal rows)

Usage:
    python -m reporting.broker_pnl                 # full report
    python -m reporting.broker_pnl --strategy ds   # filter by strategy
    python -m reporting.broker_pnl --json          # JSON output
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

API_TAG = "TA_1michaelbelaygmailcom1755679459"
ET = ZoneInfo("America/New_York")
SPX_MULTIPLIER = 100


# ---------------------------------------------------------------------------
# OSI Symbol parsing
# ---------------------------------------------------------------------------

def parse_osi(symbol: str) -> tuple[str | None, str | None, float | None]:
    """Parse SPXW  260319P06535000 or SPX   260320P06520000 → (expiry, option_type, strike)."""
    m = re.match(r"(?:SPXW|SPX)\s+(\d{6})([PC])(\d{8})", symbol)
    if not m:
        return None, None, None
    raw_date, pc, raw_strike = m.groups()
    yy, mm, dd = raw_date[:2], raw_date[2:4], raw_date[4:6]
    expiry = f"20{yy}-{mm}-{dd}"
    strike = int(raw_strike) / 1000
    option_type = "PUT" if pc == "P" else "CALL"
    return expiry, option_type, strike


# ---------------------------------------------------------------------------
# Strategy classification
# ---------------------------------------------------------------------------

def classify_order(order: dict) -> str:
    """Classify a Schwab order into a strategy based on tag, time, structure."""
    api_tagged = order.get("tag", "") == API_TAG
    ctype = order.get("complexOrderStrategyType", "")

    legs = order.get("orderLegCollection", [])
    strikes = set()
    for leg in legs:
        _, _, strike = parse_osi(leg.get("instrument", {}).get("symbol", ""))
        if strike:
            strikes.add(strike)

    sorted_strikes = sorted(strikes)
    width = sorted_strikes[-1] - sorted_strikes[0] if len(sorted_strikes) >= 2 else 0
    num_distinct_strikes = len(sorted_strikes)

    # Parse fill time
    fill_utc = order.get("closeTime", order.get("enteredTime", ""))
    dt_et = None
    if fill_utc:
        try:
            dt_utc = datetime.fromisoformat(fill_utc.replace("+0000", "+00:00"))
            dt_et = dt_utc.astimezone(ET)
        except Exception:
            pass

    time_dec = (dt_et.hour + dt_et.minute / 60) if dt_et else 0

    if not api_tagged:
        if ctype == "BUTTERFLY" or num_distinct_strikes == 3:
            return "butterfly_manual"
        return "manual"

    # API-tagged orders
    if ctype == "BUTTERFLY" or num_distinct_strikes == 3:
        return "butterfly"
    if 9.0 <= time_dec <= 10.5:
        return "cs_morning"
    if width == 10 and 16.0 <= time_dec <= 16.25:
        return "dualside"
    return "constantstable"


# ---------------------------------------------------------------------------
# Order parsing
# ---------------------------------------------------------------------------

def parse_filled_orders(raw_orders: list[dict]) -> list[dict]:
    """Parse raw Schwab orders into structured trade records."""
    trades = []

    for o in raw_orders:
        if o.get("status") != "FILLED":
            continue

        legs_raw = o.get("orderLegCollection", [])
        fill_utc = o.get("closeTime", o.get("enteredTime", ""))
        dt_et = None
        if fill_utc:
            try:
                dt_utc = datetime.fromisoformat(fill_utc.replace("+0000", "+00:00"))
                dt_et = dt_utc.astimezone(ET)
            except Exception:
                pass

        parsed_legs = []
        for leg in legs_raw:
            inst = leg.get("instrument", {})
            expiry, option_type, strike = parse_osi(inst.get("symbol", ""))
            parsed_legs.append({
                "instruction": leg.get("instruction", ""),
                "qty": leg.get("quantity", 0),
                "expiry": expiry,
                "option_type": option_type,
                "strike": strike,
            })

        if not parsed_legs:
            continue

        strikes = sorted(set(l["strike"] for l in parsed_legs if l["strike"]))
        expiries = sorted(set(l["expiry"] for l in parsed_legs if l["expiry"]))
        option_types = sorted(set(l["option_type"] for l in parsed_legs if l["option_type"]))
        instructions = set(l["instruction"] for l in parsed_legs)

        order_type = o.get("orderType", "")
        signal = "SHORT" if order_type == "NET_CREDIT" else "LONG"

        strategy = classify_order(o)

        trades.append({
            "order_id": o.get("orderId"),
            "account": o.get("accountNumber"),
            "dt_et": dt_et,
            "fill_date": dt_et.strftime("%Y-%m-%d") if dt_et else None,
            "fill_time": dt_et.strftime("%H:%M") if dt_et else None,
            "strategy": strategy,
            "strategy_type": o.get("complexOrderStrategyType", ""),
            "order_type": order_type,
            "price": o.get("price", 0),
            "qty": int(o.get("filledQuantity", 0)),
            "legs": parsed_legs,
            "strikes": strikes,
            "expiries": expiries,
            "option_types": option_types,
            "width": strikes[-1] - strikes[0] if len(strikes) >= 2 else 0,
            "is_opening": any("OPEN" in i for i in instructions),
            "is_closing": any("CLOSE" in i for i in instructions),
            "signal": signal,
        })

    return trades


# ---------------------------------------------------------------------------
# Settlement lookup
# ---------------------------------------------------------------------------

def _extract_spot_from_close5(data: dict) -> float | None:
    """Extract SPX spot price from a close5.json payload."""
    chain = data.get("chain", {})
    for key in ("_underlying_price", "underlyingPrice"):
        val = chain.get(key)
        if isinstance(val, (int, float)) and val > 0:
            return float(val)
    return None


def _extract_spot_from_features(data: dict) -> float | None:
    """Extract SPX spot price from a close5_features.json payload."""
    spot = data.get("spot")
    if spot:
        return float(spot)
    return None


def load_settlements() -> dict[str, float]:
    """Load SPX settlement prices from sim cache (local + S3) and Leo signal rows.

    Lookup chain:
      1. Local sim/cache/{date}/close5_features.json  (spot field)
      2. Local sim/cache/{date}/close5.json           (chain._underlying_price)
      3. S3 gamma-sim-cache/{date}/features_close5.json  (spot field)
      4. S3 gamma-sim-cache/{date}/close5.json           (chain._underlying_price)
      5. DuckDB strategy_signal_rows.forward              (Leo forward)
    """
    settlements: dict[str, float] = {}
    repo_root = Path(__file__).resolve().parent.parent
    cache_dir = repo_root / "sim" / "cache"

    # 1. Scan local cache
    if cache_dir.is_dir():
        for date_dir in sorted(cache_dir.iterdir()):
            if not date_dir.is_dir():
                continue
            d = date_dir.name

            feat_path = date_dir / "close5_features.json"
            if feat_path.exists():
                try:
                    with open(feat_path) as f:
                        data = json.load(f)
                    spot = _extract_spot_from_features(data)
                    if spot:
                        settlements[d] = spot
                        continue
                except Exception:
                    pass

            c5_path = date_dir / "close5.json"
            if c5_path.exists():
                try:
                    with open(c5_path) as f:
                        data = json.load(f)
                    spot = _extract_spot_from_close5(data)
                    if spot:
                        settlements[d] = spot
                except Exception:
                    pass

    # 2. Supplement from S3 (for Lambda where local cache doesn't exist)
    try:
        from sim.data.s3_cache import s3_get_json, s3_list_dates

        s3_dates = s3_list_dates()
        for d in s3_dates:
            if d in settlements:
                continue

            # Try features_close5.json first (lighter payload)
            feat_data = s3_get_json(d, "features_close5.json")
            if feat_data:
                spot = _extract_spot_from_features(feat_data)
                if spot:
                    settlements[d] = spot
                    continue

            # Fall back to close5.json
            c5_data = s3_get_json(d, "close5.json")
            if c5_data:
                spot = _extract_spot_from_close5(c5_data)
                if spot:
                    settlements[d] = spot
    except Exception:
        pass  # S3 not available (local dev without AWS creds)

    # 3. Supplement from DuckDB Leo signal rows
    try:
        from reporting.db import get_connection, query_df
        con = get_connection()
        df = query_df(
            "SELECT expiry_date, forward FROM strategy_signal_rows WHERE forward IS NOT NULL",
            con=con,
        )
        if not df.empty:
            for _, row in df.iterrows():
                exp = str(row["expiry_date"])[:10]
                if exp not in settlements and row["forward"]:
                    settlements[exp] = float(row["forward"])
    except Exception:
        pass

    return settlements


# ---------------------------------------------------------------------------
# P&L computation
# ---------------------------------------------------------------------------

def _spread_intrinsic(strikes: list[float], option_types: list[str],
                      settlement: float, num_legs: int) -> float:
    """Compute spread intrinsic value at settlement."""
    if num_legs == 3 or len(set(strikes)) == 3:
        # Butterfly
        unique = sorted(set(strikes))
        if len(unique) != 3:
            return 0.0
        low, mid, high = unique
        otype = option_types[0] if option_types else "CALL"
        if otype == "CALL":
            val = (max(0, settlement - low) - 2 * max(0, settlement - mid)
                   + max(0, settlement - high))
        else:
            val = (max(0, low - settlement) - 2 * max(0, mid - settlement)
                   + max(0, high - settlement))
        return max(0, val)

    if len(strikes) == 4:
        # Iron condor (4-leg)
        s = sorted(strikes)
        put_val = max(0, s[1] - settlement) - max(0, s[0] - settlement)
        call_val = max(0, settlement - s[2]) - max(0, settlement - s[3])
        return put_val + call_val

    if len(strikes) == 2:
        # Vertical spread
        low_s, high_s = strikes[0], strikes[1]
        otype = option_types[0] if option_types else "PUT"
        if otype == "PUT":
            return max(0, high_s - settlement) - max(0, low_s - settlement)
        else:
            return max(0, settlement - low_s) - max(0, settlement - high_s)

    if len(strikes) == 1:
        # Single leg (naked option)
        strike = strikes[0]
        otype = option_types[0] if option_types else "PUT"
        if otype == "PUT":
            return max(0, strike - settlement)
        else:
            return max(0, settlement - strike)

    return 0.0


def build_positions(
    trades: list[dict],
    settlements: dict[str, float],
    as_of: date | None = None,
    include_same_day_expiry: bool = False,
) -> list[dict]:
    """Match opens with closes, compute P&L for each position."""
    if as_of is None:
        as_of = datetime.now(ET).date()

    auto_strategies = ("constantstable", "dualside", "cs_morning", "butterfly")
    manual_strategies = ("manual", "butterfly_manual")
    all_strategies = auto_strategies + manual_strategies
    opens = [t for t in trades if t["strategy"] in all_strategies and t["is_opening"]]
    closes = [t for t in trades if t["is_closing"]]

    # Index closes by (strikes_tuple, expiries_tuple, option_types_tuple) for matching
    close_pool = list(closes)  # mutable copy

    positions = []

    for op in opens:
        expiry = op["expiries"][0] if op["expiries"] else None
        expiry_date = date.fromisoformat(expiry) if expiry else None
        op_key = (tuple(op["strikes"]), tuple(op["expiries"]), tuple(op["option_types"]))

        # Find matching close (same strikes, expiry, option type, after open time)
        # Also match by strategy_type to avoid cross-matching verticals/butterflies
        matched_close = None
        for i, cl in enumerate(close_pool):
            cl_key = (tuple(cl["strikes"]), tuple(cl["expiries"]), tuple(cl["option_types"]))
            if cl_key != op_key:
                continue
            if not (cl["dt_et"] and op["dt_et"] and cl["dt_et"] > op["dt_et"]):
                continue
            matched_close = cl
            close_pool.pop(i)  # consume this close
            break

        pnl = None
        exit_method = None
        exit_price = None
        close_date = None

        if matched_close:
            exit_method = "CLOSED_EARLY"
            close_date = matched_close["fill_date"]
            qty = min(op["qty"], matched_close.get("qty", op["qty"]))

            if op["order_type"] == "NET_CREDIT":
                if matched_close["order_type"] == "NET_DEBIT":
                    pnl_per = op["price"] - matched_close["price"]
                else:
                    pnl_per = op["price"] + matched_close["price"]
            else:
                if matched_close["order_type"] == "NET_CREDIT":
                    pnl_per = matched_close["price"] - op["price"]
                else:
                    pnl_per = -(op["price"] + matched_close["price"])

            pnl = pnl_per * qty * SPX_MULTIPLIER
            exit_price = matched_close["price"]

        elif expiry_date and (
            expiry_date < as_of or (include_same_day_expiry and expiry_date == as_of)
        ):
            settlement = settlements.get(expiry)
            if settlement:
                exit_method = "EXPIRED"
                intrinsic = _spread_intrinsic(
                    op["strikes"], op["option_types"], settlement,
                    len(set(l["strike"] for l in op["legs"] if l["strike"])),
                )
                if op["signal"] == "LONG":
                    pnl_per = intrinsic - op["price"]
                else:
                    pnl_per = op["price"] - intrinsic
                pnl = pnl_per * op["qty"] * SPX_MULTIPLIER
                exit_price = intrinsic
            else:
                exit_method = "EXPIRED_NO_SETTLEMENT"
        else:
            exit_method = "OPEN"

        positions.append({
            "strategy": op["strategy"],
            "fill_date": op["fill_date"],
            "fill_time": op["fill_time"],
            "expiry": expiry,
            "option_type": "/".join(op["option_types"]),
            "strikes": op["strikes"],
            "width": op["width"],
            "qty": op["qty"],
            "entry_price": op["price"],
            "signal": op["signal"],
            "exit_method": exit_method,
            "exit_price": exit_price,
            "close_date": close_date,
            "pnl": pnl,
        })

    return positions


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

STRATEGY_LABELS = {
    "constantstable": "ConstantStable (IC_LONG)",
    "dualside": "DualSide",
    "cs_morning": "CS Morning",
    "butterfly": "Butterfly Tuesday",
    "manual": "Discretionary",
    "butterfly_manual": "Discretionary Butterfly",
}

STRATEGY_ORDER = ("constantstable", "dualside", "cs_morning", "butterfly",
                  "manual", "butterfly_manual")


def _fmt(val: float) -> str:
    if val >= 0:
        return f"+${val:,.0f}"
    return f"-${abs(val):,.0f}"


def print_report(positions: list[dict], strategy_filter: str | None = None):
    """Print formatted P&L report."""
    today = datetime.now(ET).date()

    if strategy_filter:
        sf = strategy_filter.lower()
        alias = {"cs": "constantstable", "ds": "dualside", "bf": "butterfly",
                 "morning": "cs_morning", "ic": "constantstable"}
        sf = alias.get(sf, sf)
        positions = [p for p in positions if p["strategy"] == sf]

    print(f"\n{'=' * 75}")
    print("  GAMMA PORTFOLIO P&L — Automated Strategies")
    print(f"  Source: Schwab Broker Data  |  As of {today.isoformat()}")
    print(f"{'=' * 75}")

    strat_totals: dict[str, dict[str, Any]] = {}

    for strat in STRATEGY_ORDER:
        strat_pos = [p for p in positions if p["strategy"] == strat]
        if not strat_pos:
            continue

        label = STRATEGY_LABELS.get(strat, strat)
        settled = [p for p in strat_pos if p["exit_method"] in ("EXPIRED", "CLOSED_EARLY")]
        still_open = [p for p in strat_pos if p["exit_method"] == "OPEN"]
        no_settle = [p for p in strat_pos if p["exit_method"] == "EXPIRED_NO_SETTLEMENT"]

        totals = {"trades": 0, "wins": 0, "total": 0.0,
                  "open": len(still_open), "missing": len(no_settle)}

        print(f"\n{'─' * 75}")
        print(f"  {label}")
        print(f"{'─' * 75}")

        if settled:
            print(f"\n  {'Date':<12} {'Expiry':<12} {'Type':<6} {'Strikes':<22} "
                  f"{'Qty':>4} {'Entry':>7} {'Exit':>7} {'How':<9} {'P&L':>10}")
            print(f"  {'-' * 90}")

            for p in sorted(settled, key=lambda x: x["expiry"] or ""):
                s_str = "-".join(f"{s:.0f}" for s in p["strikes"])
                ep = f"${p['exit_price']:.2f}" if p["exit_price"] is not None else "—"
                pv = p["pnl"] or 0
                method = "closed" if p["exit_method"] == "CLOSED_EARLY" else "expired"
                print(f"  {p['fill_date']:<12} {p['expiry']:<12} {p['option_type']:<6} "
                      f"{s_str:<22} {p['qty']:>4} ${p['entry_price']:>5.2f} {ep:>7} "
                      f"{method:<9} {_fmt(pv):>10}")

                totals["trades"] += 1
                totals["total"] += pv
                if pv > 0:
                    totals["wins"] += 1

            wr = f"{totals['wins']/totals['trades']*100:.0f}%" if totals["trades"] > 0 else "—"
            print(f"\n  Settled: {totals['trades']} trades | Win rate: {wr} "
                  f"| P&L: {_fmt(totals['total'])}")

        if still_open:
            print(f"\n  Open ({len(still_open)}):")
            for p in sorted(still_open, key=lambda x: x["expiry"] or ""):
                s_str = "-".join(f"{s:.0f}" for s in p["strikes"])
                print(f"    {p['fill_date']} → exp {p['expiry']}  {p['option_type']} "
                      f"{s_str}  qty={p['qty']}  ${p['entry_price']:.2f} {p['signal']}")

        if no_settle:
            print(f"\n  Expired — need settlement ({len(no_settle)}):")
            for p in sorted(no_settle, key=lambda x: x["expiry"] or ""):
                s_str = "-".join(f"{s:.0f}" for s in p["strikes"])
                print(f"    exp {p['expiry']}  {p['option_type']} {s_str}  "
                      f"qty={p['qty']}  ${p['entry_price']:.2f}")

        strat_totals[strat] = totals

    # Grand summary
    print(f"\n{'=' * 75}")
    print(f"\n  {'Strategy':<25} {'Settled':>8} {'Win%':>6} {'P&L':>12} "
          f"{'Open':>6} {'Pending':>8}")
    print(f"  {'-' * 25} {'-' * 8} {'-' * 6} {'-' * 12} {'-' * 6} {'-' * 8}")

    grand = {"trades": 0, "wins": 0, "total": 0.0, "open": 0, "missing": 0}
    for strat in STRATEGY_ORDER:
        st = strat_totals.get(strat)
        if not st:
            continue
        t, w, p = st["trades"], st["wins"], st["total"]
        wr = f"{w/t*100:.0f}%" if t > 0 else "—"
        label = STRATEGY_LABELS.get(strat, strat)
        print(f"  {label:<25} {t:>8} {wr:>6} {_fmt(p):>12} "
              f"{st['open']:>6} {st['missing']:>8}")
        for k in grand:
            grand[k] += st[k]

    wr = f"{grand['wins']/grand['trades']*100:.0f}%" if grand["trades"] > 0 else "—"
    print(f"  {'─' * 25} {'─' * 8} {'─' * 6} {'─' * 12} {'─' * 6} {'─' * 8}")
    print(f"  {'TOTAL':<25} {grand['trades']:>8} {wr:>6} {_fmt(grand['total']):>12} "
          f"{grand['open']:>6} {grand['missing']:>8}")

    if grand["open"] > 0:
        print(f"\n  {grand['open']} positions still open (not yet expired)")
    if grand["missing"] > 0:
        print(f"  {grand['missing']} expired positions missing SPX settlement data")

    print(f"\n{'=' * 75}")


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_orders_from_file(path: str | Path) -> list[dict]:
    """Load raw Schwab orders from a JSON file."""
    with open(path) as f:
        return json.load(f)


def load_orders_from_db() -> list[dict]:
    """Load raw Schwab orders from DuckDB broker_raw_orders table."""
    try:
        from reporting.db import get_connection, query_df
        con = get_connection()
        df = query_df(
            "SELECT raw_payload FROM broker_raw_orders ORDER BY as_of DESC",
            con=con,
        )
        if df.empty:
            return []
        return [json.loads(row["raw_payload"]) for _, row in df.iterrows()]
    except Exception:
        return []


def load_orders_from_schwab(lookback_days: int = 30, max_retries: int = 3) -> list[dict]:
    """Pull orders directly from Schwab API (with retry on timeout)."""
    repo_root = Path(__file__).resolve().parent.parent
    scripts_dir = repo_root / "scripts"
    if str(scripts_dir) not in sys.path:
        sys.path.insert(0, str(scripts_dir))

    try:
        from schwab_token_keeper import schwab_client
        c = schwab_client()
    except Exception:
        import schwab
        from schwab.auth import client_from_token_file
        token_path = os.environ.get("SCHWAB_TOKEN_PATH", "/tmp/schwab_token.json")
        app_key = os.environ["SCHWAB_APP_KEY"]
        app_secret = os.environ["SCHWAB_APP_SECRET"]
        c = schwab.auth.client_from_token_file(token_path, app_key, app_secret)

    # Extend httpx timeout to 60s (default is 5s)
    try:
        import httpx
        c.session.timeout = httpx.Timeout(60.0)
    except Exception:
        pass

    resp = c.get_account_numbers()
    resp.raise_for_status()
    acct_hash = resp.json()[0]["hashValue"]

    since = datetime.combine(
        date.today() - timedelta(days=lookback_days), datetime.min.time()
    )

    for attempt in range(1, max_retries + 1):
        try:
            resp = c.get_orders_for_account(
                acct_hash,
                from_entered_datetime=since,
                to_entered_datetime=datetime.now(),
            )
            resp.raise_for_status()
            return resp.json() or []
        except Exception as exc:
            is_timeout = "timeout" in str(exc).lower() or "timed out" in str(exc).lower()
            if is_timeout and attempt < max_retries:
                wait = 5 * attempt
                print(f"[schwab] Timeout on attempt {attempt}/{max_retries}, retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Gamma P&L from Schwab broker data")
    parser.add_argument("--strategy", type=str, default=None,
                        help="Filter by strategy (cs, ds, bf, morning)")
    parser.add_argument("--json", action="store_true",
                        help="Output JSON instead of formatted table")
    parser.add_argument("--source", type=str, default="auto",
                        choices=["auto", "api", "db", "file"],
                        help="Order data source (default: auto)")
    parser.add_argument("--file", type=str, default=None,
                        help="Path to raw orders JSON file")
    args = parser.parse_args()

    # Load orders
    raw_orders = []
    if args.source == "file" or args.file:
        path = args.file or "/tmp/schwab_orders_raw.json"
        raw_orders = load_orders_from_file(path)
        print(f"[source] Loaded {len(raw_orders)} orders from {path}")
    elif args.source == "db":
        raw_orders = load_orders_from_db()
        print(f"[source] Loaded {len(raw_orders)} orders from DuckDB")
    elif args.source == "api":
        raw_orders = load_orders_from_schwab()
        print(f"[source] Loaded {len(raw_orders)} orders from Schwab API")
    else:
        # Auto: try file first, then DB, then API
        for loader, label in [
            (lambda: load_orders_from_file("/tmp/schwab_orders_raw.json"), "file"),
            (load_orders_from_db, "db"),
            (load_orders_from_schwab, "api"),
        ]:
            try:
                raw_orders = loader()
                if raw_orders:
                    print(f"[source] {label}: {len(raw_orders)} orders")
                    break
            except Exception:
                continue

    if not raw_orders:
        print("No orders found. Run with --source api to pull from Schwab.")
        return

    # Process
    trades = parse_filled_orders(raw_orders)
    settlements = load_settlements()
    positions = build_positions(trades, settlements)

    if args.json:
        out = []
        for p in positions:
            p_copy = dict(p)
            p_copy["strikes"] = [float(s) for s in p_copy["strikes"]]
            out.append(p_copy)
        print(json.dumps(out, indent=2, default=str))
    else:
        print_report(positions, strategy_filter=args.strategy)


if __name__ == "__main__":
    main()
