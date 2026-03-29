"""Weekly P&L report from Schwab broker data.

Uses the net position method for expired options (sums net qty per strike
at each expiry and computes settlement value from SPX spot price). This
correctly handles discretionary legs entered as separate orders.

Usage:
    python -m reporting.weekly_pnl                        # current week
    python -m reporting.weekly_pnl --start 2026-03-23     # specific week
"""

from __future__ import annotations

import re
from collections import defaultdict
from datetime import date, timedelta
from zoneinfo import ZoneInfo

from reporting.broker_pnl import (
    load_orders_from_schwab,
    parse_filled_orders,
    parse_osi,
    load_settlements,
    build_positions,
    classify_order,
    API_TAG,
)
from reporting.daily_pnl_email import _load_spx_spot, _compute_discretionary_pnl
from scripts.schwab_token_keeper import schwab_client

ET = ZoneInfo("America/New_York")

LABELS = {
    "constantstable": "ConstantStable",
    "dualside": "DualSide",
    "cs_morning": "CS Morning",
    "butterfly": "Butterfly Tuesday",
    "manual": "Discretionary",
    "butterfly_manual": "Discretionary Butterfly",
}
STRAT_ORDER = [
    "constantstable", "dualside", "cs_morning", "butterfly",
    "manual", "butterfly_manual",
]


def _fmt(val: float) -> str:
    sign = "+" if val >= 0 else "-"
    return f"{sign}${abs(val):,.0f}"


# ---------------------------------------------------------------------------
# SPX settlement prices from Schwab
# ---------------------------------------------------------------------------

def _load_spx_closes() -> dict[str, float]:
    """Pull SPX daily close prices from Schwab price history."""
    try:
        from schwab.client import Client
        c = schwab_client()
        resp = c.get_price_history(
            "$SPX",
            period_type=Client.PriceHistory.PeriodType.MONTH,
            period=Client.PriceHistory.Period.THREE_MONTHS,
            frequency_type=Client.PriceHistory.FrequencyType.DAILY,
            frequency=Client.PriceHistory.Frequency.DAILY,
        )
        resp.raise_for_status()
        closes = {}
        from datetime import datetime
        for candle in resp.json().get("candles", []):
            ts = candle.get("datetime", 0)
            dt = datetime.fromtimestamp(ts / 1000)
            closes[dt.strftime("%Y-%m-%d")] = candle["close"]
        return closes
    except Exception as e:
        print(f"  [warn] Could not fetch SPX from Schwab: {e}")
        return {}


def _load_open_positions_schwab() -> list[dict]:
    """Fetch current open positions from Schwab."""
    try:
        c = schwab_client()
        acct_hash = c.get_account_numbers().json()[0]["hashValue"]
        resp = c.session.get(
            f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}",
            params={"fields": "positions"}, timeout=20,
        )
        if resp.status_code != 200:
            return []
        data = resp.json()
        sa = data[0]["securitiesAccount"] if isinstance(data, list) else (
            data.get("securitiesAccount") or data
        )
        return sa.get("positions", [])
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Net position method for expired P&L
# ---------------------------------------------------------------------------

def _compute_expired_pnl(
    raw_orders: list[dict],
    settlements: dict[str, float],
    week_start: str,
    week_end: str,
) -> dict:
    """Compute P&L for positions that expired during the week.

    Uses the net position method: for each expiry, sums all order legs into
    net qty per (strike, option_type), then computes:
        P&L = net_premiums + settlement_value

    Returns dict with per-expiry and total P&L.
    """
    expiry_cash: dict[str, float] = defaultdict(float)
    expiry_legs: dict[str, dict] = defaultdict(
        lambda: defaultdict(lambda: {"net_qty": 0})
    )

    for o in raw_orders:
        if o.get("status") != "FILLED":
            continue

        legs = o.get("orderLegCollection", [])
        order_type = o.get("orderType", "")
        price = o.get("price", 0)
        filled_qty = int(o.get("filledQuantity", 0))

        # Collect expiries for this order's legs
        leg_expiries = set()
        for leg in legs:
            inst = leg.get("instrument", {})
            e, _, _ = parse_osi(inst.get("symbol", "").strip())
            if e:
                leg_expiries.add(e)

        # Only single-expiry orders (avoids cross-expiry misattribution)
        if len(leg_expiries) != 1:
            continue
        exp = list(leg_expiries)[0]
        if not (week_start <= exp < week_end):
            continue

        # Track net qty per (strike, option_type)
        for leg in legs:
            inst = leg.get("instrument", {})
            _, option_type, strike = parse_osi(inst.get("symbol", "").strip())
            if not strike:
                continue
            instruction = leg.get("instruction", "")
            qty = int(leg.get("quantity", 0))
            signed_qty = qty if "BUY" in instruction else -qty
            expiry_legs[exp][(strike, option_type)]["net_qty"] += signed_qty

        # Track cash flow
        if order_type == "NET_CREDIT":
            expiry_cash[exp] += price * filled_qty * 100
        elif order_type == "NET_DEBIT":
            expiry_cash[exp] -= price * filled_qty * 100
        elif order_type == "LIMIT":
            for leg in legs:
                instruction = leg.get("instruction", "")
                if "SELL" in instruction:
                    expiry_cash[exp] += price * filled_qty * 100
                else:
                    expiry_cash[exp] -= price * filled_qty * 100
                break

    # Compute P&L per expiry
    results = {"by_expiry": {}, "total": 0.0}

    for exp in sorted(expiry_legs.keys()):
        settlement = settlements.get(exp)
        if not settlement:
            results["by_expiry"][exp] = {
                "pnl": None, "settlement": None,
                "premiums": expiry_cash.get(exp, 0),
                "note": "NO SETTLEMENT PRICE",
            }
            continue

        cash = expiry_cash.get(exp, 0)
        settle_val = 0.0
        residual = 0

        for (strike, otype), data in sorted(expiry_legs[exp].items()):
            nq = data["net_qty"]
            if nq == 0:
                continue
            residual += 1
            if otype == "PUT":
                intrinsic = max(0, strike - settlement)
            else:
                intrinsic = max(0, settlement - strike)
            settle_val += intrinsic * nq * 100

        pnl = cash + settle_val
        results["by_expiry"][exp] = {
            "pnl": pnl,
            "settlement": settlement,
            "premiums": cash,
            "settle_value": settle_val,
            "residual_legs": residual,
        }
        results["total"] += pnl

    return results


# ---------------------------------------------------------------------------
# Main report
# ---------------------------------------------------------------------------

def run(week_start: str | None = None, week_end: str | None = None):
    today = date.today()
    if not week_start:
        monday = today - timedelta(days=today.weekday())
        week_start = monday.isoformat()
    if not week_end:
        week_end = (today + timedelta(days=1)).isoformat()

    raw = load_orders_from_schwab(lookback_days=45)
    print(f"[source] Schwab API: {len(raw)} orders (45-day lookback)")

    # SPX settlement: prefer Schwab price history, fall back to Leo signals
    settlements = _load_spx_closes()
    if not settlements:
        print("  [fallback] Using Leo signal forward prices")
        settlements = load_settlements()
    else:
        print(f"  [settlement] SPX closes from Schwab ({len(settlements)} dates)")

    # --- Expired P&L via net position method ---
    expired = _compute_expired_pnl(raw, settlements, week_start, week_end)

    sep = "=" * 75
    dash = "-" * 75

    print(f"\n{sep}")
    print(f"  SCHWAB WEEKLY P&L — {week_start} to {week_end}")
    print(sep)

    # Expired breakdown by day
    print(f"\n{dash}")
    print("  Expired Positions (net position method)")
    print(dash)
    print(
        f"  {'Expiry':<12} {'SPX':>10} {'Premiums':>12}"
        f" {'Settlement':>12} {'P&L':>10} {'Legs':>5}"
    )

    for exp, data in sorted(expired["by_expiry"].items()):
        if data["pnl"] is None:
            print(f"  {exp:<12} {'N/A':>10} {_fmt(data['premiums']):>12}"
                  f" {'---':>12} {'---':>10} {'---':>5}")
            continue
        print(
            f"  {exp:<12} {data['settlement']:>10,.2f}"
            f" {_fmt(data['premiums']):>12}"
            f" {_fmt(data['settle_value']):>12}"
            f" {_fmt(data['pnl']):>10}"
            f" {data['residual_legs']:>5}"
        )

    print(f"\n  Automated expired total: {_fmt(expired['total'])}")

    # --- Discretionary P&L by day ---
    print(f"\n{dash}")
    print("  Discretionary Trades (pure manual, excludes auto adjustments)")
    print(dash)
    print(
        f"  {'Date':<12} {'SPX':>10} {'Trades':>7} {'Adj':>5}"
        f" {'Disc P&L':>10} {'Adj P&L':>10}"
    )

    disc_week_total = 0.0
    adj_week_total = 0.0
    # Iterate through each date in the week
    d = date.fromisoformat(week_start)
    end_d = date.fromisoformat(week_end)
    while d < end_d:
        if d.weekday() < 5:  # weekdays only
            spx = settlements.get(d.isoformat())
            if not spx:
                # Try loading from Schwab price history
                spx = _load_spx_spot(for_date=d)
            if spx:
                disc_result = _compute_discretionary_pnl(raw, d, spx)
                disc_pnl = disc_result["total_pnl"]
                adj_pnl = disc_result.get("adjustment_pnl", 0)
                pure_count = disc_result.get("pure_disc_count", 0)
                adj_count = disc_result.get("adjustment_count", 0)
                disc_week_total += disc_pnl
                adj_week_total += adj_pnl
                if disc_result["trade_count"] > 0:
                    print(
                        f"  {d.isoformat():<12} {spx:>10,.2f} {pure_count:>7} {adj_count:>5}"
                        f" {_fmt(disc_pnl):>10} {_fmt(adj_pnl):>10}"
                    )
                else:
                    print(f"  {d.isoformat():<12} {spx:>10,.2f} {'—':>7} {'—':>5} {'—':>10} {'—':>10}")
            else:
                print(f"  {d.isoformat():<12} {'N/A':>10} {'—':>7} {'—':>5} {'—':>10} {'—':>10}")
        d += timedelta(days=1)

    print(f"\n  Discretionary total: {_fmt(disc_week_total)}")
    print(f"  Auto adjustments:   {_fmt(adj_week_total)}")

    # --- Summary ---
    combined = expired["total"] + disc_week_total
    print(f"\n{sep}")
    print(f"  Automated (expired):  {_fmt(expired['total'])}")
    print(f"  Discretionary:        {_fmt(disc_week_total)}")
    print(f"  WEEK TOTAL:           {_fmt(combined)}")
    print(sep)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Schwab weekly P&L report")
    parser.add_argument("--start", default=None, help="Week start YYYY-MM-DD")
    parser.add_argument("--end", default=None, help="Week end YYYY-MM-DD")
    args = parser.parse_args()
    run(week_start=args.start, week_end=args.end)
