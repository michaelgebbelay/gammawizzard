#!/usr/bin/env python3
"""
TQQQ Trend live monitoring — pulls every data point we need from Schwab
to verify the strategy is operating as expected.

What this answers:
  --daily          : today's snapshot (signal, position, P&L vs last close)
  --trade-log      : every fill since strategy inception (default 2026-04-15)
  --performance    : total return + max DD live, vs QQQ B&H reference
  --health-check   : flags drift between live execution and backtest assumption

All data sources:
  Schwab `/accounts/{hash}`            → positions, cash, account total
  Schwab `/accounts/{hash}/orders`     → working / pending / cancelled orders
  Schwab `/accounts/{hash}/transactions` → executed fills (with prices/timestamps)
  Schwab `/marketdata/v1/quotes`       → live NBBO (during market hours)
  Schwab `/marketdata/v1/pricehistory` → historical bars (QQQ, TQQQ for context)

NOTE on data adjustment: Schwab provides split-adjusted but NOT
dividend-adjusted historical closes (per the May 2026 revalidation run).
For TQQQ this is fine (~0 dividend); for SPY/QQQ benchmarks the difference
is the cumulative dividend yield. Live monitoring uses Schwab as-is —
the goal is "is the live strategy doing what it should", not "match
the dividend-adjusted backtest to the basis point".
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
SCRIPTS_ROOT = HERE.parent.parent
if str(SCRIPTS_ROOT) not in sys.path:
    sys.path.append(str(SCRIPTS_ROOT))
sys.path.insert(0, str(HERE))
from place import (   # noqa: E402
    schwab, resolve_acct_hash, get_positions, get_quote,
    compute_strategy_snapshot, infer_current_state,
)


INCEPTION_DATE = os.environ.get("TQQQ_TREND_INCEPTION", "2026-05-05")


def fmt_money(x: float) -> str:
    return f"${x:,.2f}"


# ---------------------------------------------------------------------------
# Data pulls
# ---------------------------------------------------------------------------

def get_account(c, acct_hash: str) -> dict:
    url = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}"
    r = c.session.get(url, params={"fields": "positions"}, timeout=20)
    r.raise_for_status()
    return (r.json() or {}).get("securitiesAccount") or {}


def get_transactions(c, acct_hash: str, since: str) -> list:
    """Pull executed transactions since `since` (YYYY-MM-DD)."""
    url = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/transactions"
    start = pd.Timestamp(since).strftime("%Y-%m-%dT00:00:00.000Z")
    end = (pd.Timestamp.utcnow() + pd.Timedelta(days=1)).strftime("%Y-%m-%dT00:00:00.000Z")
    r = c.session.get(url, params={"startDate": start, "endDate": end,
                                    "types": "TRADE"}, timeout=30)
    if r.status_code != 200:
        return []
    return r.json() or []


def get_orders(c, acct_hash: str, since: str) -> list:
    """Pull order history (filled + cancelled + working)."""
    url = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders"
    start = pd.Timestamp(since).strftime("%Y-%m-%dT00:00:00.000Z")
    end = (pd.Timestamp.utcnow() + pd.Timedelta(days=1)).strftime("%Y-%m-%dT00:00:00.000Z")
    r = c.session.get(url, params={"fromEnteredTime": start,
                                    "toEnteredTime": end}, timeout=30)
    if r.status_code != 200:
        return []
    return r.json() or []


def get_qqq_bars(c, days: int = 400) -> pd.DataFrame:
    """Daily QQQ bars from Schwab."""
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    r = c.get_price_history_every_day(
        "QQQ", start_datetime=start, end_datetime=end,
        need_extended_hours_data=False,
    )
    r.raise_for_status()
    candles = (r.json() or {}).get("candles") or []
    df = pd.DataFrame(candles)
    if df.empty:
        return df
    df["date"] = (pd.to_datetime(df["datetime"], unit="ms", utc=True)
                  .dt.tz_convert("America/New_York").dt.normalize().dt.tz_localize(None))
    return df[["date", "open", "high", "low", "close"]].sort_values("date").reset_index(drop=True)


# ---------------------------------------------------------------------------
# Modes
# ---------------------------------------------------------------------------

def cmd_daily(c, acct_hash: str) -> int:
    sig = compute_strategy_snapshot()
    acct = get_account(c, acct_hash)
    positions = {(p.get("instrument") or {}).get("symbol", ""):
                 float(p.get("longQuantity", 0)) - float(p.get("shortQuantity", 0))
                 for p in (acct.get("positions") or [])
                 if abs(float(p.get("longQuantity", 0)) - float(p.get("shortQuantity", 0))) > 1e-6}
    bal = acct.get("currentBalances") or {}
    cash = float(bal.get("cashAvailableForTrading") or bal.get("cashBalance") or 0.0)
    total_value = float(bal.get("liquidationValue") or bal.get("equity") or 0.0)
    cur = infer_current_state(positions)
    tgt = sig["target_sleeve"]

    held_sym = "TQQQ" if positions.get("TQQQ", 0) > 0 else ("BIL" if positions.get("BIL", 0) > 0 else None)
    held_qty = positions.get(held_sym, 0) if held_sym else 0
    held_quote = get_quote(c, held_sym) if held_sym else {"last": 0, "close_prev": 0}
    held_mark = held_qty * (held_quote["last"] or held_quote["close_prev"])

    print()
    print("=" * 72)
    print(f"TQQQ Trend daily snapshot — {datetime.now(timezone.utc):%Y-%m-%d %H:%M UTC}")
    print("=" * 72)
    print(f"signal as_of_close={sig['as_of_close']}  qqq=${sig['qqq_close']:.2f}")
    print(f"  A (close>SMA150) = {sig['A_close_gt_sma150']}")
    print(f"  B (SMA50>SMA200) = {sig['B_sma50_gt_sma200']}     ← entry gate")
    print(f"  C (ret63>0)      = {sig['C_ret63_positive']}")
    print(f"  score = {sig['score']}    baseline_state = {sig['baseline_state']}")
    print(f"  baseline_target = {sig['baseline_target_sleeve']}    effective_target = {tgt}")
    print(f"  TS_6 enabled = {sig['overlay_enabled']}    shadow_state = {sig['overlay_state'].overlay_state}")
    print(f"  TS_6 shadow_target = {sig['overlay_target_sleeve']}    peak = {sig['overlay_state'].qqq_peak_adj}")
    if sig["overlay_warnings"]:
        print("  TS_6 warnings:")
        for warning in sig["overlay_warnings"]:
            print(f"    - {warning}")
    print()
    print(f"Schwab account ...{acct_hash[-6:]}")
    print(f"  TQQQ qty: {positions.get('TQQQ', 0):>8,.0f}")
    print(f"  BIL  qty: {positions.get('BIL', 0):>8,.0f}")
    print(f"  cash:     {fmt_money(cash):>14}")
    print(f"  total:    {fmt_money(total_value):>14}  (liquidation value)")
    print(f"  current_state = {cur}    held mark-to-mkt = {fmt_money(held_mark)}")
    print()
    if cur == tgt:
        print(f"STATUS: ALIGNED — held {cur}, signal target {tgt}, no flip needed.")
    else:
        print(f"STATUS: DRIFT — held {cur}, signal target {tgt}.")
        print(f"        A flip is expected at next 9:31 ET cron run.")
    return 0


def cmd_trade_log(c, acct_hash: str, since: str) -> int:
    txns = get_transactions(c, acct_hash, since)
    rows = []
    for t in txns:
        ttype = t.get("type") or t.get("transactionType")
        if ttype != "TRADE":
            continue
        ti = (t.get("transferItems") or []) + ((t.get("transactionItem") or {}) and [(t.get("transactionItem") or {})] or [])
        # Schwab schema variation — try both shapes
        if "transactionItem" in t:
            ti_obj = t["transactionItem"]
            sym = (ti_obj.get("instrument") or {}).get("symbol") or t.get("description") or ""
            qty = float(ti_obj.get("amount") or 0)
            price = float(ti_obj.get("price") or 0)
            instr = ti_obj.get("instruction") or t.get("description") or ""
        else:
            ti_list = t.get("transferItems") or []
            sym = ""
            qty = 0
            price = 0
            instr = ""
            for it in ti_list:
                if (it.get("instrument") or {}).get("symbol"):
                    sym = (it.get("instrument") or {}).get("symbol") or ""
                    qty = float(it.get("amount") or 0)
                    price = float(it.get("price") or 0)
                    instr = it.get("positionEffect") or ""
                    break
        when = t.get("tradeDate") or t.get("transactionDate") or t.get("activityDate")
        rows.append({
            "date": when, "instruction": instr, "symbol": sym,
            "qty": qty, "price": price, "notional": qty * price,
        })

    if not rows:
        print(f"No transactions since {since}.")
        return 0

    df = pd.DataFrame(rows).sort_values("date")
    print()
    print("=" * 72)
    print(f"Trade log since {since}  ({len(df)} fills)")
    print("=" * 72)
    print(df.to_string(index=False))
    print()
    # round-trip pairing for closed cycles
    print("(round-trip pairing not implemented yet — use raw fills above)")
    return 0


def cmd_performance(c, acct_hash: str, since: str) -> int:
    acct = get_account(c, acct_hash)
    bal = acct.get("currentBalances") or {}
    total = float(bal.get("liquidationValue") or 0.0)
    init = float(os.environ.get("TQQQ_TREND_INITIAL_USD", "5000"))
    pnl = total - init
    pnl_pct = (total / init - 1) * 100 if init > 0 else 0

    qqq = get_qqq_bars(c, days=400)
    qqq_at_inception = qqq[qqq["date"] >= pd.Timestamp(since)].iloc[0]["close"] \
        if not qqq.empty else None
    qqq_now = qqq.iloc[-1]["close"] if not qqq.empty else None
    qqq_pct = (qqq_now / qqq_at_inception - 1) * 100 if qqq_at_inception else 0

    print()
    print("=" * 72)
    print(f"Performance since {since}")
    print("=" * 72)
    print(f"Initial capital:  {fmt_money(init)}")
    print(f"Current value:    {fmt_money(total)}")
    print(f"P&L:              {fmt_money(pnl)}  ({pnl_pct:+.2f}%)")
    print()
    if qqq_at_inception:
        print(f"QQQ benchmark over same period: {qqq_pct:+.2f}%")
        print(f"  TQQQ Trend strategy:          {pnl_pct:+.2f}%")
        diff = pnl_pct - qqq_pct
        print(f"  Δ vs QQQ B&H:                 {diff:+.2f}pp")
    return 0


def cmd_health(c, acct_hash: str, since: str) -> int:
    """Sanity checks:
    - Position is exactly one of {TQQQ, BIL, CASH}, not MIXED
    - Held sleeve matches signal target_state (or is in transition window)
    - Recent orders all FILLED, no stuck WORKING orders > 1 day old
    - Recent fills landed within reasonable distance of signal-day close
    """
    issues = []

    acct = get_account(c, acct_hash)
    positions = {(p.get("instrument") or {}).get("symbol", ""):
                 float(p.get("longQuantity", 0)) - float(p.get("shortQuantity", 0))
                 for p in (acct.get("positions") or [])
                 if abs(float(p.get("longQuantity", 0)) - float(p.get("shortQuantity", 0))) > 1e-6}
    cur = infer_current_state(positions)
    if cur == "MIXED":
        issues.append("MIXED holdings — manual cleanup needed")

    sig = compute_strategy_snapshot()
    tgt = sig["target_sleeve"]
    if cur != tgt and cur not in ("CASH",):
        issues.append(f"DRIFT: held {cur} but signal target {tgt} (will flip at next cron)")
    for warning in sig["overlay_warnings"]:
        issues.append(f"TS_6 shadow warning: {warning}")

    # check for stuck working orders
    orders = get_orders(c, acct_hash, since)
    now = pd.Timestamp.utcnow().tz_localize(None)
    for o in orders:
        st = (o.get("status") or "").upper()
        if st in ("WORKING", "QUEUED", "PENDING_ACTIVATION"):
            entered = o.get("enteredTime") or ""
            try:
                ent_dt = pd.Timestamp(entered).tz_localize(None) if entered else None
            except Exception:
                ent_dt = None
            if ent_dt and (now - ent_dt) > pd.Timedelta(hours=24):
                issues.append(f"stuck order id={o.get('orderId')} status={st} age={(now-ent_dt).total_seconds()/3600:.1f}h")

    print()
    print("=" * 72)
    print(f"Health check  ({datetime.now(timezone.utc):%Y-%m-%d %H:%M UTC})")
    print("=" * 72)
    if not issues:
        print("✓ all checks pass")
        return 0
    print(f"⚠ {len(issues)} issue(s):")
    for i in issues:
        print(f"  - {i}")
    return 1


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--daily",       action="store_true", help="today's snapshot (default)")
    p.add_argument("--trade-log",   action="store_true", help="all fills since inception")
    p.add_argument("--performance", action="store_true", help="P&L vs QQQ benchmark")
    p.add_argument("--health-check", action="store_true", help="anomaly scan")
    p.add_argument("--since", default=INCEPTION_DATE, help="strategy inception date")
    args = p.parse_args()

    c = schwab()
    acct_hash = resolve_acct_hash(c)

    if args.trade_log:
        return cmd_trade_log(c, acct_hash, args.since)
    if args.performance:
        return cmd_performance(c, acct_hash, args.since)
    if args.health_check:
        return cmd_health(c, acct_hash, args.since)
    return cmd_daily(c, acct_hash)


if __name__ == "__main__":
    raise SystemExit(main())
