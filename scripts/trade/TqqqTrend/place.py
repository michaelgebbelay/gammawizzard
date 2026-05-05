#!/usr/bin/env python3
"""
TQQQ Trend (a.k.a. C1-HYST) Schwab placement.

Three modes:
  --status               read positions, compute signal, print intent. Never trades.
  --initial-usd 5000     open a fresh position from cash (default --dry-run).
  --rebalance            check current Schwab holdings against today's target
                         state; flip if mismatch (default --dry-run).

Safety:
  * --dry-run (default): no orders are placed. Prints the JSON payload that
    WOULD be POSTed to Schwab plus the resolved share count.
  * --live: actually submits orders. Requires --i-understand-the-risk to be
    explicitly set on the command line; never enabled by default.

Trade convention:
  * Equity MARKET orders, DAY duration, NORMAL session, single-leg.
  * Initial entry: market BUY at next open (same day after-hours run is fine —
    Schwab queues until session open if placed pre-market).
  * Rebalance: market SELL of held sleeve, then market BUY of target sleeve.

Spec: scripts/conviction/backtest/C1_HYST_LOCKED_SPEC.md
Skill: .claude/skills/tqqq-trend/SKILL.md
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
SCRIPTS_ROOT = HERE.parent.parent   # repo/scripts
if str(SCRIPTS_ROOT) not in sys.path:
    sys.path.append(str(SCRIPTS_ROOT))

# ---------------------------------------------------------------------------
# Schwab client
# ---------------------------------------------------------------------------

def schwab():
    from schwab_token_keeper import schwab_client   # noqa: E402
    return schwab_client()


def resolve_acct_hash(c) -> str:
    ah = (os.environ.get("SCHWAB_ACCT_HASH") or "").strip()
    if ah:
        return ah
    r = c.get_account_numbers()
    r.raise_for_status()
    arr = r.json() or []
    return str((arr[0] or {}).get("hashValue") or "")


def get_positions(c, acct_hash: str) -> dict[str, float]:
    """Return {symbol: long_quantity} for the account."""
    url = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}"
    r = c.session.get(url, params={"fields": "positions"}, timeout=20)
    r.raise_for_status()
    body = r.json() or {}
    acc = (body.get("securitiesAccount") or {})
    positions = acc.get("positions") or []
    out = {}
    for p in positions:
        sym = (p.get("instrument") or {}).get("symbol") or ""
        qty_long = float(p.get("longQuantity") or 0.0)
        qty_short = float(p.get("shortQuantity") or 0.0)
        net = qty_long - qty_short
        if abs(net) > 1e-6:
            out[sym] = net
    return out


def get_quote(c, sym: str) -> dict:
    url = "https://api.schwabapi.com/marketdata/v1/quotes"
    r = c.session.get(url, params={"symbols": sym}, timeout=15)
    r.raise_for_status()
    body = r.json() or {}
    q = body.get(sym) or {}
    quote = q.get("quote") or {}
    return {
        "last": float(quote.get("lastPrice") or 0.0),
        "bid":  float(quote.get("bidPrice")  or 0.0),
        "ask":  float(quote.get("askPrice")  or 0.0),
        "close_prev": float(quote.get("closePrice") or 0.0),
    }


# ---------------------------------------------------------------------------
# Signal computation
# ---------------------------------------------------------------------------

def compute_signal_today() -> dict:
    """Return the C1-HYST signal evaluated on the most recent QQQ close."""
    import yfinance as yf
    qqq = yf.Ticker("QQQ").history(start="2024-06-01", auto_adjust=True)["Close"].astype(float)
    qqq.index = pd.to_datetime(qqq.index).tz_localize(None).normalize()
    df = pd.DataFrame({"close": qqq})
    df["sma50"]  = df["close"].rolling(50).mean()
    df["sma150"] = df["close"].rolling(150).mean()
    df["sma200"] = df["close"].rolling(200).mean()
    df["ret63"]  = df["close"].pct_change(63)
    last = df.dropna().iloc[-1]
    A = bool(last["close"] > last["sma150"])
    B = bool(last["sma50"]  > last["sma200"])
    C = bool(last["ret63"]  > 0)
    score = int(A) + int(B) + int(C)
    return {
        "as_of_close": df.index[-1].date().isoformat(),
        "qqq_close":   float(last["close"]),
        "sma50":       float(last["sma50"]),
        "sma150":      float(last["sma150"]),
        "sma200":      float(last["sma200"]),
        "ret63_pct":   float(last["ret63"]) * 100,
        "A_close_gt_sma150": A,
        "B_sma50_gt_sma200": B,
        "C_ret63_positive":  C,
        "score": score,
    }


def target_state(current_state: str, score: int) -> str:
    """Apply the locked C1-HYST hysteresis state machine."""
    if current_state == "TQQQ":
        return "BIL" if score <= 1 else "TQQQ"   # hold on score==2
    # current is BIL (or unknown — treat as BIL)
    return "TQQQ" if score == 3 else "BIL"


def infer_current_state(positions: dict[str, float]) -> str:
    """If we hold TQQQ -> 'TQQQ'. If we hold BIL -> 'BIL'. Otherwise 'CASH'."""
    tqqq = positions.get("TQQQ", 0)
    bil  = positions.get("BIL", 0)
    if tqqq > 0 and bil <= 0:
        return "TQQQ"
    if bil > 0 and tqqq <= 0:
        return "BIL"
    if tqqq > 0 and bil > 0:
        return "MIXED"
    return "CASH"


# ---------------------------------------------------------------------------
# Order placement
# ---------------------------------------------------------------------------

def build_market_order(symbol: str, instruction: str, quantity: int) -> dict:
    return {
        "orderType": "MARKET",
        "session": "NORMAL",
        "duration": "DAY",
        "orderStrategyType": "SINGLE",
        "orderLegCollection": [{
            "instruction": instruction,                # "BUY" | "SELL"
            "quantity": int(quantity),
            "instrument": {"symbol": symbol, "assetType": "EQUITY"},
        }],
    }


def submit_order(c, acct_hash: str, payload: dict) -> dict:
    url = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}/orders"
    r = c.session.post(url, json=payload, timeout=20)
    if r.status_code not in (200, 201):
        return {"ok": False, "status": r.status_code, "body": r.text[:500]}
    # Schwab returns the order id in the Location header
    loc = r.headers.get("Location") or ""
    order_id = loc.rsplit("/", 1)[-1] if loc else ""
    return {"ok": True, "status": r.status_code, "order_id": order_id, "location": loc}


# ---------------------------------------------------------------------------
# Modes
# ---------------------------------------------------------------------------

def cmd_status(args) -> int:
    sig = compute_signal_today()
    print(f"\n=== TQQQ Trend signal as of QQQ close {sig['as_of_close']} ===")
    print(f"  QQQ close:  ${sig['qqq_close']:.2f}")
    print(f"  SMA50:      ${sig['sma50']:.2f}")
    print(f"  SMA150:     ${sig['sma150']:.2f}")
    print(f"  SMA200:     ${sig['sma200']:.2f}")
    print(f"  ret63:      {sig['ret63_pct']:+.2f}%")
    print(f"  A (close > SMA150)    : {sig['A_close_gt_sma150']}")
    print(f"  B (SMA50 > SMA200)    : {sig['B_sma50_gt_sma200']}     ← load-bearing gate")
    print(f"  C (ret63 > 0)         : {sig['C_ret63_positive']}")
    print(f"  score = {sig['score']}")
    print()
    if args.no_schwab:
        print("(--no-schwab: skipped account lookup)")
        return 0
    c = schwab()
    acct_hash = resolve_acct_hash(c)
    pos = get_positions(c, acct_hash)
    cur = infer_current_state(pos)
    tgt = target_state(cur, sig["score"])
    print(f"=== Schwab account ===")
    print(f"  acct hash (last 6): ...{acct_hash[-6:]}")
    print(f"  TQQQ qty: {pos.get('TQQQ', 0):.0f}")
    print(f"  BIL  qty: {pos.get('BIL', 0):.0f}")
    print(f"  inferred current state: {cur}")
    print(f"  target state per spec:  {tgt}")
    if cur == tgt:
        print(f"  ACTION: HOLD (no flip needed)")
    elif cur == "CASH":
        print(f"  ACTION: open initial position in {tgt}")
    else:
        print(f"  ACTION: SELL {cur}, BUY {tgt}")
    return 0


def cmd_initial(args) -> int:
    if args.initial_usd <= 0:
        raise SystemExit("--initial-usd must be > 0")
    sig = compute_signal_today()
    target = target_state("CASH", sig["score"])
    if target == "TQQQ":
        symbol = "TQQQ"
    else:
        # score < 3 today: do NOT open a TQQQ position. Stay in cash (or BIL).
        if not args.force_bil:
            print(f"\nScore = {sig['score']} (need 3 to enter TQQQ). "
                  f"Initial entry into TQQQ is NOT allowed by spec right now.")
            print("To park initial $ in BIL until next score==3, re-run with --force-bil.")
            return 1
        symbol = "BIL"

    c = schwab()
    acct_hash = resolve_acct_hash(c)
    quote = get_quote(c, symbol)
    ref_price = quote["last"] or quote["close_prev"] or quote["ask"]
    if ref_price <= 0:
        raise SystemExit(f"could not get quote for {symbol}")
    qty = int(args.initial_usd // ref_price)
    if qty <= 0:
        raise SystemExit(f"${args.initial_usd} too small to buy 1 share of {symbol} at ${ref_price:.2f}")

    payload = build_market_order(symbol, "BUY", qty)
    notional_est = qty * ref_price

    print()
    print("=" * 72)
    print(f"INITIAL ENTRY  →  {symbol}  qty {qty}  ~${notional_est:,.2f} notional")
    print("=" * 72)
    print(f"  signal score:  {sig['score']}    (state machine target: {target})")
    print(f"  reference price ({symbol}):  ${ref_price:.2f}  (last)")
    print(f"  budget:        ${args.initial_usd:,.2f}")
    print(f"  shares:        {qty}")
    print(f"  notional est:  ${notional_est:,.2f}  (${args.initial_usd - notional_est:,.2f} unused)")
    print(f"  account hash:  ...{acct_hash[-6:]}")
    print(f"  payload JSON:")
    print(json.dumps(payload, indent=2))

    if not args.live:
        print()
        print("DRY-RUN: no order submitted. Re-run with --live --i-understand-the-risk to place.")
        return 0

    if not args.i_understand_the_risk:
        raise SystemExit("--live requires --i-understand-the-risk to confirm intent.")

    print()
    print("LIVE: submitting order to Schwab...")
    res = submit_order(c, acct_hash, payload)
    print(json.dumps(res, indent=2))
    return 0 if res["ok"] else 2


def cmd_rebalance(args) -> int:
    sig = compute_signal_today()
    c = schwab()
    acct_hash = resolve_acct_hash(c)
    pos = get_positions(c, acct_hash)
    cur = infer_current_state(pos)
    tgt = target_state(cur, sig["score"])

    print(f"\n=== Rebalance check  (signal as of {sig['as_of_close']} close) ===")
    print(f"  score = {sig['score']}    current={cur}    target={tgt}")
    if cur == tgt:
        print("  no flip needed.")
        return 0
    if cur == "MIXED":
        raise SystemExit("MIXED holding (both TQQQ and BIL) — manual intervention required.")

    payloads = []
    if cur in ("TQQQ", "BIL"):
        sell_qty = int(pos.get(cur, 0))
        if sell_qty > 0:
            payloads.append(("SELL", cur, sell_qty,
                             build_market_order(cur, "SELL", sell_qty)))

    if tgt in ("TQQQ", "BIL"):
        # estimate cash from sell + existing cash
        # for sizing we use the prior-position notional as a proxy
        sell_sym_quote = get_quote(c, cur) if cur in ("TQQQ","BIL") else None
        sell_notional = sell_qty * (sell_sym_quote["last"] or sell_sym_quote["close_prev"]) if cur in ("TQQQ","BIL") else 0.0
        buy_quote = get_quote(c, tgt)
        buy_ref = buy_quote["last"] or buy_quote["close_prev"]
        buy_qty = int(sell_notional // buy_ref)
        if buy_qty <= 0:
            raise SystemExit("cannot size buy leg from the sell leg notional")
        payloads.append(("BUY", tgt, buy_qty,
                         build_market_order(tgt, "BUY", buy_qty)))

    print(f"\nPlanned orders:")
    for action, sym, qty, p in payloads:
        print(f"  {action} {sym} {qty}")
        print(json.dumps(p, indent=4))

    if not args.live:
        print("\nDRY-RUN: no orders submitted.")
        return 0
    if not args.i_understand_the_risk:
        raise SystemExit("--live requires --i-understand-the-risk to confirm intent.")
    print("\nLIVE: submitting orders sequentially...")
    for action, sym, qty, p in payloads:
        res = submit_order(c, acct_hash, p)
        print(f"{action} {sym} {qty} -> {json.dumps(res)}")
        if not res["ok"]:
            print(f"abort: order failed")
            return 2
    return 0


def main():
    p = argparse.ArgumentParser(description="TQQQ Trend (C1-HYST) Schwab placement")
    p.add_argument("--status", action="store_true", help="show signal + Schwab position; no trades")
    p.add_argument("--initial-usd", type=float, default=0.0,
                   help="open a fresh position of this dollar size")
    p.add_argument("--rebalance", action="store_true", help="flip if current != target")
    p.add_argument("--dry-run", action="store_true", default=True,
                   help="(default) print payload, do not submit")
    p.add_argument("--live", action="store_true", help="actually submit orders to Schwab")
    p.add_argument("--i-understand-the-risk", action="store_true",
                   help="required alongside --live")
    p.add_argument("--force-bil", action="store_true",
                   help="park initial $ in BIL when score < 3 (otherwise error out)")
    p.add_argument("--no-schwab", action="store_true",
                   help="(--status only) skip Schwab call, signal only")
    args = p.parse_args()

    if args.live:
        args.dry_run = False

    if args.status:
        return cmd_status(args)
    if args.initial_usd > 0:
        return cmd_initial(args)
    if args.rebalance:
        return cmd_rebalance(args)
    p.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
