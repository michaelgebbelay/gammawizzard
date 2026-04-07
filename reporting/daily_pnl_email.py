"""Portfolio health email for automated strategy performance.

Pulls fresh Schwab orders, computes realized performance by window and
strategy, and sends:
  1. a portfolio-health summary email every day
  2. a separate behavior-check email only when anomalies are detected

Designed to run as a Lambda post-market step (~4:45 PM ET) after
close5 collection ensures settlement data is available.

Usage:
    python -m reporting.daily_pnl_email              # send email
    python -m reporting.daily_pnl_email --dry-run    # print only, no send
    python -m reporting.daily_pnl_email --file /tmp/schwab_orders_raw.json
"""

from __future__ import annotations

import json
import os
import smtplib
import uuid
from collections import defaultdict
from datetime import datetime, date, timedelta
from email.message import EmailMessage
from typing import Any

import re
import sys
from pathlib import Path

from reporting.broker_pnl import (
    ET,
    API_TAG,
    build_positions,
    classify_order,
    load_orders_from_schwab,
    load_orders_from_file,
    load_settlements,
    parse_filled_orders,
    parse_osi,
)


# ---------------------------------------------------------------------------
# Email formatting
# ---------------------------------------------------------------------------

def _fmt(val: float) -> str:
    val = val + 0.0  # normalise -0.0 → 0.0
    if val >= 0:
        return f"+${val:,.0f}"
    return f"-${abs(val):,.0f}"


def _fmt_wr(wins: int, trades: int) -> str:
    if trades <= 0:
        return "—"
    return f"{wins / trades * 100:.0f}%"


def _fmt_avg(total: float, trades: int) -> str:
    if trades <= 0:
        return "—"
    avg = total / trades
    return _fmt(avg)


def _settle_date(position: dict) -> date | None:
    if position["exit_method"] == "CLOSED_EARLY" and position.get("close_date"):
        return date.fromisoformat(position["close_date"])
    if position["exit_method"] == "EXPIRED" and position.get("expiry"):
        return date.fromisoformat(position["expiry"])
    return None


def _settled_positions(positions: list[dict]) -> list[dict]:
    return [p for p in positions if p["exit_method"] in ("EXPIRED", "CLOSED_EARLY")]


def _window_positions(positions: list[dict], start: date, end: date) -> list[dict]:
    out = []
    for p in positions:
        settled = _settle_date(p)
        if settled is not None and start <= settled <= end:
            out.append(p)
    return out


def _stats(positions: list[dict]) -> dict[str, Any]:
    trades = len(positions)
    pnl = float(sum(p["pnl"] or 0 for p in positions))
    wins = sum(1 for p in positions if (p["pnl"] or 0) > 0)
    return {
        "trades": trades,
        "wins": wins,
        "pnl": pnl,
        "win_rate": _fmt_wr(wins, trades),
        "avg": _fmt_avg(pnl, trades),
    }


def _sorted_settled(positions: list[dict]) -> list[dict]:
    return sorted(
        _settled_positions(positions),
        key=lambda p: (
            _settle_date(p) or date.min,
            p.get("fill_date") or "",
            p.get("strategy") or "",
        ),
    )


def _current_streak(positions: list[dict]) -> str:
    streak = 0
    direction = None
    for p in reversed(_sorted_settled(positions)):
        pnl = float(p["pnl"] or 0)
        if pnl == 0:
            continue
        sign = "W" if pnl > 0 else "L"
        if direction is None:
            direction = sign
            streak = 1
            continue
        if sign == direction:
            streak += 1
        else:
            break
    if direction is None:
        return "—"
    return f"{direction}{streak}"


def _drawdown_stats(positions: list[dict], start: date, end: date) -> tuple[float, float]:
    """Return (current_drawdown, max_drawdown) for realized P&L in the window."""
    daily = defaultdict(float)
    for p in _window_positions(positions, start, end):
        settled = _settle_date(p)
        if settled is not None:
            daily[settled] += float(p["pnl"] or 0)

    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for day in sorted(daily):
        equity += daily[day]
        peak = max(peak, equity)
        dd = peak - equity
        max_dd = max(max_dd, dd)
    current_dd = max(peak - equity, 0.0)
    return current_dd, max_dd


def _first_of_month(report_date: date) -> date:
    return report_date.replace(day=1)


def _first_of_year(report_date: date) -> date:
    return report_date.replace(month=1, day=1)


def _recent_business_days(report_date: date, count: int = 5) -> list[date]:
    days: list[date] = []
    d = report_date
    while len(days) < count:
        if d.weekday() < 5:
            days.append(d)
        d -= timedelta(days=1)
    return list(reversed(days))


# ---------------------------------------------------------------------------
# Today's Trade Summary (Schwab + TastyTrade)
# ---------------------------------------------------------------------------

# TT account labels and their currently-active strategy (from template.yaml).
# Update these when the schedule changes.
TT_ACCOUNTS = {
    "5WT20360": {"label": "TT-IRA", "strategy": "Novix"},       # CS disabled, Novix enabled
    "5WT09219": {"label": "TT-Indv", "strategy": "ConstantStable"},  # CS enabled, Novix disabled
}

# Expected ENABLED schedules for the missing-strategies check (weekdays).
# Each entry: (account_display, strategy_key_in_schwab_strategies_seen_or_tt_label)
EXPECTED_SCHWAB = [
    ("Schwab CS", "constantstable"),
    ("Schwab DualSide", "dualside"),
    # Butterfly runs daily but may legitimately SKIP — not flagged as missing
]
EXPECTED_TT = [
    ("TT-IRA Novix", "TT-IRA"),
    ("TT-Indv CS", "TT-Indv"),
]


def _load_tt_orders_for_date(report_date: date) -> list[dict]:
    """Pull today's orders from both TastyTrade accounts.

    Returns a list of normalised order dicts with an 'account_label' field.
    Fails silently per-account so Schwab email still sends if TT is down.
    """
    tt_orders: list[dict] = []

    repo_root = Path(__file__).resolve().parent.parent
    tt_script = repo_root / "TT" / "Script"
    if not tt_script.is_dir():
        print("[trade_summary] TT/Script not found — skipping TT orders")
        return tt_orders

    if str(tt_script) not in sys.path:
        sys.path.insert(0, str(tt_script))

    try:
        from tt_client import request as tt_request
    except Exception as e:
        print(f"[trade_summary] cannot import tt_client: {e}")
        return tt_orders

    for acct_num, acct_info in TT_ACCOUNTS.items():
        label = acct_info["label"]
        default_strategy = acct_info["strategy"]
        try:
            os.environ["TT_ACCOUNT_NUMBER"] = acct_num
            resp = tt_request("GET", f"/accounts/{acct_num}/orders")
            data = resp.json()
            items = data.get("data", {}).get("items", data) if isinstance(data, dict) else data
            if not isinstance(items, list):
                items = []

            for order in items:
                # TT uses kebab-case
                status = order.get("status", "")
                entered = order.get("received-at", order.get("created-at", ""))

                # Filter to today
                if entered:
                    try:
                        dt_utc = datetime.fromisoformat(
                            entered.replace("Z", "+00:00").replace("+0000", "+00:00")
                        )
                        dt_et = dt_utc.astimezone(ET)
                        if dt_et.date() != report_date:
                            continue
                    except Exception:
                        continue
                else:
                    continue

                # Parse legs
                legs = order.get("legs", [])
                strikes = set()
                leg_details = []
                for leg in legs:
                    sym = leg.get("symbol", "")
                    action = leg.get("action", "")
                    qty = leg.get("quantity", 0)
                    # Try OSI parse
                    expiry, opt_type, strike = parse_osi(sym)
                    if strike:
                        strikes.add(strike)
                    leg_details.append({
                        "action": action,
                        "qty": qty,
                        "strike": strike,
                        "option_type": opt_type,
                        "expiry": expiry,
                    })

                sorted_strikes = sorted(strikes) if strikes else []
                width = sorted_strikes[-1] - sorted_strikes[0] if len(sorted_strikes) >= 2 else 0

                # TT does not populate filled-quantity on the order; use size when Filled
                raw_filled = order.get("filled-quantity") or order.get("filledQuantity")
                total_qty = order.get("size", sum(l.get("quantity", 0) for l in legs))
                if raw_filled is not None:
                    filled_qty = int(raw_filled)
                elif status.lower() == "filled":
                    filled_qty = int(total_qty) if total_qty else 0
                else:
                    filled_qty = 0
                price_effect = order.get("price-effect", "")
                price = order.get("price", 0)

                tt_orders.append({
                    "broker": "tt",
                    "account_label": label,
                    "account_number": acct_num,
                    "default_strategy": default_strategy,
                    "status": status,
                    "dt_et": dt_et,
                    "filled_qty": filled_qty,
                    "total_qty": int(total_qty) if total_qty else 0,
                    "price": float(price) if price else 0,
                    "price_effect": price_effect,
                    "strikes": sorted_strikes,
                    "width": width,
                    "legs": leg_details,
                    "num_strikes": len(sorted_strikes),
                })

            print(f"[trade_summary] TT {label}: {len([o for o in tt_orders if o['account_label'] == label])} orders today")
        except Exception as e:
            print(f"[trade_summary] TT {label} ERROR: {e}")

    return tt_orders


def _classify_signal(order: dict, legs: list) -> str:
    """Determine signal type: IC_LONG, IC_SHORT, RR_LONG_PUT, RR_LONG_CALL, BF_BUY, BF_SELL."""
    instructions = set()
    put_legs = []
    call_legs = []

    for leg in legs:
        action = leg.get("instruction", leg.get("action", ""))
        instructions.add(action.upper() if action else "")
        opt_type = leg.get("option_type", "")
        if opt_type == "PUT":
            put_legs.append((action.upper(), leg.get("strike", 0)))
        elif opt_type == "CALL":
            call_legs.append((action.upper(), leg.get("strike", 0)))

    # Butterfly (3 strikes)
    num_strikes = order.get("num_strikes", len(set(l.get("strike", 0) for l in legs if l.get("strike"))))
    if num_strikes == 3:
        # Net debit = BUY, net credit = SELL
        price_effect = order.get("price_effect", order.get("order_type", ""))
        if "CREDIT" in str(price_effect).upper():
            return "BF_SELL"
        return "BF_BUY"

    # Vertical / IC / RR (2 strikes)
    has_buy_open = any("BUY" in i and "OPEN" in i for i in instructions)
    has_sell_open = any("SELL" in i and "OPEN" in i for i in instructions)

    # Determine direction per side: in a vertical spread both BUY/SELL exist.
    # For puts: buying the higher-strike put = debit (long put spread).
    # For calls: buying the lower-strike call = debit (long call spread).
    def _side_is_long(side_legs: list, option_type: str) -> bool:
        if len(side_legs) < 2:
            return any("BUY" in a for a, _ in side_legs)
        sorted_by_strike = sorted(side_legs, key=lambda x: x[1])
        if option_type == "PUT":
            # Higher-strike put is more expensive; BUY it = debit = long
            higher = sorted_by_strike[-1]
            return "BUY" in higher[0]
        else:
            # Lower-strike call is more expensive; BUY it = debit = long
            lower = sorted_by_strike[0]
            return "BUY" in lower[0]

    put_long = _side_is_long(put_legs, "PUT") if put_legs else None
    call_long = _side_is_long(call_legs, "CALL") if call_legs else None

    if put_legs and call_legs:
        # IC or RR (has both put and call sides)
        if put_long and call_long:
            return "IC_LONG"
        if not put_long and not call_long:
            return "IC_SHORT"
        if put_long and not call_long:
            return "RR_LONG_PUT"
        if not put_long and call_long:
            return "RR_LONG_CALL"
    elif put_legs:
        # Single-side vertical: both BUY/SELL legs exist. Use order type for direction.
        order_type = order.get("order_type", order.get("price_effect", ""))
        if "CREDIT" in str(order_type).upper():
            return "PUT_CREDIT"
        return "PUT_DEBIT"
    elif call_legs:
        order_type = order.get("order_type", order.get("price_effect", ""))
        if "CREDIT" in str(order_type).upper():
            return "CALL_CREDIT"
        return "CALL_DEBIT"

    # Fallback: use order type
    order_type = order.get("order_type", order.get("price_effect", ""))
    if "CREDIT" in str(order_type).upper():
        return "SHORT"
    return "LONG"


def _fill_status_str(status: str, filled_qty: int, total_qty: int) -> str:
    status_upper = status.upper()
    if status_upper == "FILLED":
        if total_qty > 0 and filled_qty < total_qty:
            return f"PARTIAL ({filled_qty}/{total_qty})"
        return "FILLED"
    if status_upper in ("CANCELLED", "CANCELED"):
        return "CANCELED"
    if status_upper == "REJECTED":
        return "REJECTED"
    if status_upper == "EXPIRED":
        return "EXPIRED"
    if status_upper in ("WORKING", "LIVE", "RECEIVED"):
        return "WORKING"
    return status_upper or "UNKNOWN"


def _strikes_str(strikes: list, legs: list) -> str:
    if not strikes:
        return "—"
    if len(strikes) == 3:
        return "/".join(f"{s:.0f}" for s in strikes)
    if len(strikes) == 2:
        # Show as put side / call side
        return f"{strikes[0]:.0f}/{strikes[1]:.0f}"
    return "/".join(f"{s:.0f}" for s in strikes)


def _build_today_trades_section(
    raw_schwab_orders: list[dict],
    tt_orders: list[dict],
    report_date: date,
) -> str:
    """Build the 'Today's Trades' section for the email."""
    lines = [
        "Today's Trades",
        f"{'Account':<10} {'Strategy':<16} {'Type':<10} {'Status':<10} {'Puts':>5} {'Calls':>5}",
        f"{'-' * 10} {'-' * 16} {'-' * 10} {'-' * 10} {'-' * 5} {'-' * 5}",
    ]

    STRATEGY_LABELS = {
        "constantstable": "ConstantStable",
        "cs_morning": "CS Morning",
        "dualside": "DualSide",
        "butterfly": "Butterfly",
        "novix": "Novix",
    }

    # ---- Collect orders by (account, strategy) ----
    groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
    schwab_strategies_seen: set[str] = set()
    tt_strategies_seen: set[str] = set()

    # --- Schwab orders ---
    for order in raw_schwab_orders:
        entered = order.get("enteredTime", order.get("closeTime", ""))
        if entered:
            try:
                dt_utc = datetime.fromisoformat(entered.replace("+0000", "+00:00"))
                dt_et = dt_utc.astimezone(ET)
                if dt_et.date() != report_date:
                    continue
            except Exception:
                continue
        else:
            continue

        legs_raw = order.get("orderLegCollection", [])
        instructions = set(l.get("instruction", "") for l in legs_raw)
        if all("CLOSE" in i for i in instructions if i):
            continue

        strategy = classify_order(order)
        if strategy in ("manual", "butterfly_manual"):
            continue

        schwab_strategies_seen.add(strategy)

        parsed_legs = []
        strikes = set()
        for leg in legs_raw:
            inst = leg.get("instrument", {})
            expiry, opt_type, strike = parse_osi(inst.get("symbol", ""))
            if strike:
                strikes.add(strike)
            parsed_legs.append({
                "instruction": leg.get("instruction", ""),
                "option_type": opt_type,
                "strike": strike,
                "expiry": expiry,
            })

        sorted_strikes = sorted(strikes)
        norm_order = {
            "order_type": order.get("orderType", ""),
            "num_strikes": len(sorted_strikes),
            "price_effect": order.get("orderType", ""),
        }
        signal = _classify_signal(norm_order, parsed_legs)
        status = order.get("status", "").upper()
        filled_qty = int(order.get("filledQuantity", 0))
        total_qty = max(filled_qty, max((l.get("quantity", 0) for l in legs_raw), default=0))
        price = order.get("price", 0)

        groups[("Schwab", strategy)].append({
            "status": status, "signal": signal, "strikes": sorted_strikes,
            "price": price, "filled_qty": filled_qty, "total_qty": total_qty,
        })

    # --- TT orders ---
    for order in tt_orders:
        legs = order.get("legs", [])
        actions = set(l.get("action", "").upper() for l in legs)
        if all("CLOSE" in a for a in actions if a):
            continue

        acct_label = order["account_label"]
        width = order["width"]
        if width == 10:
            tt_strategy = "dualside"
        elif order["num_strikes"] == 3:
            tt_strategy = "butterfly"
        else:
            tt_strategy = order.get("default_strategy", "ConstantStable").lower()

        tt_strategies_seen.add(acct_label)

        signal = _classify_signal(order, legs)
        status = order.get("status", "").upper()

        groups[(acct_label, tt_strategy)].append({
            "status": status, "signal": signal, "strikes": order["strikes"],
            "price": order["price"], "filled_qty": order["filled_qty"],
            "total_qty": order["total_qty"],
        })

    # ---- Render one row per (account, strategy) ----
    trade_count = 0
    html_rows: list[dict] = []
    for (acct, strat) in sorted(groups.keys()):
        order_list = groups[(acct, strat)]
        strategy_label = STRATEGY_LABELS.get(strat, strat)

        filled = [o for o in order_list if o["status"] == "FILLED"]
        working = [o for o in order_list if o["status"] in ("WORKING", "LIVE", "RECEIVED", "QUEUED", "PENDING_ACTIVATION")]
        rejected = [o for o in order_list if o["status"] == "REJECTED"]
        partials = [o for o in order_list if o["filled_qty"] > 0 and o["status"] != "FILLED"]

        # Derive trade type and per-side contract counts
        ref = filled or working or partials or order_list
        signals = [o["signal"] for o in ref]
        put_orders = [o for o in ref if "PUT" in o["signal"]]
        call_orders = [o for o in ref if "CALL" in o["signal"]]
        has_put = len(put_orders) > 0
        has_call = len(call_orders) > 0

        # Trade type label
        if any(s.startswith("BF") for s in signals):
            trade_type = "BF Buy" if any(s == "BF_BUY" for s in signals) else "BF Sell"
        elif has_put and has_call:
            put_is_long = any("LONG" in o["signal"] or "DEBIT" in o["signal"] for o in put_orders)
            call_is_long = any("LONG" in o["signal"] or "DEBIT" in o["signal"] for o in call_orders)
            if put_is_long and call_is_long:
                trade_type = "IC Long"
            elif not put_is_long and not call_is_long:
                trade_type = "IC Short"
            elif call_is_long and not put_is_long:
                trade_type = "RR Long"
            else:
                trade_type = "RR Short"
        elif has_put:
            trade_type = "Bear Put" if any("LONG" in o["signal"] or "DEBIT" in o["signal"] for o in put_orders) else "Bull Put"
        elif has_call:
            trade_type = "Bull Call" if any("LONG" in o["signal"] or "DEBIT" in o["signal"] for o in call_orders) else "Bear Call"
        else:
            trade_type = "—"

        put_qty = sum(o["filled_qty"] for o in put_orders)
        call_qty = sum(o["filled_qty"] for o in call_orders)
        put_str = str(put_qty) if put_qty > 0 else "—"
        call_str = str(call_qty) if call_qty > 0 else "—"

        # Status
        if filled:
            status_str = "FILLED"
            outcome_type = "filled"
        elif working:
            status_str = "WORKING"
            outcome_type = "working"
        elif partials:
            p = partials[-1]
            status_str = f"PARTIAL"
            outcome_type = "nofill"
        elif rejected:
            status_str = "REJECTED"
            outcome_type = "nofill"
        else:
            status_str = "NO FILL"
            outcome_type = "nofill"

        lines.append(f"{acct:<10} {strategy_label:<16} {trade_type:<10} {status_str:<10} {put_str:>5} {call_str:>5}")
        html_rows.append({
            "account": acct, "strategy": strategy_label,
            "trade_type": trade_type, "outcome_type": outcome_type,
            "puts": put_str, "calls": call_str,
        })
        trade_count += 1

    # --- Missing strategies (no orders at all) ---
    if report_date.weekday() < 5:
        for display, key in EXPECTED_SCHWAB:
            if key not in schwab_strategies_seen:
                strategy_label = STRATEGY_LABELS.get(key, key)
                lines.append(f"{'Schwab':<10} {strategy_label:<16} {'—':<10} {'NO ORDER':<10} {'—':>5} {'—':>5}")
                html_rows.append({
                    "account": "Schwab", "strategy": strategy_label,
                    "trade_type": "—", "outcome_type": "noorder", "puts": "—", "calls": "—",
                })
                trade_count += 1
        for display, key in EXPECTED_TT:
            if key not in tt_strategies_seen:
                lines.append(f"{key:<10} {'—':<16} {'—':<10} {'NO ORDER':<10} {'—':>5} {'—':>5}")
                html_rows.append({
                    "account": key, "strategy": "—",
                    "trade_type": "—", "outcome_type": "noorder", "puts": "—", "calls": "—",
                })
                trade_count += 1

    if trade_count == 0:
        lines.append("No automated trades today.")

    lines.append(f"Legend: Price=per-share, $/Ct=per-contract | Source: Broker APIs")
    lines.append("")
    text = "\n".join(lines)
    return text, {"rows": html_rows}


# ---------------------------------------------------------------------------
# Discretionary P&L (net position method)
# ---------------------------------------------------------------------------

def _load_spx_spot(for_date: date | None = None) -> float | None:
    """Pull SPX price from Schwab.

    If for_date is today or None, uses the live quote API.
    If for_date is a past date, uses price history to get that day's close.
    """
    try:
        repo_root = Path(__file__).resolve().parent.parent
        scripts_dir = repo_root / "scripts"
        if str(scripts_dir) not in sys.path:
            sys.path.insert(0, str(scripts_dir))
        from schwab_token_keeper import schwab_client
        from schwab.client import Client

        c = schwab_client()

        # Always pull price history (covers both current and past dates)
        resp = c.get_price_history(
            "$SPX",
            period_type=Client.PriceHistory.PeriodType.MONTH,
            period=Client.PriceHistory.Period.THREE_MONTHS,
            frequency_type=Client.PriceHistory.FrequencyType.DAILY,
            frequency=Client.PriceHistory.Frequency.DAILY,
        )
        resp.raise_for_status()
        candles = resp.json().get("candles", [])

        if candles:
            # Build date->close map
            closes: dict[str, float] = {}
            for candle in candles:
                ts = candle.get("datetime", 0)
                dt = datetime.fromtimestamp(ts / 1000)
                closes[dt.strftime("%Y-%m-%d")] = candle["close"]

            # If specific date requested, return that date's close
            if for_date:
                target = for_date.isoformat()
                if target in closes:
                    print(f"  [settlement] SPX close for {target}: {closes[target]}")
                    return closes[target]
                # Fall through to latest if date not found

            # Default: return latest candle
            return float(candles[-1]["close"])

        # Fallback: live quote (works for today only)
        resp = c.get_quote("$SPX")
        resp.raise_for_status()
        data = resp.json()
        for sym_data in data.values():
            quote = sym_data.get("quote", sym_data) if isinstance(sym_data, dict) else {}
            price = quote.get("lastPrice") or quote.get("closePrice") or quote.get("mark")
            if price:
                return float(price)
    except Exception as e:
        print(f"  [warn] Could not fetch SPX: {e}")
    return None


def _net_position_pnl_for_expiry(
    orders: list[dict],
    expiry_str: str,
    spx_close: float,
) -> float:
    """Compute P&L via net position method for a single expiry across given orders."""
    cash = 0.0
    legs: dict[tuple, int] = defaultdict(int)  # (strike, otype) -> net_qty

    for o in orders:
        if o.get("status") != "FILLED":
            continue
        order_legs = o.get("orderLegCollection", [])
        order_type = o.get("orderType", "")
        price = o.get("price", 0)
        filled_qty = int(o.get("filledQuantity", 0))

        # Check all legs share this expiry
        leg_expiries = set()
        for leg in order_legs:
            inst = leg.get("instrument", {})
            e, _, _ = parse_osi(inst.get("symbol", "").strip())
            if e:
                leg_expiries.add(e)
        if len(leg_expiries) != 1 or list(leg_expiries)[0] != expiry_str:
            continue

        # Net qty
        for leg in order_legs:
            inst = leg.get("instrument", {})
            _, otype, strike = parse_osi(inst.get("symbol", "").strip())
            if not strike:
                continue
            instruction = leg.get("instruction", "")
            qty = int(leg.get("quantity", 0))
            signed = qty if "BUY" in instruction else -qty
            legs[(strike, otype)] += signed

        # Cash flow
        if order_type == "NET_CREDIT":
            cash += price * filled_qty * 100
        elif order_type == "NET_DEBIT":
            cash -= price * filled_qty * 100
        elif order_type == "LIMIT":
            for leg in order_legs:
                instruction = leg.get("instruction", "")
                if "SELL" in instruction:
                    cash += price * filled_qty * 100
                else:
                    cash -= price * filled_qty * 100
                break

    # Settlement value
    settle = 0.0
    for (strike, otype), nq in legs.items():
        if nq == 0:
            continue
        if otype == "PUT":
            intrinsic = max(0, strike - spx_close)
        else:
            intrinsic = max(0, spx_close - strike)
        settle += intrinsic * nq * 100

    return cash + settle


def _compute_discretionary_pnl(
    raw_orders: list[dict],
    report_date: date,
    spx_spot: float | None,
) -> dict:
    """Compute discretionary P&L for positions expiring today.

    Separates:
      - Pure discretionary: manual opens + their closes (net position method)
      - Auto adjustments: manual closes of automated positions (shown separately)

    A manual close-only order is an "auto adjustment" if its strikes overlap
    with API-tagged orders at the same expiry.
    """
    report_str = report_date.isoformat()

    # Build auto net positions for this expiry to detect adjustments
    auto_net: dict[tuple[float, str], int] = defaultdict(int)
    for o in raw_orders:
        if o.get("status") != "FILLED":
            continue
        if o.get("tag") != API_TAG:
            continue
        for leg in o.get("orderLegCollection", []):
            inst = leg.get("instrument", {})
            e, otype, strike = parse_osi(inst.get("symbol", "").strip())
            if e == report_str and strike:
                instruction = leg.get("instruction", "")
                qty = int(leg.get("quantity", 0))
                signed = qty if "BUY" in instruction else -qty
                auto_net[(strike, otype)] += signed

    def _is_auto_adjustment(leg_details: list[dict]) -> bool:
        """A manual close-only order is an auto adjustment if every leg
        reduces (moves toward zero) the corresponding auto net position."""
        if not all("CLOSE" in ld["action"] for ld in leg_details if ld["action"]):
            return False
        for ld in leg_details:
            key = (ld["strike"], ld["type"])
            auto_qty = auto_net.get(key, 0)
            if auto_qty == 0:
                return False  # no auto position at this strike → not an adjustment
            qty = 1  # direction matters, not magnitude
            signed = qty if "BUY" in ld["action"] else -qty
            # Reducing means: auto is positive and we're adding negative, or vice versa
            if auto_qty > 0 and signed >= 0:
                return False
            if auto_qty < 0 and signed <= 0:
                return False
        return True

    # Classify manual orders into pure discretionary vs auto adjustments
    disc_orders: list[dict] = []        # pure discretionary (include in P&L)
    adjustment_orders: list[dict] = []   # manual closes of auto positions
    discretionary_trades: list[dict] = []

    for o in raw_orders:
        if o.get("status") != "FILLED":
            continue
        if o.get("tag") == API_TAG:
            continue

        legs = o.get("orderLegCollection", [])
        order_type = o.get("orderType", "")
        price = o.get("price", 0)
        filled_qty = int(o.get("filledQuantity", 0))

        leg_expiries = set()
        leg_details = []
        for leg in legs:
            inst = leg.get("instrument", {})
            e, option_type, strike = parse_osi(inst.get("symbol", "").strip())
            if e:
                leg_expiries.add(e)
            if strike:
                instruction = leg.get("instruction", "")
                leg_details.append({"strike": strike, "type": option_type, "action": instruction})

        if len(leg_expiries) != 1:
            continue
        exp = list(leg_expiries)[0]
        if exp != report_str:
            continue

        is_auto_adjustment = _is_auto_adjustment(leg_details)

        if is_auto_adjustment:
            adjustment_orders.append(o)
        else:
            disc_orders.append(o)

        # Build display record
        dt_et = None
        stamp = o.get("closeTime") or o.get("enteredTime") or ""
        if stamp:
            try:
                dt_utc = datetime.fromisoformat(stamp.replace("+0000", "+00:00"))
                dt_et = dt_utc.astimezone(ET)
            except Exception:
                pass

        strikes = sorted(set(ld["strike"] for ld in leg_details))
        n_strikes = len(strikes)
        if n_strikes == 3:
            structure = "Butterfly"
        elif n_strikes == 2:
            structure = "Vertical"
        elif n_strikes == 1:
            structure = "Single Leg"
        else:
            structure = f"{n_strikes}-leg"

        discretionary_trades.append({
            "time": dt_et.strftime("%H:%M") if dt_et else "—",
            "structure": structure,
            "strikes": strikes,
            "premium": price * filled_qty * 100,
            "order_type": order_type,
            "qty": filled_qty,
            "is_adjustment": is_auto_adjustment,
        })

    # Pure discretionary count (exclude adjustments)
    pure_disc_count = sum(1 for t in discretionary_trades if not t.get("is_adjustment"))

    result = {
        "total_pnl": 0.0,
        "trade_count": len(discretionary_trades),
        "pure_disc_count": pure_disc_count,
        "adjustment_count": len(adjustment_orders),
        "expiry_details": [],
        "spx_spot": spx_spot,
        "trades": discretionary_trades,
    }

    if not spx_spot or not discretionary_trades:
        return result

    # Pure discretionary P&L: net position method on manual-only orders
    # (excluding auto adjustments)
    disc_pnl = _net_position_pnl_for_expiry(disc_orders, report_str, spx_spot)

    # Auto adjustment P&L: computed via subtraction
    # total(all orders) - auto(API only) - disc(pure manual) = adjustment impact
    if adjustment_orders:
        total_pnl = _net_position_pnl_for_expiry(raw_orders, report_str, spx_spot)
        auto_pnl = _net_position_pnl_for_expiry(
            [o for o in raw_orders if o.get("tag") == API_TAG], report_str, spx_spot,
        )
        adj_pnl = total_pnl - auto_pnl - disc_pnl
    else:
        adj_pnl = 0.0

    result["total_pnl"] = disc_pnl
    result["adjustment_pnl"] = adj_pnl
    result["expiry_details"].append({
        "expiry": report_str,
        "disc_pnl": disc_pnl,
        "adj_pnl": adj_pnl,
    })

    return result


def _build_discretionary_section(disc_result: dict, report_date: date) -> str:
    """Build the discretionary P&L text section for the daily email."""
    lines = [
        "Discretionary Trades (Schwab)",
        f"Expiring {report_date.isoformat()} | SPX Spot: "
        + (f"${disc_result['spx_spot']:,.2f}" if disc_result["spx_spot"] else "N/A"),
    ]

    trades = disc_result["trades"]
    if not trades:
        lines.append("No discretionary trades expiring today.")
        lines.append("")
        return "\n".join(lines)

    lines.append(
        f"{'Time':<6} {'Structure':<12} {'Strikes':<20} {'Type':<12} "
        f"{'Qty':>4} {'Premium':>10}"
    )
    lines.append(
        f"{'-' * 6} {'-' * 12} {'-' * 20} {'-' * 12} {'-' * 4} {'-' * 10}"
    )

    for t in sorted(trades, key=lambda x: x["time"]):
        strikes_str = "/".join(f"{s:.0f}" for s in t["strikes"]) if t["strikes"] else "—"
        premium_str = _fmt(t["premium"])
        order_label = t["order_type"].replace("NET_", "").title() if t["order_type"] else "—"
        adj_tag = " [adj]" if t.get("is_adjustment") else ""
        lines.append(
            f"{t['time']:<6} {t['structure']:<12} {strikes_str:<20} {order_label:<12} "
            f"{t['qty']:>4} {premium_str:>10}{adj_tag}"
        )

    # P&L summary
    details = disc_result["expiry_details"]
    if details:
        lines.append("")
        d = details[0]
        lines.append(f"Discretionary P&L: {_fmt(d['disc_pnl'])}")
        adj_count = disc_result.get("adjustment_count", 0)
        if adj_count > 0:
            lines.append(
                f"Auto adjustments ({adj_count} order(s), marked [adj]): "
                f"{_fmt(d.get('adj_pnl', 0))}"
            )
        lines.append(f"Discretionary total: {_fmt(disc_result['total_pnl'])}")
    elif trades:
        lines.append("(Settlement P&L pending — SPX spot not available)")

    lines.append("")
    return "\n".join(lines)


EMAIL_GROUPS = (
    {
        "key": "constantstable",
        "label": "ConstantStable",
        "short": "CS",
        "members": ("constantstable", "cs_morning"),
    },
    {
        "key": "dualside",
        "label": "DualSide",
        "short": "DS",
        "members": ("dualside",),
    },
    {
        "key": "butterfly",
        "label": "Butterfly Tuesday",
        "short": "BF",
        "members": ("butterfly",),
    },
)


def _build_email(
    positions: list[dict],
    report_date: date,
    today_trades_section: str = "",
    discretionary_section: str = "",
    discretionary_pnl: float = 0.0,
) -> tuple[str, str]:
    """Build the portfolio-health email.

    Returns (subject, body).
    """
    as_of_str = report_date.isoformat()
    settled = _settled_positions(positions)
    recent_days = _recent_business_days(report_date, count=5)
    five_day_start = recent_days[0]
    five_day_end = recent_days[-1]
    five_day_positions = _window_positions(settled, five_day_start, five_day_end)
    mtd_positions = _window_positions(settled, _first_of_month(report_date), report_date)
    ytd_positions = _window_positions(settled, _first_of_year(report_date), report_date)

    today_positions = _window_positions(settled, report_date, report_date)
    today_stats = _stats(today_positions)
    five_day_stats = _stats(five_day_positions)
    mtd_stats = _stats(mtd_positions)
    ytd_stats = _stats(ytd_positions)

    strat_rows = []
    for group in EMAIL_GROUPS:
        members = set(group["members"])
        strat_all = [p for p in settled if p["strategy"] in members]
        strat_ytd = [p for p in ytd_positions if p["strategy"] in members]
        strat_mtd = [p for p in mtd_positions if p["strategy"] in members]
        strat_5d = [p for p in five_day_positions if p["strategy"] in members]
        if not strat_ytd and not strat_mtd and not strat_5d:
            continue
        ytd_stats_strat = _stats(strat_ytd)
        strat_rows.append({
            "label": group["label"],
            "five_day_pnl": float(sum(p["pnl"] or 0 for p in strat_5d)),
            "mtd_pnl": float(sum(p["pnl"] or 0 for p in strat_mtd)),
            "ytd_pnl": float(sum(p["pnl"] or 0 for p in strat_ytd)),
            "trades_ytd": ytd_stats_strat["trades"],
            "wr_ytd": ytd_stats_strat["win_rate"],
            "streak": _current_streak(strat_all),
        })
    strat_rows.sort(key=lambda r: r["ytd_pnl"], reverse=True)

    current_dd_ytd, max_dd_ytd = _drawdown_stats(settled, _first_of_year(report_date), report_date)
    current_dd_mtd, max_dd_mtd = _drawdown_stats(settled, _first_of_month(report_date), report_date)
    portfolio_streak = _current_streak(settled)

    today_combined = today_stats['pnl'] + discretionary_pnl
    subject = (
        f"[Gamma] Today {_fmt(today_combined)} | "
        f"MTD {_fmt(mtd_stats['pnl'])} | YTD {_fmt(ytd_stats['pnl'])} — {as_of_str}"
    )

    recent_map: dict[tuple[date, str], float] = defaultdict(float)
    for p in settled:
        settled_day = _settle_date(p)
        if settled_day in recent_days:
            for group in EMAIL_GROUPS:
                if p["strategy"] in group["members"]:
                    recent_map[(settled_day, group["key"])] += float(p["pnl"] or 0)
                    break

    lines = [
        f"Gamma Portfolio Pulse — {as_of_str}",
        f"{'=' * 50}",
        "",
    ]

    if today_trades_section:
        lines.append(today_trades_section)
        lines.append("")

    lines.append("Portfolio")
    lines.append(f"{'Window':<10} {'Trades':>7} {'Win%':>6} {'P&L':>12} {'Avg/Trade':>12}")
    lines.append(f"{'-' * 10} {'-' * 7} {'-' * 6} {'-' * 12} {'-' * 12}")
    today_combined_stats_plain = {
        **today_stats,
        "pnl": today_stats["pnl"] + discretionary_pnl,
    }
    for label, stat in (
        ("Today", today_combined_stats_plain),
        ("5D", five_day_stats),
        ("MTD", mtd_stats),
        ("YTD", ytd_stats),
    ):
        lines.append(
            f"{label:<10} {stat['trades']:>7} {stat['win_rate']:>6} "
            f"{_fmt(stat['pnl']):>12} {stat['avg']:>12}"
        )

    lines.append("")
    lines.append("Last 5 Sessions")
    lines.append(
        f"{'Date':<10} "
        f"{'CS':>10} "
        f"{'DS':>10} "
        f"{'BF':>10} "
        f"{'Total':>10}"
    )
    lines.append(
        f"{'-' * 10} {'-' * 10} {'-' * 10} {'-' * 10} {'-' * 10}"
    )
    for day in recent_days:
        cs = recent_map[(day, "constantstable")]
        ds = recent_map[(day, "dualside")]
        bf = recent_map[(day, "butterfly")]
        total = cs + ds + bf
        lines.append(
            f"{day.isoformat():<10} "
            f"{_fmt(cs):>10} "
            f"{_fmt(ds):>10} "
            f"{_fmt(bf):>10} "
            f"{_fmt(total):>10}"
        )
    lines.append("Legend: CS=ConstantStable incl. morning, DS=DualSide, BF=Butterfly")

    lines.append("")
    lines.append("Strategy Contribution")
    lines.append(
        f"{'Strategy':<25} {'5D':>10} {'MTD':>10} {'YTD':>10} "
        f"{'Trades':>7} {'Win%':>6} {'Streak':>7}"
    )
    lines.append(
        f"{'-' * 25} {'-' * 10} {'-' * 10} {'-' * 10} "
        f"{'-' * 7} {'-' * 6} {'-' * 7}"
    )
    if strat_rows:
        for row in strat_rows:
            lines.append(
                f"{row['label']:<25} {_fmt(row['five_day_pnl']):>10} {_fmt(row['mtd_pnl']):>10} "
                f"{_fmt(row['ytd_pnl']):>10} {row['trades_ytd']:>7} {row['wr_ytd']:>6} {row['streak']:>7}"
            )
    else:
        lines.append("No settled trades yet.")

    lines.append("")
    lines.append("Risk State")
    lines.append(f"- Current streak: {portfolio_streak}")
    lines.append(f"- MTD drawdown: current {_fmt(-current_dd_mtd)} | max {_fmt(-max_dd_mtd)}")
    lines.append(f"- YTD drawdown: current {_fmt(-current_dd_ytd)} | max {_fmt(-max_dd_ytd)}")

    # Discretionary section
    if discretionary_section:
        lines.append("")
        lines.append("=" * 50)
        lines.append(discretionary_section)

        # Combined bottom line
        today_auto = sum(
            float(p["pnl"] or 0) for p in _window_positions(settled, report_date, report_date)
        )
        combined = today_auto + discretionary_pnl
        lines.append("Combined (Auto + Discretionary)")
        lines.append(f"  Today auto:         {_fmt(today_auto)}")
        lines.append(f"  Today discretionary: {_fmt(discretionary_pnl)}")
        lines.append(f"  Combined total:      {_fmt(combined)}")

    lines.append("")
    lines.append("— Gamma Reporting (automated)")

    return subject, "\n".join(lines)


# ---------------------------------------------------------------------------
# HTML email builder
# ---------------------------------------------------------------------------

_CSS = """\
body { font-family: -apple-system, 'Segoe UI', Roboto, Helvetica, sans-serif;
       background: #f4f5f7; margin: 0; padding: 20px; color: #1a1a2e; }
.container { max-width: 640px; margin: 0 auto; background: #ffffff;
             border-radius: 12px; overflow: hidden;
             box-shadow: 0 2px 8px rgba(0,0,0,.08); }
.header { background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
          color: #ffffff; padding: 24px 28px 18px; }
.header h1 { margin: 0 0 4px; font-size: 20px; font-weight: 600; letter-spacing: .3px; }
.header .date { font-size: 13px; color: #a0aec0; }
.section { padding: 20px 28px 12px; }
.section h2 { font-size: 14px; text-transform: uppercase; letter-spacing: 1px;
              color: #718096; margin: 0 0 12px; font-weight: 600; }
table { width: 100%; border-collapse: collapse; font-size: 13px; }
th { text-align: left; padding: 6px 8px; color: #718096; font-weight: 500;
     font-size: 11px; text-transform: uppercase; letter-spacing: .5px;
     border-bottom: 2px solid #e2e8f0; }
td { padding: 7px 8px; border-bottom: 1px solid #f0f0f5; }
tr:last-child td { border-bottom: none; }
.right { text-align: right; }
.mono { font-family: 'SF Mono', Menlo, monospace; font-size: 12px; }
.green { color: #38a169; font-weight: 600; }
.red { color: #e53e3e; font-weight: 600; }
.muted { color: #a0aec0; }
.badge { display: inline-block; padding: 2px 8px; border-radius: 4px;
         font-size: 11px; font-weight: 600; }
.badge-filled { background: #c6f6d5; color: #276749; }
.badge-nofill { background: #fed7d7; color: #9b2c2c; }
.badge-noorder { background: #fefcbf; color: #975a16; }
.badge-working { background: #bee3f8; color: #2a4365; }
.card-row { display: flex; gap: 12px; margin-bottom: 12px; }
.card { flex: 1; background: #f7fafc; border-radius: 8px; padding: 14px 16px; text-align: center; }
.card .label { font-size: 11px; text-transform: uppercase; letter-spacing: .5px; color: #718096; margin-bottom: 4px; }
.card .value { font-size: 20px; font-weight: 700; }
.divider { border: none; border-top: 1px solid #e2e8f0; margin: 0; }
.footer { padding: 16px 28px; text-align: center; font-size: 11px; color: #a0aec0; }
"""


def _html_pnl(val: float) -> str:
    cls = "green" if val >= 0 else "red"
    sign = "+" if val >= 0 else "-"
    return f'<span class="{cls} mono">{sign}${abs(val):,.0f}</span>'


def _html_pnl_plain(val: float) -> str:
    cls = "green" if val >= 0 else "red"
    return f'<span class="{cls}">{_fmt(val)}</span>'


def _build_email_html(
    positions: list[dict],
    report_date: date,
    today_trades_data: dict,
    discretionary_section_data: dict | None = None,
    discretionary_pnl: float = 0.0,
) -> str:
    """Build the HTML email body."""
    settled = _settled_positions(positions)
    recent_days = _recent_business_days(report_date, count=5)
    five_day_start = recent_days[0]
    five_day_end = recent_days[-1]
    five_day_positions = _window_positions(settled, five_day_start, five_day_end)
    mtd_positions = _window_positions(settled, _first_of_month(report_date), report_date)
    ytd_positions = _window_positions(settled, _first_of_year(report_date), report_date)
    today_positions = _window_positions(settled, report_date, report_date)
    today_stats = _stats(today_positions)
    five_day_stats = _stats(five_day_positions)
    mtd_stats = _stats(mtd_positions)
    ytd_stats = _stats(ytd_positions)

    # --- KPI cards ---
    def _kpi_card(label: str, stats: dict) -> str:
        return (
            f'<div style="flex:1;background:#f7fafc;border-radius:8px;padding:14px 16px;text-align:center;">'
            f'<div style="font-size:11px;text-transform:uppercase;letter-spacing:.5px;color:#718096;margin-bottom:4px;">{label}</div>'
            f'<div style="font-size:20px;font-weight:700;">{_html_pnl(stats["pnl"])}</div>'
            f'<div style="font-size:11px;color:#718096;">{stats["trades"]} trades &middot; {stats["win_rate"]} WR</div></div>'
        )

    today_combined_stats = {
        **today_stats,
        "pnl": today_stats["pnl"] + discretionary_pnl,
    }
    cards_html = (
        '<div style="display:flex;gap:12px;margin-bottom:12px;">'
        + _kpi_card("Today", today_combined_stats)
        + _kpi_card("5-Day", five_day_stats)
        + _kpi_card("MTD", mtd_stats)
        + _kpi_card("YTD", ytd_stats)
        + '</div>'
    )

    # --- Today's Trades ---
    trades_rows = ""
    for row in today_trades_data.get("rows", []):
        if row["outcome_type"] == "filled":
            badge = '<span class="badge badge-filled">FILLED</span>'
        elif row["outcome_type"] == "working":
            badge = '<span class="badge badge-working">WORKING</span>'
        elif row["outcome_type"] == "noorder":
            badge = '<span class="badge badge-noorder">NO ORDER</span>'
        else:
            badge = '<span class="badge badge-nofill">NOT FILLED</span>'
        trades_rows += (
            f'<tr><td>{row["account"]}</td><td>{row["strategy"]}</td>'
            f'<td>{row.get("trade_type", row.get("signal", ""))}</td><td>{badge}</td>'
            f'<td class="right">{row.get("puts", "")}</td>'
            f'<td class="right">{row.get("calls", "")}</td></tr>'
        )

    trades_html = (
        '<table><tr><th>Account</th><th>Strategy</th><th>Type</th>'
        '<th>Status</th><th class="right">Puts</th><th class="right">Calls</th></tr>'
        f'{trades_rows}</table>'
    ) if trades_rows else '<p class="muted">No automated trades today.</p>'

    # --- Last 5 Sessions ---
    recent_map: dict[tuple[date, str], float] = defaultdict(float)
    for p in settled:
        settled_day = _settle_date(p)
        if settled_day in recent_days:
            for group in EMAIL_GROUPS:
                if p["strategy"] in group["members"]:
                    recent_map[(settled_day, group["key"])] += float(p["pnl"] or 0)
                    break

    session_rows = ""
    for day in recent_days:
        cs = recent_map[(day, "constantstable")]
        ds = recent_map[(day, "dualside")]
        bf = recent_map[(day, "butterfly")]
        total = cs + ds + bf
        session_rows += (
            f'<tr><td class="mono">{day.strftime("%m/%d")}</td>'
            f'<td class="right">{_html_pnl_plain(cs)}</td>'
            f'<td class="right">{_html_pnl_plain(ds)}</td>'
            f'<td class="right">{_html_pnl_plain(bf)}</td>'
            f'<td class="right" style="font-weight:600;">{_html_pnl_plain(total)}</td></tr>'
        )

    sessions_html = (
        '<table><tr><th>Date</th><th class="right">CS</th><th class="right">DS</th>'
        '<th class="right">BF</th><th class="right">Total</th></tr>'
        f'{session_rows}</table>'
    )

    # --- Strategy Contribution ---
    strat_rows_data = []
    for group in EMAIL_GROUPS:
        members = set(group["members"])
        strat_ytd = [p for p in ytd_positions if p["strategy"] in members]
        strat_mtd = [p for p in mtd_positions if p["strategy"] in members]
        strat_5d = [p for p in five_day_positions if p["strategy"] in members]
        strat_all = [p for p in settled if p["strategy"] in members]
        if not strat_ytd and not strat_mtd and not strat_5d:
            continue
        ytd_s = _stats(strat_ytd)
        strat_rows_data.append({
            "label": group["label"],
            "five_day_pnl": float(sum(p["pnl"] or 0 for p in strat_5d)),
            "mtd_pnl": float(sum(p["pnl"] or 0 for p in strat_mtd)),
            "ytd_pnl": float(sum(p["pnl"] or 0 for p in strat_ytd)),
            "trades_ytd": ytd_s["trades"],
            "wr_ytd": ytd_s["win_rate"],
            "streak": _current_streak(strat_all),
        })
    strat_rows_data.sort(key=lambda r: r["ytd_pnl"], reverse=True)

    strat_rows_html = ""
    for r in strat_rows_data:
        strat_rows_html += (
            f'<tr><td style="font-weight:500;">{r["label"]}</td>'
            f'<td class="right">{_html_pnl_plain(r["five_day_pnl"])}</td>'
            f'<td class="right">{_html_pnl_plain(r["mtd_pnl"])}</td>'
            f'<td class="right">{_html_pnl_plain(r["ytd_pnl"])}</td>'
            f'<td class="right">{r["trades_ytd"]}</td>'
            f'<td class="right">{r["wr_ytd"]}</td>'
            f'<td class="right">{r["streak"]}</td></tr>'
        )

    strat_html = (
        '<table><tr><th>Strategy</th><th class="right">5D</th><th class="right">MTD</th>'
        '<th class="right">YTD</th><th class="right">Trades</th><th class="right">Win%</th>'
        '<th class="right">Streak</th></tr>'
        f'{strat_rows_html}</table>'
    )

    # --- Risk State ---
    current_dd_ytd, max_dd_ytd = _drawdown_stats(settled, _first_of_year(report_date), report_date)
    current_dd_mtd, max_dd_mtd = _drawdown_stats(settled, _first_of_month(report_date), report_date)
    portfolio_streak = _current_streak(settled)

    risk_html = (
        f'<table style="font-size:13px;">'
        f'<tr><td style="padding:4px 8px;color:#718096;">Streak</td><td style="padding:4px 8px;font-weight:600;">{portfolio_streak}</td></tr>'
        f'<tr><td style="padding:4px 8px;color:#718096;">MTD Drawdown</td>'
        f'<td style="padding:4px 8px;">{_html_pnl_plain(-current_dd_mtd)} (max {_html_pnl_plain(-max_dd_mtd)})</td></tr>'
        f'<tr><td style="padding:4px 8px;color:#718096;">YTD Drawdown</td>'
        f'<td style="padding:4px 8px;">{_html_pnl_plain(-current_dd_ytd)} (max {_html_pnl_plain(-max_dd_ytd)})</td></tr>'
        f'</table>'
    )

    # --- Discretionary ---
    disc_html = ""
    if discretionary_section_data and discretionary_section_data.get("trade_count", 0) > 0:
        trades_list = discretionary_section_data.get("trades", [])
        spx = discretionary_section_data.get("spx_spot")
        disc_trade_rows = ""
        for t in sorted(trades_list, key=lambda x: x["time"]):
            strikes_str = "/".join(f"{s:.0f}" for s in t["strikes"]) if t["strikes"] else "—"
            adj_tag = ' <span style="color:#a0aec0;font-size:10px;">[adj]</span>' if t.get("is_adjustment") else ""
            disc_trade_rows += (
                f'<tr><td class="mono">{t["time"]}</td><td>{t["structure"]}{adj_tag}</td>'
                f'<td class="mono">{strikes_str}</td>'
                f'<td class="right">{_html_pnl_plain(t["premium"])}</td></tr>'
            )

        d_pnl = discretionary_section_data.get("total_pnl", 0)
        disc_html = (
            f'<div class="section"><hr class="divider" style="margin-bottom:16px;">'
            f'<h2>Discretionary Trades</h2>'
            f'<p style="font-size:12px;color:#718096;margin:0 0 10px;">SPX: ${spx:,.2f}</p>'
            f'<table><tr><th>Time</th><th>Structure</th><th>Strikes</th><th class="right">Premium</th></tr>'
            f'{disc_trade_rows}</table>'
            f'<div style="margin-top:12px;font-size:14px;font-weight:600;">Disc P&L: {_html_pnl_plain(d_pnl)}</div>'
            f'</div>'
        )

    # --- Combined bottom line ---
    combined_html = ""
    if disc_html:
        today_auto = sum(
            float(p["pnl"] or 0) for p in _window_positions(settled, report_date, report_date)
        )
        combined = today_auto + discretionary_pnl
        combined_html = (
            f'<div class="section">'
            f'<h2>Combined</h2>'
            f'<table style="font-size:14px;">'
            f'<tr><td>Auto</td><td class="right" style="font-weight:600;">{_html_pnl_plain(today_auto)}</td></tr>'
            f'<tr><td>Discretionary</td><td class="right" style="font-weight:600;">{_html_pnl_plain(discretionary_pnl)}</td></tr>'
            f'<tr style="border-top:2px solid #e2e8f0;"><td style="font-weight:700;">Total</td>'
            f'<td class="right" style="font-weight:700;font-size:16px;">{_html_pnl_plain(combined)}</td></tr>'
            f'</table></div>'
        )

    html = f"""\
<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<style>{_CSS}</style></head><body>
<div class="container">
  <div class="header">
    <h1>Gamma Portfolio Pulse</h1>
    <div class="date">{report_date.strftime('%A, %B %d, %Y')}</div>
  </div>

  <div class="section">
    <h2>Performance</h2>
    {cards_html}
  </div>

  <div class="section">
    <h2>Today's Trades</h2>
    {trades_html}
  </div>

  <hr class="divider">

  <div class="section">
    <h2>Last 5 Sessions</h2>
    {sessions_html}
  </div>

  <hr class="divider">

  <div class="section">
    <h2>Strategy Breakdown</h2>
    {strat_html}
  </div>

  <hr class="divider">

  <div class="section">
    <h2>Risk</h2>
    {risk_html}
  </div>

  {disc_html}
  {combined_html}

  <div class="footer">Gamma Reporting &middot; automated</div>
</div>
</body></html>"""

    return html


def _order_dt_et(order: dict) -> datetime | None:
    stamp = order.get("closeTime") or order.get("enteredTime") or ""
    if not stamp:
        return None
    try:
        dt_utc = datetime.fromisoformat(stamp.replace("+0000", "+00:00"))
        return dt_utc.astimezone(ET)
    except Exception:
        return None


def _behavior_context(raw_orders: list[dict], positions: list[dict], report_date: date) -> dict[str, list[dict]]:
    issues: dict[str, list[dict]] = {
        "order_anomalies": [],
        "missing_settlement": [],
        "open_past_expiry": [],
        "inventory_conflicts": [],
    }

    for order in raw_orders:
        if order.get("tag") != API_TAG:
            continue
        dt_et = _order_dt_et(order)
        if dt_et is None or dt_et.date() != report_date:
            continue
        status = str(order.get("status") or "").upper()
        if status in {"FILLED", "CANCELED"}:
            continue
        issues["order_anomalies"].append({
            "strategy": classify_order(order),
            "status": status or "UNKNOWN",
            "time": dt_et.strftime("%H:%M"),
            "order_type": order.get("orderType") or "—",
        })

    open_positions = [p for p in positions if p["exit_method"] == "OPEN"]
    for p in positions:
        if p["exit_method"] == "EXPIRED_NO_SETTLEMENT":
            issues["missing_settlement"].append({
                "strategy": p["strategy"],
                "expiry": p["expiry"],
                "option_type": p["option_type"],
                "strikes": p["strikes"],
                "qty": p["qty"],
            })

    seen_conflicts: set[tuple] = set()
    signal_map: dict[tuple, set[str]] = defaultdict(set)
    for p in open_positions:
        if p.get("expiry"):
            exp = date.fromisoformat(p["expiry"])
            if exp < report_date:
                issues["open_past_expiry"].append({
                    "strategy": p["strategy"],
                    "expiry": p["expiry"],
                    "option_type": p["option_type"],
                    "strikes": p["strikes"],
                    "qty": p["qty"],
                    "signal": p["signal"],
                })
        key = (p["strategy"], p.get("expiry"), p["option_type"], tuple(p["strikes"]))
        signal_map[key].add(p["signal"])

    for key, signals in signal_map.items():
        if len(signals) > 1:
            if key in seen_conflicts:
                continue
            seen_conflicts.add(key)
            strat, expiry, option_type, strikes = key
            issues["inventory_conflicts"].append({
                "strategy": strat,
                "expiry": expiry,
                "option_type": option_type,
                "strikes": list(strikes),
                "signals": sorted(signals),
            })

    return issues


def _has_behavior_issues(ctx: dict[str, list[dict]]) -> bool:
    return any(ctx.values())


def _build_behavior_email(ctx: dict[str, list[dict]], report_date: date) -> tuple[str, str]:
    count = sum(len(v) for v in ctx.values())
    subject = f"[Gamma] Behavior Check: {count} issue(s) — {report_date.isoformat()}"
    lines = [
        f"Gamma Behavior Check (Schwab) — {report_date.isoformat()}",
        "=" * 50,
        "",
    ]

    if ctx["order_anomalies"]:
        lines.append(f"Order anomalies ({len(ctx['order_anomalies'])})")
        for item in ctx["order_anomalies"]:
            lines.append(
                f"- {item['time']} {item['strategy']} status={item['status']} type={item['order_type']}"
            )
        lines.append("")

    if ctx["missing_settlement"]:
        lines.append(f"Settlement gaps ({len(ctx['missing_settlement'])})")
        for item in ctx["missing_settlement"]:
            strikes = "-".join(f"{s:.0f}" for s in item["strikes"])
            lines.append(
                f"- exp {item['expiry']} {item['strategy']} {item['option_type']} {strikes} qty={item['qty']}"
            )
        lines.append("")

    if ctx["open_past_expiry"]:
        lines.append(f"Open past expiry ({len(ctx['open_past_expiry'])})")
        for item in ctx["open_past_expiry"]:
            strikes = "-".join(f"{s:.0f}" for s in item["strikes"])
            lines.append(
                f"- {item['strategy']} {item['option_type']} {strikes} exp {item['expiry']} "
                f"qty={item['qty']} {item['signal']}"
            )
        lines.append("")

    if ctx["inventory_conflicts"]:
        lines.append(f"Inventory conflicts ({len(ctx['inventory_conflicts'])})")
        for item in ctx["inventory_conflicts"]:
            strikes = "-".join(f"{s:.0f}" for s in item["strikes"])
            lines.append(
                f"- {item['strategy']} {item['option_type']} {strikes} exp {item['expiry']} "
                f"signals={','.join(item['signals'])}"
            )
        lines.append("")

    lines.append("— Gamma Reporting (automated)")
    return subject, "\n".join(lines)


def _archive_email(
    *,
    report_date: date,
    subject: str,
    body: str,
    delivery_state: dict[str, Any],
    source: str,
    positions: list[dict],
) -> None:
    """Persist the rendered email for historical QA and replay."""
    from reporting.db import execute, get_connection, init_schema

    settled_today = [
        p for p in positions
        if p["exit_method"] in ("EXPIRED", "CLOSED_EARLY")
        and (
            p.get("close_date") == report_date.isoformat()
            or (p["exit_method"] == "EXPIRED" and p["expiry"] == report_date.isoformat())
        )
    ]
    payload = {
        "subject": subject,
        "body": body,
        "source": source,
        "delivery": delivery_state,
        "positions": len(positions),
        "settled_today": len(settled_today),
        "scope": "schwab_only",
    }

    con = get_connection()
    init_schema(con)
    execute(
        """INSERT INTO daily_report_outputs
           (id, report_date, format, content, trust_banner)
           VALUES (?, ?, 'email', ?, NULL)""",
        [
            uuid.uuid4().hex[:16],
            report_date.isoformat(),
            json.dumps(payload, sort_keys=True),
        ],
        con=con,
    )


# ---------------------------------------------------------------------------
# Email sending
# ---------------------------------------------------------------------------

def _send_email(
    subject: str,
    body: str,
    dry_run: bool = False,
    html_body: str | None = None,
) -> dict:
    """Send an email via SMTP (HTML with plain-text fallback).

    Uses same SMTP env vars as scripts/notify/smtp_notify.py:
    SMTP_USER, SMTP_PASS, SMTP_TO, SMTP_HOST, SMTP_PORT.
    """
    if dry_run:
        print(f"\n--- DRY RUN ---")
        print(f"Subject: {subject}")
        print(f"\n{body}")
        return {"sent": False, "dry_run": True}

    smtp_user = (os.environ.get("SMTP_USER") or "").strip()
    smtp_pass = (os.environ.get("SMTP_PASS") or "").strip()
    smtp_to = (os.environ.get("SMTP_TO") or "").strip() or smtp_user
    smtp_host = (os.environ.get("SMTP_HOST") or "smtp.gmail.com").strip()
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))

    if not smtp_user or not smtp_pass:
        print("SMTP_USER or SMTP_PASS not set — skipping email")
        return {"sent": False, "reason": "no SMTP credentials"}

    msg = EmailMessage()
    msg["From"] = smtp_user
    msg["To"] = smtp_to
    msg["Subject"] = subject
    msg.set_content(body)
    if html_body:
        msg.add_alternative(html_body, subtype="html")

    with smtplib.SMTP(smtp_host, smtp_port, timeout=20) as s:
        s.starttls()
        s.login(smtp_user, smtp_pass)
        s.send_message(msg)

    print(f"P&L email sent to {smtp_to}")
    return {"sent": True, "to": smtp_to, "subject": subject}


def send_pnl_email(subject: str, body: str, dry_run: bool = False, html_body: str | None = None) -> dict:
    return _send_email(subject, body, dry_run=dry_run, html_body=html_body)


def send_behavior_email_if_needed(
    *,
    raw_orders: list[dict],
    positions: list[dict],
    report_date: date,
    dry_run: bool = False,
) -> dict:
    ctx = _behavior_context(raw_orders, positions, report_date)
    if not _has_behavior_issues(ctx):
        return {"skipped": "no behavior issues"}
    subject, body = _build_behavior_email(ctx, report_date)
    result = _send_email(subject, body, dry_run=dry_run)
    result["issue_counts"] = {k: len(v) for k, v in ctx.items()}
    return result


# ---------------------------------------------------------------------------
# Main entry point (called from Lambda or CLI)
# ---------------------------------------------------------------------------

def run_daily_pnl_email(
    dry_run: bool = False,
    orders_file: str | None = None,
    lookback_days: int = 120,
    report_date: date | None = None,
) -> dict:
    """Full pipeline: load orders → classify → compute P&L → send email."""
    report_date = report_date or datetime.now(ET).date()

    # Load orders
    if orders_file:
        raw_orders = load_orders_from_file(orders_file)
        source = f"file:{orders_file}"
    else:
        raw_orders = load_orders_from_schwab(lookback_days=lookback_days)
        source = "schwab_api"

    if not raw_orders:
        return {"sent": False, "reason": "no orders loaded", "source": source}

    filled = [o for o in raw_orders if o.get("status") == "FILLED"]
    print(f"[daily_pnl] {len(raw_orders)} orders ({len(filled)} filled) from {source}")

    # Process
    trades = parse_filled_orders(raw_orders)
    settlements = load_settlements()
    positions = build_positions(
        trades,
        settlements,
        as_of=report_date,
        include_same_day_expiry=True,
    )

    auto = [p for p in positions if p["strategy"] not in ("manual", "butterfly_manual")]
    print(f"[daily_pnl] {len(auto)} automated positions "
          f"({sum(1 for p in auto if p['exit_method'] in ('EXPIRED','CLOSED_EARLY'))} settled, "
          f"{sum(1 for p in auto if p['exit_method'] == 'OPEN')} open)")

    # Today's trade summary (Schwab + TT)
    tt_orders: list[dict] = []
    try:
        tt_orders = _load_tt_orders_for_date(report_date)
    except Exception as e:
        print(f"[daily_pnl] TT order load ERROR (non-fatal): {e}")

    today_trades_section, today_trades_data = _build_today_trades_section(
        raw_schwab_orders=raw_orders,
        tt_orders=tt_orders,
        report_date=report_date,
    )

    # Discretionary P&L (net position method, SPX spot for today's expiry)
    disc_section = ""
    disc_pnl = 0.0
    disc_result_data: dict | None = None
    try:
        spx_spot = _load_spx_spot(for_date=report_date)
        print(f"[daily_pnl] SPX spot for {report_date}: {spx_spot}")
        disc_result = _compute_discretionary_pnl(raw_orders, report_date, spx_spot)
        disc_pnl = disc_result["total_pnl"]
        if disc_result["trade_count"] > 0:
            disc_section = _build_discretionary_section(disc_result, report_date)
            disc_result_data = disc_result
            print(f"[daily_pnl] Discretionary: {disc_result['trade_count']} trades, P&L: {disc_pnl:+,.0f}")
        else:
            print("[daily_pnl] No discretionary trades expiring today")
    except Exception as e:
        print(f"[daily_pnl] Discretionary section ERROR (non-fatal): {e}")

    # Build plain-text email (fallback)
    subject, body = _build_email(
        auto, report_date,
        today_trades_section=today_trades_section,
        discretionary_section=disc_section,
        discretionary_pnl=disc_pnl,
    )

    # Build HTML email
    html_body = _build_email_html(
        auto, report_date,
        today_trades_data=today_trades_data,
        discretionary_section_data=disc_result_data,
        discretionary_pnl=disc_pnl,
    )

    result = _send_email(subject, body, dry_run=dry_run, html_body=html_body)
    result["source"] = source
    result["positions"] = len(auto)
    result["behavior_email"] = send_behavior_email_if_needed(
        raw_orders=raw_orders,
        positions=auto,
        report_date=report_date,
        dry_run=dry_run,
    )
    try:
        _archive_email(
            report_date=report_date,
            subject=subject,
            body=body,
            delivery_state=result,
            source=source,
            positions=auto,
        )
        result["archived"] = True
    except Exception as e:
        print(f"[daily_pnl] archive ERROR: {e}")
        result["archived"] = False
        result["archive_error"] = str(e)

    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Daily P&L email")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print email, don't send")
    parser.add_argument("--file", type=str, default=None,
                        help="Use cached orders file instead of API")
    parser.add_argument("--lookback", type=int, default=30,
                        help="Days of order history to pull (default: 30)")
    parser.add_argument("--report-date", type=str, default=None,
                        help="Override report date (YYYY-MM-DD)")
    args = parser.parse_args()

    report_date = date.fromisoformat(args.report_date) if args.report_date else None

    result = run_daily_pnl_email(
        dry_run=args.dry_run,
        orders_file=args.file,
        lookback_days=args.lookback,
        report_date=report_date,
    )
    print(f"\nResult: {result}")


if __name__ == "__main__":
    main()
