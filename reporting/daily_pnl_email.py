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

TT_ACCOUNTS = {
    "5WT20360": "TT-IRA",
    "5WT09219": "TT-Indv",
}


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

    for acct_num, label in TT_ACCOUNTS.items():
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

                filled_qty = order.get("filled-quantity", order.get("filledQuantity", 0))
                total_qty = order.get("size", sum(l.get("quantity", 0) for l in legs))
                price_effect = order.get("price-effect", "")
                price = order.get("price", 0)

                tt_orders.append({
                    "broker": "tt",
                    "account_label": label,
                    "account_number": acct_num,
                    "status": status,
                    "dt_et": dt_et,
                    "filled_qty": int(filled_qty) if filled_qty else 0,
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
        f"{'Account':<10} {'Strategy':<16} {'Signal':<14} {'Strikes':<20} "
        f"{'Fill':>8} {'Price':>8} {'$/Ct':>8} {'Status':<10}",
        f"{'-' * 10} {'-' * 16} {'-' * 14} {'-' * 20} "
        f"{'-' * 8} {'-' * 8} {'-' * 8} {'-' * 10}",
    ]

    trade_count = 0
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

        # Only show opening orders (not closes)
        legs_raw = order.get("orderLegCollection", [])
        instructions = set(l.get("instruction", "") for l in legs_raw)
        if all("CLOSE" in i for i in instructions if i):
            continue

        status = order.get("status", "")
        strategy = classify_order(order)
        if strategy in ("manual", "butterfly_manual"):
            continue

        # Parse legs for signal classification
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
        filled_qty = int(order.get("filledQuantity", 0))
        total_qty = max(filled_qty, max((l.get("quantity", 0) for l in legs_raw), default=0))
        fill_str = _fill_status_str(status, filled_qty, total_qty)
        price = order.get("price", 0)
        price_str = f"${price:.2f}" if price else "—"
        dollar_str = f"${price * 100:.0f}" if price else "—"

        strategy_label = {
            "constantstable": "ConstantStable",
            "cs_morning": "CS Morning",
            "dualside": "DualSide",
            "butterfly": "Butterfly",
        }.get(strategy, strategy)

        # Build fail reason for non-FILLED orders
        reason = ""
        status_upper = status.upper()
        if status_upper == "REJECTED":
            reason = order.get("statusDescription", "") or "broker rejected"
        elif status_upper in ("CANCELED", "CANCELLED"):
            cancel_qty = order.get("cancelledQuantity", order.get("canceledQuantity", 0))
            reason = f"canceled ({cancel_qty} qty)" if cancel_qty else "canceled by system"
        elif status_upper == "EXPIRED":
            reason = "order expired unfilled"
        elif status_upper in ("WORKING", "QUEUED", "PENDING_ACTIVATION"):
            reason = "still working (not yet filled)"

        suffix = f" [{reason}]" if reason else ""

        lines.append(
            f"{'Schwab':<10} {strategy_label:<16} {signal:<14} "
            f"{_strikes_str(sorted_strikes, parsed_legs):<20} "
            f"{filled_qty:>8} {price_str:>8} {dollar_str:>8} {fill_str}{suffix}"
        )
        trade_count += 1
        schwab_strategies_seen.add(strategy)

    # --- TT orders ---
    for order in tt_orders:
        # Only show opening orders
        legs = order.get("legs", [])
        actions = set(l.get("action", "").upper() for l in legs)
        if all("CLOSE" in a for a in actions if a):
            continue

        status = order.get("status", "")
        signal = _classify_signal(order, legs)
        filled_qty = order["filled_qty"]
        total_qty = order["total_qty"]
        fill_str = _fill_status_str(status, filled_qty, total_qty)
        price = order["price"]
        price_str = f"${price:.2f}" if price else "—"
        dollar_str = f"${price * 100:.0f}" if price else "—"

        # Classify strategy by width
        width = order["width"]
        if width == 10:
            strategy_label = "DualSide"
        elif order["num_strikes"] == 3:
            strategy_label = "Butterfly"
        else:
            strategy_label = "ConstantStable"

        # Fail reason for TT
        reason = ""
        status_upper = status.upper()
        if status_upper == "REJECTED":
            reason = "broker rejected"
        elif status_upper in ("CANCELLED", "CANCELED"):
            reason = "canceled"
        elif status_upper == "EXPIRED":
            reason = "order expired unfilled"
        elif status_upper in ("LIVE", "RECEIVED"):
            reason = "still working (not yet filled)"

        suffix = f" [{reason}]" if reason else ""

        lines.append(
            f"{order['account_label']:<10} {strategy_label:<16} {signal:<14} "
            f"{_strikes_str(order['strikes'], legs):<20} "
            f"{filled_qty:>8} {price_str:>8} {dollar_str:>8} {fill_str}{suffix}"
        )
        trade_count += 1
        tt_strategies_seen.add(order["account_label"])

    if trade_count == 0:
        lines.append("No automated trades today.")

    # --- Missing strategies check ---
    # On weekdays, flag strategies with no orders at all
    if report_date.weekday() < 5:
        missing = []
        if "constantstable" not in schwab_strategies_seen:
            missing.append("Schwab CS — no order found")
        if "TT-IRA" not in tt_strategies_seen:
            missing.append("TT-IRA CS — no order found")
        if "TT-Indv" not in tt_strategies_seen:
            missing.append("TT-Indv CS — no order found")
        # DualSide and Butterfly run on Schwab only
        if "dualside" not in schwab_strategies_seen:
            missing.append("Schwab DualSide — no order found")
        # Butterfly runs daily but may legitimately SKIP
        if missing:
            lines.append("")
            lines.append("Missing (no orders seen at broker):")
            for m in missing:
                lines.append(f"  ! {m}")
            lines.append("  Check: Lambda logs, token expiry, buying power, dry-run flags")

    lines.append(f"Legend: Price=per-share, $/Ct=per-contract | Source: Broker APIs")
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

    subject = (
        f"[Gamma] Health: 5D {_fmt(five_day_stats['pnl'])} | "
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
    for label, stat in (
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

    lines.append("")
    lines.append("— Gamma Reporting (automated)")

    return subject, "\n".join(lines)


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
) -> dict:
    """Send a plain-text email via SMTP.

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

    with smtplib.SMTP(smtp_host, smtp_port, timeout=20) as s:
        s.starttls()
        s.login(smtp_user, smtp_pass)
        s.send_message(msg)

    print(f"P&L email sent to {smtp_to}")
    return {"sent": True, "to": smtp_to, "subject": subject}


def send_pnl_email(subject: str, body: str, dry_run: bool = False) -> dict:
    return _send_email(subject, body, dry_run=dry_run)


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

    today_trades_section = _build_today_trades_section(
        raw_schwab_orders=raw_orders,
        tt_orders=tt_orders,
        report_date=report_date,
    )

    # Build and send email
    subject, body = _build_email(auto, report_date, today_trades_section=today_trades_section)
    result = send_pnl_email(subject, body, dry_run=dry_run)
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
