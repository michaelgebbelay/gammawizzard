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

from reporting.broker_pnl import (
    ET,
    API_TAG,
    STRATEGY_LABELS,
    STRATEGY_ORDER,
    build_positions,
    classify_order,
    load_orders_from_schwab,
    load_orders_from_file,
    load_settlements,
    parse_filled_orders,
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


def _build_email(positions: list[dict], report_date: date) -> tuple[str, str]:
    """Build the portfolio-health email.

    Returns (subject, body).
    """
    today_str = report_date.isoformat()
    settled = _settled_positions(positions)
    today_positions = _window_positions(settled, report_date, report_date)
    mtd_positions = _window_positions(settled, _first_of_month(report_date), report_date)
    ytd_positions = _window_positions(settled, _first_of_year(report_date), report_date)

    today_stats = _stats(today_positions)
    mtd_stats = _stats(mtd_positions)
    ytd_stats = _stats(ytd_positions)

    strat_rows = []
    for strat in STRATEGY_ORDER:
        strat_all = [p for p in settled if p["strategy"] == strat]
        strat_ytd = [p for p in ytd_positions if p["strategy"] == strat]
        strat_mtd = [p for p in mtd_positions if p["strategy"] == strat]
        strat_today = [p for p in today_positions if p["strategy"] == strat]
        if not strat_ytd and not strat_mtd and not strat_today:
            continue
        ytd_stats_strat = _stats(strat_ytd)
        strat_rows.append({
            "label": STRATEGY_LABELS.get(strat, strat),
            "today_pnl": float(sum(p["pnl"] or 0 for p in strat_today)),
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

    best_mtd = max(strat_rows, key=lambda r: r["mtd_pnl"], default=None)
    worst_mtd = min(strat_rows, key=lambda r: r["mtd_pnl"], default=None)
    best_ytd = max(strat_rows, key=lambda r: r["ytd_pnl"], default=None)
    worst_ytd = min(strat_rows, key=lambda r: r["ytd_pnl"], default=None)

    subject = (
        f"[Gamma] Health: Today {_fmt(today_stats['pnl'])} | "
        f"MTD {_fmt(mtd_stats['pnl'])} | YTD {_fmt(ytd_stats['pnl'])} — {today_str}"
    )

    lines = [
        f"Gamma Portfolio Health (Schwab) — {today_str}",
        f"{'=' * 50}",
        "",
        "Realized performance only. Behavior anomalies arrive in a separate email when needed.",
        "",
    ]

    lines.append("Portfolio Summary")
    lines.append(f"{'Window':<10} {'Trades':>7} {'Win%':>6} {'P&L':>12} {'Avg/Trade':>12}")
    lines.append(f"{'-' * 10} {'-' * 7} {'-' * 6} {'-' * 12} {'-' * 12}")
    for label, stat in (
        ("Today", today_stats),
        ("MTD", mtd_stats),
        ("YTD", ytd_stats),
    ):
        lines.append(
            f"{label:<10} {stat['trades']:>7} {stat['win_rate']:>6} "
            f"{_fmt(stat['pnl']):>12} {stat['avg']:>12}"
        )

    lines.append("")
    lines.append("By Strategy")
    lines.append(
        f"{'Strategy':<25} {'Today':>10} {'MTD':>10} {'YTD':>10} "
        f"{'Trades':>7} {'Win%':>6} {'Streak':>7}"
    )
    lines.append(
        f"{'-' * 25} {'-' * 10} {'-' * 10} {'-' * 10} "
        f"{'-' * 7} {'-' * 6} {'-' * 7}"
    )
    if strat_rows:
        for row in strat_rows:
            lines.append(
                f"{row['label']:<25} {_fmt(row['today_pnl']):>10} {_fmt(row['mtd_pnl']):>10} "
                f"{_fmt(row['ytd_pnl']):>10} {row['trades_ytd']:>7} {row['wr_ytd']:>6} {row['streak']:>7}"
            )
    else:
        lines.append("No settled trades yet.")

    lines.append("")
    lines.append("Risk State")
    lines.append(f"- Current streak: {portfolio_streak}")
    lines.append(f"- Current drawdown (MTD): {_fmt(-current_dd_mtd)} from peak")
    lines.append(f"- Max drawdown (MTD): {_fmt(-max_dd_mtd)}")
    lines.append(f"- Current drawdown (YTD): {_fmt(-current_dd_ytd)} from peak")
    lines.append(f"- Max drawdown (YTD): {_fmt(-max_dd_ytd)}")
    if best_mtd and worst_mtd:
        lines.append(
            f"- MTD contributors: best {best_mtd['label']} {_fmt(best_mtd['mtd_pnl'])}, "
            f"worst {worst_mtd['label']} {_fmt(worst_mtd['mtd_pnl'])}"
        )
    if best_ytd and worst_ytd:
        lines.append(
            f"- YTD contributors: best {best_ytd['label']} {_fmt(best_ytd['ytd_pnl'])}, "
            f"worst {worst_ytd['label']} {_fmt(worst_ytd['ytd_pnl'])}"
        )

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
) -> dict:
    """Full pipeline: load orders → classify → compute P&L → send email."""
    report_date = datetime.now(ET).date()

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

    # Build and send email
    subject, body = _build_email(auto, report_date)
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
    args = parser.parse_args()

    result = run_daily_pnl_email(
        dry_run=args.dry_run,
        orders_file=args.file,
        lookback_days=args.lookback,
    )
    print(f"\nResult: {result}")


if __name__ == "__main__":
    main()
