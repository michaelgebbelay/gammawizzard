"""Daily P&L email — compact summary of automated strategy performance.

Pulls fresh orders from Schwab, classifies by strategy, computes P&L,
and sends a concise email via SMTP.

Designed to run as a Lambda post-market step (~4:45 PM ET) after
close5 collection ensures settlement data is available.

Usage:
    python -m reporting.daily_pnl_email              # send email
    python -m reporting.daily_pnl_email --dry-run    # print only, no send
    python -m reporting.daily_pnl_email --file /tmp/schwab_orders_raw.json
"""

from __future__ import annotations

import os
import smtplib
from datetime import date, timedelta
from email.message import EmailMessage
from typing import Any

from reporting.broker_pnl import (
    STRATEGY_LABELS,
    STRATEGY_ORDER,
    build_positions,
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


def _build_email(positions: list[dict], report_date: date) -> tuple[str, str]:
    """Build email subject and body from position data.

    Returns (subject, body).
    """
    # Separate positions by status
    settled = [p for p in positions if p["exit_method"] in ("EXPIRED", "CLOSED_EARLY")]
    still_open = [p for p in positions if p["exit_method"] == "OPEN"]
    missing = [p for p in positions if p["exit_method"] == "EXPIRED_NO_SETTLEMENT"]

    # Today's activity
    today_str = report_date.isoformat()
    settled_today = [p for p in settled if p.get("close_date") == today_str
                     or (p["exit_method"] == "EXPIRED" and p["expiry"] == today_str)]
    opened_today = [p for p in positions if p["fill_date"] == today_str]

    # Per-strategy totals
    strat_data: dict[str, dict[str, Any]] = {}
    for strat in STRATEGY_ORDER:
        strat_pos = [p for p in settled if p["strategy"] == strat]
        if not strat_pos:
            continue
        total = sum(p["pnl"] or 0 for p in strat_pos)
        wins = sum(1 for p in strat_pos if (p["pnl"] or 0) > 0)
        strat_data[strat] = {
            "trades": len(strat_pos),
            "wins": wins,
            "total": total,
            "open": len([p for p in still_open if p["strategy"] == strat]),
        }

    grand_pnl = sum(d["total"] for d in strat_data.values())
    grand_trades = sum(d["trades"] for d in strat_data.values())
    grand_wins = sum(d["wins"] for d in strat_data.values())

    # Subject line
    pnl_str = _fmt(grand_pnl)
    subject = f"[Gamma] Daily P&L: {pnl_str} ({grand_trades} trades) — {today_str}"

    # Body
    lines = [
        f"Gamma Portfolio — {today_str}",
        f"{'=' * 50}",
        "",
    ]

    # Strategy summary table
    lines.append(f"{'Strategy':<25} {'Trades':>7} {'Win%':>6} {'P&L':>12}")
    lines.append(f"{'-' * 25} {'-' * 7} {'-' * 6} {'-' * 12}")

    for strat in STRATEGY_ORDER:
        sd = strat_data.get(strat)
        if not sd:
            continue
        label = STRATEGY_LABELS.get(strat, strat)
        wr = f"{sd['wins']/sd['trades']*100:.0f}%" if sd["trades"] > 0 else "—"
        lines.append(f"{label:<25} {sd['trades']:>7} {wr:>6} {_fmt(sd['total']):>12}")

    wr_total = f"{grand_wins/grand_trades*100:.0f}%" if grand_trades > 0 else "—"
    lines.append(f"{'-' * 25} {'-' * 7} {'-' * 6} {'-' * 12}")
    lines.append(f"{'TOTAL':<25} {grand_trades:>7} {wr_total:>6} {_fmt(grand_pnl):>12}")

    # Today's activity
    if settled_today:
        lines.append("")
        lines.append(f"--- Settled Today ({len(settled_today)}) ---")
        for p in settled_today:
            s_str = "-".join(f"{s:.0f}" for s in p["strikes"])
            pv = p["pnl"] or 0
            method = "closed" if p["exit_method"] == "CLOSED_EARLY" else "expired"
            lines.append(
                f"  {p['strategy']:<15} {p['option_type']:<6} {s_str:<20} "
                f"qty={p['qty']}  {_fmt(pv):>10}  ({method})"
            )

    if opened_today:
        lines.append("")
        lines.append(f"--- Opened Today ({len(opened_today)}) ---")
        for p in opened_today:
            s_str = "-".join(f"{s:.0f}" for s in p["strikes"])
            lines.append(
                f"  {p['strategy']:<15} {p['option_type']:<6} {s_str:<20} "
                f"qty={p['qty']}  ${p['entry_price']:.2f}  exp {p['expiry']}  {p['signal']}"
            )

    # Open positions
    if still_open:
        lines.append("")
        lines.append(f"--- Open Positions ({len(still_open)}) ---")
        for p in sorted(still_open, key=lambda x: x["expiry"] or ""):
            s_str = "-".join(f"{s:.0f}" for s in p["strikes"])
            lines.append(
                f"  {p['strategy']:<15} {p['option_type']:<6} {s_str:<20} "
                f"qty={p['qty']}  ${p['entry_price']:.2f}  exp {p['expiry']}  {p['signal']}"
            )

    # Missing settlement
    if missing:
        lines.append("")
        lines.append(f"--- Missing Settlement ({len(missing)}) ---")
        for p in missing:
            s_str = "-".join(f"{s:.0f}" for s in p["strikes"])
            lines.append(f"  exp {p['expiry']}  {p['option_type']} {s_str}  qty={p['qty']}")

    lines.append("")
    lines.append("— Gamma Reporting (automated)")

    return subject, "\n".join(lines)


# ---------------------------------------------------------------------------
# Email sending
# ---------------------------------------------------------------------------

def send_pnl_email(
    subject: str,
    body: str,
    dry_run: bool = False,
) -> dict:
    """Send the P&L email via SMTP.

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


# ---------------------------------------------------------------------------
# Main entry point (called from Lambda or CLI)
# ---------------------------------------------------------------------------

def run_daily_pnl_email(
    dry_run: bool = False,
    orders_file: str | None = None,
    lookback_days: int = 30,
) -> dict:
    """Full pipeline: load orders → classify → compute P&L → send email."""
    report_date = date.today()

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
    positions = build_positions(trades, settlements, as_of=report_date)

    auto = [p for p in positions if p["strategy"] not in ("manual", "butterfly_manual")]
    print(f"[daily_pnl] {len(auto)} automated positions "
          f"({sum(1 for p in auto if p['exit_method'] in ('EXPIRED','CLOSED_EARLY'))} settled, "
          f"{sum(1 for p in auto if p['exit_method'] == 'OPEN')} open)")

    # Build and send email
    subject, body = _build_email(auto, report_date)
    result = send_pnl_email(subject, body, dry_run=dry_run)
    result["source"] = source
    result["positions"] = len(auto)

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
