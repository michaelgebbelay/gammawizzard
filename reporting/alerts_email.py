"""Email alerts for portfolio reporting pipeline.

Sends a compact email when:
  - ERROR or CRITICAL reconciliation items exist
  - Broker sources are stale
  - Nightly run is missing past its expected completion time

Skips email when there are no actionable conditions.

Uses the same SMTP_USER / SMTP_PASS / SMTP_TO env vars as
scripts/notify/smtp_notify.py (stored in SSM /gamma/shared/).

Usage:
    from reporting.alerts_email import send_alert_if_needed

    result = send_alert_if_needed(report_date, con=con)
    # {"sent": True, "subject": "..."} or {"skipped": "no actionable issues"}
"""

from __future__ import annotations

import hashlib
import os
import smtplib
from datetime import date, datetime, time, timezone
from email.message import EmailMessage

from reporting.db import query_df, query_one


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

# Nightly run is expected to finish by this time (US/Eastern).
# If no daily_report_outputs row exists for today after this cutoff,
# the missing-run alert fires.
NIGHTLY_DEADLINE_ET = time(16, 45)  # 4:45 PM ET


# ---------------------------------------------------------------------------
# Alert extraction
# ---------------------------------------------------------------------------

def _is_past_deadline(report_date: date, now: datetime | None = None) -> bool:
    """Check if we are past the nightly deadline for report_date.

    Only returns True on weekdays (Mon-Fri) when current ET time
    is past NIGHTLY_DEADLINE_ET and the report_date matches today.
    """
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        from backports.zoneinfo import ZoneInfo  # type: ignore[no-redef]

    et = ZoneInfo("US/Eastern")
    if now is None:
        now = datetime.now(et)
    else:
        now = now.astimezone(et)

    today_et = now.date()
    # Only check for today's report (not historical runs)
    if report_date != today_et:
        return False
    # Skip weekends
    if today_et.weekday() >= 5:
        return False
    return now.time() >= NIGHTLY_DEADLINE_ET


def _check_missing_run(report_date: date, con, *, now: datetime | None = None) -> str | None:
    """Return a message if nightly run is missing past deadline, else None.

    Checks for a ``format='nightly'`` row in ``daily_report_outputs``,
    which is stamped only by the real nightly pipeline (not shadow runs
    or standalone ``generate_report()`` calls).
    """
    if not _is_past_deadline(report_date, now=now):
        return None

    row = query_one(
        """SELECT COUNT(*) FROM daily_report_outputs
           WHERE report_date = ? AND format = 'nightly'""",
        [report_date.isoformat()], con=con,
    )
    has_nightly = (row[0] if row else 0) > 0
    if has_nightly:
        return None

    return (
        f"Nightly report for {report_date.isoformat()} has not been generated "
        f"and it is past the {NIGHTLY_DEADLINE_ET.strftime('%H:%M')} ET deadline."
    )


def _get_alert_context(report_date: date, con, *, now: datetime | None = None) -> dict:
    """Extract alert-worthy data from the DB."""
    date_str = report_date.isoformat()

    # Trust banner
    row = query_one(
        """SELECT trust_banner FROM daily_report_outputs
           WHERE report_date = ? ORDER BY generated_at DESC LIMIT 1""",
        [date_str], con=con,
    )
    banner = row[0] if row else "UNKNOWN"

    # Actionable issues (ERROR/CRITICAL only)
    issues_df = query_df(
        """SELECT check_type, entity_type, severity, message,
                  classification, entity_id
           FROM reconciliation_items
           WHERE status = 'UNRESOLVED'
             AND severity IN ('ERROR', 'CRITICAL')
           ORDER BY
             CASE severity WHEN 'CRITICAL' THEN 1 ELSE 2 END,
             opened_at DESC
           LIMIT 10""",
        con=con,
    )

    # Stale sources
    stale_df = query_df(
        """SELECT source_name, last_success_at, sla_minutes, error_message
           FROM source_freshness
           WHERE is_stale = true""",
        con=con,
    )

    # Missing run
    missing_run = _check_missing_run(report_date, con, now=now)

    return {
        "banner": banner,
        "issues": issues_df,
        "stale": stale_df,
        "missing_run": missing_run,
        "date_str": date_str,
    }


def _should_alert(ctx: dict) -> bool:
    """Decide whether to send an email."""
    return (
        len(ctx["issues"]) > 0
        or len(ctx["stale"]) > 0
        or ctx.get("missing_run") is not None
    )


def _fingerprint(ctx: dict) -> str:
    """Deterministic fingerprint for dedup: date + banner + issue IDs + stale sources + missing run."""
    parts = [ctx["date_str"], ctx["banner"]]
    for _, r in ctx["issues"].iterrows():
        parts.append(f"{r['severity']}:{r['check_type']}:{r['entity_id']}")
    for _, r in ctx["stale"].iterrows():
        parts.append(f"stale:{r['source_name']}")
    if ctx.get("missing_run"):
        parts.append("missing_run")
    raw = "|".join(str(p) for p in parts)
    return hashlib.sha256(raw.encode()).hexdigest()[:12]


# ---------------------------------------------------------------------------
# Email formatting
# ---------------------------------------------------------------------------

def _build_subject(ctx: dict) -> str:
    """Build a compact email subject line."""
    parts = [f"[Gamma] {ctx['banner']}"]

    n_issues = len(ctx["issues"])
    n_stale = len(ctx["stale"])

    if n_issues > 0:
        crits = len(ctx["issues"][ctx["issues"]["severity"] == "CRITICAL"])
        if crits > 0:
            parts.append(f"{crits} CRITICAL")
        errors = n_issues - crits
        if errors > 0:
            parts.append(f"{errors} ERROR")

    if n_stale > 0:
        parts.append(f"{n_stale} stale source(s)")

    if ctx.get("missing_run"):
        parts.append("MISSING RUN")

    return " | ".join(parts) + f" — {ctx['date_str']}"


def _build_body(ctx: dict) -> str:
    """Build plain-text email body."""
    lines = [
        f"Gamma Portfolio Report — {ctx['date_str']}",
        f"Trust Banner: {ctx['banner']}",
        "",
    ]

    # Issues
    n_issues = len(ctx["issues"])
    if n_issues > 0:
        lines.append(f"--- Actionable Issues ({n_issues}) ---")
        for _, r in ctx["issues"].iterrows():
            cls = r.get("classification") or ""
            cls_tag = f" [{cls}]" if cls else ""
            lines.append(
                f"  [{r['severity']}]{cls_tag} {r['check_type']}/{r['entity_type']}: "
                f"{r['message']}"
            )
        lines.append("")

    # Stale sources
    n_stale = len(ctx["stale"])
    if n_stale > 0:
        lines.append(f"--- Stale Sources ({n_stale}) ---")
        for _, r in ctx["stale"].iterrows():
            err = r.get("error_message") or ""
            err_tag = f" ({err})" if err else ""
            lines.append(
                f"  {r['source_name']}: last_success={r['last_success_at']}, "
                f"SLA={r['sla_minutes']}m{err_tag}"
            )
        lines.append("")

    # Missing run
    if ctx.get("missing_run"):
        lines.append("--- Missing Run ---")
        lines.append(f"  {ctx['missing_run']}")
        lines.append("")

    lines.append("Full report: reporting/data/reports/daily_{}.md".format(ctx["date_str"]))
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Sender
# ---------------------------------------------------------------------------

_LAST_FINGERPRINT: str | None = None


def send_alert_if_needed(
    report_date: date,
    con=None,
    *,
    force: bool = False,
    now: datetime | None = None,
) -> dict:
    """Check for alertable conditions and send email if needed.

    Args:
        report_date: The report date to check.
        con: DuckDB connection.
        force: Send even if fingerprint matches last send (for testing).
        now: Override current time (for testing missing-run detection).

    Returns:
        {"sent": True, "subject": str, "fingerprint": str}
        or {"skipped": str}
    """
    global _LAST_FINGERPRINT

    if con is None:
        from reporting.db import get_connection, init_schema
        con = get_connection()
        init_schema(con)

    ctx = _get_alert_context(report_date, con, now=now)

    if not _should_alert(ctx):
        return {"skipped": "no actionable issues"}

    fp = _fingerprint(ctx)
    if not force and _LAST_FINGERPRINT == fp:
        return {"skipped": f"duplicate fingerprint {fp}"}

    # Check SMTP credentials
    smtp_user = (os.environ.get("SMTP_USER") or "").strip()
    smtp_pass = (os.environ.get("SMTP_PASS") or "").strip()
    smtp_to = (os.environ.get("SMTP_TO") or "").strip() or smtp_user
    smtp_host = (os.environ.get("SMTP_HOST") or "smtp.gmail.com").strip()
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))

    if not smtp_user or not smtp_pass:
        return {"skipped": "SMTP_USER or SMTP_PASS not set"}

    subject = _build_subject(ctx)
    body = _build_body(ctx)

    msg = EmailMessage()
    msg["From"] = smtp_user
    msg["To"] = smtp_to
    msg["Subject"] = subject
    msg.set_content(body)

    with smtplib.SMTP(smtp_host, smtp_port, timeout=20) as s:
        s.starttls()
        s.login(smtp_user, smtp_pass)
        s.send_message(msg)

    _LAST_FINGERPRINT = fp
    return {"sent": True, "subject": subject, "fingerprint": fp}
