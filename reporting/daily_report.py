"""Daily report generator.

Reads from trusted views in the canonical DuckDB store and produces a
single Markdown report answering:
  - What opened today?
  - What closed today?
  - What is still open?
  - Cash, net liq, buying power by account
  - Realized vs unrealized P&L
  - Drawdown
  - Strategy drift / anomalies
  - Unresolved reconciliation issues

Usage:
    from reporting.daily_report import generate_report

    md = generate_report("2026-03-12")
    print(md)
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone

import pandas as pd

from reporting.db import execute, get_connection, init_schema, query_df, query_one
from reporting.trade_audit import render_trigger_audit_section


def _v(val, default=""):
    """Safely extract a pandas value, replacing NA/NaT/None with default."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return default
    try:
        if pd.isna(val):
            return default
    except (TypeError, ValueError):
        pass
    return val


# ---------------------------------------------------------------------------
# Report sections
# ---------------------------------------------------------------------------

def _section_header(report_date: date) -> str:
    weekday = report_date.strftime("%A")
    return f"# Daily Portfolio Report — {report_date.isoformat()} ({weekday})\n"


def _section_trust_banner(con, report_date: str) -> str:
    """Overall trust status banner.

    Only WARNING/ERROR/CRITICAL unresolved items affect the banner.
    INFO items (e.g. likely discretionary positions) are tracked but
    don't degrade trust.
    """
    # Count actionable unresolved issues (WARNING+)
    row = query_one(
        """SELECT COUNT(*) FROM reconciliation_items
           WHERE status = 'UNRESOLVED'
             AND severity IN ('WARNING', 'ERROR', 'CRITICAL')""",
        con=con,
    )
    actionable = row[0] if row else 0

    # Count INFO-only items separately for display
    info_row = query_one(
        """SELECT COUNT(*) FROM reconciliation_items
           WHERE status = 'UNRESOLVED' AND severity = 'INFO'""",
        con=con,
    )
    info_count = info_row[0] if info_row else 0

    # Check source freshness
    stale_row = query_one(
        "SELECT COUNT(*) FROM source_freshness WHERE is_stale = true",
        con=con,
    )
    stale_sources = stale_row[0] if stale_row else 0

    if actionable == 0 and stale_sources == 0:
        banner = "GREEN"
        detail = "All sources fresh, no actionable issues."
        if info_count > 0:
            detail += f" ({info_count} informational item(s) noted.)"
    elif actionable > 0 and stale_sources == 0:
        banner = "YELLOW"
        detail = f"{actionable} unresolved reconciliation item(s)."
        if info_count > 0:
            detail += f" ({info_count} informational.)"
    else:
        banner = "RED"
        parts = []
        if stale_sources > 0:
            parts.append(f"{stale_sources} stale source(s)")
        if actionable > 0:
            parts.append(f"{actionable} unresolved issue(s)")
        detail = "; ".join(parts) + "."
        if info_count > 0:
            detail += f" ({info_count} informational.)"

    return f"## Trust Status: **{banner}**\n{detail}\n"


def _section_opened_today(con, report_date: str) -> str:
    """Positions opened today."""
    df = query_df(
        """SELECT strategy, account, position_id, signal, config,
                  entry_price, qty, expiry_date
           FROM positions
           WHERE trade_date = ?
           ORDER BY strategy, account""",
        [report_date], con=con,
    )

    if df.empty:
        return "## Opened Today\nNo new positions.\n"

    lines = ["## Opened Today", f"{len(df)} position(s):\n"]
    lines.append("| Strategy | Account | Signal | Config | Entry | Qty | Expiry |")
    lines.append("|----------|---------|--------|--------|------:|----:|--------|")

    for _, r in df.iterrows():
        ep = _v(r['entry_price'], 0)
        entry = f"${ep:.2f}" if ep else "—"
        lines.append(
            f"| {r['strategy']} | {r['account']} | {_v(r['signal'], '—')} "
            f"| {_v(r['config'], '—')} | {entry} | {_v(r['qty'], 0)} | {_v(r['expiry_date'], '—')} |"
        )

    return "\n".join(lines) + "\n"


def _section_closed_today(con, report_date: str) -> str:
    """Positions closed today."""
    df = query_df(
        """SELECT strategy, account, position_id, closure_reason,
                  entry_price, exit_price, realized_pnl, qty
           FROM positions
           WHERE closed_at::DATE = ?
             AND lifecycle_state IN ('CLOSED', 'EXPIRED', 'ASSIGNED')
           ORDER BY strategy, account""",
        [report_date], con=con,
    )

    if df.empty:
        return "## Closed Today\nNo closures.\n"

    lines = ["## Closed Today", f"{len(df)} position(s):\n"]
    lines.append("| Strategy | Account | Reason | Entry | Exit | P&L | Qty |")
    lines.append("|----------|---------|--------|------:|-----:|----:|----:|")

    total_pnl = 0.0
    for _, r in df.iterrows():
        ep = _v(r['entry_price'], 0)
        entry = f"${ep:.2f}" if ep else "—"
        xp = _v(r['exit_price'], 0)
        exit_ = f"${xp:.2f}" if xp else "—"
        pnl = float(_v(r["realized_pnl"], 0))
        total_pnl += pnl
        pnl_str = f"${pnl:+.2f}"
        lines.append(
            f"| {r['strategy']} | {r['account']} | {_v(r['closure_reason'], '—')} "
            f"| {entry} | {exit_} | {pnl_str} | {_v(r['qty'], 0)} |"
        )

    lines.append(f"\n**Total realized P&L today: ${total_pnl:+.2f}**")
    return "\n".join(lines) + "\n"


def _section_open_positions(con) -> str:
    """Currently open positions."""
    df = query_df(
        """SELECT strategy, account, position_id, trade_date, expiry_date,
                  lifecycle_state, entry_price, qty, signal, config
           FROM positions
           WHERE lifecycle_state IN ('OPEN', 'PARTIALLY_OPEN', 'PARTIALLY_CLOSED')
           ORDER BY expiry_date, strategy""",
        con=con,
    )

    if df.empty:
        return "## Open Positions\nNo open positions.\n"

    lines = ["## Open Positions", f"{len(df)} position(s):\n"]
    lines.append("| Strategy | Account | Trade Date | Expiry | Entry | Qty | State |")
    lines.append("|----------|---------|------------|--------|------:|----:|-------|")

    for _, r in df.iterrows():
        ep = _v(r['entry_price'], 0)
        entry = f"${ep:.2f}" if ep else "—"
        lines.append(
            f"| {r['strategy']} | {r['account']} | {r['trade_date']} "
            f"| {_v(r['expiry_date'], '—')} | {entry} | {_v(r['qty'], 0)} | {r['lifecycle_state']} |"
        )

    return "\n".join(lines) + "\n"


def _section_account_snapshot(con, report_date: str) -> str:
    """Account snapshots."""
    df = query_df(
        """SELECT account, cash, net_liq, buying_power,
                  open_positions, realized_pnl_day, unrealized_pnl,
                  trust_status
           FROM account_snapshots
           WHERE snapshot_date = ?
           ORDER BY account""",
        [report_date], con=con,
    )

    if df.empty:
        return "## Account Summary\nNo account snapshots for today.\n"

    lines = ["## Account Summary\n"]
    lines.append("| Account | Cash | Net Liq | Buying Power | Open | Day P&L | Unreal | Trust |")
    lines.append("|---------|-----:|--------:|------------:|-----:|--------:|-------:|-------|")

    for _, r in df.iterrows():
        cash_val = _v(r['cash'], 0)
        cash_str = f"${cash_val:,.0f}" if cash_val else "—"
        nl_val = _v(r['net_liq'], 0)
        nl_str = f"${nl_val:,.0f}" if nl_val else "—"
        bp_val = _v(r['buying_power'], 0)
        bp_str = f"${bp_val:,.0f}" if bp_val else "—"
        open_pos = _v(r["open_positions"], 0)
        day_pnl = f"${float(_v(r['realized_pnl_day'], 0)):+,.0f}"
        unreal = f"${float(_v(r['unrealized_pnl'], 0)):+,.0f}"
        trust = _v(r["trust_status"], "?")
        lines.append(
            f"| {r['account']} | {cash_str} | {nl_str} | {bp_str} "
            f"| {open_pos} | {day_pnl} | {unreal} | {trust} |"
        )

    return "\n".join(lines) + "\n"


def _section_strategy_daily(con, report_date: str) -> str:
    """Per-strategy daily scorecard."""
    df = query_df(
        """SELECT strategy, account, trades_opened, trades_closed,
                  trades_skipped, realized_pnl, unrealized_pnl,
                  win_rate_20d, trust_status
           FROM strategy_daily
           WHERE report_date = ?
           ORDER BY strategy, account""",
        [report_date], con=con,
    )

    if df.empty:
        return "## Strategy Scorecard\nNo strategy data for today.\n"

    lines = ["## Strategy Scorecard\n"]
    lines.append("| Strategy | Account | Opened | Closed | Skipped | Real P&L | Unreal | WR-20d | Trust |")
    lines.append("|----------|---------|-------:|-------:|--------:|---------:|-------:|-------:|-------|")

    for _, r in df.iterrows():
        wr_val = _v(r['win_rate_20d'], None)
        wr = f"{wr_val:.0%}" if wr_val is not None else "—"
        lines.append(
            f"| {r['strategy']} | {r['account']} "
            f"| {_v(r['trades_opened'], 0)} | {_v(r['trades_closed'], 0)} | {_v(r['trades_skipped'], 0)} "
            f"| ${float(_v(r['realized_pnl'], 0)):+,.0f} | ${float(_v(r['unrealized_pnl'], 0)):+,.0f} "
            f"| {wr} | {_v(r['trust_status'], '?')} |"
        )

    return "\n".join(lines) + "\n"


def _section_recon_issues(con) -> str:
    """Unresolved reconciliation issues with classification breakdown."""
    df = query_df(
        """SELECT check_type, entity_type, severity, message,
                  internal_value, broker_value, classification, opened_at
           FROM reconciliation_items
           WHERE status = 'UNRESOLVED'
           ORDER BY
             CASE severity
               WHEN 'CRITICAL' THEN 1
               WHEN 'ERROR' THEN 2
               WHEN 'WARNING' THEN 3
               ELSE 4
             END,
             opened_at DESC
           LIMIT 20""",
        con=con,
    )

    if df.empty:
        return "## Reconciliation\nNo unresolved issues.\n"

    # Classification breakdown
    api_unmatched = len(df[df["classification"] == "api_unmatched"])
    non_api = len(df[df["classification"] == "non_api"])
    unknown_src = len(df[df["classification"] == "unknown_source"])
    other_actionable = len(df[
        df["severity"].isin(["WARNING", "ERROR", "CRITICAL"])
        & (df["classification"].isna() | ~df["classification"].isin(["api_unmatched"]))
    ]) if api_unmatched == 0 else len(df[
        df["severity"].isin(["WARNING", "ERROR", "CRITICAL"])
        & df["classification"].isna()
    ])

    lines = ["## Reconciliation", f"{len(df)} unresolved issue(s):"]
    parts = []
    if api_unmatched > 0:
        parts.append(f"{api_unmatched} API-tagged unmatched (pipeline gap)")
    if non_api > 0:
        parts.append(f"{non_api} non-API (manual/discretionary)")
    if unknown_src > 0:
        parts.append(f"{unknown_src} unknown source (legacy)")
    # Count non-position actionable items (fill_match, cash_match, freshness)
    other = len(df[
        df["severity"].isin(["WARNING", "ERROR", "CRITICAL"])
        & (df["classification"].isna())
    ])
    if other > 0:
        parts.append(f"{other} other actionable")
    if parts:
        lines.append(f"Breakdown: {', '.join(parts)}\n")
    else:
        lines.append("")

    for _, r in df.iterrows():
        sev = r["severity"]
        cls = _v(r.get("classification"), "")
        cls_tag = f" [{cls}]" if cls else ""
        lines.append(f"- **[{sev}]{cls_tag}** {r['check_type']}/{r['entity_type']}: {r['message']}")
        if _v(r["internal_value"]) or _v(r["broker_value"]):
            lines.append(f"  Internal: `{_v(r['internal_value'], '—')}` | Broker: `{_v(r['broker_value'], '—')}`")

    return "\n".join(lines) + "\n"


def _section_runs_today(con, report_date: str) -> str:
    """Strategy runs summary for today."""
    df = query_df(
        """SELECT strategy, account, signal, status, reason
           FROM strategy_runs
           WHERE trade_date = ?
           ORDER BY strategy, account""",
        [report_date], con=con,
    )

    if df.empty:
        return "## Strategy Runs\nNo runs today.\n"

    lines = ["## Strategy Runs\n"]
    for _, r in df.iterrows():
        status_icon = {"COMPLETED": "+", "SKIPPED": "~", "ERROR": "!", "RUNNING": "?"}
        icon = status_icon.get(r["status"], "?")
        lines.append(
            f"- [{icon}] **{r['strategy']}** ({r['account']}): "
            f"{_v(r['signal'], '—')} → {r['status']} — {_v(r['reason'], '')}"
        )

    return "\n".join(lines) + "\n"


def _stale_broker_sources(con) -> list[str]:
    """Return stale broker-derived sources that would make position tables unreliable."""
    df = query_df(
        """SELECT source_name
           FROM source_freshness
           WHERE is_stale = true
             AND (
               source_name LIKE '%_orders'
               OR source_name LIKE '%_positions'
               OR source_name LIKE '%_cash'
             )
           ORDER BY source_name""",
        con=con,
    )
    if df.empty:
        return []
    return [str(v) for v in df["source_name"].tolist()]


def _section_broker_data_notice(stale_sources: list[str]) -> str:
    """Warn when broker-derived position tables are stale enough to be misleading."""
    joined = ", ".join(stale_sources)
    lines = [
        "## Broker Data Freshness",
        "Broker-derived sections were suppressed because the latest sync is stale.",
        f"Stale sources: {joined}.",
        "Suppressed sections: Opened Today, Closed Today, Open Positions, Account Summary, Strategy Scorecard.",
    ]
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Report generator
# ---------------------------------------------------------------------------

def generate_report(
    report_date: date | str | None = None,
    con=None,
) -> str:
    """Generate the full daily portfolio report as Markdown.

    Returns the report string.
    """
    if con is None:
        con = get_connection()
        init_schema(con)

    if report_date is None:
        report_date = date.today()
    if isinstance(report_date, str):
        report_date = date.fromisoformat(report_date)

    date_str = report_date.isoformat()
    stale_broker_sources = _stale_broker_sources(con)

    sections = [
        _section_header(report_date),
        _section_trust_banner(con, date_str),
        _section_runs_today(con, date_str),
        render_trigger_audit_section(report_date, con=con),
        _section_recon_issues(con),
    ]
    if stale_broker_sources:
        sections.insert(4, _section_broker_data_notice(stale_broker_sources))
    else:
        sections[4:4] = [
            _section_opened_today(con, date_str),
            _section_closed_today(con, date_str),
            _section_open_positions(con),
            _section_account_snapshot(con, date_str),
            _section_strategy_daily(con, date_str),
        ]

    report = "\n".join(sections)

    # Compute trust banner for storage (severity-aware: only WARNING+ affects banner)
    _actionable = query_one(
        """SELECT COUNT(*) FROM reconciliation_items
           WHERE status = 'UNRESOLVED'
             AND severity IN ('WARNING', 'ERROR', 'CRITICAL')""",
        con=con,
    )
    _stale = query_one(
        "SELECT COUNT(*) FROM source_freshness WHERE is_stale = true",
        con=con,
    )
    n_actionable = _actionable[0] if _actionable else 0
    n_stale = _stale[0] if _stale else 0
    if n_actionable == 0 and n_stale == 0:
        computed_banner = "GREEN"
    elif n_stale > 0:
        computed_banner = "RED"
    else:
        computed_banner = "YELLOW"

    # Store in daily_report_outputs
    import uuid
    execute(
        """INSERT OR REPLACE INTO daily_report_outputs
           (id, report_date, format, content, trust_banner)
           VALUES (?, ?, 'markdown', ?, ?)""",
        [
            uuid.uuid4().hex[:16],
            date_str,
            report,
            computed_banner,
        ],
        con=con,
    )

    return report


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    report_date = sys.argv[1] if len(sys.argv) > 1 else None
    print(generate_report(report_date))
