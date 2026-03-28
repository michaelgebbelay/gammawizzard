"""Tests for reporting.alerts_email (logic only — no SMTP calls)."""

from __future__ import annotations

from datetime import date, datetime, timezone

import duckdb
import pytest

from reporting.db import init_schema, execute
from reporting.alerts_email import (
    _build_body,
    _build_subject,
    _check_missing_run,
    _fingerprint,
    _get_alert_context,
    _is_past_deadline,
    _should_alert,
    send_alert_if_needed,
)


@pytest.fixture
def db():
    con = duckdb.connect(":memory:")
    init_schema(con)
    yield con
    con.close()


def _seed_report(con, report_date="2026-03-13", banner="RED"):
    execute(
        """INSERT INTO daily_report_outputs
           (id, report_date, format, content, trust_banner)
           VALUES ('r1', ?, 'markdown', 'test', ?)""",
        [report_date, banner],
        con=con,
    )


def _seed_nightly_stamp(con, report_date="2026-03-13"):
    """Insert the nightly completion stamp (format='nightly')."""
    execute(
        """INSERT INTO daily_report_outputs
           (id, report_date, format, content, trust_banner)
           VALUES ('n1', ?, 'nightly', '', '')""",
        [report_date],
        con=con,
    )


def _seed_issue(con, *, iid="i1", recon_run_id="rr1", severity="ERROR",
                check_type="fill_match", entity_type="order",
                message="test issue", entity_id="e1"):
    # Ensure recon run exists
    try:
        execute(
            """INSERT INTO reconciliation_runs
               (id, run_date, started_at, status)
               VALUES (?, '2026-03-13', '2026-03-13 20:00:00', 'COMPLETED')""",
            [recon_run_id], con=con,
        )
    except Exception:
        pass  # already exists
    execute(
        """INSERT INTO reconciliation_items
           (id, recon_run_id, check_type, entity_type, severity, status, message, entity_id)
           VALUES (?, ?, ?, ?, ?, 'UNRESOLVED', ?, ?)""",
        [iid, recon_run_id, check_type, entity_type, severity, message, entity_id],
        con=con,
    )


def _seed_stale(con, source="schwab_orders"):
    execute(
        """INSERT INTO source_freshness
           (source_name, sla_minutes, is_stale)
           VALUES (?, 60, true)""",
        [source], con=con,
    )


class TestShouldAlert:
    def test_no_issues_no_stale(self, db):
        ctx = _get_alert_context(date(2026, 3, 13), db)
        assert not _should_alert(ctx)

    def test_error_issues_trigger(self, db):
        _seed_issue(db, severity="ERROR")
        ctx = _get_alert_context(date(2026, 3, 13), db)
        assert _should_alert(ctx)

    def test_critical_issues_trigger(self, db):
        _seed_issue(db, severity="CRITICAL")
        ctx = _get_alert_context(date(2026, 3, 13), db)
        assert _should_alert(ctx)

    def test_warning_does_not_trigger(self, db):
        """WARNING-only issues don't fire alerts in v1."""
        _seed_issue(db, severity="WARNING")
        ctx = _get_alert_context(date(2026, 3, 13), db)
        assert not _should_alert(ctx)

    def test_stale_triggers(self, db):
        _seed_stale(db)
        ctx = _get_alert_context(date(2026, 3, 13), db)
        assert _should_alert(ctx)

    def test_stale_alone_triggers(self, db):
        """Stale broker sync triggers alert even with zero issues."""
        _seed_stale(db)
        ctx = _get_alert_context(date(2026, 3, 13), db)
        assert _should_alert(ctx)
        assert len(ctx["issues"]) == 0
        assert len(ctx["stale"]) == 1


class TestEmailFormatting:
    def test_subject_includes_severity_counts(self, db):
        _seed_report(db, banner="RED")
        _seed_issue(db, iid="i1", severity="CRITICAL")
        _seed_issue(db, iid="i2", severity="ERROR", entity_id="e2")
        ctx = _get_alert_context(date(2026, 3, 13), db)
        subject = _build_subject(ctx)
        assert "1 CRITICAL" in subject
        assert "1 ERROR" in subject
        assert "2026-03-13" in subject

    def test_subject_stale_only(self, db):
        _seed_report(db, banner="RED")
        _seed_stale(db)
        ctx = _get_alert_context(date(2026, 3, 13), db)
        subject = _build_subject(ctx)
        assert "stale" in subject.lower()

    def test_body_includes_issue_details(self, db):
        _seed_issue(db, message="missing fill for order X")
        ctx = _get_alert_context(date(2026, 3, 13), db)
        body = _build_body(ctx)
        assert "missing fill for order X" in body

    def test_body_includes_stale_source(self, db):
        _seed_stale(db, source="schwab_positions")
        ctx = _get_alert_context(date(2026, 3, 13), db)
        body = _build_body(ctx)
        assert "schwab_positions" in body


class TestFingerprint:
    def test_deterministic(self, db):
        _seed_issue(db)
        ctx = _get_alert_context(date(2026, 3, 13), db)
        assert _fingerprint(ctx) == _fingerprint(ctx)

    def test_different_issues_different_fp(self, db):
        _seed_issue(db, iid="i1", entity_id="e1")
        ctx1 = _get_alert_context(date(2026, 3, 13), db)
        fp1 = _fingerprint(ctx1)

        _seed_issue(db, iid="i2", entity_id="e2")
        ctx2 = _get_alert_context(date(2026, 3, 13), db)
        fp2 = _fingerprint(ctx2)

        assert fp1 != fp2


class TestMissingRun:
    """Missing-run detection: alert when nightly report hasn't been generated past deadline."""

    def _et(self, hour, minute, *, year=2026, month=3, day=13):
        """Build an ET-aware datetime for a weekday (2026-03-13 is a Friday)."""
        from zoneinfo import ZoneInfo
        return datetime(year, month, day, hour, minute, tzinfo=ZoneInfo("US/Eastern"))

    def test_before_deadline_no_alert(self, db):
        # 4:00 PM ET, before 4:45 PM deadline
        now = self._et(16, 0)
        assert not _is_past_deadline(date(2026, 3, 13), now=now)

    def test_after_deadline_no_report_alerts(self, db):
        # 5:00 PM ET, after 4:45 PM deadline, no report row
        now = self._et(17, 0)
        assert _is_past_deadline(date(2026, 3, 13), now=now)
        msg = _check_missing_run(date(2026, 3, 13), db, now=now)
        assert msg is not None
        assert "not been generated" in msg

    def test_after_deadline_with_nightly_stamp_no_alert(self, db):
        # Nightly stamp exists → no missing-run alert
        _seed_nightly_stamp(db, report_date="2026-03-13")
        now = self._et(17, 0)
        msg = _check_missing_run(date(2026, 3, 13), db, now=now)
        assert msg is None

    def test_after_deadline_with_only_markdown_still_alerts(self, db):
        # Shadow/manual run wrote format='markdown' but nightly never ran
        _seed_report(db, report_date="2026-03-13")
        now = self._et(17, 0)
        msg = _check_missing_run(date(2026, 3, 13), db, now=now)
        assert msg is not None
        assert "not been generated" in msg

    def test_weekend_no_alert(self, db):
        # 2026-03-14 is Saturday
        now = self._et(17, 0, day=14)
        assert not _is_past_deadline(date(2026, 3, 14), now=now)

    def test_historical_date_no_alert(self, db):
        # Checking yesterday's report from today — should not fire
        now = self._et(17, 0)
        assert not _is_past_deadline(date(2026, 3, 12), now=now)

    def test_missing_run_triggers_should_alert(self, db):
        now = self._et(17, 0)
        ctx = _get_alert_context(date(2026, 3, 13), db, now=now)
        assert _should_alert(ctx)
        assert ctx["missing_run"] is not None

    def test_missing_run_in_subject(self, db):
        now = self._et(17, 0)
        ctx = _get_alert_context(date(2026, 3, 13), db, now=now)
        subject = _build_subject(ctx)
        assert "MISSING RUN" in subject

    def test_missing_run_in_body(self, db):
        now = self._et(17, 0)
        ctx = _get_alert_context(date(2026, 3, 13), db, now=now)
        body = _build_body(ctx)
        assert "Missing Run" in body
        assert "not been generated" in body

    def test_missing_run_changes_fingerprint(self, db):
        """Missing-run changes the fingerprint vs no-missing-run."""
        ctx_no_miss = _get_alert_context(date(2026, 3, 13), db)
        ctx_no_miss["missing_run"] = None  # ensure no missing run

        ctx_miss = dict(ctx_no_miss)
        ctx_miss["missing_run"] = "report missing"

        assert _fingerprint(ctx_no_miss) != _fingerprint(ctx_miss)


class TestSendAlertIfNeeded:
    def test_skips_when_no_issues(self, db):
        result = send_alert_if_needed(date(2026, 3, 13), con=db)
        assert result["skipped"] == "no actionable issues"

    def test_skips_when_no_smtp_creds(self, db, monkeypatch):
        _seed_issue(db)
        monkeypatch.delenv("SMTP_USER", raising=False)
        monkeypatch.delenv("SMTP_PASS", raising=False)
        result = send_alert_if_needed(date(2026, 3, 13), con=db)
        assert "SMTP" in result["skipped"]

    def test_missing_run_triggers_without_smtp(self, db, monkeypatch):
        """Missing run should attempt alert but skip if no SMTP creds."""
        from zoneinfo import ZoneInfo
        now = datetime(2026, 3, 13, 17, 0, tzinfo=ZoneInfo("US/Eastern"))
        monkeypatch.delenv("SMTP_USER", raising=False)
        monkeypatch.delenv("SMTP_PASS", raising=False)
        result = send_alert_if_needed(date(2026, 3, 13), con=db, now=now)
        assert "SMTP" in result["skipped"]
