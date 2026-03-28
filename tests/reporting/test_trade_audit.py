"""Tests for reporting.trade_audit."""

from __future__ import annotations

from datetime import date

import duckdb
import pytest

from reporting.daily_report import generate_report
from reporting.db import execute, init_schema
from reporting.trade_audit import get_trigger_audit


@pytest.fixture
def db():
    con = duckdb.connect(":memory:")
    init_schema(con)
    yield con
    con.close()


def _insert_run(
    con,
    *,
    run_id: str,
    strategy: str,
    account: str,
    trade_date: str,
    status: str,
    reason: str = "",
    started_at: str = "2026-03-13 20:05:00",
    completed_at: str = "2026-03-13 20:06:00",
):
    execute(
        """INSERT INTO strategy_runs
           (run_id, strategy, account, trade_date, config_version, signal, config, reason,
            status, started_at, completed_at)
           VALUES (?, ?, ?, ?, 'test-sha', 'BUY', 'test', ?, ?, ?, ?)""",
        [run_id, strategy, account, trade_date, reason, status, started_at, completed_at],
        con=con,
    )


def _insert_intent(con, *, intent_id: str, run_id: str, trade_group_id: str, strategy: str, account: str, trade_date: str, target_qty: int = 1):
    execute(
        """INSERT INTO intended_trades
           (intent_id, run_id, trade_group_id, strategy, account, trade_date,
            side, direction, legs, target_qty, limit_price, outcome)
           VALUES (?, ?, ?, ?, ?, ?, 'DEBIT', 'LONG', '[]', ?, 1.25, 'PENDING')""",
        [intent_id, run_id, trade_group_id, strategy, account, trade_date, target_qty],
        con=con,
    )


def _insert_fill(con, *, fill_id: str, run_id: str, trade_group_id: str, fill_qty: int):
    execute(
        """INSERT INTO fills
           (fill_id, trade_group_id, run_id, order_id, ts_utc, fill_qty, fill_price, legs, source)
           VALUES (?, ?, ?, 'ORD1', '2026-03-13 20:06:00', ?, 1.20, NULL, 'internal')""",
        [fill_id, trade_group_id, run_id, fill_qty],
        con=con,
    )


class TestTriggerAudit:
    def test_missing_dualside_run_is_flagged(self, db):
        frame = get_trigger_audit(date(2026, 3, 13), strategy="dualside", con=db)
        assert len(frame) == 1
        row = frame.iloc[0]
        assert row["trade_status"] == "MISSED_RUN"
        assert row["run_status"] == "NO_RUN"

    def test_skipped_run_is_reported(self, db):
        _insert_run(
            db,
            run_id="run_skip",
            strategy="dualside",
            account="schwab",
            trade_date="2026-03-13",
            status="SKIPPED",
            reason="VIX1D veto",
        )

        frame = get_trigger_audit(date(2026, 3, 13), strategy="dualside", con=db)
        row = frame.iloc[0]
        assert row["trade_status"] == "SKIPPED"
        assert "VIX1D veto" in row["note"]

    def test_partial_fill_is_reported(self, db):
        _insert_run(
            db,
            run_id="run_partial",
            strategy="dualside",
            account="schwab",
            trade_date="2026-03-13",
            status="COMPLETED",
        )
        _insert_intent(
            db,
            intent_id="intent_put",
            run_id="run_partial",
            trade_group_id="tg_put",
            strategy="dualside",
            account="schwab",
            trade_date="2026-03-13",
            target_qty=1,
        )
        _insert_intent(
            db,
            intent_id="intent_call",
            run_id="run_partial",
            trade_group_id="tg_call",
            strategy="dualside",
            account="schwab",
            trade_date="2026-03-13",
            target_qty=1,
        )
        _insert_fill(db, fill_id="fill_put", run_id="run_partial", trade_group_id="tg_put", fill_qty=1)

        frame = get_trigger_audit(date(2026, 3, 13), strategy="dualside", con=db)
        row = frame.iloc[0]
        assert row["trade_status"] == "PARTIAL_FILL"
        assert row["taken_groups"] == 1
        assert row["missed_groups"] == 1

    def test_constantstable_morning_and_evening_windows_are_distinct(self, db):
        _insert_run(
            db,
            run_id="run_morning",
            strategy="constantstable",
            account="schwab",
            trade_date="2026-03-13",
            status="SKIPPED",
            reason="NO_PENDING_PLAN",
            started_at="2026-03-13 13:35:00",
            completed_at="2026-03-13 13:35:05",
        )
        _insert_run(
            db,
            run_id="run_evening",
            strategy="constantstable",
            account="schwab",
            trade_date="2026-03-13",
            status="COMPLETED",
            started_at="2026-03-13 20:13:00",
            completed_at="2026-03-13 20:14:00",
        )
        _insert_intent(
            db,
            intent_id="intent_evening",
            run_id="run_evening",
            trade_group_id="tg_evening",
            strategy="constantstable",
            account="schwab",
            trade_date="2026-03-13",
            target_qty=1,
        )
        _insert_fill(db, fill_id="fill_evening", run_id="run_evening", trade_group_id="tg_evening", fill_qty=1)

        frame = get_trigger_audit(date(2026, 3, 13), strategy="constantstable", account="schwab", con=db)
        assert len(frame) == 2

        morning = frame.loc[frame["trigger_rule"] == "cs_morning_trigger"].iloc[0]
        evening = frame.loc[frame["trigger_rule"] == "cs_daily_trigger"].iloc[0]

        assert morning["trade_status"] == "SKIPPED"
        assert "NO_PENDING_PLAN" in morning["note"]
        assert evening["trade_status"] == "TAKEN"
        assert evening["taken_groups"] == 1

    def test_daily_report_includes_trigger_audit_section(self, db):
        report = generate_report(date(2026, 3, 13), con=db)
        assert "## Trigger Audit" in report
        assert "MISSED_RUN" in report
