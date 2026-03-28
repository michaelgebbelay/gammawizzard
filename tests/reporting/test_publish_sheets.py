"""Tests for reporting.publish_sheets (data extraction only — no Sheets API calls)."""

from __future__ import annotations

from datetime import date

import duckdb
import pytest

from reporting.db import init_schema, execute, query_one
from reporting.publish_sheets import (
    _extract_summary_row,
    _extract_positions,
)


@pytest.fixture
def db():
    con = duckdb.connect(":memory:")
    init_schema(con)
    yield con
    con.close()


def _seed_position(con, *, pid="p1", strategy="constantstable", account="schwab",
                   trade_date="2026-03-13", expiry_date="2026-03-14",
                   state="OPEN", entry_price=1.20, qty=1, signal="BUY",
                   config="TEST", closed_at=None, closure_reason=None,
                   realized_pnl=None):
    execute(
        """INSERT INTO positions
           (position_id, strategy, account, trade_date, expiry_date,
            lifecycle_state, entry_price, qty, signal, config,
            closed_at, closure_reason, realized_pnl)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        [pid, strategy, account, trade_date, expiry_date,
         state, entry_price, qty, signal, config,
         closed_at, closure_reason, realized_pnl],
        con=con,
    )


def _seed_report_output(con, report_date="2026-03-13", banner="GREEN"):
    execute(
        """INSERT INTO daily_report_outputs
           (id, report_date, format, content, trust_banner)
           VALUES ('r1', ?, 'markdown', 'test', ?)""",
        [report_date, banner],
        con=con,
    )


class TestExtractSummaryRow:
    def test_empty_db(self, db):
        row = _extract_summary_row(date(2026, 3, 13), db)
        assert row[0] == "2026-03-13"
        assert row[1] == "?"  # no report output → unknown banner
        assert row[2] == 0    # opened
        assert row[3] == 0    # closed
        assert row[4] == 0    # open

    def test_with_positions(self, db):
        _seed_report_output(db, banner="YELLOW")
        _seed_position(db, pid="p1", state="OPEN", trade_date="2026-03-13")
        _seed_position(db, pid="p2", state="OPEN", trade_date="2026-03-13")
        _seed_position(db, pid="p3", state="CLOSED", trade_date="2026-03-12",
                       closed_at="2026-03-13 20:00:00", closure_reason="EXPIRY",
                       realized_pnl=50.0)

        row = _extract_summary_row(date(2026, 3, 13), db)
        assert row[0] == "2026-03-13"
        assert row[1] == "YELLOW"
        assert row[2] == 2    # opened today
        assert row[3] == 1    # closed today
        assert row[4] == 2    # still open
        assert row[5] == 50.0 # realized P&L

    def test_stale_sources_counted(self, db):
        _seed_report_output(db, banner="RED")
        execute(
            """INSERT INTO source_freshness
               (source_name, sla_minutes, is_stale)
               VALUES ('schwab_orders', 60, true)""",
            con=db,
        )
        row = _extract_summary_row(date(2026, 3, 13), db)
        assert row[8] == 1  # stale count


class TestExtractPositions:
    def test_open_positions_only(self, db):
        _seed_position(db, pid="p1", state="OPEN")
        _seed_position(db, pid="p2", state="CLOSED")
        _seed_position(db, pid="p3", state="PARTIALLY_OPEN")

        rows = _extract_positions(db)
        assert len(rows) == 2  # p1 and p3 only
        strategies = {r[0] for r in rows}
        assert "constantstable" in strategies

    def test_empty_db(self, db):
        rows = _extract_positions(db)
        assert rows == []
