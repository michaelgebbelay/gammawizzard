"""Tests for reporting.daily_report."""

from __future__ import annotations

import duckdb
import pytest

from reporting.daily_report import generate_report
from reporting.db import execute, init_schema


@pytest.fixture
def db():
    con = duckdb.connect(":memory:")
    init_schema(con)
    yield con
    con.close()


def _seed_position(con):
    execute(
        """INSERT INTO positions
           (position_id, strategy, account, trade_date, expiry_date,
            lifecycle_state, entry_price, qty, signal, config)
           VALUES ('p1', 'constantstable', 'schwab', '2026-03-19', '2026-03-20',
                   'OPEN', 1.20, 1, 'LONG', 'TEST')""",
        con=con,
    )


def _seed_account_snapshot(con):
    execute(
        """INSERT INTO account_snapshots
           (id, account, snapshot_date, as_of, cash, net_liq, buying_power,
            open_positions, realized_pnl_day, unrealized_pnl, trust_status)
           VALUES ('a1', 'schwab', '2026-03-19', '2026-03-19 21:00:00',
                   1000, 2000, 1500, 1, 0, 10, 'TRUSTED')""",
        con=con,
    )


def _seed_strategy_daily(con):
    execute(
        """INSERT INTO strategy_daily
           (id, strategy, account, report_date, trades_opened, trades_closed,
            trades_skipped, realized_pnl, unrealized_pnl, trust_status)
           VALUES ('s1', 'constantstable', 'schwab', '2026-03-19',
                   1, 0, 0, 0, 10, 'TRUSTED')""",
        con=con,
    )


def test_generate_report_suppresses_broker_sections_when_sources_stale(db):
    _seed_position(db)
    _seed_account_snapshot(db)
    _seed_strategy_daily(db)
    execute(
        """INSERT INTO source_freshness
           (source_name, last_success_at, sla_minutes, is_stale)
           VALUES ('schwab_orders', '2026-03-19 19:00:00', 60, true)""",
        con=db,
    )

    report = generate_report("2026-03-19", con=db)

    assert "## Broker Data Freshness" in report
    assert "schwab_orders" in report
    assert "## Opened Today" not in report
    assert "## Closed Today" not in report
    assert "## Open Positions" not in report
    assert "## Account Summary" not in report
    assert "## Strategy Scorecard" not in report
    assert "## Strategy Runs" in report
    assert "## Reconciliation" in report
