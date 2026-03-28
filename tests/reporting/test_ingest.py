"""Tests for reporting.ingest — event ingest and materialization."""

import json
import os
from datetime import date

import pytest

from reporting.db import close_all, init_schema, query_df, query_one
from reporting.events import EventWriter
from reporting.ingest import ingest_events


@pytest.fixture
def fresh_db(tmp_path):
    """Provide a fresh in-memory DuckDB + temp event dir."""
    import duckdb

    os.environ["GAMMA_EVENT_DIR"] = str(tmp_path / "events")
    con = duckdb.connect(":memory:")
    init_schema(con)
    yield con, tmp_path / "events"
    con.close()
    os.environ.pop("GAMMA_EVENT_DIR", None)


class TestIngestEvents:
    def test_ingest_empty_dir(self, fresh_db):
        con, _ = fresh_db
        stats = ingest_events("2026-03-12", con=con)
        assert stats["files"] == 0
        assert stats["events_read"] == 0

    def test_ingest_strategy_run(self, fresh_db):
        con, event_dir = fresh_db
        td = date(2026, 3, 12)

        # Write events
        with EventWriter("butterfly", "schwab", trade_date=td) as ew:
            ew.strategy_run(signal="BUY", config="4DTE_D20P", reason="VIX1D=15")

        stats = ingest_events(td, con=con)
        assert stats["inserted"] == 1
        assert stats["materialized"] == 1

        # Verify strategy_runs table
        row = query_one("SELECT signal, config, reason FROM strategy_runs", con=con)
        assert row[0] == "BUY"
        assert row[1] == "4DTE_D20P"
        assert row[2] == "VIX1D=15"

    def test_ingest_full_trade_lifecycle(self, fresh_db):
        con, event_dir = fresh_db
        td = date(2026, 3, 12)

        with EventWriter("butterfly", "schwab", trade_date=td) as ew:
            ew.strategy_run(signal="BUY", config="4DTE", reason="test")
            ew.trade_intent(
                side="DEBIT", direction="LONG",
                legs=[{"osi": "SPXW260316C06000000", "strike": 6000, "option_type": "CALL", "action": "BUY_TO_OPEN", "qty": 1}],
                target_qty=1, limit_price=2.50,
            )
            ew.order_submitted(order_id="99999", legs=[], limit_price=2.50)
            ew.fill(order_id="99999", fill_qty=1, fill_price=2.45)

        stats = ingest_events(td, con=con)
        assert stats["inserted"] == 4
        assert stats["materialized"] == 4

        # Verify all tables populated
        assert query_one("SELECT COUNT(*) FROM raw_events", con=con)[0] == 4
        assert query_one("SELECT COUNT(*) FROM strategy_runs", con=con)[0] == 1
        assert query_one("SELECT COUNT(*) FROM intended_trades", con=con)[0] == 1
        assert query_one("SELECT COUNT(*) FROM order_events", con=con)[0] == 1
        assert query_one("SELECT COUNT(*) FROM fills", con=con)[0] == 1

        # Fill should mark intent as FILLED
        intent = query_one("SELECT outcome FROM intended_trades", con=con)
        assert intent[0] == "FILLED"

    def test_idempotent_reingest(self, fresh_db):
        con, event_dir = fresh_db
        td = date(2026, 3, 12)

        with EventWriter("butterfly", "schwab", trade_date=td) as ew:
            ew.strategy_run(signal="SKIP", config="test", reason="test")

        stats1 = ingest_events(td, con=con)
        assert stats1["inserted"] == 1

        # Re-ingest same file
        stats2 = ingest_events(td, con=con)
        assert stats2["duplicates"] == 1
        assert stats2["inserted"] == 0

        # Still only one row
        assert query_one("SELECT COUNT(*) FROM raw_events", con=con)[0] == 1

    def test_skip_materializes_status(self, fresh_db):
        con, event_dir = fresh_db
        td = date(2026, 3, 12)

        with EventWriter("butterfly", "schwab", trade_date=td) as ew:
            ew.strategy_run(signal="SKIP", config="test", reason="VIX1D veto")
            ew.skip(reason="VIX1D too low", signal="SKIP")

        ingest_events(td, con=con)

        row = query_one("SELECT status FROM strategy_runs", con=con)
        assert row[0] == "SKIPPED"

    def test_error_materializes_status(self, fresh_db):
        con, event_dir = fresh_db
        td = date(2026, 3, 12)

        with EventWriter("constantstable", "tt-ira", trade_date=td) as ew:
            ew.strategy_run(signal="BUY", config="test", reason="test")
            ew.error(message="API timeout", stage="chain_fetch")

        ingest_events(td, con=con)

        row = query_one("SELECT status FROM strategy_runs", con=con)
        assert row[0] == "ERROR"

    def test_multiple_writers_same_date(self, fresh_db):
        con, event_dir = fresh_db
        td = date(2026, 3, 12)

        # BF run
        with EventWriter("butterfly", "schwab", trade_date=td) as ew:
            ew.strategy_run(signal="BUY", config="4DTE", reason="ok")

        # CS run
        with EventWriter("constantstable", "tt-ira", trade_date=td) as ew:
            ew.strategy_run(signal="PUT_CREDIT", config="standard", reason="ok")

        stats = ingest_events(td, con=con)
        assert stats["files"] == 2
        assert stats["inserted"] == 2

        assert query_one("SELECT COUNT(*) FROM strategy_runs", con=con)[0] == 2
