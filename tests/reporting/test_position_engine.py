"""Tests for reporting.position_engine — lifecycle state machine."""

import os
from datetime import date

import pytest

from reporting.db import execute, init_schema, query_one
from reporting.position_engine import (
    VALID_TRANSITIONS,
    create_position_from_fill,
    get_open_positions,
    process_expiries,
    record_roll,
    transition_state,
)


@pytest.fixture
def db(tmp_path):
    """Provide a fresh in-memory DuckDB."""
    import duckdb

    con = duckdb.connect(":memory:")
    init_schema(con)
    yield con
    con.close()


def _insert_run(con, run_id="r1", strategy="butterfly", account="schwab",
                trade_date="2026-03-12"):
    execute(
        """INSERT INTO strategy_runs
           (run_id, strategy, account, trade_date, config_version, status, started_at)
           VALUES (?, ?, ?, ?, 'test', 'COMPLETED', current_timestamp)""",
        [run_id, strategy, account, trade_date],
        con=con,
    )


def _insert_position(con, position_id="pos1", strategy="butterfly", account="schwab",
                      trade_date="2026-03-12", expiry_date="2026-03-16",
                      state="OPEN"):
    execute(
        """INSERT INTO positions
           (position_id, strategy, account, trade_date, expiry_date,
            lifecycle_state, provenance, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, 'STRATEGY', current_timestamp)""",
        [position_id, strategy, account, trade_date, expiry_date, state],
        con=con,
    )


class TestValidTransitions:
    def test_open_to_closed(self, db):
        _insert_position(db, state="OPEN")
        assert transition_state(db, "pos1", "CLOSED", closure_reason="TARGET")

        row = query_one("SELECT lifecycle_state, closure_reason FROM positions WHERE position_id = 'pos1'", con=db)
        assert row[0] == "CLOSED"
        assert row[1] == "TARGET"

    def test_open_to_expired(self, db):
        _insert_position(db, state="OPEN")
        assert transition_state(db, "pos1", "EXPIRED", closure_reason="EXPIRY")

    def test_open_to_assigned(self, db):
        _insert_position(db, state="OPEN")
        assert transition_state(db, "pos1", "ASSIGNED", closure_reason="ASSIGNMENT")

    def test_open_to_broken(self, db):
        _insert_position(db, state="OPEN")
        assert transition_state(db, "pos1", "BROKEN")

    def test_broken_to_closed(self, db):
        _insert_position(db, state="BROKEN")
        assert transition_state(db, "pos1", "CLOSED", closure_reason="MANUAL")

    def test_invalid_transition(self, db):
        _insert_position(db, state="CLOSED")
        assert not transition_state(db, "pos1", "OPEN")

    def test_closed_to_anything_fails(self, db):
        _insert_position(db, state="CLOSED")
        for target in ("OPEN", "EXPIRED", "ASSIGNED", "BROKEN", "INTENDED"):
            assert not transition_state(db, "pos1", target)

    def test_nonexistent_position(self, db):
        assert not transition_state(db, "nonexistent", "CLOSED")


class TestProcessExpiries:
    def test_expire_past_positions(self, db):
        _insert_position(db, "p1", expiry_date="2026-03-10", state="OPEN")
        _insert_position(db, "p2", expiry_date="2026-03-20", state="OPEN")

        count = process_expiries(db, as_of_date=date(2026, 3, 12))
        assert count == 1

        p1 = query_one("SELECT lifecycle_state FROM positions WHERE position_id = 'p1'", con=db)
        assert p1[0] == "EXPIRED"

        p2 = query_one("SELECT lifecycle_state FROM positions WHERE position_id = 'p2'", con=db)
        assert p2[0] == "OPEN"

    def test_no_expiries(self, db):
        _insert_position(db, "p1", expiry_date="2026-03-20", state="OPEN")
        count = process_expiries(db, as_of_date=date(2026, 3, 12))
        assert count == 0


class TestRecordRoll:
    def test_roll_creates_relationship(self, db):
        _insert_position(db, "old_pos", state="OPEN")
        _insert_position(db, "new_pos", state="OPEN")

        record_roll(db, "old_pos", "new_pos")

        old = query_one("SELECT lifecycle_state, closure_reason FROM positions WHERE position_id = 'old_pos'", con=db)
        assert old[0] == "CLOSED"
        assert old[1] == "ROLL"

        rel = query_one("SELECT relationship_type FROM position_relationships", con=db)
        assert rel[0] == "ROLLED_FROM"


class TestCreatePositionFromFill:
    def test_creates_position(self, db):
        _insert_run(db, "r1")

        fill = {
            "trade_group_id": "tg1",
            "run_id": "r1",
            "fill_qty": 1,
            "fill_price": 2.45,
            "legs": '[{"osi": "SPXW260316C06000000", "option_type": "CALL", "strike": 6000, "action": "BUY_TO_OPEN", "qty": 1}]',
        }

        pos_id = create_position_from_fill(db, fill)
        assert pos_id == "tg1"

        row = query_one("SELECT lifecycle_state, entry_price, qty FROM positions WHERE position_id = 'tg1'", con=db)
        assert row[0] == "OPEN"
        assert row[1] == 2.45
        assert row[2] == 1

        # Check legs
        leg = query_one("SELECT osi, strike FROM position_legs WHERE position_id = 'tg1'", con=db)
        assert leg[0] == "SPXW260316C06000000"
        assert leg[1] == 6000

    def test_idempotent(self, db):
        _insert_run(db, "r1")

        fill = {
            "trade_group_id": "tg1",
            "run_id": "r1",
            "fill_qty": 1,
            "fill_price": 2.45,
            "legs": "[]",
        }

        create_position_from_fill(db, fill)
        create_position_from_fill(db, fill)  # Should not raise

        count = query_one("SELECT COUNT(*) FROM positions", con=db)
        assert count[0] == 1


class TestGetOpenPositions:
    def test_returns_open_only(self, db):
        _insert_position(db, "p1", state="OPEN")
        _insert_position(db, "p2", state="CLOSED")
        _insert_position(db, "p3", state="PARTIALLY_CLOSED")

        df = get_open_positions(con=db)
        assert len(df) == 2
        assert set(df["position_id"]) == {"p1", "p3"}

    def test_filter_by_strategy(self, db):
        _insert_position(db, "p1", strategy="butterfly", state="OPEN")
        _insert_position(db, "p2", strategy="dualside", state="OPEN")

        df = get_open_positions(con=db, strategy="butterfly")
        assert len(df) == 1
        assert df.iloc[0]["strategy"] == "butterfly"
