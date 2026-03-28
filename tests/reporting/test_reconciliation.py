"""Tests for reporting.reconciliation — reconciliation engine checks."""

import json
import uuid
from datetime import datetime, timedelta, timezone

import duckdb
import pytest

from reporting.db import execute, init_schema, query_df, query_one
from reporting.reconciliation import (
    _auto_resolve_prior_items,
    _check_cash_match,
    _check_fill_match,
    _check_freshness,
    _check_position_match,
    run_reconciliation,
)

REPORT_DATE = "2026-03-12"
# Broker fetches happen after US market close (4 PM ET ≈ 21:00 UTC).
# The reconciliation fetch window is report_date 14:00 UTC to report_date+1 12:00 UTC.
# Use a timestamp inside that window for test broker data.
FETCH_TS = "2026-03-12 21:00:00"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db():
    """Provide a fresh in-memory DuckDB."""
    con = duckdb.connect(":memory:")
    init_schema(con)
    yield con
    con.close()


# ---------------------------------------------------------------------------
# Test helpers — insert data into the in-memory DB
# ---------------------------------------------------------------------------

def _uid() -> str:
    return uuid.uuid4().hex[:16]


def _insert_run(con, run_id="r1", strategy="butterfly", account="schwab",
                trade_date=REPORT_DATE):
    execute(
        """INSERT INTO strategy_runs
           (run_id, strategy, account, trade_date, config_version, status, started_at)
           VALUES (?, ?, ?, ?, 'test', 'COMPLETED', current_timestamp)""",
        [run_id, strategy, account, trade_date],
        con=con,
    )


def _insert_fill(con, fill_id=None, trade_group_id="tg1", run_id="r1",
                 order_id="ORD1", fill_qty=1, fill_price=2.50):
    fill_id = fill_id or _uid()
    execute(
        """INSERT INTO fills
           (fill_id, trade_group_id, run_id, order_id, ts_utc,
            fill_qty, fill_price)
           VALUES (?, ?, ?, ?, current_timestamp, ?, ?)""",
        [fill_id, trade_group_id, run_id, order_id, fill_qty, fill_price],
        con=con,
    )


def _insert_broker_raw_fill(con, order_id="ORD1", leg_id="0", qty=1,
                            price=2.50, fetched_date=FETCH_TS):
    raw_payload = json.dumps({
        "leg_id": leg_id,
        "qty": qty,
        "price": price,
        "time": "",
    })
    execute(
        """INSERT INTO broker_raw_fills
           (id, broker, account, order_id, fill_id, fetched_at,
            raw_payload, idempotency_key)
           VALUES (?, 'schwab', 'schwab', ?, ?, ?::TIMESTAMP, ?, ?)""",
        [_uid(), order_id, leg_id, fetched_date, raw_payload, _uid()],
        con=con,
    )


def _insert_position(con, position_id="pos1", strategy="butterfly",
                     account="schwab", state="OPEN",
                     trade_date=REPORT_DATE, expiry_date="2026-03-16"):
    execute(
        """INSERT INTO positions
           (position_id, strategy, account, trade_date, expiry_date,
            lifecycle_state, provenance, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, 'STRATEGY', current_timestamp)""",
        [position_id, strategy, account, trade_date, expiry_date, state],
        con=con,
    )


def _insert_position_leg(con, position_id="pos1", osi="SPXW260316P06000000"):
    execute(
        """INSERT INTO position_legs
           (leg_id, position_id, osi, option_type, strike, action, qty)
           VALUES (?, ?, ?, 'PUT', 6000, 'BUY_TO_OPEN', 1)""",
        [_uid(), position_id, osi],
        con=con,
    )


def _insert_broker_raw_positions(con, account="schwab", positions_list=None,
                                 fetched_date=FETCH_TS):
    if positions_list is None:
        positions_list = []
    raw_payload = json.dumps({"positions": positions_list})
    execute(
        """INSERT INTO broker_raw_positions
           (id, broker, account, fetched_at, raw_payload)
           VALUES (?, 'schwab', ?, ?::TIMESTAMP, ?)""",
        [_uid(), account, fetched_date, raw_payload],
        con=con,
    )


def _insert_account_snapshot(con, account="schwab", cash=1000.0,
                             net_liq=5000.0, snapshot_date=REPORT_DATE):
    execute(
        """INSERT INTO account_snapshots
           (id, account, snapshot_date, as_of, cash, net_liq)
           VALUES (?, ?, ?, current_timestamp, ?, ?)""",
        [_uid(), account, snapshot_date, cash, net_liq],
        con=con,
    )


def _insert_broker_raw_cash(con, account="schwab", cash_balance=1000.0,
                            liquidation_value=5000.0,
                            fetched_date=FETCH_TS):
    raw_payload = json.dumps({
        "currentBalances": {
            "cashBalance": cash_balance,
            "liquidationValue": liquidation_value,
        },
    })
    execute(
        """INSERT INTO broker_raw_cash
           (id, broker, account, fetched_at, raw_payload)
           VALUES (?, 'schwab', ?, ?::TIMESTAMP, ?)""",
        [_uid(), account, fetched_date, raw_payload],
        con=con,
    )


def _insert_source_freshness(con, source_name="schwab_orders",
                             last_success_at=None, sla_minutes=60):
    execute(
        """INSERT INTO source_freshness
           (source_name, last_success_at, sla_minutes, is_stale)
           VALUES (?, ?::TIMESTAMP, ?, false)""",
        [source_name, last_success_at, sla_minutes],
        con=con,
    )


def _create_recon_run(con, run_id=None):
    """Create a reconciliation_runs row so that items can reference it."""
    run_id = run_id or _uid()
    execute(
        """INSERT INTO reconciliation_runs
           (id, run_date, started_at, status)
           VALUES (?, ?, current_timestamp, 'RUNNING')""",
        [run_id, REPORT_DATE],
        con=con,
    )
    return run_id


def _count_issues(con, check_type=None):
    if check_type:
        row = query_one(
            "SELECT COUNT(*) FROM reconciliation_items WHERE check_type = ?",
            [check_type], con=con,
        )
    else:
        row = query_one("SELECT COUNT(*) FROM reconciliation_items", con=con)
    return row[0]


# ===========================================================================
# Check 1: fill_match
# ===========================================================================

class TestFillMatch:
    def test_matching_fills_no_issues(self, db):
        """1 internal fill + 2 broker leg rows for same order → 0 issues."""
        _insert_run(db, "r1")
        _insert_fill(db, order_id="ORD1", run_id="r1", fill_qty=1)
        # 2-leg spread: leg_id "0" and "1", each qty=1
        _insert_broker_raw_fill(db, order_id="ORD1", leg_id="0", qty=1)
        _insert_broker_raw_fill(db, order_id="ORD1", leg_id="1", qty=1)

        run_id = _create_recon_run(db)
        stats = _check_fill_match(db, run_id, REPORT_DATE)

        assert stats["issues"] == 0
        assert _count_issues(db, "fill_match") == 0

    def test_broker_fill_no_internal_match(self, db):
        """Broker fill for order ORD2 but no internal fill → 1 ERROR."""
        _insert_broker_raw_fill(db, order_id="ORD2", leg_id="0", qty=1)

        run_id = _create_recon_run(db)
        stats = _check_fill_match(db, run_id, REPORT_DATE)

        assert stats["issues"] == 1
        assert _count_issues(db, "fill_match") == 1

        row = query_one(
            "SELECT severity, check_type FROM reconciliation_items WHERE check_type = 'fill_match'",
            con=db,
        )
        assert row[0] == "ERROR"
        assert row[1] == "fill_match"

    def test_internal_fill_no_broker_match(self, db):
        """Internal fill for order ORD3 but no broker fill → 1 WARNING."""
        _insert_run(db, "r1")
        _insert_fill(db, order_id="ORD3", run_id="r1", fill_qty=1)

        run_id = _create_recon_run(db)
        stats = _check_fill_match(db, run_id, REPORT_DATE)

        assert stats["issues"] == 1
        assert _count_issues(db, "fill_match") == 1

        row = query_one(
            "SELECT severity FROM reconciliation_items WHERE check_type = 'fill_match'",
            con=db,
        )
        assert row[0] == "WARNING"

    def test_quantity_mismatch(self, db):
        """Internal qty=2, broker has 2 legs each qty=1 (combo=1) → ERROR."""
        _insert_run(db, "r1")
        _insert_fill(db, order_id="ORD4", run_id="r1", fill_qty=2)
        _insert_broker_raw_fill(db, order_id="ORD4", leg_id="0", qty=1)
        _insert_broker_raw_fill(db, order_id="ORD4", leg_id="1", qty=1)

        run_id = _create_recon_run(db)
        stats = _check_fill_match(db, run_id, REPORT_DATE)

        assert stats["issues"] == 1
        row = query_one(
            "SELECT severity, message FROM reconciliation_items WHERE check_type = 'fill_match'",
            con=db,
        )
        assert row[0] == "ERROR"
        assert "quantity mismatch" in row[1].lower()

    def test_empty_tables_no_issues(self, db):
        """No fills anywhere → 0 issues."""
        run_id = _create_recon_run(db)
        stats = _check_fill_match(db, run_id, REPORT_DATE)

        assert stats["issues"] == 0
        assert _count_issues(db, "fill_match") == 0


# ===========================================================================
# Check 2: position_match
# ===========================================================================

class TestPositionMatch:
    def test_matching_positions_no_issues(self, db):
        """Internal open position with matching broker symbol → 0 issues."""
        _insert_position(db, "pos1", account="schwab", state="OPEN")
        _insert_position_leg(db, "pos1", osi="SPXW260316P06000000")
        _insert_broker_raw_positions(db, account="schwab", positions_list=[
            {"instrument": {"symbol": "SPXW260316P06000000"}},
        ])

        run_id = _create_recon_run(db)
        stats = _check_position_match(db, run_id, REPORT_DATE)

        assert stats["issues"] == 0
        assert _count_issues(db, "position_match") == 0

    def test_internal_position_missing_at_broker_auto_closed(self, db):
        """Internal open position gone from broker → auto-closed, INFO."""
        _insert_position(db, "pos1", account="schwab", state="OPEN")
        _insert_position_leg(db, "pos1", osi="SPXW260316P06000000")
        _insert_broker_raw_positions(db, account="schwab", positions_list=[])

        run_id = _create_recon_run(db)
        stats = _check_position_match(db, run_id, REPORT_DATE)

        assert stats["issues"] == 1
        assert stats["auto_closed"] == 1
        row = query_one(
            "SELECT severity, classification FROM reconciliation_items WHERE check_type = 'position_match'",
            con=db,
        )
        assert row[0] == "INFO"
        assert row[1] == "broker_closed"

        # Verify position was transitioned to CLOSED
        state = query_one("SELECT lifecycle_state, closure_reason FROM positions WHERE position_id = 'pos1'", con=db)
        assert state[0] == "CLOSED"
        assert state[1] == "MANUAL"

    def test_broker_position_not_tracked(self, db):
        """No internal positions, no broker orders → unknown_source classification."""
        _insert_broker_raw_positions(db, account="schwab", positions_list=[
            {"instrument": {"symbol": "SPXW260316C06100000"}},
        ])

        run_id = _create_recon_run(db)
        stats = _check_position_match(db, run_id, REPORT_DATE)

        assert stats["issues"] == 1
        row = query_one(
            """SELECT severity, classification, classification_reason
               FROM reconciliation_items WHERE check_type = 'position_match'""",
            con=db,
        )
        # No orders in lookback → unknown_source, INFO
        assert row[0] == "INFO"
        assert row[1] == "unknown_source"
        assert "no_order_in_lookback_window" in row[2]

    def test_broker_position_api_tagged_is_warning(self, db):
        """API-tagged broker position with no internal match → api_unmatched WARNING."""
        from reporting.reconciliation import API_TAG

        _insert_broker_raw_positions(db, account="schwab", positions_list=[
            {"instrument": {"symbol": "SPXW260316C06100000"}},
        ])
        execute(
            """INSERT INTO broker_raw_orders
               (id, broker, account, order_id, fetched_at, raw_payload, idempotency_key)
               VALUES (?, 'schwab', 'schwab', 'ORD_API', ?::TIMESTAMP, ?, ?)""",
            [
                _uid(), FETCH_TS,
                json.dumps({
                    "tag": API_TAG,
                    "orderLegCollection": [
                        {"instrument": {"symbol": "SPXW 260316C06100000"}}
                    ],
                    "orderActivityCollection": [{
                        "activityType": "EXECUTION",
                        "executionLegs": [{"time": "2026-03-12T20:15:00+00:00"}],
                    }],
                }),
                _uid(),
            ],
            con=db,
        )

        run_id = _create_recon_run(db)
        stats = _check_position_match(db, run_id, REPORT_DATE)

        assert stats["issues"] == 1
        row = query_one(
            """SELECT severity, classification, classification_reason
               FROM reconciliation_items WHERE check_type = 'position_match'""",
            con=db,
        )
        assert row[0] == "WARNING"
        assert row[1] == "api_unmatched"
        assert API_TAG in row[2]

    def test_broker_position_non_api_tag_is_info(self, db):
        """Non-API-tagged order → non_api INFO classification."""
        _insert_broker_raw_positions(db, account="schwab", positions_list=[
            {"instrument": {"symbol": "SPXW260316C06100000"}},
        ])
        execute(
            """INSERT INTO broker_raw_orders
               (id, broker, account, order_id, fetched_at, raw_payload, idempotency_key)
               VALUES (?, 'schwab', 'schwab', 'ORD_MANUAL', ?::TIMESTAMP, ?, ?)""",
            [
                _uid(), FETCH_TS,
                json.dumps({
                    "tag": "SOME_OTHER_TAG",
                    "orderLegCollection": [
                        {"instrument": {"symbol": "SPXW 260316C06100000"}}
                    ],
                    "orderActivityCollection": [{
                        "activityType": "EXECUTION",
                        "executionLegs": [{"time": "2026-03-12T18:00:00+00:00"}],
                    }],
                }),
                _uid(),
            ],
            con=db,
        )

        run_id = _create_recon_run(db)
        stats = _check_position_match(db, run_id, REPORT_DATE)

        assert stats["issues"] == 1
        row = query_one(
            """SELECT severity, classification, classification_reason
               FROM reconciliation_items WHERE check_type = 'position_match'""",
            con=db,
        )
        assert row[0] == "INFO"
        assert row[1] == "non_api"
        assert "SOME_OTHER_TAG" in row[2]

    def test_broker_position_no_tag_falls_back_to_trigger_window(self, db):
        """No tag, fill outside trigger windows → non_api via window fallback."""
        from reporting.seed_trigger_windows import seed
        seed(con=db)

        _insert_broker_raw_positions(db, account="schwab", positions_list=[
            {"instrument": {"symbol": "SPXW260316C06100000"}},
        ])
        execute(
            """INSERT INTO broker_raw_orders
               (id, broker, account, order_id, fetched_at, raw_payload, idempotency_key)
               VALUES (?, 'schwab', 'schwab', 'ORD_NOTAG', ?::TIMESTAMP, ?, ?)""",
            [
                _uid(), FETCH_TS,
                json.dumps({
                    "orderLegCollection": [
                        {"instrument": {"symbol": "SPXW 260316C06100000"}}
                    ],
                    "orderActivityCollection": [{
                        "activityType": "EXECUTION",
                        "executionLegs": [{"time": "2026-03-12T18:00:00+00:00"}],
                    }],
                }),
                _uid(),
            ],
            con=db,
        )

        run_id = _create_recon_run(db)
        stats = _check_position_match(db, run_id, REPORT_DATE)

        assert stats["issues"] == 1
        row = query_one(
            """SELECT severity, classification, classification_reason
               FROM reconciliation_items WHERE check_type = 'position_match'""",
            con=db,
        )
        assert row[0] == "INFO"
        assert row[1] == "non_api"
        assert "no_tag" in row[2]
        assert "OUTSIDE_TRIGGER_WINDOW" in row[2]

    def test_no_broker_data_skips_check(self, db):
        """Internal positions exist but no broker_raw_positions rows → 0 issues (graceful skip)."""
        _insert_position(db, "pos1", account="schwab", state="OPEN")
        _insert_position_leg(db, "pos1", osi="SPXW260316P06000000")

        run_id = _create_recon_run(db)
        stats = _check_position_match(db, run_id, REPORT_DATE)

        assert stats["issues"] == 0
        assert _count_issues(db, "position_match") == 0


# ===========================================================================
# Check 3: cash_match
# ===========================================================================

class TestCashMatch:
    def test_matching_cash_no_issues(self, db):
        """Exact cash and net_liq match → 0 issues."""
        _insert_account_snapshot(db, cash=1000.0, net_liq=5000.0)
        _insert_broker_raw_cash(db, cash_balance=1000.0, liquidation_value=5000.0)

        run_id = _create_recon_run(db)
        stats = _check_cash_match(db, run_id, REPORT_DATE)

        assert stats["issues"] == 0
        assert _count_issues(db, "cash_match") == 0

    def test_cash_within_tolerance(self, db):
        """Cash diff $45 < $50 tolerance → 0 issues."""
        _insert_account_snapshot(db, cash=1000.0, net_liq=5000.0)
        _insert_broker_raw_cash(db, cash_balance=1045.0, liquidation_value=5000.0)

        run_id = _create_recon_run(db)
        stats = _check_cash_match(db, run_id, REPORT_DATE)

        assert stats["issues"] == 0

    def test_cash_exceeds_tolerance(self, db):
        """Cash diff $100 > $50 tolerance → 1 WARNING."""
        _insert_account_snapshot(db, cash=1000.0, net_liq=5000.0)
        _insert_broker_raw_cash(db, cash_balance=1100.0, liquidation_value=5000.0)

        run_id = _create_recon_run(db)
        stats = _check_cash_match(db, run_id, REPORT_DATE)

        assert stats["issues"] == 1
        row = query_one(
            "SELECT severity FROM reconciliation_items WHERE check_type = 'cash_match'",
            con=db,
        )
        assert row[0] == "WARNING"

    def test_net_liq_mismatch(self, db):
        """Net liq diff $200 > $50 → 1 WARNING."""
        _insert_account_snapshot(db, cash=1000.0, net_liq=5000.0)
        _insert_broker_raw_cash(db, cash_balance=1000.0, liquidation_value=4800.0)

        run_id = _create_recon_run(db)
        stats = _check_cash_match(db, run_id, REPORT_DATE)

        assert stats["issues"] == 1
        row = query_one(
            "SELECT severity FROM reconciliation_items WHERE check_type = 'cash_match'",
            con=db,
        )
        assert row[0] == "WARNING"

    def test_nested_payload_reads_correctly(self, db):
        """Verify nested currentBalances payload is parsed correctly (P1 fix)."""
        _insert_account_snapshot(db, cash=1234.56, net_liq=5678.90)
        _insert_broker_raw_cash(db, cash_balance=1234.56, liquidation_value=5678.90)

        run_id = _create_recon_run(db)
        stats = _check_cash_match(db, run_id, REPORT_DATE)

        # Exact match means the nested JSON was read correctly
        assert stats["issues"] == 0
        assert _count_issues(db, "cash_match") == 0


# ===========================================================================
# Check 4: freshness
# ===========================================================================

class TestFreshness:
    def test_fresh_source_no_issues(self, db):
        """Source last success 5 min ago, SLA 60 min → 0 issues, not stale."""
        five_min_ago = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
        _insert_source_freshness(db, "schwab_orders", last_success_at=five_min_ago, sla_minutes=60)

        run_id = _create_recon_run(db)
        stats = _check_freshness(db, run_id)

        assert stats["issues"] == 0

        row = query_one(
            "SELECT is_stale FROM source_freshness WHERE source_name = 'schwab_orders'",
            con=db,
        )
        assert row[0] is False

    def test_stale_source_creates_issue(self, db):
        """Source last success 120 min ago, SLA 60 min → 1 issue, stale."""
        long_ago = (datetime.now(timezone.utc) - timedelta(minutes=120)).isoformat()
        _insert_source_freshness(db, "schwab_orders", last_success_at=long_ago, sla_minutes=60)

        run_id = _create_recon_run(db)
        stats = _check_freshness(db, run_id)

        assert stats["issues"] == 1

        row = query_one(
            "SELECT is_stale FROM source_freshness WHERE source_name = 'schwab_orders'",
            con=db,
        )
        assert row[0] is True

    def test_never_succeeded_source(self, db):
        """Source with last_success_at=NULL → 1 ERROR issue."""
        _insert_source_freshness(db, "schwab_orders", last_success_at=None, sla_minutes=60)

        run_id = _create_recon_run(db)
        stats = _check_freshness(db, run_id)

        assert stats["issues"] == 1

        row = query_one(
            "SELECT severity FROM reconciliation_items WHERE check_type = 'freshness'",
            con=db,
        )
        assert row[0] == "ERROR"

    def test_staleness_clears_when_fresh(self, db):
        """Source marked is_stale=true but last_success=now → after check, is_stale=false."""
        now_iso = datetime.now(timezone.utc).isoformat()
        # Insert as stale
        execute(
            """INSERT INTO source_freshness
               (source_name, last_success_at, sla_minutes, is_stale)
               VALUES (?, ?::TIMESTAMP, ?, true)""",
            ["schwab_orders", now_iso, 60],
            con=db,
        )

        run_id = _create_recon_run(db)
        stats = _check_freshness(db, run_id)

        assert stats["issues"] == 0

        row = query_one(
            "SELECT is_stale FROM source_freshness WHERE source_name = 'schwab_orders'",
            con=db,
        )
        assert row[0] is False


# ===========================================================================
# Auto-resolve
# ===========================================================================

class TestAutoResolve:
    def test_auto_resolve_clears_prior_issues(self, db):
        """Insert UNRESOLVED item, call _auto_resolve → status = AUTO_RESOLVED."""
        run_id = _create_recon_run(db)
        execute(
            """INSERT INTO reconciliation_items
               (id, recon_run_id, check_type, entity_type, severity,
                status, message, opened_at)
               VALUES (?, ?, 'fill_match', 'fill', 'ERROR',
                       'UNRESOLVED', 'test issue', current_timestamp)""",
            [_uid(), run_id],
            con=db,
        )

        count = _auto_resolve_prior_items(db)
        assert count == 1

        row = query_one(
            "SELECT status, resolved_at FROM reconciliation_items",
            con=db,
        )
        assert row[0] == "AUTO_RESOLVED"
        assert row[1] is not None

    def test_auto_resolve_with_no_prior_issues(self, db):
        """No items → returns 0, no errors."""
        count = _auto_resolve_prior_items(db)
        assert count == 0


# ===========================================================================
# Full run_reconciliation
# ===========================================================================

class TestRunReconciliation:
    def test_full_run_creates_recon_run_record(self, db):
        """run_reconciliation on empty DB → 1 row in reconciliation_runs, status COMPLETED."""
        run_reconciliation(con=db, report_date=REPORT_DATE)

        row = query_one(
            "SELECT status FROM reconciliation_runs",
            con=db,
        )
        assert row[0] == "COMPLETED"

    def test_full_run_returns_stats(self, db):
        """Return dict has expected keys."""
        result = run_reconciliation(con=db, report_date=REPORT_DATE)

        assert "run_id" in result
        assert "checks_run" in result
        assert "issues_found" in result
        assert "auto_resolved" in result
        assert "status" in result
        assert result["status"] == "COMPLETED"

    def test_second_run_auto_resolves_prior(self, db):
        """First run with mismatch → UNRESOLVED; second run (fixed) → auto-resolved, 0 unresolved."""
        # First run: broker fill with no internal match → creates UNRESOLVED issue
        _insert_broker_raw_fill(db, order_id="ORD_ORPHAN", leg_id="0", qty=1)
        result1 = run_reconciliation(con=db, report_date=REPORT_DATE)
        assert result1["issues_found"] >= 1

        unresolved_before = query_one(
            "SELECT COUNT(*) FROM reconciliation_items WHERE status = 'UNRESOLVED'",
            con=db,
        )
        assert unresolved_before[0] >= 1

        # Remove the orphan broker fill so the mismatch no longer exists
        execute("DELETE FROM broker_raw_fills", con=db)

        # Second run: the old issue gets auto-resolved, no new issues created
        result2 = run_reconciliation(con=db, report_date=REPORT_DATE)
        assert result2["auto_resolved"] >= 1

        unresolved_after = query_one(
            "SELECT COUNT(*) FROM reconciliation_items WHERE status = 'UNRESOLVED'",
            con=db,
        )
        assert unresolved_after[0] == 0
