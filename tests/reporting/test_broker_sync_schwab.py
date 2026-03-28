"""Tests for reporting.broker_sync_schwab — pure parsing/normalization and DB freshness."""

import duckdb
import pytest

from reporting.broker_sync_schwab import (
    _extract_fills_from_order,
    _extract_legs_from_order,
    _idem_key,
    _normalize_order_status,
    _update_freshness,
)
from reporting.db import execute, init_schema, query_one


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db():
    con = duckdb.connect(":memory:")
    init_schema(con)
    yield con
    con.close()


# ---------------------------------------------------------------------------
# TestNormalizeOrderStatus
# ---------------------------------------------------------------------------

class TestNormalizeOrderStatus:
    def test_filled(self):
        assert _normalize_order_status("FILLED") == "FILLED"

    def test_working_variants(self):
        for raw in ("WORKING", "PENDING_ACTIVATION", "QUEUED", "ACCEPTED"):
            assert _normalize_order_status(raw) == "WORKING", f"{raw} should map to WORKING"

    def test_canceled_variants(self):
        for raw in ("CANCELED", "PENDING_CANCEL", "REPLACED"):
            assert _normalize_order_status(raw) == "CANCELED", f"{raw} should map to CANCELED"

    def test_unknown_passthrough(self):
        assert _normalize_order_status("SOME_NEW_STATUS") == "SOME_NEW_STATUS"

    def test_case_insensitive(self):
        assert _normalize_order_status("filled") == "FILLED"


# ---------------------------------------------------------------------------
# TestExtractLegs
# ---------------------------------------------------------------------------

class TestExtractLegs:
    TWO_LEG_ORDER = {
        "orderLegCollection": [
            {
                "instrument": {
                    "symbol": "SPXW 260316P06000000",
                    "putCall": "PUT",
                    "strikePrice": 6000,
                },
                "instruction": "SELL_TO_OPEN",
                "quantity": 1,
            },
            {
                "instrument": {
                    "symbol": "SPXW 260316P05950000",
                    "putCall": "PUT",
                    "strikePrice": 5950,
                },
                "instruction": "BUY_TO_OPEN",
                "quantity": 1,
            },
        ]
    }

    def test_two_leg_spread(self):
        legs = _extract_legs_from_order(self.TWO_LEG_ORDER)
        assert len(legs) == 2

        sell_leg = legs[0]
        assert sell_leg["osi"] == "SPXW 260316P06000000"
        assert sell_leg["option_type"] == "PUT"
        assert sell_leg["strike"] == 6000
        assert sell_leg["action"] == "SELL_TO_OPEN"
        assert sell_leg["qty"] == 1

        buy_leg = legs[1]
        assert buy_leg["osi"] == "SPXW 260316P05950000"
        assert buy_leg["option_type"] == "PUT"
        assert buy_leg["strike"] == 5950
        assert buy_leg["action"] == "BUY_TO_OPEN"
        assert buy_leg["qty"] == 1

    def test_empty_order(self):
        assert _extract_legs_from_order({}) == []

    def test_strips_whitespace_from_osi(self):
        order = {
            "orderLegCollection": [
                {
                    "instrument": {
                        "symbol": " SPXW  260316P06000000 ",
                        "putCall": "PUT",
                        "strikePrice": 6000,
                    },
                    "instruction": "SELL_TO_OPEN",
                    "quantity": 1,
                },
            ]
        }
        legs = _extract_legs_from_order(order)
        assert legs[0]["osi"] == "SPXW  260316P06000000"


# ---------------------------------------------------------------------------
# TestExtractFills
# ---------------------------------------------------------------------------

class TestExtractFills:
    EXECUTION_ORDER = {
        "orderActivityCollection": [
            {
                "activityType": "EXECUTION",
                "executionLegs": [
                    {"legId": 1, "quantity": 1, "price": 2.50, "time": "2026-03-12T16:15:00Z"},
                    {"legId": 2, "quantity": 1, "price": 1.80, "time": "2026-03-12T16:15:00Z"},
                ],
            }
        ]
    }

    def test_single_execution_two_legs(self):
        fills = _extract_fills_from_order(self.EXECUTION_ORDER)
        assert len(fills) == 2

        assert fills[0]["leg_id"] == 1
        assert fills[0]["qty"] == 1
        assert fills[0]["price"] == 2.50
        assert fills[0]["time"] == "2026-03-12T16:15:00Z"

        assert fills[1]["leg_id"] == 2
        assert fills[1]["qty"] == 1
        assert fills[1]["price"] == 1.80

    def test_non_execution_activity_skipped(self):
        order = {
            "orderActivityCollection": [
                {
                    "activityType": "CANCEL",
                    "executionLegs": [
                        {"legId": 1, "quantity": 1, "price": 0, "time": "2026-03-12T16:15:00Z"},
                    ],
                }
            ]
        }
        assert _extract_fills_from_order(order) == []

    def test_empty_activity_collection(self):
        assert _extract_fills_from_order({}) == []


# ---------------------------------------------------------------------------
# TestIdemKey
# ---------------------------------------------------------------------------

class TestIdemKey:
    def test_deterministic(self):
        k1 = _idem_key("schwab", "order", {"order_id": "123"})
        k2 = _idem_key("schwab", "order", {"order_id": "123"})
        assert k1 == k2

    def test_different_inputs(self):
        k1 = _idem_key("schwab", "order", {"order_id": "123"})
        k2 = _idem_key("schwab", "order", {"order_id": "456"})
        assert k1 != k2

    def test_length_20(self):
        key = _idem_key("schwab", "order", {"order_id": "123"})
        assert len(key) == 20


# ---------------------------------------------------------------------------
# TestUpdateFreshness
# ---------------------------------------------------------------------------

class TestUpdateFreshness:
    def test_insert_success(self, db):
        _update_freshness(db, "schwab_test", success=True)

        row = query_one(
            "SELECT last_success_at, is_stale, error_message FROM source_freshness WHERE source_name = ?",
            ["schwab_test"], con=db,
        )
        assert row is not None
        assert row[0] is not None          # last_success_at set
        assert row[1] is False             # is_stale = false
        assert row[2] is None              # error_message NULL

    def test_insert_failure(self, db):
        _update_freshness(db, "schwab_fail", success=False, error_msg="timeout")

        row = query_one(
            "SELECT last_success_at, is_stale, error_message FROM source_freshness WHERE source_name = ?",
            ["schwab_fail"], con=db,
        )
        assert row is not None
        assert row[0] is None              # last_success_at NULL
        assert row[1] is True              # is_stale = true
        assert row[2] == "timeout"

    def test_update_existing_success(self, db):
        # Insert a failure first
        _update_freshness(db, "schwab_recover", success=False, error_msg="boom")
        # Now succeed
        _update_freshness(db, "schwab_recover", success=True)

        row = query_one(
            "SELECT last_success_at, is_stale, error_message FROM source_freshness WHERE source_name = ?",
            ["schwab_recover"], con=db,
        )
        assert row[0] is not None          # last_success_at set
        assert row[1] is False             # is_stale = false
        assert row[2] is None              # error_message cleared

    def test_update_existing_failure(self, db):
        # Insert a success first
        _update_freshness(db, "schwab_degrade", success=True)
        # Now fail
        _update_freshness(db, "schwab_degrade", success=False, error_msg="network")

        row = query_one(
            "SELECT last_success_at, is_stale, error_message FROM source_freshness WHERE source_name = ?",
            ["schwab_degrade"], con=db,
        )
        assert row[0] is not None          # last_success_at still from first call
        assert row[1] is True              # is_stale = true
        assert row[2] == "network"
