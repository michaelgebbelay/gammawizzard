"""Tests for reporting.events — EventWriter and event schema."""

import json
import os
import tempfile
from datetime import date
from pathlib import Path

import pytest

from reporting.events import (
    ALL_EVENT_TYPES,
    Event,
    EventWriter,
    _idem_key,
)


@pytest.fixture
def event_dir(tmp_path):
    """Provide a temp event directory and set env var."""
    os.environ["GAMMA_EVENT_DIR"] = str(tmp_path)
    yield tmp_path
    os.environ.pop("GAMMA_EVENT_DIR", None)


class TestIdempotencyKey:
    def test_deterministic(self):
        k1 = _idem_key("fill", {"order_id": "123", "qty": 1})
        k2 = _idem_key("fill", {"order_id": "123", "qty": 1})
        assert k1 == k2

    def test_different_payload(self):
        k1 = _idem_key("fill", {"order_id": "123", "qty": 1})
        k2 = _idem_key("fill", {"order_id": "456", "qty": 1})
        assert k1 != k2

    def test_different_type(self):
        k1 = _idem_key("fill", {"order_id": "123"})
        k2 = _idem_key("skip", {"order_id": "123"})
        assert k1 != k2

    def test_key_order_independent(self):
        k1 = _idem_key("fill", {"a": 1, "b": 2})
        k2 = _idem_key("fill", {"b": 2, "a": 1})
        assert k1 == k2

    def test_length(self):
        k = _idem_key("fill", {"x": 1})
        assert len(k) == 16


class TestEvent:
    def test_to_dict(self):
        ev = Event(
            event_id="abc",
            event_type="fill",
            ts_utc="2026-03-12T20:00:00+00:00",
            strategy="butterfly",
            account="schwab",
            trade_group_id="tg1",
            run_id="r1",
            config_version="abc123",
            payload={"qty": 1},
            idempotency_key="idem1",
        )
        d = ev.to_dict()
        assert d["event_id"] == "abc"
        assert d["payload"] == {"qty": 1}

    def test_to_json_roundtrip(self):
        ev = Event(
            event_id="abc",
            event_type="fill",
            ts_utc="2026-03-12T20:00:00+00:00",
            strategy="butterfly",
            account="schwab",
            trade_group_id="tg1",
            run_id="r1",
            config_version="abc123",
            payload={"qty": 1},
            idempotency_key="idem1",
        )
        j = ev.to_json()
        parsed = json.loads(j)
        assert parsed["event_type"] == "fill"

    def test_frozen(self):
        ev = Event("a", "b", "c", "d", "e", "f", "g", "h", {}, "i")
        with pytest.raises(AttributeError):
            ev.event_id = "new"


class TestEventWriter:
    def test_basic_lifecycle(self, event_dir):
        with EventWriter("butterfly", "schwab", trade_date=date(2026, 3, 12)) as ew:
            ew.strategy_run(signal="BUY", config="4DTE", reason="test")
            ew.trade_intent(
                side="DEBIT", direction="LONG",
                legs=[{"osi": "SPXW260316C06000000", "strike": 6000}],
                target_qty=1,
            )
            ew.order_submitted(order_id="12345", legs=[], limit_price=2.50)
            ew.fill(order_id="12345", fill_qty=1, fill_price=2.45)
            path = ew._output_path

        # File should exist with 4 events
        assert path.exists()
        lines = path.read_text().strip().split("\n")
        assert len(lines) == 4

        # Each line should be valid JSON
        for line in lines:
            ev = json.loads(line)
            assert ev["event_type"] in ALL_EVENT_TYPES
            assert ev["strategy"] == "butterfly"
            assert ev["account"] == "schwab"

    def test_skip_event(self, event_dir):
        with EventWriter("dualside", "schwab") as ew:
            ew.skip(reason="VIX1D veto", signal="NONE")
            path = ew._output_path

        lines = path.read_text().strip().split("\n")
        assert len(lines) == 1
        ev = json.loads(lines[0])
        assert ev["event_type"] == "skip"
        assert ev["payload"]["reason"] == "VIX1D veto"

    def test_error_event(self, event_dir):
        with EventWriter("constantstable", "tt-ira") as ew:
            ew.error(message="Chain fetch failed", stage="data_load")
            path = ew._output_path

        ev = json.loads(path.read_text().strip())
        assert ev["event_type"] == "error"
        assert "Chain fetch" in ev["payload"]["message"]

    def test_new_trade_group(self, event_dir):
        ew = EventWriter("dualside", "schwab")
        tg1 = ew.trade_group_id
        tg2 = ew.new_trade_group()
        assert tg1 != tg2
        assert ew.trade_group_id == tg2
        ew.close()

    def test_run_id_stable(self, event_dir):
        ew = EventWriter("butterfly", "schwab")
        ev1 = ew.strategy_run(signal="BUY", config="test", reason="test")
        ev2 = ew.fill(order_id="1", fill_qty=1, fill_price=1.0)
        assert ev1.run_id == ev2.run_id
        ew.close()

    def test_closed_writer_raises(self, event_dir):
        ew = EventWriter("butterfly", "schwab")
        ew.close()
        with pytest.raises(RuntimeError):
            ew.strategy_run(signal="BUY", config="test", reason="test")

    def test_invalid_event_type_raises(self, event_dir):
        ew = EventWriter("butterfly", "schwab")
        with pytest.raises(ValueError):
            ew._emit("INVALID_TYPE", {})
        ew.close()

    def test_context_manager_on_exception(self, event_dir):
        with pytest.raises(ValueError):
            with EventWriter("butterfly", "schwab") as ew:
                ew.strategy_run(signal="BUY", config="test", reason="test")
                raise ValueError("boom")

        # Should have strategy_run + error event
        lines = ew._output_path.read_text().strip().split("\n")
        assert len(lines) == 2
        error_ev = json.loads(lines[1])
        assert error_ev["event_type"] == "error"
        assert "boom" in error_ev["payload"]["message"]

    def test_output_directory_structure(self, event_dir):
        td = date(2026, 3, 15)
        ew = EventWriter("butterfly", "schwab", trade_date=td)
        ew.strategy_run(signal="SKIP", config="test", reason="test")
        path = ew.close()

        assert "2026-03-15" in str(path.parent)
        assert path.name.startswith("butterfly_schwab_")
        assert path.suffix == ".jsonl"

    def test_flush_clears_buffer(self, event_dir):
        ew = EventWriter("butterfly", "schwab")
        ew.strategy_run(signal="BUY", config="test", reason="test")
        assert len(ew._buffer) == 1
        ew.flush()
        assert len(ew._buffer) == 0
        ew.close()

    def test_post_step_result(self, event_dir):
        with EventWriter("butterfly", "schwab") as ew:
            ew.post_step_result(step_name="bf_trades_to_gsheet.py", outcome="OK")
            path = ew._output_path

        ev = json.loads(path.read_text().strip())
        assert ev["payload"]["step_name"] == "bf_trades_to_gsheet.py"
        assert ev["payload"]["outcome"] == "OK"
