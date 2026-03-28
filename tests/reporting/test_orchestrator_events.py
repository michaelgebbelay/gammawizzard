"""Smoke tests verifying EventWriter integration patterns used by BF/DS/CS orchestrators.

These tests do NOT import the orchestrators (which need broker credentials).
Instead they exercise the EventWriter API in the exact patterns the orchestrators use:
  - _init_events() -> creates EventWriter
  - _emit(event_type, **kwargs) -> wraps typed helpers
  - _ew global (None until init)
  - new_trade_group() for multi-leg runs
  - context-manager error capture
"""

import json
import os
from datetime import date
from pathlib import Path

import pytest

from reporting.events import EventWriter, ALL_EVENT_TYPES, Event


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read_events(ew: EventWriter) -> list[dict]:
    """Flush buffer and read back all events from the JSONL file."""
    ew.flush()
    text = ew._output_path.read_text().strip()
    if not text:
        return []
    return [json.loads(line) for line in text.split("\n") if line.strip()]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def ew(tmp_path):
    os.environ["GAMMA_EVENT_DIR"] = str(tmp_path)
    writer = EventWriter(
        strategy="test",
        account="test_acct",
        trade_date=date(2026, 3, 12),
    )
    yield writer
    if not writer._closed:
        writer.close()
    os.environ.pop("GAMMA_EVENT_DIR", None)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestEventWriterOrchestrationPatterns:
    """Verify the event emission patterns used by BF/DS/CS orchestrators."""

    def test_strategy_run_skip_pattern(self, ew):
        """BF/DS/CS weekend/time-guard: strategy_run(SKIP) then skip(WEEKEND)."""
        ew.strategy_run(signal="SKIP", config="", reason="weekend")
        ew.skip(reason="WEEKEND")
        ew.close()

        events = _read_events(ew)
        assert len(events) == 2
        assert events[0]["event_type"] == "strategy_run"
        assert events[1]["event_type"] == "skip"

    def test_strategy_run_then_trade_intent_then_fill(self, ew):
        """BF happy path: strategy_run -> trade_intent -> order_submitted -> fill."""
        ew.strategy_run(signal="BUY", config="4DTE_D20P", reason="VIX1D=15")
        ew.trade_intent(
            side="DEBIT", direction="BUY", legs=[{"osi": "X"}], target_qty=1,
        )
        ew.order_submitted(order_id="ORD1", legs=[{"osi": "X"}], limit_price=2.50)
        ew.fill(order_id="ORD1", fill_qty=1, fill_price=2.45)
        ew.close()

        events = _read_events(ew)
        assert len(events) == 4

        run_ids = {e["run_id"] for e in events}
        assert len(run_ids) == 1, "all events share the same run_id"

        group_ids = {e["trade_group_id"] for e in events}
        assert len(group_ids) == 1, "all events share the same trade_group_id"

    def test_new_trade_group_for_multi_vertical(self, ew):
        """CS/DS pattern: two independent verticals get different trade_group_ids."""
        ev1 = ew.trade_intent(
            side="CREDIT", direction="SELL", legs=[{"osi": "P1"}], target_qty=1,
        )
        gid1 = ev1.trade_group_id

        ew.new_trade_group()

        ev2 = ew.trade_intent(
            side="CREDIT", direction="SELL", legs=[{"osi": "C1"}], target_qty=1,
        )
        gid2 = ev2.trade_group_id

        assert gid1 != gid2, "multi-vertical intents must have different trade_group_ids"

        # Same run_id throughout
        assert ev1.run_id == ev2.run_id

    def test_trade_group_id_restore_for_fill_readback(self, ew):
        """CS fill readback: restore trade_group_id to associate fill with correct intent."""
        ev1 = ew.trade_intent(
            side="CREDIT", direction="SELL", legs=[{"osi": "P1"}], target_qty=1,
        )
        gid1 = ev1.trade_group_id

        gid2 = ew.new_trade_group()
        ew.trade_intent(
            side="CREDIT", direction="SELL", legs=[{"osi": "C1"}], target_qty=1,
        )

        # Restore gid1 for the fill readback of the first trade
        ew.trade_group_id = gid1
        fill_ev = ew.fill(order_id="ORD1", fill_qty=1, fill_price=1.20)

        assert fill_ev.trade_group_id == gid1
        assert fill_ev.trade_group_id != gid2

    def test_context_manager_on_error(self, tmp_path):
        """Error handling: context manager captures exception and writes error event."""
        os.environ["GAMMA_EVENT_DIR"] = str(tmp_path)
        try:
            with EventWriter(
                strategy="test_cm",
                account="test_acct",
                trade_date=date(2026, 3, 12),
            ) as ew_cm:
                ew_cm.strategy_run(signal="BUY", config="X", reason="test")
                raise ValueError("simulated broker timeout")
        except ValueError:
            pass

        # Writer should be closed and have flushed
        assert ew_cm._closed

        text = ew_cm._output_path.read_text().strip()
        events = [json.loads(line) for line in text.split("\n") if line.strip()]

        event_types = [e["event_type"] for e in events]
        assert "error" in event_types, "error event must be written on exception"
        error_ev = next(e for e in events if e["event_type"] == "error")
        assert "simulated broker timeout" in error_ev["payload"]["message"]
        os.environ.pop("GAMMA_EVENT_DIR", None)

    def test_best_effort_emit_after_close(self, ew):
        """After close(), _emit raises RuntimeError."""
        ew.close()
        with pytest.raises(RuntimeError):
            ew._emit("skip", {"reason": "too late"})

    def test_fill_event_with_per_leg_detail(self, ew):
        """Fill event preserves per-leg detail in payload."""
        legs = [
            {"osi": "SPXW260316P06000000", "option_type": "PUT", "qty": 1},
            {"osi": "SPXW260316P05950000", "option_type": "PUT", "qty": 1},
        ]
        ew.fill(order_id="ORD99", fill_qty=1, fill_price=2.30, legs=legs)
        ew.close()

        events = _read_events(ew)
        assert len(events) == 1
        payload = events[0]["payload"]
        assert payload["legs"] == legs
        assert len(payload["legs"]) == 2
        assert payload["legs"][0]["osi"] == "SPXW260316P06000000"

    def test_idempotency_keys_unique_across_accounts(self, tmp_path):
        """Two writers with different accounts produce different idempotency keys for same payload."""
        os.environ["GAMMA_EVENT_DIR"] = str(tmp_path)
        try:
            ew_a = EventWriter(
                strategy="test", account="acct_A", trade_date=date(2026, 3, 12),
            )
            ew_b = EventWriter(
                strategy="test", account="acct_B", trade_date=date(2026, 3, 12),
            )

            ev_a = ew_a.skip(reason="WEEKEND")
            ev_b = ew_b.skip(reason="WEEKEND")

            assert ev_a.idempotency_key != ev_b.idempotency_key, (
                "idempotency keys must differ across accounts"
            )

            ew_a.close()
            ew_b.close()
        finally:
            os.environ.pop("GAMMA_EVENT_DIR", None)
