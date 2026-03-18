"""Immutable event writer for strategy runs, orders, fills, and lifecycle events.

Every BF/DS/CS run emits structured events through this module.
Events are append-only JSON-lines files stored in S3 or local disk.

Usage from orchestrators:
    from reporting.events import EventWriter

    ew = EventWriter(strategy="butterfly", account="schwab")
    ew.strategy_run(signal="BUY", config="4DTE_D20P_WINGS", reason="VIX1D=15.2")
    ew.trade_intent(side="DEBIT", legs=[...], target_qty=1)
    ew.order_submitted(order_id="12345", legs=[...], limit_price=2.50)
    ew.fill(order_id="12345", fill_qty=1, fill_price=2.45)
    ew.close()
"""

from __future__ import annotations

import hashlib
import json
import os
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# Event envelope
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Event:
    """Immutable event record. Every event carries full provenance."""

    event_id: str
    event_type: str
    ts_utc: str
    strategy: str
    account: str
    trade_group_id: str
    run_id: str
    config_version: str
    payload: dict
    idempotency_key: str

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"), sort_keys=True)


# ---------------------------------------------------------------------------
# Event types (exhaustive enum)
# ---------------------------------------------------------------------------

STRATEGY_RUN = "strategy_run"
TRADE_INTENT = "trade_intent"
ORDER_SUBMITTED = "order_submitted"
ORDER_UPDATE = "order_update"
FILL = "fill"
SKIP = "skip"
ERROR = "error"
POST_STEP_RESULT = "post_step_result"
POSITION_CLOSE = "position_close"
CORRECTION = "correction"
MANUAL_ADJUSTMENT = "manual_adjustment"

ALL_EVENT_TYPES = frozenset({
    STRATEGY_RUN, TRADE_INTENT, ORDER_SUBMITTED, ORDER_UPDATE,
    FILL, SKIP, ERROR, POST_STEP_RESULT, POSITION_CLOSE,
    CORRECTION, MANUAL_ADJUSTMENT,
})


# ---------------------------------------------------------------------------
# Idempotency key generation
# ---------------------------------------------------------------------------

def _idem_key(
    event_type: str,
    payload: dict,
    strategy: str = "",
    account: str = "",
    run_id: str = "",
    trade_group_id: str = "",
) -> str:
    """Deterministic idempotency key from event context + payload.

    Includes strategy, account, run_id, and trade_group_id so that identical
    payloads from different runs/accounts never collide.
    """
    canonical = json.dumps(
        {
            "s": strategy,
            "a": account,
            "r": run_id,
            "g": trade_group_id,
            "t": event_type,
            "p": payload,
        },
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Config version detection
# ---------------------------------------------------------------------------

def _detect_config_version() -> str:
    """Best-effort config version from git SHA or deployment marker."""
    # Check Lambda deployment marker first
    marker = os.environ.get("GAMMA_DEPLOY_SHA")
    if marker:
        return marker[:12]

    # Try git rev-parse
    try:
        import subprocess
        result = subprocess.run(
            ["git", "rev-parse", "--short=12", "HEAD"],
            capture_output=True, text=True, timeout=2,
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except Exception:
        pass

    return "unknown"


# ---------------------------------------------------------------------------
# EventWriter
# ---------------------------------------------------------------------------

class EventWriter:
    """Append-only event writer for a single strategy run.

    Creates a stable run_id and trade_group_id before any events are emitted.
    Events are buffered in memory and flushed to a JSONL file on close() or
    when the buffer exceeds max_buffer_size.

    Storage locations (checked in order):
      1. GAMMA_EVENT_DIR env var (local directory)
      2. /tmp/gamma_events/ (Lambda default)
      3. S3 bucket via GAMMA_EVENT_BUCKET env var (future)
    """

    def __init__(
        self,
        strategy: str,
        account: str,
        trade_date: date | None = None,
        config_version: str | None = None,
        max_buffer_size: int = 50,
    ):
        self.strategy = strategy
        self.account = account
        self.trade_date = trade_date or date.today()
        self.run_id = uuid.uuid4().hex[:16]
        self.trade_group_id = uuid.uuid4().hex[:16]
        self.config_version = config_version or _detect_config_version()
        self._buffer: list[Event] = []
        self._max_buffer = max_buffer_size
        self._closed = False

        # Resolve output path
        event_dir = os.environ.get(
            "GAMMA_EVENT_DIR",
            "/tmp/gamma_events",
        )
        self._output_dir = Path(event_dir) / self.trade_date.isoformat()
        self._output_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{self.strategy}_{self.account}_{self.run_id}.jsonl"
        self._output_path = self._output_dir / filename

    # -- Core emit ----------------------------------------------------------

    def _emit(self, event_type: str, payload: dict) -> Event:
        if self._closed:
            raise RuntimeError("EventWriter is closed")

        if event_type not in ALL_EVENT_TYPES:
            raise ValueError(f"Unknown event type: {event_type}")

        ev = Event(
            event_id=uuid.uuid4().hex[:16],
            event_type=event_type,
            ts_utc=datetime.now(timezone.utc).isoformat(),
            strategy=self.strategy,
            account=self.account,
            trade_group_id=self.trade_group_id,
            run_id=self.run_id,
            config_version=self.config_version,
            payload=payload,
            idempotency_key=_idem_key(
                event_type, payload,
                strategy=self.strategy,
                account=self.account,
                run_id=self.run_id,
                trade_group_id=self.trade_group_id,
            ),
        )
        self._buffer.append(ev)

        if len(self._buffer) >= self._max_buffer:
            self.flush()

        return ev

    def flush(self) -> int:
        """Write buffered events to disk. Returns count written."""
        if not self._buffer:
            return 0

        with open(self._output_path, "a") as f:
            for ev in self._buffer:
                f.write(ev.to_json() + "\n")

        count = len(self._buffer)
        self._buffer.clear()
        return count

    def close(self) -> Path:
        """Flush remaining events and mark writer as closed."""
        self.flush()
        self._closed = True
        return self._output_path

    # -- Typed event helpers ------------------------------------------------

    def strategy_run(
        self,
        signal: str,
        config: str,
        reason: str,
        spot: float = 0.0,
        vix: float = 0.0,
        vix1d: float = 0.0,
        filters: dict | None = None,
        extra: dict | None = None,
    ) -> Event:
        """Emit a strategy_run event at the start of each orchestrator run."""
        payload = {
            "trade_date": self.trade_date.isoformat(),
            "signal": signal,
            "config": config,
            "reason": reason,
            "spot": spot,
            "vix": vix,
            "vix1d": vix1d,
            "filters": filters or {},
        }
        if extra:
            payload.update(extra)
        return self._emit(STRATEGY_RUN, payload)

    def trade_intent(
        self,
        side: str,
        direction: str,
        legs: list[dict],
        target_qty: int,
        limit_price: float = 0.0,
        extra: dict | None = None,
    ) -> Event:
        """Emit before any order is placed — what the strategy wants to do."""
        payload = {
            "trade_date": self.trade_date.isoformat(),
            "side": side,
            "direction": direction,
            "legs": legs,
            "target_qty": target_qty,
            "limit_price": limit_price,
        }
        if extra:
            payload.update(extra)
        return self._emit(TRADE_INTENT, payload)

    def order_submitted(
        self,
        order_id: str,
        legs: list[dict],
        limit_price: float,
        order_type: str = "LIMIT",
        extra: dict | None = None,
    ) -> Event:
        """Emit when an order is actually submitted to the broker."""
        payload = {
            "trade_date": self.trade_date.isoformat(),
            "order_id": str(order_id),
            "legs": legs,
            "limit_price": limit_price,
            "order_type": order_type,
        }
        if extra:
            payload.update(extra)
        return self._emit(ORDER_SUBMITTED, payload)

    def order_update(
        self,
        order_id: str,
        status: str,
        filled_qty: int = 0,
        remaining_qty: int = 0,
        extra: dict | None = None,
    ) -> Event:
        """Emit on any order status change (working, partial, canceled, etc)."""
        payload = {
            "order_id": str(order_id),
            "status": status,
            "filled_qty": filled_qty,
            "remaining_qty": remaining_qty,
        }
        if extra:
            payload.update(extra)
        return self._emit(ORDER_UPDATE, payload)

    def fill(
        self,
        order_id: str,
        fill_qty: int,
        fill_price: float,
        legs: list[dict] | None = None,
        extra: dict | None = None,
    ) -> Event:
        """Emit when an order is filled (full or partial)."""
        payload = {
            "order_id": str(order_id),
            "fill_qty": fill_qty,
            "fill_price": fill_price,
        }
        if legs:
            payload["legs"] = legs
        if extra:
            payload.update(extra)
        return self._emit(FILL, payload)

    def skip(
        self,
        reason: str,
        signal: str = "",
        extra: dict | None = None,
    ) -> Event:
        """Emit when a strategy decides not to trade."""
        payload = {
            "trade_date": self.trade_date.isoformat(),
            "reason": reason,
            "signal": signal,
        }
        if extra:
            payload.update(extra)
        return self._emit(SKIP, payload)

    def error(
        self,
        message: str,
        stage: str = "",
        extra: dict | None = None,
    ) -> Event:
        """Emit on any error during the run."""
        payload = {
            "trade_date": self.trade_date.isoformat(),
            "message": message,
            "stage": stage,
        }
        if extra:
            payload.update(extra)
        return self._emit(ERROR, payload)

    def post_step_result(
        self,
        step_name: str,
        outcome: str,
        extra: dict | None = None,
    ) -> Event:
        """Emit after each post-trade step completes."""
        payload = {
            "step_name": step_name,
            "outcome": outcome,
        }
        if extra:
            payload.update(extra)
        return self._emit(POST_STEP_RESULT, payload)

    def position_close(
        self,
        reason: str,
        fill_price: float = 0.0,
        pnl: float = 0.0,
        extra: dict | None = None,
    ) -> Event:
        """Emit when a position is closed (expiry, target, stop, manual)."""
        payload = {
            "trade_date": self.trade_date.isoformat(),
            "reason": reason,
            "fill_price": fill_price,
            "pnl": pnl,
        }
        if extra:
            payload.update(extra)
        return self._emit(POSITION_CLOSE, payload)

    # -- New trade group ----------------------------------------------------

    def new_trade_group(self) -> str:
        """Generate a new trade_group_id for a second trade in the same run.

        Use this when a single orchestrator run places multiple independent
        trades (e.g. DS places both a PUT and CALL vertical).
        """
        self.trade_group_id = uuid.uuid4().hex[:16]
        return self.trade_group_id

    # -- Context manager ----------------------------------------------------

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.error(
                message=str(exc_val),
                stage="unhandled_exception",
            )
        self.close()
        return False
