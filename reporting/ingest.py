"""Ingest raw event JSONL files into the canonical DuckDB store.

Reads JSONL files from the event directory, deduplicates by idempotency_key,
and materializes into normalized tables (strategy_runs, intended_trades,
order_events, fills).

Usage:
    from reporting.ingest import ingest_events

    # Ingest all events for a date
    stats = ingest_events("2026-03-12")

    # Ingest all pending event files
    stats = ingest_all_pending()
"""

from __future__ import annotations

import json
import os
from datetime import date, datetime
from pathlib import Path

import boto3

from reporting.db import execute, get_connection, init_schema, query_one


# ---------------------------------------------------------------------------
# Event directory resolution
# ---------------------------------------------------------------------------

def _event_dir() -> Path:
    return Path(os.environ.get("GAMMA_EVENT_DIR", "/tmp/gamma_events"))


def _event_bucket() -> str:
    return (
        os.environ.get("GAMMA_EVENT_BUCKET")
        or os.environ.get("SIM_CACHE_BUCKET")
        or ""
    ).strip()


def _event_prefix() -> str:
    return (os.environ.get("GAMMA_EVENT_PREFIX") or "reporting/events").strip("/")


def sync_events_from_s3(
    start_date: str | date,
    end_date: str | date | None = None,
) -> dict:
    """Download event JSONL files from S3 into the local event directory.

    Objects are expected under:
      s3://<bucket>/<prefix>/YYYY-MM-DD/*.jsonl

    Returns aggregate stats. Missing bucket/prefix or auth issues are non-fatal.
    """
    if isinstance(start_date, date):
        start = start_date
    else:
        start = date.fromisoformat(start_date)

    if end_date is None:
        end = start
    elif isinstance(end_date, date):
        end = end_date
    else:
        end = date.fromisoformat(end_date)

    bucket = _event_bucket()
    if not bucket:
        return {
            "dates": 0,
            "objects": 0,
            "downloaded": 0,
            "skipped": 0,
            "errors": 0,
            "bucket": "",
        }

    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    base_dir = _event_dir()
    prefix_root = _event_prefix()

    stats = {
        "dates": 0,
        "objects": 0,
        "downloaded": 0,
        "skipped": 0,
        "errors": 0,
        "bucket": bucket,
    }

    cur = start
    while cur <= end:
        day = cur.isoformat()
        stats["dates"] += 1
        prefix = f"{prefix_root}/{day}/"
        try:
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    key = obj.get("Key", "")
                    if not key.endswith(".jsonl"):
                        continue
                    stats["objects"] += 1
                    local_path = base_dir / day / Path(key).name
                    local_path.parent.mkdir(parents=True, exist_ok=True)
                    size = int(obj.get("Size") or 0)
                    if local_path.exists() and local_path.stat().st_size == size:
                        stats["skipped"] += 1
                        continue
                    s3.download_file(bucket, key, str(local_path))
                    stats["downloaded"] += 1
        except Exception:
            stats["errors"] += 1
        cur = cur.fromordinal(cur.toordinal() + 1)

    return stats


# ---------------------------------------------------------------------------
# JSONL reader
# ---------------------------------------------------------------------------

def _read_jsonl(path: Path) -> list[dict]:
    """Read a JSONL file, skipping blank/malformed lines."""
    events = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return events


# ---------------------------------------------------------------------------
# Raw event ingest (idempotent)
# ---------------------------------------------------------------------------

def _ingest_raw_event(con, ev: dict) -> bool:
    """Insert a single event into raw_events if not already present.

    Returns True if inserted, False if duplicate.
    """
    idem = ev.get("idempotency_key", "")
    existing = query_one(
        "SELECT 1 FROM raw_events WHERE idempotency_key = ?",
        [idem], con=con,
    )
    if existing:
        return False

    execute(
        """INSERT INTO raw_events
           (event_id, event_type, ts_utc, strategy, account,
            trade_group_id, run_id, config_version, payload,
            idempotency_key)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            ev["event_id"],
            ev["event_type"],
            ev["ts_utc"],
            ev["strategy"],
            ev["account"],
            ev["trade_group_id"],
            ev["run_id"],
            ev["config_version"],
            json.dumps(ev["payload"]),
            idem,
        ],
        con=con,
    )
    return True


# ---------------------------------------------------------------------------
# Materializers: raw_events → normalized tables
# ---------------------------------------------------------------------------

def _materialize_strategy_run(con, ev: dict) -> None:
    """Upsert a strategy_run record from a strategy_run event."""
    p = ev["payload"]
    existing = query_one(
        "SELECT 1 FROM strategy_runs WHERE run_id = ?",
        [ev["run_id"]], con=con,
    )
    if existing:
        return

    execute(
        """INSERT INTO strategy_runs
           (run_id, strategy, account, trade_date, config_version,
            signal, config, reason, spot, vix, vix1d, filters,
            status, started_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'RUNNING', ?)""",
        [
            ev["run_id"],
            ev["strategy"],
            ev["account"],
            p.get("trade_date"),
            ev["config_version"],
            p.get("signal"),
            p.get("config"),
            p.get("reason"),
            p.get("spot", 0),
            p.get("vix", 0),
            p.get("vix1d", 0),
            json.dumps(p.get("filters", {})),
            ev["ts_utc"],
        ],
        con=con,
    )


def _materialize_trade_intent(con, ev: dict) -> None:
    """Insert an intended_trades record from a trade_intent event."""
    p = ev["payload"]
    existing = query_one(
        "SELECT 1 FROM intended_trades WHERE intent_id = ?",
        [ev["event_id"]], con=con,
    )
    if existing:
        return

    execute(
        """INSERT INTO intended_trades
           (intent_id, run_id, trade_group_id, strategy, account,
            trade_date, side, direction, legs, target_qty, limit_price)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            ev["event_id"],
            ev["run_id"],
            ev["trade_group_id"],
            ev["strategy"],
            ev["account"],
            p.get("trade_date"),
            p.get("side"),
            p.get("direction"),
            json.dumps(p.get("legs", [])),
            p.get("target_qty", 0),
            p.get("limit_price", 0),
        ],
        con=con,
    )


def _materialize_order_submitted(con, ev: dict) -> None:
    """Insert an order_events record from an order_submitted event."""
    p = ev["payload"]
    existing = query_one(
        "SELECT 1 FROM order_events WHERE event_id = ?",
        [ev["event_id"]], con=con,
    )
    if existing:
        return

    execute(
        """INSERT INTO order_events
           (event_id, trade_group_id, run_id, order_id, ts_utc,
            event_type, legs, limit_price, order_type)
           VALUES (?, ?, ?, ?, ?, 'submitted', ?, ?, ?)""",
        [
            ev["event_id"],
            ev["trade_group_id"],
            ev["run_id"],
            p.get("order_id", ""),
            ev["ts_utc"],
            json.dumps(p.get("legs", [])),
            p.get("limit_price", 0),
            p.get("order_type", "LIMIT"),
        ],
        con=con,
    )


def _materialize_order_update(con, ev: dict) -> None:
    """Insert an order_events record from an order_update event."""
    p = ev["payload"]
    existing = query_one(
        "SELECT 1 FROM order_events WHERE event_id = ?",
        [ev["event_id"]], con=con,
    )
    if existing:
        return

    execute(
        """INSERT INTO order_events
           (event_id, trade_group_id, run_id, order_id, ts_utc,
            event_type, filled_qty, remaining_qty)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            ev["event_id"],
            ev["trade_group_id"],
            ev["run_id"],
            p.get("order_id", ""),
            ev["ts_utc"],
            p.get("status", "unknown"),
            p.get("filled_qty", 0),
            p.get("remaining_qty", 0),
        ],
        con=con,
    )


def _materialize_fill(con, ev: dict) -> None:
    """Insert a fills record from a fill event."""
    p = ev["payload"]
    existing = query_one(
        "SELECT 1 FROM fills WHERE fill_id = ?",
        [ev["event_id"]], con=con,
    )
    if existing:
        return

    execute(
        """INSERT INTO fills
           (fill_id, trade_group_id, run_id, order_id, ts_utc,
            fill_qty, fill_price, legs, source)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'internal')""",
        [
            ev["event_id"],
            ev["trade_group_id"],
            ev["run_id"],
            p.get("order_id", ""),
            ev["ts_utc"],
            p.get("fill_qty", 0),
            p.get("fill_price", 0),
            json.dumps(p.get("legs")) if p.get("legs") else None,
        ],
        con=con,
    )

    # Update intended_trades outcome
    execute(
        """UPDATE intended_trades SET outcome = 'FILLED'
           WHERE trade_group_id = ? AND outcome = 'PENDING'""",
        [ev["trade_group_id"]],
        con=con,
    )


def _materialize_skip(con, ev: dict) -> None:
    """Mark the strategy run as SKIPPED."""
    execute(
        """UPDATE strategy_runs SET status = 'SKIPPED', completed_at = ?
           WHERE run_id = ? AND status = 'RUNNING'""",
        [ev["ts_utc"], ev["run_id"]],
        con=con,
    )


def _materialize_error(con, ev: dict) -> None:
    """Mark the strategy run as ERROR."""
    execute(
        """UPDATE strategy_runs SET status = 'ERROR', completed_at = ?
           WHERE run_id = ? AND status = 'RUNNING'""",
        [ev["ts_utc"], ev["run_id"]],
        con=con,
    )


def _materialize_post_step(con, ev: dict) -> None:
    """Append post-step result to the strategy run's post_results JSON."""
    p = ev["payload"]
    row = query_one(
        "SELECT post_results FROM strategy_runs WHERE run_id = ?",
        [ev["run_id"]], con=con,
    )
    if not row:
        return

    existing = json.loads(row[0]) if row[0] else {}
    existing[p.get("step_name", "unknown")] = p.get("outcome", "unknown")

    execute(
        """UPDATE strategy_runs SET post_results = ?, status = 'COMPLETED',
           completed_at = ?
           WHERE run_id = ?""",
        [json.dumps(existing), ev["ts_utc"], ev["run_id"]],
        con=con,
    )


# Dispatch table
_MATERIALIZERS = {
    "strategy_run": _materialize_strategy_run,
    "trade_intent": _materialize_trade_intent,
    "order_submitted": _materialize_order_submitted,
    "order_update": _materialize_order_update,
    "fill": _materialize_fill,
    "skip": _materialize_skip,
    "error": _materialize_error,
    "post_step_result": _materialize_post_step,
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def ingest_events(
    trade_date: str | date,
    con=None,
) -> dict:
    """Ingest all JSONL event files for a given trade date.

    Returns stats: {files, events_read, inserted, duplicates, materialized}.
    """
    if con is None:
        con = get_connection()
        init_schema(con)

    if isinstance(trade_date, date):
        trade_date = trade_date.isoformat()

    sync_events_from_s3(trade_date)
    event_path = _event_dir() / trade_date
    if not event_path.exists():
        return {"files": 0, "events_read": 0, "inserted": 0,
                "duplicates": 0, "materialized": 0}

    stats = {"files": 0, "events_read": 0, "inserted": 0,
             "duplicates": 0, "materialized": 0}

    for jsonl_file in sorted(event_path.glob("*.jsonl")):
        stats["files"] += 1
        events = _read_jsonl(jsonl_file)

        for ev in events:
            stats["events_read"] += 1

            # 1. Insert raw event (deduplicated)
            inserted = _ingest_raw_event(con, ev)
            if inserted:
                stats["inserted"] += 1
            else:
                stats["duplicates"] += 1
                continue  # Skip materialization for duplicates

            # 2. Materialize into normalized tables
            materializer = _MATERIALIZERS.get(ev.get("event_type"))
            if materializer:
                materializer(con, ev)
                stats["materialized"] += 1

    return stats


def ingest_all_pending(con=None) -> dict:
    """Ingest all event files across all dates in the event directory.

    Returns aggregate stats.
    """
    if con is None:
        con = get_connection()
        init_schema(con)

    event_base = _event_dir()
    if not event_base.exists():
        return {"dates": 0, "files": 0, "events_read": 0,
                "inserted": 0, "duplicates": 0, "materialized": 0}

    totals = {"dates": 0, "files": 0, "events_read": 0,
              "inserted": 0, "duplicates": 0, "materialized": 0}

    for date_dir in sorted(event_base.iterdir()):
        if not date_dir.is_dir():
            continue
        # Validate date format
        try:
            datetime.strptime(date_dir.name, "%Y-%m-%d")
        except ValueError:
            continue

        totals["dates"] += 1
        stats = ingest_events(date_dir.name, con=con)
        for k in ("files", "events_read", "inserted", "duplicates", "materialized"):
            totals[k] += stats[k]

    return totals
