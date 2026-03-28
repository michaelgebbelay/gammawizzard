"""Schwab broker sync — pull orders, fills, positions, and balances.

Stores raw broker payloads unchanged in broker_raw_* tables, then
normalizes into canonical records with source-specific cleanup.

Usage:
    from reporting.broker_sync_schwab import sync_schwab
    stats = sync_schwab(con=con, as_of_date=date.today())

Requires:
    SCHWAB_APP_KEY, SCHWAB_APP_SECRET, SCHWAB_TOKEN_PATH (or SCHWAB_TOKEN_JSON)
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from reporting.db import execute, get_connection, init_schema, query_df, query_one


# ---------------------------------------------------------------------------
# Schwab client resolution
# ---------------------------------------------------------------------------

def _get_schwab_client():
    """Import and return a Schwab client instance.

    Tries repo's schwab_token_keeper first, then falls back to raw schwab SDK.
    """
    # Try the repo's schwab_token_keeper
    repo_root = Path(__file__).resolve().parent.parent
    scripts_dir = repo_root / "scripts"
    if str(scripts_dir) not in sys.path:
        sys.path.insert(0, str(scripts_dir))

    try:
        from schwab_token_keeper import schwab_client
        return schwab_client()
    except Exception:
        pass

    # Fallback: direct SDK
    try:
        import schwab
        from schwab.auth import client_from_token_file
        token_path = os.environ.get("SCHWAB_TOKEN_PATH", "/tmp/schwab_token.json")
        app_key = os.environ["SCHWAB_APP_KEY"]
        app_secret = os.environ["SCHWAB_APP_SECRET"]
        return client_from_token_file(token_path, app_key, app_secret)
    except Exception as e:
        raise RuntimeError(f"Cannot create Schwab client: {e}")


def _get_account_hash(c) -> str:
    resp = c.get_account_numbers()
    resp.raise_for_status()
    arr = resp.json() or []
    return str(arr[0]["hashValue"]) if arr else ""


# ---------------------------------------------------------------------------
# Idempotency key generation
# ---------------------------------------------------------------------------

def _idem_key(broker: str, entity_type: str, payload: dict) -> str:
    """Deterministic key from broker + entity + canonical payload hash."""
    canonical = json.dumps(
        {"b": broker, "t": entity_type, "p": payload},
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode()).hexdigest()[:20]


# ---------------------------------------------------------------------------
# Canonical normalization helpers
# ---------------------------------------------------------------------------

def _normalize_order_status(raw_status: str) -> str:
    """Map Schwab order status to canonical enum."""
    mapping = {
        "FILLED": "FILLED",
        "CANCELED": "CANCELED",
        "REJECTED": "REJECTED",
        "EXPIRED": "EXPIRED",
        "WORKING": "WORKING",
        "PENDING_ACTIVATION": "WORKING",
        "QUEUED": "WORKING",
        "ACCEPTED": "WORKING",
        "AWAITING_PARENT_ORDER": "WORKING",
        "AWAITING_CONDITION": "WORKING",
        "PENDING_CANCEL": "CANCELED",
        "PENDING_REPLACE": "WORKING",
        "REPLACED": "CANCELED",
    }
    return mapping.get(raw_status.upper(), raw_status.upper())


def _extract_legs_from_order(order: dict) -> list[dict]:
    """Extract normalized leg info from a Schwab order."""
    legs = []
    for leg in order.get("orderLegCollection", []):
        inst = leg.get("instrument", {})
        osi = (inst.get("symbol") or "").strip()
        legs.append({
            "osi": osi,
            "option_type": inst.get("putCall", inst.get("type", "")),
            "strike": inst.get("strikePrice"),
            "action": leg.get("instruction", ""),
            "qty": leg.get("quantity", 0),
        })
    return legs


def _extract_fills_from_order(order: dict) -> list[dict]:
    """Extract fill details from a Schwab order's activity collection."""
    fills = []
    for activity in order.get("orderActivityCollection", []):
        if str(activity.get("activityType", "")).upper() != "EXECUTION":
            continue
        exec_legs = activity.get("executionLegs", [])
        for exec_leg in exec_legs:
            fills.append({
                "leg_id": exec_leg.get("legId"),
                "qty": exec_leg.get("quantity", 0),
                "price": exec_leg.get("price", 0),
                "time": exec_leg.get("time", ""),
            })
    return fills


# ---------------------------------------------------------------------------
# Sync: Orders
# ---------------------------------------------------------------------------

def _sync_orders(c, acct_hash: str, con, since_date: date) -> dict:
    """Fetch recent orders from Schwab and store raw + normalize."""
    stats = {"fetched": 0, "inserted": 0, "duplicates": 0}

    resp = c.get_orders_for_account(
        acct_hash,
        from_entered_datetime=datetime.combine(since_date, datetime.min.time()),
        to_entered_datetime=datetime.now(),
    )
    resp.raise_for_status()
    orders = resp.json() or []
    stats["fetched"] = len(orders)

    now = datetime.now(timezone.utc).isoformat()

    for order in orders:
        order_id = str(order.get("orderId", ""))
        if not order_id:
            continue

        # Store raw order
        idem = _idem_key("schwab", "order", {
            "order_id": order_id,
            "status": order.get("status", ""),
            "filled_qty": order.get("filledQuantity", 0),
        })

        existing = query_one(
            "SELECT 1 FROM broker_raw_orders WHERE idempotency_key = ?",
            [idem], con=con,
        )
        if existing:
            stats["duplicates"] += 1
            continue

        raw_id = uuid.uuid4().hex[:16]
        execute(
            """INSERT INTO broker_raw_orders
               (id, broker, account, order_id, fetched_at, as_of, raw_payload, idempotency_key)
               VALUES (?, 'schwab', 'schwab', ?, ?, ?, ?, ?)""",
            [raw_id, order_id, now, order.get("enteredTime", now),
             json.dumps(order), idem],
            con=con,
        )
        stats["inserted"] += 1

        # Extract and store fills
        fills = _extract_fills_from_order(order)
        for fill in fills:
            fill_idem = _idem_key("schwab", "fill", {
                "order_id": order_id,
                "leg_id": fill.get("leg_id"),
                "qty": fill.get("qty"),
                "price": fill.get("price"),
                "time": fill.get("time"),
            })

            fill_existing = query_one(
                "SELECT 1 FROM broker_raw_fills WHERE idempotency_key = ?",
                [fill_idem], con=con,
            )
            if fill_existing:
                continue

            fill_id = uuid.uuid4().hex[:16]
            execute(
                """INSERT INTO broker_raw_fills
                   (id, broker, account, order_id, fill_id, fetched_at, as_of,
                    raw_payload, idempotency_key)
                   VALUES (?, 'schwab', 'schwab', ?, ?, ?, ?, ?, ?)""",
                [fill_id, order_id, str(fill.get("leg_id", "")), now,
                 fill.get("time", now), json.dumps(fill), fill_idem],
                con=con,
            )

    return stats


# ---------------------------------------------------------------------------
# Sync: Positions
# ---------------------------------------------------------------------------

def _sync_positions(c, acct_hash: str, con) -> dict:
    """Fetch current positions from Schwab and store raw snapshot."""
    stats = {"positions": 0}

    url = f"https://api.schwabapi.com/trader/v1/accounts/{acct_hash}"
    resp = c.session.get(url, params={"fields": "positions"}, timeout=20)
    if resp.status_code != 200:
        stats["error"] = f"HTTP {resp.status_code}"
        return stats

    data = resp.json()
    sa = data[0]["securitiesAccount"] if isinstance(data, list) else (data.get("securitiesAccount") or data)
    positions = sa.get("positions", [])
    stats["positions"] = len(positions)

    now = datetime.now(timezone.utc).isoformat()
    raw_id = uuid.uuid4().hex[:16]
    execute(
        """INSERT INTO broker_raw_positions
           (id, broker, account, fetched_at, as_of, raw_payload)
           VALUES (?, 'schwab', 'schwab', ?, ?, ?)""",
        [raw_id, now, now, json.dumps({"positions": positions, "account": sa.get("accountNumber", "")})],
        con=con,
    )

    return stats


# ---------------------------------------------------------------------------
# Sync: Cash / Balances
# ---------------------------------------------------------------------------

def _sync_cash(c, acct_hash: str, con, as_of_date: date | None = None) -> dict:
    """Fetch account balances from Schwab and store raw snapshot."""
    stats = {}

    resp = c.get_account(acct_hash)
    resp.raise_for_status()
    data = resp.json()
    sa = data.get("securitiesAccount") or data
    if isinstance(data, list):
        sa = data[0].get("securitiesAccount") or data[0]

    balances = {}
    for key in ("currentBalances", "initialBalances", "projectedBalances"):
        if key in sa:
            balances[key] = sa[key]

    now = datetime.now(timezone.utc).isoformat()
    raw_id = uuid.uuid4().hex[:16]
    execute(
        """INSERT INTO broker_raw_cash
           (id, broker, account, fetched_at, as_of, raw_payload)
           VALUES (?, 'schwab', 'schwab', ?, ?, ?)""",
        [raw_id, now, now, json.dumps(balances)],
        con=con,
    )

    # Materialize account snapshot
    current = balances.get("currentBalances", {})
    cash = current.get("cashBalance") or current.get("cashAvailableForTrading")
    net_liq = current.get("liquidationValue")
    buying_power = current.get("buyingPower") or current.get("availableFundsNonMarginableTrade")

    if net_liq is not None:
        snap_date = (as_of_date or date.today()).isoformat()
        existing = query_one(
            "SELECT 1 FROM account_snapshots WHERE account = 'schwab' AND snapshot_date = ?",
            [snap_date], con=con,
        )
        if not existing:
            snap_id = uuid.uuid4().hex[:16]
            execute(
                """INSERT INTO account_snapshots
                   (id, account, snapshot_date, as_of, cash, net_liq, buying_power, source)
                   VALUES (?, 'schwab', ?, ?, ?, ?, ?, 'broker_sync')""",
                [snap_id, snap_date, now, cash, net_liq, buying_power],
                con=con,
            )

    stats["cash"] = cash
    stats["net_liq"] = net_liq
    stats["buying_power"] = buying_power
    return stats


# ---------------------------------------------------------------------------
# Source freshness tracking
# ---------------------------------------------------------------------------

def _update_freshness(con, source_name: str, success: bool, error_msg: str = ""):
    """Update source_freshness table."""
    now = datetime.now(timezone.utc).isoformat()
    existing = query_one(
        "SELECT source_name FROM source_freshness WHERE source_name = ?",
        [source_name], con=con,
    )
    if existing:
        if success:
            execute(
                """UPDATE source_freshness
                   SET last_success_at = ?, source_asof = ?, is_stale = false, error_message = NULL
                   WHERE source_name = ?""",
                [now, now, source_name], con=con,
            )
        else:
            execute(
                """UPDATE source_freshness
                   SET is_stale = true, error_message = ?
                   WHERE source_name = ?""",
                [error_msg, source_name], con=con,
            )
    else:
        execute(
            """INSERT INTO source_freshness
               (source_name, last_success_at, source_asof, sla_minutes, is_stale, error_message)
               VALUES (?, ?, ?, 60, ?, ?)""",
            [source_name, now if success else None, now if success else None,
             not success, error_msg or None],
            con=con,
        )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def sync_schwab(
    con=None,
    as_of_date: date | None = None,
    lookback_days: int = 7,
) -> dict:
    """Run full Schwab broker sync.

    Fetches orders (last N days), current positions, and balances.
    Stores raw payloads and materializes account snapshots.

    Returns aggregate stats.
    """
    if con is None:
        con = get_connection()
        init_schema(con)

    if as_of_date is None:
        as_of_date = date.today()

    since_date = as_of_date - timedelta(days=lookback_days)

    stats = {"orders": {}, "positions": {}, "cash": {}}

    try:
        c = _get_schwab_client()
        acct_hash = _get_account_hash(c)

        if not acct_hash:
            _update_freshness(con, "schwab_orders", False, "no account hash")
            return {"error": "no account hash"}

        # Orders + fills
        try:
            stats["orders"] = _sync_orders(c, acct_hash, con, since_date)
            _update_freshness(con, "schwab_orders", True)
        except Exception as e:
            stats["orders"] = {"error": str(e)}
            _update_freshness(con, "schwab_orders", False, str(e))

        # Positions
        try:
            stats["positions"] = _sync_positions(c, acct_hash, con)
            _update_freshness(con, "schwab_positions", True)
        except Exception as e:
            stats["positions"] = {"error": str(e)}
            _update_freshness(con, "schwab_positions", False, str(e))

        # Cash / balances
        try:
            stats["cash"] = _sync_cash(c, acct_hash, con, as_of_date=as_of_date)
            _update_freshness(con, "schwab_cash", True)
        except Exception as e:
            stats["cash"] = {"error": str(e)}
            _update_freshness(con, "schwab_cash", False, str(e))

    except Exception as e:
        stats["error"] = str(e)
        for src in ("schwab_orders", "schwab_positions", "schwab_cash"):
            _update_freshness(con, src, False, str(e))

    return stats
