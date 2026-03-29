"""TastyTrade broker sync — pull orders, positions, and balances.

Stores raw broker payloads unchanged in broker_raw_* tables, then
materializes account snapshots. Mirrors the Schwab sync pattern.

Syncs BOTH TT accounts:
  - tt-ira        (5WT20360)
  - tt-individual (5WT09219)

Usage:
    from reporting.broker_sync_tt import sync_tt
    stats = sync_tt()

Requires:
    TT_CLIENT_ID, TT_CLIENT_SECRET, TT_TOKEN_PATH (or default TT/Token/tt_token.json)
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
# TT client resolution
# ---------------------------------------------------------------------------

TT_ACCOUNTS = {
    "5WT20360": "tt-ira",
    "5WT09219": "tt-individual",
}


def _get_tt_request():
    """Import and return the TT request function."""
    repo_root = Path(__file__).resolve().parent.parent
    tt_script = repo_root / "TT" / "Script"
    if str(tt_script) not in sys.path:
        sys.path.insert(0, str(tt_script))

    from tt_client import request as tt_request
    return tt_request


def _tt_get(path: str, tt_request) -> Any:
    """Make a GET request to the TT API, return parsed JSON."""
    resp = tt_request("GET", path)
    data = resp.json()
    # TT wraps responses in {"data": {...}} or {"data": {"items": [...]}}
    if isinstance(data, dict) and "data" in data:
        return data["data"]
    return data


# ---------------------------------------------------------------------------
# Idempotency key generation (same pattern as Schwab sync)
# ---------------------------------------------------------------------------

def _idem_key(broker: str, entity_type: str, payload: dict) -> str:
    canonical = json.dumps(
        {"b": broker, "t": entity_type, "p": payload},
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode()).hexdigest()[:20]


# ---------------------------------------------------------------------------
# TT field helpers (kebab-case)
# ---------------------------------------------------------------------------

def _get(obj: dict, *keys, default=None):
    """Try multiple key variants (kebab-case, snake_case, camelCase)."""
    for k in keys:
        if k in obj:
            return obj[k]
    return default


# ---------------------------------------------------------------------------
# Sync: Orders
# ---------------------------------------------------------------------------

def _sync_orders(acct_num: str, account_label: str, tt_request, con, since_date: date) -> dict:
    stats = {"fetched": 0, "inserted": 0, "duplicates": 0}

    data = _tt_get(f"/accounts/{acct_num}/orders", tt_request)
    items = data.get("items", data) if isinstance(data, dict) else data
    if not isinstance(items, list):
        items = []

    stats["fetched"] = len(items)
    now = datetime.now(timezone.utc).isoformat()

    for order in items:
        order_id = str(_get(order, "id", "order-id", default=""))
        if not order_id:
            continue

        # Filter by date
        entered = _get(order, "received-at", "created-at", "updated-at", default="")
        if entered:
            try:
                dt = datetime.fromisoformat(
                    entered.replace("Z", "+00:00").replace("+0000", "+00:00")
                )
                if dt.date() < since_date:
                    continue
            except Exception:
                pass

        status = _get(order, "status", default="")
        filled_qty = _get(order, "filled-quantity", "filledQuantity", "filled_quantity", default=0)

        idem = _idem_key("tastytrade", "order", {
            "order_id": order_id,
            "status": status,
            "filled_qty": filled_qty,
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
               VALUES (?, 'tastytrade', ?, ?, ?, ?, ?, ?)""",
            [raw_id, account_label, order_id, now, entered or now,
             json.dumps(order), idem],
            con=con,
        )
        stats["inserted"] += 1

        # Extract fills from legs if available
        legs = order.get("legs", [])
        for i, leg in enumerate(legs):
            fills = leg.get("fills", [])
            for fill in fills:
                fill_id_val = _get(fill, "id", "fill-id", "ext-group-fill-id", default=f"{order_id}-{i}")
                fill_idem = _idem_key("tastytrade", "fill", {
                    "order_id": order_id,
                    "fill_id": str(fill_id_val),
                    "qty": _get(fill, "quantity", "fill-quantity", default=0),
                    "price": _get(fill, "fill-price", "price", default=0),
                })

                fill_existing = query_one(
                    "SELECT 1 FROM broker_raw_fills WHERE idempotency_key = ?",
                    [fill_idem], con=con,
                )
                if fill_existing:
                    continue

                fid = uuid.uuid4().hex[:16]
                execute(
                    """INSERT INTO broker_raw_fills
                       (id, broker, account, order_id, fill_id, fetched_at, as_of,
                        raw_payload, idempotency_key)
                       VALUES (?, 'tastytrade', ?, ?, ?, ?, ?, ?, ?)""",
                    [fid, account_label, order_id, str(fill_id_val), now,
                     _get(fill, "filled-at", default=now),
                     json.dumps(fill), fill_idem],
                    con=con,
                )

    return stats


# ---------------------------------------------------------------------------
# Sync: Transactions (fills, fees, commissions)
# ---------------------------------------------------------------------------

def _sync_transactions(acct_num: str, account_label: str, tt_request, con, since_date: date) -> dict:
    stats = {"fetched": 0, "inserted": 0, "duplicates": 0}

    data = _tt_get(f"/accounts/{acct_num}/transactions", tt_request)
    items = data.get("items", data) if isinstance(data, dict) else data
    if not isinstance(items, list):
        items = []

    stats["fetched"] = len(items)
    now = datetime.now(timezone.utc).isoformat()

    for txn in items:
        txn_id = str(_get(txn, "id", default=""))
        if not txn_id:
            continue

        executed_at = _get(txn, "executed-at", "transaction-date", default="")
        if executed_at:
            try:
                dt = datetime.fromisoformat(
                    executed_at.replace("Z", "+00:00").replace("+0000", "+00:00")
                )
                if dt.date() < since_date:
                    continue
            except Exception:
                pass

        idem = _idem_key("tastytrade", "transaction", {
            "txn_id": txn_id,
            "account": acct_num,
        })

        existing = query_one(
            "SELECT 1 FROM broker_raw_fills WHERE idempotency_key = ?",
            [idem], con=con,
        )
        if existing:
            stats["duplicates"] += 1
            continue

        fid = uuid.uuid4().hex[:16]
        order_id = str(_get(txn, "order-id", default=""))
        execute(
            """INSERT INTO broker_raw_fills
               (id, broker, account, order_id, fill_id, fetched_at, as_of,
                raw_payload, idempotency_key)
               VALUES (?, 'tastytrade', ?, ?, ?, ?, ?, ?, ?)""",
            [fid, account_label, order_id, txn_id, now,
             executed_at or now, json.dumps(txn), idem],
            con=con,
        )
        stats["inserted"] += 1

    return stats


# ---------------------------------------------------------------------------
# Sync: Positions
# ---------------------------------------------------------------------------

def _sync_positions(acct_num: str, account_label: str, tt_request, con) -> dict:
    stats = {"positions": 0}

    data = _tt_get(f"/accounts/{acct_num}/positions", tt_request)
    items = data.get("items", data) if isinstance(data, dict) else data
    if not isinstance(items, list):
        items = []

    stats["positions"] = len(items)
    now = datetime.now(timezone.utc).isoformat()
    raw_id = uuid.uuid4().hex[:16]
    execute(
        """INSERT INTO broker_raw_positions
           (id, broker, account, fetched_at, as_of, raw_payload)
           VALUES (?, 'tastytrade', ?, ?, ?, ?)""",
        [raw_id, account_label, now, now,
         json.dumps({"positions": items, "account": acct_num})],
        con=con,
    )

    return stats


# ---------------------------------------------------------------------------
# Sync: Balances
# ---------------------------------------------------------------------------

def _sync_balances(acct_num: str, account_label: str, tt_request, con, as_of_date: date | None = None) -> dict:
    stats = {}

    data = _tt_get(f"/accounts/{acct_num}/balances", tt_request)
    if not isinstance(data, dict):
        stats["error"] = "unexpected response format"
        return stats

    now = datetime.now(timezone.utc).isoformat()
    raw_id = uuid.uuid4().hex[:16]
    execute(
        """INSERT INTO broker_raw_cash
           (id, broker, account, fetched_at, as_of, raw_payload)
           VALUES (?, 'tastytrade', ?, ?, ?, ?)""",
        [raw_id, account_label, now, now, json.dumps(data)],
        con=con,
    )

    # Materialize account snapshot
    cash = _get(data, "cash-balance", "cash_balance", default=None)
    net_liq = _get(data, "net-liquidating-value", "net_liquidating_value", default=None)
    buying_power = _get(data, "maintenance-excess", "buying-power", default=None)

    if cash is not None:
        try:
            cash = float(cash)
        except (TypeError, ValueError):
            cash = None
    if net_liq is not None:
        try:
            net_liq = float(net_liq)
        except (TypeError, ValueError):
            net_liq = None
    if buying_power is not None:
        try:
            buying_power = float(buying_power)
        except (TypeError, ValueError):
            buying_power = None

    if net_liq is not None:
        snap_date = (as_of_date or date.today()).isoformat()
        existing = query_one(
            "SELECT 1 FROM account_snapshots WHERE account = ? AND snapshot_date = ?",
            [account_label, snap_date], con=con,
        )
        if not existing:
            snap_id = uuid.uuid4().hex[:16]
            execute(
                """INSERT INTO account_snapshots
                   (id, account, snapshot_date, as_of, cash, net_liq, buying_power, source)
                   VALUES (?, ?, ?, ?, ?, ?, ?, 'broker_sync')""",
                [snap_id, account_label, snap_date, now, cash, net_liq, buying_power],
                con=con,
            )

    stats["cash"] = cash
    stats["net_liq"] = net_liq
    stats["buying_power"] = buying_power
    return stats


# ---------------------------------------------------------------------------
# Source freshness tracking (same pattern as Schwab sync)
# ---------------------------------------------------------------------------

def _update_freshness(con, source_name: str, success: bool, error_msg: str = ""):
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

def sync_tt(
    con=None,
    as_of_date: date | None = None,
    lookback_days: int = 7,
    accounts: dict[str, str] | None = None,
) -> dict:
    """Run full TastyTrade broker sync for all accounts.

    Fetches orders (last N days), transactions, current positions, and balances.
    Stores raw payloads and materializes account snapshots.

    Returns aggregate stats keyed by account label.
    """
    if con is None:
        con = get_connection()
        init_schema(con)

    if as_of_date is None:
        as_of_date = date.today()

    if accounts is None:
        accounts = TT_ACCOUNTS

    since_date = as_of_date - timedelta(days=lookback_days)
    all_stats: dict[str, dict] = {}

    try:
        tt_request = _get_tt_request()
    except Exception as e:
        for acct_num, label in accounts.items():
            all_stats[label] = {"error": str(e)}
            for src in (f"{label}_orders", f"{label}_positions", f"{label}_cash"):
                _update_freshness(con, src, False, str(e))
        return all_stats

    for acct_num, label in accounts.items():
        stats: dict[str, Any] = {"orders": {}, "transactions": {}, "positions": {}, "balances": {}}

        # Set the account env var for tt_client
        os.environ["TT_ACCOUNT_NUMBER"] = acct_num

        # Orders
        try:
            stats["orders"] = _sync_orders(acct_num, label, tt_request, con, since_date)
            _update_freshness(con, f"{label}_orders", True)
        except Exception as e:
            stats["orders"] = {"error": str(e)}
            _update_freshness(con, f"{label}_orders", False, str(e))

        # Transactions
        try:
            stats["transactions"] = _sync_transactions(acct_num, label, tt_request, con, since_date)
        except Exception as e:
            stats["transactions"] = {"error": str(e)}

        # Positions
        try:
            stats["positions"] = _sync_positions(acct_num, label, tt_request, con)
            _update_freshness(con, f"{label}_positions", True)
        except Exception as e:
            stats["positions"] = {"error": str(e)}
            _update_freshness(con, f"{label}_positions", False, str(e))

        # Balances
        try:
            stats["balances"] = _sync_balances(acct_num, label, tt_request, con, as_of_date=as_of_date)
            _update_freshness(con, f"{label}_cash", True)
        except Exception as e:
            stats["balances"] = {"error": str(e)}
            _update_freshness(con, f"{label}_cash", False, str(e))

        all_stats[label] = stats

    return all_stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="TastyTrade broker sync")
    parser.add_argument("--lookback", type=int, default=7, help="Days to look back")
    parser.add_argument("--account", type=str, default=None,
                        help="Sync single account (5WT20360 or 5WT09219)")
    args = parser.parse_args()

    accounts = TT_ACCOUNTS
    if args.account:
        if args.account in TT_ACCOUNTS:
            accounts = {args.account: TT_ACCOUNTS[args.account]}
        else:
            print(f"Unknown account: {args.account}. Available: {list(TT_ACCOUNTS.keys())}")
            sys.exit(1)

    stats = sync_tt(lookback_days=args.lookback, accounts=accounts)
    for label, s in stats.items():
        print(f"\n{label}:")
        for k, v in s.items():
            print(f"  {k}: {v}")
