"""Reconciliation engine v1.

Compares internal state (fills, positions, account snapshots) against
broker-reported data and flags discrepancies.

Checks implemented:
  1. fill_match    — internal fills vs broker_raw_fills
  2. position_match — open positions vs broker_raw_positions
  3. cash_match    — account_snapshots vs broker_raw_cash
  4. freshness     — source_freshness SLA enforcement

Results are written to reconciliation_runs + reconciliation_items.

Usage:
    from reporting.reconciliation import run_reconciliation

    stats = run_reconciliation(con=con, report_date=date.today())
"""

from __future__ import annotations

import json
import uuid
from datetime import date, datetime, timedelta, timezone

import pandas as pd

from reporting.db import execute, get_connection, query_df, query_one
from reporting.position_engine import transition_state


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _uuid() -> str:
    return uuid.uuid4().hex[:16]


def _broker_fetch_window(report_date: str) -> tuple[str, str]:
    """Return (start, end) UTC timestamps that cover broker fetches for a US trading date.

    Broker sync runs after US market close (4 PM ET = 20:00-21:00 UTC) but
    fetched_at is stored in UTC.  A run for report_date 2026-03-12 will have
    fetched_at around 2026-03-12 20:00 to 2026-03-13 06:00 UTC.

    We use a generous window: report_date 14:00 UTC through report_date+1 12:00 UTC.
    """
    d = date.fromisoformat(report_date)
    start = datetime(d.year, d.month, d.day, 14, 0, 0, tzinfo=timezone.utc)
    end = start + timedelta(hours=22)  # next day 12:00 UTC
    return start.isoformat(), end.isoformat()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _record_issue(
    con,
    recon_run_id: str,
    check_type: str,
    entity_type: str,
    severity: str,
    message: str,
    entity_id: str | None = None,
    internal_value: str | None = None,
    broker_value: str | None = None,
    classification: str | None = None,
    classification_reason: str | None = None,
) -> None:
    """Insert a single reconciliation_item."""
    execute(
        """INSERT INTO reconciliation_items
           (id, recon_run_id, check_type, entity_type, entity_id,
            severity, status, message, internal_value, broker_value,
            opened_at, classification, classification_reason)
           VALUES (?, ?, ?, ?, ?, ?, 'UNRESOLVED', ?, ?, ?, ?, ?, ?)""",
        [
            _uuid(),
            recon_run_id,
            check_type,
            entity_type,
            entity_id,
            severity,
            message,
            internal_value,
            broker_value,
            _now(),
            classification,
            classification_reason,
        ],
        con=con,
    )


# ---------------------------------------------------------------------------
# Check 1: Fill match
# ---------------------------------------------------------------------------

def _check_fill_match(con, recon_run_id: str, report_date: str) -> dict:
    """Compare internal fills against broker_raw_fills.

    For each broker fill, verify a matching internal fill exists (by order_id).
    For each internal fill, verify a matching broker fill exists.
    Flag quantity or price mismatches.
    """
    stats = {"checks": 0, "issues": 0}

    # Broker fills fetched in the window around this trading date
    fetch_start, fetch_end = _broker_fetch_window(report_date)
    broker_df = query_df(
        """SELECT order_id, fill_id, raw_payload
           FROM broker_raw_fills
           WHERE fetched_at >= ? AND fetched_at < ?""",
        [fetch_start, fetch_end], con=con,
    )

    # Internal fills from today's runs
    internal_df = query_df(
        """SELECT f.fill_id, f.order_id, f.fill_qty, f.fill_price
           FROM fills f
           JOIN strategy_runs sr ON f.run_id = sr.run_id
           WHERE sr.trade_date = ?""",
        [report_date], con=con,
    )

    stats["checks"] += 1

    if broker_df.empty and internal_df.empty:
        return stats

    # Build lookup: order_id -> list of internal fills
    internal_by_order: dict[str, list] = {}
    for _, row in internal_df.iterrows():
        oid = str(row["order_id"])
        internal_by_order.setdefault(oid, []).append(row)

    # Build lookup: order_id -> list of broker fills
    broker_by_order: dict[str, list] = {}
    for _, row in broker_df.iterrows():
        oid = str(row["order_id"])
        broker_by_order.setdefault(oid, []).append(row)

    # Check: broker fills without internal match
    for oid, broker_rows in broker_by_order.items():
        stats["checks"] += 1
        if oid not in internal_by_order:
            _record_issue(
                con, recon_run_id,
                check_type="fill_match",
                entity_type="fill",
                severity="ERROR",
                message=f"Broker fill for order {oid} has no internal fill record",
                entity_id=oid,
                broker_value=f"{len(broker_rows)} broker fill(s)",
                internal_value="0 internal fills",
            )
            stats["issues"] += 1

    # Check: internal fills without broker match
    for oid, int_rows in internal_by_order.items():
        stats["checks"] += 1
        if oid not in broker_by_order:
            _record_issue(
                con, recon_run_id,
                check_type="fill_match",
                entity_type="fill",
                severity="WARNING",
                message=f"Internal fill for order {oid} has no broker fill record (broker sync may be pending)",
                entity_id=oid,
                internal_value=f"{len(int_rows)} internal fill(s)",
                broker_value="0 broker fills",
            )
            stats["issues"] += 1

    # Check: quantity/price mismatch for matched orders
    #
    # Broker raw fills are stored per execution-leg (one row per leg per fill),
    # while internal fills are per combo order (one row with total qty).
    # To compare: sum broker leg quantities per order, then divide by the
    # number of legs to get combo-equivalent quantity.
    for oid in set(internal_by_order) & set(broker_by_order):
        stats["checks"] += 1
        int_total_qty = sum(int(r["fill_qty"]) for r in internal_by_order[oid])

        # Parse broker qty from raw_payload — field is "qty" as stored by
        # broker_sync_schwab._extract_fills_from_order
        broker_leg_qty = 0
        broker_leg_count = 0
        for br in broker_by_order[oid]:
            try:
                payload = json.loads(br["raw_payload"]) if isinstance(br["raw_payload"], str) else br["raw_payload"]
                broker_leg_qty += int(payload.get("qty", 0))
                broker_leg_count += 1
            except (json.JSONDecodeError, TypeError, ValueError):
                pass

        # Schwab stores one row per leg; a 2-leg spread filled 1x produces
        # 2 rows each with qty=1.  Determine legs-per-fill from the data:
        # count distinct leg_ids for this order.
        distinct_legs = len({
            str(br.get("fill_id", ""))  # fill_id column stores leg_id from broker
            for br in broker_by_order[oid]
        })
        legs_per_combo = max(distinct_legs, 1)
        broker_combo_qty = broker_leg_qty // legs_per_combo if legs_per_combo else broker_leg_qty

        if int_total_qty != broker_combo_qty and broker_combo_qty > 0:
            _record_issue(
                con, recon_run_id,
                check_type="fill_match",
                entity_type="fill",
                severity="ERROR",
                message=f"Fill quantity mismatch for order {oid}",
                entity_id=oid,
                internal_value=str(int_total_qty),
                broker_value=f"{broker_combo_qty} (from {broker_leg_qty} leg fills / {legs_per_combo} legs)",
            )
            stats["issues"] += 1

    return stats


# ---------------------------------------------------------------------------
# Position classification
# ---------------------------------------------------------------------------

# The single API automation tag used by our Lambda pipeline.
# Orders with this tag were placed by our code; anything else is manual/legacy.
API_TAG = "TA_1michaelbelaygmailcom1755679459"


def _is_within_trigger_window(con, account: str, ts_utc: datetime) -> tuple[bool, str]:
    """Check if a UTC timestamp falls within any trigger window for this account.

    Returns (is_within, reason).  Used as a secondary heuristic when no
    Schwab order tag is available.
    """
    try:
        import zoneinfo
        et_tz = zoneinfo.ZoneInfo("America/New_York")
    except ImportError:
        et_tz = timezone(timedelta(hours=-5))

    if isinstance(ts_utc, str):
        ts_utc = datetime.fromisoformat(ts_utc.replace("Z", "+00:00"))
    if ts_utc.tzinfo is None:
        ts_utc = ts_utc.replace(tzinfo=timezone.utc)

    ts_et = ts_utc.astimezone(et_tz)
    weekday = ts_et.weekday()
    time_str = ts_et.strftime("%H:%M")

    windows_df = query_df(
        """SELECT strategy, start_et, end_et, rule_name
           FROM strategy_trigger_windows
           WHERE account = ? AND weekday = ?""",
        [account, weekday],
        con=con,
    )

    if windows_df.empty:
        return False, "no_trigger_windows_defined"

    for _, w in windows_df.iterrows():
        if w["start_et"] <= time_str <= w["end_et"]:
            return True, f"within_{w['rule_name']}"

    return False, f"OUTSIDE_TRIGGER_WINDOW(time={time_str}ET,weekday={weekday})"


def _classify_untracked_positions(
    con,
    account: str,
    untracked_symbols: set[str],
    report_date: str,
) -> dict[str, tuple[str, str]]:
    """Classify untracked broker positions by source.

    Returns a dict mapping symbol → (classification, reason) where
    classification is one of:
      - "api_unmatched"  — API-tagged order with no internal match (real gap)
      - "non_api"        — no API tag → manual/discretionary/legacy
      - "unknown_source" — no order found in lookback window

    Classification order:
      1. Schwab order tag (hard provenance — definitive)
      2. Trigger window heuristic (fallback for orders without tag data)
    """
    fetch_start, fetch_end = _broker_fetch_window(report_date)
    orders_df = query_df(
        """SELECT order_id, raw_payload
           FROM broker_raw_orders
           WHERE account = ?
             AND fetched_at >= ? AND fetched_at < ?""",
        [account, fetch_start, fetch_end],
        con=con,
    )

    # Map symbol → (tag, first_fill_ts) from broker orders
    symbol_info: dict[str, dict] = {}  # sym → {"tag": str|None, "fill_ts": datetime|None}

    for _, row in orders_df.iterrows():
        try:
            payload = json.loads(row["raw_payload"]) if isinstance(row["raw_payload"], str) else row["raw_payload"]
            tag = payload.get("tag")

            order_syms = set()
            for leg in payload.get("orderLegCollection", []):
                sym = (leg.get("instrument", {}).get("symbol", "") or "").replace(" ", "")
                if sym:
                    order_syms.add(sym)

            if not order_syms & untracked_symbols:
                continue

            # Get the earliest fill time for trigger-window fallback
            earliest_fill: datetime | None = None
            for activity in payload.get("orderActivityCollection", []):
                if str(activity.get("activityType", "")).upper() != "EXECUTION":
                    continue
                for exec_leg in activity.get("executionLegs", []):
                    fill_time_str = exec_leg.get("time", "")
                    if not fill_time_str:
                        continue
                    try:
                        fill_ts = datetime.fromisoformat(fill_time_str.replace("Z", "+00:00"))
                        if earliest_fill is None or fill_ts < earliest_fill:
                            earliest_fill = fill_ts
                    except (ValueError, TypeError):
                        pass

            for sym in order_syms & untracked_symbols:
                existing = symbol_info.get(sym)
                # Keep the earliest fill; prefer tagged orders
                if existing is None:
                    symbol_info[sym] = {"tag": tag, "fill_ts": earliest_fill}
                elif tag and not existing.get("tag"):
                    symbol_info[sym] = {"tag": tag, "fill_ts": earliest_fill or existing.get("fill_ts")}
                elif earliest_fill and (existing.get("fill_ts") is None or earliest_fill < existing["fill_ts"]):
                    symbol_info[sym]["fill_ts"] = earliest_fill
                    if tag:
                        symbol_info[sym]["tag"] = tag

        except (json.JSONDecodeError, TypeError):
            pass

    result: dict[str, tuple[str, str]] = {}

    for sym in untracked_symbols:
        info = symbol_info.get(sym)

        if info is None:
            # No order found — old position from before lookback window
            result[sym] = ("unknown_source", "no_order_in_lookback_window")
            continue

        tag = info.get("tag")

        # --- Primary signal: Schwab order tag ---
        if tag == API_TAG:
            result[sym] = ("api_unmatched", f"tag={tag}")
        elif tag:
            result[sym] = ("non_api", f"tag={tag}")
        else:
            # No tag — fall back to trigger window heuristic
            fill_ts = info.get("fill_ts")
            if fill_ts:
                within, reason = _is_within_trigger_window(con, account, fill_ts)
                if within:
                    result[sym] = ("api_unmatched", f"no_tag,{reason}")
                else:
                    result[sym] = ("non_api", f"no_tag,{reason}")
            else:
                result[sym] = ("unknown_source", "no_tag,no_fill_time")

    return result


# ---------------------------------------------------------------------------
# Check 2: Position match
# ---------------------------------------------------------------------------

def _check_position_match(con, recon_run_id: str, report_date: str) -> dict:
    """Compare internal open positions against latest broker position snapshot.

    Flags:
    - Positions open internally but missing from broker
    - Positions at broker but missing internally
    """
    stats = {"checks": 0, "issues": 0}

    # Internal open positions
    internal_df = query_df(
        """SELECT position_id, strategy, account, qty
           FROM positions
           WHERE lifecycle_state IN ('OPEN', 'PARTIALLY_OPEN', 'PARTIALLY_CLOSED')""",
        con=con,
    )

    # Latest broker position snapshots (most recent per account)
    fetch_start, fetch_end = _broker_fetch_window(report_date)
    broker_df = query_df(
        """SELECT account, raw_payload, fetched_at
           FROM broker_raw_positions
           WHERE fetched_at >= ? AND fetched_at < ?
           ORDER BY fetched_at DESC""",
        [fetch_start, fetch_end], con=con,
    )

    stats["checks"] += 1

    if broker_df.empty:
        # No broker data to compare — skip rather than false-flag
        return stats

    # Parse broker positions: collect all option symbols from broker
    broker_accounts: dict[str, set] = {}
    for _, row in broker_df.iterrows():
        acct = row["account"]
        if acct in broker_accounts:
            continue  # already processed latest snapshot for this account
        try:
            payload = json.loads(row["raw_payload"]) if isinstance(row["raw_payload"], str) else row["raw_payload"]
            symbols = set()
            # Schwab positions payload is a list of position objects
            positions_list = payload if isinstance(payload, list) else payload.get("positions", [])
            for pos in positions_list:
                instrument = pos.get("instrument", {})
                sym = instrument.get("symbol", "")
                if sym:
                    symbols.add(sym.replace(" ", ""))
            broker_accounts[acct] = symbols
        except (json.JSONDecodeError, TypeError):
            broker_accounts[acct] = set()

    # Build internal position OSIs per account, tracking position_id → legs
    internal_by_account: dict[str, set] = {}
    position_legs_map: dict[str, dict] = {}  # position_id → {"account": str, "osis": set[str]}
    for _, row in internal_df.iterrows():
        acct = row["account"]
        pid = row["position_id"]
        legs_df = query_df(
            "SELECT osi FROM position_legs WHERE position_id = ?",
            [pid], con=con,
        )
        osis = set()
        for _, leg in legs_df.iterrows():
            osi = leg["osi"].replace(" ", "")
            if osi:
                osis.add(osi)
        internal_by_account.setdefault(acct, set()).update(osis)
        position_legs_map[pid] = {"account": acct, "osis": osis}

    # Compare per account
    for acct in set(internal_by_account) | set(broker_accounts):
        stats["checks"] += 1
        int_syms = internal_by_account.get(acct, set())
        brk_syms = broker_accounts.get(acct, set())

        # Internal but not at broker — auto-close positions whose legs are all gone
        missing_at_broker = int_syms - brk_syms
        if missing_at_broker:
            closed_pids: list[str] = []
            remaining_syms: set[str] = set()

            for pid, info in position_legs_map.items():
                if info["account"] != acct:
                    continue
                if not info["osis"]:
                    continue
                # All legs of this position are gone from broker → close it
                if info["osis"] <= missing_at_broker:
                    ok = transition_state(
                        con, pid, "CLOSED", closure_reason="MANUAL",
                    )
                    if ok:
                        closed_pids.append(pid)
                    else:
                        remaining_syms.update(info["osis"] & missing_at_broker)
                else:
                    # Partial — some legs still at broker, some not
                    remaining_syms.update(info["osis"] & missing_at_broker)

            if closed_pids:
                _record_issue(
                    con, recon_run_id,
                    check_type="position_match",
                    entity_type="position",
                    severity="INFO",
                    message=(
                        f"Account {acct}: auto-closed {len(closed_pids)} position(s) "
                        f"no longer at broker (manual/expiry close)"
                    ),
                    entity_id=acct,
                    internal_value=",".join(closed_pids[:5]),
                    broker_value="not found",
                    classification="broker_closed",
                    classification_reason="all_legs_gone_from_broker",
                )
                stats["issues"] += 1
                stats.setdefault("auto_closed", 0)
                stats["auto_closed"] += len(closed_pids)

            if remaining_syms:
                _record_issue(
                    con, recon_run_id,
                    check_type="position_match",
                    entity_type="position",
                    severity="ERROR",
                    message=f"Account {acct}: {len(remaining_syms)} position leg(s) open internally but not at broker",
                    entity_id=acct,
                    internal_value=",".join(sorted(remaining_syms)[:5]),
                    broker_value="not found",
                )
                stats["issues"] += 1

        # At broker but not internal — classify by source
        missing_internally = brk_syms - int_syms
        if missing_internally:
            classified = _classify_untracked_positions(
                con, acct, missing_internally, report_date,
            )

            # Group symbols by classification
            buckets: dict[str, list[tuple[str, str]]] = {}
            for sym, (cls, reason) in classified.items():
                buckets.setdefault(cls, []).append((sym, reason))

            # api_unmatched: API-tagged but no internal match → real tracking gap
            api_syms = buckets.get("api_unmatched", [])
            if api_syms:
                syms = [s for s, _ in api_syms]
                reasons = sorted({r for _, r in api_syms})
                _record_issue(
                    con, recon_run_id,
                    check_type="position_match",
                    entity_type="position",
                    severity="WARNING",
                    message=f"Account {acct}: {len(api_syms)} API-tagged position(s) at broker but not tracked internally",
                    entity_id=acct,
                    internal_value="not found",
                    broker_value=",".join(sorted(syms)[:5]),
                    classification="api_unmatched",
                    classification_reason="; ".join(reasons),
                )
                stats["issues"] += 1

            # non_api: non-API tag or outside trigger windows → discretionary/manual
            non_api_syms = buckets.get("non_api", [])
            if non_api_syms:
                syms = [s for s, _ in non_api_syms]
                reasons = sorted({r for _, r in non_api_syms})
                _record_issue(
                    con, recon_run_id,
                    check_type="position_match",
                    entity_type="position",
                    severity="INFO",
                    message=(
                        f"Account {acct}: {len(non_api_syms)} non-API broker position(s) "
                        f"(manual/discretionary/legacy)"
                    ),
                    entity_id=acct,
                    internal_value="non_api",
                    broker_value=",".join(sorted(syms)[:5]),
                    classification="non_api",
                    classification_reason="; ".join(reasons),
                )
                stats["issues"] += 1

            # unknown_source: no order found in lookback window
            unknown_syms = buckets.get("unknown_source", [])
            if unknown_syms:
                syms = [s for s, _ in unknown_syms]
                reasons = sorted({r for _, r in unknown_syms})
                _record_issue(
                    con, recon_run_id,
                    check_type="position_match",
                    entity_type="position",
                    severity="INFO",
                    message=(
                        f"Account {acct}: {len(unknown_syms)} broker position(s) "
                        f"with no order in lookback window (likely legacy)"
                    ),
                    entity_id=acct,
                    internal_value="unknown_source",
                    broker_value=",".join(sorted(syms)[:5]),
                    classification="unknown_source",
                    classification_reason="; ".join(reasons),
                )
                stats["issues"] += 1

    return stats


# ---------------------------------------------------------------------------
# Check 3: Cash match
# ---------------------------------------------------------------------------

def _check_cash_match(con, recon_run_id: str, report_date: str) -> dict:
    """Compare account_snapshots against broker_raw_cash for the day.

    Flags material differences (> $50) in cash or net_liq.
    """
    stats = {"checks": 0, "issues": 0}
    CASH_TOLERANCE = 50.0

    snapshots_df = query_df(
        """SELECT account, cash, net_liq
           FROM account_snapshots
           WHERE snapshot_date = ?""",
        [report_date], con=con,
    )

    fetch_start, fetch_end = _broker_fetch_window(report_date)
    broker_cash_df = query_df(
        """SELECT account, raw_payload
           FROM broker_raw_cash
           WHERE fetched_at >= ? AND fetched_at < ?
           ORDER BY fetched_at DESC""",
        [fetch_start, fetch_end], con=con,
    )

    stats["checks"] += 1

    if snapshots_df.empty or broker_cash_df.empty:
        return stats

    # Build broker cash lookup (latest per account)
    # broker_raw_cash.raw_payload is stored by broker_sync_schwab as:
    #   {"currentBalances": {...}, "initialBalances": {...}, "projectedBalances": {...}}
    # The actual cash/net_liq values live inside currentBalances.
    broker_cash: dict[str, dict] = {}
    for _, row in broker_cash_df.iterrows():
        acct = row["account"]
        if acct in broker_cash:
            continue
        try:
            payload = json.loads(row["raw_payload"]) if isinstance(row["raw_payload"], str) else row["raw_payload"]
            current = payload.get("currentBalances", {})
            broker_cash[acct] = {
                "cash": float(current.get("cashBalance", current.get("cashAvailableForTrading", 0)) or 0),
                "net_liq": float(current.get("liquidationValue", 0) or 0),
            }
        except (json.JSONDecodeError, TypeError, ValueError):
            pass

    # Compare
    for _, snap in snapshots_df.iterrows():
        acct = snap["account"]
        if acct not in broker_cash:
            continue

        stats["checks"] += 1
        brk = broker_cash[acct]

        # Cash comparison
        int_cash = float(snap["cash"] or 0)
        brk_cash = brk["cash"]
        if abs(int_cash - brk_cash) > CASH_TOLERANCE:
            _record_issue(
                con, recon_run_id,
                check_type="cash_match",
                entity_type="account",
                severity="WARNING",
                message=f"Account {acct}: cash differs by ${abs(int_cash - brk_cash):,.2f}",
                entity_id=acct,
                internal_value=f"${int_cash:,.2f}",
                broker_value=f"${brk_cash:,.2f}",
            )
            stats["issues"] += 1

        # Net liq comparison
        int_nl = float(snap["net_liq"] or 0)
        brk_nl = brk["net_liq"]
        if abs(int_nl - brk_nl) > CASH_TOLERANCE:
            _record_issue(
                con, recon_run_id,
                check_type="cash_match",
                entity_type="account",
                severity="WARNING",
                message=f"Account {acct}: net_liq differs by ${abs(int_nl - brk_nl):,.2f}",
                entity_id=acct,
                internal_value=f"${int_nl:,.2f}",
                broker_value=f"${brk_nl:,.2f}",
            )
            stats["issues"] += 1

    return stats


# ---------------------------------------------------------------------------
# Check 4: Source freshness
# ---------------------------------------------------------------------------

def _check_freshness(con, recon_run_id: str) -> dict:
    """Enforce SLA on all registered sources in source_freshness table.

    Marks sources as stale if last_success_at + sla_minutes < now.
    """
    stats = {"checks": 0, "issues": 0}

    df = query_df(
        "SELECT source_name, last_success_at, sla_minutes, is_stale, error_message FROM source_freshness",
        con=con,
    )

    if df.empty:
        return stats

    now = datetime.now(timezone.utc)

    for _, row in df.iterrows():
        stats["checks"] += 1
        source = row["source_name"]
        sla_min = int(row["sla_minutes"] or 60)

        if row["last_success_at"] is None or pd.isna(row["last_success_at"]):
            # Never succeeded — flag as stale
            execute(
                "UPDATE source_freshness SET is_stale = true WHERE source_name = ?",
                [source], con=con,
            )
            _record_issue(
                con, recon_run_id,
                check_type="freshness",
                entity_type="source",
                severity="ERROR",
                message=f"Source '{source}' has never reported success",
                entity_id=source,
            )
            stats["issues"] += 1
            continue

        # Parse last_success_at
        last_success = row["last_success_at"]
        if isinstance(last_success, str):
            # Handle ISO format with or without timezone
            try:
                last_success = datetime.fromisoformat(last_success.replace("Z", "+00:00"))
            except ValueError:
                continue
        # If it's already a datetime from DuckDB
        if not hasattr(last_success, "tzinfo") or last_success.tzinfo is None:
            last_success = last_success.replace(tzinfo=timezone.utc)

        age_minutes = (now - last_success).total_seconds() / 60
        is_stale = age_minutes > sla_min

        # Update staleness flag
        execute(
            "UPDATE source_freshness SET is_stale = ? WHERE source_name = ?",
            [is_stale, source], con=con,
        )

        if is_stale:
            _record_issue(
                con, recon_run_id,
                check_type="freshness",
                entity_type="source",
                severity="WARNING" if age_minutes < sla_min * 2 else "ERROR",
                message=f"Source '{source}' is stale: last success {age_minutes:.0f}m ago (SLA: {sla_min}m)",
                entity_id=source,
                internal_value=f"{age_minutes:.0f} minutes ago",
                broker_value=f"SLA: {sla_min} minutes",
            )
            stats["issues"] += 1
        else:
            # Clear staleness
            execute(
                "UPDATE source_freshness SET is_stale = false WHERE source_name = ?",
                [source], con=con,
            )

    return stats


# ---------------------------------------------------------------------------
# Auto-resolve prior issues
# ---------------------------------------------------------------------------

def _auto_resolve_prior_items(con) -> int:
    """Mark all UNRESOLVED reconciliation items as AUTO_RESOLVED.

    Called at the start of each reconciliation run.  Each check will re-create
    any issue that still exists, so stale items from prior runs get cleaned up
    automatically.  This prevents transient mismatches from degrading the trust
    banner permanently.

    Returns count of items resolved.
    """
    row = query_one(
        "SELECT COUNT(*) FROM reconciliation_items WHERE status = 'UNRESOLVED'",
        con=con,
    )
    count = row[0] if row else 0

    if count > 0:
        now = _now()
        execute(
            """UPDATE reconciliation_items
               SET status = 'AUTO_RESOLVED',
                   resolved_at = ?,
                   resolution_type = 'auto_match'
               WHERE status = 'UNRESOLVED'""",
            [now],
            con=con,
        )

    return count


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_reconciliation(
    con=None,
    report_date: date | str | None = None,
) -> dict:
    """Run all reconciliation checks for a given date.

    Creates a reconciliation_run record, executes all checks, and returns
    a stats dict with counts.

    Returns:
        {"run_id": str, "checks_run": int, "issues_found": int, "auto_resolved": int, "status": str}
    """
    if con is None:
        con = get_connection()

    if report_date is None:
        report_date = date.today()
    if isinstance(report_date, str):
        report_date = date.fromisoformat(report_date)

    date_str = report_date.isoformat()

    # Auto-resolve prior UNRESOLVED items from earlier runs.
    # Each check re-creates issues that still exist, so any old issue not
    # re-flagged is implicitly resolved.  We mark all prior items as
    # AUTO_RESOLVED before running checks; new failures get fresh rows.
    auto_resolved = _auto_resolve_prior_items(con)

    # Create reconciliation run record
    run_id = _uuid()
    started_at = _now()
    execute(
        """INSERT INTO reconciliation_runs
           (id, run_date, started_at, status)
           VALUES (?, ?, ?, 'RUNNING')""",
        [run_id, date_str, started_at],
        con=con,
    )

    total_checks = 0
    total_issues = 0

    # Run all checks
    checks = [
        ("fill_match", _check_fill_match, (con, run_id, date_str)),
        ("position_match", _check_position_match, (con, run_id, date_str)),
        ("cash_match", _check_cash_match, (con, run_id, date_str)),
        ("freshness", _check_freshness, (con, run_id)),
    ]

    for name, fn, args in checks:
        try:
            result = fn(*args)
            total_checks += result.get("checks", 0)
            total_issues += result.get("issues", 0)
        except Exception as e:
            # Record the check failure itself as an issue
            _record_issue(
                con, run_id,
                check_type=name,
                entity_type="system",
                severity="ERROR",
                message=f"Check '{name}' failed: {e}",
            )
            total_checks += 1
            total_issues += 1

    # Finalize the run
    execute(
        """UPDATE reconciliation_runs
           SET completed_at = ?, checks_run = ?, issues_found = ?,
               auto_resolved = ?, status = 'COMPLETED'
           WHERE id = ?""",
        [_now(), total_checks, total_issues, auto_resolved, run_id],
        con=con,
    )

    return {
        "run_id": run_id,
        "checks_run": total_checks,
        "issues_found": total_issues,
        "auto_resolved": auto_resolved,
        "status": "COMPLETED",
    }
