"""Historical backfill from BF/DS/CS CSV logs into the canonical store.

The backfill path generates deterministic synthetic events and reuses the
existing raw_events -> normalized materialization flow. That keeps historical
imports idempotent and structurally identical to native event capture.

Usage:
    from reporting.backfill import backfill_csv_logs

    stats = backfill_csv_logs(
        bf_csv="scripts/trade/ButterflyTuesday/logs/butterfly_tuesday_trades.csv",
        ds_csv="logs/dualside_trades.csv",
        cs_csv="logs/constantstable_vertical_trades.csv",
    )
"""

from __future__ import annotations

import csv
import hashlib
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable

from reporting.db import get_connection, init_schema, query_df, query_one
from reporting.events import _idem_key
from reporting.ingest import _MATERIALIZERS, _ingest_raw_event
from reporting.position_engine import materialize_positions


BACKFILL_CONFIG_VERSION = "backfill-csv-v1"

DEFAULT_BF_CSV = (
    "/Users/mgebremichael/Documents/Gamma/scripts/trade/ButterflyTuesday/logs/"
    "butterfly_tuesday_trades.csv"
)
DEFAULT_DS_CSV = "/Users/mgebremichael/Documents/Gamma/logs/dualside_trades.csv"
DEFAULT_CS_CSV = "/Users/mgebremichael/Documents/Gamma/logs/constantstable_vertical_trades.csv"


@dataclass(frozen=True)
class BrokerOrderMatch:
    order_id: str
    entered_time: str
    price: float
    filled_qty: int
    status: str


def _stable_id(*parts: object, n: int = 16) -> str:
    h = hashlib.sha256()
    for part in parts:
        h.update(str(part).encode("utf-8"))
        h.update(b"\x1f")
    return h.hexdigest()[:n]


def _read_csv(path: str | Path | None) -> list[dict]:
    if not path:
        return []
    p = Path(path)
    if not p.exists():
        return []
    with p.open(newline="") as f:
        return list(csv.DictReader(f))


def _f(value, default: float = 0.0) -> float:
    try:
        if value in ("", None):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _i(value, default: int = 0) -> int:
    try:
        if value in ("", None):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _s(value) -> str:
    return (value or "").strip()


def _norm_osi(osi: str) -> str:
    return _s(osi).replace(" ", "")


def _parse_ts(ts: str, fallback_date: str) -> str:
    text = _s(ts)
    if text:
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()
        except ValueError:
            pass
    return f"{fallback_date}T21:00:00+00:00"


def _parse_entered_trade_date(entered_time: str) -> str:
    text = _s(entered_time)
    if not text:
        return ""
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return ""
    try:
        import zoneinfo

        et = zoneinfo.ZoneInfo("America/New_York")
        return dt.astimezone(et).date().isoformat()
    except Exception:
        return dt.date().isoformat()


def _event(
    *,
    strategy: str,
    account: str,
    run_id: str,
    trade_group_id: str,
    event_type: str,
    ts_utc: str,
    payload: dict,
    ordinal: int = 0,
) -> dict:
    return {
        "event_id": _stable_id(
            "backfill-event",
            strategy,
            account,
            run_id,
            trade_group_id,
            event_type,
            ts_utc,
            json.dumps(payload, sort_keys=True, separators=(",", ":")),
            ordinal,
        ),
        "event_type": event_type,
        "ts_utc": ts_utc,
        "strategy": strategy,
        "account": account,
        "trade_group_id": trade_group_id,
        "run_id": run_id,
        "config_version": BACKFILL_CONFIG_VERSION,
        "payload": payload,
        "idempotency_key": _idem_key(
            event_type,
            payload,
            strategy=strategy,
            account=account,
            run_id=run_id,
            trade_group_id=trade_group_id,
        ),
    }


def _apply_events(con, events: Iterable[dict]) -> dict:
    stats = {"events": 0, "inserted": 0, "duplicates": 0, "materialized": 0}
    for ev in events:
        stats["events"] += 1
        inserted = _ingest_raw_event(con, ev)
        if not inserted:
            stats["duplicates"] += 1
            continue
        stats["inserted"] += 1
        materializer = _MATERIALIZERS.get(ev["event_type"])
        if materializer:
            materializer(con, ev)
            stats["materialized"] += 1
    return stats


def _has_activity(row: dict) -> bool:
    return _i(row.get("qty_filled")) > 0 or _i(row.get("filled_qty")) > 0 or bool(_s(row.get("order_ids")))


def _query_broker_order_payloads(con, account: str, order_ids: list[str]) -> dict[str, dict]:
    if not order_ids:
        return {}
    placeholders = ", ".join(["?"] * len(order_ids))
    rows = con.execute(
        f"""SELECT order_id, raw_payload
            FROM broker_raw_orders
            WHERE account = ?
              AND order_id IN ({placeholders})""",
        [account, *order_ids],
    ).fetchall()
    out: dict[str, dict] = {}
    for oid, raw in rows:
        out[str(oid)] = json.loads(raw) if isinstance(raw, str) else raw
    return out


def _match_broker_orders(
    con,
    *,
    account: str,
    trade_date: str,
    symbols: list[str],
    explicit_order_ids: list[str] | None = None,
) -> list[BrokerOrderMatch]:
    order_ids = [oid for oid in (explicit_order_ids or []) if oid]
    payloads = _query_broker_order_payloads(con, account, order_ids)
    matches: list[BrokerOrderMatch] = []

    for oid in order_ids:
        payload = payloads.get(oid)
        if payload is None:
            matches.append(BrokerOrderMatch(oid, "", 0.0, 0, ""))
            continue
        matches.append(
            BrokerOrderMatch(
                order_id=oid,
                entered_time=_s(payload.get("enteredTime")),
                price=_f(payload.get("price")),
                filled_qty=_i(payload.get("filledQuantity")),
                status=_s(payload.get("status")),
            )
        )
    if matches:
        return matches

    if not symbols:
        return []

    rows = con.execute(
        """SELECT order_id, raw_payload
           FROM broker_raw_orders
           WHERE account = ?""",
        [account],
    ).fetchall()

    target_syms = {_norm_osi(s) for s in symbols if _norm_osi(s)}
    for oid, raw in rows:
        payload = json.loads(raw) if isinstance(raw, str) else raw
        if _parse_entered_trade_date(payload.get("enteredTime", "")) != trade_date:
            continue
        payload_syms = {
            _norm_osi(leg.get("instrument", {}).get("symbol", ""))
            for leg in payload.get("orderLegCollection", [])
        }
        payload_syms.discard("")
        if payload_syms != target_syms:
            continue
        matches.append(
            BrokerOrderMatch(
                order_id=str(oid),
                entered_time=_s(payload.get("enteredTime")),
                price=_f(payload.get("price")),
                filled_qty=_i(payload.get("filledQuantity")),
                status=_s(payload.get("status")),
            )
        )

    return sorted(matches, key=lambda m: (m.entered_time, m.order_id))


def _fill_context(
    row: dict,
    broker_matches: list[BrokerOrderMatch],
) -> tuple[list[str], int, float, str]:
    order_ids = [oid for oid in _s(row.get("order_ids")).split(",") if oid]
    if not order_ids and broker_matches:
        order_ids = [m.order_id for m in broker_matches]

    fill_qty = _i(row.get("qty_filled"))
    if fill_qty <= 0:
        fill_qty = _i(row.get("filled_qty"))
    if fill_qty <= 0 and broker_matches:
        fill_qty = max((m.filled_qty for m in broker_matches), default=0)

    fill_price = _f(row.get("last_price"))
    if fill_price == 0.0 and broker_matches:
        for match in broker_matches:
            if match.filled_qty > 0 and match.price:
                fill_price = match.price
                break
        if fill_price == 0.0:
            fill_price = broker_matches[0].price

    fill_order_id = ""
    for match in broker_matches:
        if match.filled_qty > 0:
            fill_order_id = match.order_id
            break
    if not fill_order_id and order_ids:
        fill_order_id = order_ids[0]

    return order_ids, fill_qty, fill_price, fill_order_id


def _update_unfilled_outcome(con, trade_group_id: str, reason: str) -> None:
    reason_norm = _s(reason).upper()
    if reason_norm in {"DRY_RUN"}:
        outcome = "CANCELED"
    elif reason_norm in {"NO_FILL", "NBBO_UNAVAILABLE", "TIMEOUT"}:
        outcome = "CANCELED"
    else:
        outcome = "TIMEOUT"
    con.execute(
        "UPDATE intended_trades SET outcome = ? WHERE trade_group_id = ?",
        [outcome, trade_group_id],
    )


def _post_step(strategy: str, account: str, run_id: str, ts_utc: str, suffix: str = "") -> dict:
    payload = {
        "step_name": "backfill",
        "outcome": "BACKFILLED",
        "source": "csv",
        "suffix": suffix,
    }
    return _event(
        strategy=strategy,
        account=account,
        run_id=run_id,
        trade_group_id=_stable_id("run", strategy, account, run_id),
        event_type="post_step_result",
        ts_utc=ts_utc,
        payload=payload,
    )


def _bf_events(rows: list[dict], account: str, con, enrich_from_broker: bool) -> tuple[list[dict], list[tuple[str, str]]]:
    events: list[dict] = []
    unfilled: list[tuple[str, str]] = []
    for row in rows:
        if not _has_activity(row):
            continue
        trade_date = _s(row.get("trade_date"))
        ts_utc = _parse_ts(row.get("ts_utc", ""), trade_date)
        run_id = _stable_id("bf-run", account, trade_date, row.get("expiry_date"), ts_utc)
        trade_group_id = _stable_id("bf-group", account, trade_date, row.get("expiry_date"), row.get("lower_osi"), row.get("center_osi"), row.get("upper_osi"))
        direction = "LONG" if _s(row.get("order_side")).upper() == "DEBIT" else "SHORT"
        order_side = _s(row.get("order_side")).upper()

        events.append(
            _event(
                strategy="butterfly",
                account=account,
                run_id=run_id,
                trade_group_id=trade_group_id,
                event_type="strategy_run",
                ts_utc=ts_utc,
                payload={
                    "trade_date": trade_date,
                    "signal": _s(row.get("signal")),
                    "config": _s(row.get("config")),
                    "reason": _s(row.get("reason")) or "OK",
                    "spot": _f(row.get("spot")),
                    "vix": _f(row.get("vix")),
                    "vix1d": _f(row.get("vix1d")),
                    "filters": {},
                    "expiry_date": _s(row.get("expiry_date")),
                },
            )
        )
        events.append(
            _event(
                strategy="butterfly",
                account=account,
                run_id=run_id,
                trade_group_id=trade_group_id,
                event_type="trade_intent",
                ts_utc=ts_utc,
                payload={
                    "trade_date": trade_date,
                    "side": order_side,
                    "direction": direction,
                    "legs": [
                        {
                            "osi": _s(row.get("lower_osi")),
                            "strike": _f(row.get("lower_strike")),
                            "option_type": "CALL",
                            "action": "BUY_TO_OPEN" if order_side == "DEBIT" else "SELL_TO_OPEN",
                            "qty": 1,
                        },
                        {
                            "osi": _s(row.get("center_osi")),
                            "strike": _f(row.get("center_strike")),
                            "option_type": "CALL",
                            "action": "SELL_TO_OPEN" if order_side == "DEBIT" else "BUY_TO_OPEN",
                            "qty": 2,
                        },
                        {
                            "osi": _s(row.get("upper_osi")),
                            "strike": _f(row.get("upper_strike")),
                            "option_type": "CALL",
                            "action": "BUY_TO_OPEN" if order_side == "DEBIT" else "SELL_TO_OPEN",
                            "qty": 1,
                        },
                    ],
                    "target_qty": _i(row.get("qty"), 1),
                    "limit_price": _f(row.get("package_mid")),
                    "package_bid": _f(row.get("package_bid")),
                    "package_ask": _f(row.get("package_ask")),
                },
            )
        )

        broker_matches = []
        if enrich_from_broker:
            broker_matches = _match_broker_orders(
                con,
                account=account,
                trade_date=trade_date,
                symbols=[row.get("lower_osi"), row.get("center_osi"), row.get("upper_osi")],
                explicit_order_ids=[oid for oid in _s(row.get("order_ids")).split(",") if oid],
            )
        order_ids, fill_qty, fill_price, fill_order_id = _fill_context(row, broker_matches)

        for idx, oid in enumerate(order_ids):
            events.append(
                _event(
                    strategy="butterfly",
                    account=account,
                    run_id=run_id,
                    trade_group_id=trade_group_id,
                    event_type="order_submitted",
                    ts_utc=ts_utc,
                    payload={
                        "trade_date": trade_date,
                        "order_id": oid,
                        "legs": [],
                        "limit_price": fill_price or _f(row.get("package_mid")),
                        "order_type": order_side,
                    },
                    ordinal=idx,
                )
            )

        if fill_qty > 0:
            events.append(
                _event(
                    strategy="butterfly",
                    account=account,
                    run_id=run_id,
                    trade_group_id=trade_group_id,
                    event_type="fill",
                    ts_utc=ts_utc,
                    payload={
                        "order_id": fill_order_id,
                        "fill_qty": fill_qty,
                        "fill_price": fill_price,
                        "legs": [
                            {"osi": _s(row.get("lower_osi")), "strike": _f(row.get("lower_strike")), "option_type": "CALL", "qty": 1},
                            {"osi": _s(row.get("center_osi")), "strike": _f(row.get("center_strike")), "option_type": "CALL", "qty": 2},
                            {"osi": _s(row.get("upper_osi")), "strike": _f(row.get("upper_strike")), "option_type": "CALL", "qty": 1},
                        ],
                    },
                )
            )
        else:
            unfilled.append((trade_group_id, _s(row.get("reason"))))

        events.append(_post_step("butterfly", account, run_id, ts_utc, suffix=trade_group_id))
    return events, unfilled


def _vertical_payload(row: dict) -> tuple[list[dict], list[str]]:
    short_osi = _s(row.get("short_osi"))
    long_osi = _s(row.get("long_osi"))
    kind = _s(row.get("kind")).upper() or ("PUT" if "P" in short_osi else "CALL")
    qty = _i(row.get("qty_requested") or row.get("qty_filled") or 1, 1)
    legs = [
        {
            "osi": short_osi,
            "strike": 0.0,
            "option_type": kind,
            "action": "SELL_TO_OPEN",
            "qty": qty,
        },
        {
            "osi": long_osi,
            "strike": 0.0,
            "option_type": kind,
            "action": "BUY_TO_OPEN",
            "qty": qty,
        },
    ]
    return legs, [short_osi, long_osi]


def _group_rows(rows: list[dict]) -> dict[tuple[str, str, str], list[dict]]:
    groups: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    for row in rows:
        if not _has_activity(row):
            continue
        key = (_s(row.get("trade_date")), _s(row.get("tdate")), _s(row.get("ts_utc")))
        groups[key].append(row)
    return dict(groups)


def _vertical_strategy_events(
    strategy: str,
    rows: list[dict],
    account: str,
    con,
    enrich_from_broker: bool,
) -> tuple[list[dict], list[tuple[str, str]]]:
    events: list[dict] = []
    unfilled: list[tuple[str, str]] = []
    groups = _group_rows(rows)

    for (trade_date, tdate, ts_key), group_rows in sorted(groups.items()):
        ts_utc = _parse_ts(ts_key, trade_date)
        run_id = _stable_id(f"{strategy}-run", account, trade_date, tdate, ts_utc)
        signal = "+".join(row.get("name", "") for row in group_rows if row.get("name"))
        config = "+".join(row.get("name", "") for row in group_rows if row.get("name"))
        reason = _s(group_rows[0].get("reason")) or "OK"

        run_group_id = _stable_id(f"{strategy}-run-group", account, trade_date, tdate, ts_utc)
        events.append(
            _event(
                strategy=strategy,
                account=account,
                run_id=run_id,
                trade_group_id=run_group_id,
                event_type="strategy_run",
                ts_utc=ts_utc,
                payload={
                    "trade_date": trade_date,
                    "signal": signal,
                    "config": config,
                    "reason": reason,
                    "spot": 0.0,
                    "vix": _f(group_rows[0].get("vol_value")),
                    "vix1d": 0.0,
                    "filters": {},
                    "tdate": tdate,
                },
            )
        )

        for idx, row in enumerate(group_rows):
            name = _s(row.get("name")) or f"side{idx + 1}"
            trade_group_id = _stable_id(
                f"{strategy}-group",
                account,
                trade_date,
                tdate,
                ts_utc,
                name,
                row.get("short_osi"),
                row.get("long_osi"),
            )
            legs, symbols = _vertical_payload(row)
            target_qty = _i(row.get("qty_requested"), 1)
            events.append(
                _event(
                    strategy=strategy,
                    account=account,
                    run_id=run_id,
                    trade_group_id=trade_group_id,
                    event_type="trade_intent",
                    ts_utc=ts_utc,
                    payload={
                        "trade_date": trade_date,
                        "side": _s(row.get("side")).upper(),
                        "direction": _s(row.get("direction")).upper(),
                        "legs": legs,
                        "target_qty": target_qty,
                        "limit_price": _f(row.get("last_price")),
                        "name": name,
                        "expiration": tdate,
                    },
                    ordinal=idx,
                )
            )

            broker_matches = []
            if enrich_from_broker:
                broker_matches = _match_broker_orders(
                    con,
                    account=account,
                    trade_date=trade_date,
                    symbols=symbols,
                    explicit_order_ids=[oid for oid in _s(row.get("order_ids")).split(",") if oid],
                )
            order_ids, fill_qty, fill_price, fill_order_id = _fill_context(row, broker_matches)

            requested = target_qty
            for order_idx, oid in enumerate(order_ids):
                events.append(
                    _event(
                        strategy=strategy,
                        account=account,
                        run_id=run_id,
                        trade_group_id=trade_group_id,
                        event_type="order_submitted",
                        ts_utc=ts_utc,
                        payload={
                            "trade_date": trade_date,
                            "order_id": oid,
                            "legs": [
                                {**legs[0], "qty": requested},
                                {**legs[1], "qty": requested},
                            ],
                            "limit_price": fill_price or _f(row.get("last_price")),
                            "order_type": _s(row.get("side")).upper(),
                        },
                        ordinal=order_idx,
                    )
                )

            if fill_qty > 0:
                kind = _s(row.get("kind")).upper()
                events.append(
                    _event(
                        strategy=strategy,
                        account=account,
                        run_id=run_id,
                        trade_group_id=trade_group_id,
                        event_type="fill",
                        ts_utc=ts_utc,
                        payload={
                            "order_id": fill_order_id,
                            "fill_qty": fill_qty,
                            "fill_price": fill_price,
                            "legs": [
                                {"osi": _s(row.get("short_osi")), "strike": 0.0, "option_type": kind, "qty": fill_qty},
                                {"osi": _s(row.get("long_osi")), "strike": 0.0, "option_type": kind, "qty": fill_qty},
                            ],
                        },
                    )
                )
            else:
                unfilled.append((trade_group_id, _s(row.get("reason"))))

        events.append(_post_step(strategy, account, run_id, ts_utc))

    return events, unfilled


def backfill_csv_logs(
    *,
    bf_csv: str | Path | None = DEFAULT_BF_CSV,
    ds_csv: str | Path | None = DEFAULT_DS_CSV,
    cs_csv: str | Path | None = DEFAULT_CS_CSV,
    account: str = "schwab",
    enrich_from_broker: bool = True,
    con=None,
) -> dict:
    """Backfill BF/DS/CS trade history from CSV logs.

    Returns aggregate stats across all strategies.
    """
    if con is None:
        con = get_connection()
    init_schema(con)

    totals = {
        "strategies": {},
        "events": 0,
        "inserted": 0,
        "duplicates": 0,
        "materialized": 0,
        "positions": 0,
    }

    specs = [
        ("butterfly", _read_csv(bf_csv), _bf_events),
        ("dualside", _read_csv(ds_csv), _vertical_strategy_events),
        ("constantstable", _read_csv(cs_csv), _vertical_strategy_events),
    ]

    for strategy, rows, builder in specs:
        if builder is _bf_events:
            events, unfilled = builder(rows, account, con, enrich_from_broker)
        else:
            events, unfilled = builder(strategy, rows, account, con, enrich_from_broker)
        stats = _apply_events(con, events)
        for trade_group_id, reason in unfilled:
            _update_unfilled_outcome(con, trade_group_id, reason)
        totals["strategies"][strategy] = {
            "rows": len(rows),
            "events": stats["events"],
            "inserted": stats["inserted"],
            "duplicates": stats["duplicates"],
            "materialized": stats["materialized"],
        }
        for key in ("events", "inserted", "duplicates", "materialized"):
            totals[key] += stats[key]

    totals["positions"] = materialize_positions(con)
    return totals


# ---------------------------------------------------------------------------
# Broker-native backfill: synthesize events from broker_raw_orders
# ---------------------------------------------------------------------------

# The single API automation tag used by our Lambda pipeline.
API_TAG = "TA_1michaelbelaygmailcom1755679459"

BACKFILL_BROKER_CONFIG_VERSION = "backfill-broker-v1"


def _infer_strategy_from_order(payload: dict, trade_date_et: str, all_orders_on_date: list[dict]) -> str:
    """Infer strategy from broker order leg structure and context.

    Rules:
      - 3+ legs → butterfly
      - 2 legs with both PUT and CALL debit orders on same date → dualside
      - 2 legs otherwise → constantstable
    """
    legs = payload.get("orderLegCollection", [])
    if len(legs) >= 3:
        return "butterfly"

    # Check if there's a complementary PUT+CALL pair on same date → dualside
    this_types = {
        leg.get("instrument", {}).get("putCall", "").upper()
        for leg in legs
        if leg.get("instrument", {}).get("putCall")
    }

    peer_types: set[str] = set()
    this_oid = str(payload.get("orderId", ""))
    for peer in all_orders_on_date:
        if str(peer.get("orderId", "")) == this_oid:
            continue
        for leg in peer.get("orderLegCollection", []):
            pc = leg.get("instrument", {}).get("putCall", "").upper()
            if pc:
                peer_types.add(pc)

    # If this date has both PUT and CALL verticals → dualside
    combined = this_types | peer_types
    if "PUT" in combined and "CALL" in combined and len(legs) == 2:
        return "dualside"

    return "constantstable"


def _extract_legs_from_broker(payload: dict) -> list[dict]:
    """Extract canonical leg dicts from a Schwab order payload."""
    legs = []
    for leg in payload.get("orderLegCollection", []):
        inst = leg.get("instrument", {})
        sym = _norm_osi(inst.get("symbol", ""))
        instruction = _s(leg.get("instruction")).upper()
        option_type = _s(inst.get("putCall")).upper() or "CALL"
        qty = _i(leg.get("quantity"), 1)

        # Parse strike from OSI if available (last 8 chars = price * 1000)
        strike = 0.0
        if len(sym) >= 8:
            try:
                strike = int(sym[-8:]) / 1000.0
            except ValueError:
                pass

        legs.append({
            "osi": sym,
            "strike": strike,
            "option_type": option_type,
            "action": instruction,
            "qty": qty,
        })
    return legs


def _extract_expiry_from_legs(legs: list[dict]) -> str:
    """Extract expiry date from OSI symbols in legs.

    OSI format: SPXW  YYMMDD[CP]SSSSSSSS
    """
    for leg in legs:
        osi = leg.get("osi", "")
        # Find the date portion: after the root symbol, 6 digits
        # Standard: root (up to 6 chars) + YYMMDD + C/P + strike
        # SPXW260312C06725000 → date = 260312
        import re
        m = re.search(r"(\d{6})[CP]", osi)
        if m:
            raw = m.group(1)
            return f"20{raw[:2]}-{raw[2:4]}-{raw[4:6]}"
    return ""


def _infer_side_direction(payload: dict) -> tuple[str, str]:
    """Infer side (CREDIT/DEBIT) and direction (LONG/SHORT) from order type."""
    order_type = _s(payload.get("orderType")).upper()
    if "CREDIT" in order_type:
        return "CREDIT", "SHORT"
    return "DEBIT", "LONG"


def _broker_order_events(
    payload: dict,
    strategy: str,
    account: str,
) -> list[dict]:
    """Generate canonical events from a single broker order payload."""
    order_id = str(payload.get("orderId", ""))
    entered_time = _s(payload.get("enteredTime"))
    ts_utc = _parse_ts(entered_time, "")
    price = _f(payload.get("price"))
    filled_qty = _i(payload.get("filledQuantity"))

    # Parse trade_date in ET from enteredTime
    trade_date = _parse_entered_trade_date(entered_time)

    legs = _extract_legs_from_broker(payload)
    expiry = _extract_expiry_from_legs(legs)
    side, direction = _infer_side_direction(payload)

    run_id = _stable_id("broker-run", strategy, account, trade_date, order_id)
    trade_group_id = _stable_id("broker-group", strategy, account, order_id)

    events: list[dict] = []

    # strategy_run
    events.append(_event(
        strategy=strategy,
        account=account,
        run_id=run_id,
        trade_group_id=trade_group_id,
        event_type="strategy_run",
        ts_utc=ts_utc,
        payload={
            "trade_date": trade_date,
            "signal": direction,
            "config": f"broker_backfill_{strategy}",
            "reason": "OK",
            "spot": 0.0,
            "vix": 0.0,
            "vix1d": 0.0,
            "filters": {},
            "expiry_date": expiry,
            "broker_order_id": order_id,
        },
    ))

    # trade_intent
    target_qty = filled_qty or 1
    events.append(_event(
        strategy=strategy,
        account=account,
        run_id=run_id,
        trade_group_id=trade_group_id,
        event_type="trade_intent",
        ts_utc=ts_utc,
        payload={
            "trade_date": trade_date,
            "side": side,
            "direction": direction,
            "legs": legs,
            "target_qty": target_qty,
            "limit_price": price,
            "expiry_date": expiry,
        },
    ))

    # order_submitted
    events.append(_event(
        strategy=strategy,
        account=account,
        run_id=run_id,
        trade_group_id=trade_group_id,
        event_type="order_submitted",
        ts_utc=ts_utc,
        payload={
            "trade_date": trade_date,
            "order_id": order_id,
            "legs": legs,
            "limit_price": price,
            "order_type": side,
        },
    ))

    # fill
    if filled_qty > 0:
        # Use execution data for precise fill price if available
        fill_price = price
        fill_ts = ts_utc
        for activity in payload.get("orderActivityCollection", []):
            if str(activity.get("activityType", "")).upper() != "EXECUTION":
                continue
            for exec_leg in activity.get("executionLegs", []):
                t = _s(exec_leg.get("time"))
                if t:
                    fill_ts = _parse_ts(t, trade_date)
                    break
            break

        events.append(_event(
            strategy=strategy,
            account=account,
            run_id=run_id,
            trade_group_id=trade_group_id,
            event_type="fill",
            ts_utc=fill_ts,
            payload={
                "order_id": order_id,
                "fill_qty": filled_qty,
                "fill_price": fill_price,
                "legs": [
                    {"osi": l["osi"], "strike": l["strike"],
                     "option_type": l["option_type"], "qty": l["qty"]}
                    for l in legs
                ],
            },
        ))

    # post_step
    events.append(_post_step(strategy, account, run_id, ts_utc, suffix=order_id))

    return events


def backfill_from_broker(
    *,
    account: str = "schwab",
    tag: str = API_TAG,
    con=None,
) -> dict:
    """Backfill trades from API-tagged broker_raw_orders.

    Finds all FILLED orders with the given tag, infers strategy from
    leg structure, and generates canonical events.

    Returns stats dict.
    """
    if con is None:
        con = get_connection()
    init_schema(con)

    orders_df = query_df(
        """SELECT order_id, raw_payload
           FROM broker_raw_orders
           WHERE account = ?
             AND json_extract_string(raw_payload, '$.tag') = ?
             AND json_extract_string(raw_payload, '$.status') = 'FILLED'
           ORDER BY json_extract_string(raw_payload, '$.enteredTime')""",
        [account, tag],
        con=con,
    )

    if orders_df.empty:
        return {
            "orders": 0, "events": 0, "inserted": 0,
            "duplicates": 0, "materialized": 0, "positions": 0,
            "strategies": {},
        }

    # Parse all payloads and group by trade date for strategy inference
    parsed: list[dict] = []
    by_date: dict[str, list[dict]] = defaultdict(list)
    for _, row in orders_df.iterrows():
        payload = json.loads(row["raw_payload"]) if isinstance(row["raw_payload"], str) else row["raw_payload"]
        trade_date = _parse_entered_trade_date(_s(payload.get("enteredTime")))
        parsed.append(payload)
        by_date[trade_date].append(payload)

    # Generate events
    all_events: list[dict] = []
    strategy_counts: dict[str, int] = defaultdict(int)

    for payload in parsed:
        trade_date = _parse_entered_trade_date(_s(payload.get("enteredTime")))
        strategy = _infer_strategy_from_order(payload, trade_date, by_date[trade_date])
        strategy_counts[strategy] += 1

        events = _broker_order_events(payload, strategy, account)
        all_events.extend(events)

    # Apply events
    stats = _apply_events(con, all_events)
    positions = materialize_positions(con)

    return {
        "orders": len(parsed),
        "events": stats["events"],
        "inserted": stats["inserted"],
        "duplicates": stats["duplicates"],
        "materialized": stats["materialized"],
        "positions": positions,
        "strategies": dict(strategy_counts),
    }
