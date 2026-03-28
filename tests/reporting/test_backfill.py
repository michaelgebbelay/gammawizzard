"""Tests for reporting.backfill."""

from __future__ import annotations

import csv
import json
from pathlib import Path

import duckdb
import pytest

from reporting.backfill import API_TAG, backfill_csv_logs, backfill_from_broker
from reporting.db import init_schema, query_one


@pytest.fixture
def db():
    con = duckdb.connect(":memory:")
    init_schema(con)
    yield con
    con.close()


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _insert_broker_order(con, *, order_id: str, account: str, entered_time: str, price: float, filled_qty: int, symbols: list[str], tag: str | None = None):
    payload = {
        "orderId": order_id,
        "enteredTime": entered_time,
        "price": price,
        "filledQuantity": filled_qty,
        "status": "FILLED" if filled_qty > 0 else "CANCELED",
        "orderLegCollection": [
            {
                "instruction": "SELL_TO_OPEN" if i == 0 else "BUY_TO_OPEN",
                "quantity": 1.0,
                "instrument": {"symbol": sym, "assetType": "OPTION", "putCall": "PUT" if "P0" in sym else "CALL"},
            }
            for i, sym in enumerate(symbols)
        ],
    }
    if tag is not None:
        payload["tag"] = tag
    con.execute(
        """INSERT INTO broker_raw_orders
           (id, broker, account, order_id, fetched_at, as_of, raw_payload, idempotency_key)
           VALUES (?, 'schwab', ?, ?, ?, ?, ?, ?)""",
        [
            f"raw_{order_id}",
            account,
            order_id,
            entered_time,
            entered_time,
            json.dumps(payload),
            f"idem_{order_id}",
        ],
    )


class TestBackfill:
    def test_bf_backfill_creates_position(self, db, tmp_path):
        bf_csv = tmp_path / "bf.csv"
        _write_csv(
            bf_csv,
            [
                {
                    "ts_utc": "2026-03-11T20:15:00+00:00",
                    "ts_et": "2026-03-11T16:15:00-04:00",
                    "trade_date": "2026-03-11",
                    "expiry_date": "2026-03-14",
                    "signal": "SELL",
                    "config": "SELL_D35P",
                    "order_side": "CREDIT",
                    "qty": "1",
                    "spot": "5850",
                    "vix": "18.1",
                    "vix1d": "17.2",
                    "lower_strike": "5800",
                    "center_strike": "5850",
                    "upper_strike": "5900",
                    "width": "50",
                    "lower_osi": "SPXW  260314C05800000",
                    "center_osi": "SPXW  260314C05850000",
                    "upper_osi": "SPXW  260314C05900000",
                    "package_bid": "4.10",
                    "package_ask": "4.40",
                    "package_mid": "4.25",
                    "ladder_plan": "[4.25,4.20]",
                    "last_price": "4.20",
                    "filled_qty": "1",
                    "order_ids": "BF123",
                    "reason": "FILLED",
                }
            ],
        )

        stats = backfill_csv_logs(bf_csv=bf_csv, ds_csv=None, cs_csv=None, con=db, enrich_from_broker=False)

        assert stats["strategies"]["butterfly"]["inserted"] == 5
        assert query_one("SELECT COUNT(*) FROM strategy_runs", con=db)[0] == 1
        assert query_one("SELECT COUNT(*) FROM intended_trades", con=db)[0] == 1
        assert query_one("SELECT COUNT(*) FROM order_events", con=db)[0] == 1
        assert query_one("SELECT COUNT(*) FROM fills", con=db)[0] == 1
        assert query_one("SELECT COUNT(*) FROM positions", con=db)[0] == 1

    def test_ds_backfill_groups_two_sides_into_one_run(self, db, tmp_path):
        ds_csv = tmp_path / "ds.csv"
        _write_csv(
            ds_csv,
            [
                {
                    "ts_utc": "2026-03-12T20:13:00+00:00",
                    "ts_et": "2026-03-12T16:13:00-04:00",
                    "trade_date": "2026-03-12",
                    "tdate": "2026-03-19",
                    "name": "PUT_CREDIT",
                    "kind": "PUT",
                    "side": "CREDIT",
                    "direction": "SHORT",
                    "short_osi": "SPXW  260319P06670000",
                    "long_osi": "SPXW  260319P06660000",
                    "go": "",
                    "strength": "1.0",
                    "gw_price": "",
                    "qty_rule": "FIXED",
                    "vol_field": "VIX",
                    "vol_used": "VIX",
                    "vol_value": "18.2",
                    "vol_bucket": "1",
                    "vol_mult": "1",
                    "unit_dollars": "10000",
                    "oc": "20000",
                    "units": "1",
                    "qty_requested": "1",
                    "qty_filled": "1",
                    "ladder_prices": "[mid]",
                    "last_price": "4.05",
                    "nbbo_bid": "4.00",
                    "nbbo_ask": "4.10",
                    "nbbo_mid": "4.05",
                    "order_ids": "DSPUT1",
                    "reason": "FILLED",
                },
                {
                    "ts_utc": "2026-03-12T20:13:00+00:00",
                    "ts_et": "2026-03-12T16:13:00-04:00",
                    "trade_date": "2026-03-12",
                    "tdate": "2026-03-19",
                    "name": "CALL_DEBIT",
                    "kind": "CALL",
                    "side": "DEBIT",
                    "direction": "LONG",
                    "short_osi": "SPXW  260319C05890000",
                    "long_osi": "SPXW  260319C05880000",
                    "go": "",
                    "strength": "1.0",
                    "gw_price": "",
                    "qty_rule": "FIXED",
                    "vol_field": "VIX",
                    "vol_used": "VIX",
                    "vol_value": "18.2",
                    "vol_bucket": "1",
                    "vol_mult": "1",
                    "unit_dollars": "10000",
                    "oc": "20000",
                    "units": "1",
                    "qty_requested": "1",
                    "qty_filled": "1",
                    "ladder_prices": "[mid]",
                    "last_price": "1.25",
                    "nbbo_bid": "1.20",
                    "nbbo_ask": "1.30",
                    "nbbo_mid": "1.25",
                    "order_ids": "DSCALL1",
                    "reason": "FILLED",
                },
            ],
        )

        stats = backfill_csv_logs(bf_csv=None, ds_csv=ds_csv, cs_csv=None, con=db, enrich_from_broker=False)

        assert stats["strategies"]["dualside"]["inserted"] == 8
        assert query_one("SELECT COUNT(*) FROM strategy_runs", con=db)[0] == 1
        assert query_one("SELECT COUNT(*) FROM intended_trades", con=db)[0] == 2
        assert query_one("SELECT COUNT(*) FROM fills", con=db)[0] == 2
        assert query_one("SELECT COUNT(*) FROM positions", con=db)[0] == 2

    def test_cs_backfill_enriches_from_broker_when_order_id_missing(self, db, tmp_path):
        cs_csv = tmp_path / "cs.csv"
        _write_csv(
            cs_csv,
            [
                {
                    "ts_utc": "2026-02-03T00:15:00+00:00",
                    "ts_et": "2026-02-02T19:15:00-05:00",
                    "trade_date": "2026-02-02",
                    "tdate": "2026-02-03",
                    "name": "PUT_SHORT",
                    "kind": "PUT",
                    "side": "CREDIT",
                    "direction": "SHORT",
                    "short_osi": "SPXW  260203P06960000",
                    "long_osi": "SPXW  260203P06955000",
                    "go": "-1",
                    "strength": "0.2",
                    "qty_rule": "FIXED",
                    "vol_field": "VIX",
                    "vol_used": "VIX",
                    "vol_value": "20",
                    "vol_bucket": "2",
                    "vol_mult": "1",
                    "unit_dollars": "10000",
                    "oc": "10000",
                    "units": "1",
                    "qty_requested": "1",
                    "qty_filled": "1",
                    "ladder_prices": "[mid]",
                    "last_price": "",
                    "nbbo_bid": "",
                    "nbbo_ask": "",
                    "nbbo_mid": "",
                    "order_ids": "",
                    "reason": "FILLED",
                }
            ],
        )

        _insert_broker_order(
            db,
            order_id="CSB1",
            account="schwab",
            entered_time="2026-02-03T00:15:02+0000",
            price=1.55,
            filled_qty=1,
            symbols=["SPXW  260203P06960000", "SPXW  260203P06955000"],
        )

        stats = backfill_csv_logs(bf_csv=None, ds_csv=None, cs_csv=cs_csv, con=db, enrich_from_broker=True)

        assert stats["strategies"]["constantstable"]["inserted"] == 5
        row = query_one("SELECT order_id, fill_price FROM fills", con=db)
        assert row[0] == "CSB1"
        assert row[1] == pytest.approx(1.55)

    def test_backfill_rerun_is_idempotent(self, db, tmp_path):
        bf_csv = tmp_path / "bf.csv"
        _write_csv(
            bf_csv,
            [
                {
                    "ts_utc": "2026-03-11T20:15:00+00:00",
                    "ts_et": "2026-03-11T16:15:00-04:00",
                    "trade_date": "2026-03-11",
                    "expiry_date": "2026-03-14",
                    "signal": "SELL",
                    "config": "SELL_D35P",
                    "order_side": "CREDIT",
                    "qty": "1",
                    "spot": "5850",
                    "vix": "18.1",
                    "vix1d": "17.2",
                    "lower_strike": "5800",
                    "center_strike": "5850",
                    "upper_strike": "5900",
                    "width": "50",
                    "lower_osi": "SPXW  260314C05800000",
                    "center_osi": "SPXW  260314C05850000",
                    "upper_osi": "SPXW  260314C05900000",
                    "package_bid": "4.10",
                    "package_ask": "4.40",
                    "package_mid": "4.25",
                    "ladder_plan": "[4.25,4.20]",
                    "last_price": "4.20",
                    "filled_qty": "1",
                    "order_ids": "BF123",
                    "reason": "FILLED",
                }
            ],
        )

        first = backfill_csv_logs(bf_csv=bf_csv, ds_csv=None, cs_csv=None, con=db, enrich_from_broker=False)
        second = backfill_csv_logs(bf_csv=bf_csv, ds_csv=None, cs_csv=None, con=db, enrich_from_broker=False)

        assert first["inserted"] == 5
        assert second["duplicates"] == 5
        assert query_one("SELECT COUNT(*) FROM raw_events", con=db)[0] == 5
        assert query_one("SELECT COUNT(*) FROM positions", con=db)[0] == 1


class TestBrokerBackfill:
    def test_vertical_creates_position(self, db):
        """A single API-tagged FILLED vertical creates strategy_run + intent + fill + position."""
        _insert_broker_order(
            db,
            order_id="V100",
            account="schwab",
            entered_time="2026-03-12T20:13:00+0000",
            price=2.45,
            filled_qty=1,
            symbols=["SPXW  260319P06520000", "SPXW  260319P06510000"],
            tag=API_TAG,
        )
        stats = backfill_from_broker(account="schwab", con=db)

        assert stats["orders"] == 1
        assert stats["inserted"] == 5  # run + intent + order + fill + post_step
        assert stats["strategies"]["constantstable"] == 1
        assert query_one("SELECT COUNT(*) FROM strategy_runs", con=db)[0] == 1
        assert query_one("SELECT COUNT(*) FROM fills", con=db)[0] == 1
        assert query_one("SELECT COUNT(*) FROM positions", con=db)[0] == 1

        # Verify fill details preserved
        row = query_one("SELECT order_id, fill_price, fill_qty FROM fills", con=db)
        assert row[0] == "V100"
        assert row[1] == pytest.approx(2.45)
        assert row[2] == 1

    def test_butterfly_detected_by_leg_count(self, db):
        """A 3-leg API-tagged order is classified as butterfly."""
        payload = {
            "orderId": "BF200",
            "enteredTime": "2026-03-09T20:08:52+0000",
            "price": 11.75,
            "filledQuantity": 1,
            "status": "FILLED",
            "tag": API_TAG,
            "orderType": "NET_CREDIT",
            "orderLegCollection": [
                {"instruction": "SELL_TO_OPEN", "quantity": 1.0,
                 "instrument": {"symbol": "SPXW  260312C06725000", "putCall": "CALL", "assetType": "OPTION"}},
                {"instruction": "BUY_TO_OPEN", "quantity": 2.0,
                 "instrument": {"symbol": "SPXW  260312C06795000", "putCall": "CALL", "assetType": "OPTION"}},
                {"instruction": "SELL_TO_OPEN", "quantity": 1.0,
                 "instrument": {"symbol": "SPXW  260312C06865000", "putCall": "CALL", "assetType": "OPTION"}},
            ],
        }
        db.execute(
            """INSERT INTO broker_raw_orders
               (id, broker, account, order_id, fetched_at, as_of, raw_payload, idempotency_key)
               VALUES ('r_bf200', 'schwab', 'schwab', 'BF200', '2026-03-09T20:08:52+0000',
                        '2026-03-09T20:08:52+0000', ?, 'idem_bf200')""",
            [json.dumps(payload)],
        )

        stats = backfill_from_broker(account="schwab", con=db)
        assert stats["strategies"]["butterfly"] == 1
        assert query_one("SELECT strategy FROM strategy_runs", con=db)[0] == "butterfly"

    def test_put_and_call_pair_detected_as_dualside(self, db):
        """PUT + CALL verticals on the same date → dualside."""
        _insert_broker_order(
            db,
            order_id="DS_PUT",
            account="schwab",
            entered_time="2026-03-09T20:14:25+0000",
            price=1.15,
            filled_qty=6,
            symbols=["SPXW  260310P06720000", "SPXW  260310P06725000"],
            tag=API_TAG,
        )
        _insert_broker_order(
            db,
            order_id="DS_CALL",
            account="schwab",
            entered_time="2026-03-09T20:14:43+0000",
            price=1.30,
            filled_qty=6,
            symbols=["SPXW  260310C06845000", "SPXW  260310C06850000"],
            tag=API_TAG,
        )

        stats = backfill_from_broker(account="schwab", con=db)
        assert stats["orders"] == 2
        assert stats["strategies"]["dualside"] == 2
        assert query_one("SELECT COUNT(*) FROM positions", con=db)[0] == 2

    def test_broker_backfill_idempotent(self, db):
        """Running broker backfill twice produces no duplicates."""
        _insert_broker_order(
            db,
            order_id="V300",
            account="schwab",
            entered_time="2026-03-12T20:13:00+0000",
            price=4.05,
            filled_qty=1,
            symbols=["SPXW  260319P06660000", "SPXW  260319P06670000"],
            tag=API_TAG,
        )

        first = backfill_from_broker(account="schwab", con=db)
        second = backfill_from_broker(account="schwab", con=db)

        assert first["inserted"] == 5
        assert second["duplicates"] == 5
        assert query_one("SELECT COUNT(*) FROM raw_events", con=db)[0] == 5
        assert query_one("SELECT COUNT(*) FROM positions", con=db)[0] == 1

    def test_non_api_tagged_orders_ignored(self, db):
        """Orders without the API tag are not backfilled."""
        _insert_broker_order(
            db,
            order_id="TOS1",
            account="schwab",
            entered_time="2026-03-12T20:13:00+0000",
            price=2.00,
            filled_qty=1,
            symbols=["SPXW  260319P06600000", "SPXW  260319P06590000"],
            tag="API_TOS:Empty",
        )

        stats = backfill_from_broker(account="schwab", con=db)
        assert stats["orders"] == 0
        assert query_one("SELECT COUNT(*) FROM raw_events", con=db)[0] == 0
