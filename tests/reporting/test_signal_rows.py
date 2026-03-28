"""Tests for reporting.signal_rows."""

from __future__ import annotations

import csv
from pathlib import Path

import duckdb
import pytest

from reporting.db import init_schema, query_one
from reporting.signal_rows import import_constantstable_signal_rows


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


def test_import_constantstable_signal_rows(db, tmp_path):
    path = tmp_path / "leo_stable_test.csv"
    _write_csv(
        path,
        [
            {
                "Date": "2026-03-12",
                "TDate": "2026-03-13",
                "SPX": "6672.62",
                "Forward": "6670.5941",
                "VIX": "0.2773",
                "VixOne": "0.2858",
                "Limit": "6590",
                "CLimit": "6750",
                "Put": "0.95",
                "Call": "1.00",
                "LeftGo": "0.9992",
                "RightGo": "0.9897",
                "LImp": "0.19",
                "RImp": "0.20",
                "LReturn": "-0.0122",
                "RReturn": "0.0118",
                "FP": "",
                "RV": "",
                "RV5": "0.0097224719",
                "RV10": "0.0079413654",
                "RV20": "0.0072929960",
                "R": "",
                "AR": "",
                "AR2": "",
                "AR3": "",
                "TX": "-1.95",
                "Y": "2026",
                "M": "3",
            }
        ],
    )

    stats = import_constantstable_signal_rows(path, con=db)
    assert stats["rows_read"] == 1
    assert stats["inserted"] == 1

    row = query_one(
        """SELECT trade_date, expiry_date, spot, vix, put_strike, call_strike,
                  put_price, call_price, left_go, right_go, tx
           FROM strategy_signal_rows""",
        con=db,
    )
    assert str(row[0]) == "2026-03-12"
    assert str(row[1]) == "2026-03-13"
    assert row[2] == pytest.approx(6672.62)
    assert row[3] == pytest.approx(0.2773)
    assert row[4] == pytest.approx(6590)
    assert row[5] == pytest.approx(6750)
    assert row[6] == pytest.approx(0.95)
    assert row[7] == pytest.approx(1.00)
    assert row[8] == pytest.approx(0.9992)
    assert row[9] == pytest.approx(0.9897)
    assert row[10] == pytest.approx(-1.95)
