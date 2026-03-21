"""Tests for reporting.daily_pnl_email."""

from __future__ import annotations

import json
from datetime import date, datetime

import pytest

from reporting.db import close_all, get_connection, query_one
from reporting.daily_pnl_email import (
    _build_email,
    run_daily_pnl_email,
    send_behavior_email_if_needed,
)


@pytest.fixture
def isolated_db(tmp_path, monkeypatch):
    db_path = tmp_path / "portfolio.duckdb"
    close_all()
    monkeypatch.setenv("GAMMA_DB_PATH", str(db_path))
    yield db_path
    close_all()


class FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 3, 19, 16, 45, tzinfo=tz)


class LaterFixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 3, 21, 10, 0, tzinfo=tz)


def _same_day_expiry_order(status="FILLED"):
    return {
        "status": status,
        "tag": "TA_1michaelbelaygmailcom1755679459",
        "complexOrderStrategyType": "VERTICAL",
        "orderLegCollection": [
            {
                "instruction": "BUY_TO_OPEN",
                "quantity": 1,
                "instrument": {"symbol": "SPXW  260319P06535000"},
            },
            {
                "instruction": "SELL_TO_OPEN",
                "quantity": 1,
                "instrument": {"symbol": "SPXW  260319P06530000"},
            },
        ],
        "closeTime": "2026-03-19T20:10:00+0000",
        "enteredTime": "2026-03-19T20:10:00+0000",
        "orderType": "NET_DEBIT",
        "price": 0.95,
        "filledQuantity": 1,
        "accountNumber": "schwab",
    }


def test_build_email_reports_health_metrics():
    positions = [
        {
            "strategy": "dualside",
            "fill_date": "2026-03-10",
            "expiry": "2026-03-19",
            "option_type": "PUT",
            "strikes": [6510.0, 6520.0],
            "qty": 1,
            "entry_price": 1.20,
            "signal": "LONG",
            "exit_method": "EXPIRED",
            "close_date": None,
            "pnl": -130.0,
        },
        {
            "strategy": "constantstable",
            "fill_date": "2026-03-01",
            "expiry": "2026-03-12",
            "option_type": "PUT/CALL",
            "strikes": [6530.0, 6535.0, 6685.0, 6690.0],
            "qty": 1,
            "entry_price": 1.95,
            "signal": "LONG",
            "exit_method": "EXPIRED",
            "close_date": None,
            "pnl": -955.0,
        },
    ]

    subject, body = _build_email(positions, date(2026, 3, 19))

    assert subject == "[Gamma] Health: 5D -$130 | MTD -$1,085 | YTD -$1,085 — 2026-03-19"
    assert "Gamma Portfolio Pulse (Schwab) — 2026-03-19" in body
    assert "Portfolio" in body
    assert "Last 5 Sessions" in body
    assert "Strategy Contribution" in body
    assert "Risk State" in body
    assert "Current streak: L2" in body
    assert "DualSide" in body
    assert "ConstantStable (IC_LONG)" in body
    assert "2026-03-19" in body
    assert "Legend: BF=Butterfly, CS=ConstantStable, CS AM=Morning, DS=DualSide" in body
    assert "Open Positions" not in body


def test_behavior_email_only_sends_on_issues(monkeypatch):
    sends = []

    def fake_send(subject, body, dry_run=False):
        sends.append((subject, body, dry_run))
        return {"sent": True, "dry_run": dry_run}

    monkeypatch.setattr("reporting.daily_pnl_email._send_email", fake_send)

    raw_orders = [_same_day_expiry_order(status="PARTIALLY_FILLED")]
    positions = [
        {
            "strategy": "dualside",
            "fill_date": "2026-03-10",
            "expiry": "2026-03-18",
            "option_type": "PUT",
            "strikes": [6510.0, 6520.0],
            "qty": 1,
            "entry_price": 1.20,
            "signal": "LONG",
            "exit_method": "EXPIRED_NO_SETTLEMENT",
            "close_date": None,
            "pnl": None,
        }
    ]

    result = send_behavior_email_if_needed(
        raw_orders=raw_orders,
        positions=positions,
        report_date=date(2026, 3, 19),
    )

    assert result["sent"] is True
    assert result["issue_counts"]["order_anomalies"] == 1
    assert result["issue_counts"]["missing_settlement"] == 1
    assert sends
    assert "Behavior Check" in sends[0][0]
    assert "Order anomalies (1)" in sends[0][1]
    assert "Settlement gaps (1)" in sends[0][1]


def test_run_daily_pnl_email_sends_health_email_and_skips_behavior_when_clean(isolated_db, monkeypatch):
    sends = []

    monkeypatch.setattr("reporting.daily_pnl_email.datetime", FixedDateTime)
    monkeypatch.setattr(
        "reporting.daily_pnl_email.load_orders_from_schwab",
        lambda lookback_days=120: [_same_day_expiry_order()],
    )
    monkeypatch.setattr(
        "reporting.daily_pnl_email.load_settlements",
        lambda: {"2026-03-19": 6520.0},
    )

    def fake_send(subject, body, dry_run=False):
        sends.append({"subject": subject, "body": body, "dry_run": dry_run})
        return {"sent": True, "dry_run": dry_run, "subject": subject}

    monkeypatch.setattr("reporting.daily_pnl_email._send_email", fake_send)

    result = run_daily_pnl_email()

    assert result["archived"] is True
    assert result["positions"] == 1
    assert result["behavior_email"]["skipped"] == "no behavior issues"
    assert len(sends) == 1
    assert sends[0]["subject"] == "[Gamma] Health: 5D +$405 | MTD +$405 | YTD +$405 — 2026-03-19"
    assert "Last 5 Sessions" in sends[0]["body"]
    assert "Strategy Contribution" in sends[0]["body"]
    assert "Open Positions" not in sends[0]["body"]

    con = get_connection()
    row = query_one(
        """SELECT content FROM daily_report_outputs
           WHERE report_date = '2026-03-19' AND format = 'email'""",
        con=con,
    )
    assert row is not None
    payload = json.loads(row[0])
    assert payload["delivery"]["sent"] is True
    assert payload["settled_today"] == 1
    assert payload["scope"] == "schwab_only"


def test_run_daily_pnl_email_honors_report_date_override(isolated_db, monkeypatch):
    sends = []

    monkeypatch.setattr("reporting.daily_pnl_email.datetime", LaterFixedDateTime)
    monkeypatch.setattr(
        "reporting.daily_pnl_email.load_orders_from_schwab",
        lambda lookback_days=120: [_same_day_expiry_order()],
    )
    monkeypatch.setattr(
        "reporting.daily_pnl_email.load_settlements",
        lambda: {"2026-03-19": 6520.0},
    )

    def fake_send(subject, body, dry_run=False):
        sends.append({"subject": subject, "body": body, "dry_run": dry_run})
        return {"sent": True, "dry_run": dry_run, "subject": subject}

    monkeypatch.setattr("reporting.daily_pnl_email._send_email", fake_send)

    result = run_daily_pnl_email(report_date=date(2026, 3, 19))

    assert result["positions"] == 1
    assert len(sends) == 1
    assert sends[0]["subject"] == "[Gamma] Health: 5D +$405 | MTD +$405 | YTD +$405 — 2026-03-19"
