"""Tests for reporting.daily_pnl_email."""

from __future__ import annotations

import json
from datetime import date, datetime

import pytest

from reporting.db import close_all, get_connection, query_one
from reporting.daily_pnl_email import (
    _build_email,
    _build_today_trades_section,
    _classify_signal,
    _fill_status_str,
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
    assert "Gamma Portfolio Pulse" in body and "2026-03-19" in body
    assert "Portfolio" in body
    assert "Last 5 Sessions" in body
    assert "Strategy Contribution" in body
    assert "Risk State" in body
    assert "Current streak: L2" in body
    assert "DualSide" in body
    assert "ConstantStable" in body
    assert "2026-03-19" in body
    assert "Legend: CS=ConstantStable incl. morning, DS=DualSide, BF=Butterfly" in body
    assert "CS Morning" not in body
    assert "Behavior anomalies arrive in a separate email when needed." not in body
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
    assert "[Gamma] Health:" in sends[0]["subject"] and "2026-03-19" in sends[0]["subject"]


# ---------------------------------------------------------------------------
# Trade Summary Tests
# ---------------------------------------------------------------------------

def _schwab_order(status="FILLED", strategy_tag=True, order_type="NET_DEBIT",
                  price=1.95, filled_qty=3, legs=None, entered="2026-03-28T20:13:00+0000",
                  status_description=""):
    """Build a minimal Schwab order for testing."""
    if legs is None:
        legs = [
            {"instruction": "BUY_TO_OPEN", "quantity": filled_qty,
             "instrument": {"symbol": "SPXW  260331P05800000"}},
            {"instruction": "SELL_TO_OPEN", "quantity": filled_qty,
             "instrument": {"symbol": "SPXW  260331P05795000"}},
            {"instruction": "BUY_TO_OPEN", "quantity": filled_qty,
             "instrument": {"symbol": "SPXW  260331C05920000"}},
            {"instruction": "SELL_TO_OPEN", "quantity": filled_qty,
             "instrument": {"symbol": "SPXW  260331C05925000"}},
        ]
    order = {
        "orderId": 1,
        "status": status,
        "enteredTime": entered,
        "orderType": order_type,
        "complexOrderStrategyType": "CUSTOM",
        "price": price,
        "filledQuantity": filled_qty,
        "orderLegCollection": legs,
    }
    if strategy_tag:
        order["tag"] = "TA_1michaelbelaygmailcom1755679459"
    if status_description:
        order["statusDescription"] = status_description
    return order


def _tt_order(account_label="TT-IRA", default_strategy="Novix", status="Filled",
              filled_qty=2, total_qty=2, price=1.80, width=5):
    """Build a minimal normalised TT order for testing."""
    return {
        "broker": "tt",
        "account_label": account_label,
        "account_number": "5WT20360",
        "default_strategy": default_strategy,
        "status": status,
        "dt_et": datetime(2026, 3, 28, 16, 13),
        "filled_qty": filled_qty,
        "total_qty": total_qty,
        "price": price,
        "price_effect": "Debit",
        "strikes": [5795.0, 5800.0, 5920.0, 5925.0],
        "width": width,
        "legs": [
            {"action": "Buy to Open", "qty": filled_qty, "strike": 5800.0, "option_type": "PUT", "expiry": "2026-03-31"},
            {"action": "Sell to Open", "qty": filled_qty, "strike": 5795.0, "option_type": "PUT", "expiry": "2026-03-31"},
            {"action": "Buy to Open", "qty": filled_qty, "strike": 5920.0, "option_type": "CALL", "expiry": "2026-03-31"},
            {"action": "Sell to Open", "qty": filled_qty, "strike": 5925.0, "option_type": "CALL", "expiry": "2026-03-31"},
        ],
        "num_strikes": 4,
    }


class TestTradeSummary:
    """Tests for _build_today_trades_section."""

    def test_filled_order_shows_in_summary(self):
        section = _build_today_trades_section(
            [_schwab_order()], [], date(2026, 3, 28),
        )
        assert "FILLED" in section
        assert "IC_LONG" in section
        assert "ConstantStable" in section

    def test_rejected_order_shows_reason(self):
        order = _schwab_order(
            status="REJECTED",
            filled_qty=0,
            status_description="Insufficient buying power",
        )
        section = _build_today_trades_section([order], [], date(2026, 3, 28))
        assert "REJECTED" in section
        assert "Insufficient buying power" in section

    def test_expired_order_shows_reason(self):
        order = _schwab_order(status="EXPIRED", filled_qty=0)
        section = _build_today_trades_section([order], [], date(2026, 3, 28))
        assert "EXPIRED" in section
        assert "order expired unfilled" in section

    def test_canceled_order_shows_in_summary(self):
        order = _schwab_order(status="CANCELED", filled_qty=0)
        section = _build_today_trades_section([order], [], date(2026, 3, 28))
        assert "CANCELED" in section

    def test_tt_ira_order_labeled_novix(self):
        """P1 fix: TT-IRA orders should be labeled Novix, not ConstantStable."""
        tt = _tt_order(account_label="TT-IRA", default_strategy="Novix")
        section = _build_today_trades_section([], [tt], date(2026, 3, 28))
        assert "Novix" in section
        assert "ConstantStable" not in section

    def test_tt_individual_order_labeled_cs(self):
        """TT-Individual 5-wide orders should be labeled ConstantStable."""
        tt = _tt_order(account_label="TT-Indv", default_strategy="ConstantStable")
        section = _build_today_trades_section([], [tt], date(2026, 3, 28))
        assert "ConstantStable" in section

    def test_missing_strategies_weekday(self):
        """No orders on a weekday should flag all expected strategies."""
        section = _build_today_trades_section([], [], date(2026, 3, 27))  # Friday
        assert "Missing" in section
        assert "Schwab CS" in section
        assert "Schwab DualSide" in section
        assert "TT-IRA Novix" in section
        assert "TT-Indv CS" in section

    def test_missing_strategies_not_on_weekend(self):
        """No missing alerts on weekends."""
        section = _build_today_trades_section([], [], date(2026, 3, 29))  # Sunday
        assert "Missing" not in section

    def test_no_missing_when_all_present(self):
        """With Schwab CS + DS and both TT accounts, no missing alert."""
        schwab_cs = _schwab_order()  # classified as constantstable (4:13 PM, 5-wide)
        # DualSide: 10-wide, 4:05 PM (16:05 ET = 20:05 UTC)
        schwab_ds = _schwab_order(
            entered="2026-03-28T20:05:00+0000",
            legs=[
                {"instruction": "SELL_TO_OPEN", "quantity": 5,
                 "instrument": {"symbol": "SPXW  260331P05810000"}},
                {"instruction": "BUY_TO_OPEN", "quantity": 5,
                 "instrument": {"symbol": "SPXW  260331P05800000"}},
            ],
            order_type="NET_CREDIT",
        )
        schwab_ds["complexOrderStrategyType"] = "VERTICAL"
        tt_ira = _tt_order(account_label="TT-IRA", default_strategy="Novix")
        tt_indv = _tt_order(account_label="TT-Indv", default_strategy="ConstantStable")
        section = _build_today_trades_section(
            [schwab_cs, schwab_ds], [tt_ira, tt_indv], date(2026, 3, 28),
        )
        assert "Missing" not in section


class TestClassifySignal:
    def test_ic_long(self):
        legs = [
            {"instruction": "BUY_TO_OPEN", "option_type": "PUT", "strike": 5800},
            {"instruction": "SELL_TO_OPEN", "option_type": "PUT", "strike": 5795},
            {"instruction": "BUY_TO_OPEN", "option_type": "CALL", "strike": 5920},
            {"instruction": "SELL_TO_OPEN", "option_type": "CALL", "strike": 5925},
        ]
        assert _classify_signal({"num_strikes": 4}, legs) == "IC_LONG"

    def test_ic_short(self):
        legs = [
            {"instruction": "SELL_TO_OPEN", "option_type": "PUT", "strike": 5800},
            {"instruction": "BUY_TO_OPEN", "option_type": "PUT", "strike": 5795},
            {"instruction": "SELL_TO_OPEN", "option_type": "CALL", "strike": 5920},
            {"instruction": "BUY_TO_OPEN", "option_type": "CALL", "strike": 5925},
        ]
        assert _classify_signal({"num_strikes": 4}, legs) == "IC_SHORT"

    def test_rr_long_put(self):
        legs = [
            {"instruction": "BUY_TO_OPEN", "option_type": "PUT", "strike": 5800},
            {"instruction": "SELL_TO_OPEN", "option_type": "PUT", "strike": 5795},
            {"instruction": "SELL_TO_OPEN", "option_type": "CALL", "strike": 5920},
            {"instruction": "BUY_TO_OPEN", "option_type": "CALL", "strike": 5925},
        ]
        assert _classify_signal({"num_strikes": 4}, legs) == "RR_LONG_PUT"

    def test_butterfly_buy(self):
        legs = [
            {"instruction": "BUY_TO_OPEN", "option_type": "PUT", "strike": 5780},
            {"instruction": "SELL_TO_OPEN", "option_type": "PUT", "strike": 5800},
            {"instruction": "BUY_TO_OPEN", "option_type": "PUT", "strike": 5820},
        ]
        assert _classify_signal({"num_strikes": 3, "order_type": "NET_DEBIT"}, legs) == "BF_BUY"


class TestFillStatus:
    def test_filled(self):
        assert _fill_status_str("FILLED", 3, 3) == "FILLED"

    def test_partial(self):
        assert _fill_status_str("FILLED", 2, 4) == "PARTIAL (2/4)"

    def test_rejected(self):
        assert _fill_status_str("REJECTED", 0, 3) == "REJECTED"

    def test_expired(self):
        assert _fill_status_str("EXPIRED", 0, 3) == "EXPIRED"

    def test_working(self):
        assert _fill_status_str("WORKING", 0, 3) == "WORKING"
