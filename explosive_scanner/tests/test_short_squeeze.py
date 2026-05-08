import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from explosive_scanner.short_squeeze import (
    SECOND_DAY_CONFIRMATION,
    attach_short_interest,
    build_variant_signal_frames,
    catalyst_match,
    settlement_dates_from_calendar,
)


def test_settlement_dates_use_last_trading_day_before_15th_and_month_end():
    trading_dates = pd.to_datetime(
        [
            "2021-05-03",
            "2021-05-04",
            "2021-05-05",
            "2021-05-06",
            "2021-05-07",
            "2021-05-10",
            "2021-05-11",
            "2021-05-12",
            "2021-05-13",
            "2021-05-14",
            "2021-05-17",
            "2021-05-18",
            "2021-05-19",
            "2021-05-20",
            "2021-05-21",
            "2021-05-24",
            "2021-05-25",
            "2021-05-26",
            "2021-05-27",
            "2021-05-28",
        ]
    )
    got = settlement_dates_from_calendar(trading_dates)
    assert got == [pd.Timestamp("2021-05-14"), pd.Timestamp("2021-05-28")]


def test_attach_short_interest_uses_availability_date_not_settlement_date():
    trading_dates = pd.to_datetime(
        [
            "2024-01-10",
            "2024-01-11",
            "2024-01-12",
            "2024-01-16",
            "2024-01-17",
            "2024-01-18",
            "2024-01-19",
            "2024-01-22",
        ]
    )
    panel = pd.DataFrame(
        {
            "symbol": ["AAA", "AAA"],
            "date": pd.to_datetime(["2024-01-19", "2024-01-22"]),
            "close": [10.0, 10.5],
            "volume": [1000, 1200],
            "ret_1d": [0.01, 0.05],
            "relative_volume_20d": [1.0, 2.0],
            "close_location": [0.5, 0.8],
            "avg_20d_dollar_volume": [6_000_000.0, 6_000_000.0],
        }
    )
    short_interest = pd.DataFrame(
        {
            "symbol": ["AAA"],
            "settlement_date": pd.to_datetime(["2024-01-10"]),
            "current_short_interest": [500_000],
            "previous_short_interest": [450_000],
            "average_daily_volume": [100_000],
            "days_to_cover": [5.0],
            "short_interest_change_pct": [0.1111],
        }
    )

    out = attach_short_interest(panel, short_interest, trading_dates)
    on_0119 = out.loc[out["date"] == pd.Timestamp("2024-01-19")].iloc[0]
    on_0122 = out.loc[out["date"] == pd.Timestamp("2024-01-22")].iloc[0]

    assert pd.isna(on_0119["days_to_cover"])
    assert on_0122["days_to_cover"] == 5.0
    assert on_0122["si_available_date"] == pd.Timestamp("2024-01-22")


def test_second_day_confirmation_waits_for_follow_through():
    panel = pd.DataFrame(
        {
            "symbol": ["AAA", "AAA", "AAA"],
            "date": pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"]),
            "close": [10.0, 10.8, 10.3],
            "volume": [1000, 900, 700],
            "days_to_cover": [4.0, 4.0, 4.0],
            "days_to_cover_pctile": [0.9, 0.9, 0.9],
            "ret_1d": [0.12, 0.08, -0.03],
            "relative_volume_20d": [5.5, 4.5, 3.0],
            "close_location": [0.80, 0.65, 0.40],
            "avg_20d_dollar_volume": [6_000_000.0, 6_000_000.0, 6_000_000.0],
            "squeeze_score": [0.95, 0.80, 0.40],
            "short_interest_change_pct": [0.0, 0.0, 0.0],
        }
    )
    signals = build_variant_signal_frames(
        panel,
        analysis_start="2024-01-02",
        analysis_end="2024-01-04",
    )
    second_day = signals[SECOND_DAY_CONFIRMATION]

    assert len(second_day) == 1
    row = second_day.iloc[0]
    assert row["date"] == pd.Timestamp("2024-01-03")
    assert row["signal_date_day1"] == pd.Timestamp("2024-01-02")


def test_catalyst_match_detects_user_requested_terms():
    assert catalyst_match({"title": "Company raises outlook after earnings beat", "body": "", "channels": [], "tags": []})
    assert catalyst_match({"title": "", "body": "Strategic review follows debt refinancing news", "channels": [], "tags": []})
    assert not catalyst_match({"title": "Quiet technical bounce", "body": "No obvious catalyst here", "channels": [], "tags": []})

