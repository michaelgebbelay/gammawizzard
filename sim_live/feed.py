"""Leo data feed adapters for the live game."""

from __future__ import annotations

import csv
import os
from datetime import date, datetime
from pathlib import Path
from typing import Optional

from sim_live.config import PUBLIC_COLUMNS, SUPPRESSED_COLUMNS
from sim_live.types import PublicSnapshot, RoundOutcomes


def _to_date(v) -> date:
    if isinstance(v, date):
        return v
    return datetime.strptime(str(v)[:10], "%Y-%m-%d").date()


def _to_float(v, default: float = 0.0) -> float:
    try:
        if v is None or v == "":
            return default
        return float(v)
    except (TypeError, ValueError):
        return default


class LeoFeed:
    """Fetch rows from Leo data source and expose public/private views."""

    def __init__(
        self,
        csv_path: Optional[Path] = None,
        api_url: Optional[str] = None,
        api_token: Optional[str] = None,
    ):
        self.csv_path = Path(csv_path) if csv_path else None
        self.api_url = api_url or os.environ.get("LEO_LIVE_URL", "").strip()
        self.api_token = api_token or os.environ.get("LEO_LIVE_TOKEN", "").strip()
        self._rows_by_date: dict[str, dict] = {}
        if self.csv_path:
            self._load_csv()

    def _load_csv(self) -> None:
        with self.csv_path.open(newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                d = str(row.get("Date", "")).strip()[:10]
                if d:
                    self._rows_by_date[d] = dict(row)

    def _fetch_api_row(self, signal_date: date) -> Optional[dict]:
        if not self.api_url:
            return None

        try:
            import requests
        except ImportError:
            return None

        headers = {"Accept": "application/json"}
        if self.api_token:
            headers["Authorization"] = f"Bearer {self.api_token}"

        resp = requests.get(
            self.api_url,
            params={"date": signal_date.isoformat()},
            headers=headers,
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json()

        if isinstance(data, list):
            for row in data:
                if str(row.get("Date", "")).strip()[:10] == signal_date.isoformat():
                    return dict(row)
            return None
        if isinstance(data, dict):
            if "Date" in data:
                return dict(data)
            rows = data.get("rows") or data.get("items") or []
            if isinstance(rows, list):
                for row in rows:
                    if str(row.get("Date", "")).strip()[:10] == signal_date.isoformat():
                        return dict(row)
        return None

    def get_raw_row(self, signal_date: date) -> Optional[dict]:
        key = signal_date.isoformat()
        if key in self._rows_by_date:
            return dict(self._rows_by_date[key])

        row = self._fetch_api_row(signal_date)
        if row:
            return row
        return None

    def get_public_snapshot(self, signal_date: date) -> Optional[PublicSnapshot]:
        row = self.get_raw_row(signal_date)
        if row is None:
            return None

        # Only pass non-leaky columns to players.
        payload = {
            c: row.get(c)
            for c in PUBLIC_COLUMNS
            if c in row and c not in SUPPRESSED_COLUMNS
        }

        # Keep a narrow, typed feature object for player logic.
        return PublicSnapshot(
            signal_date=_to_date(row.get("Date")),
            tdate=_to_date(row.get("TDate")),
            spx=_to_float(row.get("SPX")),
            vix=_to_float(row.get("VIX")),
            vix_one=_to_float(row.get("VixOne")),
            rv=_to_float(row.get("RV")),
            rv5=_to_float(row.get("RV5")),
            rv10=_to_float(row.get("RV10")),
            rv20=_to_float(row.get("RV20")),
            r=_to_float(row.get("R")),
            rx=_to_float(row.get("RX")),
            forward=_to_float(row.get("Forward")),
            limit=_to_float(row.get("Limit")),
            climit=_to_float(row.get("CLimit")),
            payload=payload,
        )

    def get_outcomes(self, signal_date: date) -> Optional[RoundOutcomes]:
        row = self.get_raw_row(signal_date)
        if row is None:
            return None

        # Settlement data stays private to engine/judge.
        if "Profit" not in row or "CProfit" not in row:
            return None
        return RoundOutcomes(
            put_short_pnl_5w=_to_float(row.get("Profit")),
            call_short_pnl_5w=_to_float(row.get("CProfit")),
            tx=_to_float(row.get("TX")),
        )

