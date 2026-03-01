"""Live feed adapters for public features, settlement spot, and option chains."""

from __future__ import annotations

import csv
import json
import math
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urlsplit

from sim_gpt.config import (
    CHAIN_SOURCE,
    DEFAULT_LIVE_API_URL,
    MAX_LEG_SPREAD_POINTS,
    PUBLIC_COLUMNS,
    SCHWAB_STRIKE_COUNT,
    SCHWAB_SYMBOL,
    SUPPRESSED_COLUMNS,
)
from sim_gpt.types import ChainSnapshot, OptionQuote, PublicSnapshot, SettlementSnapshot


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


def _sanitize_token(v: str | None) -> str:
    token = (v or "").strip().strip('"').strip("'")
    if token.lower().startswith("bearer "):
        parts = token.split(None, 1)
        return parts[1] if len(parts) > 1 else ""
    return token


def _safe_dt_utc(v) -> datetime | None:
    if v in (None, ""):
        return None
    if isinstance(v, datetime):
        if v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v.astimezone(timezone.utc)
    s = str(v).strip()
    if not s:
        return None
    try:
        # Schwab often returns epoch milliseconds.
        if s.isdigit():
            iv = int(s)
            if iv > 10_000_000_000:
                return datetime.fromtimestamp(iv / 1000.0, tz=timezone.utc)
            return datetime.fromtimestamp(iv, tz=timezone.utc)
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
    except (ValueError, TypeError, OverflowError):
        return None


def _non_empty_row(row: dict | None) -> dict:
    return dict(row) if isinstance(row, dict) else {}


class LeoFeed:
    """Fetch rows from data source and expose public + settlement + chain views."""

    def __init__(
        self,
        csv_path: Optional[Path] = None,
        api_url: Optional[str] = None,
        api_token: Optional[str] = None,
    ):
        self.csv_path = Path(csv_path) if csv_path else None
        self.api_url = api_url or os.environ.get("LEO_LIVE_URL", "").strip() or DEFAULT_LIVE_API_URL
        self.api_token = _sanitize_token(api_token or os.environ.get("LEO_LIVE_TOKEN", ""))
        self.gw_email = os.environ.get("GW_EMAIL", "").strip()
        self.gw_password = os.environ.get("GW_PASSWORD", "").strip()
        self._auth_token = self.api_token
        self._rows_by_date: dict[str, list[dict]] = {}
        self._api_cache: dict[str, list[dict]] = {}
        self._chain_cache: dict[str, ChainSnapshot] = {}
        if self.csv_path:
            self._load_csv()

    def _load_csv(self) -> None:
        with self.csv_path.open(newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                d = str(row.get("Date", "")).strip()[:10]
                if d:
                    self._rows_by_date.setdefault(d, []).append(dict(row))

    def _api_base_url(self) -> str:
        parts = urlsplit(self.api_url)
        if not parts.scheme or not parts.netloc:
            return ""
        return f"{parts.scheme}://{parts.netloc}"

    def _auth_with_gw_credentials(self, requests_mod) -> str:
        if not (self.gw_email and self.gw_password):
            return ""
        base = self._api_base_url()
        if not base:
            return ""

        resp = requests_mod.post(
            f"{base}/goauth/authenticateFireUser",
            data={"email": self.gw_email, "password": self.gw_password},
            timeout=20,
        )
        resp.raise_for_status()
        payload = resp.json() if resp.content else {}
        token = _sanitize_token(payload.get("token") or payload.get("Token"))
        if token:
            self._auth_token = token
        return token

    @staticmethod
    def _rows_from_payload(payload: object) -> list[dict]:
        rows: list[dict] = []

        if isinstance(payload, list):
            rows.extend(dict(r) for r in payload if isinstance(r, dict))
        elif isinstance(payload, dict):
            if "Date" in payload:
                rows.append(dict(payload))
            else:
                for key in ("Trade", "Predictions", "rows", "items", "data", "Data"):
                    v = payload.get(key)
                    if isinstance(v, dict):
                        rows.append(dict(v))
                    elif isinstance(v, list):
                        rows.extend(dict(r) for r in v if isinstance(r, dict))

                if not rows:
                    for v in payload.values():
                        if isinstance(v, dict) and "Date" in v:
                            rows.append(dict(v))
                        elif isinstance(v, list):
                            rows.extend(dict(r) for r in v if isinstance(r, dict) and "Date" in r)

        deduped: list[dict] = []
        seen: set[str] = set()
        for row in rows:
            d = str(row.get("Date", "")).strip()[:10]
            if not d:
                continue
            sig = json.dumps(row, sort_keys=True, default=str)
            if sig in seen:
                continue
            seen.add(sig)
            deduped.append(dict(row))
        return deduped

    def _fetch_api_rows(self, signal_date: date) -> list[dict]:
        if not self.api_url:
            return []

        try:
            import requests
        except ImportError:
            return []

        headers = {"Accept": "application/json"}
        if self._auth_token:
            headers["Authorization"] = f"Bearer {_sanitize_token(self._auth_token)}"

        def _get(h: dict) -> object:
            return requests.get(
                self.api_url,
                params={"date": signal_date.isoformat()},
                headers=h,
                timeout=20,
            )

        resp = _get(headers)
        if resp.status_code in (401, 403):
            fresh = self._auth_with_gw_credentials(requests)
            if fresh:
                headers["Authorization"] = f"Bearer {fresh}"
                resp = _get(headers)
        resp.raise_for_status()
        data = resp.json()

        rows = self._rows_from_payload(data)
        return [dict(row) for row in rows if str(row.get("Date", "")).strip()[:10] == signal_date.isoformat()]

    def get_rows_for_date(self, signal_date: date) -> list[dict]:
        key = signal_date.isoformat()
        if key in self._rows_by_date:
            return [dict(r) for r in self._rows_by_date[key]]
        if key in self._api_cache:
            return [dict(r) for r in self._api_cache[key]]

        rows = self._fetch_api_rows(signal_date)
        self._api_cache[key] = [dict(r) for r in rows]
        return [dict(r) for r in rows]

    def get_raw_row(self, signal_date: date) -> Optional[dict]:
        rows = self.get_rows_for_date(signal_date)
        if not rows:
            return None
        if len(rows) > 1:
            raise ValueError(
                f"Expected exactly one feed row for {signal_date.isoformat()}, got {len(rows)}"
            )
        return dict(rows[0])

    def validate_signal_row(self, signal_date: date) -> dict:
        row = self.get_raw_row(signal_date)
        if row is None:
            raise ValueError(f"No data found for signal date: {signal_date.isoformat()}")

        row_date = _to_date(row.get("Date"))
        tdate = _to_date(row.get("TDate"))
        if row_date != signal_date:
            raise ValueError(
                f"Row date mismatch: expected {signal_date.isoformat()}, got {row_date.isoformat()}"
            )
        if tdate <= row_date:
            raise ValueError(
                f"Invalid TDate for {signal_date.isoformat()}: TDate must be > Date, got {tdate.isoformat()}"
            )
        return dict(row)

    def get_public_snapshot(self, signal_date: date) -> Optional[PublicSnapshot]:
        row = self.validate_signal_row(signal_date)

        payload = {c: row.get(c) for c in PUBLIC_COLUMNS if c in row and c not in SUPPRESSED_COLUMNS}

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
            payload=payload,
        )

    def get_settlement_snapshot(self, settlement_date: date) -> Optional[SettlementSnapshot]:
        row = self.get_raw_row(settlement_date)
        if row is None:
            return None
        spx = _to_float(row.get("SPX"), default=math.nan)
        if math.isnan(spx) or spx <= 0:
            return None
        return SettlementSnapshot(
            settlement_date=settlement_date,
            settlement_spx=spx,
        )

    def get_entry_chain(self, signal_date: date, tdate: date) -> ChainSnapshot:
        if CHAIN_SOURCE.lower() != "schwab":
            raise ValueError(f"Unsupported CHAIN_SOURCE={CHAIN_SOURCE}")

        cache_key = f"{signal_date.isoformat()}|{tdate.isoformat()}"
        if cache_key in self._chain_cache:
            return self._chain_cache[cache_key]

        try:
            from scripts.schwab_token_keeper import schwab_client
        except Exception as e:
            raise RuntimeError(f"Failed to import Schwab client: {e}") from e

        try:
            client = schwab_client()
        except KeyError as e:
            missing = str(e).strip("'")
            raise RuntimeError(
                f"Missing Schwab env var: {missing}. "
                "Set SCHWAB_APP_KEY, SCHWAB_APP_SECRET, and token env/file."
            ) from e
        resp = client.get_option_chain(
            SCHWAB_SYMBOL,
            contract_type=client.Options.ContractType.ALL,
            strike_count=SCHWAB_STRIKE_COUNT,
            include_underlying_quote=True,
            from_date=tdate,
            to_date=tdate,
            option_type=client.Options.Type.ALL,
        )
        resp.raise_for_status()
        raw = _non_empty_row(resp.json())
        chain = self._parse_schwab_chain(raw, signal_date=signal_date, tdate=tdate)
        self._chain_cache[cache_key] = chain
        return chain

    def _parse_schwab_chain(self, raw: dict, signal_date: date, tdate: date) -> ChainSnapshot:
        underlying_spx = _to_float(raw.get("underlyingPrice"))
        if underlying_spx <= 0:
            underlying = raw.get("underlying", {})
            if isinstance(underlying, dict):
                underlying_spx = _to_float(
                    underlying.get("last")
                    or underlying.get("mark")
                    or underlying.get("close")
                    or underlying.get("bid")
                    or underlying.get("ask")
                )

        asof = _safe_dt_utc(raw.get("underlyingPriceTime")) or _safe_dt_utc(raw.get("quoteTime"))
        underlying = _non_empty_row(raw.get("underlying"))
        asof = asof or _safe_dt_utc(
            underlying.get("quoteTime")
            or underlying.get("tradeTime")
            or underlying.get("closeTime")
            or underlying.get("lastTradeTime")
        )
        asof = asof or datetime.now(timezone.utc)

        puts = self._extract_side(raw.get("putExpDateMap"), "P", tdate)
        calls = self._extract_side(raw.get("callExpDateMap"), "C", tdate)
        if not puts or not calls:
            raise ValueError(
                f"No usable Schwab contracts for TDate={tdate.isoformat()} "
                f"(puts={len(puts)} calls={len(calls)})"
            )

        return ChainSnapshot(
            signal_date=signal_date,
            tdate=tdate,
            asof_utc=asof,
            underlying_spx=underlying_spx,
            puts=tuple(sorted(puts, key=lambda q: q.strike)),
            calls=tuple(sorted(calls, key=lambda q: q.strike)),
        )

    def _extract_side(self, exp_map: object, put_call: str, tdate: date) -> list[OptionQuote]:
        out: list[OptionQuote] = []
        if not isinstance(exp_map, dict):
            return out
        tdate_s = tdate.isoformat()
        for exp_key, strikes_data in exp_map.items():
            exp_date = str(exp_key).split(":")[0].strip()
            if exp_date != tdate_s or not isinstance(strikes_data, dict):
                continue

            for strike_key, contract_list in strikes_data.items():
                if not isinstance(contract_list, list) or not contract_list:
                    continue
                c = _non_empty_row(contract_list[0])

                bid = _to_float(c.get("bid"), default=math.nan)
                ask = _to_float(c.get("ask"), default=math.nan)
                strike = _to_float(strike_key, default=math.nan)
                if math.isnan(strike) or strike <= 0:
                    continue
                if math.isnan(bid) or math.isnan(ask):
                    continue
                if ask < bid:
                    continue
                if ask - bid > MAX_LEG_SPREAD_POINTS:
                    continue

                mid = _to_float(c.get("mark"), default=(bid + ask) / 2.0)
                if mid <= 0:
                    mid = (bid + ask) / 2.0

                out.append(
                    OptionQuote(
                        strike=strike,
                        put_call=put_call,
                        bid=bid,
                        ask=ask,
                        mid=mid,
                        delta=_to_float(c.get("delta")),
                        iv=_to_float(c.get("volatility")),
                    )
                )
        return out
