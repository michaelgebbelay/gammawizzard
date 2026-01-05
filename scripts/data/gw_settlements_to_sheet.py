#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SPX settlements → Google Sheet (GammaWizard preferred; Schwab fallback).

Env:
  GSHEET_ID, GOOGLE_SERVICE_ACCOUNT_JSON
  GW_BASE, GW_TOKEN, GW_EMAIL, GW_PASSWORD, GW_SETTLE_ENDPOINT (optional)
  SCHWAB_APP_KEY, SCHWAB_APP_SECRET, SCHWAB_TOKEN_JSON
  SETTLE_BACKFILL_DAYS (default 120)
"""
import base64
import json
import math
import os
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional

import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build as gbuild
from schwab.auth import client_from_token_file

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover (Python <3.9)
    from backports.zoneinfo import ZoneInfo  # type: ignore

ET = ZoneInfo("America/New_York")

HEADERS = ["exp_primary", "settle", "source", "updated_at"]
SETTLE_TAB = "sw_settlements"

# ---------- Sheets helpers ----------

def _decode_service_account(sa_json: str) -> Dict[str, Any]:
    s = sa_json.strip()
    if not s:
        raise RuntimeError("Missing GOOGLE_SERVICE_ACCOUNT_JSON")
    try:
        dec = base64.b64decode(s).decode("utf-8")
        if dec.strip().startswith("{"):
            s = dec
    except Exception:
        pass
    return json.loads(s)


def sheets_client():
    sid = os.environ["GSHEET_ID"]
    svc = gbuild(
        "sheets",
        "v4",
        credentials=service_account.Credentials.from_service_account_info(
            _decode_service_account(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]),
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        ),
    )
    return svc, sid


def ensure_tab_with_header(svc, sid: str, tab: str, headers: List[str]) -> None:
    meta = svc.spreadsheets().get(spreadsheetId=sid).execute()
    titles = {s["properties"]["title"] for s in meta.get("sheets", [])}
    if tab not in titles:
        svc.spreadsheets().batchUpdate(
            spreadsheetId=sid,
            body={"requests": [{"addSheet": {"properties": {"title": tab}}}]},
        ).execute()
    got = (
        svc.spreadsheets()
        .values()
        .get(spreadsheetId=sid, range=f"{tab}!1:1")
        .execute()
        .get("values", [])
    )
    if not got or got[0] != headers:
        svc.spreadsheets().values().update(
            spreadsheetId=sid,
            range=f"{tab}!A1",
            valueInputOption="RAW",
            body={"values": [headers]},
        ).execute()


def read_rows(svc, sid: str, tab: str, headers: List[str]) -> List[List[Any]]:
    last_col = chr(ord("A") + len(headers) - 1)
    resp = (
        svc.spreadsheets()
        .values()
        .get(spreadsheetId=sid, range=f"{tab}!A2:{last_col}")
        .execute()
    )
    vals = resp.get("values", [])
    out: List[List[Any]] = []
    for r in vals:
        row = list(r) + [""] * (len(headers) - len(r))
        out.append(row[: len(headers)])
    return out


def write_all_rows(
    svc, sid: str, tab: str, headers: List[str], rows: List[List[Any]]
) -> None:
    svc.spreadsheets().values().clear(spreadsheetId=sid, range=tab).execute()
    svc.spreadsheets().values().update(
        spreadsheetId=sid,
        range=f"{tab}!A1",
        valueInputOption="RAW",
        body={"values": [headers] + rows},
    ).execute()


# ---------- Shared parsing helpers ----------

def _parse_date(val: Any) -> Optional[date]:
    if isinstance(val, date) and not isinstance(val, datetime):
        return val
    if isinstance(val, datetime):
        dt = val.astimezone(ET) if val.tzinfo else val.replace(tzinfo=timezone.utc)
        return dt.date()
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(ET).date()
    except Exception:
        pass
    # Try YYYYMMDD, YYMMDD
    m = re.match(r"^(\d{4})[-/]?(\d{2})[-/]?(\d{2})$", s)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except Exception:
            return None
    m2 = re.match(r"^(\d{2})(\d{2})(\d{2})$", s)
    if m2:
        try:
            y = int(m2.group(1))
            y += 2000 if y < 70 else 1900
            return date(y, int(m2.group(2)), int(m2.group(3)))
        except Exception:
            return None
    return None


def _safe_float(val: Any) -> Optional[float]:
    try:
        if val is None:
            return None
        s = str(val).strip()
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def _fmt_price(x: Optional[float]) -> str:
    if x is None:
        return ""
    if math.isnan(x) or math.isinf(x):
        return ""
    return f"{x:.2f}"


# ---------- GammaWizard ----------

def _sanitize_token(tok: str) -> str:
    t = (tok or "").strip().strip('"').strip("'")
    return t.split(None, 1)[1] if t.lower().startswith("bearer ") else t


def _gw_timeout() -> int:
    try:
        return int(os.environ.get("GW_TIMEOUT", "30"))
    except Exception:
        return 30


def _gw_login_token() -> str:
    email = os.environ.get("GW_EMAIL", "")
    password = os.environ.get("GW_PASSWORD", "")
    if not (email and password):
        raise RuntimeError("GW_LOGIN_MISSING_CREDS")
    base = os.environ.get("GW_BASE", "https://gandalf.gammawizard.com")
    r = requests.post(
        f"{base}/goauth/authenticateFireUser",
        data={"email": email, "password": password},
        timeout=_gw_timeout(),
    )
    if r.status_code != 200:
        raise RuntimeError(f"GW_LOGIN_HTTP_{r.status_code}:{(r.text or '')[:180]}")
    token = r.json().get("token")
    if not token:
        raise RuntimeError("GW_LOGIN_NO_TOKEN")
    return token


def _gw_fetch(endpoint: str) -> Optional[Dict[date, float]]:
    base = os.environ.get("GW_BASE", "https://gandalf.gammawizard.com")
    endpoint = endpoint.strip()
    if not endpoint:
        return None
    token = _sanitize_token(os.environ.get("GW_TOKEN", ""))

    def _hit(tok: str):
        hdr = {
            "Accept": "application/json",
            "Authorization": f"Bearer {_sanitize_token(tok)}",
            "User-Agent": "gw-settlements/1.0",
        }
        url = f"{base.rstrip('/')}/{endpoint.lstrip('/')}"
        return requests.get(url, headers=hdr, timeout=_gw_timeout())

    r = _hit(token) if token else None
    if r is not None and r.status_code in (401, 403):
        token = _gw_login_token()
        r = _hit(token)
    if r is None:
        token = _gw_login_token()
        r = _hit(token)
    if r.status_code != 200:
        print(f"GW_SETTLE_HTTP_{r.status_code}: {(r.text or '')[:180]}")
        return None
    try:
        payload = r.json()
    except Exception as exc:
        print(f"GW_SETTLE_JSON_ERROR: {exc}")
        return None
    return _parse_gw_payload(payload)


def _parse_gw_payload(payload: Any) -> Dict[date, float]:
    out: Dict[date, float] = {}

    date_keys = {
        "exp", "expiry", "expiration", "exp_primary", "settlementdate", "date", "tradingday"
    }
    price_keys = {
        "settle", "settlement", "settlementprice", "value", "close", "price", "spxsettle"
    }

    def handle_candidate(dval: Any, sval: Any) -> None:
        d = _parse_date(dval)
        s = _safe_float(sval)
        if d and s is not None:
            out[d] = s

    def walk(obj: Any) -> None:
        if isinstance(obj, dict):
            keys_lower = {k.lower(): k for k in obj.keys()}
            d_candidate: Optional[Any] = None
            s_candidate: Optional[Any] = None
            for kl, orig in keys_lower.items():
                val = obj[orig]
                if kl in date_keys or ("date" in kl and kl not in {"updated_at", "last_updated"}):
                    d_candidate = val
                elif kl in price_keys or "settle" in kl:
                    s_candidate = val
            if d_candidate is not None and s_candidate is not None:
                handle_candidate(d_candidate, s_candidate)
            for v in obj.values():
                if isinstance(v, (dict, list)):
                    walk(v)
        elif isinstance(obj, list):
            for item in obj:
                if isinstance(item, (dict, list)):
                    walk(item)

    walk(payload)
    return out


def gw_fetch_settlements() -> Dict[date, float]:
    prefer = os.environ.get("GW_SETTLE_ENDPOINT", "")
    tried: List[str] = []
    candidates = [prefer] if prefer else []
    candidates += [
        "/rapi/GetSpxSettlements",
        "/rapi/GetSPXSettlements",
        "/rapi/GetSpxSettlementHistory",
        "/rapi/GetSPXSettlementHistory",
    ]
    for endpoint in candidates:
        if not endpoint or endpoint in tried:
            continue
        tried.append(endpoint)
        data = _gw_fetch(endpoint)
        if data:
            return data
    return {}


# ---------- Schwab fallback ----------

def _decode_token_to_path() -> Optional[str]:
    token_env = os.environ.get("SCHWAB_TOKEN_JSON", "") or ""
    if not token_env:
        return None
    try:
        dec = base64.b64decode(token_env).decode("utf-8")
        if dec.strip().startswith("{"):
            token_env = dec
    except Exception:
        pass
    path = "schwab_token.json"
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(token_env)
    return path


def _schwab_client():
    token_path = _decode_token_to_path()
    if not token_path:
        return None
    try:
        return client_from_token_file(
            token_path,
            os.environ["SCHWAB_APP_KEY"],
            os.environ["SCHWAB_APP_SECRET"],
        )
    except Exception as exc:
        print(f"SCHWAB_CLIENT_ERROR: {exc}")
        return None


def schwab_fetch_settlements(dates: Iterable[date]) -> Dict[date, float]:
    targets = sorted(set(dates))
    if not targets:
        return {}
    client = _schwab_client()
    if client is None:
        return {}
    start = min(targets) - timedelta(days=2)
    end = max(targets) + timedelta(days=2)
    start_ms = int(datetime(start.year, start.month, start.day, tzinfo=timezone.utc).timestamp() * 1000)
    end_ms = int(datetime(end.year, end.month, end.day, 23, 59, tzinfo=timezone.utc).timestamp() * 1000)
    symbols = ["$SPX", "$SPX.X", "SPX", ".SPX"]
    candles: List[Dict[str, Any]] = []
    for sym in symbols:
        try:
            resp = client.get_price_history(
                symbol=sym,
                startDate=start_ms,
                endDate=end_ms,
                frequencyType="daily",
                frequency=1,
            )
        except Exception:
            continue
        if resp.status_code != 200:
            continue
        try:
            data = resp.json()
        except Exception:
            continue
        candles = data.get("candles") or data.get("priceHistory", {}).get("candles", [])
        if candles:
            break
    out: Dict[date, float] = {}
    for candle in candles:
        ts = candle.get("datetime") or candle.get("time")
        close = candle.get("close") or candle.get("closePrice")
        d = _parse_date(ts / 1000 if isinstance(ts, (int, float)) else ts)
        s = _safe_float(close)
        if d and s is not None:
            out[d] = s
    return {d: out[d] for d in targets if d in out}


# ---------- Core ----------

def build_target_dates(backfill_days: int) -> List[date]:
    today = datetime.now(ET).date()
    start = today - timedelta(days=backfill_days)
    dates: List[date] = []
    cur = today
    while cur >= start:
        if cur.weekday() < 5:  # Mon-Fri
            dates.append(cur)
        cur -= timedelta(days=1)
    return sorted(dates)


def main() -> int:
    backfill_days = int(os.environ.get("SETTLE_BACKFILL_DAYS", "120"))
    svc, sid = sheets_client()
    ensure_tab_with_header(svc, sid, SETTLE_TAB, HEADERS)
    existing_rows = read_rows(svc, sid, SETTLE_TAB, HEADERS)

    existing: Dict[date, Dict[str, Any]] = {}
    for row in existing_rows:
        d = _parse_date(row[0])
        if not d:
            continue
        existing[d] = {
            "settle": _safe_float(row[1]),
            "source": row[2] if len(row) > 2 else "",
            "updated_at": row[3] if len(row) > 3 else "",
        }

    gw_data = gw_fetch_settlements()
    today_ts = datetime.now(ET).isoformat()
    for d, price in gw_data.items():
        if price is None:
            continue
        existing[d] = {"settle": price, "source": "GW", "updated_at": today_ts}

    targets = set(build_target_dates(backfill_days))
    targets.update(d for d in existing.keys() if (datetime.now(ET).date() - d).days <= backfill_days + 5)

    missing = [d for d in sorted(targets) if d not in existing or existing[d].get("settle") is None]
    schwab_data = schwab_fetch_settlements(missing)
    for d, price in schwab_data.items():
        existing[d] = {"settle": price, "source": "Schwab", "updated_at": today_ts}

    rows: List[List[Any]] = []
    for d in sorted(existing.keys()):
        info = existing[d]
        rows.append([
            d.isoformat(),
            _fmt_price(info.get("settle")),
            info.get("source", ""),
            info.get("updated_at", ""),
        ])

    write_all_rows(svc, sid, SETTLE_TAB, HEADERS, rows)
    print(f"SETTLEMENTS: wrote {len(rows)} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
