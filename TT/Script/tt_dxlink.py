#!/usr/bin/env python3
"""
DXLink streaming quotes with cached quote-token.
"""

import asyncio
import json
import os
import time
from typing import Dict, Iterable, Tuple

import requests
import websockets

from tt_client import request


def _token_path() -> str:
    return (os.environ.get("TT_QUOTE_TOKEN_PATH") or "TT/Token/tt_quote_token.json").strip()


def _load_cached_token() -> dict:
    path = _token_path()
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_cached_token(token: dict):
    path = _token_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(token, f)


def _token_valid(tok: dict) -> bool:
    exp = tok.get("expires_at")
    if not exp:
        return False
    try:
        return float(exp) > time.time()
    except Exception:
        return False


def _expires_at_from_response(data: dict) -> float:
    ttl = data.get("expires-in") or data.get("expires_in") or 0
    try:
        ttl = float(ttl)
    except Exception:
        ttl = 0
    if ttl <= 0:
        ttl = 23 * 3600
    return time.time() + ttl


def get_quote_token() -> tuple[str, str]:
    cached = _load_cached_token()
    min_ttl = float(os.environ.get("TT_QUOTE_TOKEN_MIN_TTL", "7200"))
    if cached and _token_valid(cached) and float(cached.get("expires_at", 0)) - time.time() > min_ttl:
        return cached["token"], cached["dxlink_url"]

    backoff = 0.6
    last_err = ""
    for i in range(4):
        try:
            j = request("GET", "/api-quote-tokens").json()
            data = j.get("data") if isinstance(j, dict) else {}
            token = data.get("token") or data.get("quote-token") or data.get("quote_token")
            url = (
                data.get("dxlink-url")
                or data.get("dxlinkUrl")
                or data.get("streamer-url")
                or data.get("streamerUrl")
            )
            if token and url:
                out = {
                    "token": token,
                    "dxlink_url": url,
                    "expires_at": _expires_at_from_response(data),
                }
                _save_cached_token(out)
                return token, url
            last_err = "missing token/url"
        except requests.RequestException as e:
            last_err = str(e)[:200]
        time.sleep(backoff + (0.2 * i))
        backoff *= 2

    if cached and _token_valid(cached):
        return cached.get("token", ""), cached.get("dxlink_url", "")
    raise RuntimeError(f"quote token fetch failed: {last_err or 'unknown'}")


def _extract_quotes(msg, want: set[str], field_map: dict | None) -> Dict[str, Tuple[float, float]]:
    out: Dict[str, Tuple[float, float]] = {}

    if isinstance(msg, dict) and msg.get("type") == "FEED_DATA":
        data = msg.get("data") or []
        # Compact format: ["Quote", row] or ["Quote", [row, row, ...]]
        if isinstance(data, list) and len(data) == 2 and data[0] == "Quote" and isinstance(data[1], list):
            rows = data[1]
            if rows and isinstance(rows[0], list):
                row_list = rows
            else:
                row_list = [rows]
            for row in row_list:
                if isinstance(row, list) and len(row) >= 4:
                    sym = row[1]
                    bid = row[2]
                    ask = row[3]
                    if sym in want and bid is not None and ask is not None:
                        out[sym] = (float(bid), float(ask))
            return out
        for row in data:
            if isinstance(row, dict):
                sym = row.get("eventSymbol") or row.get("symbol")
                bid = row.get("bidPrice") or row.get("bid")
                ask = row.get("askPrice") or row.get("ask")
                if sym and sym in want and bid is not None and ask is not None:
                    out[sym] = (float(bid), float(ask))
            elif isinstance(row, list) and field_map:
                sym_i = field_map.get("eventSymbol")
                bid_i = field_map.get("bidPrice")
                ask_i = field_map.get("askPrice")
                try:
                    sym = row[sym_i] if sym_i is not None else None
                    bid = row[bid_i] if bid_i is not None else None
                    ask = row[ask_i] if ask_i is not None else None
                except Exception:
                    continue
                if sym and sym in want and bid is not None and ask is not None:
                    out[sym] = (float(bid), float(ask))
    return out


async def _fetch_quotes_async(symbols: Iterable[str], timeout_s: float) -> Dict[str, Tuple[float, float]]:
    token, url = get_quote_token()
    want = set(symbols)
    deadline = time.time() + timeout_s
    out: Dict[str, Tuple[float, float]] = {}
    data_format = (os.environ.get("TT_DXLINK_DATA_FORMAT") or "COMPACT").upper()
    field_map = None
    debug = str(os.environ.get("TT_DXLINK_DEBUG", "1")).strip().lower() in ("1", "true", "yes")
    raw_dump = str(os.environ.get("TT_DXLINK_RAW", "1")).strip().lower() in ("1", "true", "yes")

    async with websockets.connect(url) as ws:
        if debug:
            print(f"TT_DXLINK CONNECT url={url}")
        await ws.send(json.dumps({
            "type": "SETUP", "channel": 0,
            "keepaliveTimeout": 60, "acceptKeepaliveTimeout": 60,
            "version": "0.1-js/1.0.0",
        }))
        if debug:
            print("TT_DXLINK SENT SETUP")
        await ws.send(json.dumps({"type": "AUTH", "channel": 0, "token": token}))
        if debug:
            print("TT_DXLINK SENT AUTH")

        while time.time() < deadline:
            raw = await ws.recv()
            msg = json.loads(raw)
            if debug and msg.get("type"):
                print(f"TT_DXLINK RECV {msg.get('type')}")
            if msg.get("type") == "AUTH_STATE" and msg.get("state") == "AUTHORIZED":
                if debug:
                    print("TT_DXLINK AUTHORIZED")
                break

        await ws.send(json.dumps({
            "type": "CHANNEL_REQUEST",
            "channel": 1,
            "service": "FEED",
            "parameters": {"contract": "AUTO"},
        }))
        if debug:
            print("TT_DXLINK SENT CHANNEL_REQUEST")

        while time.time() < deadline:
            raw = await ws.recv()
            msg = json.loads(raw)
            if debug and msg.get("type"):
                print(f"TT_DXLINK RECV {msg.get('type')}")
            if msg.get("type") == "CHANNEL_OPENED":
                if debug:
                    print("TT_DXLINK CHANNEL_OPENED")
                break

        await ws.send(json.dumps({
            "type": "FEED_SETUP",
            "channel": 1,
            "acceptAggregationPeriod": 10,
            "acceptDataFormat": data_format,
            "acceptEventFields": {
                "Quote": ["eventType", "eventSymbol", "bidPrice", "askPrice", "bidSize", "askSize"]
            },
        }))
        if debug:
            print("TT_DXLINK SENT FEED_SETUP")

        while time.time() < deadline:
            raw = await ws.recv()
            msg = json.loads(raw)
            if debug and msg.get("type"):
                print(f"TT_DXLINK RECV {msg.get('type')}")
            if msg.get("type") == "FEED_CONFIG":
                if data_format == "COMPACT":
                    fields = (msg.get("eventFields") or {}).get("Quote") or []
                    field_map = {name: idx for idx, name in enumerate(fields)}
                if debug:
                    print("TT_DXLINK FEED_CONFIG")
                break

        await ws.send(json.dumps({
            "type": "FEED_SUBSCRIPTION",
            "channel": 1,
            "add": [{"symbol": s, "type": "Quote"} for s in symbols],
        }))
        if debug:
            print(f"TT_DXLINK SENT FEED_SUBSCRIPTION symbols={list(symbols)}")

        dumped = False
        while time.time() < deadline and len(out) < len(want):
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            msg = json.loads(raw)
            if debug and msg.get("type"):
                print(f"TT_DXLINK RECV {msg.get('type')}")
            if raw_dump and msg.get("type") == "FEED_DATA" and not dumped:
                print(f"TT_DXLINK RAW_FEED_DATA: {str(msg)[:1200]}")
                dumped = True
            out.update(_extract_quotes(msg, want, field_map))
            if debug and out:
                print(f"TT_DXLINK QUOTES_OK {out}")

    return out


def get_quotes_once(symbols: Iterable[str], timeout_s: float = 6.0) -> Dict[str, Tuple[float, float]]:
    return asyncio.run(_fetch_quotes_async(symbols, timeout_s))
