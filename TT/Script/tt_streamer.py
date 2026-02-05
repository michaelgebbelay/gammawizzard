#!/usr/bin/env python3
"""
Minimal Tastytrade streaming quote helper (DXLink).
"""

import asyncio
import json
import os
import time
from typing import Dict, Iterable, Tuple

import websockets

from tt_client import request


def _get_quote_token() -> tuple[str, str]:
    j = request("GET", "/api-quote-tokens").json()
    data = j.get("data") if isinstance(j, dict) else {}
    token = data.get("token") or data.get("quote-token") or data.get("quote_token") or ""
    url = (
        data.get("streamer-url")
        or data.get("streamerUrl")
        or data.get("dxlink-url")
        or data.get("dxlinkUrl")
        or ""
    )
    if not token or not url:
        raise RuntimeError("quote token response missing token/url")
    return token, url


def _build_messages(token: str, symbols: Iterable[str]) -> list[dict]:
    raw = (os.environ.get("TT_STREAM_MESSAGES") or "").strip()
    if raw:
        payload = json.loads(raw)
        out = []
        for msg in payload:
            if isinstance(msg, dict):
                s = json.dumps(msg)
                s = s.replace("{TOKEN}", token)
                s = s.replace("{SYMBOLS}", ",".join(symbols))
                out.append(json.loads(s))
        return out

    return [
        {"type": "SETUP", "channel": 0, "keepaliveTimeout": 60, "acceptKeepaliveTimeout": 60, "version": "1.0.0"},
        {"type": "AUTH", "channel": 0, "token": token},
        {"type": "CHANNEL_REQUEST", "channel": 1, "service": "QUOTE", "parameters": {"symbols": ",".join(symbols)}},
    ]


def _fnum(x):
    try:
        return float(x)
    except Exception:
        return None


def _extract_quotes(msg, want: set[str]) -> Dict[str, Tuple[float, float]]:
    out: Dict[str, Tuple[float, float]] = {}

    def walk(obj):
        if isinstance(obj, dict):
            sym = obj.get("symbol") or obj.get("eventSymbol") or obj.get("event_symbol")
            bid = obj.get("bid") or obj.get("bidPrice") or obj.get("bid-price")
            ask = obj.get("ask") or obj.get("askPrice") or obj.get("ask-price")
            if sym and sym in want:
                b = _fnum(bid)
                a = _fnum(ask)
                if b is not None and a is not None:
                    out[sym] = (b, a)
            for v in obj.values():
                if isinstance(v, (dict, list)):
                    walk(v)
        elif isinstance(obj, list):
            for it in obj:
                walk(it)

    walk(msg)
    return out


async def _fetch_quotes_async(symbols: Iterable[str], timeout_s: float) -> Dict[str, Tuple[float, float]]:
    token, url = _get_quote_token()
    want = set(symbols)
    deadline = time.time() + timeout_s
    out: Dict[str, Tuple[float, float]] = {}
    debug = str(os.environ.get("TT_STREAM_DEBUG", "")).strip().lower() in ("1", "true", "yes")

    async with websockets.connect(url) as ws:
        for msg in _build_messages(token, symbols):
            await ws.send(json.dumps(msg))

        while time.time() < deadline and len(out) < len(want):
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            if debug:
                print(f"TT_STREAM_MSG: {str(raw)[:400]}")
            try:
                j = json.loads(raw)
            except Exception:
                continue
            out.update(_extract_quotes(j, want))

    return out


def get_quotes_once(symbols: Iterable[str], timeout_s: float = 6.0) -> Dict[str, Tuple[float, float]]:
    return asyncio.run(_fetch_quotes_async(symbols, timeout_s))
