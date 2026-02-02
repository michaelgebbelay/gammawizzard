#!/usr/bin/env python3
"""
Minimal Tastytrade API client with auto-refresh.
"""

import os
import requests

from tt_token_keeper import get_access_token, refresh_token, load_token


def _base_url() -> str:
    return os.environ.get("TT_BASE_URL", "https://api.tastyworks.com").rstrip("/")


def _auth_header() -> dict:
    tok = get_access_token()
    return {"Authorization": f"Bearer {tok}"}


def request(method: str, path: str, **kwargs):
    url = f"{_base_url()}/{path.lstrip('/')}"
    headers = kwargs.pop("headers", {})
    headers.update(_auth_header())
    r = requests.request(method, url, headers=headers, timeout=20, **kwargs)
    if r.status_code == 401:
        token = load_token()
        refresh_token(token)
        headers = kwargs.pop("headers", {})
        headers.update(_auth_header())
        r = requests.request(method, url, headers=headers, timeout=20, **kwargs)
    r.raise_for_status()
    return r

