#!/usr/bin/env python3
"""
Tastytrade token keeper with refresh + single-writer locking.
"""

import json
import os
import time
from contextlib import contextmanager

import requests

try:
    import fcntl
except Exception:  # pragma: no cover - non-posix
    fcntl = None


def _ensure_dir(path: str):
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


@contextmanager
def _file_lock(lock_path: str, timeout_s: float = 10.0):
    if fcntl is None:
        yield
        return
    _ensure_dir(lock_path)
    with open(lock_path, "a") as fh:
        end = time.time() + timeout_s
        while True:
            try:
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.time() >= end:
                    raise TimeoutError(f"Token lock timeout: {lock_path}")
                time.sleep(0.1)
        try:
            yield
        finally:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)


def _write_json_atomic(path: str, data):
    _ensure_dir(path)
    tmp = f"{path}.tmp"
    with open(tmp, "w") as f:
        json.dump(data, f)
    os.replace(tmp, path)


def _write_token_locked(token_path: str, token_obj):
    lock_path = f"{token_path}.lock"
    with _file_lock(lock_path):
        _write_json_atomic(token_path, token_obj)


def _token_path() -> str:
    token_path = (os.environ.get("TT_TOKEN_PATH") or "").strip()
    if token_path:
        return token_path
    default_path = os.path.join("TT", "Token", "tt_token.json")
    return default_path


def load_token() -> dict:
    token_path = _token_path()
    if not os.path.exists(token_path):
        raise RuntimeError(f"TT token file missing: {token_path}")
    with open(token_path, "r") as f:
        return json.load(f)


def save_token(token: dict):
    _write_token_locked(_token_path(), token)


def refresh_token(token: dict) -> dict:
    base = os.environ.get("TT_BASE_URL", "https://api.tastyworks.com").rstrip("/")
    token_url = os.environ.get("TT_TOKEN_URL", f"{base}/oauth/token").strip()
    client_id = os.environ.get("TT_CLIENT_ID", "").strip()
    client_secret = os.environ.get("TT_CLIENT_SECRET", "").strip()
    refresh = token.get("refresh_token") or ""
    if not (client_id and client_secret and refresh):
        raise RuntimeError("Missing TT_CLIENT_ID/TT_CLIENT_SECRET or refresh_token")

    auth_mode = (os.environ.get("TT_CLIENT_AUTH") or "body").strip().lower()
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh,
        "client_id": client_id,
    }
    req_kwargs = {"data": data, "timeout": 20}
    if auth_mode == "basic":
        req_kwargs["auth"] = (client_id, client_secret)
    else:
        data["client_secret"] = client_secret

    r = requests.post(token_url, **req_kwargs)
    r.raise_for_status()
    new_token = r.json()
    save_token(new_token)
    return new_token


def get_access_token() -> str:
    token = load_token()
    access = token.get("access_token") or ""
    if not access:
        token = refresh_token(token)
        access = token.get("access_token") or ""
    return access
