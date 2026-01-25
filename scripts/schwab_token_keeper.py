#!/usr/bin/env python3
"""
Schwab token keeper with single-writer locking and refresh-token overwrite.
"""

import base64
import json
import os
import time
from contextlib import contextmanager

from schwab.auth import client_from_token_file

try:
    import fcntl
except Exception:  # pragma: no cover - non-posix
    fcntl = None


def _decode_token_env(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""
    if raw.lstrip().startswith("{"):
        return raw
    try:
        dec = base64.b64decode(raw).decode("utf-8")
        if dec.strip().startswith("{"):
            return dec
    except Exception:
        pass
    return raw


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


def ensure_token_file(token_path: str) -> str:
    token_env = _decode_token_env(os.environ.get("SCHWAB_TOKEN_JSON", "") or "")
    if token_env:
        try:
            token_obj = json.loads(token_env)
        except Exception:
            token_obj = token_env
        _write_token_locked(token_path, token_obj)
        return token_path
    if not os.path.exists(token_path):
        raise RuntimeError(f"SCHWAB_TOKEN_JSON missing and token file not found: {token_path}")
    return token_path


def schwab_client():
    app_key = os.environ["SCHWAB_APP_KEY"]
    app_secret = os.environ["SCHWAB_APP_SECRET"]
    token_path = os.environ.get("SCHWAB_TOKEN_PATH", "").strip()
    if not token_path:
        default_path = os.path.join("Token", "schwab_token.json")
        token_path = default_path if os.path.exists("Token") else "schwab_token.json"

    token_path = ensure_token_file(token_path)

    def token_write_func(token, *args, **kwargs):
        _write_token_locked(token_path, token)

    return client_from_token_file(
        token_path=token_path,
        api_key=app_key,
        app_secret=app_secret,
        token_write_func=token_write_func,
    )
