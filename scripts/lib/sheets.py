"""Shared Google Sheets helpers for data scripts."""

import base64
import json
import os
from typing import Any, List

from google.oauth2 import service_account
from googleapiclient.discovery import build as gbuild

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def sheets_client():
    """Return (sheets_service, spreadsheet_id) using env vars.

    Handles GOOGLE_SERVICE_ACCOUNT_JSON as raw JSON or base64-encoded.
    """
    sa_json = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
    try:
        dec = base64.b64decode(sa_json).decode("utf-8")
        if dec.strip().startswith("{"):
            sa_json = dec
    except Exception:
        pass
    creds = service_account.Credentials.from_service_account_info(
        json.loads(sa_json), scopes=SCOPES
    )
    svc = gbuild("sheets", "v4", credentials=creds)
    sid = os.environ["GSHEET_ID"]
    return svc, sid


def ensure_tab(svc, sid: str, tab: str, headers: List[str]) -> None:
    """Create *tab* if missing and set header row."""
    meta = svc.spreadsheets().get(spreadsheetId=sid).execute()
    names = {s["properties"]["title"] for s in meta.get("sheets", [])}
    if tab not in names:
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


def overwrite_rows(
    svc, sid: str, tab: str, headers: List[str], rows: List[List[Any]]
) -> None:
    """Clear *tab* then write *headers* + *rows*."""
    svc.spreadsheets().values().clear(spreadsheetId=sid, range=tab).execute()
    svc.spreadsheets().values().update(
        spreadsheetId=sid,
        range=f"{tab}!A1",
        valueInputOption="RAW",
        body={"values": [headers] + rows},
    ).execute()


def read_existing(svc, sid: str, tab: str, headers: List[str]) -> List[List[Any]]:
    """Read all data rows from *tab* (row 2+), padding each to *headers* length."""
    last_col = chr(ord("A") + len(headers) - 1)
    vals = (
        svc.spreadsheets()
        .values()
        .get(spreadsheetId=sid, range=f"{tab}!A2:{last_col}")
        .execute()
        .get("values", [])
    )
    out = []
    for r in vals:
        row = list(r) + [""] * (len(headers) - len(r))
        out.append(row[: len(headers)])
    return out


def get_values(svc, sid: str, range_str: str) -> List[List[Any]]:
    """Raw ``spreadsheets.values.get`` wrapper."""
    return (
        svc.spreadsheets()
        .values()
        .get(spreadsheetId=sid, range=range_str)
        .execute()
        .get("values", [])
    )
