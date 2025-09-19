#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Update ``sw_txn_summary`` in Sheets with the desired formula."""

import base64
import json
import os
from textwrap import dedent

from google.oauth2 import service_account
from googleapiclient.discovery import build as gbuild

SUMMARY_TAB = "sw_txn_summary"
SUMMARY_FORMULA = dedent(
    """\
    =ARRAYFORMULA({
      "exp_primary","net_amount_sum";
      SORT(
        QUERY(sw_txn_raw!A:Q,
          "select H, sum(N) where H is not null group by H label sum(N) ''",
          1
        ),
        1, TRUE
      )
    })
    """
).strip()


def sheets_client():
    sid = os.environ["GSHEET_ID"]
    sa_json = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
    try:
        dec = base64.b64decode(sa_json).decode("utf-8")
        if dec.strip().startswith("{"):
            sa_json = dec
    except Exception:
        pass
    creds = service_account.Credentials.from_service_account_info(
        json.loads(sa_json),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gbuild("sheets", "v4", credentials=creds), sid


def ensure_tab_exists(svc, sid: str, tab: str) -> None:
    meta = svc.spreadsheets().get(spreadsheetId=sid).execute()
    titles = {s["properties"]["title"] for s in meta.get("sheets", [])}
    if tab not in titles:
        svc.spreadsheets().batchUpdate(
            spreadsheetId=sid,
            body={"requests": [{"addSheet": {"properties": {"title": tab}}}]},
        ).execute()


def overwrite_with_formula(svc, sid: str, tab: str, formula: str) -> None:
    svc.spreadsheets().values().clear(
        spreadsheetId=sid,
        range=tab,
    ).execute()
    svc.spreadsheets().values().update(
        spreadsheetId=sid,
        range=f"{tab}!A1",
        valueInputOption="USER_ENTERED",
        body={"values": [[formula]]},
    ).execute()


def main() -> int:
    svc, sid = sheets_client()
    ensure_tab_exists(svc, sid, SUMMARY_TAB)
    overwrite_with_formula(svc, sid, SUMMARY_TAB, SUMMARY_FORMULA)
    print("SUMMARY: applied formula to sw_txn_summary!A1.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
