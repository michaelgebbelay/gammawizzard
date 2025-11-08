#!/usr/bin/env python3
from __future__ import annotations
import gspread
from google.oauth2.service_account import Credentials

# Uses a service account (same pattern you’ve been using)
# ENV you already keep: path to JSON key; the spreadsheet is shared with that SA email.
def gsheet_client_from_path(json_path: str):
    scopes = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(json_path, scopes=scopes)
    return gspread.authorize(creds)

def ensure_ws(sh, name: str):
    try:
        return sh.worksheet(name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=name, rows=5000, cols=40)
        ws.append_row([
            "ts_pt","event","trade_id","side","dir","expiry",
            "short_strike","long_strike","width","qty",
            "short_delta","mid","assumed_price","offset",
            "underlying","accel_t1","accel_t2","note"
        ], value_input_option="RAW")
        return ws

def append_rows(json_path: str, spreadsheet_id: str, worksheet: str, rows: list[list]):
    gc = gsheet_client_from_path(json_path)
    sh = gc.open_by_key(spreadsheet_id)
    ws = ensure_ws(sh, worksheet)
    ws.append_rows(rows, value_input_option="RAW")
