#!/usr/bin/env python3
import os, json, sys
from google.oauth2 import service_account
from googleapiclient.discovery import build

SHEET_ID = os.environ["GSHEET_ID"]
TAB = os.environ.get("GSHEET_TAB", "leocross")  # default to your tab

def main():
    payload = json.loads(os.environ.get("GSHEET_VALUES_JSON", "[]"))
    # Accept either: [["a","b","c"]] or {"headers":["A","B","C"],"row":["a","b","c"]}
    if isinstance(payload, dict) and "headers" in payload and "row" in payload:
        headers = payload["headers"]
        row = payload["row"]
        # Optionally ensure headers exist in row 1
        svc.spreadsheets().values().update(
            spreadsheetId=SHEET_ID,
            range=f"{TAB}!1:1",
            valueInputOption="RAW",
            body={"values": [headers]}
        ).execute()
        values = [row]
    elif isinstance(payload, list):
        values = payload
    else:
        raise SystemExit("Bad GSHEET_VALUES_JSON")

    svc.spreadsheets().values().append(
        spreadsheetId=SHEET_ID,
        range=f"{TAB}!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": values}
    ).execute()

if __name__ == "__main__":
    info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    svc = build("sheets", "v4", credentials=creds)
    main()
