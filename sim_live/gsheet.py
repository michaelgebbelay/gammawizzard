"""Google Sheets export for live game results."""

from __future__ import annotations

import base64
import json
import os
from typing import Any

from google.oauth2 import service_account
from googleapiclient.discovery import build as gbuild

from sim_live.config import (
    DEFAULT_GSHEET_ID,
    DEFAULT_LEADERBOARD_TAB,
    DEFAULT_RESULTS_TAB,
)
from sim_live.store import Store

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

RESULT_HEADERS = [
    "signal_date",
    "tdate",
    "player_id",
    "put_action",
    "put_width",
    "call_action",
    "call_width",
    "size",
    "template_id",
    "put_pnl",
    "call_pnl",
    "total_pnl",
    "equity_pnl",
    "drawdown",
    "max_drawdown",
    "risk_adjusted",
    "judge_score",
    "judge_notes",
]

LEADERBOARD_HEADERS = [
    "rank",
    "player_id",
    "rounds",
    "total_pnl",
    "max_drawdown",
    "risk_adjusted",
    "win_rate",
    "avg_pnl",
    "avg_judge",
]


def _service_account_json() -> str:
    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if not raw:
        # Fallbacks to file-based credentials.
        sa_file = os.environ.get("GOOGLE_SERVICE_ACCOUNT_FILE", "").strip()
        if not sa_file:
            sa_file = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
        if sa_file and os.path.exists(sa_file):
            with open(sa_file, "r") as f:
                raw = f.read().strip()
    if not raw:
        raise RuntimeError(
            "Google auth missing. Set one of: GOOGLE_SERVICE_ACCOUNT_JSON, "
            "GOOGLE_SERVICE_ACCOUNT_FILE, GOOGLE_APPLICATION_CREDENTIALS"
        )
    try:
        dec = base64.b64decode(raw).decode("utf-8")
        if dec.strip().startswith("{"):
            return dec
    except Exception:
        pass
    return raw


def _sheets_client():
    info = json.loads(_service_account_json())
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return gbuild("sheets", "v4", credentials=creds)


def _ensure_tab(svc, sid: str, tab: str, headers: list[str]) -> None:
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


def _overwrite_rows(svc, sid: str, tab: str, headers: list[str], rows: list[list[Any]]) -> None:
    svc.spreadsheets().values().clear(spreadsheetId=sid, range=tab).execute()
    svc.spreadsheets().values().update(
        spreadsheetId=sid,
        range=f"{tab}!A1",
        valueInputOption="RAW",
        body={"values": [headers] + rows},
    ).execute()


def _all_result_rows(store: Store) -> list[list[Any]]:
    cur = store.conn.execute(
        """SELECT r.signal_date,
                  r.tdate,
                  x.player_id,
                  d.decision_json,
                  x.put_pnl,
                  x.call_pnl,
                  x.total_pnl,
                  x.equity_pnl,
                  x.drawdown,
                  x.max_drawdown,
                  x.risk_adjusted,
                  x.judge_score,
                  x.judge_notes
           FROM results x
           JOIN rounds r
             ON r.signal_date = x.signal_date
           LEFT JOIN decisions d
             ON d.signal_date = x.signal_date AND d.player_id = x.player_id
           ORDER BY r.signal_date DESC, x.total_pnl DESC, x.player_id ASC"""
    )
    out: list[list[Any]] = []
    for row in cur.fetchall():
        r = dict(row)
        dec = {}
        raw = r.get("decision_json")
        if raw:
            try:
                dec = json.loads(raw)
            except json.JSONDecodeError:
                dec = {}
        out.append(
            [
                r["signal_date"],
                r["tdate"],
                r["player_id"],
                dec.get("put_action", ""),
                dec.get("put_width", ""),
                dec.get("call_action", ""),
                dec.get("call_width", ""),
                dec.get("size", ""),
                dec.get("template_id", ""),
                round(float(r["put_pnl"]), 2),
                round(float(r["call_pnl"]), 2),
                round(float(r["total_pnl"]), 2),
                round(float(r["equity_pnl"]), 2),
                round(float(r["drawdown"]), 2),
                round(float(r["max_drawdown"]), 2),
                round(float(r["risk_adjusted"]), 2),
                round(float(r["judge_score"]), 2),
                r.get("judge_notes", ""),
            ]
        )
    return out


def _leaderboard_rows(store: Store) -> list[list[Any]]:
    rows = store.leaderboard()
    out: list[list[Any]] = []
    for i, r in enumerate(rows, 1):
        out.append(
            [
                i,
                r["player_id"],
                int(r["rounds"]),
                round(float(r["total_pnl"]), 2),
                round(float(r["max_drawdown"]), 2),
                round(float(r["risk_adjusted"]), 2),
                round(float(r["win_rate"]) * 100.0, 2),
                round(float(r["avg_pnl"]), 2),
                round(float(r["avg_judge"]), 2),
            ]
        )
    return out


def sync_game_to_sheet(
    store: Store,
    sheet_id: str = "",
    results_tab: str = DEFAULT_RESULTS_TAB,
    leaderboard_tab: str = DEFAULT_LEADERBOARD_TAB,
) -> dict:
    sid = (sheet_id or "").strip() or os.environ.get("GSHEET_ID", "").strip() or DEFAULT_GSHEET_ID
    if not sid:
        raise RuntimeError("No spreadsheet ID configured")

    svc = _sheets_client()

    results_rows = _all_result_rows(store)
    lb_rows = _leaderboard_rows(store)

    _ensure_tab(svc, sid, results_tab, RESULT_HEADERS)
    _overwrite_rows(svc, sid, results_tab, RESULT_HEADERS, results_rows)

    _ensure_tab(svc, sid, leaderboard_tab, LEADERBOARD_HEADERS)
    _overwrite_rows(svc, sid, leaderboard_tab, LEADERBOARD_HEADERS, lb_rows)

    return {
        "sheet_id": sid,
        "results_tab": results_tab,
        "leaderboard_tab": leaderboard_tab,
        "results_rows": len(results_rows),
        "leaderboard_rows": len(lb_rows),
    }
