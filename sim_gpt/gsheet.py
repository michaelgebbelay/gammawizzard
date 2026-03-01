"""Google Sheets export for live game results."""

from __future__ import annotations

import base64
import hashlib
import json
import os
from typing import Any

from google.oauth2 import service_account
from googleapiclient.discovery import build as gbuild

from sim_gpt.config import (
    DEFAULT_DECISIONS_TAB,
    DEFAULT_GSHEET_ID,
    DEFAULT_LEADERBOARD_TAB,
    DEFAULT_RESULTS_TAB,
)
from sim_gpt.store import Store

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

RESULT_HEADERS = [
    "round_id",
    "signal_date",
    "signal_timestamp_utc",
    "tdate",
    "settlement_timestamp_utc",
    "player_id",
    "put_action",
    "put_width",
    "put_target_delta",
    "call_action",
    "call_width",
    "call_target_delta",
    "size",
    "template_id",
    "pricing_model",
    "fill_model",
    "chain_asof_utc",
    "chain_underlying_spx",
    "put_long_strike",
    "put_short_strike",
    "call_long_strike",
    "call_short_strike",
    "put_entry_price_points",
    "call_entry_price_points",
    "account_value",
    "risk_budget",
    "pre_max_loss",
    "max_loss",
    "risk_used_pct",
    "risk_guard",
    "trade_rate_context",
    "consecutive_holds_context",
    "target_trade_rate",
    "put_pnl",
    "call_pnl",
    "gross_total_pnl",
    "fees",
    "total_pnl",
    "equity_pnl",
    "drawdown",
    "max_drawdown",
    "risk_adjusted",
    "decision_checksum",
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
]

DECISION_HEADERS = [
    "signal_date",
    "tdate",
    "status",
    "player_id",
    "valid",
    "error",
    "put_action",
    "put_width",
    "put_target_delta",
    "put_target_delta_hit",
    "put_lower_strike",
    "put_upper_strike",
    "put_long_strike",
    "put_short_strike",
    "put_entry_price_points",
    "put_bid_points",
    "put_ask_points",
    "put_mid_points",
    "call_action",
    "call_width",
    "call_target_delta",
    "call_target_delta_hit",
    "call_lower_strike",
    "call_upper_strike",
    "call_long_strike",
    "call_short_strike",
    "call_entry_price_points",
    "call_bid_points",
    "call_ask_points",
    "call_mid_points",
    "size",
    "template_id",
    "pricing_model",
    "fill_model",
    "chain_asof_utc",
    "chain_underlying_spx",
    "account_value",
    "risk_budget",
    "max_loss",
    "risk_used_pct",
    "trade_rate_context",
    "consecutive_holds_context",
    "target_trade_rate",
    "decision_checksum",
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
                  r.signal_timestamp_utc,
                  r.tdate,
                  r.settlement_timestamp_utc,
                  x.player_id,
                  d.decision_json,
                  x.put_pnl,
                  x.call_pnl,
                  x.gross_total_pnl,
                  x.fees,
                  x.total_pnl,
                  x.equity_pnl,
                  x.drawdown,
                  x.max_drawdown,
                  x.risk_adjusted
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
        round_id = f"{r['signal_date']}|{r['player_id']}"
        checksum = _decision_checksum(r, dec)
        out.append(
            [
                round_id,
                r["signal_date"],
                r.get("signal_timestamp_utc", ""),
                r["tdate"],
                r.get("settlement_timestamp_utc", ""),
                r["player_id"],
                dec.get("put_action", ""),
                dec.get("put_width", ""),
                dec.get("put_target_delta", ""),
                dec.get("call_action", ""),
                dec.get("call_width", ""),
                dec.get("call_target_delta", ""),
                dec.get("size", ""),
                dec.get("template_id", ""),
                dec.get("pricing_model", ""),
                dec.get("fill_model", ""),
                dec.get("chain_asof_utc", ""),
                dec.get("chain_underlying_spx", ""),
                dec.get("put_long_strike", ""),
                dec.get("put_short_strike", ""),
                dec.get("call_long_strike", ""),
                dec.get("call_short_strike", ""),
                dec.get("put_entry_price_points", ""),
                dec.get("call_entry_price_points", ""),
                dec.get("account_value", ""),
                dec.get("risk_budget", ""),
                dec.get("pre_max_loss", ""),
                dec.get("max_loss", ""),
                dec.get("risk_used_pct", ""),
                dec.get("risk_guard", ""),
                dec.get("trade_rate_context", ""),
                dec.get("consecutive_holds_context", ""),
                dec.get("target_trade_rate", ""),
                round(float(r["put_pnl"]), 2),
                round(float(r["call_pnl"]), 2),
                round(float(r["gross_total_pnl"]), 2),
                round(float(r["fees"]), 2),
                round(float(r["total_pnl"]), 2),
                round(float(r["equity_pnl"]), 2),
                round(float(r["drawdown"]), 2),
                round(float(r["max_drawdown"]), 2),
                round(float(r["risk_adjusted"]), 2),
                checksum,
            ]
        )
    return out


def _decision_checksum(result_row: dict, decision: dict) -> str:
    payload = {
        "signal_date": result_row.get("signal_date"),
        "player_id": result_row.get("player_id"),
        "decision": decision,
        "put_pnl": round(float(result_row.get("put_pnl", 0.0)), 4),
        "call_pnl": round(float(result_row.get("call_pnl", 0.0)), 4),
        "gross_total_pnl": round(float(result_row.get("gross_total_pnl", 0.0)), 4),
        "fees": round(float(result_row.get("fees", 0.0)), 4),
        "total_pnl": round(float(result_row.get("total_pnl", 0.0)), 4),
        "risk_adjusted": round(float(result_row.get("risk_adjusted", 0.0)), 4),
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _decision_only_checksum(row: dict, decision: dict) -> str:
    payload = {
        "signal_date": row.get("signal_date"),
        "tdate": row.get("tdate"),
        "status": row.get("status"),
        "player_id": row.get("player_id"),
        "valid": row.get("valid"),
        "error": row.get("error"),
        "decision": decision,
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _all_decision_rows(store: Store) -> list[list[Any]]:
    cur = store.conn.execute(
        """SELECT d.signal_date,
                  r.tdate,
                  r.status,
                  d.player_id,
                  d.valid,
                  d.error,
                  d.decision_json
           FROM decisions d
           JOIN rounds r ON r.signal_date = d.signal_date
           ORDER BY d.signal_date DESC, d.player_id ASC"""
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
        checksum = _decision_only_checksum(r, dec)
        out.append(
            [
                r.get("signal_date", ""),
                r.get("tdate", ""),
                r.get("status", ""),
                r.get("player_id", ""),
                bool(r.get("valid", 0)),
                r.get("error", ""),
                dec.get("put_action", ""),
                dec.get("put_width", ""),
                dec.get("put_target_delta", ""),
                dec.get("put_target_delta_hit", ""),
                dec.get("put_lower_strike", ""),
                dec.get("put_upper_strike", ""),
                dec.get("put_long_strike", ""),
                dec.get("put_short_strike", ""),
                dec.get("put_entry_price_points", ""),
                dec.get("put_bid_points", ""),
                dec.get("put_ask_points", ""),
                dec.get("put_mid_points", ""),
                dec.get("call_action", ""),
                dec.get("call_width", ""),
                dec.get("call_target_delta", ""),
                dec.get("call_target_delta_hit", ""),
                dec.get("call_lower_strike", ""),
                dec.get("call_upper_strike", ""),
                dec.get("call_long_strike", ""),
                dec.get("call_short_strike", ""),
                dec.get("call_entry_price_points", ""),
                dec.get("call_bid_points", ""),
                dec.get("call_ask_points", ""),
                dec.get("call_mid_points", ""),
                dec.get("size", ""),
                dec.get("template_id", ""),
                dec.get("pricing_model", ""),
                dec.get("fill_model", ""),
                dec.get("chain_asof_utc", ""),
                dec.get("chain_underlying_spx", ""),
                dec.get("account_value", ""),
                dec.get("risk_budget", ""),
                dec.get("max_loss", ""),
                dec.get("risk_used_pct", ""),
                dec.get("trade_rate_context", ""),
                dec.get("consecutive_holds_context", ""),
                dec.get("target_trade_rate", ""),
                checksum,
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
            ]
        )
    return out


def sync_game_to_sheet(
    store: Store,
    sheet_id: str = "",
    results_tab: str = DEFAULT_RESULTS_TAB,
    leaderboard_tab: str = DEFAULT_LEADERBOARD_TAB,
    decisions_tab: str = DEFAULT_DECISIONS_TAB,
) -> dict:
    sid = (sheet_id or "").strip() or os.environ.get("GSHEET_ID", "").strip() or DEFAULT_GSHEET_ID
    if not sid:
        raise RuntimeError("No spreadsheet ID configured")

    svc = _sheets_client()

    results_rows = _all_result_rows(store)
    lb_rows = _leaderboard_rows(store)
    decision_rows = _all_decision_rows(store)

    _ensure_tab(svc, sid, results_tab, RESULT_HEADERS)
    _overwrite_rows(svc, sid, results_tab, RESULT_HEADERS, results_rows)

    _ensure_tab(svc, sid, leaderboard_tab, LEADERBOARD_HEADERS)
    _overwrite_rows(svc, sid, leaderboard_tab, LEADERBOARD_HEADERS, lb_rows)

    _ensure_tab(svc, sid, decisions_tab, DECISION_HEADERS)
    _overwrite_rows(svc, sid, decisions_tab, DECISION_HEADERS, decision_rows)

    return {
        "sheet_id": sid,
        "results_tab": results_tab,
        "leaderboard_tab": leaderboard_tab,
        "decisions_tab": decisions_tab,
        "results_rows": len(results_rows),
        "leaderboard_rows": len(lb_rows),
        "decisions_rows": len(decision_rows),
    }
