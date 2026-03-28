"""Trigger-window audit for strategy reporting.

This module answers two related questions for a given trade date:
  1. Which strategies/accounts were expected to run?
  2. For each expected trigger, was a trade taken, skipped, missed, or only partially filled?

The expected trigger schedule is sourced from ``reporting.seed_trigger_windows.WINDOWS``
so the audit can still work even when the trigger-window table has stale rows.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd

from reporting.db import get_connection, init_schema, query_df
from reporting.seed_trigger_windows import WINDOWS

ET = ZoneInfo("America/New_York")
UTC = timezone.utc


def _ensure_date(value: date | str) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


def _expected_triggers(report_date: date, strategy: str | None = None, account: str | None = None) -> pd.DataFrame:
    weekday = report_date.weekday()
    rows: list[dict] = []
    for win_strategy, win_account, weekdays, start_et, end_et, rule_name in WINDOWS:
        if weekday not in weekdays:
            continue
        if strategy and win_strategy != strategy:
            continue
        if account and win_account != account:
            continue
        rows.append(
            {
                "trade_date": report_date.isoformat(),
                "strategy": win_strategy,
                "account": win_account,
                "trigger_rule": rule_name,
                "trigger_window": f"{start_et}-{end_et} ET",
                "trigger_start_et": start_et,
                "trigger_end_et": end_et,
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    return df.sort_values(["trigger_start_et", "strategy", "account"]).reset_index(drop=True)


def _load_runs(report_date: date, con, strategy: str | None = None, account: str | None = None) -> pd.DataFrame:
    sql = """
        SELECT strategy, account, run_id, status, signal, reason, started_at, completed_at
        FROM strategy_runs
        WHERE (trade_date = ? OR CAST(started_at AS DATE) = ?)
    """
    params: list[object] = [report_date.isoformat(), report_date.isoformat()]
    if strategy:
        sql += " AND strategy = ?"
        params.append(strategy)
    if account:
        sql += " AND account = ?"
        params.append(account)
    return query_df(sql, params, con=con)


def _load_intents(report_date: date, con, strategy: str | None = None, account: str | None = None) -> pd.DataFrame:
    sql = """
        SELECT t.strategy,
               t.account,
               t.run_id,
               t.trade_group_id,
               t.target_qty,
               t.outcome,
               sr.started_at,
               COALESCE(SUM(f.fill_qty), 0) AS filled_qty
        FROM intended_trades t
        JOIN strategy_runs sr
          ON sr.run_id = t.run_id
        LEFT JOIN fills f
          ON f.trade_group_id = t.trade_group_id
        WHERE (t.trade_date = ? OR CAST(sr.started_at AS DATE) = ?)
    """
    params: list[object] = [report_date.isoformat(), report_date.isoformat()]
    if strategy:
        sql += " AND t.strategy = ?"
        params.append(strategy)
    if account:
        sql += " AND t.account = ?"
        params.append(account)
    sql += """
        GROUP BY t.strategy, t.account, t.run_id, t.trade_group_id, t.target_qty, t.outcome, sr.started_at
    """
    return query_df(sql, params, con=con)


def _window_bounds(report_date: date, start_et: str, end_et: str) -> tuple[datetime, datetime]:
    start_hour, start_min = (int(x) for x in start_et.split(":"))
    end_hour, end_min = (int(x) for x in end_et.split(":"))
    start_local = datetime.combine(report_date, time(start_hour, start_min), tzinfo=ET)
    end_local = datetime.combine(report_date, time(end_hour, end_min), tzinfo=ET)
    return start_local.astimezone(UTC), end_local.astimezone(UTC)


def _select_runs_for_window(runs: pd.DataFrame, report_date: date, start_et: str, end_et: str) -> pd.DataFrame:
    if runs.empty:
        return runs

    start_utc, end_utc = _window_bounds(report_date, start_et, end_et)
    started = pd.to_datetime(runs["started_at"], utc=True, errors="coerce")
    mask = started.notna() & (started >= start_utc) & (started <= end_utc)
    matched = runs.loc[mask].copy()
    if not matched.empty:
        return matched

    # Backfill/manual rows may not have started_at. Fall back only when there is
    # a single candidate run for the day so we do not smear one run across multiple windows.
    missing = runs.loc[started.isna()].copy()
    if len(missing) == 1:
        return missing
    return matched


def _aggregate_runs(runs: pd.DataFrame) -> dict | None:
    if runs.empty:
        return None

    priority = {"COMPLETED": 4, "ERROR": 3, "SKIPPED": 2, "RUNNING": 1}
    latest = runs.sort_values("started_at", na_position="last").iloc[-1]
    statuses = [str(s or "RUNNING") for s in runs["status"].tolist()]
    best_status = max(statuses, key=lambda s: priority.get(s, 0))
    return {
        "run_count": int(len(runs)),
        "run_status": best_status,
        "signal": str(latest.get("signal") or ""),
        "reason": str(latest.get("reason") or ""),
        "started_at": latest.get("started_at"),
        "completed_at": latest.get("completed_at"),
    }


def _group_fill_status(filled_qty: float, target_qty: float) -> str:
    if filled_qty <= 0:
        return "MISSED"
    if filled_qty < target_qty:
        return "PARTIAL_FILL"
    return "TAKEN"


def _aggregate_intents(intents: pd.DataFrame) -> dict | None:
    if intents.empty:
        return None

    target_qty = intents["target_qty"].fillna(0).astype(float)
    filled_qty = intents["filled_qty"].fillna(0).astype(float)
    per_group_status = [
        _group_fill_status(float(filled), float(target))
        for filled, target in zip(filled_qty.tolist(), target_qty.tolist())
    ]
    return {
        "intended_groups": int(len(intents)),
        "target_qty_total": float(target_qty.sum()),
        "filled_qty_total": float(filled_qty.sum()),
        "taken_groups": int(sum(s == "TAKEN" for s in per_group_status)),
        "partial_groups": int(sum(s == "PARTIAL_FILL" for s in per_group_status)),
        "missed_groups": int(sum(s == "MISSED" for s in per_group_status)),
    }


def _classify_trade_status(run_info: dict | None, intent_info: dict | None) -> tuple[str, str]:
    if not run_info:
        return "MISSED_RUN", "no strategy_run recorded"

    run_status = run_info["run_status"]
    reason = run_info.get("reason") or ""

    if not intent_info:
        if run_status == "SKIPPED":
            return "SKIPPED", reason or "strategy skipped before trade_intent"
        if run_status == "ERROR":
            return "ERROR", reason or "strategy errored before trade_intent"
        if run_status == "RUNNING":
            return "RUNNING", "run still marked RUNNING"
        return "NO_TRADE", reason or "completed with no trade_intent"

    intended = intent_info["intended_groups"]
    taken = intent_info["taken_groups"]
    partial = intent_info["partial_groups"]
    missed = intent_info["missed_groups"]

    if partial > 0 or (taken > 0 and missed > 0):
        return "PARTIAL_FILL", f"{taken + partial}/{intended} trade group(s) received fills"
    if taken == intended:
        return "TAKEN", f"{taken}/{intended} trade group(s) fully filled"
    if missed == intended:
        if run_status == "ERROR":
            return "ERROR", reason or "trade_intent recorded but run ended in ERROR"
        return "MISSED", "trade_intent recorded but no fills materialized"

    return "UNKNOWN", "could not classify execution outcome"


def get_trigger_audit(
    report_date: date | str,
    *,
    strategy: str | None = None,
    account: str | None = None,
    con=None,
) -> pd.DataFrame:
    """Return one row per expected trigger with actual run/execution outcome."""
    if con is None:
        con = get_connection()
        init_schema(con)

    report_date = _ensure_date(report_date)
    expected = _expected_triggers(report_date, strategy=strategy, account=account)
    if expected.empty:
        return expected

    runs = _load_runs(report_date, con, strategy=strategy, account=account)
    intents = _load_intents(report_date, con, strategy=strategy, account=account)
    rows: list[dict] = []
    for _, row in expected.iterrows():
        key_mask = (
            (runs["strategy"] == row["strategy"]) &
            (runs["account"] == row["account"])
        ) if not runs.empty else []
        runs_for_key = runs.loc[key_mask].copy() if not runs.empty else pd.DataFrame()
        runs_for_window = _select_runs_for_window(
            runs_for_key,
            report_date,
            row["trigger_start_et"],
            row["trigger_end_et"],
        )
        run_info = _aggregate_runs(runs_for_window)

        if run_info is not None and not intents.empty:
            intent_subset = intents.loc[intents["run_id"].isin(runs_for_window["run_id"].tolist())].copy()
        else:
            intent_subset = pd.DataFrame()
        intent_info = _aggregate_intents(intent_subset)
        trade_status, note = _classify_trade_status(run_info, intent_info)
        rows.append(
            {
                "trade_date": row["trade_date"],
                "strategy": row["strategy"],
                "account": row["account"],
                "trigger_rule": row["trigger_rule"],
                "trigger_window": row["trigger_window"],
                "run_count": int(run_info["run_count"]) if run_info else 0,
                "run_status": run_info["run_status"] if run_info else "NO_RUN",
                "signal": run_info["signal"] if run_info else "",
                "intended_groups": int(intent_info["intended_groups"]) if intent_info else 0,
                "taken_groups": int(intent_info["taken_groups"]) if intent_info else 0,
                "partial_groups": int(intent_info["partial_groups"]) if intent_info else 0,
                "missed_groups": int(intent_info["missed_groups"]) if intent_info else 0,
                "filled_qty_total": round(float(intent_info["filled_qty_total"]), 4) if intent_info else 0.0,
                "target_qty_total": round(float(intent_info["target_qty_total"]), 4) if intent_info else 0.0,
                "trade_status": trade_status,
                "note": note,
            }
        )

    return pd.DataFrame(rows).sort_values(["trigger_window", "strategy", "account"]).reset_index(drop=True)


def get_trigger_audit_range(
    start_date: date | str,
    end_date: date | str,
    *,
    strategy: str | None = None,
    account: str | None = None,
    con=None,
) -> pd.DataFrame:
    """Return trigger audit rows for every weekday in the inclusive date range."""
    if con is None:
        con = get_connection()
        init_schema(con)

    start = _ensure_date(start_date)
    end = _ensure_date(end_date)
    frames: list[pd.DataFrame] = []
    cur = start
    while cur <= end:
        if cur.weekday() < 5:
            frame = get_trigger_audit(cur, strategy=strategy, account=account, con=con)
            if not frame.empty:
                frames.append(frame)
        cur += timedelta(days=1)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def render_trigger_audit_section(
    report_date: date | str,
    *,
    strategy: str | None = None,
    account: str | None = None,
    con=None,
) -> str:
    """Render a markdown section for the daily report."""
    if con is None:
        con = get_connection()
        init_schema(con)

    frame = get_trigger_audit(report_date, strategy=strategy, account=account, con=con)
    if frame.empty:
        return "## Trigger Audit\nNo expected trigger windows today.\n"

    lines = ["## Trigger Audit\n"]
    lines.append("| Strategy | Account | Trigger | Run | Trade Status | Details |")
    lines.append("|----------|---------|---------|-----|--------------|---------|")

    for _, row in frame.iterrows():
        run = row["run_status"]
        if row["run_count"] > 1:
            run = f"{run} ({row['run_count']}x)"
        detail = str(row["note"] or "")
        if row["intended_groups"]:
            detail = (
                f"{detail}; intents={row['intended_groups']} "
                f"taken={row['taken_groups']} partial={row['partial_groups']} missed={row['missed_groups']}"
            )
        lines.append(
            f"| {row['strategy']} | {row['account']} | {row['trigger_window']} ({row['trigger_rule']}) "
            f"| {run} | {row['trade_status']} | {detail} |"
        )

    return "\n".join(lines) + "\n"
