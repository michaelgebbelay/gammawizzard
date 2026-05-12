"""Seed strategy_trigger_windows from known Lambda schedules.

Run once after schema init, or re-run to update windows.

Usage:
    python -m reporting.seed_trigger_windows
"""

from __future__ import annotations

import uuid

from reporting.db import execute, get_connection, init_schema, query_one


# ---------------------------------------------------------------------------
# Known trigger windows (from lambda/template.yaml EventBridge schedules)
# ---------------------------------------------------------------------------
# Each entry:
#   strategy/account/weekdays/start_et/end_et/rule_name are required
#   start_date/end_date are optional YYYY-MM-DD cutover bounds
# weekdays: list of ints (0=Mon .. 4=Fri)
# Window is generous: schedule time ± ~15 min for Lambda lag / retries.

WINDOWS = [
    # ButterflyTuesday — 16:01 ET Mon-Fri, allow 15:55–16:25
    {"strategy": "butterfly", "account": "schwab", "weekdays": range(5), "start_et": "15:55", "end_et": "16:25", "rule_name": "bf_daily_trigger"},

    # DualSide — 16:05 ET Mon-Fri, allow 16:00–16:30
    {"strategy": "dualside", "account": "schwab", "weekdays": range(5), "start_et": "16:00", "end_et": "16:30", "rule_name": "ds_daily_trigger"},

    # ConstantStable — 16:13 ET Mon-Fri (main), allow 16:08–16:45
    {"strategy": "constantstable", "account": "schwab", "weekdays": range(5), "start_et": "16:08", "end_et": "16:45", "rule_name": "cs_daily_trigger"},
    {"strategy": "constantstable", "account": "tt-ira", "weekdays": range(5), "start_et": "16:08", "end_et": "16:45", "rule_name": "cs_daily_trigger", "end_date": "2026-04-27"},
    {"strategy": "leoprofit", "account": "tt-ira", "weekdays": range(5), "start_et": "16:08", "end_et": "16:45", "rule_name": "leo_daily_trigger", "start_date": "2026-04-28"},
    {"strategy": "constantstable", "account": "tt-individual", "weekdays": range(5), "start_et": "16:08", "end_et": "16:45", "rule_name": "cs_daily_trigger"},

    # ConstantStable — 09:35 ET Mon-Fri (deferred IC_LONG morning entry)
    {"strategy": "constantstable", "account": "schwab", "weekdays": range(5), "start_et": "09:30", "end_et": "10:00", "rule_name": "cs_morning_trigger"},

    # Novix — 16:15 ET Mon-Fri, allow 16:10–16:45
    {"strategy": "novix", "account": "novix-tt-ira", "weekdays": range(5), "start_et": "16:10", "end_et": "16:45", "rule_name": "nx_daily_trigger"},
    {"strategy": "novix", "account": "novix-tt-individual", "weekdays": range(5), "start_et": "16:10", "end_et": "16:45", "rule_name": "nx_daily_trigger"},
]


def seed(con=None) -> int:
    """Insert trigger windows, skipping duplicates. Returns count inserted."""
    if con is None:
        con = get_connection()
        init_schema(con)

    count = 0
    for window in WINDOWS:
        strategy = window["strategy"]
        account = window["account"]
        weekdays = window["weekdays"]
        start_et = window["start_et"]
        end_et = window["end_et"]
        rule_name = window["rule_name"]
        for wd in weekdays:
            existing = query_one(
                """SELECT 1 FROM strategy_trigger_windows
                   WHERE strategy = ? AND account = ? AND weekday = ? AND rule_name = ?""",
                [strategy, account, wd, rule_name],
                con=con,
            )
            if existing:
                continue

            execute(
                """INSERT INTO strategy_trigger_windows
                   (id, strategy, account, weekday, start_et, end_et, rule_name)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                [uuid.uuid4().hex[:16], strategy, account, wd, start_et, end_et, rule_name],
                con=con,
            )
            count += 1

    return count


if __name__ == "__main__":
    n = seed()
    print(f"Seeded {n} trigger window(s)")
