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
# Each tuple: (strategy, account, weekdays, start_et, end_et, rule_name)
# weekdays: list of ints (0=Mon .. 4=Fri)
# Window is generous: schedule time ± ~15 min for Lambda lag / retries.

WINDOWS = [
    # ButterflyTuesday — 16:01 ET Mon-Fri, allow 15:55–16:25
    ("butterfly", "schwab", range(5), "15:55", "16:25", "bf_daily_trigger"),

    # DualSide — 16:05 ET Mon-Fri, allow 16:00–16:30
    ("dualside", "schwab", range(5), "16:00", "16:30", "ds_daily_trigger"),

    # ConstantStable — 16:13 ET Mon-Fri (main), allow 16:08–16:45
    ("constantstable", "schwab", range(5), "16:08", "16:45", "cs_daily_trigger"),
    ("constantstable", "tt-ira", range(5), "16:08", "16:45", "cs_daily_trigger"),
    ("constantstable", "tt-individual", range(5), "16:08", "16:45", "cs_daily_trigger"),

    # ConstantStable — 09:35 ET Mon-Fri (deferred IC_LONG morning entry)
    ("constantstable", "schwab", range(5), "09:30", "10:00", "cs_morning_trigger"),

    # Novix — 16:15 ET Mon-Fri, allow 16:10–16:45
    ("novix", "novix-tt-ira", range(5), "16:10", "16:45", "nx_daily_trigger"),
    ("novix", "novix-tt-individual", range(5), "16:10", "16:45", "nx_daily_trigger"),
]


def seed(con=None) -> int:
    """Insert trigger windows, skipping duplicates. Returns count inserted."""
    if con is None:
        con = get_connection()
        init_schema(con)

    count = 0
    for strategy, account, weekdays, start_et, end_et, rule_name in WINDOWS:
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
