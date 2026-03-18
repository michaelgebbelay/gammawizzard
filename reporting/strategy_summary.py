"""Materialize strategy_daily and portfolio_daily tables from positions.

Populates the Phase 6 rollup tables so reporting queries have pre-computed
daily scorecards per strategy and a whole-portfolio rollup.

Usage:
    from reporting.strategy_summary import materialize_all
    stats = materialize_all(con, since_date=date(2026, 3, 1))
"""

from __future__ import annotations

import uuid
from datetime import date, timedelta

from reporting.db import execute, get_connection, init_schema, query_df, query_one


# ---------------------------------------------------------------------------
# Strategy daily
# ---------------------------------------------------------------------------

def materialize_strategy_daily(con, report_date: date) -> int:
    """Populate strategy_daily for a single date. Returns rows upserted."""
    if con is None:
        con = get_connection()

    dt = report_date.isoformat()
    count = 0

    # Get all (strategy, account) combos that had activity on this date
    combos = query_df(
        """SELECT DISTINCT strategy, account FROM (
             SELECT strategy, account FROM positions WHERE trade_date = ?
             UNION
             SELECT strategy, account FROM positions
               WHERE closed_at IS NOT NULL AND CAST(closed_at AS DATE) = ?
             UNION
             SELECT strategy, account FROM strategy_runs WHERE trade_date = ?
           )""",
        [dt, dt, dt],
        con=con,
    )

    for _, combo in combos.iterrows():
        strat = combo["strategy"]
        acct = combo["account"]

        # Trades opened
        opened = query_one(
            "SELECT count(*) FROM positions WHERE strategy=? AND account=? AND trade_date=?",
            [strat, acct, dt], con=con,
        )
        trades_opened = opened[0] if opened else 0

        # Trades closed (terminal on this date)
        closed = query_one(
            """SELECT count(*) FROM positions
               WHERE strategy=? AND account=? AND CAST(closed_at AS DATE)=?
                 AND lifecycle_state IN ('CLOSED','EXPIRED','ASSIGNED')""",
            [strat, acct, dt], con=con,
        )
        trades_closed = closed[0] if closed else 0

        # Trades skipped
        skipped = query_one(
            "SELECT count(*) FROM strategy_runs WHERE strategy=? AND account=? AND trade_date=? AND status='SKIPPED'",
            [strat, acct, dt], con=con,
        )
        trades_skipped = skipped[0] if skipped else 0

        # Realized P&L (from positions closed on this date)
        pnl = query_one(
            """SELECT coalesce(sum(realized_pnl), 0) FROM positions
               WHERE strategy=? AND account=? AND CAST(closed_at AS DATE)=?
                 AND lifecycle_state IN ('CLOSED','EXPIRED','ASSIGNED')
                 AND realized_pnl IS NOT NULL""",
            [strat, acct, dt], con=con,
        )
        realized_pnl = float(pnl[0]) if pnl else 0.0

        # Win rate (last 20 closed trades for this strategy)
        recent = query_df(
            """SELECT realized_pnl FROM positions
               WHERE strategy=? AND account=? AND realized_pnl IS NOT NULL
                 AND lifecycle_state IN ('CLOSED','EXPIRED','ASSIGNED')
               ORDER BY closed_at DESC LIMIT 20""",
            [strat, acct], con=con,
        )
        win_rate = None
        if not recent.empty:
            wins = (recent["realized_pnl"] > 0).sum()
            win_rate = round(wins / len(recent), 4)

        # Upsert (delete + insert for DuckDB)
        execute(
            "DELETE FROM strategy_daily WHERE strategy=? AND account=? AND report_date=?",
            [strat, acct, dt], con=con,
        )
        row_id = uuid.uuid4().hex[:16]
        execute(
            """INSERT INTO strategy_daily
               (id, strategy, account, report_date, trades_opened, trades_closed,
                trades_skipped, realized_pnl, win_rate_20d)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [row_id, strat, acct, dt, trades_opened, trades_closed,
             trades_skipped, realized_pnl, win_rate],
            con=con,
        )
        count += 1

    return count


# ---------------------------------------------------------------------------
# Portfolio daily
# ---------------------------------------------------------------------------

def materialize_portfolio_daily(con, report_date: date) -> int:
    """Populate portfolio_daily for a single date. Returns 1 if row created."""
    dt = report_date.isoformat()

    # Aggregate from strategy_daily
    agg = query_one(
        """SELECT coalesce(sum(realized_pnl), 0),
                  coalesce(sum(trades_opened), 0),
                  coalesce(sum(trades_closed), 0)
           FROM strategy_daily WHERE report_date = ?""",
        [dt], con=con,
    )
    if not agg or (agg[1] == 0 and agg[2] == 0):
        return 0

    realized_pnl_day = float(agg[0])
    open_count = query_one(
        "SELECT count(*) FROM positions WHERE lifecycle_state IN ('OPEN','PARTIALLY_OPEN')",
        con=con,
    )

    execute(
        "DELETE FROM portfolio_daily WHERE report_date = ?",
        [dt], con=con,
    )
    row_id = uuid.uuid4().hex[:16]
    execute(
        """INSERT INTO portfolio_daily
           (id, report_date, total_open_positions, realized_pnl_day)
           VALUES (?, ?, ?, ?)""",
        [row_id, dt, open_count[0] if open_count else 0, realized_pnl_day],
        con=con,
    )
    return 1


# ---------------------------------------------------------------------------
# Batch
# ---------------------------------------------------------------------------

def materialize_all(con=None, since_date: date | None = None) -> dict:
    """Materialize strategy_daily + portfolio_daily for a date range.

    Defaults to last 30 days if since_date not provided.
    """
    if con is None:
        con = get_connection()
        init_schema(con)

    if since_date is None:
        since_date = date.today() - timedelta(days=30)

    stats = {"strategy_daily_rows": 0, "portfolio_daily_rows": 0, "dates_processed": 0}
    d = since_date
    today = date.today()

    while d <= today:
        stats["dates_processed"] += 1
        stats["strategy_daily_rows"] += materialize_strategy_daily(con, d)
        stats["portfolio_daily_rows"] += materialize_portfolio_daily(con, d)
        d += timedelta(days=1)

    return stats
