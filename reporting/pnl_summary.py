"""CLI tool to answer: am I making or losing money?

Queries the DuckDB portfolio store and prints a simple P&L summary
per strategy, with open positions and monthly breakdown.

Usage:
    python -m reporting.pnl_summary
    python -m reporting.pnl_summary --strategy dualside
"""

from __future__ import annotations

import sys
from datetime import date

from reporting.db import get_connection, init_schema, query_df, query_one


def print_summary(strategy_filter: str | None = None):
    con = get_connection()
    init_schema(con)

    today = date.today()

    # ── Per-Strategy Realized P&L ───────────────────────────────────────
    where = ""
    params = []
    if strategy_filter:
        where = "AND strategy = ?"
        params = [strategy_filter]

    closed = query_df(
        f"""SELECT strategy,
                   count(*) as trades,
                   sum(case when realized_pnl > 0 then 1 else 0 end) as wins,
                   sum(case when realized_pnl <= 0 then 1 else 0 end) as losses,
                   coalesce(sum(realized_pnl), 0) as total_pnl,
                   coalesce(avg(realized_pnl), 0) as avg_pnl
            FROM positions
            WHERE lifecycle_state IN ('CLOSED', 'EXPIRED', 'ASSIGNED')
              AND realized_pnl IS NOT NULL
              {where}
            GROUP BY strategy
            ORDER BY strategy""",
        params,
        con=con,
    )

    # This month's P&L
    month_start = today.replace(day=1).isoformat()
    closed_month = query_df(
        f"""SELECT strategy,
                   coalesce(sum(realized_pnl), 0) as month_pnl
            FROM positions
            WHERE lifecycle_state IN ('CLOSED', 'EXPIRED', 'ASSIGNED')
              AND realized_pnl IS NOT NULL
              AND closed_at >= ?
              {where}
            GROUP BY strategy""",
        [month_start] + params,
        con=con,
    )
    month_map = {}
    if not closed_month.empty:
        month_map = dict(zip(closed_month["strategy"], closed_month["month_pnl"]))

    print(f"\n{'='*60}")
    print(f"  Gamma Portfolio P&L Summary — {today.isoformat()}")
    print(f"{'='*60}")

    if closed.empty:
        print("\n  No closed positions with P&L found.")
        print("  Run the pipeline first: python -m reporting.run_pipeline")
    else:
        print(f"\n{'Strategy':<20} {'Total':>10} {'This Mo':>10} {'Trades':>7} {'Win%':>6}")
        print("-" * 60)
        grand_total = 0
        grand_month = 0
        grand_trades = 0
        grand_wins = 0

        for _, row in closed.iterrows():
            strat = row["strategy"]
            total = row["total_pnl"]
            month = month_map.get(strat, 0)
            trades = int(row["trades"])
            wins = int(row["wins"])
            wr = f"{wins/trades*100:.0f}%" if trades > 0 else "—"

            print(f"{strat:<20} {_fmt(total):>10} {_fmt(month):>10} {trades:>7} {wr:>6}")
            grand_total += total
            grand_month += month
            grand_trades += trades
            grand_wins += wins

        print("-" * 60)
        grand_wr = f"{grand_wins/grand_trades*100:.0f}%" if grand_trades > 0 else "—"
        print(f"{'TOTAL':<20} {_fmt(grand_total):>10} {_fmt(grand_month):>10} {grand_trades:>7} {grand_wr:>6}")

    # ── Missing P&L ────────────────────────────────────────────────────
    missing = query_one(
        f"""SELECT count(*) FROM positions
            WHERE lifecycle_state IN ('CLOSED', 'EXPIRED', 'ASSIGNED')
              AND realized_pnl IS NULL {where}""",
        params,
        con=con,
    )
    if missing and missing[0] > 0:
        print(f"\n  ⚠ {missing[0]} closed position(s) missing P&L (no settlement data)")

    # ── Open Positions ──────────────────────────────────────────────────
    open_pos = query_df(
        f"""SELECT strategy, account, trade_date, expiry_date,
                   entry_price, qty, signal, lifecycle_state
            FROM positions
            WHERE lifecycle_state IN ('OPEN', 'PARTIALLY_OPEN')
              {where}
            ORDER BY expiry_date""",
        params,
        con=con,
    )

    print(f"\n{'─'*60}")
    if open_pos.empty:
        print("  No open positions.")
    else:
        print(f"  Open Positions ({len(open_pos)})")
        print(f"\n{'Strategy':<16} {'Expiry':<12} {'Entry':>7} {'Qty':>4} {'Signal':<7} {'DTE':>4}")
        print("-" * 60)
        for _, p in open_pos.iterrows():
            exp = p["expiry_date"]
            if exp:
                exp_date = exp.date() if hasattr(exp, 'date') else exp
                dte = (exp_date - today).days
                exp_str = exp_date.isoformat()
            else:
                dte = "?"
                exp_str = "?"
            flag = " ← past!" if isinstance(dte, int) and dte < 0 else ""
            print(f"{p['strategy']:<16} {exp_str:<12} ${p['entry_price']:>5.2f} {p['qty']:>4} "
                  f"{p['signal'] or '?':<7} {dte:>4}{flag}")

    # ── Monthly Breakdown ───────────────────────────────────────────────
    monthly = query_df(
        f"""SELECT strftime(closed_at, '%Y-%m') as month,
                   count(*) as trades,
                   sum(case when realized_pnl > 0 then 1 else 0 end) as wins,
                   coalesce(sum(realized_pnl), 0) as pnl
            FROM positions
            WHERE lifecycle_state IN ('CLOSED', 'EXPIRED', 'ASSIGNED')
              AND realized_pnl IS NOT NULL
              {where}
            GROUP BY 1
            ORDER BY 1 DESC
            LIMIT 6""",
        params,
        con=con,
    )

    if not monthly.empty:
        print(f"\n{'─'*60}")
        print("  Monthly Breakdown")
        print(f"\n{'Month':<12} {'Trades':>7} {'Wins':>6} {'P&L':>12}")
        print("-" * 40)
        for _, m in monthly.iterrows():
            print(f"{m['month']:<12} {int(m['trades']):>7} {int(m['wins']):>6} {_fmt(m['pnl']):>12}")

    print()


def _fmt(val: float) -> str:
    """Format dollar value with sign."""
    if val >= 0:
        return f"+${val:,.0f}"
    return f"-${abs(val):,.0f}"


def main():
    strategy = None
    if len(sys.argv) > 1:
        for i, arg in enumerate(sys.argv[1:], 1):
            if arg == "--strategy" and i < len(sys.argv) - 1:
                strategy = sys.argv[i + 1]
    print_summary(strategy_filter=strategy)


if __name__ == "__main__":
    main()
