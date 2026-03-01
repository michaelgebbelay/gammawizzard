"""CLI for the live binary-vertical game."""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

from sim_live.config import (
    DB_PATH,
    LIVE_START_DATE,
    DEFAULT_GSHEET_ID,
    DEFAULT_LEADERBOARD_TAB,
    DEFAULT_RESULTS_TAB,
)
from sim_live.engine import LiveGameEngine
from sim_live.feed import LeoFeed
from sim_live.store import Store


def _parse_date(v: str | None) -> date:
    if not v:
        return date.today()
    return date.fromisoformat(v)


def cmd_run_live(args) -> None:
    signal_date = _parse_date(args.date)
    feed = LeoFeed(csv_path=Path(args.csv) if args.csv else None, api_url=args.api_url)
    store = Store(Path(args.db) if args.db else DB_PATH)
    engine = LiveGameEngine(store)

    result = engine.run_live_round(
        signal_date=signal_date,
        feed=feed,
        allow_prestart=args.allow_prestart,
    )

    print(
        f"Round created: signal_date={result['signal_date']} "
        f"tdate={result['tdate']} "
        f"start_guard={LIVE_START_DATE.isoformat()}"
    )
    if result["settled_rounds"]:
        print(f"Auto-settled {len(result['settled_rounds'])} due round(s)")
    for d in result["decisions"]:
        status = "VALID" if d["valid"] else f"INVALID ({d['error']})"
        dec = d["decision"]
        print(
            f"- {d['player_id']}: {status} | put={dec['put_action']}[{dec['put_width']}] "
            f"call={dec['call_action']}[{dec['call_width']}] size={dec['size']} "
            f"template={dec['template_id']}"
        )


def cmd_settle(args) -> None:
    settle_date = _parse_date(args.date)
    feed = LeoFeed(csv_path=Path(args.csv) if args.csv else None, api_url=args.api_url)
    store = Store(Path(args.db) if args.db else DB_PATH)
    engine = LiveGameEngine(store)

    settled = engine.settle_due(settle_date, feed)
    print(f"Settle run @ {settle_date.isoformat()}: {len(settled)} round(s) settled")
    for s in settled:
        print(f"- signal_date={s['signal_date']} tdate={s['tdate']}")

    if args.push_sheet:
        from sim_live.gsheet import sync_game_to_sheet

        try:
            summary = sync_game_to_sheet(
                store=store,
                sheet_id=args.sheet_id,
                results_tab=args.results_tab,
                leaderboard_tab=args.leaderboard_tab,
            )
            print(
                "Sheet synced: "
                f"id={summary['sheet_id']} "
                f"results_tab={summary['results_tab']} rows={summary['results_rows']} "
                f"leaderboard_tab={summary['leaderboard_tab']} rows={summary['leaderboard_rows']}"
            )
        except Exception as e:
            print(f"Sheet sync failed: {type(e).__name__}: {e}")


def cmd_leaderboard(args) -> None:
    store = Store(Path(args.db) if args.db else DB_PATH)
    rows = store.leaderboard()
    if not rows:
        print("No settled rounds yet.")
        return

    print("=== Live Game Leaderboard ===")
    print(
        f"{'Rank':>4}  {'Player':<20} {'Rounds':>6} {'Total PnL':>12} "
        f"{'Max DD':>10} {'Risk Adj':>10} {'Win %':>8} {'Avg PnL':>10} {'Avg Judge':>10}"
    )
    for i, r in enumerate(rows, 1):
        print(
            f"{i:>4}  {r['player_id']:<20} {r['rounds']:>6} "
            f"{r['total_pnl']:>12.2f} {r['max_drawdown']:>10.2f} {r['risk_adjusted']:>10.2f} "
            f"{r['win_rate'] * 100:>8.2f} {r['avg_pnl']:>10.2f} {r['avg_judge']:>10.2f}"
        )


def cmd_round(args) -> None:
    signal_date = _parse_date(args.date).isoformat()
    store = Store(Path(args.db) if args.db else DB_PATH)

    r = store.get_round(signal_date)
    if not r:
        print(f"No round found for {signal_date}")
        return

    print(f"Round {signal_date}: status={r['status']} tdate={r['tdate']}")
    decisions = store.get_decisions(signal_date)
    if decisions:
        print("Decisions:")
        for d in decisions:
            print(f"- {d['player_id']}: valid={bool(d['valid'])} error={d['error']}")
    results = store.get_results(signal_date)
    if results:
        print("Results:")
        for x in results:
            print(
                f"- {x['player_id']}: put={x['put_pnl']:.2f} call={x['call_pnl']:.2f} "
                f"total={x['total_pnl']:.2f} equity={x['equity_pnl']:.2f} "
                f"dd={x['drawdown']:.2f} maxdd={x['max_drawdown']:.2f} "
                f"risk_adj={x['risk_adjusted']:.2f} judge={x['judge_score']:.2f}"
            )


def cmd_sync_sheet(args) -> None:
    from sim_live.gsheet import sync_game_to_sheet

    store = Store(Path(args.db) if args.db else DB_PATH)
    try:
        summary = sync_game_to_sheet(
            store=store,
            sheet_id=args.sheet_id,
            results_tab=args.results_tab,
            leaderboard_tab=args.leaderboard_tab,
        )
        print(
            "Sheet synced: "
            f"id={summary['sheet_id']} "
            f"results_tab={summary['results_tab']} rows={summary['results_rows']} "
            f"leaderboard_tab={summary['leaderboard_tab']} rows={summary['leaderboard_rows']}"
        )
    except Exception as e:
        print(f"Sheet sync failed: {type(e).__name__}: {e}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Live binary-vertical game")
    parser.add_argument("--db", help=f"SQLite DB path (default: {DB_PATH})")
    sub = parser.add_subparsers(dest="cmd")

    p_run = sub.add_parser("run-live", help="Run one live decision round")
    p_run.add_argument("--date", help="Signal date YYYY-MM-DD (default: today)")
    p_run.add_argument("--csv", help="CSV source file path")
    p_run.add_argument("--api-url", help="Leo API URL")
    p_run.add_argument(
        "--allow-prestart",
        action="store_true",
        help="Allow running rounds before 2026-03-02 (for validation only)",
    )

    p_settle = sub.add_parser("settle", help="Settle all due rounds")
    p_settle.add_argument("--date", help="Settlement date YYYY-MM-DD (default: today)")
    p_settle.add_argument("--csv", help="CSV source file path")
    p_settle.add_argument("--api-url", help="Leo API URL")
    p_settle.add_argument("--push-sheet", action="store_true", help="Push settled/leaderboard data to Google Sheets")
    p_settle.add_argument("--sheet-id", default=DEFAULT_GSHEET_ID, help="Google Sheet ID")
    p_settle.add_argument("--results-tab", default=DEFAULT_RESULTS_TAB, help="Results tab name")
    p_settle.add_argument("--leaderboard-tab", default=DEFAULT_LEADERBOARD_TAB, help="Leaderboard tab name")

    sub.add_parser("leaderboard", help="Show live leaderboard")

    p_sync = sub.add_parser("sync-sheet", help="Push current results + leaderboard to Google Sheets")
    p_sync.add_argument("--sheet-id", default=DEFAULT_GSHEET_ID, help="Google Sheet ID")
    p_sync.add_argument("--results-tab", default=DEFAULT_RESULTS_TAB, help="Results tab name")
    p_sync.add_argument("--leaderboard-tab", default=DEFAULT_LEADERBOARD_TAB, help="Leaderboard tab name")

    p_round = sub.add_parser("round", help="Show one round details")
    p_round.add_argument("--date", required=True, help="Signal date YYYY-MM-DD")

    args = parser.parse_args()

    commands = {
        "run-live": cmd_run_live,
        "settle": cmd_settle,
        "leaderboard": cmd_leaderboard,
        "round": cmd_round,
        "sync-sheet": cmd_sync_sheet,
    }
    if args.cmd in commands:
        commands[args.cmd](args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
