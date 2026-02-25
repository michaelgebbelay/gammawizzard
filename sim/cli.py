"""CLI entry point for the SPX multi-agent trading simulation.

Usage:
    python -m sim.cli run-session --track adaptive --session 1 --date 2026-03-01
    python -m sim.cli run-all --track clean --max-sessions 80
    python -m sim.cli leaderboard --track adaptive
    python -m sim.cli report --track clean --session 5
    python -m sim.cli collect-chain --phase open
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from sim.config import DB_PATH, SESSIONS_PER_TRACK, VALID_TRACKS


def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


def cmd_run_all(args) -> None:
    """Run all sessions for a track."""
    import anthropic
    from sim.orchestrator.scheduler import Scheduler

    client = anthropic.Anthropic()
    db_path = Path(args.db) if args.db else DB_PATH

    scheduler = Scheduler(
        track=args.track,
        anthropic_client=client,
        db_path=db_path,
        max_sessions=args.max_sessions,
    )

    dates = None
    if args.dates:
        dates = args.dates.split(",")

    result = scheduler.run(trading_dates=dates)
    print(f"\nCompleted {result.get('sessions_completed', 0)} sessions "
          f"({result.get('total_completed', 0)} total for {args.track})")


def cmd_leaderboard(args) -> None:
    """Display leaderboards for a track."""
    from sim.persistence.db import get_connection
    from sim.reporting.leaderboard import financial_leaderboard, judge_leaderboard

    db_path = Path(args.db) if args.db else DB_PATH
    conn = get_connection(db_path)

    print(financial_leaderboard(conn, args.track))
    print()
    print(judge_leaderboard(conn, args.track))
    conn.close()


def cmd_report(args) -> None:
    """Generate a session report."""
    from sim.persistence.db import get_connection
    from sim.reporting.session_report import generate_session_report

    db_path = Path(args.db) if args.db else DB_PATH
    conn = get_connection(db_path)

    report = generate_session_report(conn, args.session, args.track)
    print(report)
    conn.close()


def cmd_collect_chain(args) -> None:
    """Collect chain data (wrapper around collect_chain.py)."""
    from sim.data.collect_chain import main as collect_main
    sys.argv = ["collect_chain", args.phase]
    collect_main()


def cmd_sync_cache(args) -> None:
    """Sync chain data from S3 to local cache."""
    from sim.config import CACHE_DIR
    from sim.data.s3_cache import sync_s3_to_local, s3_list_dates, S3_BUCKET

    bucket = args.bucket or S3_BUCKET
    dates = None
    if args.dates:
        dates = args.dates.split(",")

    print(f"Syncing from s3://{bucket}/ to {CACHE_DIR}/")

    if dates is None:
        all_dates = s3_list_dates(bucket)
        print(f"Found {len(all_dates)} dates in S3")
    else:
        all_dates = dates
        print(f"Syncing {len(all_dates)} specified dates")

    downloaded = sync_s3_to_local(CACHE_DIR, bucket=bucket, dates=all_dates)
    print(f"Downloaded {downloaded} new files")


def cmd_status(args) -> None:
    """Show simulation status."""
    from sim.persistence.db import get_connection
    from sim.persistence.queries import count_sessions

    db_path = Path(args.db) if args.db else DB_PATH

    if not db_path.exists():
        print("No database found. Run 'run-all' to start.")
        return

    conn = get_connection(db_path)

    for track in sorted(VALID_TRACKS):
        n = count_sessions(conn, track)
        print(f"  {track}: {n}/{SESSIONS_PER_TRACK} sessions completed")

    conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="SPX Multi-Agent Trading Simulation",
        prog="sim",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("--db", help="Custom database path")
    sub = parser.add_subparsers(dest="command")

    # run-all
    p_run = sub.add_parser("run-all", help="Run all sessions for a track")
    p_run.add_argument("--track", required=True, choices=sorted(VALID_TRACKS))
    p_run.add_argument("--max-sessions", type=int, default=SESSIONS_PER_TRACK)
    p_run.add_argument("--dates", help="Comma-separated trading dates")

    # leaderboard
    p_lb = sub.add_parser("leaderboard", help="Show leaderboards")
    p_lb.add_argument("--track", required=True, choices=sorted(VALID_TRACKS))

    # report
    p_rpt = sub.add_parser("report", help="Generate session report")
    p_rpt.add_argument("--track", required=True, choices=sorted(VALID_TRACKS))
    p_rpt.add_argument("--session", type=int, required=True)

    # collect-chain
    p_cc = sub.add_parser("collect-chain", help="Collect chain data")
    p_cc.add_argument("--phase", required=True, choices=["open", "mid", "close", "close5"])

    # sync-cache
    p_sync = sub.add_parser("sync-cache", help="Sync chain data from S3 to local cache")
    p_sync.add_argument("--bucket", help="S3 bucket name (default: gamma-sim-cache)")
    p_sync.add_argument("--dates", help="Comma-separated dates to sync (default: all)")

    # status
    sub.add_parser("status", help="Show simulation status")

    args = parser.parse_args()
    setup_logging(args.verbose)

    commands = {
        "run-all": cmd_run_all,
        "leaderboard": cmd_leaderboard,
        "report": cmd_report,
        "collect-chain": cmd_collect_chain,
        "sync-cache": cmd_sync_cache,
        "status": cmd_status,
    }

    if args.command in commands:
        commands[args.command](args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
