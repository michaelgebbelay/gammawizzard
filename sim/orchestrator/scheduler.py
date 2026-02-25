"""Multi-session runner with pause/resume capability."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import List, Optional

import anthropic

from sim.config import CACHE_DIR, SESSIONS_PER_TRACK, VALID_TRACKS
from sim.data.cache import find_contiguous_windows, load_from_cache
from sim.data.chain_snapshot import parse_schwab_chain, parse_tt_chain
from sim.judge.rubric import Rubric
from sim.orchestrator.session import SessionRunner
from sim.persistence.db import init_db
from sim.persistence.queries import count_sessions, get_latest_rubric

logger = logging.getLogger(__name__)


class Scheduler:
    """Run multiple sessions across cached chain data with pause/resume."""

    def __init__(self, track: str,
                 anthropic_client: Optional[anthropic.Anthropic] = None,
                 db_path: Optional[Path] = None,
                 max_sessions: int = SESSIONS_PER_TRACK):
        if track not in VALID_TRACKS:
            raise ValueError(f"Invalid track: {track}. Must be one of {VALID_TRACKS}")

        self.track = track
        self.client = anthropic_client or anthropic.Anthropic()
        self.db_path = db_path
        self.max_sessions = max_sessions

        self.conn = init_db(db_path) if db_path else init_db()
        self.runner = SessionRunner(
            track=track, anthropic_client=self.client, db_path=db_path,
        )

    def run(self, trading_dates: Optional[List[str]] = None) -> dict:
        """Run all sessions for the track.

        Args:
            trading_dates: Explicit list of trading dates to use.
                          If None, discovers from cache.

        Returns:
            Summary dict with results per session.
        """
        # Determine trading dates
        if trading_dates is None:
            trading_dates = self._discover_dates()

        if not trading_dates:
            logger.error("No trading dates available")
            return {"error": "No trading dates found in cache"}

        # Check how many sessions already completed (for resume)
        completed = count_sessions(self.conn, self.track)
        start_session = completed + 1

        remaining = min(self.max_sessions - completed, len(trading_dates) - completed)
        if remaining <= 0:
            logger.info("Track %s already has %d/%d sessions complete",
                        self.track, completed, self.max_sessions)
            return {"completed": completed, "sessions": []}

        logger.info("Track %s: starting from session %d, running %d sessions",
                     self.track, start_session, remaining)

        # Load rubric state (for resume)
        rubric = self._load_rubric()

        # Run sessions
        results = []
        prior_spx_close = None

        for i in range(remaining):
            session_id = start_session + i
            date_idx = completed + i

            if date_idx >= len(trading_dates):
                logger.warning("Ran out of trading dates at session %d", session_id)
                break

            trading_date = trading_dates[date_idx]

            # Load chain data from cache
            open_chain = self._load_chain(trading_date, "open")
            close_chain = self._load_chain(trading_date, "close")
            mid_chain = self._load_chain(trading_date, "mid")
            close5_chain = self._load_chain(trading_date, "close5")

            if open_chain is None or close_chain is None:
                logger.error("Missing chain data for %s, skipping", trading_date)
                continue

            if close5_chain is None:
                logger.warning("No close5 chain for %s — CLOSE+5 window will be skipped",
                               trading_date)

            # Run session
            result = self.runner.run_session(
                session_id=session_id,
                trading_date=trading_date,
                open_chain=open_chain,
                close_chain=close_chain,
                close5_chain=close5_chain,
                prior_spx_close=prior_spx_close,
                rubric=rubric,
                mid_chain=mid_chain,
            )

            results.append(result)
            prior_spx_close = close_chain.underlying_price

            # Update rubric for next session
            if "rubric" in result and result["rubric"] is not None:
                rubric = result["rubric"]

            logger.info("Session %d/%d complete (date=%s, SPX=%.0f→%.0f)",
                        session_id, self.max_sessions, trading_date,
                        open_chain.underlying_price, close_chain.underlying_price)

        return {
            "track": self.track,
            "sessions_completed": len(results),
            "total_completed": completed + len(results),
            "sessions": results,
        }

    def _discover_dates(self) -> List[str]:
        """Find contiguous trading dates from cache."""
        windows = find_contiguous_windows(min_length=1)
        if not windows:
            return []

        # Use the longest contiguous window
        best = max(windows, key=len)
        logger.info("Found %d contiguous dates (from %s to %s)",
                     len(best), best[0], best[-1])

        # Limit to max sessions needed
        return best[:self.max_sessions]

    def _load_chain(self, trading_date: str, phase: str) -> Optional:
        """Load chain snapshot from cache. Auto-detects TT vs Schwab format."""
        raw = load_from_cache(trading_date, phase)
        if raw is None:
            return None
        try:
            # Auto-detect source format
            if raw.get("_source") == "tastytrade":
                return parse_tt_chain(raw)
            else:
                vix = raw.get("_vix", 0.0)
                return parse_schwab_chain(raw, phase, vix=vix)
        except Exception as e:
            logger.error("Failed to parse chain for %s/%s: %s",
                         trading_date, phase, e)
            return None

    def _load_rubric(self) -> Rubric:
        """Load the latest rubric from DB, or create default."""
        saved = get_latest_rubric(self.conn, self.track)
        if saved:
            return Rubric(
                weights=saved["weights"],
                rationale=saved.get("rationale", ""),
            )
        return Rubric()
