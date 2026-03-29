"""Multi-session runner with pause/resume (v14 — single track, 1DTE only)."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Optional

from sim.config import CACHE_DIR
from sim.data.cache import find_contiguous_windows, load_from_cache
from sim.data.chain_snapshot import parse_cboe_chain, parse_schwab_chain, parse_tt_chain
from sim.orchestrator.session import SessionRunner
from sim.persistence.db import init_db
from sim.persistence.queries import count_sessions

logger = logging.getLogger(__name__)


class Scheduler:
    """Run multiple sessions across cached chain data with pause/resume."""

    def __init__(self, anthropic_client=None, openai_client=None,
                 db_path: Optional[Path] = None,
                 max_sessions: int = 200):
        self.db_path = db_path
        self.max_sessions = max_sessions

        self.conn = init_db(db_path) if db_path else init_db()
        self.runner = SessionRunner(
            anthropic_client=anthropic_client,
            openai_client=openai_client,
            db_path=db_path,
        )

    def run(self, trading_dates: Optional[List[str]] = None) -> dict:
        """Run all sessions.

        Args:
            trading_dates: Explicit list of trading dates.
                          If None, discovers from cache.

        Returns:
            Summary dict with results per session.
        """
        if trading_dates is None:
            trading_dates = self._discover_dates()

        if not trading_dates:
            logger.error("No trading dates available")
            return {"error": "No trading dates found in cache"}

        # Resume from last completed session
        completed = count_sessions(self.conn)
        start_session = completed + 1

        remaining = min(self.max_sessions - completed, len(trading_dates) - completed)
        if remaining <= 0:
            logger.info("Already have %d/%d sessions complete",
                        completed, self.max_sessions)
            return {"sessions_completed": 0, "total_completed": completed}

        logger.info("Starting from session %d, running %d sessions",
                     start_session, remaining)

        results = []
        has_open_positions = False

        for i in range(remaining):
            session_id = start_session + i
            date_idx = completed + i

            if date_idx >= len(trading_dates):
                logger.warning("Ran out of trading dates at session %d", session_id)
                break

            trading_date = trading_dates[date_idx]

            # Load close5 chain (1DTE, ~4:05 PM)
            chain = self._load_chain(trading_date, "close5")

            if chain is None:
                # Fall back to close chain if close5 not available
                chain = self._load_chain(trading_date, "close")

            if chain is None:
                logger.error("Missing chain data for %s, skipping", trading_date)
                continue

            # Skip dates with empty chains (holidays, data gaps)
            if not chain.contracts:
                logger.warning("Empty chain for %s (no contracts), skipping",
                               trading_date)
                continue

            # Settlement: 1DTE positions from prior session expire today.
            # Use today's SPX close (chain.underlying_price) as settlement price.
            settle_price = chain.underlying_price if has_open_positions else None

            result = self.runner.run_session(
                session_id=session_id,
                trading_date=trading_date,
                chain=chain,
                prior_spx_close=settle_price,
            )

            results.append(result)
            has_open_positions = True

            logger.info("Session %d/%d complete (date=%s, SPX=%.0f)",
                        session_id, self.max_sessions, trading_date,
                        chain.underlying_price)

        return {
            "sessions_completed": len(results),
            "total_completed": completed + len(results),
        }

    def _discover_dates(self) -> List[str]:
        """Find all usable trading dates from cache."""
        windows = find_contiguous_windows(min_length=1, backtest=True)
        if not windows:
            return []

        # Concatenate all windows into a single sorted list
        all_dates = sorted(set(d for w in windows for d in w))
        logger.info("Found %d trading dates across %d windows (from %s to %s)",
                     len(all_dates), len(windows), all_dates[0], all_dates[-1])

        return all_dates[:self.max_sessions]

    def _load_chain(self, trading_date: str, phase: str) -> Optional:
        """Load chain snapshot from cache."""
        raw = load_from_cache(trading_date, phase)
        if raw is None:
            return None
        try:
            source = raw.get("_source", "")
            if source == "tastytrade":
                return parse_tt_chain(raw)
            elif source in ("cboe", "thetadata"):
                return parse_cboe_chain(raw)
            else:
                vix = raw.get("_vix", 0.0)
                return parse_schwab_chain(raw, phase, vix=vix)
        except Exception as e:
            logger.error("Failed to parse chain for %s/%s: %s",
                         trading_date, phase, e)
            return None
