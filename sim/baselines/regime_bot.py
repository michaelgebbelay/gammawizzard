"""Regime Bot — VIX + intraday move decision tree for structure selection."""

from __future__ import annotations

from typing import Optional

from sim.config import SPREAD_WIDTH
from sim.data.chain_snapshot import ChainSnapshot
from sim.engine.account import Account
from sim.engine.order import (
    Order,
    make_bear_call_vertical,
    make_bull_put_vertical,
    make_iron_condor,
    make_iron_fly,
)
from sim.baselines.base_baseline import BaseBaseline


class RegimeBot(BaseBaseline):
    """Bot 4: Simple VIX + move decision tree.

    Decision rules:
    ┌─────────────────────────────────────────────────────────┐
    │  VIX < 15 (low vol)                                     │
    │    → Iron fly at ATM (collect max premium in low vol)    │
    │                                                          │
    │  15 ≤ VIX < 25 (normal vol)                             │
    │    → 10-delta iron condor (standard premium selling)     │
    │                                                          │
    │  VIX ≥ 25 (high vol)                                    │
    │    If SPX gap down > 0.5%: bull put vertical (mean rev)  │
    │    If SPX gap up > 0.5%: bear call vertical (mean rev)   │
    │    Else: hold cash (too volatile, no edge)               │
    └─────────────────────────────────────────────────────────┘

    The gap is approximated as (underlying_price - prior_close) / prior_close.
    Since we don't track prior close directly, we use the chain's open vs.
    the SPX level provided in the session context. When prior close isn't
    available, the bot defaults to the IC path.
    """

    def __init__(self, agent_id: str = "bot-regime",
                 spx_prior_close: Optional[float] = None):
        super().__init__(agent_id)
        self.spx_prior_close = spx_prior_close

    def set_prior_close(self, price: float) -> None:
        """Set prior session's SPX close for gap calculation."""
        self.spx_prior_close = price

    def decide(self, chain: ChainSnapshot, account: Account,
               session_id: int, track: str) -> Optional[Order]:
        vix = chain.vix
        expiration = chain.expirations[0] if chain.expirations else None

        if vix < 15:
            return self._iron_fly(chain, expiration)
        elif vix < 25:
            return self._iron_condor(chain, expiration)
        else:
            return self._high_vol_decision(chain, expiration)

    def _iron_fly(self, chain: ChainSnapshot,
                  expiration) -> Optional[Order]:
        """Low vol → iron fly at ATM for max premium."""
        atm = chain.atm_strike()
        return make_iron_fly(
            agent_id=self.agent_id,
            center_strike=atm,
            wing_width=SPREAD_WIDTH,
            qty=1,
            thesis=f"Regime: low VIX ({chain.vix:.1f}) → ATM iron fly for max premium.",
        )

    def _iron_condor(self, chain: ChainSnapshot,
                     expiration) -> Optional[Order]:
        """Normal vol → standard 10-delta IC."""
        put_short = chain.nearest_delta_strike(-0.10, "P", expiration)
        call_short = chain.nearest_delta_strike(0.10, "C", expiration)

        if put_short is None or call_short is None:
            return None

        put_long = put_short - SPREAD_WIDTH
        call_long = call_short + SPREAD_WIDTH

        if (chain.get_contract(put_long, "P", expiration) is None
                or chain.get_contract(call_long, "C", expiration) is None):
            return None

        return make_iron_condor(
            agent_id=self.agent_id,
            put_long=put_long,
            put_short=put_short,
            call_short=call_short,
            call_long=call_long,
            qty=1,
            thesis=f"Regime: normal VIX ({chain.vix:.1f}) → 10-delta IC.",
        )

    def _high_vol_decision(self, chain: ChainSnapshot,
                           expiration) -> Optional[Order]:
        """High vol → directional vertical on gap, or hold cash."""
        gap_pct = self._gap_pct(chain.underlying_price)

        if gap_pct is not None and gap_pct < -0.005:
            # Gap down → sell bull put (mean reversion bet)
            return self._bull_put(chain, expiration, gap_pct)
        elif gap_pct is not None and gap_pct > 0.005:
            # Gap up → sell bear call (mean reversion bet)
            return self._bear_call(chain, expiration, gap_pct)
        else:
            # No clear gap or no prior close data → hold cash
            return None

    def _bull_put(self, chain: ChainSnapshot,
                  expiration, gap_pct: float) -> Optional[Order]:
        """Sell bull put vertical on gap-down (mean reversion)."""
        short_strike = chain.nearest_delta_strike(-0.15, "P", expiration)
        if short_strike is None:
            return None
        long_strike = short_strike - SPREAD_WIDTH

        if chain.get_contract(long_strike, "P", expiration) is None:
            return None

        return make_bull_put_vertical(
            agent_id=self.agent_id,
            short_strike=short_strike,
            long_strike=long_strike,
            qty=1,
            thesis=(f"Regime: high VIX ({chain.vix:.1f}), "
                    f"gap down {gap_pct*100:.1f}% → bull put for mean reversion."),
        )

    def _bear_call(self, chain: ChainSnapshot,
                   expiration, gap_pct: float) -> Optional[Order]:
        """Sell bear call vertical on gap-up (mean reversion)."""
        short_strike = chain.nearest_delta_strike(0.15, "C", expiration)
        if short_strike is None:
            return None
        long_strike = short_strike + SPREAD_WIDTH

        if chain.get_contract(long_strike, "C", expiration) is None:
            return None

        return make_bear_call_vertical(
            agent_id=self.agent_id,
            short_strike=short_strike,
            long_strike=long_strike,
            qty=1,
            thesis=(f"Regime: high VIX ({chain.vix:.1f}), "
                    f"gap up {gap_pct*100:.1f}% → bear call for mean reversion."),
        )

    def _gap_pct(self, current_price: float) -> Optional[float]:
        """Calculate gap percentage from prior close."""
        if self.spx_prior_close is None or self.spx_prior_close == 0:
            return None
        return (current_price - self.spx_prior_close) / self.spx_prior_close
