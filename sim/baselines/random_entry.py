"""Random Entry bot — picks a random structure and random strikes each session."""

from __future__ import annotations

import random
from typing import Optional

from sim.config import RNG_SEED, SPREAD_WIDTH
from sim.data.chain_snapshot import ChainSnapshot
from sim.engine.account import Account
from sim.engine.order import (
    Order,
    make_bear_call_vertical,
    make_bull_put_vertical,
    make_iron_condor,
    make_iron_fly,
    make_call_butterfly,
    make_put_butterfly,
)
from sim.baselines.base_baseline import BaseBaseline


class RandomEntry(BaseBaseline):
    """Bot 2: Random structure and strikes every session (seeded RNG).

    - Picks uniformly from: bull put, bear call, IC, iron fly, call butterfly, put butterfly
    - Strikes chosen randomly from available OTM options
    - Always trades 1 lot
    - Deterministic via shared RNG seed for reproducibility across tracks
    """

    def __init__(self, agent_id: str = "bot-random-entry", seed: int = RNG_SEED):
        super().__init__(agent_id)
        self.rng = random.Random(seed)

    def decide(self, chain: ChainSnapshot, account: Account,
               session_id: int, track: str) -> Optional[Order]:
        expiration = chain.expirations[0] if chain.expirations else None
        atm = chain.atm_strike()

        # Get OTM puts (below ATM) and OTM calls (above ATM)
        otm_put_strikes = sorted(
            [s for s in chain.strikes if s < atm and chain.get_contract(s, "P", expiration)],
        )
        otm_call_strikes = sorted(
            [s for s in chain.strikes if s > atm and chain.get_contract(s, "C", expiration)],
        )

        if len(otm_put_strikes) < 3 or len(otm_call_strikes) < 3:
            return None

        structure = self.rng.choice([
            "bull_put", "bear_call", "iron_condor",
            "iron_fly", "call_butterfly", "put_butterfly",
        ])

        if structure == "bull_put":
            return self._random_bull_put(otm_put_strikes)
        elif structure == "bear_call":
            return self._random_bear_call(otm_call_strikes)
        elif structure == "iron_condor":
            return self._random_ic(otm_put_strikes, otm_call_strikes)
        elif structure == "iron_fly":
            return self._random_iron_fly(atm, chain)
        elif structure == "call_butterfly":
            return self._random_call_butterfly(otm_call_strikes, atm)
        else:
            return self._random_put_butterfly(otm_put_strikes, atm)

    def _random_bull_put(self, put_strikes: list) -> Order:
        """Random bull put vertical from available OTM put strikes."""
        idx = self.rng.randint(0, len(put_strikes) - 2)
        long_strike = put_strikes[idx]
        short_strike = long_strike + SPREAD_WIDTH
        # If short strike not in available strikes, use next higher available
        if short_strike not in put_strikes and short_strike > put_strikes[-1]:
            short_strike = put_strikes[idx + 1]
            long_strike = short_strike - SPREAD_WIDTH
        return make_bull_put_vertical(
            agent_id=self.agent_id,
            short_strike=short_strike,
            long_strike=long_strike,
            qty=1,
            thesis="Random entry — bull put vertical.",
        )

    def _random_bear_call(self, call_strikes: list) -> Order:
        """Random bear call vertical from available OTM call strikes."""
        idx = self.rng.randint(0, len(call_strikes) - 2)
        short_strike = call_strikes[idx]
        long_strike = short_strike + SPREAD_WIDTH
        if long_strike not in call_strikes:
            long_strike = call_strikes[min(idx + 1, len(call_strikes) - 1)]
            short_strike = long_strike - SPREAD_WIDTH
        return make_bear_call_vertical(
            agent_id=self.agent_id,
            short_strike=short_strike,
            long_strike=long_strike,
            qty=1,
            thesis="Random entry — bear call vertical.",
        )

    def _random_ic(self, put_strikes: list, call_strikes: list) -> Order:
        """Random iron condor from available OTM strikes."""
        put_idx = self.rng.randint(0, len(put_strikes) - 2)
        call_idx = self.rng.randint(0, len(call_strikes) - 2)

        put_short = put_strikes[put_idx + 1]
        put_long = put_short - SPREAD_WIDTH
        call_short = call_strikes[call_idx]
        call_long = call_short + SPREAD_WIDTH

        return make_iron_condor(
            agent_id=self.agent_id,
            put_long=put_long,
            put_short=put_short,
            call_short=call_short,
            call_long=call_long,
            qty=1,
            thesis="Random entry — iron condor.",
        )

    def _random_iron_fly(self, atm: float, chain: ChainSnapshot) -> Order:
        """Iron fly centered at ATM."""
        return make_iron_fly(
            agent_id=self.agent_id,
            center_strike=atm,
            wing_width=SPREAD_WIDTH,
            qty=1,
            thesis="Random entry — iron fly at ATM.",
        )

    def _random_call_butterfly(self, call_strikes: list, atm: float) -> Order:
        """Random call butterfly near ATM."""
        # Center near ATM, wings at SPREAD_WIDTH on each side
        center = atm
        lower = center - SPREAD_WIDTH
        upper = center + SPREAD_WIDTH
        return make_call_butterfly(
            agent_id=self.agent_id,
            lower=lower,
            center=center,
            upper=upper,
            qty=1,
            thesis="Random entry — call butterfly.",
        )

    def _random_put_butterfly(self, put_strikes: list, atm: float) -> Order:
        """Random put butterfly near ATM."""
        center = atm
        lower = center - SPREAD_WIDTH
        upper = center + SPREAD_WIDTH
        return make_put_butterfly(
            agent_id=self.agent_id,
            lower=lower,
            center=center,
            upper=upper,
            qty=1,
            thesis="Random entry — put butterfly.",
        )
