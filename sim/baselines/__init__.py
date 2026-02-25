"""Baseline (mechanical) trading bots for benchmarking."""

from sim.baselines.hold_cash import HoldCash
from sim.baselines.mechanical_ic import MechanicalIC
from sim.baselines.random_entry import RandomEntry
from sim.baselines.regime_bot import RegimeBot

ALL_BASELINES = [MechanicalIC, RandomEntry, HoldCash, RegimeBot]

__all__ = ["BaseBaseline", "MechanicalIC", "RandomEntry", "HoldCash", "RegimeBot",
           "ALL_BASELINES"]
