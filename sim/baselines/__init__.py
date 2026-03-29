"""Baseline (mechanical) trading bots for benchmarking (v14)."""

from sim.baselines.hold_cash import HoldCash
from sim.baselines.narrow_ic import NarrowIC
from sim.baselines.wide_ic import WideIC
from sim.baselines.directional_put import DirectionalPut
from sim.baselines.iron_fly import IronFly

ALL_BASELINES = [NarrowIC, WideIC, DirectionalPut, IronFly, HoldCash]

__all__ = [
    "BaseBaseline", "HoldCash", "NarrowIC", "WideIC",
    "DirectionalPut", "IronFly", "ALL_BASELINES",
]
