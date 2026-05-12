from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_module():
    path = Path(__file__).resolve().parents[1] / "TT" / "Script" / "LeoProfit" / "place.py"
    spec = importlib.util.spec_from_file_location("leo_place", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def test_build_debit_rungs_walks_to_cap():
    mod = _load_module()
    rungs, cap = mod.build_debit_rungs(1.90, 3.10, 2.20, 0.05)
    assert cap == 2.20
    assert rungs == [1.9, 1.95, 2.0, 2.05, 2.1, 2.15, 2.2]


def test_build_debit_rungs_never_starts_above_cap():
    mod = _load_module()
    rungs, cap = mod.build_debit_rungs(2.35, 3.10, 2.20, 0.05)
    assert cap == 2.20
    assert rungs == [2.2]


def test_build_credit_rungs_walks_down_to_floor():
    mod = _load_module()
    rungs, floor = mod.build_credit_rungs(2.00, 1.60, 1.80, 0.05)
    assert floor == 1.80
    assert rungs == [2.0, 1.95, 1.9, 1.85, 1.8]


def test_build_credit_rungs_never_starts_below_floor():
    mod = _load_module()
    rungs, floor = mod.build_credit_rungs(1.70, 1.60, 1.80, 0.05)
    assert floor == 1.80
    assert rungs == [1.8]


def test_build_credit_rungs_respects_higher_bid():
    mod = _load_module()
    rungs, floor = mod.build_credit_rungs(2.10, 1.95, 1.80, 0.05)
    assert floor == 1.95
    assert rungs == [2.1, 2.05, 2.0, 1.95]
