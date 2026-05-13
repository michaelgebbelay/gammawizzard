from __future__ import annotations

import importlib.util
import json
from pathlib import Path


def _load_module():
    path = Path(__file__).resolve().parents[1] / "TT" / "Script" / "LeoProfit" / "place.py"
    spec = importlib.util.spec_from_file_location("leo_place", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def _seed_live_env(monkeypatch, tmp_path, *, step_wait: str = "1"):
    result_path = tmp_path / "leo_result.json"
    env = {
        "TT_ACCOUNT_NUMBER": "5WT20360",
        "LEO_SIDE": "DEBIT",
        "LEO_QTY": "1",
        "LEO_CALL_MULT": "1",
        "LEO_DRY_RUN": "false",
        "LEO_STEP_WAIT": step_wait,
        "LEO_CANCEL_SETTLE": "0",
        "LEO_MAX_LADDER": "1",
        "LEO_LADDER_STEP": "0.05",
        "LEO_MAX_DEBIT": "2.20",
        "LEO_OCC_BUY_PUT": "SPXW  260513P07365000",
        "LEO_OCC_SELL_PUT": "SPXW  260513P07360000",
        "LEO_OCC_SELL_CALL": "SPXW  260513C07435000",
        "LEO_OCC_BUY_CALL": "SPXW  260513C07430000",
        "LEO_RESULT_PATH": str(result_path),
    }
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    return result_path


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


def test_main_stops_after_filled_status_without_qty(monkeypatch, tmp_path):
    mod = _load_module()
    result_path = _seed_live_env(monkeypatch, tmp_path, step_wait="1")

    posts = []
    deletes = []

    monkeypatch.setattr(mod, "nbbo_debit_synth", lambda legs: (1.4, 2.3, 1.85))
    monkeypatch.setattr(mod, "order_symbol", lambda osi: osi)

    def fake_post(_c, _url, payload, tag=""):
        oid = f"oid-{len(posts) + 1}"
        posts.append({"oid": oid, "payload": payload, "tag": tag})
        return {"id": oid}

    monkeypatch.setattr(mod, "post_with_retry", fake_post)
    monkeypatch.setattr(mod, "parse_order_id", lambda resp: resp["id"])
    monkeypatch.setattr(mod, "get_status", lambda *_args, **_kwargs: {"data": {"status": "Filled"}})
    monkeypatch.setattr(mod, "delete_with_retry", lambda *_args, **_kwargs: deletes.append(True) or True)

    assert mod.main() == 0

    result = json.loads(result_path.read_text())
    assert len(posts) == 1
    assert deletes == []
    assert result["filled_quantity"] == 1
    assert result["attempts"] == [
        {
            "cycle": 1,
            "filled_qty": 1,
            "order_id": "oid-1",
            "price": 1.85,
            "qty": 1,
            "status": "FILLED",
        }
    ]


def test_main_halts_when_cancel_cannot_be_confirmed(monkeypatch, tmp_path):
    mod = _load_module()
    result_path = _seed_live_env(monkeypatch, tmp_path, step_wait="0")

    posts = []
    delete_calls = []

    monkeypatch.setattr(mod, "nbbo_debit_synth", lambda legs: (1.4, 2.3, 1.85))
    monkeypatch.setattr(mod, "order_symbol", lambda osi: osi)
    monkeypatch.setattr(mod.time, "sleep", lambda _secs: None)

    def fake_post(_c, _url, payload, tag=""):
        oid = f"oid-{len(posts) + 1}"
        posts.append({"oid": oid, "payload": payload, "tag": tag})
        return {"id": oid}

    monkeypatch.setattr(mod, "post_with_retry", fake_post)
    monkeypatch.setattr(mod, "parse_order_id", lambda resp: resp["id"])
    monkeypatch.setattr(mod, "get_status", lambda *_args, **_kwargs: {"data": {"status": "Live"}})
    monkeypatch.setattr(mod, "delete_with_retry", lambda *_args, **_kwargs: delete_calls.append(True) or False)

    assert mod.main() == 0

    result = json.loads(result_path.read_text())
    assert len(posts) == 1
    assert delete_calls == [True]
    assert result["filled_quantity"] == 0
    assert result["halt_reason"] == "CANCEL_FAILED_STATUS_LIVE"
    assert result["attempts"] == [
        {
            "cycle": 1,
            "filled_qty": 0,
            "order_id": "oid-1",
            "price": 1.85,
            "qty": 1,
            "status": "LIVE",
        }
    ]
