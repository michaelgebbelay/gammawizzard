from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts" / "trade" / "TqqqTrend"))

import overlay_state as overlay_module  # noqa: E402
from overlay_state import (  # noqa: E402
    Ts6OverlayState,
    compute_overlay_state,
    advance_state,
    default_state,
    load_state,
    save_state,
)


def _advance(
    state: Ts6OverlayState,
    signal_date: str,
    baseline_state: str,
    adj_close: float | None,
    *,
    enabled: bool = False,
) -> Ts6OverlayState:
    return advance_state(
        state,
        signal_date=signal_date,
        baseline_state=baseline_state,
        adj_close=adj_close,
        enabled=enabled,
    )


def test_baseline_bil_sets_inactive_state_with_no_peak() -> None:
    state = _advance(default_state(), "2026-05-01", "BIL", 100.0)
    assert state.baseline_state == "BIL"
    assert state.overlay_state == "INACTIVE"
    assert state.qqq_peak_adj is None
    assert state.last_decision == "STAY_BIL"
    assert state.last_signal_date == "2026-05-01"


def test_baseline_riskon_enters_active_and_initializes_peak() -> None:
    state = _advance(default_state(), "2026-05-01", "RISKON", 100.0)
    assert state.overlay_state == "ACTIVE"
    assert state.qqq_peak_adj == pytest.approx(100.0)
    assert state.last_decision == "ENTER_TQQQ"


def test_new_high_updates_peak() -> None:
    state = _advance(default_state(), "2026-05-01", "RISKON", 100.0)
    state = _advance(state, "2026-05-02", "RISKON", 104.0)
    assert state.overlay_state == "ACTIVE"
    assert state.qqq_peak_adj == pytest.approx(104.0)
    assert state.last_decision == "HOLD_TQQQ"


def test_drop_inside_band_does_not_stop() -> None:
    state = _advance(default_state(), "2026-05-01", "RISKON", 100.0)
    state = _advance(state, "2026-05-02", "RISKON", 94.01)
    assert state.overlay_state == "ACTIVE"
    assert state.qqq_peak_adj == pytest.approx(100.0)
    assert state.last_decision == "HOLD_TQQQ"


def test_drop_at_threshold_fires_stop() -> None:
    state = _advance(default_state(), "2026-05-01", "RISKON", 100.0)
    state = _advance(state, "2026-05-02", "RISKON", 94.0)
    assert state.overlay_state == "BLOCKED"
    assert state.qqq_peak_adj is None
    assert state.stopped_on_date == "2026-05-02"
    assert state.stopped_on_adj_close == pytest.approx(94.0)
    assert state.last_decision == "EXIT_TO_BIL"


def test_after_stop_baseline_riskon_remains_blocked_in_bil() -> None:
    state = _advance(default_state(), "2026-05-01", "RISKON", 100.0)
    state = _advance(state, "2026-05-02", "RISKON", 94.0)
    state = _advance(state, "2026-05-05", "RISKON", 95.0)
    assert state.overlay_state == "BLOCKED"
    assert state.last_decision == "STAY_BIL"
    assert state.stopped_on_date == "2026-05-02"


def test_blocked_state_ignores_rallies_until_baseline_exit() -> None:
    state = _advance(default_state(), "2026-05-01", "RISKON", 100.0)
    state = _advance(state, "2026-05-02", "RISKON", 94.0)
    state = _advance(state, "2026-05-05", "RISKON", 110.0)
    assert state.overlay_state == "BLOCKED"
    assert state.qqq_peak_adj is None
    assert state.last_decision == "STAY_BIL"


def test_baseline_exit_resets_block() -> None:
    state = _advance(default_state(), "2026-05-01", "RISKON", 100.0)
    state = _advance(state, "2026-05-02", "RISKON", 94.0)
    state = _advance(state, "2026-05-05", "BIL", 93.0)
    assert state.overlay_state == "INACTIVE"
    assert state.qqq_peak_adj is None
    assert state.stopped_on_date is None
    assert state.last_decision == "STAY_BIL"


def test_next_riskon_episode_starts_fresh_peak() -> None:
    state = _advance(default_state(), "2026-05-01", "RISKON", 100.0)
    state = _advance(state, "2026-05-02", "RISKON", 94.0)
    state = _advance(state, "2026-05-05", "BIL", 93.0)
    state = _advance(state, "2026-05-06", "RISKON", 97.0)
    assert state.overlay_state == "ACTIVE"
    assert state.qqq_peak_adj == pytest.approx(97.0)
    assert state.last_decision == "ENTER_TQQQ"


def test_same_signal_date_is_idempotent() -> None:
    state = _advance(default_state(), "2026-05-01", "RISKON", 100.0)
    again = _advance(state, "2026-05-01", "RISKON", 90.0)
    assert again == state


def test_missing_adjusted_close_preserves_prior_state() -> None:
    state = _advance(default_state(), "2026-05-01", "RISKON", 100.0)
    again = _advance(state, "2026-05-02", "RISKON", None)
    assert again == state


def test_adjusted_close_basis_controls_stop_trigger() -> None:
    state = _advance(default_state(), "2026-05-01", "RISKON", 100.0)
    # A raw close of 94.00 would stop here, but the adjusted close basis is 94.01.
    state = _advance(state, "2026-05-02", "RISKON", 94.01)
    assert state.overlay_state == "ACTIVE"
    assert state.last_decision == "HOLD_TQQQ"


def test_state_roundtrip_save_and_load(tmp_path: Path) -> None:
    path = tmp_path / "ts6_overlay_state.json"
    state = _advance(default_state(enabled=True), "2026-05-01", "RISKON", 101.25, enabled=True)
    save_state(state, path)
    loaded = load_state(path, enabled=True)
    assert loaded.overlay_state == "ACTIVE"
    assert loaded.enabled is True
    assert loaded.qqq_peak_adj == pytest.approx(101.25)


def test_enabled_overlay_requires_existing_state_file(tmp_path: Path) -> None:
    missing = tmp_path / "missing.json"
    with pytest.raises(RuntimeError):
        load_state(missing, enabled=True)


def test_disabled_overlay_uses_fallback_when_state_file_is_corrupt(tmp_path: Path) -> None:
    path = tmp_path / "corrupt.json"
    path.write_text("{not-json", encoding="utf-8")
    state, warnings, degraded, returned_path = compute_overlay_state(
        signal_date="2026-05-01",
        baseline_state="RISKON",
        adj_close=101.0,
        path=path,
        enabled=False,
        persist=True,
    )
    assert returned_path == path
    assert degraded is True
    assert warnings
    assert state.enabled is False
    assert state.overlay_state == "ACTIVE"
    assert state.qqq_peak_adj == pytest.approx(101.0)


def test_disabled_overlay_save_failure_warns_but_does_not_raise(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "ts6_overlay_state.json"

    def boom(*args, **kwargs):
        raise RuntimeError("disk full")

    monkeypatch.setattr(overlay_module, "save_state", boom)
    state, warnings, degraded, returned_path = compute_overlay_state(
        signal_date="2026-05-01",
        baseline_state="RISKON",
        adj_close=100.0,
        path=path,
        enabled=False,
        persist=True,
    )
    assert returned_path == path
    assert degraded is True
    assert any("disk full" in warning for warning in warnings)
    assert state.enabled is False
    assert state.overlay_state == "ACTIVE"
    assert state.qqq_peak_adj == pytest.approx(100.0)
