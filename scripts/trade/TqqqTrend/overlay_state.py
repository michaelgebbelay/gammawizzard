from __future__ import annotations

import json
import math
import os
import tempfile
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


HERE = Path(__file__).resolve().parent
DEFAULT_STATE_PATH = HERE / "state" / "ts6_overlay_state.json"
DEFAULT_OVERLAY_NAME = "TS_6"
DEFAULT_SIGNAL_SOURCE = "adjusted_qqq_close"
DEFAULT_EXECUTION_SOURCE = "schwab_tqqq_bil"

VALID_BASELINE_STATES = {"RISKON", "BIL"}
VALID_OVERLAY_STATES = {"ACTIVE", "BLOCKED", "INACTIVE"}
VALID_DECISIONS = {"HOLD_TQQQ", "EXIT_TO_BIL", "STAY_BIL", "ENTER_TQQQ"}
TRUE_VALUES = {"1", "true", "yes", "y", "on"}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _is_finite_positive(value: float | None) -> bool:
    return value is not None and math.isfinite(value) and value > 0


def parse_bool_env(name: str, default: bool = False) -> bool:
    raw = (os.environ.get(name) or "").strip().lower()
    if not raw:
        return default
    return raw in TRUE_VALUES


def overlay_enabled_from_env(default: bool = False) -> bool:
    return parse_bool_env("TQQQ_TS6_ENABLED", default=default)


def state_path_from_env() -> Path:
    raw = (os.environ.get("TQQQ_TS6_STATE_PATH") or "").strip()
    return Path(raw) if raw else DEFAULT_STATE_PATH


@dataclass(frozen=True)
class Ts6OverlayState:
    schema_version: int = 1
    overlay_name: str = DEFAULT_OVERLAY_NAME
    enabled: bool = False
    signal_source: str = DEFAULT_SIGNAL_SOURCE
    execution_source: str = DEFAULT_EXECUTION_SOURCE
    baseline_state: str = "BIL"
    overlay_state: str = "INACTIVE"
    qqq_peak_adj: float | None = None
    stop_threshold_pct: float = 0.06
    stopped_on_date: str | None = None
    stopped_on_adj_close: float | None = None
    last_signal_date: str | None = None
    last_adj_qqq_close: float | None = None
    last_decision: str = "STAY_BIL"
    updated_at_utc: str | None = None

    def target_sleeve(self) -> str:
        return "TQQQ" if self.overlay_state == "ACTIVE" else "BIL"

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


def default_state(enabled: bool = False) -> Ts6OverlayState:
    return Ts6OverlayState(enabled=enabled, updated_at_utc=_utc_now_iso())


def _state_from_payload(payload: dict[str, Any], enabled: bool) -> Ts6OverlayState:
    base = default_state(enabled=enabled).to_payload()
    base.update(payload or {})
    base["enabled"] = enabled
    return Ts6OverlayState(**base)


def validate_state(state: Ts6OverlayState) -> None:
    if state.overlay_name != DEFAULT_OVERLAY_NAME:
        raise ValueError(f"unexpected overlay_name={state.overlay_name!r}")
    if state.signal_source != DEFAULT_SIGNAL_SOURCE:
        raise ValueError(f"unexpected signal_source={state.signal_source!r}")
    if state.execution_source != DEFAULT_EXECUTION_SOURCE:
        raise ValueError(f"unexpected execution_source={state.execution_source!r}")
    if state.baseline_state not in VALID_BASELINE_STATES:
        raise ValueError(f"invalid baseline_state={state.baseline_state!r}")
    if state.overlay_state not in VALID_OVERLAY_STATES:
        raise ValueError(f"invalid overlay_state={state.overlay_state!r}")
    if state.last_decision not in VALID_DECISIONS:
        raise ValueError(f"invalid last_decision={state.last_decision!r}")
    if not _is_finite_positive(state.stop_threshold_pct):
        raise ValueError(f"invalid stop_threshold_pct={state.stop_threshold_pct!r}")
    if state.overlay_state == "ACTIVE":
        if not _is_finite_positive(state.qqq_peak_adj):
            raise ValueError("ACTIVE state requires qqq_peak_adj")
        if state.stopped_on_date is not None or state.stopped_on_adj_close is not None:
            raise ValueError("ACTIVE state cannot retain stop trigger fields")
    else:
        if state.qqq_peak_adj is not None:
            raise ValueError(f"{state.overlay_state} state cannot retain qqq_peak_adj")
    if state.overlay_state == "BLOCKED":
        if state.stopped_on_date is None or not _is_finite_positive(state.stopped_on_adj_close):
            raise ValueError("BLOCKED state requires stop trigger metadata")
    if state.overlay_state == "INACTIVE":
        if state.baseline_state != "BIL":
            raise ValueError("INACTIVE state requires baseline_state=BIL")
        if state.stopped_on_date is not None or state.stopped_on_adj_close is not None:
            raise ValueError("INACTIVE state cannot retain stop trigger metadata")
    if state.last_adj_qqq_close is not None and not _is_finite_positive(state.last_adj_qqq_close):
        raise ValueError("last_adj_qqq_close must be positive when present")


def _hydrate_from_env_if_needed(path: Path) -> None:
    raw_env = os.environ.get("TQQQ_TS6_STATE_JSON")
    if path.exists() or not raw_env:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(raw_env, encoding="utf-8")


def load_state(path: Path | None = None, *, enabled: bool | None = None) -> Ts6OverlayState:
    state_path = path or state_path_from_env()
    overlay_enabled = overlay_enabled_from_env() if enabled is None else enabled
    _hydrate_from_env_if_needed(state_path)

    if not state_path.exists():
        if overlay_enabled:
            raise RuntimeError(
                f"TS_6 overlay is enabled but state file is missing: {state_path}"
            )
        return default_state(enabled=overlay_enabled)

    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(f"failed to load TS_6 state from {state_path}: {exc}") from exc

    state = _state_from_payload(payload, enabled=overlay_enabled)
    validate_state(state)
    return state


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f"{path.name}.", suffix=".tmp", dir=path.parent)
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
        try:
            dir_fd = os.open(path.parent, os.O_DIRECTORY)
        except Exception:
            dir_fd = None
        if dir_fd is not None:
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
    finally:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)


def save_state(state: Ts6OverlayState, path: Path | None = None) -> None:
    validate_state(state)
    state_path = path or state_path_from_env()
    payload = replace(state, updated_at_utc=_utc_now_iso()).to_payload()
    _atomic_write_json(state_path, payload)


def baseline_target_sleeve(baseline_state: str) -> str:
    if baseline_state not in VALID_BASELINE_STATES:
        raise ValueError(f"invalid baseline_state={baseline_state!r}")
    return "TQQQ" if baseline_state == "RISKON" else "BIL"


def effective_target_sleeve(state: Ts6OverlayState) -> str:
    return state.target_sleeve() if state.enabled else baseline_target_sleeve(state.baseline_state)


def compute_overlay_state(
    *,
    signal_date: str | None,
    baseline_state: str,
    adj_close: float | None,
    path: Path | None = None,
    enabled: bool | None = None,
    persist: bool = True,
) -> tuple[Ts6OverlayState, list[str], bool, Path]:
    state_path = path or state_path_from_env()
    overlay_enabled = overlay_enabled_from_env() if enabled is None else enabled
    warnings: list[str] = []
    degraded = False

    try:
        state = load_state(state_path, enabled=overlay_enabled)
        state = advance_state(
            state,
            signal_date=signal_date,
            baseline_state=baseline_state,
            adj_close=adj_close,
            enabled=overlay_enabled,
        )
        if persist:
            save_state(state, state_path)
        return state, warnings, degraded, state_path
    except Exception as exc:
        if overlay_enabled:
            raise
        degraded = True
        warnings.append(f"TS_6 shadow state degraded while disabled: {exc}")

    fallback = default_state(enabled=False)
    try:
        fallback = advance_state(
            fallback,
            signal_date=signal_date,
            baseline_state=baseline_state,
            adj_close=adj_close,
            enabled=False,
        )
    except Exception as exc:
        warnings.append(f"TS_6 shadow fallback failed: {exc}")
        fallback = default_state(enabled=False)
    return fallback, warnings, degraded, state_path


def advance_state(
    state: Ts6OverlayState,
    *,
    signal_date: str | None,
    baseline_state: str,
    adj_close: float | None,
    enabled: bool | None = None,
) -> Ts6OverlayState:
    if baseline_state not in VALID_BASELINE_STATES:
        raise ValueError(f"invalid baseline_state={baseline_state!r}")

    next_state = replace(state, enabled=state.enabled if enabled is None else enabled)

    if not signal_date or not _is_finite_positive(adj_close):
        return next_state

    if next_state.last_signal_date == signal_date:
        return next_state
    if next_state.last_signal_date and signal_date < next_state.last_signal_date:
        raise RuntimeError(
            f"refusing out-of-order TS_6 update: {signal_date} < {next_state.last_signal_date}"
        )

    previous_target = next_state.target_sleeve()
    stop_triggered = False
    overlay_state = next_state.overlay_state
    peak = next_state.qqq_peak_adj
    stopped_on_date = next_state.stopped_on_date
    stopped_on_adj_close = next_state.stopped_on_adj_close
    decision = next_state.last_decision

    if baseline_state == "BIL":
        overlay_state = "INACTIVE"
        peak = None
        stopped_on_date = None
        stopped_on_adj_close = None
        decision = "EXIT_TO_BIL" if previous_target == "TQQQ" else "STAY_BIL"
    elif overlay_state == "INACTIVE":
        overlay_state = "ACTIVE"
        peak = float(adj_close)
        stopped_on_date = None
        stopped_on_adj_close = None
        decision = "ENTER_TQQQ"
    elif overlay_state == "ACTIVE":
        peak = max(float(peak if peak is not None else adj_close), float(adj_close))
        if peak > 0 and (float(adj_close) / peak - 1.0) <= -next_state.stop_threshold_pct:
            overlay_state = "BLOCKED"
            peak = None
            stopped_on_date = signal_date
            stopped_on_adj_close = float(adj_close)
            decision = "EXIT_TO_BIL"
            stop_triggered = True
        else:
            decision = "HOLD_TQQQ"
            stopped_on_date = None
            stopped_on_adj_close = None
    elif overlay_state == "BLOCKED":
        decision = "STAY_BIL"
    else:
        raise RuntimeError(f"unexpected overlay_state={overlay_state!r}")

    if baseline_state == "RISKON" and overlay_state == "BLOCKED" and not stop_triggered:
        decision = "STAY_BIL"

    out = replace(
        next_state,
        baseline_state=baseline_state,
        overlay_state=overlay_state,
        qqq_peak_adj=peak,
        stopped_on_date=stopped_on_date,
        stopped_on_adj_close=stopped_on_adj_close,
        last_signal_date=signal_date,
        last_adj_qqq_close=float(adj_close),
        last_decision=decision,
        updated_at_utc=_utc_now_iso(),
    )
    validate_state(out)
    return out
