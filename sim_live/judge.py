"""Judge for process-oriented scoring in the live game."""

from __future__ import annotations

from sim_live.types import Decision, PublicSnapshot, SideAction


class Judge:
    """Rule-based judge for transparent, stable scoring."""

    def score(self, snapshot: PublicSnapshot, decision: Decision, total_pnl: float) -> tuple[float, str]:
        outcome = self._outcome_score(total_pnl)
        discipline = self._discipline_score(decision)
        regime_fit = self._regime_fit(snapshot, decision)
        total = (0.45 * outcome) + (0.30 * discipline) + (0.25 * regime_fit)
        notes = (
            f"outcome={outcome:.1f}/10, discipline={discipline:.1f}/10, "
            f"regime_fit={regime_fit:.1f}/10"
        )
        return round(total, 2), notes

    def _outcome_score(self, pnl: float) -> float:
        if pnl >= 300:
            return 10.0
        if pnl >= 150:
            return 8.5
        if pnl >= 0:
            return 7.0
        if pnl >= -150:
            return 4.5
        if pnl >= -300:
            return 2.5
        return 1.0

    def _discipline_score(self, decision: Decision) -> float:
        active = decision.active_sides()
        width_penalty = 0.0
        if decision.put_width == 10:
            width_penalty += 0.8
        if decision.call_width == 10:
            width_penalty += 0.8
        size_penalty = max(0.0, (decision.size - 1) * 0.7)

        if active == 0:
            base = 7.5
        elif active == 1:
            base = 8.0
        else:
            base = 6.5
        return max(0.0, min(10.0, base - width_penalty - size_penalty))

    def _regime_fit(self, snapshot: PublicSnapshot, decision: Decision) -> float:
        # High VIX1D: prefer conservative (one side or none).
        vix1d = snapshot.vix_one * 100.0 if snapshot.vix_one < 1.0 else snapshot.vix_one
        active = decision.active_sides()

        if vix1d >= 20:
            if active == 0:
                return 9.0
            if active == 1:
                return 8.0
            return 5.0

        if vix1d <= 12:
            if active == 2 and decision.put_action == SideAction.SELL and decision.call_action == SideAction.SELL:
                return 8.5
            if active == 1:
                return 7.0
            return 6.0

        # Mid-vol default
        if active == 1:
            return 8.0
        if active == 2:
            return 7.0
        return 6.5

