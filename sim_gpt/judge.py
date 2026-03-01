"""Objective judge for risk-adjusted scoring in the live game."""

from __future__ import annotations

from typing import Mapping


class Judge:
    """Scores only reward-vs-risk, not player style."""

    def __init__(self, drawdown_weight: float = 0.60):
        self.drawdown_weight = drawdown_weight

    def score(self, total_pnl: float, metrics: Mapping[str, float]) -> tuple[float, str]:
        equity = float(metrics.get("equity_pnl", 0.0))
        max_drawdown = float(metrics.get("max_drawdown", 0.0))
        current_drawdown = float(metrics.get("current_drawdown", 0.0))
        trade_rate = float(metrics.get("trade_rate", 1.0))
        consecutive_holds = int(metrics.get("consecutive_holds", 0))
        target_trade_rate = float(metrics.get("target_trade_rate", 0.90))
        risk_adjusted = float(
            metrics.get(
                "risk_adjusted",
                equity - (self.drawdown_weight * max_drawdown),
            )
        )

        # Map risk-adjusted dollars to a bounded 0..10 score.
        base = 5.0 + (risk_adjusted / 250.0)
        dd_penalty = min(3.0, current_drawdown / 200.0)
        round_term = max(-1.5, min(1.5, total_pnl / 200.0))
        hold_rate_penalty = min(2.5, max(0.0, target_trade_rate - trade_rate) * 8.0)
        hold_streak_penalty = min(1.5, max(0, consecutive_holds - 1) * 0.35)

        score = max(
            0.0,
            min(10.0, base - dd_penalty + round_term - hold_rate_penalty - hold_streak_penalty),
        )
        notes = (
            f"equity={equity:.2f}, max_dd={max_drawdown:.2f}, "
            f"curr_dd={current_drawdown:.2f}, risk_adj={risk_adjusted:.2f}, "
            f"trade_rate={trade_rate:.2f}, hold_streak={consecutive_holds}"
        )
        return round(score, 2), notes
