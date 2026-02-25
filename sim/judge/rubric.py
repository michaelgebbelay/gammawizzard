"""Rubric dataclass and weight evolution logic."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict

from sim.judge.prompts.rubric_template import DEFAULT_WEIGHTS


@dataclass
class Rubric:
    """Weighted scoring rubric for the judge."""

    weights: Dict[str, int] = field(default_factory=lambda: dict(DEFAULT_WEIGHTS))
    rationale: str = "Default rubric weights."

    def weighted_total(self, scores: Dict[str, int]) -> float:
        """Compute weighted total score (0-100 scale).

        Each dimension is scored 0-10, weighted by rubric percentages.
        """
        total = 0.0
        for dim, weight in self.weights.items():
            score = scores.get(dim, 0)
            total += score * (weight / 100.0)
        return round(total, 2)

    def format_for_prompt(self) -> str:
        """Format rubric weights for inclusion in judge prompts."""
        lines = []
        for dim, weight in self.weights.items():
            label = dim.replace("_", " ").title()
            lines.append(f"- {label}: {weight}%")
        return "\n".join(lines)

    def evolve(self, session_id: int, performance_data: dict) -> "Rubric":
        """Evolve rubric weights based on aggregate performance.

        Adaptive track only. Shifts weight toward dimensions where agents
        show the most variance (where differentiation matters most).

        Args:
            session_id: Current session for tracking.
            performance_data: Dict with per-dimension score variance across agents.

        Returns:
            New Rubric with adjusted weights.
        """
        if not performance_data:
            return Rubric(weights=dict(self.weights), rationale="No data to evolve.")

        # Compute variance per dimension
        variances = {}
        for dim in self.weights:
            scores = performance_data.get(dim, [])
            if len(scores) < 2:
                variances[dim] = 0
            else:
                mean = sum(scores) / len(scores)
                variances[dim] = sum((s - mean) ** 2 for s in scores) / len(scores)

        total_var = sum(variances.values())
        if total_var == 0:
            return Rubric(weights=dict(self.weights),
                          rationale=f"Session {session_id}: no variance — weights unchanged.")

        # Shift weights toward high-variance dimensions (where agents differ most)
        # Blend: 70% current weights + 30% variance-proportional
        new_weights = {}
        for dim in self.weights:
            var_share = variances[dim] / total_var * 100
            blended = 0.7 * self.weights[dim] + 0.3 * var_share
            new_weights[dim] = max(5, round(blended))  # minimum 5% per dimension

        # Normalize to sum to 100
        total = sum(new_weights.values())
        if total != 100:
            diff = 100 - total
            # Add/subtract from largest weight
            largest = max(new_weights, key=new_weights.get)
            new_weights[largest] += diff

        rationale = (
            f"Session {session_id}: evolved weights based on score variance. "
            f"Highest variance: {max(variances, key=variances.get)}."
        )

        return Rubric(weights=new_weights, rationale=rationale)
