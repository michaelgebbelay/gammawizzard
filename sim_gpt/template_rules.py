"""Template-level risk and payoff scaling rules."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TemplateRule:
    risk_per_width_dollars: float = 100.0
    pnl_scale_per_width: float = 0.2  # width * 0.2 == width/5


DEFAULT_RULE = TemplateRule()

RULES_BY_TEMPLATE: dict[str, TemplateRule] = {
    "flat": TemplateRule(risk_per_width_dollars=0.0, pnl_scale_per_width=0.0),
    "put_sell_5": DEFAULT_RULE,
    "call_sell_5": DEFAULT_RULE,
    "put_buy_5": DEFAULT_RULE,
    "call_buy_5": DEFAULT_RULE,
    "both_sell_5": DEFAULT_RULE,
    "both_buy_5": DEFAULT_RULE,
    "both_sell_10": DEFAULT_RULE,
    "both_buy_10": DEFAULT_RULE,
    "risk_guard_flat": TemplateRule(risk_per_width_dollars=0.0, pnl_scale_per_width=0.0),
}


def template_rule(template_id: str) -> TemplateRule:
    return RULES_BY_TEMPLATE.get(template_id, DEFAULT_RULE)

