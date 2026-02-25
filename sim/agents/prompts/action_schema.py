"""JSON schema for agent trade/hold decisions."""

# The agent must respond with exactly this JSON structure.
# No prose, no explanation — just the JSON object.

ACTION_SCHEMA = {
    "type": "object",
    "properties": {
        "action": {
            "type": "string",
            "enum": ["trade", "hold"],
            "description": "Whether to place a trade or hold cash this session.",
        },
        "structure": {
            "type": "string",
            "enum": [
                "bull_put_vertical",
                "bear_call_vertical",
                "iron_condor",
                "iron_fly",
                "call_butterfly",
                "put_butterfly",
            ],
            "description": "The option structure to trade (required if action=trade).",
        },
        "strikes": {
            "type": "object",
            "description": "Strike prices for the structure (required if action=trade).",
            "properties": {
                "short_put": {"type": "number"},
                "long_put": {"type": "number"},
                "short_call": {"type": "number"},
                "long_call": {"type": "number"},
                "center": {"type": "number"},
                "lower": {"type": "number"},
                "upper": {"type": "number"},
            },
        },
        "quantity": {
            "type": "integer",
            "minimum": 1,
            "maximum": 5,
            "description": "Number of contracts (default 1).",
        },
        "thesis": {
            "type": "string",
            "maxLength": 200,
            "description": "Brief reasoning for this decision (1-2 sentences).",
        },
    },
    "required": ["action", "thesis"],
}

# Human-readable schema description for the system prompt
SCHEMA_DESCRIPTION = """You must respond with ONLY a JSON object (no markdown, no code fences, no explanation).

If you decide to TRADE:
{
  "action": "trade",
  "structure": "<one of: bull_put_vertical, bear_call_vertical, iron_condor, iron_fly, call_butterfly, put_butterfly>",
  "strikes": {
    // For verticals:
    "short_put": <strike>, "long_put": <strike>          // bull_put_vertical
    "short_call": <strike>, "long_call": <strike>        // bear_call_vertical
    // For iron condor / iron fly:
    "long_put": <strike>, "short_put": <strike>, "short_call": <strike>, "long_call": <strike>
    // For butterflies:
    "lower": <strike>, "center": <strike>, "upper": <strike>
  },
  "quantity": 1,
  "thesis": "Brief reasoning (1-2 sentences)."
}

If you decide to HOLD:
{
  "action": "hold",
  "thesis": "Brief reasoning why you're sitting out."
}

CONSTRAINTS:
- All spreads must be exactly 5 points wide ($5 between adjacent strikes).
- Strikes must exist in the provided chain data.
- Quantity: 1-5 contracts.
- You MUST pick strikes from the chain. Do not invent strikes."""
