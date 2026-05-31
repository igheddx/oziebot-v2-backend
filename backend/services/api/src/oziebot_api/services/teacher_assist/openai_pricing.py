from __future__ import annotations

MODEL_PRICING_PER_MILLION_TOKENS: dict[str, tuple[float, float]] = {
    "gpt-4.1-mini": (0.40, 1.60),
    "gpt-4.1": (2.00, 8.00),
    "gpt-4o-mini": (0.15, 0.60),
}


def estimate_openai_cost_cents(model_name: str, *, input_tokens: int, output_tokens: int) -> int:
    pricing = MODEL_PRICING_PER_MILLION_TOKENS.get(model_name)
    if pricing is None:
        return 0
    input_cost_usd = (input_tokens / 1_000_000) * pricing[0]
    output_cost_usd = (output_tokens / 1_000_000) * pricing[1]
    return max(0, round((input_cost_usd + output_cost_usd) * 100))
