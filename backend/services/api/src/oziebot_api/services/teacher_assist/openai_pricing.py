from __future__ import annotations

MODEL_PRICING_PER_MILLION_TOKENS: dict[str, tuple[float, float]] = {
    "gpt-4.1-mini": (0.40, 1.60),
    "gpt-4.1": (2.00, 8.00),
    "gpt-4o-mini": (0.15, 0.60),
    # Gemini models (via Google AI Studio)
    "gemini-2.5-flash": (0.30, 2.50),
    "gemini-2.0-flash-001": (0.075, 0.30),
    "gemini-2.0-flash": (0.075, 0.30),
    "gemini-2.0-flash-lite": (0.0375, 0.15),
    "gemini-1.5-flash": (0.075, 0.30),
    "gemini-1.5-pro": (1.25, 5.00),
}


def estimate_openai_cost_cents(model_name: str, *, input_tokens: int, output_tokens: int) -> int:
    pricing = MODEL_PRICING_PER_MILLION_TOKENS.get(model_name)
    if pricing is None:
        return 0
    input_cost_usd = (input_tokens / 1_000_000) * pricing[0]
    output_cost_usd = (output_tokens / 1_000_000) * pricing[1]
    return max(0, round((input_cost_usd + output_cost_usd) * 100))
