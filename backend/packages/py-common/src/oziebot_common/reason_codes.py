from __future__ import annotations

from collections.abc import Iterable

CANONICAL_REASON_CODES = {
    "allocation_unavailable",
    "cooldown_active",
    "execution_validation_failed",
    "existing_open_position",
    "insufficient_buying_power",
    "insufficient_confidence",
    "insufficient_volume",
    "liquidity_window_closed",
    "max_exposure_reached",
    "paper_only_mode",
    "policy_blocked",
    "spread_too_wide",
    "stale_market_data",
    "strategy_disabled",
    "token_disabled",
    "unspecified_blocker",
}

_ALIASES = {
    "allocation_cap": "allocation_unavailable",
    "below_min_confidence": "insufficient_confidence",
    "blocked_token": "policy_blocked",
    "capital_limit": "allocation_unavailable",
    "confidence_gate": "insufficient_confidence",
    "cooldown": "cooldown_active",
    "cooldown_after_losses": "cooldown_active",
    "duplicate_worker_execution": "cooldown_active",
    "execution_quality": "spread_too_wide",
    "fee_economics": "spread_too_wide",
    "insufficient_allocation": "insufficient_buying_power",
    "limit_exceeded": "max_exposure_reached",
    "liquidity_hours": "liquidity_window_closed",
    "max_daily_loss_pct": "max_exposure_reached",
    "max_open_positions": "max_exposure_reached",
    "max_position_usd": "max_exposure_reached",
    "missing_price_hint": "execution_validation_failed",
    "non_finite_notional": "execution_validation_failed",
    "non_finite_price_hint": "execution_validation_failed",
    "non_finite_quantity": "execution_validation_failed",
    "notional_rounded_to_zero": "execution_validation_failed",
    "paper_only": "paper_only_mode",
    "position_cap": "max_exposure_reached",
    "position_limit": "max_exposure_reached",
    "quantity_precision_exceeded": "execution_validation_failed",
    "signal_size_positive": "execution_validation_failed",
    "skipped_due_to_interval": "cooldown_active",
    "token_strategy_policy": "policy_blocked",
    "volume_confirmation_failed": "insufficient_volume",
}


def normalize_reason_code(
    reason_code: str | None,
    *,
    reason_detail: str | None = None,
) -> str:
    raw_code = _normalize_token(reason_code)
    if raw_code in CANONICAL_REASON_CODES:
        return raw_code
    if raw_code in _ALIASES:
        return _ALIASES[raw_code]

    if raw_code:
        for prefix, mapped in (
            ("max_open_positions", "max_exposure_reached"),
            ("max_position_usd", "max_exposure_reached"),
            ("max_daily_loss_pct", "max_exposure_reached"),
            ("token_strategy_policy", "policy_blocked"),
            ("execution_validation", "execution_validation_failed"),
        ):
            if raw_code.startswith(prefix):
                return mapped

    text = " ".join(part for part in (reason_code, reason_detail) if part)
    normalized_text = _normalize_text(text)
    return _map_text_to_code(normalized_text, fallback=raw_code or "unspecified_blocker")


def summarize_rejection_reason(reason_code: str | None) -> str:
    normalized = normalize_reason_code(reason_code)
    if normalized == "insufficient_confidence":
        return "confidence"
    if normalized == "insufficient_volume":
        return "volume"
    if normalized in {"allocation_unavailable", "insufficient_buying_power"}:
        return "allocation"
    if normalized in {"policy_blocked", "token_disabled", "strategy_disabled"}:
        return "token_strategy_policy"
    if normalized in {"cooldown_active", "paper_only_mode"}:
        return "cooldown"
    if normalized == "liquidity_window_closed":
        return "liquidity_hours"
    if normalized in {"spread_too_wide", "stale_market_data", "max_exposure_reached"}:
        return "risk_engine"
    return normalized


def top_reason_rows(reason_counts: dict[str, int] | Iterable[tuple[str, int]], *, limit: int = 3) -> list[dict[str, int | str]]:
    items = (
        reason_counts.items()
        if isinstance(reason_counts, dict)
        else reason_counts
    )
    rows = sorted(
        ((reason, int(count)) for reason, count in items if int(count) > 0),
        key=lambda item: (-item[1], item[0]),
    )
    return [{"reason": reason, "count": count} for reason, count in rows[:limit]]


def _map_text_to_code(text: str, *, fallback: str) -> str:
    if any(
        token in text
        for token in (
            "token strategy blocked",
            "blocked by token strategy policy",
            "token_strategy_policy",
            " policy ",
            " policy_",
            "policy blocked",
        )
    ):
        return "policy_blocked"
    if "token disabled" in text or "token_disabled" in text:
        return "token_disabled"
    if "strategy disabled" in text or "strategy_disabled" in text:
        return "strategy_disabled"
    if "paper only" in text or "paper_only" in text:
        return "paper_only_mode"
    if "existing open position" in text or "existing_open_position" in text:
        return "existing_open_position"
    if any(
        token in text
        for token in (
            "max open positions",
            "max_open_positions",
            "max position usd",
            "max_position_usd",
            "position limit",
            "position_limit",
            "drawdown",
            "max daily loss",
            "max_daily_loss",
            "limit exceeded",
            "limit_exceeded",
        )
    ):
        return "max_exposure_reached"
    if any(
        token in text
        for token in (
            "buying power",
            "insufficient allocation",
            "insufficient_allocation",
            "available cash",
        )
    ):
        return "insufficient_buying_power"
    if "allocation" in text or "capital" in text or "sizing blocked" in text:
        return "allocation_unavailable"
    if "confidence" in text:
        return "insufficient_confidence"
    if "volume" in text:
        return "insufficient_volume"
    if any(token in text for token in ("stale", "market data", "freshness")):
        return "stale_market_data"
    if any(
        token in text
        for token in (
            "outside liquid hours",
            "outside liquid-hours",
            "liquidity hours",
            "liquidity_hours",
            "liquid-hours",
        )
    ):
        return "liquidity_window_closed"
    if any(token in text for token in ("spread", "slippage", "fee economics", "fee_economics")):
        return "spread_too_wide"
    if any(
        token in text
        for token in (
            "cooldown",
            "skipped due to interval",
            "skipped_due_to_interval",
            "duplicate worker execution",
            "duplicate_worker_execution",
        )
    ):
        return "cooldown_active"
    if any(
        token in text
        for token in (
            "quantity",
            "notional",
            "precision",
            "price hint",
            "execution_validation",
            "signal size",
        )
    ):
        return "execution_validation_failed"
    if fallback in CANONICAL_REASON_CODES:
        return fallback
    return fallback or "unspecified_blocker"


def _normalize_token(value: str | None) -> str:
    return (value or "").strip().lower().replace(" ", "_").replace("-", "_")


def _normalize_text(value: str) -> str:
    normalized = f" {value.lower().replace('-', ' ').replace('_', ' ')} "
    return " ".join(normalized.split())
