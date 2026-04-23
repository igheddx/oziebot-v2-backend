from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

WARNING_DRAWDOWN_PCT = Decimal("0.05")
CRITICAL_DRAWDOWN_PCT = Decimal("0.10")
MONEY_STEP = Decimal("0.01")


def _clamp_non_negative(value: Decimal) -> Decimal:
    return value if value > 0 else Decimal("0")


def _clamp_pct(value: Decimal) -> Decimal:
    if value <= 0:
        return Decimal("0")
    if value >= 1:
        return Decimal("1")
    return value


def _money(value: Decimal) -> Decimal:
    return _clamp_non_negative(value).quantize(MONEY_STEP)


@dataclass(frozen=True)
class DynamicSizingInput:
    confidence: Decimal
    total_capital_usd: Decimal
    assigned_capital_usd: Decimal
    available_buying_power_usd: Decimal
    reserved_capital_usd: Decimal = Decimal("0")
    locked_capital_usd: Decimal = Decimal("0")
    current_position_usd: Decimal = Decimal("0")
    position_size_fraction: Decimal = Decimal("0")
    buy_amount_usd: Decimal = Decimal("0")
    min_trade_usd: Decimal = Decimal("0")
    max_trade_usd: Decimal = Decimal("0")
    max_position_usd: Decimal = Decimal("0")
    target_bucket_utilization_pct: Decimal = Decimal("0")
    dynamic_sizing_enabled: bool = True
    drawdown_size_reduction_enabled: bool = True
    drawdown_reduction_multiplier: Decimal = Decimal("0.75")
    realized_drawdown_pct: Decimal = Decimal("0")
    daily_loss_pct: Decimal = Decimal("0")
    token_policy_size_multiplier: Decimal = Decimal("1")
    token_policy_max_position_pct_override: Decimal | None = None


@dataclass(frozen=True)
class DynamicSizingResult:
    strategy_base_trade_usd: Decimal
    target_bucket_trade_gap_usd: Decimal
    target_bucket_utilization_pct: Decimal
    current_bucket_utilization_pct: Decimal
    confidence_scaled_trade_usd: Decimal
    drawdown_state: str
    drawdown_multiplier_applied: Decimal
    token_policy_size_multiplier: Decimal
    pre_cap_trade_usd: Decimal
    max_position_remaining_usd: Decimal | None
    token_policy_max_position_remaining_usd: Decimal | None
    max_allowed_trade_usd: Decimal
    final_trade_usd: Decimal
    reduction_reasons: tuple[str, ...]


def calculate_dynamic_trade_size(ctx: DynamicSizingInput) -> DynamicSizingResult:
    confidence = _clamp_pct(ctx.confidence)
    assigned_capital_usd = _clamp_non_negative(ctx.assigned_capital_usd)
    available_buying_power_usd = _clamp_non_negative(ctx.available_buying_power_usd)
    reserved_capital_usd = _clamp_non_negative(ctx.reserved_capital_usd)
    locked_capital_usd = _clamp_non_negative(ctx.locked_capital_usd)
    current_position_usd = _clamp_non_negative(ctx.current_position_usd)
    total_capital_usd = _clamp_non_negative(ctx.total_capital_usd)
    position_size_fraction = _clamp_non_negative(ctx.position_size_fraction)
    buy_amount_usd = _clamp_non_negative(ctx.buy_amount_usd)
    min_trade_usd = _clamp_non_negative(ctx.min_trade_usd)
    max_trade_usd = _clamp_non_negative(ctx.max_trade_usd)
    max_position_usd = _clamp_non_negative(ctx.max_position_usd)
    target_bucket_utilization_pct = _clamp_pct(ctx.target_bucket_utilization_pct)
    drawdown_reduction_multiplier = _clamp_pct(ctx.drawdown_reduction_multiplier)
    token_policy_size_multiplier = _clamp_pct(ctx.token_policy_size_multiplier)

    deployed_capital_usd = reserved_capital_usd + locked_capital_usd
    current_bucket_utilization_pct = (
        deployed_capital_usd / assigned_capital_usd
        if assigned_capital_usd > 0
        else Decimal("0")
    )
    target_bucket_trade_gap_usd = _clamp_non_negative(
        (assigned_capital_usd * target_bucket_utilization_pct) - deployed_capital_usd
    )

    strategy_base_trade_usd = Decimal("0")
    if buy_amount_usd > 0:
        strategy_base_trade_usd = buy_amount_usd
    elif assigned_capital_usd > 0 and position_size_fraction > 0:
        strategy_base_trade_usd = assigned_capital_usd * position_size_fraction
    elif max_position_usd > 0 and position_size_fraction > 0:
        strategy_base_trade_usd = max_position_usd * position_size_fraction

    desired_trade_usd = strategy_base_trade_usd
    reduction_reasons: list[str] = []
    if ctx.dynamic_sizing_enabled and target_bucket_trade_gap_usd > desired_trade_usd:
        desired_trade_usd = target_bucket_trade_gap_usd
        reduction_reasons.append("target_bucket_utilization_gap")

    confidence_scaled_trade_usd = desired_trade_usd * confidence
    if ctx.dynamic_sizing_enabled and confidence_scaled_trade_usd > 0 and min_trade_usd > 0:
        if confidence_scaled_trade_usd < min_trade_usd:
            confidence_scaled_trade_usd = min_trade_usd
            reduction_reasons.append("min_trade_floor")

    drawdown_state = "normal"
    drawdown_multiplier_applied = Decimal("1")
    observed_drawdown_pct = max(
        _clamp_non_negative(ctx.realized_drawdown_pct),
        _clamp_non_negative(ctx.daily_loss_pct),
    )
    if ctx.drawdown_size_reduction_enabled:
        if observed_drawdown_pct >= CRITICAL_DRAWDOWN_PCT:
            drawdown_state = "critical"
            drawdown_multiplier_applied = (
                drawdown_reduction_multiplier * drawdown_reduction_multiplier
            )
        elif observed_drawdown_pct >= WARNING_DRAWDOWN_PCT:
            drawdown_state = "elevated"
            drawdown_multiplier_applied = drawdown_reduction_multiplier
    pre_cap_trade_usd = confidence_scaled_trade_usd * drawdown_multiplier_applied
    if drawdown_multiplier_applied < Decimal("1"):
        reduction_reasons.append(f"drawdown_{drawdown_state}")

    max_allowed_trade_usd = available_buying_power_usd

    max_position_remaining_usd: Decimal | None = None
    if max_position_usd > 0:
        max_position_remaining_usd = _clamp_non_negative(
            max_position_usd - current_position_usd
        )
        max_allowed_trade_usd = min(max_allowed_trade_usd, max_position_remaining_usd)
        if pre_cap_trade_usd > max_position_remaining_usd:
            reduction_reasons.append("max_position_usd_cap")

    if max_trade_usd > 0:
        max_allowed_trade_usd = min(max_allowed_trade_usd, max_trade_usd)
        if pre_cap_trade_usd > max_trade_usd:
            reduction_reasons.append("max_trade_usd_cap")

    token_policy_max_position_remaining_usd: Decimal | None = None
    if (
        ctx.token_policy_max_position_pct_override is not None
        and ctx.token_policy_max_position_pct_override > 0
        and total_capital_usd > 0
    ):
        token_position_cap_usd = (
            total_capital_usd * _clamp_pct(ctx.token_policy_max_position_pct_override)
        )
        token_policy_max_position_remaining_usd = _clamp_non_negative(
            token_position_cap_usd - current_position_usd
        )
        max_allowed_trade_usd = min(
            max_allowed_trade_usd, token_policy_max_position_remaining_usd
        )
        if pre_cap_trade_usd > token_policy_max_position_remaining_usd:
            reduction_reasons.append("token_policy_position_cap")

    if pre_cap_trade_usd > available_buying_power_usd:
        reduction_reasons.append("bucket_buying_power_cap")

    final_trade_usd = min(pre_cap_trade_usd, max_allowed_trade_usd)
    return DynamicSizingResult(
        strategy_base_trade_usd=_money(strategy_base_trade_usd),
        target_bucket_trade_gap_usd=_money(target_bucket_trade_gap_usd),
        target_bucket_utilization_pct=target_bucket_utilization_pct.quantize(
            Decimal("0.0001")
        ),
        current_bucket_utilization_pct=_clamp_pct(
            current_bucket_utilization_pct
        ).quantize(Decimal("0.0001")),
        confidence_scaled_trade_usd=_money(confidence_scaled_trade_usd),
        drawdown_state=drawdown_state,
        drawdown_multiplier_applied=drawdown_multiplier_applied.quantize(
            Decimal("0.0001")
        ),
        token_policy_size_multiplier=token_policy_size_multiplier.quantize(
            Decimal("0.0001")
        ),
        pre_cap_trade_usd=_money(pre_cap_trade_usd),
        max_position_remaining_usd=(
            _money(max_position_remaining_usd)
            if max_position_remaining_usd is not None
            else None
        ),
        token_policy_max_position_remaining_usd=(
            _money(token_policy_max_position_remaining_usd)
            if token_policy_max_position_remaining_usd is not None
            else None
        ),
        max_allowed_trade_usd=_money(max_allowed_trade_usd),
        final_trade_usd=_money(final_trade_usd),
        reduction_reasons=tuple(dict.fromkeys(reduction_reasons)),
    )
