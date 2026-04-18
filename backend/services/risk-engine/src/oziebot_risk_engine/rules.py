from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from oziebot_common.fee_model import bps_to_decimal, is_trade_net_positive
from oziebot_domain.risk import RejectionReason


@dataclass
class RuleContext:
    signal: Any
    action: str
    trading_mode: str
    symbol: str
    suggested_size: Decimal
    mid_price: Decimal
    spread_pct: Decimal
    est_slippage_pct: Decimal
    max_spread_pct_allowed: Decimal
    max_slippage_pct_allowed: Decimal
    fee_pct: Decimal
    expected_profit_buffer_pct: Decimal
    expected_gross_edge_bps: int
    estimated_fee_bps: int
    estimated_slippage_bps: int
    estimated_total_cost_bps: int
    expected_net_edge_bps: int
    min_notional_per_trade: Decimal
    min_expected_edge_bps: int
    min_expected_net_profit_dollars: Decimal
    max_fee_percent_of_expected_profit: Decimal
    max_slippage_bps: int
    skip_trade_if_fee_too_high: bool
    execution_preference: str
    fallback_behavior: str
    maker_timeout_seconds: int
    limit_price_offset_bps: int
    now: datetime

    # Database-backed facts.
    platform_paused: bool
    entitled: bool
    token_platform_enabled: bool
    token_user_enabled: bool
    strategy_enabled: bool
    token_policy_admin_enabled: bool
    token_policy_status: str
    token_policy_reason: str | None
    token_policy_size_multiplier: Decimal
    bucket: dict[str, Any] | None
    total_capital_cents: int
    daily_loss_cents: int
    recent_loss_count: int
    cooldown_loss_threshold: int
    cooldown_until: datetime | None
    current_strategy_token_exposure_cents: int
    current_strategy_exposure_cents: int
    current_token_exposure_cents: int
    token_policy_max_position_cents: int
    max_strategy_exposure_cents: int
    max_token_exposure_cents: int
    global_daily_loss_limit_pct: Decimal
    stale_flags: dict[str, bool]
    critical_stale_flags: dict[str, bool]
    stale_ages: dict[str, float | None]


@dataclass
class RuleResult:
    rule_name: str
    verdict: str  # approve | reduce_size | reject
    reason: RejectionReason | None
    detail: str
    reduced_size: Decimal | None = None


class RiskRule:
    name: str = "base"

    def evaluate(self, ctx: RuleContext) -> RuleResult | None:
        raise NotImplementedError


def _notional_cents(ctx: RuleContext) -> Decimal:
    return (ctx.suggested_size * ctx.mid_price * Decimal("100")).quantize(Decimal("1"))


def _cents_to_dollars(value_cents: int | Decimal) -> Decimal:
    return Decimal(str(value_cents)) / Decimal("100")


class PlatformPauseRule(RiskRule):
    name = "platform_pause"

    def evaluate(self, ctx: RuleContext) -> RuleResult | None:
        if ctx.platform_paused:
            return RuleResult(
                self.name, "reject", RejectionReason.POLICY, "Platform trading paused"
            )
        return None


class SubscriptionEntitlementRule(RiskRule):
    name = "subscription_entitlement"

    def evaluate(self, ctx: RuleContext) -> RuleResult | None:
        if not ctx.entitled:
            return RuleResult(
                self.name,
                "reject",
                RejectionReason.POLICY,
                "No active strategy entitlement",
            )
        return None


class TokenAllowlistRule(RiskRule):
    name = "token_allowlist"

    def evaluate(self, ctx: RuleContext) -> RuleResult | None:
        if not ctx.token_platform_enabled:
            return RuleResult(
                self.name,
                "reject",
                RejectionReason.POLICY,
                "Token not platform-enabled",
            )
        return None


class UserTokenRule(RiskRule):
    name = "user_token_enabled"

    def evaluate(self, ctx: RuleContext) -> RuleResult | None:
        if not ctx.token_user_enabled:
            return RuleResult(
                self.name,
                "reject",
                RejectionReason.POLICY,
                "Token not enabled for user",
            )
        return None


class StrategyEnabledRule(RiskRule):
    name = "strategy_enabled"

    def evaluate(self, ctx: RuleContext) -> RuleResult | None:
        if not ctx.strategy_enabled:
            return RuleResult(
                self.name, "reject", RejectionReason.POLICY, "Strategy is disabled"
            )
        return None


class TokenStrategyPolicyRule(RiskRule):
    name = "token_strategy_policy"

    def evaluate(self, ctx: RuleContext) -> RuleResult | None:
        if not ctx.token_policy_admin_enabled:
            return RuleResult(
                self.name,
                "reject",
                RejectionReason.POLICY,
                "Token strategy policy disabled by admin",
            )
        if ctx.token_policy_status == "blocked":
            detail = (
                ctx.token_policy_reason or "Token strategy policy blocked this trade"
            )
            return RuleResult(self.name, "reject", RejectionReason.POLICY, detail)
        return None


class DiscouragedTokenPolicySizingRule(RiskRule):
    name = "token_strategy_discouraged"

    def evaluate(self, ctx: RuleContext) -> RuleResult | None:
        if ctx.action != "buy":
            return None
        if ctx.token_policy_status != "discouraged":
            return None
        if (
            ctx.token_policy_size_multiplier <= 0
            or ctx.token_policy_size_multiplier >= Decimal("1")
        ):
            return None
        reduced = (ctx.suggested_size * ctx.token_policy_size_multiplier).quantize(
            Decimal("0.00000001")
        )
        return RuleResult(
            self.name,
            "reduce_size",
            RejectionReason.POLICY,
            ctx.token_policy_reason or "Reduced by discouraged token strategy policy",
            reduced_size=max(reduced, Decimal("0")),
        )


class TokenStrategyPositionOverrideRule(RiskRule):
    name = "token_strategy_position_override"

    def evaluate(self, ctx: RuleContext) -> RuleResult | None:
        if (
            ctx.action != "buy"
            or ctx.token_policy_max_position_cents <= 0
            or ctx.mid_price <= 0
        ):
            return None
        notional = _notional_cents(ctx)
        projected = Decimal(str(ctx.current_strategy_token_exposure_cents)) + notional
        limit = Decimal(str(ctx.token_policy_max_position_cents))
        if projected <= limit:
            return None
        allowed_notional = max(
            Decimal("0"),
            limit - Decimal(str(ctx.current_strategy_token_exposure_cents)),
        )
        if allowed_notional <= 0:
            return RuleResult(
                self.name,
                "reject",
                RejectionReason.POSITION_CAP,
                "Token strategy position override cap reached",
            )
        reduced = (_cents_to_dollars(allowed_notional) / ctx.mid_price).quantize(
            Decimal("0.00000001")
        )
        return RuleResult(
            self.name,
            "reduce_size",
            RejectionReason.POSITION_CAP,
            "Reduced by token strategy position override",
            reduced_size=max(reduced, Decimal("0")),
        )


class CapitalBucketRule(RiskRule):
    name = "capital_bucket"

    def evaluate(self, ctx: RuleContext) -> RuleResult | None:
        if ctx.bucket is None:
            return RuleResult(
                self.name,
                "reject",
                RejectionReason.POLICY,
                "No strategy capital bucket",
            )
        buying_power = int(ctx.bucket["available_buying_power_cents"])
        notional = int(_notional_cents(ctx))
        if notional > buying_power:
            if buying_power <= 0:
                return RuleResult(
                    self.name,
                    "reject",
                    RejectionReason.LIMIT_EXCEEDED,
                    "No buying power",
                )
            reduced = (_cents_to_dollars(buying_power) / ctx.mid_price).quantize(
                Decimal("0.00000001")
            )
            return RuleResult(
                self.name,
                "reduce_size",
                RejectionReason.LIMIT_EXCEEDED,
                "Reduced by bucket buying power",
                reduced_size=max(reduced, Decimal("0")),
            )
        return None


class MaxPerTradeRiskRule(RiskRule):
    name = "max_per_trade_risk"

    def __init__(self, max_pct: float):
        self._max_pct = Decimal(str(max_pct))

    def evaluate(self, ctx: RuleContext) -> RuleResult | None:
        if ctx.bucket is None:
            return None
        buying_power = Decimal(str(ctx.bucket["available_buying_power_cents"]))
        if buying_power <= 0:
            return None
        max_notional = buying_power * self._max_pct
        notional = _notional_cents(ctx)
        if notional > max_notional:
            reduced = (_cents_to_dollars(max_notional) / ctx.mid_price).quantize(
                Decimal("0.00000001")
            )
            return RuleResult(
                self.name,
                "reduce_size",
                RejectionReason.LIMIT_EXCEEDED,
                "Per-trade risk limit hit",
                reduced_size=max(reduced, Decimal("0")),
            )
        return None


class MaxPositionSizeRule(RiskRule):
    name = "max_position_size"

    def __init__(self, max_cents: int):
        self._max_cents = max_cents

    def evaluate(self, ctx: RuleContext) -> RuleResult | None:
        if ctx.bucket is None:
            return None
        locked = int(ctx.bucket["locked_capital_cents"])
        notional = int(_notional_cents(ctx))
        projected = locked + notional
        if projected > self._max_cents:
            allowed = max(0, self._max_cents - locked)
            if allowed <= 0:
                return RuleResult(
                    self.name,
                    "reject",
                    RejectionReason.POSITION_CAP,
                    "Max position size reached",
                )
            reduced = (_cents_to_dollars(allowed) / ctx.mid_price).quantize(
                Decimal("0.00000001")
            )
            return RuleResult(
                self.name,
                "reduce_size",
                RejectionReason.POSITION_CAP,
                "Reduced by max position size",
                reduced_size=max(reduced, Decimal("0")),
            )
        return None


class MaxStrategyAllocationRule(RiskRule):
    name = "max_strategy_allocation"

    def __init__(self, max_pct: float):
        self._max_pct = Decimal(str(max_pct))

    def evaluate(self, ctx: RuleContext) -> RuleResult | None:
        if ctx.bucket is None:
            return None
        assigned = Decimal(str(ctx.bucket["assigned_capital_cents"]))
        notional = _notional_cents(ctx)
        max_allowed = assigned * self._max_pct
        if notional > max_allowed:
            reduced = (_cents_to_dollars(max_allowed) / ctx.mid_price).quantize(
                Decimal("0.00000001")
            )
            return RuleResult(
                self.name,
                "reduce_size",
                RejectionReason.LIMIT_EXCEEDED,
                "Reduced by strategy allocation cap",
                reduced_size=max(reduced, Decimal("0")),
            )
        return None


class MaxTokenConcentrationRule(RiskRule):
    name = "max_token_concentration"

    def __init__(self, max_pct: float):
        self._max_pct = Decimal(str(max_pct))

    def evaluate(self, ctx: RuleContext) -> RuleResult | None:
        if ctx.total_capital_cents <= 0:
            return None
        notional = _notional_cents(ctx)
        ratio = notional / Decimal(str(ctx.total_capital_cents))
        if ratio > self._max_pct:
            allowed_notional = Decimal(str(ctx.total_capital_cents)) * self._max_pct
            reduced = (_cents_to_dollars(allowed_notional) / ctx.mid_price).quantize(
                Decimal("0.00000001")
            )
            return RuleResult(
                self.name,
                "reduce_size",
                RejectionReason.LIMIT_EXCEEDED,
                "Reduced by token concentration cap",
                reduced_size=max(reduced, Decimal("0")),
            )
        return None


class MaxStrategyExposureRule(RiskRule):
    name = "max_strategy_exposure"

    def evaluate(self, ctx: RuleContext) -> RuleResult | None:
        if (
            ctx.action != "buy"
            or ctx.max_strategy_exposure_cents <= 0
            or ctx.mid_price <= 0
        ):
            return None
        notional = _notional_cents(ctx)
        projected = Decimal(str(ctx.current_strategy_exposure_cents)) + notional
        limit = Decimal(str(ctx.max_strategy_exposure_cents))
        if projected <= limit:
            return None
        allowed_notional = max(
            Decimal("0"), limit - Decimal(str(ctx.current_strategy_exposure_cents))
        )
        if allowed_notional <= 0:
            return RuleResult(
                self.name,
                "reject",
                RejectionReason.LIMIT_EXCEEDED,
                "Strategy exposure cap reached",
            )
        reduced = (_cents_to_dollars(allowed_notional) / ctx.mid_price).quantize(
            Decimal("0.00000001")
        )
        return RuleResult(
            self.name,
            "reduce_size",
            RejectionReason.LIMIT_EXCEEDED,
            "Reduced by strategy exposure cap",
            reduced_size=max(reduced, Decimal("0")),
        )


class MaxTokenExposureRule(RiskRule):
    name = "max_token_exposure"

    def evaluate(self, ctx: RuleContext) -> RuleResult | None:
        if (
            ctx.action != "buy"
            or ctx.max_token_exposure_cents <= 0
            or ctx.mid_price <= 0
        ):
            return None
        notional = _notional_cents(ctx)
        projected = Decimal(str(ctx.current_token_exposure_cents)) + notional
        limit = Decimal(str(ctx.max_token_exposure_cents))
        if projected <= limit:
            return None
        allowed_notional = max(
            Decimal("0"), limit - Decimal(str(ctx.current_token_exposure_cents))
        )
        if allowed_notional <= 0:
            return RuleResult(
                self.name,
                "reject",
                RejectionReason.LIMIT_EXCEEDED,
                "Token exposure cap reached",
            )
        reduced = (_cents_to_dollars(allowed_notional) / ctx.mid_price).quantize(
            Decimal("0.00000001")
        )
        return RuleResult(
            self.name,
            "reduce_size",
            RejectionReason.LIMIT_EXCEEDED,
            "Reduced by token exposure cap",
            reduced_size=max(reduced, Decimal("0")),
        )


class MaxDailyLossRule(RiskRule):
    name = "max_daily_loss"

    def __init__(self, max_daily_loss_cents: int):
        self._max_daily_loss_cents = max_daily_loss_cents

    def evaluate(self, ctx: RuleContext) -> RuleResult | None:
        if ctx.daily_loss_cents >= self._max_daily_loss_cents:
            return RuleResult(
                self.name,
                "reject",
                RejectionReason.DRAWDOWN,
                f"Daily loss limit reached ({ctx.daily_loss_cents} >= {self._max_daily_loss_cents})",
            )
        return None


class GlobalDailyLossGuardRule(RiskRule):
    name = "global_daily_loss_guard"

    def evaluate(self, ctx: RuleContext) -> RuleResult | None:
        if ctx.global_daily_loss_limit_pct <= 0 or ctx.total_capital_cents <= 0:
            return None
        loss_pct = (Decimal(str(ctx.daily_loss_cents)) * Decimal("100")) / Decimal(
            str(ctx.total_capital_cents)
        )
        if loss_pct >= ctx.global_daily_loss_limit_pct:
            return RuleResult(
                self.name,
                "reject",
                RejectionReason.DRAWDOWN,
                (
                    "Global daily loss guard active "
                    f"({loss_pct:.2f}% >= {ctx.global_daily_loss_limit_pct:.2f}%)"
                ),
            )
        return None


class CooldownAfterLossesRule(RiskRule):
    name = "cooldown_after_losses"

    def evaluate(self, ctx: RuleContext) -> RuleResult | None:
        if (
            ctx.recent_loss_count >= ctx.cooldown_loss_threshold
            and ctx.cooldown_until is not None
        ):
            if ctx.now < ctx.cooldown_until:
                return RuleResult(
                    self.name,
                    "reject",
                    RejectionReason.DRAWDOWN,
                    f"Cooldown active until {ctx.cooldown_until.isoformat()}",
                )
        return None


class StaleDataRule(RiskRule):
    name = "stale_data"

    def evaluate(self, ctx: RuleContext) -> RuleResult | None:
        if any(ctx.critical_stale_flags.values()):
            return RuleResult(
                self.name,
                "reject",
                RejectionReason.POLICY,
                (
                    "Critically stale market data: "
                    f"critical={ctx.critical_stale_flags}, stale={ctx.stale_flags}, ages={ctx.stale_ages}"
                ),
            )
        return None


class ExecutionQualityRule(RiskRule):
    name = "execution_quality"

    def evaluate(self, ctx: RuleContext) -> RuleResult | None:
        if ctx.action != "buy":
            return None
        if (
            ctx.max_spread_pct_allowed > 0
            and ctx.spread_pct > ctx.max_spread_pct_allowed
        ):
            return RuleResult(
                self.name,
                "reject",
                RejectionReason.POLICY,
                f"Spread too wide ({ctx.spread_pct:.6f} > {ctx.max_spread_pct_allowed:.6f})",
            )
        if (
            ctx.max_slippage_pct_allowed > 0
            and ctx.est_slippage_pct > ctx.max_slippage_pct_allowed
        ):
            return RuleResult(
                self.name,
                "reject",
                RejectionReason.POLICY,
                (
                    "Estimated slippage too high "
                    f"({ctx.est_slippage_pct:.6f} > {ctx.max_slippage_pct_allowed:.6f})"
                ),
            )
        if ctx.fee_pct > 0 and ctx.expected_profit_buffer_pct > 0 and ctx.mid_price > 0:
            notional = ctx.suggested_size * ctx.mid_price
            estimated_fees = notional * ctx.fee_pct
            expected_profit_buffer = notional * ctx.expected_profit_buffer_pct
            if estimated_fees >= expected_profit_buffer:
                return RuleResult(
                    self.name,
                    "reject",
                    RejectionReason.POLICY,
                    (
                        "Estimated fees exceed expected profit buffer "
                        f"({estimated_fees:.8f} >= {expected_profit_buffer:.8f})"
                    ),
                )
        return None


class FeeEconomicsRule(RiskRule):
    name = "fee_economics"

    def evaluate(self, ctx: RuleContext) -> RuleResult | None:
        if ctx.action != "buy":
            return None
        notional = ctx.suggested_size * ctx.mid_price
        if ctx.min_notional_per_trade > 0 and notional < ctx.min_notional_per_trade:
            return RuleResult(
                self.name,
                "reject",
                RejectionReason.POLICY,
                (
                    f"Trade notional too small (${notional:.2f} < "
                    f"${ctx.min_notional_per_trade:.2f})"
                ),
            )
        if not ctx.skip_trade_if_fee_too_high:
            return None
        if (
            ctx.max_slippage_bps > 0
            and ctx.estimated_slippage_bps > ctx.max_slippage_bps
        ):
            return RuleResult(
                self.name,
                "reject",
                RejectionReason.POLICY,
                (
                    "Estimated slippage exceeds configured cap "
                    f"({ctx.estimated_slippage_bps}bps > {ctx.max_slippage_bps}bps)"
                ),
            )
        if not is_trade_net_positive(
            ctx.expected_gross_edge_bps,
            ctx.estimated_total_cost_bps,
            ctx.min_expected_edge_bps,
        ):
            return RuleResult(
                self.name,
                "reject",
                RejectionReason.POLICY,
                (
                    "Expected net edge below threshold "
                    f"({ctx.expected_net_edge_bps}bps < {ctx.min_expected_edge_bps}bps)"
                ),
            )
        expected_gross_profit = notional * bps_to_decimal(ctx.expected_gross_edge_bps)
        estimated_cost = notional * bps_to_decimal(ctx.estimated_total_cost_bps)
        expected_net_profit = expected_gross_profit - estimated_cost
        if (
            ctx.min_expected_net_profit_dollars > 0
            and expected_net_profit < ctx.min_expected_net_profit_dollars
        ):
            return RuleResult(
                self.name,
                "reject",
                RejectionReason.POLICY,
                (
                    "Expected net profit too low "
                    f"(${expected_net_profit:.2f} < ${ctx.min_expected_net_profit_dollars:.2f})"
                ),
            )
        if (
            expected_gross_profit > 0
            and ctx.max_fee_percent_of_expected_profit > 0
            and estimated_cost
            > (expected_gross_profit * ctx.max_fee_percent_of_expected_profit)
        ):
            allowed_cost = (
                expected_gross_profit * ctx.max_fee_percent_of_expected_profit
            )
            return RuleResult(
                self.name,
                "reject",
                RejectionReason.POLICY,
                (
                    "Estimated execution cost too high for expected edge "
                    f"(${estimated_cost:.2f} > ${allowed_cost:.2f})"
                ),
            )
        return None


def default_rules(settings) -> list[RiskRule]:
    return [
        PlatformPauseRule(),
        SubscriptionEntitlementRule(),
        TokenAllowlistRule(),
        UserTokenRule(),
        StrategyEnabledRule(),
        TokenStrategyPolicyRule(),
        DiscouragedTokenPolicySizingRule(),
        CapitalBucketRule(),
        MaxPerTradeRiskRule(settings.risk_max_per_trade_risk_pct),
        MaxPositionSizeRule(settings.risk_max_position_size_cents),
        MaxStrategyAllocationRule(settings.risk_max_strategy_allocation_pct),
        MaxTokenConcentrationRule(settings.risk_max_token_concentration_pct),
        TokenStrategyPositionOverrideRule(),
        MaxStrategyExposureRule(),
        MaxTokenExposureRule(),
        MaxDailyLossRule(settings.risk_max_daily_loss_cents),
        GlobalDailyLossGuardRule(),
        CooldownAfterLossesRule(),
        StaleDataRule(),
        FeeEconomicsRule(),
        ExecutionQualityRule(),
    ]
