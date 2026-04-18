from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

from oziebot_domain.risk import RejectionReason


@dataclass
class RuleContext:
    signal: Any
    trading_mode: str
    symbol: str
    suggested_size: Decimal
    mid_price: Decimal
    spread_pct: Decimal
    est_slippage_pct: Decimal
    now: datetime

    # Database-backed facts.
    platform_paused: bool
    entitled: bool
    token_platform_enabled: bool
    token_user_enabled: bool
    strategy_enabled: bool
    bucket: dict[str, Any] | None
    total_capital_cents: int
    daily_loss_cents: int
    recent_loss_count: int
    cooldown_until: datetime | None
    stale_flags: dict[str, bool]


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


class PlatformPauseRule(RiskRule):
    name = "platform_pause"

    def evaluate(self, ctx: RuleContext) -> RuleResult | None:
        if ctx.platform_paused:
            return RuleResult(self.name, "reject", RejectionReason.POLICY, "Platform trading paused")
        return None


class SubscriptionEntitlementRule(RiskRule):
    name = "subscription_entitlement"

    def evaluate(self, ctx: RuleContext) -> RuleResult | None:
        if not ctx.entitled:
            return RuleResult(self.name, "reject", RejectionReason.POLICY, "No active strategy entitlement")
        return None


class TokenAllowlistRule(RiskRule):
    name = "token_allowlist"

    def evaluate(self, ctx: RuleContext) -> RuleResult | None:
        if not ctx.token_platform_enabled:
            return RuleResult(self.name, "reject", RejectionReason.POLICY, "Token not platform-enabled")
        return None


class UserTokenRule(RiskRule):
    name = "user_token_enabled"

    def evaluate(self, ctx: RuleContext) -> RuleResult | None:
        if not ctx.token_user_enabled:
            return RuleResult(self.name, "reject", RejectionReason.POLICY, "Token not enabled for user")
        return None


class StrategyEnabledRule(RiskRule):
    name = "strategy_enabled"

    def evaluate(self, ctx: RuleContext) -> RuleResult | None:
        if not ctx.strategy_enabled:
            return RuleResult(self.name, "reject", RejectionReason.POLICY, "Strategy is disabled")
        return None


class CapitalBucketRule(RiskRule):
    name = "capital_bucket"

    def evaluate(self, ctx: RuleContext) -> RuleResult | None:
        if ctx.bucket is None:
            return RuleResult(self.name, "reject", RejectionReason.POLICY, "No strategy capital bucket")
        buying_power = int(ctx.bucket["available_buying_power_cents"])
        notional = int((ctx.suggested_size * ctx.mid_price).quantize(Decimal("1")))
        if notional > buying_power:
            if buying_power <= 0:
                return RuleResult(self.name, "reject", RejectionReason.LIMIT_EXCEEDED, "No buying power")
            reduced = (Decimal(buying_power) / ctx.mid_price).quantize(Decimal("0.00000001"))
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
        notional = ctx.suggested_size * ctx.mid_price
        if notional > max_notional:
            reduced = (max_notional / ctx.mid_price).quantize(Decimal("0.00000001"))
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
        notional = int((ctx.suggested_size * ctx.mid_price).quantize(Decimal("1")))
        projected = locked + notional
        if projected > self._max_cents:
            allowed = max(0, self._max_cents - locked)
            if allowed <= 0:
                return RuleResult(self.name, "reject", RejectionReason.POSITION_CAP, "Max position size reached")
            reduced = (Decimal(allowed) / ctx.mid_price).quantize(Decimal("0.00000001"))
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
        notional = ctx.suggested_size * ctx.mid_price
        max_allowed = assigned * self._max_pct
        if notional > max_allowed:
            reduced = (max_allowed / ctx.mid_price).quantize(Decimal("0.00000001"))
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
        notional = ctx.suggested_size * ctx.mid_price
        ratio = notional / Decimal(str(ctx.total_capital_cents))
        if ratio > self._max_pct:
            allowed_notional = Decimal(str(ctx.total_capital_cents)) * self._max_pct
            reduced = (allowed_notional / ctx.mid_price).quantize(Decimal("0.00000001"))
            return RuleResult(
                self.name,
                "reduce_size",
                RejectionReason.LIMIT_EXCEEDED,
                "Reduced by token concentration cap",
                reduced_size=max(reduced, Decimal("0")),
            )
        return None


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


class CooldownAfterLossesRule(RiskRule):
    name = "cooldown_after_losses"

    def __init__(self, loss_count: int, cooldown_minutes: int):
        self._loss_count = loss_count
        self._cooldown_minutes = cooldown_minutes

    def evaluate(self, ctx: RuleContext) -> RuleResult | None:
        if ctx.recent_loss_count >= self._loss_count and ctx.cooldown_until is not None:
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
        if any(ctx.stale_flags.values()):
            return RuleResult(self.name, "reject", RejectionReason.POLICY, f"Stale market data: {ctx.stale_flags}")
        return None


class AbnormalSpreadRule(RiskRule):
    name = "abnormal_spread_slippage"

    def __init__(self, max_spread_pct: float, max_slippage_pct: float):
        self._max_spread_pct = Decimal(str(max_spread_pct))
        self._max_slippage_pct = Decimal(str(max_slippage_pct))

    def evaluate(self, ctx: RuleContext) -> RuleResult | None:
        if ctx.spread_pct > self._max_spread_pct:
            return RuleResult(
                self.name,
                "reject",
                RejectionReason.POLICY,
                f"Spread too wide ({ctx.spread_pct:.6f} > {self._max_spread_pct:.6f})",
            )
        if ctx.est_slippage_pct > self._max_slippage_pct:
            return RuleResult(
                self.name,
                "reject",
                RejectionReason.POLICY,
                f"Estimated slippage too high ({ctx.est_slippage_pct:.6f} > {self._max_slippage_pct:.6f})",
            )
        return None


def default_rules(settings) -> list[RiskRule]:
    return [
        PlatformPauseRule(),
        SubscriptionEntitlementRule(),
        TokenAllowlistRule(),
        UserTokenRule(),
        StrategyEnabledRule(),
        CapitalBucketRule(),
        MaxPerTradeRiskRule(settings.risk_max_per_trade_risk_pct),
        MaxPositionSizeRule(settings.risk_max_position_size_cents),
        MaxStrategyAllocationRule(settings.risk_max_strategy_allocation_pct),
        MaxTokenConcentrationRule(settings.risk_max_token_concentration_pct),
        MaxDailyLossRule(settings.risk_max_daily_loss_cents),
        CooldownAfterLossesRule(settings.risk_cooldown_loss_count, settings.risk_cooldown_minutes),
        StaleDataRule(),
        AbnormalSpreadRule(settings.risk_max_spread_pct, settings.risk_max_slippage_pct),
    ]
