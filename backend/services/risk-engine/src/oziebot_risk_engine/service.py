from __future__ import annotations

import json
import logging
import uuid
from collections import Counter
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import create_engine, text

from oziebot_common.queues import QueueNames, notification_event_to_json, push_json
from oziebot_common.token_policy import resolve_effective_token_policy
from oziebot_domain.events import NotificationEvent, NotificationEventType
from oziebot_domain.intents import TradeIntent
from oziebot_domain.risk import RejectionReason, RiskDecision, RiskOutcome
from oziebot_domain.signal_pipeline import StrategySignalEvent
from oziebot_domain.strategy import SignalType
from oziebot_domain.trading import Instrument, OrderType, Quantity, Side
from oziebot_domain.trading_mode import TradingMode
from oziebot_risk_engine.config import Settings
from oziebot_risk_engine.rules import RuleContext, RuleResult, default_rules

log = logging.getLogger("risk-engine.service")


class RiskEngineService:
    def __init__(self, settings: Settings, redis_client):
        self._settings = settings
        self._redis = redis_client
        self._engine = create_engine(settings.database_url)
        self._rules = default_rules(settings)
        self._paper_relaxed = {
            x.strip() for x in settings.risk_relaxed_paper_rules.split(",") if x.strip()
        }
        self._metrics: Counter[str] = Counter()
        self._rejection_reasons: Counter[str] = Counter()

    def evaluate(
        self, signal: StrategySignalEvent, trace_id: str
    ) -> tuple[RiskDecision, TradeIntent | None]:
        now = datetime.now(UTC)
        facts = self._load_facts(signal, now)
        signal = self._apply_market_data_degradation(signal, facts)
        self._maybe_emit_global_loss_alert(signal, facts, now, trace_id)
        size = Decimal(str(signal.suggested_size))
        if signal.action.value == "hold":
            decision = RiskDecision(
                outcome=RiskOutcome.APPROVE,
                approved=True,
                signal_id=signal.signal_id,
                run_id=signal.run_id,
                user_id=signal.user_id,
                strategy_name=signal.strategy_name,
                symbol=signal.symbol,
                original_size=str(signal.suggested_size),
                final_size="0",
                trading_mode=signal.trading_mode,
                reason=None,
                detail=None,
                rules_evaluated=[],
                trace_id=trace_id,
            )
            self._persist_risk_event(signal, decision)
            self._record_metric()
            self._log_decision(signal, decision)
            return decision, None
        if size <= 0:
            decision = RiskDecision(
                outcome=RiskOutcome.REJECT,
                approved=False,
                signal_id=signal.signal_id,
                run_id=signal.run_id,
                user_id=signal.user_id,
                strategy_name=signal.strategy_name,
                symbol=signal.symbol,
                original_size=str(signal.suggested_size),
                final_size="0",
                trading_mode=signal.trading_mode,
                reason=RejectionReason.POLICY,
                detail="Signal size must be positive",
                rules_evaluated=["signal_size_positive"],
                trace_id=trace_id,
            )
            self._persist_risk_event(signal, decision)
            self._record_metric(rejected=True, rejection_reason="signal_size_positive")
            self._log_decision(signal, decision)
            return decision, None

        ctx = RuleContext(
            signal=signal,
            action=signal.action.value,
            trading_mode=signal.trading_mode.value,
            symbol=signal.symbol,
            suggested_size=size,
            mid_price=facts["mid_price"],
            spread_pct=facts["spread_pct"],
            est_slippage_pct=facts["est_slippage_pct"],
            max_spread_pct_allowed=facts["max_spread_pct_allowed"],
            max_slippage_pct_allowed=facts["max_slippage_pct_allowed"],
            fee_pct=facts["fee_pct"],
            expected_profit_buffer_pct=facts["expected_profit_buffer_pct"],
            now=now,
            platform_paused=facts["platform_paused"],
            entitled=facts["entitled"],
            token_platform_enabled=facts["token_platform_enabled"],
            token_user_enabled=facts["token_user_enabled"],
            strategy_enabled=facts["strategy_enabled"],
            token_policy_admin_enabled=facts["token_policy_admin_enabled"],
            token_policy_status=facts["token_policy_status"],
            token_policy_reason=facts["token_policy_reason"],
            token_policy_size_multiplier=facts["token_policy_size_multiplier"],
            bucket=facts["bucket"],
            total_capital_cents=facts["total_capital_cents"],
            daily_loss_cents=facts["daily_loss_cents"],
            recent_loss_count=facts["recent_loss_count"],
            cooldown_loss_threshold=facts["cooldown_loss_threshold"],
            cooldown_until=facts["cooldown_until"],
            current_strategy_token_exposure_cents=facts[
                "current_strategy_token_exposure_cents"
            ],
            current_strategy_exposure_cents=facts["current_strategy_exposure_cents"],
            current_token_exposure_cents=facts["current_token_exposure_cents"],
            token_policy_max_position_cents=facts["token_policy_max_position_cents"],
            max_strategy_exposure_cents=facts["max_strategy_exposure_cents"],
            max_token_exposure_cents=facts["max_token_exposure_cents"],
            global_daily_loss_limit_pct=facts["global_daily_loss_limit_pct"],
            stale_flags=facts["stale_flags"],
            critical_stale_flags=facts["critical_stale_flags"],
            stale_ages=facts["stale_ages"],
        )

        rules_evaluated: list[str] = []
        reduced = False
        reject_result: RuleResult | None = None

        for rule in self._rules:
            if (
                signal.trading_mode == TradingMode.PAPER
                and rule.name in self._paper_relaxed
            ):
                continue
            rules_evaluated.append(rule.name)
            res = rule.evaluate(ctx)
            if res is None:
                continue
            if res.verdict == "reject":
                reject_result = res
                break
            if res.verdict == "reduce_size" and res.reduced_size is not None:
                ctx.suggested_size = min(ctx.suggested_size, res.reduced_size)
                if ctx.suggested_size <= 0:
                    reject_result = RuleResult(
                        rule_name=rule.name,
                        verdict="reject",
                        reason=RejectionReason.LIMIT_EXCEEDED,
                        detail="Reduced size reached zero",
                    )
                    break
                reduced = True

        if reject_result is not None:
            decision = RiskDecision(
                outcome=RiskOutcome.REJECT,
                approved=False,
                signal_id=signal.signal_id,
                run_id=signal.run_id,
                user_id=signal.user_id,
                strategy_name=signal.strategy_name,
                symbol=signal.symbol,
                original_size=str(signal.suggested_size),
                final_size="0",
                trading_mode=signal.trading_mode,
                reason=reject_result.reason,
                detail=f"{reject_result.rule_name}: {reject_result.detail}",
                rules_evaluated=rules_evaluated,
                trace_id=trace_id,
            )
            self._persist_risk_event(signal, decision)
            self._record_metric(
                rejected=True,
                rejection_reason=reject_result.rule_name,
            )
            self._log_decision(signal, decision)
            return decision, None

        final_size = ctx.suggested_size
        outcome = RiskOutcome.REDUCE_SIZE if reduced else RiskOutcome.APPROVE
        decision = RiskDecision(
            outcome=outcome,
            approved=True,
            signal_id=signal.signal_id,
            run_id=signal.run_id,
            user_id=signal.user_id,
            strategy_name=signal.strategy_name,
            symbol=signal.symbol,
            original_size=str(signal.suggested_size),
            final_size=str(final_size),
            trading_mode=signal.trading_mode,
            reason=None,
            detail=None,
            rules_evaluated=rules_evaluated,
            trace_id=trace_id,
        )

        intent = self._to_intent(signal, final_size)
        self._persist_risk_event(signal, decision)
        self._record_metric()
        self._log_decision(signal, decision)
        return decision, intent

    def metrics_snapshot(self) -> dict[str, Any]:
        return {
            "signals_generated": int(self._metrics["signals_generated"]),
            "signals_rejected": int(self._metrics["signals_rejected"]),
            "signals_executed": int(self._metrics["signals_executed"]),
            "rejection_reasons": dict(self._rejection_reasons),
        }

    def _record_metric(
        self,
        *,
        rejected: bool = False,
        executed: bool = False,
        rejection_reason: str | None = None,
    ) -> None:
        self._metrics["signals_generated"] += 1
        if rejected:
            self._metrics["signals_rejected"] += 1
        if executed:
            self._metrics["signals_executed"] += 1
        if rejection_reason:
            self._rejection_reasons[rejection_reason] += 1

    def _apply_market_data_degradation(
        self,
        signal: StrategySignalEvent,
        facts: dict[str, Any],
    ) -> StrategySignalEvent:
        stale_flags = facts["stale_flags"]
        critical_stale_flags = facts["critical_stale_flags"]
        if not any(stale_flags.values()):
            return signal

        metadata = dict(signal.reasoning_metadata or {})
        quality = {
            "stale_flags": stale_flags,
            "critical_stale_flags": critical_stale_flags,
            "stale_ages": facts["stale_ages"],
            "degraded": not any(critical_stale_flags.values()),
        }
        metadata["market_data_quality"] = quality
        if any(critical_stale_flags.values()):
            return signal.model_copy(update={"reasoning_metadata": metadata})

        multiplier = Decimal(
            str(self._settings.risk_stale_degraded_confidence_multiplier)
        )
        adjusted_size = (Decimal(str(signal.suggested_size)) * multiplier).quantize(
            Decimal("0.00000001")
        )
        adjusted_confidence = min(
            1.0,
            max(0.0, float(Decimal(str(signal.confidence)) * multiplier)),
        )
        quality["confidence_multiplier"] = str(multiplier)
        quality["adjusted_confidence"] = adjusted_confidence
        quality["adjusted_size"] = str(adjusted_size)
        return signal.model_copy(
            update={
                "confidence": adjusted_confidence,
                "suggested_size": adjusted_size,
                "reasoning_metadata": metadata,
            }
        )

    def _to_intent(
        self, signal: StrategySignalEvent, final_size: Decimal
    ) -> TradeIntent | None:
        action = signal.action.value
        if action == "hold":
            return None

        tenant_id = self._lookup_primary_tenant(signal.user_id)
        side = Side.BUY
        if action in {"sell", "close"}:
            side = Side.SELL

        return TradeIntent(
            intent_id=uuid.uuid4(),
            correlation_id=uuid.uuid4(),
            tenant_id=tenant_id,
            trading_mode=signal.trading_mode,
            strategy_id=signal.strategy_name,
            instrument=Instrument(symbol=signal.symbol),
            side=side,
            order_type=OrderType.MARKET,
            quantity=Quantity(amount=str(final_size)),
        )

    def _lookup_primary_tenant(self, user_id: uuid.UUID):
        stmt = text(
            """
            SELECT tenant_id
            FROM tenant_memberships
            WHERE user_id = :user_id
            ORDER BY created_at ASC
            LIMIT 1
            """
        )
        with self._engine.begin() as conn:
            row = conn.execute(stmt, {"user_id": str(user_id)}).first()
        if row is None:
            raise ValueError("User has no tenant membership")
        return row.tenant_id

    @staticmethod
    def _json_dict(value: Any) -> dict[str, Any]:
        if value is None:
            return {}
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except Exception:
                return {}
        return value if isinstance(value, dict) else {}

    def _strategy_quality_controls(
        self,
        *,
        strategy_params: dict[str, Any],
        signal_rules: dict[str, Any],
    ) -> dict[str, Decimal]:
        max_spread_pct = strategy_params.get("max_spread_pct")
        if (
            max_spread_pct is None
            and signal_rules.get("skip_if_spread_bps_over") is not None
        ):
            max_spread_pct = Decimal(
                str(signal_rules["skip_if_spread_bps_over"])
            ) / Decimal("10000")
        if max_spread_pct is None:
            max_spread_pct = self._settings.risk_max_spread_pct

        max_slippage_pct = strategy_params.get(
            "max_slippage_pct", self._settings.risk_max_slippage_pct
        )
        fee_pct = strategy_params.get("fee_pct", 0)
        expected_profit_buffer_pct = strategy_params.get(
            "expected_profit_buffer_pct", 0
        )
        return {
            "max_spread_pct_allowed": Decimal(str(max_spread_pct)),
            "max_slippage_pct_allowed": Decimal(str(max_slippage_pct)),
            "fee_pct": Decimal(str(fee_pct)),
            "expected_profit_buffer_pct": Decimal(str(expected_profit_buffer_pct)),
        }

    def _load_facts(self, signal: StrategySignalEvent, now: datetime) -> dict[str, Any]:
        symbol = signal.symbol
        user_id = str(signal.user_id)
        now_iso = now.isoformat()
        token, quote = symbol.split("-", 1) if "-" in symbol else (symbol, "USD")

        bbo_raw = self._redis.get(f"oziebot:md:bbo:{symbol}")
        bbo = json.loads(bbo_raw) if bbo_raw else None
        bid = Decimal(str(bbo.get("best_bid_price", "0"))) if bbo else Decimal("0")
        ask = Decimal(str(bbo.get("best_ask_price", "0"))) if bbo else Decimal("0")
        mid = (bid + ask) / Decimal("2") if bid > 0 and ask > 0 else Decimal("0")
        spread_pct = Decimal("1")
        if mid > 0:
            spread_pct = (ask - bid) / mid
        est_slippage_pct = Decimal("0")
        if bbo and mid > 0:
            depth = max(
                Decimal(str(bbo.get("best_bid_size", "0"))),
                Decimal(str(bbo.get("best_ask_size", "0"))),
            )
            if depth > 0:
                participation = min(
                    Decimal("1"), Decimal(str(signal.suggested_size)) / depth
                )
                est_slippage_pct = spread_pct * participation
                candle_history = []
                if hasattr(self._redis, "lrange"):
                    candle_history = list(
                        self._redis.lrange(f"oziebot:md:candles:60:{symbol}", 0, 19)
                        or []
                    )
                closes: list[Decimal] = []
                for raw in reversed(candle_history):
                    try:
                        candle = json.loads(raw)
                        closes.append(Decimal(str(candle.get("close"))))
                    except Exception:
                        continue
                volatility_component = Decimal("0")
                if len(closes) >= 2:
                    returns = []
                    for prev, cur in zip(closes, closes[1:]):
                        if prev > 0:
                            returns.append(abs(cur - prev) / prev)
                    if returns:
                        volatility_component = sum(returns, Decimal("0")) / Decimal(
                            len(returns)
                        )
                est_slippage_pct = max(
                    est_slippage_pct,
                    volatility_component
                    * min(Decimal("1"), participation * Decimal("2")),
                )

        stale_assessments = {
            "trade": self._staleness_state(
                key=f"oziebot:md:last_update:trade:{symbol}",
                threshold_seconds=self._settings.risk_stale_trade_seconds,
                now=now,
            ),
            "bbo": self._staleness_state(
                key=f"oziebot:md:last_update:bbo:{symbol}",
                threshold_seconds=self._settings.risk_stale_bbo_seconds,
                now=now,
            ),
            "candle": self._staleness_state(
                key=f"oziebot:md:last_update:candle:{symbol}",
                threshold_seconds=self._settings.risk_stale_candle_seconds,
                now=now,
            ),
        }
        stale_flags = {
            name: bool(assessment["stale"])
            for name, assessment in stale_assessments.items()
        }
        critical_stale_flags = {
            name: bool(assessment["critical"])
            for name, assessment in stale_assessments.items()
        }
        stale_ages = {
            name: assessment["age_seconds"]
            for name, assessment in stale_assessments.items()
        }

        with self._engine.begin() as conn:
            platform_paused = conn.execute(
                text("SELECT value FROM platform_settings WHERE key = :k"),
                {"k": "trading.global.pause"},
            ).first()
            paper_without_subscription = conn.execute(
                text("SELECT value FROM platform_settings WHERE key = :k"),
                {"k": "billing.allow_paper_without_subscription"},
            ).first()
            global_loss_guard = conn.execute(
                text("SELECT value FROM platform_settings WHERE key = :k"),
                {"k": "trading.global.daily_loss_guard"},
            ).first()
            paused = False
            if platform_paused:
                val = platform_paused.value
                if isinstance(val, str):
                    val = json.loads(val)
                paused = bool(val.get("paused", False))
            allow_paper_without_subscription = True
            if paper_without_subscription:
                val = paper_without_subscription.value
                if isinstance(val, str):
                    val = json.loads(val)
                allow_paper_without_subscription = bool(val.get("enabled", True))

            strategy_row = (
                conn.execute(
                    text(
                        """
                    SELECT is_enabled, config FROM user_strategies
                    WHERE user_id = :user_id AND strategy_id = :sid
                    LIMIT 1
                    """
                    ),
                    {"user_id": user_id, "sid": signal.strategy_name},
                )
                .mappings()
                .first()
            )
            platform_strategy_row = (
                conn.execute(
                    text(
                        """
                    SELECT config_schema
                    FROM platform_strategies
                    WHERE slug = :slug
                    LIMIT 1
                    """
                    ),
                    {"slug": signal.strategy_name},
                )
                .mappings()
                .first()
            )

            token_platform_enabled = conn.execute(
                text(
                    """
                    SELECT is_enabled
                    FROM platform_token_allowlist
                    WHERE symbol = :symbol
                    LIMIT 1
                    """
                ),
                {"symbol": symbol},
            ).first()
            token_policy_row = (
                conn.execute(
                    text(
                        """
                    SELECT
                      tsp.admin_enabled,
                      tsp.recommendation_status,
                      tsp.recommendation_reason,
                      tsp.recommendation_status_override,
                      tsp.recommendation_reason_override,
                      tsp.max_position_pct_override
                    FROM platform_token_allowlist p
                    LEFT JOIN token_strategy_policy tsp
                      ON tsp.token_id = p.id
                     AND tsp.strategy_id = :strategy_id
                    WHERE p.symbol = :symbol
                    LIMIT 1
                    """
                    ),
                    {"symbol": symbol, "strategy_id": signal.strategy_name},
                )
                .mappings()
                .first()
            )

            token_user_enabled = conn.execute(
                text(
                    """
                    SELECT ut.is_enabled
                    FROM user_token_permissions ut
                    JOIN platform_token_allowlist p ON p.id = ut.platform_token_id
                    WHERE ut.user_id = :user_id
                      AND p.symbol = :symbol
                    LIMIT 1
                    """
                ),
                {"user_id": user_id, "symbol": symbol},
            ).first()

            bucket = (
                conn.execute(
                    text(
                        """
                    SELECT assigned_capital_cents, available_buying_power_cents, locked_capital_cents
                    FROM strategy_capital_buckets
                    WHERE user_id = :user_id
                      AND strategy_id = :sid
                      AND trading_mode = :mode
                    LIMIT 1
                    """
                    ),
                    {
                        "user_id": user_id,
                        "sid": signal.strategy_name,
                        "mode": signal.trading_mode.value,
                    },
                )
                .mappings()
                .first()
            )

            total_capital = conn.execute(
                text(
                    """
                    SELECT COALESCE(SUM(assigned_capital_cents), 0) AS total
                    FROM strategy_capital_buckets
                    WHERE user_id = :user_id AND trading_mode = :mode
                    """
                ),
                {"user_id": user_id, "mode": signal.trading_mode.value},
            ).first()
            strategy_exposure = conn.execute(
                text(
                    """
                    SELECT COALESCE(SUM(CAST(quantity AS NUMERIC) * CAST(avg_entry_price AS NUMERIC)), 0) AS total
                    FROM execution_positions
                    WHERE user_id = :user_id
                      AND trading_mode = :mode
                      AND strategy_id = :sid
                      AND CAST(quantity AS NUMERIC) > 0
                    """
                ),
                {
                    "user_id": user_id,
                    "mode": signal.trading_mode.value,
                    "sid": signal.strategy_name,
                },
            ).first()
            strategy_token_exposure = conn.execute(
                text(
                    """
                    SELECT COALESCE(SUM(CAST(quantity AS NUMERIC) * CAST(avg_entry_price AS NUMERIC)), 0) AS total
                    FROM execution_positions
                    WHERE user_id = :user_id
                      AND trading_mode = :mode
                      AND strategy_id = :sid
                      AND symbol = :symbol
                      AND CAST(quantity AS NUMERIC) > 0
                    """
                ),
                {
                    "user_id": user_id,
                    "mode": signal.trading_mode.value,
                    "sid": signal.strategy_name,
                    "symbol": symbol,
                },
            ).first()
            token_exposure = conn.execute(
                text(
                    """
                    SELECT COALESCE(SUM(CAST(quantity AS NUMERIC) * CAST(avg_entry_price AS NUMERIC)), 0) AS total
                    FROM execution_positions
                    WHERE user_id = :user_id
                      AND trading_mode = :mode
                      AND symbol = :symbol
                      AND CAST(quantity AS NUMERIC) > 0
                    """
                ),
                {
                    "user_id": user_id,
                    "mode": signal.trading_mode.value,
                    "symbol": symbol,
                },
            ).first()

            tenant = conn.execute(
                text(
                    """
                    SELECT tenant_id
                    FROM tenant_memberships
                    WHERE user_id = :user_id
                    ORDER BY created_at ASC
                    LIMIT 1
                    """
                ),
                {"user_id": user_id},
            ).first()

            entitled = False
            if tenant is not None:
                ent = conn.execute(
                    text(
                        """
                        SELECT te.id
                        FROM tenant_entitlements te
                        LEFT JOIN platform_strategies ps ON ps.id = te.platform_strategy_id
                        WHERE te.tenant_id = :tenant_id
                          AND te.is_active = TRUE
                          AND te.valid_from <= :now
                          AND (te.valid_until IS NULL OR te.valid_until >= :now)
                          AND (te.platform_strategy_id IS NULL OR ps.slug = :slug)
                        LIMIT 1
                        """
                    ),
                    {
                        "tenant_id": tenant.tenant_id,
                        "now": now_iso,
                        "slug": signal.strategy_name,
                    },
                ).first()
                entitled = ent is not None
            if (
                signal.trading_mode == TradingMode.PAPER
                and allow_paper_without_subscription
            ):
                entitled = True

            day_start = datetime(now.year, now.month, now.day, tzinfo=UTC).isoformat()
            settles = conn.execute(
                text(
                    """
                    SELECT metadata, created_at
                    FROM strategy_capital_ledger
                    WHERE user_id = :user_id
                      AND trading_mode = :mode
                      AND event_type = 'settle'
                      AND created_at >= :day_start
                    ORDER BY created_at DESC
                    """
                ),
                {
                    "user_id": user_id,
                    "mode": signal.trading_mode.value,
                    "day_start": day_start,
                },
            ).all()
            strategy_settles = conn.execute(
                text(
                    """
                    SELECT metadata, created_at
                    FROM strategy_capital_ledger
                    WHERE user_id = :user_id
                      AND strategy_id = :strategy_id
                      AND trading_mode = :mode
                      AND event_type = 'settle'
                    ORDER BY created_at DESC
                    """
                ),
                {
                    "user_id": user_id,
                    "strategy_id": signal.strategy_name,
                    "mode": signal.trading_mode.value,
                },
            ).all()

        daily_loss = 0
        recent_loss_count = 0
        cooldown_until = None
        for row in settles:
            md = row.metadata or {}
            if isinstance(md, str):
                md = json.loads(md)
            created_at = row.created_at
            if isinstance(created_at, str):
                created_at = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
            pnl = int(md.get("realized_pnl_delta_cents", 0))
            if pnl < 0:
                daily_loss += abs(pnl)
        platform_cfg = self._json_dict(
            platform_strategy_row["config_schema"] if platform_strategy_row else None
        )
        user_cfg = self._json_dict(strategy_row["config"] if strategy_row else None)
        strategy_params = (
            platform_cfg.get("strategy_params")
            if isinstance(platform_cfg, dict)
            else {}
        )
        signal_rules = (
            platform_cfg.get("signal_rules") if isinstance(platform_cfg, dict) else {}
        )
        risk_caps = (
            platform_cfg.get("risk_caps") if isinstance(platform_cfg, dict) else {}
        )
        if not isinstance(strategy_params, dict):
            strategy_params = {}
        if not isinstance(signal_rules, dict):
            signal_rules = {}
        if not isinstance(risk_caps, dict):
            risk_caps = {}
        merged_strategy_params = {**user_cfg, **strategy_params}
        quality_controls = self._strategy_quality_controls(
            strategy_params=merged_strategy_params,
            signal_rules=signal_rules,
        )

        max_consecutive_losses = int(
            risk_caps.get("max_consecutive_losses")
            or self._settings.risk_cooldown_loss_count
        )
        loss_cooldown_minutes = int(
            risk_caps.get("loss_cooldown_minutes")
            or self._settings.risk_cooldown_minutes
        )
        latest_loss_at = None
        for row in strategy_settles:
            md = row.metadata or {}
            if isinstance(md, str):
                md = json.loads(md)
            pnl = int(md.get("realized_pnl_delta_cents", 0))
            if pnl >= 0:
                break
            recent_loss_count += 1
            if latest_loss_at is None:
                latest_loss_at = row.created_at
                if isinstance(latest_loss_at, str):
                    latest_loss_at = datetime.fromisoformat(
                        latest_loss_at.replace("Z", "+00:00")
                    )
        if recent_loss_count >= max_consecutive_losses and latest_loss_at is not None:
            cooldown_until = latest_loss_at + timedelta(minutes=loss_cooldown_minutes)

        global_guard_cfg = self._json_dict(
            global_loss_guard.value if global_loss_guard else None
        )
        global_daily_loss_limit_pct = Decimal(
            str(
                global_guard_cfg.get("daily_loss_pct", 0)
                if global_guard_cfg.get("enabled", True)
                else 0
            )
        )
        effective_token_policy = resolve_effective_token_policy(
            dict(token_policy_row) if token_policy_row else None
        )
        total_capital_cents = int(total_capital.total if total_capital else 0)
        token_policy_max_position_cents = 0
        max_position_pct_override = effective_token_policy["max_position_pct_override"]
        if max_position_pct_override is not None and total_capital_cents > 0:
            token_policy_max_position_cents = int(
                (
                    Decimal(str(total_capital_cents)) * max_position_pct_override
                ).quantize(Decimal("1"))
            )

        return {
            "platform_paused": paused,
            "strategy_enabled": bool(strategy_row and strategy_row["is_enabled"]),
            "token_platform_enabled": bool(
                token_platform_enabled and token_platform_enabled.is_enabled
            ),
            "token_user_enabled": bool(
                token_user_enabled and token_user_enabled.is_enabled
            ),
            "token_policy_admin_enabled": bool(effective_token_policy["admin_enabled"]),
            "token_policy_status": str(
                effective_token_policy["effective_recommendation_status"]
            ),
            "token_policy_reason": effective_token_policy[
                "effective_recommendation_reason"
            ],
            "token_policy_size_multiplier": effective_token_policy["size_multiplier"],
            "bucket": dict(bucket) if bucket else None,
            "total_capital_cents": total_capital_cents,
            "entitled": entitled,
            "daily_loss_cents": daily_loss,
            "recent_loss_count": recent_loss_count,
            "cooldown_loss_threshold": max_consecutive_losses,
            "cooldown_until": cooldown_until,
            "current_strategy_token_exposure_cents": int(
                (
                    Decimal(
                        str(
                            strategy_token_exposure.total
                            if strategy_token_exposure
                            else 0
                        )
                    )
                    * Decimal("100")
                ).quantize(Decimal("1"))
            ),
            "current_strategy_exposure_cents": int(
                (
                    Decimal(str(strategy_exposure.total if strategy_exposure else 0))
                    * Decimal("100")
                ).quantize(Decimal("1"))
            ),
            "current_token_exposure_cents": int(
                (
                    Decimal(str(token_exposure.total if token_exposure else 0))
                    * Decimal("100")
                ).quantize(Decimal("1"))
            ),
            "token_policy_max_position_cents": token_policy_max_position_cents,
            "max_strategy_exposure_cents": int(
                Decimal(str(risk_caps.get("max_exposure_per_strategy") or 0))
                * Decimal("100")
            ),
            "max_token_exposure_cents": int(
                Decimal(str(risk_caps.get("max_exposure_per_token") or 0))
                * Decimal("100")
            ),
            "global_daily_loss_limit_pct": global_daily_loss_limit_pct,
            "mid_price": mid,
            "spread_pct": spread_pct,
            "est_slippage_pct": est_slippage_pct,
            **quality_controls,
            "stale_flags": stale_flags,
            "critical_stale_flags": critical_stale_flags,
            "stale_ages": stale_ages,
        }

    def _staleness_state(
        self, *, key: str, threshold_seconds: int, now: datetime
    ) -> dict[str, float | bool | None]:
        raw = self._redis.get(key)
        if not raw:
            return {"stale": True, "critical": True, "age_seconds": None}
        try:
            last = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        except Exception:
            return {"stale": True, "critical": True, "age_seconds": None}
        age_seconds = max(0.0, (now - last).total_seconds())
        critical_threshold = (
            threshold_seconds * self._settings.risk_critical_stale_multiplier
        )
        return {
            "stale": age_seconds > threshold_seconds,
            "critical": age_seconds > critical_threshold,
            "age_seconds": age_seconds,
        }

    def _persist_risk_event(
        self, signal: StrategySignalEvent, decision: RiskDecision
    ) -> None:
        stmt = text(
            """
            INSERT INTO risk_events (
              id, signal_id, run_id, user_id, strategy_name, symbol,
              trading_mode, outcome, reason, detail, original_size, final_size,
              trace_id, rules_evaluated, signal_payload, created_at
            ) VALUES (
              :id, :signal_id, :run_id, :user_id, :strategy_name, :symbol,
              :trading_mode, :outcome, :reason, :detail, :original_size, :final_size,
              :trace_id, :rules_evaluated, :signal_payload, :created_at
            )
            """
        )
        with self._engine.begin() as conn:
            conn.execute(
                stmt,
                {
                    "id": str(uuid.uuid4()),
                    "signal_id": str(decision.signal_id),
                    "run_id": str(decision.run_id),
                    "user_id": str(decision.user_id),
                    "strategy_name": decision.strategy_name,
                    "symbol": decision.symbol,
                    "trading_mode": decision.trading_mode.value,
                    "outcome": decision.outcome.value,
                    "reason": decision.reason.value if decision.reason else None,
                    "detail": decision.detail,
                    "original_size": decision.original_size,
                    "final_size": decision.final_size,
                    "trace_id": decision.trace_id,
                    "rules_evaluated": json.dumps({"rules": decision.rules_evaluated}),
                    "signal_payload": json.dumps(
                        signal.model_dump(mode="json"), default=str
                    ),
                    "created_at": datetime.now(UTC),
                },
            )

    def _load_runtime_state(
        self, user_id: uuid.UUID, strategy_id: str, trading_mode: str
    ) -> dict[str, Any]:
        stmt = text(
            """
            SELECT state
            FROM user_strategy_states
            WHERE user_id = :user_id
              AND strategy_id = :strategy_id
              AND trading_mode = :trading_mode
            LIMIT 1
            """
        )
        with self._engine.begin() as conn:
            row = (
                conn.execute(
                    stmt,
                    {
                        "user_id": str(user_id),
                        "strategy_id": strategy_id,
                        "trading_mode": trading_mode,
                    },
                )
                .mappings()
                .first()
            )
        return self._json_dict(row["state"] if row else None)

    def _upsert_runtime_state(
        self,
        user_id: uuid.UUID,
        strategy_id: str,
        trading_mode: str,
        state: dict[str, Any],
        now: datetime,
    ) -> None:
        stmt = text(
            """
            INSERT INTO user_strategy_states (id, user_id, strategy_id, trading_mode, state, created_at, updated_at)
            VALUES (:id, :user_id, :strategy_id, :trading_mode, CAST(:state AS JSON), :created_at, :updated_at)
            ON CONFLICT (user_id, strategy_id, trading_mode)
            DO UPDATE SET state = CAST(:state AS JSON), updated_at = :updated_at
            """
        )
        with self._engine.begin() as conn:
            conn.execute(
                stmt,
                {
                    "id": str(uuid.uuid4()),
                    "user_id": str(user_id),
                    "strategy_id": strategy_id,
                    "trading_mode": trading_mode,
                    "state": json.dumps(state, default=str),
                    "created_at": now,
                    "updated_at": now,
                },
            )

    def _maybe_emit_global_loss_alert(
        self,
        signal: StrategySignalEvent,
        facts: dict[str, Any],
        now: datetime,
        trace_id: str,
    ) -> None:
        limit = Decimal(str(facts.get("global_daily_loss_limit_pct", 0)))
        total_capital_cents = int(facts.get("total_capital_cents", 0) or 0)
        daily_loss_cents = int(facts.get("daily_loss_cents", 0) or 0)
        if limit <= 0 or total_capital_cents <= 0 or daily_loss_cents <= 0:
            return
        loss_pct = (Decimal(str(daily_loss_cents)) * Decimal("100")) / Decimal(
            str(total_capital_cents)
        )
        if loss_pct < limit:
            return

        state = self._load_runtime_state(
            signal.user_id, "__global__", signal.trading_mode.value
        )
        guard_state = self._json_dict(state.get("global_daily_loss_guard"))
        today = now.date().isoformat()
        if guard_state.get("day") == today:
            return

        state["global_daily_loss_guard"] = {
            "day": today,
            "triggered_at": now.isoformat(),
            "daily_loss_cents": daily_loss_cents,
            "loss_pct": float(loss_pct),
            "limit_pct": float(limit),
        }
        self._upsert_runtime_state(
            signal.user_id, "__global__", signal.trading_mode.value, state, now
        )
        notification = NotificationEvent(
            event_id=uuid.uuid4(),
            tenant_id=self._lookup_primary_tenant(signal.user_id),
            user_id=signal.user_id,
            trading_mode=signal.trading_mode,
            event_type=NotificationEventType.STRATEGY_PAUSED,
            trace_id=trace_id,
            title="Global loss guard triggered",
            message=(
                f"All strategies paused for {signal.trading_mode.value} after daily loss reached "
                f"{loss_pct:.2f}%."
            ),
            payload={
                "scope": "global",
                "daily_loss_cents": daily_loss_cents,
                "daily_loss_pct": float(loss_pct),
                "limit_pct": float(limit),
            },
        )
        push_json(
            self._redis,
            QueueNames.alerts(signal.trading_mode),
            notification_event_to_json(notification),
        )

    def _log_decision(
        self, signal: StrategySignalEvent, decision: RiskDecision
    ) -> None:
        log.info(
            "risk_decision %s",
            json.dumps(
                {
                    "stage": "risk",
                    "strategy": signal.strategy_name,
                    "token": signal.symbol,
                    "signal_generated": signal.action != SignalType.HOLD,
                    "signal_reason": signal.reasoning_metadata.get("reason"),
                    "rejection_reason": decision.reason.value
                    if decision.reason
                    else None,
                    "confidence_score": signal.confidence,
                    "applied_risk_rules": decision.rules_evaluated,
                    "token_policy": signal.reasoning_metadata.get("token_policy"),
                    "outcome": decision.outcome.value,
                    "final_decision": decision.outcome.value,
                    "final_size": decision.final_size,
                    "detail": decision.detail,
                    "market_data_quality": signal.reasoning_metadata.get(
                        "market_data_quality"
                    ),
                    "metrics": self.metrics_snapshot(),
                },
                default=str,
            ),
        )
