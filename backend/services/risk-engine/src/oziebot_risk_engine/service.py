from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import create_engine, text

from oziebot_domain.intents import TradeIntent
from oziebot_domain.risk import RejectionReason, RiskDecision, RiskOutcome
from oziebot_domain.signal_pipeline import StrategySignalEvent
from oziebot_domain.trading import Instrument, OrderType, Quantity, Side
from oziebot_domain.trading_mode import TradingMode
from oziebot_risk_engine.config import Settings
from oziebot_risk_engine.rules import RuleContext, RuleResult, default_rules


class RiskEngineService:
    def __init__(self, settings: Settings, redis_client):
        self._settings = settings
        self._redis = redis_client
        self._engine = create_engine(settings.database_url)
        self._rules = default_rules(settings)
        self._paper_relaxed = {
            x.strip() for x in settings.risk_relaxed_paper_rules.split(",") if x.strip()
        }

    def evaluate(self, signal: StrategySignalEvent, trace_id: str) -> tuple[RiskDecision, TradeIntent | None]:
        now = datetime.now(UTC)
        facts = self._load_facts(signal, now)
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
            return decision, None

        ctx = RuleContext(
            signal=signal,
            trading_mode=signal.trading_mode.value,
            symbol=signal.symbol,
            suggested_size=size,
            mid_price=facts["mid_price"],
            spread_pct=facts["spread_pct"],
            est_slippage_pct=facts["est_slippage_pct"],
            now=now,
            platform_paused=facts["platform_paused"],
            entitled=facts["entitled"],
            token_platform_enabled=facts["token_platform_enabled"],
            token_user_enabled=facts["token_user_enabled"],
            strategy_enabled=facts["strategy_enabled"],
            bucket=facts["bucket"],
            total_capital_cents=facts["total_capital_cents"],
            daily_loss_cents=facts["daily_loss_cents"],
            recent_loss_count=facts["recent_loss_count"],
            cooldown_until=facts["cooldown_until"],
            stale_flags=facts["stale_flags"],
        )

        rules_evaluated: list[str] = []
        reduced = False
        reject_result: RuleResult | None = None

        for rule in self._rules:
            if signal.trading_mode == TradingMode.PAPER and rule.name in self._paper_relaxed:
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
        return decision, intent

    def _to_intent(self, signal: StrategySignalEvent, final_size: Decimal) -> TradeIntent | None:
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
            row = conn.execute(stmt, {"user_id": user_id.hex if hasattr(user_id, 'hex') else str(user_id).replace('-', '')}).first()
        if row is None:
            raise ValueError("User has no tenant membership")
        return row.tenant_id

    def _load_facts(self, signal: StrategySignalEvent, now: datetime) -> dict[str, Any]:
        symbol = signal.symbol
        user_id = signal.user_id.hex if hasattr(signal.user_id, 'hex') else str(signal.user_id).replace('-', '')
        now_iso = now.strftime("%Y-%m-%d %H:%M:%S")
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
                participation = min(Decimal("1"), Decimal(str(signal.suggested_size)) / depth)
                est_slippage_pct = spread_pct * participation

        stale_flags = {
            "trade": self._is_stale(
                f"oziebot:md:last_update:trade:{symbol}", self._settings.risk_stale_trade_seconds, now
            ),
            "bbo": self._is_stale(
                f"oziebot:md:last_update:bbo:{symbol}", self._settings.risk_stale_bbo_seconds, now
            ),
            "candle": self._is_stale(
                f"oziebot:md:last_update:candle:{symbol}", self._settings.risk_stale_candle_seconds, now
            ),
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

            strategy_enabled = conn.execute(
                text(
                    """
                    SELECT is_enabled FROM user_strategies
                    WHERE user_id = :user_id AND strategy_id = :sid
                    LIMIT 1
                    """
                ),
                {"user_id": user_id, "sid": signal.strategy_name},
            ).first()

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

            bucket = conn.execute(
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
            ).mappings().first()

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
            if signal.trading_mode == TradingMode.PAPER and allow_paper_without_subscription:
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
                {"user_id": user_id, "mode": signal.trading_mode.value, "day_start": day_start},
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
                if created_at >= now - timedelta(minutes=self._settings.risk_cooldown_minutes):
                    recent_loss_count += 1
        if recent_loss_count >= self._settings.risk_cooldown_loss_count and settles:
            latest_created_at = settles[0].created_at
            if isinstance(latest_created_at, str):
                latest_created_at = datetime.fromisoformat(latest_created_at.replace("Z", "+00:00"))
            cooldown_until = latest_created_at + timedelta(minutes=self._settings.risk_cooldown_minutes)

        return {
            "platform_paused": paused,
            "strategy_enabled": bool(strategy_enabled and strategy_enabled.is_enabled),
            "token_platform_enabled": bool(token_platform_enabled and token_platform_enabled.is_enabled),
            "token_user_enabled": bool(token_user_enabled and token_user_enabled.is_enabled),
            "bucket": dict(bucket) if bucket else None,
            "total_capital_cents": int(total_capital.total if total_capital else 0),
            "entitled": entitled,
            "daily_loss_cents": daily_loss,
            "recent_loss_count": recent_loss_count,
            "cooldown_until": cooldown_until,
            "mid_price": mid,
            "spread_pct": spread_pct,
            "est_slippage_pct": est_slippage_pct,
            "stale_flags": stale_flags,
        }

    def _is_stale(self, key: str, threshold_seconds: int, now: datetime) -> bool:
        raw = self._redis.get(key)
        if not raw:
            return True
        try:
            last = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        except Exception:
            return True
        return (now - last).total_seconds() > threshold_seconds

    def _persist_risk_event(self, signal: StrategySignalEvent, decision: RiskDecision) -> None:
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
                    "signal_payload": json.dumps(signal.model_dump(mode="json"), default=str),
                    "created_at": datetime.now(UTC),
                },
            )
