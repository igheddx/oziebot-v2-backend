from __future__ import annotations

import json
import logging
import uuid
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from oziebot_common.queues import QueueNames, push_json, strategy_signal_to_json
from oziebot_common.token_policy import resolve_effective_token_policy
from oziebot_domain.signal_pipeline import StrategySignalEvent
from oziebot_domain.strategy import SignalType, StrategySignal
from oziebot_domain.tenant import TenantId
from oziebot_domain.trading_mode import TradingMode
from oziebot_strategy_engine.registry import StrategyRegistry
from oziebot_strategy_engine.strategy import (
    MarketSnapshot,
    PositionState,
    StrategyContext,
)

log = logging.getLogger("strategy-engine.runner")

STRATEGY_INTERVAL_SECONDS: dict[str, int] = {
    "momentum": 30,
    "day_trading": 60,
    "dca": 300,
}


@dataclass
class StrategyScheduleState:
    last_run: dict[tuple[str, str, str, str], datetime] = field(default_factory=dict)

    def should_run(
        self,
        *,
        user_id: str,
        strategy_name: str,
        trading_mode: str,
        symbol: str,
        now: datetime,
        interval_seconds: int,
    ) -> bool:
        key = (user_id, strategy_name, trading_mode, symbol)
        prev = self.last_run.get(key)
        if prev is None or (now - prev).total_seconds() >= interval_seconds:
            self.last_run[key] = now
            return True
        return False


class StrategyRunner:
    def __init__(
        self, *, engine: Engine, redis_client, candle_granularity_sec: int = 60
    ):
        self._engine = engine
        self._redis = redis_client
        self._schedule = StrategyScheduleState()
        self._candle_granularity_sec = candle_granularity_sec
        self._metrics: Counter[str] = Counter()
        self._rejection_reasons: Counter[str] = Counter()

    def run_once(self) -> int:
        rows = self._load_enabled_user_strategies()
        processed = 0
        now = datetime.now(UTC)

        for row in rows:
            user_id = str(row["user_id"])
            strategy_name = str(row["strategy_id"])
            tenant_id_raw = row.get("tenant_id")
            if tenant_id_raw is None:
                continue
            tenant_id = (
                tenant_id_raw
                if isinstance(tenant_id_raw, uuid.UUID)
                else TenantId(str(tenant_id_raw))
            )
            user_config = row.get("config") or {}
            if isinstance(user_config, str):
                user_config = json.loads(user_config)

            platform_cfg = self._load_platform_strategy_config(strategy_name)
            strategy_params = (
                platform_cfg.get("strategy_params")
                if isinstance(platform_cfg, dict)
                else {}
            )
            signal_rules = (
                platform_cfg.get("signal_rules")
                if isinstance(platform_cfg, dict)
                else {}
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

            # Platform strategy_params are authoritative defaults, while user config can still
            # provide optional overrides (e.g. symbol) for keys not set at platform level.
            config = {**user_config, **strategy_params}

            allowed_symbols = self._load_allowed_symbols(user_id)
            if not allowed_symbols:
                continue

            symbols = self._resolve_symbols(
                config=config, allowed_symbols=allowed_symbols
            )
            if not symbols:
                continue

            for symbol in symbols:
                market = self._load_market_snapshot(symbol)
                if market is None:
                    continue
                token_policy = self._load_token_strategy_policy(
                    symbol=symbol,
                    strategy_name=strategy_name,
                )

                for mode in (TradingMode.PAPER, TradingMode.LIVE):
                    interval = STRATEGY_INTERVAL_SECONDS.get(strategy_name, 60)
                    if not self._schedule.should_run(
                        user_id=user_id,
                        strategy_name=strategy_name,
                        trading_mode=mode.value,
                        symbol=symbol,
                        now=now,
                        interval_seconds=interval,
                    ):
                        continue

                    position_state = self._load_position_state(
                        user_id=user_id,
                        strategy_name=strategy_name,
                        trading_mode=mode.value,
                        symbol=symbol,
                    )
                    position_state = self._sync_position_runtime_state(
                        user_id=user_id,
                        strategy_name=strategy_name,
                        trading_mode=mode.value,
                        position_state=position_state,
                        market=market,
                        now=now,
                    )
                    runtime_state: dict[str, Any] = {}
                    if strategy_name == "dca":
                        runtime_state = self._load_strategy_runtime_state(
                            user_id=user_id,
                            strategy_name=strategy_name,
                            trading_mode=mode.value,
                        )
                    run_id = uuid.uuid4()
                    trace_id = str(run_id)
                    try:
                        policy_reason = self._token_policy_suppression_reason(
                            token_policy
                        )
                        if policy_reason is not None:
                            self._record_signal_metric(
                                rejected=True, rejection_reason=policy_reason
                            )
                            self._persist_run(
                                run_id=run_id,
                                user_id=user_id,
                                strategy_name=strategy_name,
                                symbol=symbol,
                                trading_mode=mode.value,
                                status="completed",
                                trace_id=trace_id,
                                metadata={
                                    "suppressed": True,
                                    "suppression_reason": policy_reason,
                                    "token_policy": token_policy,
                                },
                                started_at=now,
                                completed_at=datetime.now(UTC),
                            )
                            self._log_signal_evaluation(
                                stage="strategy",
                                strategy_name=strategy_name,
                                symbol=symbol,
                                trading_mode=mode,
                                signal_generated=False,
                                rejection_reason=policy_reason,
                                confidence_score=None,
                                final_decision="rejected",
                            )
                            continue
                        schedule_reason = self._scheduler_reason(
                            strategy_name=strategy_name,
                            config=config,
                            trading_mode=mode,
                            symbol=symbol,
                            runtime_state=runtime_state,
                            now=now,
                        )
                        if schedule_reason is not None:
                            self._persist_run(
                                run_id=run_id,
                                user_id=user_id,
                                strategy_name=strategy_name,
                                symbol=symbol,
                                trading_mode=mode.value,
                                status="completed",
                                trace_id=trace_id,
                                metadata={
                                    "suppressed": True,
                                    "suppression_reason": schedule_reason,
                                    "scheduler": True,
                                },
                                started_at=now,
                                completed_at=datetime.now(UTC),
                            )
                            self._log_signal_evaluation(
                                stage="strategy",
                                strategy_name=strategy_name,
                                symbol=symbol,
                                trading_mode=mode,
                                signal_generated=False,
                                rejection_reason=schedule_reason,
                                confidence_score=None,
                                final_decision="scheduled_out",
                            )
                            continue

                        signal = self._generate_signal(
                            tenant_id=tenant_id,
                            strategy_name=strategy_name,
                            trading_mode=mode,
                            market=market,
                            position_state=position_state,
                            config=config,
                        )
                        signal = self._apply_token_policy_to_signal(
                            signal=signal, token_policy=token_policy
                        )
                        suppress_reason = self._suppression_reason(
                            user_id=user_id,
                            strategy_name=strategy_name,
                            trading_mode=mode,
                            signal=signal,
                            market=market,
                            position_state=position_state,
                            signal_rules=signal_rules,
                            risk_caps=risk_caps,
                        )
                        if suppress_reason is not None:
                            self._record_signal_metric(
                                rejected=True,
                                rejection_reason=suppress_reason,
                            )
                            self._persist_run(
                                run_id=run_id,
                                user_id=user_id,
                                strategy_name=strategy_name,
                                symbol=symbol,
                                trading_mode=mode.value,
                                status="completed",
                                trace_id=trace_id,
                                metadata={
                                    "suppressed": True,
                                    "suppression_reason": suppress_reason,
                                    "confidence": float(signal.confidence),
                                    "token_policy": (signal.metadata or {}).get(
                                        "token_policy"
                                    ),
                                },
                                started_at=now,
                                completed_at=datetime.now(UTC),
                            )
                            self._log_signal_evaluation(
                                stage="suppression",
                                strategy_name=strategy_name,
                                symbol=symbol,
                                trading_mode=mode,
                                signal_generated=False,
                                rejection_reason=suppress_reason,
                                confidence_score=signal.confidence,
                                final_decision="rejected",
                            )
                            continue

                        event = self._to_signal_event(
                            run_id=run_id,
                            user_id=uuid.UUID(user_id),
                            strategy_name=strategy_name,
                            symbol=symbol,
                            signal=signal,
                            trading_mode=mode,
                            timestamp=now,
                            position_state=position_state,
                            risk_caps=risk_caps,
                            market=market,
                        )
                        self._persist_run(
                            run_id=run_id,
                            user_id=user_id,
                            strategy_name=strategy_name,
                            symbol=symbol,
                            trading_mode=mode.value,
                            status="completed",
                            trace_id=trace_id,
                            metadata={
                                "confidence": float(signal.confidence),
                                "token_policy": (signal.metadata or {}).get(
                                    "token_policy"
                                ),
                            },
                            started_at=now,
                            completed_at=datetime.now(UTC),
                        )
                        self._persist_signal(event)
                        q = QueueNames.signal_generated(mode)
                        push_json(
                            self._redis,
                            q,
                            {
                                "signal": strategy_signal_to_json(event),
                                "trace_id": trace_id,
                            },
                        )
                        processed += 1
                        signal_generated = event.action != SignalType.HOLD
                        if signal_generated:
                            self._record_signal_metric(generated=True)
                        self._log_signal_evaluation(
                            stage="strategy",
                            strategy_name=strategy_name,
                            symbol=symbol,
                            trading_mode=mode,
                            signal_generated=signal_generated,
                            rejection_reason=None,
                            confidence_score=event.confidence,
                            final_decision=event.action.value,
                            extra={
                                "suggested_size": str(event.suggested_size),
                                "reason": event.reasoning_metadata.get("reason"),
                                "metrics": self.metrics_snapshot(),
                            },
                        )
                    except Exception as exc:
                        self._record_signal_metric(
                            rejected=True, rejection_reason="strategy_run_failed"
                        )
                        self._persist_run(
                            run_id=run_id,
                            user_id=user_id,
                            strategy_name=strategy_name,
                            symbol=symbol,
                            trading_mode=mode.value,
                            status="failed",
                            trace_id=trace_id,
                            metadata={"error": str(exc)},
                            started_at=now,
                            completed_at=datetime.now(UTC),
                        )
                        self._log_signal_evaluation(
                            stage="strategy",
                            strategy_name=strategy_name,
                            symbol=symbol,
                            trading_mode=mode,
                            signal_generated=False,
                            rejection_reason="strategy_run_failed",
                            confidence_score=None,
                            final_decision="error",
                            extra={
                                "error": str(exc),
                                "metrics": self.metrics_snapshot(),
                            },
                        )
                        log.exception(
                            "strategy_run_failed run_id=%s user_id=%s strategy=%s mode=%s symbol=%s",
                            run_id,
                            user_id,
                            strategy_name,
                            mode.value,
                            symbol,
                        )
        return processed

    def metrics_snapshot(self) -> dict[str, Any]:
        return {
            "signals_generated": int(self._metrics["signals_generated"]),
            "signals_rejected": int(self._metrics["signals_rejected"]),
            "signals_executed": int(self._metrics["signals_executed"]),
            "rejection_reasons": dict(self._rejection_reasons),
        }

    def _record_signal_metric(
        self,
        *,
        generated: bool = False,
        rejected: bool = False,
        executed: bool = False,
        rejection_reason: str | None = None,
    ) -> None:
        if generated:
            self._metrics["signals_generated"] += 1
        if rejected:
            self._metrics["signals_rejected"] += 1
        if executed:
            self._metrics["signals_executed"] += 1
        if rejection_reason:
            self._rejection_reasons[rejection_reason] += 1

    def _log_signal_evaluation(
        self,
        *,
        stage: str,
        strategy_name: str,
        symbol: str,
        trading_mode: TradingMode,
        signal_generated: bool,
        rejection_reason: str | None,
        confidence_score: float | None,
        final_decision: str,
        extra: dict[str, Any] | None = None,
    ) -> None:
        payload = {
            "stage": stage,
            "strategy": strategy_name,
            "token": symbol,
            "trading_mode": trading_mode.value,
            "signal_generated": signal_generated,
            "rejection_reason": rejection_reason,
            "confidence_score": confidence_score,
            "final_decision": final_decision,
        }
        if extra:
            payload.update(extra)
        log.info("signal_evaluation %s", json.dumps(payload, default=str))

    def _signal_action(self, signal: StrategySignal) -> str:
        raw = getattr(signal, "signal_type", "")
        val = getattr(raw, "value", raw)
        return str(val).lower()

    def _to_decimal(self, value: Any, default: Decimal = Decimal("0")) -> Decimal:
        if value is None:
            return default
        try:
            return Decimal(str(value))
        except Exception:
            return default

    def _load_platform_strategy_config(self, strategy_id: str) -> dict[str, Any]:
        stmt = text(
            """
            SELECT config_schema
            FROM platform_strategies
            WHERE slug = :strategy_id AND is_enabled = true
            LIMIT 1
            """
        )
        with self._engine.begin() as conn:
            row = conn.execute(stmt, {"strategy_id": strategy_id}).mappings().first()
        if not row:
            return {}
        payload = row["config_schema"]
        if payload is None:
            return {}
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except Exception:
                return {}
        return payload if isinstance(payload, dict) else {}

    def _last_action_signal_ts(
        self,
        *,
        user_id: str,
        strategy_name: str,
        trading_mode: str,
    ) -> datetime | None:
        stmt = text(
            """
            SELECT MAX(timestamp) AS last_ts
            FROM strategy_signals
            WHERE user_id = :user_id
              AND strategy_name = :strategy_name
              AND trading_mode = :trading_mode
              AND lower(action) != 'hold'
            """
        )
        with self._engine.begin() as conn:
            row = (
                conn.execute(
                    stmt,
                    {
                        "user_id": user_id,
                        "strategy_name": strategy_name,
                        "trading_mode": trading_mode,
                    },
                )
                .mappings()
                .first()
            )
        if not row:
            return None
        ts = row.get("last_ts")
        if ts is None:
            return None
        if isinstance(ts, datetime):
            return ts
        try:
            return datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        except Exception:
            return None

    def _count_action_signals_today(
        self,
        *,
        user_id: str,
        strategy_name: str,
        trading_mode: str,
        now: datetime,
    ) -> int:
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        stmt = text(
            """
            SELECT COUNT(1) AS c
            FROM strategy_signals
            WHERE user_id = :user_id
              AND strategy_name = :strategy_name
              AND trading_mode = :trading_mode
              AND lower(action) != 'hold'
              AND timestamp >= :day_start
            """
        )
        with self._engine.begin() as conn:
            row = (
                conn.execute(
                    stmt,
                    {
                        "user_id": user_id,
                        "strategy_name": strategy_name,
                        "trading_mode": trading_mode,
                        "day_start": day_start,
                    },
                )
                .mappings()
                .first()
            )
        return int(row["c"]) if row and row.get("c") is not None else 0

    def _open_positions_count(
        self,
        *,
        user_id: str,
        strategy_name: str,
        trading_mode: str,
    ) -> int:
        stmt = text(
            """
            SELECT COUNT(1) AS c
            FROM execution_positions
            WHERE user_id = :user_id
              AND strategy_id = :strategy_name
              AND trading_mode = :trading_mode
              AND CAST(quantity AS REAL) != 0
            """
        )
        with self._engine.begin() as conn:
            row = (
                conn.execute(
                    stmt,
                    {
                        "user_id": user_id,
                        "strategy_name": strategy_name,
                        "trading_mode": trading_mode,
                    },
                )
                .mappings()
                .first()
            )
        return int(row["c"]) if row and row.get("c") is not None else 0

    def _bucket_snapshot(
        self,
        *,
        user_id: str,
        strategy_name: str,
        trading_mode: str,
    ) -> tuple[int, int] | None:
        stmt = text(
            """
            SELECT assigned_capital_cents, realized_pnl_cents
            FROM strategy_capital_buckets
            WHERE user_id = :user_id
              AND strategy_id = :strategy_name
              AND trading_mode = :trading_mode
            LIMIT 1
            """
        )
        with self._engine.begin() as conn:
            row = (
                conn.execute(
                    stmt,
                    {
                        "user_id": user_id,
                        "strategy_name": strategy_name,
                        "trading_mode": trading_mode,
                    },
                )
                .mappings()
                .first()
            )
        if not row:
            return None
        return int(row.get("assigned_capital_cents") or 0), int(
            row.get("realized_pnl_cents") or 0
        )

    def _estimate_signal_notional_usd(
        self,
        *,
        signal: StrategySignal,
        market: MarketSnapshot,
        risk_caps: dict[str, Any],
    ) -> Decimal:
        if signal.quantity is not None:
            qty = self._to_decimal(getattr(signal.quantity, "amount", None))
            return abs(qty * market.current_price)

        metadata = signal.metadata or {}
        if "buy_amount_usd" in metadata:
            return self._to_decimal(metadata.get("buy_amount_usd"))

        if "position_size_fraction" in metadata:
            fraction = self._to_decimal(metadata.get("position_size_fraction"))
            cap = self._to_decimal(risk_caps.get("max_position_usd"))
            if cap > 0 and fraction > 0:
                return cap * fraction

        return Decimal("0")

    def _suppression_reason(
        self,
        *,
        user_id: str,
        strategy_name: str,
        trading_mode: TradingMode,
        signal: StrategySignal,
        market: MarketSnapshot,
        position_state: PositionState,
        signal_rules: dict[str, Any],
        risk_caps: dict[str, Any],
    ) -> str | None:
        action = self._signal_action(signal)
        if action == "hold":
            return None

        if (
            bool(signal_rules.get("paper_only", False))
            and trading_mode != TradingMode.PAPER
        ):
            return "paper_only strategy"

        min_confidence = self._to_decimal(signal_rules.get("min_confidence"))
        if min_confidence > 0 and Decimal(str(signal.confidence)) < min_confidence:
            return "below min_confidence"

        if (
            bool(signal_rules.get("require_volume_confirmation", False))
            and market.volume_24h <= 0
        ):
            return "volume confirmation failed"

        if bool(signal_rules.get("only_during_liquid_hours", False)):
            hour = datetime.now(UTC).hour
            if hour < 13 or hour >= 22:
                return "outside liquid-hours window"

        sizing_reason = self._usd_sizing_suppression_reason(
            signal=signal,
            market=market,
            risk_caps=risk_caps,
        )
        if sizing_reason is not None:
            return sizing_reason

        cooldown_seconds = int(signal_rules.get("cooldown_seconds") or 0)
        if cooldown_seconds > 0:
            last_ts = self._last_action_signal_ts(
                user_id=user_id,
                strategy_name=strategy_name,
                trading_mode=trading_mode.value,
            )
            if last_ts is not None:
                elapsed = (datetime.now(UTC) - last_ts).total_seconds()
                if elapsed < cooldown_seconds:
                    return "cooldown active"

        max_signals_per_day = int(signal_rules.get("max_signals_per_day") or 0)
        if max_signals_per_day > 0:
            emitted = self._count_action_signals_today(
                user_id=user_id,
                strategy_name=strategy_name,
                trading_mode=trading_mode.value,
                now=datetime.now(UTC),
            )
            if emitted >= max_signals_per_day:
                return "max_signals_per_day reached"

        if action == "buy":
            max_open_positions = int(risk_caps.get("max_open_positions") or 0)
            if max_open_positions > 0 and position_state.quantity == 0:
                open_count = self._open_positions_count(
                    user_id=user_id,
                    strategy_name=strategy_name,
                    trading_mode=trading_mode.value,
                )
                if open_count >= max_open_positions:
                    return "max_open_positions reached"

            max_position_usd = self._to_decimal(risk_caps.get("max_position_usd"))
            if max_position_usd > 0:
                current_notional = abs(position_state.quantity * market.current_price)
                next_notional = current_notional + self._estimate_signal_notional_usd(
                    signal=signal,
                    market=market,
                    risk_caps=risk_caps,
                )
                if next_notional > max_position_usd:
                    return "max_position_usd exceeded"

            max_daily_loss_pct = self._to_decimal(risk_caps.get("max_daily_loss_pct"))
            if max_daily_loss_pct > 0:
                snap = self._bucket_snapshot(
                    user_id=user_id,
                    strategy_name=strategy_name,
                    trading_mode=trading_mode.value,
                )
                if snap is not None:
                    assigned_cents, realized_pnl_cents = snap
                    if assigned_cents > 0 and realized_pnl_cents < 0:
                        loss_pct = (
                            Decimal(abs(realized_pnl_cents)) * Decimal("100")
                        ) / Decimal(assigned_cents)
                        if loss_pct >= max_daily_loss_pct:
                            return "max_daily_loss_pct reached"

        return None

    def _usd_sizing_suppression_reason(
        self,
        *,
        signal: StrategySignal,
        market: MarketSnapshot,
        risk_caps: dict[str, Any],
    ) -> str | None:
        metadata = signal.metadata or {}
        if signal.quantity is not None or self._signal_action(signal) != "buy":
            return None
        if "buy_amount_usd" in metadata and market.current_price <= 0:
            return "market price unavailable for usd-normalized sizing"
        if "position_size_fraction" not in metadata:
            return None
        max_position_usd = self._to_decimal(risk_caps.get("max_position_usd"))
        if max_position_usd <= 0:
            return "max_position_usd required for usd-normalized sizing"
        if market.current_price <= 0:
            return "market price unavailable for usd-normalized sizing"
        return None

    def _scheduler_reason(
        self,
        *,
        strategy_name: str,
        config: dict[str, Any],
        trading_mode: TradingMode,
        symbol: str,
        runtime_state: dict[str, Any],
        now: datetime,
    ) -> str | None:
        if strategy_name != "dca":
            return None

        symbol_state = self._coerce_symbol_runtime_states(runtime_state).get(symbol, {})
        last_buy_at_raw = symbol_state.get("last_buy_at")
        if not last_buy_at_raw:
            return None

        if isinstance(last_buy_at_raw, str):
            last_buy_at = datetime.fromisoformat(last_buy_at_raw.replace("Z", "+00:00"))
        else:
            last_buy_at = last_buy_at_raw

        interval_hours = int(config.get("buy_interval_hours", 24) or 24)
        next_due_at = last_buy_at + timedelta(hours=interval_hours)
        if now < next_due_at:
            return f"dca interval active until {next_due_at.isoformat()}"
        return None

    def _load_token_strategy_policy(
        self,
        *,
        symbol: str,
        strategy_name: str,
    ) -> dict[str, Any] | None:
        if self._engine is None:
            return None
        stmt = text(
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
             AND tsp.strategy_id = :strategy_name
            WHERE p.symbol = :symbol
            LIMIT 1
            """
        )
        with self._engine.begin() as conn:
            row = (
                conn.execute(
                    stmt,
                    {"symbol": symbol, "strategy_name": strategy_name},
                )
                .mappings()
                .first()
            )
        return dict(row) if row else None

    def _token_policy_suppression_reason(
        self, token_policy: dict[str, Any] | None
    ) -> str | None:
        effective = resolve_effective_token_policy(token_policy)
        if not effective["admin_enabled"]:
            return "token strategy disabled by admin"
        if effective["effective_recommendation_status"] == "blocked":
            reason = (
                effective["effective_recommendation_reason"]
                or "blocked by token strategy policy"
            )
            return f"token strategy blocked: {reason}"
        return None

    def _apply_token_policy_to_signal(
        self,
        *,
        signal: StrategySignal,
        token_policy: dict[str, Any] | None,
    ) -> StrategySignal:
        if token_policy is None:
            return signal
        effective = resolve_effective_token_policy(token_policy)
        metadata = dict(signal.metadata or {})
        metadata["token_policy"] = {
            "admin_enabled": effective["admin_enabled"],
            "computed_recommendation_status": effective[
                "computed_recommendation_status"
            ],
            "recommendation_status": effective["effective_recommendation_status"],
            "recommendation_reason": effective["effective_recommendation_reason"],
            "size_multiplier": str(effective["size_multiplier"]),
            "max_position_pct_override": str(effective["max_position_pct_override"])
            if effective["max_position_pct_override"] is not None
            else None,
        }
        signal.metadata = metadata
        return signal

    def _load_enabled_user_strategies(self) -> list[dict[str, Any]]:
        stmt = text(
            """
            SELECT
              us.user_id,
              us.strategy_id,
              us.config,
              (
                SELECT tm.tenant_id
                FROM tenant_memberships tm
                WHERE tm.user_id = us.user_id
                ORDER BY tm.created_at ASC
                LIMIT 1
              ) AS tenant_id
            FROM user_strategies us
            JOIN users u ON u.id = us.user_id
            WHERE us.is_enabled = true AND u.is_active = true
            """
        )
        with self._engine.begin() as conn:
            rows = conn.execute(stmt).mappings().all()
        return [dict(r) for r in rows]

    def _load_allowed_symbols(self, user_id: str) -> list[str]:
        stmt = text(
            """
            SELECT DISTINCT p.symbol, p.quote_currency
            FROM user_token_permissions ut
            JOIN platform_token_allowlist p ON p.id = ut.platform_token_id
            WHERE ut.user_id = :user_id
              AND ut.is_enabled = true
              AND p.is_enabled = true
            ORDER BY p.symbol, p.quote_currency
            """
        )
        with self._engine.begin() as conn:
            rows = conn.execute(stmt, {"user_id": user_id}).all()
        return [r.symbol for r in rows]

    def _resolve_symbols(
        self, *, config: dict[str, Any], allowed_symbols: list[str]
    ) -> list[str]:
        requested = config.get("symbols")
        if isinstance(requested, (list, tuple, set)):
            requested_symbols = {str(symbol) for symbol in requested if str(symbol)}
            return [symbol for symbol in allowed_symbols if symbol in requested_symbols]

        requested_symbol = config.get("symbol")
        if requested_symbol:
            symbol = str(requested_symbol)
            return [symbol] if symbol in allowed_symbols else []

        return allowed_symbols

    @staticmethod
    def _coerce_symbol_runtime_states(state: Any) -> dict[str, dict[str, Any]]:
        if isinstance(state, str):
            try:
                state = json.loads(state)
            except Exception:
                return {}
        if not isinstance(state, dict):
            return {}

        symbol_states = state.get("symbols")
        if isinstance(symbol_states, dict):
            return {
                str(symbol): value
                for symbol, value in symbol_states.items()
                if isinstance(value, dict)
            }

        legacy_symbol = state.get("symbol")
        if legacy_symbol:
            return {
                str(legacy_symbol): {
                    "peak_price": state.get("peak_price"),
                    "opened_at": state.get("opened_at"),
                }
            }

        return {}

    @staticmethod
    def _merge_symbol_runtime_states(
        symbol_states: dict[str, dict[str, Any]],
        *,
        position_state: PositionState,
        market: MarketSnapshot,
        now: datetime,
    ) -> dict[str, dict[str, Any]]:
        merged = dict(symbol_states)
        existing = dict(merged.get(position_state.symbol, {}))
        if position_state.quantity > 0:
            baseline_peak = max(
                position_state.entry_price or Decimal("0"),
                position_state.peak_price or Decimal("0"),
                market.current_price,
            )
            position_state.peak_price = baseline_peak if baseline_peak > 0 else None
            if position_state.opened_at is None:
                position_state.opened_at = now
            existing["peak_price"] = str(position_state.peak_price)
            existing["opened_at"] = position_state.opened_at.isoformat()
            merged[position_state.symbol] = existing
            return merged

        position_state.peak_price = None
        position_state.opened_at = None
        existing.pop("peak_price", None)
        existing.pop("opened_at", None)
        if existing:
            merged[position_state.symbol] = existing
        else:
            merged.pop(position_state.symbol, None)
        return merged

    def _load_strategy_runtime_state(
        self,
        *,
        user_id: str,
        strategy_name: str,
        trading_mode: str,
    ) -> dict[str, Any]:
        stmt = text(
            """
            SELECT state
            FROM user_strategy_states
            WHERE user_id = :user_id
              AND strategy_id = :strategy_name
              AND trading_mode = :trading_mode
            LIMIT 1
            """
        )
        with self._engine.begin() as conn:
            row = (
                conn.execute(
                    stmt,
                    {
                        "user_id": user_id,
                        "strategy_name": strategy_name,
                        "trading_mode": trading_mode,
                    },
                )
                .mappings()
                .first()
            )
        state = row["state"] if row else {}
        if isinstance(state, str):
            try:
                state = json.loads(state)
            except Exception:
                return {}
        return state if isinstance(state, dict) else {}

    def _load_position_state(
        self,
        *,
        user_id: str,
        strategy_name: str,
        trading_mode: str,
        symbol: str,
    ) -> PositionState:
        runtime_stmt = text(
            """
            SELECT state
            FROM user_strategy_states
            WHERE user_id = :user_id
              AND strategy_id = :strategy_name
              AND trading_mode = :trading_mode
            LIMIT 1
            """
        )
        position_stmt = text(
            """
            SELECT quantity, avg_entry_price, last_trade_at
            FROM execution_positions
            WHERE user_id = :user_id
              AND strategy_id = :strategy_name
              AND trading_mode = :trading_mode
              AND symbol = :symbol
            LIMIT 1
            """
        )
        with self._engine.begin() as conn:
            runtime_row = (
                conn.execute(
                    runtime_stmt,
                    {
                        "user_id": user_id,
                        "strategy_name": strategy_name,
                        "trading_mode": trading_mode,
                    },
                )
                .mappings()
                .first()
            )
            position_row = (
                conn.execute(
                    position_stmt,
                    {
                        "user_id": user_id,
                        "strategy_name": strategy_name,
                        "trading_mode": trading_mode,
                        "symbol": symbol,
                    },
                )
                .mappings()
                .first()
            )

        symbol_state = self._coerce_symbol_runtime_states(
            runtime_row["state"] if runtime_row else {}
        ).get(symbol, {})

        qty = Decimal(str(position_row["quantity"])) if position_row else Decimal("0")
        entry_price = None
        if position_row and position_row.get("avg_entry_price") is not None:
            raw_entry = Decimal(str(position_row["avg_entry_price"]))
            if raw_entry > 0:
                entry_price = raw_entry

        peak_price = None
        if symbol_state.get("peak_price") is not None:
            raw_peak = Decimal(str(symbol_state["peak_price"]))
            if raw_peak > 0:
                peak_price = raw_peak

        opened_at = None
        opened_at_raw = symbol_state.get("opened_at")
        if opened_at_raw:
            opened_at = (
                datetime.fromisoformat(opened_at_raw.replace("Z", "+00:00"))
                if isinstance(opened_at_raw, str)
                else opened_at_raw
            )
        elif position_row and position_row.get("last_trade_at") is not None:
            last_trade_at = position_row["last_trade_at"]
            opened_at = (
                datetime.fromisoformat(last_trade_at.replace("Z", "+00:00"))
                if isinstance(last_trade_at, str)
                else last_trade_at
            )

        return PositionState(
            symbol=symbol,
            quantity=qty,
            entry_price=entry_price,
            peak_price=peak_price,
            opened_at=opened_at,
        )

    def _sync_position_runtime_state(
        self,
        *,
        user_id: str,
        strategy_name: str,
        trading_mode: str,
        position_state: PositionState,
        market: MarketSnapshot,
        now: datetime,
    ) -> PositionState:
        runtime_stmt = text(
            """
            SELECT state
            FROM user_strategy_states
            WHERE user_id = :user_id
              AND strategy_id = :strategy_name
              AND trading_mode = :trading_mode
            LIMIT 1
            """
        )
        with self._engine.begin() as conn:
            runtime_row = (
                conn.execute(
                    runtime_stmt,
                    {
                        "user_id": user_id,
                        "strategy_name": strategy_name,
                        "trading_mode": trading_mode,
                    },
                )
                .mappings()
                .first()
            )
        symbol_states = self._coerce_symbol_runtime_states(
            runtime_row["state"] if runtime_row else {}
        )

        symbol_states = self._merge_symbol_runtime_states(
            symbol_states,
            position_state=position_state,
            market=market,
            now=now,
        )

        state = {"symbols": symbol_states} if symbol_states else {}

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
                    "user_id": user_id,
                    "strategy_id": strategy_name,
                    "trading_mode": trading_mode,
                    "state": json.dumps(state),
                    "created_at": now,
                    "updated_at": now,
                },
            )
        return position_state

    def _load_market_snapshot(self, symbol: str) -> MarketSnapshot | None:
        bbo_raw = self._redis.get(f"oziebot:md:bbo:{symbol}")
        candle_raw = self._redis.get(
            f"oziebot:md:candle:{self._candle_granularity_sec}:{symbol}"
        )
        if not bbo_raw or not candle_raw:
            return None

        bbo = json.loads(bbo_raw)
        candle = json.loads(candle_raw)

        # Load rolling candle history for MA calculations (newest first)
        history_raw = self._redis.lrange(
            f"oziebot:md:candles:{self._candle_granularity_sec}:{symbol}", 0, 49
        )
        candle_closes: list[float] = []
        candle_volumes: list[float] = []
        candle_highs: list[float] = []
        candle_lows: list[float] = []
        for raw in reversed(history_raw):  # reverse to chronological order
            try:
                c = json.loads(raw)
                candle_closes.append(float(c["close"]))
                candle_volumes.append(float(c.get("volume", 0)))
                candle_highs.append(float(c["high"]))
                candle_lows.append(float(c["low"]))
            except Exception:
                pass

        bid = Decimal(str(bbo["best_bid_price"]))
        ask = Decimal(str(bbo["best_ask_price"]))
        current = (bid + ask) / Decimal("2")
        return MarketSnapshot(
            timestamp=datetime.now(UTC),
            symbol=symbol,
            current_price=current,
            bid_price=bid,
            ask_price=ask,
            volume_24h=Decimal(str(candle.get("volume", "0"))),
            open_price=Decimal(str(candle["open"])),
            high_price=Decimal(str(candle["high"])),
            low_price=Decimal(str(candle["low"])),
            close_price=Decimal(str(candle["close"])),
            candle_closes=candle_closes,
            candle_volumes=candle_volumes,
            candle_highs=candle_highs,
            candle_lows=candle_lows,
        )

    def _generate_signal(
        self,
        *,
        tenant_id: TenantId,
        strategy_name: str,
        trading_mode: TradingMode,
        market: MarketSnapshot,
        position_state: PositionState,
        config: dict[str, Any],
    ) -> StrategySignal:
        strategy = StrategyRegistry.get_strategy(strategy_name)
        signal = strategy.generate_signal(
            StrategyContext(
                tenant_id=tenant_id,
                trading_mode=trading_mode,
                market_snapshot=market,
                position_state=position_state,
            ),
            config,
            signal_id=uuid.uuid4(),
            correlation_id=uuid.uuid4(),
        )
        return signal

    @staticmethod
    def _to_signal_event(
        *,
        run_id: uuid.UUID,
        user_id: uuid.UUID,
        strategy_name: str,
        symbol: str,
        signal: StrategySignal,
        trading_mode: TradingMode,
        timestamp: datetime,
        position_state: PositionState | None = None,
        risk_caps: dict | None = None,
        market: "MarketSnapshot | None" = None,
    ) -> StrategySignalEvent:
        confidence = Decimal(str(signal.confidence))
        suggested_size = Decimal("0")
        if signal.quantity is not None:
            suggested_size = Decimal(str(signal.quantity.amount))
        elif (
            signal.signal_type in {SignalType.CLOSE, SignalType.SELL}
            and position_state is not None
        ):
            suggested_size = abs(position_state.quantity)
        elif signal.metadata and "buy_amount_usd" in signal.metadata:
            price = market.current_price if market is not None else Decimal("0")
            usd_amount = Decimal(str(signal.metadata["buy_amount_usd"])) * confidence
            if price > 0:
                suggested_size = usd_amount / price
        elif signal.metadata and "position_size_fraction" in signal.metadata:
            fraction = Decimal(str(signal.metadata["position_size_fraction"]))
            caps = risk_caps or {}
            max_pos_usd = Decimal(str(caps.get("max_position_usd", 0)))
            price = market.current_price if market is not None else Decimal("0")
            if max_pos_usd > 0 and price > 0:
                # Convert: fraction of max capital → USD → token quantity
                usd_amount = fraction * max_pos_usd * confidence
                suggested_size = usd_amount / price

        return StrategySignalEvent(
            signal_id=signal.signal_id,
            run_id=run_id,
            user_id=user_id,
            strategy_name=strategy_name,
            symbol=symbol,
            action=signal.signal_type,
            confidence=float(signal.confidence),
            suggested_size=suggested_size,
            reasoning_metadata={
                "reason": signal.reason,
                "signal_metadata": signal.metadata or {},
                "token_policy": (signal.metadata or {}).get("token_policy"),
            },
            trading_mode=trading_mode,
            timestamp=timestamp,
        )

    def _persist_run(
        self,
        *,
        run_id: uuid.UUID,
        user_id: str,
        strategy_name: str,
        symbol: str,
        trading_mode: str,
        status: str,
        trace_id: str,
        metadata: dict[str, Any] | None,
        started_at: datetime,
        completed_at: datetime,
    ) -> None:
        stmt = text(
            """
            INSERT INTO strategy_runs (
              id, run_id, user_id, strategy_name, symbol, trading_mode,
              status, trace_id, metadata, started_at, completed_at
            ) VALUES (
              :id, :run_id, :user_id, :strategy_name, :symbol, :trading_mode,
              :status, :trace_id, :metadata, :started_at, :completed_at
            )
            """
        )
        with self._engine.begin() as conn:
            conn.execute(
                stmt,
                {
                    "id": uuid.uuid4().hex,
                    "run_id": str(run_id),
                    "user_id": user_id,
                    "strategy_name": strategy_name,
                    "symbol": symbol,
                    "trading_mode": trading_mode,
                    "status": status,
                    "trace_id": trace_id,
                    "metadata": json.dumps(metadata) if metadata else None,
                    "started_at": started_at,
                    "completed_at": completed_at,
                },
            )

    def _persist_signal(self, event: StrategySignalEvent) -> None:
        stmt = text(
            """
            INSERT INTO strategy_signals (
              id, signal_id, run_id, user_id, strategy_name, symbol,
              action, confidence, suggested_size, reasoning_metadata,
              trading_mode, timestamp
            ) VALUES (
              :id, :signal_id, :run_id, :user_id, :strategy_name, :symbol,
              :action, :confidence, :suggested_size, :reasoning_metadata,
              :trading_mode, :timestamp
            )
            """
        )
        with self._engine.begin() as conn:
            conn.execute(
                stmt,
                {
                    "id": uuid.uuid4().hex,
                    "signal_id": str(event.signal_id),
                    "run_id": str(event.run_id),
                    "user_id": str(event.user_id),
                    "strategy_name": event.strategy_name,
                    "symbol": event.symbol,
                    "action": event.action.value,
                    "confidence": event.confidence,
                    "suggested_size": str(event.suggested_size),
                    "reasoning_metadata": json.dumps(event.reasoning_metadata)
                    if event.reasoning_metadata
                    else None,
                    "trading_mode": event.trading_mode.value,
                    "timestamp": event.timestamp,
                },
            )


def build_runner(database_url: str, redis_client) -> StrategyRunner:
    engine = create_engine(database_url)
    return StrategyRunner(engine=engine, redis_client=redis_client)
