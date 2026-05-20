from __future__ import annotations

import hashlib
import json
import logging
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from collections.abc import Callable
from typing import Any

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from oziebot_common.queues import (
    QueueNames,
    execution_event_to_json,
    notification_event_to_json,
    risk_decision_from_json,
    trade_intent_from_json,
)
from oziebot_common.reason_codes import normalize_reason_code
from oziebot_common.worker_outbox import enqueue_worker_payload
from oziebot_common.strategy_defaults import normalize_platform_strategy_config
from oziebot_common.token_policy import resolve_effective_token_policy
from oziebot_common.trade_intelligence import (
    DecisionAuditDecision,
    DecisionAuditStage,
    LifecycleEventStatus,
    StrategyLifecycleStage,
    extract_signal_snapshot_id,
    persist_decision_audit,
    persist_strategy_lifecycle_event,
    persist_trade_outcome_feature,
)
from oziebot_domain.events import NotificationEvent, NotificationEventType
from oziebot_domain.execution import (
    ExecutionEvent,
    ExecutionOrderStatus,
    ExecutionRequest,
    ExecutionSubmission,
)
from oziebot_domain.risk import RiskDecision, RiskOutcome
from oziebot_domain.trading import OrderType, Side, Venue
from oziebot_domain.trading_mode import TradingMode

from oziebot_execution_engine.adapters import ExecutionAdapter
from oziebot_execution_engine.credential_crypto import CredentialCrypto
from oziebot_execution_engine.state_machine import ensure_transition

log = logging.getLogger("execution-engine.service")

MAX_COINBASE_DECIMAL_PLACES = 8
COINBASE_MIN_NOTIONAL_USD = Decimal("1.00")
EXECUTION_VALIDATION_FAILURE_CODE = "execution_validation_failed"


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _to_hex(uid: Any) -> str:
    """Convert a UUID (with or without dashes) to hex string for SQLite."""
    if isinstance(uid, uuid.UUID):
        return uid.hex
    return str(uid).replace("-", "")


def _money_to_cents(amount: Decimal) -> int:
    return int((amount * Decimal("100")).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


@dataclass(frozen=True)
class ProcessResult:
    order_id: str
    state: ExecutionOrderStatus
    duplicated: bool


@dataclass(frozen=True)
class ExecutionValidationFailure:
    code: str
    detail: str


class ExecutionService:
    def __init__(
        self,
        settings,
        engine,
        *,
        runtime_kv,
        paper_adapter: ExecutionAdapter,
        live_adapter: ExecutionAdapter,
        enqueue_fn: Callable[[str, dict[str, Any]], None] | None = None,
    ) -> None:
        self._settings = settings
        self._engine = engine
        self._kv = runtime_kv
        self._paper_adapter = paper_adapter
        self._live_adapter = live_adapter
        self._enqueue_fn = enqueue_fn
        self._crypto = CredentialCrypto(settings.exchange_credentials_encryption_key)
        self._metrics: Counter[str] = Counter()
        self._rejection_reasons: Counter[str] = Counter()

    def _enqueue(self, queue_name: str, payload: dict[str, Any]) -> None:
        if self._engine is None:
            return
        if self._enqueue_fn is not None:
            self._enqueue_fn(queue_name, payload)
            return
        enqueue_worker_payload(self._engine, queue_name, payload)

    @staticmethod
    def build_idempotency_key(intent_id: str, trading_mode: TradingMode) -> str:
        raw = f"{trading_mode.value}:{intent_id}".encode()
        return hashlib.sha256(raw).hexdigest()

    @staticmethod
    def build_client_order_id(intent_id: str, trading_mode: TradingMode) -> str:
        compact = intent_id.replace("-", "")[:24]
        return f"ozie-{trading_mode.value}-{compact}"

    def load_live_credentials(self, tenant_id: uuid.UUID) -> tuple[str, str]:
        if self._engine is None:
            raise RuntimeError("DATABASE_URL is required")
        if not self._crypto.configured:
            raise RuntimeError("EXCHANGE_CREDENTIALS_ENCRYPTION_KEY is not configured")
        stmt = text(
            """
            SELECT api_key_name, encrypted_secret, validation_status, can_trade
            FROM exchange_connections
            WHERE tenant_id = :tenant_id AND provider = 'coinbase'
            LIMIT 1
            """
        )
        with self._engine.begin() as conn:
            row = conn.execute(stmt, {"tenant_id": str(tenant_id)}).mappings().first()
        if row is None:
            raise ValueError("Coinbase connection not found")
        if row["validation_status"] != "valid" or not row["can_trade"]:
            raise ValueError("Coinbase connection is not trade-enabled")
        return str(row["api_key_name"]), self._crypto.decrypt(
            row["encrypted_secret"]
        ).decode("utf-8")

    def process_queue_message(self, raw: dict[str, Any]) -> ProcessResult:
        intent = trade_intent_from_json(raw["intent"])
        risk = risk_decision_from_json(raw["risk"])
        request = self._build_request(
            intent=intent.model_dump(mode="json"),
            risk=risk,
            trace_id=str(raw.get("trace_id") or risk.trace_id),
        )
        return self.process_request(request)

    def process_request(self, request: ExecutionRequest) -> ProcessResult:
        if self._engine is None:
            raise RuntimeError("DATABASE_URL is required")

        existing = self._get_existing_order(request.intent_id, request.trading_mode)
        if existing is not None:
            return ProcessResult(
                order_id=str(existing["id"]),
                state=ExecutionOrderStatus(existing["state"]),
                duplicated=True,
            )

        original_quantity = request.quantity
        request, policy_failure = self._apply_token_strategy_policy(request)
        reserve_cents = self._estimate_reserve_cents(request)
        validation_failure = self._validate_request(
            request=request,
            reserve_cents=reserve_cents,
        )
        if validation_failure is not None:
            request = self._annotate_validation_failure(request, validation_failure)
        now = _utcnow()
        order_id = str(uuid.uuid4())
        policy_failure_code = (
            normalize_reason_code("token_strategy_policy", reason_detail=policy_failure)
            if policy_failure is not None
            else None
        )
        validation_failure_code = (
            self._validation_failure_reason_code(validation_failure)
            if validation_failure is not None
            else None
        )
        self._persist_lifecycle_event(
            request=request,
            stage=StrategyLifecycleStage.EXECUTION_REQUESTED,
            status=LifecycleEventStatus.OBSERVED,
            occurred_at=now,
            order_id=order_id,
            metadata={"quantity": str(request.quantity)},
        )
        if request.side == Side.SELL:
            exit_reason = self._resolve_exit_reason(request)
            trigger_stage = self._exit_trigger_stage(exit_reason)
            if trigger_stage is not None:
                self._persist_lifecycle_event(
                    request=request,
                    stage=trigger_stage,
                    status=LifecycleEventStatus.OBSERVED,
                    occurred_at=now,
                    order_id=order_id,
                    reason_code=exit_reason,
                    reason_detail=exit_reason,
                )
            self._persist_lifecycle_event(
                request=request,
                stage=StrategyLifecycleStage.EXIT_EXECUTION_REQUESTED,
                status=LifecycleEventStatus.OBSERVED,
                occurred_at=now,
                order_id=order_id,
                reason_code=exit_reason,
                reason_detail=exit_reason,
            )
        insert_stmt = text(
            """
            INSERT INTO execution_orders (
              id, intent_id, correlation_id, tenant_id, user_id, strategy_id, symbol, side, order_type,
              trading_mode, venue, state, quantity, requested_notional_cents, reserved_cash_cents,
              locked_cash_cents, filled_quantity, avg_fill_price, fees_cents, expected_gross_edge_bps,
              estimated_fee_bps, estimated_slippage_bps, estimated_total_cost_bps, expected_net_edge_bps,
              execution_preference, fallback_behavior, maker_timeout_seconds, limit_price_offset_bps,
              actual_fill_type, fallback_triggered, idempotency_key,
              client_order_id, venue_order_id, failure_code, failure_detail, trace_id,
              intent_payload, risk_payload, adapter_payload, created_at, updated_at, submitted_at,
              completed_at, cancelled_at, failed_at
            ) VALUES (
              :id, :intent_id, :correlation_id, :tenant_id, :user_id, :strategy_id, :symbol, :side, :order_type,
              :trading_mode, :venue, :state, :quantity, :requested_notional_cents, :reserved_cash_cents,
              :locked_cash_cents, :filled_quantity, :avg_fill_price, :fees_cents, :expected_gross_edge_bps,
              :estimated_fee_bps, :estimated_slippage_bps, :estimated_total_cost_bps, :expected_net_edge_bps,
              :execution_preference, :fallback_behavior, :maker_timeout_seconds, :limit_price_offset_bps,
              NULL, :fallback_triggered, :idempotency_key,
              :client_order_id, NULL, :failure_code, :failure_detail, :trace_id,
              :intent_payload, :risk_payload, :adapter_payload, :created_at, :updated_at, NULL,
              NULL, NULL, :failed_at
            )
            """
        )
        try:
            with self._engine.begin() as conn:
                conn.execute(
                    insert_stmt,
                    {
                        "id": order_id,
                        "intent_id": str(request.intent_id),
                        "correlation_id": str(request.risk.run_id),
                        "tenant_id": _to_hex(request.tenant_id),
                        "user_id": _to_hex(request.user_id),
                        "strategy_id": request.strategy_id,
                        "symbol": request.symbol,
                        "side": request.side.value,
                        "order_type": request.order_type.value,
                        "trading_mode": request.trading_mode.value,
                        "venue": request.venue.value,
                        "state": (
                            ExecutionOrderStatus.FAILED.value
                            if policy_failure is not None
                            or validation_failure is not None
                            else ExecutionOrderStatus.CREATED.value
                        ),
                        "quantity": str(request.quantity),
                        "requested_notional_cents": reserve_cents or 0,
                        "reserved_cash_cents": 0,
                        "locked_cash_cents": 0,
                        "filled_quantity": "0",
                        "avg_fill_price": None,
                        "fees_cents": 0,
                        "expected_gross_edge_bps": request.expected_gross_edge_bps,
                        "estimated_fee_bps": request.estimated_fee_bps,
                        "estimated_slippage_bps": request.estimated_slippage_bps,
                        "estimated_total_cost_bps": request.estimated_total_cost_bps,
                        "expected_net_edge_bps": request.expected_net_edge_bps,
                        "execution_preference": request.execution_preference,
                        "fallback_behavior": request.fallback_behavior,
                        "maker_timeout_seconds": request.maker_timeout_seconds,
                        "limit_price_offset_bps": request.limit_price_offset_bps,
                        "fallback_triggered": False,
                        "idempotency_key": request.idempotency_key,
                        "client_order_id": request.client_order_id,
                        "failure_code": (
                            policy_failure_code
                            if policy_failure_code is not None
                            else validation_failure_code
                        ),
                        "failure_detail": (
                            policy_failure
                            if policy_failure is not None
                            else validation_failure.detail
                            if validation_failure is not None
                            else None
                        ),
                        "trace_id": request.trace_id,
                        "intent_payload": json.dumps(
                            request.intent_payload, default=str
                        ),
                        "risk_payload": json.dumps(
                            request.risk.model_dump(mode="json"), default=str
                        ),
                        "adapter_payload": json.dumps({}, default=str),
                        "created_at": now,
                        "updated_at": now,
                        "failed_at": (
                            now
                            if policy_failure is not None
                            or validation_failure is not None
                            else None
                        ),
                    },
                )
        except IntegrityError:
            existing = self._get_existing_order(request.intent_id, request.trading_mode)
            if existing is None:
                raise
            return ProcessResult(
                order_id=str(existing["id"]),
                state=ExecutionOrderStatus(existing["state"]),
                duplicated=True,
            )

        if request.quantity != original_quantity:
            self._persist_decision_audit_record(
                request=request,
                decision=DecisionAuditDecision.REDUCED,
                reason_code=policy_failure_code or "policy_blocked",
                reason_detail="Execution quantity adjusted by token policy",
                size_before=original_quantity,
                size_after=request.quantity,
                created_at=now,
            )

        if policy_failure is not None:
            self._persist_lifecycle_event(
                request=request,
                stage=StrategyLifecycleStage.POLICY_VALIDATION,
                status=LifecycleEventStatus.FAILED,
                occurred_at=now,
                order_id=order_id,
                reason_code=policy_failure_code or "policy_blocked",
                reason_detail=policy_failure,
            )
            self._persist_decision_audit_record(
                request=request,
                decision=DecisionAuditDecision.REJECTED,
                reason_code=policy_failure_code or "policy_blocked",
                reason_detail=policy_failure,
                size_before=original_quantity,
                size_after=Decimal("0"),
                created_at=now,
            )
            self._record_metric(
                rejected=True,
                rejection_reason=policy_failure_code or "policy_blocked",
            )
            self._persist_lifecycle_event(
                request=request,
                stage=StrategyLifecycleStage.EXECUTION_FAILED,
                status=LifecycleEventStatus.FAILED,
                occurred_at=now,
                order_id=order_id,
                reason_code=policy_failure_code or "policy_blocked",
                reason_detail=policy_failure,
            )
            self._emit_event(
                order_id,
                request,
                ExecutionOrderStatus.FAILED,
                detail=policy_failure,
                payload={"failure_code": policy_failure_code or "policy_blocked"},
            )
            return ProcessResult(
                order_id=order_id,
                state=ExecutionOrderStatus.FAILED,
                duplicated=False,
            )

        if validation_failure is not None:
            self._persist_decision_audit_record(
                request=request,
                decision=DecisionAuditDecision.REJECTED,
                reason_code=validation_failure_code
                or EXECUTION_VALIDATION_FAILURE_CODE,
                reason_detail=validation_failure.detail,
                size_before=original_quantity,
                size_after=Decimal("0"),
                created_at=now,
            )
            self._record_metric(
                rejected=True,
                rejection_reason=validation_failure_code
                or EXECUTION_VALIDATION_FAILURE_CODE,
            )
            self._persist_lifecycle_event(
                request=request,
                stage=StrategyLifecycleStage.EXECUTION_FAILED,
                status=LifecycleEventStatus.FAILED,
                occurred_at=now,
                order_id=order_id,
                reason_code=validation_failure_code
                or EXECUTION_VALIDATION_FAILURE_CODE,
                reason_detail=validation_failure.detail,
                metadata={
                    "validation_code": validation_failure.code,
                    "validation_detail": validation_failure.detail,
                },
            )
            self._emit_event(
                order_id,
                request,
                ExecutionOrderStatus.FAILED,
                detail=validation_failure.detail,
                payload={
                    "failure_code": validation_failure_code
                    or EXECUTION_VALIDATION_FAILURE_CODE,
                    "validation_code": validation_failure.code,
                },
            )
            return ProcessResult(
                order_id=order_id,
                state=ExecutionOrderStatus.FAILED,
                duplicated=False,
            )

        self._persist_decision_audit_record(
            request=request,
            decision=DecisionAuditDecision.EMITTED,
            reason_code="order_created",
            reason_detail="Execution request persisted",
            size_before=request.quantity,
            size_after=request.quantity,
            created_at=now,
        )
        self._emit_event(
            order_id, request, ExecutionOrderStatus.CREATED, detail="Order created"
        )

        if reserve_cents > 0:
            self._reserve_capital(request, reserve_cents, order_id)
            self._set_order_state(
                order_id,
                ExecutionOrderStatus.CAPITAL_RESERVED,
                reserved_cash_cents=reserve_cents,
            )
            self._emit_event(
                order_id,
                request,
                ExecutionOrderStatus.CAPITAL_RESERVED,
                detail="Capital reserved",
            )

        pre_submit_policy = self._pre_submit_policy_check(request)
        if pre_submit_policy["failure"] is not None:
            failure_detail = str(pre_submit_policy["failure"])
            self._handle_failure(
                order_id,
                request,
                reserve_cents,
                ExecutionSubmission(
                    status=ExecutionOrderStatus.FAILED,
                    venue=request.venue,
                    raw_payload={"policy_check": pre_submit_policy["snapshot"]},
                    failure_code=normalize_reason_code(
                        "token_strategy_policy",
                        reason_detail=str(pre_submit_policy["failure"]),
                    ),
                    failure_detail=failure_detail,
                ),
            )
            return ProcessResult(
                order_id=order_id,
                state=ExecutionOrderStatus.FAILED,
                duplicated=False,
            )

        adapter = (
            self._paper_adapter
            if request.trading_mode == TradingMode.PAPER
            else self._live_adapter
        )
        submission = adapter.submit(request)
        submission = submission.model_copy(
            update={
                "raw_payload": self._merge_payloads(
                    submission.raw_payload,
                    {"policy_check": pre_submit_policy["snapshot"]},
                )
            }
        )
        return self._apply_submission(request, order_id, reserve_cents, submission)

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

    def _apply_token_strategy_policy(
        self,
        request: ExecutionRequest,
    ) -> tuple[ExecutionRequest, str | None]:
        policy_row = self._load_token_strategy_policy(
            symbol=request.symbol,
            strategy_id=request.strategy_id,
        )
        effective = resolve_effective_token_policy(
            policy_row,
            trading_mode=request.trading_mode.value,
        )
        intent_payload = dict(request.intent_payload)
        metadata = dict(intent_payload.get("metadata") or {})
        metadata["token_policy_execution"] = {
            "policy_id": policy_row.get("policy_id") if policy_row else None,
            "policy_updated_at": str(policy_row.get("policy_updated_at"))
            if policy_row and policy_row.get("policy_updated_at") is not None
            else None,
            "is_enabled": effective["is_enabled"],
            "admin_enabled": effective["admin_enabled"],
            "effective_recommendation_status": effective[
                "effective_recommendation_status"
            ],
            "recommendation_status": effective["effective_recommendation_status"],
            "recommendation_reason": effective["effective_recommendation_reason"],
            "size_multiplier": str(effective["size_multiplier"]),
            "configured_size_multiplier": str(effective["configured_size_multiplier"]),
            "max_position_usd_override": str(effective["max_position_usd_override"])
            if effective["max_position_usd_override"] is not None
            else None,
            "max_position_pct_override": str(effective["max_position_pct_override"])
            if effective["max_position_pct_override"] is not None
            else None,
            "checked_at": _utcnow().isoformat(),
        }
        intent_payload["metadata"] = metadata
        request = request.model_copy(update={"intent_payload": intent_payload})

        if request.side != Side.BUY:
            return request, None

        if not effective["admin_enabled"]:
            return request, "Execution rejected: token strategy disabled by admin"
        if effective["effective_recommendation_status"] == "blocked":
            reason = (
                effective["effective_recommendation_reason"]
                or "blocked by token strategy policy"
            )
            return request, f"Execution rejected: token strategy blocked ({reason})"

        adjusted_quantity = request.quantity
        if effective["effective_recommendation_status"] == "discouraged":
            adjusted_quantity = (
                adjusted_quantity * effective["size_multiplier"]
            ).quantize(
                Decimal("0.00000001"),
                rounding=ROUND_DOWN,
            )
            if adjusted_quantity <= 0:
                return (
                    request,
                    "Execution rejected: token strategy policy reduced size to zero",
                )

        max_position_usd_override = effective["max_position_usd_override"]
        max_position_pct_override = effective["max_position_pct_override"]
        if (
            max_position_usd_override is not None
            or max_position_pct_override is not None
        ):
            if request.price_hint is None or request.price_hint <= 0:
                return (
                    request,
                    "Execution rejected: missing price hint for token strategy position cap",
                )
            total_capital_cents = self._load_total_capital_cents(
                user_id=request.user_id,
                trading_mode=request.trading_mode,
            )
            if max_position_usd_override is not None:
                max_position_cents = int(
                    (max_position_usd_override * Decimal("100")).quantize(
                        Decimal("1"),
                        rounding=ROUND_DOWN,
                    )
                )
            else:
                max_position_cents = int(
                    (
                        Decimal(str(total_capital_cents)) * max_position_pct_override
                    ).quantize(
                        Decimal("1"),
                        rounding=ROUND_DOWN,
                    )
                )
            current_exposure_cents = self._load_strategy_token_exposure_cents(request)
            remaining_cents = max_position_cents - current_exposure_cents
            if remaining_cents <= 0:
                return (
                    request,
                    "Execution rejected: token strategy position override cap reached",
                )
            max_quantity = (
                (Decimal(str(remaining_cents)) / Decimal("100")) / request.price_hint
            ).quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)
            adjusted_quantity = min(adjusted_quantity, max_quantity)
            if adjusted_quantity <= 0:
                return (
                    request,
                    "Execution rejected: token strategy position override cap reached",
                )

        if adjusted_quantity == request.quantity:
            return request, None

        metadata["token_policy_execution"]["adjusted_quantity"] = str(adjusted_quantity)
        intent_payload["quantity"] = {
            **dict(intent_payload.get("quantity") or {}),
            "amount": str(adjusted_quantity),
        }
        intent_payload["metadata"] = metadata
        return (
            request.model_copy(
                update={
                    "quantity": adjusted_quantity,
                    "intent_payload": intent_payload,
                }
            ),
            None,
        )

    def _load_token_strategy_policy(
        self,
        *,
        symbol: str,
        strategy_id: str,
    ) -> dict[str, Any] | None:
        stmt = text(
            """
            SELECT
              tsp.id AS policy_id,
              tsp.updated_at AS policy_updated_at,
              tsp.admin_enabled,
              tsp.recommendation_status,
              tsp.recommendation_reason,
              tsp.recommendation_status_override,
              tsp.recommendation_reason_override,
              tsp.size_multiplier,
              tsp.max_position_usd_override,
              tsp.max_position_pct_override
            FROM platform_token_allowlist p
            LEFT JOIN token_strategy_policy tsp
              ON tsp.token_id = p.id
             AND tsp.strategy_id = :strategy_id
            WHERE p.symbol = :symbol
            LIMIT 1
            """
        )
        with self._engine.begin() as conn:
            row = (
                conn.execute(
                    stmt,
                    {"symbol": symbol, "strategy_id": strategy_id},
                )
                .mappings()
                .first()
            )
        return dict(row) if row else None

    def _load_total_capital_cents(
        self,
        *,
        user_id: uuid.UUID,
        trading_mode: TradingMode,
    ) -> int:
        with self._engine.begin() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT COALESCE(SUM(assigned_capital_cents), 0) AS total
                    FROM strategy_capital_buckets
                    WHERE user_id = :user_id
                      AND trading_mode = :trading_mode
                    """
                ),
                {
                    "user_id": _to_hex(user_id),
                    "trading_mode": trading_mode.value,
                },
            ).first()
        return int(row.total or 0) if row is not None else 0

    def _load_strategy_token_exposure_cents(self, request: ExecutionRequest) -> int:
        with self._engine.begin() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT COALESCE(SUM(CAST(quantity AS NUMERIC) * CAST(avg_entry_price AS NUMERIC)), 0) AS total
                    FROM execution_positions
                    WHERE user_id = :user_id
                      AND strategy_id = :strategy_id
                      AND symbol = :symbol
                      AND trading_mode = :trading_mode
                      AND CAST(quantity AS NUMERIC) > 0
                    """
                ),
                {
                    "user_id": _to_hex(request.user_id),
                    "strategy_id": request.strategy_id,
                    "symbol": request.symbol,
                    "trading_mode": request.trading_mode.value,
                },
            ).first()
        return int(
            (
                Decimal(str(row.total if row is not None else 0)) * Decimal("100")
            ).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
        )

    def enforce_runtime_controls(self) -> int:
        if self._engine is None:
            raise RuntimeError("DATABASE_URL is required")
        enforced = 0
        with self._engine.begin() as conn:
            positions = (
                conn.execute(
                    text(
                        """
                    SELECT *
                    FROM execution_positions
                    WHERE strategy_id = 'day_trading'
                      AND CAST(quantity AS NUMERIC) > 0
                    """
                    )
                )
                .mappings()
                .all()
            )
        for row in positions:
            if self._enforce_day_trading_position_age(dict(row)):
                enforced += 1
        return enforced

    def _build_request(
        self, *, intent: dict[str, Any], risk: RiskDecision, trace_id: str
    ) -> ExecutionRequest:
        trading_mode = TradingMode(intent["trading_mode"])
        intent_id = str(intent["intent_id"])
        fee_economics = dict(intent.get("metadata", {}).get("fee_economics") or {})
        return ExecutionRequest(
            intent_id=intent["intent_id"],
            trace_id=trace_id,
            user_id=risk.user_id,
            risk=risk,
            tenant_id=intent["tenant_id"],
            trading_mode=trading_mode,
            strategy_id=intent["strategy_id"],
            symbol=intent["instrument"]["symbol"],
            side=intent["side"],
            order_type=intent["order_type"],
            quantity=intent["quantity"]["amount"],
            price_hint=self._market_price_hint(
                intent["instrument"]["symbol"], intent["side"]
            ),
            execution_preference=str(
                fee_economics.get("execution_preference", "maker_preferred")
            ),
            fallback_behavior=str(
                fee_economics.get("fallback_behavior", "convert_to_taker")
            ),
            maker_timeout_seconds=int(
                fee_economics.get("maker_timeout_seconds", 0) or 0
            ),
            limit_price_offset_bps=int(
                fee_economics.get("limit_price_offset_bps", 0) or 0
            ),
            expected_gross_edge_bps=int(
                fee_economics.get("expected_gross_edge_bps", 0) or 0
            ),
            estimated_fee_bps=int(fee_economics.get("estimated_fee_bps", 0) or 0),
            estimated_slippage_bps=int(
                fee_economics.get("estimated_slippage_bps", 0) or 0
            ),
            estimated_total_cost_bps=int(
                fee_economics.get("estimated_total_cost_bps", 0) or 0
            ),
            expected_net_edge_bps=int(
                fee_economics.get("expected_net_edge_bps", 0) or 0
            ),
            fee_profile=fee_economics,
            idempotency_key=self.build_idempotency_key(intent_id, trading_mode),
            client_order_id=self.build_client_order_id(intent_id, trading_mode),
            intent_payload=intent,
        )

    def _market_price_hint(self, symbol: str, side: str) -> Decimal | None:
        raw = self._kv.get(f"oziebot:md:bbo:{symbol}") if self._kv else None
        if not raw:
            return None
        payload = json.loads(raw)
        if side == Side.BUY.value:
            price = payload.get("best_ask_price")
        else:
            price = payload.get("best_bid_price")
        return Decimal(str(price)) if price is not None else None

    def _estimate_reserve_cents(self, request: ExecutionRequest) -> int:
        if request.side != Side.BUY:
            return 0
        price = request.price_hint or Decimal("0")
        if price <= 0:
            return 0
        return _money_to_cents(request.quantity * price)

    def _validate_request(
        self,
        *,
        request: ExecutionRequest,
        reserve_cents: int,
    ) -> ExecutionValidationFailure | None:
        if not request.quantity.is_finite():
            return ExecutionValidationFailure(
                code="non_finite_quantity",
                detail="Execution rejected: quantity must be finite",
            )
        if request.quantity <= 0:
            return ExecutionValidationFailure(
                code="invalid_quantity",
                detail="Execution rejected: quantity must be greater than zero",
            )
        if self._decimal_places(request.quantity) > MAX_COINBASE_DECIMAL_PLACES:
            return ExecutionValidationFailure(
                code="quantity_precision_exceeded",
                detail=(
                    "Execution rejected: quantity precision exceeds Coinbase-supported "
                    f"{MAX_COINBASE_DECIMAL_PLACES} decimal places"
                ),
            )
        if request.side != Side.BUY:
            return None

        price = request.price_hint
        if price is None:
            return ExecutionValidationFailure(
                code="missing_price_hint",
                detail="Execution rejected: missing price hint for buy execution",
            )
        if not price.is_finite():
            return ExecutionValidationFailure(
                code="non_finite_price_hint",
                detail="Execution rejected: price hint must be finite",
            )
        if price <= 0:
            return ExecutionValidationFailure(
                code="invalid_price_hint",
                detail="Execution rejected: price hint must be greater than zero",
            )

        requested_notional = request.quantity * price
        if not requested_notional.is_finite():
            return ExecutionValidationFailure(
                code="non_finite_notional",
                detail="Execution rejected: requested notional must be finite",
            )
        if requested_notional <= 0:
            return ExecutionValidationFailure(
                code="invalid_notional",
                detail="Execution rejected: requested notional must be greater than zero",
            )
        if reserve_cents <= 0:
            return ExecutionValidationFailure(
                code="notional_rounded_to_zero",
                detail=(
                    "Execution rejected: requested notional rounded to zero cents after "
                    "precision checks"
                ),
            )
        if reserve_cents < _money_to_cents(COINBASE_MIN_NOTIONAL_USD):
            return ExecutionValidationFailure(
                code="below_minimum_notional",
                detail=(
                    "Execution rejected: requested notional is below Coinbase minimum "
                    f"order size of ${COINBASE_MIN_NOTIONAL_USD}"
                ),
            )
        if not self._has_sufficient_buying_power(request, reserve_cents):
            return ExecutionValidationFailure(
                code="insufficient_allocation",
                detail=(
                    "Execution rejected: insufficient available cash or buying power "
                    "for requested notional"
                ),
            )
        return None

    @staticmethod
    def _validation_failure_reason_code(
        failure: ExecutionValidationFailure | None,
    ) -> str:
        return normalize_reason_code(
            EXECUTION_VALIDATION_FAILURE_CODE,
            reason_detail=failure.detail if failure is not None else None,
        )

    def _annotate_validation_failure(
        self,
        request: ExecutionRequest,
        failure: ExecutionValidationFailure,
    ) -> ExecutionRequest:
        intent_payload = dict(request.intent_payload)
        metadata = dict(intent_payload.get("metadata") or {})
        metadata["execution_validation_failure"] = {
            "code": failure.code,
            "detail": failure.detail,
            "checked_at": _utcnow().isoformat(),
        }
        intent_payload["metadata"] = metadata
        return request.model_copy(update={"intent_payload": intent_payload})

    @staticmethod
    def _decimal_places(value: Decimal) -> int:
        exponent = value.normalize().as_tuple().exponent
        return 0 if exponent >= 0 else -exponent

    def _pre_submit_policy_check(self, request: ExecutionRequest) -> dict[str, Any]:
        policy_row = self._load_token_strategy_policy(
            symbol=request.symbol,
            strategy_id=request.strategy_id,
        )
        effective = resolve_effective_token_policy(
            policy_row,
            trading_mode=request.trading_mode.value,
        )
        snapshot = {
            "policy_id": policy_row.get("policy_id") if policy_row else None,
            "policy_updated_at": str(policy_row.get("policy_updated_at"))
            if policy_row and policy_row.get("policy_updated_at") is not None
            else None,
            "checked_at": _utcnow().isoformat(),
            "admin_enabled": effective["admin_enabled"],
            "effective_recommendation_status": effective[
                "effective_recommendation_status"
            ],
            "effective_recommendation_reason": effective[
                "effective_recommendation_reason"
            ],
        }
        failure: str | None = None
        if request.side == Side.BUY:
            if not effective["admin_enabled"]:
                failure = "Execution rejected: token strategy disabled by admin"
            elif effective["effective_recommendation_status"] == "blocked":
                reason = (
                    effective["effective_recommendation_reason"]
                    or "blocked by token strategy policy"
                )
                failure = f"Execution rejected: token strategy blocked ({reason})"
        return {"snapshot": snapshot, "failure": failure}

    @staticmethod
    def _merge_payloads(
        base_payload: dict[str, Any] | None,
        extra_payload: dict[str, Any] | None,
    ) -> dict[str, Any]:
        merged = dict(base_payload or {})
        if extra_payload:
            merged.update(extra_payload)
        return merged

    def _has_sufficient_buying_power(
        self, request: ExecutionRequest, reserve_cents: int
    ) -> bool:
        bucket = self._load_capital_bucket(request)
        if bucket is None:
            return False
        return reserve_cents <= min(
            int(bucket["available_cash_cents"]),
            int(bucket["available_buying_power_cents"]),
        )

    def _load_capital_bucket(self, request: ExecutionRequest) -> dict[str, Any] | None:
        with self._engine.begin() as conn:
            row = (
                conn.execute(
                    text(
                        """
                        SELECT available_cash_cents, reserved_cash_cents, available_buying_power_cents
                        FROM strategy_capital_buckets
                        WHERE user_id = :user_id
                          AND strategy_id = :strategy_id
                          AND trading_mode = :trading_mode
                        LIMIT 1
                        """
                    ),
                    {
                        "user_id": _to_hex(request.user_id),
                        "strategy_id": request.strategy_id,
                        "trading_mode": request.trading_mode.value,
                    },
                )
                .mappings()
                .first()
            )
        return dict(row) if row is not None else None

    def build_reconciliation_report(
        self,
        *,
        user_id: uuid.UUID | str | None = None,
        strategy_id: str | None = None,
        symbol: str | None = None,
        trading_mode: TradingMode | str | None = None,
    ) -> dict[str, Any]:
        if self._engine is None:
            raise RuntimeError("DATABASE_URL is required")

        filters: list[str] = []
        params: dict[str, Any] = {}
        if user_id is not None:
            filters.append("user_id = :user_id")
            params["user_id"] = _to_hex(user_id)
        if strategy_id is not None:
            filters.append("strategy_id = :strategy_id")
            params["strategy_id"] = strategy_id
        if symbol is not None:
            filters.append("symbol = :symbol")
            params["symbol"] = symbol
        if trading_mode is not None:
            filters.append("trading_mode = :trading_mode")
            params["trading_mode"] = (
                trading_mode.value
                if isinstance(trading_mode, TradingMode)
                else str(trading_mode)
            )
        where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""

        with self._engine.begin() as conn:
            trade_rows = [
                dict(row)
                for row in conn.execute(
                    text(
                        f"""
                        SELECT user_id, strategy_id, symbol, trading_mode, side, quantity,
                               position_quantity_after, realized_pnl_cents, executed_at
                        FROM execution_trades
                        {where_clause}
                        ORDER BY executed_at ASC
                        """
                    ),
                    params,
                )
                .mappings()
                .all()
            ]
            position_rows = [
                dict(row)
                for row in conn.execute(
                    text(
                        f"""
                        SELECT user_id, strategy_id, symbol, trading_mode, quantity,
                               avg_entry_price, realized_pnl_cents, updated_at
                        FROM execution_positions
                        {where_clause}
                        """
                    ),
                    params,
                )
                .mappings()
                .all()
            ]
            bucket_filters = [
                clause for clause in filters if not clause.startswith("symbol =")
            ]
            bucket_params = {
                key: value for key, value in params.items() if key != "symbol"
            }
            bucket_where = (
                f"WHERE {' AND '.join(bucket_filters)}" if bucket_filters else ""
            )
            bucket_rows = [
                dict(row)
                for row in conn.execute(
                    text(
                        f"""
                        SELECT user_id, strategy_id, trading_mode, locked_capital_cents,
                               realized_pnl_cents, unrealized_pnl_cents,
                               available_cash_cents, available_buying_power_cents
                        FROM strategy_capital_buckets
                        {bucket_where}
                        """
                    ),
                    bucket_params,
                )
                .mappings()
                .all()
            ]

        trades_by_scope: defaultdict[
            tuple[str, str, str, str], list[dict[str, Any]]
        ] = defaultdict(list)
        for row in trade_rows:
            trades_by_scope[
                (
                    str(row["user_id"]),
                    str(row["strategy_id"]),
                    str(row["symbol"]),
                    str(row["trading_mode"]),
                )
            ].append(row)
        positions_by_scope = {
            (
                str(row["user_id"]),
                str(row["strategy_id"]),
                str(row["symbol"]),
                str(row["trading_mode"]),
            ): row
            for row in position_rows
        }
        mismatches: list[dict[str, Any]] = []
        scopes = sorted(set(trades_by_scope) | set(positions_by_scope))
        per_bucket_expected: defaultdict[tuple[str, str, str], dict[str, int]] = (
            defaultdict(lambda: {"locked_capital_cents": 0, "realized_pnl_cents": 0})
        )

        for scope in scopes:
            scoped_trades = trades_by_scope.get(scope, [])
            position = positions_by_scope.get(scope)
            expected_quantity = Decimal("0")
            expected_realized_pnl_cents = 0
            latest_trade_quantity: Decimal | None = None
            for trade in scoped_trades:
                quantity = Decimal(str(trade["quantity"]))
                if str(trade["side"]).lower() == Side.BUY.value:
                    expected_quantity += quantity
                else:
                    expected_quantity -= quantity
                    expected_realized_pnl_cents += int(trade["realized_pnl_cents"] or 0)
                latest_trade_quantity = Decimal(str(trade["position_quantity_after"]))
            actual_quantity = (
                Decimal(str(position["quantity"]))
                if position is not None
                else Decimal("0")
            )
            actual_realized_pnl_cents = (
                int(position["realized_pnl_cents"] or 0) if position is not None else 0
            )
            avg_entry_price = (
                Decimal(str(position["avg_entry_price"]))
                if position is not None
                else Decimal("0")
            )
            if abs(expected_quantity - actual_quantity) > Decimal("0.00000001"):
                mismatches.append(
                    {
                        "type": "position_quantity_mismatch",
                        "user_id": scope[0],
                        "strategy_id": scope[1],
                        "symbol": scope[2],
                        "trading_mode": scope[3],
                        "expected_quantity": str(expected_quantity),
                        "actual_quantity": str(actual_quantity),
                    }
                )
            if latest_trade_quantity is not None and abs(
                latest_trade_quantity - actual_quantity
            ) > Decimal("0.00000001"):
                mismatches.append(
                    {
                        "type": "latest_trade_quantity_mismatch",
                        "user_id": scope[0],
                        "strategy_id": scope[1],
                        "symbol": scope[2],
                        "trading_mode": scope[3],
                        "latest_trade_quantity_after": str(latest_trade_quantity),
                        "actual_quantity": str(actual_quantity),
                    }
                )
            if expected_realized_pnl_cents != actual_realized_pnl_cents:
                mismatches.append(
                    {
                        "type": "realized_pnl_mismatch",
                        "user_id": scope[0],
                        "strategy_id": scope[1],
                        "symbol": scope[2],
                        "trading_mode": scope[3],
                        "expected_realized_pnl_cents": expected_realized_pnl_cents,
                        "actual_realized_pnl_cents": actual_realized_pnl_cents,
                    }
                )

            bucket_scope = (scope[0], scope[1], scope[3])
            per_bucket_expected[bucket_scope]["realized_pnl_cents"] += (
                actual_realized_pnl_cents
            )
            if actual_quantity > 0 and avg_entry_price > 0:
                per_bucket_expected[bucket_scope]["locked_capital_cents"] += (
                    _money_to_cents(actual_quantity * avg_entry_price)
                )

        for row in bucket_rows:
            bucket_scope = (
                str(row["user_id"]),
                str(row["strategy_id"]),
                str(row["trading_mode"]),
            )
            expected = per_bucket_expected[bucket_scope]
            if (
                int(row["locked_capital_cents"] or 0)
                != expected["locked_capital_cents"]
            ):
                mismatches.append(
                    {
                        "type": "bucket_locked_capital_mismatch",
                        "user_id": bucket_scope[0],
                        "strategy_id": bucket_scope[1],
                        "trading_mode": bucket_scope[2],
                        "expected_locked_capital_cents": expected[
                            "locked_capital_cents"
                        ],
                        "actual_locked_capital_cents": int(
                            row["locked_capital_cents"] or 0
                        ),
                    }
                )
            if int(row["realized_pnl_cents"] or 0) != expected["realized_pnl_cents"]:
                mismatches.append(
                    {
                        "type": "bucket_realized_pnl_mismatch",
                        "user_id": bucket_scope[0],
                        "strategy_id": bucket_scope[1],
                        "trading_mode": bucket_scope[2],
                        "expected_realized_pnl_cents": expected["realized_pnl_cents"],
                        "actual_realized_pnl_cents": int(
                            row["realized_pnl_cents"] or 0
                        ),
                    }
                )
        return {
            "scope_count": len(scopes),
            "bucket_count": len(bucket_rows),
            "mismatch_count": len(mismatches),
            "mismatches": mismatches,
        }

    def _apply_submission(
        self,
        request: ExecutionRequest,
        order_id: str,
        reserve_cents: int,
        submission: ExecutionSubmission,
    ) -> ProcessResult:
        current_state = self._get_order(order_id)["state"]
        target = submission.status
        base_state = ExecutionOrderStatus(current_state)
        if target in {
            ExecutionOrderStatus.PENDING,
            ExecutionOrderStatus.PARTIALLY_FILLED,
            ExecutionOrderStatus.FILLED,
        }:
            if base_state == ExecutionOrderStatus.CAPITAL_RESERVED:
                self._lock_reserved_capital(request, reserve_cents, order_id)
                self._set_order_reserved_locked(
                    order_id, reserved_cash_cents=0, locked_cash_cents=reserve_cents
                )
                base_state = ExecutionOrderStatus.CAPITAL_RESERVED
            self._set_order_state(
                order_id,
                ExecutionOrderStatus.SUBMITTED,
                venue_order_id=submission.venue_order_id,
                adapter_payload=submission.raw_payload,
                submitted_at=_utcnow(),
            )
            self._emit_event(
                order_id,
                request,
                ExecutionOrderStatus.SUBMITTED,
                detail="Order submitted",
            )

        if target == ExecutionOrderStatus.FAILED:
            self._handle_failure(order_id, request, reserve_cents, submission)
            return ProcessResult(
                order_id=order_id, state=ExecutionOrderStatus.FAILED, duplicated=False
            )
        if target == ExecutionOrderStatus.CANCELLED:
            if reserve_cents > 0:
                self._release_reserved_capital(request, reserve_cents, order_id)
            self._set_order_state(
                order_id,
                ExecutionOrderStatus.CANCELLED,
                venue_order_id=submission.venue_order_id,
                adapter_payload=submission.raw_payload,
                cancelled_at=_utcnow(),
                reserved_cash_cents=0,
                actual_fill_type=submission.actual_fill_type,
                fallback_triggered=submission.fallback_triggered,
            )
            self._record_metric(
                rejected=True,
                rejection_reason="execution_cancelled",
            )
            self._persist_lifecycle_event(
                request=request,
                stage=StrategyLifecycleStage.EXECUTION_FAILED,
                status=LifecycleEventStatus.FAILED,
                occurred_at=_utcnow(),
                order_id=order_id,
                reason_code="execution_cancelled",
                reason_detail=submission.failure_detail or "Order cancelled",
            )
            self._persist_decision_audit_record(
                request=request,
                decision=DecisionAuditDecision.REJECTED,
                reason_code="execution_cancelled",
                reason_detail=submission.failure_detail or "Order cancelled",
                size_before=request.quantity,
                size_after=Decimal("0"),
                created_at=_utcnow(),
            )
            self._emit_event(
                order_id,
                request,
                ExecutionOrderStatus.CANCELLED,
                detail=submission.failure_detail or "Order cancelled",
                payload=submission.raw_payload,
            )
            return ProcessResult(
                order_id=order_id,
                state=ExecutionOrderStatus.CANCELLED,
                duplicated=False,
            )

        self._set_order_state(
            order_id,
            target,
            venue_order_id=submission.venue_order_id,
            adapter_payload=submission.raw_payload,
            actual_fill_type=submission.actual_fill_type,
            fallback_triggered=submission.fallback_triggered,
        )
        self._emit_event(
            order_id,
            request,
            target,
            detail="Order state updated",
            payload=submission.raw_payload,
        )

        if submission.fills:
            self._persist_fills_and_positions(order_id, request, submission)
        if target == ExecutionOrderStatus.FILLED:
            self._persist_decision_audit_record(
                request=request,
                decision=DecisionAuditDecision.EXECUTED,
                reason_code="filled",
                reason_detail="Execution filled",
                size_before=request.quantity,
                size_after=request.quantity,
                created_at=_utcnow(),
            )
        self._record_metric(executed=True)

        return ProcessResult(order_id=order_id, state=target, duplicated=False)

    def _handle_failure(
        self,
        order_id: str,
        request: ExecutionRequest,
        reserve_cents: int,
        submission: ExecutionSubmission,
    ) -> None:
        order = self._get_order(order_id)
        if int(order["reserved_cash_cents"] or 0) > 0:
            self._release_reserved_capital(
                request, int(order["reserved_cash_cents"]), order_id
            )
        self._set_order_state(
            order_id,
            ExecutionOrderStatus.FAILED,
            failure_code=submission.failure_code,
            failure_detail=submission.failure_detail,
            adapter_payload=submission.raw_payload,
            failed_at=_utcnow(),
            reserved_cash_cents=0,
        )
        self._record_metric(
            rejected=True,
            rejection_reason=submission.failure_code or "execution_failed",
        )
        self._persist_lifecycle_event(
            request=request,
            stage=StrategyLifecycleStage.EXECUTION_FAILED,
            status=LifecycleEventStatus.FAILED,
            occurred_at=_utcnow(),
            order_id=order_id,
            reason_code=submission.failure_code or "execution_failed",
            reason_detail=submission.failure_detail or "Execution failed",
        )
        self._persist_decision_audit_record(
            request=request,
            decision=DecisionAuditDecision.REJECTED,
            reason_code=submission.failure_code or "execution_failed",
            reason_detail=submission.failure_detail or "Execution failed",
            size_before=request.quantity,
            size_after=Decimal("0"),
            created_at=_utcnow(),
        )
        self._emit_event(
            order_id,
            request,
            ExecutionOrderStatus.FAILED,
            detail=submission.failure_detail or "Execution failed",
        )

    def _persist_fills_and_positions(
        self, order_id: str, request: ExecutionRequest, submission: ExecutionSubmission
    ) -> None:
        order = self._get_order(order_id)
        total_qty = Decimal(str(order["filled_quantity"]))
        weighted_notional = Decimal("0")
        total_notional_cents = 0
        if order["avg_fill_price"]:
            weighted_notional = total_qty * Decimal(str(order["avg_fill_price"]))
        fees_cents = int(order["fees_cents"] or 0)
        for index, fill in enumerate(submission.fills, start=1):
            fill_notional_cents = _money_to_cents(fill.quantity * fill.price)
            fill_fee_cents = _money_to_cents(fill.fee)
            total_qty += fill.quantity
            weighted_notional += fill.quantity * fill.price
            total_notional_cents += fill_notional_cents
            fees_cents += fill_fee_cents
            fill_row_id = str(uuid.uuid4())
            with self._engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        INSERT INTO execution_fills (
                          id, order_id, venue_fill_id, fill_sequence, quantity, price,
                          gross_notional_cents, fee_cents, liquidity, raw_payload, filled_at
                        ) VALUES (
                          :id, :order_id, :venue_fill_id, :fill_sequence, :quantity, :price,
                          :gross_notional_cents, :fee_cents, :liquidity, :raw_payload, :filled_at
                        )
                        """
                    ),
                    {
                        "id": fill_row_id,
                        "order_id": order_id,
                        "venue_fill_id": fill.fill_id,
                        "fill_sequence": index,
                        "quantity": str(fill.quantity),
                        "price": str(fill.price),
                        "gross_notional_cents": fill_notional_cents,
                        "fee_cents": fill_fee_cents,
                        "liquidity": fill.liquidity,
                        "raw_payload": json.dumps(fill.raw_payload, default=str),
                        "filled_at": fill.occurred_at,
                    },
                )
            self._apply_fill_to_position(
                order_id,
                fill_row_id,
                request,
                fill,
                fill_notional_cents,
                fill_fee_cents,
            )

        avg_price = (
            (weighted_notional / total_qty).quantize(Decimal("0.00000001"))
            if total_qty > 0
            else None
        )
        completed_at = (
            _utcnow() if submission.status == ExecutionOrderStatus.FILLED else None
        )
        self._set_order_state(
            order_id,
            submission.status,
            filled_quantity=str(total_qty),
            avg_fill_price=str(avg_price) if avg_price is not None else None,
            fees_cents=fees_cents,
            completed_at=completed_at,
        )
        if (
            request.side == Side.BUY
            and submission.status == ExecutionOrderStatus.FILLED
        ):
            self._reconcile_filled_buy_locked_capital(
                order_id,
                request,
                actual_locked_cents=total_notional_cents + fees_cents,
            )

    def _apply_fill_to_position(
        self,
        order_id: str,
        fill_row_id: str,
        request: ExecutionRequest,
        fill,
        fill_notional_cents: int,
        fill_fee_cents: int,
    ) -> None:
        position = self._get_position(request)
        qty_before = Decimal(str(position["quantity"])) if position else Decimal("0")
        avg_before = (
            Decimal(str(position["avg_entry_price"]))
            if position and position["avg_entry_price"]
            else Decimal("0")
        )
        realized_pnl_cents = 0
        qty_after = qty_before
        avg_after = avg_before
        close_qty = Decimal("0")
        if request.side == Side.BUY:
            qty_after = qty_before + fill.quantity
            total_cost = (
                (qty_before * avg_before)
                + (fill.quantity * fill.price)
                + (Decimal(str(fill_fee_cents)) / Decimal("100"))
            )
            avg_after = (
                (total_cost / qty_after).quantize(Decimal("0.00000001"))
                if qty_after > 0
                else Decimal("0")
            )
        else:
            close_qty = min(qty_before, fill.quantity)
            qty_after = max(Decimal("0"), qty_before - close_qty)
            if close_qty > 0 and avg_before > 0:
                basis_cents = _money_to_cents(close_qty * avg_before)
                proceeds_cents = fill_notional_cents
                realized_pnl_cents = proceeds_cents - basis_cents - fill_fee_cents
                self._settle_capital(request, basis_cents, realized_pnl_cents, order_id)
            if qty_after == 0:
                avg_after = Decimal("0")
        self._upsert_position(request, qty_after, avg_after, realized_pnl_cents)
        trade_id = self._insert_trade(
            order_id,
            fill_row_id,
            request,
            fill,
            qty_after,
            avg_after,
            realized_pnl_cents,
            fill_notional_cents,
            fill_fee_cents,
        )
        self._persist_lifecycle_event(
            request=request,
            stage=StrategyLifecycleStage.EXECUTION_SUCCEEDED,
            status=LifecycleEventStatus.SUCCEEDED,
            occurred_at=fill.occurred_at,
            order_id=order_id,
            trade_id=trade_id,
            metadata={
                "fill_id": fill.fill_id,
                "quantity": str(fill.quantity),
                "price": str(fill.price),
            },
        )
        if request.side == Side.BUY and qty_before <= 0 and qty_after > 0:
            self._persist_lifecycle_event(
                request=request,
                stage=StrategyLifecycleStage.POSITION_OPENED,
                status=LifecycleEventStatus.OBSERVED,
                occurred_at=fill.occurred_at,
                order_id=order_id,
                trade_id=trade_id,
                metadata={"quantity_after": str(qty_after)},
            )
            self._persist_lifecycle_event(
                request=request,
                stage=StrategyLifecycleStage.EXIT_MONITORING_STARTED,
                status=LifecycleEventStatus.OBSERVED,
                occurred_at=fill.occurred_at,
                order_id=order_id,
                trade_id=trade_id,
            )
        if request.side == Side.SELL and close_qty > 0 and qty_after == 0:
            self._persist_lifecycle_event(
                request=request,
                stage=StrategyLifecycleStage.POSITION_CLOSED,
                status=LifecycleEventStatus.OBSERVED,
                occurred_at=fill.occurred_at,
                order_id=order_id,
                trade_id=trade_id,
                reason_code=self._resolve_exit_reason(request),
                reason_detail=self._resolve_exit_reason(request),
                metadata={"close_quantity": str(close_qty)},
            )
        self._record_trade_outcome_feature(
            trade_id=trade_id,
            request=request,
            fill=fill,
            qty_before=qty_before,
            qty_after=qty_after,
            avg_before=avg_before,
            avg_after=avg_after,
            close_qty=close_qty,
            realized_pnl_cents=realized_pnl_cents,
            fill_fee_cents=fill_fee_cents,
        )
        self._record_strategy_runtime_activity(request, fill.occurred_at)

    def _reconcile_filled_buy_locked_capital(
        self, order_id: str, request: ExecutionRequest, *, actual_locked_cents: int
    ) -> None:
        order = self._get_order(order_id)
        locked_cash_cents = int(order["locked_cash_cents"] or 0)
        delta = actual_locked_cents - locked_cash_cents
        if delta > 0:
            self._reserve_capital(request, delta, order_id)
            self._lock_reserved_capital(request, delta, order_id)
        elif delta < 0:
            self._settle_capital(request, -delta, 0, order_id)
        self._set_order_reserved_locked(
            order_id,
            reserved_cash_cents=0,
            locked_cash_cents=actual_locked_cents,
        )

    def _upsert_position(
        self,
        request: ExecutionRequest,
        quantity: Decimal,
        avg_entry_price: Decimal,
        realized_pnl_delta_cents: int,
    ) -> None:
        existing = self._get_position(request)
        now = _utcnow()
        opened_at, closed_at, lifecycle_event = self._position_lifecycle_state(
            existing=existing,
            quantity=quantity,
            now=now,
        )
        if existing is None:
            with self._engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        INSERT INTO execution_positions (
                          id, tenant_id, user_id, strategy_id, symbol, trading_mode,
                          quantity, avg_entry_price, realized_pnl_cents, created_at, updated_at, opened_at, last_trade_at, closed_at
                        ) VALUES (
                          :id, :tenant_id, :user_id, :strategy_id, :symbol, :trading_mode,
                          :quantity, :avg_entry_price, :realized_pnl_cents, :created_at, :updated_at, :opened_at, :last_trade_at, :closed_at
                        )
                        """
                    ),
                    {
                        "id": str(uuid.uuid4()),
                        "tenant_id": _to_hex(request.tenant_id),
                        "user_id": _to_hex(request.user_id),
                        "strategy_id": request.strategy_id,
                        "symbol": request.symbol,
                        "trading_mode": request.trading_mode.value,
                        "quantity": str(quantity),
                        "avg_entry_price": str(avg_entry_price),
                        "realized_pnl_cents": realized_pnl_delta_cents,
                        "created_at": now,
                        "updated_at": now,
                        "opened_at": opened_at,
                        "last_trade_at": now,
                        "closed_at": closed_at,
                    },
                )
            self._log_position_lifecycle(
                request=request,
                lifecycle_event=lifecycle_event,
                quantity_before=Decimal("0"),
                quantity_after=quantity,
                opened_at=opened_at,
                last_trade_at=now,
                closed_at=closed_at,
            )
            return
        quantity_before = Decimal(str(existing["quantity"] or "0"))
        with self._engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE execution_positions
                    SET quantity = :quantity,
                        avg_entry_price = :avg_entry_price,
                        realized_pnl_cents = :realized_pnl_cents,
                        updated_at = :updated_at,
                        opened_at = :opened_at,
                        last_trade_at = :last_trade_at,
                        closed_at = :closed_at
                    WHERE id = :id
                    """
                ),
                {
                    "id": str(existing["id"]),
                    "quantity": str(quantity),
                    "avg_entry_price": str(avg_entry_price),
                    "realized_pnl_cents": int(existing["realized_pnl_cents"] or 0)
                    + realized_pnl_delta_cents,
                    "updated_at": now,
                    "opened_at": opened_at,
                    "last_trade_at": now,
                    "closed_at": closed_at,
                },
            )
        self._log_position_lifecycle(
            request=request,
            lifecycle_event=lifecycle_event,
            quantity_before=quantity_before,
            quantity_after=quantity,
            opened_at=opened_at,
            last_trade_at=now,
            closed_at=closed_at,
        )

    def _position_lifecycle_state(
        self,
        *,
        existing: dict[str, Any] | None,
        quantity: Decimal,
        now: datetime,
    ) -> tuple[datetime | None, datetime | None, str]:
        previous_quantity = (
            Decimal(str(existing["quantity"] or "0"))
            if existing is not None
            else Decimal("0")
        )
        previous_opened_at = (
            self._parse_db_timestamp(existing.get("opened_at"))
            if existing is not None
            else None
        )
        abs_previous = abs(previous_quantity)
        abs_current = abs(quantity)

        if abs_previous <= 0 and abs_current > 0:
            return now, None, "position_opened"
        if abs_current <= 0:
            return previous_opened_at, now, "position_closed"
        if abs_current < abs_previous:
            return previous_opened_at or now, None, "position_reduced"
        return previous_opened_at or now, None, "position_resized"

    def _log_position_lifecycle(
        self,
        *,
        request: ExecutionRequest,
        lifecycle_event: str,
        quantity_before: Decimal,
        quantity_after: Decimal,
        opened_at: datetime | None,
        last_trade_at: datetime,
        closed_at: datetime | None,
    ) -> None:
        log.info(
            "position_lifecycle %s",
            json.dumps(
                {
                    "event": lifecycle_event,
                    "user_id": str(request.user_id),
                    "strategy_id": request.strategy_id,
                    "symbol": request.symbol,
                    "trading_mode": request.trading_mode.value,
                    "quantity_before": str(quantity_before),
                    "quantity_after": str(quantity_after),
                    "opened_at": opened_at.isoformat() if opened_at else None,
                    "last_trade_at": last_trade_at.isoformat(),
                    "closed_at": closed_at.isoformat() if closed_at else None,
                },
                default=str,
            ),
        )

    def _insert_trade(
        self,
        order_id: str,
        fill_row_id: str,
        request: ExecutionRequest,
        fill,
        qty_after: Decimal,
        avg_after: Decimal,
        realized_pnl_cents: int,
        fill_notional_cents: int,
        fill_fee_cents: int,
    ) -> str:
        trade_id = str(uuid.uuid4())
        with self._engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO execution_trades (
                      id, order_id, fill_id, tenant_id, user_id, strategy_id, symbol, trading_mode,
                      side, quantity, price, gross_notional_cents, fee_cents, realized_pnl_cents,
                      position_quantity_after, avg_entry_price_after, executed_at, raw_payload
                    ) VALUES (
                      :id, :order_id, :fill_id, :tenant_id, :user_id, :strategy_id, :symbol, :trading_mode,
                      :side, :quantity, :price, :gross_notional_cents, :fee_cents, :realized_pnl_cents,
                      :position_quantity_after, :avg_entry_price_after, :executed_at, :raw_payload
                    )
                    """
                ),
                {
                    "id": trade_id,
                    "order_id": order_id,
                    "fill_id": fill_row_id,
                    "tenant_id": _to_hex(request.tenant_id),
                    "user_id": _to_hex(request.user_id),
                    "strategy_id": request.strategy_id,
                    "symbol": request.symbol,
                    "trading_mode": request.trading_mode.value,
                    "side": request.side.value,
                    "quantity": str(fill.quantity),
                    "price": str(fill.price),
                    "gross_notional_cents": fill_notional_cents,
                    "fee_cents": fill_fee_cents,
                    "realized_pnl_cents": realized_pnl_cents,
                    "position_quantity_after": str(qty_after),
                    "avg_entry_price_after": str(avg_after),
                    "executed_at": fill.occurred_at,
                    "raw_payload": json.dumps(fill.raw_payload, default=str),
                },
            )
        return trade_id

    def _reserve_capital(
        self, request: ExecutionRequest, amount_cents: int, order_id: str
    ) -> None:
        if amount_cents <= 0:
            return
        with self._engine.begin() as conn:
            bucket = (
                conn.execute(
                    text(
                        "SELECT available_cash_cents, reserved_cash_cents, available_buying_power_cents FROM strategy_capital_buckets WHERE user_id = :user_id AND strategy_id = :strategy_id AND trading_mode = :trading_mode LIMIT 1"
                    ),
                    {
                        "user_id": _to_hex(request.user_id),
                        "strategy_id": request.strategy_id,
                        "trading_mode": request.trading_mode.value,
                    },
                )
                .mappings()
                .first()
            )
            if bucket is None:
                raise ValueError("Capital bucket not found")
            if amount_cents > int(bucket["available_cash_cents"]) or amount_cents > int(
                bucket["available_buying_power_cents"]
            ):
                raise ValueError("Insufficient buying power for execution")
            conn.execute(
                text(
                    "UPDATE strategy_capital_buckets SET available_cash_cents = available_cash_cents - :amount, reserved_cash_cents = reserved_cash_cents + :amount, available_buying_power_cents = available_buying_power_cents - :amount, version = version + 1, updated_at = :updated_at WHERE user_id = :user_id AND strategy_id = :strategy_id AND trading_mode = :trading_mode"
                ),
                {
                    "amount": amount_cents,
                    "updated_at": _utcnow(),
                    "user_id": _to_hex(request.user_id),
                    "strategy_id": request.strategy_id,
                    "trading_mode": request.trading_mode.value,
                },
            )

    def _release_reserved_capital(
        self, request: ExecutionRequest, amount_cents: int, order_id: str
    ) -> None:
        if amount_cents <= 0:
            return
        with self._engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE strategy_capital_buckets SET available_cash_cents = available_cash_cents + :amount, reserved_cash_cents = reserved_cash_cents - :amount, available_buying_power_cents = available_buying_power_cents + :amount, version = version + 1, updated_at = :updated_at WHERE user_id = :user_id AND strategy_id = :strategy_id AND trading_mode = :trading_mode"
                ),
                {
                    "amount": amount_cents,
                    "updated_at": _utcnow(),
                    "user_id": _to_hex(request.user_id),
                    "strategy_id": request.strategy_id,
                    "trading_mode": request.trading_mode.value,
                },
            )

    def _lock_reserved_capital(
        self, request: ExecutionRequest, amount_cents: int, order_id: str
    ) -> None:
        if amount_cents <= 0:
            return
        with self._engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE strategy_capital_buckets SET reserved_cash_cents = reserved_cash_cents - :amount, locked_capital_cents = locked_capital_cents + :amount, version = version + 1, updated_at = :updated_at WHERE user_id = :user_id AND strategy_id = :strategy_id AND trading_mode = :trading_mode"
                ),
                {
                    "amount": amount_cents,
                    "updated_at": _utcnow(),
                    "user_id": _to_hex(request.user_id),
                    "strategy_id": request.strategy_id,
                    "trading_mode": request.trading_mode.value,
                },
            )

    def _settle_capital(
        self,
        request: ExecutionRequest,
        released_locked_cents: int,
        realized_pnl_delta_cents: int,
        order_id: str,
    ) -> None:
        with self._engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE strategy_capital_buckets SET locked_capital_cents = locked_capital_cents - :released, realized_pnl_cents = realized_pnl_cents + :pnl, available_cash_cents = CASE WHEN available_cash_cents + :released + :pnl < 0 THEN 0 ELSE available_cash_cents + :released + :pnl END, available_buying_power_cents = CASE WHEN available_buying_power_cents + :released + :pnl < 0 THEN 0 ELSE available_buying_power_cents + :released + :pnl END, version = version + 1, updated_at = :updated_at WHERE user_id = :user_id AND strategy_id = :strategy_id AND trading_mode = :trading_mode"
                ),
                {
                    "released": released_locked_cents,
                    "pnl": realized_pnl_delta_cents,
                    "updated_at": _utcnow(),
                    "user_id": _to_hex(request.user_id),
                    "strategy_id": request.strategy_id,
                    "trading_mode": request.trading_mode.value,
                },
            )

    def _emit_event(
        self,
        order_id: str,
        request: ExecutionRequest,
        state: ExecutionOrderStatus,
        *,
        detail: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        payload = payload or {}
        self._log_execution_decision(
            request=request,
            state=state,
            detail=detail,
            payload=payload,
        )
        if self._engine is None:
            return
        event = ExecutionEvent(
            order_id=order_id,
            intent_id=request.intent_id,
            tenant_id=request.tenant_id,
            user_id=request.user_id,
            strategy_id=request.strategy_id,
            symbol=request.symbol,
            trading_mode=request.trading_mode,
            state=state,
            venue=Venue.COINBASE,
            client_order_id=request.client_order_id,
            detail=detail,
            payload=payload,
        )
        self._enqueue(
            QueueNames.execution_events(request.trading_mode),
            execution_event_to_json(event),
        )
        self._enqueue(
            QueueNames.execution_reconciliation(request.trading_mode),
            execution_event_to_json(event),
        )
        alert_type: NotificationEventType | None = None
        if state == ExecutionOrderStatus.SUBMITTED:
            alert_type = NotificationEventType.TRADE_OPENED
        elif state == ExecutionOrderStatus.FILLED:
            alert_type = NotificationEventType.TRADE_CLOSED
        elif (
            state == ExecutionOrderStatus.FAILED
            and (detail or "").lower().find("insufficient") >= 0
        ):
            alert_type = NotificationEventType.INSUFFICIENT_BALANCE

        if alert_type is not None:
            notif = NotificationEvent(
                event_id=uuid.uuid4(),
                tenant_id=request.tenant_id,
                user_id=request.user_id,
                trading_mode=request.trading_mode,
                event_type=alert_type,
                trace_id=request.trace_id,
                title="Trade update",
                message=detail
                or f"{request.symbol} {request.side.value} {request.quantity} is {state.value}",
                payload={
                    "order_id": order_id,
                    "strategy_id": request.strategy_id,
                    "symbol": request.symbol,
                    "side": request.side.value,
                    "quantity": str(request.quantity),
                    "state": state.value,
                },
            )
            self._enqueue(
                QueueNames.alerts(request.trading_mode),
                notification_event_to_json(notif),
            )

    def _log_execution_decision(
        self,
        *,
        request: ExecutionRequest,
        state: ExecutionOrderStatus,
        detail: str | None,
        payload: dict[str, Any],
    ) -> None:
        rejection_reason = None
        if state == ExecutionOrderStatus.FAILED:
            rejection_reason = payload.get(
                "failure_code"
            ) or request.intent_payload.get("metadata", {}).get(
                "token_policy_execution", {}
            ).get("recommendation_status")
        log.info(
            "execution_decision %s",
            json.dumps(
                {
                    "stage": "execution",
                    "strategy": request.strategy_id,
                    "token": request.symbol,
                    "trading_mode": request.trading_mode.value,
                    "signal_generated": state
                    not in {
                        ExecutionOrderStatus.FAILED,
                        ExecutionOrderStatus.CANCELLED,
                    },
                    "rejection_reason": rejection_reason,
                    "confidence_score": request.intent_payload.get("metadata", {}).get(
                        "confidence_score"
                    ),
                    "final_decision": state.value,
                    "detail": detail,
                    "quantity": str(request.quantity),
                    "expected_gross_edge_bps": request.expected_gross_edge_bps,
                    "estimated_total_cost_bps": request.estimated_total_cost_bps,
                    "expected_net_edge_bps": request.expected_net_edge_bps,
                    "execution_preference": request.execution_preference,
                    "fallback_behavior": request.fallback_behavior,
                    "token_policy": request.intent_payload.get("metadata", {}).get(
                        "token_policy_execution"
                    ),
                    "fee_economics": request.intent_payload.get("metadata", {}).get(
                        "fee_economics"
                    ),
                    "metrics": self.metrics_snapshot(),
                },
                default=str,
            ),
        )

    def _persist_decision_audit_record(
        self,
        *,
        request: ExecutionRequest,
        decision: DecisionAuditDecision,
        reason_code: str | None,
        reason_detail: str | None,
        size_before: Decimal | None,
        size_after: Decimal | None,
        created_at: datetime,
    ) -> None:
        if self._engine is None:
            return
        metadata = dict(request.intent_payload.get("metadata") or {})
        persist_decision_audit(
            self._engine,
            signal_snapshot_id=extract_signal_snapshot_id(metadata),
            stage=DecisionAuditStage.EXECUTION.value,
            decision=decision.value,
            reason_code=reason_code,
            reason_detail=reason_detail,
            size_before=size_before,
            size_after=size_after,
            created_at=created_at,
        )

    def _persist_lifecycle_event(
        self,
        *,
        request: ExecutionRequest,
        stage: StrategyLifecycleStage,
        status: LifecycleEventStatus,
        occurred_at: datetime,
        order_id: str | None = None,
        trade_id: str | None = None,
        reason_code: str | None = None,
        reason_detail: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        if self._engine is None:
            return
        persist_strategy_lifecycle_event(
            self._engine,
            correlation_id=request.trace_id,
            user_id=str(request.user_id),
            tenant_id=str(request.tenant_id),
            strategy_name=request.strategy_id,
            token_symbol=request.symbol,
            trading_mode=request.trading_mode.value,
            side=request.side.value,
            stage=stage.value,
            status=status.value,
            occurred_at=occurred_at,
            signal_snapshot_id=extract_signal_snapshot_id(
                dict(request.intent_payload.get("metadata") or {})
            ),
            run_id=str(request.risk.run_id),
            intent_id=str(request.intent_id),
            order_id=order_id,
            trade_id=trade_id,
            reason_code=reason_code,
            reason_detail=reason_detail,
            metadata=metadata or dict(request.intent_payload.get("metadata") or {}),
        )

    @staticmethod
    def _exit_trigger_stage(
        exit_reason: str | None,
    ) -> StrategyLifecycleStage | None:
        reason = (exit_reason or "").lower()
        if "trailing" in reason:
            return StrategyLifecycleStage.TRAILING_STOP_TRIGGERED
        if "take" in reason or "profit" in reason:
            return StrategyLifecycleStage.TAKE_PROFIT_TRIGGERED
        if "stop" in reason or "loss" in reason:
            return StrategyLifecycleStage.STOP_LOSS_TRIGGERED
        return None

    def _record_trade_outcome_feature(
        self,
        *,
        trade_id: str,
        request: ExecutionRequest,
        fill,
        qty_before: Decimal,
        qty_after: Decimal,
        avg_before: Decimal,
        avg_after: Decimal,
        close_qty: Decimal,
        realized_pnl_cents: int,
        fill_fee_cents: int,
    ) -> None:
        state = self._load_trade_intelligence_state(request)
        if request.side == Side.BUY:
            opened_at = state.get("opened_at") or fill.occurred_at.isoformat()
            entry_snapshot_id = state.get(
                "entry_signal_snapshot_id"
            ) or extract_signal_snapshot_id(
                dict(request.intent_payload.get("metadata") or {})
            )
            self._store_trade_intelligence_state(
                request,
                {
                    "opened_at": opened_at,
                    "entry_price": str(avg_after if avg_after > 0 else fill.price),
                    "entry_signal_snapshot_id": entry_snapshot_id,
                    "partial_profit_taken": False,
                    "partial_profit_outcome_id": None,
                },
                fill.occurred_at,
            )
            return
        if close_qty <= 0:
            return
        opened_at = self._parse_db_timestamp(state.get("opened_at")) or fill.occurred_at
        metadata = dict(request.intent_payload.get("metadata") or {})
        entry_signal_snapshot_id = state.get(
            "entry_signal_snapshot_id"
        ) or extract_signal_snapshot_id(metadata)
        entry_price = (
            avg_before
            if avg_before > 0
            else Decimal(str(state.get("entry_price") or fill.price))
        )
        hold_seconds = max(0, int((fill.occurred_at - opened_at).total_seconds()))
        realized_pnl = Decimal(str(realized_pnl_cents)) / Decimal("100")
        basis = close_qty * entry_price
        realized_return_pct = realized_pnl / basis if basis > 0 else None
        mfe_pct, mae_pct = self._load_excursion_pct(
            symbol=request.symbol,
            opened_at=opened_at,
            closed_at=fill.occurred_at,
            entry_price=entry_price,
        )
        profit_giveback_pct = (
            mfe_pct - realized_return_pct
            if mfe_pct is not None and realized_return_pct is not None
            else None
        )
        exit_reason = self._resolve_exit_reason(request)
        prior_partial_profit_taken = bool(state.get("partial_profit_taken"))
        is_partial_profit_capture = (
            exit_reason == "partial_take_profit" and qty_after > 0
        )
        partial_profit_taken = prior_partial_profit_taken or is_partial_profit_capture
        remaining_position_outcome = (
            self._classify_remaining_position_outcome(
                realized_pnl=realized_pnl,
                max_favorable_excursion_pct=mfe_pct,
                profit_giveback_pct=profit_giveback_pct,
            )
            if prior_partial_profit_taken and not is_partial_profit_capture
            else None
        )
        outcome_id = persist_trade_outcome_feature(
            self._engine,
            trade_id=trade_id,
            signal_snapshot_id=str(entry_signal_snapshot_id)
            if entry_signal_snapshot_id
            else None,
            trading_mode=request.trading_mode.value,
            strategy_name=request.strategy_id,
            token_symbol=request.symbol,
            entry_price=entry_price,
            exit_price=fill.price,
            filled_size=close_qty,
            fee_paid=Decimal(str(fill_fee_cents)) / Decimal("100"),
            slippage_realized=Decimal(str(getattr(fill, "slippage_bps", 0)))
            / Decimal("10000"),
            hold_seconds=hold_seconds,
            realized_pnl=realized_pnl,
            realized_return_pct=realized_return_pct,
            max_favorable_excursion_pct=mfe_pct,
            max_adverse_excursion_pct=mae_pct,
            profit_giveback_pct=profit_giveback_pct,
            partial_profit_taken=partial_profit_taken,
            remaining_position_outcome=remaining_position_outcome,
            exit_reason=exit_reason,
            win_loss_label="win" if realized_pnl >= 0 else "loss",
            profitable_after_fees_label=(
                "profitable" if realized_pnl > 0 else "not_profitable"
            ),
            created_at=fill.occurred_at,
        )
        if remaining_position_outcome and state.get("partial_profit_outcome_id"):
            self._update_trade_outcome_remaining_position_outcome(
                outcome_id=str(state["partial_profit_outcome_id"]),
                remaining_position_outcome=remaining_position_outcome,
            )
        if qty_after > 0:
            self._store_trade_intelligence_state(
                request,
                {
                    "opened_at": opened_at.isoformat(),
                    "entry_price": str(avg_after if avg_after > 0 else entry_price),
                    "entry_signal_snapshot_id": entry_signal_snapshot_id,
                    "partial_profit_taken": partial_profit_taken,
                    "partial_profit_outcome_id": (
                        outcome_id
                        if is_partial_profit_capture
                        else state.get("partial_profit_outcome_id")
                    ),
                },
                fill.occurred_at,
            )
        else:
            self._store_trade_intelligence_state(request, None, fill.occurred_at)

    def _resolve_exit_reason(self, request: ExecutionRequest) -> str | None:
        metadata = dict(request.intent_payload.get("metadata") or {})
        if metadata.get("reason_code"):
            return str(metadata["reason_code"])
        if metadata.get("guard"):
            return str(metadata["guard"])
        if metadata.get("reason"):
            return str(metadata["reason"])
        if request.risk.detail:
            return str(request.risk.detail)
        if request.risk.reason is not None:
            return request.risk.reason.value
        return None

    def _classify_remaining_position_outcome(
        self,
        *,
        realized_pnl: Decimal,
        max_favorable_excursion_pct: Decimal | None,
        profit_giveback_pct: Decimal | None,
    ) -> str:
        if realized_pnl <= 0:
            return "lost"
        if (
            max_favorable_excursion_pct is not None
            and max_favorable_excursion_pct > 0
            and profit_giveback_pct is not None
            and profit_giveback_pct > 0
        ):
            return "gave_back_profit"
        return "won"

    def _update_trade_outcome_remaining_position_outcome(
        self, *, outcome_id: str, remaining_position_outcome: str
    ) -> None:
        if self._engine is None:
            return
        with self._engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE trade_outcome_features
                    SET remaining_position_outcome = :remaining_position_outcome
                    WHERE id = :id
                    """
                ),
                {
                    "id": outcome_id,
                    "remaining_position_outcome": remaining_position_outcome,
                },
            )

    def _load_excursion_pct(
        self,
        *,
        symbol: str,
        opened_at: datetime,
        closed_at: datetime,
        entry_price: Decimal,
    ) -> tuple[Decimal | None, Decimal | None]:
        if self._engine is None or entry_price <= 0:
            return None, None
        with self._engine.begin() as conn:
            row = (
                conn.execute(
                    text(
                        """
                    SELECT MAX(high) AS max_high, MIN(low) AS min_low
                    FROM market_data_candles
                    WHERE product_id = :symbol
                      AND bucket_start >= :opened_at
                      AND bucket_start <= :closed_at
                    """
                    ),
                    {
                        "symbol": symbol,
                        "opened_at": opened_at,
                        "closed_at": closed_at,
                    },
                )
                .mappings()
                .first()
            )
        if row is None:
            return None, None
        max_high = row.get("max_high")
        min_low = row.get("min_low")
        mfe = (
            (Decimal(str(max_high)) - entry_price) / entry_price
            if max_high is not None
            else None
        )
        mae = (
            (Decimal(str(min_low)) - entry_price) / entry_price
            if min_low is not None
            else None
        )
        return mfe, mae

    def _set_order_reserved_locked(
        self, order_id: str, *, reserved_cash_cents: int, locked_cash_cents: int
    ) -> None:
        with self._engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE execution_orders SET reserved_cash_cents = :reserved_cash_cents, locked_cash_cents = :locked_cash_cents, updated_at = :updated_at WHERE id = :id"
                ),
                {
                    "id": order_id,
                    "reserved_cash_cents": reserved_cash_cents,
                    "locked_cash_cents": locked_cash_cents,
                    "updated_at": _utcnow(),
                },
            )

    def _set_order_state(
        self, order_id: str, state: ExecutionOrderStatus, **updates: Any
    ) -> None:
        order = self._get_order(order_id)
        ensure_transition(ExecutionOrderStatus(order["state"]), state)
        fields = {"state": state.value, "updated_at": _utcnow(), **updates}
        for key, value in list(fields.items()):
            if isinstance(value, (dict, list)):
                fields[key] = json.dumps(value, default=str)
        assignments = ", ".join(f"{key} = :{key}" for key in fields)
        fields["id"] = order_id
        with self._engine.begin() as conn:
            conn.execute(
                text(f"UPDATE execution_orders SET {assignments} WHERE id = :id"),
                fields,
            )

    def _get_existing_order(self, intent_id: uuid.UUID, trading_mode: TradingMode):
        with self._engine.begin() as conn:
            return (
                conn.execute(
                    text(
                        "SELECT * FROM execution_orders WHERE intent_id = :intent_id AND trading_mode = :trading_mode LIMIT 1"
                    ),
                    {"intent_id": str(intent_id), "trading_mode": trading_mode.value},
                )
                .mappings()
                .first()
            )

    def _get_order(self, order_id: str):
        with self._engine.begin() as conn:
            row = (
                conn.execute(
                    text("SELECT * FROM execution_orders WHERE id = :id LIMIT 1"),
                    {"id": order_id},
                )
                .mappings()
                .first()
            )
        if row is None:
            raise ValueError("Order not found")
        return row

    def _get_position(self, request: ExecutionRequest):
        with self._engine.begin() as conn:
            return (
                conn.execute(
                    text(
                        "SELECT * FROM execution_positions WHERE tenant_id = :tenant_id AND user_id = :user_id AND strategy_id = :strategy_id AND symbol = :symbol AND trading_mode = :trading_mode LIMIT 1"
                    ),
                    {
                        "tenant_id": _to_hex(request.tenant_id),
                        "user_id": _to_hex(request.user_id),
                        "strategy_id": request.strategy_id,
                        "symbol": request.symbol,
                        "trading_mode": request.trading_mode.value,
                    },
                )
                .mappings()
                .first()
            )

    @staticmethod
    def _is_terminal_state(state: ExecutionOrderStatus) -> bool:
        return state in {
            ExecutionOrderStatus.FILLED,
            ExecutionOrderStatus.CANCELLED,
            ExecutionOrderStatus.FAILED,
        }

    @staticmethod
    def _parse_db_timestamp(value: Any) -> datetime | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))

    @staticmethod
    def _uuid_from_db(value: Any) -> uuid.UUID:
        raw = str(value)
        return uuid.UUID(hex=raw) if "-" not in raw else uuid.UUID(raw)

    def _load_strategy_state(
        self, user_id: str, strategy_id: str, trading_mode: str
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
                        "user_id": user_id,
                        "strategy_id": strategy_id,
                        "trading_mode": trading_mode,
                    },
                )
                .mappings()
                .first()
            )
        if row is None:
            return {}
        state = row["state"]
        if isinstance(state, str):
            try:
                state = json.loads(state)
            except Exception:
                return {}
        return state if isinstance(state, dict) else {}

    def _upsert_strategy_state(
        self,
        *,
        user_id: str,
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
                    "user_id": user_id,
                    "strategy_id": strategy_id,
                    "trading_mode": trading_mode,
                    "state": json.dumps(state, default=str),
                    "created_at": now,
                    "updated_at": now,
                },
            )

    def _load_trade_intelligence_state(
        self, request: ExecutionRequest
    ) -> dict[str, Any]:
        state = self._load_strategy_runtime_symbol_state(
            user_id=_to_hex(request.user_id),
            strategy_id=request.strategy_id,
            trading_mode=request.trading_mode.value,
            symbol=request.symbol,
        )
        intelligence = state.get("trade_intelligence")
        return dict(intelligence) if isinstance(intelligence, dict) else {}

    def _store_trade_intelligence_state(
        self,
        request: ExecutionRequest,
        intelligence_state: dict[str, Any] | None,
        now: datetime,
    ) -> None:
        user_id = _to_hex(request.user_id)
        trading_mode = request.trading_mode.value
        state = self._load_strategy_state(user_id, request.strategy_id, trading_mode)
        symbols = state.get("symbols")
        if not isinstance(symbols, dict):
            symbols = {}
        symbol_state = symbols.get(request.symbol)
        if not isinstance(symbol_state, dict):
            symbol_state = {}
        if intelligence_state:
            symbol_state["trade_intelligence"] = intelligence_state
        else:
            symbol_state.pop("trade_intelligence", None)
        if symbol_state:
            symbols[request.symbol] = symbol_state
        else:
            symbols.pop(request.symbol, None)
        state["symbols"] = symbols
        self._upsert_strategy_state(
            user_id=user_id,
            strategy_id=request.strategy_id,
            trading_mode=trading_mode,
            state=state,
            now=now,
        )

    def _record_strategy_runtime_activity(
        self, request: ExecutionRequest, occurred_at: datetime
    ) -> None:
        if request.strategy_id != "dca" or request.side != Side.BUY:
            return
        user_id = _to_hex(request.user_id)
        trading_mode = request.trading_mode.value
        state = self._load_strategy_state(user_id, request.strategy_id, trading_mode)
        symbols = state.get("symbols")
        if not isinstance(symbols, dict):
            symbols = {}
        symbol_state = symbols.get(request.symbol)
        if not isinstance(symbol_state, dict):
            symbol_state = {}
        symbol_state["last_buy_at"] = occurred_at.isoformat()
        symbols[request.symbol] = symbol_state
        state["symbols"] = symbols
        self._upsert_strategy_state(
            user_id=user_id,
            strategy_id=request.strategy_id,
            trading_mode=trading_mode,
            state=state,
            now=occurred_at,
        )

    def _load_strategy_runtime_symbol_state(
        self,
        *,
        user_id: str,
        strategy_id: str,
        trading_mode: str,
        symbol: str,
    ) -> dict[str, Any]:
        state = self._load_strategy_state(user_id, strategy_id, trading_mode)
        symbols = state.get("symbols")
        if not isinstance(symbols, dict):
            return {}
        symbol_state = symbols.get(symbol)
        return symbol_state if isinstance(symbol_state, dict) else {}

    def _load_day_trading_config(self, user_id: str) -> dict[str, Any]:
        stmt = text(
            """
            SELECT
              us.config AS user_config,
              ps.config_schema AS platform_config
            FROM user_strategies us
            LEFT JOIN platform_strategies ps ON ps.slug = us.strategy_id
            WHERE us.user_id = :user_id
              AND us.strategy_id = 'day_trading'
            LIMIT 1
            """
        )
        with self._engine.begin() as conn:
            row = conn.execute(stmt, {"user_id": user_id}).mappings().first()
        if row is None:
            return {}
        user_config = row["user_config"]
        if isinstance(user_config, str):
            user_config = json.loads(user_config)
        platform_config = row["platform_config"]
        if isinstance(platform_config, str):
            platform_config = json.loads(platform_config)
        platform_config = normalize_platform_strategy_config(
            "day_trading", platform_config
        )
        user_config = user_config if isinstance(user_config, dict) else {}
        strategy_params = platform_config.get("strategy_params")
        if not isinstance(strategy_params, dict):
            strategy_params = {}
        return {**user_config, **strategy_params}

    def _has_open_exit_order(
        self,
        *,
        user_id: str,
        strategy_id: str,
        symbol: str,
        trading_mode: str,
    ) -> bool:
        stmt = text(
            """
            SELECT 1
            FROM execution_orders
            WHERE user_id = :user_id
              AND strategy_id = :strategy_id
              AND symbol = :symbol
              AND trading_mode = :trading_mode
              AND side = :side
              AND state IN ('created', 'capital_reserved', 'submitted', 'pending', 'partially_filled')
            LIMIT 1
            """
        )
        with self._engine.begin() as conn:
            row = conn.execute(
                stmt,
                {
                    "user_id": user_id,
                    "strategy_id": strategy_id,
                    "symbol": symbol,
                    "trading_mode": trading_mode,
                    "side": Side.SELL.value,
                },
            ).first()
        return row is not None

    def _enforce_day_trading_position_age(self, position: dict[str, Any]) -> bool:
        user_id = str(position["user_id"])
        config = self._load_day_trading_config(user_id)
        max_age_hours = int(config.get("max_position_age_hours", 3) or 3)
        opened_at = self._parse_db_timestamp(position.get("opened_at"))
        if opened_at is None:
            return False
        now = _utcnow()
        if now - opened_at < timedelta(hours=max_age_hours):
            return False
        if self._has_open_exit_order(
            user_id=user_id,
            strategy_id=str(position["strategy_id"]),
            symbol=str(position["symbol"]),
            trading_mode=str(position["trading_mode"]),
        ):
            return False
        request = self._build_day_trading_guard_close_request(
            position, opened_at, max_age_hours
        )
        result = self.process_request(request)
        log.info(
            "position_age_guard reason_code=max_position_age_exceeded order_id=%s mode=%s symbol=%s duplicated=%s",
            result.order_id,
            position["trading_mode"],
            position["symbol"],
            result.duplicated,
        )
        return True

    def _build_day_trading_guard_close_request(
        self,
        position: dict[str, Any],
        opened_at: datetime,
        max_age_hours: int,
    ) -> ExecutionRequest:
        intent_id = uuid.uuid4()
        trading_mode = TradingMode(str(position["trading_mode"]))
        tenant_id = self._uuid_from_db(position["tenant_id"])
        user_id = self._uuid_from_db(position["user_id"])
        quantity = abs(Decimal(str(position["quantity"])))
        trace_id = f"position-age-{intent_id.hex[:16]}"
        risk = RiskDecision(
            outcome=RiskOutcome.APPROVE,
            approved=True,
            signal_id=uuid.uuid4(),
            run_id=uuid.uuid4(),
            user_id=user_id,
            strategy_name=str(position["strategy_id"]),
            symbol=str(position["symbol"]),
            original_size=str(quantity),
            final_size=str(quantity),
            trading_mode=trading_mode,
            detail=(
                f"max_position_age_exceeded: opened_at={opened_at.isoformat()} "
                f"max_age_hours={max_age_hours}"
            ),
            rules_evaluated=["max_position_age_exceeded"],
            trace_id=trace_id,
        )
        return ExecutionRequest(
            intent_id=intent_id,
            trace_id=trace_id,
            user_id=user_id,
            risk=risk,
            tenant_id=tenant_id,
            trading_mode=trading_mode,
            strategy_id=str(position["strategy_id"]),
            symbol=str(position["symbol"]),
            side=Side.SELL,
            order_type=OrderType.MARKET,
            quantity=quantity,
            price_hint=self._market_price_hint(
                str(position["symbol"]), Side.SELL.value
            ),
            idempotency_key=self.build_idempotency_key(str(intent_id), trading_mode),
            client_order_id=self.build_client_order_id(str(intent_id), trading_mode),
            intent_payload={
                "intent_id": str(intent_id),
                "tenant_id": str(tenant_id),
                "trading_mode": trading_mode.value,
                "strategy_id": str(position["strategy_id"]),
                "instrument": {"symbol": str(position["symbol"])},
                "side": Side.SELL.value,
                "order_type": OrderType.MARKET.value,
                "quantity": {"amount": str(quantity)},
                "metadata": {
                    "guard": "max_position_age_exceeded",
                    "reason_code": "max_position_age_exceeded",
                    "opened_at": opened_at.isoformat(),
                    "max_age_hours": max_age_hours,
                    "enforcement_source": "execution_engine_backstop",
                },
            },
        )
