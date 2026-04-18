from __future__ import annotations

import hashlib
import json
import logging
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

from oziebot_common.queues import (
    QueueNames,
    execution_event_to_json,
    notification_event_to_json,
    push_json,
    risk_decision_from_json,
    trade_intent_from_json,
)
from oziebot_common.strategy_defaults import normalize_platform_strategy_config
from oziebot_common.token_policy import resolve_effective_token_policy
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


class ExecutionService:
    def __init__(
        self,
        settings,
        redis_client,
        *,
        paper_adapter: ExecutionAdapter,
        live_adapter: ExecutionAdapter,
    ) -> None:
        self._settings = settings
        self._redis = redis_client
        self._engine = (
            create_engine(settings.database_url) if settings.database_url else None
        )
        self._paper_adapter = paper_adapter
        self._live_adapter = live_adapter
        self._crypto = CredentialCrypto(settings.exchange_credentials_encryption_key)
        self._metrics: Counter[str] = Counter()
        self._rejection_reasons: Counter[str] = Counter()

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

        request, policy_failure = self._apply_token_strategy_policy(request)
        reserve_cents = self._estimate_reserve_cents(request)
        now = _utcnow()
        order_id = str(uuid.uuid4())
        insert_stmt = text(
            """
            INSERT INTO execution_orders (
              id, intent_id, correlation_id, tenant_id, user_id, strategy_id, symbol, side, order_type,
              trading_mode, venue, state, quantity, requested_notional_cents, reserved_cash_cents,
              locked_cash_cents, filled_quantity, avg_fill_price, fees_cents, idempotency_key,
              client_order_id, venue_order_id, failure_code, failure_detail, trace_id,
              intent_payload, risk_payload, adapter_payload, created_at, updated_at, submitted_at,
              completed_at, cancelled_at, failed_at
            ) VALUES (
              :id, :intent_id, :correlation_id, :tenant_id, :user_id, :strategy_id, :symbol, :side, :order_type,
              :trading_mode, :venue, :state, :quantity, :requested_notional_cents, :reserved_cash_cents,
              :locked_cash_cents, :filled_quantity, :avg_fill_price, :fees_cents, :idempotency_key,
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
                            else ExecutionOrderStatus.CREATED.value
                        ),
                        "quantity": str(request.quantity),
                        "requested_notional_cents": reserve_cents or 0,
                        "reserved_cash_cents": 0,
                        "locked_cash_cents": 0,
                        "filled_quantity": "0",
                        "avg_fill_price": None,
                        "fees_cents": 0,
                        "idempotency_key": request.idempotency_key,
                        "client_order_id": request.client_order_id,
                        "failure_code": "token_strategy_policy"
                        if policy_failure is not None
                        else None,
                        "failure_detail": policy_failure,
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
                        "failed_at": now if policy_failure is not None else None,
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

        if policy_failure is not None:
            self._record_metric(
                rejected=True,
                rejection_reason="token_strategy_policy",
            )
            self._emit_event(
                order_id,
                request,
                ExecutionOrderStatus.FAILED,
                detail=policy_failure,
                payload={"failure_code": "token_strategy_policy"},
            )
            return ProcessResult(
                order_id=order_id,
                state=ExecutionOrderStatus.FAILED,
                duplicated=False,
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

        adapter = (
            self._paper_adapter
            if request.trading_mode == TradingMode.PAPER
            else self._live_adapter
        )
        submission = adapter.submit(request)
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
        if request.side != Side.BUY:
            return request, None

        policy_row = self._load_token_strategy_policy(
            symbol=request.symbol,
            strategy_id=request.strategy_id,
        )
        effective = resolve_effective_token_policy(policy_row)
        intent_payload = dict(request.intent_payload)
        metadata = dict(intent_payload.get("metadata") or {})
        metadata["token_policy_execution"] = {
            "admin_enabled": effective["admin_enabled"],
            "recommendation_status": effective["effective_recommendation_status"],
            "recommendation_reason": effective["effective_recommendation_reason"],
            "size_multiplier": str(effective["size_multiplier"]),
            "max_position_pct_override": str(effective["max_position_pct_override"])
            if effective["max_position_pct_override"] is not None
            else None,
        }
        intent_payload["metadata"] = metadata
        request = request.model_copy(update={"intent_payload": intent_payload})

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

        max_position_pct_override = effective["max_position_pct_override"]
        if max_position_pct_override is not None:
            if request.price_hint is None or request.price_hint <= 0:
                return (
                    request,
                    "Execution rejected: missing price hint for token strategy position cap",
                )
            total_capital_cents = self._load_total_capital_cents(
                user_id=request.user_id,
                trading_mode=request.trading_mode,
            )
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
            idempotency_key=self.build_idempotency_key(intent_id, trading_mode),
            client_order_id=self.build_client_order_id(intent_id, trading_mode),
            intent_payload=intent,
        )

    def _market_price_hint(self, symbol: str, side: str) -> Decimal | None:
        raw = self._redis.get(f"oziebot:md:bbo:{symbol}") if self._redis else None
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

        self._set_order_state(
            order_id,
            target,
            venue_order_id=submission.venue_order_id,
            adapter_payload=submission.raw_payload,
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
        self._insert_trade(
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
        if existing is None:
            with self._engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        INSERT INTO execution_positions (
                          id, tenant_id, user_id, strategy_id, symbol, trading_mode,
                          quantity, avg_entry_price, realized_pnl_cents, created_at, updated_at, last_trade_at
                        ) VALUES (
                          :id, :tenant_id, :user_id, :strategy_id, :symbol, :trading_mode,
                          :quantity, :avg_entry_price, :realized_pnl_cents, :created_at, :updated_at, :last_trade_at
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
                        "last_trade_at": now,
                    },
                )
            return
        with self._engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE execution_positions
                    SET quantity = :quantity,
                        avg_entry_price = :avg_entry_price,
                        realized_pnl_cents = :realized_pnl_cents,
                        updated_at = :updated_at,
                        last_trade_at = :last_trade_at
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
                    "last_trade_at": now,
                },
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
    ) -> None:
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
                    "id": str(uuid.uuid4()),
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
        if self._redis is None:
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
        push_json(
            self._redis,
            QueueNames.execution_events(request.trading_mode),
            execution_event_to_json(event),
        )
        push_json(
            self._redis,
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
            push_json(
                self._redis,
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
                    "token_policy": request.intent_payload.get("metadata", {}).get(
                        "token_policy_execution"
                    ),
                    "metrics": self.metrics_snapshot(),
                },
                default=str,
            ),
        )

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
        max_age_hours = int(config.get("max_position_age_hours", 4) or 4)
        symbol_state = self._load_day_trading_runtime_symbol_state(position)
        opened_at = self._parse_db_timestamp(
            symbol_state.get("opened_at")
        ) or self._parse_db_timestamp(position.get("last_trade_at"))
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
            "position_age_guard order_id=%s mode=%s symbol=%s duplicated=%s",
            result.order_id,
            position["trading_mode"],
            position["symbol"],
            result.duplicated,
        )
        return True

    def _load_day_trading_runtime_symbol_state(
        self, position: dict[str, Any]
    ) -> dict[str, Any]:
        return self._load_strategy_runtime_symbol_state(
            user_id=str(position["user_id"]),
            strategy_id=str(position["strategy_id"]),
            trading_mode=str(position["trading_mode"]),
            symbol=str(position["symbol"]),
        )

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
                f"execution_position_age_guard: opened_at={opened_at.isoformat()} "
                f"max_age_hours={max_age_hours}"
            ),
            rules_evaluated=["execution_position_age_guard"],
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
                    "guard": "max_position_age_hours",
                    "opened_at": opened_at.isoformat(),
                    "max_age_hours": max_age_hours,
                },
            },
        )
