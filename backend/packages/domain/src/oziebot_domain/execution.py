from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any
from uuid import UUID

from pydantic import Field

from oziebot_domain.risk import RiskDecision
from oziebot_domain.trading import OrderType, Side, Venue
from oziebot_domain.trading_mode import TradingMode
from oziebot_domain.types import OziebotModel


class OrderRef(OziebotModel):
    venue: Venue
    trading_mode: TradingMode
    client_order_id: str = Field(..., min_length=8, max_length=128)
    venue_order_id: str | None = None


class ExecutionOrderStatus(StrEnum):
    CREATED = "created"
    CAPITAL_RESERVED = "capital_reserved"
    SUBMITTED = "submitted"
    PENDING = "pending"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    FAILED = "failed"


class ExecutionFill(OziebotModel):
    fill_id: str = Field(..., min_length=1, max_length=128)
    quantity: Decimal = Field(..., gt=0)
    price: Decimal = Field(..., gt=0)
    fee: Decimal = Field(default=Decimal("0"), ge=0)
    liquidity: str | None = Field(default=None, max_length=32)
    slippage_bps: Decimal = Field(default=Decimal("0"), ge=0)
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    raw_payload: dict[str, Any] = Field(default_factory=dict)


class ExecutionRequest(OziebotModel):
    intent_id: UUID
    trace_id: str = Field(..., min_length=1, max_length=64)
    user_id: UUID
    risk: RiskDecision
    tenant_id: UUID
    trading_mode: TradingMode
    strategy_id: str = Field(..., min_length=1, max_length=128)
    symbol: str = Field(..., min_length=3, max_length=32)
    side: Side
    order_type: OrderType
    quantity: Decimal = Field(..., gt=0)
    venue: Venue = Venue.COINBASE
    price_hint: Decimal | None = Field(default=None, gt=0)
    execution_preference: str = Field(default="maker_preferred", min_length=5, max_length=32)
    fallback_behavior: str = Field(default="convert_to_taker", min_length=3, max_length=32)
    maker_timeout_seconds: int = Field(default=15, ge=0, le=3600)
    limit_price_offset_bps: int = Field(default=2, ge=0, le=1000)
    expected_gross_edge_bps: int = Field(default=0, ge=0)
    estimated_fee_bps: int = Field(default=0, ge=0)
    estimated_slippage_bps: int = Field(default=0, ge=0)
    estimated_total_cost_bps: int = Field(default=0, ge=0)
    expected_net_edge_bps: int = Field(default=0)
    fee_profile: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str = Field(..., min_length=8, max_length=128)
    client_order_id: str = Field(..., min_length=8, max_length=128)
    intent_payload: dict[str, Any] = Field(default_factory=dict)


class ExecutionSubmission(OziebotModel):
    status: ExecutionOrderStatus
    venue: Venue = Venue.COINBASE
    venue_order_id: str | None = None
    fills: list[ExecutionFill] = Field(default_factory=list)
    raw_payload: dict[str, Any] = Field(default_factory=dict)
    failure_code: str | None = Field(default=None, max_length=64)
    failure_detail: str | None = Field(default=None, max_length=512)
    fallback_triggered: bool = False
    actual_fill_type: str | None = Field(default=None, max_length=32)


class ExecutionEvent(OziebotModel):
    order_id: UUID
    intent_id: UUID
    tenant_id: UUID
    user_id: UUID
    strategy_id: str
    symbol: str
    trading_mode: TradingMode
    state: ExecutionOrderStatus
    venue: Venue
    client_order_id: str
    venue_order_id: str | None = None
    detail: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
