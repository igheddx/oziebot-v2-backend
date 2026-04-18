from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    JSON,
    String,
    Uuid,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base


class ExecutionOrder(Base):
    __tablename__ = "execution_orders"
    __table_args__ = (
        UniqueConstraint("intent_id", "trading_mode", name="uq_execution_order_intent_mode"),
        UniqueConstraint("idempotency_key", name="uq_execution_order_idempotency_key"),
        UniqueConstraint("client_order_id", name="uq_execution_order_client_order_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    intent_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), nullable=False, index=True)
    correlation_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), nullable=False, index=True
    )
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False, index=True
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    strategy_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    side: Mapped[str] = mapped_column(String(16), nullable=False)
    order_type: Mapped[str] = mapped_column(String(16), nullable=False)
    trading_mode: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    venue: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    state: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    quantity: Mapped[str] = mapped_column(String(64), nullable=False)
    requested_notional_cents: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    reserved_cash_cents: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    locked_cash_cents: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    filled_quantity: Mapped[str] = mapped_column(String(64), nullable=False, default="0")
    avg_fill_price: Mapped[str | None] = mapped_column(String(64), nullable=True)
    fees_cents: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    expected_gross_edge_bps: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    estimated_fee_bps: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    estimated_slippage_bps: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    estimated_total_cost_bps: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    expected_net_edge_bps: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    execution_preference: Mapped[str] = mapped_column(
        String(32), nullable=False, default="maker_preferred"
    )
    fallback_behavior: Mapped[str] = mapped_column(
        String(32), nullable=False, default="convert_to_taker"
    )
    maker_timeout_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    limit_price_offset_bps: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    actual_fill_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    fallback_triggered: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    client_order_id: Mapped[str] = mapped_column(String(128), nullable=False)
    venue_order_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    failure_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    failure_detail: Mapped[str | None] = mapped_column(String(512), nullable=True)
    trace_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    intent_payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    risk_payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    adapter_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    submitted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    cancelled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    failed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    fills: Mapped[list["ExecutionFillRecord"]] = relationship(
        "ExecutionFillRecord", back_populates="order", cascade="all, delete-orphan"
    )
    trades: Mapped[list["ExecutionTradeRecord"]] = relationship(
        "ExecutionTradeRecord", back_populates="order", cascade="all, delete-orphan"
    )


class ExecutionFillRecord(Base):
    __tablename__ = "execution_fills"
    __table_args__ = (
        UniqueConstraint("order_id", "venue_fill_id", name="uq_execution_fill_order_venue_fill"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    order_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("execution_orders.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    venue_fill_id: Mapped[str] = mapped_column(String(128), nullable=False)
    fill_sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    quantity: Mapped[str] = mapped_column(String(64), nullable=False)
    price: Mapped[str] = mapped_column(String(64), nullable=False)
    gross_notional_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    fee_cents: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    liquidity: Mapped[str | None] = mapped_column(String(32), nullable=True)
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    filled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)

    order: Mapped[ExecutionOrder] = relationship("ExecutionOrder", back_populates="fills")


class ExecutionTradeRecord(Base):
    __tablename__ = "execution_trades"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    order_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("execution_orders.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    fill_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("execution_fills.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False, index=True
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    strategy_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    trading_mode: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    side: Mapped[str] = mapped_column(String(16), nullable=False)
    quantity: Mapped[str] = mapped_column(String(64), nullable=False)
    price: Mapped[str] = mapped_column(String(64), nullable=False)
    gross_notional_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    fee_cents: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    realized_pnl_cents: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    position_quantity_after: Mapped[str] = mapped_column(String(64), nullable=False)
    avg_entry_price_after: Mapped[str] = mapped_column(String(64), nullable=False)
    executed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
    raw_payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)

    order: Mapped[ExecutionOrder] = relationship("ExecutionOrder", back_populates="trades")


class ExecutionPosition(Base):
    __tablename__ = "execution_positions"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "user_id",
            "strategy_id",
            "symbol",
            "trading_mode",
            name="uq_execution_position_scope",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False, index=True
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    strategy_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    trading_mode: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    quantity: Mapped[str] = mapped_column(String(64), nullable=False, default="0")
    avg_entry_price: Mapped[str] = mapped_column(String(64), nullable=False, default="0")
    realized_pnl_cents: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_trade_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


from typing import TYPE_CHECKING  # noqa: E402

if TYPE_CHECKING:
    pass
