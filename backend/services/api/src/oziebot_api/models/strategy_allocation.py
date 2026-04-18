from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, ForeignKey, Integer, JSON, String, Uuid, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base


class StrategyAllocationPlan(Base):
    """Top-level allocation settings per user and trading mode."""

    __tablename__ = "strategy_allocation_plans"
    __table_args__ = (
        UniqueConstraint("user_id", "trading_mode", name="uq_strategy_allocation_plan_user_mode"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    trading_mode: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    allocation_mode: Mapped[str] = mapped_column(String(16), nullable=False)  # manual|guided
    preset_name: Mapped[str | None] = mapped_column(String(32), nullable=True)
    total_capital_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    user: Mapped["User"] = relationship("User")
    items: Mapped[list["StrategyAllocationItem"]] = relationship(
        "StrategyAllocationItem", back_populates="plan", cascade="all, delete-orphan"
    )


class StrategyAllocationItem(Base):
    """Per-strategy percentage split within an allocation plan."""

    __tablename__ = "strategy_allocation_items"
    __table_args__ = (
        UniqueConstraint(
            "plan_id", "strategy_id", name="uq_strategy_allocation_item_plan_strategy"
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    plan_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("strategy_allocation_plans.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    strategy_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    allocation_bps: Mapped[int] = mapped_column(Integer, nullable=False)  # 10000 = 100%
    assigned_capital_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    plan: Mapped[StrategyAllocationPlan] = relationship(
        "StrategyAllocationPlan", back_populates="items"
    )


class StrategyCapitalBucket(Base):
    """Virtual capital bucket per user, strategy and trading mode."""

    __tablename__ = "strategy_capital_buckets"
    __table_args__ = (
        UniqueConstraint(
            "user_id", "strategy_id", "trading_mode", name="uq_strategy_bucket_user_strategy_mode"
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    strategy_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    trading_mode: Mapped[str] = mapped_column(String(16), nullable=False, index=True)

    assigned_capital_cents: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    available_cash_cents: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    reserved_cash_cents: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    locked_capital_cents: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    realized_pnl_cents: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    unrealized_pnl_cents: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    available_buying_power_cents: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    version: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    user: Mapped["User"] = relationship("User")


class StrategyCapitalLedger(Base):
    """Immutable ledger for auditable bucket movements."""

    __tablename__ = "strategy_capital_ledger"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    strategy_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    trading_mode: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    event_type: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    amount_cents: Mapped[int] = mapped_column(Integer, nullable=False)

    before_available_cash_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    after_available_cash_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    before_reserved_cash_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    after_reserved_cash_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    before_locked_capital_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    after_locked_capital_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    before_realized_pnl_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    after_realized_pnl_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    before_unrealized_pnl_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    after_unrealized_pnl_cents: Mapped[int] = mapped_column(Integer, nullable=False)

    reference_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column("metadata", JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )

    user: Mapped["User"] = relationship("User")


from typing import TYPE_CHECKING  # noqa: E402

if TYPE_CHECKING:
    from oziebot_api.models.user import User
