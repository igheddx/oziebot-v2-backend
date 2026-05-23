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


class VolatilityHarvestConfig(Base):
    __tablename__ = "volatility_harvest_config"
    __table_args__ = (
        UniqueConstraint(
            "user_id",
            "strategy_id",
            "trading_mode",
            name="uq_volatility_harvest_config_scope",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    strategy_id: Mapped[str] = mapped_column(
        String(64), nullable=False, default="volatility_harvest"
    )
    trading_mode: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    is_enabled: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="false"
    )
    total_allocated_amount_usd: Mapped[dict[str, Any]] = mapped_column(
        JSON, nullable=False, default=dict, server_default="{}"
    )
    selected_tokens: Mapped[list[str]] = mapped_column(
        JSON, nullable=False, default=list, server_default="[]"
    )
    core_position_percentage: Mapped[str] = mapped_column(
        String(32), nullable=False, default="70", server_default="70"
    )
    trading_position_percentage: Mapped[str] = mapped_column(
        String(32), nullable=False, default="30", server_default="30"
    )
    entry_layers: Mapped[list[dict[str, Any]]] = mapped_column(
        JSON, nullable=False, default=list, server_default="[]"
    )
    harvest_bands: Mapped[list[dict[str, Any]]] = mapped_column(
        JSON, nullable=False, default=list, server_default="[]"
    )
    rebuy_bands: Mapped[list[dict[str, Any]]] = mapped_column(
        JSON, nullable=False, default=list, server_default="[]"
    )
    volatility_settings: Mapped[dict[str, Any]] = mapped_column(
        JSON, nullable=False, default=dict, server_default="{}"
    )
    risk_controls: Mapped[dict[str, Any]] = mapped_column(
        JSON, nullable=False, default=dict, server_default="{}"
    )
    fee_settings: Mapped[dict[str, Any]] = mapped_column(
        JSON, nullable=False, default=dict, server_default="{}"
    )
    mode_settings: Mapped[dict[str, Any]] = mapped_column(
        JSON, nullable=False, default=dict, server_default="{}"
    )
    admin_overrides: Mapped[dict[str, Any]] = mapped_column(
        JSON, nullable=False, default=dict, server_default="{}"
    )
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column("metadata", JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    user: Mapped["User"] = relationship("User")


class VolatilityHarvestPosition(Base):
    __tablename__ = "volatility_harvest_positions"
    __table_args__ = (
        UniqueConstraint(
            "user_id",
            "strategy_id",
            "trading_mode",
            "symbol",
            name="uq_volatility_harvest_position_scope",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    strategy_id: Mapped[str] = mapped_column(
        String(64), nullable=False, default="volatility_harvest"
    )
    trading_mode: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    core_quantity: Mapped[str] = mapped_column(
        String(64), nullable=False, default="0", server_default="0"
    )
    trading_quantity: Mapped[str] = mapped_column(
        String(64), nullable=False, default="0", server_default="0"
    )
    avg_core_entry_price: Mapped[str] = mapped_column(
        String(64), nullable=False, default="0", server_default="0"
    )
    avg_trading_entry_price: Mapped[str] = mapped_column(
        String(64), nullable=False, default="0", server_default="0"
    )
    harvested_cash_cents: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    realized_gains_cents: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    unrealized_gains_cents: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    total_harvested_gains_cents: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    token_accumulation_quantity: Mapped[str] = mapped_column(
        String(64), nullable=False, default="0", server_default="0"
    )
    token_accumulation_pct: Mapped[str] = mapped_column(
        String(32), nullable=False, default="0", server_default="0"
    )
    total_harvest_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    total_rebuy_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    last_local_high: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_harvest_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_rebuy_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_action_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column("metadata", JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    user: Mapped["User"] = relationship("User")


class VolatilityHarvestTransaction(Base):
    __tablename__ = "volatility_harvest_transactions"
    __table_args__ = (UniqueConstraint("order_id", name="uq_volatility_harvest_transaction_order"),)

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    strategy_id: Mapped[str] = mapped_column(
        String(64), nullable=False, default="volatility_harvest"
    )
    trading_mode: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    order_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), nullable=True, index=True
    )
    transaction_type: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    bucket_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    band_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    quantity: Mapped[str] = mapped_column(
        String(64), nullable=False, default="0", server_default="0"
    )
    price: Mapped[str] = mapped_column(String(64), nullable=False, default="0", server_default="0")
    gross_notional_cents: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    fee_cents: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    slippage_cents: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    net_profit_cents: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    harvested_cash_balance_cents: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    token_quantity_after: Mapped[str] = mapped_column(
        String(64), nullable=False, default="0", server_default="0"
    )
    signal_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), nullable=True, index=True
    )
    correlation_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), nullable=True, index=True
    )
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column("metadata", JSON, nullable=True)
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    user: Mapped["User"] = relationship("User")


class VolatilityHarvestMetric(Base):
    __tablename__ = "volatility_harvest_metrics"
    __table_args__ = (
        UniqueConstraint(
            "user_id",
            "strategy_id",
            "trading_mode",
            name="uq_volatility_harvest_metrics_scope",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    strategy_id: Mapped[str] = mapped_column(
        String(64), nullable=False, default="volatility_harvest"
    )
    trading_mode: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    harvested_cash_cents: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    total_harvested_gains_cents: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    realized_gains_cents: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    unrealized_gains_cents: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    total_core_quantity: Mapped[str] = mapped_column(
        String(64), nullable=False, default="0", server_default="0"
    )
    total_trading_quantity: Mapped[str] = mapped_column(
        String(64), nullable=False, default="0", server_default="0"
    )
    total_token_accumulation_quantity: Mapped[str] = mapped_column(
        String(64), nullable=False, default="0", server_default="0"
    )
    total_token_accumulation_pct: Mapped[str] = mapped_column(
        String(32), nullable=False, default="0", server_default="0"
    )
    lifetime_harvest_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    lifetime_rebuy_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    avg_rebuy_efficiency_pct: Mapped[str] = mapped_column(
        String(32), nullable=False, default="0", server_default="0"
    )
    accumulation_history: Mapped[list[dict[str, Any]]] = mapped_column(
        JSON, nullable=False, default=list, server_default="[]"
    )
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column("metadata", JSON, nullable=True)
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    user: Mapped["User"] = relationship("User")


from typing import TYPE_CHECKING  # noqa: E402

if TYPE_CHECKING:
    from oziebot_api.models.user import User
