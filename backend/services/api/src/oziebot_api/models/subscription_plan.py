from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import Boolean, DateTime, Integer, JSON, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from oziebot_api.db.base import Base


class SubscriptionPlan(Base):
    """Stripe-backed subscription plan catalog (prices managed in Stripe, mirrored here)."""

    __tablename__ = "subscription_plans"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    slug: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    plan_kind: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="all_strategies",
        server_default="all_strategies",
    )
    display_name: Mapped[str] = mapped_column(String(256), nullable=False)
    description: Mapped[str | None] = mapped_column(Text(), nullable=True)
    stripe_price_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    stripe_product_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    billing_interval: Mapped[str] = mapped_column(String(16), nullable=False)
    amount_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    currency: Mapped[str] = mapped_column(String(8), nullable=False, default="usd")
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    features: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    trial_days_override: Mapped[int | None] = mapped_column(Integer, nullable=True)
    sort_order: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
