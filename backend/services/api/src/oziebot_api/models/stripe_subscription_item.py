from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, String, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base


class StripeSubscriptionItem(Base):
    __tablename__ = "stripe_subscription_items"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    stripe_subscription_row_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("stripe_subscriptions.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    stripe_subscription_item_id: Mapped[str] = mapped_column(
        String(255), unique=True, nullable=False
    )
    stripe_price_id: Mapped[str] = mapped_column(String(255), nullable=False)
    platform_strategy_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("platform_strategies.id", ondelete="SET NULL"),
        nullable=True,
    )
    quantity: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    stripe_subscription_row: Mapped["StripeSubscription"] = relationship(
        "StripeSubscription",
        back_populates="items",
        foreign_keys=[stripe_subscription_row_id],
    )
    platform_strategy: Mapped["PlatformStrategy | None"] = relationship("PlatformStrategy")
    entitlements: Mapped[list["TenantEntitlement"]] = relationship(
        "TenantEntitlement",
        back_populates="stripe_subscription_item",
    )


from typing import TYPE_CHECKING  # noqa: E402

if TYPE_CHECKING:
    from oziebot_api.models.platform_strategy import PlatformStrategy
    from oziebot_api.models.stripe_subscription import StripeSubscription
    from oziebot_api.models.tenant_entitlement import TenantEntitlement
