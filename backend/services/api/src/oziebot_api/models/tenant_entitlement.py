from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, String, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base


class TenantEntitlement(Base):
    """Strategy-level access; platform_strategy_id NULL means all strategies (trial or all-strategies plan)."""

    __tablename__ = "tenant_entitlements"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    platform_strategy_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("platform_strategies.id", ondelete="CASCADE"),
        nullable=True,
    )
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    valid_from: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    valid_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    stripe_subscription_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("stripe_subscriptions.id", ondelete="CASCADE"),
        nullable=True,
    )
    stripe_subscription_item_row_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("stripe_subscription_items.id", ondelete="SET NULL"),
        nullable=True,
    )
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    tenant: Mapped["Tenant"] = relationship("Tenant", back_populates="entitlements")
    platform_strategy: Mapped["PlatformStrategy | None"] = relationship("PlatformStrategy")
    stripe_subscription: Mapped["StripeSubscription | None"] = relationship(
        "StripeSubscription", back_populates="entitlements"
    )
    stripe_subscription_item: Mapped["StripeSubscriptionItem | None"] = relationship(
        "StripeSubscriptionItem", back_populates="entitlements"
    )


from typing import TYPE_CHECKING  # noqa: E402

if TYPE_CHECKING:
    from oziebot_api.models.platform_strategy import PlatformStrategy
    from oziebot_api.models.stripe_subscription import StripeSubscription
    from oziebot_api.models.stripe_subscription_item import StripeSubscriptionItem
    from oziebot_api.models.tenant import Tenant
