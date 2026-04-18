from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import Boolean, DateTime, ForeignKey, JSON, String, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base


class StripeSubscription(Base):
    __tablename__ = "stripe_subscriptions"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    stripe_subscription_id: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    stripe_customer_id: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    subscription_plan_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("subscription_plans.id", ondelete="SET NULL"),
        nullable=True,
    )
    primary_stripe_price_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    current_period_start: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    current_period_end: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    cancel_at_period_end: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column(
        "metadata_json", JSON, nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    tenant: Mapped["Tenant"] = relationship("Tenant", back_populates="stripe_subscriptions")
    subscription_plan: Mapped["SubscriptionPlan | None"] = relationship("SubscriptionPlan")
    items: Mapped[list["StripeSubscriptionItem"]] = relationship(
        "StripeSubscriptionItem",
        back_populates="stripe_subscription_row",
        cascade="all, delete-orphan",
    )
    entitlements: Mapped[list["TenantEntitlement"]] = relationship(
        "TenantEntitlement",
        back_populates="stripe_subscription",
    )


from typing import TYPE_CHECKING  # noqa: E402

if TYPE_CHECKING:
    from oziebot_api.models.stripe_subscription_item import StripeSubscriptionItem
    from oziebot_api.models.subscription_plan import SubscriptionPlan
    from oziebot_api.models.tenant import Tenant
    from oziebot_api.models.tenant_entitlement import TenantEntitlement
