import uuid
from datetime import datetime

from sqlalchemy import DateTime, String, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base


class Tenant(Base):
    """Tenant row; trading rows must include trading_mode — never mix PAPER/LIVE in one partition."""

    __tablename__ = "tenants"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(256), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    default_trading_mode: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        default="paper",
        server_default="paper",
    )
    trial_started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    trial_ends_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    memberships: Mapped[list["TenantMembership"]] = relationship(
        "TenantMembership",
        back_populates="tenant",
        cascade="all, delete-orphan",
    )
    integration: Mapped["TenantIntegration | None"] = relationship(
        "TenantIntegration",
        back_populates="tenant",
        uselist=False,
        cascade="all, delete-orphan",
    )
    stripe_customer: Mapped["StripeCustomer | None"] = relationship(
        "StripeCustomer",
        back_populates="tenant",
        uselist=False,
        cascade="all, delete-orphan",
    )
    stripe_subscriptions: Mapped[list["StripeSubscription"]] = relationship(
        "StripeSubscription",
        back_populates="tenant",
        cascade="all, delete-orphan",
    )
    entitlements: Mapped[list["TenantEntitlement"]] = relationship(
        "TenantEntitlement",
        back_populates="tenant",
        cascade="all, delete-orphan",
    )
    billing_checkout_sessions: Mapped[list["BillingCheckoutSession"]] = relationship(
        "BillingCheckoutSession",
        back_populates="tenant",
        cascade="all, delete-orphan",
    )
    exchange_connections: Mapped[list["ExchangeConnection"]] = relationship(
        "ExchangeConnection",
        back_populates="tenant",
        cascade="all, delete-orphan",
    )


from typing import TYPE_CHECKING  # noqa: E402

if TYPE_CHECKING:
    from oziebot_api.models.billing_checkout_session import BillingCheckoutSession
    from oziebot_api.models.exchange_connection import ExchangeConnection
    from oziebot_api.models.membership import TenantMembership
    from oziebot_api.models.stripe_customer import StripeCustomer
    from oziebot_api.models.stripe_subscription import StripeSubscription
    from oziebot_api.models.tenant_entitlement import TenantEntitlement
    from oziebot_api.models.tenant_integration import TenantIntegration
