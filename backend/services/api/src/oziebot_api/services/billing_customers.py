"""Stripe customer records per tenant."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.stripe_customer import StripeCustomer
from oziebot_api.services.stripe_service import StripeService


def get_or_create_stripe_customer(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    email: str,
    stripe_svc: StripeService,
) -> StripeCustomer:
    row = db.scalars(select(StripeCustomer).where(StripeCustomer.tenant_id == tenant_id)).first()
    if row is not None:
        return row
    if not stripe_svc.enabled:
        raise RuntimeError("Stripe is not configured")
    cust = stripe_svc.customers_create(
        email=email,
        metadata={"tenant_id": str(tenant_id)},
    )
    now = datetime.now(UTC)
    row = StripeCustomer(
        id=uuid.uuid4(),
        tenant_id=tenant_id,
        stripe_customer_id=cust.id,
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    return row
