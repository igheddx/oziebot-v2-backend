"""Checkout and billing summary for tenant users."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select

from oziebot_api.config import Settings
from oziebot_api.deps import DbSession, settings_dep
from oziebot_api.deps.auth import CurrentUser
from oziebot_api.models.billing_checkout_session import BillingCheckoutSession
from oziebot_api.models.stripe_subscription import StripeSubscription
from oziebot_api.models.subscription_plan import SubscriptionPlan
from oziebot_api.models.tenant import Tenant
from oziebot_api.schemas.billing import BillingSummaryOut, CheckoutRequest, CheckoutResponse
from oziebot_api.services.billing_customers import get_or_create_stripe_customer
from oziebot_api.services.entitlements import is_trial_active
from oziebot_api.services.stripe_service import StripeService
from oziebot_api.services.tenant_scope import primary_tenant_id

router = APIRouter(prefix="/billing", tags=["billing"])


def _stripe_dep(settings: Settings = Depends(settings_dep)) -> StripeService:
    return StripeService(secret_key=settings.stripe_secret_key)


@router.post("/checkout", response_model=CheckoutResponse)
def create_checkout_session(
    body: CheckoutRequest,
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
    stripe_svc: StripeService = Depends(_stripe_dep),
) -> CheckoutResponse:
    if not stripe_svc.enabled:
        raise HTTPException(status_code=503, detail="Stripe billing is not configured")
    tenant_id = primary_tenant_id(db, user)
    if tenant_id is None:
        raise HTTPException(status_code=400, detail="No tenant membership")
    plan = db.scalars(
        select(SubscriptionPlan).where(
            SubscriptionPlan.slug == body.plan_slug.strip().lower(),
            SubscriptionPlan.is_active.is_(True),
        )
    ).first()
    if plan is None:
        raise HTTPException(status_code=404, detail="Subscription plan not found")

    if plan.plan_kind == "per_strategy":
        slugs = body.strategy_slugs or []
        if len(slugs) < 1:
            raise HTTPException(
                status_code=400,
                detail="strategy_slugs is required for per-strategy plans",
            )
        line_items = [{"price": plan.stripe_price_id, "quantity": 1} for _ in slugs]
        strategy_slugs = [s.strip().lower() for s in slugs]
    else:
        line_items = [{"price": plan.stripe_price_id, "quantity": 1}]
        strategy_slugs = None

    customer = get_or_create_stripe_customer(
        db, tenant_id=tenant_id, email=user.email, stripe_svc=stripe_svc
    )
    now = datetime.now(UTC)
    sess = stripe_svc.checkout_sessions_create(
        customer=customer.stripe_customer_id,
        mode="subscription",
        line_items=line_items,
        success_url=settings.stripe_checkout_success_url,
        cancel_url=settings.stripe_checkout_cancel_url,
        client_reference_id=str(tenant_id),
        metadata={
            "tenant_id": str(tenant_id),
            "plan_id": str(plan.id),
        },
        subscription_data={
            "metadata": {
                "tenant_id": str(tenant_id),
                "plan_id": str(plan.id),
            },
        },
    )
    pending = BillingCheckoutSession(
        id=uuid.uuid4(),
        tenant_id=tenant_id,
        subscription_plan_id=plan.id,
        stripe_checkout_session_id=sess.id,
        strategy_slugs=strategy_slugs,
        created_at=now,
    )
    db.add(pending)
    db.flush()
    return CheckoutResponse(checkout_url=sess.url, stripe_checkout_session_id=sess.id)


@router.get("/summary", response_model=BillingSummaryOut)
def billing_summary(user: CurrentUser, db: DbSession) -> BillingSummaryOut:
    tenant_id = primary_tenant_id(db, user)
    if tenant_id is None:
        raise HTTPException(status_code=400, detail="No tenant membership")
    tenant = db.get(Tenant, tenant_id)
    if tenant is None:
        raise HTTPException(status_code=404, detail="Tenant not found")
    sub = db.scalars(
        select(StripeSubscription)
        .where(StripeSubscription.tenant_id == tenant_id)
        .order_by(StripeSubscription.created_at.desc())
    ).first()
    cpe = sub.current_period_end.isoformat() if sub and sub.current_period_end else None
    return BillingSummaryOut(
        trial_started_at=tenant.trial_started_at.isoformat() if tenant.trial_started_at else None,
        trial_ends_at=tenant.trial_ends_at.isoformat() if tenant.trial_ends_at else None,
        trial_active=is_trial_active(db, tenant_id),
        subscription_status=sub.status if sub else None,
        stripe_subscription_id=sub.stripe_subscription_id if sub else None,
        current_period_end=cpe,
    )
