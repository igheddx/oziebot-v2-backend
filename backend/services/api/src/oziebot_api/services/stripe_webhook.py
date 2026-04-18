"""Process verified Stripe webhook events (subscription sync)."""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.billing_checkout_session import BillingCheckoutSession
from oziebot_api.models.stripe_subscription import StripeSubscription
from oziebot_api.models.subscription_plan import SubscriptionPlan
from oziebot_api.services.billing_sync import upsert_subscription_from_stripe
from oziebot_api.services.stripe_service import StripeService


def _get_meta(obj: Any, key: str) -> str | None:
    meta = getattr(obj, "metadata", None) or (
        obj.get("metadata") if isinstance(obj, dict) else None
    )
    if not meta:
        return None
    if isinstance(meta, dict):
        v = meta.get(key)
        return str(v) if v is not None else None
    return getattr(meta, key, None)


def process_stripe_event(db: Session, event: Any, stripe_svc: StripeService) -> None:
    if isinstance(event, dict):
        et = event["type"]
        data_obj = event["data"]["object"]
    else:
        et = event.type
        data_obj = event.data.object

    if et == "checkout.session.completed":
        _checkout_completed(db, data_obj, stripe_svc)
    elif et in ("customer.subscription.updated", "customer.subscription.deleted"):
        _subscription_sync(db, data_obj, stripe_svc)


def _checkout_completed(db: Session, session_obj: Any, stripe_svc: StripeService) -> None:
    mode = getattr(session_obj, "mode", None) or (
        session_obj.get("mode") if isinstance(session_obj, dict) else None
    )
    if mode != "subscription":
        return
    sub_id = getattr(session_obj, "subscription", None) or (
        session_obj.get("subscription") if isinstance(session_obj, dict) else None
    )
    if not sub_id:
        return
    meta_tenant = _get_meta(session_obj, "tenant_id")
    meta_plan = _get_meta(session_obj, "plan_id")
    if not meta_tenant or not meta_plan:
        return
    tenant_id = uuid.UUID(meta_tenant)
    plan = db.get(SubscriptionPlan, uuid.UUID(meta_plan))
    sid = getattr(session_obj, "id", None) or session_obj["id"]
    pending = db.scalars(
        select(BillingCheckoutSession).where(
            BillingCheckoutSession.stripe_checkout_session_id == str(sid)
        )
    ).first()
    strategy_slugs: list[str] | None = None
    if pending and pending.strategy_slugs:
        strategy_slugs = [str(s).strip().lower() for s in pending.strategy_slugs]

    stripe_sub = stripe_svc.subscriptions_retrieve(
        str(sub_id),
        expand=["items.data.price"],
    )
    upsert_subscription_from_stripe(
        db,
        tenant_id=tenant_id,
        stripe_sub=stripe_sub,
        plan=plan,
        strategy_slugs_ordered=strategy_slugs,
    )


def _subscription_sync(db: Session, sub_obj: Any, stripe_svc: StripeService) -> None:
    sid = getattr(sub_obj, "id", None) or sub_obj["id"]
    row = db.scalars(
        select(StripeSubscription).where(StripeSubscription.stripe_subscription_id == str(sid))
    ).first()
    tenant_id: uuid.UUID | None = row.tenant_id if row else None
    plan: SubscriptionPlan | None = None
    if row and row.subscription_plan_id:
        plan = db.get(SubscriptionPlan, row.subscription_plan_id)
    if tenant_id is None:
        mt = _get_meta(sub_obj, "tenant_id")
        mp = _get_meta(sub_obj, "plan_id")
        if not mt:
            return
        tenant_id = uuid.UUID(mt)
        if mp:
            plan = db.get(SubscriptionPlan, uuid.UUID(mp))

    stripe_sub = stripe_svc.subscriptions_retrieve(
        str(sid),
        expand=["items.data.price"],
    )
    upsert_subscription_from_stripe(
        db,
        tenant_id=tenant_id,
        stripe_sub=stripe_sub,
        plan=plan,
        strategy_slugs_ordered=None,
    )
