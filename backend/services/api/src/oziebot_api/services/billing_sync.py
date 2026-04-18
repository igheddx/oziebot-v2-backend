"""Map Stripe subscription objects to local billing rows and entitlements."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from oziebot_api.models.platform_strategy import PlatformStrategy
from oziebot_api.models.stripe_subscription import StripeSubscription
from oziebot_api.models.stripe_subscription_item import StripeSubscriptionItem
from oziebot_api.models.subscription_plan import SubscriptionPlan
from oziebot_api.models.tenant_entitlement import TenantEntitlement
from oziebot_api.services.entitlements import revoke_subscription_entitlements


def _stripe_ts(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    return datetime.fromtimestamp(int(value), tz=UTC)


def _subscription_items_data(stripe_sub: Any) -> list[Any]:
    items = getattr(stripe_sub, "items", None)
    if items is None and isinstance(stripe_sub, dict):
        items = stripe_sub.get("items")
    if items is None:
        return []
    data = getattr(items, "data", None)
    if data is None and isinstance(items, dict):
        data = items.get("data", [])
    return list(data or [])


def upsert_subscription_from_stripe(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    stripe_sub: Any,
    plan: SubscriptionPlan | None,
    strategy_slugs_ordered: list[str] | None,
) -> StripeSubscription:
    stripe_id = getattr(stripe_sub, "id", None) or stripe_sub["id"]
    customer_id = getattr(stripe_sub, "customer", None) or stripe_sub.get("customer")
    status = getattr(stripe_sub, "status", None) or stripe_sub.get("status")
    now = datetime.now(UTC)
    stripe_meta = (
        dict(getattr(stripe_sub, "metadata", {}) or {})
        if not isinstance(stripe_sub, dict)
        else dict(stripe_sub.get("metadata") or {})
    )

    row = db.scalars(
        select(StripeSubscription).where(StripeSubscription.stripe_subscription_id == stripe_id)
    ).first()
    preserved_slugs: list[str] | None = None
    if row and row.metadata_json:
        raw = row.metadata_json.get("_strategy_slugs_ordered")
        if isinstance(raw, list):
            preserved_slugs = [str(x) for x in raw]
    if strategy_slugs_ordered is None:
        strategy_slugs_ordered = preserved_slugs
    if row is None:
        row = StripeSubscription(
            id=uuid.uuid4(),
            tenant_id=tenant_id,
            stripe_subscription_id=stripe_id,
            stripe_customer_id=str(customer_id),
            status=status,
            subscription_plan_id=plan.id if plan else None,
            primary_stripe_price_id=None,
            current_period_start=_stripe_ts(
                getattr(stripe_sub, "current_period_start", None)
                or (
                    stripe_sub.get("current_period_start") if isinstance(stripe_sub, dict) else None
                )
            ),
            current_period_end=_stripe_ts(
                getattr(stripe_sub, "current_period_end", None)
                or (stripe_sub.get("current_period_end") if isinstance(stripe_sub, dict) else None)
            ),
            cancel_at_period_end=bool(
                getattr(stripe_sub, "cancel_at_period_end", None)
                if not isinstance(stripe_sub, dict)
                else stripe_sub.get("cancel_at_period_end", False)
            ),
            metadata_json=stripe_meta,
            created_at=now,
            updated_at=now,
        )
        db.add(row)
    else:
        row.tenant_id = tenant_id
        row.stripe_customer_id = str(customer_id)
        row.status = status
        if plan is not None:
            row.subscription_plan_id = plan.id
        row.current_period_start = _stripe_ts(
            getattr(stripe_sub, "current_period_start", None)
            or (stripe_sub.get("current_period_start") if isinstance(stripe_sub, dict) else None)
        )
        row.current_period_end = _stripe_ts(
            getattr(stripe_sub, "current_period_end", None)
            or (stripe_sub.get("current_period_end") if isinstance(stripe_sub, dict) else None)
        )
        row.cancel_at_period_end = bool(
            getattr(stripe_sub, "cancel_at_period_end", None)
            if not isinstance(stripe_sub, dict)
            else stripe_sub.get("cancel_at_period_end", False)
        )
        row.metadata_json = stripe_meta
        row.updated_at = now

    db.flush()
    merged_meta = dict(stripe_meta)
    if strategy_slugs_ordered is not None:
        merged_meta["_strategy_slugs_ordered"] = strategy_slugs_ordered
    row.metadata_json = merged_meta
    db.flush()

    db.execute(
        delete(StripeSubscriptionItem).where(
            StripeSubscriptionItem.stripe_subscription_row_id == row.id,
        )
    )
    revoke_subscription_entitlements(db, row.id)
    db.flush()

    items_data = _subscription_items_data(stripe_sub)
    if items_data:
        primary_price = (
            getattr(items_data[0].price, "id", None)
            if not isinstance(items_data[0], dict)
            else items_data[0].get("price", {}).get("id")
        )
        row.primary_stripe_price_id = primary_price

    entitling_statuses = status in ("active", "trialing")
    if not entitling_statuses or not plan:
        db.flush()
        return row

    if plan.plan_kind == "all_strategies":
        for si in items_data:
            si_id = getattr(si, "id", None) or si["id"]
            price_obj = getattr(si, "price", None) or si.get("price")
            if isinstance(price_obj, dict):
                pid = price_obj.get("id")
            else:
                pid = getattr(price_obj, "id", None)
            item_row = StripeSubscriptionItem(
                id=uuid.uuid4(),
                stripe_subscription_row_id=row.id,
                stripe_subscription_item_id=str(si_id),
                stripe_price_id=str(pid),
                platform_strategy_id=None,
                quantity=int(getattr(si, "quantity", 1) or si.get("quantity", 1) or 1),
                created_at=now,
                updated_at=now,
            )
            db.add(item_row)
        db.flush()
        valid_until = row.current_period_end
        db.add(
            TenantEntitlement(
                id=uuid.uuid4(),
                tenant_id=tenant_id,
                platform_strategy_id=None,
                source="subscription",
                valid_from=now,
                valid_until=valid_until,
                stripe_subscription_id=row.id,
                stripe_subscription_item_row_id=None,
                is_active=True,
                created_at=now,
                updated_at=now,
            )
        )
        db.flush()
        return row

    # per_strategy: map line items to strategies by order
    slugs = strategy_slugs_ordered or []
    for i, si in enumerate(items_data):
        si_id = getattr(si, "id", None) or si["id"]
        price_obj = getattr(si, "price", None) or si.get("price")
        if isinstance(price_obj, dict):
            pid = price_obj.get("id")
        else:
            pid = getattr(price_obj, "id", None)
        strat_id: uuid.UUID | None = None
        if i < len(slugs):
            slug = slugs[i].strip().lower()
            strat = db.scalar(select(PlatformStrategy).where(PlatformStrategy.slug == slug))
            if strat is not None:
                strat_id = strat.id
        item_row = StripeSubscriptionItem(
            id=uuid.uuid4(),
            stripe_subscription_row_id=row.id,
            stripe_subscription_item_id=str(si_id),
            stripe_price_id=str(pid),
            platform_strategy_id=strat_id,
            quantity=int(getattr(si, "quantity", 1) or si.get("quantity", 1) or 1),
            created_at=now,
            updated_at=now,
        )
        db.add(item_row)
    db.flush()

    valid_until = row.current_period_end
    for item_row in db.scalars(
        select(StripeSubscriptionItem).where(
            StripeSubscriptionItem.stripe_subscription_row_id == row.id,
        )
    ).all():
        if item_row.platform_strategy_id is None:
            continue
        db.add(
            TenantEntitlement(
                id=uuid.uuid4(),
                tenant_id=tenant_id,
                platform_strategy_id=item_row.platform_strategy_id,
                source="subscription",
                valid_from=now,
                valid_until=valid_until,
                stripe_subscription_id=row.id,
                stripe_subscription_item_row_id=item_row.id,
                is_active=True,
                created_at=now,
                updated_at=now,
            )
        )
    db.flush()
    return row
