"""Free trial start on tenant creation."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy.orm import Session

from oziebot_api.models.tenant import Tenant
from oziebot_api.models.tenant_entitlement import TenantEntitlement
from oziebot_api.services.platform_management import get_or_create_trial_policy


def start_trial_for_new_tenant(db: Session, tenant_id: uuid.UUID) -> None:
    policy = get_or_create_trial_policy(db)
    if not policy.is_enabled:
        return
    tenant = db.get(Tenant, tenant_id)
    if tenant is None:
        return
    now = datetime.now(UTC)
    days = policy.trial_duration_days
    tenant.trial_started_at = now
    tenant.trial_ends_at = now + timedelta(days=days)
    db.add(
        TenantEntitlement(
            id=uuid.uuid4(),
            tenant_id=tenant_id,
            platform_strategy_id=None,
            source="trial",
            valid_from=now,
            valid_until=tenant.trial_ends_at,
            stripe_subscription_id=None,
            stripe_subscription_item_row_id=None,
            is_active=True,
            created_at=now,
            updated_at=now,
        )
    )
    db.flush()
