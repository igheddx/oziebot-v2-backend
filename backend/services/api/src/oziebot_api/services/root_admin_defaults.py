from __future__ import annotations

import uuid
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.platform_strategy import PlatformStrategy
from oziebot_api.models.tenant_entitlement import TenantEntitlement
from oziebot_api.models.user import User
from oziebot_api.models.user_strategy import UserStrategy
from oziebot_api.services.strategy_catalog import ensure_platform_strategy_catalog
from oziebot_api.services.tenant_scope import primary_tenant_id


def ensure_root_admin_strategy_access(db: Session, user: User) -> None:
    if not user.is_root_admin:
        return

    tenant_id = primary_tenant_id(db, user)
    if tenant_id is None:
        return

    ensure_platform_strategy_catalog(db)

    from oziebot_strategy_engine.registry import StrategyRegistry

    strategy_slugs = [
        str(item["strategy_id"]).strip().lower() for item in StrategyRegistry.list_strategies()
    ]
    if not strategy_slugs:
        return

    now = datetime.now(UTC)
    entitlement = db.scalars(
        select(TenantEntitlement)
        .where(
            TenantEntitlement.tenant_id == tenant_id,
            TenantEntitlement.source == "root_admin",
            TenantEntitlement.platform_strategy_id.is_(None),
        )
        .limit(1)
    ).first()
    if entitlement is None:
        db.add(
            TenantEntitlement(
                id=uuid.uuid4(),
                tenant_id=tenant_id,
                platform_strategy_id=None,
                source="root_admin",
                valid_from=now,
                valid_until=None,
                stripe_subscription_id=None,
                stripe_subscription_item_row_id=None,
                is_active=True,
                created_at=now,
                updated_at=now,
            )
        )
    else:
        entitlement.is_active = True
        entitlement.valid_until = None
        entitlement.updated_at = now

    platform_rows = db.scalars(
        select(PlatformStrategy.slug).where(PlatformStrategy.slug.in_(strategy_slugs))
    ).all()
    platform_strategy_ids = {str(slug).strip().lower() for slug in platform_rows}
    existing = set(
        db.scalars(
            select(UserStrategy.strategy_id).where(
                UserStrategy.user_id == user.id,
                UserStrategy.strategy_id.in_(platform_strategy_ids),
            )
        ).all()
    )
    for strategy_id in sorted(platform_strategy_ids):
        if strategy_id in existing:
            continue
        db.add(
            UserStrategy(
                id=uuid.uuid4(),
                user_id=user.id,
                strategy_id=strategy_id,
                is_enabled=True,
                config={},
                metadata_json={"bootstrap": "root_admin"},
                created_at=now,
                updated_at=now,
            )
        )

    db.flush()
