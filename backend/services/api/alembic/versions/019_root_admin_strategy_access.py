"""backfill root admin strategy access

Revision ID: 019_root_admin_strategy_access
Revises: 018_token_strategy_policy
Create Date: 2026-04-18 15:58:00.000000
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "019_root_admin_strategy_access"
down_revision: Union[str, None] = "018_token_strategy_policy"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


CORE_STRATEGIES = ("momentum", "day_trading", "dca", "reversion")


def upgrade() -> None:
    bind = op.get_bind()
    now = datetime.now(UTC)

    users = sa.table(
        "users",
        sa.column("id", sa.Uuid()),
        sa.column("is_root_admin", sa.Boolean()),
    )
    memberships = sa.table(
        "tenant_memberships",
        sa.column("user_id", sa.Uuid()),
        sa.column("tenant_id", sa.Uuid()),
        sa.column("created_at", sa.DateTime(timezone=True)),
    )
    tenant_entitlements = sa.table(
        "tenant_entitlements",
        sa.column("id", sa.Uuid()),
        sa.column("tenant_id", sa.Uuid()),
        sa.column("platform_strategy_id", sa.Uuid()),
        sa.column("source", sa.String()),
        sa.column("valid_from", sa.DateTime(timezone=True)),
        sa.column("valid_until", sa.DateTime(timezone=True)),
        sa.column("stripe_subscription_id", sa.Uuid()),
        sa.column("stripe_subscription_item_row_id", sa.Uuid()),
        sa.column("is_active", sa.Boolean()),
        sa.column("created_at", sa.DateTime(timezone=True)),
        sa.column("updated_at", sa.DateTime(timezone=True)),
    )
    platform_strategies = sa.table(
        "platform_strategies",
        sa.column("slug", sa.String()),
    )
    user_strategies = sa.table(
        "user_strategies",
        sa.column("id", sa.Uuid()),
        sa.column("user_id", sa.Uuid()),
        sa.column("strategy_id", sa.String()),
        sa.column("is_enabled", sa.Boolean()),
        sa.column("config", sa.JSON()),
        sa.column("metadata", sa.JSON()),
        sa.column("created_at", sa.DateTime(timezone=True)),
        sa.column("updated_at", sa.DateTime(timezone=True)),
    )

    membership_rows = bind.execute(
        sa.select(users.c.id, memberships.c.tenant_id)
        .select_from(users.join(memberships, users.c.id == memberships.c.user_id))
        .where(users.c.is_root_admin.is_(True))
        .order_by(users.c.id.asc(), memberships.c.created_at.asc())
    ).all()

    primary_tenants_by_user: dict[uuid.UUID, uuid.UUID] = {}
    for user_id, tenant_id in membership_rows:
        primary_tenants_by_user.setdefault(user_id, tenant_id)

    if not primary_tenants_by_user:
        return

    existing_root_admin_entitlements = {
        row.tenant_id: row.id
        for row in bind.execute(
            sa.select(tenant_entitlements.c.id, tenant_entitlements.c.tenant_id).where(
                tenant_entitlements.c.source == "root_admin",
                tenant_entitlements.c.platform_strategy_id.is_(None),
            )
        )
    }

    for tenant_id in set(primary_tenants_by_user.values()):
        entitlement_id = existing_root_admin_entitlements.get(tenant_id)
        if entitlement_id is None:
            bind.execute(
                tenant_entitlements.insert().values(
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
            continue
        bind.execute(
            tenant_entitlements.update()
            .where(tenant_entitlements.c.id == entitlement_id)
            .values(is_active=True, valid_until=None, updated_at=now)
        )

    available_strategy_ids = {
        str(row.slug).strip().lower()
        for row in bind.execute(
            sa.select(platform_strategies.c.slug).where(
                platform_strategies.c.slug.in_(CORE_STRATEGIES)
            )
        )
    }
    if not available_strategy_ids:
        return

    existing_user_strategies = {
        (row.user_id, str(row.strategy_id).strip().lower())
        for row in bind.execute(
            sa.select(user_strategies.c.user_id, user_strategies.c.strategy_id).where(
                user_strategies.c.user_id.in_(tuple(primary_tenants_by_user.keys())),
                user_strategies.c.strategy_id.in_(available_strategy_ids),
            )
        )
    }

    inserts: list[dict[str, object]] = []
    for user_id in primary_tenants_by_user:
        for strategy_id in sorted(available_strategy_ids):
            key = (user_id, strategy_id)
            if key in existing_user_strategies:
                continue
            inserts.append(
                {
                    "id": uuid.uuid4(),
                    "user_id": user_id,
                    "strategy_id": strategy_id,
                    "is_enabled": True,
                    "config": {},
                    "metadata": {"bootstrap": "root_admin"},
                    "created_at": now,
                    "updated_at": now,
                }
            )
    if inserts:
        bind.execute(user_strategies.insert(), inserts)


def downgrade() -> None:
    pass
