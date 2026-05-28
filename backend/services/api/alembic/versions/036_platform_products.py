"""add platform product access foundation

Revision ID: 036_platform_products
Revises: 035_volatility_harvest
Create Date: 2026-05-26 21:39:20.000000
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Union
import uuid

from alembic import op
import sqlalchemy as sa


revision: str = "036_platform_products"
down_revision: Union[str, None] = "035_volatility_harvest"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "platform_products",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("product_key", sa.String(length=64), nullable=False),
        sa.Column("display_name", sa.String(length=128), nullable=False),
        sa.Column("description", sa.String(length=512), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("product_key"),
    )
    op.create_index(
        "ix_platform_products_product_key",
        "platform_products",
        ["product_key"],
        unique=True,
    )

    op.create_table(
        "tenant_product_access",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("product_id", sa.Uuid(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="active"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["product_id"], ["platform_products.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("tenant_id", "product_id", name="uq_tenant_product_access"),
    )
    op.create_index(
        "ix_tenant_product_access_tenant_id",
        "tenant_product_access",
        ["tenant_id"],
    )
    op.create_index(
        "ix_tenant_product_access_product_id",
        "tenant_product_access",
        ["product_id"],
    )

    op.create_table(
        "user_product_preferences",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("default_product_id", sa.Uuid(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["default_product_id"], ["platform_products.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_user_product_preferences_user_id",
        "user_product_preferences",
        ["user_id"],
        unique=True,
    )

    bind = op.get_bind()
    now = datetime.now(UTC)
    platform_products = sa.table(
        "platform_products",
        sa.column("id", sa.Uuid()),
        sa.column("product_key", sa.String()),
        sa.column("display_name", sa.String()),
        sa.column("description", sa.String()),
        sa.column("is_active", sa.Boolean()),
        sa.column("created_at", sa.DateTime(timezone=True)),
        sa.column("updated_at", sa.DateTime(timezone=True)),
    )
    tenant_product_access = sa.table(
        "tenant_product_access",
        sa.column("id", sa.Uuid()),
        sa.column("tenant_id", sa.Uuid()),
        sa.column("product_id", sa.Uuid()),
        sa.column("status", sa.String()),
        sa.column("created_at", sa.DateTime(timezone=True)),
        sa.column("updated_at", sa.DateTime(timezone=True)),
    )
    user_product_preferences = sa.table(
        "user_product_preferences",
        sa.column("id", sa.Uuid()),
        sa.column("user_id", sa.Uuid()),
        sa.column("default_product_id", sa.Uuid()),
        sa.column("created_at", sa.DateTime(timezone=True)),
        sa.column("updated_at", sa.DateTime(timezone=True)),
    )
    tenant_memberships = sa.table(
        "tenant_memberships",
        sa.column("user_id", sa.Uuid()),
    )
    tenants = sa.table("tenants", sa.column("id", sa.Uuid()))

    trading_product_id = uuid.uuid4()
    teacher_assist_product_id = uuid.uuid4()
    bind.execute(
        sa.insert(platform_products),
        [
            {
                "id": trading_product_id,
                "product_key": "trading",
                "display_name": "Oziebot Trading",
                "description": "Oziebot trading console and portfolio workflows.",
                "is_active": True,
                "created_at": now,
                "updated_at": now,
            },
            {
                "id": teacher_assist_product_id,
                "product_key": "teacher_assist",
                "display_name": "TeacherAssist AI",
                "description": "Teacher planning and classroom workflow product module.",
                "is_active": True,
                "created_at": now,
                "updated_at": now,
            },
        ],
    )

    tenant_ids = [row[0] for row in bind.execute(sa.select(tenants.c.id)).all()]
    if tenant_ids:
        bind.execute(
            sa.insert(tenant_product_access),
            [
                {
                    "id": uuid.uuid4(),
                    "tenant_id": tenant_id,
                    "product_id": trading_product_id,
                    "status": "active",
                    "created_at": now,
                    "updated_at": now,
                }
                for tenant_id in tenant_ids
            ],
        )

    user_ids = sorted({row[0] for row in bind.execute(sa.select(tenant_memberships.c.user_id)).all()})
    if user_ids:
        bind.execute(
            sa.insert(user_product_preferences),
            [
                {
                    "id": uuid.uuid4(),
                    "user_id": user_id,
                    "default_product_id": trading_product_id,
                    "created_at": now,
                    "updated_at": now,
                }
                for user_id in user_ids
            ],
        )


def downgrade() -> None:
    op.drop_index("ix_user_product_preferences_user_id", table_name="user_product_preferences")
    op.drop_table("user_product_preferences")
    op.drop_index("ix_tenant_product_access_product_id", table_name="tenant_product_access")
    op.drop_index("ix_tenant_product_access_tenant_id", table_name="tenant_product_access")
    op.drop_table("tenant_product_access")
    op.drop_index("ix_platform_products_product_key", table_name="platform_products")
    op.drop_table("platform_products")
