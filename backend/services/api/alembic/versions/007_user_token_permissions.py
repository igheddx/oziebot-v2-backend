"""Add user token permissions - two-tier token trading allowlist

Revision ID: 007
Revises: 006
Create Date: 2026-04-12

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "007"
down_revision: Union[str, None] = "006"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "user_token_permissions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("platform_token_id", sa.Uuid(), nullable=False),
        sa.Column("is_enabled", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["platform_token_id"], ["platform_token_allowlist.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("user_id", "platform_token_id", name="uq_user_token_permission"),
    )
    op.create_index("ix_user_token_permissions_user_id", "user_token_permissions", ["user_id"])
    op.create_index("ix_user_token_permissions_platform_token_id", "user_token_permissions", ["platform_token_id"])


def downgrade() -> None:
    op.drop_table("user_token_permissions")
