"""Encrypted exchange connections (Coinbase) per tenant

Revision ID: 006
Revises: 005
Create Date: 2026-04-12

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "006"
down_revision: Union[str, None] = "005"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "exchange_connections",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("provider", sa.String(length=32), nullable=False, server_default="coinbase"),
        sa.Column("api_key_name", sa.String(length=512), nullable=False),
        sa.Column("encrypted_secret", sa.LargeBinary(), nullable=False),
        sa.Column("secret_ciphertext_version", sa.SmallInteger(), nullable=False, server_default="1"),
        sa.Column("validation_status", sa.String(length=32), nullable=False, server_default="never_validated"),
        sa.Column("last_validated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("health_status", sa.String(length=32), nullable=True),
        sa.Column("last_health_check_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("can_trade", sa.Boolean(), nullable=True),
        sa.Column("can_read_balances", sa.Boolean(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("tenant_id", "provider", name="uq_exchange_connections_tenant_provider"),
    )
    op.create_index("ix_exchange_connections_tenant", "exchange_connections", ["tenant_id"])


def downgrade() -> None:
    op.drop_table("exchange_connections")
