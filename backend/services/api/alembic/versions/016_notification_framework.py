"""Add notification channels, preferences, and delivery attempts

Revision ID: 016
Revises: 015
Create Date: 2026-04-12

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "016"
down_revision: Union[str, None] = "015"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "notification_channel_configs",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("channel", sa.String(length=32), nullable=False),
        sa.Column("destination", sa.String(length=256), nullable=False),
        sa.Column("is_enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("settings", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("user_id", "channel", name="uq_notification_channel_user_channel"),
    )
    op.create_index("ix_notification_channel_configs_user_id", "notification_channel_configs", ["user_id"])
    op.create_index("ix_notification_channel_configs_channel", "notification_channel_configs", ["channel"])

    op.create_table(
        "notification_preferences",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("trading_mode", sa.String(length=16), nullable=False),
        sa.Column("is_enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "user_id",
            "event_type",
            "trading_mode",
            name="uq_notification_pref_user_event_mode",
        ),
    )
    op.create_index("ix_notification_preferences_user_id", "notification_preferences", ["user_id"])
    op.create_index("ix_notification_preferences_event_type", "notification_preferences", ["event_type"])
    op.create_index("ix_notification_preferences_trading_mode", "notification_preferences", ["trading_mode"])

    op.create_table(
        "notification_delivery_attempts",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("event_id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("trading_mode", sa.String(length=16), nullable=False),
        sa.Column("channel", sa.String(length=32), nullable=False),
        sa.Column("destination", sa.String(length=256), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("error", sa.String(length=512), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_notification_delivery_attempts_event_id", "notification_delivery_attempts", ["event_id"])
    op.create_index("ix_notification_delivery_attempts_tenant_id", "notification_delivery_attempts", ["tenant_id"])
    op.create_index("ix_notification_delivery_attempts_user_id", "notification_delivery_attempts", ["user_id"])
    op.create_index("ix_notification_delivery_attempts_event_type", "notification_delivery_attempts", ["event_type"])
    op.create_index("ix_notification_delivery_attempts_trading_mode", "notification_delivery_attempts", ["trading_mode"])
    op.create_index("ix_notification_delivery_attempts_channel", "notification_delivery_attempts", ["channel"])
    op.create_index("ix_notification_delivery_attempts_status", "notification_delivery_attempts", ["status"])


def downgrade() -> None:
    op.drop_table("notification_delivery_attempts")
    op.drop_table("notification_preferences")
    op.drop_table("notification_channel_configs")