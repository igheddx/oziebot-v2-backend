"""Postgres-backed worker queue, runtime KV cache, trade logs.

Replaces Redis for message queues and ephemeral cache keys.
Revision ID: 030_no_redis_outbox_kv
Revises: 029_bbo_product_event_idx
"""

from alembic import op
import sqlalchemy as sa


revision = "030_no_redis_outbox_kv"
down_revision = "029_bbo_product_event_idx"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "worker_message_outbox",
        sa.Column(
            "id",
            sa.Uuid(as_uuid=False),
            server_default=sa.text("gen_random_uuid()"),
            primary_key=True,
        ),
        sa.Column("queue_name", sa.String(length=512), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="pending"),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("retry_after", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'leased', 'dead')", name="ck_worker_outbox_status"
        ),
    )
    op.create_index(
        "ix_worker_outbox_queue_created",
        "worker_message_outbox",
        ["queue_name", "created_at"],
        postgresql_where=sa.text("status = 'pending'"),
    )
    op.create_index(
        "ix_worker_outbox_stale_lease",
        "worker_message_outbox",
        ["lease_expires_at"],
        postgresql_where=sa.text("status = 'leased'"),
    )

    op.create_table(
        "runtime_kv",
        sa.Column("cache_key", sa.String(length=1024), primary_key=True),
        sa.Column("value_text", sa.Text(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_runtime_kv_expires",
        "runtime_kv",
        ["expires_at"],
    )

    op.create_table(
        "trade_raw_log",
        sa.Column(
            "id",
            sa.Uuid(as_uuid=False),
            server_default=sa.text("gen_random_uuid()"),
            primary_key=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column("symbol", sa.String(length=64), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
    )
    op.create_index(
        "ix_trade_raw_sym_time",
        "trade_raw_log",
        ["symbol", "created_at"],
        postgresql_ops={"created_at": "DESC NULLS LAST"},
    )

    op.create_table(
        "trade_signal_samples",
        sa.Column(
            "id",
            sa.Uuid(as_uuid=False),
            server_default=sa.text("gen_random_uuid()"),
            primary_key=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column("symbol", sa.String(length=64), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
    )
    op.create_index(
        "ix_trade_sig_samples_sym_time",
        "trade_signal_samples",
        ["symbol", "created_at"],
        postgresql_ops={"created_at": "DESC NULLS LAST"},
    )

    op.create_table(
        "trade_signal_summaries",
        sa.Column("symbol", sa.String(length=64), primary_key=True),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
    )


def downgrade() -> None:
    op.drop_table("trade_signal_summaries")
    op.drop_table("trade_signal_samples")
    op.drop_table("trade_raw_log")
    op.drop_table("runtime_kv")
    op.drop_table("worker_message_outbox")
