"""Add market-data historical tables

Revision ID: 011
Revises: 010
Create Date: 2026-04-12

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "011"
down_revision: Union[str, None] = "010"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "market_data_candles",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("source", sa.String(length=32), nullable=False),
        sa.Column("product_id", sa.String(length=32), nullable=False),
        sa.Column("granularity_sec", sa.Integer(), nullable=False),
        sa.Column("bucket_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("open", sa.Numeric(precision=28, scale=10), nullable=False),
        sa.Column("high", sa.Numeric(precision=28, scale=10), nullable=False),
        sa.Column("low", sa.Numeric(precision=28, scale=10), nullable=False),
        sa.Column("close", sa.Numeric(precision=28, scale=10), nullable=False),
        sa.Column("volume", sa.Numeric(precision=28, scale=10), nullable=False),
        sa.Column("event_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ingest_time", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "source",
            "product_id",
            "granularity_sec",
            "bucket_start",
            name="uq_market_data_candles_bucket",
        ),
    )
    op.create_index("ix_market_data_candles_source", "market_data_candles", ["source"])
    op.create_index("ix_market_data_candles_product_id", "market_data_candles", ["product_id"])
    op.create_index("ix_market_data_candles_bucket_start", "market_data_candles", ["bucket_start"])

    op.create_table(
        "market_data_trade_snapshots",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("source", sa.String(length=32), nullable=False),
        sa.Column("product_id", sa.String(length=32), nullable=False),
        sa.Column("trade_id", sa.String(length=64), nullable=False),
        sa.Column("side", sa.String(length=8), nullable=False),
        sa.Column("price", sa.Numeric(precision=28, scale=10), nullable=False),
        sa.Column("size", sa.Numeric(precision=28, scale=10), nullable=False),
        sa.Column("event_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ingest_time", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "source", "product_id", "trade_id", name="uq_market_data_trade_snapshot"
        ),
    )
    op.create_index(
        "ix_market_data_trade_snapshots_source", "market_data_trade_snapshots", ["source"]
    )
    op.create_index(
        "ix_market_data_trade_snapshots_product_id", "market_data_trade_snapshots", ["product_id"]
    )
    op.create_index(
        "ix_market_data_trade_snapshots_trade_id", "market_data_trade_snapshots", ["trade_id"]
    )
    op.create_index(
        "ix_market_data_trade_snapshots_event_time", "market_data_trade_snapshots", ["event_time"]
    )

    op.create_table(
        "market_data_bbo_snapshots",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("source", sa.String(length=32), nullable=False),
        sa.Column("product_id", sa.String(length=32), nullable=False),
        sa.Column("best_bid_price", sa.Numeric(precision=28, scale=10), nullable=False),
        sa.Column("best_bid_size", sa.Numeric(precision=28, scale=10), nullable=False),
        sa.Column("best_ask_price", sa.Numeric(precision=28, scale=10), nullable=False),
        sa.Column("best_ask_size", sa.Numeric(precision=28, scale=10), nullable=False),
        sa.Column("event_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ingest_time", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_market_data_bbo_snapshots_source", "market_data_bbo_snapshots", ["source"])
    op.create_index(
        "ix_market_data_bbo_snapshots_product_id", "market_data_bbo_snapshots", ["product_id"]
    )
    op.create_index(
        "ix_market_data_bbo_snapshots_event_time", "market_data_bbo_snapshots", ["event_time"]
    )


def downgrade() -> None:
    op.drop_table("market_data_bbo_snapshots")
    op.drop_table("market_data_trade_snapshots")
    op.drop_table("market_data_candles")
