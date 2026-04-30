"""composite index for latest BBO per product (dashboard marks)

Revision ID: 029_bbo_product_event_idx
Revises: 028_trade_outcome_retention
Create Date: 2026-04-30
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

revision: str = "029_bbo_product_event_idx"
down_revision: Union[str, None] = "028_trade_outcome_retention"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index(
        "ix_market_data_bbo_product_event_time",
        "market_data_bbo_snapshots",
        ["product_id", "event_time"],
    )


def downgrade() -> None:
    op.drop_index("ix_market_data_bbo_product_event_time", table_name="market_data_bbo_snapshots")
