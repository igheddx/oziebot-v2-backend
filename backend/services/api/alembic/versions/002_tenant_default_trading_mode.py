"""tenant default_trading_mode

Revision ID: 002
Revises: 001
Create Date: 2026-04-12

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "002"
down_revision: Union[str, None] = "001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "tenants",
        sa.Column(
            "default_trading_mode",
            sa.String(length=16),
            nullable=False,
            server_default="paper",
        ),
    )


def downgrade() -> None:
    op.drop_column("tenants", "default_trading_mode")
