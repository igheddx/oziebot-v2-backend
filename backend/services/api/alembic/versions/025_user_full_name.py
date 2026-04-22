"""add user full name

Revision ID: 025_user_full_name
Revises: 024_position_lifecycle_ts
Create Date: 2026-04-22
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "025_user_full_name"
down_revision: Union[str, None] = "024_position_lifecycle_ts"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("users", sa.Column("full_name", sa.String(length=256), nullable=True))
    op.execute(
        sa.text(
            """
            UPDATE users
            SET full_name = :full_name
            WHERE lower(email) = :email
              AND (full_name IS NULL OR full_name = '')
            """
        ).bindparams(full_name="Dominic Ighedosa", email="dominic@oziebot.com")
    )


def downgrade() -> None:
    op.drop_column("users", "full_name")
