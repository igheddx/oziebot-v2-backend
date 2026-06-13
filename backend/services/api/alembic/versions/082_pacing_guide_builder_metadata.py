"""Pacing guide builder metadata for unit/week daily plans."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "082_pacing_guide_builder_metadata"
down_revision = "081_teacher_assist_v2_google_forms"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("pacing_guides", sa.Column("metadata_json", sa.JSON(), nullable=True))
    op.add_column("pacing_guide_periods", sa.Column("metadata_json", sa.JSON(), nullable=True))


def downgrade() -> None:
    op.drop_column("pacing_guide_periods", "metadata_json")
    op.drop_column("pacing_guides", "metadata_json")
