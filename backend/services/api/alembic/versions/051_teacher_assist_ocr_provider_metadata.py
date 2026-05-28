"""add teacher assist ocr provider metadata columns

Revision ID: 051_teacher_assist_ocr_provider_metadata
Revises: 050_teacher_assist_extraction_review_workspace
Create Date: 2026-05-28 16:00:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

from alembic import op
import sqlalchemy as sa


revision: str = "051_teacher_assist_ocr_provider_metadata"
down_revision: Union[str, None] = "050_teacher_assist_extraction_review_workspace"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "teacher_assist_extraction_jobs",
        sa.Column("provider_model", sa.String(length=128), nullable=True),
    )
    op.add_column(
        "teacher_assist_extraction_jobs",
        sa.Column("provider_version", sa.String(length=64), nullable=True),
    )
    op.add_column(
        "teacher_assist_extraction_jobs",
        sa.Column("provider_mode", sa.String(length=16), nullable=True),
    )
    op.add_column(
        "teacher_assist_extraction_jobs",
        sa.Column("page_count", sa.Integer(), nullable=True),
    )
    op.add_column(
        "teacher_assist_extraction_jobs",
        sa.Column("processing_duration_ms", sa.Integer(), nullable=True),
    )
    op.add_column(
        "teacher_assist_extraction_jobs",
        sa.Column("estimated_cost_cents", sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("teacher_assist_extraction_jobs", "estimated_cost_cents")
    op.drop_column("teacher_assist_extraction_jobs", "processing_duration_ms")
    op.drop_column("teacher_assist_extraction_jobs", "page_count")
    op.drop_column("teacher_assist_extraction_jobs", "provider_mode")
    op.drop_column("teacher_assist_extraction_jobs", "provider_version")
    op.drop_column("teacher_assist_extraction_jobs", "provider_model")
