"""add teacher assist extraction review workspace

Revision ID: 050_teacher_assist_extraction_review_workspace
Revises: 049_teacher_assist_extraction_foundation
Create Date: 2026-05-28 14:00:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

from alembic import op
import sqlalchemy as sa


revision: str = "050_teacher_assist_extraction_review_workspace"
down_revision: Union[str, None] = "049_teacher_assist_extraction_foundation"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "teacher_assist_extraction_jobs",
        sa.Column("parent_extraction_job_id", sa.Uuid(), nullable=True),
    )
    op.add_column(
        "teacher_assist_extraction_jobs",
        sa.Column("retry_root_job_id", sa.Uuid(), nullable=True),
    )
    op.add_column(
        "teacher_assist_extraction_jobs",
        sa.Column("attempt_number", sa.Integer(), nullable=False, server_default="1"),
    )
    op.create_foreign_key(
        "fk_ta_extraction_jobs_parent_job_id",
        "teacher_assist_extraction_jobs",
        "teacher_assist_extraction_jobs",
        ["parent_extraction_job_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_ta_extraction_jobs_retry_root_id",
        "teacher_assist_extraction_jobs",
        "teacher_assist_extraction_jobs",
        ["retry_root_job_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        op.f("ix_teacher_assist_extraction_jobs_parent_extraction_job_id"),
        "teacher_assist_extraction_jobs",
        ["parent_extraction_job_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_teacher_assist_extraction_jobs_retry_root_job_id"),
        "teacher_assist_extraction_jobs",
        ["retry_root_job_id"],
        unique=False,
    )

    op.add_column(
        "teacher_assist_extracted_text_records",
        sa.Column("review_status", sa.String(length=32), nullable=False, server_default="pending_review"),
    )
    op.add_column(
        "teacher_assist_extracted_text_records",
        sa.Column("provider_confidence_score", sa.Float(), nullable=True),
    )
    op.add_column(
        "teacher_assist_extracted_text_records",
        sa.Column("confidence_level", sa.String(length=16), nullable=False, server_default="unknown"),
    )
    op.add_column(
        "teacher_assist_extracted_text_records",
        sa.Column("teacher_corrected_text", sa.Text(), nullable=True),
    )
    op.add_column(
        "teacher_assist_extracted_text_records",
        sa.Column("approved_text", sa.Text(), nullable=True),
    )
    op.add_column(
        "teacher_assist_extracted_text_records",
        sa.Column("reviewed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "teacher_assist_extracted_text_records",
        sa.Column("reviewed_by_user_id", sa.Uuid(), nullable=True),
    )
    op.add_column(
        "teacher_assist_extracted_text_records",
        sa.Column("source_extraction_job_id", sa.Uuid(), nullable=True),
    )
    op.create_foreign_key(
        "fk_ta_extracted_text_reviewed_by_user_id",
        "teacher_assist_extracted_text_records",
        "users",
        ["reviewed_by_user_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_ta_extracted_text_source_job_id",
        "teacher_assist_extracted_text_records",
        "teacher_assist_extraction_jobs",
        ["source_extraction_job_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        op.f("ix_teacher_assist_extracted_text_records_review_status"),
        "teacher_assist_extracted_text_records",
        ["review_status"],
        unique=False,
    )
    op.create_index(
        op.f("ix_teacher_assist_extracted_text_records_confidence_level"),
        "teacher_assist_extracted_text_records",
        ["confidence_level"],
        unique=False,
    )
    op.create_index(
        op.f("ix_teacher_assist_extracted_text_records_source_extraction_job_id"),
        "teacher_assist_extracted_text_records",
        ["source_extraction_job_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_teacher_assist_extracted_text_records_source_extraction_job_id"),
        table_name="teacher_assist_extracted_text_records",
    )
    op.drop_index(
        op.f("ix_teacher_assist_extracted_text_records_confidence_level"),
        table_name="teacher_assist_extracted_text_records",
    )
    op.drop_index(
        op.f("ix_teacher_assist_extracted_text_records_review_status"),
        table_name="teacher_assist_extracted_text_records",
    )
    op.drop_constraint(
        "fk_ta_extracted_text_source_job_id",
        "teacher_assist_extracted_text_records",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_ta_extracted_text_reviewed_by_user_id",
        "teacher_assist_extracted_text_records",
        type_="foreignkey",
    )
    op.drop_column("teacher_assist_extracted_text_records", "source_extraction_job_id")
    op.drop_column("teacher_assist_extracted_text_records", "reviewed_by_user_id")
    op.drop_column("teacher_assist_extracted_text_records", "reviewed_at")
    op.drop_column("teacher_assist_extracted_text_records", "approved_text")
    op.drop_column("teacher_assist_extracted_text_records", "teacher_corrected_text")
    op.drop_column("teacher_assist_extracted_text_records", "confidence_level")
    op.drop_column("teacher_assist_extracted_text_records", "provider_confidence_score")
    op.drop_column("teacher_assist_extracted_text_records", "review_status")

    op.drop_index(
        op.f("ix_teacher_assist_extraction_jobs_retry_root_job_id"),
        table_name="teacher_assist_extraction_jobs",
    )
    op.drop_index(
        op.f("ix_teacher_assist_extraction_jobs_parent_extraction_job_id"),
        table_name="teacher_assist_extraction_jobs",
    )
    op.drop_constraint(
        "fk_ta_extraction_jobs_retry_root_id",
        "teacher_assist_extraction_jobs",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_ta_extraction_jobs_parent_job_id",
        "teacher_assist_extraction_jobs",
        type_="foreignkey",
    )
    op.drop_column("teacher_assist_extraction_jobs", "attempt_number")
    op.drop_column("teacher_assist_extraction_jobs", "retry_root_job_id")
    op.drop_column("teacher_assist_extraction_jobs", "parent_extraction_job_id")
