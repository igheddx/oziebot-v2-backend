"""add teacher assist setup foundation

Revision ID: 037_teacher_assist_foundation
Revises: 036_platform_products
Create Date: 2026-05-26 22:10:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

from alembic import op
import sqlalchemy as sa


revision: str = "037_teacher_assist_foundation"
down_revision: Union[str, None] = "036_platform_products"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "teacher_profiles",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("preferred_grade_level", sa.String(length=32), nullable=True),
        sa.Column("default_student_count", sa.Integer(), nullable=True),
        sa.Column("preferred_grading_period_type", sa.String(length=32), nullable=True),
        sa.Column("timezone", sa.String(length=128), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_teacher_profiles_user_id", "teacher_profiles", ["user_id"], unique=True)

    op.create_table(
        "school_years",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("title", sa.String(length=64), nullable=False),
        sa.Column("start_date", sa.Date(), nullable=False),
        sa.Column("end_date", sa.Date(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_school_years_tenant_id", "school_years", ["tenant_id"])

    op.create_table(
        "grading_periods",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("school_year_id", sa.Uuid(), nullable=False),
        sa.Column("title", sa.String(length=128), nullable=False),
        sa.Column("grading_period_type", sa.String(length=32), nullable=False),
        sa.Column("start_date", sa.Date(), nullable=False),
        sa.Column("end_date", sa.Date(), nullable=False),
        sa.Column("sort_order", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["school_year_id"], ["school_years.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_grading_periods_school_year_id", "grading_periods", ["school_year_id"])

    op.create_table(
        "subjects",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("code", sa.String(length=32), nullable=True),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_subjects_tenant_id", "subjects", ["tenant_id"])

    op.create_table(
        "classes",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("school_year_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column("grade_level", sa.String(length=32), nullable=False),
        sa.Column("student_count", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["school_year_id"], ["school_years.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_classes_tenant_id", "classes", ["tenant_id"])
    op.create_index("ix_classes_school_year_id", "classes", ["school_year_id"])

    op.create_table(
        "class_subjects",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("class_id", sa.Uuid(), nullable=False),
        sa.Column("subject_id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["class_id"], ["classes.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["subject_id"], ["subjects.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("class_id", "subject_id", name="uq_class_subject_pair"),
    )
    op.create_index("ix_class_subjects_class_id", "class_subjects", ["class_id"])
    op.create_index("ix_class_subjects_subject_id", "class_subjects", ["subject_id"])

    op.create_table(
        "standards",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("subject_id", sa.Uuid(), nullable=True),
        sa.Column("standard_type", sa.String(length=32), nullable=False),
        sa.Column("code", sa.String(length=64), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("grade_level", sa.String(length=32), nullable=True),
        sa.Column("school_year_id", sa.Uuid(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["school_year_id"], ["school_years.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["subject_id"], ["subjects.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_standards_tenant_id", "standards", ["tenant_id"])
    op.create_index("ix_standards_subject_id", "standards", ["subject_id"])
    op.create_index("ix_standards_school_year_id", "standards", ["school_year_id"])


def downgrade() -> None:
    op.drop_index("ix_standards_school_year_id", table_name="standards")
    op.drop_index("ix_standards_subject_id", table_name="standards")
    op.drop_index("ix_standards_tenant_id", table_name="standards")
    op.drop_table("standards")
    op.drop_index("ix_class_subjects_subject_id", table_name="class_subjects")
    op.drop_index("ix_class_subjects_class_id", table_name="class_subjects")
    op.drop_table("class_subjects")
    op.drop_index("ix_classes_school_year_id", table_name="classes")
    op.drop_index("ix_classes_tenant_id", table_name="classes")
    op.drop_table("classes")
    op.drop_index("ix_subjects_tenant_id", table_name="subjects")
    op.drop_table("subjects")
    op.drop_index("ix_grading_periods_school_year_id", table_name="grading_periods")
    op.drop_table("grading_periods")
    op.drop_index("ix_school_years_tenant_id", table_name="school_years")
    op.drop_table("school_years")
    op.drop_index("ix_teacher_profiles_user_id", table_name="teacher_profiles")
    op.drop_table("teacher_profiles")
