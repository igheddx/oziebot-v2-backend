"""add education catalog foundation

Revision ID: 059_education_catalog_foundation
Revises: 058_teacher_assist_user_preferences_foundation
Create Date: 2026-05-29 12:30:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

from alembic import op
import sqlalchemy as sa


revision: str = "059_education_catalog_foundation"
down_revision: Union[str, None] = "058_teacher_assist_user_preferences_foundation"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "education_states",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column("abbreviation", sa.String(length=16), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("abbreviation"),
    )
    op.create_index("ix_education_states_abbreviation", "education_states", ["abbreviation"])

    op.create_table(
        "education_districts",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("state_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=256), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["state_id"], ["education_states.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("state_id", "name", name="uq_education_districts_state_name"),
    )
    op.create_index("ix_education_districts_state_id", "education_districts", ["state_id"])

    op.create_table(
        "education_schools",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("district_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=256), nullable=False),
        sa.Column("school_type", sa.String(length=64), nullable=True),
        sa.Column("active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["district_id"], ["education_districts.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("district_id", "name", name="uq_education_schools_district_name"),
    )
    op.create_index("ix_education_schools_district_id", "education_schools", ["district_id"])

    op.create_table(
        "education_grades",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("school_id", sa.Uuid(), nullable=True),
        sa.Column("grade_code", sa.String(length=16), nullable=False),
        sa.Column("display_name", sa.String(length=64), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False, server_default="true"),
        sa.ForeignKeyConstraint(["school_id"], ["education_schools.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("school_id", "grade_code", name="uq_education_grades_school_code"),
    )
    op.create_index("ix_education_grades_school_id", "education_grades", ["school_id"])

    op.create_table(
        "education_subjects",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("grade_id", sa.Uuid(), nullable=True),
        sa.Column("subject_code", sa.String(length=64), nullable=False),
        sa.Column("display_name", sa.String(length=128), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False, server_default="true"),
        sa.ForeignKeyConstraint(["grade_id"], ["education_grades.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("grade_id", "subject_code", name="uq_education_subjects_grade_code"),
    )
    op.create_index("ix_education_subjects_grade_id", "education_subjects", ["grade_id"])

    op.create_table(
        "education_objectives",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("state_id", sa.Uuid(), nullable=False),
        sa.Column("grade_level", sa.String(length=16), nullable=False),
        sa.Column("subject_code", sa.String(length=64), nullable=False),
        sa.Column("objective_type", sa.String(length=32), nullable=False),
        sa.Column("objective_id", sa.String(length=64), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("coverage_type", sa.String(length=32), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["state_id"], ["education_states.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("state_id", "objective_id", name="uq_education_objectives_state_objective_id"),
    )
    op.create_index("ix_education_objectives_state_id", "education_objectives", ["state_id"])

    op.create_table(
        "education_curriculum_resources",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("state_id", sa.Uuid(), nullable=True),
        sa.Column("district_id", sa.Uuid(), nullable=True),
        sa.Column("school_id", sa.Uuid(), nullable=True),
        sa.Column("grade_level", sa.String(length=16), nullable=False),
        sa.Column("subject_code", sa.String(length=64), nullable=False),
        sa.Column("resource_type", sa.String(length=32), nullable=False),
        sa.Column("title", sa.String(length=256), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("storage_key", sa.String(length=512), nullable=True),
        sa.Column("active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["district_id"], ["education_districts.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["school_id"], ["education_schools.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["state_id"], ["education_states.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    for column in ("state_id", "district_id", "school_id"):
        op.create_index(f"ix_education_curriculum_resources_{column}", "education_curriculum_resources", [column])

    op.create_table(
        "education_resource_links",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("curriculum_resource_id", sa.Uuid(), nullable=False),
        sa.Column("link_title", sa.String(length=256), nullable=False),
        sa.Column("url", sa.String(length=2048), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["curriculum_resource_id"], ["education_curriculum_resources.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_education_resource_links_curriculum_resource_id",
        "education_resource_links",
        ["curriculum_resource_id"],
    )

    op.create_table(
        "objective_resource_mapping",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("objective_id", sa.Uuid(), nullable=False),
        sa.Column("resource_id", sa.Uuid(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["objective_id"], ["education_objectives.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["resource_id"], ["education_curriculum_resources.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("objective_id", "resource_id", name="uq_objective_resource_mapping_pair"),
    )
    op.create_index("ix_objective_resource_mapping_objective_id", "objective_resource_mapping", ["objective_id"])
    op.create_index("ix_objective_resource_mapping_resource_id", "objective_resource_mapping", ["resource_id"])

    op.create_table(
        "teacher_school_assignments",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("state_id", sa.Uuid(), nullable=False),
        sa.Column("district_id", sa.Uuid(), nullable=False),
        sa.Column("school_id", sa.Uuid(), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["district_id"], ["education_districts.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["school_id"], ["education_schools.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["state_id"], ["education_states.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("user_id", "school_id", name="uq_teacher_school_assignments_user_school"),
    )
    for column in ("user_id", "state_id", "district_id", "school_id"):
        op.create_index(f"ix_teacher_school_assignments_{column}", "teacher_school_assignments", [column])


def downgrade() -> None:
    op.drop_table("teacher_school_assignments")
    op.drop_table("objective_resource_mapping")
    op.drop_table("education_resource_links")
    op.drop_table("education_curriculum_resources")
    op.drop_table("education_objectives")
    op.drop_table("education_subjects")
    op.drop_table("education_grades")
    op.drop_table("education_schools")
    op.drop_table("education_districts")
    op.drop_table("education_states")
