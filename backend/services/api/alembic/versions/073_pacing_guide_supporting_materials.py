"""District pacing guide supporting files, links, and notes."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "073_pacing_guide_supporting_materials"
down_revision = "072_teacher_assist_v2_teacher_onboarding"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "pacing_guide_supporting_materials",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("pacing_guide_id", sa.Uuid(), nullable=False),
        sa.Column("period_id", sa.Uuid(), nullable=True),
        sa.Column("education_objective_id", sa.Uuid(), nullable=True),
        sa.Column("platform_school_year_id", sa.Uuid(), nullable=True),
        sa.Column("catalog_state_id", sa.Uuid(), nullable=False),
        sa.Column("catalog_district_id", sa.Uuid(), nullable=False),
        sa.Column("catalog_school_id", sa.Uuid(), nullable=True),
        sa.Column("catalog_grade_id", sa.Uuid(), nullable=False),
        sa.Column("catalog_subject_id", sa.Uuid(), nullable=False),
        sa.Column("material_kind", sa.String(length=16), nullable=False),
        sa.Column("resource_type", sa.String(length=64), nullable=False),
        sa.Column("title", sa.String(length=256), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("note_body", sa.Text(), nullable=True),
        sa.Column("external_url", sa.String(length=2048), nullable=True),
        sa.Column("storage_key", sa.String(length=512), nullable=True),
        sa.Column("original_filename", sa.String(length=512), nullable=True),
        sa.Column("mime_type", sa.String(length=128), nullable=True),
        sa.Column("file_size", sa.Integer(), nullable=True),
        sa.Column("visibility_scope", sa.String(length=32), nullable=False, server_default="district"),
        sa.Column("uploaded_by_user_id", sa.Uuid(), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["catalog_district_id"], ["education_districts.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["catalog_grade_id"], ["education_grades.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["catalog_school_id"], ["education_schools.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["catalog_state_id"], ["education_states.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["catalog_subject_id"], ["education_subjects.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["education_objective_id"], ["education_objectives.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["pacing_guide_id"], ["pacing_guides.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["period_id"], ["pacing_guide_periods.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["platform_school_year_id"], ["education_school_years.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["uploaded_by_user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_pacing_guide_supporting_materials_guide",
        "pacing_guide_supporting_materials",
        ["pacing_guide_id"],
    )
    op.create_index(
        "ix_pacing_guide_supporting_materials_period",
        "pacing_guide_supporting_materials",
        ["period_id"],
    )
    op.create_index(
        "ix_pacing_guide_supporting_materials_objective",
        "pacing_guide_supporting_materials",
        ["education_objective_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_pacing_guide_supporting_materials_objective", table_name="pacing_guide_supporting_materials")
    op.drop_index("ix_pacing_guide_supporting_materials_period", table_name="pacing_guide_supporting_materials")
    op.drop_index("ix_pacing_guide_supporting_materials_guide", table_name="pacing_guide_supporting_materials")
    op.drop_table("pacing_guide_supporting_materials")
