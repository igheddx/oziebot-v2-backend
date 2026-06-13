"""Add TeacherAssist v2 slide visual assets."""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "087_teacher_assist_v2_slide_visual_assets"
down_revision = "086_teacher_assist_v2_document_extractions"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "teacher_assist_v2_slide_visual_assets",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("artifact_id", sa.Uuid(), nullable=False),
        sa.Column("slide_id", sa.String(length=128), nullable=False),
        sa.Column("visual_type", sa.String(length=64), nullable=False),
        sa.Column("title", sa.String(length=256), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("source_type", sa.String(length=64), nullable=False),
        sa.Column("source_url", sa.String(length=2048), nullable=True),
        sa.Column("attribution", sa.String(length=512), nullable=True),
        sa.Column("local_asset_key", sa.String(length=512), nullable=True),
        sa.Column("prompt_hint", sa.Text(), nullable=True),
        sa.Column("educational_purpose", sa.Text(), nullable=True),
        sa.Column("suggested_placement", sa.String(length=64), nullable=True),
        sa.Column("layout_template", sa.String(length=64), nullable=True),
        sa.Column(
            "visual_generation_status",
            sa.String(length=32),
            nullable=False,
            server_default="recommendation_only",
        ),
        sa.Column("search_terms_json", sa.JSON(), nullable=False),
        sa.Column("suggested_sources_json", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["artifact_id"],
            ["teacher_assist_v2_instructional_package_artifacts.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_ta_v2_slide_visual_asset_artifact",
        "teacher_assist_v2_slide_visual_assets",
        ["artifact_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_ta_v2_slide_visual_asset_artifact", table_name="teacher_assist_v2_slide_visual_assets")
    op.drop_table("teacher_assist_v2_slide_visual_assets")
