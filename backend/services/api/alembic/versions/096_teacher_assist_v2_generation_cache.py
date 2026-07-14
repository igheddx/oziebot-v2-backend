"""TeacherAssist v2 — Layered curriculum/planning generation cache table."""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB
from alembic import op

revision = "096_teacher_assist_v2_generation_cache"
down_revision = "095_teacher_assist_v2_resource_bank"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "teacher_assist_v2_generation_cache",
        sa.Column("id", sa.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "tenant_id",
            sa.UUID(as_uuid=True),
            sa.ForeignKey("tenants.id", ondelete="CASCADE"),
            nullable=False,
            index=True,
        ),
        # "curriculum" or "planning"
        sa.Column("cache_layer", sa.String(32), nullable=False),
        # Stable SHA-256 hash of all inputs that would invalidate this entry
        sa.Column("cache_key", sa.String(64), nullable=False),
        # Prompt version string used to generate this entry
        sa.Column("prompt_version", sa.String(64), nullable=False),
        # AI model used (null for deterministic)
        sa.Column("model", sa.String(128), nullable=True),
        # The cached output (curriculum_sequence_plan + resource_bank, or design_plan + strand_journeys, etc.)
        sa.Column("content_json", JSONB(), nullable=False),
        # Total AI cost to produce this entry (in cents)
        sa.Column("total_cost_cents", sa.Integer(), nullable=True),
        # How many times this entry has been served from cache
        sa.Column("hit_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
        ),
        # Cache entries never expire by TTL; invalidated only by cache_key or prompt_version change.
        # This column is reserved for future use.
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
    )
    # One entry per (tenant, layer, key) — prompt_version is embedded in the cache_key
    op.create_unique_constraint(
        "uq_v2_gen_cache_tenant_layer_key",
        "teacher_assist_v2_generation_cache",
        ["tenant_id", "cache_layer", "cache_key"],
    )
    op.create_index(
        "ix_v2_gen_cache_tenant_layer",
        "teacher_assist_v2_generation_cache",
        ["tenant_id", "cache_layer"],
    )


def downgrade() -> None:
    op.drop_index("ix_v2_gen_cache_tenant_layer", table_name="teacher_assist_v2_generation_cache")
    op.drop_constraint("uq_v2_gen_cache_tenant_layer_key", "teacher_assist_v2_generation_cache")
    op.drop_table("teacher_assist_v2_generation_cache")
