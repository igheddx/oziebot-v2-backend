"""token strategy policy

Revision ID: 018_token_strategy_policy
Revises: 017_backtesting_analytics
Create Date: 2026-04-18 00:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "018_token_strategy_policy"
down_revision = "017_backtesting_analytics"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "token_market_profile",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("token_id", sa.Uuid(), nullable=False),
        sa.Column("liquidity_score", sa.Numeric(10, 4), nullable=False),
        sa.Column("spread_score", sa.Numeric(10, 4), nullable=False),
        sa.Column("volatility_score", sa.Numeric(10, 4), nullable=False),
        sa.Column("trend_score", sa.Numeric(10, 4), nullable=False),
        sa.Column("reversion_score", sa.Numeric(10, 4), nullable=False),
        sa.Column("slippage_score", sa.Numeric(10, 4), nullable=False),
        sa.Column("avg_daily_volume_usd", sa.Numeric(28, 10), nullable=False),
        sa.Column("avg_spread_pct", sa.Numeric(18, 10), nullable=False),
        sa.Column("avg_intraday_volatility_pct", sa.Numeric(18, 10), nullable=False),
        sa.Column("last_computed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("raw_metrics_json", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(["token_id"], ["platform_token_allowlist.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("token_id"),
    )
    op.create_index(
        "ix_token_market_profile_token_id",
        "token_market_profile",
        ["token_id"],
        unique=True,
    )

    op.create_table(
        "token_strategy_policy",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("token_id", sa.Uuid(), nullable=False),
        sa.Column("strategy_id", sa.String(length=64), nullable=False),
        sa.Column("admin_enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("suitability_score", sa.Numeric(10, 4), nullable=False),
        sa.Column("recommendation_status", sa.String(length=32), nullable=False),
        sa.Column("recommendation_reason", sa.Text(), nullable=True),
        sa.Column("recommendation_status_override", sa.String(length=32), nullable=True),
        sa.Column("recommendation_reason_override", sa.Text(), nullable=True),
        sa.Column("max_position_pct_override", sa.Numeric(12, 6), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("computed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["token_id"], ["platform_token_allowlist.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("token_id", "strategy_id", name="uq_token_strategy_policy"),
    )
    op.create_index(
        "ix_token_strategy_policy_token_id",
        "token_strategy_policy",
        ["token_id"],
        unique=False,
    )
    op.create_index(
        "ix_token_strategy_policy_strategy_id",
        "token_strategy_policy",
        ["strategy_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_token_strategy_policy_strategy_id", table_name="token_strategy_policy")
    op.drop_index("ix_token_strategy_policy_token_id", table_name="token_strategy_policy")
    op.drop_table("token_strategy_policy")
    op.drop_index("ix_token_market_profile_token_id", table_name="token_market_profile")
    op.drop_table("token_market_profile")
