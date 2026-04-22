"""Add lifecycle timestamps to execution positions.

Revision ID: 024_position_lifecycle_ts
Revises: 023_api_performance_indexes
Create Date: 2026-04-22
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from decimal import Decimal

import sqlalchemy as sa
from alembic import op


revision = "024_position_lifecycle_ts"
down_revision = "023_api_performance_indexes"
branch_labels = None
depends_on = None


def _coerce_state(value: object) -> dict[str, object]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


def upgrade() -> None:
    op.add_column(
        "execution_positions",
        sa.Column("opened_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "execution_positions",
        sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
    )

    bind = op.get_bind()
    rows = bind.execute(
        sa.text(
            """
            SELECT
              ep.id,
              ep.user_id,
              ep.strategy_id,
              ep.symbol,
              ep.quantity,
              ep.created_at,
              ep.updated_at,
              ep.last_trade_at,
              uss.state AS runtime_state
            FROM execution_positions ep
            LEFT JOIN user_strategy_states uss
              ON uss.user_id = ep.user_id
             AND uss.strategy_id = ep.strategy_id
             AND uss.trading_mode = ep.trading_mode
            """
        )
    ).mappings()

    updated = 0
    for row in rows:
        runtime_state = _coerce_state(row.get("runtime_state"))
        symbols = runtime_state.get("symbols")
        symbol_state = {}
        if isinstance(symbols, Mapping):
            candidate = symbols.get(str(row["symbol"]))
            if isinstance(candidate, Mapping):
                symbol_state = dict(candidate)

        qty = Decimal(str(row["quantity"] or "0"))
        opened_at = (
            symbol_state.get("opened_at")
            or row["last_trade_at"]
            or row["updated_at"]
            or row["created_at"]
        )
        closed_at = (
            None if qty > 0 else (row["last_trade_at"] or row["updated_at"] or row["created_at"])
        )

        bind.execute(
            sa.text(
                """
                UPDATE execution_positions
                SET opened_at = :opened_at,
                    closed_at = :closed_at
                WHERE id = :id
                """
            ),
            {
                "id": row["id"],
                "opened_at": opened_at,
                "closed_at": closed_at,
            },
        )
        updated += 1

    op.get_context().config.print_stdout(
        f"Backfilled execution_positions lifecycle timestamps for {updated} rows"
    )


def downgrade() -> None:
    op.drop_column("execution_positions", "closed_at")
    op.drop_column("execution_positions", "opened_at")
