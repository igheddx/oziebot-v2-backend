from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError


class SymbolUniverseProvider:
    """Loads token universe restricted to platform- and user-enabled tokens."""

    def __init__(self, engine: Engine):
        self._engine = engine

    def list_active_product_ids(self) -> list[str]:
        user_sql = text(
            """
            SELECT DISTINCT p.symbol, p.quote_currency
            FROM platform_token_allowlist p
            JOIN user_token_permissions ut
              ON ut.platform_token_id = p.id
             AND ut.is_enabled = true
            JOIN users u
              ON u.id = ut.user_id
             AND u.is_active = true
            WHERE p.is_enabled = true
            ORDER BY p.symbol, p.quote_currency
            """
        )
        fallback_sql = text(
            """
            SELECT DISTINCT p.symbol, p.quote_currency
            FROM platform_token_allowlist p
            WHERE p.is_enabled = true
            ORDER BY p.symbol, p.quote_currency
            """
        )
        with self._engine.connect() as conn:
            try:
                rows = conn.execute(user_sql).all()
            except OperationalError:
                rows = []
            if not rows:
                rows = conn.execute(fallback_sql).all()
        # symbol column already contains the full product_id (e.g. "BTC-USD")
        return [r.symbol for r in rows]
