from __future__ import annotations

import json
from collections import defaultdict
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError


class SymbolUniverseProvider:
    """Loads token universe restricted to platform- and user-enabled tokens."""

    def __init__(self, engine: Engine):
        self._engine = engine

    def list_active_product_ids(self) -> list[str]:
        strategy_sql = text(
            """
            SELECT us.user_id, us.config
            FROM user_strategies us
            JOIN users u
              ON u.id = us.user_id
             AND u.is_active = true
            WHERE us.is_enabled = true
            """
        )
        allowed_sql = text(
            """
            SELECT DISTINCT ut.user_id, p.symbol
            FROM user_token_permissions ut
            JOIN platform_token_allowlist p
              ON p.id = ut.platform_token_id
             AND p.is_enabled = true
            JOIN users u
              ON u.id = ut.user_id
             AND u.is_active = true
            WHERE ut.is_enabled = true
            ORDER BY ut.user_id, p.symbol
            """
        )
        open_positions_sql = text(
            """
            SELECT DISTINCT ep.user_id, ep.symbol
            FROM execution_positions ep
            JOIN users u
              ON u.id = ep.user_id
             AND u.is_active = true
            WHERE CAST(ep.quantity AS NUMERIC) > 0
            ORDER BY ep.user_id, ep.symbol
            """
        )
        legacy_user_sql = text(
            """
            SELECT DISTINCT symbol
            FROM (
              SELECT p.symbol AS symbol
              FROM platform_token_allowlist p
              JOIN user_token_permissions ut
                ON ut.platform_token_id = p.id
               AND ut.is_enabled = true
              JOIN users u
                ON u.id = ut.user_id
               AND u.is_active = true
              WHERE p.is_enabled = true
              UNION
              SELECT ep.symbol AS symbol
              FROM execution_positions ep
              JOIN users u
                ON u.id = ep.user_id
               AND u.is_active = true
              WHERE CAST(ep.quantity AS NUMERIC) > 0
            ) symbols
            ORDER BY symbol
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
                strategy_rows = conn.execute(strategy_sql).all()
                allowed_rows = conn.execute(allowed_sql).all()
                position_rows = conn.execute(open_positions_sql).all()
            except OperationalError:
                strategy_rows = []
                allowed_rows = []
                position_rows = []
            symbols = self._resolve_strategy_symbols(
                strategy_rows=strategy_rows,
                allowed_rows=allowed_rows,
                position_rows=position_rows,
            )
            if symbols:
                return symbols
            rows: list[Any] = []
            if not symbols:
                try:
                    rows = conn.execute(legacy_user_sql).all()
                except OperationalError:
                    rows = []
            if not rows:
                rows = conn.execute(fallback_sql).all()
        # symbol column already contains the full product_id (e.g. "BTC-USD")
        return [r.symbol for r in rows]

    @classmethod
    def _resolve_strategy_symbols(
        cls,
        *,
        strategy_rows: list[Any],
        allowed_rows: list[Any],
        position_rows: list[Any],
    ) -> list[str]:
        allowed_by_user: dict[str, list[str]] = defaultdict(list)
        for row in allowed_rows:
            user_id = str(row.user_id)
            symbol = str(row.symbol)
            if symbol not in allowed_by_user[user_id]:
                allowed_by_user[user_id].append(symbol)

        monitored_symbols = {str(row.symbol) for row in position_rows if str(row.symbol)}
        for row in strategy_rows:
            user_id = str(row.user_id)
            allowed_symbols = allowed_by_user.get(user_id, [])
            monitored_symbols.update(
                cls._resolve_configured_symbols(row.config, allowed_symbols)
            )

        return sorted(monitored_symbols)

    @staticmethod
    def _resolve_configured_symbols(
        raw_config: Any, allowed_symbols: list[str]
    ) -> list[str]:
        config = SymbolUniverseProvider._coerce_config(raw_config)
        requested = config.get("symbols")
        if isinstance(requested, (list, tuple, set)):
            requested_symbols = {str(symbol) for symbol in requested if str(symbol)}
            return [symbol for symbol in allowed_symbols if symbol in requested_symbols]

        requested_symbol = config.get("symbol")
        if requested_symbol:
            requested_value = str(requested_symbol)
            return [requested_value] if requested_value in allowed_symbols else []

        return list(allowed_symbols)

    @staticmethod
    def _coerce_config(raw_config: Any) -> dict[str, Any]:
        if isinstance(raw_config, dict):
            return raw_config
        if isinstance(raw_config, str):
            try:
                parsed = json.loads(raw_config)
            except json.JSONDecodeError:
                return {}
            return parsed if isinstance(parsed, dict) else {}
        return {}
