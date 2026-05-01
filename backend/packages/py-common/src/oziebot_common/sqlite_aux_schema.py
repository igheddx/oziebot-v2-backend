"""SQLite DDL for KV / trade-log tables used in dev and tests (production uses Postgres migrations)."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine


def ensure_sqlite_aux_schema(engine: Engine) -> None:
    if getattr(engine.dialect, "name", "") != "sqlite":
        return
    stmts = (
        """
        CREATE TABLE IF NOT EXISTS runtime_kv (
            cache_key TEXT PRIMARY KEY NOT NULL,
            value_text TEXT NOT NULL,
            expires_at TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS trade_raw_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TIMESTAMP NOT NULL DEFAULT (datetime('now')),
            symbol TEXT NOT NULL,
            payload TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS trade_signal_samples (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TIMESTAMP NOT NULL DEFAULT (datetime('now')),
            symbol TEXT NOT NULL,
            payload TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS trade_signal_summaries (
            symbol TEXT PRIMARY KEY NOT NULL,
            payload TEXT NOT NULL,
            updated_at TIMESTAMP NOT NULL DEFAULT (datetime('now'))
        )
        """,
    )
    with engine.begin() as conn:
        for ddl in stmts:
            conn.execute(text(ddl))
