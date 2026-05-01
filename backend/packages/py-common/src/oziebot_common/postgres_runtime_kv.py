"""Postgres TTL key-value replacing Redis STRING cache keys."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

from sqlalchemy import text
from sqlalchemy.engine import Engine

log = logging.getLogger("oziebot.runtime-kv")


class PostgresRuntimeKV:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def purge_expired(self, conn=None) -> None:
        stmt = text("DELETE FROM runtime_kv WHERE expires_at < :now")

        def _run(c) -> None:
            c.execute(stmt, {"now": datetime.now(UTC)})

        if conn is not None:
            _run(conn)
            return
        with self._engine.begin() as c:
            _run(c)

    def get(self, key: str) -> str | None:
        now = datetime.now(UTC)
        with self._engine.begin() as conn:
            conn.execute(
                text("DELETE FROM runtime_kv WHERE expires_at < :now"),
                {"now": now},
            )
            row = conn.execute(
                text(
                    """
                    SELECT value_text FROM runtime_kv
                    WHERE cache_key = :k AND expires_at >= :now
                    LIMIT 1
                    """
                ),
                {"k": key, "now": now},
            ).scalar()
            return str(row) if row is not None else None

    def setex(self, key: str, ttl_seconds: int, value: str) -> None:
        ttl = max(1, int(ttl_seconds))
        expires = datetime.now(UTC) + timedelta(seconds=ttl)
        with self._engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO runtime_kv (cache_key, value_text, expires_at)
                    VALUES (:k, :v, :exp)
                    ON CONFLICT (cache_key)
                    DO UPDATE SET value_text = EXCLUDED.value_text,
                      expires_at = EXCLUDED.expires_at
                    """
                ),
                {"k": key, "v": value, "exp": expires},
            )

    def list_prepend_trim(
        self, key: str, element_json: str, *, max_len: int, ttl_seconds: int
    ) -> None:
        import json as _json

        cap = max(0, int(max_len))
        ttl = max(1, int(ttl_seconds))
        expires = datetime.now(UTC) + timedelta(seconds=ttl)
        with self._engine.begin() as conn:
            raw = conn.execute(
                text(
                    "SELECT value_text FROM runtime_kv WHERE cache_key = :k FOR UPDATE"
                ),
                {"k": key},
            ).scalar()
            lst: list[str] = []
            if raw:
                try:
                    data = _json.loads(str(raw))
                    if isinstance(data, list):
                        lst = [str(x) for x in data]
                except _json.JSONDecodeError:
                    lst = []
            lst.insert(0, element_json)
            if cap > 0:
                lst = lst[:cap]
            payload = _json.dumps(lst, separators=(",", ":"))
            conn.execute(
                text(
                    """
                    INSERT INTO runtime_kv (cache_key, value_text, expires_at)
                    VALUES (:k, :v, :exp)
                    ON CONFLICT (cache_key)
                    DO UPDATE SET value_text = EXCLUDED.value_text,
                      expires_at = EXCLUDED.expires_at
                    """
                ),
                {"k": key, "v": payload, "exp": expires},
            )

    def lrange_strings(self, key: str, start: int, end: int) -> list[str]:
        import json as _json

        raw = self.get(key)
        if not raw:
            return []
        try:
            data = _json.loads(raw)
        except _json.JSONDecodeError:
            return []
        if not isinstance(data, list):
            return []
        if end < 0:
            slice_stop = len(data)
        else:
            slice_stop = min(len(data), end + 1)
        out = []
        for i in range(max(0, start), slice_stop):
            if i >= len(data):
                break
            elem = data[i]
            out.append(elem if isinstance(elem, str) else _json.dumps(elem))
        return out
