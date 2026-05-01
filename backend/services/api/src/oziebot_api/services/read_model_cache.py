from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from typing import Any, TypeVar

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from oziebot_api.config import Settings
from oziebot_common.postgres_runtime_kv import PostgresRuntimeKV

T = TypeVar("T")


class ReadModelCache:
    def __init__(self, settings: Settings, bind: Engine | None = None):
        self._settings = settings
        self._bind = bind
        self._kv: PostgresRuntimeKV | None = None

    def _runtime_kv(self) -> PostgresRuntimeKV | None:
        if self._kv is not None:
            return self._kv
        engine = self._bind
        if engine is None:
            if not self._settings.database_url:
                return None
            engine = create_engine(self._settings.database_url)
        self._kv = PostgresRuntimeKV(engine)
        return self._kv

    def get_or_build(
        self,
        *,
        namespace: str,
        identity: str,
        params: dict[str, Any],
        ttl_seconds: int,
        builder: Callable[[], T],
        force_refresh: bool = False,
    ) -> T:
        key = self._cache_key(namespace=namespace, identity=identity, params=params)
        cached = None if force_refresh else self._read_json(key)
        if cached is not None:
            return cached

        payload = builder()
        self._write_json(key, payload, ttl_seconds=ttl_seconds)
        return payload

    def _cache_key(self, *, namespace: str, identity: str, params: dict[str, Any]) -> str:
        serialized = json.dumps(params, sort_keys=True, separators=(",", ":"), default=str)
        digest = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
        return f"oziebot:read-model:{namespace}:{identity}:{digest}"

    def _read_json(self, key: str) -> Any | None:
        kv = self._runtime_kv()
        if kv is None:
            return None
        try:
            raw = kv.get(key)
            if not raw:
                return None
            return json.loads(raw)
        except (ValueError, TypeError, json.JSONDecodeError):
            return None

    def _write_json(self, key: str, payload: Any, *, ttl_seconds: int) -> None:
        kv = self._runtime_kv()
        if kv is None:
            return
        try:
            kv.setex(
                key,
                ttl_seconds,
                json.dumps(payload, separators=(",", ":"), default=str),
            )
        except (TypeError, ValueError):
            return
