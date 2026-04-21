from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from typing import Any, TypeVar

import redis

from oziebot_api.config import Settings
from oziebot_common import redis_from_url
from oziebot_common.queues import disconnect_redis

T = TypeVar("T")


class ReadModelCache:
    def __init__(self, settings: Settings):
        self._settings = settings

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

    def _client(self) -> redis.Redis | None:
        try:
            return redis_from_url(
                self._settings.redis_url,
                probe=True,
                socket_connect_timeout=0.5,
                socket_timeout=0.5,
            )
        except (redis.RedisError, ValueError):
            return None

    def _read_json(self, key: str) -> Any | None:
        client = self._client()
        if client is None:
            return None
        try:
            payload = client.get(key)
            if not payload:
                return None
            return json.loads(payload)
        except (redis.RedisError, ValueError, TypeError, json.JSONDecodeError):
            return None
        finally:
            disconnect_redis(client)

    def _write_json(self, key: str, payload: Any, *, ttl_seconds: int) -> None:
        client = self._client()
        if client is None:
            return
        try:
            client.setex(key, ttl_seconds, json.dumps(payload, separators=(",", ":"), default=str))
        except (redis.RedisError, TypeError, ValueError):
            return
        finally:
            disconnect_redis(client)
