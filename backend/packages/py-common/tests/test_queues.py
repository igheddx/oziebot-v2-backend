from __future__ import annotations

from unittest.mock import patch

import redis

from oziebot_common.queues import (
    disconnect_redis,
    redis_from_url,
    redis_url_candidates,
    reset_redis_connection,
)


class _RedisClient:
    def __init__(self, *, should_fail: bool) -> None:
        self._should_fail = should_fail
        self.closed = False
        self.connection_pool = _ConnectionPool()

    def ping(self) -> bool:
        if self._should_fail:
            raise redis.TimeoutError("timed out")
        return True

    def close(self) -> None:
        self.closed = True


class _ConnectionPool:
    def __init__(self) -> None:
        self.disconnect_calls = 0

    def disconnect(self) -> None:
        self.disconnect_calls += 1


def test_redis_url_candidates_include_tls_variant_for_elasticache() -> None:
    assert redis_url_candidates(
        "redis://master.oziebot-prod-redis.je1lax.use1.cache.amazonaws.com:6379/0"
    ) == [
        "redis://master.oziebot-prod-redis.je1lax.use1.cache.amazonaws.com:6379/0",
        "rediss://master.oziebot-prod-redis.je1lax.use1.cache.amazonaws.com:6379/0",
    ]


@patch("oziebot_common.queues.redis.Redis.from_url")
def test_redis_from_url_falls_back_to_tls_candidate(mock_from_url) -> None:
    def _build_client(url: str, **kwargs):
        return _RedisClient(should_fail=url.startswith("redis://"))

    mock_from_url.side_effect = _build_client

    client = redis_from_url(
        "redis://master.oziebot-prod-redis.je1lax.use1.cache.amazonaws.com:6379/0",
        probe=True,
        socket_connect_timeout=1,
        socket_timeout=1,
    )

    assert isinstance(client, _RedisClient)
    assert mock_from_url.call_args_list[0].args[0].startswith("redis://")
    assert mock_from_url.call_args_list[1].args[0].startswith("rediss://")


def test_reset_redis_connection_disconnects_pool_without_closing_client() -> None:
    client = _RedisClient(should_fail=False)

    reset_redis_connection(client)

    assert client.closed is False
    assert client.connection_pool.disconnect_calls == 1


def test_disconnect_redis_closes_client_and_disconnects_pool() -> None:
    client = _RedisClient(should_fail=False)

    disconnect_redis(client)

    assert client.closed is True
    assert client.connection_pool.disconnect_calls == 1
