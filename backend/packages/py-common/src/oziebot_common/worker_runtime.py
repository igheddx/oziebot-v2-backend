from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from typing import Any

from redis import Redis, RedisError

from oziebot_common.health import HealthState
from oziebot_common.queues import brpop_json_any, redis_from_url, reset_redis_connection

DEFAULT_QUEUE_POP_TIMEOUT_SECONDS = 5
DEFAULT_REDIS_SOCKET_CONNECT_TIMEOUT_SECONDS = 3
DEFAULT_REDIS_RETRY_DELAY_SECONDS = 1


def redis_socket_timeout_seconds(queue_pop_timeout_seconds: int) -> int:
    return queue_pop_timeout_seconds + 5


def redis_client_for_worker(
    redis_url: str,
    *,
    queue_pop_timeout_seconds: int = DEFAULT_QUEUE_POP_TIMEOUT_SECONDS,
    socket_connect_timeout_seconds: int = DEFAULT_REDIS_SOCKET_CONNECT_TIMEOUT_SECONDS,
) -> Redis:
    return redis_from_url(
        redis_url,
        probe=True,
        socket_connect_timeout=socket_connect_timeout_seconds,
        socket_timeout=redis_socket_timeout_seconds(queue_pop_timeout_seconds),
    )


def run_redis_queue_worker(
    *,
    worker_name: str,
    redis_client: Redis,
    queue_keys: list[str],
    stop_event: threading.Event,
    health: HealthState,
    handle_message: Callable[[str, dict[str, Any]], None],
    logger: logging.Logger,
    on_iteration: Callable[[], None] | None = None,
    queue_pop_timeout_seconds: int = DEFAULT_QUEUE_POP_TIMEOUT_SECONDS,
    retry_delay_seconds: int = DEFAULT_REDIS_RETRY_DELAY_SECONDS,
) -> None:
    health.mark_ready()
    while not stop_event.is_set():
        try:
            got = brpop_json_any(
                redis_client, queue_keys, timeout=queue_pop_timeout_seconds
            )
        except RedisError as exc:
            if stop_event.is_set():
                break
            health.mark_not_ready()
            reset_redis_connection(redis_client)
            logger.warning("%s redis_receive_failed error=%s", worker_name, exc)
            time.sleep(retry_delay_seconds)
            continue

        health.mark_ready()
        if on_iteration is not None:
            on_iteration()
        if got is None:
            continue
        queue_key, raw = got
        handle_message(queue_key, raw)
        health.touch()
