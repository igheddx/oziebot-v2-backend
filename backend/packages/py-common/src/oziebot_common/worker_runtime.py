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
DEFAULT_REDIS_FAILURE_THRESHOLD = 3
DEFAULT_REDIS_CIRCUIT_OPEN_SECONDS = 15


def _runtime_details(
    *,
    queue_pop_timeout_seconds: int,
    failure_threshold: int,
    circuit_open_seconds: int,
    receive_failures_total: int,
    circuit_open_total: int,
    receive_recoveries_total: int,
    consecutive_failures: int,
) -> dict[str, object]:
    return {
        "queuePopTimeoutSeconds": queue_pop_timeout_seconds,
        "failureThreshold": failure_threshold,
        "circuitOpenSeconds": circuit_open_seconds,
        "redisReceiveFailuresTotal": receive_failures_total,
        "redisCircuitOpenTotal": circuit_open_total,
        "redisReceiveRecoveriesTotal": receive_recoveries_total,
        "consecutiveFailures": consecutive_failures,
        "autoRecoveredWithoutRestart": receive_recoveries_total > 0,
        "sloStatus": "degraded" if consecutive_failures else "ok",
    }


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
    failure_threshold: int = DEFAULT_REDIS_FAILURE_THRESHOLD,
    circuit_open_seconds: int = DEFAULT_REDIS_CIRCUIT_OPEN_SECONDS,
) -> None:
    consecutive_failures = 0
    receive_failures_total = 0
    circuit_open_total = 0
    receive_recoveries_total = 0
    health.set_detail(
        "workerRuntime",
        _runtime_details(
            queue_pop_timeout_seconds=queue_pop_timeout_seconds,
            failure_threshold=failure_threshold,
            circuit_open_seconds=circuit_open_seconds,
            receive_failures_total=receive_failures_total,
            circuit_open_total=circuit_open_total,
            receive_recoveries_total=receive_recoveries_total,
            consecutive_failures=consecutive_failures,
        ),
    )
    health.mark_ready()
    while not stop_event.is_set():
        try:
            got = brpop_json_any(
                redis_client, queue_keys, timeout=queue_pop_timeout_seconds
            )
        except RedisError as exc:
            if stop_event.is_set():
                break
            consecutive_failures += 1
            receive_failures_total += 1
            health.mark_degraded("redis_receive_failed")
            reset_redis_connection(redis_client)
            sleep_seconds = retry_delay_seconds
            if consecutive_failures >= failure_threshold:
                sleep_seconds = circuit_open_seconds
                circuit_open_total += 1
                logger.warning(
                    "%s redis_circuit_open consecutive_failures=%s sleep_seconds=%s error=%s",
                    worker_name,
                    consecutive_failures,
                    sleep_seconds,
                    exc,
                )
            else:
                logger.warning(
                    "%s redis_receive_failed consecutive_failures=%s error=%s",
                    worker_name,
                    consecutive_failures,
                    exc,
                )
            health.set_detail(
                "workerRuntime",
                _runtime_details(
                    queue_pop_timeout_seconds=queue_pop_timeout_seconds,
                    failure_threshold=failure_threshold,
                    circuit_open_seconds=circuit_open_seconds,
                    receive_failures_total=receive_failures_total,
                    circuit_open_total=circuit_open_total,
                    receive_recoveries_total=receive_recoveries_total,
                    consecutive_failures=consecutive_failures,
                ),
            )
            time.sleep(sleep_seconds)
            continue

        if consecutive_failures:
            receive_recoveries_total += 1
            logger.info(
                "%s redis_receive_recovered consecutive_failures=%s",
                worker_name,
                consecutive_failures,
            )
            consecutive_failures = 0
            health.set_detail(
                "workerRuntime",
                _runtime_details(
                    queue_pop_timeout_seconds=queue_pop_timeout_seconds,
                    failure_threshold=failure_threshold,
                    circuit_open_seconds=circuit_open_seconds,
                    receive_failures_total=receive_failures_total,
                    circuit_open_total=circuit_open_total,
                    receive_recoveries_total=receive_recoveries_total,
                    consecutive_failures=consecutive_failures,
                ),
            )
        health.mark_ready()
        if on_iteration is not None:
            on_iteration()
        if got is None:
            continue
        queue_key, raw = got
        handle_message(queue_key, raw)
        health.touch()
