from __future__ import annotations

import logging
import threading

from redis import RedisError

from oziebot_common.health import HealthState
from oziebot_common.worker_runtime import (
    redis_socket_timeout_seconds,
    run_redis_queue_worker,
)
import oziebot_common.worker_runtime as worker_runtime


class _DummyRedis:
    pass


def test_redis_socket_timeout_exceeds_pop_timeout() -> None:
    assert redis_socket_timeout_seconds(5) == 10


def test_run_redis_queue_worker_recovers_from_redis_errors(monkeypatch) -> None:
    events: list[object] = [RedisError("boom"), None, ("queue:key", {"payload": 1})]
    reset_calls: list[str] = []
    sleep_calls: list[int] = []
    handled: list[tuple[str, dict[str, int]]] = []
    iterations: list[str] = []
    health = HealthState("worker-runtime-test")
    stop_event = threading.Event()

    def fake_brpop_json_any(_redis, _keys, timeout: int):
        assert timeout == 5
        item = events.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    def fake_reset(_redis) -> None:
        reset_calls.append("reset")

    def fake_sleep(seconds: int) -> None:
        sleep_calls.append(seconds)

    def on_iteration() -> None:
        iterations.append("tick")

    def handle_message(queue_key: str, raw: dict[str, int]) -> None:
        handled.append((queue_key, raw))
        stop_event.set()

    monkeypatch.setattr(worker_runtime, "brpop_json_any", fake_brpop_json_any)
    monkeypatch.setattr(worker_runtime, "reset_redis_connection", fake_reset)
    monkeypatch.setattr(worker_runtime.time, "sleep", fake_sleep)

    run_redis_queue_worker(
        worker_name="worker-runtime-test",
        redis_client=_DummyRedis(),
        queue_keys=["queue:key"],
        stop_event=stop_event,
        health=health,
        handle_message=handle_message,
        logger=logging.getLogger("worker-runtime-test"),
        on_iteration=on_iteration,
    )

    assert reset_calls == ["reset"]
    assert sleep_calls == [1]
    assert handled == [("queue:key", {"payload": 1})]
    assert iterations == ["tick", "tick"]
    assert health.snapshot()["ready"] is True


def test_run_redis_queue_worker_opens_circuit_after_threshold(monkeypatch) -> None:
    events: list[object] = [RedisError("boom-1"), RedisError("boom-2"), None]
    reset_calls: list[str] = []
    sleep_calls: list[int] = []
    statuses_during_sleep: list[str] = []
    stop_event = threading.Event()
    health = HealthState("worker-runtime-circuit")

    def fake_brpop_json_any(_redis, _keys, timeout: int):
        item = events.pop(0)
        if item is None:
            stop_event.set()
            return None
        raise item

    def fake_reset(_redis) -> None:
        reset_calls.append("reset")

    def fake_sleep(seconds: int) -> None:
        sleep_calls.append(seconds)
        statuses_during_sleep.append(str(health.snapshot()["status"]))

    monkeypatch.setattr(worker_runtime, "brpop_json_any", fake_brpop_json_any)
    monkeypatch.setattr(worker_runtime, "reset_redis_connection", fake_reset)
    monkeypatch.setattr(worker_runtime.time, "sleep", fake_sleep)

    run_redis_queue_worker(
        worker_name="worker-runtime-circuit",
        redis_client=_DummyRedis(),
        queue_keys=["queue:key"],
        stop_event=stop_event,
        health=health,
        handle_message=lambda _queue_key, _raw: None,
        logger=logging.getLogger("worker-runtime-circuit"),
        failure_threshold=2,
        circuit_open_seconds=9,
    )

    assert reset_calls == ["reset", "reset"]
    assert sleep_calls == [1, 9]
    assert statuses_during_sleep == ["degraded", "degraded"]
    snapshot = health.snapshot()
    assert snapshot["status"] == "ok"
    assert snapshot["degraded_reason"] is None
