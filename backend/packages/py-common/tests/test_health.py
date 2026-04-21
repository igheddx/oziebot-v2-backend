from __future__ import annotations

from datetime import UTC, datetime, timedelta

from oziebot_common.health import HealthState


def test_health_state_marks_ready_and_healthy() -> None:
    state = HealthState(service_name="worker", stale_after_seconds=90)

    state.mark_ready()
    snapshot = state.snapshot()

    assert snapshot["service"] == "worker"
    assert snapshot["status"] == "ok"
    assert snapshot["ready"] is True


def test_health_state_reports_stale_when_heartbeat_is_old() -> None:
    state = HealthState(service_name="worker", stale_after_seconds=5, ready=True)
    state.last_heartbeat_at = datetime.now(UTC) - timedelta(seconds=6)

    snapshot = state.snapshot()

    assert snapshot["status"] == "stale"
    assert snapshot["ready"] is False


def test_health_state_mark_not_ready_clears_readiness() -> None:
    state = HealthState(service_name="worker", stale_after_seconds=90)

    state.mark_ready()
    state.mark_not_ready()
    snapshot = state.snapshot()

    assert snapshot["status"] == "ok"
    assert snapshot["ready"] is False


def test_health_state_mark_degraded_reports_alive_but_not_ready() -> None:
    state = HealthState(service_name="worker", stale_after_seconds=90)

    state.mark_degraded("redis_receive_failed")
    snapshot = state.snapshot()

    assert snapshot["status"] == "degraded"
    assert snapshot["degraded"] is True
    assert snapshot["degraded_reason"] == "redis_receive_failed"
    assert snapshot["ready"] is False


def test_health_state_details_are_exposed_in_snapshot() -> None:
    state = HealthState(service_name="worker", stale_after_seconds=90)

    state.set_detail("workerRuntime", {"redisReceiveFailuresTotal": 2})
    snapshot = state.snapshot()

    assert snapshot["details"]["workerRuntime"]["redisReceiveFailuresTotal"] == 2
