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
