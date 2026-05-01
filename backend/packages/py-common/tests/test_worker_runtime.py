"""Postgres-backed worker_runtime has no Redis helpers; sanity-check defaults."""

from __future__ import annotations

from oziebot_common.worker_runtime import DEFAULT_POLL_IDLE_SECONDS


def test_default_poll_interval_is_positive() -> None:
    assert DEFAULT_POLL_IDLE_SECONDS > 0
