"""Postgres-backed worker dequeue loop."""

from __future__ import annotations

import logging
import threading
import time
import traceback
from collections.abc import Callable
from typing import Any

from sqlalchemy.exc import OperationalError

from sqlalchemy.engine import Engine

from oziebot_common.health import HealthState
from oziebot_common.worker_outbox import (
    LEASE_SECONDS_DEFAULT,
    claim_next_worker_message,
    finalize_worker_retry,
    finalize_worker_success,
    reclaim_stale_leases,
)


DEFAULT_POLL_IDLE_SECONDS = 0.35


def run_postgres_queue_worker(
    *,
    worker_name: str,
    engine: Engine,
    queue_names: list[str],
    stop_event: threading.Event,
    health: HealthState,
    handle_message: Callable[[str, dict[str, Any]], None],
    logger: logging.Logger,
    on_iteration: Callable[[], None] | None = None,
    poll_idle_seconds: float = DEFAULT_POLL_IDLE_SECONDS,
    lease_seconds: int = LEASE_SECONDS_DEFAULT,
    receive_retry_delay_seconds: float = 0.75,
) -> None:
    health.mark_ready()
    poll_idle_seconds = max(0.05, float(poll_idle_seconds))
    while not stop_event.is_set():
        try:
            reclaim_stale_leases(engine)

            claimed: tuple[str, Any, dict[str, Any]] | None = None
            with engine.begin() as conn:
                got = claim_next_worker_message(
                    conn,
                    queue_names,
                    lease_seconds=lease_seconds,
                )
                if got is None:
                    pass
                else:
                    claimed = got

            if on_iteration is not None:
                on_iteration()

            if claimed is None:
                health.touch()
                time.sleep(poll_idle_seconds)
                continue

            queue_key, mid, payload = claimed
            try:
                handle_message(queue_key, payload)
                with engine.begin() as conn:
                    finalize_worker_success(conn, mid)
            except Exception:
                logger.exception(
                    "%s worker handle failed queue=%s", worker_name, queue_key
                )
                with engine.begin() as conn:
                    finalize_worker_retry(
                        conn, mid, retry_after_seconds=int(receive_retry_delay_seconds)
                    )

            health.touch()

        except OperationalError:
            logger.warning("%s postgres_queue_receive_failed backing_off", worker_name)
            health.mark_degraded("postgres_receive_failed")
            time.sleep(max(1.0, receive_retry_delay_seconds))
            health.mark_ready()

        except Exception:
            traceback.print_exc()
            logger.exception("%s postgres worker loop crashed", worker_name)
            time.sleep(max(1.0, receive_retry_delay_seconds))
