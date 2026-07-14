from __future__ import annotations

import logging
import time

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from oziebot_api.config import get_settings
from oziebot_api.services.teacher_assist.extraction_jobs import (
    process_next_teacher_assist_extraction_job_with_engine,
    recover_stale_extraction_jobs,
)
from oziebot_api.services.teacher_assist.export_generation import (
    process_next_teacher_assist_export_with_engine,
)
from oziebot_api.services.teacher_assist.workflow_service import (
    process_next_teacher_assist_workflow_with_engine,
)
from oziebot_api.services.teacher_assist_v2.instructional_package_generation import (
    claim_and_run_next_queued_v2_package,
    recover_stale_v2_running_packages,
)
from oziebot_common.health import install_shutdown_handlers, start_health_server

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("teacher-assist-worker")


def main() -> None:
    settings = get_settings()
    if not settings.database_url:
        raise RuntimeError("DATABASE_URL is required for teacher-assist-worker")
    if not settings.teacher_assist_worker_enabled:
        log.info("teacher-assist-worker disabled by config")
        return

    engine = create_engine(settings.database_url)
    health = start_health_server("teacher-assist-worker")
    stop_event = install_shutdown_handlers(
        "teacher-assist-worker",
        health_state=health,
    )
    poll_seconds = max(0.1, float(settings.teacher_assist_worker_poll_interval_seconds))
    log.info("teacher-assist-worker started poll_interval=%s", poll_seconds)

    # Recover any packages that were mid-generation when the worker last died
    recovery_session = sessionmaker(
        bind=engine, autoflush=False, autocommit=False, expire_on_commit=False
    )()
    try:
        recover_stale_v2_running_packages(recovery_session)
    finally:
        recovery_session.close()

    while not stop_event.is_set():
        recovery_session = sessionmaker(
            bind=engine, autoflush=False, autocommit=False, expire_on_commit=False
        )()
        try:
            recover_stale_extraction_jobs(recovery_session, settings=settings)
            recovery_session.commit()
        finally:
            recovery_session.close()

        # V2 package generation takes priority over v1 jobs (long-running, user-facing)
        claimed_id = claim_and_run_next_queued_v2_package(engine, settings=settings)

        if claimed_id is None:
            claimed_id = process_next_teacher_assist_extraction_job_with_engine(
                engine,
                settings=settings,
                worker_name="teacher-assist-worker",
            )
        if claimed_id is None:
            claimed_id = process_next_teacher_assist_export_with_engine(
                engine,
                settings=settings,
                worker_name="teacher-assist-worker",
            )
        if claimed_id is None:
            claimed_id = process_next_teacher_assist_workflow_with_engine(
                engine,
                settings=settings,
                worker_name="teacher-assist-worker",
            )
        health.touch()
        if claimed_id is None:
            time.sleep(poll_seconds)

    log.info("teacher-assist-worker shutdown complete")


if __name__ == "__main__":
    main()
