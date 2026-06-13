"""Backfill the golden-path ELA Week 1 demo package for test2@teacher.com."""

from __future__ import annotations

import os

import oziebot_api.models  # noqa: F401
from oziebot_api.models.teacher_assist_v2_student_submission import TeacherAssistV2StudentSubmission  # noqa: F401
from oziebot_api.config import get_settings
from oziebot_api.db.session import make_session_factory
from oziebot_api.services.teacher_assist_v2.package_demo_backfill import backfill_demo_package_for_user


def main() -> None:
    email = os.environ.get("DEMO_BACKFILL_EMAIL", "test2@teacher.com")
    title_contains = os.environ.get("DEMO_BACKFILL_TITLE_CONTAINS", "ELA")
    settings = get_settings()
    factory = make_session_factory(settings)
    if factory is None:
        raise RuntimeError("DATABASE_URL is required")
    with factory() as db:
        result = backfill_demo_package_for_user(
            db,
            settings=settings,
            user_email=email,
            title_contains=title_contains,
        )
    print(f"Backfilled package {result['package_id']} ({result['title']}) with {result['artifact_count']} artifacts.")
    if result.get("qr_student_packet"):
        packet = result["qr_student_packet"]
        print(f"QR student packet generated for {packet['student_count']} students (packet_id={packet['packet_id']}).")


if __name__ == "__main__":
    main()
