"""Backfill an existing instructional package with golden-path demo content."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from oziebot_api.config import Settings
from oziebot_api.models.teacher_assist_v2_assignment import TeacherAssistV2Assignment
from oziebot_api.models.teacher_assist_v2_instructional_package import (
    TeacherAssistV2InstructionalPackage,
    TeacherAssistV2InstructionalPackageArtifact,
)
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist_v2.assignment_print_packets import (
    generate_assignment_print_packet,
)
from oziebot_api.services.teacher_assist_v2.demo_content.ela_week1_main_idea import (
    CONTENT_BUILDERS,
    PACKAGE_TITLE,
    build_daily_lesson_plan,
)
from oziebot_api.services.teacher_assist_v2.package_export import (
    render_artifact_preview_html,
    save_artifact_export,
)


def _now() -> datetime:
    return datetime.now(UTC)


def _resolve_content(artifact: TeacherAssistV2InstructionalPackageArtifact) -> dict[str, Any]:
    if artifact.artifact_type == "daily_lesson_plan":
        day = artifact.day_label or "Monday"
        return build_daily_lesson_plan(day)
    builder = CONTENT_BUILDERS.get(artifact.artifact_type)
    if builder is None:
        raise ValueError(f"No demo content builder for artifact type '{artifact.artifact_type}'")
    return builder()


def find_demo_package(
    db: Session,
    *,
    user_email: str,
    title_contains: str | None = None,
) -> TeacherAssistV2InstructionalPackage:
    user = db.scalars(select(User).where(User.email == user_email)).one_or_none()
    if user is None:
        raise LookupError(f"User not found: {user_email}")

    query = (
        select(TeacherAssistV2InstructionalPackage)
        .where(TeacherAssistV2InstructionalPackage.teacher_user_id == user.id)
        .options(selectinload(TeacherAssistV2InstructionalPackage.artifacts))
        .order_by(TeacherAssistV2InstructionalPackage.created_at.desc())
    )
    rows = db.scalars(query).all()
    if not rows:
        raise LookupError(f"No instructional packages found for {user_email}")

    if title_contains:
        lowered = title_contains.lower()
        for row in rows:
            if lowered in row.title.lower():
                return row

    return rows[0]


def backfill_package_demo_content(
    db: Session,
    *,
    settings: Settings,
    package: TeacherAssistV2InstructionalPackage,
) -> dict[str, Any]:
    now = _now()
    package.title = PACKAGE_TITLE
    package.provider_name = "demo"
    metadata = dict(package.metadata_json or {})
    metadata.update(
        {"is_mock": False, "demo_backfill": True, "content_profile": "ela_week1_main_idea"}
    )
    package.metadata_json = metadata
    package.updated_at = now

    assignment_artifact: TeacherAssistV2InstructionalPackageArtifact | None = None
    assignment_content: dict[str, Any] | None = None
    qr_packet: dict[str, Any] | None = None

    for artifact in sorted(package.artifacts, key=lambda item: (item.sequence_number, item.title)):
        content = _resolve_content(artifact)
        artifact.title = str(content["title"])
        artifact.content_json = content
        artifact.preview_html = render_artifact_preview_html(
            artifact_type=artifact.artifact_type, content=content
        )
        artifact.status = "ready"
        artifact.metadata_json = {
            "provider": "demo",
            "description": content.get("description") or content.get("summary"),
            "objective_mapping": content.get("objective_mapping"),
            "additional_exports": [],
        }
        storage_key, export_format = save_artifact_export(
            settings=settings,
            tenant_id=package.tenant_id,
            artifact_id=artifact.id,
            artifact_type=artifact.artifact_type,
            content=content,
        )
        artifact.storage_key = storage_key
        artifact.export_format = export_format
        artifact.updated_at = now

        if (
            artifact.artifact_type in {"quiz", "assignment", "writing_response"}
            and artifact.assignment_id
        ):
            from oziebot_api.services.teacher_assist_v2.assessment_student_exports import (
                refresh_assessment_student_exports,
            )

            refresh_assessment_student_exports(
                db, settings=settings, package=package, artifact=artifact
            )

        if artifact.assignment_id:
            assignment = db.get(TeacherAssistV2Assignment, artifact.assignment_id)
            if assignment is not None:
                assignment.title = artifact.title
                assignment.description = str(
                    content.get("summary") or content.get("description") or ""
                )
                assignment.updated_at = now
                if artifact.artifact_type == "assignment":
                    assignment_artifact = artifact
                    assignment_content = content

    if assignment_artifact and assignment_content and assignment_artifact.assignment_id:
        assignment = db.get(TeacherAssistV2Assignment, assignment_artifact.assignment_id)
        if assignment is not None:
            qr_packet = generate_assignment_print_packet(
                db,
                settings=settings,
                assignment=assignment,
                assignment_content=assignment_content,
            )
            assignment_artifact.metadata_json = {
                **(assignment_artifact.metadata_json or {}),
                "qr_student_packet": qr_packet,
            }

    db.flush()
    return {
        "package_id": str(package.id),
        "title": package.title,
        "artifact_count": len(package.artifacts),
        "qr_student_packet": qr_packet,
    }


def backfill_demo_package_for_user(
    db: Session,
    *,
    settings: Settings,
    user_email: str,
    title_contains: str | None = "ELA",
) -> dict[str, Any]:
    package = find_demo_package(db, user_email=user_email, title_contains=title_contains)
    result = backfill_package_demo_content(db, settings=settings, package=package)
    db.commit()
    return result
