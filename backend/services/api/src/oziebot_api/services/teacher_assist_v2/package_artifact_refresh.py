"""Idempotent refresh of instructional package artifacts."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.models.teacher_assist_v2_instructional_package import TeacherAssistV2InstructionalPackage
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist_v2.artifact_persistence import (
    attach_qr_student_packet,
    refresh_package_artifact_exports,
)
from oziebot_api.services.teacher_assist_v2.deterministic_package_content import (
    build_daily_lesson_plan,
    build_deterministic_fallback,
)
from oziebot_api.services.teacher_assist_v2.pacing_plan_resolver import (
    build_subject_lesson_block_from_pacing,
    resolve_daily_plan_objective_text,
    resolve_daily_plan_summary,
    resolve_subject_daily_topic,
)
from oziebot_api.services.teacher_assist_v2.package_demo_backfill import backfill_package_demo_content
from oziebot_api.services.teacher_assist_v2.planning_constants import WEEKDAY_LABELS
from oziebot_api.services.teacher_assist_v2.planning_context import build_teacher_planning_generation_context

SLIDE_DECK_EXPORT_NOTE = (
    "PowerPoint export is not available yet. Use Present for classroom display "
    "or Print / Save as PDF for offline use."
)


def _now() -> datetime:
    return datetime.now(UTC)


def _content_looks_mock(content: dict[str, Any]) -> bool:
    blob = json.dumps(content).lower()
    return "[mock output]" in blob or "coming soon" in blob or "placeholder" in blob


def _artifact_needs_refresh(artifact, *, package: TeacherAssistV2InstructionalPackage) -> bool:
    metadata = artifact.metadata_json if isinstance(artifact.metadata_json, dict) else {}
    content = artifact.content_json if isinstance(artifact.content_json, dict) else {}
    if metadata.get("provider") == "mock":
        return True
    if _content_looks_mock(content):
        return True
    if artifact.artifact_type == "quiz":
        exports = metadata.get("additional_exports") or []
        labels = {str(item.get("label", "")).lower() for item in exports if isinstance(item, dict)}
        if not any("student quiz docx" in label for label in labels):
            return True
        if not any("google forms json" in label for label in labels):
            return True
    if artifact.artifact_type in {"assignment", "writing_response"}:
        exports = metadata.get("additional_exports") or []
        labels = {str(item.get("label", "")).lower() for item in exports if isinstance(item, dict)}
        expected = "student assignment docx" if artifact.artifact_type == "assignment" else "writing response docx"
        if not any(expected in label for label in labels):
            return True
    if artifact.artifact_type == "assignment" and not metadata.get("qr_student_packet"):
        return True
    if artifact.artifact_type == "subject_slide_deck":
        note = str(metadata.get("export_note") or "").lower()
        if "coming soon" in note:
            return True
    return False


def _objective_fields(
    week_subject: dict[str, Any] | None, subject_name: str
) -> tuple[str | None, str, list[str]]:
    objective_code = None
    objective_text = f"Students demonstrate understanding in {subject_name}."
    objectives_list: list[str] = []
    if week_subject and week_subject.get("objectives"):
        first = week_subject["objectives"][0]
        objective_code = first.get("objective_code")
        objective_text = first.get("description") or objective_text
        objectives_list = [
            str(row.get("objective_code") or row.get("description"))
            for row in week_subject["objectives"]
            if row.get("objective_code") or row.get("description")
        ]
    return objective_code, objective_text, objectives_list


def _is_ela_demo_package(package: TeacherAssistV2InstructionalPackage) -> bool:
    metadata = package.metadata_json if isinstance(package.metadata_json, dict) else {}
    return metadata.get("content_profile") == "ela_week1_main_idea"


def _rebuild_artifact_content(
    *,
    artifact,
    context: dict[str, Any],
    package: TeacherAssistV2InstructionalPackage,
) -> dict[str, Any]:
    subject_lookup = {row["subject_id"]: row for row in context["subjects"]}
    teaching_order_keys = [str(value) for value in package.teaching_order_json]
    week = context["weeks"][0] if context["weeks"] else None
    if week is None:
        return artifact.content_json if isinstance(artifact.content_json, dict) else {}

    week_label = week["title"]
    week_subjects = {row["subject_id"]: row for row in week["subjects"]}
    subject_id = str(artifact.subject_id) if artifact.subject_id else teaching_order_keys[0]
    subject_meta = subject_lookup.get(subject_id) or {"subject_name": "Instruction", "subject_id": subject_id}
    week_subject = week_subjects.get(subject_id)
    objective_code = None
    objective_text = f"Students demonstrate understanding in {subject_meta['subject_name']}."
    objectives_list: list[str] = []
    if week_subject and week_subject.get("objectives"):
        first = week_subject["objectives"][0]
        objective_code = first.get("objective_code")
        objective_text = first.get("description") or objective_text
        objectives_list = [
            str(row.get("objective_code") or row.get("description"))
            for row in week_subject["objectives"]
            if row.get("objective_code") or row.get("description")
        ]

    if artifact.artifact_type == "daily_lesson_plan":
        day_label = artifact.day_label or "Monday"
        day_index = WEEKDAY_LABELS.index(day_label) if day_label in WEEKDAY_LABELS else 0
        focus_rotation = [
            "Launch the week's learning target and activate prior knowledge.",
            "Build understanding through guided practice and discussion.",
            "Apply the skill with collaborative and independent tasks.",
            "Use evidence from text or problems to justify thinking.",
            "Review, reflect, and prepare for the weekly assessment.",
        ]
        day_focus = focus_rotation[day_index % len(focus_rotation)]
        subject_blocks = []
        for ordered_id in teaching_order_keys:
            ordered_subject = subject_lookup[ordered_id]
            ordered_week_subject = week_subjects.get(ordered_id)
            _, ordered_objective_text, _ = _objective_fields(
                ordered_week_subject, ordered_subject["subject_name"]
            )
            subject_blocks.append(
                build_subject_lesson_block_from_pacing(
                    subject_name=ordered_subject["subject_name"],
                    week_subject=ordered_week_subject,
                    day_label=day_label,
                    fallback_objective_text=ordered_objective_text,
                )
            )
        primary_week_subject = week_subjects.get(subject_id)
        if primary_week_subject:
            _, week_objective_text, _ = _objective_fields(primary_week_subject, subject_meta["subject_name"])
        else:
            week_objective_text = objective_text
        daily_topic = resolve_subject_daily_topic(primary_week_subject, day_label=day_label)
        day_objective_text = resolve_daily_plan_objective_text(
            primary_week_subject,
            day_label,
            fallback=week_objective_text,
        )
        return build_daily_lesson_plan(
            day_label=day_label,
            week_label=week_label,
            package_title=package.title,
            subject_blocks=subject_blocks,
            objective_code=objective_code,
            objective_text=day_objective_text,
            summary=resolve_daily_plan_summary(primary_week_subject, day_label, fallback=day_focus),
            daily_topic=daily_topic,
        )

    return build_deterministic_fallback(
        artifact.artifact_type,
        subject_name=subject_meta["subject_name"],
        week_label=week_label,
        package_title=package.title,
        objective_code=objective_code,
        objective_text=objective_text,
        day_label=artifact.day_label,
        daily_topic=resolve_subject_daily_topic(week_subject, day_label="Monday"),
        objectives_list=objectives_list,
    )


def regenerate_package_artifacts(
    db: Session,
    *,
    settings: Settings,
    package: TeacherAssistV2InstructionalPackage,
) -> dict[str, Any]:
    if _is_ela_demo_package(package):
        return backfill_package_demo_content(db, settings=settings, package=package)

    user = db.get(User, package.teacher_user_id)
    if user is None:
        raise LookupError("Package teacher not found")

    now = _now()
    context = build_teacher_planning_generation_context(
        db,
        user=user,
        week_start=package.week_start,
        week_end=package.week_end,
        teaching_order=[uuid.UUID(value) for value in package.teaching_order_json],
        selected_outputs=list(package.selected_outputs_json),
        settings=settings,
    )
    metadata = dict(package.metadata_json or {})
    metadata["is_mock"] = False
    package.metadata_json = metadata
    package.provider_name = "deterministic"
    package.updated_at = now

    updated = 0
    skipped = 0
    assignment_artifact = None
    assignment_content = None

    for artifact in sorted(package.artifacts, key=lambda item: (item.sequence_number, item.title)):
        if not _artifact_needs_refresh(artifact, package=package):
            skipped += 1
            if artifact.artifact_type == "assignment":
                assignment_artifact = artifact
                assignment_content = artifact.content_json if isinstance(artifact.content_json, dict) else None
            continue

        content = _rebuild_artifact_content(artifact=artifact, context=context, package=package)
        export_note = SLIDE_DECK_EXPORT_NOTE if artifact.artifact_type == "subject_slide_deck" else None
        refresh_package_artifact_exports(
            db,
            settings=settings,
            package=package,
            artifact=artifact,
            content=content,
            provider_name="deterministic",
            updated_at=now,
            export_note=export_note,
        )
        updated += 1
        if artifact.artifact_type == "assignment":
            assignment_artifact = artifact
            assignment_content = content

    qr_packet = None
    if assignment_artifact and assignment_content:
        qr_packet = attach_qr_student_packet(
            db,
            settings=settings,
            assignment_artifact=assignment_artifact,
            assignment_content=assignment_content,
        )

    db.flush()
    return {
        "package_id": str(package.id),
        "title": package.title,
        "artifacts_updated": updated,
        "artifacts_skipped": skipped,
        "qr_student_packet": qr_packet,
    }


def load_package_for_regeneration(db: Session, package_id: uuid.UUID) -> TeacherAssistV2InstructionalPackage:
    package = db.scalars(
        select(TeacherAssistV2InstructionalPackage).where(TeacherAssistV2InstructionalPackage.id == package_id)
    ).one_or_none()
    if package is None:
        raise LookupError("Instructional package not found")
    return package
