"""Generate additional assignments for an existing instructional package."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session, selectinload

from oziebot_api.config import Settings
from oziebot_api.models.education_catalog import EducationSubject
from oziebot_api.models.teacher_assist_v2_instructional_package import (
    TeacherAssistV2InstructionalPackage,
    TeacherAssistV2InstructionalPackageArtifact,
)
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.ai_mode import is_teacher_assist_real_ai_active
from oziebot_api.services.teacher_assist_v2.artifact_persistence import persist_package_artifact
from oziebot_api.services.teacher_assist_v2.assignments import maybe_create_assignment_for_artifact
from oziebot_api.services.teacher_assist_v2.assignment_constants import ASSIGNMENT_CREATING_ARTIFACT_TYPES
from oziebot_api.services.teacher_assist_v2.deterministic_package_content import (
    build_deterministic_fallback,
)
from oziebot_api.services.teacher_assist_v2.instructional_package_generation import (
    _objective_fields,
    _persist_linked_assignment_rubric,
    _persist_linked_writing_rubric,
    _resolve_artifact_content,
)
from oziebot_api.services.teacher_assist_v2.pacing_plan_resolver import resolve_subject_daily_topic
from oziebot_api.services.teacher_assist_v2.planning_context import build_teacher_planning_generation_context
from oziebot_api.services.teacher_assist_v2.planning_workflow import _assignment_context

ADDITIONAL_ASSIGNMENT_ARTIFACT_TYPES = ("quiz", "assignment", "writing_response")

ADDITIONAL_ASSIGNMENT_TYPE_LABELS = {
    "quiz": "Quiz",
    "assignment": "Written assignment",
    "writing_response": "Writing response",
}


def _now() -> datetime:
    return datetime.now(UTC)


def _get_owned_package(
    db: Session,
    *,
    user: User,
    package_id: uuid.UUID,
    require_open: bool = False,
) -> TeacherAssistV2InstructionalPackage:
    base = _assignment_context(db, user=user)
    row = db.scalars(
        select(TeacherAssistV2InstructionalPackage)
        .where(
            TeacherAssistV2InstructionalPackage.id == package_id,
            TeacherAssistV2InstructionalPackage.teacher_user_id == user.id,
            TeacherAssistV2InstructionalPackage.tenant_id == base["ctx"].tenant_id,
        )
        .options(selectinload(TeacherAssistV2InstructionalPackage.artifacts))
    ).one_or_none()
    if row is None:
        raise LookupError("Instructional package not found")
    if require_open and row.status == "processing":
        raise ValueError("This package is still generating and cannot be modified yet.")
    if require_open and row.status in {"completed", "archived"}:
        raise ValueError("Closed packages cannot receive additional assignments.")
    return row


def _package_subjects(db: Session, package: TeacherAssistV2InstructionalPackage) -> list[dict[str, str]]:
    subject_ids = [uuid.UUID(str(value)) for value in package.subject_ids_json]
    names = {
        row.id: row.display_name
        for row in db.scalars(select(EducationSubject).where(EducationSubject.id.in_(subject_ids))).all()
    }
    return [
        {"subject_id": str(subject_id), "subject_name": names.get(subject_id, str(subject_id))}
        for subject_id in subject_ids
    ]


def _serialize_existing_assignment(artifact: TeacherAssistV2InstructionalPackageArtifact) -> dict[str, Any]:
    metadata = artifact.metadata_json if isinstance(artifact.metadata_json, dict) else {}
    return {
        "artifact_id": str(artifact.id),
        "assignment_id": str(artifact.assignment_id) if artifact.assignment_id else None,
        "artifact_type": artifact.artifact_type,
        "title": artifact.title,
        "subject_id": str(artifact.subject_id) if artifact.subject_id else None,
        "is_additional": bool(metadata.get("package_additional")),
    }


def build_additional_assignment_form(
    db: Session,
    *,
    user: User,
    package_id: uuid.UUID,
) -> dict[str, Any]:
    package = _get_owned_package(db, user=user, package_id=package_id)
    existing = [
        _serialize_existing_assignment(artifact)
        for artifact in sorted(package.artifacts, key=lambda row: (row.sequence_number, row.title))
        if artifact.artifact_type in ADDITIONAL_ASSIGNMENT_ARTIFACT_TYPES
    ]
    return {
        "package_id": str(package.id),
        "subjects": _package_subjects(db, package),
        "assignment_types": [
            {"id": key, "label": ADDITIONAL_ASSIGNMENT_TYPE_LABELS[key]}
            for key in ADDITIONAL_ASSIGNMENT_ARTIFACT_TYPES
        ],
        "existing_assignments": existing,
    }


def _find_week_subject(
    context: dict[str, Any],
    *,
    subject_id: uuid.UUID,
    week_number: int,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    subject_key = str(subject_id)
    subject_meta = next((row for row in context["subjects"] if row["subject_id"] == subject_key), None)
    if subject_meta is None:
        raise ValueError({"subject_id": "Subject is not part of this instructional package."})
    week = next((row for row in context["weeks"] if row["sequence_number"] == week_number), None)
    if week is None:
        raise ValueError({"week_number": "Week not found in package planning context."})
    week_subject = next((row for row in week["subjects"] if row["subject_id"] == subject_key), None)
    return week, week_subject


def _next_sequence_number(db: Session, *, package_id: uuid.UUID) -> int:
    current = db.scalar(
        select(func.max(TeacherAssistV2InstructionalPackageArtifact.sequence_number)).where(
            TeacherAssistV2InstructionalPackageArtifact.package_id == package_id
        )
    )
    return int(current or 0) + 1


def _existing_assignments_for_subject(
    package: TeacherAssistV2InstructionalPackage,
    *,
    subject_id: uuid.UUID,
) -> list[dict[str, Any]]:
    rows = []
    for artifact in package.artifacts:
        if artifact.subject_id != subject_id:
            continue
        if artifact.artifact_type not in ASSIGNMENT_CREATING_ARTIFACT_TYPES:
            continue
        content = artifact.content_json if isinstance(artifact.content_json, dict) else {}
        rows.append(
            {
                "artifact_type": artifact.artifact_type,
                "title": artifact.title,
                "summary": content.get("summary"),
                "prompt": content.get("prompt"),
            }
        )
    return rows


def generate_additional_package_assignment(
    db: Session,
    *,
    settings: Settings,
    user: User,
    package_id: uuid.UUID,
    subject_id: uuid.UUID,
    artifact_type: str,
    teacher_notes: str | None = None,
    title_hint: str | None = None,
) -> TeacherAssistV2InstructionalPackageArtifact:
    normalized_type = artifact_type.strip()
    if normalized_type not in ADDITIONAL_ASSIGNMENT_ARTIFACT_TYPES:
        raise ValueError({"artifact_type": "Unsupported additional assignment type."})

    notes = teacher_notes.strip() if teacher_notes else ""
    if not notes:
        raise ValueError({"teacher_notes": "Add notes to guide how this assignment should differ."})

    package = _get_owned_package(db, user=user, package_id=package_id, require_open=True)

    teaching_order = [uuid.UUID(str(value)) for value in package.teaching_order_json]
    context = build_teacher_planning_generation_context(
        db,
        user=user,
        week_start=package.week_start,
        week_end=package.week_end,
        teaching_order=teaching_order,
        selected_outputs=list(package.selected_outputs_json),
        settings=settings,
    )
    week_number = package.week_start
    week, week_subject = _find_week_subject(context, subject_id=subject_id, week_number=week_number)
    subject_meta = next(row for row in context["subjects"] if row["subject_id"] == str(subject_id))

    existing_for_subject = _existing_assignments_for_subject(package, subject_id=subject_id)
    context["generation_mode"] = "package_additional_assignment"
    context["teacher_generation_notes"] = notes
    context["existing_package_assignments"] = existing_for_subject
    context["require_distinct_from_existing"] = True

    objective_code, objective_text, objectives_list = _objective_fields(
        week_subject, subject_meta["subject_name"]
    )
    deterministic = build_deterministic_fallback(
        normalized_type,
        subject_name=subject_meta["subject_name"],
        week_label=week["title"],
        package_title=package.title,
        objective_code=objective_code,
        objective_text=objective_text,
        daily_topic=resolve_subject_daily_topic(week_subject, day_label="Monday"),
        objectives_list=objectives_list,
    )
    deterministic["title"] = title_hint.strip() if title_hint and title_hint.strip() else f"{deterministic['title']} — Additional"
    if normalized_type == "writing_response":
        deterministic["prompt"] = f"{deterministic.get('prompt', objective_text)} ({notes})"
    elif normalized_type == "assignment":
        deterministic["student_instructions"] = list(deterministic.get("student_instructions") or []) + [notes]
    else:
        deterministic["summary"] = f"{deterministic.get('summary', '')} {notes}".strip()
    deterministic["teacher_generation_notes"] = notes
    deterministic["package_additional"] = True

    provider_name = "openai" if is_teacher_assist_real_ai_active(db, settings) else "deterministic"
    content = _resolve_artifact_content(
        db,
        settings=settings,
        user=user,
        package=package,
        context=context,
        artifact_type=normalized_type,
        deterministic_content=deterministic,
        week=week,
        subject_meta=subject_meta,
        week_subject=week_subject,
        title_hint=deterministic["title"],
    )
    content["teacher_generation_notes"] = notes
    content["package_additional"] = True

    now = _now()
    sequence = _next_sequence_number(db, package_id=package.id)
    artifact = persist_package_artifact(
        db,
        settings=settings,
        package=package,
        artifact_type=normalized_type,
        content=content,
        provider_name=provider_name,
        sequence_number=sequence,
        created_at=now,
        subject_id=subject_id,
        period_id=uuid.UUID(week_subject["period_id"]) if week_subject and week_subject.get("period_id") else None,
    )
    artifact.metadata_json = {
        **(artifact.metadata_json or {}),
        "package_additional": True,
        "teacher_generation_notes": notes,
    }

    objective_ids = [
        uuid.UUID(str(row["education_objective_id"]))
        for row in (week_subject or {}).get("objectives", [])
        if row.get("education_objective_id")
    ]
    pacing_guide_id = (
        uuid.UUID(week_subject["pacing_guide_id"])
        if week_subject and week_subject.get("pacing_guide_id")
        else None
    )
    maybe_create_assignment_for_artifact(
        db,
        user=user,
        package=package,
        artifact=artifact,
        week_number=week_number,
        pacing_guide_id=pacing_guide_id,
        education_objective_ids=objective_ids,
    )

    if normalized_type in {"quiz", "assignment", "writing_response"}:
        from oziebot_api.services.teacher_assist_v2.assessment_student_exports import (
            refresh_assessment_student_exports,
        )

        refresh_assessment_student_exports(
            db,
            settings=settings,
            package=package,
            artifact=artifact,
        )

    if normalized_type == "assignment":
        sequence, rubric_artifact = _persist_linked_assignment_rubric(
            db,
            settings=settings,
            user=user,
            package=package,
            assignment_artifact=artifact,
            assignment_content=dict(content),
            context=context,
            week=week,
            week_subject=week_subject,
            subject_meta=subject_meta,
            provider_name=provider_name,
            sequence=sequence,
            now=now,
        )
        rubric_metadata = dict(rubric_artifact.metadata_json or {})
        rubric_metadata["package_additional"] = True
        rubric_metadata["teacher_generation_notes"] = notes
        rubric_artifact.metadata_json = rubric_metadata
        refresh_assessment_student_exports(
            db,
            settings=settings,
            package=package,
            artifact=artifact,
        )

    if normalized_type == "writing_response":
        sequence, rubric_artifact = _persist_linked_writing_rubric(
            db,
            settings=settings,
            user=user,
            package=package,
            writing_artifact=artifact,
            writing_content=dict(content),
            context=context,
            week=week,
            week_subject=week_subject,
            subject_meta=subject_meta,
            provider_name=provider_name,
            sequence=sequence,
            now=now,
        )
        rubric_metadata = dict(rubric_artifact.metadata_json or {})
        rubric_metadata["package_additional"] = True
        rubric_metadata["teacher_generation_notes"] = notes
        rubric_artifact.metadata_json = rubric_metadata
        refresh_assessment_student_exports(
            db,
            settings=settings,
            package=package,
            artifact=artifact,
        )

    package.updated_at = now
    db.flush()
    return artifact
