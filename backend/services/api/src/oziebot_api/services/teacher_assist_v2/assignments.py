"""TeacherAssist v2 assignments anchored to instructional packages."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from oziebot_api.config import Settings
from oziebot_api.models.education_catalog import EducationObjective, EducationSubject
from oziebot_api.models.teacher_assist_v2_assignment import TeacherAssistV2Assignment
from oziebot_api.models.teacher_assist_v2_instructional_package import (
    TeacherAssistV2InstructionalPackage,
    TeacherAssistV2InstructionalPackageArtifact,
)
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist_v2.assignment_constants import (
    ARTIFACT_TO_ASSIGNMENT_TYPE,
    ASSIGNMENT_CREATING_ARTIFACT_TYPES,
    ASSIGNMENT_STATUSES,
    ASSIGNMENT_TYPES,
)
from oziebot_api.services.teacher_assist_v2.package_export import artifact_download_url
from oziebot_api.services.teacher_assist_v2.assignment_print_packets import get_assignment_cover_sheets
from oziebot_api.services.teacher_assist_v2.submission_intake import get_assignment_submission_summary
from oziebot_api.services.teacher_assist_v2.gradebook_workspace import build_assignment_gradebook_summary
from oziebot_api.services.teacher_assist_v2.objective_performance import ObjectivePerformanceService
from oziebot_api.services.teacher_assist_v2.grade_reviews import (
    build_assignment_completion_summary,
    list_assignment_grade_reviews,
)
from oziebot_api.services.teacher_assist_v2.grading_rubric import (
    grading_template_from_package_rubric,
    resolve_assignment_rubric_content,
)
from oziebot_api.services.teacher_assist_v2.planning_workflow import _require_planning_ready
from oziebot_api.services.teacher_assist_v2.rubric_score_exports import assignment_rubric_score_report_status
from oziebot_api.services.teacher_assist_v2.submission_workflow import refresh_assignment_completion_status


def _now() -> datetime:
    return datetime.now(UTC)


def _validate_assignment_anchors(
    *,
    platform_school_year_id: uuid.UUID,
    catalog_district_id: uuid.UUID,
    catalog_school_id: uuid.UUID | None,
    catalog_grade_id: uuid.UUID,
    catalog_subject_id: uuid.UUID,
    instructional_package_id: uuid.UUID,
    pacing_guide_id: uuid.UUID,
    week_number: int,
    education_objective_ids: list[uuid.UUID],
) -> None:
    missing: list[str] = []
    if not platform_school_year_id:
        missing.append("school_year")
    if not catalog_district_id:
        missing.append("district")
    if not catalog_grade_id:
        missing.append("grade")
    if not catalog_subject_id:
        missing.append("subject")
    if not instructional_package_id:
        missing.append("instructional_plan")
    if not pacing_guide_id:
        missing.append("pacing_guide")
    if week_number < 1:
        missing.append("week_number")
    if not education_objective_ids:
        missing.append("objectives")
    if missing:
        raise ValueError(f"Assignment is missing required anchors: {', '.join(missing)}")


def create_assignment_for_artifact(
    db: Session,
    *,
    user: User,
    package: TeacherAssistV2InstructionalPackage,
    artifact: TeacherAssistV2InstructionalPackageArtifact,
    assignment_type: str,
    week_number: int,
    pacing_guide_id: uuid.UUID,
    education_objective_ids: list[uuid.UUID],
    title: str,
    description: str | None = None,
    status: str = "GENERATED",
) -> TeacherAssistV2Assignment:
    if assignment_type not in ASSIGNMENT_TYPES:
        raise ValueError(f"Unsupported assignment type '{assignment_type}'")
    if status not in ASSIGNMENT_STATUSES:
        raise ValueError(f"Unsupported assignment status '{status}'")
    if artifact.subject_id is None:
        raise ValueError("Assignment artifact must be linked to a subject.")

    _validate_assignment_anchors(
        platform_school_year_id=package.platform_school_year_id,
        catalog_district_id=package.catalog_district_id,
        catalog_school_id=package.catalog_school_id,
        catalog_grade_id=package.catalog_grade_id,
        catalog_subject_id=artifact.subject_id,
        instructional_package_id=package.id,
        pacing_guide_id=pacing_guide_id,
        week_number=week_number,
        education_objective_ids=education_objective_ids,
    )

    now = _now()
    row = TeacherAssistV2Assignment(
        id=uuid.uuid4(),
        tenant_id=package.tenant_id,
        teacher_user_id=user.id,
        platform_school_year_id=package.platform_school_year_id,
        catalog_district_id=package.catalog_district_id,
        catalog_school_id=package.catalog_school_id,
        catalog_grade_id=package.catalog_grade_id,
        catalog_subject_id=artifact.subject_id,
        instructional_package_id=package.id,
        pacing_guide_id=pacing_guide_id,
        week_number=week_number,
        assignment_type=assignment_type,
        title=title,
        description=description,
        status=status,
        education_objective_ids_json=[str(value) for value in education_objective_ids],
        created_by_user_id=user.id,
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    artifact.assignment_id = row.id
    artifact.updated_at = now
    return row


def maybe_create_assignment_for_artifact(
    db: Session,
    *,
    user: User,
    package: TeacherAssistV2InstructionalPackage,
    artifact: TeacherAssistV2InstructionalPackageArtifact,
    week_number: int,
    pacing_guide_id: uuid.UUID | None,
    education_objective_ids: list[uuid.UUID],
) -> TeacherAssistV2Assignment | None:
    if artifact.artifact_type not in ASSIGNMENT_CREATING_ARTIFACT_TYPES:
        return None
    if pacing_guide_id is None or not education_objective_ids:
        return None
    assignment_type = ARTIFACT_TO_ASSIGNMENT_TYPE[artifact.artifact_type]
    description = None
    if isinstance(artifact.content_json, dict):
        description = artifact.content_json.get("summary")
    return create_assignment_for_artifact(
        db,
        user=user,
        package=package,
        artifact=artifact,
        assignment_type=assignment_type,
        week_number=week_number,
        pacing_guide_id=pacing_guide_id,
        education_objective_ids=education_objective_ids,
        title=artifact.title,
        description=str(description) if description else None,
        status="GENERATED",
    )


def _subject_name(db: Session, subject_id: uuid.UUID) -> str | None:
    row = db.get(EducationSubject, subject_id)
    return row.display_name if row else None


def serialize_assignment_summary(
    row: TeacherAssistV2Assignment,
    *,
    subject_name: str | None = None,
) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "title": row.title,
        "assignment_type": row.assignment_type,
        "week_number": row.week_number,
        "subject_id": str(row.catalog_subject_id),
        "subject_name": subject_name,
        "status": row.status,
        "creation_origin": row.creation_origin,
        "instructional_plan_id": str(row.instructional_package_id),
        "created_at": row.created_at.isoformat(),
    }


def list_teacher_assignments(
    db: Session,
    *,
    user: User,
    status: str | None = None,
    assignment_type: str | None = None,
) -> list[dict[str, Any]]:
    _require_planning_ready(db, user=user)
    stmt = (
        select(TeacherAssistV2Assignment)
        .where(
            TeacherAssistV2Assignment.teacher_user_id == user.id,
            TeacherAssistV2Assignment.status != "ARCHIVED",
        )
        .order_by(TeacherAssistV2Assignment.created_at.desc())
    )
    rows = db.scalars(stmt).all()
    subject_ids = {row.catalog_subject_id for row in rows}
    subjects = {
        row.id: row.display_name
        for row in db.scalars(select(EducationSubject).where(EducationSubject.id.in_(subject_ids))).all()
    } if subject_ids else {}

    summaries: list[dict[str, Any]] = []
    for row in rows:
        if status is not None and row.status != status:
            continue
        if assignment_type is not None and row.assignment_type != assignment_type:
            continue
        summaries.append(
            serialize_assignment_summary(row, subject_name=subjects.get(row.catalog_subject_id))
        )
    return summaries


def list_recent_assignments(
    db: Session,
    *,
    user: User,
    limit: int = 6,
) -> list[dict[str, Any]]:
    return list_teacher_assignments(db, user=user)[:limit]


def get_teacher_assignment_detail(
    db: Session,
    *,
    user: User,
    assignment_id: uuid.UUID,
    settings: Settings | None = None,
) -> dict[str, Any]:
    _require_planning_ready(db, user=user)
    row = db.scalars(
        select(TeacherAssistV2Assignment)
        .where(
            TeacherAssistV2Assignment.id == assignment_id,
            TeacherAssistV2Assignment.teacher_user_id == user.id,
        )
        .options(selectinload(TeacherAssistV2Assignment.artifacts))
    ).one_or_none()
    if row is None:
        raise LookupError("Assignment not found")

    package = db.get(TeacherAssistV2InstructionalPackage, row.instructional_package_id)
    if package is None:
        raise ValueError("Assignment is missing its instructional plan.")

    objective_ids = [uuid.UUID(str(value)) for value in row.education_objective_ids_json]
    objectives = db.scalars(
        select(EducationObjective).where(EducationObjective.id.in_(objective_ids))
    ).all() if objective_ids else []

    artifacts = []
    for artifact in sorted(row.artifacts, key=lambda item: item.sequence_number):
        artifacts.append(
            {
                "id": str(artifact.id),
                "artifact_type": artifact.artifact_type,
                "title": artifact.title,
                "preview_html": artifact.preview_html,
                "download_url": artifact_download_url(artifact, settings=settings) if settings else None,
            }
        )

    submission_summary = get_assignment_submission_summary(db, assignment_id=row.id)
    completion_summary = build_assignment_completion_summary(
        db,
        user=user,
        assignment_id=row.id,
        submission_summary=submission_summary,
    )
    submission_summary["teacher_reviewed_count"] = completion_summary["grades_confirmed_count"]

    google_form = None
    google_connection = None
    if row.assignment_type == "QUIZ" and settings is not None:
        from oziebot_api.services.teacher_assist_v2.google_form_quizzes import (
            build_teacher_google_status,
            get_assignment_google_form,
            serialize_assignment_google_form,
        )

        google_connection = build_teacher_google_status(db, user=user, settings=settings)
        google_form = serialize_assignment_google_form(get_assignment_google_form(db, assignment_id=row.id))

    rubric_content = resolve_assignment_rubric_content(db, assignment=row)
    rubric_template = grading_template_from_package_rubric(rubric_content)
    refresh_assignment_completion_status(db, user=user, assignment=row)
    rubric_report_available, rubric_report_blocker = assignment_rubric_score_report_status(
        db,
        user=user,
        assignment=row,
    )

    return {
        "id": str(row.id),
        "title": row.title,
        "description": row.description,
        "assignment_type": row.assignment_type,
        "status": row.status,
        "creation_origin": row.creation_origin,
        "week_number": row.week_number,
        "school_year_id": str(row.platform_school_year_id),
        "district_id": str(row.catalog_district_id),
        "school_id": str(row.catalog_school_id) if row.catalog_school_id else None,
        "grade_id": str(row.catalog_grade_id),
        "subject_id": str(row.catalog_subject_id),
        "subject_name": _subject_name(db, row.catalog_subject_id),
        "instructional_plan_id": str(row.instructional_package_id),
        "instructional_plan_title": package.title,
        "pacing_guide_id": str(row.pacing_guide_id),
        "objectives": [
            {
                "id": str(objective.id),
                "objective_id": objective.objective_id,
                "description": objective.description,
                "objective_type": objective.objective_type,
                "coverage_type": objective.coverage_type,
            }
            for objective in objectives
        ],
        "artifacts": artifacts,
        "submission_summary": submission_summary,
        "completion_summary": completion_summary,
        "grade_reviews": list_assignment_grade_reviews(db, user=user, assignment_id=row.id),
        "gradebook_summary": build_assignment_gradebook_summary(db, user=user, assignment_id=row.id),
        "objective_performance": ObjectivePerformanceService.summarize_assignment_objectives(
            db, user=user, assignment_id=row.id
        ),
        "created_at": row.created_at.isoformat(),
        "updated_at": row.updated_at.isoformat(),
        "google_connection": google_connection,
        "google_form": google_form,
        "cover_sheet": get_assignment_cover_sheets(db, assignment_id=row.id, settings=settings)
        if settings is not None
        else None,
        "assignment_rubric": rubric_content,
        "rubric_template": rubric_template,
        "rubric_score_report_available": rubric_report_available,
        "rubric_score_report_blocker": rubric_report_blocker,
    }
