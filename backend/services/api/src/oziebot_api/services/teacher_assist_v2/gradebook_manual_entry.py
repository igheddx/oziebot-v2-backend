"""Manual grade entry assignments and direct gradebook cell updates."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.models.education_catalog import EducationObjective
from oziebot_api.models.teacher_assist_v2_assignment import TeacherAssistV2Assignment
from oziebot_api.models.teacher_assist_v2_submission_batch import TeacherAssistV2SubmissionBatch
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist_v2.assignments import (
    _validate_assignment_anchors,
    get_teacher_assignment_detail,
)
from oziebot_api.services.teacher_assist_v2.grade_reviews import record_manual_assignment_grade
from oziebot_api.services.teacher_assist_v2.manual_assignments import (
    _ensure_manual_shell_package,
    _field_errors,
    _resolve_subject_context,
)
from oziebot_api.services.teacher_assist_v2.planning_workflow import (
    _assignment_context,
    _require_planning_ready,
)
from oziebot_api.services.teacher_assist_v2.submission_workflow import (
    ensure_roster_placeholder_submissions,
    existing_submission_for_student,
    not_uploaded_file_key,
)
from oziebot_api.models.teacher_assist_v2_student_submission import TeacherAssistV2StudentSubmission


def _now() -> datetime:
    return datetime.now(UTC)


def _manual_grade_batch_key(*, assignment_id: uuid.UUID) -> str:
    return f"manual-grade-entry://{assignment_id}"


def _get_or_create_manual_grade_batch(
    db: Session,
    *,
    user: User,
    assignment: TeacherAssistV2Assignment,
) -> TeacherAssistV2SubmissionBatch:
    batch_key = _manual_grade_batch_key(assignment_id=assignment.id)
    existing = db.scalars(
        select(TeacherAssistV2SubmissionBatch).where(
            TeacherAssistV2SubmissionBatch.assignment_id == assignment.id,
            TeacherAssistV2SubmissionBatch.uploaded_file_key == batch_key,
        )
    ).first()
    if existing is not None:
        return existing

    now = _now()
    batch = TeacherAssistV2SubmissionBatch(
        id=uuid.uuid4(),
        tenant_id=assignment.tenant_id,
        teacher_user_id=user.id,
        assignment_id=assignment.id,
        platform_school_year_id=assignment.platform_school_year_id,
        catalog_district_id=assignment.catalog_district_id,
        catalog_school_id=assignment.catalog_school_id,
        catalog_grade_id=assignment.catalog_grade_id,
        catalog_subject_id=assignment.catalog_subject_id,
        status="MATCHED",
        uploaded_file_key=batch_key,
        original_filename="manual-grade-entry",
        mime_type="application/octet-stream",
        file_size=0,
        created_at=now,
    )
    db.add(batch)
    db.flush()
    return batch


def ensure_grade_entry_roster(
    db: Session,
    *,
    user: User,
    assignment: TeacherAssistV2Assignment,
) -> None:
    batch = _get_or_create_manual_grade_batch(db, user=user, assignment=assignment)
    placeholders = ensure_roster_placeholder_submissions(
        db,
        user=user,
        assignment=assignment,
        batch=batch,
        uploaded_student_numbers=set(),
    )
    if placeholders:
        db.add_all(placeholders)
        db.flush()


def create_teacher_grade_entry_assignment(
    db: Session,
    *,
    settings: Settings,
    user: User,
    title: str,
    week_number: int,
    subject_id: uuid.UUID,
    education_objective_ids: list[uuid.UUID],
    description: str | None = None,
) -> dict[str, Any]:
    _require_planning_ready(db, user=user)
    normalized_title = title.strip()
    if not normalized_title:
        raise _field_errors(title="Assignment title is required.")
    if not education_objective_ids:
        raise _field_errors(education_objective_ids="Select at least one TEKS objective.")

    base = _assignment_context(db, user=user)
    onboarding = base["onboarding"]
    _, pacing_guide_id, available_objectives = _resolve_subject_context(
        db,
        user=user,
        subject_id=subject_id,
        week_number=week_number,
    )
    allowed_objective_ids = {row["education_objective_id"] for row in available_objectives}
    normalized_objective_ids = [str(value) for value in education_objective_ids]
    if any(value not in allowed_objective_ids for value in normalized_objective_ids):
        raise _field_errors(education_objective_ids="Selected objectives must belong to the chosen week and subject.")

    objectives = db.scalars(
        select(EducationObjective).where(EducationObjective.id.in_(education_objective_ids))
    ).all()
    if len(objectives) != len(education_objective_ids):
        raise _field_errors(education_objective_ids="One or more objectives could not be found.")

    shell_package = _ensure_manual_shell_package(db, user=user, base=base)
    _validate_assignment_anchors(
        platform_school_year_id=base["platform_year"].id,
        catalog_district_id=onboarding.district_id,
        catalog_school_id=onboarding.school_id,
        catalog_grade_id=onboarding.grade_id,
        catalog_subject_id=subject_id,
        instructional_package_id=shell_package.id,
        pacing_guide_id=pacing_guide_id,
        week_number=week_number,
        education_objective_ids=education_objective_ids,
    )

    now = _now()
    assignment = TeacherAssistV2Assignment(
        id=uuid.uuid4(),
        tenant_id=base["ctx"].tenant_id,
        teacher_user_id=user.id,
        platform_school_year_id=base["platform_year"].id,
        catalog_district_id=onboarding.district_id,
        catalog_school_id=onboarding.school_id,
        catalog_grade_id=onboarding.grade_id,
        catalog_subject_id=subject_id,
        instructional_package_id=shell_package.id,
        pacing_guide_id=pacing_guide_id,
        week_number=week_number,
        assignment_type="GRADE_ENTRY",
        title=normalized_title,
        description=description.strip() if description else None,
        status="ACTIVE",
        creation_origin="TEACHER_GRADE_ENTRY",
        education_objective_ids_json=normalized_objective_ids,
        created_by_user_id=user.id,
        created_at=now,
        updated_at=now,
    )
    db.add(assignment)
    db.flush()
    ensure_grade_entry_roster(db, user=user, assignment=assignment)
    return get_teacher_assignment_detail(db, user=user, assignment_id=assignment.id, settings=settings)


def _ensure_submission_for_manual_grade(
    db: Session,
    *,
    user: User,
    assignment: TeacherAssistV2Assignment,
    student_number: int,
) -> TeacherAssistV2StudentSubmission:
    existing = existing_submission_for_student(
        db,
        assignment_id=assignment.id,
        student_number=student_number,
    )
    if existing is not None:
        return existing

    batch = _get_or_create_manual_grade_batch(db, user=user, assignment=assignment)
    now = _now()
    row = TeacherAssistV2StudentSubmission(
        id=uuid.uuid4(),
        tenant_id=assignment.tenant_id,
        teacher_user_id=user.id,
        assignment_id=assignment.id,
        submission_batch_id=batch.id,
        packet_id=None,
        platform_school_year_id=assignment.platform_school_year_id,
        catalog_district_id=assignment.catalog_district_id,
        catalog_school_id=assignment.catalog_school_id,
        catalog_grade_id=assignment.catalog_grade_id,
        catalog_subject_id=assignment.catalog_subject_id,
        student_number=student_number,
        status="NOT_UPLOADED",
        file_key=not_uploaded_file_key(assignment_id=assignment.id, student_number=student_number),
        original_filename="Manual grade entry",
        mime_type="application/octet-stream",
        file_size=0,
        page_range=None,
        qr_identifier=None,
        match_method="MANUAL",
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    return row


def save_gradebook_grid_cell(
    db: Session,
    *,
    user: User,
    assignment_id: uuid.UUID,
    student_number: int,
    score: float,
    max_score: float = 100.0,
    teacher_comment: str = "",
) -> dict[str, Any]:
    assignment = db.scalars(
        select(TeacherAssistV2Assignment).where(
            TeacherAssistV2Assignment.id == assignment_id,
            TeacherAssistV2Assignment.teacher_user_id == user.id,
        )
    ).first()
    if assignment is None:
        raise ValueError("Assignment not found.")
    if student_number < 1:
        raise ValueError("Student number must be at least 1.")

    submission = _ensure_submission_for_manual_grade(
        db,
        user=user,
        assignment=assignment,
        student_number=student_number,
    )
    grade = record_manual_assignment_grade(
        db,
        user=user,
        submission=submission,
        score=score,
        max_score=max_score,
        teacher_comment=teacher_comment,
    )
    return grade
