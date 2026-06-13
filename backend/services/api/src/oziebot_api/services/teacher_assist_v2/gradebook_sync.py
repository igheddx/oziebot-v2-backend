"""Sync teacher-confirmed assignment grades into gradebook and mastery evidence."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.teacher_assist_v2_assignment import TeacherAssistV2Assignment
from oziebot_api.models.teacher_assist_v2_assignment_grade import TeacherAssistV2AssignmentGrade
from oziebot_api.models.teacher_assist_v2_gradebook_record import TeacherAssistV2GradebookRecord
from oziebot_api.models.teacher_assist_v2_gradebook_record_revision import TeacherAssistV2GradebookRecordRevision
from oziebot_api.models.teacher_assist_v2_mastery_evidence import TeacherAssistV2MasteryEvidence
from oziebot_api.models.teacher_assist_v2_student_submission import TeacherAssistV2StudentSubmission
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist_v2.mastery_constants import (
    EVIDENCE_TYPE_CONFIRMED_GRADE,
    format_mastery_level_label,
    resolve_mastery_level,
    serialize_mastery_level_fields,
)


def _now() -> datetime:
    return datetime.now(UTC)


def _require_student_number(submission: TeacherAssistV2StudentSubmission) -> int:
    if submission.student_number is None:
        raise ValueError("Confirmed grades require an assigned student number before gradebook sync.")
    return submission.student_number


def _objective_ids_from_assignment(assignment: TeacherAssistV2Assignment) -> list[uuid.UUID]:
    objective_ids: list[uuid.UUID] = []
    for raw in assignment.education_objective_ids_json or []:
        try:
            objective_ids.append(uuid.UUID(str(raw)))
        except ValueError:
            continue
    return objective_ids


def _supersede_current_mastery(
    db: Session,
    *,
    gradebook_record_id: uuid.UUID,
) -> None:
    rows = db.scalars(
        select(TeacherAssistV2MasteryEvidence).where(
            TeacherAssistV2MasteryEvidence.gradebook_record_id == gradebook_record_id,
            TeacherAssistV2MasteryEvidence.is_current.is_(True),
        )
    ).all()
    for row in rows:
        row.is_current = False
    db.flush()


def _create_mastery_evidence_rows(
    db: Session,
    *,
    gradebook_record: TeacherAssistV2GradebookRecord,
    grade: TeacherAssistV2AssignmentGrade,
    user: User,
    objective_ids: list[uuid.UUID],
) -> list[TeacherAssistV2MasteryEvidence]:
    mastery_level = resolve_mastery_level(grade.percentage)
    now = _now()
    rows: list[TeacherAssistV2MasteryEvidence] = []
    for objective_id in objective_ids:
        row = TeacherAssistV2MasteryEvidence(
            id=uuid.uuid4(),
            tenant_id=gradebook_record.tenant_id,
            teacher_user_id=user.id,
            student_number=gradebook_record.student_number,
            education_objective_id=objective_id,
            assignment_id=gradebook_record.assignment_id,
            gradebook_record_id=gradebook_record.id,
            assignment_grade_id=grade.id,
            evidence_type=EVIDENCE_TYPE_CONFIRMED_GRADE,
            score=grade.score,
            percentage=grade.percentage,
            mastery_level=mastery_level,
            teacher_confirmed=True,
            is_current=True,
            created_at=now,
        )
        db.add(row)
        rows.append(row)
    db.flush()
    return rows


def sync_confirmed_grade_to_gradebook_and_mastery(
    db: Session,
    *,
    user: User,
    grade: TeacherAssistV2AssignmentGrade,
    submission: TeacherAssistV2StudentSubmission,
) -> TeacherAssistV2GradebookRecord:
    assignment = db.get(TeacherAssistV2Assignment, submission.assignment_id)
    if assignment is None:
        raise ValueError("Assignment not found for gradebook sync.")
    if grade.confirmed_at is None:
        raise ValueError("Only confirmed grades can sync to the gradebook.")

    student_number = _require_student_number(submission)
    objective_ids = _objective_ids_from_assignment(assignment)
    if not objective_ids:
        raise ValueError("Assignment must be linked to at least one objective before gradebook sync.")

    now = _now()
    existing = db.scalars(
        select(TeacherAssistV2GradebookRecord).where(
            TeacherAssistV2GradebookRecord.assignment_id == assignment.id,
            TeacherAssistV2GradebookRecord.student_number == student_number,
        )
    ).first()

    if existing is None:
        record = TeacherAssistV2GradebookRecord(
            id=uuid.uuid4(),
            tenant_id=assignment.tenant_id,
            teacher_user_id=user.id,
            platform_school_year_id=assignment.platform_school_year_id,
            catalog_district_id=assignment.catalog_district_id,
            catalog_school_id=assignment.catalog_school_id,
            catalog_grade_id=assignment.catalog_grade_id,
            catalog_subject_id=assignment.catalog_subject_id,
            pacing_guide_id=assignment.pacing_guide_id,
            instructional_package_id=assignment.instructional_package_id,
            assignment_id=assignment.id,
            student_number=student_number,
            education_objective_ids_json=[str(value) for value in objective_ids],
            assignment_grade_id=grade.id,
            score=grade.score,
            max_score=grade.max_score,
            percentage=grade.percentage,
            mastery_level=grade.mastery_level,
            teacher_comment=grade.teacher_comment,
            confirmed_at=grade.confirmed_at,
            sync_status="SYNCED",
            created_at=now,
            updated_at=now,
        )
        db.add(record)
        db.flush()
        _create_mastery_evidence_rows(
            db,
            gradebook_record=record,
            grade=grade,
            user=user,
            objective_ids=objective_ids,
        )
        return record

    db.add(
        TeacherAssistV2GradebookRecordRevision(
            id=uuid.uuid4(),
            tenant_id=existing.tenant_id,
            gradebook_record_id=existing.id,
            assignment_grade_id=grade.id,
            previous_score=existing.score,
            previous_max_score=existing.max_score,
            previous_percentage=existing.percentage,
            previous_teacher_comment=existing.teacher_comment,
            new_score=grade.score,
            new_max_score=grade.max_score,
            new_percentage=grade.percentage,
            new_teacher_comment=grade.teacher_comment,
            revised_by_user_id=user.id,
            revised_at=now,
        )
    )

    existing.assignment_grade_id = grade.id
    existing.score = grade.score
    existing.max_score = grade.max_score
    existing.percentage = grade.percentage
    existing.mastery_level = grade.mastery_level
    existing.teacher_comment = grade.teacher_comment
    existing.confirmed_at = grade.confirmed_at
    existing.education_objective_ids_json = [str(value) for value in objective_ids]
    existing.sync_status = "SYNCED"
    existing.updated_at = now
    db.flush()

    _supersede_current_mastery(db, gradebook_record_id=existing.id)
    _create_mastery_evidence_rows(
        db,
        gradebook_record=existing,
        grade=grade,
        user=user,
        objective_ids=objective_ids,
    )
    return existing


def serialize_gradebook_record(row: TeacherAssistV2GradebookRecord, *, assignment_title: str | None = None) -> dict[str, Any]:
    payload = {
        "id": str(row.id),
        "assignment_id": str(row.assignment_id),
        "assignment_title": assignment_title,
        "instructional_package_id": str(row.instructional_package_id),
        "pacing_guide_id": str(row.pacing_guide_id),
        "school_year_id": str(row.platform_school_year_id),
        "district_id": str(row.catalog_district_id),
        "school_id": str(row.catalog_school_id) if row.catalog_school_id else None,
        "grade_id": str(row.catalog_grade_id),
        "subject_id": str(row.catalog_subject_id),
        "student_number": row.student_number,
        "objective_ids": list(row.education_objective_ids_json or []),
        "assignment_grade_id": str(row.assignment_grade_id),
        "score": row.score,
        "max_score": row.max_score,
        "percentage": row.percentage,
        "teacher_comment": row.teacher_comment,
        "confirmed_at": row.confirmed_at.isoformat(),
        "sync_status": row.sync_status,
        "created_at": row.created_at.isoformat(),
        "updated_at": row.updated_at.isoformat(),
    }
    payload.update(
        serialize_mastery_level_fields(percentage=row.percentage, mastery_level=row.mastery_level)
    )
    return payload


def serialize_mastery_evidence(row: TeacherAssistV2MasteryEvidence, *, objective_label: str | None = None) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "student_number": row.student_number,
        "education_objective_id": str(row.education_objective_id),
        "objective_label": objective_label,
        "assignment_id": str(row.assignment_id),
        "gradebook_record_id": str(row.gradebook_record_id),
        "assignment_grade_id": str(row.assignment_grade_id),
        "evidence_type": row.evidence_type,
        "score": row.score,
        "percentage": row.percentage,
        "mastery_level": row.mastery_level,
        "mastery_level_label": format_mastery_level_label(row.mastery_level),
        "teacher_confirmed": row.teacher_confirmed,
        "is_current": row.is_current,
        "created_at": row.created_at.isoformat(),
    }
