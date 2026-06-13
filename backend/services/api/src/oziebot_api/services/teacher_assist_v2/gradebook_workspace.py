"""Teacher gradebook and mastery workspace queries."""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.education_catalog import EducationObjective
from oziebot_api.models.teacher_assist_v2_assignment import TeacherAssistV2Assignment
from oziebot_api.models.teacher_assist_v2_assignment_grade import TeacherAssistV2AssignmentGrade
from oziebot_api.models.teacher_assist_v2_gradebook_record import TeacherAssistV2GradebookRecord
from oziebot_api.models.teacher_assist_v2_mastery_evidence import TeacherAssistV2MasteryEvidence
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist_v2.gradebook_sync import (
    serialize_gradebook_record,
    serialize_mastery_evidence,
)
from oziebot_api.services.teacher_assist_v2.grade_review_constants import OFFICIAL_ASSIGNMENT_GRADE_STATUSES


def list_gradebook_records(
    db: Session,
    *,
    user: User,
    school_year_id: uuid.UUID | None = None,
    subject_id: uuid.UUID | None = None,
    assignment_id: uuid.UUID | None = None,
    objective_id: uuid.UUID | None = None,
) -> list[dict[str, Any]]:
    stmt = select(TeacherAssistV2GradebookRecord).where(
        TeacherAssistV2GradebookRecord.teacher_user_id == user.id
    )
    if school_year_id is not None:
        stmt = stmt.where(TeacherAssistV2GradebookRecord.platform_school_year_id == school_year_id)
    if subject_id is not None:
        stmt = stmt.where(TeacherAssistV2GradebookRecord.catalog_subject_id == subject_id)
    if assignment_id is not None:
        stmt = stmt.where(TeacherAssistV2GradebookRecord.assignment_id == assignment_id)
    rows = db.scalars(stmt.order_by(TeacherAssistV2GradebookRecord.confirmed_at.desc())).all()
    if objective_id is not None:
        objective_key = str(objective_id)
        rows = [row for row in rows if objective_key in (row.education_objective_ids_json or [])]
    assignment_ids = {row.assignment_id for row in rows}
    assignments = {
        row.id: row
        for row in db.scalars(
            select(TeacherAssistV2Assignment).where(TeacherAssistV2Assignment.id.in_(assignment_ids))
        ).all()
    } if assignment_ids else {}

    return [
        serialize_gradebook_record(
            row,
            assignment_title=assignments.get(row.assignment_id).title if assignments.get(row.assignment_id) else None,
        )
        for row in rows
    ]


def list_mastery_evidence(
    db: Session,
    *,
    user: User,
    objective_id: uuid.UUID | None = None,
    student_number: int | None = None,
    assignment_id: uuid.UUID | None = None,
    current_only: bool = True,
) -> list[dict[str, Any]]:
    stmt = select(TeacherAssistV2MasteryEvidence).where(
        TeacherAssistV2MasteryEvidence.teacher_user_id == user.id
    )
    if current_only:
        stmt = stmt.where(TeacherAssistV2MasteryEvidence.is_current.is_(True))
    if objective_id is not None:
        stmt = stmt.where(TeacherAssistV2MasteryEvidence.education_objective_id == objective_id)
    if student_number is not None:
        stmt = stmt.where(TeacherAssistV2MasteryEvidence.student_number == student_number)
    if assignment_id is not None:
        stmt = stmt.where(TeacherAssistV2MasteryEvidence.assignment_id == assignment_id)

    rows = db.scalars(stmt.order_by(TeacherAssistV2MasteryEvidence.created_at.desc())).all()
    objective_ids = {row.education_objective_id for row in rows}
    objectives = {
        row.id: row
        for row in db.scalars(
            select(EducationObjective).where(EducationObjective.id.in_(objective_ids))
        ).all()
    } if objective_ids else {}

    return [
        serialize_mastery_evidence(
            row,
            objective_label=objectives.get(row.education_objective_id).objective_id
            if objectives.get(row.education_objective_id)
            else None,
        )
        for row in rows
    ]


def count_recent_confirmed_grades(db: Session, *, user: User, limit: int = 5) -> int:
    rows = db.scalars(
        select(TeacherAssistV2AssignmentGrade).where(
            TeacherAssistV2AssignmentGrade.teacher_user_id == user.id,
            TeacherAssistV2AssignmentGrade.status.in_(OFFICIAL_ASSIGNMENT_GRADE_STATUSES),
        ).order_by(TeacherAssistV2AssignmentGrade.confirmed_at.desc()).limit(limit)
    ).all()
    return len(rows)


def build_assignment_gradebook_summary(
    db: Session,
    *,
    user: User,
    assignment_id: uuid.UUID,
) -> dict[str, Any]:
    records = list_gradebook_records(db, user=user, assignment_id=assignment_id)
    return {
        "confirmed_grades_count": len(records),
        "gradebook_sync_status": "SYNCED" if records else "PENDING",
        "gradebook_records_count": len(records),
    }
