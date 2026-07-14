"""Instructional reflections — reusable week-level teacher knowledge."""

from __future__ import annotations

from datetime import UTC, datetime
import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.teacher_assist_instructional_reflection import (
    TeacherAssistInstructionalReflection,
)
from oziebot_api.services.teacher_assist.constants import validate_instructional_reflection_status


def _now() -> datetime:
    return datetime.now(UTC)


def serialize_instructional_reflection(row: TeacherAssistInstructionalReflection) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "instructional_week_id": str(row.instructional_week_id)
        if row.instructional_week_id
        else None,
        "class_id": str(row.class_id) if row.class_id else None,
        "subject_id": str(row.subject_id) if row.subject_id else None,
        "what_worked": row.what_worked,
        "what_didnt_work": row.what_didnt_work,
        "student_challenges": row.student_challenges,
        "adjustments_needed": row.adjustments_needed,
        "future_recommendations": row.future_recommendations,
        "status": row.status,
        "created_at": row.created_at.isoformat(),
        "updated_at": row.updated_at.isoformat(),
    }


def upsert_instructional_reflection(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    instructional_week_id: uuid.UUID | None = None,
    class_id: uuid.UUID | None = None,
    subject_id: uuid.UUID | None = None,
    what_worked: str | None = None,
    what_didnt_work: str | None = None,
    student_challenges: str | None = None,
    adjustments_needed: str | None = None,
    future_recommendations: str | None = None,
    status: str | None = None,
    reflection_id: uuid.UUID | None = None,
) -> TeacherAssistInstructionalReflection:
    now = _now()
    row = None
    if reflection_id is not None:
        row = db.scalars(
            select(TeacherAssistInstructionalReflection).where(
                TeacherAssistInstructionalReflection.id == reflection_id,
                TeacherAssistInstructionalReflection.tenant_id == tenant_id,
                TeacherAssistInstructionalReflection.owner_user_id == user_id,
            )
        ).one_or_none()
    elif instructional_week_id is not None:
        row = db.scalars(
            select(TeacherAssistInstructionalReflection).where(
                TeacherAssistInstructionalReflection.tenant_id == tenant_id,
                TeacherAssistInstructionalReflection.owner_user_id == user_id,
                TeacherAssistInstructionalReflection.instructional_week_id == instructional_week_id,
            )
        ).one_or_none()

    if row is None:
        row = TeacherAssistInstructionalReflection(
            tenant_id=tenant_id,
            owner_user_id=user_id,
            instructional_week_id=instructional_week_id,
            class_id=class_id,
            subject_id=subject_id,
            what_worked=what_worked,
            what_didnt_work=what_didnt_work,
            student_challenges=student_challenges,
            adjustments_needed=adjustments_needed,
            future_recommendations=future_recommendations,
            status=validate_instructional_reflection_status(status or "draft"),
            created_at=now,
            updated_at=now,
        )
        db.add(row)
    else:
        if what_worked is not None:
            row.what_worked = what_worked
        if what_didnt_work is not None:
            row.what_didnt_work = what_didnt_work
        if student_challenges is not None:
            row.student_challenges = student_challenges
        if adjustments_needed is not None:
            row.adjustments_needed = adjustments_needed
        if future_recommendations is not None:
            row.future_recommendations = future_recommendations
        if status is not None:
            row.status = validate_instructional_reflection_status(status)
        row.updated_at = now
    db.flush()
    return row


def list_instructional_reflections(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    instructional_week_id: uuid.UUID | None = None,
) -> list[TeacherAssistInstructionalReflection]:
    query = select(TeacherAssistInstructionalReflection).where(
        TeacherAssistInstructionalReflection.tenant_id == tenant_id,
        TeacherAssistInstructionalReflection.owner_user_id == user_id,
    )
    if instructional_week_id is not None:
        query = query.where(
            TeacherAssistInstructionalReflection.instructional_week_id == instructional_week_id
        )
    return list(
        db.scalars(query.order_by(TeacherAssistInstructionalReflection.updated_at.desc())).all()
    )
