"""Instructional evidence — teacher-confirmed mastery support records."""

from __future__ import annotations

from datetime import UTC, datetime
import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.teacher_assist_instructional_evidence import TeacherAssistInstructionalEvidence
from oziebot_api.services.teacher_assist.constants import (
    validate_instructional_evidence_source_type,
    validate_mastery_level,
)


def _now() -> datetime:
    return datetime.now(UTC)


def serialize_instructional_evidence(row: TeacherAssistInstructionalEvidence) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "student_identifier": row.student_identifier,
        "objective_id": str(row.objective_id) if row.objective_id else None,
        "standard_id": str(row.standard_id) if row.standard_id else None,
        "source_type": row.source_type,
        "source_id": str(row.source_id),
        "score": row.score,
        "mastery_level": row.mastery_level,
        "teacher_confirmed": row.teacher_confirmed,
        "teacher_notes": row.teacher_notes,
        "class_id": str(row.class_id) if row.class_id else None,
        "subject_id": str(row.subject_id) if row.subject_id else None,
        "instructional_week_id": str(row.instructional_week_id) if row.instructional_week_id else None,
        "created_at": row.created_at.isoformat(),
        "updated_at": row.updated_at.isoformat(),
    }


def record_instructional_evidence(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    student_identifier: str,
    source_type: str,
    source_id: uuid.UUID,
    objective_id: uuid.UUID | None = None,
    standard_id: uuid.UUID | None = None,
    class_id: uuid.UUID | None = None,
    subject_id: uuid.UUID | None = None,
    instructional_week_id: uuid.UUID | None = None,
    score: float | None = None,
    mastery_level: str | None = None,
    teacher_confirmed: bool = False,
    teacher_notes: str | None = None,
) -> TeacherAssistInstructionalEvidence:
    now = _now()
    row = TeacherAssistInstructionalEvidence(
        tenant_id=tenant_id,
        owner_user_id=user_id,
        class_id=class_id,
        subject_id=subject_id,
        instructional_week_id=instructional_week_id,
        student_identifier=student_identifier.strip(),
        objective_id=objective_id,
        standard_id=standard_id,
        source_type=validate_instructional_evidence_source_type(source_type),
        source_id=source_id,
        score=score,
        mastery_level=validate_mastery_level(mastery_level) if mastery_level else None,
        teacher_confirmed=teacher_confirmed,
        teacher_notes=teacher_notes,
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    return row


def confirm_instructional_evidence(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    evidence_id: uuid.UUID,
    mastery_level: str | None = None,
    teacher_notes: str | None = None,
) -> TeacherAssistInstructionalEvidence:
    row = db.scalars(
        select(TeacherAssistInstructionalEvidence).where(
            TeacherAssistInstructionalEvidence.id == evidence_id,
            TeacherAssistInstructionalEvidence.tenant_id == tenant_id,
            TeacherAssistInstructionalEvidence.owner_user_id == user_id,
        )
    ).one_or_none()
    if row is None:
        raise LookupError("Instructional evidence not found")
    row.teacher_confirmed = True
    if mastery_level is not None:
        row.mastery_level = validate_mastery_level(mastery_level)
    if teacher_notes is not None:
        row.teacher_notes = teacher_notes
    row.updated_at = _now()
    db.flush()
    return row


def list_instructional_evidence(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    class_id: uuid.UUID | None = None,
    instructional_week_id: uuid.UUID | None = None,
    source_type: str | None = None,
    teacher_confirmed: bool | None = None,
) -> list[TeacherAssistInstructionalEvidence]:
    query = select(TeacherAssistInstructionalEvidence).where(
        TeacherAssistInstructionalEvidence.tenant_id == tenant_id,
        TeacherAssistInstructionalEvidence.owner_user_id == user_id,
    )
    if class_id is not None:
        query = query.where(TeacherAssistInstructionalEvidence.class_id == class_id)
    if instructional_week_id is not None:
        query = query.where(TeacherAssistInstructionalEvidence.instructional_week_id == instructional_week_id)
    if source_type is not None:
        query = query.where(
            TeacherAssistInstructionalEvidence.source_type == validate_instructional_evidence_source_type(source_type)
        )
    if teacher_confirmed is not None:
        query = query.where(TeacherAssistInstructionalEvidence.teacher_confirmed == teacher_confirmed)
    return list(db.scalars(query.order_by(TeacherAssistInstructionalEvidence.created_at.desc())).all())
