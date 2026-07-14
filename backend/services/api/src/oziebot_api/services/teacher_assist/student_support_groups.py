"""Student support groups — teacher-reviewed reteach groupings."""

from __future__ import annotations

from datetime import UTC, datetime
import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from oziebot_api.models.teacher_assist_student_support_group import (
    TeacherAssistStudentSupportGroup,
    TeacherAssistStudentSupportGroupMember,
)
from oziebot_api.services.teacher_assist.constants import validate_student_support_group_status


def _now() -> datetime:
    return datetime.now(UTC)


def serialize_support_group(row: TeacherAssistStudentSupportGroup) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "title": row.title,
        "status": row.status,
        "class_id": str(row.class_id),
        "subject_id": str(row.subject_id),
        "instructional_week_id": str(row.instructional_week_id)
        if row.instructional_week_id
        else None,
        "objective_id": str(row.objective_id) if row.objective_id else None,
        "standard_id": str(row.standard_id) if row.standard_id else None,
        "notes": row.notes,
        "suggested_activities": row.suggested_activities_json or [],
        "members": [
            {"student_identifier": member.student_identifier} for member in (row.members or [])
        ],
        "member_count": len(row.members or []),
        "created_at": row.created_at.isoformat(),
        "updated_at": row.updated_at.isoformat(),
    }


def list_support_groups(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    class_id: uuid.UUID | None = None,
    instructional_week_id: uuid.UUID | None = None,
    status: str | None = None,
) -> list[TeacherAssistStudentSupportGroup]:
    query = select(TeacherAssistStudentSupportGroup).where(
        TeacherAssistStudentSupportGroup.tenant_id == tenant_id,
        TeacherAssistStudentSupportGroup.owner_user_id == user_id,
    )
    if class_id is not None:
        query = query.where(TeacherAssistStudentSupportGroup.class_id == class_id)
    if instructional_week_id is not None:
        query = query.where(
            TeacherAssistStudentSupportGroup.instructional_week_id == instructional_week_id
        )
    if status is not None:
        query = query.where(
            TeacherAssistStudentSupportGroup.status == validate_student_support_group_status(status)
        )
    return list(
        db.scalars(
            query.options(selectinload(TeacherAssistStudentSupportGroup.members)).order_by(
                TeacherAssistStudentSupportGroup.updated_at.desc()
            )
        ).all()
    )


def create_support_group(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    class_id: uuid.UUID,
    subject_id: uuid.UUID,
    title: str,
    student_identifiers: list[str],
    instructional_week_id: uuid.UUID | None = None,
    objective_id: uuid.UUID | None = None,
    standard_id: uuid.UUID | None = None,
    notes: str | None = None,
    suggested_activities: list[dict[str, Any]] | None = None,
    status: str = "draft",
) -> TeacherAssistStudentSupportGroup:
    normalized_title = title.strip()
    if not normalized_title:
        raise ValueError("Support group title is required")
    if not student_identifiers:
        raise ValueError("At least one student is required")
    now = _now()
    row = TeacherAssistStudentSupportGroup(
        tenant_id=tenant_id,
        owner_user_id=user_id,
        class_id=class_id,
        subject_id=subject_id,
        instructional_week_id=instructional_week_id,
        objective_id=objective_id,
        standard_id=standard_id,
        title=normalized_title,
        status=validate_student_support_group_status(status),
        notes=notes,
        suggested_activities_json=suggested_activities or [],
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    for student in student_identifiers:
        db.add(
            TeacherAssistStudentSupportGroupMember(
                tenant_id=tenant_id,
                support_group_id=row.id,
                student_identifier=student.strip(),
                created_at=now,
            )
        )
    db.flush()
    db.refresh(row)
    return row


def update_support_group_status(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    group_id: uuid.UUID,
    status: str,
) -> TeacherAssistStudentSupportGroup:
    row = db.scalars(
        select(TeacherAssistStudentSupportGroup)
        .where(
            TeacherAssistStudentSupportGroup.id == group_id,
            TeacherAssistStudentSupportGroup.tenant_id == tenant_id,
            TeacherAssistStudentSupportGroup.owner_user_id == user_id,
        )
        .options(selectinload(TeacherAssistStudentSupportGroup.members))
    ).one_or_none()
    if row is None:
        raise LookupError("Support group not found")
    row.status = validate_student_support_group_status(status)
    row.updated_at = _now()
    db.flush()
    return row
