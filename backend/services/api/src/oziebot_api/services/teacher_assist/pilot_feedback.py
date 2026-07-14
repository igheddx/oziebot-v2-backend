"""Pilot feedback workspace — capture teacher pilot feedback."""

from __future__ import annotations

from datetime import UTC, datetime
import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.teacher_assist_pilot_feedback import TeacherAssistPilotFeedback
from oziebot_api.services.teacher_assist.constants import (
    validate_pilot_feedback_category,
    validate_pilot_feedback_severity,
    validate_pilot_feedback_status,
)


def _now() -> datetime:
    return datetime.now(UTC)


def serialize_pilot_feedback(row: TeacherAssistPilotFeedback) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "category": row.category,
        "severity": row.severity,
        "feature_area": row.feature_area,
        "description": row.description,
        "requested_improvement": row.requested_improvement,
        "status": row.status,
        "created_at": row.created_at.isoformat(),
        "updated_at": row.updated_at.isoformat(),
    }


def list_pilot_feedback(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID | None = None,
    status: str | None = None,
    limit: int = 50,
) -> list[TeacherAssistPilotFeedback]:
    query = select(TeacherAssistPilotFeedback).where(
        TeacherAssistPilotFeedback.tenant_id == tenant_id
    )
    if user_id is not None:
        query = query.where(TeacherAssistPilotFeedback.user_id == user_id)
    if status:
        query = query.where(
            TeacherAssistPilotFeedback.status == validate_pilot_feedback_status(status)
        )
    return list(
        db.scalars(
            query.order_by(TeacherAssistPilotFeedback.created_at.desc()).limit(
                max(1, min(limit, 200))
            )
        ).all()
    )


def create_pilot_feedback(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    category: str,
    severity: str,
    feature_area: str,
    description: str,
    requested_improvement: str | None = None,
) -> TeacherAssistPilotFeedback:
    now = _now()
    row = TeacherAssistPilotFeedback(
        tenant_id=tenant_id,
        user_id=user_id,
        category=validate_pilot_feedback_category(category),
        severity=validate_pilot_feedback_severity(severity),
        feature_area=feature_area.strip()[:128],
        description=description.strip(),
        requested_improvement=(requested_improvement or "").strip() or None,
        status="open",
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    return row


def update_pilot_feedback_status(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    feedback_id: uuid.UUID,
    status: str,
    allow_any_user: bool = False,
    user_id: uuid.UUID | None = None,
) -> TeacherAssistPilotFeedback:
    row = db.scalars(
        select(TeacherAssistPilotFeedback).where(
            TeacherAssistPilotFeedback.id == feedback_id,
            TeacherAssistPilotFeedback.tenant_id == tenant_id,
        )
    ).one_or_none()
    if row is None:
        raise LookupError("Pilot feedback not found")
    if not allow_any_user and user_id is not None and row.user_id != user_id:
        raise PermissionError("Cannot update another user's feedback")
    row.status = validate_pilot_feedback_status(status)
    row.updated_at = _now()
    db.flush()
    return row
