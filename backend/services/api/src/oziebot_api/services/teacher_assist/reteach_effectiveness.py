"""Reteach effectiveness tracking."""

from __future__ import annotations

from datetime import UTC, datetime
import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.teacher_assist_reteach_effectiveness import (
    TeacherAssistReteachEffectivenessRecord,
)
from oziebot_api.services.teacher_assist.reteach_plans import get_reteach_plan_or_404


def _now() -> datetime:
    return datetime.now(UTC)


def record_reteach_effectiveness(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    reteach_plan_id: uuid.UUID,
    before_mastery_pct: float | None,
    after_mastery_pct: float | None,
    teacher_reflection: str | None = None,
) -> TeacherAssistReteachEffectivenessRecord:
    get_reteach_plan_or_404(
        db, tenant_id=tenant_id, user_id=user_id, reteach_plan_id=reteach_plan_id
    )
    improvement_pct = None
    if before_mastery_pct is not None and after_mastery_pct is not None:
        improvement_pct = round(after_mastery_pct - before_mastery_pct, 1)
    now = _now()
    row = TeacherAssistReteachEffectivenessRecord(
        tenant_id=tenant_id,
        owner_user_id=user_id,
        reteach_plan_id=reteach_plan_id,
        before_mastery_pct=before_mastery_pct,
        after_mastery_pct=after_mastery_pct,
        improvement_pct=improvement_pct,
        teacher_reflection=teacher_reflection,
        recorded_at=now,
        created_at=now,
    )
    db.add(row)
    db.flush()
    return row


def list_reteach_effectiveness(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    reteach_plan_id: uuid.UUID | None = None,
) -> list[TeacherAssistReteachEffectivenessRecord]:
    query = select(TeacherAssistReteachEffectivenessRecord).where(
        TeacherAssistReteachEffectivenessRecord.tenant_id == tenant_id,
        TeacherAssistReteachEffectivenessRecord.owner_user_id == user_id,
    )
    if reteach_plan_id is not None:
        query = query.where(
            TeacherAssistReteachEffectivenessRecord.reteach_plan_id == reteach_plan_id
        )
    return list(
        db.scalars(query.order_by(TeacherAssistReteachEffectivenessRecord.recorded_at.desc())).all()
    )


def serialize_reteach_effectiveness(row: TeacherAssistReteachEffectivenessRecord) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "reteach_plan_id": str(row.reteach_plan_id),
        "before_mastery_pct": row.before_mastery_pct,
        "after_mastery_pct": row.after_mastery_pct,
        "improvement_pct": row.improvement_pct,
        "teacher_reflection": row.teacher_reflection,
        "recorded_at": row.recorded_at.isoformat(),
    }
