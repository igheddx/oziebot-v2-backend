from __future__ import annotations

from datetime import UTC, datetime
import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from oziebot_api.models.teacher_assist_time_savings import (
    TeacherAssistPlanningGroup,
    TeacherAssistPlanningGroupMember,
)
from oziebot_api.models.user import User


def _now() -> datetime:
    return datetime.now(UTC)


def create_planning_group(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user: User,
    name: str,
    description: str | None = None,
    subject: str | None = None,
    grade_level: str | None = None,
    visibility: str = "TEAM",
) -> TeacherAssistPlanningGroup:
    now = _now()
    group = TeacherAssistPlanningGroup(
        tenant_id=tenant_id,
        name=name.strip(),
        description=description,
        subject=subject,
        grade_level=grade_level,
        visibility=visibility.upper(),
        created_by_user_id=user.id,
        created_at=now,
        updated_at=now,
    )
    db.add(group)
    db.flush()
    db.add(
        TeacherAssistPlanningGroupMember(
            group_id=group.id,
            user_id=user.id,
            role="owner",
            joined_at=now,
        )
    )
    db.flush()
    return group


def list_planning_groups(
    db: Session, *, tenant_id: uuid.UUID, user_id: uuid.UUID
) -> list[TeacherAssistPlanningGroup]:
    group_ids = [
        row.group_id
        for row in db.scalars(
            select(TeacherAssistPlanningGroupMember).where(
                TeacherAssistPlanningGroupMember.user_id == user_id
            )
        ).all()
    ]
    if not group_ids:
        return []
    return list(
        db.scalars(
            select(TeacherAssistPlanningGroup)
            .where(
                TeacherAssistPlanningGroup.tenant_id == tenant_id,
                TeacherAssistPlanningGroup.id.in_(group_ids),
            )
            .options(selectinload(TeacherAssistPlanningGroup.members))
            .order_by(TeacherAssistPlanningGroup.name)
        ).all()
    )


def join_planning_group(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    group_id: uuid.UUID,
) -> TeacherAssistPlanningGroupMember:
    group = db.scalars(
        select(TeacherAssistPlanningGroup).where(
            TeacherAssistPlanningGroup.id == group_id,
            TeacherAssistPlanningGroup.tenant_id == tenant_id,
        )
    ).one_or_none()
    if group is None:
        raise LookupError("Planning group not found")
    existing = db.scalars(
        select(TeacherAssistPlanningGroupMember).where(
            TeacherAssistPlanningGroupMember.group_id == group_id,
            TeacherAssistPlanningGroupMember.user_id == user_id,
        )
    ).one_or_none()
    if existing is not None:
        return existing
    row = TeacherAssistPlanningGroupMember(
        group_id=group_id,
        user_id=user_id,
        role="member",
        joined_at=_now(),
    )
    db.add(row)
    db.flush()
    return row


def serialize_planning_group(group: TeacherAssistPlanningGroup) -> dict[str, Any]:
    return {
        "id": str(group.id),
        "name": group.name,
        "description": group.description,
        "subject": group.subject,
        "grade_level": group.grade_level,
        "visibility": group.visibility,
        "member_count": len(group.members),
        "members": [{"user_id": str(row.user_id), "role": row.role} for row in group.members],
    }
