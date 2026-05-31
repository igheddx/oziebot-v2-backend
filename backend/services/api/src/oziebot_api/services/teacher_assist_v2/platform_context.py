from __future__ import annotations

import uuid

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.membership import TenantMembership
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist_v2.roles import resolve_teacher_assist_role


class V2PlatformContextError(PermissionError):
    pass


def resolve_v2_platform_context(db: Session, *, user: User) -> tuple[uuid.UUID, User]:
    role = resolve_teacher_assist_role(db, user=user)
    if role != "root_admin":
        raise V2PlatformContextError("Root admin access required")
    membership = db.scalars(
        select(TenantMembership)
        .where(TenantMembership.user_id == user.id)
        .order_by(TenantMembership.created_at.asc())
    ).first()
    if membership is None:
        raise V2PlatformContextError("Root admin tenant membership not found")
    return membership.tenant_id, user


def resolve_instructional_catalog_tenant_id(db: Session) -> uuid.UUID:
    from oziebot_api.models.teacher_assist_pacing_guide import TeacherAssistPacingGuide

    guide = db.scalars(
        select(TeacherAssistPacingGuide)
        .where(
            TeacherAssistPacingGuide.guide_type == "DISTRICT",
            TeacherAssistPacingGuide.is_active.is_(True),
        )
        .order_by(TeacherAssistPacingGuide.created_at.asc())
        .limit(1)
    ).first()
    if guide is None:
        raise LookupError("District pacing guides are not available yet")
    return guide.tenant_id
