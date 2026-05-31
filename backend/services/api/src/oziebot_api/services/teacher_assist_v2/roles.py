from __future__ import annotations

from typing import Literal

from sqlalchemy.orm import Session

from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.setup import teacher_assist_context_for_user

TeacherAssistRole = Literal["root_admin", "admin", "teacher"]
TEACHER_ASSIST_V2_ROOT_ADMIN_EMAILS = frozenset({"dominic@oziebot.com"})


def normalize_teacher_assist_role(value: str | None) -> TeacherAssistRole | None:
    if not value:
        return None
    normalized = value.strip().lower()
    if normalized in {"root_admin", "admin", "teacher"}:
        return normalized  # type: ignore[return-value]
    return None


def resolve_teacher_assist_role(db: Session, *, user: User) -> TeacherAssistRole | None:
    explicit = normalize_teacher_assist_role(getattr(user, "teacher_assist_role", None))
    if explicit == "root_admin" or user.is_root_admin:
        return "root_admin"
    if explicit == "admin":
        return "admin"
    try:
        teacher_assist_context_for_user(db, user)
    except PermissionError:
        return None
    if explicit == "teacher":
        return "teacher"
    return "teacher"


def ensure_v2_root_admin_role(db: Session, *, email: str) -> bool:
    from sqlalchemy import func, select

    from oziebot_api.services.teacher_assist.access_seed import ensure_teacher_assist_root_admin

    normalized = email.strip().lower()
    if normalized not in TEACHER_ASSIST_V2_ROOT_ADMIN_EMAILS:
        return False
    if not ensure_teacher_assist_root_admin(db, email=email):
        return False
    row = db.scalars(select(User).where(func.lower(User.email) == normalized)).one_or_none()
    if row is None:
        return False
    row.teacher_assist_role = "root_admin"
    row.is_root_admin = True
    db.flush()
    return True
