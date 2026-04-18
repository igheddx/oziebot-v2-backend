"""JWT access tokens and opaque refresh tokens."""

from __future__ import annotations

import hashlib
import secrets
from datetime import UTC, datetime, timedelta
from uuid import UUID

from jose import JWTError, jwt
from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.models.auth_session import AuthSession
from oziebot_api.models.user import User


def _aware_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt


def hash_refresh_token(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def new_refresh_token() -> str:
    return secrets.token_urlsafe(32)


def create_access_token(
    settings: Settings,
    *,
    user: User,
    session_id: UUID,
    tenant_id: UUID | None,
) -> str:
    expire = datetime.now(UTC) + timedelta(minutes=settings.jwt_access_exp_minutes)
    payload: dict = {
        "sub": str(user.id),
        "sid": str(session_id),
        "is_root": user.is_root_admin,
        "typ": "access",
        "exp": expire,
    }
    if tenant_id is not None:
        payload["tenant_id"] = str(tenant_id)
    return jwt.encode(payload, settings.jwt_secret, algorithm="HS256")


def decode_access_token(settings: Settings, token: str) -> dict:
    return jwt.decode(
        token,
        settings.jwt_secret,
        algorithms=["HS256"],
        options={"verify_aud": False},
    )


def validate_access_payload(payload: dict) -> tuple[UUID, UUID, bool, UUID | None]:
    """Returns user_id, session_id, is_root, tenant_id."""
    try:
        uid = UUID(payload["sub"])
        sid = UUID(payload["sid"])
        is_root = bool(payload.get("is_root", False))
        tid_raw = payload.get("tenant_id")
        tid = UUID(tid_raw) if tid_raw else None
    except (KeyError, ValueError, TypeError) as e:
        raise JWTError(str(e)) from e
    return uid, sid, is_root, tid


def create_refresh_session(
    db: Session,
    settings: Settings,
    user: User,
) -> tuple[AuthSession, str]:
    raw = new_refresh_token()
    now = datetime.now(UTC)
    exp = now + timedelta(days=settings.jwt_refresh_exp_days)
    row = AuthSession(
        user_id=user.id,
        refresh_token_hash=hash_refresh_token(raw),
        expires_at=exp,
        created_at=now,
        revoked_at=None,
    )
    db.add(row)
    db.flush()
    return row, raw


def revoke_session_by_refresh(db: Session, raw_refresh: str) -> bool:
    h = hash_refresh_token(raw_refresh)
    row = db.scalars(select(AuthSession).where(AuthSession.refresh_token_hash == h)).one_or_none()
    if row is None or row.revoked_at is not None:
        return False
    if _aware_utc(row.expires_at) < datetime.now(UTC):
        return False
    row.revoked_at = datetime.now(UTC)
    return True


def get_valid_session(db: Session, session_id: UUID, user_id: UUID) -> AuthSession | None:
    return db.scalars(
        select(AuthSession).where(
            AuthSession.id == session_id,
            AuthSession.user_id == user_id,
            AuthSession.revoked_at.is_(None),
            AuthSession.expires_at > datetime.now(UTC),
        )
    ).one_or_none()


def get_session_by_refresh_hash(db: Session, raw_refresh: str) -> AuthSession | None:
    h = hash_refresh_token(raw_refresh)
    row = db.scalars(select(AuthSession).where(AuthSession.refresh_token_hash == h)).one_or_none()
    if row is None or row.revoked_at is not None:
        return None
    if _aware_utc(row.expires_at) < datetime.now(UTC):
        return None
    return row
