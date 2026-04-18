"""JWT Bearer dependencies and role gates."""

from __future__ import annotations

from typing import Annotated

from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError

from oziebot_api.config import Settings
from oziebot_api.deps import DbSession, settings_dep
from oziebot_api.models.user import User
from oziebot_api.services.tokens import (
    decode_access_token,
    get_valid_session,
    validate_access_payload,
)

_bearer = HTTPBearer(auto_error=False)


def get_access_token_payload(
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer),
    settings: Settings = Depends(settings_dep),
) -> dict | None:
    if credentials is None or credentials.scheme.lower() != "bearer":
        return None
    try:
        return decode_access_token(settings, credentials.credentials)
    except JWTError:
        return None


def get_current_user(
    db: DbSession,
    settings: Settings = Depends(settings_dep),
    payload: dict | None = Depends(get_access_token_payload),
) -> User | None:
    if payload is None:
        return None
    try:
        uid, sid, _, _ = validate_access_payload(payload)
    except JWTError:
        return None
    user = db.get(User, uid)
    if user is None or not user.is_active:
        return None
    if get_valid_session(db, sid, uid) is None:
        return None
    return user


def require_user(user: User | None = Depends(get_current_user)) -> User:
    if user is None:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return user


def require_root_admin(user: User = Depends(require_user)) -> User:
    if not user.is_root_admin:
        raise HTTPException(status_code=403, detail="Root admin only")
    return user


CurrentUser = Annotated[User, Depends(require_user)]
RootAdminUser = Annotated[User, Depends(require_root_admin)]
