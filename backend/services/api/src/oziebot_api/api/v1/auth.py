"""Register, login, refresh, logout (JWT access + opaque refresh)."""

from __future__ import annotations

from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func, select
from starlette.responses import Response

from oziebot_api.config import Settings
from oziebot_api.deps import DbSession, settings_dep
from oziebot_api.models.membership import TenantMembership
from oziebot_api.models.tenant import Tenant
from oziebot_api.models.tenant_integration import TenantIntegration
from oziebot_api.models.user import User
from oziebot_api.schemas.auth import (
    LoginRequest,
    LogoutRequest,
    RefreshRequest,
    RegisterRequest,
    TokenResponse,
)
from oziebot_api.services.passwords import hash_password, verify_password
from oziebot_api.services.root_admin_defaults import ensure_root_admin_strategy_access
from oziebot_api.services.strategy_catalog import ensure_platform_strategy_catalog
from oziebot_api.services.tenant_scope import primary_tenant_id
from oziebot_api.services.token_permissions import TokenPermissionService
from oziebot_api.services.trial import start_trial_for_new_tenant
from oziebot_api.services.tokens import (
    create_access_token,
    create_refresh_session,
    get_session_by_refresh_hash,
    revoke_session_by_refresh,
)

router = APIRouter(prefix="/auth", tags=["auth"])


def _normalized_full_name(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def _issue_token_pair(db: DbSession, settings: Settings, user: User) -> TokenResponse:
    ensure_root_admin_strategy_access(db, user)
    tenant_id = primary_tenant_id(db, user)
    sess, raw = create_refresh_session(db, settings, user)
    access = create_access_token(
        settings,
        user=user,
        session_id=sess.id,
        tenant_id=tenant_id,
    )
    return TokenResponse(
        access_token=access,
        refresh_token=raw,
        expires_in=settings.jwt_access_exp_minutes * 60,
        role="root_admin" if user.is_root_admin else "user",
    )


@router.post("/register", response_model=TokenResponse, status_code=201)
def register(
    body: RegisterRequest, db: DbSession, settings: Settings = Depends(settings_dep)
) -> TokenResponse:
    email = body.email.lower().strip()
    exists = db.scalars(select(User).where(func.lower(User.email) == email)).one_or_none()
    if exists is not None:
        raise HTTPException(status_code=409, detail="Email already registered")
    now = datetime.now(UTC)
    tenant = Tenant(
        name=body.tenant_name.strip(),
        created_at=now,
        default_trading_mode="paper",
    )
    db.add(tenant)
    db.flush()
    db.add(
        TenantIntegration(
            tenant_id=tenant.id,
            coinbase_connected=False,
            updated_at=now,
        )
    )
    user = User(
        email=email,
        full_name=_normalized_full_name(body.full_name),
        password_hash=hash_password(body.password),
        is_root_admin=False,
        is_active=True,
        email_verified_at=None,
        current_trading_mode="paper",
        created_at=now,
        updated_at=now,
    )
    db.add(user)
    db.flush()
    db.add(
        TenantMembership(
            user_id=user.id,
            tenant_id=tenant.id,
            role="user",
            created_at=now,
        )
    )
    db.flush()
    ensure_platform_strategy_catalog(db)
    start_trial_for_new_tenant(db, tenant.id)
    # Initialize user with permissions for all currently enabled platform tokens
    TokenPermissionService.initialize_user_tokens(db, user.id, enabled=True)
    return _issue_token_pair(db, settings, user)


@router.post("/login", response_model=TokenResponse)
def login(
    body: LoginRequest, db: DbSession, settings: Settings = Depends(settings_dep)
) -> TokenResponse:
    email = body.email.lower().strip()
    user = db.scalars(select(User).where(func.lower(User.email) == email)).one_or_none()
    if user is None or not verify_password(body.password, user.password_hash):
        raise HTTPException(status_code=401, detail="Invalid email or password")
    if not user.is_active:
        raise HTTPException(status_code=403, detail="Account disabled")
    return _issue_token_pair(db, settings, user)


@router.post("/refresh", response_model=TokenResponse)
def refresh_token(
    body: RefreshRequest, db: DbSession, settings: Settings = Depends(settings_dep)
) -> TokenResponse:
    row = get_session_by_refresh_hash(db, body.refresh_token)
    if row is None:
        raise HTTPException(status_code=401, detail="Invalid or expired refresh token")
    user = db.get(User, row.user_id)
    if user is None or not user.is_active:
        raise HTTPException(status_code=401, detail="Invalid refresh token")
    row.revoked_at = datetime.now(UTC)
    db.flush()
    return _issue_token_pair(db, settings, user)


@router.post("/logout", status_code=204)
def logout(body: LogoutRequest, db: DbSession) -> Response:
    if not revoke_session_by_refresh(db, body.refresh_token):
        raise HTTPException(status_code=400, detail="Invalid refresh token")
    return Response(status_code=204)
