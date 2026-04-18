"""User token permissions API - token trading allowlist for individual users."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request

from oziebot_api.deps import DbSession
from oziebot_api.deps.auth import CurrentUser, RootAdminUser
from oziebot_api.models.platform_token import PlatformTokenAllowlist
from oziebot_api.models.user_token_permission import UserTokenPermission
from oziebot_api.schemas.tokens import (
    PlatformTokenAdminUpdate,
    PlatformTokenResponse,
    UserTokenPermissionResponse,
    UserTokenPermissionUpdate,
    UserTokenPermissionWithTokenDetail,
    UserTokenTradabilityCheck,
    UserTradableTokensList,
)
from oziebot_api.services.audit import record_admin_action
from oziebot_api.services.token_permissions import TokenPermissionService

router = APIRouter(tags=["tokens"])


# ============================================================================
# Root Admin Endpoints - Token Allowlist Management
# ============================================================================


@router.get("/admin/tokens")
def admin_list_platform_tokens(
    _admin: RootAdminUser,
    db: DbSession,
    skip: int = 0,
    limit: int = 100,
) -> dict[str, Any]:
    """
    List all platform tokens (admin only).

    Shows all tokens including disabled ones. Used by admin to manage
    the allowlist.
    """
    total = db.query(PlatformTokenAllowlist).count()
    tokens = (
        db.query(PlatformTokenAllowlist)
        .order_by(PlatformTokenAllowlist.sort_order, PlatformTokenAllowlist.symbol)
        .offset(skip)
        .limit(limit)
        .all()
    )
    return {
        "total": total,
        "skip": skip,
        "limit": limit,
        "items": [PlatformTokenResponse.model_validate(t) for t in tokens],
    }


@router.patch("/admin/tokens/{token_id}")
def admin_update_platform_token(
    token_id: UUID,
    body: PlatformTokenAdminUpdate,
    admin: RootAdminUser,
    db: DbSession,
    request: Request,
) -> PlatformTokenResponse:
    """
    Update a platform token (admin only).

    Can enable/disable tokens, change display name, sort order, and metadata.
    When a token is disabled, users can no longer trade it even if they
    have individual permissions.
    """
    token = db.query(PlatformTokenAllowlist).filter(PlatformTokenAllowlist.id == token_id).first()

    if not token:
        raise HTTPException(status_code=404, detail="Token not found")

    old_enabled = token.is_enabled
    updates = {}

    if body.is_enabled is not None:
        token.is_enabled = body.is_enabled
        updates["is_enabled"] = (old_enabled, body.is_enabled)

    if body.display_name is not None:
        token.display_name = body.display_name
        updates["display_name"] = body.display_name

    if body.sort_order is not None:
        token.sort_order = body.sort_order
        updates["sort_order"] = body.sort_order

    if body.extra is not None:
        token.extra = body.extra
        updates["extra"] = body.extra

    token.updated_at = datetime.now(UTC)
    db.add(token)
    db.commit()
    db.refresh(token)

    # Audit log
    record_admin_action(
        db,
        actor_user_id=admin.id,
        action="platform_token.update",
        resource_type="platform_token",
        resource_id=str(token_id),
        details=updates,
        ip_address=request.client.host if request.client else None,
        user_agent=request.headers.get("user-agent"),
    )

    return PlatformTokenResponse.model_validate(token)


# ============================================================================
# User Endpoints - Personal Token Trading Permissions
# ============================================================================


@router.get("/me/tokens")
def list_my_tokens(
    user: CurrentUser,
    db: DbSession,
) -> UserTradableTokensList:
    """
    List all user's token permissions (mobile-friendly format).

    Shows:
    - All platform tokens with user's enable/disable status
    - Count of enabled tokens
    - Count of actually tradable tokens (both enabled)
    """
    # Get all platform tokens
    all_tokens = (
        db.query(PlatformTokenAllowlist)
        .order_by(PlatformTokenAllowlist.sort_order, PlatformTokenAllowlist.symbol)
        .all()
    )

    # Get user's permissions
    user_perms = db.query(UserTokenPermission).filter(UserTokenPermission.user_id == user.id).all()

    # Create a map for fast lookup
    perm_map = {p.platform_token_id: p for p in user_perms}

    # Count stats
    tradable_count = 0
    user_enabled_count = 0

    # Build token list
    tokens_with_detail = []
    for token in all_tokens:
        perm = perm_map.get(token.id)
        is_user_enabled = perm.is_enabled if perm else False

        if is_user_enabled:
            user_enabled_count += 1

        if token.is_enabled and is_user_enabled:
            tradable_count += 1

        # Create response with embedded token
        perm_response = UserTokenPermissionWithTokenDetail(
            id=perm.id if perm else None,
            platform_token_id=token.id,
            is_enabled=is_user_enabled,
            created_at=perm.created_at if perm else datetime.now(UTC),
            updated_at=perm.updated_at if perm else datetime.now(UTC),
            token=PlatformTokenResponse.model_validate(token),
        )
        tokens_with_detail.append(perm_response)

    return UserTradableTokensList(
        total_platform_tokens=len(all_tokens),
        user_enabled_count=user_enabled_count,
        tradable_count=tradable_count,
        tokens=tokens_with_detail,
    )


@router.post("/me/tokens/{token_id}/enable")
def enable_token(
    token_id: UUID,
    user: CurrentUser,
    db: DbSession,
) -> UserTokenPermissionResponse:
    """
    Enable a token for trading (user can now trade it if admin has enabled it too).
    """
    try:
        perm = TokenPermissionService.enable_token_for_user(db, user.id, token_id)
        return UserTokenPermissionResponse.model_validate(perm)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/me/tokens/{token_id}/disable")
def disable_token(
    token_id: UUID,
    user: CurrentUser,
    db: DbSession,
) -> UserTokenPermissionResponse:
    """
    Disable a token for trading (user will not be able to trade it).
    """
    perm = TokenPermissionService.disable_token_for_user(db, user.id, token_id)

    if not perm:
        raise HTTPException(status_code=404, detail="Token permission not found")

    return UserTokenPermissionResponse.model_validate(perm)


@router.patch("/me/tokens/{token_id}")
def update_token_permission(
    token_id: UUID,
    body: UserTokenPermissionUpdate,
    user: CurrentUser,
    db: DbSession,
) -> UserTokenPermissionResponse:
    """
    Update a single token permission (enable or disable).
    """
    perm = TokenPermissionService.get_user_token_permission(db, user.id, token_id)

    if not perm:
        raise HTTPException(status_code=404, detail="Token permission not found")

    perm.is_enabled = body.is_enabled
    perm.updated_at = datetime.now(UTC)
    db.add(perm)
    db.commit()
    db.refresh(perm)

    return UserTokenPermissionResponse.model_validate(perm)


@router.get("/me/tokens/{token_id}/tradable")
def check_token_tradable(
    token_id: UUID,
    user: CurrentUser,
    db: DbSession,
) -> UserTokenTradabilityCheck:
    """
    Check if a token is tradable for the current user.

    Returns detailed breakdown of:
    - Is platform enabled? (admin control)
    - Is user enabled? (user control)
    - Is actually tradable? (both must be true)
    """
    # Get platform token
    platform_token = (
        db.query(PlatformTokenAllowlist).filter(PlatformTokenAllowlist.id == token_id).first()
    )

    if not platform_token:
        raise HTTPException(status_code=404, detail="Token not found")

    # Get user permission
    user_perm = TokenPermissionService.get_user_token_permission(db, user.id, token_id)
    is_user_enabled = user_perm.is_enabled if user_perm else False

    is_tradable = platform_token.is_enabled and is_user_enabled

    return UserTokenTradabilityCheck(
        platform_token_id=token_id,
        symbol=platform_token.symbol,
        is_platform_enabled=platform_token.is_enabled,
        is_user_enabled=is_user_enabled,
        is_tradable=is_tradable,
    )


@router.get("/me/tokens/tradable")
def list_tradable_tokens(
    user: CurrentUser,
    db: DbSession,
) -> dict[str, Any]:
    """
    Get list of tokens user can actually trade right now.

    Returns only tokens where BOTH conditions are true:
    - Platform token is_enabled=true
    - User permission is_enabled=true
    """
    tradable = TokenPermissionService.get_user_tradable_tokens(db, user.id)

    return {
        "count": len(tradable),
        "tokens": [
            {
                "id": str(t.id),
                "symbol": t.symbol,
                "quote_currency": t.quote_currency,
                "network": t.network,
                "display_name": t.display_name,
            }
            for t in tradable
        ],
    }
