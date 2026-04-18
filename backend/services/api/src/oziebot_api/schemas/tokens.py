"""Schemas for two-tier token permission model."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field


# ============================================================================
# Admin Schemas - for managing platform tokens
# ============================================================================


class PlatformTokenResponse(BaseModel):
    """Read-only view of a platform-approved token."""

    id: UUID
    symbol: str
    quote_currency: str
    network: str
    contract_address: str | None
    display_name: str | None
    is_enabled: bool
    sort_order: int
    extra: dict[str, Any] | None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class PlatformTokenAdminUpdate(BaseModel):
    """Admin update for platform token (can change is_enabled, sort_order, etc.)."""

    is_enabled: bool | None = None
    display_name: str | None = None
    sort_order: int | None = None
    extra: dict[str, Any] | None = None


# ============================================================================
# User Schemas - for managing user token permissions
# ============================================================================


class UserTokenPermissionCreate(BaseModel):
    """Request to create or reset user token permission (enable by default)."""

    platform_token_id: UUID = Field(description="ID of platform-approved token")
    is_enabled: bool = Field(default=True, description="Whether user can trade this token")


class UserTokenPermissionUpdate(BaseModel):
    """Toggle a user's access to a specific token."""

    is_enabled: bool = Field(description="Enable or disable this token for the user's trading")


class UserTokenPermissionResponse(BaseModel):
    """User's permission for a single platform token."""

    id: UUID
    user_id: UUID
    platform_token_id: UUID
    is_enabled: bool
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class UserTokenPermissionWithTokenDetail(BaseModel):
    """User token permission with embedded token details (for UI lists)."""

    id: UUID | None
    platform_token_id: UUID
    is_enabled: bool
    created_at: datetime
    updated_at: datetime
    # Embedded token fields for mobile/web UI
    token: PlatformTokenResponse

    model_config = {"from_attributes": True}


class UserTokenTradabilityCheck(BaseModel):
    """Response for checking if a token is tradable for a user."""

    platform_token_id: UUID
    symbol: str
    is_platform_enabled: bool
    is_user_enabled: bool
    is_tradable: bool = Field(description="true if both platform_enabled AND user_enabled")


class UserTradableTokensList(BaseModel):
    """Mobile-friendly list of a user's tokens and their trading status."""

    total_platform_tokens: int
    user_enabled_count: int
    tradable_count: int
    tokens: list[UserTokenPermissionWithTokenDetail]
