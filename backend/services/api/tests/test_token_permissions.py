"""Tests for two-tier token permission model."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest
from sqlalchemy.orm import Session

from oziebot_api.models.platform_token import PlatformTokenAllowlist
from oziebot_api.models.user import User
from oziebot_api.models.user_token_permission import UserTokenPermission
from oziebot_api.services.passwords import hash_password
from oziebot_api.services.token_permissions import TokenPermissionService


# ============================================================================
# Fixtures - Create test data
# ============================================================================


@pytest.fixture
def admin_user_and_token(client, db_session: Session) -> tuple[str, str]:
    """Create a root admin user and return email + token."""
    email = "admin@example.com"
    password = "admin-password-123"
    now = datetime.now(UTC)
    u = User(
        id=uuid.uuid4(),
        email=email,
        full_name="Admin Example",
        password_hash=hash_password(password),
        is_root_admin=True,
        is_active=True,
        email_verified_at=None,
        current_trading_mode="paper",
        created_at=now,
        updated_at=now,
    )
    db_session.add(u)
    db_session.commit()
    r = client.post("/v1/auth/login", json={"email": email, "password": password})
    assert r.status_code == 200, r.text
    return email, r.json()["access_token"]


@pytest.fixture
def regular_user_and_token(client, db_session: Session) -> tuple[str, str]:
    """Create a regular (non-admin) user and return email + token."""
    email = "user@example.com"
    password = "user-password-123"
    r = client.post(
        "/v1/auth/register",
        json={
            "email": email,
            "password": password,
            "tenant_name": "User Tenant",
        },
    )
    assert r.status_code == 201, r.text
    return email, r.json()["access_token"]


@pytest.fixture
def platform_tokens(db_session: Session) -> list[PlatformTokenAllowlist]:
    """Create test platform tokens."""
    now = datetime.now(UTC)
    tokens = [
        PlatformTokenAllowlist(
            id=uuid.uuid4(),
            symbol="BTC",
            quote_currency="USD",
            network="mainnet",
            display_name="Bitcoin",
            is_enabled=True,
            sort_order=1,
            created_at=now,
            updated_at=now,
        ),
        PlatformTokenAllowlist(
            id=uuid.uuid4(),
            symbol="ETH",
            quote_currency="USD",
            network="mainnet",
            display_name="Ethereum",
            is_enabled=True,
            sort_order=2,
            created_at=now,
            updated_at=now,
        ),
        PlatformTokenAllowlist(
            id=uuid.uuid4(),
            symbol="USDC",
            quote_currency="USD",
            network="mainnet",
            display_name="USDC",
            is_enabled=False,  # Disabled token
            sort_order=3,
            created_at=now,
            updated_at=now,
        ),
    ]
    db_session.add_all(tokens)
    db_session.commit()
    return tokens


# ============================================================================
# Admin API Tests
# ============================================================================


def test_admin_list_platform_tokens_requires_root(client, platform_tokens):
    """Only root admins can list platform tokens."""
    r = client.get("/v1/admin/tokens")
    assert r.status_code == 401


def test_admin_list_platform_tokens_success(client, admin_user_and_token, platform_tokens):
    """Root admin can list all platform tokens (including disabled ones)."""
    _, token = admin_user_and_token
    r = client.get(
        "/v1/admin/tokens",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r.status_code == 200
    data = r.json()
    assert data["total"] == 3
    assert len(data["items"]) == 3
    # Check tokens are ordered
    assert data["items"][0]["symbol"] == "BTC"
    assert data["items"][1]["symbol"] == "ETH"
    assert data["items"][2]["symbol"] == "USDC"


def test_admin_update_token_enable_disable(client, admin_user_and_token, platform_tokens):
    """Root admin can enable/disable tokens."""
    _, token = admin_user_and_token
    usdc_token = platform_tokens[2]  # Disabled token

    # Enable the disabled token
    r = client.patch(
        f"/v1/admin/tokens/{usdc_token.id}",
        headers={"Authorization": f"Bearer {token}"},
        json={"is_enabled": True},
    )
    assert r.status_code == 200
    assert r.json()["is_enabled"] is True


def test_admin_update_token_metadata(client, admin_user_and_token, platform_tokens):
    """Root admin can update token metadata."""
    _, token = admin_user_and_token
    btc_token = platform_tokens[0]

    r = client.patch(
        f"/v1/admin/tokens/{btc_token.id}",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "display_name": "Bitcoin (Updated)",
            "sort_order": 10,
        },
    )
    assert r.status_code == 200
    data = r.json()
    assert data["display_name"] == "Bitcoin (Updated)"
    assert data["sort_order"] == 10


def test_admin_update_nonexistent_token(client, admin_user_and_token):
    """Admin cannot update a token that doesn't exist."""
    _, token = admin_user_and_token

    r = client.patch(
        f"/v1/admin/tokens/{uuid.uuid4()}",
        headers={"Authorization": f"Bearer {token}"},
        json={"is_enabled": False},
    )
    assert r.status_code == 404


# ============================================================================
# User Token Permissions API Tests
# ============================================================================


def test_user_list_tokens_requires_auth(client, platform_tokens):
    """User must be authenticated to list tokens."""
    r = client.get("/v1/me/tokens")
    assert r.status_code == 401


def test_user_list_tokens_empty(client, regular_user_and_token, platform_tokens):
    """New user has no initial token permissions."""
    _, token = regular_user_and_token
    r = client.get(
        "/v1/me/tokens",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r.status_code == 200
    data = r.json()
    assert data["total_platform_tokens"] == 3
    assert data["user_enabled_count"] == 0
    assert data["tradable_count"] == 0
    assert len(data["tokens"]) == 3


def test_user_enable_token(client, regular_user_and_token, platform_tokens):
    """User can enable a platform token for trading."""
    _, token = regular_user_and_token
    btc_token = platform_tokens[0]

    r = client.post(
        f"/v1/me/tokens/{btc_token.id}/enable",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r.status_code == 200
    data = r.json()
    assert str(data["platform_token_id"]) == str(btc_token.id)
    assert data["is_enabled"] is True


def test_user_enable_disabled_platform_token_fails(client, regular_user_and_token, platform_tokens):
    """User cannot enable a platform token that admin has disabled."""
    _, token = regular_user_and_token
    usdc_token = platform_tokens[2]  # Disabled token

    r = client.post(
        f"/v1/me/tokens/{usdc_token.id}/enable",
        headers={"Authorization": f"Bearer {token}"},
    )
    # Should fail with 404 since token is disabled by admin
    assert r.status_code == 404


def test_user_disable_token(client, regular_user_and_token, platform_tokens):
    """User can disable a token they previously enabled."""
    _, token = regular_user_and_token
    btc_token = platform_tokens[0]

    # First enable
    r = client.post(
        f"/v1/me/tokens/{btc_token.id}/enable",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r.status_code == 200

    # Then disable
    r = client.post(
        f"/v1/me/tokens/{btc_token.id}/disable",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r.status_code == 200
    data = r.json()
    assert data["is_enabled"] is False


def test_user_patch_permission(client, regular_user_and_token, platform_tokens):
    """User can update token permission with PATCH."""
    _, token = regular_user_and_token
    btc_token = platform_tokens[0]

    # Enable first
    client.post(
        f"/v1/me/tokens/{btc_token.id}/enable",
        headers={"Authorization": f"Bearer {token}"},
    )

    # Disable via PATCH
    r = client.patch(
        f"/v1/me/tokens/{btc_token.id}",
        headers={"Authorization": f"Bearer {token}"},
        json={"is_enabled": False},
    )
    assert r.status_code == 200
    assert r.json()["is_enabled"] is False


# ============================================================================
# Token Tradability Tests
# ============================================================================


def test_token_tradable_requires_both_conditions(
    client, regular_user_and_token, admin_user_and_token, platform_tokens
):
    """Token is tradable only if BOTH platform enabled AND user enabled."""
    _, user_token = regular_user_and_token
    _, admin_token = admin_user_and_token
    btc_token = platform_tokens[0]  # Enabled

    # Case 1: Platform enabled, user not enabled -> not tradable
    r = client.get(
        f"/v1/me/tokens/{btc_token.id}/tradable",
        headers={"Authorization": f"Bearer {user_token}"},
    )
    assert r.status_code == 200
    data = r.json()
    assert data["is_platform_enabled"] is True
    assert data["is_user_enabled"] is False
    assert data["is_tradable"] is False

    # Case 2: User enables it -> now tradable
    client.post(
        f"/v1/me/tokens/{btc_token.id}/enable",
        headers={"Authorization": f"Bearer {user_token}"},
    )

    r = client.get(
        f"/v1/me/tokens/{btc_token.id}/tradable",
        headers={"Authorization": f"Bearer {user_token}"},
    )
    assert r.status_code == 200
    data = r.json()
    assert data["is_tradable"] is True

    # Case 3: Platform token is disabled by admin -> not tradable
    # First, have admin disable it
    client.patch(
        f"/v1/admin/tokens/{btc_token.id}",
        headers={"Authorization": f"Bearer {admin_token}"},
        json={"is_enabled": False},
    )

    r = client.get(
        f"/v1/me/tokens/{btc_token.id}/tradable",
        headers={"Authorization": f"Bearer {user_token}"},
    )
    assert r.status_code == 200
    data = r.json()
    assert data["is_platform_enabled"] is False
    assert data["is_user_enabled"] is True  # User still enabled it
    assert data["is_tradable"] is False  # But not tradable


def test_list_tradable_tokens(client, regular_user_and_token, platform_tokens):
    """List only tokens that are actually tradable."""
    _, token = regular_user_and_token
    btc_token = platform_tokens[0]
    eth_token = platform_tokens[1]

    # Enable BTC and ETH
    client.post(
        f"/v1/me/tokens/{btc_token.id}/enable",
        headers={"Authorization": f"Bearer {token}"},
    )
    client.post(
        f"/v1/me/tokens/{eth_token.id}/enable",
        headers={"Authorization": f"Bearer {token}"},
    )

    # Get tradable tokens
    r = client.get(
        "/v1/me/tokens/tradable",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r.status_code == 200
    data = r.json()
    assert data["count"] == 2
    assert len(data["tokens"]) == 2
    symbols = [t["symbol"] for t in data["tokens"]]
    assert "BTC" in symbols
    assert "ETH" in symbols
    assert "USDC" not in symbols


# ============================================================================
# Service Logic Tests
# ============================================================================


def test_service_is_token_tradable_for_user(platform_tokens, db_session):
    """Service correctly determines if token is tradable."""
    user_id = uuid.uuid4()
    btc_token = platform_tokens[0]

    # Add user permission for BTC
    now = datetime.now(UTC)
    perm = UserTokenPermission(
        id=uuid.uuid4(),
        user_id=user_id,
        platform_token_id=btc_token.id,
        is_enabled=True,
        created_at=now,
        updated_at=now,
    )
    db_session.add(perm)
    db_session.commit()

    # Should be tradable
    assert TokenPermissionService.is_token_tradable_for_user(db_session, user_id, btc_token.id)

    # If admin disables it, not tradable anymore
    btc_token.is_enabled = False
    db_session.commit()

    assert not TokenPermissionService.is_token_tradable_for_user(db_session, user_id, btc_token.id)


def test_service_initialize_user_tokens(platform_tokens, db_session):
    """Service initializes new user with all enabled platform tokens."""
    user_id = uuid.uuid4()

    perms = TokenPermissionService.initialize_user_tokens(db_session, user_id, enabled=True)

    # Should have created permissions for BTC and ETH (not USDC, which is disabled)
    assert len(perms) == 2
    symbols = {p.platform_token.symbol for p in perms}
    assert "BTC" in symbols
    assert "ETH" in symbols
    assert "USDC" not in symbols


def test_service_get_tradable_tokens(platform_tokens, db_session):
    """Service returns only tradable tokens."""
    user_id = uuid.uuid4()
    btc_token = platform_tokens[0]
    eth_token = platform_tokens[1]

    # Set up permissions: enable BTC, disable ETH
    now = datetime.now(UTC)
    perms = [
        UserTokenPermission(
            id=uuid.uuid4(),
            user_id=user_id,
            platform_token_id=btc_token.id,
            is_enabled=True,
            created_at=now,
            updated_at=now,
        ),
        UserTokenPermission(
            id=uuid.uuid4(),
            user_id=user_id,
            platform_token_id=eth_token.id,
            is_enabled=False,
            created_at=now,
            updated_at=now,
        ),
    ]
    db_session.add_all(perms)
    db_session.commit()

    # Get tradable
    tradable = TokenPermissionService.get_user_tradable_tokens(db_session, user_id)

    # Only BTC should be tradable
    assert len(tradable) == 1
    assert tradable[0].symbol == "BTC"


# ============================================================================
# Permission Boundary Tests
# ============================================================================


def test_regular_user_cannot_update_platform_tokens(
    client, regular_user_and_token, platform_tokens
):
    """Regular users cannot call admin endpoints."""
    _, token = regular_user_and_token
    btc_token = platform_tokens[0]

    r = client.patch(
        f"/v1/admin/tokens/{btc_token.id}",
        headers={"Authorization": f"Bearer {token}"},
        json={"is_enabled": False},
    )
    assert r.status_code == 403


def test_unauthenticated_cannot_access_admin_endpoints(client, platform_tokens):
    """No auth -> 401 on admin endpoints."""
    btc_token = platform_tokens[0]

    r = client.patch(
        f"/v1/admin/tokens/{btc_token.id}",
        json={"is_enabled": False},
    )
    assert r.status_code == 401
