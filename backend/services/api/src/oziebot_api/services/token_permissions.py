"""Service logic for two-tier token permission model."""

from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import and_
from sqlalchemy.orm import Session

from oziebot_api.models.platform_token import PlatformTokenAllowlist
from oziebot_api.models.user_token_permission import UserTokenPermission


class TokenPermissionService:
    """Business logic for token trading permissions."""

    @staticmethod
    def is_token_tradable_for_user(db: Session, user_id: UUID, platform_token_id: UUID) -> bool:
        """
        Check if a user can trade a specific token.

        Token is tradable if BOTH conditions are true:
        1. Platform token exists and is_enabled=true (admin control)
        2. User has a permission record for it with is_enabled=true (user control)

        Args:
            db: Database session
            user_id: UUID of the user
            platform_token_id: UUID of the platform token

        Returns:
            bool: True if token is tradable for user, False otherwise
        """
        # Check platform token is enabled
        platform_token = (
            db.query(PlatformTokenAllowlist)
            .filter(
                PlatformTokenAllowlist.id == platform_token_id,
                PlatformTokenAllowlist.is_enabled == True,  # noqa: E712
            )
            .first()
        )

        if not platform_token:
            return False

        # Check user has permission enabled
        user_permission = (
            db.query(UserTokenPermission)
            .filter(
                UserTokenPermission.user_id == user_id,
                UserTokenPermission.platform_token_id == platform_token_id,
                UserTokenPermission.is_enabled == True,  # noqa: E712
            )
            .first()
        )

        return user_permission is not None

    @staticmethod
    def get_user_tradable_tokens(db: Session, user_id: UUID) -> list[dict]:
        """
        Get all tokens a user can currently trade.

        Returns a list of platform tokens where both:
        - Platform token is_enabled=true
        - User permission is_enabled=true
        """
        result = (
            db.query(PlatformTokenAllowlist)
            .join(
                UserTokenPermission,
                and_(
                    UserTokenPermission.platform_token_id == PlatformTokenAllowlist.id,
                    UserTokenPermission.user_id == user_id,
                    UserTokenPermission.is_enabled == True,  # noqa: E712
                ),
            )
            .filter(
                PlatformTokenAllowlist.is_enabled == True,  # noqa: E712
            )
            .all()
        )

        return result

    @staticmethod
    def enable_token_for_user(
        db: Session, user_id: UUID, platform_token_id: UUID
    ) -> UserTokenPermission:
        """
        Enable a token for a user (or create if doesn't exist).

        Args:
            db: Database session
            user_id: UUID of the user
            platform_token_id: UUID of the platform token

        Returns:
            UserTokenPermission: The permission record (newly created or updated)

        Raises:
            ValueError: If platform token doesn't exist or is disabled
        """
        # Verify platform token exists and is enabled
        platform_token = (
            db.query(PlatformTokenAllowlist)
            .filter(
                PlatformTokenAllowlist.id == platform_token_id,
                PlatformTokenAllowlist.is_enabled == True,  # noqa: E712
            )
            .first()
        )

        if not platform_token:
            raise ValueError(f"Platform token {platform_token_id} not found or is disabled")

        # Check if permission already exists
        existing = (
            db.query(UserTokenPermission)
            .filter(
                UserTokenPermission.user_id == user_id,
                UserTokenPermission.platform_token_id == platform_token_id,
            )
            .first()
        )

        if existing:
            if not existing.is_enabled:
                existing.is_enabled = True
                existing.updated_at = datetime.now(UTC)
                db.add(existing)
                db.commit()
                db.refresh(existing)
            return existing

        # Create new permission
        permission = UserTokenPermission(
            user_id=user_id,
            platform_token_id=platform_token_id,
            is_enabled=True,
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
        )
        db.add(permission)
        db.commit()
        db.refresh(permission)
        return permission

    @staticmethod
    def disable_token_for_user(
        db: Session, user_id: UUID, platform_token_id: UUID
    ) -> UserTokenPermission | None:
        """
        Disable a token for a user.

        Returns the updated permission, or None if it didn't exist.
        """
        permission = (
            db.query(UserTokenPermission)
            .filter(
                UserTokenPermission.user_id == user_id,
                UserTokenPermission.platform_token_id == platform_token_id,
            )
            .first()
        )

        if permission:
            permission.is_enabled = False
            permission.updated_at = datetime.now(UTC)
            db.add(permission)
            db.commit()
            db.refresh(permission)

        return permission

    @staticmethod
    def get_user_token_permission(
        db: Session, user_id: UUID, platform_token_id: UUID
    ) -> UserTokenPermission | None:
        """Get a specific user token permission record."""
        return (
            db.query(UserTokenPermission)
            .filter(
                UserTokenPermission.user_id == user_id,
                UserTokenPermission.platform_token_id == platform_token_id,
            )
            .first()
        )

    @staticmethod
    def list_user_token_permissions(db: Session, user_id: UUID) -> list[UserTokenPermission]:
        """List all token permissions for a user (enabled and disabled)."""
        return db.query(UserTokenPermission).filter(UserTokenPermission.user_id == user_id).all()

    @staticmethod
    def initialize_user_tokens(
        db: Session, user_id: UUID, enabled: bool = True
    ) -> list[UserTokenPermission]:
        """
        Initialize permissions for a new user with all currently enabled platform tokens.

        Called when a user is created to grant them access to all currently
        enabled platform tokens. Returns list of created permissions.
        """
        enabled_tokens = (
            db.query(PlatformTokenAllowlist)
            .filter(
                PlatformTokenAllowlist.is_enabled == True,  # noqa: E712
            )
            .all()
        )

        permissions = []
        now = datetime.now(UTC)

        for token in enabled_tokens:
            permission = UserTokenPermission(
                user_id=user_id,
                platform_token_id=token.id,
                is_enabled=enabled,
                created_at=now,
                updated_at=now,
            )
            db.add(permission)
            permissions.append(permission)

        db.commit()
        return permissions
