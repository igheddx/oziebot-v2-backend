"""User token trading permissions - users can enable/disable specific platform tokens."""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Uuid, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base


class UserTokenPermission(Base):
    """
    Tracks which platform tokens a user is allowed to trade.
    
    Users can only trade tokens that are:
    1. In the PlatformTokenAllowlist and is_enabled=true (admin control)
    2. In their UserTokenPermission record with is_enabled=true (user control)
    
    Both conditions must be true for a token to be tradable.
    """

    __tablename__ = "user_token_permissions"
    __table_args__ = (
        UniqueConstraint(
            "user_id", "platform_token_id", name="uq_user_token_permission"
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    platform_token_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("platform_token_allowlist.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    is_enabled: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default="true"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )

    user: Mapped["User"] = relationship("User")
    platform_token: Mapped["PlatformTokenAllowlist"] = relationship(
        "PlatformTokenAllowlist"
    )


from typing import TYPE_CHECKING  # noqa: E402

if TYPE_CHECKING:
    from oziebot_api.models.platform_token import PlatformTokenAllowlist
    from oziebot_api.models.user import User
