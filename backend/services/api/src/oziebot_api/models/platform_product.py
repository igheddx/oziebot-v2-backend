from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Boolean, DateTime, String, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base


class PlatformProduct(Base):
    __tablename__ = "platform_products"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    product_key: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)
    display_name: Mapped[str] = mapped_column(String(128), nullable=False)
    description: Mapped[str | None] = mapped_column(String(512), nullable=True)
    is_active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default="true"
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    tenant_access_rows: Mapped[list["TenantProductAccess"]] = relationship(
        "TenantProductAccess",
        back_populates="product",
        cascade="all, delete-orphan",
    )
    user_preferences: Mapped[list["UserProductPreference"]] = relationship(
        "UserProductPreference",
        back_populates="default_product",
    )


from typing import TYPE_CHECKING  # noqa: E402

if TYPE_CHECKING:
    from oziebot_api.models.tenant_product_access import TenantProductAccess
    from oziebot_api.models.user_product_preference import UserProductPreference
