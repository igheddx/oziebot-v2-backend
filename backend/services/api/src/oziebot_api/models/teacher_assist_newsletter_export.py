from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base


class TeacherAssistNewsletterExport(Base):
    __tablename__ = "teacher_assist_newsletter_exports"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    owner_user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    newsletter_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("teacher_assist_newsletters.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    newsletter_version_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("teacher_assist_newsletter_versions.id", ondelete="SET NULL"),
        nullable=True,
    )
    export_format: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    storage_key: Mapped[str] = mapped_column(Text(), nullable=False)
    file_size_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    tenant: Mapped["Tenant"] = relationship("Tenant")
    owner_user: Mapped["User"] = relationship("User", foreign_keys=[owner_user_id])
    newsletter: Mapped["TeacherAssistNewsletter"] = relationship(
        "TeacherAssistNewsletter",
        back_populates="exports",
    )


from typing import TYPE_CHECKING  # noqa: E402

if TYPE_CHECKING:
    from oziebot_api.models.teacher_assist_newsletter import TeacherAssistNewsletter
    from oziebot_api.models.tenant import Tenant
    from oziebot_api.models.user import User
