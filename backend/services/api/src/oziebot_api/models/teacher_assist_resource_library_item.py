from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base


class TeacherAssistResourceLibraryItem(Base):
    __tablename__ = "resource_library_items"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    uploaded_by_user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    title: Mapped[str] = mapped_column(String(160), nullable=False)
    description: Mapped[str | None] = mapped_column(Text(), nullable=True)
    resource_type: Mapped[str] = mapped_column(String(32), nullable=False)
    storage_key: Mapped[str | None] = mapped_column(String(512), nullable=True)
    original_filename: Mapped[str | None] = mapped_column(String(255), nullable=True)
    mime_type: Mapped[str | None] = mapped_column(String(255), nullable=True)
    file_size: Mapped[int | None] = mapped_column(Integer, nullable=True)
    external_url: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    uploaded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    tenant: Mapped["Tenant"] = relationship("Tenant")
    uploaded_by_user: Mapped["User"] = relationship("User")
    pacing_item_links: Mapped[list["TeacherAssistPacingItemResource"]] = relationship(
        "TeacherAssistPacingItemResource",
        back_populates="resource_library_item",
        cascade="all, delete-orphan",
    )
    planning_draft_links: Mapped[list["TeacherAssistPlanningInputDraftResource"]] = relationship(
        "TeacherAssistPlanningInputDraftResource",
        back_populates="resource_library_item",
        cascade="all, delete-orphan",
    )


from typing import TYPE_CHECKING  # noqa: E402

if TYPE_CHECKING:
    from oziebot_api.models.teacher_assist_pacing_item_resource import TeacherAssistPacingItemResource
    from oziebot_api.models.teacher_assist_planning_input_draft_resource import (
        TeacherAssistPlanningInputDraftResource,
    )
    from oziebot_api.models.tenant import Tenant
    from oziebot_api.models.user import User
