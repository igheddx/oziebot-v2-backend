from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base


class TeacherAssistPacingGuide(Base):
    __tablename__ = "pacing_guides"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    school_year_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("school_years.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    title: Mapped[str] = mapped_column(String(128), nullable=False)
    description: Mapped[str | None] = mapped_column(Text(), nullable=True)
    grade_level: Mapped[str | None] = mapped_column(String(32), nullable=True)
    subject_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("subjects.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    is_shared: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default="false"
    )
    created_by_user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    tenant: Mapped["Tenant"] = relationship("Tenant")
    school_year: Mapped["TeacherAssistSchoolYear"] = relationship("TeacherAssistSchoolYear")
    subject: Mapped["TeacherAssistSubject | None"] = relationship("TeacherAssistSubject")
    created_by_user: Mapped["User"] = relationship("User")
    items: Mapped[list["TeacherAssistPacingItem"]] = relationship(
        "TeacherAssistPacingItem",
        back_populates="pacing_guide",
        cascade="all, delete-orphan",
    )


from typing import TYPE_CHECKING  # noqa: E402

if TYPE_CHECKING:
    from oziebot_api.models.teacher_assist_pacing_item import TeacherAssistPacingItem
    from oziebot_api.models.teacher_assist_school_year import TeacherAssistSchoolYear
    from oziebot_api.models.teacher_assist_subject import TeacherAssistSubject
    from oziebot_api.models.tenant import Tenant
    from oziebot_api.models.user import User
