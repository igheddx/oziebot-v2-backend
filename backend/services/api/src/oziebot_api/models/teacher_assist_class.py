from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, String, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base


class TeacherAssistClass(Base):
    __tablename__ = "classes"

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
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    grade_level: Mapped[str] = mapped_column(String(32), nullable=False)
    student_count: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    tenant: Mapped["Tenant"] = relationship("Tenant")
    school_year: Mapped["TeacherAssistSchoolYear"] = relationship(
        "TeacherAssistSchoolYear", back_populates="classes"
    )
    class_subjects: Mapped[list["TeacherAssistClassSubject"]] = relationship(
        "TeacherAssistClassSubject",
        back_populates="teacher_class",
        cascade="all, delete-orphan",
    )


from typing import TYPE_CHECKING  # noqa: E402

if TYPE_CHECKING:
    from oziebot_api.models.teacher_assist_class_subject import TeacherAssistClassSubject
    from oziebot_api.models.teacher_assist_school_year import TeacherAssistSchoolYear
    from oziebot_api.models.tenant import Tenant
