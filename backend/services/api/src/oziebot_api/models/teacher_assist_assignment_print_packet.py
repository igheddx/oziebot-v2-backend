from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, String, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base


class TeacherAssistAssignmentPrintPacket(Base):
    __tablename__ = "assignment_print_packets"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    teacher_user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    assignment_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("assignments.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    class_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("classes.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    school_year_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("school_years.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    grading_period_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("grading_periods.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    subject_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("subjects.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    packet_status: Mapped[str] = mapped_column(String(32), nullable=False)
    pages_per_student: Mapped[int] = mapped_column(Integer, nullable=False)
    student_count: Mapped[int] = mapped_column(Integer, nullable=False)
    template_type: Mapped[str] = mapped_column(String(64), nullable=False)
    output_format: Mapped[str] = mapped_column(String(32), nullable=False)
    storage_key: Mapped[str | None] = mapped_column(String(512), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    tenant: Mapped["Tenant"] = relationship("Tenant")
    teacher_user: Mapped["User"] = relationship("User")
    assignment: Mapped["TeacherAssistAssignment"] = relationship(
        "TeacherAssistAssignment", back_populates="print_packets"
    )
    teacher_class: Mapped["TeacherAssistClass"] = relationship("TeacherAssistClass")
    school_year: Mapped["TeacherAssistSchoolYear"] = relationship("TeacherAssistSchoolYear")
    grading_period: Mapped["TeacherAssistGradingPeriod | None"] = relationship("TeacherAssistGradingPeriod")
    subject: Mapped["TeacherAssistSubject"] = relationship("TeacherAssistSubject")
    pages: Mapped[list["TeacherAssistAssignmentPrintPage"]] = relationship(
        "TeacherAssistAssignmentPrintPage",
        back_populates="packet",
        cascade="all, delete-orphan",
        order_by="TeacherAssistAssignmentPrintPage.student_number.asc(), TeacherAssistAssignmentPrintPage.page_number.asc()",
    )


from typing import TYPE_CHECKING  # noqa: E402

if TYPE_CHECKING:
    from oziebot_api.models.teacher_assist_assignment import TeacherAssistAssignment
    from oziebot_api.models.teacher_assist_assignment_print_page import TeacherAssistAssignmentPrintPage
    from oziebot_api.models.teacher_assist_class import TeacherAssistClass
    from oziebot_api.models.teacher_assist_grading_period import TeacherAssistGradingPeriod
    from oziebot_api.models.teacher_assist_school_year import TeacherAssistSchoolYear
    from oziebot_api.models.teacher_assist_subject import TeacherAssistSubject
    from oziebot_api.models.tenant import Tenant
    from oziebot_api.models.user import User
