from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Any

from sqlalchemy import JSON, Date, DateTime, ForeignKey, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base


class TeacherAssistAssignment(Base):
    __tablename__ = "assignments"

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
    class_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("classes.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    subject_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("subjects.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    title: Mapped[str] = mapped_column(String(160), nullable=False)
    description: Mapped[str | None] = mapped_column(Text(), nullable=True)
    assignment_type: Mapped[str] = mapped_column(String(32), nullable=False)
    due_date: Mapped[date | None] = mapped_column(Date(), nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    instructions: Mapped[str | None] = mapped_column(Text(), nullable=True)
    rubric_json: Mapped[dict[str, Any] | None] = mapped_column(JSON(), nullable=True)
    source_plan_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("weekly_plans.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    source_context_json: Mapped[dict[str, Any] | None] = mapped_column(JSON(), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    tenant: Mapped["Tenant"] = relationship("Tenant")
    teacher_user: Mapped["User"] = relationship("User")
    school_year: Mapped["TeacherAssistSchoolYear"] = relationship("TeacherAssistSchoolYear")
    grading_period: Mapped["TeacherAssistGradingPeriod | None"] = relationship("TeacherAssistGradingPeriod")
    teacher_class: Mapped["TeacherAssistClass"] = relationship("TeacherAssistClass")
    subject: Mapped["TeacherAssistSubject"] = relationship("TeacherAssistSubject")
    source_plan: Mapped["TeacherAssistWeeklyPlan | None"] = relationship("TeacherAssistWeeklyPlan")
    standard_links: Mapped[list["TeacherAssistAssignmentStandard"]] = relationship(
        "TeacherAssistAssignmentStandard",
        back_populates="assignment",
        cascade="all, delete-orphan",
        order_by="TeacherAssistAssignmentStandard.created_at.asc()",
    )
    resource_links: Mapped[list["TeacherAssistAssignmentResource"]] = relationship(
        "TeacherAssistAssignmentResource",
        back_populates="assignment",
        cascade="all, delete-orphan",
        order_by="TeacherAssistAssignmentResource.created_at.asc()",
    )
    print_packets: Mapped[list["TeacherAssistAssignmentPrintPacket"]] = relationship(
        "TeacherAssistAssignmentPrintPacket",
        back_populates="assignment",
        cascade="all, delete-orphan",
        order_by="TeacherAssistAssignmentPrintPacket.created_at.desc()",
    )
    student_work_submissions: Mapped[list["TeacherAssistStudentWorkSubmission"]] = relationship(
        "TeacherAssistStudentWorkSubmission",
        back_populates="assignment",
        cascade="all, delete-orphan",
        order_by="TeacherAssistStudentWorkSubmission.created_at.desc()",
    )


from typing import TYPE_CHECKING  # noqa: E402

if TYPE_CHECKING:
    from oziebot_api.models.teacher_assist_assignment_print_packet import (
        TeacherAssistAssignmentPrintPacket,
    )
    from oziebot_api.models.teacher_assist_assignment_resource import TeacherAssistAssignmentResource
    from oziebot_api.models.teacher_assist_assignment_standard import TeacherAssistAssignmentStandard
    from oziebot_api.models.teacher_assist_student_work_submission import (
        TeacherAssistStudentWorkSubmission,
    )
    from oziebot_api.models.teacher_assist_class import TeacherAssistClass
    from oziebot_api.models.teacher_assist_grading_period import TeacherAssistGradingPeriod
    from oziebot_api.models.teacher_assist_school_year import TeacherAssistSchoolYear
    from oziebot_api.models.teacher_assist_subject import TeacherAssistSubject
    from oziebot_api.models.teacher_assist_weekly_plan import TeacherAssistWeeklyPlan
    from oziebot_api.models.tenant import Tenant
    from oziebot_api.models.user import User
