from __future__ import annotations

import uuid
from datetime import date, datetime

from sqlalchemy import Date, DateTime, ForeignKey, Integer, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base


class TeacherAssistPlanningInputDraft(Base):
    __tablename__ = "planning_input_drafts"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    school_year_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("school_years.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    grading_period_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("grading_periods.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    class_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("classes.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    subject_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("subjects.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    pacing_guide_period_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("pacing_guide_periods.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    planning_scope: Mapped[str] = mapped_column(String(32), nullable=False, server_default="weekly")
    module_title: Mapped[str | None] = mapped_column(String(160), nullable=True)
    start_date: Mapped[date | None] = mapped_column(Date(), nullable=True)
    end_date: Mapped[date | None] = mapped_column(Date(), nullable=True)
    estimated_weeks: Mapped[int | None] = mapped_column(Integer, nullable=True)
    instructional_days_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    title: Mapped[str | None] = mapped_column(String(160), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text(), nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, server_default="draft")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    tenant: Mapped["Tenant"] = relationship("Tenant")
    user: Mapped["User"] = relationship("User")
    school_year: Mapped["TeacherAssistSchoolYear | None"] = relationship("TeacherAssistSchoolYear")
    grading_period: Mapped["TeacherAssistGradingPeriod | None"] = relationship(
        "TeacherAssistGradingPeriod"
    )
    teacher_class: Mapped["TeacherAssistClass | None"] = relationship("TeacherAssistClass")
    subject: Mapped["TeacherAssistSubject | None"] = relationship("TeacherAssistSubject")
    subject_links: Mapped[list["TeacherAssistPlanningInputDraftSubject"]] = relationship(
        "TeacherAssistPlanningInputDraftSubject",
        back_populates="planning_input_draft",
        cascade="all, delete-orphan",
    )
    pacing_item_links: Mapped[list["TeacherAssistPlanningInputDraftPacingItem"]] = relationship(
        "TeacherAssistPlanningInputDraftPacingItem",
        back_populates="planning_input_draft",
        cascade="all, delete-orphan",
    )
    standard_links: Mapped[list["TeacherAssistPlanningInputDraftStandard"]] = relationship(
        "TeacherAssistPlanningInputDraftStandard",
        back_populates="planning_input_draft",
        cascade="all, delete-orphan",
    )
    resource_links: Mapped[list["TeacherAssistPlanningInputDraftResource"]] = relationship(
        "TeacherAssistPlanningInputDraftResource",
        back_populates="planning_input_draft",
        cascade="all, delete-orphan",
    )


from typing import TYPE_CHECKING  # noqa: E402

if TYPE_CHECKING:
    from oziebot_api.models.teacher_assist_class import TeacherAssistClass
    from oziebot_api.models.teacher_assist_grading_period import TeacherAssistGradingPeriod
    from oziebot_api.models.teacher_assist_planning_input_draft_pacing_item import (
        TeacherAssistPlanningInputDraftPacingItem,
    )
    from oziebot_api.models.teacher_assist_planning_input_draft_resource import (
        TeacherAssistPlanningInputDraftResource,
    )
    from oziebot_api.models.teacher_assist_planning_input_draft_standard import (
        TeacherAssistPlanningInputDraftStandard,
    )
    from oziebot_api.models.teacher_assist_planning_input_draft_subject import (
        TeacherAssistPlanningInputDraftSubject,
    )
    from oziebot_api.models.teacher_assist_school_year import TeacherAssistSchoolYear
    from oziebot_api.models.teacher_assist_subject import TeacherAssistSubject
    from oziebot_api.models.tenant import Tenant
    from oziebot_api.models.user import User
