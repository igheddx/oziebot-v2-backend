from __future__ import annotations

import uuid
from datetime import date, datetime

from sqlalchemy import Date, DateTime, ForeignKey, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base


class TeacherAssistNewsletter(Base):
    __tablename__ = "teacher_assist_newsletters"

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
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    week_start_date: Mapped[date | None] = mapped_column(Date(), nullable=True)
    week_end_date: Mapped[date | None] = mapped_column(Date(), nullable=True)
    teacher_notes: Mapped[str | None] = mapped_column(Text(), nullable=True)
    pacing_guide_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("pacing_guides.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    pacing_guide_period_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("pacing_guide_periods.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    instructional_week_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("instructional_weeks.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    current_version_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("teacher_assist_newsletter_versions.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    latest_ai_usage_event_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("teacher_assist_ai_usage_events.id", ondelete="SET NULL"),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    tenant: Mapped["Tenant"] = relationship("Tenant")
    owner_user: Mapped["User"] = relationship("User", foreign_keys=[owner_user_id])
    school_year: Mapped["TeacherAssistSchoolYear"] = relationship("TeacherAssistSchoolYear")
    grading_period: Mapped["TeacherAssistGradingPeriod | None"] = relationship(
        "TeacherAssistGradingPeriod"
    )
    teacher_class: Mapped["TeacherAssistClass"] = relationship("TeacherAssistClass")
    subject: Mapped["TeacherAssistSubject"] = relationship("TeacherAssistSubject")
    current_version: Mapped["TeacherAssistNewsletterVersion | None"] = relationship(
        "TeacherAssistNewsletterVersion",
        foreign_keys=[current_version_id],
    )
    versions: Mapped[list["TeacherAssistNewsletterVersion"]] = relationship(
        "TeacherAssistNewsletterVersion",
        back_populates="newsletter",
        foreign_keys="TeacherAssistNewsletterVersion.newsletter_id",
        order_by="TeacherAssistNewsletterVersion.version_number.asc()",
    )
    exports: Mapped[list["TeacherAssistNewsletterExport"]] = relationship(
        "TeacherAssistNewsletterExport",
        back_populates="newsletter",
        order_by="TeacherAssistNewsletterExport.created_at.desc()",
    )


from typing import TYPE_CHECKING  # noqa: E402

if TYPE_CHECKING:
    from oziebot_api.models.teacher_assist_class import TeacherAssistClass
    from oziebot_api.models.teacher_assist_grading_period import TeacherAssistGradingPeriod
    from oziebot_api.models.teacher_assist_newsletter_export import TeacherAssistNewsletterExport
    from oziebot_api.models.teacher_assist_newsletter_version import TeacherAssistNewsletterVersion
    from oziebot_api.models.teacher_assist_school_year import TeacherAssistSchoolYear
    from oziebot_api.models.teacher_assist_subject import TeacherAssistSubject
    from oziebot_api.models.tenant import Tenant
    from oziebot_api.models.user import User
