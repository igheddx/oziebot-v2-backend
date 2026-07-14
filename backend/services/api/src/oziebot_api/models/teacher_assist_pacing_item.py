from __future__ import annotations

import uuid
from datetime import date, datetime

from sqlalchemy import Date, DateTime, ForeignKey, Integer, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base


class TeacherAssistPacingItem(Base):
    __tablename__ = "pacing_items"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pacing_guide_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("pacing_guides.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    grading_period_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("grading_periods.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    subject_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("subjects.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    week_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    day_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    instructional_date: Mapped[date | None] = mapped_column(Date(), nullable=True)
    title: Mapped[str] = mapped_column(String(160), nullable=False)
    instructional_focus: Mapped[str | None] = mapped_column(Text(), nullable=True)
    objectives: Mapped[str | None] = mapped_column(Text(), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text(), nullable=True)
    sort_order: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    pacing_guide: Mapped["TeacherAssistPacingGuide"] = relationship(
        "TeacherAssistPacingGuide", back_populates="items"
    )
    grading_period: Mapped["TeacherAssistGradingPeriod | None"] = relationship(
        "TeacherAssistGradingPeriod"
    )
    subject: Mapped["TeacherAssistSubject | None"] = relationship("TeacherAssistSubject")
    standard_links: Mapped[list["TeacherAssistPacingItemStandard"]] = relationship(
        "TeacherAssistPacingItemStandard",
        back_populates="pacing_item",
        cascade="all, delete-orphan",
    )
    resource_links: Mapped[list["TeacherAssistPacingItemResource"]] = relationship(
        "TeacherAssistPacingItemResource",
        back_populates="pacing_item",
        cascade="all, delete-orphan",
    )


from typing import TYPE_CHECKING  # noqa: E402

if TYPE_CHECKING:
    from oziebot_api.models.teacher_assist_grading_period import TeacherAssistGradingPeriod
    from oziebot_api.models.teacher_assist_pacing_guide import TeacherAssistPacingGuide
    from oziebot_api.models.teacher_assist_pacing_item_resource import (
        TeacherAssistPacingItemResource,
    )
    from oziebot_api.models.teacher_assist_pacing_item_standard import (
        TeacherAssistPacingItemStandard,
    )
    from oziebot_api.models.teacher_assist_subject import TeacherAssistSubject
