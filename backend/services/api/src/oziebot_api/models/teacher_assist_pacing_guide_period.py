from __future__ import annotations

import uuid
from datetime import date, datetime

from sqlalchemy import Date, DateTime, ForeignKey, Integer, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base


class TeacherAssistPacingGuidePeriod(Base):
    __tablename__ = "pacing_guide_periods"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pacing_guide_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("pacing_guides.id", ondelete="CASCADE"), nullable=False, index=True
    )
    period_type: Mapped[str] = mapped_column(String(32), nullable=False)
    title: Mapped[str] = mapped_column(String(160), nullable=False)
    description: Mapped[str | None] = mapped_column(Text(), nullable=True)
    sequence_number: Mapped[int] = mapped_column(Integer, nullable=False)
    start_date: Mapped[date | None] = mapped_column(Date(), nullable=True)
    end_date: Mapped[date | None] = mapped_column(Date(), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    pacing_guide: Mapped["TeacherAssistPacingGuide"] = relationship(
        "TeacherAssistPacingGuide", back_populates="periods"
    )
    objectives: Mapped[list["TeacherAssistPacingGuideObjective"]] = relationship(
        "TeacherAssistPacingGuideObjective",
        back_populates="period",
        cascade="all, delete-orphan",
    )
    resources: Mapped[list["TeacherAssistPacingGuideResource"]] = relationship(
        "TeacherAssistPacingGuideResource",
        back_populates="period",
        cascade="all, delete-orphan",
    )


from typing import TYPE_CHECKING  # noqa: E402

if TYPE_CHECKING:
    from oziebot_api.models.teacher_assist_pacing_guide import TeacherAssistPacingGuide
    from oziebot_api.models.teacher_assist_pacing_guide_objective import TeacherAssistPacingGuideObjective
    from oziebot_api.models.teacher_assist_pacing_guide_resource import TeacherAssistPacingGuideResource
