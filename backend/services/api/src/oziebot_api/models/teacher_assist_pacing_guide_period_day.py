from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base


class TeacherAssistPacingGuidePeriodDay(Base):
    __tablename__ = "pacing_guide_period_days"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    period_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("pacing_guide_periods.id", ondelete="CASCADE"), nullable=False, index=True
    )
    day_label: Mapped[str] = mapped_column(String(32), nullable=False)
    sequence_number: Mapped[int] = mapped_column(Integer, nullable=False)
    daily_topic: Mapped[str] = mapped_column(Text(), nullable=False)
    objective_focus: Mapped[str | None] = mapped_column(Text(), nullable=True)
    teacher_notes: Mapped[str | None] = mapped_column(Text(), nullable=True)
    materials_needed: Mapped[str | None] = mapped_column(Text(), nullable=True)
    assessment_check: Mapped[str | None] = mapped_column(Text(), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    period: Mapped["TeacherAssistPacingGuidePeriod"] = relationship(
        "TeacherAssistPacingGuidePeriod", back_populates="days"
    )
    supporting_materials: Mapped[list["TeacherAssistPacingGuideSupportingMaterial"]] = relationship(
        "TeacherAssistPacingGuideSupportingMaterial",
        back_populates="period_day",
    )


from typing import TYPE_CHECKING  # noqa: E402

if TYPE_CHECKING:
    from oziebot_api.models.teacher_assist_pacing_guide_period import TeacherAssistPacingGuidePeriod
    from oziebot_api.models.teacher_assist_pacing_guide_supporting_material import (
        TeacherAssistPacingGuideSupportingMaterial,
    )
