from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base


class TeacherAssistPacingGuideObjective(Base):
    __tablename__ = "pacing_guide_objectives"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    period_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("pacing_guide_periods.id", ondelete="CASCADE"), nullable=False, index=True
    )
    objective_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("education_objectives.id", ondelete="CASCADE"), nullable=False, index=True
    )
    is_required: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true")
    notes: Mapped[str | None] = mapped_column(Text(), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    period: Mapped["TeacherAssistPacingGuidePeriod"] = relationship(
        "TeacherAssistPacingGuidePeriod", back_populates="objectives"
    )
    objective: Mapped["EducationObjective"] = relationship("EducationObjective")


from typing import TYPE_CHECKING  # noqa: E402

if TYPE_CHECKING:
    from oziebot_api.models.education_catalog import EducationObjective
    from oziebot_api.models.teacher_assist_pacing_guide_period import TeacherAssistPacingGuidePeriod
