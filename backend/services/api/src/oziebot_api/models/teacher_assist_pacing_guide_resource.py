from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base


class TeacherAssistPacingGuideResource(Base):
    __tablename__ = "pacing_guide_resources"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    period_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("pacing_guide_periods.id", ondelete="CASCADE"), nullable=False, index=True
    )
    catalog_resource_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("education_curriculum_resources.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    resource_library_item_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("resource_library_items.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )
    is_primary: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default="false")
    notes: Mapped[str | None] = mapped_column(Text(), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    period: Mapped["TeacherAssistPacingGuidePeriod"] = relationship(
        "TeacherAssistPacingGuidePeriod", back_populates="resources"
    )
    catalog_resource: Mapped["EducationCurriculumResource | None"] = relationship("EducationCurriculumResource")
    resource_library_item: Mapped["TeacherAssistResourceLibraryItem | None"] = relationship(
        "TeacherAssistResourceLibraryItem"
    )


from typing import TYPE_CHECKING  # noqa: E402

if TYPE_CHECKING:
    from oziebot_api.models.education_catalog import EducationCurriculumResource
    from oziebot_api.models.teacher_assist_pacing_guide_period import TeacherAssistPacingGuidePeriod
    from oziebot_api.models.teacher_assist_resource_library_item import TeacherAssistResourceLibraryItem
