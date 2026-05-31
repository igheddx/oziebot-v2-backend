from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, Float, ForeignKey, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base


class TeacherAssistV2GradebookRecordRevision(Base):
    __tablename__ = "teacher_assist_v2_gradebook_record_revisions"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False, index=True
    )
    gradebook_record_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("teacher_assist_v2_gradebook_records.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    assignment_grade_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("teacher_assist_v2_assignment_grades.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    previous_score: Mapped[float] = mapped_column(Float(), nullable=False)
    previous_max_score: Mapped[float] = mapped_column(Float(), nullable=False)
    previous_percentage: Mapped[float] = mapped_column(Float(), nullable=False)
    previous_teacher_comment: Mapped[str] = mapped_column(Text(), nullable=False)
    new_score: Mapped[float] = mapped_column(Float(), nullable=False)
    new_max_score: Mapped[float] = mapped_column(Float(), nullable=False)
    new_percentage: Mapped[float] = mapped_column(Float(), nullable=False)
    new_teacher_comment: Mapped[str] = mapped_column(Text(), nullable=False)
    revised_by_user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    revised_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    gradebook_record: Mapped["TeacherAssistV2GradebookRecord"] = relationship(
        "TeacherAssistV2GradebookRecord",
        back_populates="revisions",
    )


if TYPE_CHECKING:
    from oziebot_api.models.teacher_assist_v2_gradebook_record import TeacherAssistV2GradebookRecord
