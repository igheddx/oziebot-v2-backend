from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Uuid, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base


class TeacherAssistClassSubject(Base):
    __tablename__ = "class_subjects"
    __table_args__ = (UniqueConstraint("class_id", "subject_id", name="uq_class_subject_pair"),)

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
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
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    teacher_class: Mapped["TeacherAssistClass"] = relationship(
        "TeacherAssistClass", back_populates="class_subjects"
    )
    subject: Mapped["TeacherAssistSubject"] = relationship(
        "TeacherAssistSubject", back_populates="class_subjects"
    )


from typing import TYPE_CHECKING  # noqa: E402

if TYPE_CHECKING:
    from oziebot_api.models.teacher_assist_class import TeacherAssistClass
    from oziebot_api.models.teacher_assist_subject import TeacherAssistSubject
