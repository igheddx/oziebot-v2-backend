from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Uuid, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base


class TeacherAssistPacingItemStandard(Base):
    __tablename__ = "pacing_item_standards"
    __table_args__ = (UniqueConstraint("pacing_item_id", "standard_id", name="uq_pacing_item_standard"),)

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pacing_item_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("pacing_items.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    standard_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("standards.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    pacing_item: Mapped["TeacherAssistPacingItem"] = relationship(
        "TeacherAssistPacingItem", back_populates="standard_links"
    )
    standard: Mapped["TeacherAssistStandard"] = relationship("TeacherAssistStandard")


from typing import TYPE_CHECKING  # noqa: E402

if TYPE_CHECKING:
    from oziebot_api.models.teacher_assist_pacing_item import TeacherAssistPacingItem
    from oziebot_api.models.teacher_assist_standard import TeacherAssistStandard
