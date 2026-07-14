from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Uuid, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base


class TeacherAssistPlanningInputDraftPacingItem(Base):
    __tablename__ = "planning_input_draft_pacing_items"
    __table_args__ = (
        UniqueConstraint(
            "planning_input_draft_id",
            "pacing_item_id",
            name="uq_planning_draft_pacing_item",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    planning_input_draft_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("planning_input_drafts.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    pacing_item_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("pacing_items.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    planning_input_draft: Mapped["TeacherAssistPlanningInputDraft"] = relationship(
        "TeacherAssistPlanningInputDraft", back_populates="pacing_item_links"
    )
    pacing_item: Mapped["TeacherAssistPacingItem"] = relationship("TeacherAssistPacingItem")


from typing import TYPE_CHECKING  # noqa: E402

if TYPE_CHECKING:
    from oziebot_api.models.teacher_assist_pacing_item import TeacherAssistPacingItem
    from oziebot_api.models.teacher_assist_planning_input_draft import (
        TeacherAssistPlanningInputDraft,
    )
