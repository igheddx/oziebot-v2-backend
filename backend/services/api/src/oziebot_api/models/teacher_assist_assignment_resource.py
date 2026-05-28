from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Uuid, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base


class TeacherAssistAssignmentResource(Base):
    __tablename__ = "assignment_resources"
    __table_args__ = (
        UniqueConstraint(
            "assignment_id",
            "resource_library_item_id",
            name="uq_assignment_resource",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    assignment_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("assignments.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    resource_library_item_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("resource_library_items.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    assignment: Mapped["TeacherAssistAssignment"] = relationship(
        "TeacherAssistAssignment", back_populates="resource_links"
    )
    resource_library_item: Mapped["TeacherAssistResourceLibraryItem"] = relationship(
        "TeacherAssistResourceLibraryItem"
    )


from typing import TYPE_CHECKING  # noqa: E402

if TYPE_CHECKING:
    from oziebot_api.models.teacher_assist_assignment import TeacherAssistAssignment
    from oziebot_api.models.teacher_assist_resource_library_item import (
        TeacherAssistResourceLibraryItem,
    )
