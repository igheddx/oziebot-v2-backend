from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import JSON, DateTime, ForeignKey, Integer, String, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base


class TeacherAssistAssignmentPrintPage(Base):
    __tablename__ = "assignment_print_pages"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    packet_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("assignment_print_packets.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    assignment_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("assignments.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    student_number: Mapped[int] = mapped_column(Integer, nullable=False)
    page_number: Mapped[int] = mapped_column(Integer, nullable=False)
    qr_payload_json: Mapped[dict[str, Any]] = mapped_column(JSON(), nullable=False)
    qr_token: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    packet: Mapped["TeacherAssistAssignmentPrintPacket"] = relationship(
        "TeacherAssistAssignmentPrintPacket", back_populates="pages"
    )
    assignment: Mapped["TeacherAssistAssignment"] = relationship("TeacherAssistAssignment")
    student_work_submissions: Mapped[list["TeacherAssistStudentWorkSubmission"]] = relationship(
        "TeacherAssistStudentWorkSubmission",
        back_populates="assignment_print_page",
    )


from typing import TYPE_CHECKING  # noqa: E402

if TYPE_CHECKING:
    from oziebot_api.models.teacher_assist_assignment import TeacherAssistAssignment
    from oziebot_api.models.teacher_assist_assignment_print_packet import (
        TeacherAssistAssignmentPrintPacket,
    )
    from oziebot_api.models.teacher_assist_student_work_submission import (
        TeacherAssistStudentWorkSubmission,
    )
