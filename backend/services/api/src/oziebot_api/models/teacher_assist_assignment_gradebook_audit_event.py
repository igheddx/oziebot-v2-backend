from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import JSON, DateTime, ForeignKey, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base


class TeacherAssistAssignmentGradebookAuditEvent(Base):
    __tablename__ = "teacher_assist_assignment_gradebook_audit_events"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    teacher_user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    grade_record_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("teacher_assist_assignment_grade_records.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    gradebook_commit_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("teacher_assist_assignment_gradebook_commits.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    assignment_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("assignments.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    student_number: Mapped[int] = mapped_column(nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    summary_text: Mapped[str] = mapped_column(Text(), nullable=False)
    details_json: Mapped[dict[str, Any] | None] = mapped_column(JSON(), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    tenant: Mapped["Tenant"] = relationship("Tenant")
    teacher_user: Mapped["User"] = relationship("User")
    grade_record: Mapped["TeacherAssistAssignmentGradeRecord | None"] = relationship(
        "TeacherAssistAssignmentGradeRecord"
    )
    gradebook_commit: Mapped["TeacherAssistAssignmentGradebookCommit | None"] = relationship(
        "TeacherAssistAssignmentGradebookCommit"
    )
    assignment: Mapped["TeacherAssistAssignment"] = relationship("TeacherAssistAssignment")


from typing import TYPE_CHECKING  # noqa: E402

if TYPE_CHECKING:
    from oziebot_api.models.teacher_assist_assignment import TeacherAssistAssignment
    from oziebot_api.models.teacher_assist_assignment_grade_record import (
        TeacherAssistAssignmentGradeRecord,
    )
    from oziebot_api.models.teacher_assist_assignment_gradebook_commit import (
        TeacherAssistAssignmentGradebookCommit,
    )
    from oziebot_api.models.tenant import Tenant
    from oziebot_api.models.user import User
