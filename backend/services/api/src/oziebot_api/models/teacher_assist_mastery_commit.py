from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base


class TeacherAssistMasteryCommit(Base):
    __tablename__ = "teacher_assist_mastery_commit_history"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    owner_user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    mastery_evaluation_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("teacher_assist_mastery_evaluations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    mastery_matrix_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("teacher_assist_mastery_matrices.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    student_number: Mapped[int] = mapped_column(nullable=False, index=True)
    standard_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("standards.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    commit_type: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    commit_status: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    previous_mastery_level: Mapped[str | None] = mapped_column(String(32), nullable=True)
    new_mastery_level: Mapped[str] = mapped_column(String(32), nullable=False)
    confidence_level: Mapped[str | None] = mapped_column(String(32), nullable=True)
    evidence_source_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    evidence_source_id: Mapped[uuid.UUID | None] = mapped_column(Uuid(as_uuid=True), nullable=True)
    teacher_notes: Mapped[str | None] = mapped_column(Text(), nullable=True)
    commit_reason: Mapped[str | None] = mapped_column(Text(), nullable=True)
    supersedes_commit_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("teacher_assist_mastery_commit_history.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    reversed_by_commit_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("teacher_assist_mastery_commit_history.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    reversed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    reversed_by_user_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    tenant: Mapped["Tenant"] = relationship("Tenant")
    owner_user: Mapped["User"] = relationship("User", foreign_keys=[owner_user_id])
    mastery_evaluation: Mapped["TeacherAssistMasteryEvaluation"] = relationship(
        "TeacherAssistMasteryEvaluation",
        back_populates="commits",
        foreign_keys=[mastery_evaluation_id],
    )
    mastery_matrix: Mapped["TeacherAssistMasteryMatrix"] = relationship("TeacherAssistMasteryMatrix")
    standard: Mapped["TeacherAssistStandard"] = relationship("TeacherAssistStandard")


from typing import TYPE_CHECKING  # noqa: E402

if TYPE_CHECKING:
    from oziebot_api.models.teacher_assist_mastery_evaluation import TeacherAssistMasteryEvaluation
    from oziebot_api.models.teacher_assist_mastery_matrix import TeacherAssistMasteryMatrix
    from oziebot_api.models.teacher_assist_standard import TeacherAssistStandard
    from oziebot_api.models.tenant import Tenant
    from oziebot_api.models.user import User
