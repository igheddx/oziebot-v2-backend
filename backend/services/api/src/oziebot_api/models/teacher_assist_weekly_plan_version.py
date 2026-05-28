from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import JSON, DateTime, ForeignKey, Integer, Text, Uuid, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base


class TeacherAssistWeeklyPlanVersion(Base):
    __tablename__ = "weekly_plan_versions"
    __table_args__ = (
        UniqueConstraint("weekly_plan_id", "version_number", name="uq_weekly_plan_version_number"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    weekly_plan_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("weekly_plans.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    version_number: Mapped[int] = mapped_column(Integer, nullable=False)
    content_json: Mapped[dict[str, Any]] = mapped_column(JSON(), nullable=False)
    source_context_json: Mapped[dict[str, Any]] = mapped_column(JSON(), nullable=False)
    created_by_user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    change_reason: Mapped[str | None] = mapped_column(Text(), nullable=True)

    weekly_plan: Mapped["TeacherAssistWeeklyPlan"] = relationship(
        "TeacherAssistWeeklyPlan", back_populates="versions"
    )
    created_by_user: Mapped["User"] = relationship("User")


from typing import TYPE_CHECKING  # noqa: E402

if TYPE_CHECKING:
    from oziebot_api.models.teacher_assist_weekly_plan import TeacherAssistWeeklyPlan
    from oziebot_api.models.user import User
