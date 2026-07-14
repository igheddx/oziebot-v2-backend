from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import JSON, DateTime, ForeignKey, Integer, String, Text, Uuid, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base


class TeacherAssistReteachPlanVersion(Base):
    __tablename__ = "teacher_assist_reteach_plan_versions"
    __table_args__ = (
        UniqueConstraint(
            "reteach_plan_id", "version_number", name="uq_reteach_plan_version_number"
        ),
    )

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
    reteach_plan_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("teacher_assist_reteach_plans.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    version_number: Mapped[int] = mapped_column(Integer, nullable=False)
    version_source: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    content_json: Mapped[dict[str, Any]] = mapped_column(JSON(), nullable=False)
    prompt_context_json: Mapped[dict[str, Any] | None] = mapped_column(JSON(), nullable=True)
    provider_name: Mapped[str | None] = mapped_column(String(64), nullable=True)
    provider_model: Mapped[str | None] = mapped_column(String(128), nullable=True)
    prompt_version: Mapped[str | None] = mapped_column(String(64), nullable=True)
    ai_usage_event_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("teacher_assist_ai_usage_events.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    created_by_user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    change_reason: Mapped[str | None] = mapped_column(Text(), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    tenant: Mapped["Tenant"] = relationship("Tenant")
    owner_user: Mapped["User"] = relationship("User", foreign_keys=[owner_user_id])
    reteach_plan: Mapped["TeacherAssistReteachPlan"] = relationship(
        "TeacherAssistReteachPlan",
        back_populates="versions",
        foreign_keys=[reteach_plan_id],
    )
    created_by_user: Mapped["User"] = relationship("User", foreign_keys=[created_by_user_id])


from typing import TYPE_CHECKING  # noqa: E402

if TYPE_CHECKING:
    from oziebot_api.models.teacher_assist_reteach_plan import TeacherAssistReteachPlan
    from oziebot_api.models.tenant import Tenant
    from oziebot_api.models.user import User
