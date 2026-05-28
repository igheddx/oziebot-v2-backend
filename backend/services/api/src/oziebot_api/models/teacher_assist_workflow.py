from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import JSON, DateTime, ForeignKey, Integer, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base


class TeacherAssistWorkflow(Base):
    __tablename__ = "teacher_assist_workflows"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    planning_input_draft_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("planning_input_drafts.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    workflow_type: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    input_snapshot_json: Mapped[dict[str, Any]] = mapped_column(JSON(), nullable=False)
    output_ref_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    output_ref_id: Mapped[uuid.UUID | None] = mapped_column(Uuid(as_uuid=True), nullable=True, index=True)
    error_message: Mapped[str | None] = mapped_column(Text(), nullable=True)
    progress_percent: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    leased_by_worker: Mapped[str | None] = mapped_column(String(128), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    max_retries: Mapped[int] = mapped_column(Integer, nullable=False, default=3, server_default="3")
    timeout_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    provider_name: Mapped[str | None] = mapped_column(String(64), nullable=True)
    provider_model: Mapped[str | None] = mapped_column(String(128), nullable=True)
    prompt_version: Mapped[str | None] = mapped_column(String(64), nullable=True)
    input_tokens_total: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    output_tokens_total: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    estimated_cost_cents_total: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0"
    )
    last_error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    execution_log_json: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON(), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    tenant: Mapped["Tenant"] = relationship("Tenant")
    user: Mapped["User"] = relationship("User")
    planning_input_draft: Mapped["TeacherAssistPlanningInputDraft | None"] = relationship(
        "TeacherAssistPlanningInputDraft"
    )
    steps: Mapped[list["TeacherAssistWorkflowStep"]] = relationship(
        "TeacherAssistWorkflowStep",
        back_populates="workflow",
        cascade="all, delete-orphan",
    )
    weekly_plans: Mapped[list["TeacherAssistWeeklyPlan"]] = relationship(
        "TeacherAssistWeeklyPlan",
        back_populates="workflow",
    )
    usage_events: Mapped[list["TeacherAssistAIUsageEvent"]] = relationship(
        "TeacherAssistAIUsageEvent",
        back_populates="workflow",
        cascade="all, delete-orphan",
    )


from typing import TYPE_CHECKING  # noqa: E402

if TYPE_CHECKING:
    from oziebot_api.models.teacher_assist_ai_usage_event import TeacherAssistAIUsageEvent
    from oziebot_api.models.teacher_assist_planning_input_draft import TeacherAssistPlanningInputDraft
    from oziebot_api.models.teacher_assist_weekly_plan import TeacherAssistWeeklyPlan
    from oziebot_api.models.teacher_assist_workflow_step import TeacherAssistWorkflowStep
    from oziebot_api.models.tenant import Tenant
    from oziebot_api.models.user import User
