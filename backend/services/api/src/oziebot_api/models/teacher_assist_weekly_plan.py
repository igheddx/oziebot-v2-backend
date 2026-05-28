from __future__ import annotations

import uuid
from datetime import date, datetime
from typing import Any

from sqlalchemy import JSON, Boolean, Date, DateTime, ForeignKey, Integer, String, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base


class TeacherAssistWeeklyPlan(Base):
    __tablename__ = "weekly_plans"

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
    planning_input_draft_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("planning_input_drafts.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    workflow_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("teacher_assist_workflows.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    planning_scope: Mapped[str] = mapped_column(String(32), nullable=False, server_default="weekly")
    module_title: Mapped[str | None] = mapped_column(String(160), nullable=True)
    start_date: Mapped[date | None] = mapped_column(Date(), nullable=True)
    end_date: Mapped[date | None] = mapped_column(Date(), nullable=True)
    estimated_weeks: Mapped[int | None] = mapped_column(Integer, nullable=True)
    instructional_days_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    owner_user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    source_plan_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("weekly_plans.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    derived_from_plan_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("weekly_plans.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    is_template: Mapped[bool] = mapped_column(Boolean(), nullable=False, server_default="false")
    visibility_scope: Mapped[str] = mapped_column(String(32), nullable=False, server_default="private")
    reuse_status: Mapped[str] = mapped_column(String(32), nullable=False, server_default="active")
    school_year_origin_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("school_years.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    title: Mapped[str] = mapped_column(String(160), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    content_json: Mapped[dict[str, Any]] = mapped_column(JSON(), nullable=False)
    source_context_json: Mapped[dict[str, Any]] = mapped_column(JSON(), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    tenant: Mapped["Tenant"] = relationship("Tenant")
    user: Mapped["User"] = relationship("User", foreign_keys=[user_id])
    owner_user: Mapped["User"] = relationship("User", foreign_keys=[owner_user_id])
    planning_input_draft: Mapped["TeacherAssistPlanningInputDraft"] = relationship(
        "TeacherAssistPlanningInputDraft"
    )
    workflow: Mapped["TeacherAssistWorkflow | None"] = relationship(
        "TeacherAssistWorkflow", back_populates="weekly_plans"
    )
    versions: Mapped[list["TeacherAssistWeeklyPlanVersion"]] = relationship(
        "TeacherAssistWeeklyPlanVersion",
        back_populates="weekly_plan",
        cascade="all, delete-orphan",
        order_by="TeacherAssistWeeklyPlanVersion.version_number.asc()",
    )
    source_plan: Mapped["TeacherAssistWeeklyPlan | None"] = relationship(
        "TeacherAssistWeeklyPlan",
        remote_side=[id],
        foreign_keys=[source_plan_id],
        post_update=True,
    )
    derived_from_plan: Mapped["TeacherAssistWeeklyPlan | None"] = relationship(
        "TeacherAssistWeeklyPlan",
        remote_side=[id],
        foreign_keys=[derived_from_plan_id],
        post_update=True,
    )


from typing import TYPE_CHECKING  # noqa: E402

if TYPE_CHECKING:
    from oziebot_api.models.teacher_assist_planning_input_draft import TeacherAssistPlanningInputDraft
    from oziebot_api.models.teacher_assist_weekly_plan_version import TeacherAssistWeeklyPlanVersion
    from oziebot_api.models.teacher_assist_workflow import TeacherAssistWorkflow
    from oziebot_api.models.tenant import Tenant
    from oziebot_api.models.user import User
