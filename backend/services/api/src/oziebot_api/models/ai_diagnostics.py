from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import Boolean, DateTime, ForeignKey, JSON, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base


class DiagnosticSnapshot(Base):
    __tablename__ = "diagnostic_snapshots"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tenants.id", ondelete="SET NULL"), nullable=True, index=True
    )
    generated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
    trading_mode: Mapped[str | None] = mapped_column(String(16), nullable=True, index=True)
    strategy_filter: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    token_filter: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    days_filter: Mapped[int] = mapped_column(nullable=False, default=7)
    raw_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )


class AiDiagnosticReview(Base):
    __tablename__ = "ai_diagnostic_reviews"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tenants.id", ondelete="SET NULL"), nullable=True, index=True
    )
    snapshot_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("diagnostic_snapshots.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    status: Mapped[str] = mapped_column(String(32), nullable=False, index=True, default="queued")
    overall_health: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    confidence_score: Mapped[float | None] = mapped_column(nullable=True)
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    model_name: Mapped[str] = mapped_column(String(128), nullable=False, default="rule-based")
    prompt_version: Mapped[str] = mapped_column(
        String(64), nullable=False, default="ai-diagnostics-v1"
    )
    created_by_admin_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    snapshot: Mapped[DiagnosticSnapshot] = relationship("DiagnosticSnapshot")
    findings: Mapped[list["AiDiagnosticFinding"]] = relationship(
        "AiDiagnosticFinding", back_populates="review", cascade="all, delete-orphan"
    )
    created_by_admin: Mapped["User"] = relationship("User")


class AiDiagnosticFinding(Base):
    __tablename__ = "ai_diagnostic_findings"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    review_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("ai_diagnostic_reviews.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    severity: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    category: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    strategy: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    token: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    finding_title: Mapped[str] = mapped_column(String(255), nullable=False)
    finding_detail: Mapped[str] = mapped_column(Text, nullable=False)
    evidence_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    recommendation: Mapped[str] = mapped_column(Text, nullable=False)
    risk_if_ignored: Mapped[str | None] = mapped_column(Text, nullable=True)
    confidence_score: Mapped[float | None] = mapped_column(nullable=True)
    automation_eligibility: Mapped[str] = mapped_column(
        String(64), nullable=False, default="not_eligible"
    )
    status: Mapped[str] = mapped_column(String(32), nullable=False, index=True, default="new")
    future_config_change_candidate: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False
    )
    proposed_config_change_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    approval_required: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    eligible_for_auto_tune: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    rollback_plan: Mapped[str | None] = mapped_column(Text, nullable=True)
    expected_impact: Mapped[str | None] = mapped_column(Text, nullable=True)
    risk_level: Mapped[str | None] = mapped_column(String(32), nullable=True)
    affected_strategy: Mapped[str | None] = mapped_column(String(128), nullable=True)
    affected_token: Mapped[str | None] = mapped_column(String(32), nullable=True)
    parameter_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    current_value_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    proposed_value_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    review: Mapped[AiDiagnosticReview] = relationship(
        "AiDiagnosticReview", back_populates="findings"
    )
    audits: Mapped[list["AiDiagnosticRecommendationAudit"]] = relationship(
        "AiDiagnosticRecommendationAudit",
        back_populates="finding",
        cascade="all, delete-orphan",
    )


class AiDiagnosticRecommendationAudit(Base):
    __tablename__ = "ai_diagnostic_recommendation_audit"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    finding_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("ai_diagnostic_findings.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    action: Mapped[str] = mapped_column(String(64), nullable=False)
    previous_status: Mapped[str | None] = mapped_column(String(32), nullable=True)
    new_status: Mapped[str] = mapped_column(String(32), nullable=False)
    admin_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    note: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )

    finding: Mapped[AiDiagnosticFinding] = relationship(
        "AiDiagnosticFinding", back_populates="audits"
    )
    admin: Mapped["User"] = relationship("User")


from typing import TYPE_CHECKING  # noqa: E402

if TYPE_CHECKING:
    from oziebot_api.models.user import User
