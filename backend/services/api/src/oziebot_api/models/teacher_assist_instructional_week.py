from __future__ import annotations

import uuid
from datetime import date, datetime

from sqlalchemy import Boolean, Date, DateTime, ForeignKey, Integer, JSON, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base


class TeacherAssistInstructionalWeek(Base):
    __tablename__ = "instructional_weeks"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False, index=True
    )
    school_year_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("school_years.id", ondelete="CASCADE"), nullable=False
    )
    grading_period_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("grading_periods.id", ondelete="SET NULL"), nullable=True
    )
    pacing_guide_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("pacing_guides.id", ondelete="CASCADE"), nullable=False
    )
    pacing_guide_period_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("pacing_guide_periods.id", ondelete="CASCADE"), nullable=False, index=True
    )
    week_number: Mapped[int] = mapped_column(Integer, nullable=False)
    title: Mapped[str] = mapped_column(String(160), nullable=False)
    description: Mapped[str | None] = mapped_column(Text(), nullable=True)
    start_date: Mapped[date | None] = mapped_column(Date(), nullable=True)
    end_date: Mapped[date | None] = mapped_column(Date(), nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    notes: Mapped[str | None] = mapped_column(Text(), nullable=True)
    created_by_user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True
    )
    updated_by_user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    objectives: Mapped[list["TeacherAssistInstructionalWeekObjective"]] = relationship(
        "TeacherAssistInstructionalWeekObjective",
        back_populates="instructional_week",
        cascade="all, delete-orphan",
    )
    snapshots: Mapped[list["TeacherAssistInstructionalWeekSnapshot"]] = relationship(
        "TeacherAssistInstructionalWeekSnapshot",
        back_populates="instructional_week",
        cascade="all, delete-orphan",
    )


class TeacherAssistInstructionalWeekObjective(Base):
    __tablename__ = "instructional_week_objectives"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    instructional_week_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("instructional_weeks.id", ondelete="CASCADE"), nullable=False, index=True
    )
    objective_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("education_objectives.id", ondelete="SET NULL"), nullable=True
    )
    objective_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    source_type: Mapped[str] = mapped_column(String(32), nullable=False)
    is_required: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true")
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, server_default="true")
    notes: Mapped[str | None] = mapped_column(Text(), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    instructional_week: Mapped["TeacherAssistInstructionalWeek"] = relationship(
        "TeacherAssistInstructionalWeek", back_populates="objectives"
    )


class TeacherAssistInstructionalWeekSnapshot(Base):
    __tablename__ = "instructional_week_snapshots"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    instructional_week_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("instructional_weeks.id", ondelete="CASCADE"), nullable=False, index=True
    )
    name: Mapped[str] = mapped_column(String(160), nullable=False)
    snapshot_data: Mapped[dict] = mapped_column(JSON(), nullable=False)
    created_by_user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    instructional_week: Mapped["TeacherAssistInstructionalWeek"] = relationship(
        "TeacherAssistInstructionalWeek", back_populates="snapshots"
    )


from typing import TYPE_CHECKING  # noqa: E402

if TYPE_CHECKING:
    pass
