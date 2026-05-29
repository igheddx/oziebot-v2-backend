from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, Float, ForeignKey, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from oziebot_api.db.base import Base


class TeacherAssistReteachEffectivenessRecord(Base):
    __tablename__ = "teacher_assist_reteach_effectiveness_records"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
    )
    owner_user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )
    reteach_plan_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("teacher_assist_reteach_plans.id", ondelete="CASCADE"),
        nullable=False,
    )
    before_mastery_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    after_mastery_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    improvement_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    teacher_reflection: Mapped[str | None] = mapped_column(Text(), nullable=True)
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
