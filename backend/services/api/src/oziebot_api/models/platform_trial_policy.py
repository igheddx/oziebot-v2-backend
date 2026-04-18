import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, JSON, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from oziebot_api.db.base import Base


class PlatformTrialPolicy(Base):
    """Singleton row: free trial policy (enforce one row in application code)."""

    __tablename__ = "platform_trial_policy"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    is_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    trial_duration_days: Mapped[int] = mapped_column(Integer, nullable=False, default=30)
    max_trials_per_tenant: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    grace_period_days: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    metadata_: Mapped[dict[str, Any] | None] = mapped_column("metadata", JSON, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_by_user_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
