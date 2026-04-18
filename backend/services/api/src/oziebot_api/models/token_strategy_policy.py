from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Numeric, String, Text, Uuid, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from oziebot_api.db.base import Base


class TokenStrategyPolicy(Base):
    __tablename__ = "token_strategy_policy"
    __table_args__ = (
        UniqueConstraint("token_id", "strategy_id", name="uq_token_strategy_policy"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    token_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("platform_token_allowlist.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    strategy_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    admin_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    suitability_score: Mapped[float] = mapped_column(Numeric(10, 4), nullable=False, default=0)
    recommendation_status: Mapped[str] = mapped_column(String(32), nullable=False, default="allowed")
    recommendation_reason: Mapped[str | None] = mapped_column(Text(), nullable=True)
    recommendation_status_override: Mapped[str | None] = mapped_column(String(32), nullable=True)
    recommendation_reason_override: Mapped[str | None] = mapped_column(Text(), nullable=True)
    max_position_pct_override: Mapped[float | None] = mapped_column(Numeric(12, 6), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text(), nullable=True)
    computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
