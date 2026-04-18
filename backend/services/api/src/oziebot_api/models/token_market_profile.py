from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, ForeignKey, JSON, Numeric, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from oziebot_api.db.base import Base


class TokenMarketProfile(Base):
    __tablename__ = "token_market_profile"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    token_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("platform_token_allowlist.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
        index=True,
    )
    liquidity_score: Mapped[float] = mapped_column(Numeric(10, 4), nullable=False, default=0)
    spread_score: Mapped[float] = mapped_column(Numeric(10, 4), nullable=False, default=0)
    volatility_score: Mapped[float] = mapped_column(Numeric(10, 4), nullable=False, default=0)
    trend_score: Mapped[float] = mapped_column(Numeric(10, 4), nullable=False, default=0)
    reversion_score: Mapped[float] = mapped_column(Numeric(10, 4), nullable=False, default=0)
    slippage_score: Mapped[float] = mapped_column(Numeric(10, 4), nullable=False, default=0)
    avg_daily_volume_usd: Mapped[float] = mapped_column(Numeric(28, 10), nullable=False, default=0)
    avg_spread_pct: Mapped[float] = mapped_column(Numeric(18, 10), nullable=False, default=0)
    avg_intraday_volatility_pct: Mapped[float] = mapped_column(
        Numeric(18, 10), nullable=False, default=0
    )
    last_computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    raw_metrics_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
