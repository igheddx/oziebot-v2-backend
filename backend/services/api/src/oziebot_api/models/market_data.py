from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, Integer, Numeric, String, Uuid, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from oziebot_api.db.base import Base


class MarketDataCandle(Base):
    __tablename__ = "market_data_candles"
    __table_args__ = (
        UniqueConstraint(
            "source",
            "product_id",
            "granularity_sec",
            "bucket_start",
            name="uq_market_data_candles_bucket",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    product_id: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    granularity_sec: Mapped[int] = mapped_column(Integer, nullable=False)
    bucket_start: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )

    open: Mapped[float] = mapped_column(Numeric(28, 10), nullable=False)
    high: Mapped[float] = mapped_column(Numeric(28, 10), nullable=False)
    low: Mapped[float] = mapped_column(Numeric(28, 10), nullable=False)
    close: Mapped[float] = mapped_column(Numeric(28, 10), nullable=False)
    volume: Mapped[float] = mapped_column(Numeric(28, 10), nullable=False)

    event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ingest_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class MarketDataTradeSnapshot(Base):
    __tablename__ = "market_data_trade_snapshots"
    __table_args__ = (
        UniqueConstraint("source", "product_id", "trade_id", name="uq_market_data_trade_snapshot"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    product_id: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    trade_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    side: Mapped[str] = mapped_column(String(8), nullable=False)
    price: Mapped[float] = mapped_column(Numeric(28, 10), nullable=False)
    size: Mapped[float] = mapped_column(Numeric(28, 10), nullable=False)
    event_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
    ingest_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class MarketDataBboSnapshot(Base):
    __tablename__ = "market_data_bbo_snapshots"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    product_id: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    best_bid_price: Mapped[float] = mapped_column(Numeric(28, 10), nullable=False)
    best_bid_size: Mapped[float] = mapped_column(Numeric(28, 10), nullable=False)
    best_ask_price: Mapped[float] = mapped_column(Numeric(28, 10), nullable=False)
    best_ask_size: Mapped[float] = mapped_column(Numeric(28, 10), nullable=False)
    event_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
    ingest_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
