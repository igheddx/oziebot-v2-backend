from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import Field

from oziebot_domain.types import OziebotModel


class MarketDataSource(StrEnum):
    COINBASE = "coinbase"


class NormalizedTrade(OziebotModel):
    source: MarketDataSource = MarketDataSource.COINBASE
    product_id: str = Field(min_length=3, max_length=32)
    trade_id: str = Field(min_length=1, max_length=64)
    side: str = Field(pattern="^(buy|sell)$")
    price: Decimal
    size: Decimal
    event_time: datetime
    ingest_time: datetime


class NormalizedBestBidAsk(OziebotModel):
    source: MarketDataSource = MarketDataSource.COINBASE
    product_id: str = Field(min_length=3, max_length=32)
    best_bid_price: Decimal
    best_bid_size: Decimal
    best_ask_price: Decimal
    best_ask_size: Decimal
    event_time: datetime
    ingest_time: datetime


class NormalizedCandle(OziebotModel):
    source: MarketDataSource = MarketDataSource.COINBASE
    product_id: str = Field(min_length=3, max_length=32)
    granularity_sec: int = Field(gt=0)
    bucket_start: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    event_time: datetime
    ingest_time: datetime


class NormalizedOrderBookTop(OziebotModel):
    source: MarketDataSource = MarketDataSource.COINBASE
    product_id: str = Field(min_length=3, max_length=32)
    depth: int = Field(default=10, gt=0)
    bids: list[tuple[Decimal, Decimal]]
    asks: list[tuple[Decimal, Decimal]]
    event_time: datetime
    ingest_time: datetime
