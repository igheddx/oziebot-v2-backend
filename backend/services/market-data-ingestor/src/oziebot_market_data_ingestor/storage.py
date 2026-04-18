from __future__ import annotations

import uuid

from sqlalchemy import text
from sqlalchemy.engine import Engine

from oziebot_domain.market_data import NormalizedBestBidAsk, NormalizedCandle, NormalizedTrade


def _uid() -> str:
    return uuid.uuid4().hex


class MarketDataStore:
    """Persists selected historical data into PostgreSQL tables."""

    def __init__(self, engine: Engine):
        self._engine = engine

    def insert_candle(self, c: NormalizedCandle) -> None:
        stmt = text(
            """
            INSERT INTO market_data_candles (
              id, source, product_id, granularity_sec, bucket_start,
              open, high, low, close, volume,
              event_time, ingest_time
            ) VALUES (
              :id, :source, :product_id, :granularity_sec, :bucket_start,
              :open, :high, :low, :close, :volume,
              :event_time, :ingest_time
            )
            ON CONFLICT (source, product_id, granularity_sec, bucket_start)
            DO UPDATE SET
              open = excluded.open,
              high = excluded.high,
              low = excluded.low,
              close = excluded.close,
              volume = excluded.volume,
              event_time = excluded.event_time,
              ingest_time = excluded.ingest_time
            """
        )
        with self._engine.begin() as conn:
            conn.execute(stmt, {"id": _uid(), **c.model_dump(mode="json")})

    def insert_trade_snapshot(self, t: NormalizedTrade) -> None:
        stmt = text(
            """
            INSERT INTO market_data_trade_snapshots (
              id, source, product_id, trade_id, side, price, size, event_time, ingest_time
            ) VALUES (
              :id, :source, :product_id, :trade_id, :side, :price, :size, :event_time, :ingest_time
            )
            ON CONFLICT (source, product_id, trade_id) DO NOTHING
            """
        )
        with self._engine.begin() as conn:
            conn.execute(stmt, {"id": _uid(), **t.model_dump(mode="json")})

    def insert_bbo_snapshot(self, b: NormalizedBestBidAsk) -> None:
        stmt = text(
            """
            INSERT INTO market_data_bbo_snapshots (
              id, source, product_id,
              best_bid_price, best_bid_size,
              best_ask_price, best_ask_size,
              event_time, ingest_time
            ) VALUES (
              :id, :source, :product_id,
              :best_bid_price, :best_bid_size,
              :best_ask_price, :best_ask_size,
              :event_time, :ingest_time
            )
            """
        )
        with self._engine.begin() as conn:
            conn.execute(stmt, {"id": _uid(), **b.model_dump(mode="json")})
