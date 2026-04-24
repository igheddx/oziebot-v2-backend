from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import redis

from oziebot_market_data_ingestor.redis_cache import RedisMarketCache
from oziebot_domain.market_data import (
    NormalizedBestBidAsk,
    NormalizedCandle,
    NormalizedTrade,
)


class OomRedis:
    def setex(self, *args, **kwargs):  # noqa: ANN002, ANN003
        raise redis.exceptions.OutOfMemoryError(
            "command not allowed when used memory > 'maxmemory'"
        )

    def lpush(self, *args, **kwargs):  # noqa: ANN002, ANN003
        raise redis.exceptions.OutOfMemoryError(
            "command not allowed when used memory > 'maxmemory'"
        )

    def ltrim(self, *args, **kwargs):  # noqa: ANN002, ANN003
        raise redis.exceptions.OutOfMemoryError(
            "command not allowed when used memory > 'maxmemory'"
        )

    def expire(self, *args, **kwargs):  # noqa: ANN002, ANN003
        raise redis.exceptions.OutOfMemoryError(
            "command not allowed when used memory > 'maxmemory'"
        )

    def publish(self, *args, **kwargs):  # noqa: ANN002, ANN003
        raise redis.exceptions.OutOfMemoryError(
            "command not allowed when used memory > 'maxmemory'"
        )


def test_redis_market_cache_degrades_when_redis_is_full(caplog) -> None:
    cache = RedisMarketCache(OomRedis(), write_error_log_interval_seconds=0)
    now = datetime.now(UTC)

    cache.put_trade(
        NormalizedTrade(
            source="coinbase",
            product_id="BTC-USD",
            trade_id="t1",
            side="buy",
            price=Decimal("62000"),
            size=Decimal("0.01"),
            event_time=now,
            ingest_time=now,
        )
    )
    cache.put_bbo(
        NormalizedBestBidAsk(
            source="coinbase",
            product_id="BTC-USD",
            best_bid_price=Decimal("61999"),
            best_bid_size=Decimal("1"),
            best_ask_price=Decimal("62001"),
            best_ask_size=Decimal("1"),
            event_time=now,
            ingest_time=now,
        )
    )
    cache.put_candle(
        NormalizedCandle(
            source="coinbase",
            product_id="BTC-USD",
            granularity_sec=60,
            bucket_start=now,
            open=Decimal("61950"),
            high=Decimal("62050"),
            low=Decimal("61900"),
            close=Decimal("62000"),
            volume=Decimal("12"),
            event_time=now,
            ingest_time=now,
        )
    )
    cache.publish_stale("market-data-stale", {"symbol": "BTC-USD"})

    warnings = [
        record.message for record in caplog.records if record.levelname == "WARNING"
    ]
    assert warnings
    assert all("redis cache write failed" in message for message in warnings)
