from __future__ import annotations

import json
from datetime import UTC, datetime

import redis

from oziebot_domain.market_data import (
    NormalizedBestBidAsk,
    NormalizedCandle,
    NormalizedOrderBookTop,
    NormalizedTrade,
)


class RedisMarketCache:
    """Caches latest market snapshots for low-latency reads by other services."""

    def __init__(self, client: redis.Redis, ttl_seconds: int = 300):
        self._r = client
        self._ttl_seconds = ttl_seconds

    @staticmethod
    def key_trade(product_id: str) -> str:
        return f"oziebot:md:trade:last:{product_id}"

    @staticmethod
    def key_bbo(product_id: str) -> str:
        return f"oziebot:md:bbo:{product_id}"

    @staticmethod
    def key_candle(product_id: str, granularity_sec: int) -> str:
        return f"oziebot:md:candle:{granularity_sec}:{product_id}"

    @staticmethod
    def key_orderbook(product_id: str, depth: int) -> str:
        return f"oziebot:md:book:top:{depth}:{product_id}"

    def put_trade(self, item: NormalizedTrade) -> None:
        self._r.setex(
            self.key_trade(item.product_id),
            self._ttl_seconds,
            json.dumps(item.model_dump(mode="json")),
        )
        self._r.setex(
            f"oziebot:md:last_update:trade:{item.product_id}",
            self._ttl_seconds,
            datetime.now(UTC).isoformat(),
        )

    def put_bbo(self, item: NormalizedBestBidAsk) -> None:
        self._r.setex(
            self.key_bbo(item.product_id),
            self._ttl_seconds,
            json.dumps(item.model_dump(mode="json")),
        )
        self._r.setex(
            f"oziebot:md:last_update:bbo:{item.product_id}",
            self._ttl_seconds,
            datetime.now(UTC).isoformat(),
        )

    def put_candle(self, item: NormalizedCandle) -> None:
        self._r.setex(
            self.key_candle(item.product_id, item.granularity_sec),
            self._ttl_seconds,
            json.dumps(item.model_dump(mode="json")),
        )
        self._r.setex(
            f"oziebot:md:last_update:candle:{item.product_id}",
            self._ttl_seconds,
            datetime.now(UTC).isoformat(),
        )
        # Maintain a rolling history list for MA calculations
        history_key = f"oziebot:md:candles:{item.granularity_sec}:{item.product_id}"
        payload = json.dumps(item.model_dump(mode="json"))
        self._r.lpush(history_key, payload)
        self._r.ltrim(history_key, 0, 49)  # keep last 50
        self._r.expire(history_key, 7200)  # 2hr TTL

    def put_orderbook(self, item: NormalizedOrderBookTop) -> None:
        self._r.setex(
            self.key_orderbook(item.product_id, item.depth),
            self._ttl_seconds,
            json.dumps(item.model_dump(mode="json")),
        )

    def publish_stale(self, channel: str, payload: dict) -> None:
        self._r.publish(channel, json.dumps(payload, default=str))
