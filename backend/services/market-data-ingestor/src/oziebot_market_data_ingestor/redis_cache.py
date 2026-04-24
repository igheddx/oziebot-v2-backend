from __future__ import annotations

import json
import logging
from datetime import UTC, datetime

import redis

from oziebot_domain.market_data import (
    NormalizedBestBidAsk,
    NormalizedCandle,
    NormalizedOrderBookTop,
    NormalizedTrade,
)

log = logging.getLogger("market-data-ingestor.redis-cache")


class RedisMarketCache:
    """Caches latest market snapshots for low-latency reads by other services."""

    def __init__(
        self,
        client: redis.Redis,
        ttl_seconds: int = 120,
        *,
        candle_history_ttl_seconds: int = 1800,
        candle_history_limit: int = 50,
        write_error_log_interval_seconds: int = 30,
    ):
        self._r = client
        self._ttl_seconds = ttl_seconds
        self._candle_history_ttl_seconds = candle_history_ttl_seconds
        self._candle_history_limit = candle_history_limit
        self._write_error_log_interval_seconds = write_error_log_interval_seconds
        self._last_write_error_at: datetime | None = None

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

    def _write_cache(self, op_name: str, operation) -> bool:  # noqa: ANN001
        try:
            operation()
        except redis.RedisError as exc:
            now = datetime.now(UTC)
            should_log = (
                self._last_write_error_at is None
                or (now - self._last_write_error_at).total_seconds()
                >= self._write_error_log_interval_seconds
            )
            if should_log:
                self._last_write_error_at = now
                log.warning("redis cache write failed op=%s err=%s", op_name, exc)
            return False
        return True

    def put_trade(self, item: NormalizedTrade) -> None:
        self._write_cache(
            "trade",
            lambda: (
                self._r.setex(
                    self.key_trade(item.product_id),
                    self._ttl_seconds,
                    json.dumps(item.model_dump(mode="json")),
                ),
                self._r.setex(
                    f"oziebot:md:last_update:trade:{item.product_id}",
                    self._ttl_seconds,
                    datetime.now(UTC).isoformat(),
                ),
            ),
        )

    def put_bbo(self, item: NormalizedBestBidAsk) -> None:
        self._write_cache(
            "bbo",
            lambda: (
                self._r.setex(
                    self.key_bbo(item.product_id),
                    self._ttl_seconds,
                    json.dumps(item.model_dump(mode="json")),
                ),
                self._r.setex(
                    f"oziebot:md:last_update:bbo:{item.product_id}",
                    self._ttl_seconds,
                    datetime.now(UTC).isoformat(),
                ),
            ),
        )

    def put_candle(self, item: NormalizedCandle) -> None:
        def _write() -> None:
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
            history_key = f"oziebot:md:candles:{item.granularity_sec}:{item.product_id}"
            payload = json.dumps(item.model_dump(mode="json"))
            self._r.lpush(history_key, payload)
            self._r.ltrim(history_key, 0, self._candle_history_limit - 1)
            self._r.expire(history_key, self._candle_history_ttl_seconds)

        self._write_cache("candle", _write)

    def put_orderbook(self, item: NormalizedOrderBookTop) -> None:
        self._write_cache(
            "orderbook",
            lambda: self._r.setex(
                self.key_orderbook(item.product_id, item.depth),
                self._ttl_seconds,
                json.dumps(item.model_dump(mode="json")),
            ),
        )

    def publish_stale(self, channel: str, payload: dict) -> None:
        self._write_cache(
            "publish_stale",
            lambda: self._r.publish(channel, json.dumps(payload, default=str)),
        )
