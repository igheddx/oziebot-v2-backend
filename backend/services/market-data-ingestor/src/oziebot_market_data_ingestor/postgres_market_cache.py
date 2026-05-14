from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any

from oziebot_common.postgres_runtime_kv import PostgresRuntimeKV
from oziebot_domain.market_data import (
    NormalizedBestBidAsk,
    NormalizedCandle,
    NormalizedOrderBookTop,
    NormalizedTrade,
)

log = logging.getLogger("market-data-ingestor.postgres-market-cache")


class PostgresMarketCache:
    """Ephemeral Coinbase-derived snapshots for workers (via ``runtime_kv``)."""

    def __init__(
        self,
        kv: PostgresRuntimeKV,
        ttl_seconds: int = 120,
        *,
        candle_history_ttl_seconds: int = 1800,
        candle_history_limit: int = 50,
        write_error_log_interval_seconds: int = 30,
    ) -> None:
        self._kv = kv
        self._ttl_seconds = ttl_seconds
        self._candle_history_ttl_seconds = candle_history_ttl_seconds
        self._candle_history_limit = max(0, candle_history_limit)
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
        except Exception as exc:
            now = datetime.now(UTC)
            should_log = (
                self._last_write_error_at is None
                or (now - self._last_write_error_at).total_seconds()
                >= self._write_error_log_interval_seconds
            )
            if should_log:
                self._last_write_error_at = now
                log.warning(
                    "postgres market cache write failed op=%s err=%s", op_name, exc
                )
            return False
        return True

    def put_trade(self, item: NormalizedTrade) -> None:
        def _run() -> None:
            payload = json.dumps(item.model_dump(mode="json"))
            self._kv.setex(self.key_trade(item.product_id), self._ttl_seconds, payload)
            self._kv.setex(
                f"oziebot:md:last_update:trade:{item.product_id}",
                self._ttl_seconds,
                datetime.now(UTC).isoformat(),
            )

        self._write_cache("trade", _run)

    def put_bbo(self, item: NormalizedBestBidAsk) -> None:
        def _run() -> None:
            payload = json.dumps(item.model_dump(mode="json"))
            self._kv.setex(self.key_bbo(item.product_id), self._ttl_seconds, payload)
            self._kv.setex(
                f"oziebot:md:last_update:bbo:{item.product_id}",
                self._ttl_seconds,
                datetime.now(UTC).isoformat(),
            )

        self._write_cache("bbo", _run)

    def put_candle(self, item: NormalizedCandle) -> None:
        def _run() -> None:
            self._kv.setex(
                self.key_candle(item.product_id, item.granularity_sec),
                self._ttl_seconds,
                json.dumps(item.model_dump(mode="json")),
            )
            self._kv.setex(
                f"oziebot:md:last_update:candle:{item.product_id}",
                self._ttl_seconds,
                datetime.now(UTC).isoformat(),
            )
            if self._candle_history_limit > 0 and self._candle_history_ttl_seconds > 0:
                history_key = (
                    f"oziebot:md:candles:{item.granularity_sec}:{item.product_id}"
                )
                existing = self._kv.lrange_strings(
                    history_key, 0, self._candle_history_limit - 1
                )
                history = self._merge_candle_history(existing, item)
                self._kv.setex(
                    history_key,
                    self._candle_history_ttl_seconds,
                    json.dumps(history, separators=(",", ":")),
                )

        self._write_cache("candle", _run)

    def _merge_candle_history(
        self, existing_entries: list[str], latest_item: NormalizedCandle
    ) -> list[str]:
        by_bucket: dict[str, dict[str, Any]] = {}
        for raw in existing_entries:
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if not isinstance(parsed, dict):
                continue
            bucket_key = self._candle_bucket_key(parsed)
            if bucket_key is None or bucket_key in by_bucket:
                continue
            by_bucket[bucket_key] = parsed

        latest_payload = latest_item.model_dump(mode="json")
        latest_bucket = self._candle_bucket_key(latest_payload)
        if latest_bucket is not None:
            by_bucket[latest_bucket] = latest_payload

        ordered = sorted(
            by_bucket.values(),
            key=lambda payload: self._candle_sort_key(payload),
            reverse=True,
        )
        return [
            json.dumps(payload, separators=(",", ":"))
            for payload in ordered[: self._candle_history_limit]
        ]

    @staticmethod
    def _candle_bucket_key(payload: dict[str, Any]) -> str | None:
        bucket_start = payload.get("bucket_start") or payload.get("start")
        if bucket_start in (None, ""):
            return None
        return str(bucket_start)

    @staticmethod
    def _candle_sort_key(payload: dict[str, Any]) -> datetime:
        bucket_key = PostgresMarketCache._candle_bucket_key(payload)
        if bucket_key is None:
            return datetime.min.replace(tzinfo=UTC)
        normalized = bucket_key.strip()
        if normalized.isdigit():
            return datetime.fromtimestamp(int(normalized), tz=UTC)
        return datetime.fromisoformat(normalized.replace("Z", "+00:00"))

    def put_orderbook(self, item: NormalizedOrderBookTop) -> None:
        self._write_cache(
            "orderbook",
            lambda: self._kv.setex(
                self.key_orderbook(item.product_id, item.depth),
                self._ttl_seconds,
                json.dumps(item.model_dump(mode="json")),
            ),
        )

    def publish_stale(self, channel: str, payload: dict) -> None:  # noqa: ARG002
        """Previously Redis pub/sub; downstream consumers poll Postgres KV instead."""

        return None
