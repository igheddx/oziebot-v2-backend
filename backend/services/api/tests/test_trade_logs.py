from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import patch

import redis

from oziebot_common.trade_log import append_trade_log_event
from oziebot_common.trade_log_intelligence import write_trade_log_summary


class FakePipeline:
    def __init__(self, client: "FakeRedis") -> None:
        self._client = client
        self._ops: list[tuple[str, tuple, dict]] = []

    def zadd(self, *args, **kwargs):
        self._ops.append(("zadd", args, kwargs))
        return self

    def zremrangebyscore(self, *args, **kwargs):
        self._ops.append(("zremrangebyscore", args, kwargs))
        return self

    def expire(self, *args, **kwargs):
        self._ops.append(("expire", args, kwargs))
        return self

    def sadd(self, *args, **kwargs):
        self._ops.append(("sadd", args, kwargs))
        return self

    def setex(self, *args, **kwargs):
        self._ops.append(("setex", args, kwargs))
        return self

    def get(self, *args, **kwargs):
        self._ops.append(("get", args, kwargs))
        return self

    def execute(self):
        results = []
        for name, args, kwargs in self._ops:
            results.append(getattr(self._client, name)(*args, **kwargs))
        return results


class FakeRedis:
    def __init__(self) -> None:
        self._sorted: dict[str, dict[str, float]] = {}
        self._sets: dict[str, set[str]] = {}
        self._strings: dict[str, str] = {}

    def pipeline(self) -> FakePipeline:
        return FakePipeline(self)

    def zadd(self, key: str, mapping: dict[str, float]) -> None:
        bucket = self._sorted.setdefault(key, {})
        bucket.update(mapping)

    def zremrangebyscore(self, key: str, min_score, max_score) -> None:
        bucket = self._sorted.setdefault(key, {})
        max_value = float(max_score)
        if min_score == "-inf":
            to_delete = [member for member, score in bucket.items() if score <= max_value]
        else:
            min_value = float(min_score)
            to_delete = [
                member for member, score in bucket.items() if min_value <= score <= max_value
            ]
        for member in to_delete:
            bucket.pop(member, None)

    def expire(self, key: str, seconds: int) -> None:  # noqa: ARG002
        return None

    def sadd(self, key: str, *values: str) -> None:
        bucket = self._sets.setdefault(key, set())
        bucket.update(values)

    def smembers(self, key: str) -> set[str]:
        return self._sets.get(key, set())

    def setex(self, key: str, seconds: int, value: str) -> None:  # noqa: ARG002
        self._strings[key] = value

    def get(self, key: str) -> str | None:
        return self._strings.get(key)

    def zrevrangebyscore(
        self,
        key: str,
        max_score,
        min_score,
        *,
        start: int = 0,
        num: int | None = None,
    ) -> list[str]:
        bucket = self._sorted.get(key, {})
        min_value = float(min_score)
        max_value = float("inf") if max_score == "+inf" else float(max_score)
        rows = [
            member
            for member, score in sorted(bucket.items(), key=lambda item: item[1], reverse=True)
            if min_value <= score <= max_value
        ]
        if num is None:
            return rows[start:]
        return rows[start : start + num]


@patch("oziebot_api.api.v1.logs.redis_from_url")
def test_trade_log_endpoint_returns_recent_events(
    mock_redis_from_url, client, regular_user_and_token
):
    _, token = regular_user_and_token
    fake_redis = FakeRedis()
    mock_redis_from_url.return_value = fake_redis

    now = datetime.now(UTC)
    append_trade_log_event(
        fake_redis,
        symbol="BTC-USD",
        event_type="market_snapshot",
        message="BTC-USD market snapshot pulled",
        timestamp=now - timedelta(seconds=30),
    )
    append_trade_log_event(
        fake_redis,
        symbol="ETH-USD",
        event_type="bbo_update",
        message="ETH-USD BBO updated",
        timestamp=now - timedelta(seconds=5),
        details={
            "best_bid": Decimal("2450.10"),
            "best_ask": Decimal("2451.25"),
            "spread_pct": Decimal("0.0469"),
        },
    )
    write_trade_log_summary(
        fake_redis,
        symbol="ETH-USD",
        summary={
            "timestamp": now.isoformat(),
            "symbol": "ETH-USD",
            "summary_line": "Trend: UP | Volatility: MEDIUM | Liquidity: HIGH | Bias: BUY",
            "market_state": {
                "trend": "UP",
                "volatility": "MEDIUM",
                "liquidity": "HIGH",
                "trade_bias": "BUY",
            },
            "signal_quality_score": 78,
            "signal_quality_label": "HIGH",
            "raw_metrics": {"spread_pct": "0.0469"},
        },
    )

    response = client.get(
        "/v1/logs/trade?window_seconds=120&limit=200&symbol=ETH-USD&event_type=bbo_update",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["count"] == 1
    assert payload["symbol"] == "ETH-USD"
    assert payload["event_type"] == "bbo_update"
    assert payload["events"][0]["message"] == "ETH-USD BBO updated"
    assert payload["events"][0]["source"] == "coinbase"
    assert payload["events"][0]["details"] == {
        "best_bid": "2450.1",
        "best_ask": "2451.25",
        "spread_pct": "0.0469",
    }
    assert payload["available_symbols"] == ["ETH-USD"]
    assert payload["available_event_types"] == ["bbo_update"]
    assert payload["summaries"][0]["signal_quality_score"] == 78


@patch("oziebot_api.api.v1.logs.redis_from_url")
def test_trade_log_endpoint_returns_503_when_trade_log_redis_unavailable(
    mock_redis_from_url, client, regular_user_and_token
):
    _, token = regular_user_and_token
    mock_redis_from_url.side_effect = redis.TimeoutError("redis timed out")

    response = client.get(
        "/v1/logs/trade?window_seconds=120&limit=200",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 503, response.text
    assert response.json()["detail"] == "Trade log temporarily unavailable"
