from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import redis

from oziebot_common.trade_log import append_trade_log_event


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

    def execute(self):
        for name, args, kwargs in self._ops:
            getattr(self._client, name)(*args, **kwargs)
        return []


class FakeRedis:
    def __init__(self) -> None:
        self._sorted: dict[str, dict[str, float]] = {}

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
    )

    response = client.get(
        "/v1/logs/trade?window_seconds=120&limit=200",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["count"] == 2
    assert payload["events"][0]["symbol"] == "BTC-USD"
    assert payload["events"][1]["message"] == "ETH-USD BBO updated"


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
