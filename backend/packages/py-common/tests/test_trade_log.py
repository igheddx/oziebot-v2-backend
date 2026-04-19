from __future__ import annotations

from datetime import UTC, datetime, timedelta

from oziebot_common.trade_log import append_trade_log_event, read_trade_log_events


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
            to_delete = [
                member for member, score in bucket.items() if score <= max_value
            ]
        else:
            min_value = float(min_score)
            to_delete = [
                member
                for member, score in bucket.items()
                if min_value <= score <= max_value
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
        if max_score == "+inf":
            max_value = float("inf")
        else:
            max_value = float(max_score)
        rows = [
            member
            for member, score in sorted(
                bucket.items(), key=lambda item: item[1], reverse=True
            )
            if min_value <= score <= max_value
        ]
        if num is None:
            return rows[start:]
        return rows[start : start + num]


def test_trade_log_keeps_recent_events_in_chronological_order() -> None:
    client = FakeRedis()
    now = datetime.now(UTC)

    append_trade_log_event(
        client,
        symbol="BTC-USD",
        event_type="market_snapshot",
        message="BTC-USD market snapshot pulled",
        timestamp=now - timedelta(seconds=150),
    )
    append_trade_log_event(
        client,
        symbol="ETH-USD",
        event_type="bbo_update",
        message="ETH-USD BBO updated",
        timestamp=now - timedelta(seconds=50),
    )
    append_trade_log_event(
        client,
        symbol="SOL-USD",
        event_type="candles_refresh",
        message="SOL-USD candles refreshed",
        timestamp=now - timedelta(seconds=10),
    )

    events = read_trade_log_events(client, now=now, window_seconds=120, limit=10)

    assert [event["symbol"] for event in events] == ["ETH-USD", "SOL-USD"]
    assert events[0]["message"] == "ETH-USD BBO updated"
    assert events[1]["message"] == "SOL-USD candles refreshed"
