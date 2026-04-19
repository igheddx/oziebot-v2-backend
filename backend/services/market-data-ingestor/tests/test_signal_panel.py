from __future__ import annotations

from datetime import UTC, datetime, timedelta

from oziebot_common.trade_log import read_trade_log_events
from oziebot_common.trade_log_intelligence import read_trade_log_summaries
from oziebot_market_data_ingestor.normalizer import normalize_bbo, normalize_trade
from oziebot_market_data_ingestor.signal_panel import SignalPanelEmitter


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
            for member, score in sorted(
                bucket.items(), key=lambda item: item[1], reverse=True
            )
            if min_value <= score <= max_value
        ]
        if num is None:
            return rows[start:]
        return rows[start : start + num]


def test_signal_panel_emits_summary_and_market_snapshot_events() -> None:
    fake_redis = FakeRedis()
    emitter = SignalPanelEmitter(fake_redis)
    start = datetime.now(UTC)

    first_bbo = normalize_bbo(
        {
            "product_id": "BTC-USD",
            "best_bid": "64000",
            "best_bid_size": "2.4",
            "best_ask": "64004",
            "best_ask_size": "2.2",
            "time": start.isoformat(),
        }
    )
    first_trade = normalize_trade(
        {
            "product_id": "BTC-USD",
            "trade_id": "1",
            "side": "buy",
            "price": "64003",
            "size": "0.8",
            "time": start.isoformat(),
        }
    )
    second_trade_at = start + timedelta(seconds=3)
    second_trade = normalize_trade(
        {
            "product_id": "BTC-USD",
            "trade_id": "2",
            "side": "buy",
            "price": "64060",
            "size": "1.2",
            "time": second_trade_at.isoformat(),
        }
    )

    emitter.observe_bbo(first_bbo)
    emitter.observe_trade(first_trade)
    emitter.observe_trade(second_trade)
    emitter.force_emit("BTC-USD", now=second_trade_at)

    summaries = read_trade_log_summaries(fake_redis)
    assert len(summaries) == 1
    assert summaries[0]["symbol"] == "BTC-USD"
    assert summaries[0]["market_state"]["trend"] in {"UP", "FLAT"}

    events = read_trade_log_events(
        fake_redis,
        now=second_trade_at,
        window_seconds=120,
        limit=20,
    )
    event_types = [event["event_type"] for event in events]
    assert "market_snapshot" in event_types
    assert any("MARKET SNAPSHOT" in event["message"] for event in events)
