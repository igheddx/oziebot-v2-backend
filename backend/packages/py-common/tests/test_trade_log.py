from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

import oziebot_common.trade_log as trade_log_module
import oziebot_common.trade_log_intelligence as trade_log_intelligence_module
from oziebot_common.sqlite_aux_schema import ensure_sqlite_aux_schema
from oziebot_common.trade_log import append_trade_log_event, read_trade_log_events
from oziebot_common.trade_log_intelligence import (
    append_trade_log_sample,
    build_market_signal_snapshot,
    read_trade_log_samples,
    read_trade_log_summaries,
    write_trade_log_summary,
)


@pytest.fixture
def sqlite_engine():
    eng = create_engine(
        "sqlite+pysqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    ensure_sqlite_aux_schema(eng)
    return eng


class FakeObservabilityStore:
    def __init__(self) -> None:
        self.events: list[dict[str, object]] = []
        self.samples: dict[str, list[dict[str, object]]] = {}
        self.summaries: dict[str, dict[str, object]] = {}

    def append_trade_event(self, event: dict[str, object]) -> None:
        self.events.append(event)

    def read_trade_events(
        self,
        *,
        window_seconds: int,  # noqa: ARG002
        limit: int,
        symbol: str | None = None,
        event_type: str | None = None,
        now: datetime | None = None,  # noqa: ARG002
    ) -> list[dict[str, object]]:
        rows = list(self.events)
        if symbol:
            rows = [row for row in rows if row["symbol"] == str(symbol).upper()]
        if event_type:
            rows = [row for row in rows if row["event_type"] == event_type]
        return rows[-limit:]

    def append_trade_sample(self, payload: dict[str, object]) -> None:
        self.samples.setdefault(str(payload["symbol"]), []).append(payload)

    def read_trade_samples(
        self,
        *,
        symbol: str,
        window_seconds: int,  # noqa: ARG002
        now: datetime | None = None,  # noqa: ARG002
    ) -> list[dict[str, object]]:
        return list(self.samples.get(str(symbol).upper(), []))

    def write_trade_summary(self, summary: dict[str, object]) -> None:
        self.summaries[str(summary["symbol"])] = summary

    def read_trade_summaries(
        self,
        *,
        symbol: str | None = None,
    ) -> list[dict[str, object]]:
        if symbol:
            summary = self.summaries.get(str(symbol).upper())
            return [summary] if summary is not None else []
        return list(self.summaries.values())


def test_trade_log_keeps_recent_events_in_chronological_order(sqlite_engine) -> None:
    now = datetime.now(UTC)

    append_trade_log_event(
        sqlite_engine,
        symbol="BTC-USD",
        event_type="market_snapshot",
        message="BTC-USD market snapshot pulled",
        timestamp=now - timedelta(seconds=150),
    )
    append_trade_log_event(
        sqlite_engine,
        symbol="ETH-USD",
        event_type="bbo_update",
        message="ETH-USD BBO updated",
        timestamp=now - timedelta(seconds=50),
    )
    append_trade_log_event(
        sqlite_engine,
        symbol="SOL-USD",
        event_type="candles_refresh",
        message="SOL-USD candles refreshed",
        timestamp=now - timedelta(seconds=10),
    )

    events = read_trade_log_events(sqlite_engine, now=now, window_seconds=120, limit=10)

    assert [event["symbol"] for event in events] == ["ETH-USD", "SOL-USD"]
    assert events[0]["message"] == "ETH-USD BBO updated"
    assert events[1]["message"] == "SOL-USD candles refreshed"


def test_trade_log_preserves_structured_details(sqlite_engine) -> None:
    now = datetime.now(UTC)

    append_trade_log_event(
        sqlite_engine,
        symbol="BTC-USD",
        event_type="trade_tick",
        message="BTC-USD trade tick sampled",
        timestamp=now,
        details={
            "price": Decimal("64250.12"),
            "size": Decimal("0.045"),
            "event_time": now,
            "nested": {"spread_pct": Decimal("0.0234")},
        },
    )

    events = read_trade_log_events(sqlite_engine, now=now, window_seconds=120, limit=10)

    assert events[0]["source"] == "coinbase"
    assert events[0]["details"] == {
        "price": "64250.12",
        "size": "0.045",
        "event_time": now.isoformat(),
        "nested": {"spread_pct": "0.0234"},
    }


def test_trade_log_filters_by_symbol_and_event_type(sqlite_engine) -> None:
    now = datetime.now(UTC)
    append_trade_log_event(
        sqlite_engine,
        symbol="BTC-USD",
        event_type="market_snapshot",
        message="btc snapshot",
        timestamp=now - timedelta(seconds=5),
    )
    append_trade_log_event(
        sqlite_engine,
        symbol="ETH-USD",
        event_type="trade_tick",
        message="eth tick",
        timestamp=now - timedelta(seconds=4),
    )

    events = read_trade_log_events(
        sqlite_engine,
        now=now,
        window_seconds=120,
        limit=10,
        symbol="BTC-USD",
        event_type="market_snapshot",
    )

    assert len(events) == 1
    assert events[0]["symbol"] == "BTC-USD"
    assert events[0]["event_type"] == "market_snapshot"


def test_trade_log_summary_round_trip_and_snapshot_build(sqlite_engine) -> None:
    now = datetime.now(UTC)
    for seconds, mid_price, volume, buy_volume, sell_volume in [
        (30, "64000", "120000", "0.9", "0.6"),
        (18, "64035", "185000", "1.3", "0.5"),
        (6, "64105", "260000", "1.7", "0.4"),
    ]:
        append_trade_log_sample(
            sqlite_engine,
            symbol="BTC-USD",
            timestamp=now - timedelta(seconds=seconds),
            sample={
                "mid_price": Decimal(mid_price),
                "spread_pct": Decimal("0.018"),
                "best_bid": Decimal(mid_price) - Decimal("2"),
                "best_ask": Decimal(mid_price) + Decimal("2"),
                "bid_size": Decimal("2.4"),
                "ask_size": Decimal("2.1"),
                "trade_volume": Decimal("2.0"),
                "trade_notional_usd": Decimal(volume),
                "buy_volume": Decimal(buy_volume),
                "sell_volume": Decimal(sell_volume),
                "trade_count": 12,
                "last_price": Decimal(mid_price),
                "price_high": Decimal(mid_price) + Decimal("10"),
                "price_low": Decimal(mid_price) - Decimal("10"),
            },
        )

    snapshot = build_market_signal_snapshot(
        symbol="BTC-USD",
        samples=read_trade_log_samples(
            sqlite_engine, symbol="BTC-USD", now=now, window_seconds=60
        ),
    )

    assert snapshot is not None
    assert snapshot["symbol"] == "BTC-USD"
    assert snapshot["market_state"]["trend"] == "UP"
    assert snapshot["signal_quality_label"] in {"MODERATE", "HIGH"}

    write_trade_log_summary(sqlite_engine, symbol="BTC-USD", summary=snapshot)
    summaries = read_trade_log_summaries(sqlite_engine)
    assert len(summaries) == 1
    assert summaries[0]["symbol"] == "BTC-USD"


def test_trade_log_writes_degrade_when_database_unavailable(caplog) -> None:
    engine = MagicMock()
    engine.dialect.name = "postgresql"
    cm = MagicMock()
    cm.__enter__.side_effect = RuntimeError("db unavailable")
    engine.begin.return_value = cm
    now = datetime.now(UTC)

    event = append_trade_log_event(
        engine,
        symbol="BTC-USD",
        event_type="trade_tick",
        message="test",
        timestamp=now,
    )
    sample = append_trade_log_sample(
        engine,
        symbol="BTC-USD",
        sample={"mid_price": Decimal("62000")},
        timestamp=now,
    )
    summary = write_trade_log_summary(
        engine,
        symbol="BTC-USD",
        summary={"symbol": "BTC-USD", "signal_quality_score": 80},
    )

    assert event["symbol"] == "BTC-USD"
    assert sample["symbol"] == "BTC-USD"
    assert summary["symbol"] == "BTC-USD"
    warnings = [
        record.message for record in caplog.records if record.levelname == "WARNING"
    ]
    assert any("trade log write failed" in message for message in warnings)
    assert any("trade log sample write failed" in message for message in warnings)
    assert any("trade log summary write failed" in message for message in warnings)


def test_trade_log_can_use_s3_observability_store(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = FakeObservabilityStore()
    monkeypatch.setattr(trade_log_module, "get_observability_store", lambda: store)
    monkeypatch.setattr(
        trade_log_intelligence_module,
        "get_observability_store",
        lambda: store,
    )
    now = datetime.now(UTC)

    append_trade_log_event(
        None,
        symbol="BTC-USD",
        event_type="trade_tick",
        message="s3 event",
        timestamp=now,
    )
    append_trade_log_sample(
        None,
        symbol="BTC-USD",
        sample={"mid_price": Decimal("62000")},
        timestamp=now,
    )
    write_trade_log_summary(
        None,
        symbol="BTC-USD",
        summary={"symbol": "BTC-USD", "signal_quality_score": 90},
    )

    events = read_trade_log_events(None, symbol="BTC-USD")
    samples = read_trade_log_samples(None, symbol="BTC-USD")
    summaries = read_trade_log_summaries(None, symbol="BTC-USD")

    assert len(events) == 1
    assert events[0]["message"] == "s3 event"
    assert len(samples) == 1
    assert samples[0]["sample"]["mid_price"] == "62000"
    assert summaries[0]["signal_quality_score"] == 90
