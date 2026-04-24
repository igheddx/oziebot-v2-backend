from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import create_engine, text

from oziebot_domain.events import (
    NotificationEvent,
    NotificationEventType,
    OperationalAlert,
    OperationalAlertSeverity,
)
from oziebot_domain.trading_mode import TradingMode

from oziebot_alerts_worker.service import NotificationService


class FakeRedis:
    def __init__(self) -> None:
        self.pushed: list[tuple[str, dict]] = []

    def lpush(self, key: str, value: str) -> None:
        self.pushed.append((key, json.loads(value)))


class CapturingAdapter:
    def __init__(self, *, should_fail: bool = False) -> None:
        self.should_fail = should_fail
        self.calls: list[tuple[str, str, dict]] = []

    def send(self, destination: str, message: str, payload: dict) -> None:
        self.calls.append((destination, message, payload))
        if self.should_fail:
            raise RuntimeError("simulated delivery failure")


@dataclass
class _Settings:
    database_url: str
    notify_max_retries: int = 2
    slack_webhook_url: str | None = "https://hooks.slack.com/services/test/test/test"


def _create_tables(db_url: str) -> None:
    engine = create_engine(db_url)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE notification_channel_configs (
                    id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    destination TEXT NOT NULL,
                    is_enabled BOOLEAN NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE notification_preferences (
                    id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    trading_mode TEXT NOT NULL,
                    is_enabled BOOLEAN NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE notification_delivery_attempts (
                    id TEXT PRIMARY KEY,
                    event_id TEXT NOT NULL,
                    tenant_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    trading_mode TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    destination TEXT NOT NULL,
                    status TEXT NOT NULL,
                    attempt INTEGER NOT NULL,
                    error TEXT,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
        )


def _seed_user_config(db_url: str, *, user_id: uuid.UUID, mode: str) -> None:
    engine = create_engine(db_url)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO notification_preferences (id, user_id, event_type, trading_mode, is_enabled)
                VALUES (:id, :user_id, :event_type, :trading_mode, :is_enabled)
                """
            ),
            {
                "id": str(uuid.uuid4()),
                "user_id": str(user_id),
                "event_type": "trade_opened",
                "trading_mode": mode,
                "is_enabled": True,
            },
        )
        for channel, destination in (("sms", "+10000000000"), ("slack", "#desk")):
            conn.execute(
                text(
                    """
                    INSERT INTO notification_channel_configs (id, user_id, channel, destination, is_enabled)
                    VALUES (:id, :user_id, :channel, :destination, :is_enabled)
                    """
                ),
                {
                    "id": str(uuid.uuid4()),
                    "user_id": str(user_id),
                    "channel": channel,
                    "destination": destination,
                    "is_enabled": True,
                },
            )


def _attempt_rows(db_url: str) -> list[dict]:
    engine = create_engine(db_url)
    with engine.begin() as conn:
        rows = (
            conn.execute(
                text(
                    "SELECT channel, status, attempt FROM notification_delivery_attempts ORDER BY rowid ASC"
                )
            )
            .mappings()
            .all()
        )
    return [dict(r) for r in rows]


def test_route_event_honors_mode_and_isolates_channel_failures(tmp_path: Path):
    db_path = tmp_path / "alerts.db"
    db_url = f"sqlite+pysqlite:///{db_path}"
    _create_tables(db_url)

    user_id = uuid.uuid4()
    tenant_id = uuid.uuid4()
    _seed_user_config(db_url, user_id=user_id, mode="paper")

    redis = FakeRedis()
    sms = CapturingAdapter(should_fail=False)
    slack = CapturingAdapter(should_fail=True)
    service = NotificationService(
        _Settings(database_url=db_url, notify_max_retries=2),
        redis,
        {"sms": sms, "slack": slack},
    )

    event = NotificationEvent(
        event_id=uuid.uuid4(),
        tenant_id=tenant_id,
        user_id=user_id,
        trading_mode=TradingMode.PAPER,
        event_type=NotificationEventType.TRADE_OPENED,
        payload={"symbol": "BTC-USD", "strategy_id": "s1"},
        message="opened",
    )
    service.route_event(event)

    assert len(sms.calls) == 1
    assert len(slack.calls) == 1
    assert len(redis.pushed) == 1
    retry_key, retry_payload = redis.pushed[0]
    assert retry_key.endswith(":alerts_retry:paper")
    assert retry_payload["attempt"] == 2

    rows = _attempt_rows(db_url)
    statuses = [r["status"] for r in rows]
    assert "sent" in statuses
    assert "retry_scheduled" in statuses


def test_route_event_skips_when_mode_preference_does_not_match(tmp_path: Path):
    db_path = tmp_path / "alerts_mode.db"
    db_url = f"sqlite+pysqlite:///{db_path}"
    _create_tables(db_url)

    user_id = uuid.uuid4()
    tenant_id = uuid.uuid4()
    _seed_user_config(db_url, user_id=user_id, mode="live")

    redis = FakeRedis()
    sms = CapturingAdapter(should_fail=False)
    service = NotificationService(
        _Settings(database_url=db_url, notify_max_retries=2), redis, {"sms": sms}
    )

    event = NotificationEvent(
        event_id=uuid.uuid4(),
        tenant_id=tenant_id,
        user_id=user_id,
        trading_mode=TradingMode.PAPER,
        event_type=NotificationEventType.TRADE_OPENED,
        payload={"symbol": "ETH-USD"},
        message="opened",
    )
    service.route_event(event)

    assert sms.calls == []
    assert redis.pushed == []
    assert _attempt_rows(db_url) == []


def test_retry_delivery_stops_at_max_retries(tmp_path: Path):
    db_path = tmp_path / "alerts_retry.db"
    db_url = f"sqlite+pysqlite:///{db_path}"
    _create_tables(db_url)

    user_id = uuid.uuid4()
    tenant_id = uuid.uuid4()
    redis = FakeRedis()
    failing = CapturingAdapter(should_fail=True)
    service = NotificationService(
        _Settings(database_url=db_url, notify_max_retries=2), redis, {"slack": failing}
    )

    event = NotificationEvent(
        event_id=uuid.uuid4(),
        tenant_id=tenant_id,
        user_id=user_id,
        trading_mode=TradingMode.LIVE,
        event_type=NotificationEventType.TRADE_OPENED,
        payload={"symbol": "SOL-USD"},
        message="opened",
    )
    envelope = {
        "event": event.model_dump(mode="json"),
        "channel": {"channel": "slack", "destination": "#desk"},
        "attempt": 2,
        "message": "[LIVE] Trade opened",
    }

    service.retry_delivery(envelope)

    rows = _attempt_rows(db_url)
    statuses = [r["status"] for r in rows]
    assert statuses == ["retry_scheduled", "failed"]
    assert redis.pushed == []


def test_route_operational_alert_uses_slack_adapter(tmp_path: Path):
    db_path = tmp_path / "alerts_ops.db"
    db_url = f"sqlite+pysqlite:///{db_path}"
    _create_tables(db_url)

    redis = FakeRedis()
    slack = CapturingAdapter(should_fail=False)
    service = NotificationService(
        _Settings(database_url=db_url, notify_max_retries=2),
        redis,
        {"slack": slack},
    )

    alert = OperationalAlert(
        alert_id=uuid.uuid4(),
        source_service="market-data-ingestor",
        alert_type="redis_memory_pressure",
        severity=OperationalAlertSeverity.CRITICAL,
        title="Redis memory pressure detected",
        message="Redis memory usage is 91.2%.",
        payload={"usage_pct": 91.2},
    )
    service.route_operational_alert(alert)

    assert len(slack.calls) == 1
    destination, message, payload = slack.calls[0]
    assert destination == "https://hooks.slack.com/services/test/test/test"
    assert "[OPS][CRITICAL] Redis memory pressure detected" in message
    assert payload["usage_pct"] == 91.2
