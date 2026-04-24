from datetime import UTC, datetime, timedelta

from oziebot_domain.events import OperationalAlertSeverity
from oziebot_market_data_ingestor.monitoring import (
    PersistentStaleMonitor,
    RedisPressureMonitor,
)


class FakeRedis:
    def __init__(self, *, used_memory: int, maxmemory: int) -> None:
        self.used_memory = used_memory
        self.maxmemory = maxmemory

    def info(self, section: str):  # noqa: ANN001
        assert section == "memory"
        return {"used_memory": self.used_memory, "maxmemory": self.maxmemory}


def test_redis_pressure_monitor_emits_alert_and_recovery():
    monitor = RedisPressureMonitor(
        warning_pct=70,
        critical_pct=85,
        check_interval_seconds=0,
        alert_cooldown_seconds=300,
    )
    now = datetime.now(UTC)
    redis = FakeRedis(used_memory=91, maxmemory=100)

    snapshot, alert = monitor.sample(redis, now=now)

    assert snapshot["usagePct"] == 91.0
    assert alert is not None
    assert alert.severity == OperationalAlertSeverity.CRITICAL
    assert alert.resolved is False

    redis.used_memory = 40
    snapshot, recovery = monitor.sample(redis, now=now + timedelta(seconds=10))

    assert snapshot["usagePct"] == 40.0
    assert recovery is not None
    assert recovery.severity == OperationalAlertSeverity.INFO
    assert recovery.resolved is True


def test_persistent_stale_monitor_alerts_after_threshold_and_recovers():
    monitor = PersistentStaleMonitor(
        alert_after_seconds=60,
        alert_cooldown_seconds=300,
    )
    now = datetime.now(UTC)

    details, alert = monitor.evaluate(
        {"trade": ["BTC-USD"], "bbo": ["BTC-USD"], "candle": []},
        now=now,
    )
    assert details["alertSymbolCount"] == 1
    assert alert is None

    details, alert = monitor.evaluate(
        {"trade": ["BTC-USD"], "bbo": ["BTC-USD"], "candle": []},
        now=now + timedelta(seconds=61),
    )
    assert details["activeForSeconds"] == 61
    assert alert is not None
    assert alert.severity == OperationalAlertSeverity.WARNING
    assert alert.resolved is False

    _, recovery = monitor.evaluate(
        {"trade": [], "bbo": [], "candle": []},
        now=now + timedelta(seconds=75),
    )
    assert recovery is not None
    assert recovery.severity == OperationalAlertSeverity.INFO
    assert recovery.resolved is True
