from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from oziebot_domain.events import OperationalAlert, OperationalAlertSeverity


def _sample(values: list[str], *, limit: int = 5) -> list[str]:
    return sorted(values)[:limit]


@dataclass
class RedisPressureMonitor:
    warning_pct: float = 70.0
    critical_pct: float = 85.0
    check_interval_seconds: int = 30
    alert_cooldown_seconds: int = 300
    _last_checked_at: datetime | None = None
    _last_snapshot: dict[str, Any] = field(default_factory=dict)
    _active_severity: OperationalAlertSeverity | None = None
    _last_alert_at: datetime | None = None

    def sample(
        self, client: Any, *, now: datetime
    ) -> tuple[dict[str, Any], OperationalAlert | None]:
        if (
            self._last_checked_at is not None
            and (now - self._last_checked_at).total_seconds()
            < self.check_interval_seconds
            and self._last_snapshot
        ):
            return dict(self._last_snapshot), None

        self._last_checked_at = now
        info = client.info("memory")
        used_memory = int(info.get("used_memory") or 0)
        maxmemory = int(info.get("maxmemory") or 0)
        usage_pct = round((used_memory / maxmemory) * 100, 2) if maxmemory > 0 else None
        severity = self._severity_for(usage_pct)
        snapshot = {
            "usedMemoryBytes": used_memory,
            "maxMemoryBytes": maxmemory,
            "usagePct": usage_pct,
            "severity": severity.value if severity is not None else "ok",
            "checkedAt": now.isoformat(),
        }
        self._last_snapshot = snapshot

        if severity is None:
            if self._active_severity is None:
                return dict(snapshot), None
            previous = self._active_severity
            self._active_severity = None
            self._last_alert_at = now
            return dict(snapshot), OperationalAlert(
                alert_id=uuid.uuid4(),
                source_service="market-data-ingestor",
                alert_type="redis_memory_pressure",
                severity=OperationalAlertSeverity.INFO,
                title="Redis memory pressure recovered",
                message="Redis memory usage has returned below the alert threshold.",
                payload={
                    "usage_pct": usage_pct,
                    "used_memory_bytes": used_memory,
                    "maxmemory_bytes": maxmemory,
                    "previous_severity": previous.value,
                },
                resolved=True,
                occurred_at=now,
            )

        should_emit = (
            self._active_severity != severity
            or self._last_alert_at is None
            or (now - self._last_alert_at).total_seconds()
            >= self.alert_cooldown_seconds
        )
        self._active_severity = severity
        if not should_emit:
            return dict(snapshot), None
        self._last_alert_at = now
        return dict(snapshot), OperationalAlert(
            alert_id=uuid.uuid4(),
            source_service="market-data-ingestor",
            alert_type="redis_memory_pressure",
            severity=severity,
            title="Redis memory pressure detected",
            message=(
                f"Redis memory usage is {usage_pct:.2f}% "
                f"({used_memory}/{maxmemory} bytes)."
            ),
            payload={
                "usage_pct": usage_pct,
                "used_memory_bytes": used_memory,
                "maxmemory_bytes": maxmemory,
            },
            occurred_at=now,
        )

    def _severity_for(self, usage_pct: float | None) -> OperationalAlertSeverity | None:
        if usage_pct is None:
            return None
        if usage_pct >= self.critical_pct:
            return OperationalAlertSeverity.CRITICAL
        if usage_pct >= self.warning_pct:
            return OperationalAlertSeverity.WARNING
        return None


@dataclass
class PersistentStaleMonitor:
    alert_after_seconds: int = 90
    alert_cooldown_seconds: int = 300
    _active_since: datetime | None = None
    _alert_open: bool = False
    _last_alert_at: datetime | None = None

    def evaluate(
        self, stale_map: dict[str, list[str]], *, now: datetime
    ) -> tuple[dict[str, Any], OperationalAlert | None]:
        stale_trade = stale_map.get("trade", [])
        stale_bbo = stale_map.get("bbo", [])
        stale_candle = stale_map.get("candle", [])
        alert_symbols = sorted(set(stale_bbo) | set(stale_candle))
        details = {
            "tradeCount": len(stale_trade),
            "bboCount": len(stale_bbo),
            "candleCount": len(stale_candle),
            "sampleTrade": _sample(stale_trade),
            "sampleBbo": _sample(stale_bbo),
            "sampleCandle": _sample(stale_candle),
            "alertSymbolCount": len(alert_symbols),
            "sampleAlertSymbols": _sample(alert_symbols),
            "activeSince": self._active_since.isoformat()
            if self._active_since
            else None,
        }

        if not alert_symbols:
            details["activeForSeconds"] = 0
            if not self._alert_open:
                self._active_since = None
                return details, None
            active_since = self._active_since or now
            active_for_seconds = int((now - active_since).total_seconds())
            self._active_since = None
            self._alert_open = False
            self._last_alert_at = now
            details["activeForSeconds"] = active_for_seconds
            return details, OperationalAlert(
                alert_id=uuid.uuid4(),
                source_service="market-data-ingestor",
                alert_type="market_data_stale",
                severity=OperationalAlertSeverity.INFO,
                title="Market data freshness recovered",
                message="BBO/candle freshness has recovered for all monitored symbols.",
                payload={"active_for_seconds": active_for_seconds},
                resolved=True,
                occurred_at=now,
            )

        if self._active_since is None:
            self._active_since = now
        active_for_seconds = int((now - self._active_since).total_seconds())
        details["activeSince"] = self._active_since.isoformat()
        details["activeForSeconds"] = active_for_seconds
        should_open = active_for_seconds >= self.alert_after_seconds
        if not should_open:
            return details, None

        should_emit = (
            not self._alert_open
            or self._last_alert_at is None
            or (now - self._last_alert_at).total_seconds()
            >= self.alert_cooldown_seconds
        )
        self._alert_open = True
        if not should_emit:
            return details, None
        self._last_alert_at = now
        return details, OperationalAlert(
            alert_id=uuid.uuid4(),
            source_service="market-data-ingestor",
            alert_type="market_data_stale",
            severity=OperationalAlertSeverity.WARNING,
            title="Persistent stale market data detected",
            message=(
                f"Market data has remained stale for {active_for_seconds}s "
                f"across {len(alert_symbols)} symbols."
            ),
            payload={
                "active_for_seconds": active_for_seconds,
                "trade_count": len(stale_trade),
                "bbo_count": len(stale_bbo),
                "candle_count": len(stale_candle),
                "sample_trade": _sample(stale_trade),
                "sample_bbo": _sample(stale_bbo),
                "sample_candle": _sample(stale_candle),
            },
            occurred_at=now,
        )
