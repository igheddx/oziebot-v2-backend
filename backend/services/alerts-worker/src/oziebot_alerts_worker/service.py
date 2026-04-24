from __future__ import annotations

import json
import logging
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import create_engine, text

from oziebot_common.queues import QueueNames, notification_event_to_json, push_json
from oziebot_domain.events import NotificationEvent, OperationalAlert

from oziebot_alerts_worker.templates import render_message

log = logging.getLogger("alerts-worker.service")


class NotificationService:
    def __init__(self, settings, redis_client, adapters: dict[str, Any]) -> None:
        self._settings = settings
        self._redis = redis_client
        self._adapters = adapters
        self._engine = (
            create_engine(settings.database_url) if settings.database_url else None
        )

    def route_event(self, event: NotificationEvent) -> None:
        if self._engine is None:
            return
        channels = self._load_enabled_channels(event)
        message = render_message(event)
        for ch in channels:
            self._attempt_delivery(
                event=event, channel_row=ch, message=message, attempt=1
            )

    def retry_delivery(self, envelope: dict[str, Any]) -> None:
        event = NotificationEvent.model_validate(envelope["event"])
        channel_row = envelope["channel"]
        attempt = int(envelope.get("attempt", 1))
        message = str(envelope.get("message") or render_message(event))
        self._attempt_delivery(
            event=event, channel_row=channel_row, message=message, attempt=attempt
        )

    def route_operational_alert(self, alert: OperationalAlert) -> None:
        adapter = self._adapters.get("slack")
        if adapter is None:
            return
        webhook_url = getattr(self._settings, "slack_webhook_url", None)
        if not webhook_url:
            return
        try:
            adapter.send(
                webhook_url,
                self._render_operational_message(alert),
                {
                    "severity": alert.severity.value,
                    "source_service": alert.source_service,
                    "alert_type": alert.alert_type,
                    "resolved": alert.resolved,
                    **alert.payload,
                },
            )
        except Exception:
            log.exception(
                "operational alert delivery failed source=%s alert_type=%s severity=%s",
                alert.source_service,
                alert.alert_type,
                alert.severity.value,
            )

    @staticmethod
    def _render_operational_message(alert: OperationalAlert) -> str:
        status = "RESOLVED" if alert.resolved else alert.severity.value.upper()
        return (
            f"[OPS][{status}] {alert.title}\n"
            f"Source: {alert.source_service}\n"
            f"{alert.message}"
        )

    def _attempt_delivery(
        self,
        *,
        event: NotificationEvent,
        channel_row: dict[str, Any],
        message: str,
        attempt: int,
    ) -> None:
        now = datetime.now(UTC)
        adapter = self._adapters.get(str(channel_row["channel"]))
        destination = str(channel_row["destination"])
        if adapter is None:
            self._record_delivery(
                event,
                channel_row,
                status="failed",
                attempt=attempt,
                error="adapter_not_found",
            )
            return
        try:
            adapter.send(destination, message, event.payload)
            self._record_delivery(
                event, channel_row, status="sent", attempt=attempt, error=None
            )
        except Exception as exc:
            err = str(exc)[:512]
            self._record_delivery(
                event, channel_row, status="retry_scheduled", attempt=attempt, error=err
            )
            if attempt >= self._settings.notify_max_retries:
                self._record_delivery(
                    event, channel_row, status="failed", attempt=attempt, error=err
                )
                return
            push_json(
                self._redis,
                QueueNames.alerts_retry(event.trading_mode),
                {
                    "event": notification_event_to_json(event),
                    "channel": {
                        "channel": channel_row["channel"],
                        "destination": destination,
                        "id": str(channel_row.get("id") or ""),
                    },
                    "attempt": attempt + 1,
                    "message": message,
                    "scheduled_at": now.isoformat(),
                },
            )

    def _load_enabled_channels(self, event: NotificationEvent) -> list[dict[str, Any]]:
        if self._engine is None:
            return []
        with self._engine.begin() as conn:
            pref = conn.execute(
                text(
                    """
                    SELECT id
                    FROM notification_preferences
                    WHERE user_id = :user_id
                      AND event_type = :event_type
                      AND is_enabled = true
                      AND trading_mode IN (:mode, 'all')
                    LIMIT 1
                    """
                ),
                {
                    "user_id": str(event.user_id),
                    "event_type": event.event_type.value,
                    "mode": event.trading_mode.value,
                },
            ).first()
            if pref is None:
                return []
            rows = (
                conn.execute(
                    text(
                        """
                    SELECT id, channel, destination
                    FROM notification_channel_configs
                    WHERE user_id = :user_id
                      AND is_enabled = true
                    """
                    ),
                    {"user_id": str(event.user_id)},
                )
                .mappings()
                .all()
            )
        return [dict(r) for r in rows]

    def _record_delivery(
        self,
        event: NotificationEvent,
        channel_row: dict[str, Any],
        *,
        status: str,
        attempt: int,
        error: str | None,
    ) -> None:
        if self._engine is None:
            return
        now = datetime.now(UTC)
        with self._engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO notification_delivery_attempts (
                      id, event_id, tenant_id, user_id, event_type, trading_mode,
                      channel, destination, status, attempt, error, payload, created_at, updated_at
                    ) VALUES (
                      :id, :event_id, :tenant_id, :user_id, :event_type, :trading_mode,
                      :channel, :destination, :status, :attempt, :error, :payload, :created_at, :updated_at
                    )
                    """
                ),
                {
                    "id": str(uuid.uuid4()),
                    "event_id": str(event.event_id),
                    "tenant_id": str(event.tenant_id),
                    "user_id": str(event.user_id),
                    "event_type": event.event_type.value,
                    "trading_mode": event.trading_mode.value,
                    "channel": str(channel_row["channel"]),
                    "destination": str(channel_row["destination"]),
                    "status": status,
                    "attempt": attempt,
                    "error": error,
                    "payload": json.dumps(event.payload, default=str),
                    "created_at": now,
                    "updated_at": now,
                },
            )
