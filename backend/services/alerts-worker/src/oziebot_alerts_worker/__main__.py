from __future__ import annotations

import logging

from sqlalchemy import create_engine

from oziebot_common.health import install_shutdown_handlers, start_health_server
from oziebot_common.queues import (
    QueueNames,
    notification_event_from_json,
    operational_alert_from_json,
)
from oziebot_common.worker_runtime import (
    DEFAULT_POLL_IDLE_SECONDS,
    run_postgres_queue_worker,
)
from oziebot_domain.events import NotificationEvent, NotificationEventType
from oziebot_domain.trading_mode import TradingMode

from oziebot_alerts_worker.adapters import SlackAdapter, SmsAdapter, TelegramAdapter
from oziebot_alerts_worker.config import get_settings
from oziebot_alerts_worker.service import NotificationService

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("alerts-worker")


def main() -> None:
    settings = get_settings()
    if not settings.database_url:
        raise RuntimeError("DATABASE_URL is required for alerts-worker")
    engine = create_engine(settings.database_url)
    keys = (
        QueueNames.all_alerts_keys()
        + QueueNames.all_alerts_retry_keys()
        + QueueNames.all_ops_alert_keys()
    )
    service = NotificationService(
        settings,
        engine,
        adapters={
            "sms": SmsAdapter(settings.sms_webhook_url),
            "slack": SlackAdapter(settings.slack_webhook_url),
            "telegram": TelegramAdapter(settings.telegram_bot_token),
        },
    )
    health = start_health_server("alerts-worker")
    stop_event = install_shutdown_handlers(
        "alerts-worker",
        health_state=health,
    )
    log.info("alerts-worker listening on %s", keys)

    def _handle_message(queue_key: str, raw: dict[str, object]) -> None:
        if queue_key == QueueNames.ops_alerts():
            alert = operational_alert_from_json(raw)
            service.route_operational_alert(alert)
            return
        if ":alerts_retry:" in queue_key:
            service.retry_delivery(raw)
            return
        try:
            event = notification_event_from_json(raw)
        except Exception:
            mode = TradingMode(str(raw.get("trading_mode") or "paper"))
            event = NotificationEvent(
                event_id=raw.get("event_id") or __import__("uuid").uuid4(),
                tenant_id=raw.get("tenant_id"),
                user_id=raw.get("user_id") or raw.get("tenant_id"),
                trading_mode=mode,
                event_type=NotificationEventType.DAILY_SUMMARY,
                message=str(raw.get("message") or "alert"),
                payload=raw,
            )
        service.route_event(event)

    run_postgres_queue_worker(
        worker_name="alerts-worker",
        engine=engine,
        queue_names=keys,
        stop_event=stop_event,
        health=health,
        handle_message=_handle_message,
        logger=log,
        poll_idle_seconds=DEFAULT_POLL_IDLE_SECONDS,
    )
    log.info("alerts-worker shutdown complete")


if __name__ == "__main__":
    main()
