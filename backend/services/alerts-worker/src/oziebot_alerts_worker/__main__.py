from __future__ import annotations

import logging

from oziebot_common.health import install_shutdown_handlers, start_health_server
from oziebot_common.queues import (
    QueueNames,
    disconnect_redis,
    notification_event_from_json,
)
from oziebot_common.worker_runtime import (
    DEFAULT_QUEUE_POP_TIMEOUT_SECONDS,
    redis_client_for_worker,
    run_redis_queue_worker,
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
    r = redis_client_for_worker(
        settings.redis_url,
        queue_pop_timeout_seconds=DEFAULT_QUEUE_POP_TIMEOUT_SECONDS,
    )
    keys = QueueNames.all_alerts_keys() + QueueNames.all_alerts_retry_keys()
    service = NotificationService(
        settings,
        r,
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
        on_shutdown=lambda: disconnect_redis(r),
    )
    log.info("alerts-worker listening on %s", keys)

    def _handle_message(queue_key: str, raw: dict[str, object]) -> None:
        if ":alerts_retry:" in queue_key:
            service.retry_delivery(raw)
            return
        try:
            event = notification_event_from_json(raw)
        except Exception:
            # Backward compatibility for legacy alert payloads.
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

    run_redis_queue_worker(
        worker_name="alerts-worker",
        redis_client=r,
        queue_keys=keys,
        stop_event=stop_event,
        health=health,
        handle_message=_handle_message,
        logger=log,
        queue_pop_timeout_seconds=DEFAULT_QUEUE_POP_TIMEOUT_SECONDS,
    )
    log.info("alerts-worker shutdown complete")


if __name__ == "__main__":
    main()
