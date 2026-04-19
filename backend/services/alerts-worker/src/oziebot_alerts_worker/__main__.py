from __future__ import annotations

import logging

from redis import RedisError

from oziebot_common.health import install_shutdown_handlers, start_health_server
from oziebot_common.queues import (
    QueueNames,
    brpop_json_any,
    disconnect_redis,
    notification_event_from_json,
    redis_from_url,
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
    r = redis_from_url(
        settings.redis_url, probe=True, socket_connect_timeout=3, socket_timeout=3
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
    health.mark_ready()
    while not stop_event.is_set():
        try:
            got = brpop_json_any(r, keys, timeout=5)
        except RedisError:
            if stop_event.is_set():
                break
            raise
        health.touch()
        if got is None:
            continue
        queue_key, raw = got
        if ":alerts_retry:" in queue_key:
            service.retry_delivery(raw)
            continue
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
        health.touch()
    log.info("alerts-worker shutdown complete")


if __name__ == "__main__":
    main()
