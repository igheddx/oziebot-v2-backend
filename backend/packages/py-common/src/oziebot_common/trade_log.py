from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

TRADE_LOG_REDIS_KEY = "oziebot:logs:trade"
MAX_TRADE_LOG_WINDOW_SECONDS = 120
MAX_TRADE_LOG_LIMIT = 200


def build_trade_log_event(
    *,
    symbol: str,
    event_type: str,
    message: str,
    timestamp: datetime | None = None,
) -> dict[str, str]:
    event_time = (timestamp or datetime.now(UTC)).astimezone(UTC)
    return {
        "timestamp": event_time.isoformat(),
        "symbol": str(symbol).upper(),
        "event_type": str(event_type),
        "message": str(message),
    }


def append_trade_log_event(
    client: Any,
    *,
    symbol: str,
    event_type: str,
    message: str,
    timestamp: datetime | None = None,
    retention_seconds: int = MAX_TRADE_LOG_WINDOW_SECONDS,
) -> dict[str, str]:
    clamped_retention = max(
        1, min(int(retention_seconds), MAX_TRADE_LOG_WINDOW_SECONDS)
    )
    event = build_trade_log_event(
        symbol=symbol,
        event_type=event_type,
        message=message,
        timestamp=timestamp,
    )
    event_time = datetime.fromisoformat(event["timestamp"])
    score = event_time.timestamp()
    cutoff = (event_time - timedelta(seconds=clamped_retention)).timestamp()
    payload = json.dumps(event, separators=(",", ":"))

    pipeline = client.pipeline()
    pipeline.zadd(TRADE_LOG_REDIS_KEY, {payload: score})
    pipeline.zremrangebyscore(TRADE_LOG_REDIS_KEY, "-inf", cutoff)
    pipeline.expire(TRADE_LOG_REDIS_KEY, clamped_retention + 30)
    pipeline.execute()
    return event


def read_trade_log_events(
    client: Any,
    *,
    window_seconds: int = MAX_TRADE_LOG_WINDOW_SECONDS,
    limit: int = MAX_TRADE_LOG_LIMIT,
    now: datetime | None = None,
) -> list[dict[str, str]]:
    clamped_window = max(1, min(int(window_seconds), MAX_TRADE_LOG_WINDOW_SECONDS))
    clamped_limit = max(1, min(int(limit), MAX_TRADE_LOG_LIMIT))
    current_time = (now or datetime.now(UTC)).astimezone(UTC)
    min_score = (current_time - timedelta(seconds=clamped_window)).timestamp()
    rows = client.zrevrangebyscore(
        TRADE_LOG_REDIS_KEY,
        "+inf",
        min_score,
        start=0,
        num=clamped_limit,
    )

    events: list[dict[str, str]] = []
    for raw in reversed(rows):
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        timestamp = str(payload.get("timestamp") or "")
        symbol = str(payload.get("symbol") or "").upper()
        event_type = str(payload.get("event_type") or "")
        message = str(payload.get("message") or "")
        if not timestamp or not symbol or not event_type or not message:
            continue
        events.append(
            {
                "timestamp": timestamp,
                "symbol": symbol,
                "event_type": event_type,
                "message": message,
            }
        )
    return events
