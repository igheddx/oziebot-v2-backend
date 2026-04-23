from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Mapping

TRADE_LOG_REDIS_KEY = "oziebot:logs:trade"
MAX_TRADE_LOG_WINDOW_SECONDS = 120
MAX_TRADE_LOG_LIMIT = 200
DEFAULT_TRADE_LOG_RETENTION_SECONDS = 60


def build_trade_log_event(
    *,
    symbol: str,
    event_type: str,
    message: str,
    timestamp: datetime | None = None,
    source: str = "coinbase",
    details: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    event_time = (timestamp or datetime.now(UTC)).astimezone(UTC)
    event: dict[str, Any] = {
        "timestamp": event_time.isoformat(),
        "symbol": str(symbol).upper(),
        "event_type": str(event_type),
        "message": str(message),
        "source": str(source).lower(),
    }
    normalized_details = normalize_trade_log_payload(details)
    if normalized_details:
        event["details"] = normalized_details
    return event


def normalize_trade_log_payload(details: Mapping[str, Any] | None) -> dict[str, Any]:
    if not details:
        return {}

    normalized: dict[str, Any] = {}
    for key, value in details.items():
        if value is None:
            continue
        if isinstance(value, Decimal):
            normalized[str(key)] = format(value.normalize(), "f")
        elif isinstance(value, datetime):
            normalized[str(key)] = value.astimezone(UTC).isoformat()
        elif isinstance(value, bool | int | float | str):
            normalized[str(key)] = value
        elif isinstance(value, Mapping):
            nested = normalize_trade_log_payload(value)
            if nested:
                normalized[str(key)] = nested
        elif isinstance(value, list | tuple):
            items: list[Any] = []
            for item in value:
                if isinstance(item, Decimal):
                    items.append(format(item.normalize(), "f"))
                elif isinstance(item, datetime):
                    items.append(item.astimezone(UTC).isoformat())
                elif isinstance(item, bool | int | float | str):
                    items.append(item)
                else:
                    items.append(str(item))
            if items:
                normalized[str(key)] = items
        else:
            normalized[str(key)] = str(value)
    return normalized


def append_trade_log_event(
    client: Any,
    *,
    symbol: str,
    event_type: str,
    message: str,
    timestamp: datetime | None = None,
    source: str = "coinbase",
    details: Mapping[str, Any] | None = None,
    retention_seconds: int = DEFAULT_TRADE_LOG_RETENTION_SECONDS,
) -> dict[str, Any]:
    clamped_retention = max(
        1, min(int(retention_seconds), MAX_TRADE_LOG_WINDOW_SECONDS)
    )
    event = build_trade_log_event(
        symbol=symbol,
        event_type=event_type,
        message=message,
        timestamp=timestamp,
        source=source,
        details=details,
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
    symbol: str | None = None,
    event_type: str | None = None,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    clamped_window = max(1, min(int(window_seconds), MAX_TRADE_LOG_WINDOW_SECONDS))
    clamped_limit = max(1, min(int(limit), MAX_TRADE_LOG_LIMIT))
    current_time = (now or datetime.now(UTC)).astimezone(UTC)
    min_score = (current_time - timedelta(seconds=clamped_window)).timestamp()
    normalized_symbol = str(symbol or "").strip().upper()
    normalized_event_type = str(event_type or "").strip().lower()
    rows = client.zrevrangebyscore(
        TRADE_LOG_REDIS_KEY,
        "+inf",
        min_score,
        start=0,
        num=clamped_limit,
    )

    events: list[dict[str, Any]] = []
    for raw in reversed(rows):
        try:
            payload = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        timestamp = str(payload.get("timestamp") or "")
        symbol = str(payload.get("symbol") or "").upper()
        event_type = str(payload.get("event_type") or "")
        message = str(payload.get("message") or "")
        source = str(payload.get("source") or "coinbase").lower()
        if not timestamp or not symbol or not event_type or not message:
            continue
        if normalized_symbol and symbol != normalized_symbol:
            continue
        if normalized_event_type and event_type.lower() != normalized_event_type:
            continue
        event: dict[str, Any] = {
            "timestamp": timestamp,
            "symbol": symbol,
            "event_type": event_type,
            "message": message,
            "source": source,
        }
        details = payload.get("details")
        if isinstance(details, dict) and details:
            event["details"] = details
        events.append(event)
    return events
