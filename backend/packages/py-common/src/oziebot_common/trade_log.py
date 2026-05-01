from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Mapping

from sqlalchemy import text
from sqlalchemy.engine import Engine

from oziebot_common.s3_observability import get_observability_store

"""Trade log backed by Postgres table ``trade_raw_log`` when observability store is absent."""

MAX_TRADE_LOG_WINDOW_SECONDS = 120
MAX_TRADE_LOG_LIMIT = 200
DEFAULT_TRADE_LOG_RETENTION_SECONDS = 60
TRADE_LOG_REDIS_KEY = "oziebot:logs:trade"  # legacy identifier; persisted in Postgres
log = logging.getLogger("oziebot-trade-log")


def _trade_log_payload_sql_fragment(engine: Engine) -> str:
    return (
        "CAST(:payload AS jsonb)" if engine.dialect.name == "postgresql" else ":payload"
    )


def _trade_log_where_order_by_logical_ts(engine: Engine) -> tuple[str, str]:
    """Compare using embedded ISO timestamps so read windows match semantic event times."""
    if engine.dialect.name == "postgresql":
        ts = "(payload ->> 'timestamp')::timestamptz"
        return f"{ts} >= CAST(:min_time AS timestamptz)", f"{ts} DESC NULLS LAST"
    ts_expr = "json_extract(payload, '$.timestamp')"
    return f"{ts_expr} >= :min_iso", f"{ts_expr} DESC"


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
    engine: Engine | None,
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
    sym = str(event["symbol"]).upper()
    store = get_observability_store()
    if store is not None:
        try:
            store.append_trade_event(event)
        except Exception as exc:
            log.warning(
                "trade log write failed symbol=%s event_type=%s err=%s",
                event["symbol"],
                event["event_type"],
                exc,
            )
        return event

    if engine is None:
        return event

    cutoff = event_time - timedelta(seconds=clamped_retention)
    payload_json = json.dumps(event, separators=(",", ":"), default=str)
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    f"""
                    INSERT INTO trade_raw_log (symbol, payload)
                    VALUES (:sym, {_trade_log_payload_sql_fragment(engine)})
                    """
                ),
                {"sym": sym, "payload": payload_json},
            )
            cutoff_iso = cutoff.astimezone(UTC).isoformat()
            if engine.dialect.name == "postgresql":
                delete_stmt = (
                    "DELETE FROM trade_raw_log WHERE symbol = :sym AND "
                    "(payload ->> 'timestamp')::timestamptz < CAST(:cutoff_ts AS timestamptz)"
                )
                del_params = {"sym": sym, "cutoff_ts": cutoff}
            else:
                delete_stmt = (
                    "DELETE FROM trade_raw_log WHERE symbol = :sym AND "
                    "json_extract(payload, '$.timestamp') < :cutoff_iso"
                )
                del_params = {"sym": sym, "cutoff_iso": cutoff_iso}

            conn.execute(text(delete_stmt), del_params)
    except Exception as exc:
        log.warning(
            "trade log write failed symbol=%s event_type=%s err=%s",
            sym,
            event["event_type"],
            exc,
        )
    return event


def read_trade_log_events(
    engine: Engine | None,
    *,
    window_seconds: int = MAX_TRADE_LOG_WINDOW_SECONDS,
    limit: int = MAX_TRADE_LOG_LIMIT,
    symbol: str | None = None,
    event_type: str | None = None,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    clamped_window = max(1, min(int(window_seconds), MAX_TRADE_LOG_WINDOW_SECONDS))
    clamped_limit = max(1, min(int(limit), MAX_TRADE_LOG_LIMIT))
    store = get_observability_store()
    if store is not None:
        return store.read_trade_events(
            window_seconds=clamped_window,
            limit=clamped_limit,
            symbol=symbol,
            event_type=event_type,
            now=now,
        )
    if engine is None:
        return []

    current_time = (now or datetime.now(UTC)).astimezone(UTC)
    min_time = current_time - timedelta(seconds=clamped_window)
    normalized_symbol = str(symbol or "").strip().upper()
    normalized_event_type = str(event_type or "").strip().lower()

    sym_filter = normalized_symbol or None
    ts_where, order_by = _trade_log_where_order_by_logical_ts(engine)
    stmt = f"""
        SELECT payload FROM trade_raw_log
        WHERE {ts_where}
    """
    params: dict[str, Any] = {"lim": clamped_limit}
    if engine.dialect.name == "postgresql":
        params["min_time"] = min_time
    else:
        params["min_iso"] = min_time.astimezone(UTC).isoformat()
    if sym_filter:
        stmt += " AND symbol = :sym"
        params["sym"] = sym_filter
    stmt += f" ORDER BY {order_by} LIMIT :lim"

    try:
        with engine.connect() as conn:
            rows = list(conn.execute(text(stmt), params).all())
    except Exception as exc:
        log.warning("trade log read failed err=%s", exc)
        return []

    events: list[dict[str, Any]] = []
    for row in reversed(rows):
        raw_payload = row[0]
        if isinstance(raw_payload, dict):
            payload = raw_payload
        else:
            try:
                payload = (
                    json.loads(raw_payload)
                    if isinstance(raw_payload, str)
                    else json.loads(raw_payload.decode("utf-8"))
                )
            except (json.JSONDecodeError, AttributeError, TypeError):
                continue
        if not isinstance(payload, dict):
            continue
        ts = str(payload.get("timestamp") or "")
        sym = str(payload.get("symbol") or "").upper()
        et = str(payload.get("event_type") or "")
        msg = str(payload.get("message") or "")
        src = str(payload.get("source") or "coinbase").lower()
        if not ts or not sym or not et or not msg:
            continue
        if normalized_event_type and et.lower() != normalized_event_type:
            continue
        evt: dict[str, Any] = {
            "timestamp": ts,
            "symbol": sym,
            "event_type": et,
            "message": msg,
            "source": src,
        }
        d = payload.get("details")
        if isinstance(d, dict) and d:
            evt["details"] = d
        events.append(evt)

    return events[-clamped_limit:]
