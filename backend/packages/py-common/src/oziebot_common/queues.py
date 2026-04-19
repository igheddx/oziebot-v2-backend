from __future__ import annotations

import json
from urllib.parse import SplitResult, urlsplit, urlunsplit
from typing import Any

import redis
from pydantic import TypeAdapter

from oziebot_domain.events import NotificationEvent
from oziebot_domain.execution import ExecutionEvent
from oziebot_domain.intents import TradeIntent
from oziebot_domain.risk import RiskDecision
from oziebot_domain.signal_pipeline import StrategySignalEvent
from oziebot_domain.trading_mode import TradingMode


class QueueNames:
    """Redis list keys partitioned by TradingMode so PAPER and LIVE never share a queue."""

    @staticmethod
    def intent_submitted(mode: TradingMode) -> str:
        return f"oziebot:queue:intent_submitted:{mode.value}"

    @staticmethod
    def intent_approved(mode: TradingMode) -> str:
        return f"oziebot:queue:intent_approved:{mode.value}"

    @staticmethod
    def intent_rejected(mode: TradingMode) -> str:
        return f"oziebot:queue:intent_rejected:{mode.value}"

    @staticmethod
    def alerts(mode: TradingMode) -> str:
        return f"oziebot:queue:alerts:{mode.value}"

    @staticmethod
    def alerts_retry(mode: TradingMode) -> str:
        return f"oziebot:queue:alerts_retry:{mode.value}"

    @staticmethod
    def execution_events(mode: TradingMode) -> str:
        return f"oziebot:queue:execution_events:{mode.value}"

    @staticmethod
    def execution_reconciliation(mode: TradingMode) -> str:
        return f"oziebot:queue:execution_reconciliation:{mode.value}"

    @staticmethod
    def signal_generated(mode: TradingMode) -> str:
        return f"oziebot:queue:signal_generated:{mode.value}"

    @staticmethod
    def all_intent_submitted_keys() -> list[str]:
        return [QueueNames.intent_submitted(m) for m in TradingMode]

    @staticmethod
    def all_intent_approved_keys() -> list[str]:
        return [QueueNames.intent_approved(m) for m in TradingMode]

    @staticmethod
    def all_alerts_keys() -> list[str]:
        return [QueueNames.alerts(m) for m in TradingMode]

    @staticmethod
    def all_alerts_retry_keys() -> list[str]:
        return [QueueNames.alerts_retry(m) for m in TradingMode]

    @staticmethod
    def all_execution_event_keys() -> list[str]:
        return [QueueNames.execution_events(m) for m in TradingMode]

    @staticmethod
    def all_signal_generated_keys() -> list[str]:
        return [QueueNames.signal_generated(m) for m in TradingMode]


def redis_url_candidates(url: str) -> list[str]:
    stripped = url.strip()
    if not stripped:
        return [stripped]

    parsed = urlsplit(stripped)
    if parsed.scheme not in {"redis", "rediss"}:
        return [stripped]

    candidates = [stripped]
    hostname = (parsed.hostname or "").lower()
    if hostname.endswith(".cache.amazonaws.com"):
        alternate_scheme = "rediss" if parsed.scheme == "redis" else "redis"
        alternate = urlunsplit(
            SplitResult(
                scheme=alternate_scheme,
                netloc=parsed.netloc,
                path=parsed.path,
                query=parsed.query,
                fragment=parsed.fragment,
            )
        )
        if alternate not in candidates:
            candidates.append(alternate)
    return candidates


def redis_from_url(
    url: str,
    *,
    probe: bool = False,
    **kwargs: Any,
) -> redis.Redis:
    last_error: Exception | None = None
    for candidate in redis_url_candidates(url):
        try:
            client = redis.Redis.from_url(candidate, decode_responses=True, **kwargs)
            if probe:
                client.ping()
            return client
        except (redis.RedisError, ValueError) as exc:
            last_error = exc
    if last_error is not None:
        raise last_error
    raise ValueError("Redis URL must not be empty")


def push_json(r: redis.Redis, key: str, payload: dict[str, Any]) -> None:
    r.lpush(key, json.dumps(payload, default=str))


def brpop_json(r: redis.Redis, key: str, timeout: int = 5) -> dict[str, Any] | None:
    item = r.brpop(key, timeout=timeout)
    if item is None:
        return None
    _, raw = item
    return json.loads(raw)


def brpop_json_any(
    r: redis.Redis, keys: list[str], timeout: int = 5
) -> tuple[str, dict[str, Any]] | None:
    """Block on the first available message across mode-specific queues."""
    if not keys:
        return None
    item = r.brpop(keys, timeout=timeout)
    if item is None:
        return None
    key, raw = item
    return key, json.loads(raw)


def disconnect_redis(r: redis.Redis) -> None:
    try:
        r.close()
    finally:
        r.connection_pool.disconnect()


_intent_adapter = TypeAdapter(TradeIntent)
_risk_adapter = TypeAdapter(RiskDecision)
_signal_adapter = TypeAdapter(StrategySignalEvent)
_execution_event_adapter = TypeAdapter(ExecutionEvent)
_notification_event_adapter = TypeAdapter(NotificationEvent)


def trade_intent_to_json(intent: TradeIntent) -> dict[str, Any]:
    return intent.model_dump(mode="json")


def trade_intent_from_json(data: dict[str, Any]) -> TradeIntent:
    return _intent_adapter.validate_python(data)


def risk_decision_to_json(decision: RiskDecision) -> dict[str, Any]:
    return decision.model_dump(mode="json")


def risk_decision_from_json(data: dict[str, Any]) -> RiskDecision:
    return _risk_adapter.validate_python(data)


def strategy_signal_to_json(signal: StrategySignalEvent) -> dict[str, Any]:
    return signal.model_dump(mode="json")


def strategy_signal_from_json(data: dict[str, Any]) -> StrategySignalEvent:
    return _signal_adapter.validate_python(data)


def execution_event_to_json(event: ExecutionEvent) -> dict[str, Any]:
    return event.model_dump(mode="json")


def execution_event_from_json(data: dict[str, Any]) -> ExecutionEvent:
    return _execution_event_adapter.validate_python(data)


def notification_event_to_json(event: NotificationEvent) -> dict[str, Any]:
    return event.model_dump(mode="json")


def notification_event_from_json(data: dict[str, Any]) -> NotificationEvent:
    return _notification_event_adapter.validate_python(data)
