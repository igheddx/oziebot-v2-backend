from __future__ import annotations

from typing import Any

from pydantic import TypeAdapter

from oziebot_domain.events import NotificationEvent, OperationalAlert
from oziebot_domain.execution import ExecutionEvent
from oziebot_domain.intents import TradeIntent
from oziebot_domain.risk import RiskDecision
from oziebot_domain.signal_pipeline import StrategySignalEvent
from oziebot_domain.trading_mode import TradingMode


class QueueNames:
    """Logical Postgres outbox queues partitioned by TradingMode."""

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
    def ops_alerts() -> str:
        return "oziebot:queue:ops_alerts"

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
    def all_ops_alert_keys() -> list[str]:
        return [QueueNames.ops_alerts()]

    @staticmethod
    def all_execution_event_keys() -> list[str]:
        return [QueueNames.execution_events(m) for m in TradingMode]

    @staticmethod
    def all_execution_reconciliation_keys() -> list[str]:
        return [QueueNames.execution_reconciliation(m) for m in TradingMode]

    @staticmethod
    def all_signal_generated_keys() -> list[str]:
        return [QueueNames.signal_generated(m) for m in TradingMode]


BOUNDED_QUEUE_MAX_LENGTH = 200

_intent_adapter = TypeAdapter(TradeIntent)
_risk_adapter = TypeAdapter(RiskDecision)
_signal_adapter = TypeAdapter(StrategySignalEvent)
_execution_event_adapter = TypeAdapter(ExecutionEvent)
_notification_event_adapter = TypeAdapter(NotificationEvent)
_operational_alert_adapter = TypeAdapter(OperationalAlert)


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


def operational_alert_to_json(alert: OperationalAlert) -> dict[str, Any]:
    return alert.model_dump(mode="json")


def operational_alert_from_json(data: dict[str, Any]) -> OperationalAlert:
    return _operational_alert_adapter.validate_python(data)
