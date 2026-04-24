from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import UUID

from pydantic import Field

from oziebot_domain.intents import TradeIntent
from oziebot_domain.risk import RiskDecision
from oziebot_domain.tenant import TenantId
from oziebot_domain.trading_mode import TradingMode
from oziebot_domain.types import OziebotModel


class DomainEvent(OziebotModel):
    """Base envelope for persisted or queued events."""

    event_id: UUID
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    tenant_id: TenantId
    name: str
    payload: dict[str, Any]
    trading_mode: TradingMode | None = Field(
        default=None,
        description="Set for trading-domain events; omit for billing/admin-only events.",
    )


class TradeIntentSubmitted(OziebotModel):
    intent: TradeIntent


class TradeIntentApproved(OziebotModel):
    intent: TradeIntent
    risk: RiskDecision


class TradeIntentRejected(OziebotModel):
    intent: TradeIntent
    risk: RiskDecision


class NotificationChannel(StrEnum):
    SMS = "sms"
    SLACK = "slack"
    TELEGRAM = "telegram"


class OperationalAlertSeverity(StrEnum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class NotificationEventType(StrEnum):
    TRADE_OPENED = "trade_opened"
    TRADE_CLOSED = "trade_closed"
    STOP_LOSS_HIT = "stop_loss_hit"
    TAKE_PROFIT_HIT = "take_profit_hit"
    STRATEGY_PAUSED = "strategy_paused"
    COINBASE_CONNECTION_ISSUE = "coinbase_connection_issue"
    INSUFFICIENT_BALANCE = "insufficient_balance"
    DAILY_SUMMARY = "daily_summary"


class NotificationEvent(OziebotModel):
    """Internal channel-agnostic event consumed by alerts worker."""

    event_id: UUID
    tenant_id: TenantId
    user_id: UUID
    trading_mode: TradingMode
    event_type: NotificationEventType
    trace_id: str | None = None
    title: str | None = None
    message: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class OperationalAlert(OziebotModel):
    """Internal operational alert consumed by alerts worker."""

    alert_id: UUID
    source_service: str
    alert_type: str
    severity: OperationalAlertSeverity
    title: str
    message: str
    payload: dict[str, Any] = Field(default_factory=dict)
    resolved: bool = False
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
