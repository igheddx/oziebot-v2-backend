"""Shared domain model: pure types, no infrastructure imports."""

from oziebot_domain.backtesting import (
    BacktestCandle,
    BacktestConfig,
    BacktestRunRequest,
    BacktestRunResult,
    BacktestSnapshotScope,
    BacktestTrade,
    PerformanceSnapshot,
    StrategyAnalyticsArtifact,
)
from oziebot_domain.events import (
    DomainEvent,
    NotificationChannel,
    NotificationEvent,
    NotificationEventType,
    TradeIntentApproved,
    TradeIntentRejected,
    TradeIntentSubmitted,
)
from oziebot_domain.execution import (
    ExecutionEvent,
    ExecutionFill,
    ExecutionOrderStatus,
    ExecutionRequest,
    ExecutionSubmission,
    OrderRef,
)
from oziebot_domain.identity import Role, UserId
from oziebot_domain.intents import TradeIntent
from oziebot_domain.market_data import (
    MarketDataSource,
    NormalizedBestBidAsk,
    NormalizedCandle,
    NormalizedOrderBookTop,
    NormalizedTrade,
)
from oziebot_domain.risk import RejectionReason, RiskDecision
from oziebot_domain.signal_pipeline import StrategySignalEvent
from oziebot_domain.strategy import SignalType, StrategyPerformanceMetrics, StrategySignal
from oziebot_domain.tenant import TenantId
from oziebot_domain.trading_mode import TradingMode

__all__ = [
    "BacktestCandle",
    "BacktestConfig",
    "BacktestRunRequest",
    "BacktestRunResult",
    "BacktestSnapshotScope",
    "BacktestTrade",
    "DomainEvent",
    "NotificationChannel",
    "NotificationEvent",
    "NotificationEventType",
    "ExecutionEvent",
    "ExecutionFill",
    "ExecutionOrderStatus",
    "ExecutionRequest",
    "ExecutionSubmission",
    "MarketDataSource",
    "NormalizedBestBidAsk",
    "NormalizedCandle",
    "NormalizedOrderBookTop",
    "NormalizedTrade",
    "OrderRef",
    "PerformanceSnapshot",
    "RejectionReason",
    "RiskDecision",
    "Role",
    "SignalType",
    "StrategySignalEvent",
    "StrategyPerformanceMetrics",
    "StrategyAnalyticsArtifact",
    "StrategySignal",
    "TenantId",
    "TradingMode",
    "TradeIntent",
    "TradeIntentApproved",
    "TradeIntentRejected",
    "TradeIntentSubmitted",
    "UserId",
]
