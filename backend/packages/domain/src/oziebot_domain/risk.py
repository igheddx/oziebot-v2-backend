from enum import StrEnum
from uuid import UUID

from pydantic import Field

from oziebot_domain.trading_mode import TradingMode
from oziebot_domain.types import OziebotModel


class RejectionReason(StrEnum):
    LIMIT_EXCEEDED = "limit_exceeded"
    POSITION_CAP = "position_cap"
    DRAWDOWN = "drawdown"
    POLICY = "policy"
    UNKNOWN = "unknown"


class RiskOutcome(StrEnum):
    APPROVE = "approve"
    REDUCE_SIZE = "reduce_size"
    REJECT = "reject"


class RiskDecision(OziebotModel):
    """Result of centralized risk evaluation."""

    outcome: RiskOutcome
    approved: bool
    signal_id: UUID
    run_id: UUID
    user_id: UUID
    strategy_name: str
    symbol: str
    original_size: str
    final_size: str
    trading_mode: TradingMode
    reason: RejectionReason | None = Field(default=None)
    detail: str | None = None
    rules_evaluated: list[str] = Field(default_factory=list)
    trace_id: str
    metadata: dict = Field(default_factory=dict)
