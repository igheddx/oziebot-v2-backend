from uuid import UUID

from pydantic import Field

from oziebot_domain.trading_mode import TradingMode
from oziebot_domain.types import OziebotModel

TenantId = UUID


class TenantSnapshot(OziebotModel):
    """Tenant reference for cross-aggregate use (no I/O)."""

    id: TenantId = Field(..., description="Tenant identifier")
    name: str = Field(..., min_length=1, max_length=256)
    default_trading_mode: TradingMode = Field(
        default=TradingMode.PAPER,
        description="UI default only; persisted trading rows still key by trading_mode.",
    )
