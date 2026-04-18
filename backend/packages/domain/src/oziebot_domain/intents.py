from uuid import UUID

from pydantic import Field

from oziebot_domain.tenant import TenantId
from oziebot_domain.trading import Instrument, OrderType, Quantity, Side
from oziebot_domain.trading_mode import TradingMode
from oziebot_domain.types import OziebotModel


class TradeIntent(OziebotModel):
    """Strategy output: desired trade, before risk and execution."""

    intent_id: UUID
    correlation_id: UUID
    tenant_id: TenantId
    trading_mode: TradingMode
    strategy_id: str = Field(..., min_length=1, max_length=128)
    instrument: Instrument
    side: Side
    order_type: OrderType
    quantity: Quantity
