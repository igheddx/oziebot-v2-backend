from decimal import Decimal
from enum import StrEnum

from pydantic import Field

from oziebot_domain.types import OziebotModel


class Side(StrEnum):
    BUY = "buy"
    SELL = "sell"


class Venue(StrEnum):
    COINBASE = "coinbase"


class OrderType(StrEnum):
    MARKET = "market"
    LIMIT = "limit"


class Instrument(OziebotModel):
    """Trading pair (e.g. BTC-USD on Coinbase)."""

    symbol: str = Field(..., min_length=3, max_length=32)


class Quantity(OziebotModel):
    amount: Decimal = Field(..., gt=0)


class Price(OziebotModel):
    """Optional for market orders."""

    amount: Decimal = Field(..., gt=0)
