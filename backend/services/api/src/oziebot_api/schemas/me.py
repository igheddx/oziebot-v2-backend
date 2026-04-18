import uuid
from datetime import datetime

from pydantic import BaseModel, Field

from oziebot_domain.trading_mode import TradingMode


class TenantBrief(BaseModel):
    id: uuid.UUID
    name: str
    role: str


class MeOut(BaseModel):
    id: uuid.UUID
    email: str
    role: str
    is_root_admin: bool
    current_trading_mode: TradingMode
    email_verified_at: datetime | None
    tenants: list[TenantBrief]


class TradingModePatch(BaseModel):
    trading_mode: TradingMode = Field(
        description="PAPER works without exchange credentials; LIVE requires a valid Coinbase connection"
    )
