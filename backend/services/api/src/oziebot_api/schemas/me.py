from typing import Literal
import uuid
from datetime import datetime

from pydantic import BaseModel, Field

from oziebot_domain.trading_mode import TradingMode


class TenantBrief(BaseModel):
    id: uuid.UUID
    name: str
    role: str


class ProductBrief(BaseModel):
    product_key: str
    display_name: str
    status: Literal["active", "trial", "disabled"]
    is_default: bool


class MeOut(BaseModel):
    id: uuid.UUID
    email: str
    full_name: str | None = None
    role: str
    is_root_admin: bool
    current_trading_mode: TradingMode
    email_verified_at: datetime | None
    tenants: list[TenantBrief]
    products: list[ProductBrief] = Field(default_factory=list)
    default_product: str | None = None


class ProductsOut(BaseModel):
    products: list[ProductBrief] = Field(default_factory=list)
    default_product: str | None = None


class DefaultProductPatch(BaseModel):
    product_key: str = Field(min_length=1, max_length=64)


class TradingModePatch(BaseModel):
    trading_mode: TradingMode = Field(
        description="PAPER works without exchange credentials; LIVE requires a valid Coinbase connection"
    )
