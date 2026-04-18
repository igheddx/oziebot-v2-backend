from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, Field


AlertChannel = Literal["sms", "slack", "telegram"]
AlertTradingMode = Literal["paper", "live", "all"]


class AlertChannelConfigUpsert(BaseModel):
    destination: str = Field(min_length=1, max_length=256)
    is_enabled: bool = True
    settings: dict[str, Any] | None = None


class AlertChannelConfigOut(BaseModel):
    id: UUID
    channel: AlertChannel
    destination: str
    is_enabled: bool
    settings: dict[str, Any] | None
    updated_at: datetime


class AlertPreferenceUpsert(BaseModel):
    trading_mode: AlertTradingMode
    is_enabled: bool


class AlertPreferenceOut(BaseModel):
    id: UUID
    event_type: str
    trading_mode: AlertTradingMode
    is_enabled: bool
    updated_at: datetime


class AlertsConfigOut(BaseModel):
    channels: list[AlertChannelConfigOut]
    preferences: list[AlertPreferenceOut]
    supported_channels: list[AlertChannel]
    supported_event_types: list[str]