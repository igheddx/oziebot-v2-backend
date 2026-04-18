from __future__ import annotations

from datetime import UTC, datetime

from fastapi import APIRouter, HTTPException

from oziebot_api.deps import DbSession
from oziebot_api.deps.auth import CurrentUser
from oziebot_api.models.notification import NotificationChannelConfig, NotificationPreference
from oziebot_api.schemas.alerts import (
    AlertChannelConfigOut,
    AlertChannelConfigUpsert,
    AlertsConfigOut,
    AlertPreferenceOut,
    AlertPreferenceUpsert,
)
from oziebot_domain.events import NotificationChannel, NotificationEventType

router = APIRouter(prefix="/me/alerts", tags=["alerts"])


@router.get("/config", response_model=AlertsConfigOut)
def get_alert_config(user: CurrentUser, db: DbSession) -> AlertsConfigOut:
    channels = (
        db.query(NotificationChannelConfig)
        .filter(NotificationChannelConfig.user_id == user.id)
        .order_by(NotificationChannelConfig.channel)
        .all()
    )
    preferences = (
        db.query(NotificationPreference)
        .filter(NotificationPreference.user_id == user.id)
        .order_by(NotificationPreference.event_type, NotificationPreference.trading_mode)
        .all()
    )
    return AlertsConfigOut(
        channels=[
            AlertChannelConfigOut(
                id=row.id,
                channel=row.channel,
                destination=row.destination,
                is_enabled=row.is_enabled,
                settings=row.settings_json,
                updated_at=row.updated_at,
            )
            for row in channels
        ],
        preferences=[
            AlertPreferenceOut(
                id=row.id,
                event_type=row.event_type,
                trading_mode=row.trading_mode,
                is_enabled=row.is_enabled,
                updated_at=row.updated_at,
            )
            for row in preferences
        ],
        supported_channels=[c.value for c in NotificationChannel],
        supported_event_types=[e.value for e in NotificationEventType],
    )


@router.put("/channels/{channel}", response_model=AlertChannelConfigOut)
def upsert_channel(channel: str, body: AlertChannelConfigUpsert, user: CurrentUser, db: DbSession) -> AlertChannelConfigOut:
    if channel not in {c.value for c in NotificationChannel}:
        raise HTTPException(status_code=400, detail="Unsupported channel")
    row = (
        db.query(NotificationChannelConfig)
        .filter(
            NotificationChannelConfig.user_id == user.id,
            NotificationChannelConfig.channel == channel,
        )
        .first()
    )
    now = datetime.now(UTC)
    if row is None:
        row = NotificationChannelConfig(
            user_id=user.id,
            channel=channel,
            destination=body.destination,
            is_enabled=body.is_enabled,
            settings_json=body.settings,
            created_at=now,
            updated_at=now,
        )
    else:
        row.destination = body.destination
        row.is_enabled = body.is_enabled
        row.settings_json = body.settings
        row.updated_at = now
    db.add(row)
    db.commit()
    db.refresh(row)
    return AlertChannelConfigOut(
        id=row.id,
        channel=row.channel,
        destination=row.destination,
        is_enabled=row.is_enabled,
        settings=row.settings_json,
        updated_at=row.updated_at,
    )


@router.put("/preferences/{event_type}", response_model=AlertPreferenceOut)
def upsert_preference(
    event_type: str,
    body: AlertPreferenceUpsert,
    user: CurrentUser,
    db: DbSession,
) -> AlertPreferenceOut:
    if event_type not in {e.value for e in NotificationEventType}:
        raise HTTPException(status_code=400, detail="Unsupported event type")
    row = (
        db.query(NotificationPreference)
        .filter(
            NotificationPreference.user_id == user.id,
            NotificationPreference.event_type == event_type,
            NotificationPreference.trading_mode == body.trading_mode,
        )
        .first()
    )
    now = datetime.now(UTC)
    if row is None:
        row = NotificationPreference(
            user_id=user.id,
            event_type=event_type,
            trading_mode=body.trading_mode,
            is_enabled=body.is_enabled,
            created_at=now,
            updated_at=now,
        )
    else:
        row.is_enabled = body.is_enabled
        row.updated_at = now
    db.add(row)
    db.commit()
    db.refresh(row)
    return AlertPreferenceOut(
        id=row.id,
        event_type=row.event_type,
        trading_mode=row.trading_mode,
        is_enabled=row.is_enabled,
        updated_at=row.updated_at,
    )