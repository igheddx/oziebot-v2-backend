from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
import redis

from oziebot_api.config import Settings
from oziebot_api.deps import settings_dep
from oziebot_api.deps.auth import CurrentUser
from oziebot_common.trade_log import (
    MAX_TRADE_LOG_LIMIT,
    MAX_TRADE_LOG_WINDOW_SECONDS,
    read_trade_log_events,
)

router = APIRouter(prefix="/logs", tags=["logs"])


@router.get("/trade")
def get_trade_log(
    _user: CurrentUser,
    window_seconds: int = Query(default=120, ge=1, le=MAX_TRADE_LOG_WINDOW_SECONDS),
    limit: int = Query(default=200, ge=1, le=MAX_TRADE_LOG_LIMIT),
    settings: Settings = Depends(settings_dep),
) -> dict[str, object]:
    client = redis.Redis.from_url(
        settings.redis_url,
        decode_responses=True,
        socket_connect_timeout=1,
        socket_timeout=1,
    )
    try:
        events = read_trade_log_events(
            client,
            window_seconds=window_seconds,
            limit=limit,
        )
    except redis.RedisError as exc:
        raise HTTPException(
            status_code=503, detail="Trade log temporarily unavailable"
        ) from exc
    return {
        "window_seconds": window_seconds,
        "limit": limit,
        "count": len(events),
        "events": events,
    }
