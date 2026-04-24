from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import redis
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.models.execution import ExecutionOrder, ExecutionTradeRecord
from oziebot_api.models.market_data import MarketDataBboSnapshot, MarketDataTradeSnapshot
from oziebot_api.models.risk_event import RiskEvent
from oziebot_api.models.trade_intelligence import StrategySignalSnapshot
from oziebot_common import redis_from_url
from oziebot_common.queues import disconnect_redis
from oziebot_common.runtime_status import read_runtime_statuses

log = logging.getLogger("oziebot-runtime-status")

RUNTIME_SERVICES: tuple[dict[str, str], ...] = (
    {
        "service": "market-data-ingestor",
        "label": "Market Data",
        "description": "Coinbase market data ingestion and freshness monitoring.",
    },
    {
        "service": "strategy-engine",
        "label": "Strategy Engine",
        "description": "Signal evaluation across enabled strategies.",
    },
    {
        "service": "risk-engine",
        "label": "Risk Engine",
        "description": "Trade gating, sizing caps, and compliance checks.",
    },
    {
        "service": "execution-engine",
        "label": "Execution Engine",
        "description": "Paper/live order placement and reconciliation.",
    },
    {
        "service": "alerts-worker",
        "label": "Alerts Worker",
        "description": "Slack, SMS, and Telegram delivery.",
    },
)


def _normalize_runtime_service(
    definition: dict[str, str],
    snapshot: dict[str, object] | None,
) -> dict[str, Any]:
    if snapshot is None:
        return {
            **definition,
            "level": "unknown",
            "status": "missing",
            "ready": False,
            "degraded": False,
            "degraded_reason": None,
            "started_at": None,
            "last_heartbeat_at": None,
            "heartbeat_age_seconds": None,
            "stale_after_seconds": None,
            "details": {},
        }

    status = str(snapshot.get("status") or "unknown")
    ready = bool(snapshot.get("ready"))
    degraded = bool(snapshot.get("degraded"))
    if status == "stale":
        level = "critical"
    elif degraded or status == "degraded":
        level = "warning"
    elif status == "ok" and ready:
        level = "healthy"
    elif status == "ok":
        level = "warning"
    else:
        level = "unknown"
    return {
        **definition,
        "level": level,
        "status": status,
        "ready": ready,
        "degraded": degraded,
        "degraded_reason": snapshot.get("degraded_reason"),
        "started_at": snapshot.get("started_at"),
        "last_heartbeat_at": snapshot.get("last_heartbeat_at"),
        "heartbeat_age_seconds": snapshot.get("heartbeat_age_seconds"),
        "stale_after_seconds": snapshot.get("stale_after_seconds"),
        "details": snapshot.get("details") if isinstance(snapshot.get("details"), dict) else {},
    }


def _read_runtime_registry(settings: Settings) -> tuple[dict[str, dict[str, object]], str | None]:
    client = None
    try:
        client = redis_from_url(
            settings.redis_url,
            probe=True,
            socket_connect_timeout=1,
            socket_timeout=1,
        )
        snapshots = read_runtime_statuses(
            client,
            [service["service"] for service in RUNTIME_SERVICES],
        )
        return snapshots, None
    except (redis.RedisError, ValueError) as exc:
        log.warning("runtime status registry unavailable", exc_info=True)
        return {}, str(exc)
    finally:
        if client is not None:
            disconnect_redis(client)


def _aggregate_mode_activity(
    db: Session,
    model: Any,
    timestamp_column: Any,
    *,
    window_start: datetime,
) -> dict[str, dict[str, Any]]:
    rows = db.execute(
        select(
            model.trading_mode,
            func.count(),
            func.max(timestamp_column),
        )
        .where(timestamp_column >= window_start)
        .group_by(model.trading_mode)
    ).all()
    activity: dict[str, dict[str, Any]] = {
        "paper": {"count": 0, "last_at": None},
        "live": {"count": 0, "last_at": None},
    }
    for mode, count, last_at in rows:
        if mode not in activity:
            continue
        activity[str(mode)] = {
            "count": int(count or 0),
            "last_at": last_at.isoformat() if last_at is not None else None,
        }
    return activity


def _aggregate_market_data_activity(
    db: Session,
    *,
    window_start: datetime,
) -> dict[str, Any]:
    trade_count, latest_trade_at = db.execute(
        select(
            func.count(),
            func.max(MarketDataTradeSnapshot.event_time),
        ).where(MarketDataTradeSnapshot.event_time >= window_start)
    ).one()
    bbo_count, latest_bbo_at = db.execute(
        select(
            func.count(),
            func.max(MarketDataBboSnapshot.event_time),
        ).where(MarketDataBboSnapshot.event_time >= window_start)
    ).one()
    latest_activity = max(
        [value for value in (latest_trade_at, latest_bbo_at) if value is not None],
        default=None,
    )
    return {
        "trade_ticks": int(trade_count or 0),
        "bbo_updates": int(bbo_count or 0),
        "last_at": latest_activity.isoformat() if latest_activity is not None else None,
    }


def build_runtime_status_payload(settings: Settings, db: Session) -> dict[str, Any]:
    now = datetime.now(UTC)
    window_minutes = 15
    window_start = now - timedelta(minutes=window_minutes)
    snapshots, registry_error = _read_runtime_registry(settings)
    services = [
        _normalize_runtime_service(definition, snapshots.get(definition["service"]))
        for definition in RUNTIME_SERVICES
    ]
    healthy_count = sum(1 for service in services if service["level"] == "healthy")
    warning_count = sum(1 for service in services if service["level"] == "warning")
    critical_count = sum(1 for service in services if service["level"] == "critical")
    unknown_count = sum(1 for service in services if service["level"] == "unknown")
    if critical_count:
        overall_status = "critical"
    elif warning_count or registry_error:
        overall_status = "warning"
    elif healthy_count:
        overall_status = "healthy"
    else:
        overall_status = "unknown"

    market_data = _aggregate_market_data_activity(db, window_start=window_start)
    strategy = _aggregate_mode_activity(
        db,
        StrategySignalSnapshot,
        StrategySignalSnapshot.timestamp,
        window_start=window_start,
    )
    risk = _aggregate_mode_activity(
        db,
        RiskEvent,
        RiskEvent.created_at,
        window_start=window_start,
    )
    execution_orders = _aggregate_mode_activity(
        db,
        ExecutionOrder,
        ExecutionOrder.created_at,
        window_start=window_start,
    )
    execution_trades = _aggregate_mode_activity(
        db,
        ExecutionTradeRecord,
        ExecutionTradeRecord.executed_at,
        window_start=window_start,
    )
    pipeline_active = any(
        item["count"] > 0
        for item in (
            strategy["paper"],
            strategy["live"],
            risk["paper"],
            risk["live"],
            execution_orders["paper"],
            execution_orders["live"],
        )
    )
    trade_active = any(
        item["count"] > 0 for item in (execution_trades["paper"], execution_trades["live"])
    )
    if critical_count:
        pipeline_status = "problem"
    elif (
        pipeline_active or trade_active or market_data["trade_ticks"] or market_data["bbo_updates"]
    ):
        pipeline_status = "active"
    else:
        pipeline_status = "idle"

    return {
        "generated_at": now.isoformat(),
        "window_minutes": window_minutes,
        "overall_status": overall_status,
        "pipeline_status": pipeline_status,
        "registry": {
            "connected": registry_error is None,
            "error": registry_error,
        },
        "summary": {
            "healthy_services": healthy_count,
            "warning_services": warning_count,
            "critical_services": critical_count,
            "unknown_services": unknown_count,
            "paper_orders_recent": execution_orders["paper"]["count"],
            "live_orders_recent": execution_orders["live"]["count"],
            "paper_fills_recent": execution_trades["paper"]["count"],
            "live_fills_recent": execution_trades["live"]["count"],
        },
        "activity": {
            "market_data": market_data,
            "strategy": strategy,
            "risk": risk,
            "execution_orders": execution_orders,
            "execution_trades": execution_trades,
        },
        "services": services,
    }
