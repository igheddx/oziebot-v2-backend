"""Centralized TeacherAssist AI usage tracking and cost guardrails."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.models.teacher_assist_ai_usage_event import TeacherAssistAIUsageEvent
from oziebot_api.services.teacher_assist.runtime_settings import resolve_teacher_assist_settings

TEACHER_ASSIST_DAILY_COST_LIMIT_MESSAGE = (
    "AI generation is temporarily disabled because the daily cost limit has been reached."
)


def get_effective_daily_cost_limit_cents(db: Session | None, settings: Settings) -> int:
    effective = resolve_teacher_assist_settings(db, settings)
    return max(0, int(effective.teacher_assist_ai_daily_cost_limit_cents or 0))


def get_teacher_assist_daily_usage_cents(db: Session) -> int:
    now = datetime.now(UTC)
    day_start = datetime(now.year, now.month, now.day, tzinfo=UTC)
    total = db.scalar(
        select(func.coalesce(func.sum(TeacherAssistAIUsageEvent.estimated_cost_cents), 0)).where(
            TeacherAssistAIUsageEvent.created_at >= day_start
        )
    )
    return int(total or 0)


def assert_teacher_assist_ai_cost_available(db: Session, settings: Settings) -> None:
    limit = get_effective_daily_cost_limit_cents(db, settings)
    if limit <= 0:
        return
    current_total = get_teacher_assist_daily_usage_cents(db)
    if current_total >= limit:
        raise RuntimeError(TEACHER_ASSIST_DAILY_COST_LIMIT_MESSAGE)


def record_teacher_assist_ai_usage(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    feature: str,
    provider: str,
    model: str | None,
    input_tokens: int,
    output_tokens: int,
    estimated_cost_cents: int,
    workflow_id: uuid.UUID | None = None,
    metadata: dict[str, Any] | None = None,
) -> TeacherAssistAIUsageEvent:
    event = TeacherAssistAIUsageEvent(
        id=uuid.uuid4(),
        tenant_id=tenant_id,
        user_id=user_id,
        workflow_id=workflow_id,
        provider=provider,
        model=model,
        feature=feature,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        estimated_cost_cents=estimated_cost_cents,
        metadata_json=metadata,
        created_at=datetime.now(UTC),
    )
    db.add(event)
    return event


def get_package_ai_cost_by_feature(db: Session, *, package_id: uuid.UUID) -> dict[str, int]:
    """Return estimated AI cost in cents, grouped by feature, for a specific package.

    Usage events store package_id in metadata_json->>'package_id'. We filter on
    that JSON path so we only count events produced during this package's generation.
    """
    from sqlalchemy import text as _text

    rows = db.execute(
        _text(
            "SELECT feature, estimated_cost_cents "
            "FROM teacher_assist_ai_usage_events "
            "WHERE metadata_json->>'package_id' = :pkg_id"
        ),
        {"pkg_id": str(package_id)},
    ).fetchall()
    by_feature: dict[str, int] = {}
    for feature, cost in rows:
        by_feature[feature] = by_feature.get(feature, 0) + int(cost or 0)
    return by_feature


def get_teacher_assist_ai_usage_summary(db: Session, *, hours: int = 24) -> dict[str, Any]:
    since = datetime.now(UTC) - timedelta(hours=max(1, hours))
    rows = db.scalars(
        select(TeacherAssistAIUsageEvent).where(TeacherAssistAIUsageEvent.created_at >= since)
    ).all()
    total_cost_cents = sum(int(row.estimated_cost_cents or 0) for row in rows)
    total_input_tokens = sum(int(row.input_tokens or 0) for row in rows)
    total_output_tokens = sum(int(row.output_tokens or 0) for row in rows)
    by_feature: dict[str, int] = {}
    for row in rows:
        by_feature[row.feature] = by_feature.get(row.feature, 0) + int(
            row.estimated_cost_cents or 0
        )
    return {
        "window_hours": hours,
        "event_count": len(rows),
        "total_cost_cents": total_cost_cents,
        "total_input_tokens": total_input_tokens,
        "total_output_tokens": total_output_tokens,
        "cost_cents_by_feature": by_feature,
        "daily_usage_cents": get_teacher_assist_daily_usage_cents(db),
    }
