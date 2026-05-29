"""Usage metrics foundation — daily aggregates for pilot observability."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
import uuid
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from oziebot_api.models.teacher_assist_ai_usage_event import TeacherAssistAIUsageEvent
from oziebot_api.models.teacher_assist_assignment import TeacherAssistAssignment
from oziebot_api.models.teacher_assist_instructional_week import TeacherAssistInstructionalWeek
from oziebot_api.models.teacher_assist_newsletter import TeacherAssistNewsletter
from oziebot_api.models.teacher_assist_usage_metric import TeacherAssistUsageMetric
from oziebot_api.models.teacher_copilot_session import TeacherCopilotMessage
from oziebot_api.services.teacher_assist.constants import USAGE_METRIC_KEYS


def _now() -> datetime:
    return datetime.now(UTC)


def _today() -> date:
    return _now().date()


def increment_usage_metric(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    metric_key: str,
    user_id: uuid.UUID | None = None,
    amount: int = 1,
    period_date: date | None = None,
    metadata: dict[str, Any] | None = None,
) -> TeacherAssistUsageMetric:
    normalized_key = metric_key.strip().lower()
    if normalized_key not in USAGE_METRIC_KEYS:
        raise ValueError("Unsupported usage metric key")
    target_date = period_date or _today()
    row = db.scalars(
        select(TeacherAssistUsageMetric).where(
            TeacherAssistUsageMetric.tenant_id == tenant_id,
            TeacherAssistUsageMetric.user_id == user_id,
            TeacherAssistUsageMetric.metric_key == normalized_key,
            TeacherAssistUsageMetric.period_date == target_date,
        )
    ).one_or_none()
    now = _now()
    if row is None:
        row = TeacherAssistUsageMetric(
            tenant_id=tenant_id,
            user_id=user_id,
            metric_key=normalized_key,
            metric_value=max(0, amount),
            period_date=target_date,
            metadata_json=metadata,
            created_at=now,
            updated_at=now,
        )
        db.add(row)
    else:
        row.metric_value = max(0, row.metric_value + amount)
        if metadata:
            row.metadata_json = {**(row.metadata_json or {}), **metadata}
        row.updated_at = now
    db.flush()
    return row


def _count_since(db: Session, model, tenant_id: uuid.UUID, *, column_name: str = "created_at", days: int = 30) -> int:
    since = _now() - timedelta(days=days)
    column = getattr(model, column_name)
    return int(
        db.scalar(
            select(func.count()).select_from(model).where(
                model.tenant_id == tenant_id,
                column >= since,
            )
        )
        or 0
    )


def build_usage_metrics_snapshot(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    days: int = 30,
) -> dict[str, Any]:
    since = _now() - timedelta(days=days)
    copilot_messages = int(
        db.scalar(
            select(func.count())
            .select_from(TeacherCopilotMessage)
            .where(
                TeacherCopilotMessage.tenant_id == tenant_id,
                TeacherCopilotMessage.role == "teacher",
                TeacherCopilotMessage.created_at >= since,
            )
        )
        or 0
    )
    ai_events = int(
        db.scalar(
            select(func.count())
            .select_from(TeacherAssistAIUsageEvent)
            .where(
                TeacherAssistAIUsageEvent.tenant_id == tenant_id,
                TeacherAssistAIUsageEvent.created_at >= since,
            )
        )
        or 0
    )
    stored = list(
        db.scalars(
            select(TeacherAssistUsageMetric).where(
                TeacherAssistUsageMetric.tenant_id == tenant_id,
                TeacherAssistUsageMetric.period_date >= (_today() - timedelta(days=days)),
            )
        ).all()
    )
    stored_totals: dict[str, int] = {}
    for row in stored:
        stored_totals[row.metric_key] = stored_totals.get(row.metric_key, 0) + row.metric_value

    return {
        "period_days": days,
        "metrics": {
            "instructional_weeks_created": _count_since(db, TeacherAssistInstructionalWeek, tenant_id, days=days),
            "assignments_created": _count_since(db, TeacherAssistAssignment, tenant_id, days=days),
            "newsletters_generated": _count_since(db, TeacherAssistNewsletter, tenant_id, days=days),
            "copilot_usage": copilot_messages,
            "feature_usage": ai_events,
            "login_activity": stored_totals.get("login_activity", 0),
            "template_usage": stored_totals.get("template_usage", 0),
            "reuse_usage": stored_totals.get("reuse_usage", 0),
            "exports_generated": stored_totals.get("exports_generated", 0),
            "assessments_created": stored_totals.get("assessments_created", 0),
        },
        "stored_daily_rows": len(stored),
    }


def record_teacher_login(db: Session, *, tenant_id: uuid.UUID, user_id: uuid.UUID) -> None:
    increment_usage_metric(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        metric_key="login_activity",
        amount=1,
    )
