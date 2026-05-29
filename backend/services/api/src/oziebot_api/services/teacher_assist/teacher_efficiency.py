from __future__ import annotations

from datetime import UTC, datetime, time
import uuid
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from oziebot_api.models.teacher_assist_school_year import TeacherAssistSchoolYear
from oziebot_api.models.teacher_assist_time_savings import TeacherAssistReuseEvent, TeacherAssistWeekTemplate
from oziebot_api.services.teacher_assist.time_savings_constants import TIME_SAVINGS_MINUTES


def build_teacher_efficiency_dashboard(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    school_year_id: uuid.UUID | None = None,
) -> dict[str, Any]:
    query = select(TeacherAssistReuseEvent).where(
        TeacherAssistReuseEvent.tenant_id == tenant_id,
        TeacherAssistReuseEvent.user_id == user_id,
    )
    if school_year_id is not None:
        school_year = db.get(TeacherAssistSchoolYear, school_year_id)
        if school_year is not None:
            query = query.where(
                TeacherAssistReuseEvent.created_at
                >= datetime.combine(school_year.start_date, time.min, tzinfo=UTC)
            )
    events = list(db.scalars(query).all())

    artifacts_reused = len([row for row in events if row.event_type == "reuse_artifact"])
    weeks_duplicated = len([row for row in events if row.event_type == "duplicate_week"])
    templates_used = len([row for row in events if row.event_type == "apply_template"])
    next_week_generated = len([row for row in events if row.event_type == "generate_next_week"])
    rollovers = len([row for row in events if row.event_type == "rollover_v2"])
    total_minutes = sum(row.estimated_minutes_saved for row in events)
    total_actions = len(events)
    reuse_rate = round((artifacts_reused + weeks_duplicated + templates_used) / max(total_actions, 1) * 100, 1)

    recent_templates = list(
        db.scalars(
            select(TeacherAssistWeekTemplate)
            .where(
                TeacherAssistWeekTemplate.tenant_id == tenant_id,
                TeacherAssistWeekTemplate.created_by_user_id == user_id,
            )
            .order_by(TeacherAssistWeekTemplate.updated_at.desc())
            .limit(5)
        ).all()
    )

    return {
        "artifacts_reused": artifacts_reused,
        "weeks_duplicated": weeks_duplicated,
        "templates_used": templates_used,
        "next_week_generated": next_week_generated,
        "rollovers_completed": rollovers,
        "estimated_hours_saved": round(total_minutes / 60, 1),
        "estimated_minutes_saved": total_minutes,
        "reuse_rate_percent": reuse_rate,
        "total_reuse_actions": total_actions,
        "time_savings_assumptions_minutes": TIME_SAVINGS_MINUTES,
        "recent_templates": [
            {
                "id": str(row.id),
                "name": row.name,
                "artifact_type": row.artifact_type,
                "navigation_href": f"/teacher-assist/planning/templates?id={row.id}",
            }
            for row in recent_templates
        ],
    }


def build_home_time_savings_summary(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
) -> dict[str, Any]:
    year_start = datetime.now(UTC).replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    total_minutes = db.scalar(
        select(func.coalesce(func.sum(TeacherAssistReuseEvent.estimated_minutes_saved), 0)).where(
            TeacherAssistReuseEvent.tenant_id == tenant_id,
            TeacherAssistReuseEvent.user_id == user_id,
            TeacherAssistReuseEvent.created_at >= year_start,
        )
    )
    return {
        "time_saved_this_year_hours": round((total_minutes or 0) / 60, 1),
        "time_saved_this_year_minutes": int(total_minutes or 0),
    }
