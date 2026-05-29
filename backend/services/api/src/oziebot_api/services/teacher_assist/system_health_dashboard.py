"""System health dashboard — root admin operational summary."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
import uuid
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.models.teacher_assist_ai_usage_event import TeacherAssistAIUsageEvent
from oziebot_api.models.teacher_assist_assignment import TeacherAssistAssignment
from oziebot_api.models.teacher_assist_extraction_job import TeacherAssistExtractionJob
from oziebot_api.models.teacher_assist_pilot_feedback import TeacherAssistPilotFeedback
from oziebot_api.models.teacher_assist_workflow import TeacherAssistWorkflow
from oziebot_api.models.teacher_copilot_session import TeacherCopilotMessage, TeacherCopilotSession
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.constants import COPILOT_FEATURE
from oziebot_api.services.teacher_assist.storage import STORAGE_BACKENDS, STORAGE_AREAS
from oziebot_api.services.teacher_assist.usage_metrics import build_usage_metrics_snapshot


def _now() -> datetime:
    return datetime.now(UTC)


def build_system_health_dashboard(
    db: Session,
    *,
    settings: Settings,
    tenant_id: uuid.UUID | None = None,
) -> dict[str, Any]:
    since = _now() - timedelta(days=30)
    user_query = select(func.count()).select_from(User)
    teacher_query = select(func.count()).select_from(User).where(User.is_root_admin.is_(False))
    if tenant_id is not None:
        from oziebot_api.models.membership import TenantMembership

        user_query = (
            select(func.count(func.distinct(TenantMembership.user_id)))
            .select_from(TenantMembership)
            .where(TenantMembership.tenant_id == tenant_id)
        )
        teacher_query = user_query

    total_users = int(db.scalar(user_query) or 0)
    total_teachers = int(db.scalar(teacher_query) or 0)

    workflow_failed = int(
        db.scalar(
            select(func.count())
            .select_from(TeacherAssistWorkflow)
            .where(
                TeacherAssistWorkflow.status == "failed",
                TeacherAssistWorkflow.updated_at >= since,
                *([TeacherAssistWorkflow.tenant_id == tenant_id] if tenant_id else []),
            )
        )
        or 0
    )
    workflow_active = int(
        db.scalar(
            select(func.count())
            .select_from(TeacherAssistWorkflow)
            .where(
                TeacherAssistWorkflow.status.in_(("queued", "running", "retrying")),
                *([TeacherAssistWorkflow.tenant_id == tenant_id] if tenant_id else []),
            )
        )
        or 0
    )
    extraction_failed = int(
        db.scalar(
            select(func.count())
            .select_from(TeacherAssistExtractionJob)
            .where(
                TeacherAssistExtractionJob.status == "failed",
                TeacherAssistExtractionJob.updated_at >= since,
                *([TeacherAssistExtractionJob.tenant_id == tenant_id] if tenant_id else []),
            )
        )
        or 0
    )

    copilot_filter = [TeacherCopilotMessage.created_at >= since, TeacherCopilotMessage.role == "teacher"]
    if tenant_id:
        copilot_filter.append(TeacherCopilotMessage.tenant_id == tenant_id)
    copilot_messages = int(
        db.scalar(select(func.count()).select_from(TeacherCopilotMessage).where(*copilot_filter)) or 0
    )
    copilot_sessions = int(
        db.scalar(
            select(func.count())
            .select_from(TeacherCopilotSession)
            .where(
                TeacherCopilotSession.created_at >= since,
                *([TeacherCopilotSession.tenant_id == tenant_id] if tenant_id else []),
            )
        )
        or 0
    )

    export_usage = int(
        db.scalar(
            select(func.count())
            .select_from(TeacherAssistAIUsageEvent)
            .where(
                TeacherAssistAIUsageEvent.created_at >= since,
                TeacherAssistAIUsageEvent.feature.like("%export%"),
                *([TeacherAssistAIUsageEvent.tenant_id == tenant_id] if tenant_id else []),
            )
        )
        or 0
    )
    copilot_ai_usage = int(
        db.scalar(
            select(func.count())
            .select_from(TeacherAssistAIUsageEvent)
            .where(
                TeacherAssistAIUsageEvent.created_at >= since,
                TeacherAssistAIUsageEvent.feature == COPILOT_FEATURE,
                *([TeacherAssistAIUsageEvent.tenant_id == tenant_id] if tenant_id else []),
            )
        )
        or 0
    )

    open_feedback = int(
        db.scalar(
            select(func.count())
            .select_from(TeacherAssistPilotFeedback)
            .where(
                TeacherAssistPilotFeedback.status.in_(("open", "reviewing")),
                *([TeacherAssistPilotFeedback.tenant_id == tenant_id] if tenant_id else []),
            )
        )
        or 0
    )

    assignment_count = int(
        db.scalar(
            select(func.count())
            .select_from(TeacherAssistAssignment)
            .where(*([TeacherAssistAssignment.tenant_id == tenant_id] if tenant_id else []))
        )
        or 0
    )

    usage = (
        build_usage_metrics_snapshot(db, tenant_id=tenant_id, days=30)
        if tenant_id is not None
        else {"metrics": {}, "period_days": 30}
    )

    return {
        "generated_at": _now().isoformat(),
        "scope_tenant_id": str(tenant_id) if tenant_id else None,
        "users": {"total": total_users, "teachers": total_teachers},
        "schools": {"note": "School counts derive from education catalog assignments per tenant"},
        "storage": {
            "backend": settings.teacher_assist_storage_backend,
            "supported_backends": list(STORAGE_BACKENDS),
            "areas": list(STORAGE_AREAS),
            "s3_bucket_configured": bool(settings.teacher_assist_s3_bucket),
        },
        "jobs": {
            "workflows_active": workflow_active,
            "workflows_failed_30d": workflow_failed,
            "extractions_failed_30d": extraction_failed,
        },
        "copilot_usage": {
            "sessions_30d": copilot_sessions,
            "teacher_messages_30d": copilot_messages,
            "ai_usage_events_30d": copilot_ai_usage,
        },
        "export_usage_30d": export_usage,
        "assignments_total": assignment_count,
        "open_pilot_feedback": open_feedback,
        "usage_metrics": usage.get("metrics") or {},
        "system_errors": {
            "failed_workflows_30d": workflow_failed,
            "failed_extractions_30d": extraction_failed,
        },
    }
