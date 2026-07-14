"""Teacher Copilot sessions, messages, and orchestration."""

from __future__ import annotations

from datetime import UTC, datetime
import uuid
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session, selectinload

from oziebot_api.config import Settings
from oziebot_api.models.teacher_assist_ai_usage_event import TeacherAssistAIUsageEvent
from oziebot_api.models.teacher_copilot_session import TeacherCopilotMessage, TeacherCopilotSession
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.constants import COPILOT_FEATURE, COPILOT_MESSAGE_ROLES
from oziebot_api.services.teacher_assist.provider_config import TeacherAssistProviderCircuitBreaker
from oziebot_api.services.teacher_assist.teacher_context_engine import build_teacher_context
from oziebot_api.services.teacher_assist.teacher_copilot_intents import (
    SUGGESTED_QUESTIONS,
    analyze_admin_copilot_question,
    analyze_copilot_question,
)


def _now() -> datetime:
    return datetime.now(UTC)


def _json_safe(value: Any) -> Any:
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    return value


def _validate_role(role: str) -> str:
    normalized = role.strip().lower()
    if normalized not in COPILOT_MESSAGE_ROLES:
        raise ValueError("Unsupported copilot message role")
    return normalized


def get_suggested_questions(*, is_root_admin: bool = False) -> list[str]:
    questions = list(SUGGESTED_QUESTIONS)
    if is_root_admin:
        questions.extend(
            [
                "Which objectives lack resources?",
                "Which grades have incomplete pacing guides?",
                "Which curriculum mappings are missing?",
            ]
        )
    return questions


def list_copilot_sessions(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    teacher_id: uuid.UUID,
    limit: int = 20,
) -> list[TeacherCopilotSession]:
    return list(
        db.scalars(
            select(TeacherCopilotSession)
            .where(
                TeacherCopilotSession.tenant_id == tenant_id,
                TeacherCopilotSession.teacher_id == teacher_id,
            )
            .order_by(TeacherCopilotSession.updated_at.desc())
            .limit(limit)
        ).all()
    )


def create_copilot_session(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    teacher_id: uuid.UUID,
    title: str | None = None,
) -> TeacherCopilotSession:
    now = _now()
    row = TeacherCopilotSession(
        tenant_id=tenant_id,
        teacher_id=teacher_id,
        title=(title or "Teacher Copilot").strip()[:255],
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    return row


def get_copilot_session(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    teacher_id: uuid.UUID,
    session_id: uuid.UUID,
    load_messages: bool = False,
) -> TeacherCopilotSession:
    query = select(TeacherCopilotSession).where(
        TeacherCopilotSession.id == session_id,
        TeacherCopilotSession.tenant_id == tenant_id,
        TeacherCopilotSession.teacher_id == teacher_id,
    )
    if load_messages:
        query = query.options(selectinload(TeacherCopilotSession.messages))
    row = db.scalars(query).one_or_none()
    if row is None:
        raise LookupError("Copilot session not found")
    return row


def list_session_messages(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    teacher_id: uuid.UUID,
    session_id: uuid.UUID,
) -> list[TeacherCopilotMessage]:
    get_copilot_session(db, tenant_id=tenant_id, teacher_id=teacher_id, session_id=session_id)
    return list(
        db.scalars(
            select(TeacherCopilotMessage)
            .where(
                TeacherCopilotMessage.tenant_id == tenant_id,
                TeacherCopilotMessage.session_id == session_id,
            )
            .order_by(TeacherCopilotMessage.created_at.asc())
        ).all()
    )


def _check_daily_cost_limit(
    db: Session, *, settings: Settings, tenant_id: uuid.UUID, user_id: uuid.UUID
) -> None:
    limit = max(0, settings.teacher_assist_ai_daily_cost_limit_cents)
    if limit <= 0:
        return
    start_of_day = _now().replace(hour=0, minute=0, second=0, microsecond=0)
    spent = db.scalar(
        select(func.coalesce(func.sum(TeacherAssistAIUsageEvent.estimated_cost_cents), 0)).where(
            TeacherAssistAIUsageEvent.tenant_id == tenant_id,
            TeacherAssistAIUsageEvent.user_id == user_id,
            TeacherAssistAIUsageEvent.feature == COPILOT_FEATURE,
            TeacherAssistAIUsageEvent.created_at >= start_of_day,
        )
    )
    if int(spent or 0) >= limit:
        raise ValueError("Daily Teacher Copilot AI cost limit reached")


def serialize_message(row: TeacherCopilotMessage) -> dict[str, Any]:
    snapshot = row.context_snapshot or {}
    return {
        "id": str(row.id),
        "session_id": str(row.session_id),
        "role": row.role,
        "content": row.content,
        "context_snapshot": snapshot,
        "analysis": snapshot.get("analysis"),
        "created_at": row.created_at.isoformat(),
    }


def serialize_session(row: TeacherCopilotSession) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "title": row.title,
        "created_at": row.created_at.isoformat(),
        "updated_at": row.updated_at.isoformat(),
    }


def send_copilot_message(
    db: Session,
    *,
    settings: Settings,
    tenant_id: uuid.UUID,
    user: User,
    session_id: uuid.UUID,
    question: str,
    provider_mode: str = "mock",
    instructional_week_id: uuid.UUID | None = None,
    is_root_admin: bool = False,
) -> dict[str, Any]:
    normalized_mode = provider_mode.strip().lower()
    if normalized_mode not in {"mock", "real"}:
        raise ValueError("Unsupported copilot provider mode")
    if normalized_mode == "real":
        if not (
            settings.teacher_assist_real_provider_enabled
            or settings.teacher_assist_ai_enable_real_provider
        ):
            raise ValueError("Real Teacher Copilot provider is disabled")
        TeacherAssistProviderCircuitBreaker().assert_can_execute(
            settings, settings.teacher_assist_ai_provider
        )
        raise ValueError("Real Teacher Copilot provider is not enabled in this phase")

    _check_daily_cost_limit(db, settings=settings, tenant_id=tenant_id, user_id=user.id)
    session = get_copilot_session(
        db, tenant_id=tenant_id, teacher_id=user.id, session_id=session_id
    )
    context = build_teacher_context(
        db,
        settings=settings,
        tenant_id=tenant_id,
        user=user,
        instructional_week_id=instructional_week_id,
    )

    now = _now()
    teacher_message = TeacherCopilotMessage(
        tenant_id=tenant_id,
        session_id=session.id,
        role=_validate_role("teacher"),
        content=question.strip(),
        context_snapshot={
            "context_packet_keys": list((context.get("context_packets") or {}).keys())
        },
        created_at=now,
    )
    db.add(teacher_message)

    analysis = analyze_copilot_question(question=question, context=context)
    if is_root_admin and any(
        keyword in question.lower()
        for keyword in ("catalog", "pacing guide", "mapping", "district", "school")
    ):
        analysis = analyze_admin_copilot_question(question=question, context=context)

    answer_text = analysis.get("answer") or "No analysis available."
    usage_event = TeacherAssistAIUsageEvent(
        tenant_id=tenant_id,
        user_id=user.id,
        workflow_id=None,
        provider="mock",
        model="mock",
        feature=COPILOT_FEATURE,
        input_tokens=min(len(question.split()), settings.teacher_assist_ai_max_input_tokens),
        output_tokens=min(len(answer_text.split()), settings.teacher_assist_ai_max_output_tokens),
        estimated_cost_cents=0,
        metadata_json={
            "session_id": str(session.id),
            "intent": analysis.get("intent"),
            "provider_mode": normalized_mode,
            "teacher_review_required": True,
            "context_packets_used": analysis.get("context_packets_used"),
        },
        created_at=now,
    )
    db.add(usage_event)
    db.flush()

    assistant_snapshot = _json_safe(
        {
            "analysis": analysis,
            "context_packets": context.get("context_packets"),
            "audit": {
                "prompt": question.strip(),
                "intent": analysis.get("intent"),
                "provider": "mock",
                "timestamp": now.isoformat(),
            },
        }
    )
    assistant_message = TeacherCopilotMessage(
        tenant_id=tenant_id,
        session_id=session.id,
        role=_validate_role("assistant"),
        content=answer_text,
        context_snapshot=assistant_snapshot,
        ai_usage_event_id=usage_event.id,
        created_at=now,
    )
    db.add(assistant_message)
    session.updated_at = now
    if session.title == "Teacher Copilot" and question.strip():
        session.title = question.strip()[:255]
    db.flush()
    return {
        "teacher_message": serialize_message(teacher_message),
        "assistant_message": serialize_message(assistant_message),
        "analysis": analysis,
        "requires_teacher_review": True,
    }
