"""Teacher Copilot API — context-aware instructional assistant."""

from __future__ import annotations

import uuid

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from oziebot_api.config import get_settings
from oziebot_api.deps import DbSession
from oziebot_api.deps.auth import CurrentUser
from oziebot_api.services.teacher_assist.teacher_context_engine import build_teacher_context
from oziebot_api.services.teacher_assist.teacher_copilot_intents import analyze_admin_copilot_question
from oziebot_api.services.teacher_assist.teacher_copilot_service import (
    create_copilot_session,
    get_copilot_session,
    get_suggested_questions,
    list_copilot_sessions,
    list_session_messages,
    send_copilot_message,
    serialize_message,
    serialize_session,
)


router = APIRouter(prefix="/teacher-assist/copilot", tags=["teacher_assist_copilot"])


def _tenant_id(db, user) -> uuid.UUID:
    from oziebot_api.api.v1.teacher_assist_pacing_guides import _tenant_id as pacing_tenant_id

    return pacing_tenant_id(db, user)


def _handle(fn):
    try:
        return fn()
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


class CopilotSessionCreateIn(BaseModel):
    title: str | None = None


class CopilotMessageIn(BaseModel):
    question: str = Field(min_length=1)
    provider_mode: str = "mock"
    instructional_week_id: uuid.UUID | None = None


class AdminCopilotQueryIn(BaseModel):
    question: str = Field(min_length=1)


@router.get("/suggested-questions")
def read_suggested_questions(user: CurrentUser) -> dict:
    return {"questions": get_suggested_questions(is_root_admin=user.is_root_admin)}


@router.get("/context")
def read_copilot_context(
    user: CurrentUser,
    db: DbSession,
    instructional_week_id: uuid.UUID | None = None,
) -> dict:
    tenant_id = _tenant_id(db, user)
    settings = get_settings()
    return build_teacher_context(
        db,
        settings=settings,
        tenant_id=tenant_id,
        user=user,
        instructional_week_id=instructional_week_id,
    )


@router.get("/sessions")
def read_copilot_sessions(user: CurrentUser, db: DbSession) -> list[dict]:
    tenant_id = _tenant_id(db, user)
    rows = list_copilot_sessions(db, tenant_id=tenant_id, teacher_id=user.id)
    return [serialize_session(row) for row in rows]


@router.post("/sessions", status_code=201)
def create_copilot_session_route(user: CurrentUser, db: DbSession, body: CopilotSessionCreateIn) -> dict:
    tenant_id = _tenant_id(db, user)

    def _create():
        row = create_copilot_session(
            db,
            tenant_id=tenant_id,
            teacher_id=user.id,
            title=body.title,
        )
        db.commit()
        return serialize_session(row)

    return _handle(_create)


@router.get("/sessions/{session_id}/messages")
def read_session_messages(session_id: uuid.UUID, user: CurrentUser, db: DbSession) -> list[dict]:
    tenant_id = _tenant_id(db, user)

    def _read():
        rows = list_session_messages(
            db,
            tenant_id=tenant_id,
            teacher_id=user.id,
            session_id=session_id,
        )
        return [serialize_message(row) for row in rows]

    return _handle(_read)


@router.post("/sessions/{session_id}/messages")
def post_session_message(
    session_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
    body: CopilotMessageIn,
) -> dict:
    tenant_id = _tenant_id(db, user)
    settings = get_settings()

    def _send():
        payload = send_copilot_message(
            db,
            settings=settings,
            tenant_id=tenant_id,
            user=user,
            session_id=session_id,
            question=body.question,
            provider_mode=body.provider_mode,
            instructional_week_id=body.instructional_week_id,
            is_root_admin=user.is_root_admin,
        )
        db.commit()
        return payload

    return _handle(_send)


@router.post("/admin/query")
def post_admin_copilot_query(user: CurrentUser, db: DbSession, body: AdminCopilotQueryIn) -> dict:
    if not user.is_root_admin:
        raise HTTPException(status_code=403, detail="Admin copilot is restricted to root administrators")
    tenant_id = _tenant_id(db, user)
    settings = get_settings()
    context = build_teacher_context(db, settings=settings, tenant_id=tenant_id, user=user)
    analysis = analyze_admin_copilot_question(question=body.question, context=context)
    return {"analysis": analysis, "requires_teacher_review": True}
