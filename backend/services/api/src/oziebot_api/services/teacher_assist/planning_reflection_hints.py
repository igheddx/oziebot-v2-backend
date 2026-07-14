from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from oziebot_api.config import Settings
from oziebot_api.models.teacher_assist_lesson_reflection import TeacherAssistLessonReflection
from oziebot_api.services.teacher_assist.lesson_effectiveness import list_lesson_effectiveness
from oziebot_api.services.teacher_assist.lesson_effectiveness_history import (
    build_lesson_effectiveness_historical_comparison,
)
from oziebot_api.services.teacher_assist.planning import get_planning_draft_context_preview


def _reflection_note_snippets(reflections: list) -> list[dict[str, Any]]:
    snippets: list[dict[str, Any]] = []
    for reflection in reflections:
        content = (
            reflection.current_version.content_json
            if reflection.current_version is not None
            else {}
        )
        snippets.append(
            {
                "reflection_id": str(reflection.id),
                "title": reflection.title,
                "weekly_plan_id": str(reflection.weekly_plan_id)
                if reflection.weekly_plan_id
                else None,
                "what_worked": content.get("what_worked") or [],
                "what_failed": content.get("what_failed") or [],
                "notes_for_next_year": content.get("notes_for_next_year") or [],
                "status": reflection.status,
            }
        )
    return snippets


def build_planning_reflection_hints(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    planning_draft_id: uuid.UUID,
    settings: Settings | None = None,
) -> dict[str, Any]:
    settings = settings or Settings()
    preview = get_planning_draft_context_preview(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        planning_draft_id=planning_draft_id,
    )
    draft = preview.draft
    if draft.class_id is None or draft.subject_id is None or draft.school_year_id is None:
        return {
            "planning_draft_id": str(planning_draft_id),
            "last_year_notes": [],
            "reflection_notes": [],
            "prior_effectiveness": [],
            "read_only": True,
        }

    current_reflections = db.scalars(
        select(TeacherAssistLessonReflection)
        .where(
            TeacherAssistLessonReflection.tenant_id == tenant_id,
            TeacherAssistLessonReflection.owner_user_id == user_id,
            TeacherAssistLessonReflection.school_year_id == draft.school_year_id,
            TeacherAssistLessonReflection.class_id == draft.class_id,
            TeacherAssistLessonReflection.subject_id == draft.subject_id,
        )
        .options(selectinload(TeacherAssistLessonReflection.current_version))
        .order_by(TeacherAssistLessonReflection.updated_at.desc())
    ).all()
    if draft.grading_period_id is not None:
        current_reflections = [
            row for row in current_reflections if row.grading_period_id == draft.grading_period_id
        ]

    prior_year_reflections = db.scalars(
        select(TeacherAssistLessonReflection)
        .where(
            TeacherAssistLessonReflection.tenant_id == tenant_id,
            TeacherAssistLessonReflection.owner_user_id == user_id,
            TeacherAssistLessonReflection.class_id == draft.class_id,
            TeacherAssistLessonReflection.subject_id == draft.subject_id,
            TeacherAssistLessonReflection.school_year_id != draft.school_year_id,
        )
        .options(selectinload(TeacherAssistLessonReflection.current_version))
        .order_by(TeacherAssistLessonReflection.updated_at.desc())
    ).all()

    current_effectiveness = list_lesson_effectiveness(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        school_year_id=draft.school_year_id,
        grading_period_id=draft.grading_period_id,
        class_id=draft.class_id,
        subject_id=draft.subject_id,
        settings=settings,
    )
    historical = build_lesson_effectiveness_historical_comparison(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        school_year_id=draft.school_year_id,
        grading_period_id=draft.grading_period_id,
        class_id=draft.class_id,
        subject_id=draft.subject_id,
        settings=settings,
    )

    last_year_notes: list[str] = []
    for snippet in _reflection_note_snippets(prior_year_reflections):
        for note in snippet.get("notes_for_next_year") or []:
            if isinstance(note, str) and note.strip():
                last_year_notes.append(note.strip())

    reflection_notes: list[str] = []
    for snippet in _reflection_note_snippets(current_reflections):
        for bucket in ("what_worked", "what_failed", "notes_for_next_year"):
            for note in snippet.get(bucket) or []:
                if isinstance(note, str) and note.strip():
                    reflection_notes.append(note.strip())

    prior_effectiveness: list[dict[str, Any]] = []
    for scope_key in ("prior_grading_period", "prior_school_year"):
        scope = historical.get(scope_key)
        if scope is None:
            continue
        summary = dict(scope.get("summary") or {})
        prior_effectiveness.append(
            {
                "scope": scope_key,
                "school_year_title": scope.get("school_year_title"),
                "grading_period_title": scope.get("grading_period_title"),
                "lesson_count": summary.get("lesson_count", 0),
                "average_mastery_percentage": summary.get("average_mastery_percentage", 0.0),
                "classification_counts": summary.get("classification_counts") or {},
            }
        )
    for row in current_effectiveness[:5]:
        prior_effectiveness.append(
            {
                "scope": "current_lessons",
                "weekly_plan_id": str(row.get("weekly_plan_id")),
                "weekly_plan_title": row.get("weekly_plan_title"),
                "classification": row.get("classification"),
                "aggregate_mastery_percentage": row.get("aggregate_mastery_percentage"),
            }
        )

    return {
        "planning_draft_id": str(planning_draft_id),
        "last_year_notes": last_year_notes[:12],
        "reflection_notes": reflection_notes[:12],
        "prior_effectiveness": prior_effectiveness[:12],
        "read_only": True,
    }
