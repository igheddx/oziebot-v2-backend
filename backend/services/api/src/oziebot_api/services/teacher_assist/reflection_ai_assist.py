from __future__ import annotations

from datetime import UTC, datetime
import hashlib
import uuid
from typing import Any

from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.models.teacher_assist_ai_usage_event import TeacherAssistAIUsageEvent
from oziebot_api.models.teacher_assist_lesson_reflection import TeacherAssistLessonReflection
from oziebot_api.models.teacher_assist_lesson_reflection_version import (
    TeacherAssistLessonReflectionVersion,
)
from oziebot_api.services.teacher_assist.activity_events import record_activity_event
from oziebot_api.services.teacher_assist.lesson_reflections import (
    build_lesson_reflection_prompt_context,
    create_lesson_reflection_version,
    get_lesson_reflection_or_404,
)
from oziebot_api.services.teacher_assist.prompt_contracts import (
    LESSON_REFLECTION_AI_FEATURE,
    LESSON_REFLECTION_AI_PROMPT_VERSION,
)
from oziebot_api.services.teacher_assist.provider_config import TeacherAssistProviderCircuitBreaker


def _build_mock_reflection_suggestions(*, prompt_context: dict[str, Any]) -> dict[str, Any]:
    effectiveness = dict(prompt_context.get("lesson_effectiveness") or {})
    classification = str(effectiveness.get("classification") or "insufficient_data")
    subject_name = str(prompt_context.get("subject_name") or "this subject")
    digest = hashlib.sha256(classification.encode("utf-8")).hexdigest()
    focus_index = int(digest[:2], 16) % 3
    strength_templates = [
        f"Clear modeling sequence helped students engage with {subject_name} concepts.",
        "Structured partner practice increased participation during the lesson block.",
        "Formative checks surfaced misconceptions before independent practice.",
    ]
    weakness_templates = [
        "Pacing left limited time for differentiated small-group support.",
        "Transition routines between activities consumed instructional minutes.",
        "Independent practice tasks were not tightly aligned to the lesson objective.",
    ]
    improvement_templates = [
        "Build a 5-minute reteach checkpoint before the exit task next time.",
        "Prepare a scaffolded reference sheet for students still developing the skill.",
        "Shorten the opening routine to protect time for guided practice.",
    ]
    teacher_notes = dict(prompt_context.get("teacher_notes") or {})
    what_worked = list(teacher_notes.get("what_worked") or [])
    what_failed = list(teacher_notes.get("what_failed") or [])
    notes_for_next_year = list(teacher_notes.get("notes_for_next_year") or [])

    if classification in {"highly_effective", "effective"}:
        what_worked.append(strength_templates[focus_index])
    elif classification in {"needs_adjustment", "ineffective"}:
        what_failed.append(weakness_templates[focus_index])
        notes_for_next_year.append(improvement_templates[focus_index])

    return {
        "what_worked": what_worked[:6],
        "what_failed": what_failed[:6],
        "notes_for_next_year": notes_for_next_year[:6],
        "strengths": [
            strength_templates[focus_index],
            "Teacher pacing notes indicate strong engagement during the modeled segment.",
        ],
        "weaknesses": [
            weakness_templates[focus_index],
            "Some students needed additional scaffolded practice before independent work.",
        ],
        "improvements": [
            improvement_templates[focus_index],
            "Review committed mastery evidence before planning the next lesson sequence.",
        ],
        "teacher_review_required": True,
        "is_ai_draft": True,
        "prompt_version": LESSON_REFLECTION_AI_PROMPT_VERSION,
    }


def generate_lesson_reflection_ai_suggestions(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    lesson_reflection_id: uuid.UUID,
    provider_mode: str = "mock",
    teacher_instructions: str | None = None,
    settings: Settings | None = None,
) -> tuple[TeacherAssistLessonReflection, TeacherAssistLessonReflectionVersion, dict[str, Any]]:
    settings = settings or Settings()
    reflection = get_lesson_reflection_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        lesson_reflection_id=lesson_reflection_id,
        load_versions=True,
    )
    if reflection.status == "archived":
        raise ValueError("Archived reflections cannot receive AI suggestions")

    normalized_provider_mode = (provider_mode or "mock").strip().lower()
    if normalized_provider_mode not in {"mock", "real"}:
        raise ValueError("Unsupported lesson reflection AI provider mode")
    if normalized_provider_mode == "real":
        if not (
            settings.teacher_assist_real_provider_enabled
            or settings.teacher_assist_ai_enable_real_provider
        ):
            raise ValueError("Real lesson reflection AI is disabled")
        TeacherAssistProviderCircuitBreaker().assert_can_execute(
            settings, settings.teacher_assist_ai_provider
        )
        raise ValueError(
            "Real lesson reflection AI provider execution is not enabled in this phase"
        )

    prompt_context = build_lesson_reflection_prompt_context(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        lesson_reflection=reflection,
        settings=settings,
    )
    if teacher_instructions and teacher_instructions.strip():
        prompt_context["teacher_instructions"] = teacher_instructions.strip()

    draft_content = _build_mock_reflection_suggestions(prompt_context=prompt_context)
    now = datetime.now(UTC)
    usage_event = TeacherAssistAIUsageEvent(
        tenant_id=tenant_id,
        user_id=user_id,
        workflow_id=None,
        provider="mock",
        model="mock",
        feature=LESSON_REFLECTION_AI_FEATURE,
        input_tokens=0,
        output_tokens=0,
        estimated_cost_cents=0,
        metadata_json={
            "is_mock": True,
            "provider_mode": normalized_provider_mode,
            "lesson_reflection_id": str(reflection.id),
            "weekly_plan_id": str(reflection.weekly_plan_id) if reflection.weekly_plan_id else None,
            "teacher_review_required": True,
            "prompt_version": LESSON_REFLECTION_AI_PROMPT_VERSION,
        },
        created_at=now,
    )
    db.add(usage_event)
    db.flush()

    version = create_lesson_reflection_version(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        lesson_reflection=reflection,
        content_json=draft_content,
        version_source="ai_draft",
        change_reason="Mock AI reflection suggestions generated",
        prompt_context_json=prompt_context,
        provider_name="mock",
        provider_model="mock",
        prompt_version=LESSON_REFLECTION_AI_PROMPT_VERSION,
        ai_usage_event_id=usage_event.id,
    )
    record_activity_event(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        event_type="lesson_reflection_ai_suggested",
        event_category="insights",
        entity_type="lesson_reflection",
        entity_id=reflection.id,
        school_year_id=reflection.school_year_id,
        grading_period_id=reflection.grading_period_id,
        class_id=reflection.class_id,
        subject_id=reflection.subject_id,
        summary_text=f"Generated AI reflection suggestions for '{reflection.title}'.",
        details_json={
            "lesson_reflection_id": str(reflection.id),
            "version_id": str(version.id),
            "provider_mode": normalized_provider_mode,
            "teacher_review_required": True,
        },
    )
    db.flush()
    return (
        reflection,
        version,
        {
            "provider_mode": normalized_provider_mode,
            "teacher_review_required": True,
            "prompt_version": LESSON_REFLECTION_AI_PROMPT_VERSION,
        },
    )
