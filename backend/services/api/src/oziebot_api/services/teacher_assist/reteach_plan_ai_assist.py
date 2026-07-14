from __future__ import annotations

from datetime import UTC, datetime
import hashlib
import uuid
from typing import Any

from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.models.teacher_assist_ai_usage_event import TeacherAssistAIUsageEvent
from oziebot_api.models.teacher_assist_reteach_plan import TeacherAssistReteachPlan
from oziebot_api.models.teacher_assist_reteach_plan_version import TeacherAssistReteachPlanVersion
from oziebot_api.services.teacher_assist.activity_events import record_activity_event
from oziebot_api.services.teacher_assist.prompt_contracts import (
    RETEACH_PLAN_AI_FEATURE,
    RETEACH_PLAN_AI_PROMPT_VERSION,
)
from oziebot_api.services.teacher_assist.provider_config import TeacherAssistProviderCircuitBreaker
from oziebot_api.services.teacher_assist.reteach_plans import (
    build_reteach_plan_prompt_context,
    create_reteach_plan_version,
    get_reteach_plan_or_404,
)


def _build_mock_reteach_draft(*, prompt_context: dict[str, Any]) -> dict[str, Any]:
    standard_insight = dict(prompt_context.get("standard_insight") or {})
    standard_code = str(standard_insight.get("standard_code") or "Standard")
    digest = hashlib.sha256(standard_code.encode("utf-8")).hexdigest()
    focus_index = int(digest[:2], 16) % 3
    focus_templates = [
        "conceptual understanding with visual models",
        "procedural fluency through guided practice",
        "academic vocabulary in context",
    ]
    focus = focus_templates[focus_index]
    mastery_pct = int(float(standard_insight.get("mastery_percentage") or 0) * 100)
    developing_pct = int(float(standard_insight.get("developing_percentage") or 0) * 100)

    return {
        "reteach_objectives": [
            f"Rebuild foundational understanding of {standard_code} using {focus}.",
            "Increase the share of STUDENT # summaries reaching committed mastery through targeted checks.",
            "Provide differentiated support for students still developing or beginning on this standard.",
        ],
        "instructional_strategies": [
            "Use a short teacher-model segment followed by structured partner practice.",
            "Embed formative checks every 8–10 minutes using anonymous response cards or whiteboards.",
            f"Revisit prerequisite skills tied to {standard_code} before advancing to application tasks.",
        ],
        "small_group_recommendations": [
            f"Group A: students with beginning levels ({developing_pct}% developing context) — reteach core concept.",
            "Group B: students approaching mastery — error analysis on missed assessment checks.",
            "Group C: extension group — apply the standard in a novel but low-stakes scenario.",
        ],
        "intervention_ideas": [
            "Provide a one-page reference sheet with worked examples before independent practice.",
            "Schedule a 10-minute daily warm-up cycle for one week focused on this standard.",
            "Use teacher-confirmed assignment evidence to select the next reteach mini-task.",
        ],
        "vocabulary_focus": [
            f"Key terms for {standard_code} with student-friendly definitions.",
            "Signal words that indicate when to apply the target skill or concept.",
            "Academic talk stems for explaining reasoning using anonymous practice responses.",
        ],
        "assessment_checks": [
            "Exit ticket with 3 items aligned to the target standard (teacher-scored).",
            "Anonymous mastery observation notes captured only after teacher review.",
            f"Compare post-reteach committed mastery distribution to current {mastery_pct}% baseline.",
        ],
        "teacher_review_required": True,
        "is_ai_draft": True,
        "prompt_version": RETEACH_PLAN_AI_PROMPT_VERSION,
    }


def generate_reteach_plan_ai_draft(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    reteach_plan_id: uuid.UUID,
    provider_mode: str = "mock",
    teacher_instructions: str | None = None,
    settings: Settings | None = None,
) -> tuple[TeacherAssistReteachPlan, TeacherAssistReteachPlanVersion, dict[str, Any]]:
    settings = settings or Settings()
    plan = get_reteach_plan_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        reteach_plan_id=reteach_plan_id,
        load_versions=True,
    )
    if plan.status == "archived":
        raise ValueError("Archived reteach plans cannot receive AI drafts")

    normalized_provider_mode = (provider_mode or "mock").strip().lower()
    if normalized_provider_mode not in {"mock", "real"}:
        raise ValueError("Unsupported reteach plan AI provider mode")
    if normalized_provider_mode == "real":
        if not (
            settings.teacher_assist_real_provider_enabled
            or settings.teacher_assist_ai_enable_real_provider
        ):
            raise ValueError("Real reteach plan AI is disabled")
        TeacherAssistProviderCircuitBreaker().assert_can_execute(
            settings, settings.teacher_assist_ai_provider
        )
        raise ValueError("Real reteach plan AI provider execution is not enabled in this phase")

    prompt_context = build_reteach_plan_prompt_context(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        reteach_plan=plan,
        settings=settings,
    )
    if teacher_instructions and teacher_instructions.strip():
        prompt_context["teacher_instructions"] = teacher_instructions.strip()

    draft_content = _build_mock_reteach_draft(prompt_context=prompt_context)
    now = datetime.now(UTC)
    usage_event = TeacherAssistAIUsageEvent(
        tenant_id=tenant_id,
        user_id=user_id,
        workflow_id=None,
        provider="mock",
        model="mock",
        feature=RETEACH_PLAN_AI_FEATURE,
        input_tokens=0,
        output_tokens=0,
        estimated_cost_cents=0,
        metadata_json={
            "is_mock": True,
            "provider_mode": normalized_provider_mode,
            "reteach_plan_id": str(plan.id),
            "mastery_matrix_id": str(plan.mastery_matrix_id),
            "standard_id": str(plan.standard_id),
            "teacher_review_required": True,
            "prompt_version": RETEACH_PLAN_AI_PROMPT_VERSION,
        },
        created_at=now,
    )
    db.add(usage_event)
    db.flush()

    version = create_reteach_plan_version(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        reteach_plan=plan,
        content_json=draft_content,
        version_source="ai_draft",
        prompt_context_json=prompt_context,
        provider_name="mock",
        provider_model="mock",
        prompt_version=RETEACH_PLAN_AI_PROMPT_VERSION,
        ai_usage_event_id=usage_event.id,
        change_reason="AI-generated reteach draft awaiting teacher review.",
    )

    record_activity_event(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        event_type="reteach_plan_ai_drafted",
        event_category="planning",
        entity_type="reteach_plan",
        entity_id=plan.id,
        school_year_id=plan.school_year_id,
        grading_period_id=plan.grading_period_id,
        class_id=plan.class_id,
        subject_id=plan.subject_id,
        summary_text=f"Generated AI reteach draft for plan '{plan.title}'.",
        details_json={
            "reteach_plan_id": str(plan.id),
            "version_id": str(version.id),
            "provider_mode": normalized_provider_mode,
            "teacher_review_required": True,
        },
    )
    db.flush()
    db.refresh(plan)
    db.refresh(version)

    response_meta = {
        "teacher_review_required": True,
        "provider_mode": normalized_provider_mode,
        "prompt_version": RETEACH_PLAN_AI_PROMPT_VERSION,
        "message": (
            "AI reteach draft saved as a new version. Edit and review before any classroom use. "
            "This does not update mastery or publish automatically."
        ),
    }
    return plan, version, response_meta
