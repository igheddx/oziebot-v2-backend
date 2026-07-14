from __future__ import annotations

from datetime import UTC, datetime
import hashlib
import uuid
from typing import Any

from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.models.teacher_assist_ai_usage_event import TeacherAssistAIUsageEvent
from oziebot_api.models.teacher_assist_newsletter import TeacherAssistNewsletter
from oziebot_api.models.teacher_assist_newsletter_version import TeacherAssistNewsletterVersion
from oziebot_api.services.teacher_assist.activity_events import record_activity_event
from oziebot_api.services.teacher_assist.constants import validate_newsletter_regeneratable_section
from oziebot_api.services.teacher_assist.newsletters import (
    build_newsletter_prompt_context,
    create_newsletter_version,
    get_newsletter_or_404,
)
from oziebot_api.services.teacher_assist.prompt_contracts import (
    NEWSLETTER_AI_FEATURE,
    NEWSLETTER_AI_PROMPT_VERSION,
    NEWSLETTER_SECTION_REGEN_FEATURE,
    NEWSLETTER_SECTION_REGEN_PROMPT_VERSION,
)
from oziebot_api.services.teacher_assist.provider_config import TeacherAssistProviderCircuitBreaker


def _section_content_key(section: str) -> str:
    if section == "upcoming_learning":
        return "upcoming_topics"
    return section


def _build_mock_newsletter_draft(*, prompt_context: dict[str, Any]) -> dict[str, Any]:
    subject_label = "this week's learning"
    plans = prompt_context.get("weekly_plan_summaries") or []
    if plans:
        subject_label = str(plans[0].get("title") or plans[0].get("module_title") or subject_label)
    standards = list(prompt_context.get("standards_covered_hints") or [])
    assignments = list(prompt_context.get("instructional_assignments") or [])
    digest = hashlib.sha256(subject_label.encode("utf-8")).hexdigest()
    focus_index = int(digest[:2], 16) % 3
    focus_templates = [
        "conceptual understanding and vocabulary",
        "collaborative practice and discussion",
        "application tasks tied to real-world examples",
    ]
    focus = focus_templates[focus_index]
    period_title = prompt_context.get("grading_period_title") or "the current grading period"

    return {
        "overview": (
            f"This week our class continued {focus} during {subject_label}. "
            f"Families can ask students to explain one idea they practiced in class."
        ),
        "what_we_learned": [
            f"Students explored key concepts through {focus}.",
            f"We connected classroom work to standards-aligned goals for {period_title}.",
            "Students practiced explaining their thinking without sharing individual scores.",
        ],
        "standards_covered": standards[:6]
        or [
            "Standards-aligned objectives from recent lesson plans.",
            "Skills reinforced through class assignments and activities.",
        ],
        "upcoming_topics": [
            "Continue building on this week's foundational skills.",
            "Introduce a short application task to deepen understanding.",
            "Review prior concepts with a low-stakes classroom check.",
        ],
        "reminders": [
            "Please ensure students arrive with required materials each day.",
            "Check the class communication channel for schedule updates.",
            "Reach out to the teacher with questions — TeacherAssist does not send messages automatically.",
        ],
        "celebration_highlights": [
            "Strong class participation during instructional activities.",
            "Students showed persistence while working through challenging tasks.",
            "Collaborative learning routines are improving week over week.",
        ],
        "teacher_message": (
            "Thank you for supporting learning at home. Ask your student to teach back one concept "
            "from this week using everyday language."
        ),
        "teacher_review_required": True,
        "is_ai_draft": True,
        "prompt_version": NEWSLETTER_AI_PROMPT_VERSION,
        "source_assignment_count": len(assignments),
        "source_plan_count": len(plans),
    }


def _build_mock_section_regen(
    *,
    section: str,
    prompt_context: dict[str, Any],
    existing_content: dict[str, Any] | None = None,
) -> Any:
    draft = _build_mock_newsletter_draft(prompt_context=prompt_context)
    content_key = _section_content_key(section)
    value = draft.get(content_key)
    if section == "overview" and existing_content:
        return f"{value} (Regenerated for teacher review.)"
    if section == "teacher_message" and existing_content:
        return f"{value} Updated message awaiting your approval."
    if isinstance(value, list):
        return [f"{item} (Regenerated section.)" for item in value[:4]]
    return value


def _create_ai_usage_event(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    newsletter: TeacherAssistNewsletter,
    feature: str,
    prompt_version: str,
    provider_mode: str,
    section: str | None = None,
) -> TeacherAssistAIUsageEvent:
    now = datetime.now(UTC)
    metadata: dict[str, Any] = {
        "is_mock": True,
        "provider_mode": provider_mode,
        "newsletter_id": str(newsletter.id),
        "teacher_review_required": True,
        "prompt_version": prompt_version,
        "pii_policy": "NO_STUDENT_NAMES_GRADES_BEHAVIOR",
    }
    if section:
        metadata["section"] = section
    usage_event = TeacherAssistAIUsageEvent(
        tenant_id=tenant_id,
        user_id=user_id,
        workflow_id=None,
        provider="mock",
        model="mock",
        feature=feature,
        input_tokens=0,
        output_tokens=0,
        estimated_cost_cents=0,
        metadata_json=metadata,
        created_at=now,
    )
    db.add(usage_event)
    db.flush()
    return usage_event


def generate_newsletter_ai_draft(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    newsletter_id: uuid.UUID,
    provider_mode: str = "mock",
    teacher_instructions: str | None = None,
    settings: Settings | None = None,
) -> tuple[TeacherAssistNewsletter, TeacherAssistNewsletterVersion, dict[str, Any]]:
    settings = settings or Settings()
    newsletter = get_newsletter_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        newsletter_id=newsletter_id,
        load_versions=True,
    )
    if newsletter.status == "archived":
        raise ValueError("Archived newsletters cannot receive AI drafts")

    normalized_provider_mode = (provider_mode or "mock").strip().lower()
    if normalized_provider_mode not in {"mock", "real"}:
        raise ValueError("Unsupported newsletter AI provider mode")
    if normalized_provider_mode == "real":
        if not (
            settings.teacher_assist_real_provider_enabled
            or settings.teacher_assist_ai_enable_real_provider
        ):
            raise ValueError("Real newsletter AI is disabled")
        TeacherAssistProviderCircuitBreaker().assert_can_execute(
            settings, settings.teacher_assist_ai_provider
        )
        raise ValueError("Real newsletter AI provider execution is not enabled in this phase")

    prompt_context = build_newsletter_prompt_context(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        newsletter=newsletter,
    )
    if teacher_instructions and teacher_instructions.strip():
        prompt_context["teacher_instructions"] = teacher_instructions.strip()

    draft_content = _build_mock_newsletter_draft(prompt_context=prompt_context)
    usage_event = _create_ai_usage_event(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        newsletter=newsletter,
        feature=NEWSLETTER_AI_FEATURE,
        prompt_version=NEWSLETTER_AI_PROMPT_VERSION,
        provider_mode=normalized_provider_mode,
    )
    version = create_newsletter_version(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        newsletter=newsletter,
        content_json=draft_content,
        version_source="ai_draft",
        prompt_context_json=prompt_context,
        provider_name="mock",
        provider_model="mock",
        prompt_version=NEWSLETTER_AI_PROMPT_VERSION,
        ai_usage_event_id=usage_event.id,
        change_reason="AI-generated newsletter draft awaiting teacher review.",
    )
    record_activity_event(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        event_type="newsletter_ai_drafted",
        event_category="communication",
        entity_type="newsletter",
        entity_id=newsletter.id,
        school_year_id=newsletter.school_year_id,
        grading_period_id=newsletter.grading_period_id,
        class_id=newsletter.class_id,
        subject_id=newsletter.subject_id,
        summary_text=f"Generated AI newsletter draft for '{newsletter.title}'.",
        details_json={
            "newsletter_id": str(newsletter.id),
            "version_id": str(version.id),
            "provider_mode": normalized_provider_mode,
            "teacher_review_required": True,
        },
    )
    db.flush()
    db.refresh(newsletter)
    db.refresh(version)
    response_meta = {
        "teacher_review_required": True,
        "provider_mode": normalized_provider_mode,
        "prompt_version": NEWSLETTER_AI_PROMPT_VERSION,
        "message": (
            "AI newsletter draft saved as a new version. Review and edit before any family communication. "
            "TeacherAssist never sends messages automatically."
        ),
    }
    return newsletter, version, response_meta


def regenerate_newsletter_section(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    newsletter_id: uuid.UUID,
    section: str,
    provider_mode: str = "mock",
    teacher_instructions: str | None = None,
    settings: Settings | None = None,
) -> tuple[TeacherAssistNewsletter, TeacherAssistNewsletterVersion, dict[str, Any]]:
    settings = settings or Settings()
    normalized_section = validate_newsletter_regeneratable_section(section)
    newsletter = get_newsletter_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        newsletter_id=newsletter_id,
        load_versions=True,
    )
    if newsletter.status == "archived":
        raise ValueError("Archived newsletters cannot be regenerated")

    normalized_provider_mode = (provider_mode or "mock").strip().lower()
    if normalized_provider_mode not in {"mock", "real"}:
        raise ValueError("Unsupported newsletter AI provider mode")
    if normalized_provider_mode == "real":
        raise ValueError("Real newsletter section regeneration is not enabled in this phase")

    base_content = dict(
        (newsletter.current_version.content_json if newsletter.current_version else {}) or {}
    )
    if newsletter.current_version is None and normalized_section != "overview":
        raise ValueError("Generate a full AI draft before regenerating individual sections")
    prompt_context = build_newsletter_prompt_context(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        newsletter=newsletter,
    )
    if teacher_instructions and teacher_instructions.strip():
        prompt_context["teacher_instructions"] = teacher_instructions.strip()
    prompt_context["regenerated_section"] = normalized_section

    content_key = _section_content_key(normalized_section)
    regenerated_value = _build_mock_section_regen(
        section=normalized_section,
        prompt_context=prompt_context,
        existing_content=base_content,
    )
    updated_content = {
        **base_content,
        content_key: regenerated_value,
        "teacher_review_required": True,
        "is_ai_draft": True,
        "prompt_version": NEWSLETTER_SECTION_REGEN_PROMPT_VERSION,
        "last_regenerated_section": normalized_section,
    }
    usage_event = _create_ai_usage_event(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        newsletter=newsletter,
        feature=NEWSLETTER_SECTION_REGEN_FEATURE,
        prompt_version=NEWSLETTER_SECTION_REGEN_PROMPT_VERSION,
        provider_mode=normalized_provider_mode,
        section=normalized_section,
    )
    version = create_newsletter_version(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        newsletter=newsletter,
        content_json=updated_content,
        version_source="ai_section_regen",
        prompt_context_json=prompt_context,
        provider_name="mock",
        provider_model="mock",
        prompt_version=NEWSLETTER_SECTION_REGEN_PROMPT_VERSION,
        ai_usage_event_id=usage_event.id,
        change_reason=f"AI regenerated newsletter section '{normalized_section}'.",
    )
    record_activity_event(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        event_type="newsletter_section_regenerated",
        event_category="communication",
        entity_type="newsletter",
        entity_id=newsletter.id,
        school_year_id=newsletter.school_year_id,
        grading_period_id=newsletter.grading_period_id,
        class_id=newsletter.class_id,
        subject_id=newsletter.subject_id,
        summary_text=f"Regenerated newsletter section '{normalized_section}'.",
        details_json={
            "newsletter_id": str(newsletter.id),
            "version_id": str(version.id),
            "section": normalized_section,
            "teacher_review_required": True,
        },
    )
    db.flush()
    db.refresh(newsletter)
    db.refresh(version)
    response_meta = {
        "teacher_review_required": True,
        "provider_mode": normalized_provider_mode,
        "prompt_version": NEWSLETTER_SECTION_REGEN_PROMPT_VERSION,
        "section": normalized_section,
        "message": "Section regenerated and saved as a new version. Teacher review required before sending.",
    }
    return newsletter, version, response_meta
