from __future__ import annotations

from datetime import UTC, datetime
import hashlib
import uuid

from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.models.teacher_assist_ai_usage_event import TeacherAssistAIUsageEvent
from oziebot_api.models.teacher_assist_assignment_grading_review import (
    TeacherAssistAssignmentGradingReview,
)
from oziebot_api.services.teacher_assist.activity_events import record_activity_event
from oziebot_api.services.teacher_assist.assignments import get_assignment_or_404
from oziebot_api.services.teacher_assist.constants import (
    validate_assignment_grading_review_source,
    validate_assignment_grading_review_status,
)
from oziebot_api.services.teacher_assist.grading_prep_service import (
    get_student_work_grading_prep_context,
)
from oziebot_api.services.teacher_assist.grading_reviews import (
    get_grading_review_or_404,
)
from oziebot_api.services.teacher_assist.prompt_contracts import (
    GRADING_ASSIST_FEATURE,
    GRADING_ASSIST_PROMPT_VERSION,
)
from oziebot_api.services.teacher_assist.provider_config import TeacherAssistProviderCircuitBreaker


def _mock_confidence_level(*, approved_text: str, student_number: int) -> str:
    digest = hashlib.sha256(f"{student_number}:{approved_text[:256]}".encode("utf-8")).hexdigest()
    bucket = int(digest[:2], 16) % 3
    return ("low", "medium", "high")[bucket]


def _build_mock_grading_suggestion(
    *,
    approved_text: str,
    student_number: int,
    max_score: float | None,
    teacher_instructions: str | None,
    text_source: str,
) -> dict:
    normalized_max = max_score if max_score is not None and max_score > 0 else 10.0
    confidence_level = _mock_confidence_level(
        approved_text=approved_text, student_number=student_number
    )
    word_count = len(approved_text.split())
    completeness_ratio = min(1.0, word_count / 120)
    suggested_score = round(normalized_max * (0.55 + (0.35 * completeness_ratio)), 1)
    instruction_note = (
        f" Teacher focus: {teacher_instructions.strip()}"
        if teacher_instructions and teacher_instructions.strip()
        else ""
    )
    return {
        "suggested_score": suggested_score,
        "max_score": normalized_max,
        "feedback_summary": (
            f"[MOCK AI] Draft feedback for STUDENT #{student_number} based on teacher-approved "
            f"{text_source.replace('_', ' ')}.{instruction_note} Review and edit before confirming."
        ),
        "strengths": [
            "Response reflects student thinking aligned to the approved extraction text.",
            "Work shows enough detail for a formative grading conversation.",
        ],
        "improvement_areas": [
            "Add clearer evidence or examples where the assignment expects justification.",
            "Tighten organization so the main claim is easier to follow.",
        ],
        "rubric_notes": (
            "Mock rubric note: score suggestion is a draft only and must be teacher-confirmed."
        ),
        "confidence_level": confidence_level,
        "teacher_review_required": True,
    }


def generate_grading_review_ai_suggestion(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    grading_review_id: uuid.UUID,
    provider_mode: str = "mock",
    teacher_instructions: str | None = None,
    settings: Settings | None = None,
) -> tuple[TeacherAssistAssignmentGradingReview, dict[str, object]]:
    settings = settings or Settings()
    review = get_grading_review_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        grading_review_id=grading_review_id,
    )
    if review.status == "teacher_confirmed":
        raise ValueError("Teacher-confirmed grading reviews cannot receive new AI suggestions")

    prep_context = get_student_work_grading_prep_context(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        submission_id=review.student_work_submission_id,
    )
    if not prep_context["ready_for_grading_prep"]:
        blocked_reason = prep_context.get("blocked_reason") or "grading_prep_not_ready"
        raise ValueError(f"Grading prep is not ready: {blocked_reason}")

    normalized_provider_mode = (provider_mode or "mock").strip().lower()
    if normalized_provider_mode not in {"mock", "real"}:
        raise ValueError("Unsupported grading assist provider mode")
    if normalized_provider_mode == "real":
        if not (
            settings.teacher_assist_real_provider_enabled
            or settings.teacher_assist_ai_enable_real_provider
        ):
            raise ValueError("Real grading assist is disabled")
        TeacherAssistProviderCircuitBreaker().assert_can_execute(
            settings, settings.teacher_assist_ai_provider
        )
        raise ValueError("Real grading assist provider execution is not enabled in this phase")

    approved_text = str(prep_context["approved_text"] or "")
    text_source = str(prep_context.get("text_source") or "approved_text")
    suggestion = _build_mock_grading_suggestion(
        approved_text=approved_text,
        student_number=review.student_number,
        max_score=review.max_score,
        teacher_instructions=teacher_instructions,
        text_source=text_source,
    )

    now = datetime.now(UTC)
    review.score_suggestion = float(suggestion["suggested_score"])
    review.max_score = float(suggestion["max_score"])
    review.feedback_summary = str(suggestion["feedback_summary"])
    review.strengths = list(suggestion["strengths"])
    review.improvement_areas = list(suggestion["improvement_areas"])
    review.teacher_notes = (
        f"{suggestion['rubric_notes']}\nAI confidence: {suggestion['confidence_level']}\n"
        "Teacher review required before confirmation."
    )
    review.status = validate_assignment_grading_review_status("ai_suggested")
    review.review_source = validate_assignment_grading_review_source("ai_placeholder")
    review.provider_name = "mock"
    review.provider_model = "mock"
    review.prompt_version = GRADING_ASSIST_PROMPT_VERSION
    review.updated_at = now

    usage_event = TeacherAssistAIUsageEvent(
        tenant_id=review.tenant_id,
        user_id=review.teacher_user_id,
        workflow_id=None,
        provider="mock",
        model="mock",
        feature=GRADING_ASSIST_FEATURE,
        input_tokens=0,
        output_tokens=0,
        estimated_cost_cents=0,
        metadata_json={
            "is_mock": True,
            "provider_mode": normalized_provider_mode,
            "grading_review_id": str(review.id),
            "student_work_submission_id": str(review.student_work_submission_id),
            "text_source": text_source,
            "confidence_level": suggestion["confidence_level"],
            "teacher_review_required": True,
        },
        created_at=now,
    )
    db.add(usage_event)
    db.flush()
    review.ai_usage_event_id = usage_event.id

    assignment = get_assignment_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        assignment_id=review.assignment_id,
    )
    record_activity_event(
        db,
        tenant_id=review.tenant_id,
        user_id=review.teacher_user_id,
        event_type="grading_review_ai_suggested",
        event_category="review",
        entity_type="grading_review",
        entity_id=review.id,
        school_year_id=review.school_year_id,
        grading_period_id=review.grading_period_id,
        class_id=review.class_id,
        subject_id=review.subject_id,
        summary_text=f"Generated AI grading suggestion for STUDENT #{review.student_number}.",
        details_json={
            "assignment_id": str(review.assignment_id),
            "assignment_title": assignment.title,
            "student_work_submission_id": str(review.student_work_submission_id),
            "text_source": text_source,
            "confidence_level": suggestion["confidence_level"],
            "provider_mode": normalized_provider_mode,
        },
    )
    db.flush()
    db.refresh(review)

    response_meta = {
        "confidence_level": suggestion["confidence_level"],
        "teacher_review_required": True,
        "rubric_notes": suggestion["rubric_notes"],
        "text_source": text_source,
        "message": (
            "AI grading suggestion saved as draft. Edit the review and manually confirm when ready."
        ),
    }
    return review, response_meta
