from __future__ import annotations

from datetime import UTC, datetime
import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.teacher_assist_assignment_grading_review import (
    TeacherAssistAssignmentGradingReview,
)
from oziebot_api.models.teacher_assist_assignment_grading_review_item import (
    TeacherAssistAssignmentGradingReviewItem,
)
from oziebot_api.services.teacher_assist.activity_events import record_activity_event
from oziebot_api.services.teacher_assist.assignments import get_assignment_or_404
from oziebot_api.services.teacher_assist.constants import (
    validate_assignment_grading_review_source,
    validate_assignment_grading_review_status,
)
from oziebot_api.services.teacher_assist.instructional_plan_validator import (
    contains_pii_like_content,
)
from oziebot_api.services.teacher_assist.student_work import get_student_work_submission_or_404

GRADING_REVIEW_STATUS_TRANSITIONS: dict[str, set[str]] = {
    "draft": {
        "draft",
        "teacher_reviewing",
        "teacher_confirmed",
        "returned_for_revision",
        "archived",
    },
    "ai_suggested": {
        "ai_suggested",
        "teacher_reviewing",
        "teacher_confirmed",
        "returned_for_revision",
        "archived",
    },
    "teacher_reviewing": {
        "teacher_reviewing",
        "teacher_confirmed",
        "returned_for_revision",
        "archived",
    },
    "teacher_confirmed": {"teacher_confirmed", "returned_for_revision", "archived"},
    "returned_for_revision": {
        "returned_for_revision",
        "teacher_reviewing",
        "teacher_confirmed",
        "archived",
    },
    "archived": {"archived"},
}


def _normalize_string(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def _normalize_string_list(values: list[str] | None) -> list[str]:
    normalized: list[str] = []
    for value in values or []:
        item = value.strip()
        if item:
            normalized.append(item)
    return normalized


def _validate_grading_review_content(
    *,
    score_suggestion: float | None,
    max_score: float | None,
    feedback_summary: str | None,
    strengths: list[str] | None,
    improvement_areas: list[str] | None,
    teacher_notes: str | None,
    teacher_confirmed_score: float | None,
    teacher_confirmed_feedback: str | None,
) -> tuple[
    float | None,
    float | None,
    str | None,
    list[str],
    list[str],
    str | None,
    float | None,
    str | None,
]:
    normalized_feedback_summary = _normalize_string(feedback_summary)
    normalized_strengths = _normalize_string_list(strengths)
    normalized_improvement_areas = _normalize_string_list(improvement_areas)
    normalized_teacher_notes = _normalize_string(teacher_notes)
    normalized_teacher_confirmed_feedback = _normalize_string(teacher_confirmed_feedback)
    if max_score is not None and max_score < 0:
        raise ValueError("Max score cannot be negative")
    if score_suggestion is not None and score_suggestion < 0:
        raise ValueError("Score suggestion cannot be negative")
    if teacher_confirmed_score is not None and teacher_confirmed_score < 0:
        raise ValueError("Teacher confirmed score cannot be negative")
    if contains_pii_like_content(
        {
            "feedback_summary": normalized_feedback_summary,
            "strengths": normalized_strengths,
            "improvement_areas": normalized_improvement_areas,
            "teacher_notes": normalized_teacher_notes,
            "teacher_confirmed_feedback": normalized_teacher_confirmed_feedback,
        }
    ):
        raise ValueError(
            "Grading review content cannot include student-identifying or PII-like content"
        )
    return (
        score_suggestion,
        max_score,
        normalized_feedback_summary,
        normalized_strengths,
        normalized_improvement_areas,
        normalized_teacher_notes,
        teacher_confirmed_score,
        normalized_teacher_confirmed_feedback,
    )


def _validate_review_status_payload(
    *,
    current_status: str | None,
    next_status: str,
    teacher_confirmed_score: float | None,
    teacher_confirmed_feedback: str | None,
) -> str:
    normalized_next = validate_assignment_grading_review_status(next_status)
    if current_status is not None:
        normalized_current = validate_assignment_grading_review_status(current_status)
        allowed = GRADING_REVIEW_STATUS_TRANSITIONS[normalized_current]
        if normalized_next not in allowed:
            raise ValueError(
                f"Grading review status cannot transition from {normalized_current} to {normalized_next}"
            )
    if (
        normalized_next == "teacher_confirmed"
        and teacher_confirmed_score is None
        and not (teacher_confirmed_feedback and teacher_confirmed_feedback.strip())
    ):
        raise ValueError(
            "Teacher confirmed grading reviews require a teacher confirmed score or teacher confirmed feedback"
        )
    return normalized_next


def _normalize_review_item_inputs(
    items: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    normalized_items: list[dict[str, Any]] = []
    for index, item in enumerate(items or []):
        criterion_title = str(item.get("criterion_title") or "").strip()
        if not criterion_title:
            raise ValueError("Grading review items require a criterion title")
        (
            score_suggestion,
            max_score,
            feedback_summary,
            strengths,
            improvement_areas,
            teacher_notes,
            _teacher_confirmed_score,
            _teacher_confirmed_feedback,
        ) = _validate_grading_review_content(
            score_suggestion=item.get("score_suggestion"),
            max_score=item.get("max_score"),
            feedback_summary=item.get("feedback_summary"),
            strengths=item.get("strengths"),
            improvement_areas=item.get("improvement_areas"),
            teacher_notes=item.get("teacher_notes"),
            teacher_confirmed_score=None,
            teacher_confirmed_feedback=None,
        )
        if contains_pii_like_content({"criterion_title": criterion_title}):
            raise ValueError(
                "Grading review item content cannot include student-identifying or PII-like content"
            )
        normalized_items.append(
            {
                "criterion_title": criterion_title,
                "score_suggestion": score_suggestion,
                "max_score": max_score,
                "feedback_summary": feedback_summary,
                "strengths": strengths,
                "improvement_areas": improvement_areas,
                "teacher_notes": teacher_notes,
                "sort_order": int(item.get("sort_order", index)),
            }
        )
    return normalized_items


def _sync_grading_review_items(
    db: Session,
    *,
    review: TeacherAssistAssignmentGradingReview,
    items: list[dict[str, Any]],
) -> None:
    for row in list(review.items):
        db.delete(row)
    now = datetime.now(UTC)
    for item in items:
        db.add(
            TeacherAssistAssignmentGradingReviewItem(
                grading_review_id=review.id,
                criterion_title=item["criterion_title"],
                score_suggestion=item["score_suggestion"],
                max_score=item["max_score"],
                feedback_summary=item["feedback_summary"],
                strengths=item["strengths"],
                improvement_areas=item["improvement_areas"],
                teacher_notes=item["teacher_notes"],
                sort_order=item["sort_order"],
                created_at=now,
                updated_at=now,
            )
        )
    db.flush()
    db.refresh(review)


def get_grading_review_or_404(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    grading_review_id: uuid.UUID,
) -> TeacherAssistAssignmentGradingReview:
    row = db.scalars(
        select(TeacherAssistAssignmentGradingReview).where(
            TeacherAssistAssignmentGradingReview.id == grading_review_id,
            TeacherAssistAssignmentGradingReview.tenant_id == tenant_id,
            TeacherAssistAssignmentGradingReview.teacher_user_id == user_id,
        )
    ).one_or_none()
    if row is None:
        raise LookupError("Grading review not found")
    return row


def list_assignment_grading_reviews(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    assignment_id: uuid.UUID,
) -> list[TeacherAssistAssignmentGradingReview]:
    get_assignment_or_404(db, tenant_id=tenant_id, user_id=user_id, assignment_id=assignment_id)
    return db.scalars(
        select(TeacherAssistAssignmentGradingReview)
        .where(
            TeacherAssistAssignmentGradingReview.tenant_id == tenant_id,
            TeacherAssistAssignmentGradingReview.teacher_user_id == user_id,
            TeacherAssistAssignmentGradingReview.assignment_id == assignment_id,
        )
        .order_by(
            TeacherAssistAssignmentGradingReview.updated_at.desc(),
            TeacherAssistAssignmentGradingReview.created_at.desc(),
        )
    ).all()


def create_grading_review_from_student_work(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    student_work_submission_id: uuid.UUID,
    student_number: int,
    max_score: float | None,
    score_suggestion: float | None = None,
    feedback_summary: str | None = None,
    strengths: list[str] | None = None,
    improvement_areas: list[str] | None = None,
    teacher_notes: str | None = None,
    items: list[dict[str, Any]] | None = None,
) -> TeacherAssistAssignmentGradingReview:
    submission = get_student_work_submission_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        submission_id=student_work_submission_id,
    )
    if submission.student_number != student_number:
        raise ValueError("Student number must match the selected student work submission")
    existing = db.scalars(
        select(TeacherAssistAssignmentGradingReview).where(
            TeacherAssistAssignmentGradingReview.student_work_submission_id == submission.id,
            TeacherAssistAssignmentGradingReview.tenant_id == tenant_id,
            TeacherAssistAssignmentGradingReview.teacher_user_id == user_id,
        )
    ).one_or_none()
    if existing is not None:
        raise ValueError("A grading review already exists for this student work submission")

    (
        normalized_score_suggestion,
        normalized_max_score,
        normalized_feedback_summary,
        normalized_strengths,
        normalized_improvement_areas,
        normalized_teacher_notes,
        normalized_teacher_confirmed_score,
        normalized_teacher_confirmed_feedback,
    ) = _validate_grading_review_content(
        score_suggestion=score_suggestion,
        max_score=max_score,
        feedback_summary=feedback_summary,
        strengths=strengths,
        improvement_areas=improvement_areas,
        teacher_notes=teacher_notes,
        teacher_confirmed_score=None,
        teacher_confirmed_feedback=None,
    )
    normalized_items = _normalize_review_item_inputs(items)
    now = datetime.now(UTC)
    review = TeacherAssistAssignmentGradingReview(
        tenant_id=submission.tenant_id,
        teacher_user_id=submission.teacher_user_id,
        assignment_id=submission.assignment_id,
        student_work_submission_id=submission.id,
        student_number=submission.student_number,
        school_year_id=submission.school_year_id,
        grading_period_id=submission.grading_period_id,
        class_id=submission.class_id,
        subject_id=submission.subject_id,
        status=validate_assignment_grading_review_status("draft"),
        review_source=validate_assignment_grading_review_source("manual"),
        provider_name=None,
        provider_model=None,
        prompt_version=None,
        ai_usage_event_id=None,
        score_suggestion=normalized_score_suggestion,
        max_score=normalized_max_score,
        feedback_summary=normalized_feedback_summary,
        strengths=normalized_strengths,
        improvement_areas=normalized_improvement_areas,
        teacher_notes=normalized_teacher_notes,
        teacher_confirmed_score=normalized_teacher_confirmed_score,
        teacher_confirmed_feedback=normalized_teacher_confirmed_feedback,
        created_at=now,
        updated_at=now,
    )
    db.add(review)
    db.flush()
    if normalized_items:
        _sync_grading_review_items(db, review=review, items=normalized_items)
    record_activity_event(
        db,
        tenant_id=review.tenant_id,
        user_id=review.teacher_user_id,
        event_type="grading_review_created",
        event_category="review",
        entity_type="grading_review",
        entity_id=review.id,
        school_year_id=review.school_year_id,
        grading_period_id=review.grading_period_id,
        class_id=review.class_id,
        subject_id=review.subject_id,
        summary_text=f"Created grading review for STUDENT #{review.student_number}.",
        details_json={
            "assignment_id": str(review.assignment_id),
            "student_work_submission_id": str(review.student_work_submission_id),
            "status": review.status,
            "review_source": review.review_source,
        },
    )
    db.refresh(review)
    return review


def update_grading_review(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    grading_review_id: uuid.UUID,
    status: str,
    max_score: float | None,
    score_suggestion: float | None = None,
    feedback_summary: str | None = None,
    strengths: list[str] | None = None,
    improvement_areas: list[str] | None = None,
    teacher_notes: str | None = None,
    teacher_confirmed_score: float | None = None,
    teacher_confirmed_feedback: str | None = None,
    items: list[dict[str, Any]] | None = None,
) -> TeacherAssistAssignmentGradingReview:
    review = get_grading_review_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        grading_review_id=grading_review_id,
    )
    previous_status = review.status
    (
        normalized_score_suggestion,
        normalized_max_score,
        normalized_feedback_summary,
        normalized_strengths,
        normalized_improvement_areas,
        normalized_teacher_notes,
        normalized_teacher_confirmed_score,
        normalized_teacher_confirmed_feedback,
    ) = _validate_grading_review_content(
        score_suggestion=score_suggestion,
        max_score=max_score,
        feedback_summary=feedback_summary,
        strengths=strengths,
        improvement_areas=improvement_areas,
        teacher_notes=teacher_notes,
        teacher_confirmed_score=teacher_confirmed_score,
        teacher_confirmed_feedback=teacher_confirmed_feedback,
    )
    normalized_status = _validate_review_status_payload(
        current_status=review.status,
        next_status=status,
        teacher_confirmed_score=normalized_teacher_confirmed_score,
        teacher_confirmed_feedback=normalized_teacher_confirmed_feedback,
    )
    normalized_items = _normalize_review_item_inputs(items)
    review.status = normalized_status
    review.score_suggestion = normalized_score_suggestion
    review.max_score = normalized_max_score
    review.feedback_summary = normalized_feedback_summary
    review.strengths = normalized_strengths
    review.improvement_areas = normalized_improvement_areas
    review.teacher_notes = normalized_teacher_notes
    review.teacher_confirmed_score = normalized_teacher_confirmed_score
    review.teacher_confirmed_feedback = normalized_teacher_confirmed_feedback
    review.updated_at = datetime.now(UTC)
    db.flush()
    _sync_grading_review_items(db, review=review, items=normalized_items)
    record_activity_event(
        db,
        tenant_id=review.tenant_id,
        user_id=review.teacher_user_id,
        event_type="grading_review_confirmed"
        if review.status == "teacher_confirmed"
        else "grading_review_updated",
        event_category="review",
        entity_type="grading_review",
        entity_id=review.id,
        school_year_id=review.school_year_id,
        grading_period_id=review.grading_period_id,
        class_id=review.class_id,
        subject_id=review.subject_id,
        summary_text=(
            f"Confirmed grading review for STUDENT #{review.student_number}."
            if review.status == "teacher_confirmed"
            else f"Updated grading review for STUDENT #{review.student_number}."
        ),
        details_json={
            "assignment_id": str(review.assignment_id),
            "student_work_submission_id": str(review.student_work_submission_id),
            "previous_status": previous_status,
            "status": review.status,
        },
    )
    db.refresh(review)
    return review


def update_grading_review_status(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    grading_review_id: uuid.UUID,
    status: str,
) -> TeacherAssistAssignmentGradingReview:
    review = get_grading_review_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        grading_review_id=grading_review_id,
    )
    previous_status = review.status
    review.status = _validate_review_status_payload(
        current_status=review.status,
        next_status=status,
        teacher_confirmed_score=review.teacher_confirmed_score,
        teacher_confirmed_feedback=review.teacher_confirmed_feedback,
    )
    review.updated_at = datetime.now(UTC)
    record_activity_event(
        db,
        tenant_id=review.tenant_id,
        user_id=review.teacher_user_id,
        event_type="grading_review_confirmed"
        if review.status == "teacher_confirmed"
        else "grading_review_updated",
        event_category="review",
        entity_type="grading_review",
        entity_id=review.id,
        school_year_id=review.school_year_id,
        grading_period_id=review.grading_period_id,
        class_id=review.class_id,
        subject_id=review.subject_id,
        summary_text=(
            f"Confirmed grading review for STUDENT #{review.student_number}."
            if review.status == "teacher_confirmed"
            else f"Updated grading review status for STUDENT #{review.student_number}."
        ),
        details_json={"previous_status": previous_status, "status": review.status},
    )
    db.flush()
    return review
