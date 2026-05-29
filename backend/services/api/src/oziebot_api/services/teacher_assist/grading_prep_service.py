from __future__ import annotations

from dataclasses import dataclass
import uuid
from typing import Any

from sqlalchemy.orm import Session

from oziebot_api.models.teacher_assist_extracted_text_record import TeacherAssistExtractedTextRecord
from oziebot_api.models.teacher_assist_student_work_submission import TeacherAssistStudentWorkSubmission
from oziebot_api.services.teacher_assist.assignments import get_assignment_or_404
from oziebot_api.services.teacher_assist.extraction_jobs import latest_extraction_state_for_submissions
from oziebot_api.services.teacher_assist.student_work import (
    get_student_work_submission_or_404,
    list_assignment_student_work_submissions,
)

GRADING_PREP_APPROVED_REVIEW_STATUSES = frozenset({"teacher_approved", "reviewed"})
GRADING_PREP_BLOCKED_REVIEW_STATUSES = frozenset(
    {
        "pending_review",
        "teacher_reviewing",
        "teacher_rejected",
        "issue_flagged",
        "needs_retry",
        "archived",
    }
)

AI_GRADING_ENABLED = False


@dataclass(frozen=True)
class ApprovedTextResolution:
    approved_text: str
    text_source: str
    review_status: str
    extracted_text_record_id: uuid.UUID
    extraction_job_id: uuid.UUID
    text_char_count: int


def is_review_status_eligible_for_grading_prep(review_status: str) -> bool:
    return review_status in GRADING_PREP_APPROVED_REVIEW_STATUSES


def resolve_approved_text_from_record(
    record: TeacherAssistExtractedTextRecord,
) -> ApprovedTextResolution | None:
    if not is_review_status_eligible_for_grading_prep(record.review_status):
        return None

    text: str | None = None
    text_source: str | None = None
    if record.approved_text and record.approved_text.strip():
        text = record.approved_text.strip()
        text_source = "approved_text"
    elif record.teacher_corrected_text and record.teacher_corrected_text.strip():
        text = record.teacher_corrected_text.strip()
        text_source = "teacher_corrected_text"
    elif record.extracted_text and record.extracted_text.strip():
        text = record.extracted_text.strip()
        text_source = "extracted_text"

    if text is None or text_source is None:
        return None

    return ApprovedTextResolution(
        approved_text=text,
        text_source=text_source,
        review_status=record.review_status,
        extracted_text_record_id=record.id,
        extraction_job_id=record.extraction_job_id,
        text_char_count=len(text),
    )


def grading_prep_blocked_reason(record: TeacherAssistExtractedTextRecord | None) -> str | None:
    if record is None:
        return "no_extracted_text"
    if record.review_status in GRADING_PREP_BLOCKED_REVIEW_STATUSES:
        return f"review_status:{record.review_status}"
    if not is_review_status_eligible_for_grading_prep(record.review_status):
        return f"review_status:{record.review_status}"
    if resolve_approved_text_from_record(record) is None:
        return "empty_approved_text"
    return None


def _submission_grading_prep_item(
    *,
    submission: TeacherAssistStudentWorkSubmission,
    record: TeacherAssistExtractedTextRecord | None,
) -> dict[str, Any]:
    resolution = resolve_approved_text_from_record(record) if record is not None else None
    blocked_reason = grading_prep_blocked_reason(record)
    ready = resolution is not None and blocked_reason is None
    return {
        "student_work_submission_id": submission.id,
        "student_number": submission.student_number,
        "ready_for_grading_prep": ready,
        "blocked_reason": None if ready else blocked_reason,
        "review_status": record.review_status if record is not None else None,
        "text_source": resolution.text_source if resolution is not None else None,
        "text_char_count": resolution.text_char_count if resolution is not None else None,
        "extracted_text_record_id": resolution.extracted_text_record_id if resolution is not None else None,
        "extraction_job_id": resolution.extraction_job_id if resolution is not None else None,
    }


def get_student_work_grading_prep_context(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    submission_id: uuid.UUID,
) -> dict[str, Any]:
    submission = get_student_work_submission_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        submission_id=submission_id,
    )
    latest_state = latest_extraction_state_for_submissions(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        submission_ids=[submission.id],
    )
    _, record = latest_state.get(submission.id, (None, None))
    item = _submission_grading_prep_item(submission=submission, record=record)
    ready = bool(item["ready_for_grading_prep"])
    resolution = resolve_approved_text_from_record(record) if record is not None else None
    if ready:
        message = (
            "Teacher-approved text is available for future grading-prep workflows. "
            "AI grading remains disabled in this phase."
        )
    elif item["blocked_reason"] == "no_extracted_text":
        message = "Upload and extract student work, then complete teacher review before grading prep."
    elif item["blocked_reason"] and str(item["blocked_reason"]).startswith("review_status:"):
        message = (
            "Extracted text is not teacher-approved yet. Approve or mark reviewed before grading prep."
        )
    else:
        message = "Approved text is not available for grading prep yet."

    return {
        "student_work_submission_id": submission.id,
        "assignment_id": submission.assignment_id,
        "student_number": submission.student_number,
        "ready_for_grading_prep": ready,
        "blocked_reason": item["blocked_reason"],
        "review_status": item["review_status"],
        "text_source": item["text_source"],
        "approved_text": resolution.approved_text if resolution is not None else None,
        "text_char_count": item["text_char_count"],
        "extracted_text_record_id": item["extracted_text_record_id"],
        "extraction_job_id": item["extraction_job_id"],
        "ai_grading_enabled": AI_GRADING_ENABLED,
        "message": message,
    }


def get_assignment_grading_prep_summary(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    assignment_id: uuid.UUID,
) -> dict[str, Any]:
    assignment = get_assignment_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        assignment_id=assignment_id,
    )
    submissions = list_assignment_student_work_submissions(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        assignment_id=assignment_id,
    )
    latest_state = latest_extraction_state_for_submissions(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        submission_ids=[row.id for row in submissions],
    )
    submission_items: list[dict[str, Any]] = []
    ready_count = 0
    for submission in submissions:
        _, record = latest_state.get(submission.id, (None, None))
        item = _submission_grading_prep_item(submission=submission, record=record)
        submission_items.append(item)
        if item["ready_for_grading_prep"]:
            ready_count += 1

    blocked_count = len(submission_items) - ready_count
    return {
        "assignment_id": assignment.id,
        "assignment_title": assignment.title,
        "total_submissions": len(submission_items),
        "ready_for_grading_prep_count": ready_count,
        "blocked_count": blocked_count,
        "submissions": submission_items,
        "ai_grading_enabled": AI_GRADING_ENABLED,
        "message": (
            "Grading prep summary reflects teacher-approved extraction text only. "
            "AI grading and mastery updates remain disabled. Gradebook commits require a separate teacher-confirmed manual action."
        ),
    }
