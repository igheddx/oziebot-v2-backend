"""TeacherAssist v2 teacher grade review, override, and confirmation."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from oziebot_api.models.teacher_assist_v2_assignment_grade import TeacherAssistV2AssignmentGrade
from oziebot_api.models.teacher_assist_v2_assignment_grade_audit_event import (
    TeacherAssistV2AssignmentGradeAuditEvent,
)
from oziebot_api.models.teacher_assist_v2_grading_draft import TeacherAssistV2GradingDraft
from oziebot_api.models.teacher_assist_v2_student_submission import TeacherAssistV2StudentSubmission
from oziebot_api.models.teacher_assist_v2_submission_review_view import TeacherAssistV2SubmissionReviewView
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist_v2.grade_review_constants import (
    ASSIGNMENT_GRADE_STATUSES,
    GRADE_REVIEW_ACTIONS,
    OFFICIAL_ASSIGNMENT_GRADE_STATUSES,
)
from oziebot_api.services.teacher_assist_v2.submission_intake import (
    _get_assignment_or_404,
    get_student_submission_or_404,
)
from oziebot_api.services.teacher_assist_v2.teacher_onboarding import get_v2_onboarding


def _now() -> datetime:
    return datetime.now(UTC)


def _percentage(score: float, max_score: float) -> float:
    if max_score <= 0:
        return 0.0
    return round((score / max_score) * 100, 2)


def _validate_grade_status(status: str) -> str:
    normalized = status.strip().upper()
    if normalized not in ASSIGNMENT_GRADE_STATUSES:
        raise ValueError(f"Unsupported assignment grade status '{status}'")
    return normalized


def _validate_review_action(action: str) -> str:
    normalized = action.strip().upper()
    if normalized not in GRADE_REVIEW_ACTIONS:
        raise ValueError(f"Unsupported grade review action '{action}'")
    return normalized


def _get_latest_grading_draft_row(
    db: Session,
    *,
    user: User,
    submission_id: uuid.UUID,
) -> TeacherAssistV2GradingDraft | None:
    return db.scalars(
        select(TeacherAssistV2GradingDraft)
        .where(
            TeacherAssistV2GradingDraft.student_submission_id == submission_id,
            TeacherAssistV2GradingDraft.teacher_user_id == user.id,
        )
        .order_by(TeacherAssistV2GradingDraft.created_at.desc())
    ).first()


def _get_latest_active_grade(
    db: Session,
    *,
    submission_id: uuid.UUID,
    statuses: set[str] | frozenset[str],
) -> TeacherAssistV2AssignmentGrade | None:
    return db.scalars(
        select(TeacherAssistV2AssignmentGrade)
        .where(
            TeacherAssistV2AssignmentGrade.student_submission_id == submission_id,
            TeacherAssistV2AssignmentGrade.status.in_(statuses),
        )
        .order_by(TeacherAssistV2AssignmentGrade.updated_at.desc())
    ).first()


def _has_review_view(db: Session, *, user: User, submission_id: uuid.UUID) -> bool:
    row = db.scalars(
        select(TeacherAssistV2SubmissionReviewView).where(
            TeacherAssistV2SubmissionReviewView.teacher_user_id == user.id,
            TeacherAssistV2SubmissionReviewView.student_submission_id == submission_id,
        )
    ).first()
    return row is not None


def record_submission_review_view(
    db: Session,
    *,
    user: User,
    submission: TeacherAssistV2StudentSubmission,
) -> None:
    existing = db.scalars(
        select(TeacherAssistV2SubmissionReviewView).where(
            TeacherAssistV2SubmissionReviewView.teacher_user_id == user.id,
            TeacherAssistV2SubmissionReviewView.student_submission_id == submission.id,
        )
    ).first()
    if existing is not None:
        return
    db.add(
        TeacherAssistV2SubmissionReviewView(
            id=uuid.uuid4(),
            tenant_id=submission.tenant_id,
            teacher_user_id=user.id,
            student_submission_id=submission.id,
            first_viewed_at=_now(),
        )
    )
    db.flush()


def resolve_review_queue_status(
    *,
    has_grading_draft: bool,
    has_review_view: bool,
    grade: TeacherAssistV2AssignmentGrade | None,
) -> str:
    if grade is not None and grade.status in OFFICIAL_ASSIGNMENT_GRADE_STATUSES:
        if grade.review_action == "REJECT":
            return "REJECTED"
        if grade.status == "CONFIRMED":
            return "CONFIRMED"
        return "REVISED"
    if grade is not None and grade.status == "DRAFT":
        return "DRAFT"
    if has_grading_draft and has_review_view:
        return "REVIEWED"
    if has_grading_draft:
        return "READY_FOR_REVIEW"
    return "PENDING"


def serialize_assignment_grade(row: TeacherAssistV2AssignmentGrade) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "assignment_id": str(row.assignment_id),
        "student_submission_id": str(row.student_submission_id),
        "student_number": row.student_number,
        "grading_draft_id": str(row.grading_draft_id) if row.grading_draft_id else None,
        "score": row.score,
        "max_score": row.max_score,
        "percentage": row.percentage,
        "rubric_json": row.rubric_json,
        "teacher_comment": row.teacher_comment,
        "teacher_override_reason": row.teacher_override_reason,
        "review_action": row.review_action,
        "confirmed_by": str(row.confirmed_by) if row.confirmed_by else None,
        "confirmed_at": row.confirmed_at.isoformat() if row.confirmed_at else None,
        "status": row.status,
        "created_at": row.created_at.isoformat(),
        "updated_at": row.updated_at.isoformat(),
        "is_official": row.status in OFFICIAL_ASSIGNMENT_GRADE_STATUSES,
    }


def serialize_grade_audit_event(row: TeacherAssistV2AssignmentGradeAuditEvent) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "assignment_grade_id": str(row.assignment_grade_id),
        "assignment_id": str(row.assignment_id),
        "student_submission_id": str(row.student_submission_id),
        "grading_draft_id": str(row.grading_draft_id) if row.grading_draft_id else None,
        "original_ai_score": row.original_ai_score,
        "original_ai_max_score": row.original_ai_max_score,
        "final_score": row.final_score,
        "final_max_score": row.final_max_score,
        "score_difference": row.score_difference,
        "teacher_override_reason": row.teacher_override_reason,
        "review_action": row.review_action,
        "teacher_comment": row.teacher_comment,
        "rubric_json": row.rubric_json,
        "reviewer_user_id": str(row.reviewer_user_id),
        "created_at": row.created_at.isoformat(),
    }


def _archive_active_grades(
    db: Session,
    *,
    submission_id: uuid.UUID,
    exclude_grade_id: uuid.UUID | None = None,
) -> None:
    rows = db.scalars(
        select(TeacherAssistV2AssignmentGrade).where(
            TeacherAssistV2AssignmentGrade.student_submission_id == submission_id,
            TeacherAssistV2AssignmentGrade.status.in_(OFFICIAL_ASSIGNMENT_GRADE_STATUSES | {"DRAFT"}),
        )
    ).all()
    now = _now()
    for row in rows:
        if exclude_grade_id is not None and row.id == exclude_grade_id:
            continue
        row.status = "ARCHIVED"
        row.updated_at = now
    db.flush()


def _create_audit_event(
    db: Session,
    *,
    grade: TeacherAssistV2AssignmentGrade,
    draft: TeacherAssistV2GradingDraft | None,
    reviewer: User,
) -> TeacherAssistV2AssignmentGradeAuditEvent:
    original_score = draft.score if draft is not None else None
    original_max = draft.max_score if draft is not None else None
    score_difference = None
    if original_score is not None:
        score_difference = round(grade.score - original_score, 2)
    event = TeacherAssistV2AssignmentGradeAuditEvent(
        id=uuid.uuid4(),
        tenant_id=grade.tenant_id,
        assignment_grade_id=grade.id,
        assignment_id=grade.assignment_id,
        student_submission_id=grade.student_submission_id,
        grading_draft_id=grade.grading_draft_id,
        original_ai_score=original_score,
        original_ai_max_score=original_max,
        final_score=grade.score,
        final_max_score=grade.max_score,
        score_difference=score_difference,
        teacher_override_reason=grade.teacher_override_reason,
        review_action=grade.review_action,
        teacher_comment=grade.teacher_comment,
        rubric_json=grade.rubric_json,
        reviewer_user_id=reviewer.id,
        created_at=_now(),
    )
    db.add(event)
    db.flush()
    return event


def _persist_grade(
    db: Session,
    *,
    user: User,
    submission: TeacherAssistV2StudentSubmission,
    draft: TeacherAssistV2GradingDraft | None,
    score: float,
    max_score: float,
    rubric_json: dict[str, Any],
    teacher_comment: str,
    teacher_override_reason: str | None,
    review_action: str,
    status: str,
) -> TeacherAssistV2AssignmentGrade:
    if max_score <= 0:
        raise ValueError("Max score must be greater than zero.")
    if score < 0 or score > max_score:
        raise ValueError("Score must be between 0 and max score.")

    normalized_action = _validate_review_action(review_action)
    normalized_status = _validate_grade_status(status)
    now = _now()
    _archive_active_grades(db, submission_id=submission.id)

    grade = TeacherAssistV2AssignmentGrade(
        id=uuid.uuid4(),
        tenant_id=submission.tenant_id,
        teacher_user_id=user.id,
        assignment_id=submission.assignment_id,
        student_submission_id=submission.id,
        student_number=submission.student_number,
        grading_draft_id=draft.id if draft is not None else None,
        score=score,
        max_score=max_score,
        percentage=_percentage(score, max_score),
        rubric_json=rubric_json,
        teacher_comment=teacher_comment.strip(),
        teacher_override_reason=teacher_override_reason.strip() if teacher_override_reason else None,
        review_action=normalized_action,
        confirmed_by=user.id if normalized_status in OFFICIAL_ASSIGNMENT_GRADE_STATUSES else None,
        confirmed_at=now if normalized_status in OFFICIAL_ASSIGNMENT_GRADE_STATUSES else None,
        status=normalized_status,
        created_at=now,
        updated_at=now,
    )
    db.add(grade)
    db.flush()
    _create_audit_event(db, grade=grade, draft=draft, reviewer=user)
    return grade


def get_assignment_grade_for_submission(
    db: Session,
    *,
    user: User,
    submission_id: uuid.UUID,
) -> dict[str, Any] | None:
    get_student_submission_or_404(db, user=user, submission_id=submission_id)
    row = _get_latest_active_grade(
        db,
        submission_id=submission_id,
        statuses=OFFICIAL_ASSIGNMENT_GRADE_STATUSES | {"DRAFT"},
    )
    if row is None:
        return None
    return serialize_assignment_grade(row)


def get_grade_audit_history(
    db: Session,
    *,
    user: User,
    submission_id: uuid.UUID,
) -> list[dict[str, Any]]:
    get_student_submission_or_404(db, user=user, submission_id=submission_id)
    rows = db.scalars(
        select(TeacherAssistV2AssignmentGradeAuditEvent)
        .where(TeacherAssistV2AssignmentGradeAuditEvent.student_submission_id == submission_id)
        .order_by(TeacherAssistV2AssignmentGradeAuditEvent.created_at.desc())
    ).all()
    return [serialize_grade_audit_event(row) for row in rows]


def accept_grading_draft(
    db: Session,
    *,
    user: User,
    submission_id: uuid.UUID,
) -> dict[str, Any]:
    submission = get_student_submission_or_404(db, user=user, submission_id=submission_id)
    draft = _get_latest_grading_draft_row(db, user=user, submission_id=submission.id)
    if draft is None:
        raise ValueError("No AI grading draft exists for this submission.")
    if _get_latest_active_grade(db, submission_id=submission.id, statuses=OFFICIAL_ASSIGNMENT_GRADE_STATUSES):
        raise ValueError("An official grade already exists for this submission.")

    grade = _persist_grade(
        db,
        user=user,
        submission=submission,
        draft=draft,
        score=draft.score,
        max_score=draft.max_score,
        rubric_json=draft.rubric_json,
        teacher_comment=draft.teacher_comment_draft,
        teacher_override_reason=None,
        review_action="ACCEPT",
        status="CONFIRMED",
    )
    return serialize_assignment_grade(grade)


def modify_grading_draft(
    db: Session,
    *,
    user: User,
    submission_id: uuid.UUID,
    score: float,
    max_score: float,
    teacher_comment: str,
    rubric_json: dict[str, Any],
    teacher_override_reason: str,
) -> dict[str, Any]:
    submission = get_student_submission_or_404(db, user=user, submission_id=submission_id)
    draft = _get_latest_grading_draft_row(db, user=user, submission_id=submission.id)
    if draft is None:
        raise ValueError("No AI grading draft exists for this submission.")
    if not teacher_override_reason.strip():
        raise ValueError("Teacher override reason is required when modifying a grade.")

    grade = _persist_grade(
        db,
        user=user,
        submission=submission,
        draft=draft,
        score=score,
        max_score=max_score,
        rubric_json=rubric_json,
        teacher_comment=teacher_comment,
        teacher_override_reason=teacher_override_reason,
        review_action="MODIFY",
        status="REVISED",
    )
    return serialize_assignment_grade(grade)


def reject_grading_draft(
    db: Session,
    *,
    user: User,
    submission_id: uuid.UUID,
    score: float,
    max_score: float,
    teacher_comment: str,
    rubric_json: dict[str, Any],
    teacher_override_reason: str | None = None,
) -> dict[str, Any]:
    submission = get_student_submission_or_404(db, user=user, submission_id=submission_id)
    draft = _get_latest_grading_draft_row(db, user=user, submission_id=submission.id)
    if draft is None:
        raise ValueError("No AI grading draft exists for this submission.")

    grade = _persist_grade(
        db,
        user=user,
        submission=submission,
        draft=draft,
        score=score,
        max_score=max_score,
        rubric_json=rubric_json,
        teacher_comment=teacher_comment,
        teacher_override_reason=teacher_override_reason,
        review_action="REJECT",
        status="REVISED",
    )
    return serialize_assignment_grade(grade)


def save_grade_review_draft(
    db: Session,
    *,
    user: User,
    submission_id: uuid.UUID,
    score: float,
    max_score: float,
    teacher_comment: str,
    rubric_json: dict[str, Any],
    teacher_override_reason: str | None = None,
) -> dict[str, Any]:
    submission = get_student_submission_or_404(db, user=user, submission_id=submission_id)
    draft = _get_latest_grading_draft_row(db, user=user, submission_id=submission.id)
    if draft is None:
        raise ValueError("No AI grading draft exists for this submission.")
    if _get_latest_active_grade(db, submission_id=submission.id, statuses=OFFICIAL_ASSIGNMENT_GRADE_STATUSES):
        raise ValueError("An official grade already exists for this submission.")

    grade = _persist_grade(
        db,
        user=user,
        submission=submission,
        draft=draft,
        score=score,
        max_score=max_score,
        rubric_json=rubric_json,
        teacher_comment=teacher_comment,
        teacher_override_reason=teacher_override_reason,
        review_action="SAVE_DRAFT",
        status="DRAFT",
    )
    return serialize_assignment_grade(grade)


def accept_all_viewed_drafts(
    db: Session,
    *,
    user: User,
    assignment_id: uuid.UUID,
) -> dict[str, Any]:
    _get_assignment_or_404(db, user=user, assignment_id=assignment_id)
    submissions = db.scalars(
        select(TeacherAssistV2StudentSubmission).where(
            TeacherAssistV2StudentSubmission.assignment_id == assignment_id,
            TeacherAssistV2StudentSubmission.teacher_user_id == user.id,
            TeacherAssistV2StudentSubmission.status != "ARCHIVED",
        )
    ).all()
    if not submissions:
        raise ValueError("No submissions found for this assignment.")

    accepted: list[dict[str, Any]] = []
    skipped: list[dict[str, str]] = []
    for submission in submissions:
        draft = _get_latest_grading_draft_row(db, user=user, submission_id=submission.id)
        if draft is None:
            continue
        if not _has_review_view(db, user=user, submission_id=submission.id):
            skipped.append(
                {
                    "student_submission_id": str(submission.id),
                    "reason": "Submission has not been opened for review.",
                }
            )
            continue
        if _get_latest_active_grade(db, submission_id=submission.id, statuses=OFFICIAL_ASSIGNMENT_GRADE_STATUSES):
            skipped.append(
                {
                    "student_submission_id": str(submission.id),
                    "reason": "Official grade already confirmed.",
                }
            )
            continue
        try:
            accepted.append(accept_grading_draft(db, user=user, submission_id=submission.id))
        except ValueError as exc:
            skipped.append({"student_submission_id": str(submission.id), "reason": str(exc)})

    if not accepted:
        raise ValueError("No viewed submissions with AI drafts are ready for bulk acceptance.")
    return {
        "accepted_count": len(accepted),
        "skipped_count": len(skipped),
        "grades": accepted,
        "skipped": skipped,
    }


def list_assignment_grade_reviews(
    db: Session,
    *,
    user: User,
    assignment_id: uuid.UUID,
) -> list[dict[str, Any]]:
    _get_assignment_or_404(db, user=user, assignment_id=assignment_id)
    submissions = db.scalars(
        select(TeacherAssistV2StudentSubmission).where(
            TeacherAssistV2StudentSubmission.assignment_id == assignment_id,
            TeacherAssistV2StudentSubmission.teacher_user_id == user.id,
            TeacherAssistV2StudentSubmission.status != "ARCHIVED",
        )
        .order_by(
            TeacherAssistV2StudentSubmission.student_number.asc().nulls_last(),
            TeacherAssistV2StudentSubmission.created_at.desc(),
        )
    ).all()
    if not submissions:
        return []

    submission_ids = [row.id for row in submissions]
    drafts: dict[uuid.UUID, TeacherAssistV2GradingDraft] = {}
    for row in db.scalars(
        select(TeacherAssistV2GradingDraft)
        .where(
            TeacherAssistV2GradingDraft.student_submission_id.in_(submission_ids),
            TeacherAssistV2GradingDraft.teacher_user_id == user.id,
        )
        .order_by(TeacherAssistV2GradingDraft.created_at.desc())
    ).all():
        if row.student_submission_id not in drafts:
            drafts[row.student_submission_id] = row

    grades: dict[uuid.UUID, TeacherAssistV2AssignmentGrade] = {}
    for row in db.scalars(
        select(TeacherAssistV2AssignmentGrade)
        .where(
            TeacherAssistV2AssignmentGrade.student_submission_id.in_(submission_ids),
            TeacherAssistV2AssignmentGrade.status.in_(OFFICIAL_ASSIGNMENT_GRADE_STATUSES | {"DRAFT"}),
        )
        .order_by(TeacherAssistV2AssignmentGrade.updated_at.desc())
    ).all():
        if row.student_submission_id not in grades:
            grades[row.student_submission_id] = row
    viewed_ids = set(
        db.scalars(
            select(TeacherAssistV2SubmissionReviewView.student_submission_id).where(
                TeacherAssistV2SubmissionReviewView.teacher_user_id == user.id,
                TeacherAssistV2SubmissionReviewView.student_submission_id.in_(submission_ids),
            )
        ).all()
    )

    rows: list[dict[str, Any]] = []
    for submission in submissions:
        draft = drafts.get(submission.id)
        grade = grades.get(submission.id)
        review_status = resolve_review_queue_status(
            has_grading_draft=draft is not None,
            has_review_view=submission.id in viewed_ids,
            grade=grade,
        )
        rows.append(
            {
                "student_submission_id": str(submission.id),
                "student_number": submission.student_number,
                "draft_score": draft.score if draft is not None else None,
                "draft_max_score": draft.max_score if draft is not None else None,
                "review_status": review_status,
                "grade_status": grade.status if grade is not None else None,
                "confirmed_by": str(grade.confirmed_by) if grade and grade.confirmed_by else None,
                "confirmed_at": grade.confirmed_at.isoformat() if grade and grade.confirmed_at else None,
                "official_score": grade.score if grade and grade.status in OFFICIAL_ASSIGNMENT_GRADE_STATUSES else None,
                "teacher_viewed": submission.id in viewed_ids,
                "has_grading_draft": draft is not None,
            }
        )
    return rows


def count_grade_review_metrics(
    db: Session,
    *,
    user: User,
    assignment_id: uuid.UUID,
) -> dict[str, int]:
    reviews = list_assignment_grade_reviews(db, user=user, assignment_id=assignment_id)
    counts = {
        "ready_for_review_count": 0,
        "reviewed_count": 0,
        "confirmed_count": 0,
        "rejected_count": 0,
        "pending_count": 0,
        "grades_confirmed_count": 0,
    }
    for row in reviews:
        status = row["review_status"]
        if status == "READY_FOR_REVIEW":
            counts["ready_for_review_count"] += 1
        elif status in {"REVIEWED", "DRAFT"}:
            counts["reviewed_count"] += 1
        elif status == "CONFIRMED":
            counts["confirmed_count"] += 1
            counts["grades_confirmed_count"] += 1
        elif status == "REJECTED":
            counts["rejected_count"] += 1
            counts["grades_confirmed_count"] += 1
        elif status == "REVISED":
            counts["grades_confirmed_count"] += 1
        elif status == "PENDING":
            counts["pending_count"] += 1
    return counts


def count_assignments_requiring_review(db: Session, *, user: User) -> int:
    draft_rows = db.execute(
        select(
            TeacherAssistV2GradingDraft.assignment_id,
            TeacherAssistV2GradingDraft.student_submission_id,
        ).where(TeacherAssistV2GradingDraft.teacher_user_id == user.id)
    ).all()
    if not draft_rows:
        return 0

    submission_ids = {row.student_submission_id for row in draft_rows}
    official_submission_ids = set(
        db.scalars(
            select(TeacherAssistV2AssignmentGrade.student_submission_id).where(
                TeacherAssistV2AssignmentGrade.student_submission_id.in_(submission_ids),
                TeacherAssistV2AssignmentGrade.status.in_(OFFICIAL_ASSIGNMENT_GRADE_STATUSES),
            )
        ).all()
    )
    pending_submission_ids = submission_ids - official_submission_ids
    if not pending_submission_ids:
        return 0

    assignment_ids = {
        row.assignment_id
        for row in draft_rows
        if row.student_submission_id in pending_submission_ids
    }
    return len(assignment_ids)


def students_assigned_count(db: Session, *, user: User) -> int:
    onboarding = get_v2_onboarding(db, user_id=user.id)
    return onboarding.student_count if onboarding and onboarding.student_count else 0


def build_assignment_completion_summary(
    db: Session,
    *,
    user: User,
    assignment_id: uuid.UUID,
    submission_summary: dict[str, int],
) -> dict[str, int]:
    review_metrics = count_grade_review_metrics(db, user=user, assignment_id=assignment_id)
    return {
        "students_assigned_count": students_assigned_count(db, user=user),
        "submissions_received_count": submission_summary.get("submitted_count", 0),
        "ai_drafts_generated_count": submission_summary.get("grading_complete_count", 0),
        "grades_confirmed_count": review_metrics["grades_confirmed_count"],
        **review_metrics,
    }
