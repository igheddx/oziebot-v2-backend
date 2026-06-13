"""Post-upload submission workflow: placeholders, auto-grading, and assignment completion."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.models.teacher_assist_v2_assignment import TeacherAssistV2Assignment
from oziebot_api.models.teacher_assist_v2_assignment_grade import TeacherAssistV2AssignmentGrade
from oziebot_api.models.teacher_assist_v2_student_submission import TeacherAssistV2StudentSubmission
from oziebot_api.models.teacher_assist_v2_submission_batch import TeacherAssistV2SubmissionBatch
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist_v2.grade_review_constants import OFFICIAL_ASSIGNMENT_GRADE_STATUSES
from oziebot_api.services.teacher_assist_v2.submission_intake_constants import (
    TERMINAL_STUDENT_SUBMISSION_STATUSES,
)
from oziebot_api.services.teacher_assist_v2.teacher_onboarding import get_v2_onboarding

ROSTER_PLACEHOLDER_BATCH_FILENAME = "__roster_placeholders__"


def _now() -> datetime:
    return datetime.now(UTC)


def not_uploaded_file_key(*, assignment_id: uuid.UUID, student_number: int) -> str:
    return f"not-uploaded://{assignment_id}/{student_number}"


def is_uploaded_submission(row: TeacherAssistV2StudentSubmission) -> bool:
    return row.status not in {"NOT_UPLOADED", "ARCHIVED"}


def roster_student_count(db: Session, *, user: User) -> int:
    onboarding = get_v2_onboarding(db, user_id=user.id)
    return onboarding.student_count if onboarding and onboarding.student_count else 0


def existing_submission_for_student(
    db: Session,
    *,
    assignment_id: uuid.UUID,
    student_number: int,
) -> TeacherAssistV2StudentSubmission | None:
    rows = db.scalars(
        select(TeacherAssistV2StudentSubmission)
        .where(
            TeacherAssistV2StudentSubmission.assignment_id == assignment_id,
            TeacherAssistV2StudentSubmission.student_number == student_number,
            TeacherAssistV2StudentSubmission.status != "ARCHIVED",
        )
        .order_by(TeacherAssistV2StudentSubmission.updated_at.desc())
    ).all()
    if not rows:
        return None
    for row in rows:
        if _submission_counts_as_resolved(db, submission=row):
            return row
    return rows[0]


def _submission_has_official_grade(
    db: Session,
    *,
    submission: TeacherAssistV2StudentSubmission,
) -> bool:
    row = db.scalar(
        select(TeacherAssistV2AssignmentGrade.id).where(
            TeacherAssistV2AssignmentGrade.student_submission_id == submission.id,
            TeacherAssistV2AssignmentGrade.status.in_(OFFICIAL_ASSIGNMENT_GRADE_STATUSES),
        )
    )
    return row is not None


def _submission_counts_as_resolved(
    db: Session,
    *,
    submission: TeacherAssistV2StudentSubmission,
) -> bool:
    if submission.status in TERMINAL_STUDENT_SUBMISSION_STATUSES:
        return True
    return _submission_has_official_grade(db, submission=submission)


def pending_submission_student_numbers(
    db: Session,
    *,
    user: User,
    assignment: TeacherAssistV2Assignment,
) -> list[int]:
    submissions = db.scalars(
        select(TeacherAssistV2StudentSubmission).where(
            TeacherAssistV2StudentSubmission.assignment_id == assignment.id,
            TeacherAssistV2StudentSubmission.teacher_user_id == user.id,
            TeacherAssistV2StudentSubmission.status != "ARCHIVED",
        )
    ).all()
    pending: list[int] = []
    for submission in submissions:
        if not _submission_counts_as_resolved(db, submission=submission):
            pending.append(submission.student_number or 0)
    return sorted({number for number in pending})


def ensure_roster_placeholder_submissions(
    db: Session,
    *,
    user: User,
    assignment: TeacherAssistV2Assignment,
    batch: TeacherAssistV2SubmissionBatch,
    uploaded_student_numbers: set[int],
    now: datetime | None = None,
) -> list[TeacherAssistV2StudentSubmission]:
    created_at = now or _now()
    roster_size = roster_student_count(db, user=user)
    if roster_size <= 0:
        return []

    placeholders: list[TeacherAssistV2StudentSubmission] = []
    for student_number in range(1, roster_size + 1):
        if student_number in uploaded_student_numbers:
            continue
        if existing_submission_for_student(db, assignment_id=assignment.id, student_number=student_number):
            continue
        row = TeacherAssistV2StudentSubmission(
            id=uuid.uuid4(),
            tenant_id=assignment.tenant_id,
            teacher_user_id=user.id,
            assignment_id=assignment.id,
            submission_batch_id=batch.id,
            packet_id=None,
            platform_school_year_id=assignment.platform_school_year_id,
            catalog_district_id=assignment.catalog_district_id,
            catalog_school_id=assignment.catalog_school_id,
            catalog_grade_id=assignment.catalog_grade_id,
            catalog_subject_id=assignment.catalog_subject_id,
            student_number=student_number,
            status="NOT_UPLOADED",
            file_key=not_uploaded_file_key(assignment_id=assignment.id, student_number=student_number),
            original_filename="Assignment not uploaded",
            mime_type="application/octet-stream",
            file_size=0,
            page_range=None,
            qr_identifier=None,
            match_method="UNKNOWN",
            created_at=created_at,
            updated_at=created_at,
        )
        placeholders.append(row)
    return placeholders


def auto_grade_submissions(
    db: Session,
    *,
    user: User,
    submissions: list[TeacherAssistV2StudentSubmission],
    settings: Settings,
) -> dict[str, Any]:
    from oziebot_api.services.teacher_assist_v2.grading_drafts import create_grading_job_for_submission

    graded: list[str] = []
    failed: list[dict[str, str]] = []
    for submission in submissions:
        if submission.status != "PROCESSING":
            continue
        try:
            create_grading_job_for_submission(
                db,
                user=user,
                submission_id=submission.id,
                settings=settings,
            )
            submission.status = "READY_FOR_REVIEW"
            submission.updated_at = _now()
            graded.append(str(submission.id))
        except Exception as exc:
            failed.append(
                {
                    "student_submission_id": str(submission.id),
                    "student_number": str(submission.student_number),
                    "error": str(exc),
                }
            )
    db.flush()
    return {"graded_count": len(graded), "failed_count": len(failed), "graded": graded, "failed": failed}


def get_or_create_roster_placeholder_batch(
    db: Session,
    *,
    user: User,
    assignment: TeacherAssistV2Assignment,
    now: datetime | None = None,
) -> TeacherAssistV2SubmissionBatch:
    existing = db.scalars(
        select(TeacherAssistV2SubmissionBatch).where(
            TeacherAssistV2SubmissionBatch.assignment_id == assignment.id,
            TeacherAssistV2SubmissionBatch.teacher_user_id == user.id,
            TeacherAssistV2SubmissionBatch.original_filename == ROSTER_PLACEHOLDER_BATCH_FILENAME,
        )
    ).first()
    if existing is not None:
        return existing

    created_at = now or _now()
    batch = TeacherAssistV2SubmissionBatch(
        id=uuid.uuid4(),
        tenant_id=assignment.tenant_id,
        teacher_user_id=user.id,
        assignment_id=assignment.id,
        platform_school_year_id=assignment.platform_school_year_id,
        catalog_district_id=assignment.catalog_district_id,
        catalog_school_id=assignment.catalog_school_id,
        catalog_grade_id=assignment.catalog_grade_id,
        catalog_subject_id=assignment.catalog_subject_id,
        status="READY_FOR_REVIEW",
        uploaded_file_key=f"roster://{assignment.id}",
        original_filename=ROSTER_PLACEHOLDER_BATCH_FILENAME,
        mime_type="application/octet-stream",
        file_size=0,
        created_at=created_at,
    )
    db.add(batch)
    db.flush()
    return batch


def ensure_roster_submission_slots(
    db: Session,
    *,
    user: User,
    assignment: TeacherAssistV2Assignment,
    now: datetime | None = None,
) -> None:
    roster_size = roster_student_count(db, user=user)
    if roster_size <= 0:
        return

    created_at = now or _now()
    batch = get_or_create_roster_placeholder_batch(db, user=user, assignment=assignment, now=created_at)
    for student_number in range(1, roster_size + 1):
        if existing_submission_for_student(db, assignment_id=assignment.id, student_number=student_number):
            continue
        row = TeacherAssistV2StudentSubmission(
            id=uuid.uuid4(),
            tenant_id=assignment.tenant_id,
            teacher_user_id=user.id,
            assignment_id=assignment.id,
            submission_batch_id=batch.id,
            packet_id=None,
            platform_school_year_id=assignment.platform_school_year_id,
            catalog_district_id=assignment.catalog_district_id,
            catalog_school_id=assignment.catalog_school_id,
            catalog_grade_id=assignment.catalog_grade_id,
            catalog_subject_id=assignment.catalog_subject_id,
            student_number=student_number,
            status="NOT_UPLOADED",
            file_key=not_uploaded_file_key(assignment_id=assignment.id, student_number=student_number),
            original_filename="Assignment not uploaded",
            mime_type="application/octet-stream",
            file_size=0,
            page_range=None,
            qr_identifier=None,
            match_method="UNKNOWN",
            created_at=created_at,
            updated_at=created_at,
        )
        db.add(row)
    db.flush()


def mark_submission_review_outcome(
    submission: TeacherAssistV2StudentSubmission,
    *,
    outcome: str,
) -> None:
    normalized = outcome.strip().upper()
    if normalized not in {"CONFIRMED", "INCOMPLETE"}:
        raise ValueError("Unsupported submission review outcome.")
    submission.status = normalized
    submission.updated_at = _now()


def refresh_assignment_completion_status(
    db: Session,
    *,
    user: User,
    assignment: TeacherAssistV2Assignment,
) -> bool:
    if pending_submission_student_numbers(db, user=user, assignment=assignment):
        return False

    submissions = db.scalars(
        select(TeacherAssistV2StudentSubmission).where(
            TeacherAssistV2StudentSubmission.assignment_id == assignment.id,
            TeacherAssistV2StudentSubmission.teacher_user_id == user.id,
            TeacherAssistV2StudentSubmission.status != "ARCHIVED",
        )
    ).all()
    if not submissions:
        return False

    if assignment.status != "COMPLETED":
        assignment.status = "COMPLETED"
        assignment.updated_at = _now()
        db.flush()
    return True


def list_assignment_review_queue(
    db: Session,
    *,
    user: User,
    assignment_id: uuid.UUID,
) -> list[dict[str, Any]]:
    from oziebot_api.services.teacher_assist_v2.submission_intake import _get_assignment_or_404

    assignment = _get_assignment_or_404(db, user=user, assignment_id=assignment_id)
    rows = db.scalars(
        select(TeacherAssistV2StudentSubmission).where(
            TeacherAssistV2StudentSubmission.assignment_id == assignment.id,
            TeacherAssistV2StudentSubmission.teacher_user_id == user.id,
            TeacherAssistV2StudentSubmission.status != "ARCHIVED",
        )
        .order_by(
            TeacherAssistV2StudentSubmission.student_number.asc().nulls_last(),
            TeacherAssistV2StudentSubmission.created_at.asc(),
        )
    ).all()
    return [
        {
            "id": str(row.id),
            "student_number": row.student_number,
            "status": row.status,
            "match_method": row.match_method,
            "page_range": row.page_range,
            "original_filename": row.original_filename,
            "created_at": row.created_at.isoformat(),
        }
        for row in rows
    ]
