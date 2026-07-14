from __future__ import annotations

from datetime import UTC, datetime
import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.teacher_assist_assignment_grade_record import (
    TeacherAssistAssignmentGradeRecord,
)
from oziebot_api.models.teacher_assist_assignment_gradebook_audit_event import (
    TeacherAssistAssignmentGradebookAuditEvent,
)
from oziebot_api.models.teacher_assist_assignment_gradebook_commit import (
    TeacherAssistAssignmentGradebookCommit,
)
from oziebot_api.services.teacher_assist.activity_events import record_activity_event
from oziebot_api.services.teacher_assist.assignments import get_assignment_or_404
from oziebot_api.services.teacher_assist.constants import (
    validate_assignment_grade_record_status,
    validate_assignment_gradebook_audit_event_type,
    validate_assignment_gradebook_commit_status,
    validate_assignment_gradebook_commit_type,
)
from oziebot_api.services.teacher_assist.grading_reviews import get_grading_review_or_404
from oziebot_api.services.teacher_assist.instructional_plan_validator import (
    contains_pii_like_content,
)


def _normalize_string(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def _validate_grade_payload(
    *,
    committed_score: float | None,
    max_score: float | None,
    committed_feedback: str | None,
    require_score_or_feedback: bool = True,
) -> tuple[float | None, float | None, str | None]:
    normalized_feedback = _normalize_string(committed_feedback)
    if max_score is not None and max_score < 0:
        raise ValueError("Max score cannot be negative")
    if committed_score is not None and committed_score < 0:
        raise ValueError("Committed score cannot be negative")
    if require_score_or_feedback and committed_score is None and not normalized_feedback:
        raise ValueError("Grade commits require a committed score or committed feedback")
    if contains_pii_like_content({"committed_feedback": normalized_feedback}):
        raise ValueError(
            "Grade commit content cannot include student-identifying or PII-like content"
        )
    return committed_score, max_score, normalized_feedback


def _record_gradebook_audit_event(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    teacher_user_id: uuid.UUID,
    assignment_id: uuid.UUID,
    student_number: int,
    event_type: str,
    summary_text: str,
    grade_record_id: uuid.UUID | None = None,
    gradebook_commit_id: uuid.UUID | None = None,
    details_json: dict[str, Any] | None = None,
) -> TeacherAssistAssignmentGradebookAuditEvent:
    row = TeacherAssistAssignmentGradebookAuditEvent(
        tenant_id=tenant_id,
        teacher_user_id=teacher_user_id,
        grade_record_id=grade_record_id,
        gradebook_commit_id=gradebook_commit_id,
        assignment_id=assignment_id,
        student_number=student_number,
        event_type=validate_assignment_gradebook_audit_event_type(event_type),
        summary_text=summary_text.strip(),
        details_json=details_json,
        created_at=datetime.now(UTC),
    )
    db.add(row)
    db.flush()
    return row


def get_grade_record_or_404(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    grade_record_id: uuid.UUID,
) -> TeacherAssistAssignmentGradeRecord:
    row = db.scalars(
        select(TeacherAssistAssignmentGradeRecord).where(
            TeacherAssistAssignmentGradeRecord.id == grade_record_id,
            TeacherAssistAssignmentGradeRecord.tenant_id == tenant_id,
            TeacherAssistAssignmentGradeRecord.teacher_user_id == user_id,
        )
    ).one_or_none()
    if row is None:
        raise LookupError("Grade record not found")
    return row


def get_gradebook_commit_or_404(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    gradebook_commit_id: uuid.UUID,
) -> TeacherAssistAssignmentGradebookCommit:
    row = db.scalars(
        select(TeacherAssistAssignmentGradebookCommit).where(
            TeacherAssistAssignmentGradebookCommit.id == gradebook_commit_id,
            TeacherAssistAssignmentGradebookCommit.tenant_id == tenant_id,
            TeacherAssistAssignmentGradebookCommit.teacher_user_id == user_id,
        )
    ).one_or_none()
    if row is None:
        raise LookupError("Gradebook commit not found")
    return row


def list_assignment_grade_records(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    assignment_id: uuid.UUID,
    record_status: str | None = None,
) -> list[TeacherAssistAssignmentGradeRecord]:
    get_assignment_or_404(db, tenant_id=tenant_id, user_id=user_id, assignment_id=assignment_id)
    query = select(TeacherAssistAssignmentGradeRecord).where(
        TeacherAssistAssignmentGradeRecord.tenant_id == tenant_id,
        TeacherAssistAssignmentGradeRecord.teacher_user_id == user_id,
        TeacherAssistAssignmentGradeRecord.assignment_id == assignment_id,
    )
    if record_status:
        query = query.where(
            TeacherAssistAssignmentGradeRecord.record_status
            == validate_assignment_grade_record_status(record_status)
        )
    return db.scalars(
        query.order_by(
            TeacherAssistAssignmentGradeRecord.student_number.asc(),
            TeacherAssistAssignmentGradeRecord.updated_at.desc(),
        )
    ).all()


def list_grade_record_commits(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    grade_record_id: uuid.UUID,
) -> list[TeacherAssistAssignmentGradebookCommit]:
    get_grade_record_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        grade_record_id=grade_record_id,
    )
    return db.scalars(
        select(TeacherAssistAssignmentGradebookCommit)
        .where(
            TeacherAssistAssignmentGradebookCommit.tenant_id == tenant_id,
            TeacherAssistAssignmentGradebookCommit.teacher_user_id == user_id,
            TeacherAssistAssignmentGradebookCommit.grade_record_id == grade_record_id,
        )
        .order_by(TeacherAssistAssignmentGradebookCommit.created_at.asc())
    ).all()


def list_gradebook_audit_events(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    assignment_id: uuid.UUID | None = None,
    grade_record_id: uuid.UUID | None = None,
    limit: int = 100,
) -> list[TeacherAssistAssignmentGradebookAuditEvent]:
    query = select(TeacherAssistAssignmentGradebookAuditEvent).where(
        TeacherAssistAssignmentGradebookAuditEvent.tenant_id == tenant_id,
        TeacherAssistAssignmentGradebookAuditEvent.teacher_user_id == user_id,
    )
    if assignment_id is not None:
        get_assignment_or_404(db, tenant_id=tenant_id, user_id=user_id, assignment_id=assignment_id)
        query = query.where(
            TeacherAssistAssignmentGradebookAuditEvent.assignment_id == assignment_id
        )
    if grade_record_id is not None:
        get_grade_record_or_404(
            db,
            tenant_id=tenant_id,
            user_id=user_id,
            grade_record_id=grade_record_id,
        )
        query = query.where(
            TeacherAssistAssignmentGradebookAuditEvent.grade_record_id == grade_record_id
        )
    return db.scalars(
        query.order_by(TeacherAssistAssignmentGradebookAuditEvent.created_at.desc()).limit(
            max(1, min(limit, 250))
        )
    ).all()


def build_assignment_gradebook_export_view(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    assignment_id: uuid.UUID,
) -> dict[str, Any]:
    assignment = get_assignment_or_404(
        db, tenant_id=tenant_id, user_id=user_id, assignment_id=assignment_id
    )
    records = list_assignment_grade_records(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        assignment_id=assignment_id,
    )
    commits = db.scalars(
        select(TeacherAssistAssignmentGradebookCommit)
        .where(
            TeacherAssistAssignmentGradebookCommit.tenant_id == tenant_id,
            TeacherAssistAssignmentGradebookCommit.teacher_user_id == user_id,
            TeacherAssistAssignmentGradebookCommit.assignment_id == assignment_id,
        )
        .order_by(TeacherAssistAssignmentGradebookCommit.created_at.asc())
    ).all()
    return {
        "assignment_id": str(assignment.id),
        "assignment_title": assignment.title,
        "assignment_type": assignment.assignment_type,
        "class_id": str(assignment.class_id),
        "subject_id": str(assignment.subject_id),
        "school_year_id": str(assignment.school_year_id),
        "grading_period_id": str(assignment.grading_period_id)
        if assignment.grading_period_id
        else None,
        "generated_at": datetime.now(UTC),
        "record_count": len(records),
        "active_record_count": sum(1 for row in records if row.record_status == "active"),
        "records": [
            {
                "grade_record_id": str(row.id),
                "student_number": row.student_number,
                "record_status": row.record_status,
                "committed_score": row.committed_score,
                "max_score": row.max_score,
                "committed_feedback": row.committed_feedback,
                "grading_review_id": str(row.grading_review_id),
                "current_commit_id": str(row.current_commit_id) if row.current_commit_id else None,
                "updated_at": row.updated_at.isoformat(),
            }
            for row in records
        ],
        "commits": [
            {
                "commit_id": str(row.id),
                "grade_record_id": str(row.grade_record_id),
                "student_number": row.student_number,
                "commit_type": row.commit_type,
                "commit_status": row.commit_status,
                "committed_score": row.committed_score,
                "max_score": row.max_score,
                "committed_feedback": row.committed_feedback,
                "reason": row.reason,
                "teacher_confirmation_checkpoint_at": row.teacher_confirmation_checkpoint_at.isoformat(),
                "created_at": row.created_at.isoformat(),
            }
            for row in commits
        ],
    }


def commit_grade_from_grading_review(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    grading_review_id: uuid.UUID,
    teacher_confirmation_note: str | None = None,
) -> tuple[TeacherAssistAssignmentGradeRecord, TeacherAssistAssignmentGradebookCommit]:
    review = get_grading_review_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        grading_review_id=grading_review_id,
    )
    if review.status != "teacher_confirmed":
        raise ValueError("Only teacher-confirmed grading reviews can be committed to the gradebook")
    existing = db.scalars(
        select(TeacherAssistAssignmentGradeRecord).where(
            TeacherAssistAssignmentGradeRecord.tenant_id == tenant_id,
            TeacherAssistAssignmentGradeRecord.teacher_user_id == user_id,
            TeacherAssistAssignmentGradeRecord.assignment_id == review.assignment_id,
            TeacherAssistAssignmentGradeRecord.student_number == review.student_number,
            TeacherAssistAssignmentGradeRecord.record_status == "active",
        )
    ).one_or_none()
    if existing is not None:
        raise ValueError(
            "An active grade record already exists for this assignment and student number"
        )

    committed_score = review.teacher_confirmed_score
    max_score = review.max_score
    committed_feedback = review.teacher_confirmed_feedback or review.feedback_summary
    committed_score, max_score, committed_feedback = _validate_grade_payload(
        committed_score=committed_score,
        max_score=max_score,
        committed_feedback=committed_feedback,
    )
    now = datetime.now(UTC)
    checkpoint_at = review.updated_at if review.updated_at else now

    grade_record = TeacherAssistAssignmentGradeRecord(
        tenant_id=review.tenant_id,
        teacher_user_id=review.teacher_user_id,
        assignment_id=review.assignment_id,
        student_work_submission_id=review.student_work_submission_id,
        grading_review_id=review.id,
        student_number=review.student_number,
        school_year_id=review.school_year_id,
        grading_period_id=review.grading_period_id,
        class_id=review.class_id,
        subject_id=review.subject_id,
        record_status=validate_assignment_grade_record_status("active"),
        current_commit_id=None,
        committed_score=committed_score,
        max_score=max_score,
        committed_feedback=committed_feedback,
        created_at=now,
        updated_at=now,
    )
    db.add(grade_record)
    db.flush()

    commit = TeacherAssistAssignmentGradebookCommit(
        tenant_id=review.tenant_id,
        teacher_user_id=review.teacher_user_id,
        grade_record_id=grade_record.id,
        assignment_id=review.assignment_id,
        student_work_submission_id=review.student_work_submission_id,
        grading_review_id=review.id,
        student_number=review.student_number,
        school_year_id=review.school_year_id,
        grading_period_id=review.grading_period_id,
        class_id=review.class_id,
        subject_id=review.subject_id,
        commit_type=validate_assignment_gradebook_commit_type("initial_commit"),
        commit_status=validate_assignment_gradebook_commit_status("active"),
        committed_score=committed_score,
        max_score=max_score,
        committed_feedback=committed_feedback,
        teacher_confirmation_checkpoint_at=checkpoint_at,
        reason=_normalize_string(teacher_confirmation_note),
        supersedes_commit_id=None,
        reversed_by_commit_id=None,
        audit_metadata_json={
            "grading_review_status": review.status,
            "review_source": review.review_source,
            "teacher_confirmation_note": _normalize_string(teacher_confirmation_note),
        },
        created_at=now,
    )
    db.add(commit)
    db.flush()
    grade_record.current_commit_id = commit.id
    grade_record.updated_at = now
    db.flush()

    assignment = get_assignment_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        assignment_id=review.assignment_id,
    )
    _record_gradebook_audit_event(
        db,
        tenant_id=review.tenant_id,
        teacher_user_id=review.teacher_user_id,
        assignment_id=review.assignment_id,
        student_number=review.student_number,
        event_type="commit_created",
        summary_text=f"Committed grade for STUDENT #{review.student_number} on {assignment.title}.",
        grade_record_id=grade_record.id,
        gradebook_commit_id=commit.id,
        details_json={
            "commit_type": commit.commit_type,
            "committed_score": committed_score,
            "max_score": max_score,
            "grading_review_id": str(review.id),
        },
    )
    record_activity_event(
        db,
        tenant_id=review.tenant_id,
        user_id=review.teacher_user_id,
        event_type="gradebook_commit_created",
        event_category="grading",
        entity_type="grade_record",
        entity_id=grade_record.id,
        school_year_id=review.school_year_id,
        grading_period_id=review.grading_period_id,
        class_id=review.class_id,
        subject_id=review.subject_id,
        summary_text=f"Committed grade for STUDENT #{review.student_number}.",
        details_json={
            "assignment_id": str(review.assignment_id),
            "assignment_title": assignment.title,
            "grading_review_id": str(review.id),
            "gradebook_commit_id": str(commit.id),
            "commit_type": commit.commit_type,
        },
    )
    db.refresh(grade_record)
    db.refresh(commit)
    return grade_record, commit


def create_grade_correction(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    grade_record_id: uuid.UUID,
    committed_score: float | None,
    max_score: float | None,
    committed_feedback: str | None,
    reason: str,
) -> tuple[TeacherAssistAssignmentGradeRecord, TeacherAssistAssignmentGradebookCommit]:
    grade_record = get_grade_record_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        grade_record_id=grade_record_id,
    )
    if grade_record.record_status != "active":
        raise ValueError("Only active grade records can be corrected")
    normalized_reason = _normalize_string(reason)
    if not normalized_reason:
        raise ValueError("Grade corrections require a reason")
    committed_score, max_score, committed_feedback = _validate_grade_payload(
        committed_score=committed_score,
        max_score=max_score,
        committed_feedback=committed_feedback,
    )
    if (
        committed_score == grade_record.committed_score
        and max_score == grade_record.max_score
        and committed_feedback == grade_record.committed_feedback
    ):
        raise ValueError("Grade correction must change the score, max score, or feedback")

    now = datetime.now(UTC)
    previous_commit = grade_record.current_commit_id
    if previous_commit:
        prior = get_gradebook_commit_or_404(
            db,
            tenant_id=tenant_id,
            user_id=user_id,
            gradebook_commit_id=previous_commit,
        )
        prior.commit_status = validate_assignment_gradebook_commit_status("superseded")
        _record_gradebook_audit_event(
            db,
            tenant_id=grade_record.tenant_id,
            teacher_user_id=grade_record.teacher_user_id,
            assignment_id=grade_record.assignment_id,
            student_number=grade_record.student_number,
            event_type="commit_superseded",
            summary_text=f"Superseded prior grade commit for STUDENT #{grade_record.student_number}.",
            grade_record_id=grade_record.id,
            gradebook_commit_id=prior.id,
            details_json={"superseded_by_correction": True},
        )

    commit = TeacherAssistAssignmentGradebookCommit(
        tenant_id=grade_record.tenant_id,
        teacher_user_id=grade_record.teacher_user_id,
        grade_record_id=grade_record.id,
        assignment_id=grade_record.assignment_id,
        student_work_submission_id=grade_record.student_work_submission_id,
        grading_review_id=grade_record.grading_review_id,
        student_number=grade_record.student_number,
        school_year_id=grade_record.school_year_id,
        grading_period_id=grade_record.grading_period_id,
        class_id=grade_record.class_id,
        subject_id=grade_record.subject_id,
        commit_type=validate_assignment_gradebook_commit_type("correction"),
        commit_status=validate_assignment_gradebook_commit_status("active"),
        committed_score=committed_score,
        max_score=max_score,
        committed_feedback=committed_feedback,
        teacher_confirmation_checkpoint_at=now,
        reason=normalized_reason,
        supersedes_commit_id=previous_commit,
        reversed_by_commit_id=None,
        audit_metadata_json={"correction_reason": normalized_reason},
        created_at=now,
    )
    db.add(commit)
    db.flush()

    grade_record.committed_score = committed_score
    grade_record.max_score = max_score
    grade_record.committed_feedback = committed_feedback
    grade_record.current_commit_id = commit.id
    grade_record.updated_at = now
    db.flush()

    assignment = get_assignment_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        assignment_id=grade_record.assignment_id,
    )
    _record_gradebook_audit_event(
        db,
        tenant_id=grade_record.tenant_id,
        teacher_user_id=grade_record.teacher_user_id,
        assignment_id=grade_record.assignment_id,
        student_number=grade_record.student_number,
        event_type="commit_corrected",
        summary_text=f"Corrected grade for STUDENT #{grade_record.student_number} on {assignment.title}.",
        grade_record_id=grade_record.id,
        gradebook_commit_id=commit.id,
        details_json={"reason": normalized_reason, "committed_score": committed_score},
    )
    record_activity_event(
        db,
        tenant_id=grade_record.tenant_id,
        user_id=grade_record.teacher_user_id,
        event_type="gradebook_commit_corrected",
        event_category="grading",
        entity_type="grade_record",
        entity_id=grade_record.id,
        school_year_id=grade_record.school_year_id,
        grading_period_id=grade_record.grading_period_id,
        class_id=grade_record.class_id,
        subject_id=grade_record.subject_id,
        summary_text=f"Corrected grade for STUDENT #{grade_record.student_number}.",
        details_json={
            "assignment_id": str(grade_record.assignment_id),
            "gradebook_commit_id": str(commit.id),
            "reason": normalized_reason,
        },
    )
    db.refresh(grade_record)
    db.refresh(commit)
    return grade_record, commit


def create_grade_reversal(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    grade_record_id: uuid.UUID,
    reason: str,
) -> tuple[TeacherAssistAssignmentGradeRecord, TeacherAssistAssignmentGradebookCommit]:
    grade_record = get_grade_record_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        grade_record_id=grade_record_id,
    )
    if grade_record.record_status != "active":
        raise ValueError("Only active grade records can be reversed")
    normalized_reason = _normalize_string(reason)
    if not normalized_reason:
        raise ValueError("Grade reversals require a reason")

    now = datetime.now(UTC)
    previous_commit_id = grade_record.current_commit_id
    if previous_commit_id:
        prior = get_gradebook_commit_or_404(
            db,
            tenant_id=tenant_id,
            user_id=user_id,
            gradebook_commit_id=previous_commit_id,
        )
        prior.commit_status = validate_assignment_gradebook_commit_status("reversed")
        prior.reversed_by_commit_id = None

    commit = TeacherAssistAssignmentGradebookCommit(
        tenant_id=grade_record.tenant_id,
        teacher_user_id=grade_record.teacher_user_id,
        grade_record_id=grade_record.id,
        assignment_id=grade_record.assignment_id,
        student_work_submission_id=grade_record.student_work_submission_id,
        grading_review_id=grade_record.grading_review_id,
        student_number=grade_record.student_number,
        school_year_id=grade_record.school_year_id,
        grading_period_id=grade_record.grading_period_id,
        class_id=grade_record.class_id,
        subject_id=grade_record.subject_id,
        commit_type=validate_assignment_gradebook_commit_type("reversal"),
        commit_status=validate_assignment_gradebook_commit_status("reversed"),
        committed_score=grade_record.committed_score,
        max_score=grade_record.max_score,
        committed_feedback=grade_record.committed_feedback,
        teacher_confirmation_checkpoint_at=now,
        reason=normalized_reason,
        supersedes_commit_id=previous_commit_id,
        reversed_by_commit_id=None,
        audit_metadata_json={"reversal_reason": normalized_reason},
        created_at=now,
    )
    db.add(commit)
    db.flush()

    if previous_commit_id:
        prior = get_gradebook_commit_or_404(
            db,
            tenant_id=tenant_id,
            user_id=user_id,
            gradebook_commit_id=previous_commit_id,
        )
        prior.reversed_by_commit_id = commit.id

    grade_record.record_status = validate_assignment_grade_record_status("reversed")
    grade_record.current_commit_id = commit.id
    grade_record.updated_at = now
    db.flush()

    assignment = get_assignment_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        assignment_id=grade_record.assignment_id,
    )
    _record_gradebook_audit_event(
        db,
        tenant_id=grade_record.tenant_id,
        teacher_user_id=grade_record.teacher_user_id,
        assignment_id=grade_record.assignment_id,
        student_number=grade_record.student_number,
        event_type="commit_reversed",
        summary_text=f"Reversed grade for STUDENT #{grade_record.student_number} on {assignment.title}.",
        grade_record_id=grade_record.id,
        gradebook_commit_id=commit.id,
        details_json={"reason": normalized_reason},
    )
    record_activity_event(
        db,
        tenant_id=grade_record.tenant_id,
        user_id=grade_record.teacher_user_id,
        event_type="gradebook_commit_reversed",
        event_category="grading",
        entity_type="grade_record",
        entity_id=grade_record.id,
        school_year_id=grade_record.school_year_id,
        grading_period_id=grade_record.grading_period_id,
        class_id=grade_record.class_id,
        subject_id=grade_record.subject_id,
        summary_text=f"Reversed grade for STUDENT #{grade_record.student_number}.",
        details_json={
            "assignment_id": str(grade_record.assignment_id),
            "gradebook_commit_id": str(commit.id),
            "reason": normalized_reason,
        },
    )
    db.refresh(grade_record)
    db.refresh(commit)
    return grade_record, commit
