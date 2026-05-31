"""TeacherAssist v2 assignment-scoped student work submission intake."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session, selectinload

from oziebot_api.config import Settings
from oziebot_api.models.education_catalog import EducationObjective
from oziebot_api.models.teacher_assist_v2_assignment import TeacherAssistV2Assignment
from oziebot_api.models.teacher_assist_v2_grading_draft import TeacherAssistV2GradingDraft
from oziebot_api.models.teacher_assist_v2_student_submission import TeacherAssistV2StudentSubmission
from oziebot_api.models.teacher_assist_v2_submission_batch import TeacherAssistV2SubmissionBatch
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.storage import (
    StoredTeacherAssistUpload,
    get_teacher_assist_download_url,
    teacher_assist_file_exists,
)
from oziebot_api.services.teacher_assist_v2.planning_workflow import _require_planning_ready
from oziebot_api.services.teacher_assist_v2.qr_matching import (
    extract_qr_identifiers,
    lookup_qr_match,
    parse_student_number_from_filename,
)
from oziebot_api.services.teacher_assist_v2.submission_intake_constants import (
    STUDENT_SUBMISSION_STATUSES,
    STUDENT_WORK_MIME_TYPES,
    SUBMISSION_MATCH_METHODS,
)
from oziebot_api.services.teacher_assist_v2.teacher_onboarding import get_v2_onboarding


def _now() -> datetime:
    return datetime.now(UTC)


def _validate_submission_status(status: str) -> str:
    normalized = status.strip().upper()
    if normalized not in STUDENT_SUBMISSION_STATUSES:
        raise ValueError(f"Unsupported student submission status '{status}'")
    return normalized


def _validate_match_method(match_method: str) -> str:
    normalized = match_method.strip().upper()
    if normalized not in SUBMISSION_MATCH_METHODS:
        raise ValueError(f"Unsupported submission match method '{match_method}'")
    return normalized


def _validate_student_work_mime_type(mime_type: str) -> str:
    normalized = mime_type.strip().lower()
    if normalized not in STUDENT_WORK_MIME_TYPES:
        raise ValueError("Student work uploads must be PDF or image files.")
    return normalized


def _get_assignment_or_404(
    db: Session,
    *,
    user: User,
    assignment_id: uuid.UUID,
) -> TeacherAssistV2Assignment:
    _require_planning_ready(db, user=user)
    row = db.scalars(
        select(TeacherAssistV2Assignment).where(
            TeacherAssistV2Assignment.id == assignment_id,
            TeacherAssistV2Assignment.teacher_user_id == user.id,
        )
    ).one_or_none()
    if row is None:
        raise LookupError("Assignment not found")
    return row


def _validate_submission_anchors(*, assignment: TeacherAssistV2Assignment) -> None:
    missing: list[str] = []
    if not assignment.platform_school_year_id:
        missing.append("school_year")
    if not assignment.catalog_district_id:
        missing.append("district")
    if not assignment.catalog_grade_id:
        missing.append("grade")
    if not assignment.catalog_subject_id:
        missing.append("subject")
    if missing:
        raise ValueError(f"Submission is missing required anchors: {', '.join(missing)}")


def _validate_student_number(
    db: Session,
    *,
    user: User,
    student_number: int,
) -> int:
    if student_number < 1:
        raise ValueError("Student number must be greater than zero.")
    onboarding = get_v2_onboarding(db, user_id=user.id)
    roster_size = onboarding.student_count if onboarding and onboarding.student_count else None
    if roster_size is not None and student_number > roster_size:
        raise ValueError(f"Student number must be between 1 and {roster_size}.")
    return student_number


def _build_submission_counts(db: Session, *, assignment_id: uuid.UUID) -> dict[str, int]:
    rows = db.execute(
        select(
            TeacherAssistV2StudentSubmission.status,
            func.count(TeacherAssistV2StudentSubmission.id),
        )
        .where(
            TeacherAssistV2StudentSubmission.assignment_id == assignment_id,
            TeacherAssistV2StudentSubmission.status != "ARCHIVED",
        )
        .group_by(TeacherAssistV2StudentSubmission.status)
    ).all()
    counts = {status: count for status, count in rows}
    matched = counts.get("MATCHED", 0) + counts.get("MANUAL_MATCH", 0)
    summary = {
        "submitted_count": sum(counts.values()),
        "matched_count": matched,
        "needs_review_count": counts.get("NEEDS_REVIEW", 0),
        "ready_for_grading_count": counts.get("READY_FOR_GRADING", 0),
    }
    summary.update(
        {
            "grading_complete_count": int(
                db.scalar(
                    select(func.count(func.distinct(TeacherAssistV2GradingDraft.student_submission_id))).where(
                        TeacherAssistV2GradingDraft.assignment_id == assignment_id
                    )
                )
                or 0
            ),
            "teacher_reviewed_count": 0,
        }
    )
    return summary


def serialize_submission_batch(row: TeacherAssistV2SubmissionBatch) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "assignment_id": str(row.assignment_id),
        "status": row.status,
        "original_filename": row.original_filename,
        "mime_type": row.mime_type,
        "file_size": row.file_size,
        "created_at": row.created_at.isoformat(),
        "submission_count": len(row.student_submissions),
    }


def serialize_student_submission_summary(
    row: TeacherAssistV2StudentSubmission,
    *,
    has_grading_draft: bool = False,
) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "assignment_id": str(row.assignment_id),
        "submission_batch_id": str(row.submission_batch_id),
        "student_number": row.student_number,
        "status": row.status,
        "match_method": row.match_method,
        "original_filename": row.original_filename,
        "created_at": row.created_at.isoformat(),
        "has_grading_draft": has_grading_draft,
    }


def serialize_student_submission_detail(
    db: Session,
    *,
    row: TeacherAssistV2StudentSubmission,
    assignment: TeacherAssistV2Assignment,
    user: User | None = None,
    settings: Settings | None = None,
) -> dict[str, Any]:
    objective_ids = [uuid.UUID(str(value)) for value in assignment.education_objective_ids_json]
    objectives = db.scalars(
        select(EducationObjective).where(EducationObjective.id.in_(objective_ids))
    ).all() if objective_ids else []
    download_url = None
    if settings and teacher_assist_file_exists(settings, storage_key=row.file_key):
        download_url = get_teacher_assist_download_url(
            settings,
            storage_key=row.file_key,
            original_filename=row.original_filename,
            mime_type=row.mime_type,
        )
    return {
        "id": str(row.id),
        "assignment_id": str(row.assignment_id),
        "assignment_title": assignment.title,
        "submission_batch_id": str(row.submission_batch_id),
        "student_number": row.student_number,
        "status": row.status,
        "match_method": row.match_method,
        "packet_id": str(row.packet_id) if row.packet_id else None,
        "page_range": row.page_range,
        "qr_identifier": row.qr_identifier,
        "original_filename": row.original_filename,
        "mime_type": row.mime_type,
        "file_size": row.file_size,
        "download_url": download_url,
        "preview_url": download_url,
        "objectives": [
            {
                "id": str(objective.id),
                "objective_id": objective.objective_id,
                "description": objective.description,
            }
            for objective in objectives
        ],
        "created_at": row.created_at.isoformat(),
        "updated_at": row.updated_at.isoformat(),
        "grading_draft": _serialize_latest_grading_draft(db, user=user, submission_id=row.id),
    }


def _serialize_latest_grading_draft(
    db: Session,
    *,
    user: User | None,
    submission_id: uuid.UUID,
) -> dict[str, Any] | None:
    if user is None:
        return None
    row = db.scalars(
        select(TeacherAssistV2GradingDraft)
        .where(
            TeacherAssistV2GradingDraft.student_submission_id == submission_id,
            TeacherAssistV2GradingDraft.teacher_user_id == user.id,
        )
        .order_by(TeacherAssistV2GradingDraft.created_at.desc())
    ).first()
    if row is None:
        return None
    return {
        "id": str(row.id),
        "score": row.score,
        "max_score": row.max_score,
        "percentage": row.percentage,
        "rubric_json": row.rubric_json,
        "teacher_comment_draft": row.teacher_comment_draft,
        "strengths": row.strengths,
        "improvements": row.improvements,
        "objective_evidence": row.objective_evidence,
        "confidence_score": row.confidence_score,
        "provider": row.provider,
        "model": row.model,
        "created_at": row.created_at.isoformat(),
        "teacher_review_required": True,
    }


def get_assignment_submission_summary(db: Session, *, assignment_id: uuid.UUID) -> dict[str, int]:
    return _build_submission_counts(db, assignment_id=assignment_id)


def list_assignment_submission_batches(
    db: Session,
    *,
    user: User,
    assignment_id: uuid.UUID,
) -> list[dict[str, Any]]:
    _get_assignment_or_404(db, user=user, assignment_id=assignment_id)
    rows = db.scalars(
        select(TeacherAssistV2SubmissionBatch)
        .where(
            TeacherAssistV2SubmissionBatch.assignment_id == assignment_id,
            TeacherAssistV2SubmissionBatch.teacher_user_id == user.id,
            TeacherAssistV2SubmissionBatch.status != "ARCHIVED",
        )
        .options(selectinload(TeacherAssistV2SubmissionBatch.student_submissions))
        .order_by(TeacherAssistV2SubmissionBatch.created_at.desc())
    ).all()
    return [serialize_submission_batch(row) for row in rows]


def list_assignment_student_submissions(
    db: Session,
    *,
    user: User,
    assignment_id: uuid.UUID,
) -> list[dict[str, Any]]:
    _get_assignment_or_404(db, user=user, assignment_id=assignment_id)
    rows = db.scalars(
        select(TeacherAssistV2StudentSubmission)
        .where(
            TeacherAssistV2StudentSubmission.assignment_id == assignment_id,
            TeacherAssistV2StudentSubmission.teacher_user_id == user.id,
            TeacherAssistV2StudentSubmission.status != "ARCHIVED",
        )
        .order_by(
            TeacherAssistV2StudentSubmission.student_number.asc().nulls_last(),
            TeacherAssistV2StudentSubmission.created_at.desc(),
        )
    ).all()
    draft_submission_ids = set(
        db.scalars(
            select(TeacherAssistV2GradingDraft.student_submission_id).where(
                TeacherAssistV2GradingDraft.assignment_id == assignment_id,
                TeacherAssistV2GradingDraft.teacher_user_id == user.id,
            )
        ).all()
    )
    return [
        serialize_student_submission_summary(row, has_grading_draft=row.id in draft_submission_ids)
        for row in rows
    ]


def _create_student_submission_row(
    *,
    assignment: TeacherAssistV2Assignment,
    batch: TeacherAssistV2SubmissionBatch,
    user: User,
    stored: StoredTeacherAssistUpload,
    student_number: int | None,
    status: str,
    match_method: str,
    packet_id: uuid.UUID | None = None,
    qr_identifier: str | None = None,
    page_range: str | None = None,
) -> TeacherAssistV2StudentSubmission:
    now = _now()
    row = TeacherAssistV2StudentSubmission(
        id=uuid.uuid4(),
        tenant_id=assignment.tenant_id,
        teacher_user_id=user.id,
        assignment_id=assignment.id,
        submission_batch_id=batch.id,
        packet_id=packet_id,
        platform_school_year_id=assignment.platform_school_year_id,
        catalog_district_id=assignment.catalog_district_id,
        catalog_school_id=assignment.catalog_school_id,
        catalog_grade_id=assignment.catalog_grade_id,
        catalog_subject_id=assignment.catalog_subject_id,
        student_number=student_number,
        status=_validate_submission_status(status),
        file_key=stored.storage_key,
        original_filename=stored.original_filename,
        mime_type=stored.mime_type,
        file_size=stored.file_size,
        page_range=page_range,
        qr_identifier=qr_identifier,
        match_method=_validate_match_method(match_method),
        created_at=now,
        updated_at=now,
    )
    return row


def _process_submission_batch(
    db: Session,
    *,
    user: User,
    assignment: TeacherAssistV2Assignment,
    batch: TeacherAssistV2SubmissionBatch,
    stored: StoredTeacherAssistUpload,
    file_bytes: bytes,
    student_number: int | None,
) -> None:
    batch.status = "PROCESSING"
    db.flush()

    submissions: list[TeacherAssistV2StudentSubmission] = []

    if student_number is not None:
        normalized_student = _validate_student_number(db, user=user, student_number=student_number)
        submissions.append(
            _create_student_submission_row(
                assignment=assignment,
                batch=batch,
                user=user,
                stored=stored,
                student_number=normalized_student,
                status="MANUAL_MATCH",
                match_method="MANUAL",
            )
        )
    else:
        qr_identifiers = extract_qr_identifiers(
            original_filename=stored.original_filename,
            file_bytes=file_bytes,
        )
        for qr_identifier in qr_identifiers:
            match = lookup_qr_match(db, assignment_id=assignment.id, qr_identifier=qr_identifier)
            if match is None:
                continue
            submissions.append(
                _create_student_submission_row(
                    assignment=assignment,
                    batch=batch,
                    user=user,
                    stored=stored,
                    student_number=match.student_number,
                    status="MATCHED",
                    match_method="QR",
                    packet_id=match.packet_id,
                    qr_identifier=match.qr_identifier,
                    page_range=str(match.page_number),
                )
            )

        if not submissions:
            filename_student = parse_student_number_from_filename(stored.original_filename)
            if filename_student is not None:
                normalized_student = _validate_student_number(
                    db,
                    user=user,
                    student_number=filename_student,
                )
                submissions.append(
                    _create_student_submission_row(
                        assignment=assignment,
                        batch=batch,
                        user=user,
                        stored=stored,
                        student_number=normalized_student,
                        status="MANUAL_MATCH",
                        match_method="FILENAME",
                    )
                )

        if not submissions:
            submissions.append(
                _create_student_submission_row(
                    assignment=assignment,
                    batch=batch,
                    user=user,
                    stored=stored,
                    student_number=None,
                    status="NEEDS_REVIEW",
                    match_method="UNKNOWN",
                )
            )

    for submission in submissions:
        db.add(submission)
    db.flush()

    if any(row.status == "NEEDS_REVIEW" for row in submissions):
        batch.status = "NEEDS_REVIEW"
    elif len(submissions) == 1 and submissions[0].status in {"MATCHED", "MANUAL_MATCH"}:
        batch.status = "MATCHED"
    elif submissions:
        batch.status = "MATCHED"
    else:
        batch.status = "FAILED"
    db.flush()


def create_assignment_submission_batch(
    db: Session,
    *,
    user: User,
    assignment_id: uuid.UUID,
    stored: StoredTeacherAssistUpload,
    file_bytes: bytes,
    student_number: int | None = None,
) -> dict[str, Any]:
    assignment = _get_assignment_or_404(db, user=user, assignment_id=assignment_id)
    _validate_submission_anchors(assignment=assignment)
    _validate_student_work_mime_type(stored.mime_type)

    normalized_filename = Path(stored.original_filename.strip()).name
    if not normalized_filename:
        raise ValueError("Uploaded file must include a filename.")

    now = _now()
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
        status="UPLOADED",
        uploaded_file_key=stored.storage_key,
        original_filename=normalized_filename,
        mime_type=stored.mime_type,
        file_size=stored.file_size,
        created_at=now,
    )
    db.add(batch)
    db.flush()

    try:
        _process_submission_batch(
            db,
            user=user,
            assignment=assignment,
            batch=batch,
            stored=stored,
            file_bytes=file_bytes,
            student_number=student_number,
        )
    except Exception:
        batch.status = "FAILED"
        db.flush()
        raise

    db.refresh(batch)
    return {
        **serialize_submission_batch(batch),
        "submissions": [serialize_student_submission_summary(row) for row in batch.student_submissions],
    }


def get_student_submission_or_404(
    db: Session,
    *,
    user: User,
    submission_id: uuid.UUID,
) -> TeacherAssistV2StudentSubmission:
    row = db.scalars(
        select(TeacherAssistV2StudentSubmission).where(
            TeacherAssistV2StudentSubmission.id == submission_id,
            TeacherAssistV2StudentSubmission.teacher_user_id == user.id,
        )
    ).one_or_none()
    if row is None:
        raise LookupError("Student submission not found")
    return row


def get_student_submission_detail(
    db: Session,
    *,
    user: User,
    submission_id: uuid.UUID,
    settings: Settings | None = None,
) -> dict[str, Any]:
    row = get_student_submission_or_404(db, user=user, submission_id=submission_id)
    assignment = _get_assignment_or_404(db, user=user, assignment_id=row.assignment_id)
    return serialize_student_submission_detail(
        db,
        row=row,
        assignment=assignment,
        user=user,
        settings=settings,
    )


def manually_match_student_submission(
    db: Session,
    *,
    user: User,
    submission_id: uuid.UUID,
    student_number: int,
) -> dict[str, Any]:
    row = get_student_submission_or_404(db, user=user, submission_id=submission_id)
    if row.status not in {"NEEDS_REVIEW", "MATCHED", "MANUAL_MATCH"}:
        raise ValueError("Submission cannot be manually matched in its current status.")
    normalized_student = _validate_student_number(db, user=user, student_number=student_number)
    row.student_number = normalized_student
    row.status = "MANUAL_MATCH"
    row.match_method = "MANUAL"
    row.updated_at = _now()
    db.flush()

    batch = db.get(TeacherAssistV2SubmissionBatch, row.submission_batch_id)
    if batch is not None and batch.status == "NEEDS_REVIEW":
        pending = db.scalar(
            select(func.count(TeacherAssistV2StudentSubmission.id)).where(
                TeacherAssistV2StudentSubmission.submission_batch_id == batch.id,
                TeacherAssistV2StudentSubmission.status == "NEEDS_REVIEW",
            )
        )
        if pending == 0:
            batch.status = "MATCHED"

    assignment = _get_assignment_or_404(db, user=user, assignment_id=row.assignment_id)
    return serialize_student_submission_detail(db, row=row, assignment=assignment, user=user)


def update_student_submission_status(
    db: Session,
    *,
    user: User,
    submission_id: uuid.UUID,
    status: str,
) -> dict[str, Any]:
    row = get_student_submission_or_404(db, user=user, submission_id=submission_id)
    normalized = _validate_submission_status(status)
    if normalized == "READY_FOR_GRADING":
        if row.student_number is None:
            raise ValueError("Assign a student number before marking ready for grading.")
        if row.status not in {"MATCHED", "MANUAL_MATCH", "NEEDS_REVIEW"}:
            raise ValueError("Submission cannot be marked ready for grading from its current status.")
    elif normalized == "ARCHIVED":
        if row.status == "READY_FOR_GRADING":
            raise ValueError("Ready-for-grading submissions cannot be archived in this phase.")
    else:
        raise ValueError("Only READY_FOR_GRADING or ARCHIVED status updates are supported.")
    row.status = normalized
    row.updated_at = _now()
    db.flush()
    assignment = _get_assignment_or_404(db, user=user, assignment_id=row.assignment_id)
    return serialize_student_submission_detail(db, row=row, assignment=assignment, user=user)
