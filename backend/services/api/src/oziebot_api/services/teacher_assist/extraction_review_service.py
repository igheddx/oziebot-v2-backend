from __future__ import annotations

from datetime import UTC, datetime, timedelta
import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.models.teacher_assist_activity_event import TeacherAssistActivityEvent
from oziebot_api.models.teacher_assist_extracted_text_record import TeacherAssistExtractedTextRecord
from oziebot_api.models.teacher_assist_extraction_job import TeacherAssistExtractionJob
from oziebot_api.services.teacher_assist.activity_events import record_activity_event
from oziebot_api.services.teacher_assist.constants import (
    validate_extraction_review_status,
    validate_teacher_assist_extraction_artifact_type,
    validate_teacher_assist_extraction_job_status,
)

REVIEW_TRANSITIONS: dict[str, set[str]] = {
    "pending_review": {"teacher_reviewing", "reviewed", "issue_flagged", "archived"},
    "teacher_reviewing": {"teacher_approved", "reviewed", "teacher_rejected", "issue_flagged", "archived"},
    "teacher_rejected": {"needs_retry", "issue_flagged", "archived"},
    "issue_flagged": {"needs_retry", "teacher_reviewing", "archived"},
    "needs_retry": {"archived"},
    "teacher_approved": {"archived"},
    "reviewed": {"archived"},
    "archived": set(),
}


def _coerce_utc_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _processing_duration_seconds(job: TeacherAssistExtractionJob) -> int | None:
    started_at = _coerce_utc_datetime(job.started_at)
    completed_at = _coerce_utc_datetime(job.completed_at)
    if started_at is None or completed_at is None:
        return None
    return max(0, int((completed_at - started_at).total_seconds()))


def get_extracted_text_or_404(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    extracted_text_id: uuid.UUID,
) -> TeacherAssistExtractedTextRecord:
    row = db.scalars(
        select(TeacherAssistExtractedTextRecord).where(
            TeacherAssistExtractedTextRecord.id == extracted_text_id,
            TeacherAssistExtractedTextRecord.tenant_id == tenant_id,
            TeacherAssistExtractedTextRecord.teacher_user_id == user_id,
        )
    ).one_or_none()
    if row is None:
        raise LookupError("TeacherAssist extracted text record not found")
    return row


def _lineage_jobs(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    job: TeacherAssistExtractionJob,
) -> list[TeacherAssistExtractionJob]:
    root_id = job.retry_root_job_id or job.id
    return db.scalars(
        select(TeacherAssistExtractionJob)
        .where(
            TeacherAssistExtractionJob.tenant_id == tenant_id,
            TeacherAssistExtractionJob.teacher_user_id == user_id,
            (
                (TeacherAssistExtractionJob.retry_root_job_id == root_id)
                | (TeacherAssistExtractionJob.id == root_id)
            ),
        )
        .order_by(TeacherAssistExtractionJob.attempt_number.asc(), TeacherAssistExtractionJob.created_at.asc())
    ).all()


def _is_low_confidence_record(record: TeacherAssistExtractedTextRecord | None) -> bool:
    if record is None:
        return False
    return record.confidence_level == "low" or (
        record.provider_confidence_score is not None and record.provider_confidence_score < 0.4
    )


def is_extraction_job_retry_eligible(
    job: TeacherAssistExtractionJob,
    record: TeacherAssistExtractedTextRecord | None = None,
) -> bool:
    if job.status in {"queued", "running"}:
        return False
    if job.status in {"failed", "cancelled"}:
        return True
    if job.status == "completed" and _is_low_confidence_record(record):
        return True
    if record is not None and record.review_status in {"teacher_rejected", "needs_retry", "issue_flagged"}:
        return True
    return False


def is_extraction_job_cancel_eligible(job: TeacherAssistExtractionJob) -> bool:
    return job.status in {"queued", "running"}


def _artifact_metadata(job: TeacherAssistExtractionJob) -> dict[str, Any]:
    return {
        "artifact_type": job.artifact_type,
        "original_filename": job.original_filename,
        "mime_type": job.mime_type,
        "file_size": job.file_size,
        "resource_library_item_id": job.resource_library_item_id,
        "student_work_submission_id": job.student_work_submission_id,
        "assignment_id": job.assignment_id,
        "student_number": job.student_number,
    }


def retry_extraction_job(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    extraction_job_id: uuid.UUID,
    settings: Settings,
) -> TeacherAssistExtractionJob:
    from oziebot_api.services.teacher_assist.extraction_jobs import get_extraction_job_or_404

    source_job = get_extraction_job_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        extraction_job_id=extraction_job_id,
    )
    source_record = source_job.extracted_text_records[0] if source_job.extracted_text_records else None
    if not is_extraction_job_retry_eligible(source_job, source_record):
        raise ValueError("Extraction job is not eligible for retry")

    active_query = select(TeacherAssistExtractionJob).where(
        TeacherAssistExtractionJob.tenant_id == tenant_id,
        TeacherAssistExtractionJob.teacher_user_id == user_id,
        TeacherAssistExtractionJob.status.in_(("queued", "running")),
    )
    if source_job.resource_library_item_id is not None:
        active_query = active_query.where(
            TeacherAssistExtractionJob.resource_library_item_id == source_job.resource_library_item_id
        )
    else:
        active_query = active_query.where(
            TeacherAssistExtractionJob.student_work_submission_id == source_job.student_work_submission_id
        )
    if db.scalars(active_query).first() is not None:
        raise ValueError("An extraction job is already queued or running for this artifact")

    retry_root_id = source_job.retry_root_job_id or source_job.id
    lineage = _lineage_jobs(db, tenant_id=tenant_id, user_id=user_id, job=source_job)
    next_attempt = max((row.attempt_number for row in lineage), default=source_job.attempt_number) + 1
    now = datetime.now(UTC)
    row = TeacherAssistExtractionJob(
        tenant_id=source_job.tenant_id,
        teacher_user_id=source_job.teacher_user_id,
        artifact_type=validate_teacher_assist_extraction_artifact_type(source_job.artifact_type),
        resource_library_item_id=source_job.resource_library_item_id,
        student_work_submission_id=source_job.student_work_submission_id,
        assignment_id=source_job.assignment_id,
        school_year_id=source_job.school_year_id,
        grading_period_id=source_job.grading_period_id,
        class_id=source_job.class_id,
        subject_id=source_job.subject_id,
        student_number=source_job.student_number,
        storage_key=source_job.storage_key,
        original_filename=source_job.original_filename,
        mime_type=source_job.mime_type,
        file_size=source_job.file_size,
        status=validate_teacher_assist_extraction_job_status("queued"),
        progress_percent=0,
        provider_name=settings.teacher_assist_ocr_provider.strip() or "mock",
        error_code=None,
        error_message=None,
        error_metadata_json=None,
        execution_log_json=[
            {
                "event": "extraction_retry_requested",
                "message": "TeacherAssist extraction retry requested",
                "metadata": {
                    "parent_extraction_job_id": str(source_job.id),
                    "retry_root_job_id": str(retry_root_id),
                    "attempt_number": next_attempt,
                },
                "recorded_at": now.isoformat(),
            }
        ],
        leased_by_worker=None,
        lease_expires_at=None,
        heartbeat_at=None,
        retry_count=0,
        max_retries=max(0, settings.teacher_assist_worker_max_retries),
        parent_extraction_job_id=source_job.id,
        retry_root_job_id=retry_root_id,
        attempt_number=next_attempt,
        timeout_at=None,
        created_at=now,
        started_at=None,
        completed_at=None,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    if source_record is not None and source_record.review_status == "teacher_rejected":
        source_record.review_status = validate_extraction_review_status("needs_retry")
        source_record.updated_at = now
    record_activity_event(
        db,
        tenant_id=row.tenant_id,
        user_id=row.teacher_user_id,
        event_type="extraction_retry_requested",
        event_category="review",
        entity_type="extraction_job",
        entity_id=row.id,
        school_year_id=row.school_year_id,
        grading_period_id=row.grading_period_id,
        class_id=row.class_id,
        subject_id=row.subject_id,
        summary_text="TeacherAssist extraction retry requested.",
        details_json={
            "parent_extraction_job_id": str(source_job.id),
            "retry_root_job_id": str(retry_root_id),
            "attempt_number": next_attempt,
        },
    )
    db.flush()
    return row


def _apply_review_notes(
    record: TeacherAssistExtractedTextRecord,
    *,
    teacher_review_notes: str | None = None,
    teacher_issue_reason: str | None = None,
) -> None:
    metadata = dict(record.metadata_json or {})
    if teacher_review_notes is not None:
        normalized_notes = teacher_review_notes.strip()
        if normalized_notes:
            metadata["teacher_review_notes"] = normalized_notes
        else:
            metadata.pop("teacher_review_notes", None)
    if teacher_issue_reason is not None:
        normalized_reason = teacher_issue_reason.strip()
        if normalized_reason:
            metadata["teacher_issue_reason"] = normalized_reason
        else:
            metadata.pop("teacher_issue_reason", None)
    record.metadata_json = metadata or None


def update_extracted_text_review_status(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    extracted_text_id: uuid.UUID,
    review_status: str,
    teacher_review_notes: str | None = None,
    teacher_issue_reason: str | None = None,
) -> TeacherAssistExtractedTextRecord:
    record = get_extracted_text_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        extracted_text_id=extracted_text_id,
    )
    normalized = validate_extraction_review_status(review_status)
    allowed = REVIEW_TRANSITIONS.get(record.review_status, set())
    if normalized not in allowed and normalized != record.review_status:
        raise ValueError(f"Cannot transition extraction review status from '{record.review_status}' to '{normalized}'")
    now = datetime.now(UTC)
    record.review_status = normalized
    record.updated_at = now
    _apply_review_notes(
        record,
        teacher_review_notes=teacher_review_notes,
        teacher_issue_reason=teacher_issue_reason if normalized == "issue_flagged" else None,
    )
    if normalized in {"teacher_reviewing", "teacher_approved", "teacher_rejected", "reviewed", "issue_flagged"}:
        record.reviewed_at = now
        record.reviewed_by_user_id = user_id
    event_type = {
        "teacher_reviewing": "extraction_review_started",
        "teacher_approved": "extraction_review_approved",
        "teacher_rejected": "extraction_review_rejected",
        "reviewed": "extraction_review_approved",
        "issue_flagged": "extraction_issue_flagged",
    }.get(normalized)
    if event_type is not None:
        details_json: dict[str, Any] = {
            "review_status": normalized,
            "extraction_job_id": str(record.extraction_job_id),
        }
        metadata = dict(record.metadata_json or {})
        if metadata.get("teacher_issue_reason"):
            details_json["teacher_issue_reason"] = metadata["teacher_issue_reason"]
        record_activity_event(
            db,
            tenant_id=record.tenant_id,
            user_id=user_id,
            event_type=event_type,
            event_category="review",
            entity_type="extracted_text",
            entity_id=record.id,
            school_year_id=record.school_year_id,
            grading_period_id=record.grading_period_id,
            class_id=record.class_id,
            subject_id=record.subject_id,
            summary_text="TeacherAssist extraction review updated.",
            details_json=details_json,
        )
    db.flush()
    return record


def save_extracted_text_approved_content(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    extracted_text_id: uuid.UUID,
    approved_text: str | None = None,
    teacher_corrected_text: str | None = None,
) -> TeacherAssistExtractedTextRecord:
    record = get_extracted_text_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        extracted_text_id=extracted_text_id,
    )
    now = datetime.now(UTC)
    corrected = teacher_corrected_text.strip() if teacher_corrected_text is not None else None
    if corrected == "":
        corrected = None
    if corrected is not None:
        record.teacher_corrected_text = corrected
        record_activity_event(
            db,
            tenant_id=record.tenant_id,
            user_id=user_id,
            event_type="extraction_text_corrected",
            event_category="review",
            entity_type="extracted_text",
            entity_id=record.id,
            school_year_id=record.school_year_id,
            grading_period_id=record.grading_period_id,
            class_id=record.class_id,
            subject_id=record.subject_id,
            summary_text="TeacherAssist extraction text corrected.",
            details_json={"extraction_job_id": str(record.extraction_job_id)},
        )
    resolved_approved = approved_text.strip() if approved_text is not None else None
    if resolved_approved == "":
        resolved_approved = None
    if resolved_approved is None and corrected is not None:
        resolved_approved = corrected
    if resolved_approved is not None:
        record.approved_text = resolved_approved
    record.updated_at = now
    db.flush()
    return record


def _activity_events_for_extracted_text(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    record: TeacherAssistExtractedTextRecord,
    job: TeacherAssistExtractionJob,
) -> list[TeacherAssistActivityEvent]:
    entity_ids = {record.id, job.id}
    if job.retry_root_job_id is not None:
        entity_ids.add(job.retry_root_job_id)
    if job.parent_extraction_job_id is not None:
        entity_ids.add(job.parent_extraction_job_id)
    return db.scalars(
        select(TeacherAssistActivityEvent)
        .where(
            TeacherAssistActivityEvent.tenant_id == tenant_id,
            TeacherAssistActivityEvent.user_id == user_id,
            TeacherAssistActivityEvent.entity_id.in_(entity_ids),
        )
        .order_by(
            TeacherAssistActivityEvent.event_timestamp.desc(),
            TeacherAssistActivityEvent.created_at.desc(),
        )
    ).all()


def get_extracted_text_detail(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    extracted_text_id: uuid.UUID,
) -> dict[str, Any]:
    record = get_extracted_text_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        extracted_text_id=extracted_text_id,
    )
    job = db.scalars(
        select(TeacherAssistExtractionJob).where(TeacherAssistExtractionJob.id == record.extraction_job_id)
    ).one()
    lineage = _lineage_jobs(db, tenant_id=tenant_id, user_id=user_id, job=job)
    return {
        "record": record,
        "job": job,
        "lineage_jobs": lineage,
        "retry_eligible": is_extraction_job_retry_eligible(job, record),
        "cancel_eligible": is_extraction_job_cancel_eligible(job),
        "processing_duration_seconds": _processing_duration_seconds(job),
        "activity_events": _activity_events_for_extracted_text(
            db,
            tenant_id=tenant_id,
            user_id=user_id,
            record=record,
            job=job,
        ),
    }


def get_extracted_text_history(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    extracted_text_id: uuid.UUID,
) -> dict[str, Any]:
    detail = get_extracted_text_detail(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        extracted_text_id=extracted_text_id,
    )
    record: TeacherAssistExtractedTextRecord = detail["record"]
    job: TeacherAssistExtractionJob = detail["job"]
    lineage_jobs: list[TeacherAssistExtractionJob] = detail["lineage_jobs"]
    job_ids = [row.id for row in lineage_jobs]
    records = db.scalars(
        select(TeacherAssistExtractedTextRecord)
        .where(
            TeacherAssistExtractedTextRecord.tenant_id == tenant_id,
            TeacherAssistExtractedTextRecord.teacher_user_id == user_id,
            TeacherAssistExtractedTextRecord.extraction_job_id.in_(job_ids),
        )
        .order_by(TeacherAssistExtractedTextRecord.created_at.asc())
    ).all()
    return {
        "current_record": record,
        "current_job": job,
        "attempt_jobs": lineage_jobs,
        "attempt_records": records,
        "activity_events": detail["activity_events"],
    }


def list_extraction_summaries(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    limit: int = 100,
) -> list[dict[str, Any]]:
    jobs = db.scalars(
        select(TeacherAssistExtractionJob)
        .where(
            TeacherAssistExtractionJob.tenant_id == tenant_id,
            TeacherAssistExtractionJob.teacher_user_id == user_id,
        )
        .order_by(TeacherAssistExtractionJob.updated_at.desc(), TeacherAssistExtractionJob.created_at.desc())
        .limit(max(1, limit))
    ).all()
    summaries: list[dict[str, Any]] = []
    for job in jobs:
        record = job.extracted_text_records[0] if job.extracted_text_records else None
        summaries.append(
            {
                "job": job,
                "record": record,
                "retry_eligible": is_extraction_job_retry_eligible(job, record),
                "processing_duration_seconds": _processing_duration_seconds(job),
            }
        )
    return summaries


def get_extraction_job_detail(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    extraction_job_id: uuid.UUID,
) -> dict[str, Any]:
    from oziebot_api.services.teacher_assist.extraction_jobs import get_extraction_job_or_404

    job = get_extraction_job_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        extraction_job_id=extraction_job_id,
    )
    record = job.extracted_text_records[0] if job.extracted_text_records else None
    lineage = _lineage_jobs(db, tenant_id=tenant_id, user_id=user_id, job=job)
    activity_events = (
        _activity_events_for_extracted_text(
            db,
            tenant_id=tenant_id,
            user_id=user_id,
            record=record,
            job=job,
        )
        if record is not None
        else db.scalars(
            select(TeacherAssistActivityEvent)
            .where(
                TeacherAssistActivityEvent.tenant_id == tenant_id,
                TeacherAssistActivityEvent.user_id == user_id,
                TeacherAssistActivityEvent.entity_id.in_(
                    [
                        job.id,
                        *([job.retry_root_job_id] if job.retry_root_job_id is not None else []),
                        *([job.parent_extraction_job_id] if job.parent_extraction_job_id is not None else []),
                    ]
                ),
            )
            .order_by(
                TeacherAssistActivityEvent.event_timestamp.desc(),
                TeacherAssistActivityEvent.created_at.desc(),
            )
        ).all()
    )
    return {
        "job": job,
        "extracted_text": record,
        "lineage_jobs": lineage,
        "retry_eligible": is_extraction_job_retry_eligible(job, record),
        "cancel_eligible": is_extraction_job_cancel_eligible(job),
        "processing_duration_seconds": _processing_duration_seconds(job),
        "execution_timeline": list(job.execution_log_json or []),
        "source_artifact": _artifact_metadata(job),
        "activity_events": activity_events,
    }


def retry_latest_eligible_extraction_for_resource(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    resource_id: uuid.UUID,
    settings: Settings,
) -> TeacherAssistExtractionJob:
    from oziebot_api.services.teacher_assist.extraction_jobs import list_resource_extraction_runs

    runs = list_resource_extraction_runs(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        resource_id=resource_id,
    )
    if not runs:
        raise ValueError("No extraction jobs found for this resource")
    job, record = runs[0]
    if not is_extraction_job_retry_eligible(job, record):
        raise ValueError("Latest resource extraction job is not eligible for retry")
    return retry_extraction_job(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        extraction_job_id=job.id,
        settings=settings,
    )


def retry_latest_eligible_extraction_for_submission(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    submission_id: uuid.UUID,
    settings: Settings,
) -> TeacherAssistExtractionJob:
    from oziebot_api.services.teacher_assist.extraction_jobs import list_student_work_extraction_runs

    runs = list_student_work_extraction_runs(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        submission_id=submission_id,
    )
    if not runs:
        raise ValueError("No extraction jobs found for this student-work submission")
    job, record = runs[0]
    if not is_extraction_job_retry_eligible(job, record):
        raise ValueError("Latest student-work extraction job is not eligible for retry")
    return retry_extraction_job(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        extraction_job_id=job.id,
        settings=settings,
    )


def is_stale_extraction_job(job: TeacherAssistExtractionJob, *, settings: Settings, now: datetime | None = None) -> bool:
    if job.status != "running":
        return False
    current = now or datetime.now(UTC)
    lease_expires_at = _coerce_utc_datetime(job.lease_expires_at)
    heartbeat_at = _coerce_utc_datetime(job.heartbeat_at)
    stale_cutoff = current - timedelta(seconds=max(1, settings.teacher_assist_worker_lease_seconds))
    if lease_expires_at is not None and lease_expires_at < current:
        return True
    if heartbeat_at is not None and heartbeat_at < stale_cutoff:
        return True
    return False
