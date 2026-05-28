from __future__ import annotations

from datetime import UTC, datetime, timedelta
import time
import traceback
import uuid

from sqlalchemy import or_, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from oziebot_api.config import Settings
from oziebot_api.models.teacher_assist_extracted_text_record import TeacherAssistExtractedTextRecord
from oziebot_api.models.teacher_assist_extraction_job import TeacherAssistExtractionJob
from oziebot_api.services.teacher_assist.activity_events import record_activity_event
from oziebot_api.services.teacher_assist.artifact_processing import sanitize_extracted_text
from oziebot_api.services.teacher_assist.constants import (
    validate_extraction_confidence_level,
    validate_extraction_review_status,
    validate_teacher_assist_extraction_artifact_type,
    validate_teacher_assist_extraction_job_status,
)
from oziebot_api.services.teacher_assist.ocr_errors import TeacherAssistOCRProviderError
from oziebot_api.services.teacher_assist.ocr_provider import get_teacher_assist_ocr_provider
from oziebot_api.services.teacher_assist.ocr_provider_config import (
    assert_ocr_artifact_supported,
    resolve_teacher_assist_ocr_provider_mode,
)
from oziebot_api.services.teacher_assist.planning import get_resource_or_404
from oziebot_api.services.teacher_assist.storage import open_teacher_assist_stream
from oziebot_api.services.teacher_assist.student_work import get_student_work_submission_or_404

EXTRACTION_LOG_LIMIT = 50


def _coerce_utc_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _append_job_log(
    job: TeacherAssistExtractionJob,
    *,
    event: str,
    message: str,
    metadata: dict[str, object] | None = None,
) -> None:
    entries = list(job.execution_log_json or [])
    entries.append(
        {
            "event": event,
            "message": message,
            "metadata": metadata or {},
            "recorded_at": datetime.now(UTC).isoformat(),
        }
    )
    job.execution_log_json = entries[-EXTRACTION_LOG_LIMIT:]


def _clear_job_lease(job: TeacherAssistExtractionJob) -> None:
    job.leased_by_worker = None
    job.lease_expires_at = None
    job.heartbeat_at = None
    job.timeout_at = None


def _touch_job_heartbeat(
    job: TeacherAssistExtractionJob,
    *,
    settings: Settings,
    worker_name: str,
    progress_percent: int | None = None,
) -> None:
    now = datetime.now(UTC)
    job.leased_by_worker = worker_name
    job.heartbeat_at = now
    job.lease_expires_at = now + timedelta(seconds=max(1, settings.teacher_assist_worker_lease_seconds))
    if job.timeout_at is None:
        job.timeout_at = now + timedelta(seconds=max(1, settings.teacher_assist_extraction_timeout_seconds))
    job.updated_at = now
    if progress_percent is not None:
        job.progress_percent = progress_percent


def _set_job_status(
    job: TeacherAssistExtractionJob,
    *,
    status: str,
    progress_percent: int | None = None,
    error_message: str | None = None,
) -> None:
    now = datetime.now(UTC)
    job.status = validate_teacher_assist_extraction_job_status(status)
    job.updated_at = now
    job.error_message = error_message
    if progress_percent is not None:
        job.progress_percent = progress_percent
    if status == "running" and job.started_at is None:
        job.started_at = now
    if status in {"completed", "failed", "cancelled", "skipped"}:
        job.completed_at = now
        _clear_job_lease(job)


class TeacherAssistExtractionCancelledError(RuntimeError):
    pass


def _refresh_extraction_job_for_execution(
    session: Session, extraction_job_id: uuid.UUID
) -> TeacherAssistExtractionJob:
    row = session.scalars(
        select(TeacherAssistExtractionJob).where(TeacherAssistExtractionJob.id == extraction_job_id)
    ).one_or_none()
    if row is None:
        raise LookupError("TeacherAssist extraction job not found")
    return row


def _ensure_job_still_active(
    session: Session,
    *,
    extraction_job_id: uuid.UUID,
    settings: Settings,
    worker_name: str,
    progress_percent: int | None = None,
) -> TeacherAssistExtractionJob:
    job = _refresh_extraction_job_for_execution(session, extraction_job_id)
    if job.status == "cancelled":
        raise TeacherAssistExtractionCancelledError("TeacherAssist extraction job was cancelled")
    timeout_at = _coerce_utc_datetime(job.timeout_at)
    if timeout_at is not None and timeout_at <= datetime.now(UTC):
        raise TimeoutError("TeacherAssist extraction job timed out")
    _touch_job_heartbeat(
        job,
        settings=settings,
        worker_name=worker_name,
        progress_percent=progress_percent,
    )
    session.flush()
    return job


def _mark_running_job_failed(
    job: TeacherAssistExtractionJob, *, error_message: str, error_code: str, error_metadata: dict | None = None
) -> None:
    job.error_code = error_code
    metadata = {"error_code": error_code, "traceback": traceback.format_exc(limit=5)}
    if error_metadata:
        metadata.update(error_metadata)
    job.error_metadata_json = metadata
    _append_job_log(
        job,
        event="extraction_failed",
        message=error_message,
        metadata={"error_code": error_code},
    )


def _mark_job_for_retry_or_failure(
    job: TeacherAssistExtractionJob,
    *,
    exc: Exception,
    error_code: str,
    error_metadata: dict | None = None,
) -> None:
    attempt_number = job.retry_count + 1
    job.retry_count = attempt_number
    job.error_code = error_code
    job.error_message = str(exc)
    job.updated_at = datetime.now(UTC)
    _mark_running_job_failed(
        job,
        error_message=str(exc),
        error_code=error_code,
        error_metadata=error_metadata,
    )
    if attempt_number <= job.max_retries:
        job.status = validate_teacher_assist_extraction_job_status("queued")
        job.completed_at = None
        _clear_job_lease(job)
        return
    _set_job_status(job, status="failed", progress_percent=min(max(job.progress_percent, 5), 95), error_message=str(exc))


def _artifact_summary_text(job: TeacherAssistExtractionJob, *, event_type: str) -> str:
    if job.artifact_type == "student_work":
        prefix = f"STUDENT #{job.student_number or '?'} student-work extraction"
    else:
        prefix = "TeacherAssist resource extraction"
    if event_type == "extraction_started":
        return f"Started {prefix}."
    if event_type == "extraction_completed":
        return f"Completed {prefix}."
    if event_type == "extraction_cancelled":
        return f"Cancelled {prefix}."
    return f"Failed {prefix}."


def get_extraction_job_or_404(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    extraction_job_id: uuid.UUID,
) -> TeacherAssistExtractionJob:
    row = db.scalars(
        select(TeacherAssistExtractionJob).where(
            TeacherAssistExtractionJob.id == extraction_job_id,
            TeacherAssistExtractionJob.tenant_id == tenant_id,
            TeacherAssistExtractionJob.teacher_user_id == user_id,
        )
    ).one_or_none()
    if row is None:
        raise LookupError("TeacherAssist extraction job not found")
    return row


def _get_active_artifact_job(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    resource_library_item_id: uuid.UUID | None = None,
    student_work_submission_id: uuid.UUID | None = None,
) -> TeacherAssistExtractionJob | None:
    return db.scalars(
        select(TeacherAssistExtractionJob).where(
            TeacherAssistExtractionJob.tenant_id == tenant_id,
            TeacherAssistExtractionJob.teacher_user_id == user_id,
            TeacherAssistExtractionJob.status.in_(("queued", "running")),
            TeacherAssistExtractionJob.resource_library_item_id == resource_library_item_id,
            TeacherAssistExtractionJob.student_work_submission_id == student_work_submission_id,
        )
    ).first()


def create_resource_extraction_job(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    resource_id: uuid.UUID,
    settings: Settings,
) -> TeacherAssistExtractionJob:
    resource = get_resource_or_404(db, tenant_id=tenant_id, resource_id=resource_id)
    if not resource.storage_key:
        raise ValueError("Only uploaded TeacherAssist resource files can be extracted")
    active_job = _get_active_artifact_job(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        resource_library_item_id=resource.id,
    )
    if active_job is not None:
        raise ValueError("An extraction job is already queued or running for this resource")
    now = datetime.now(UTC)
    row = TeacherAssistExtractionJob(
        tenant_id=tenant_id,
        teacher_user_id=user_id,
        artifact_type=validate_teacher_assist_extraction_artifact_type("resource"),
        resource_library_item_id=resource.id,
        student_work_submission_id=None,
        assignment_id=None,
        school_year_id=None,
        grading_period_id=None,
        class_id=None,
        subject_id=None,
        student_number=None,
        storage_key=resource.storage_key,
        original_filename=resource.original_filename or resource.title,
        mime_type=resource.mime_type or "application/octet-stream",
        file_size=int(resource.file_size or 0),
        status=validate_teacher_assist_extraction_job_status("queued"),
        progress_percent=0,
        provider_name=settings.teacher_assist_ocr_provider.strip() or "mock",
        error_code=None,
        error_message=None,
        error_metadata_json=None,
        execution_log_json=[
            {
                "event": "extraction_queued",
                "message": "TeacherAssist resource extraction queued for worker execution",
                "metadata": {"resource_id": str(resource.id)},
                "recorded_at": now.isoformat(),
            }
        ],
        leased_by_worker=None,
        lease_expires_at=None,
        heartbeat_at=None,
        retry_count=0,
        max_retries=max(0, settings.teacher_assist_worker_max_retries),
        parent_extraction_job_id=None,
        retry_root_job_id=None,
        attempt_number=1,
        timeout_at=None,
        created_at=now,
        started_at=None,
        completed_at=None,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    return row


def create_student_work_extraction_job(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    submission_id: uuid.UUID,
    settings: Settings,
) -> TeacherAssistExtractionJob:
    submission = get_student_work_submission_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        submission_id=submission_id,
    )
    active_job = _get_active_artifact_job(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        student_work_submission_id=submission.id,
    )
    if active_job is not None:
        raise ValueError("An extraction job is already queued or running for this student-work submission")
    now = datetime.now(UTC)
    row = TeacherAssistExtractionJob(
        tenant_id=tenant_id,
        teacher_user_id=user_id,
        artifact_type=validate_teacher_assist_extraction_artifact_type("student_work"),
        resource_library_item_id=None,
        student_work_submission_id=submission.id,
        assignment_id=submission.assignment_id,
        school_year_id=submission.school_year_id,
        grading_period_id=submission.grading_period_id,
        class_id=submission.class_id,
        subject_id=submission.subject_id,
        student_number=submission.student_number,
        storage_key=submission.storage_key,
        original_filename=submission.original_filename,
        mime_type=submission.mime_type,
        file_size=submission.file_size,
        status=validate_teacher_assist_extraction_job_status("queued"),
        progress_percent=0,
        provider_name=settings.teacher_assist_ocr_provider.strip() or "mock",
        error_code=None,
        error_message=None,
        error_metadata_json=None,
        execution_log_json=[
            {
                "event": "extraction_queued",
                "message": "TeacherAssist student-work extraction queued for worker execution",
                "metadata": {"submission_id": str(submission.id), "student_number": submission.student_number},
                "recorded_at": now.isoformat(),
            }
        ],
        leased_by_worker=None,
        lease_expires_at=None,
        heartbeat_at=None,
        retry_count=0,
        max_retries=max(0, settings.teacher_assist_worker_max_retries),
        parent_extraction_job_id=None,
        retry_root_job_id=None,
        attempt_number=1,
        timeout_at=None,
        created_at=now,
        started_at=None,
        completed_at=None,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    return row


def cancel_extraction_job(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    extraction_job_id: uuid.UUID,
) -> TeacherAssistExtractionJob:
    job = get_extraction_job_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        extraction_job_id=extraction_job_id,
    )
    if job.status in {"completed", "failed", "cancelled", "skipped"}:
        raise ValueError("Extraction job can no longer be cancelled")
    _set_job_status(job, status="cancelled", progress_percent=job.progress_percent)
    _append_job_log(
        job,
        event="extraction_cancel_requested",
        message="TeacherAssist extraction cancellation requested",
        metadata={"extraction_job_id": str(job.id)},
    )
    record_activity_event(
        db,
        tenant_id=job.tenant_id,
        user_id=job.teacher_user_id,
        event_type="extraction_cancelled",
        event_category="system",
        entity_type="extraction_job",
        entity_id=job.id,
        school_year_id=job.school_year_id,
        grading_period_id=job.grading_period_id,
        class_id=job.class_id,
        subject_id=job.subject_id,
        summary_text=_artifact_summary_text(job, event_type="extraction_cancelled"),
        details_json={"artifact_type": job.artifact_type, "status": job.status},
    )
    db.flush()
    return job


def list_resource_extraction_runs(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    resource_id: uuid.UUID,
) -> list[tuple[TeacherAssistExtractionJob, TeacherAssistExtractedTextRecord | None]]:
    get_resource_or_404(db, tenant_id=tenant_id, resource_id=resource_id)
    jobs = db.scalars(
        select(TeacherAssistExtractionJob)
        .where(
            TeacherAssistExtractionJob.tenant_id == tenant_id,
            TeacherAssistExtractionJob.teacher_user_id == user_id,
            TeacherAssistExtractionJob.resource_library_item_id == resource_id,
        )
        .order_by(TeacherAssistExtractionJob.created_at.desc())
    ).all()
    return [(job, job.extracted_text_records[0] if job.extracted_text_records else None) for job in jobs]


def list_student_work_extraction_runs(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    submission_id: uuid.UUID,
) -> list[tuple[TeacherAssistExtractionJob, TeacherAssistExtractedTextRecord | None]]:
    get_student_work_submission_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        submission_id=submission_id,
    )
    jobs = db.scalars(
        select(TeacherAssistExtractionJob)
        .where(
            TeacherAssistExtractionJob.tenant_id == tenant_id,
            TeacherAssistExtractionJob.teacher_user_id == user_id,
            TeacherAssistExtractionJob.student_work_submission_id == submission_id,
        )
        .order_by(TeacherAssistExtractionJob.created_at.desc())
    ).all()
    return [(job, job.extracted_text_records[0] if job.extracted_text_records else None) for job in jobs]


def latest_extraction_state_for_resources(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    resource_ids: list[uuid.UUID],
) -> dict[uuid.UUID, tuple[TeacherAssistExtractionJob | None, TeacherAssistExtractedTextRecord | None]]:
    if not resource_ids:
        return {}
    jobs = db.scalars(
        select(TeacherAssistExtractionJob)
        .where(
            TeacherAssistExtractionJob.tenant_id == tenant_id,
            TeacherAssistExtractionJob.teacher_user_id == user_id,
            TeacherAssistExtractionJob.resource_library_item_id.in_(resource_ids),
        )
        .order_by(TeacherAssistExtractionJob.created_at.desc())
    ).all()
    result: dict[uuid.UUID, tuple[TeacherAssistExtractionJob | None, TeacherAssistExtractedTextRecord | None]] = {}
    for job in jobs:
        resource_id = job.resource_library_item_id
        if resource_id is None or resource_id in result:
            continue
        result[resource_id] = (job, job.extracted_text_records[0] if job.extracted_text_records else None)
    return result


def latest_extraction_state_for_submissions(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    submission_ids: list[uuid.UUID],
) -> dict[uuid.UUID, tuple[TeacherAssistExtractionJob | None, TeacherAssistExtractedTextRecord | None]]:
    if not submission_ids:
        return {}
    jobs = db.scalars(
        select(TeacherAssistExtractionJob)
        .where(
            TeacherAssistExtractionJob.tenant_id == tenant_id,
            TeacherAssistExtractionJob.teacher_user_id == user_id,
            TeacherAssistExtractionJob.student_work_submission_id.in_(submission_ids),
        )
        .order_by(TeacherAssistExtractionJob.created_at.desc())
    ).all()
    result: dict[uuid.UUID, tuple[TeacherAssistExtractionJob | None, TeacherAssistExtractedTextRecord | None]] = {}
    for job in jobs:
        submission_id = job.student_work_submission_id
        if submission_id is None or submission_id in result:
            continue
        result[submission_id] = (job, job.extracted_text_records[0] if job.extracted_text_records else None)
    return result


def recover_stale_extraction_jobs(db: Session, *, settings: Settings | None = None) -> int:
    settings = settings or Settings()
    now = datetime.now(UTC)
    stale_cutoff = now - timedelta(seconds=max(1, settings.teacher_assist_worker_lease_seconds))
    stale_rows = db.scalars(
        select(TeacherAssistExtractionJob).where(
            TeacherAssistExtractionJob.status == "running",
            or_(
                (TeacherAssistExtractionJob.lease_expires_at.is_not(None) & (TeacherAssistExtractionJob.lease_expires_at < now)),
                (
                    TeacherAssistExtractionJob.heartbeat_at.is_not(None)
                    & (TeacherAssistExtractionJob.heartbeat_at < stale_cutoff)
                ),
            ),
        )
    ).all()
    for job in stale_rows:
        _append_job_log(
            job,
            event="extraction_stale_recovered",
            message="TeacherAssist extraction job recovered after stale lease or heartbeat",
            metadata={"recovery_state": "stale"},
        )
        _mark_job_for_retry_or_failure(
            job,
            exc=TimeoutError("TeacherAssist extraction lease or heartbeat expired"),
            error_code="stale_job",
        )
    db.flush()
    return len(stale_rows)


def reclaim_stale_extraction_jobs(db: Session) -> int:
    return recover_stale_extraction_jobs(db)


def claim_next_teacher_assist_extraction_job(
    db: Session,
    *,
    settings: Settings,
    worker_name: str,
    extraction_job_id: uuid.UUID | None = None,
) -> TeacherAssistExtractionJob | None:
    reclaim_stale_extraction_jobs(db)
    query = (
        select(TeacherAssistExtractionJob)
        .where(TeacherAssistExtractionJob.status == "queued")
        .order_by(TeacherAssistExtractionJob.created_at.asc())
    )
    if extraction_job_id is not None:
        query = query.where(TeacherAssistExtractionJob.id == extraction_job_id)
    job = db.scalars(query).first()
    if job is None:
        return None
    _set_job_status(job, status="running", progress_percent=max(job.progress_percent, 1))
    job.max_retries = max(0, settings.teacher_assist_worker_max_retries)
    job.provider_name = settings.teacher_assist_ocr_provider.strip() or "mock"
    job.error_code = None
    job.error_message = None
    job.error_metadata_json = None
    _touch_job_heartbeat(
        job,
        settings=settings,
        worker_name=worker_name,
        progress_percent=max(job.progress_percent, 5),
    )
    _append_job_log(
        job,
        event="extraction_claimed",
        message="TeacherAssist extraction job claimed by worker",
        metadata={"worker_name": worker_name, "retry_count": job.retry_count, "max_retries": job.max_retries},
    )
    record_activity_event(
        db,
        tenant_id=job.tenant_id,
        user_id=job.teacher_user_id,
        event_type="extraction_started",
        event_category="system",
        entity_type="extraction_job",
        entity_id=job.id,
        school_year_id=job.school_year_id,
        grading_period_id=job.grading_period_id,
        class_id=job.class_id,
        subject_id=job.subject_id,
        summary_text=_artifact_summary_text(job, event_type="extraction_started"),
        details_json={"artifact_type": job.artifact_type, "retry_count": job.retry_count},
    )
    db.flush()
    return job


def _persist_extraction_success(
    session: Session,
    *,
    extraction_job_id: uuid.UUID,
    settings: Settings,
    worker_name: str,
) -> None:
    job = _ensure_job_still_active(
        session,
        extraction_job_id=extraction_job_id,
        settings=settings,
        worker_name=worker_name,
        progress_percent=10,
    )
    _append_job_log(job, event="storage_read_started", message="Reading artifact via TeacherAssist storage abstraction")
    session.commit()

    job = _ensure_job_still_active(
        session,
        extraction_job_id=extraction_job_id,
        settings=settings,
        worker_name=worker_name,
        progress_percent=35,
    )
    with open_teacher_assist_stream(settings, storage_key=job.storage_key) as stream:
        file_bytes = stream.read()
    _append_job_log(job, event="storage_read_completed", message="Artifact read through storage abstraction")
    session.commit()

    job = _ensure_job_still_active(
        session,
        extraction_job_id=extraction_job_id,
        settings=settings,
        worker_name=worker_name,
        progress_percent=60,
    )
    provider_name = settings.teacher_assist_ocr_provider.strip() or "mock"
    assert_ocr_artifact_supported(
        settings,
        mime_type=job.mime_type,
        file_size=len(file_bytes),
        provider_name=provider_name,
    )
    provider = get_teacher_assist_ocr_provider(settings)
    provider_started = time.perf_counter()
    try:
        provider_result = provider.extract_text(
            artifact_type=job.artifact_type,
            mime_type=job.mime_type,
            original_filename=job.original_filename,
            file_bytes=file_bytes,
            settings=settings,
        )
    except TeacherAssistOCRProviderError:
        raise
    except TimeoutError as exc:
        raise TeacherAssistOCRProviderError(
            "TeacherAssist OCR provider timed out",
            error_code="provider_timeout",
            metadata={"provider": provider_name},
        ) from exc
    processing_duration_ms = max(0, int((time.perf_counter() - provider_started) * 1000))
    sanitized = sanitize_extracted_text(provider_result.extracted_text)
    provider_metadata = dict(provider_result.metadata_json or {})
    confidence_score = provider_metadata.get("provider_confidence_score")
    confidence_level_raw = provider_metadata.get("confidence_level")
    confidence_level = (
        validate_extraction_confidence_level(str(confidence_level_raw))
        if confidence_level_raw is not None
        else "unknown"
    )
    if confidence_score is not None:
        try:
            confidence_score = float(confidence_score)
        except (TypeError, ValueError):
            confidence_score = None
    provider_mode = str(provider_metadata.get("provider_mode") or resolve_teacher_assist_ocr_provider_mode(provider_result.provider))
    page_count_raw = provider_metadata.get("page_count")
    page_count = int(page_count_raw) if page_count_raw is not None else None
    estimated_cost_raw = provider_metadata.get("estimated_cost_cents")
    estimated_cost_cents = int(estimated_cost_raw) if estimated_cost_raw is not None else None
    provider_version = provider_metadata.get("provider_version")
    now = datetime.now(UTC)
    record = TeacherAssistExtractedTextRecord(
        tenant_id=job.tenant_id,
        teacher_user_id=job.teacher_user_id,
        extraction_job_id=job.id,
        artifact_type=job.artifact_type,
        resource_library_item_id=job.resource_library_item_id,
        student_work_submission_id=job.student_work_submission_id,
        assignment_id=job.assignment_id,
        school_year_id=job.school_year_id,
        grading_period_id=job.grading_period_id,
        class_id=job.class_id,
        subject_id=job.subject_id,
        student_number=job.student_number,
        extracted_text=sanitized.extracted_text,
        preview_text=sanitized.preview_text,
        text_char_count=sanitized.text_char_count,
        pii_flagged=sanitized.pii_flagged,
        redaction_applied=sanitized.redaction_applied,
        review_status=validate_extraction_review_status("pending_review"),
        provider_confidence_score=confidence_score,
        confidence_level=confidence_level,
        teacher_corrected_text=None,
        approved_text=None,
        reviewed_at=None,
        reviewed_by_user_id=None,
        source_extraction_job_id=job.id,
        metadata_json={
            **provider_metadata,
            "provider": provider_result.provider,
            "model": provider_result.model,
            "provider_mode": provider_mode,
            "processing_duration_ms": processing_duration_ms,
        },
        created_at=now,
        updated_at=now,
    )
    session.add(record)
    session.flush()
    job.provider_name = provider_result.provider
    job.provider_model = provider_result.model
    job.provider_version = str(provider_version) if provider_version is not None else None
    job.provider_mode = provider_mode
    job.page_count = page_count
    job.processing_duration_ms = processing_duration_ms
    job.estimated_cost_cents = estimated_cost_cents
    _set_job_status(job, status="completed", progress_percent=100)
    _append_job_log(
        job,
        event="extraction_completed",
        message="TeacherAssist extraction completed successfully",
        metadata={
            "record_id": str(record.id),
            "provider": provider_result.provider,
            "model": provider_result.model,
            "provider_mode": provider_mode,
            "confidence_level": confidence_level,
            "page_count": page_count,
            "processing_duration_ms": processing_duration_ms,
            "estimated_cost_cents": estimated_cost_cents,
            "low_confidence_output": bool(provider_metadata.get("low_confidence_output")),
        },
    )
    record_activity_event(
        session,
        tenant_id=job.tenant_id,
        user_id=job.teacher_user_id,
        event_type="extraction_completed",
        event_category="system",
        entity_type="extraction_job",
        entity_id=job.id,
        school_year_id=job.school_year_id,
        grading_period_id=job.grading_period_id,
        class_id=job.class_id,
        subject_id=job.subject_id,
        summary_text=_artifact_summary_text(job, event_type="extraction_completed"),
        details_json={
            "artifact_type": job.artifact_type,
            "record_id": str(record.id),
            "pii_flagged": sanitized.pii_flagged,
            "redaction_applied": sanitized.redaction_applied,
        },
    )
    session.commit()


def _persist_extraction_failure(
    factory,
    *,
    extraction_job_id: uuid.UUID,
    exc: Exception,
    error_code: str,
    error_metadata: dict | None = None,
) -> None:
    failure_session = factory()
    try:
        job = _refresh_extraction_job_for_execution(failure_session, extraction_job_id)
        _mark_job_for_retry_or_failure(
            job,
            exc=exc,
            error_code=error_code,
            error_metadata=error_metadata,
        )
        if job.status == "failed":
            record_activity_event(
                failure_session,
                tenant_id=job.tenant_id,
                user_id=job.teacher_user_id,
                event_type="extraction_failed",
                event_category="system",
                entity_type="extraction_job",
                entity_id=job.id,
                school_year_id=job.school_year_id,
                grading_period_id=job.grading_period_id,
                class_id=job.class_id,
                subject_id=job.subject_id,
                summary_text=_artifact_summary_text(job, event_type="extraction_failed"),
                details_json={"artifact_type": job.artifact_type, "error_code": error_code},
            )
        failure_session.commit()
    finally:
        failure_session.close()


def _persist_extraction_cancelled(factory, *, extraction_job_id: uuid.UUID) -> None:
    cancel_session = factory()
    try:
        job = _refresh_extraction_job_for_execution(cancel_session, extraction_job_id)
        _set_job_status(job, status="cancelled", progress_percent=job.progress_percent)
        _append_job_log(job, event="extraction_cancelled", message="TeacherAssist extraction cancelled")
        record_activity_event(
            cancel_session,
            tenant_id=job.tenant_id,
            user_id=job.teacher_user_id,
            event_type="extraction_cancelled",
            event_category="system",
            entity_type="extraction_job",
            entity_id=job.id,
            school_year_id=job.school_year_id,
            grading_period_id=job.grading_period_id,
            class_id=job.class_id,
            subject_id=job.subject_id,
            summary_text=_artifact_summary_text(job, event_type="extraction_cancelled"),
            details_json={"artifact_type": job.artifact_type},
        )
        cancel_session.commit()
    finally:
        cancel_session.close()


def _process_extraction_with_factory(
    factory,
    extraction_job_id: uuid.UUID,
    settings: Settings,
    *,
    worker_name: str,
) -> None:
    session = factory()
    try:
        _persist_extraction_success(
            session,
            extraction_job_id=extraction_job_id,
            settings=settings,
            worker_name=worker_name,
        )
    except TeacherAssistExtractionCancelledError:
        session.rollback()
        _persist_extraction_cancelled(factory, extraction_job_id=extraction_job_id)
    except TeacherAssistOCRProviderError as exc:
        session.rollback()
        _persist_extraction_failure(
            factory,
            extraction_job_id=extraction_job_id,
            exc=exc,
            error_code=exc.error_code,
            error_metadata=exc.metadata,
        )
    except TimeoutError as exc:
        session.rollback()
        _persist_extraction_failure(
            factory,
            extraction_job_id=extraction_job_id,
            exc=exc,
            error_code="timeout",
        )
    except Exception as exc:
        session.rollback()
        _persist_extraction_failure(
            factory,
            extraction_job_id=extraction_job_id,
            exc=exc,
            error_code="execution_failed",
        )
    finally:
        session.close()


def process_next_teacher_assist_extraction_job_with_engine(
    engine: Engine,
    *,
    settings: Settings | None = None,
    worker_name: str = "teacher-assist-worker",
    extraction_job_id: uuid.UUID | None = None,
) -> uuid.UUID | None:
    settings = settings or Settings()
    factory = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    claim_session = factory()
    try:
        job = claim_next_teacher_assist_extraction_job(
            claim_session,
            settings=settings,
            worker_name=worker_name,
            extraction_job_id=extraction_job_id,
        )
        if job is None:
            claim_session.commit()
            return None
        claimed_id = job.id
        claim_session.commit()
    finally:
        claim_session.close()
    _process_extraction_with_factory(factory, claimed_id, settings, worker_name=worker_name)
    return claimed_id
