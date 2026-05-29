from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.models.teacher_assist_assignment import TeacherAssistAssignment
from oziebot_api.models.teacher_assist_assignment_grading_review import TeacherAssistAssignmentGradingReview
from oziebot_api.models.teacher_assist_assignment_print_packet import TeacherAssistAssignmentPrintPacket
from oziebot_api.models.teacher_assist_class import TeacherAssistClass
from oziebot_api.models.teacher_assist_extracted_text_record import TeacherAssistExtractedTextRecord
from oziebot_api.models.teacher_assist_extraction_job import TeacherAssistExtractionJob
from oziebot_api.models.teacher_assist_grading_period import TeacherAssistGradingPeriod
from oziebot_api.models.teacher_assist_school_year import TeacherAssistSchoolYear
from oziebot_api.models.teacher_assist_student_work_submission import TeacherAssistStudentWorkSubmission
from oziebot_api.models.teacher_assist_weekly_plan import TeacherAssistWeeklyPlan
from oziebot_api.models.teacher_assist_workflow import TeacherAssistWorkflow
from oziebot_api.services.teacher_assist.activity_events import list_recent_activity_events
from oziebot_api.services.teacher_assist.extraction_review_service import is_stale_extraction_job
from oziebot_api.services.teacher_assist.mastery_dashboard import build_mastery_dashboard
from oziebot_api.services.teacher_assist.mastery_workspace_insights import build_workspace_mastery_insights


def _uuid_from_value(value: Any) -> uuid.UUID | None:
    if not value:
        return None
    try:
        return uuid.UUID(str(value))
    except (TypeError, ValueError):
        return None


def _plan_class_id(plan: TeacherAssistWeeklyPlan) -> uuid.UUID | None:
    source_context = dict(plan.source_context_json or {})
    class_context = dict(source_context.get("class") or {})
    draft = dict(source_context.get("draft") or {})
    return _uuid_from_value(class_context.get("id") or draft.get("class_id"))


def _plan_school_year_id(plan: TeacherAssistWeeklyPlan) -> uuid.UUID | None:
    source_context = dict(plan.source_context_json or {})
    school_year = dict(source_context.get("school_year") or {})
    draft = dict(source_context.get("draft") or {})
    return _uuid_from_value(
        school_year.get("id") or draft.get("school_year_id") or plan.school_year_origin_id
    )


def _workflow_class_id(workflow: TeacherAssistWorkflow) -> uuid.UUID | None:
    snapshot = dict(workflow.input_snapshot_json or {})
    draft = dict(snapshot.get("draft") or {})
    class_context = dict(snapshot.get("class") or {})
    return _uuid_from_value(class_context.get("id") or draft.get("class_id"))


def _workflow_school_year_id(workflow: TeacherAssistWorkflow) -> uuid.UUID | None:
    snapshot = dict(workflow.input_snapshot_json or {})
    draft = dict(snapshot.get("draft") or {})
    school_year = dict(snapshot.get("school_year") or {})
    return _uuid_from_value(school_year.get("id") or draft.get("school_year_id"))


def _workflow_grading_period_id(workflow: TeacherAssistWorkflow) -> uuid.UUID | None:
    snapshot = dict(workflow.input_snapshot_json or {})
    draft = dict(snapshot.get("draft") or {})
    grading_period = dict(snapshot.get("grading_period") or {})
    return _uuid_from_value(grading_period.get("id") or draft.get("grading_period_id"))


def _as_utc_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _current_school_year(rows: list[TeacherAssistSchoolYear]) -> TeacherAssistSchoolYear | None:
    if not rows:
        return None
    active = next((row for row in rows if row.is_active), None)
    if active is not None:
        return active
    return sorted(rows, key=lambda row: (row.start_date, row.created_at), reverse=True)[0]


def _active_grading_period(
    rows: list[TeacherAssistGradingPeriod],
    *,
    current_school_year: TeacherAssistSchoolYear | None,
    today: date,
) -> TeacherAssistGradingPeriod | None:
    scoped = [
        row
        for row in rows
        if current_school_year is None or row.school_year_id == current_school_year.id
    ]
    if not scoped:
        return None
    current = next((row for row in scoped if row.start_date <= today <= row.end_date), None)
    if current is not None:
        return current
    upcoming = [row for row in scoped if row.end_date >= today]
    if upcoming:
        return sorted(upcoming, key=lambda row: (row.start_date, row.sort_order))[0]
    return sorted(scoped, key=lambda row: (row.start_date, row.sort_order), reverse=True)[0]


def _review_required_for_plan(plan: TeacherAssistWeeklyPlan) -> bool:
    content = dict(plan.content_json or {})
    return plan.status == "in_progress" or bool(content.get("review_required"))


def _grading_review_pending_confirmation(review: TeacherAssistAssignmentGradingReview) -> bool:
    return review.status not in {"teacher_confirmed", "archived"}


def _assignment_summary(row: TeacherAssistAssignment) -> dict[str, Any]:
    return {
        "id": row.id,
        "class_id": row.class_id,
        "subject_id": row.subject_id,
        "title": row.title,
        "status": row.status,
        "assignment_type": row.assignment_type,
        "due_date": row.due_date,
        "updated_at": row.updated_at,
    }


def _packet_summary(row: TeacherAssistAssignmentPrintPacket) -> dict[str, Any]:
    return {
        "id": row.id,
        "assignment_id": row.assignment_id,
        "class_id": row.class_id,
        "packet_status": row.packet_status,
        "pages_per_student": row.pages_per_student,
        "student_count": row.student_count,
        "template_type": row.template_type,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def _submission_summary(row: TeacherAssistStudentWorkSubmission) -> dict[str, Any]:
    return {
        "id": row.id,
        "assignment_id": row.assignment_id,
        "class_id": row.class_id,
        "student_number": row.student_number,
        "original_filename": row.original_filename,
        "upload_status": row.upload_status,
        "processing_status": row.processing_status,
        "latest_extraction_status": None,
        "extraction_ready_for_teacher_review": False,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def _grading_review_summary(row: TeacherAssistAssignmentGradingReview) -> dict[str, Any]:
    return {
        "id": row.id,
        "assignment_id": row.assignment_id,
        "student_work_submission_id": row.student_work_submission_id,
        "class_id": row.class_id,
        "student_number": row.student_number,
        "status": row.status,
        "teacher_confirmed_score": row.teacher_confirmed_score,
        "updated_at": row.updated_at,
    }


def _submission_ready_for_extraction(
    submission: TeacherAssistStudentWorkSubmission,
    latest_job: TeacherAssistExtractionJob | None,
    latest_record: TeacherAssistExtractedTextRecord | None,
) -> bool:
    if submission.upload_status == "archived":
        return False
    if latest_record is not None:
        return False
    if latest_job is None:
        return True
    return latest_job.status not in {"queued", "running", "completed"}


def _submission_ready_for_teacher_review(
    submission: TeacherAssistStudentWorkSubmission,
    latest_record: TeacherAssistExtractedTextRecord | None,
    grading_reviews: list[TeacherAssistAssignmentGradingReview],
) -> bool:
    if latest_record is None:
        return False
    if latest_record.review_status in {"pending_review", "teacher_reviewing"}:
        return True
    related_reviews = [row for row in grading_reviews if row.student_work_submission_id == submission.id]
    return not any(row.status != "archived" for row in related_reviews)


def _workflow_summary(row: TeacherAssistWorkflow) -> dict[str, Any]:
    return {
        "id": row.id,
        "workflow_type": row.workflow_type,
        "status": row.status,
        "class_id": _workflow_class_id(row),
        "school_year_id": _workflow_school_year_id(row),
        "grading_period_id": _workflow_grading_period_id(row),
        "progress_percent": row.progress_percent,
        "retry_count": row.retry_count,
        "max_retries": row.max_retries,
        "provider_name": row.provider_name,
        "provider_model": row.provider_model,
        "last_error_code": row.last_error_code,
        "heartbeat_at": row.heartbeat_at,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
        "completed_at": row.completed_at,
        "error_message": row.error_message,
    }


def _plan_summary(row: TeacherAssistWeeklyPlan) -> dict[str, Any]:
    content = dict(row.content_json or {})
    return {
        "id": row.id,
        "title": row.title,
        "planning_scope": row.planning_scope,
        "status": row.status,
        "workflow_id": row.workflow_id,
        "class_id": _plan_class_id(row),
        "school_year_id": _plan_school_year_id(row),
        "review_required": bool(content.get("review_required")),
        "quality_flags": list(content.get("quality_flags") or []),
        "missing_context_warnings": list(content.get("missing_context_warnings") or []),
        "updated_at": row.updated_at,
    }


def _attention_item(
    *,
    item_type: str,
    severity: str,
    title: str,
    message: str,
    entity_type: str,
    entity_id: uuid.UUID,
    created_at: datetime,
    class_id: uuid.UUID | None = None,
) -> dict[str, Any]:
    return {
        "type": item_type,
        "severity": severity,
        "title": title,
        "message": message,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "class_id": class_id,
        "created_at": created_at,
    }


def _review_required_item(
    *,
    entity_type: str,
    entity_id: uuid.UUID,
    class_id: uuid.UUID | None,
    title: str,
    status: str,
    review_reason: str,
    updated_at: datetime,
) -> dict[str, Any]:
    return {
        "entity_type": entity_type,
        "entity_id": entity_id,
        "class_id": class_id,
        "title": title,
        "status": status,
        "review_reason": review_reason,
        "updated_at": updated_at,
    }


def get_teacher_assist_workspace(
    db: Session,
    *,
    settings: Settings,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
) -> dict[str, Any]:
    today = datetime.now(UTC).date()
    now = datetime.now(UTC)
    recent_cutoff = now - timedelta(days=7)
    stale_heartbeat_cutoff = now - timedelta(seconds=max(1, settings.teacher_assist_worker_lease_seconds))

    school_years = db.scalars(
        select(TeacherAssistSchoolYear)
        .where(TeacherAssistSchoolYear.tenant_id == tenant_id)
        .order_by(TeacherAssistSchoolYear.start_date.desc(), TeacherAssistSchoolYear.created_at.desc())
    ).all()
    current_school_year = _current_school_year(school_years)

    grading_periods = db.scalars(
        select(TeacherAssistGradingPeriod)
        .join(TeacherAssistSchoolYear, TeacherAssistSchoolYear.id == TeacherAssistGradingPeriod.school_year_id)
        .where(TeacherAssistSchoolYear.tenant_id == tenant_id)
        .order_by(
            TeacherAssistGradingPeriod.start_date.asc(),
            TeacherAssistGradingPeriod.sort_order.asc(),
        )
    ).all()
    active_grading_period = _active_grading_period(
        grading_periods,
        current_school_year=current_school_year,
        today=today,
    )

    classes = db.scalars(
        select(TeacherAssistClass)
        .where(TeacherAssistClass.tenant_id == tenant_id)
        .order_by(TeacherAssistClass.name.asc(), TeacherAssistClass.created_at.asc())
    ).all()
    if current_school_year is not None:
        classes = [row for row in classes if row.school_year_id == current_school_year.id]

    assignments = db.scalars(
        select(TeacherAssistAssignment)
        .where(
            TeacherAssistAssignment.tenant_id == tenant_id,
            TeacherAssistAssignment.teacher_user_id == user_id,
        )
        .order_by(TeacherAssistAssignment.updated_at.desc(), TeacherAssistAssignment.created_at.desc())
    ).all()

    submissions = db.scalars(
        select(TeacherAssistStudentWorkSubmission)
        .where(
            TeacherAssistStudentWorkSubmission.tenant_id == tenant_id,
            TeacherAssistStudentWorkSubmission.teacher_user_id == user_id,
        )
        .order_by(
            TeacherAssistStudentWorkSubmission.updated_at.desc(),
            TeacherAssistStudentWorkSubmission.created_at.desc(),
        )
    ).all()

    grading_reviews = db.scalars(
        select(TeacherAssistAssignmentGradingReview)
        .where(
            TeacherAssistAssignmentGradingReview.tenant_id == tenant_id,
            TeacherAssistAssignmentGradingReview.teacher_user_id == user_id,
        )
        .order_by(
            TeacherAssistAssignmentGradingReview.updated_at.desc(),
            TeacherAssistAssignmentGradingReview.created_at.desc(),
        )
    ).all()

    extraction_jobs = db.scalars(
        select(TeacherAssistExtractionJob)
        .where(
            TeacherAssistExtractionJob.tenant_id == tenant_id,
            TeacherAssistExtractionJob.teacher_user_id == user_id,
        )
        .order_by(TeacherAssistExtractionJob.updated_at.desc(), TeacherAssistExtractionJob.created_at.desc())
    ).all()

    extracted_text_records = db.scalars(
        select(TeacherAssistExtractedTextRecord)
        .where(
            TeacherAssistExtractedTextRecord.tenant_id == tenant_id,
            TeacherAssistExtractedTextRecord.teacher_user_id == user_id,
        )
        .order_by(
            TeacherAssistExtractedTextRecord.updated_at.desc(),
            TeacherAssistExtractedTextRecord.created_at.desc(),
        )
    ).all()

    packets = db.scalars(
        select(TeacherAssistAssignmentPrintPacket)
        .where(
            TeacherAssistAssignmentPrintPacket.tenant_id == tenant_id,
            TeacherAssistAssignmentPrintPacket.teacher_user_id == user_id,
        )
        .order_by(
            TeacherAssistAssignmentPrintPacket.updated_at.desc(),
            TeacherAssistAssignmentPrintPacket.created_at.desc(),
        )
    ).all()

    workflows = db.scalars(
        select(TeacherAssistWorkflow)
        .where(
            TeacherAssistWorkflow.tenant_id == tenant_id,
            TeacherAssistWorkflow.user_id == user_id,
        )
        .order_by(TeacherAssistWorkflow.updated_at.desc(), TeacherAssistWorkflow.created_at.desc())
    ).all()

    plans = db.scalars(
        select(TeacherAssistWeeklyPlan)
        .where(
            TeacherAssistWeeklyPlan.tenant_id == tenant_id,
            TeacherAssistWeeklyPlan.user_id == user_id,
        )
        .order_by(TeacherAssistWeeklyPlan.updated_at.desc(), TeacherAssistWeeklyPlan.created_at.desc())
    ).all()

    recent_activity = list_recent_activity_events(db, tenant_id=tenant_id, user_id=user_id, limit=50)
    active_workflows = [row for row in workflows if row.status in {"queued", "running"}]
    latest_extraction_job_by_submission_id: dict[uuid.UUID, TeacherAssistExtractionJob] = {}
    latest_extraction_record_by_submission_id: dict[uuid.UUID, TeacherAssistExtractedTextRecord] = {}
    for row in extraction_jobs:
        if row.student_work_submission_id is not None and row.student_work_submission_id not in latest_extraction_job_by_submission_id:
            latest_extraction_job_by_submission_id[row.student_work_submission_id] = row
    for row in extracted_text_records:
        if (
            row.student_work_submission_id is not None
            and row.student_work_submission_id not in latest_extraction_record_by_submission_id
        ):
            latest_extraction_record_by_submission_id[row.student_work_submission_id] = row

    needs_attention: list[dict[str, Any]] = []
    review_required_items: list[dict[str, Any]] = []

    for workflow in workflows:
        class_id = _workflow_class_id(workflow)
        if workflow.status == "failed":
            needs_attention.append(
                _attention_item(
                    item_type="workflow_failed",
                    severity="critical",
                    title="Workflow failed",
                    message=workflow.error_message or "TeacherAssist workflow failed and needs review.",
                    entity_type="workflow",
                    entity_id=workflow.id,
                    created_at=workflow.updated_at,
                    class_id=class_id,
                )
            )
        if workflow.status == "queued" and workflow.retry_count > 0:
            needs_attention.append(
                _attention_item(
                    item_type="workflow_retrying",
                    severity="warning",
                    title="Workflow retrying",
                    message=f"Workflow has retried {workflow.retry_count} time(s) and is queued again.",
                    entity_type="workflow",
                    entity_id=workflow.id,
                    created_at=workflow.updated_at,
                    class_id=class_id,
                )
            )
        if workflow.status == "cancelled":
            needs_attention.append(
                _attention_item(
                    item_type="workflow_cancelled",
                    severity="info",
                    title="Workflow cancelled",
                    message="A TeacherAssist workflow was cancelled before completion.",
                    entity_type="workflow",
                    entity_id=workflow.id,
                    created_at=workflow.updated_at,
                    class_id=class_id,
                )
            )
        workflow_heartbeat_at = _as_utc_datetime(workflow.heartbeat_at)
        if workflow.status == "running" and workflow_heartbeat_at is not None and workflow_heartbeat_at < stale_heartbeat_cutoff:
            needs_attention.append(
                _attention_item(
                    item_type="workflow_stale_heartbeat",
                    severity="critical",
                    title="Workflow heartbeat is stale",
                    message="A running workflow has not updated its heartbeat within the expected lease window.",
                    entity_type="workflow",
                    entity_id=workflow.id,
                    created_at=workflow.updated_at,
                    class_id=class_id,
                )
            )

    for extraction_job in extraction_jobs:
        extraction_record = extraction_job.extracted_text_records[0] if extraction_job.extracted_text_records else None
        if extraction_job.status == "failed" and extraction_job.artifact_type == "resource":
            needs_attention.append(
                _attention_item(
                    item_type="extraction_failed",
                    severity="critical",
                    title="Resource extraction failed",
                    message="A resource extraction job failed and needs retry or manual follow-up.",
                    entity_type="extraction_job",
                    entity_id=extraction_job.id,
                    created_at=extraction_job.updated_at,
                    class_id=None,
                )
            )
        if is_stale_extraction_job(extraction_job, settings=settings, now=now):
            needs_attention.append(
                _attention_item(
                    item_type="stale_extraction_job",
                    severity="critical",
                    title="Stale extraction job",
                    message="A running extraction job has an expired lease or stale heartbeat.",
                    entity_type="extraction_job",
                    entity_id=extraction_job.id,
                    created_at=extraction_job.updated_at,
                    class_id=extraction_job.class_id,
                )
            )
        if extraction_job.status == "queued" and extraction_job.retry_count > 0:
            needs_attention.append(
                _attention_item(
                    item_type="extraction_retrying",
                    severity="warning",
                    title="Extraction retrying",
                    message=f"Extraction attempt {extraction_job.attempt_number} is queued after a prior failure.",
                    entity_type="extraction_job",
                    entity_id=extraction_job.id,
                    created_at=extraction_job.updated_at,
                    class_id=extraction_job.class_id,
                )
            )
        if extraction_record is not None and extraction_record.confidence_level == "low":
            needs_attention.append(
                _attention_item(
                    item_type="low_confidence_extraction",
                    severity="warning",
                    title="Low confidence extraction",
                    message="Extracted text has low provider confidence and should be reviewed before use.",
                    entity_type="extracted_text",
                    entity_id=extraction_record.id,
                    created_at=extraction_record.updated_at,
                    class_id=extraction_record.class_id,
                )
            )
        if extraction_record is not None and extraction_record.review_status == "teacher_rejected":
            needs_attention.append(
                _attention_item(
                    item_type="teacher_rejected_extraction",
                    severity="critical",
                    title="Teacher rejected extraction",
                    message="A teacher rejected extracted text and remediation may be required.",
                    entity_type="extracted_text",
                    entity_id=extraction_record.id,
                    created_at=extraction_record.updated_at,
                    class_id=extraction_record.class_id,
                )
            )
        if extraction_job.status == "failed" and extraction_job.attempt_number >= 3:
            needs_attention.append(
                _attention_item(
                    item_type="multiple_failed_retries",
                    severity="critical",
                    title="Multiple extraction retries failed",
                    message="An extraction lineage has multiple failed attempts and needs operator review.",
                    entity_type="extraction_job",
                    entity_id=extraction_job.id,
                    created_at=extraction_job.updated_at,
                    class_id=extraction_job.class_id,
                )
            )
        if extraction_record is not None and extraction_record.review_status in {"pending_review", "teacher_reviewing"}:
            review_required_items.append(
                _review_required_item(
                    entity_type="extracted_text",
                    entity_id=extraction_record.id,
                    class_id=extraction_record.class_id,
                    title=(
                        f"Extraction review for STUDENT #{extraction_record.student_number}"
                        if extraction_record.student_number is not None
                        else "Resource extraction review"
                    ),
                    status=extraction_record.review_status,
                    review_reason="Teacher extraction approval is required before downstream use.",
                    updated_at=extraction_record.updated_at,
                )
            )

    for plan in plans:
        class_id = _plan_class_id(plan)
        content = dict(plan.content_json or {})
        quality_flags = list(content.get("quality_flags") or [])
        missing_context_warnings = list(content.get("missing_context_warnings") or [])
        if plan.status == "in_progress":
            needs_attention.append(
                _attention_item(
                    item_type="plan_in_progress",
                    severity="warning",
                    title="Instructional plan still in progress",
                    message=f"'{plan.title}' still requires teacher review before it is considered complete.",
                    entity_type="weekly_plan",
                    entity_id=plan.id,
                    created_at=plan.updated_at,
                    class_id=class_id,
                )
            )
        if "standards-context-missing" in quality_flags:
            needs_attention.append(
                _attention_item(
                    item_type="missing_standards_alignment",
                    severity="warning",
                    title="Standards alignment needs attention",
                    message=f"'{plan.title}' is missing standards context and should be reviewed manually.",
                    entity_type="weekly_plan",
                    entity_id=plan.id,
                    created_at=plan.updated_at,
                    class_id=class_id,
                )
            )
        if quality_flags:
            needs_attention.append(
                _attention_item(
                    item_type="plan_quality_flags",
                    severity="warning",
                    title="Plan quality flags present",
                    message=f"'{plan.title}' has quality flags: {', '.join(quality_flags)}.",
                    entity_type="weekly_plan",
                    entity_id=plan.id,
                    created_at=plan.updated_at,
                    class_id=class_id,
                )
            )
        if missing_context_warnings:
            needs_attention.append(
                _attention_item(
                    item_type="missing_context_warnings",
                    severity="info",
                    title="Plan context warnings present",
                    message=f"'{plan.title}' has context warnings that should be reviewed.",
                    entity_type="weekly_plan",
                    entity_id=plan.id,
                    created_at=plan.updated_at,
                    class_id=class_id,
                )
            )
        if _review_required_for_plan(plan):
            review_required_items.append(
                _review_required_item(
                    entity_type="weekly_plan",
                    entity_id=plan.id,
                    class_id=class_id,
                    title=plan.title,
                    status=plan.status,
                    review_reason="Teacher review required before classroom use.",
                    updated_at=plan.updated_at,
                )
            )

    for submission in submissions:
        latest_extraction_job = latest_extraction_job_by_submission_id.get(submission.id)
        latest_extraction_record = latest_extraction_record_by_submission_id.get(submission.id)
        if submission.processing_status == "pending_review":
            needs_attention.append(
                _attention_item(
                    item_type="submission_pending_review",
                    severity="warning",
                    title="Student work pending review",
                    message=f"STUDENT #{submission.student_number} upload is still pending review.",
                    entity_type="student_work_submission",
                    entity_id=submission.id,
                    created_at=submission.updated_at,
                    class_id=submission.class_id,
                )
            )
        if latest_extraction_job is not None and latest_extraction_job.status == "failed":
            needs_attention.append(
                _attention_item(
                    item_type="extraction_failed",
                    severity="critical",
                    title="Extraction failed",
                    message=f"Extraction failed for STUDENT #{submission.student_number} and needs a retry or manual review.",
                    entity_type="extraction_job",
                    entity_id=latest_extraction_job.id,
                    created_at=latest_extraction_job.updated_at,
                    class_id=submission.class_id,
                )
            )
        if _submission_ready_for_extraction(submission, latest_extraction_job, latest_extraction_record):
            needs_attention.append(
                _attention_item(
                    item_type="student_work_ready_for_extraction",
                    severity="warning",
                    title="Student work ready for extraction",
                    message=f"STUDENT #{submission.student_number} has uploaded work that has not been extracted yet.",
                    entity_type="student_work_submission",
                    entity_id=submission.id,
                    created_at=submission.updated_at,
                    class_id=submission.class_id,
                )
            )
        if _submission_ready_for_teacher_review(submission, latest_extraction_record, grading_reviews):
            needs_attention.append(
                _attention_item(
                    item_type="extracted_work_ready_for_teacher_review",
                    severity="warning",
                    title="Extracted work ready for teacher review",
                    message=f"Extraction is complete for STUDENT #{submission.student_number}; teacher review can begin.",
                    entity_type="student_work_submission",
                    entity_id=submission.id,
                    created_at=(latest_extraction_record.updated_at if latest_extraction_record else submission.updated_at),
                    class_id=submission.class_id,
                )
            )
            review_required_items.append(
                _review_required_item(
                    entity_type="student_work_submission",
                    entity_id=submission.id,
                    class_id=submission.class_id,
                    title=f"Extracted work for STUDENT #{submission.student_number}",
                    status=latest_extraction_job.status if latest_extraction_job is not None else "completed",
                    review_reason="Extracted text is ready for teacher review.",
                    updated_at=latest_extraction_record.updated_at if latest_extraction_record else submission.updated_at,
                )
            )

    for review in grading_reviews:
        if _grading_review_pending_confirmation(review):
            needs_attention.append(
                _attention_item(
                    item_type="grading_review_pending_confirmation",
                    severity="warning",
                    title="Grading review awaiting confirmation",
                    message=f"STUDENT #{review.student_number} grading review still needs teacher confirmation.",
                    entity_type="grading_review",
                    entity_id=review.id,
                    created_at=review.updated_at,
                    class_id=review.class_id,
                )
            )
            review_required_items.append(
                _review_required_item(
                    entity_type="grading_review",
                    entity_id=review.id,
                    class_id=review.class_id,
                    title=f"Grading review for STUDENT #{review.student_number}",
                    status=review.status,
                    review_reason="Teacher confirmation is still required.",
                    updated_at=review.updated_at,
                )
            )

    class_workspaces: list[dict[str, Any]] = []
    for teacher_class in classes:
        class_plans = [row for row in plans if _plan_class_id(row) == teacher_class.id]
        class_assignments = [row for row in assignments if row.class_id == teacher_class.id]
        class_reviews = [row for row in grading_reviews if row.class_id == teacher_class.id]
        class_submissions = [row for row in submissions if row.class_id == teacher_class.id]
        class_workflows = [row for row in workflows if _workflow_class_id(row) == teacher_class.id]
        class_packets = [row for row in packets if row.class_id == teacher_class.id]
        class_attention_count = sum(1 for item in needs_attention if item.get("class_id") == teacher_class.id)
        class_workspaces.append(
            {
                "class_row": teacher_class,
                "active_plans": [_plan_summary(row) for row in class_plans],
                "assignments": [_assignment_summary(row) for row in class_assignments],
                "pending_grading_reviews": [
                    _grading_review_summary(row) for row in class_reviews if _grading_review_pending_confirmation(row)
                ],
                "recent_submissions": [
                    {
                        **_submission_summary(row),
                        "latest_extraction_status": (
                            latest_extraction_job_by_submission_id.get(row.id).status
                            if latest_extraction_job_by_submission_id.get(row.id) is not None
                            else None
                        ),
                        "extraction_ready_for_teacher_review": _submission_ready_for_teacher_review(
                            row,
                            latest_extraction_record_by_submission_id.get(row.id),
                            grading_reviews,
                        ),
                    }
                    for row in class_submissions[:5]
                ],
                "workflow_summaries": [_workflow_summary(row) for row in class_workflows[:5]],
                "packet_summaries": [_packet_summary(row) for row in class_packets[:5]],
                "needs_attention_count": class_attention_count,
            }
        )

    class_workspaces.sort(
        key=lambda item: (
            -int(item["needs_attention_count"]),
            item["class_row"].name.lower(),
        )
    )

    review_required_items.sort(key=lambda item: item["updated_at"], reverse=True)
    needs_attention.sort(
        key=lambda item: (
            {"critical": 0, "warning": 1, "info": 2}.get(item["severity"], 3),
            item["created_at"],
        ),
        reverse=False,
    )

    low_confidence_count = sum(
        1 for row in extracted_text_records if row.confidence_level == "low" and row.review_status != "archived"
    )
    rejected_extraction_count = sum(
        1 for row in extracted_text_records if row.review_status == "teacher_rejected"
    )
    retry_required_count = sum(
        1 for row in extracted_text_records if row.review_status == "needs_retry"
    )
    awaiting_teacher_review_count = sum(
        1 for row in extracted_text_records if row.review_status in {"pending_review", "teacher_reviewing"}
    )
    stale_extraction_job_count = sum(
        1 for row in extraction_jobs if is_stale_extraction_job(row, settings=settings, now=now)
    )
    recently_approved_count = sum(
        1
        for row in extracted_text_records
        if row.review_status == "teacher_approved"
        and (_as_utc_datetime(row.reviewed_at) or datetime.min.replace(tzinfo=UTC)) >= recent_cutoff
    )

    mastery_dashboard = build_mastery_dashboard(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        school_year_id=current_school_year.id if current_school_year else None,
        grading_period_id=active_grading_period.id if active_grading_period else None,
        settings=settings,
    )
    mastery_insights = build_workspace_mastery_insights(mastery_dashboard)

    return {
        "current_school_year": current_school_year,
        "active_grading_period": active_grading_period,
        "today_summary": {
            "active_grading_period_title": active_grading_period.title if active_grading_period else None,
            "active_workflows_count": len(active_workflows),
            "plans_needing_review_count": sum(1 for row in plans if _review_required_for_plan(row)),
            "grading_reviews_pending_confirmation_count": sum(
                1 for row in grading_reviews if _grading_review_pending_confirmation(row)
            ),
            "recent_uploads_count": sum(
                1 for row in submissions if (_as_utc_datetime(row.created_at) or datetime.min.replace(tzinfo=UTC)) >= recent_cutoff
            ),
            "workflow_failures_count": sum(1 for row in workflows if row.status == "failed"),
            "extraction_failures_count": sum(1 for row in extraction_jobs if row.status == "failed"),
            "student_work_ready_for_extraction_count": sum(
                1
                for row in submissions
                if _submission_ready_for_extraction(
                    row,
                    latest_extraction_job_by_submission_id.get(row.id),
                    latest_extraction_record_by_submission_id.get(row.id),
                )
            ),
            "extracted_artifacts_ready_for_teacher_review_count": sum(
                1
                for row in submissions
                if _submission_ready_for_teacher_review(
                    row,
                    latest_extraction_record_by_submission_id.get(row.id),
                    grading_reviews,
                )
            ),
            "low_confidence_extractions_count": low_confidence_count,
            "rejected_extractions_count": rejected_extraction_count,
            "retry_required_extractions_count": retry_required_count,
            "awaiting_teacher_review_count": awaiting_teacher_review_count,
            "stale_extraction_jobs_count": stale_extraction_job_count,
            "recently_approved_extractions_count": recently_approved_count,
        },
        "class_workspaces": class_workspaces,
        "needs_attention": needs_attention,
        "recent_activity": recent_activity,
        "active_workflows": [_workflow_summary(row) for row in active_workflows],
        "review_required_items": review_required_items[:50],
        "workspace_stats": {
            "active_plans_count": sum(1 for row in plans if row.status != "completed"),
            "plans_in_review_count": sum(1 for row in plans if _review_required_for_plan(row)),
            "pending_grading_reviews_count": sum(
                1 for row in grading_reviews if _grading_review_pending_confirmation(row)
            ),
            "recent_upload_count": sum(
                1 for row in submissions if (_as_utc_datetime(row.created_at) or datetime.min.replace(tzinfo=UTC)) >= recent_cutoff
            ),
            "workflow_failure_count": sum(1 for row in workflows if row.status == "failed"),
            "assignments_in_review_count": sum(
                1 for row in assignments if row.status in {"collected", "review_in_progress"}
            ),
            "extraction_failure_count": sum(1 for row in extraction_jobs if row.status == "failed"),
            "student_work_ready_for_extraction_count": sum(
                1
                for row in submissions
                if _submission_ready_for_extraction(
                    row,
                    latest_extraction_job_by_submission_id.get(row.id),
                    latest_extraction_record_by_submission_id.get(row.id),
                )
            ),
            "extracted_artifacts_ready_for_teacher_review_count": sum(
                1
                for row in submissions
                if _submission_ready_for_teacher_review(
                    row,
                    latest_extraction_record_by_submission_id.get(row.id),
                    grading_reviews,
                )
            ),
            "low_confidence_extractions_count": low_confidence_count,
            "rejected_extractions_count": rejected_extraction_count,
            "retry_required_extractions_count": retry_required_count,
            "awaiting_teacher_review_count": awaiting_teacher_review_count,
            "stale_extraction_jobs_count": stale_extraction_job_count,
            "recently_approved_extractions_count": recently_approved_count,
        },
        "mastery_insights": mastery_insights,
    }
