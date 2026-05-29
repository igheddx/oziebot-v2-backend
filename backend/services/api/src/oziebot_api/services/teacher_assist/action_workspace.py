from __future__ import annotations

from datetime import UTC, datetime, timedelta
import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.models.teacher_assist_assignment import TeacherAssistAssignment
from oziebot_api.models.teacher_assist_assignment_grade_record import TeacherAssistAssignmentGradeRecord
from oziebot_api.models.teacher_assist_assignment_gradebook_audit_event import (
    TeacherAssistAssignmentGradebookAuditEvent,
)
from oziebot_api.models.teacher_assist_assignment_grading_review import TeacherAssistAssignmentGradingReview
from oziebot_api.models.teacher_assist_class import TeacherAssistClass
from oziebot_api.models.teacher_assist_export_artifact import TeacherAssistExportArtifact
from oziebot_api.models.teacher_assist_extracted_text_record import TeacherAssistExtractedTextRecord
from oziebot_api.models.teacher_assist_extraction_job import TeacherAssistExtractionJob
from oziebot_api.models.teacher_assist_student_work_submission import TeacherAssistStudentWorkSubmission
from oziebot_api.models.teacher_assist_weekly_plan import TeacherAssistWeeklyPlan
from oziebot_api.models.teacher_assist_workflow import TeacherAssistWorkflow
from oziebot_api.services.teacher_assist.activity_events import list_recent_activity_events
from oziebot_api.services.teacher_assist.constants import (
    ACTION_WORKSPACE_SECTION_KEYS,
    ACTION_WORKSPACE_SEVERITIES,
    validate_action_workspace_navigation_href,
)
from oziebot_api.services.teacher_assist.extraction_review_service import is_stale_extraction_job
from oziebot_api.services.teacher_assist.grading_prep_service import GRADING_PREP_APPROVED_REVIEW_STATUSES
from oziebot_api.services.teacher_assist.mastery_dashboard import build_mastery_dashboard
from oziebot_api.services.teacher_assist.workspace_service import (
    _as_utc_datetime,
    _plan_class_id,
    _review_required_for_plan,
    _submission_ready_for_extraction,
    _workflow_class_id,
)

ACTION_SECTION_TITLES: dict[str, str] = {
    "extractions": "Extractions",
    "grading": "Grading",
    "gradebook": "Gradebook",
    "workflows_exports": "Workflows / Exports",
    "planning_assignments": "Planning / Assignments",
}

SEVERITY_SORT_ORDER: dict[str, int] = {
    "critical": 0,
    "warning": 1,
    "review": 2,
    "ready": 3,
    "info": 4,
}


def _action_item(
    *,
    action_key: str,
    action_type: str,
    severity: str,
    title: str,
    description: str,
    tenant_id: uuid.UUID,
    navigation_label: str,
    navigation_href: str,
    section_key: str,
    school_year_id: uuid.UUID | None = None,
    grading_period_id: uuid.UUID | None = None,
    class_id: uuid.UUID | None = None,
    assignment_id: uuid.UUID | None = None,
    student_work_id: uuid.UUID | None = None,
    grading_review_id: uuid.UUID | None = None,
    gradebook_record_id: uuid.UUID | None = None,
    workflow_id: uuid.UUID | None = None,
    export_artifact_id: uuid.UUID | None = None,
    extraction_job_id: uuid.UUID | None = None,
    extracted_text_id: uuid.UUID | None = None,
    created_at: datetime | None = None,
    updated_at: datetime | None = None,
) -> dict[str, Any]:
    if severity not in ACTION_WORKSPACE_SEVERITIES:
        raise ValueError(f"Unsupported action workspace severity '{severity}'")
    if section_key not in ACTION_WORKSPACE_SECTION_KEYS:
        raise ValueError(f"Unsupported action workspace section '{section_key}'")
    safe_href = validate_action_workspace_navigation_href(navigation_href)
    return {
        "action_key": action_key,
        "action_type": action_type,
        "severity": severity,
        "title": title,
        "description": description,
        "tenant_id": tenant_id,
        "school_year_id": school_year_id,
        "grading_period_id": grading_period_id,
        "class_id": class_id,
        "assignment_id": assignment_id,
        "student_work_id": student_work_id,
        "grading_review_id": grading_review_id,
        "gradebook_record_id": gradebook_record_id,
        "workflow_id": workflow_id,
        "export_artifact_id": export_artifact_id,
        "extraction_job_id": extraction_job_id,
        "extracted_text_id": extracted_text_id,
        "navigation": {"label": navigation_label, "href": safe_href},
        "created_at": created_at,
        "updated_at": updated_at,
        "section_key": section_key,
    }


def _sort_timestamp(item: dict[str, Any]) -> datetime:
    candidate = item.get("updated_at") or item.get("created_at")
    if isinstance(candidate, datetime):
        return _as_utc_datetime(candidate) or datetime.min.replace(tzinfo=UTC)
    return datetime.min.replace(tzinfo=UTC)


def _assignment_href(assignment_id: uuid.UUID | None) -> str:
    if assignment_id is None:
        return "/teacher-assist/assignments"
    return f"/teacher-assist/assignments?assignment_id={assignment_id}"


def _public_item(item: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in item.items() if key != "section_key"}


def get_teacher_assist_action_workspace(
    db: Session,
    *,
    settings: Settings,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
) -> dict[str, Any]:
    now = datetime.now(UTC)
    recent_cutoff = now - timedelta(days=7)
    stale_heartbeat_cutoff = now - timedelta(seconds=max(1, settings.teacher_assist_worker_lease_seconds))

    assignments = db.scalars(
        select(TeacherAssistAssignment).where(
            TeacherAssistAssignment.tenant_id == tenant_id,
            TeacherAssistAssignment.teacher_user_id == user_id,
        )
    ).all()
    assignment_titles = {row.id: row.title for row in assignments}

    submissions = db.scalars(
        select(TeacherAssistStudentWorkSubmission).where(
            TeacherAssistStudentWorkSubmission.tenant_id == tenant_id,
            TeacherAssistStudentWorkSubmission.teacher_user_id == user_id,
        )
    ).all()

    grading_reviews = db.scalars(
        select(TeacherAssistAssignmentGradingReview).where(
            TeacherAssistAssignmentGradingReview.tenant_id == tenant_id,
            TeacherAssistAssignmentGradingReview.teacher_user_id == user_id,
        )
    ).all()

    grade_records = db.scalars(
        select(TeacherAssistAssignmentGradeRecord).where(
            TeacherAssistAssignmentGradeRecord.tenant_id == tenant_id,
            TeacherAssistAssignmentGradeRecord.teacher_user_id == user_id,
        )
    ).all()

    extraction_jobs = db.scalars(
        select(TeacherAssistExtractionJob).where(
            TeacherAssistExtractionJob.tenant_id == tenant_id,
            TeacherAssistExtractionJob.teacher_user_id == user_id,
        )
    ).all()

    extracted_text_records = db.scalars(
        select(TeacherAssistExtractedTextRecord).where(
            TeacherAssistExtractedTextRecord.tenant_id == tenant_id,
            TeacherAssistExtractedTextRecord.teacher_user_id == user_id,
        )
    ).all()

    workflows = db.scalars(
        select(TeacherAssistWorkflow).where(
            TeacherAssistWorkflow.tenant_id == tenant_id,
            TeacherAssistWorkflow.user_id == user_id,
        )
    ).all()

    export_artifacts = db.scalars(
        select(TeacherAssistExportArtifact).where(
            TeacherAssistExportArtifact.tenant_id == tenant_id,
            TeacherAssistExportArtifact.user_id == user_id,
        )
    ).all()

    plans = db.scalars(
        select(TeacherAssistWeeklyPlan).where(
            TeacherAssistWeeklyPlan.tenant_id == tenant_id,
            TeacherAssistWeeklyPlan.user_id == user_id,
        )
    ).all()

    classes = db.scalars(select(TeacherAssistClass).where(TeacherAssistClass.tenant_id == tenant_id)).all()
    class_names = {row.id: row.name for row in classes}

    latest_job_by_submission: dict[uuid.UUID, TeacherAssistExtractionJob] = {}
    for row in sorted(extraction_jobs, key=lambda item: item.updated_at, reverse=True):
        if row.student_work_submission_id and row.student_work_submission_id not in latest_job_by_submission:
            latest_job_by_submission[row.student_work_submission_id] = row

    latest_record_by_submission: dict[uuid.UUID, TeacherAssistExtractedTextRecord] = {}
    for row in sorted(extracted_text_records, key=lambda item: item.updated_at, reverse=True):
        if row.student_work_submission_id and row.student_work_submission_id not in latest_record_by_submission:
            latest_record_by_submission[row.student_work_submission_id] = row

    latest_record_by_job: dict[uuid.UUID, TeacherAssistExtractedTextRecord] = {}
    for row in sorted(extracted_text_records, key=lambda item: item.updated_at, reverse=True):
        if row.extraction_job_id not in latest_record_by_job:
            latest_record_by_job[row.extraction_job_id] = row

    active_review_by_submission = {
        row.student_work_submission_id: row for row in grading_reviews if row.status != "archived"
    }
    active_grade_by_review = {
        row.grading_review_id: row for row in grade_records if row.record_status == "active"
    }

    items: list[dict[str, Any]] = []
    seen_keys: set[str] = set()

    def add_item(**kwargs: Any) -> None:
        key = kwargs["action_key"]
        if key in seen_keys:
            return
        seen_keys.add(key)
        items.append(_action_item(tenant_id=tenant_id, **kwargs))

    for job in extraction_jobs:
        record = latest_record_by_job.get(job.id)
        assignment_title = assignment_titles.get(job.assignment_id, "Assignment") if job.assignment_id else "Resource"
        if job.status == "failed":
            add_item(
                action_key=f"extraction_failed:{job.id}",
                action_type="extraction_failed",
                severity="critical",
                title="Extraction failed",
                description=job.error_message or f"Extraction failed for {assignment_title}. Retry or review manually.",
                school_year_id=job.school_year_id,
                grading_period_id=job.grading_period_id,
                class_id=job.class_id,
                assignment_id=job.assignment_id,
                student_work_id=job.student_work_submission_id,
                extraction_job_id=job.id,
                extracted_text_id=record.id if record else None,
                navigation_label="Open extraction",
                navigation_href=(
                    f"/teacher-assist/extractions?id={record.id}"
                    if record
                    else f"/teacher-assist/extractions?jobId={job.id}"
                ),
                created_at=job.created_at,
                updated_at=job.updated_at,
                section_key="extractions",
            )
        if is_stale_extraction_job(job, settings=settings, now=now):
            add_item(
                action_key=f"extraction_stale:{job.id}",
                action_type="stale_extraction_job",
                severity="critical",
                title="Stale extraction job",
                description="A running extraction job has a stale heartbeat and may need recovery.",
                school_year_id=job.school_year_id,
                grading_period_id=job.grading_period_id,
                class_id=job.class_id,
                assignment_id=job.assignment_id,
                student_work_id=job.student_work_submission_id,
                extraction_job_id=job.id,
                extracted_text_id=record.id if record else None,
                navigation_label="Open extraction",
                navigation_href=(
                    f"/teacher-assist/extractions?id={record.id}"
                    if record
                    else f"/teacher-assist/extractions?jobId={job.id}"
                ),
                created_at=job.created_at,
                updated_at=job.updated_at,
                section_key="extractions",
            )
        if job.status == "queued" and job.retry_count > 0:
            add_item(
                action_key=f"extraction_retry_required:{job.id}",
                action_type="extraction_retry_required",
                severity="warning",
                title="Extraction retry queued",
                description=f"Extraction attempt {job.attempt_number} is queued after a prior failure.",
                school_year_id=job.school_year_id,
                grading_period_id=job.grading_period_id,
                class_id=job.class_id,
                assignment_id=job.assignment_id,
                student_work_id=job.student_work_submission_id,
                extraction_job_id=job.id,
                navigation_label="Open extraction",
                navigation_href=f"/teacher-assist/extractions?jobId={job.id}",
                created_at=job.created_at,
                updated_at=job.updated_at,
                section_key="extractions",
            )

    for record in extracted_text_records:
        if record.review_status in {"pending_review", "teacher_reviewing"}:
            add_item(
                action_key=f"extraction_pending_review:{record.id}",
                action_type="extraction_pending_review",
                severity="review",
                title="Extracted text awaiting review",
                description=(
                    f"STUDENT #{record.student_number} extraction needs teacher approval."
                    if record.student_number is not None
                    else "Extracted text needs teacher approval."
                ),
                school_year_id=record.school_year_id,
                grading_period_id=record.grading_period_id,
                class_id=record.class_id,
                assignment_id=record.assignment_id,
                student_work_id=record.student_work_submission_id,
                extraction_job_id=record.extraction_job_id,
                extracted_text_id=record.id,
                navigation_label="Review extraction",
                navigation_href=f"/teacher-assist/extractions?id={record.id}",
                created_at=record.created_at,
                updated_at=record.updated_at,
                section_key="extractions",
            )
        if record.confidence_level == "low" and record.review_status not in {"archived", "teacher_approved", "reviewed"}:
            add_item(
                action_key=f"extraction_low_confidence:{record.id}",
                action_type="extraction_low_confidence",
                severity="warning",
                title="Low confidence extraction",
                description="Extracted text has low confidence and should be reviewed carefully.",
                school_year_id=record.school_year_id,
                grading_period_id=record.grading_period_id,
                class_id=record.class_id,
                assignment_id=record.assignment_id,
                student_work_id=record.student_work_submission_id,
                extraction_job_id=record.extraction_job_id,
                extracted_text_id=record.id,
                navigation_label="Review extraction",
                navigation_href=f"/teacher-assist/extractions?id={record.id}",
                created_at=record.created_at,
                updated_at=record.updated_at,
                section_key="extractions",
            )
        if record.review_status in {"issue_flagged", "teacher_rejected", "needs_retry"}:
            severity = "critical" if record.review_status in {"teacher_rejected", "issue_flagged"} else "warning"
            add_item(
                action_key=f"extraction_remediation:{record.id}",
                action_type=f"extraction_{record.review_status}",
                severity=severity,
                title=f"Extraction {record.review_status.replace('_', ' ')}",
                description="Extracted text needs remediation before downstream grading prep.",
                school_year_id=record.school_year_id,
                grading_period_id=record.grading_period_id,
                class_id=record.class_id,
                assignment_id=record.assignment_id,
                student_work_id=record.student_work_submission_id,
                extraction_job_id=record.extraction_job_id,
                extracted_text_id=record.id,
                navigation_label="Open extraction review",
                navigation_href=f"/teacher-assist/extractions?id={record.id}",
                created_at=record.created_at,
                updated_at=record.updated_at,
                section_key="extractions",
            )

    for submission in submissions:
        latest_job = latest_job_by_submission.get(submission.id)
        latest_record = latest_record_by_submission.get(submission.id)
        assignment_title = assignment_titles.get(submission.assignment_id, "Assignment")
        if _submission_ready_for_extraction(submission, latest_job, latest_record):
            add_item(
                action_key=f"student_work_ready_for_extraction:{submission.id}",
                action_type="student_work_ready_for_extraction",
                severity="warning",
                title="Student work ready for extraction",
                description=(
                    f"STUDENT #{submission.student_number} upload on '{assignment_title}' has not been extracted yet."
                ),
                school_year_id=submission.school_year_id,
                grading_period_id=submission.grading_period_id,
                class_id=submission.class_id,
                assignment_id=submission.assignment_id,
                student_work_id=submission.id,
                extraction_job_id=latest_job.id if latest_job else None,
                navigation_label="Open assignment",
                navigation_href=_assignment_href(submission.assignment_id),
                created_at=submission.created_at,
                updated_at=submission.updated_at,
                section_key="planning_assignments",
            )
        if (
            latest_record is not None
            and latest_record.review_status in GRADING_PREP_APPROVED_REVIEW_STATUSES
            and submission.id not in active_review_by_submission
        ):
            add_item(
                action_key=f"grading_prep_ready_no_review:{submission.id}",
                action_type="grading_prep_ready_no_review",
                severity="ready",
                title="Ready for grading prep",
                description=(
                    f"STUDENT #{submission.student_number} has teacher-approved extraction text but no grading review yet."
                ),
                school_year_id=submission.school_year_id,
                grading_period_id=submission.grading_period_id,
                class_id=submission.class_id,
                assignment_id=submission.assignment_id,
                student_work_id=submission.id,
                extracted_text_id=latest_record.id,
                navigation_label="Open assignment",
                navigation_href=_assignment_href(submission.assignment_id),
                created_at=submission.created_at,
                updated_at=latest_record.updated_at,
                section_key="grading",
            )

    for review in grading_reviews:
        assignment_title = assignment_titles.get(review.assignment_id, "Assignment")
        review_action: tuple[str, str, str] | None = None
        if review.status == "draft":
            review_action = ("warning", "grading_review_draft", "Draft grading review")
        elif review.status == "ai_suggested":
            review_action = ("review", "grading_review_ai_suggested", "AI suggestion awaiting teacher review")
        elif review.status == "teacher_reviewing":
            review_action = ("review", "grading_review_teacher_reviewing", "Grading review in progress")
        elif review.status == "returned_for_revision":
            review_action = ("warning", "grading_review_returned_for_revision", "Grading review returned for revision")

        if review_action is not None:
            severity, action_type, title = review_action
            add_item(
                action_key=f"grading_review:{review.id}",
                action_type=action_type,
                severity=severity,
                title=title,
                description=f"STUDENT #{review.student_number} on '{assignment_title}' needs teacher attention.",
                school_year_id=review.school_year_id,
                grading_period_id=review.grading_period_id,
                class_id=review.class_id,
                assignment_id=review.assignment_id,
                student_work_id=review.student_work_submission_id,
                grading_review_id=review.id,
                navigation_label="Open grading review",
                navigation_href=_assignment_href(review.assignment_id),
                created_at=review.created_at,
                updated_at=review.updated_at,
                section_key="grading",
            )

        if review.status == "teacher_confirmed" and review.id not in active_grade_by_review:
            add_item(
                action_key=f"gradebook_ready_to_commit:{review.id}",
                action_type="gradebook_ready_to_commit",
                severity="ready",
                title="Ready to commit to gradebook",
                description=(
                    f"STUDENT #{review.student_number} grading review is teacher-confirmed and ready for manual gradebook commit."
                ),
                school_year_id=review.school_year_id,
                grading_period_id=review.grading_period_id,
                class_id=review.class_id,
                assignment_id=review.assignment_id,
                student_work_id=review.student_work_submission_id,
                grading_review_id=review.id,
                navigation_label="Commit in assignments",
                navigation_href=_assignment_href(review.assignment_id),
                created_at=review.created_at,
                updated_at=review.updated_at,
                section_key="gradebook",
            )

    for record in grade_records:
        assignment_title = assignment_titles.get(record.assignment_id, "Assignment")
        if record.record_status == "reversed":
            add_item(
                action_key=f"gradebook_reversed:{record.id}",
                action_type="gradebook_record_reversed",
                severity="warning",
                title="Grade reversed",
                description=f"STUDENT #{record.student_number} grade on '{assignment_title}' was reversed. Review audit history.",
                school_year_id=record.school_year_id,
                grading_period_id=record.grading_period_id,
                class_id=record.class_id,
                assignment_id=record.assignment_id,
                student_work_id=record.student_work_submission_id,
                grading_review_id=record.grading_review_id,
                gradebook_record_id=record.id,
                navigation_label="Open gradebook",
                navigation_href=f"/teacher-assist/gradebook?assignment_id={record.assignment_id}",
                created_at=record.created_at,
                updated_at=record.updated_at,
                section_key="gradebook",
            )
        elif (_as_utc_datetime(record.updated_at) or datetime.min.replace(tzinfo=UTC)) >= recent_cutoff:
            add_item(
                action_key=f"gradebook_recent_commit:{record.id}",
                action_type="gradebook_recent_activity",
                severity="info",
                title="Recent gradebook commit",
                description=f"STUDENT #{record.student_number} grade was recently committed on '{assignment_title}'.",
                school_year_id=record.school_year_id,
                grading_period_id=record.grading_period_id,
                class_id=record.class_id,
                assignment_id=record.assignment_id,
                student_work_id=record.student_work_submission_id,
                grading_review_id=record.grading_review_id,
                gradebook_record_id=record.id,
                navigation_label="Open gradebook",
                navigation_href=f"/teacher-assist/gradebook?assignment_id={record.assignment_id}",
                created_at=record.created_at,
                updated_at=record.updated_at,
                section_key="gradebook",
            )

    recent_audit_events = db.scalars(
        select(TeacherAssistAssignmentGradebookAuditEvent)
        .where(
            TeacherAssistAssignmentGradebookAuditEvent.tenant_id == tenant_id,
            TeacherAssistAssignmentGradebookAuditEvent.teacher_user_id == user_id,
            TeacherAssistAssignmentGradebookAuditEvent.created_at >= recent_cutoff,
        )
        .order_by(TeacherAssistAssignmentGradebookAuditEvent.created_at.desc())
        .limit(20)
    ).all()
    for audit_event in recent_audit_events:
        if audit_event.event_type in {"commit_corrected", "commit_reversed"}:
            add_item(
                action_key=f"gradebook_audit:{audit_event.id}",
                action_type=f"gradebook_{audit_event.event_type}",
                severity="review" if audit_event.event_type == "commit_corrected" else "warning",
                title=(
                    "Gradebook correction activity"
                    if audit_event.event_type == "commit_corrected"
                    else "Gradebook reversal activity"
                ),
                description=audit_event.summary_text,
                assignment_id=audit_event.assignment_id,
                gradebook_record_id=audit_event.grade_record_id,
                navigation_label="Open gradebook",
                navigation_href=f"/teacher-assist/gradebook?assignment_id={audit_event.assignment_id}",
                created_at=audit_event.created_at,
                updated_at=audit_event.created_at,
                section_key="gradebook",
            )

    for workflow in workflows:
        class_id = _workflow_class_id(workflow)
        if workflow.status == "failed":
            add_item(
                action_key=f"workflow_failed:{workflow.id}",
                action_type="workflow_failed",
                severity="critical",
                title="Workflow failed",
                description=workflow.error_message or "A TeacherAssist workflow failed and needs review.",
                class_id=class_id,
                workflow_id=workflow.id,
                navigation_label="Open weekly planning",
                navigation_href="/teacher-assist/weekly-planning",
                created_at=workflow.created_at,
                updated_at=workflow.updated_at,
                section_key="workflows_exports",
            )
        heartbeat_at = _as_utc_datetime(workflow.heartbeat_at)
        if workflow.status == "running" and heartbeat_at is not None and heartbeat_at < stale_heartbeat_cutoff:
            add_item(
                action_key=f"workflow_stale:{workflow.id}",
                action_type="workflow_stale_running",
                severity="critical",
                title="Stale running workflow",
                description="A running workflow has not updated its heartbeat within the expected lease window.",
                class_id=class_id,
                workflow_id=workflow.id,
                navigation_label="Open weekly planning",
                navigation_href="/teacher-assist/weekly-planning",
                created_at=workflow.created_at,
                updated_at=workflow.updated_at,
                section_key="workflows_exports",
            )
        if workflow.status == "queued" and workflow.retry_count > 0:
            add_item(
                action_key=f"workflow_retrying:{workflow.id}",
                action_type="workflow_retrying",
                severity="warning",
                title="Workflow retry queued",
                description=f"Workflow has retried {workflow.retry_count} time(s) and is queued again.",
                class_id=class_id,
                workflow_id=workflow.id,
                navigation_label="Open weekly planning",
                navigation_href="/teacher-assist/weekly-planning",
                created_at=workflow.created_at,
                updated_at=workflow.updated_at,
                section_key="workflows_exports",
            )

    for export in export_artifacts:
        if export.artifact_status == "failed":
            add_item(
                action_key=f"export_failed:{export.id}",
                action_type="export_failed",
                severity="critical",
                title="Export failed",
                description=f"Export '{export.title}' failed and may need to be re-queued.",
                export_artifact_id=export.id,
                workflow_id=export.workflow_id,
                navigation_label="Open exports",
                navigation_href="/teacher-assist/exports",
                created_at=export.created_at,
                updated_at=export.updated_at,
                section_key="workflows_exports",
            )
        elif export.artifact_status == "ready" and (_as_utc_datetime(export.updated_at) or datetime.min.replace(tzinfo=UTC)) >= recent_cutoff:
            add_item(
                action_key=f"export_ready:{export.id}",
                action_type="export_ready_for_download",
                severity="ready",
                title="Export ready for download",
                description=f"'{export.title}' is ready to download.",
                export_artifact_id=export.id,
                workflow_id=export.workflow_id,
                navigation_label="Download export",
                navigation_href="/teacher-assist/exports",
                created_at=export.created_at,
                updated_at=export.updated_at,
                section_key="workflows_exports",
            )

    for plan in plans:
        class_id = _plan_class_id(plan)
        content = dict(plan.content_json or {})
        quality_flags = list(content.get("quality_flags") or [])
        missing_context_warnings = list(content.get("missing_context_warnings") or [])
        if plan.status == "in_progress" or _review_required_for_plan(plan):
            add_item(
                action_key=f"plan_in_progress:{plan.id}",
                action_type="plan_in_progress",
                severity="warning",
                title="Instructional plan needs review",
                description=f"'{plan.title}' still requires teacher review before classroom use.",
                class_id=class_id,
                navigation_label="Open plan",
                navigation_href=f"/teacher-assist/weekly-planning/plans?id={plan.id}",
                created_at=plan.created_at,
                updated_at=plan.updated_at,
                section_key="planning_assignments",
            )
        if quality_flags or missing_context_warnings:
            add_item(
                action_key=f"plan_quality:{plan.id}",
                action_type="plan_quality_or_context_warning",
                severity="info",
                title="Plan quality or context warnings",
                description=f"'{plan.title}' has quality flags or missing-context warnings to review.",
                class_id=class_id,
                navigation_label="Open plan",
                navigation_href=f"/teacher-assist/weekly-planning/plans?id={plan.id}",
                created_at=plan.created_at,
                updated_at=plan.updated_at,
                section_key="planning_assignments",
            )

    for assignment in assignments:
        if assignment.status in {"collected", "review_in_progress"}:
            add_item(
                action_key=f"assignment_in_review:{assignment.id}",
                action_type="assignment_in_review",
                severity="review",
                title="Assignment awaiting review",
                description=f"'{assignment.title}' has student work awaiting teacher review.",
                school_year_id=assignment.school_year_id,
                grading_period_id=assignment.grading_period_id,
                class_id=assignment.class_id,
                assignment_id=assignment.id,
                navigation_label="Open assignment",
                navigation_href=_assignment_href(assignment.id),
                created_at=assignment.created_at,
                updated_at=assignment.updated_at,
                section_key="planning_assignments",
            )

    mastery_dashboard = build_mastery_dashboard(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        settings=settings,
    )
    for item in mastery_dashboard.get("low_mastery_alerts", []):
        add_item(
            action_key=f"mastery_low_alert:{item.get('standard_id')}:{item.get('mastery_matrix_id')}",
            action_type="mastery_low_alert",
            severity="critical",
            title="Critical mastery attention needed",
            description=(
                f"{item.get('standard_code') or 'Standard'} in {item.get('matrix_title') or 'matrix'} "
                f"shows {int(float(item.get('mastery_percentage', 0)) * 100)}% committed mastery."
            ),
            class_id=item.get("class_id"),
            navigation_label="Open mastery heatmap",
            navigation_href=f"/teacher-assist/mastery?id={item.get('mastery_matrix_id')}",
            section_key="planning_assignments",
        )
    for item in mastery_dashboard.get("reteach_recommended_standards", []):
        if item.get("operational_status") != "reteach_recommended":
            continue
        add_item(
            action_key=f"mastery_reteach:{item.get('standard_id')}:{item.get('mastery_matrix_id')}",
            action_type="mastery_reteach_recommended",
            severity="warning",
            title="Reteach insight recommended",
            description=(
                f"{item.get('standard_code') or 'Standard'} in {item.get('matrix_title') or 'matrix'} "
                "may benefit from reteach planning."
            ),
            class_id=item.get("class_id"),
            navigation_label="Review reteach insights",
            navigation_href=f"/teacher-assist/mastery?id={item.get('mastery_matrix_id')}",
            section_key="planning_assignments",
        )
    for item in mastery_dashboard.get("unassessed_standards", [])[:5]:
        add_item(
            action_key=f"mastery_unassessed:{item.get('standard_id')}:{item.get('mastery_matrix_id')}",
            action_type="mastery_unassessed_standard",
            severity="info",
            title="Unassessed standard",
            description=(
                f"{item.get('standard_code') or 'Standard'} in {item.get('matrix_title') or 'matrix'} "
                "has no committed mastery evaluations yet."
            ),
            class_id=item.get("class_id"),
            navigation_label="Open mastery matrix",
            navigation_href=f"/teacher-assist/mastery?id={item.get('mastery_matrix_id')}",
            section_key="planning_assignments",
        )

    open_items = [item for item in items if item["severity"] != "info"]
    summary = {
        "total_open_actions": len(open_items),
        "critical_count": sum(1 for item in items if item["severity"] == "critical"),
        "warning_count": sum(1 for item in items if item["severity"] == "warning"),
        "review_count": sum(1 for item in items if item["severity"] == "review"),
        "ready_count": sum(1 for item in items if item["severity"] == "ready"),
        "mastery_alert_count": sum(
            1
            for item in items
            if str(item.get("action_type", "")).startswith("mastery_")
        ),
    }

    priority_items = [
        _public_item(item)
        for item in sorted(
            open_items,
            key=lambda row: (SEVERITY_SORT_ORDER[row["severity"]], -_sort_timestamp(row).timestamp()),
        )[:10]
    ]

    sections: list[dict[str, Any]] = []
    for section_key in ACTION_WORKSPACE_SECTION_KEYS:
        section_items = [_public_item(item) for item in items if item["section_key"] == section_key]
        section_items.sort(key=lambda row: (SEVERITY_SORT_ORDER[row["severity"]], -_sort_timestamp(row).timestamp()))
        sections.append(
            {
                "section_key": section_key,
                "title": ACTION_SECTION_TITLES[section_key],
                "count": len(section_items),
                "items": section_items,
            }
        )

    class_rollups: list[dict[str, Any]] = []
    for teacher_class in classes:
        class_items = [item for item in open_items if item.get("class_id") == teacher_class.id]
        if not class_items:
            continue
        class_rollups.append(
            {
                "class_id": teacher_class.id,
                "class_name": class_names.get(teacher_class.id, teacher_class.name),
                "open_action_count": len(class_items),
                "extraction_count": sum(1 for item in class_items if item["section_key"] == "extractions"),
                "grading_count": sum(1 for item in class_items if item["section_key"] == "grading"),
                "gradebook_count": sum(1 for item in class_items if item["section_key"] == "gradebook"),
                "workflow_export_count": sum(1 for item in class_items if item["section_key"] == "workflows_exports"),
                "planning_assignment_count": sum(
                    1 for item in class_items if item["section_key"] == "planning_assignments"
                ),
            }
        )
    class_rollups.sort(key=lambda row: (-int(row["open_action_count"]), str(row["class_name"]).lower()))

    recent_activity_rows = list_recent_activity_events(db, tenant_id=tenant_id, user_id=user_id, limit=20)
    recent_activity = [
        {
            "id": row.id,
            "event_category": row.event_category,
            "event_type": row.event_type,
            "entity_type": row.entity_type,
            "entity_id": row.entity_id,
            "summary_text": row.summary_text,
            "class_id": row.class_id,
            "created_at": row.created_at,
        }
        for row in recent_activity_rows
    ]

    return {
        "summary": summary,
        "sections": sections,
        "priority_items": priority_items,
        "class_rollups": class_rollups,
        "recent_activity": recent_activity,
    }
