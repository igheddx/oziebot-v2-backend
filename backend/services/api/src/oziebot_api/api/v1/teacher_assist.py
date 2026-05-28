from __future__ import annotations

from datetime import UTC, datetime, timedelta
import uuid

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import Response

from oziebot_api.config import Settings
from oziebot_api.deps import DbSession, settings_dep
from oziebot_api.deps.auth import CurrentUser
from oziebot_api.models.teacher_assist_assignment import TeacherAssistAssignment
from oziebot_api.models.teacher_assist_activity_event import TeacherAssistActivityEvent
from oziebot_api.models.teacher_assist_assignment_grading_review import (
    TeacherAssistAssignmentGradingReview,
)
from oziebot_api.models.teacher_assist_assignment_grading_review_item import (
    TeacherAssistAssignmentGradingReviewItem,
)
from oziebot_api.models.teacher_assist_assignment_print_packet import TeacherAssistAssignmentPrintPacket
from oziebot_api.models.teacher_assist_assignment_print_page import TeacherAssistAssignmentPrintPage
from oziebot_api.models.teacher_assist_assignment_resource import TeacherAssistAssignmentResource
from oziebot_api.models.teacher_assist_assignment_standard import TeacherAssistAssignmentStandard
from oziebot_api.models.teacher_assist_extracted_text_record import TeacherAssistExtractedTextRecord
from oziebot_api.models.teacher_assist_extraction_job import TeacherAssistExtractionJob
from oziebot_api.models.teacher_assist_student_work_submission import TeacherAssistStudentWorkSubmission
from oziebot_api.models.teacher_assist_class import TeacherAssistClass
from oziebot_api.models.teacher_assist_class_subject import TeacherAssistClassSubject
from oziebot_api.models.teacher_assist_grading_period import TeacherAssistGradingPeriod
from oziebot_api.models.teacher_assist_ai_usage_event import TeacherAssistAIUsageEvent
from oziebot_api.models.teacher_assist_pacing_guide import TeacherAssistPacingGuide
from oziebot_api.models.teacher_assist_pacing_item import TeacherAssistPacingItem
from oziebot_api.models.teacher_assist_profile import TeacherAssistProfile
from oziebot_api.models.teacher_assist_resource_library_item import TeacherAssistResourceLibraryItem
from oziebot_api.models.teacher_assist_planning_input_draft import TeacherAssistPlanningInputDraft
from oziebot_api.models.teacher_assist_school_year import TeacherAssistSchoolYear
from oziebot_api.models.teacher_assist_standard import TeacherAssistStandard
from oziebot_api.models.teacher_assist_subject import TeacherAssistSubject
from oziebot_api.models.teacher_assist_weekly_plan import TeacherAssistWeeklyPlan
from oziebot_api.models.teacher_assist_weekly_plan_version import TeacherAssistWeeklyPlanVersion
from oziebot_api.models.teacher_assist_workflow import TeacherAssistWorkflow
from oziebot_api.models.teacher_assist_workflow_step import TeacherAssistWorkflowStep
from oziebot_api.schemas.teacher_assist import (
    AssignmentCreate,
    TeacherAssistActivityEventOut,
    AssignmentGradingReviewCreate,
    AssignmentGradingReviewItemOut,
    AssignmentGradingReviewOut,
    AssignmentGradingReviewStatusUpdate,
    AssignmentGradingReviewUpdate,
    AssignmentOut,
    AssignmentPrintPacketCreate,
    AssignmentPrintPacketOut,
    AssignmentPrintPageOut,
    AssignmentResourceCreate,
    AssignmentStandardCreate,
    AssignmentStudentWorkOut,
    AssignmentStudentWorkPacketContextUpdate,
    AssignmentStudentWorkStatusUpdate,
    AssignmentStatusUpdate,
    ClassCreate,
    ClassOut,
    ClassSubjectCreate,
    ClassSubjectOut,
    CurriculumRolloverCandidateOut,
    CurriculumRolloverCandidatesOut,
    CurriculumRolloverCopyCreate,
    CurriculumRolloverCopyOut,
    GradingPeriodCreate,
    GradingPeriodOut,
    InstructionalPlanLibraryItemOut,
    PacingGuideCreate,
    PacingGuideOut,
    PacingItemCreate,
    PacingItemOut,
    PacingItemResourceCreate,
    PacingItemStandardCreate,
    PlanningDraftContextPreviewOut,
    PlanningDraftCreate,
    PlanningDraftGenerationPreviewOut,
    PlanningDraftOut,
    PlanningDraftReadinessOut,
    PlanningDraftResourceCreate,
    PlanningDraftStatusUpdate,
    ResourceLinkCreate,
    ResourceOut,
    SchoolYearCreate,
    SchoolYearOut,
    StandardCreate,
    StandardOut,
    SubjectCreate,
    SubjectOut,
    TeacherAssistFileDownloadOut,
    TeacherAssistExtractedTextApprovedTextUpdate,
    TeacherAssistExtractedTextDetailAggregateOut,
    TeacherAssistExtractedTextDetailOut,
    TeacherAssistExtractedTextHistoryOut,
    TeacherAssistExtractedTextRecordOut,
    TeacherAssistExtractedTextReviewStatusUpdate,
    TeacherAssistExtractionJobCancelUpdate,
    TeacherAssistExtractionJobDetailOut,
    TeacherAssistExtractionJobOut,
    TeacherAssistExtractionRunOut,
    TeacherAssistExtractionSourceArtifactOut,
    TeacherAssistExtractionSummaryOut,
    TeacherAssistOptionsOut,
    TeacherAssistWorkflowCancelUpdate,
    TeacherAssistAIUsageEventOut,
    TeacherAssistClassWorkspaceOut,
    TeacherAssistWorkflowDetailOut,
    TeacherAssistWorkspaceAssignmentSummaryOut,
    TeacherAssistWorkspaceGradingReviewSummaryOut,
    TeacherAssistWorkspaceNeedsAttentionOut,
    TeacherAssistWorkflowOut,
    TeacherAssistWorkspaceOut,
    TeacherAssistWorkspacePacketSummaryOut,
    TeacherAssistWorkspacePlanSummaryOut,
    TeacherAssistWorkspaceReviewRequiredItemOut,
    TeacherAssistWorkspaceStatsOut,
    TeacherAssistWorkspaceSubmissionSummaryOut,
    TeacherAssistWorkspaceTodaySummaryOut,
    TeacherAssistWorkspaceWorkflowSummaryOut,
    TeacherAssistWorkflowStepOut,
    TeacherProfileOut,
    TeacherProfileUpsert,
    WeeklyPlanAssignmentCreate,
    WeeklyPlanCopyCreate,
    WeeklyPlanOut,
    WeeklyPlanSectionRegenerationCreate,
    WeeklyPlanSharingUpdate,
    WeeklyPlanUpdate,
    WeeklyPlanVersionOut,
)
from oziebot_api.services.teacher_assist.constants import (
    ASSIGNMENT_STATUSES,
    ASSIGNMENT_TYPES,
    ASSIGNMENT_PRINT_OUTPUT_FORMATS,
    ASSIGNMENT_PRINT_PACKET_STATUSES,
    ASSIGNMENT_PRINT_TEMPLATE_TYPES,
    ASSIGNMENT_GRADING_REVIEW_SOURCES,
    ASSIGNMENT_GRADING_REVIEW_STATUSES,
    TEACHER_ASSIST_EXTRACTION_ARTIFACT_TYPES,
    TEACHER_ASSIST_EXTRACTION_JOB_STATUSES,
    EXTRACTION_CONFIDENCE_LEVELS,
    EXTRACTION_REVIEW_STATUSES,
    ASSIGNMENT_STUDENT_WORK_PROCESSING_STATUSES,
    ASSIGNMENT_STUDENT_WORK_UPLOAD_STATUSES,
    GRADING_PERIOD_TYPES,
    PLANNING_SCOPES,
    PLANNING_DRAFT_STATUSES,
    RESOURCE_TYPES,
    STANDARD_TYPES,
    SUPPORTED_GRADE_LEVELS,
)
from oziebot_api.services.teacher_assist.assignments import (
    attach_assignment_resource,
    attach_assignment_standard,
    create_assignment,
    create_assignment_from_weekly_plan,
    get_assignment_or_404,
    list_assignment_resources,
    list_assignment_standards,
    list_assignments,
    update_assignment,
    update_assignment_status,
)
from oziebot_api.services.teacher_assist.grading_reviews import (
    create_grading_review_from_student_work,
    get_grading_review_or_404,
    list_assignment_grading_reviews,
    update_grading_review,
    update_grading_review_status,
)
from oziebot_api.services.teacher_assist.print_packets import (
    create_assignment_print_packet,
    get_print_packet_or_404,
    list_assignment_print_packets,
    list_print_packet_pages,
    render_qr_svg_data_uri,
)
from oziebot_api.services.teacher_assist.extraction_review_service import (
    get_extracted_text_detail,
    get_extracted_text_history,
    get_extraction_job_detail,
    list_extraction_summaries,
    retry_extraction_job,
    retry_latest_eligible_extraction_for_resource,
    retry_latest_eligible_extraction_for_submission,
    save_extracted_text_approved_content,
    update_extracted_text_review_status,
)
from oziebot_api.services.teacher_assist.extraction_jobs import (
    cancel_extraction_job,
    create_resource_extraction_job,
    create_student_work_extraction_job,
    latest_extraction_state_for_resources,
    latest_extraction_state_for_submissions,
    list_resource_extraction_runs,
    list_student_work_extraction_runs,
)
from oziebot_api.services.teacher_assist.student_work import (
    create_student_work_submission,
    get_student_work_submission_or_404,
    link_student_work_submission_context,
    list_assignment_student_work_submissions,
    update_student_work_submission_processing_status,
)
from oziebot_api.services.teacher_assist.planning import (
    attach_pacing_item_resource,
    attach_pacing_item_standard,
    attach_planning_draft_resource,
    create_link_resource,
    create_pacing_guide,
    create_pacing_item,
    create_planning_draft,
    create_uploaded_resource,
    get_pacing_guide_or_404,
    get_planning_draft_context_preview,
    get_pacing_item_or_404,
    get_resource_or_404,
    list_pacing_guides,
    list_pacing_item_resources,
    list_pacing_items,
    list_pacing_item_standards,
    list_planning_draft_pacing_item_links,
    list_planning_draft_resources,
    list_planning_draft_standard_links,
    list_planning_draft_subject_links,
    list_planning_drafts,
    list_resource_link_counts,
    list_resources,
    update_pacing_guide,
    update_pacing_item,
    update_planning_draft,
    update_planning_draft_status,
)
from oziebot_api.services.teacher_assist.workflow_service import (
    cancel_teacher_assist_workflow,
    curriculum_rollover_candidates,
    curriculum_rollover_copy,
    create_weekly_plan_workflow,
    copy_weekly_plan,
    get_visible_weekly_plan_or_404,
    get_teacher_assist_workflow_or_404,
    get_weekly_plan_version_or_404,
    list_instructional_plan_library,
    list_workflow_usage_events,
    list_teacher_assist_workflows,
    list_teacher_assist_usage_events,
    list_weekly_plan_versions,
    list_weekly_plans,
    regenerate_weekly_plan_section,
    update_weekly_plan_sharing,
    update_weekly_plan,
    _plan_source_metadata,
)
from oziebot_api.services.teacher_assist.workspace_service import get_teacher_assist_workspace
from oziebot_api.services.teacher_assist.setup import (
    attach_class_subject,
    create_class,
    create_grading_period,
    create_school_year,
    create_standard,
    create_subject,
    get_teacher_profile,
    list_class_subjects,
    list_classes,
    list_grading_periods,
    list_school_years,
    list_standards,
    list_subjects,
    teacher_assist_context_for_user,
    update_class,
    update_grading_period,
    update_school_year,
    upsert_teacher_profile,
)
from oziebot_api.services.teacher_assist.storage import (
    build_content_disposition,
    get_teacher_assist_download_url,
    open_teacher_assist_stream,
    resolve_teacher_assist_local_download,
    store_teacher_assist_upload,
    teacher_assist_file_exists,
)

router = APIRouter(prefix="/teacher-assist", tags=["teacher_assist"])


def _profile_out(row: TeacherAssistProfile | None) -> TeacherProfileOut:
    if row is None:
        return TeacherProfileOut()
    return TeacherProfileOut(
        id=row.id,
        preferred_grade_level=row.preferred_grade_level,
        default_student_count=row.default_student_count,
        preferred_grading_period_type=row.preferred_grading_period_type,
        timezone=row.timezone,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _school_year_out(row: TeacherAssistSchoolYear) -> SchoolYearOut:
    return SchoolYearOut(
        id=row.id,
        tenant_id=row.tenant_id,
        title=row.title,
        start_date=row.start_date,
        end_date=row.end_date,
        is_active=row.is_active,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _grading_period_out(row: TeacherAssistGradingPeriod) -> GradingPeriodOut:
    return GradingPeriodOut(
        id=row.id,
        school_year_id=row.school_year_id,
        title=row.title,
        grading_period_type=row.grading_period_type,
        start_date=row.start_date,
        end_date=row.end_date,
        sort_order=row.sort_order,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _subject_out(row: TeacherAssistSubject) -> SubjectOut:
    return SubjectOut(
        id=row.id,
        tenant_id=row.tenant_id,
        code=row.code,
        name=row.name,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _class_subject_map(rows: list[TeacherAssistClassSubject]) -> dict[uuid.UUID, list[uuid.UUID]]:
    mapping: dict[uuid.UUID, list[uuid.UUID]] = {}
    for row in rows:
        mapping.setdefault(row.class_id, []).append(row.subject_id)
    return mapping


def _assignment_standard_map(
    rows: list[TeacherAssistAssignmentStandard],
) -> dict[uuid.UUID, list[uuid.UUID]]:
    mapping: dict[uuid.UUID, list[uuid.UUID]] = {}
    for row in rows:
        mapping.setdefault(row.assignment_id, []).append(row.standard_id)
    return mapping


def _assignment_resource_map(
    rows: list[TeacherAssistAssignmentResource],
) -> dict[uuid.UUID, list[uuid.UUID]]:
    mapping: dict[uuid.UUID, list[uuid.UUID]] = {}
    for row in rows:
        mapping.setdefault(row.assignment_id, []).append(row.resource_library_item_id)
    return mapping


def _class_out(row: TeacherAssistClass, *, subject_ids: list[uuid.UUID]) -> ClassOut:
    return ClassOut(
        id=row.id,
        tenant_id=row.tenant_id,
        school_year_id=row.school_year_id,
        name=row.name,
        grade_level=row.grade_level,
        student_count=row.student_count,
        subject_ids=subject_ids,
        student_number_range_start=1,
        student_number_range_end=row.student_count,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _standard_out(row: TeacherAssistStandard) -> StandardOut:
    return StandardOut(
        id=row.id,
        tenant_id=row.tenant_id,
        subject_id=row.subject_id,
        standard_type=row.standard_type,
        code=row.code,
        description=row.description,
        grade_level=row.grade_level,
        school_year_id=row.school_year_id,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _pacing_guide_out(row: TeacherAssistPacingGuide, *, item_count: int) -> PacingGuideOut:
    return PacingGuideOut(
        id=row.id,
        tenant_id=row.tenant_id,
        school_year_id=row.school_year_id,
        title=row.title,
        description=row.description,
        grade_level=row.grade_level,
        subject_id=row.subject_id,
        is_shared=row.is_shared,
        created_by_user_id=row.created_by_user_id,
        item_count=item_count,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _resource_out(
    row: TeacherAssistResourceLibraryItem,
    *,
    linked_pacing_items_count: int,
    linked_planning_drafts_count: int,
    latest_extraction_job: TeacherAssistExtractionJob | None = None,
    latest_extracted_text: TeacherAssistExtractedTextRecord | None = None,
) -> ResourceOut:
    return ResourceOut(
        id=row.id,
        tenant_id=row.tenant_id,
        uploaded_by_user_id=row.uploaded_by_user_id,
        title=row.title,
        description=row.description,
        resource_type=row.resource_type,
        storage_key=row.storage_key,
        original_filename=row.original_filename,
        mime_type=row.mime_type,
        file_size=row.file_size,
        external_url=row.external_url,
        uploaded_at=row.uploaded_at,
        linked_pacing_items_count=linked_pacing_items_count,
        linked_planning_drafts_count=linked_planning_drafts_count,
        latest_extraction_job=(
            _extraction_job_out(latest_extraction_job) if latest_extraction_job is not None else None
        ),
        latest_extracted_text=(
            _extracted_text_record_out(latest_extracted_text)
            if latest_extracted_text is not None
            else None
        ),
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _assignment_out(
    row: TeacherAssistAssignment,
    *,
    standard_ids: list[uuid.UUID],
    resource_ids: list[uuid.UUID],
) -> AssignmentOut:
    return AssignmentOut(
        id=row.id,
        tenant_id=row.tenant_id,
        teacher_user_id=row.teacher_user_id,
        school_year_id=row.school_year_id,
        grading_period_id=row.grading_period_id,
        class_id=row.class_id,
        subject_id=row.subject_id,
        title=row.title,
        description=row.description,
        assignment_type=row.assignment_type,
        due_date=row.due_date,
        status=row.status,
        instructions=row.instructions,
        rubric_json=row.rubric_json,
        source_plan_id=row.source_plan_id,
        source_context_json=row.source_context_json,
        standard_ids=standard_ids,
        resource_ids=resource_ids,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _assignment_print_packet_out(row: TeacherAssistAssignmentPrintPacket) -> AssignmentPrintPacketOut:
    return AssignmentPrintPacketOut(
        id=row.id,
        tenant_id=row.tenant_id,
        teacher_user_id=row.teacher_user_id,
        assignment_id=row.assignment_id,
        class_id=row.class_id,
        school_year_id=row.school_year_id,
        grading_period_id=row.grading_period_id,
        subject_id=row.subject_id,
        packet_status=row.packet_status,
        pages_per_student=row.pages_per_student,
        student_count=row.student_count,
        template_type=row.template_type,
        output_format=row.output_format,
        storage_key=row.storage_key,
        total_page_count=row.student_count * row.pages_per_student,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _assignment_print_page_out(row: TeacherAssistAssignmentPrintPage) -> AssignmentPrintPageOut:
    return AssignmentPrintPageOut(
        id=row.id,
        packet_id=row.packet_id,
        assignment_id=row.assignment_id,
        student_number=row.student_number,
        page_number=row.page_number,
        qr_payload_json=row.qr_payload_json,
        qr_token=row.qr_token,
        qr_svg_data_uri=render_qr_svg_data_uri(dict(row.qr_payload_json or {})),
        created_at=row.created_at,
    )


def _assignment_student_work_out(
    row: TeacherAssistStudentWorkSubmission,
    *,
    latest_extraction_job: TeacherAssistExtractionJob | None = None,
    latest_extracted_text: TeacherAssistExtractedTextRecord | None = None,
) -> AssignmentStudentWorkOut:
    return AssignmentStudentWorkOut(
        id=row.id,
        tenant_id=row.tenant_id,
        teacher_user_id=row.teacher_user_id,
        assignment_id=row.assignment_id,
        assignment_print_packet_id=row.assignment_print_packet_id,
        assignment_print_page_id=row.assignment_print_page_id,
        school_year_id=row.school_year_id,
        grading_period_id=row.grading_period_id,
        class_id=row.class_id,
        subject_id=row.subject_id,
        student_number=row.student_number,
        original_filename=row.original_filename,
        mime_type=row.mime_type,
        file_size=row.file_size,
        storage_key=row.storage_key,
        upload_status=row.upload_status,
        processing_status=row.processing_status,
        latest_extraction_job=(
            _extraction_job_out(latest_extraction_job) if latest_extraction_job is not None else None
        ),
        latest_extracted_text=(
            _extracted_text_record_out(latest_extracted_text)
            if latest_extracted_text is not None
            else None
        ),
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _extraction_job_out(row: TeacherAssistExtractionJob) -> TeacherAssistExtractionJobOut:
    return TeacherAssistExtractionJobOut(
        id=row.id,
        artifact_type=row.artifact_type,
        resource_library_item_id=row.resource_library_item_id,
        student_work_submission_id=row.student_work_submission_id,
        assignment_id=row.assignment_id,
        school_year_id=row.school_year_id,
        grading_period_id=row.grading_period_id,
        class_id=row.class_id,
        subject_id=row.subject_id,
        student_number=row.student_number,
        status=row.status,
        progress_percent=row.progress_percent,
        provider_name=row.provider_name,
        provider_model=row.provider_model,
        provider_version=row.provider_version,
        provider_mode=row.provider_mode,
        page_count=row.page_count,
        processing_duration_ms=row.processing_duration_ms,
        estimated_cost_cents=row.estimated_cost_cents,
        error_code=row.error_code,
        error_message=row.error_message,
        error_metadata_json=dict(row.error_metadata_json or {}) or None,
        retry_count=row.retry_count,
        max_retries=row.max_retries,
        parent_extraction_job_id=row.parent_extraction_job_id,
        retry_root_job_id=row.retry_root_job_id,
        attempt_number=row.attempt_number,
        leased_by_worker=row.leased_by_worker,
        lease_expires_at=row.lease_expires_at,
        heartbeat_at=row.heartbeat_at,
        execution_log_json=list(row.execution_log_json or []) or None,
        created_at=row.created_at,
        started_at=row.started_at,
        completed_at=row.completed_at,
        updated_at=row.updated_at,
    )


def _extracted_text_record_out(
    row: TeacherAssistExtractedTextRecord,
) -> TeacherAssistExtractedTextRecordOut:
    metadata = dict(row.metadata_json or {})
    return TeacherAssistExtractedTextRecordOut(
        id=row.id,
        extraction_job_id=row.extraction_job_id,
        artifact_type=row.artifact_type,
        resource_library_item_id=row.resource_library_item_id,
        student_work_submission_id=row.student_work_submission_id,
        assignment_id=row.assignment_id,
        class_id=row.class_id,
        subject_id=row.subject_id,
        student_number=row.student_number,
        preview_text=row.preview_text,
        text_char_count=row.text_char_count,
        pii_flagged=row.pii_flagged,
        redaction_applied=row.redaction_applied,
        review_status=row.review_status,
        provider_confidence_score=row.provider_confidence_score,
        confidence_level=row.confidence_level,
        teacher_corrected_text=row.teacher_corrected_text,
        approved_text=row.approved_text,
        reviewed_at=row.reviewed_at,
        reviewed_by_user_id=row.reviewed_by_user_id,
        source_extraction_job_id=row.source_extraction_job_id,
        teacher_review_notes=str(metadata.get("teacher_review_notes") or "") or None,
        teacher_issue_reason=str(metadata.get("teacher_issue_reason") or "") or None,
        metadata_json=metadata or None,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _extracted_text_detail_out(
    row: TeacherAssistExtractedTextRecord,
) -> TeacherAssistExtractedTextDetailOut:
    base = _extracted_text_record_out(row)
    return TeacherAssistExtractedTextDetailOut(
        **base.model_dump(),
        extracted_text=row.extracted_text,
    )


def _extraction_run_out(
    job: TeacherAssistExtractionJob,
    record: TeacherAssistExtractedTextRecord | None,
) -> TeacherAssistExtractionRunOut:
    return TeacherAssistExtractionRunOut(
        job=_extraction_job_out(job),
        extracted_text=_extracted_text_record_out(record) if record is not None else None,
    )


def _file_download_out(*, settings: Settings, url: str) -> TeacherAssistFileDownloadOut:
    return TeacherAssistFileDownloadOut(
        url=url,
        expires_at=datetime.now(UTC)
        + timedelta(seconds=max(1, settings.teacher_assist_s3_presign_expiration_seconds)),
    )


def _assignment_grading_review_item_out(
    row: TeacherAssistAssignmentGradingReviewItem,
) -> AssignmentGradingReviewItemOut:
    return AssignmentGradingReviewItemOut(
        id=row.id,
        grading_review_id=row.grading_review_id,
        criterion_title=row.criterion_title,
        score_suggestion=row.score_suggestion,
        max_score=row.max_score,
        feedback_summary=row.feedback_summary,
        strengths=list(row.strengths or []),
        improvement_areas=list(row.improvement_areas or []),
        teacher_notes=row.teacher_notes,
        sort_order=row.sort_order,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _assignment_grading_review_out(row: TeacherAssistAssignmentGradingReview) -> AssignmentGradingReviewOut:
    return AssignmentGradingReviewOut(
        id=row.id,
        tenant_id=row.tenant_id,
        teacher_user_id=row.teacher_user_id,
        assignment_id=row.assignment_id,
        student_work_submission_id=row.student_work_submission_id,
        student_number=row.student_number,
        school_year_id=row.school_year_id,
        grading_period_id=row.grading_period_id,
        class_id=row.class_id,
        subject_id=row.subject_id,
        status=row.status,
        review_source=row.review_source,
        provider_name=row.provider_name,
        provider_model=row.provider_model,
        prompt_version=row.prompt_version,
        ai_usage_event_id=row.ai_usage_event_id,
        score_suggestion=row.score_suggestion,
        max_score=row.max_score,
        feedback_summary=row.feedback_summary,
        strengths=list(row.strengths or []),
        improvement_areas=list(row.improvement_areas or []),
        teacher_notes=row.teacher_notes,
        teacher_confirmed_score=row.teacher_confirmed_score,
        teacher_confirmed_feedback=row.teacher_confirmed_feedback,
        items=[_assignment_grading_review_item_out(item) for item in row.items],
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _activity_event_out(row: TeacherAssistActivityEvent) -> TeacherAssistActivityEventOut:
    return TeacherAssistActivityEventOut(
        id=row.id,
        event_category=row.event_category,
        event_type=row.event_type,
        entity_type=row.entity_type,
        entity_id=row.entity_id,
        timestamp=row.event_timestamp,
        summary_text=row.summary_text,
        workflow_id=row.workflow_id,
        school_year_id=row.school_year_id,
        grading_period_id=row.grading_period_id,
        class_id=row.class_id,
        subject_id=row.subject_id,
        details_json=row.details_json,
        created_at=row.created_at,
    )


def _workspace_plan_summary_out(data: dict[str, object]) -> TeacherAssistWorkspacePlanSummaryOut:
    return TeacherAssistWorkspacePlanSummaryOut(**data)


def _workspace_assignment_summary_out(data: dict[str, object]) -> TeacherAssistWorkspaceAssignmentSummaryOut:
    return TeacherAssistWorkspaceAssignmentSummaryOut(**data)


def _workspace_packet_summary_out(data: dict[str, object]) -> TeacherAssistWorkspacePacketSummaryOut:
    return TeacherAssistWorkspacePacketSummaryOut(**data)


def _workspace_submission_summary_out(data: dict[str, object]) -> TeacherAssistWorkspaceSubmissionSummaryOut:
    return TeacherAssistWorkspaceSubmissionSummaryOut(**data)


def _workspace_grading_review_summary_out(
    data: dict[str, object],
) -> TeacherAssistWorkspaceGradingReviewSummaryOut:
    return TeacherAssistWorkspaceGradingReviewSummaryOut(**data)


def _workspace_workflow_summary_out(data: dict[str, object]) -> TeacherAssistWorkspaceWorkflowSummaryOut:
    return TeacherAssistWorkspaceWorkflowSummaryOut(**data)


def _workspace_needs_attention_out(data: dict[str, object]) -> TeacherAssistWorkspaceNeedsAttentionOut:
    return TeacherAssistWorkspaceNeedsAttentionOut(**data)


def _workspace_review_required_item_out(
    data: dict[str, object],
) -> TeacherAssistWorkspaceReviewRequiredItemOut:
    return TeacherAssistWorkspaceReviewRequiredItemOut(**data)


def _workspace_class_out(
    data: dict[str, object], *, subject_ids_by_class: dict[uuid.UUID, list[uuid.UUID]]
) -> TeacherAssistClassWorkspaceOut:
    teacher_class = data["class_row"]
    return TeacherAssistClassWorkspaceOut(
        class_context=_class_out(teacher_class, subject_ids=subject_ids_by_class.get(teacher_class.id, [])),
        active_plans=[_workspace_plan_summary_out(item) for item in data.get("active_plans", [])],
        assignments=[_workspace_assignment_summary_out(item) for item in data.get("assignments", [])],
        pending_grading_reviews=[
            _workspace_grading_review_summary_out(item)
            for item in data.get("pending_grading_reviews", [])
        ],
        recent_submissions=[
            _workspace_submission_summary_out(item) for item in data.get("recent_submissions", [])
        ],
        workflow_summaries=[
            _workspace_workflow_summary_out(item) for item in data.get("workflow_summaries", [])
        ],
        packet_summaries=[_workspace_packet_summary_out(item) for item in data.get("packet_summaries", [])],
        needs_attention_count=int(data.get("needs_attention_count", 0)),
    )


def _planning_draft_out(
    row: TeacherAssistPlanningInputDraft,
    *,
    subject_ids: list[uuid.UUID],
    pacing_item_ids: list[uuid.UUID],
    standard_ids: list[uuid.UUID],
    resource_ids: list[uuid.UUID],
) -> PlanningDraftOut:
    return PlanningDraftOut(
        id=row.id,
        tenant_id=row.tenant_id,
        user_id=row.user_id,
        planning_scope=row.planning_scope,
        school_year_id=row.school_year_id,
        grading_period_id=row.grading_period_id,
        class_id=row.class_id,
        subject_id=row.subject_id,
        subject_ids=subject_ids,
        pacing_item_ids=pacing_item_ids,
        standard_ids=standard_ids,
        title=row.title,
        plan_title=row.title,
        module_title=row.module_title,
        start_date=row.start_date,
        end_date=row.end_date,
        estimated_weeks=row.estimated_weeks,
        instructional_days_count=row.instructional_days_count,
        notes=row.notes,
        status=row.status,
        resource_ids=resource_ids,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _pacing_item_out(
    row: TeacherAssistPacingItem,
    *,
    standard_ids: list[uuid.UUID],
    resource_ids: list[uuid.UUID],
) -> PacingItemOut:
    return PacingItemOut(
        id=row.id,
        pacing_guide_id=row.pacing_guide_id,
        grading_period_id=row.grading_period_id,
        subject_id=row.subject_id,
        week_number=row.week_number,
        day_number=row.day_number,
        instructional_date=row.instructional_date,
        title=row.title,
        instructional_focus=row.instructional_focus,
        objectives=row.objectives,
        notes=row.notes,
        sort_order=row.sort_order,
        standard_ids=standard_ids,
        resource_ids=resource_ids,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _pacing_item_standard_map(
    rows: list,
) -> dict[uuid.UUID, list[uuid.UUID]]:
    mapping: dict[uuid.UUID, list[uuid.UUID]] = {}
    for row in rows:
        mapping.setdefault(row.pacing_item_id, []).append(row.standard_id)
    return mapping


def _pacing_item_resource_map(
    rows: list,
) -> dict[uuid.UUID, list[uuid.UUID]]:
    mapping: dict[uuid.UUID, list[uuid.UUID]] = {}
    for row in rows:
        mapping.setdefault(row.pacing_item_id, []).append(row.resource_library_item_id)
    return mapping


def _planning_draft_resource_map(
    rows: list,
) -> dict[uuid.UUID, list[uuid.UUID]]:
    mapping: dict[uuid.UUID, list[uuid.UUID]] = {}
    for row in rows:
        mapping.setdefault(row.planning_input_draft_id, []).append(row.resource_library_item_id)
    return mapping


def _planning_draft_subject_map(
    rows: list,
) -> dict[uuid.UUID, list[uuid.UUID]]:
    mapping: dict[uuid.UUID, list[uuid.UUID]] = {}
    for row in rows:
        mapping.setdefault(row.planning_input_draft_id, []).append(row.subject_id)
    return mapping


def _planning_draft_pacing_item_map(
    rows: list,
) -> dict[uuid.UUID, list[uuid.UUID]]:
    mapping: dict[uuid.UUID, list[uuid.UUID]] = {}
    for row in rows:
        mapping.setdefault(row.planning_input_draft_id, []).append(row.pacing_item_id)
    return mapping


def _planning_draft_standard_map(
    rows: list,
) -> dict[uuid.UUID, list[uuid.UUID]]:
    mapping: dict[uuid.UUID, list[uuid.UUID]] = {}
    for row in rows:
        mapping.setdefault(row.planning_input_draft_id, []).append(row.standard_id)
    return mapping


def _planning_draft_maps(
    db: DbSession, *, tenant_id: uuid.UUID, user_id: uuid.UUID
) -> tuple[
    dict[uuid.UUID, list[uuid.UUID]],
    dict[uuid.UUID, list[uuid.UUID]],
    dict[uuid.UUID, list[uuid.UUID]],
    dict[uuid.UUID, list[uuid.UUID]],
]:
    return (
        _planning_draft_subject_map(
            list_planning_draft_subject_links(db, tenant_id=tenant_id, user_id=user_id)
        ),
        _planning_draft_pacing_item_map(
            list_planning_draft_pacing_item_links(db, tenant_id=tenant_id, user_id=user_id)
        ),
        _planning_draft_standard_map(
            list_planning_draft_standard_links(db, tenant_id=tenant_id, user_id=user_id)
        ),
        _planning_draft_resource_map(
            list_planning_draft_resources(db, tenant_id=tenant_id, user_id=user_id)
        ),
    )


def _workflow_step_out(row: TeacherAssistWorkflowStep) -> TeacherAssistWorkflowStepOut:
    return TeacherAssistWorkflowStepOut(
        id=row.id,
        workflow_id=row.workflow_id,
        step_name=row.step_name,
        status=row.status,
        metadata_json=row.metadata_json,
        error_message=row.error_message,
        started_at=row.started_at,
        completed_at=row.completed_at,
        created_at=row.created_at,
    )


def _workflow_out(row: TeacherAssistWorkflow) -> TeacherAssistWorkflowOut:
    return TeacherAssistWorkflowOut(
        id=row.id,
        tenant_id=row.tenant_id,
        user_id=row.user_id,
        planning_input_draft_id=row.planning_input_draft_id,
        workflow_type=row.workflow_type,
        status=row.status,
        input_snapshot_json=row.input_snapshot_json,
        output_ref_type=row.output_ref_type,
        output_ref_id=row.output_ref_id,
        error_message=row.error_message,
        last_error_code=row.last_error_code,
        progress_percent=row.progress_percent,
        leased_by_worker=row.leased_by_worker,
        lease_expires_at=row.lease_expires_at,
        heartbeat_at=row.heartbeat_at,
        retry_count=row.retry_count,
        max_retries=row.max_retries,
        timeout_at=row.timeout_at,
        provider_name=row.provider_name,
        provider_model=row.provider_model,
        prompt_version=row.prompt_version,
        input_tokens_total=row.input_tokens_total,
        output_tokens_total=row.output_tokens_total,
        estimated_cost_cents_total=row.estimated_cost_cents_total,
        execution_log_json=list(row.execution_log_json or []),
        created_at=row.created_at,
        started_at=row.started_at,
        completed_at=row.completed_at,
        updated_at=row.updated_at,
    )


def _usage_event_out(row: TeacherAssistAIUsageEvent) -> TeacherAssistAIUsageEventOut:
    return TeacherAssistAIUsageEventOut(
        id=row.id,
        tenant_id=row.tenant_id,
        user_id=row.user_id,
        workflow_id=row.workflow_id,
        provider=row.provider,
        model=row.model,
        feature=row.feature,
        input_tokens=row.input_tokens,
        output_tokens=row.output_tokens,
        estimated_cost_cents=row.estimated_cost_cents,
        metadata_json=row.metadata_json,
        created_at=row.created_at,
    )


def _workflow_detail_out(row: TeacherAssistWorkflow) -> TeacherAssistWorkflowDetailOut:
    return TeacherAssistWorkflowDetailOut(
        **_workflow_out(row).model_dump(),
        steps=[_workflow_step_out(step) for step in row.steps],
        usage_events=[_usage_event_out(event) for event in sorted(row.usage_events, key=lambda event: event.created_at, reverse=True)],
    )


def _weekly_plan_out(
    row: TeacherAssistWeeklyPlan,
    *,
    latest_usage_event: TeacherAssistAIUsageEvent | None = None,
) -> WeeklyPlanOut:
    return WeeklyPlanOut(
        id=row.id,
        tenant_id=row.tenant_id,
        user_id=row.user_id,
        owner_user_id=row.owner_user_id,
        planning_input_draft_id=row.planning_input_draft_id,
        workflow_id=row.workflow_id,
        planning_scope=row.planning_scope,
        title=row.title,
        plan_title=row.title,
        module_title=row.module_title,
        start_date=row.start_date,
        end_date=row.end_date,
        estimated_weeks=row.estimated_weeks,
        instructional_days_count=row.instructional_days_count,
        source_plan_id=row.source_plan_id,
        derived_from_plan_id=row.derived_from_plan_id,
        is_template=row.is_template,
        visibility_scope=row.visibility_scope,
        reuse_status=row.reuse_status,
        school_year_origin_id=row.school_year_origin_id,
        status=row.status,
        content_json=row.content_json,
        source_context_json=row.source_context_json,
        current_version_number=max((version.version_number for version in row.versions), default=1),
        latest_usage_event=_usage_event_out(latest_usage_event) if latest_usage_event is not None else None,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _instructional_plan_library_item_out(
    row: TeacherAssistWeeklyPlan,
    *,
    current_user_id: uuid.UUID,
) -> InstructionalPlanLibraryItemOut:
    metadata = _plan_source_metadata(row)
    owner_name = None
    if row.owner_user is not None:
        owner_name = row.owner_user.full_name or row.owner_user.email
    return InstructionalPlanLibraryItemOut(
        id=row.id,
        tenant_id=row.tenant_id,
        user_id=row.user_id,
        owner_user_id=row.owner_user_id,
        owner_name=owner_name,
        is_owner=row.owner_user_id == current_user_id,
        planning_input_draft_id=row.planning_input_draft_id,
        workflow_id=row.workflow_id,
        planning_scope=row.planning_scope,
        title=row.title,
        plan_title=row.content_json.get("plan_title") or row.title,
        module_title=row.module_title,
        start_date=row.start_date,
        end_date=row.end_date,
        estimated_weeks=row.estimated_weeks,
        instructional_days_count=row.instructional_days_count,
        source_plan_id=row.source_plan_id,
        derived_from_plan_id=row.derived_from_plan_id,
        is_template=row.is_template,
        visibility_scope=row.visibility_scope,
        reuse_status=row.reuse_status,
        school_year_origin_id=row.school_year_origin_id,
        source_school_year_id=uuid.UUID(metadata["source_school_year_id"])
        if metadata.get("source_school_year_id")
        else None,
        source_school_year_title=metadata.get("source_school_year_title"),
        subject_ids=[uuid.UUID(subject_id) for subject_id in metadata.get("subject_ids", [])],
        subject_names=metadata.get("subject_names", []),
        class_id=uuid.UUID(metadata["class_id"]) if metadata.get("class_id") else None,
        class_name=metadata.get("class_name"),
        grading_period_id=uuid.UUID(metadata["grading_period_id"])
        if metadata.get("grading_period_id")
        else None,
        grading_period_title=metadata.get("grading_period_title"),
        status=row.status,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _weekly_plan_version_out(row: TeacherAssistWeeklyPlanVersion) -> WeeklyPlanVersionOut:
    return WeeklyPlanVersionOut(
        id=row.id,
        weekly_plan_id=row.weekly_plan_id,
        version_number=row.version_number,
        content_json=row.content_json,
        source_context_json=row.source_context_json,
        created_by_user_id=row.created_by_user_id,
        created_at=row.created_at,
        change_reason=row.change_reason,
    )


def _latest_usage_event_by_plan_id(
    db: DbSession,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    plans: list[TeacherAssistWeeklyPlan],
) -> dict[uuid.UUID, TeacherAssistAIUsageEvent]:
    workflow_ids = [plan.workflow_id for plan in plans if plan.workflow_id is not None]
    usage_events = list_workflow_usage_events(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        workflow_ids=workflow_ids,
    )
    latest_events: dict[uuid.UUID, TeacherAssistAIUsageEvent] = {}
    for plan in plans:
        if plan.workflow_id is not None and usage_events.get(plan.workflow_id):
            latest_events[plan.id] = usage_events[plan.workflow_id][0]
    unresolved_plan_ids = {plan.id for plan in plans if plan.id not in latest_events}
    if not unresolved_plan_ids:
        return latest_events
    for event in list_teacher_assist_usage_events(db, tenant_id=tenant_id, user_id=user_id):
        metadata = dict(event.metadata_json or {})
        plan_id_value = metadata.get("weekly_plan_id")
        if not plan_id_value:
            continue
        try:
            plan_id = uuid.UUID(str(plan_id_value))
        except (TypeError, ValueError):
            continue
        if plan_id in unresolved_plan_ids:
            latest_events[plan_id] = event
            unresolved_plan_ids.remove(plan_id)
        if not unresolved_plan_ids:
            break
    return latest_events


def _teacher_assist_tenant_id(db: DbSession, user: CurrentUser) -> uuid.UUID:
    try:
        return teacher_assist_context_for_user(db, user).tenant_id
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc


@router.get("/options", response_model=TeacherAssistOptionsOut)
def read_teacher_assist_options(user: CurrentUser, db: DbSession) -> TeacherAssistOptionsOut:
    _teacher_assist_tenant_id(db, user)
    return TeacherAssistOptionsOut(
        grading_period_types=list(GRADING_PERIOD_TYPES),
        standard_types=list(STANDARD_TYPES),
        resource_types=list(RESOURCE_TYPES),
        assignment_types=list(ASSIGNMENT_TYPES),
        assignment_statuses=list(ASSIGNMENT_STATUSES),
        assignment_print_packet_statuses=list(ASSIGNMENT_PRINT_PACKET_STATUSES),
        assignment_print_template_types=list(ASSIGNMENT_PRINT_TEMPLATE_TYPES),
        assignment_print_output_formats=list(ASSIGNMENT_PRINT_OUTPUT_FORMATS),
        assignment_student_work_upload_statuses=list(ASSIGNMENT_STUDENT_WORK_UPLOAD_STATUSES),
        assignment_student_work_processing_statuses=list(ASSIGNMENT_STUDENT_WORK_PROCESSING_STATUSES),
        assignment_grading_review_statuses=list(ASSIGNMENT_GRADING_REVIEW_STATUSES),
        assignment_grading_review_sources=list(ASSIGNMENT_GRADING_REVIEW_SOURCES),
        extraction_artifact_types=list(TEACHER_ASSIST_EXTRACTION_ARTIFACT_TYPES),
        extraction_job_statuses=list(TEACHER_ASSIST_EXTRACTION_JOB_STATUSES),
        extraction_review_statuses=list(EXTRACTION_REVIEW_STATUSES),
        extraction_confidence_levels=list(EXTRACTION_CONFIDENCE_LEVELS),
        planning_draft_statuses=list(PLANNING_DRAFT_STATUSES),
        planning_scopes=list(PLANNING_SCOPES),
        supported_grade_levels=list(SUPPORTED_GRADE_LEVELS),
    )


@router.get("/workspace", response_model=TeacherAssistWorkspaceOut)
def read_teacher_assist_workspace(
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> TeacherAssistWorkspaceOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    payload = get_teacher_assist_workspace(
        db,
        settings=settings,
        tenant_id=tenant_id,
        user_id=user.id,
    )
    class_subjects = _class_subject_map(list_class_subjects(db, tenant_id=tenant_id))
    return TeacherAssistWorkspaceOut(
        current_school_year=_school_year_out(payload["current_school_year"])
        if payload.get("current_school_year") is not None
        else None,
        active_grading_period=_grading_period_out(payload["active_grading_period"])
        if payload.get("active_grading_period") is not None
        else None,
        today_summary=TeacherAssistWorkspaceTodaySummaryOut(**dict(payload.get("today_summary") or {})),
        class_workspaces=[
            _workspace_class_out(item, subject_ids_by_class=class_subjects)
            for item in payload.get("class_workspaces", [])
        ],
        needs_attention=[
            _workspace_needs_attention_out(item) for item in payload.get("needs_attention", [])
        ],
        recent_activity=[_activity_event_out(item) for item in payload.get("recent_activity", [])],
        active_workflows=[
            _workspace_workflow_summary_out(item) for item in payload.get("active_workflows", [])
        ],
        review_required_items=[
            _workspace_review_required_item_out(item)
            for item in payload.get("review_required_items", [])
        ],
        workspace_stats=TeacherAssistWorkspaceStatsOut(**dict(payload.get("workspace_stats") or {})),
    )


@router.get("/profile", response_model=TeacherProfileOut)
def read_teacher_profile(user: CurrentUser, db: DbSession) -> TeacherProfileOut:
    _teacher_assist_tenant_id(db, user)
    return _profile_out(get_teacher_profile(db, user_id=user.id))


@router.put("/profile", response_model=TeacherProfileOut)
def save_teacher_profile(
    body: TeacherProfileUpsert,
    user: CurrentUser,
    db: DbSession,
) -> TeacherProfileOut:
    _teacher_assist_tenant_id(db, user)
    try:
        row = upsert_teacher_profile(
            db,
            user=user,
            preferred_grade_level=body.preferred_grade_level,
            default_student_count=body.default_student_count,
            preferred_grading_period_type=body.preferred_grading_period_type,
            timezone=body.timezone,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _profile_out(row)


@router.get("/school-years", response_model=list[SchoolYearOut])
def read_school_years(user: CurrentUser, db: DbSession) -> list[SchoolYearOut]:
    tenant_id = _teacher_assist_tenant_id(db, user)
    return [_school_year_out(row) for row in list_school_years(db, tenant_id=tenant_id)]


@router.post("/school-years", response_model=SchoolYearOut, status_code=201)
def create_teacher_school_year(
    body: SchoolYearCreate,
    user: CurrentUser,
    db: DbSession,
) -> SchoolYearOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = create_school_year(
            db,
            tenant_id=tenant_id,
            title=body.title,
            start_date=body.start_date,
            end_date=body.end_date,
            is_active=body.is_active,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _school_year_out(row)


@router.put("/school-years/{school_year_id}", response_model=SchoolYearOut)
def update_teacher_school_year(
    school_year_id: uuid.UUID,
    body: SchoolYearCreate,
    user: CurrentUser,
    db: DbSession,
) -> SchoolYearOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = update_school_year(
            db,
            tenant_id=tenant_id,
            school_year_id=school_year_id,
            title=body.title,
            start_date=body.start_date,
            end_date=body.end_date,
            is_active=body.is_active,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _school_year_out(row)


@router.get("/grading-periods", response_model=list[GradingPeriodOut])
def read_grading_periods(
    user: CurrentUser,
    db: DbSession,
    school_year_id: uuid.UUID | None = Query(default=None),
) -> list[GradingPeriodOut]:
    tenant_id = _teacher_assist_tenant_id(db, user)
    return [
        _grading_period_out(row)
        for row in list_grading_periods(db, tenant_id=tenant_id, school_year_id=school_year_id)
    ]


@router.post("/grading-periods", response_model=GradingPeriodOut, status_code=201)
def create_teacher_grading_period(
    body: GradingPeriodCreate,
    user: CurrentUser,
    db: DbSession,
) -> GradingPeriodOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = create_grading_period(
            db,
            tenant_id=tenant_id,
            school_year_id=body.school_year_id,
            title=body.title,
            grading_period_type=body.grading_period_type,
            start_date=body.start_date,
            end_date=body.end_date,
            sort_order=body.sort_order,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _grading_period_out(row)


@router.put("/grading-periods/{grading_period_id}", response_model=GradingPeriodOut)
def update_teacher_grading_period(
    grading_period_id: uuid.UUID,
    body: GradingPeriodCreate,
    user: CurrentUser,
    db: DbSession,
) -> GradingPeriodOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = update_grading_period(
            db,
            tenant_id=tenant_id,
            grading_period_id=grading_period_id,
            school_year_id=body.school_year_id,
            title=body.title,
            grading_period_type=body.grading_period_type,
            start_date=body.start_date,
            end_date=body.end_date,
            sort_order=body.sort_order,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _grading_period_out(row)


@router.get("/subjects", response_model=list[SubjectOut])
def read_subjects(user: CurrentUser, db: DbSession) -> list[SubjectOut]:
    tenant_id = _teacher_assist_tenant_id(db, user)
    return [_subject_out(row) for row in list_subjects(db, tenant_id=tenant_id)]


@router.post("/subjects", response_model=SubjectOut, status_code=201)
def create_teacher_subject(body: SubjectCreate, user: CurrentUser, db: DbSession) -> SubjectOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    row = create_subject(db, tenant_id=tenant_id, code=body.code, name=body.name)
    return _subject_out(row)


@router.get("/classes", response_model=list[ClassOut])
def read_classes(user: CurrentUser, db: DbSession) -> list[ClassOut]:
    tenant_id = _teacher_assist_tenant_id(db, user)
    class_subjects = _class_subject_map(list_class_subjects(db, tenant_id=tenant_id))
    return [
        _class_out(row, subject_ids=class_subjects.get(row.id, []))
        for row in list_classes(db, tenant_id=tenant_id)
    ]


@router.post("/classes", response_model=ClassOut, status_code=201)
def create_teacher_class(body: ClassCreate, user: CurrentUser, db: DbSession) -> ClassOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = create_class(
            db,
            tenant_id=tenant_id,
            school_year_id=body.school_year_id,
            name=body.name,
            grade_level=body.grade_level,
            student_count=body.student_count,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _class_out(row, subject_ids=[])


@router.put("/classes/{class_id}", response_model=ClassOut)
def update_teacher_class(
    class_id: uuid.UUID,
    body: ClassCreate,
    user: CurrentUser,
    db: DbSession,
) -> ClassOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = update_class(
            db,
            tenant_id=tenant_id,
            class_id=class_id,
            school_year_id=body.school_year_id,
            name=body.name,
            grade_level=body.grade_level,
            student_count=body.student_count,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    class_subjects = _class_subject_map(list_class_subjects(db, tenant_id=tenant_id))
    return _class_out(row, subject_ids=class_subjects.get(row.id, []))


@router.post("/class-subjects", response_model=ClassSubjectOut, status_code=201)
def create_teacher_class_subject(
    body: ClassSubjectCreate,
    user: CurrentUser,
    db: DbSession,
) -> ClassSubjectOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = attach_class_subject(
            db,
            tenant_id=tenant_id,
            class_id=body.class_id,
            subject_id=body.subject_id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return ClassSubjectOut(
        id=row.id,
        class_id=row.class_id,
        subject_id=row.subject_id,
        created_at=row.created_at,
    )


@router.get("/standards", response_model=list[StandardOut])
def read_standards(user: CurrentUser, db: DbSession) -> list[StandardOut]:
    tenant_id = _teacher_assist_tenant_id(db, user)
    return [_standard_out(row) for row in list_standards(db, tenant_id=tenant_id)]


@router.post("/standards", response_model=StandardOut, status_code=201)
def create_teacher_standard(body: StandardCreate, user: CurrentUser, db: DbSession) -> StandardOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = create_standard(
            db,
            tenant_id=tenant_id,
            subject_id=body.subject_id,
            standard_type=body.standard_type,
            code=body.code,
            description=body.description,
            grade_level=body.grade_level,
            school_year_id=body.school_year_id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _standard_out(row)


@router.get("/pacing-guides", response_model=list[PacingGuideOut])
def read_pacing_guides(user: CurrentUser, db: DbSession) -> list[PacingGuideOut]:
    tenant_id = _teacher_assist_tenant_id(db, user)
    guides = list_pacing_guides(db, tenant_id=tenant_id)
    item_counts = {
        guide.id: len(list_pacing_items(db, tenant_id=tenant_id, pacing_guide_id=guide.id))
        for guide in guides
    }
    return [_pacing_guide_out(row, item_count=item_counts.get(row.id, 0)) for row in guides]


@router.post("/pacing-guides", response_model=PacingGuideOut, status_code=201)
def create_teacher_pacing_guide(
    body: PacingGuideCreate,
    user: CurrentUser,
    db: DbSession,
) -> PacingGuideOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = create_pacing_guide(
            db,
            tenant_id=tenant_id,
            created_by_user=user,
            school_year_id=body.school_year_id,
            title=body.title,
            description=body.description,
            grade_level=body.grade_level,
            subject_id=body.subject_id,
            is_shared=body.is_shared,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _pacing_guide_out(row, item_count=0)


@router.put("/pacing-guides/{pacing_guide_id}", response_model=PacingGuideOut)
def update_teacher_pacing_guide(
    pacing_guide_id: uuid.UUID,
    body: PacingGuideCreate,
    user: CurrentUser,
    db: DbSession,
) -> PacingGuideOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = update_pacing_guide(
            db,
            tenant_id=tenant_id,
            pacing_guide_id=pacing_guide_id,
            school_year_id=body.school_year_id,
            title=body.title,
            description=body.description,
            grade_level=body.grade_level,
            subject_id=body.subject_id,
            is_shared=body.is_shared,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    item_count = len(list_pacing_items(db, tenant_id=tenant_id, pacing_guide_id=row.id))
    return _pacing_guide_out(row, item_count=item_count)


@router.get("/pacing-guides/{pacing_guide_id}/items", response_model=list[PacingItemOut])
def read_pacing_guide_items(
    pacing_guide_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> list[PacingItemOut]:
    tenant_id = _teacher_assist_tenant_id(db, user)
    get_pacing_guide_or_404(db, tenant_id=tenant_id, pacing_guide_id=pacing_guide_id)
    standards_map = _pacing_item_standard_map(
        list_pacing_item_standards(db, tenant_id=tenant_id, pacing_guide_id=pacing_guide_id)
    )
    resources_map = _pacing_item_resource_map(
        list_pacing_item_resources(db, tenant_id=tenant_id, pacing_guide_id=pacing_guide_id)
    )
    return [
        _pacing_item_out(
            row,
            standard_ids=standards_map.get(row.id, []),
            resource_ids=resources_map.get(row.id, []),
        )
        for row in list_pacing_items(db, tenant_id=tenant_id, pacing_guide_id=pacing_guide_id)
    ]


@router.post("/pacing-guides/{pacing_guide_id}/items", response_model=PacingItemOut, status_code=201)
def create_teacher_pacing_item(
    pacing_guide_id: uuid.UUID,
    body: PacingItemCreate,
    user: CurrentUser,
    db: DbSession,
) -> PacingItemOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = create_pacing_item(
            db,
            tenant_id=tenant_id,
            pacing_guide_id=pacing_guide_id,
            grading_period_id=body.grading_period_id,
            subject_id=body.subject_id,
            week_number=body.week_number,
            day_number=body.day_number,
            instructional_date=body.instructional_date,
            title=body.title,
            instructional_focus=body.instructional_focus,
            objectives=body.objectives,
            notes=body.notes,
            sort_order=body.sort_order,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _pacing_item_out(row, standard_ids=[], resource_ids=[])


@router.put("/pacing-items/{pacing_item_id}", response_model=PacingItemOut)
def update_teacher_pacing_item(
    pacing_item_id: uuid.UUID,
    body: PacingItemCreate,
    user: CurrentUser,
    db: DbSession,
) -> PacingItemOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = update_pacing_item(
            db,
            tenant_id=tenant_id,
            pacing_item_id=pacing_item_id,
            grading_period_id=body.grading_period_id,
            subject_id=body.subject_id,
            week_number=body.week_number,
            day_number=body.day_number,
            instructional_date=body.instructional_date,
            title=body.title,
            instructional_focus=body.instructional_focus,
            objectives=body.objectives,
            notes=body.notes,
            sort_order=body.sort_order,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    standards_map = _pacing_item_standard_map(list_pacing_item_standards(db, tenant_id=tenant_id))
    resources_map = _pacing_item_resource_map(list_pacing_item_resources(db, tenant_id=tenant_id))
    return _pacing_item_out(
        row,
        standard_ids=standards_map.get(row.id, []),
        resource_ids=resources_map.get(row.id, []),
    )


@router.post("/pacing-items/{pacing_item_id}/standards", response_model=PacingItemOut)
def create_teacher_pacing_item_standard(
    pacing_item_id: uuid.UUID,
    body: PacingItemStandardCreate,
    user: CurrentUser,
    db: DbSession,
) -> PacingItemOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        attach_pacing_item_standard(
            db,
            tenant_id=tenant_id,
            pacing_item_id=pacing_item_id,
            standard_id=body.standard_id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    row = get_pacing_item_or_404(db, tenant_id=tenant_id, pacing_item_id=pacing_item_id)
    standards_map = _pacing_item_standard_map(list_pacing_item_standards(db, tenant_id=tenant_id))
    resources_map = _pacing_item_resource_map(list_pacing_item_resources(db, tenant_id=tenant_id))
    return _pacing_item_out(
        row,
        standard_ids=standards_map.get(row.id, []),
        resource_ids=resources_map.get(row.id, []),
    )


@router.post("/pacing-items/{pacing_item_id}/resources", response_model=PacingItemOut)
def create_teacher_pacing_item_resource(
    pacing_item_id: uuid.UUID,
    body: PacingItemResourceCreate,
    user: CurrentUser,
    db: DbSession,
) -> PacingItemOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        attach_pacing_item_resource(
            db,
            tenant_id=tenant_id,
            pacing_item_id=pacing_item_id,
            resource_library_item_id=body.resource_library_item_id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    row = get_pacing_item_or_404(db, tenant_id=tenant_id, pacing_item_id=pacing_item_id)
    standards_map = _pacing_item_standard_map(list_pacing_item_standards(db, tenant_id=tenant_id))
    resources_map = _pacing_item_resource_map(list_pacing_item_resources(db, tenant_id=tenant_id))
    return _pacing_item_out(
        row,
        standard_ids=standards_map.get(row.id, []),
        resource_ids=resources_map.get(row.id, []),
    )


@router.get("/resources", response_model=list[ResourceOut])
def read_resources(user: CurrentUser, db: DbSession) -> list[ResourceOut]:
    tenant_id = _teacher_assist_tenant_id(db, user)
    counts = list_resource_link_counts(db, tenant_id=tenant_id)
    rows = list_resources(db, tenant_id=tenant_id)
    latest_state = latest_extraction_state_for_resources(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        resource_ids=[row.id for row in rows],
    )
    return [
        _resource_out(
            row,
            linked_pacing_items_count=counts.pacing_items.get(row.id, 0),
            linked_planning_drafts_count=counts.planning_drafts.get(row.id, 0),
            latest_extraction_job=latest_state.get(row.id, (None, None))[0],
            latest_extracted_text=latest_state.get(row.id, (None, None))[1],
        )
        for row in rows
    ]


@router.post("/resources/upload", response_model=ResourceOut, status_code=201)
async def upload_teacher_resource(
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
    file: UploadFile = File(...),
    title: str | None = Form(default=None),
    description: str | None = Form(default=None),
) -> ResourceOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        stored = await store_teacher_assist_upload(
            settings,
            tenant_id=tenant_id,
            upload=file,
            area="resources",
        )
        row = create_uploaded_resource(
            db,
            tenant_id=tenant_id,
            uploaded_by_user=user,
            title=title,
            description=description,
            resource_type=stored.resource_type,
            storage_key=stored.storage_key,
            original_filename=stored.original_filename,
            mime_type=stored.mime_type,
            file_size=stored.file_size,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _resource_out(row, linked_pacing_items_count=0, linked_planning_drafts_count=0)


@router.post("/resources/link", response_model=ResourceOut, status_code=201)
def create_teacher_link_resource(
    body: ResourceLinkCreate,
    user: CurrentUser,
    db: DbSession,
) -> ResourceOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = create_link_resource(
            db,
            tenant_id=tenant_id,
            uploaded_by_user=user,
            title=body.title,
            description=body.description,
            external_url=body.external_url,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _resource_out(row, linked_pacing_items_count=0, linked_planning_drafts_count=0)


@router.get("/resources/{resource_id}", response_model=ResourceOut)
def read_resource(
    resource_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> ResourceOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    counts = list_resource_link_counts(db, tenant_id=tenant_id)
    try:
        row = get_resource_or_404(db, tenant_id=tenant_id, resource_id=resource_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return _resource_out(
        row,
        linked_pacing_items_count=counts.pacing_items.get(row.id, 0),
        linked_planning_drafts_count=counts.planning_drafts.get(row.id, 0),
        latest_extraction_job=row.extraction_jobs[0] if row.extraction_jobs else None,
        latest_extracted_text=row.extracted_text_records[0] if row.extracted_text_records else None,
    )


@router.post("/resources/{resource_id}/extraction-jobs", response_model=TeacherAssistExtractionJobOut, status_code=201)
def create_teacher_resource_extraction_job(
    resource_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> TeacherAssistExtractionJobOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = create_resource_extraction_job(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            resource_id=resource_id,
            settings=settings,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _extraction_job_out(row)


@router.post("/resources/{resource_id}/extraction-jobs/retry", response_model=TeacherAssistExtractionJobOut, status_code=201)
def retry_teacher_resource_extraction_job(
    resource_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> TeacherAssistExtractionJobOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = retry_latest_eligible_extraction_for_resource(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            resource_id=resource_id,
            settings=settings,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _extraction_job_out(row)


@router.get("/resources/{resource_id}/extractions", response_model=list[TeacherAssistExtractionRunOut])
def read_teacher_resource_extractions(
    resource_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> list[TeacherAssistExtractionRunOut]:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        rows = list_resource_extraction_runs(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            resource_id=resource_id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return [_extraction_run_out(job, record) for job, record in rows]


@router.get("/resources/{resource_id}/download-url", response_model=TeacherAssistFileDownloadOut)
def read_resource_download_url(
    resource_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> TeacherAssistFileDownloadOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = get_resource_or_404(db, tenant_id=tenant_id, resource_id=resource_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    if not row.storage_key or not row.original_filename or not row.mime_type:
        raise HTTPException(status_code=400, detail="Resource does not have a stored file to download")
    if not teacher_assist_file_exists(settings, storage_key=row.storage_key):
        raise HTTPException(status_code=404, detail="Stored resource file not found")
    return _file_download_out(
        settings=settings,
        url=get_teacher_assist_download_url(
            settings,
            storage_key=row.storage_key,
            original_filename=row.original_filename,
            mime_type=row.mime_type,
        ),
    )


@router.get("/storage/local-download")
def download_teacher_assist_local_file(
    token: str = Query(..., min_length=1),
    settings: Settings = Depends(settings_dep),
) -> Response:
    try:
        download_request = resolve_teacher_assist_local_download(settings, token=token)
        stream = open_teacher_assist_stream(settings, storage_key=download_request.storage_key)
        try:
            contents = stream.read()
        finally:
            stream.close()
    except (ValueError, FileNotFoundError) as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return Response(
        content=contents,
        media_type=download_request.mime_type,
        headers={
            "Content-Disposition": build_content_disposition(download_request.original_filename),
            "Cache-Control": "private, max-age=60",
        },
    )


@router.get("/assignments", response_model=list[AssignmentOut])
def read_assignments(
    user: CurrentUser,
    db: DbSession,
    school_year_id: uuid.UUID | None = Query(default=None),
    grading_period_id: uuid.UUID | None = Query(default=None),
    class_id: uuid.UUID | None = Query(default=None),
    subject_id: uuid.UUID | None = Query(default=None),
    status: str | None = Query(default=None),
    assignment_type: str | None = Query(default=None),
    q: str | None = Query(default=None),
) -> list[AssignmentOut]:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        rows = list_assignments(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            school_year_id=school_year_id,
            grading_period_id=grading_period_id,
            class_id=class_id,
            subject_id=subject_id,
            status=status,
            assignment_type=assignment_type,
            q=q,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    assignment_ids = [row.id for row in rows]
    standards_map = _assignment_standard_map(
        list_assignment_standards(db, tenant_id=tenant_id, user_id=user.id, assignment_ids=assignment_ids)
    )
    resources_map = _assignment_resource_map(
        list_assignment_resources(db, tenant_id=tenant_id, user_id=user.id, assignment_ids=assignment_ids)
    )
    return [
        _assignment_out(
            row,
            standard_ids=standards_map.get(row.id, []),
            resource_ids=resources_map.get(row.id, []),
        )
        for row in rows
    ]


@router.post("/assignments", response_model=AssignmentOut, status_code=201)
def create_teacher_assignment(
    body: AssignmentCreate,
    user: CurrentUser,
    db: DbSession,
) -> AssignmentOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = create_assignment(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            school_year_id=body.school_year_id,
            grading_period_id=body.grading_period_id,
            class_id=body.class_id,
            subject_id=body.subject_id,
            title=body.title,
            description=body.description,
            assignment_type=body.assignment_type,
            due_date=body.due_date,
            status=body.status,
            instructions=body.instructions,
            rubric_json=body.rubric_json,
            source_plan_id=body.source_plan_id,
            source_context_json=body.source_context_json,
            standard_ids=body.standard_ids,
            resource_ids=body.resource_ids,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    standards_map = _assignment_standard_map(
        list_assignment_standards(db, tenant_id=tenant_id, user_id=user.id, assignment_ids=[row.id])
    )
    resources_map = _assignment_resource_map(
        list_assignment_resources(db, tenant_id=tenant_id, user_id=user.id, assignment_ids=[row.id])
    )
    return _assignment_out(
        row,
        standard_ids=standards_map.get(row.id, []),
        resource_ids=resources_map.get(row.id, []),
    )


@router.get("/assignments/{assignment_id}", response_model=AssignmentOut)
def read_assignment(
    assignment_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> AssignmentOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = get_assignment_or_404(db, tenant_id=tenant_id, user_id=user.id, assignment_id=assignment_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    standards_map = _assignment_standard_map(
        list_assignment_standards(db, tenant_id=tenant_id, user_id=user.id, assignment_ids=[row.id])
    )
    resources_map = _assignment_resource_map(
        list_assignment_resources(db, tenant_id=tenant_id, user_id=user.id, assignment_ids=[row.id])
    )
    return _assignment_out(
        row,
        standard_ids=standards_map.get(row.id, []),
        resource_ids=resources_map.get(row.id, []),
    )


@router.put("/assignments/{assignment_id}", response_model=AssignmentOut)
def update_teacher_assignment(
    assignment_id: uuid.UUID,
    body: AssignmentCreate,
    user: CurrentUser,
    db: DbSession,
) -> AssignmentOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = update_assignment(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            assignment_id=assignment_id,
            school_year_id=body.school_year_id,
            grading_period_id=body.grading_period_id,
            class_id=body.class_id,
            subject_id=body.subject_id,
            title=body.title,
            description=body.description,
            assignment_type=body.assignment_type,
            due_date=body.due_date,
            status=body.status,
            instructions=body.instructions,
            rubric_json=body.rubric_json,
            source_plan_id=body.source_plan_id,
            source_context_json=body.source_context_json,
            standard_ids=body.standard_ids,
            resource_ids=body.resource_ids,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    standards_map = _assignment_standard_map(
        list_assignment_standards(db, tenant_id=tenant_id, user_id=user.id, assignment_ids=[row.id])
    )
    resources_map = _assignment_resource_map(
        list_assignment_resources(db, tenant_id=tenant_id, user_id=user.id, assignment_ids=[row.id])
    )
    return _assignment_out(
        row,
        standard_ids=standards_map.get(row.id, []),
        resource_ids=resources_map.get(row.id, []),
    )


@router.patch("/assignments/{assignment_id}/status", response_model=AssignmentOut)
def update_teacher_assignment_status(
    assignment_id: uuid.UUID,
    body: AssignmentStatusUpdate,
    user: CurrentUser,
    db: DbSession,
) -> AssignmentOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = update_assignment_status(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            assignment_id=assignment_id,
            status=body.status,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    standards_map = _assignment_standard_map(
        list_assignment_standards(db, tenant_id=tenant_id, user_id=user.id, assignment_ids=[row.id])
    )
    resources_map = _assignment_resource_map(
        list_assignment_resources(db, tenant_id=tenant_id, user_id=user.id, assignment_ids=[row.id])
    )
    return _assignment_out(
        row,
        standard_ids=standards_map.get(row.id, []),
        resource_ids=resources_map.get(row.id, []),
    )


@router.post("/assignments/{assignment_id}/standards", response_model=AssignmentOut)
def create_teacher_assignment_standard(
    assignment_id: uuid.UUID,
    body: AssignmentStandardCreate,
    user: CurrentUser,
    db: DbSession,
) -> AssignmentOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = attach_assignment_standard(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            assignment_id=assignment_id,
            standard_id=body.standard_id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    standards_map = _assignment_standard_map(
        list_assignment_standards(db, tenant_id=tenant_id, user_id=user.id, assignment_ids=[row.id])
    )
    resources_map = _assignment_resource_map(
        list_assignment_resources(db, tenant_id=tenant_id, user_id=user.id, assignment_ids=[row.id])
    )
    return _assignment_out(
        row,
        standard_ids=standards_map.get(row.id, []),
        resource_ids=resources_map.get(row.id, []),
    )


@router.post("/assignments/{assignment_id}/resources", response_model=AssignmentOut)
def create_teacher_assignment_resource(
    assignment_id: uuid.UUID,
    body: AssignmentResourceCreate,
    user: CurrentUser,
    db: DbSession,
) -> AssignmentOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = attach_assignment_resource(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            assignment_id=assignment_id,
            resource_library_item_id=body.resource_library_item_id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    standards_map = _assignment_standard_map(
        list_assignment_standards(db, tenant_id=tenant_id, user_id=user.id, assignment_ids=[row.id])
    )
    resources_map = _assignment_resource_map(
        list_assignment_resources(db, tenant_id=tenant_id, user_id=user.id, assignment_ids=[row.id])
    )
    return _assignment_out(
        row,
        standard_ids=standards_map.get(row.id, []),
        resource_ids=resources_map.get(row.id, []),
    )


@router.post("/assignments/{assignment_id}/print-packets", response_model=AssignmentPrintPacketOut, status_code=201)
def create_teacher_assignment_print_packet(
    assignment_id: uuid.UUID,
    body: AssignmentPrintPacketCreate,
    user: CurrentUser,
    db: DbSession,
) -> AssignmentPrintPacketOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = create_assignment_print_packet(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            assignment_id=assignment_id,
            pages_per_student=body.pages_per_student,
            template_type=body.template_type,
            output_format=body.output_format,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _assignment_print_packet_out(row)


@router.get("/assignments/{assignment_id}/print-packets", response_model=list[AssignmentPrintPacketOut])
def read_teacher_assignment_print_packets(
    assignment_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> list[AssignmentPrintPacketOut]:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        rows = list_assignment_print_packets(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            assignment_id=assignment_id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return [_assignment_print_packet_out(row) for row in rows]


@router.get("/print-packets/{packet_id}", response_model=AssignmentPrintPacketOut)
def read_teacher_assignment_print_packet(
    packet_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> AssignmentPrintPacketOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = get_print_packet_or_404(db, tenant_id=tenant_id, user_id=user.id, packet_id=packet_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return _assignment_print_packet_out(row)


@router.get("/print-packets/{packet_id}/pages", response_model=list[AssignmentPrintPageOut])
def read_teacher_assignment_print_packet_pages(
    packet_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> list[AssignmentPrintPageOut]:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        rows = list_print_packet_pages(db, tenant_id=tenant_id, user_id=user.id, packet_id=packet_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return [_assignment_print_page_out(row) for row in rows]


@router.get("/assignments/{assignment_id}/student-work", response_model=list[AssignmentStudentWorkOut])
def read_teacher_assignment_student_work(
    assignment_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> list[AssignmentStudentWorkOut]:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        rows = list_assignment_student_work_submissions(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            assignment_id=assignment_id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    latest_state = latest_extraction_state_for_submissions(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        submission_ids=[row.id for row in rows],
    )
    return [
        _assignment_student_work_out(
            row,
            latest_extraction_job=latest_state.get(row.id, (None, None))[0],
            latest_extracted_text=latest_state.get(row.id, (None, None))[1],
        )
        for row in rows
    ]


@router.post("/assignments/{assignment_id}/student-work", response_model=AssignmentStudentWorkOut, status_code=201)
async def upload_teacher_assignment_student_work(
    assignment_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
    file: UploadFile = File(...),
    student_number: int = Form(...),
    assignment_print_packet_id: uuid.UUID | None = Form(default=None),
    assignment_print_page_id: uuid.UUID | None = Form(default=None),
) -> AssignmentStudentWorkOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        stored = await store_teacher_assist_upload(
            settings,
            tenant_id=tenant_id,
            upload=file,
            area="student-work",
        )
        row = create_student_work_submission(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            assignment_id=assignment_id,
            student_number=student_number,
            original_filename=stored.original_filename,
            mime_type=stored.mime_type,
            file_size=stored.file_size,
            storage_key=stored.storage_key,
            assignment_print_packet_id=assignment_print_packet_id,
            assignment_print_page_id=assignment_print_page_id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _assignment_student_work_out(
        row,
        latest_extraction_job=row.extraction_jobs[0] if row.extraction_jobs else None,
        latest_extracted_text=row.extracted_text_records[0] if row.extracted_text_records else None,
    )


@router.get("/student-work/{submission_id}", response_model=AssignmentStudentWorkOut)
def read_teacher_assignment_student_work_submission(
    submission_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> AssignmentStudentWorkOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = get_student_work_submission_or_404(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            submission_id=submission_id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return _assignment_student_work_out(
        row,
        latest_extraction_job=row.extraction_jobs[0] if row.extraction_jobs else None,
        latest_extracted_text=row.extracted_text_records[0] if row.extracted_text_records else None,
    )


@router.post("/student-work/{submission_id}/extraction-jobs", response_model=TeacherAssistExtractionJobOut, status_code=201)
def create_teacher_student_work_extraction_job(
    submission_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> TeacherAssistExtractionJobOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = create_student_work_extraction_job(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            submission_id=submission_id,
            settings=settings,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _extraction_job_out(row)


@router.post(
    "/student-work/{submission_id}/extraction-jobs/retry",
    response_model=TeacherAssistExtractionJobOut,
    status_code=201,
)
def retry_teacher_student_work_extraction_job(
    submission_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> TeacherAssistExtractionJobOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = retry_latest_eligible_extraction_for_submission(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            submission_id=submission_id,
            settings=settings,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _extraction_job_out(row)


@router.get("/student-work/{submission_id}/extractions", response_model=list[TeacherAssistExtractionRunOut])
def read_teacher_student_work_extractions(
    submission_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> list[TeacherAssistExtractionRunOut]:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        rows = list_student_work_extraction_runs(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            submission_id=submission_id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return [_extraction_run_out(job, record) for job, record in rows]


@router.get("/extraction-jobs/{extraction_job_id}", response_model=TeacherAssistExtractionJobDetailOut)
def read_teacher_extraction_job(
    extraction_job_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> TeacherAssistExtractionJobDetailOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        payload = get_extraction_job_detail(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            extraction_job_id=extraction_job_id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    source_artifact = payload["source_artifact"]
    return TeacherAssistExtractionJobDetailOut(
        job=_extraction_job_out(payload["job"]),
        extracted_text=(
            _extracted_text_record_out(payload["extracted_text"])
            if payload.get("extracted_text") is not None
            else None
        ),
        lineage_jobs=[_extraction_job_out(row) for row in payload.get("lineage_jobs", [])],
        retry_eligible=bool(payload.get("retry_eligible")),
        cancel_eligible=bool(payload.get("cancel_eligible")),
        processing_duration_seconds=payload.get("processing_duration_seconds"),
        execution_timeline=list(payload.get("execution_timeline") or []),
        source_artifact=TeacherAssistExtractionSourceArtifactOut(**source_artifact),
        activity_events=[_activity_event_out(row) for row in payload.get("activity_events", [])],
    )


@router.patch("/extraction-jobs/{extraction_job_id}/cancel", response_model=TeacherAssistExtractionJobOut)
def cancel_teacher_extraction_job(
    extraction_job_id: uuid.UUID,
    body: TeacherAssistExtractionJobCancelUpdate,
    user: CurrentUser,
    db: DbSession,
) -> TeacherAssistExtractionJobOut:
    del body
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = cancel_extraction_job(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            extraction_job_id=extraction_job_id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _extraction_job_out(row)


@router.get("/extractions", response_model=list[TeacherAssistExtractionSummaryOut])
def read_teacher_extraction_summaries(
    user: CurrentUser,
    db: DbSession,
    limit: int = Query(default=100, ge=1, le=200),
) -> list[TeacherAssistExtractionSummaryOut]:
    tenant_id = _teacher_assist_tenant_id(db, user)
    rows = list_extraction_summaries(db, tenant_id=tenant_id, user_id=user.id, limit=limit)
    return [
        TeacherAssistExtractionSummaryOut(
            job=_extraction_job_out(item["job"]),
            extracted_text=(
                _extracted_text_record_out(item["record"]) if item.get("record") is not None else None
            ),
            retry_eligible=bool(item.get("retry_eligible")),
            processing_duration_seconds=item.get("processing_duration_seconds"),
        )
        for item in rows
    ]


@router.post("/extraction-jobs/{extraction_job_id}/retry", response_model=TeacherAssistExtractionJobOut, status_code=201)
def retry_teacher_extraction_job(
    extraction_job_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> TeacherAssistExtractionJobOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = retry_extraction_job(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            extraction_job_id=extraction_job_id,
            settings=settings,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _extraction_job_out(row)


@router.get("/extracted-text/{extracted_text_id}", response_model=TeacherAssistExtractedTextDetailAggregateOut)
def read_teacher_extracted_text_detail(
    extracted_text_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> TeacherAssistExtractedTextDetailAggregateOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        payload = get_extracted_text_detail(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            extracted_text_id=extracted_text_id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return TeacherAssistExtractedTextDetailAggregateOut(
        record=_extracted_text_detail_out(payload["record"]),
        job=_extraction_job_out(payload["job"]),
        lineage_jobs=[_extraction_job_out(row) for row in payload.get("lineage_jobs", [])],
        retry_eligible=bool(payload.get("retry_eligible")),
        cancel_eligible=bool(payload.get("cancel_eligible")),
        processing_duration_seconds=payload.get("processing_duration_seconds"),
        activity_events=[_activity_event_out(row) for row in payload.get("activity_events", [])],
    )


@router.get("/extracted-text/{extracted_text_id}/history", response_model=TeacherAssistExtractedTextHistoryOut)
def read_teacher_extracted_text_history(
    extracted_text_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> TeacherAssistExtractedTextHistoryOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        payload = get_extracted_text_history(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            extracted_text_id=extracted_text_id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return TeacherAssistExtractedTextHistoryOut(
        current_record=_extracted_text_detail_out(payload["current_record"]),
        current_job=_extraction_job_out(payload["current_job"]),
        attempt_jobs=[_extraction_job_out(row) for row in payload.get("attempt_jobs", [])],
        attempt_records=[_extracted_text_record_out(row) for row in payload.get("attempt_records", [])],
        activity_events=[_activity_event_out(row) for row in payload.get("activity_events", [])],
    )


@router.patch("/extracted-text/{extracted_text_id}/review-status", response_model=TeacherAssistExtractedTextRecordOut)
def update_teacher_extracted_text_review_status(
    extracted_text_id: uuid.UUID,
    body: TeacherAssistExtractedTextReviewStatusUpdate,
    user: CurrentUser,
    db: DbSession,
) -> TeacherAssistExtractedTextRecordOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = update_extracted_text_review_status(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            extracted_text_id=extracted_text_id,
            review_status=body.review_status,
            teacher_review_notes=body.teacher_review_notes,
            teacher_issue_reason=body.teacher_issue_reason,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _extracted_text_record_out(row)


@router.put("/extracted-text/{extracted_text_id}/approved-text", response_model=TeacherAssistExtractedTextRecordOut)
def update_teacher_extracted_text_approved_text(
    extracted_text_id: uuid.UUID,
    body: TeacherAssistExtractedTextApprovedTextUpdate,
    user: CurrentUser,
    db: DbSession,
) -> TeacherAssistExtractedTextRecordOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = save_extracted_text_approved_content(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            extracted_text_id=extracted_text_id,
            approved_text=body.approved_text,
            teacher_corrected_text=body.teacher_corrected_text,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _extracted_text_record_out(row)


@router.get("/student-work/{submission_id}/download-url", response_model=TeacherAssistFileDownloadOut)
def read_teacher_assignment_student_work_download_url(
    submission_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> TeacherAssistFileDownloadOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = get_student_work_submission_or_404(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            submission_id=submission_id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    if not teacher_assist_file_exists(settings, storage_key=row.storage_key):
        raise HTTPException(status_code=404, detail="Stored student-work file not found")
    return _file_download_out(
        settings=settings,
        url=get_teacher_assist_download_url(
            settings,
            storage_key=row.storage_key,
            original_filename=row.original_filename,
            mime_type=row.mime_type,
        ),
    )


@router.patch("/student-work/{submission_id}/status", response_model=AssignmentStudentWorkOut)
def update_teacher_assignment_student_work_status(
    submission_id: uuid.UUID,
    body: AssignmentStudentWorkStatusUpdate,
    user: CurrentUser,
    db: DbSession,
) -> AssignmentStudentWorkOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = update_student_work_submission_processing_status(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            submission_id=submission_id,
            processing_status=body.processing_status,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _assignment_student_work_out(row)


@router.patch("/student-work/{submission_id}/packet-context", response_model=AssignmentStudentWorkOut)
def update_teacher_assignment_student_work_packet_context(
    submission_id: uuid.UUID,
    body: AssignmentStudentWorkPacketContextUpdate,
    user: CurrentUser,
    db: DbSession,
) -> AssignmentStudentWorkOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = link_student_work_submission_context(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            submission_id=submission_id,
            assignment_print_packet_id=body.assignment_print_packet_id,
            assignment_print_page_id=body.assignment_print_page_id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _assignment_student_work_out(row)


@router.get("/assignments/{assignment_id}/grading-reviews", response_model=list[AssignmentGradingReviewOut])
def read_teacher_assignment_grading_reviews(
    assignment_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> list[AssignmentGradingReviewOut]:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        rows = list_assignment_grading_reviews(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            assignment_id=assignment_id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return [_assignment_grading_review_out(row) for row in rows]


@router.post("/student-work/{submission_id}/grading-review", response_model=AssignmentGradingReviewOut, status_code=201)
def create_teacher_assignment_grading_review(
    submission_id: uuid.UUID,
    body: AssignmentGradingReviewCreate,
    user: CurrentUser,
    db: DbSession,
) -> AssignmentGradingReviewOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = create_grading_review_from_student_work(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            student_work_submission_id=submission_id,
            student_number=body.student_number,
            score_suggestion=body.score_suggestion,
            max_score=body.max_score,
            feedback_summary=body.feedback_summary,
            strengths=body.strengths,
            improvement_areas=body.improvement_areas,
            teacher_notes=body.teacher_notes,
            items=[item.model_dump() for item in body.items],
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _assignment_grading_review_out(row)


@router.get("/grading-reviews/{grading_review_id}", response_model=AssignmentGradingReviewOut)
def read_teacher_assignment_grading_review(
    grading_review_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> AssignmentGradingReviewOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = get_grading_review_or_404(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            grading_review_id=grading_review_id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return _assignment_grading_review_out(row)


@router.put("/grading-reviews/{grading_review_id}", response_model=AssignmentGradingReviewOut)
def update_teacher_assignment_grading_review(
    grading_review_id: uuid.UUID,
    body: AssignmentGradingReviewUpdate,
    user: CurrentUser,
    db: DbSession,
) -> AssignmentGradingReviewOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = update_grading_review(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            grading_review_id=grading_review_id,
            status=body.status,
            score_suggestion=body.score_suggestion,
            max_score=body.max_score,
            feedback_summary=body.feedback_summary,
            strengths=body.strengths,
            improvement_areas=body.improvement_areas,
            teacher_notes=body.teacher_notes,
            teacher_confirmed_score=body.teacher_confirmed_score,
            teacher_confirmed_feedback=body.teacher_confirmed_feedback,
            items=[item.model_dump() for item in body.items],
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _assignment_grading_review_out(row)


@router.patch("/grading-reviews/{grading_review_id}/status", response_model=AssignmentGradingReviewOut)
def update_teacher_assignment_grading_review_status(
    grading_review_id: uuid.UUID,
    body: AssignmentGradingReviewStatusUpdate,
    user: CurrentUser,
    db: DbSession,
) -> AssignmentGradingReviewOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = update_grading_review_status(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            grading_review_id=grading_review_id,
            status=body.status,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _assignment_grading_review_out(row)


@router.get("/planning-drafts", response_model=list[PlanningDraftOut])
def read_planning_drafts(user: CurrentUser, db: DbSession) -> list[PlanningDraftOut]:
    tenant_id = _teacher_assist_tenant_id(db, user)
    subject_map, pacing_item_map, standard_map, resource_map = _planning_draft_maps(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
    )
    return [
        _planning_draft_out(
            row,
            subject_ids=subject_map.get(row.id, [row.subject_id] if row.subject_id is not None else []),
            pacing_item_ids=pacing_item_map.get(row.id, []),
            standard_ids=standard_map.get(row.id, []),
            resource_ids=resource_map.get(row.id, []),
        )
        for row in list_planning_drafts(db, tenant_id=tenant_id, user_id=user.id)
    ]


@router.post("/planning-drafts", response_model=PlanningDraftOut, status_code=201)
def create_teacher_planning_draft(
    body: PlanningDraftCreate,
    user: CurrentUser,
    db: DbSession,
) -> PlanningDraftOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = create_planning_draft(
            db,
            tenant_id=tenant_id,
            user=user,
            planning_scope=body.planning_scope,
            school_year_id=body.school_year_id,
            grading_period_id=body.grading_period_id,
            class_id=body.class_id,
            subject_id=body.subject_id,
            subject_ids=body.subject_ids,
            pacing_item_ids=body.pacing_item_ids,
            standard_ids=body.standard_ids,
            title=body.plan_title or body.title,
            module_title=body.module_title,
            start_date=body.start_date,
            end_date=body.end_date,
            estimated_weeks=body.estimated_weeks,
            instructional_days_count=body.instructional_days_count,
            notes=body.notes,
            status=body.status,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _planning_draft_out(
        row,
        subject_ids=body.subject_ids or ([row.subject_id] if row.subject_id is not None else []),
        pacing_item_ids=body.pacing_item_ids,
        standard_ids=body.standard_ids,
        resource_ids=[],
    )


@router.put("/planning-drafts/{planning_draft_id}", response_model=PlanningDraftOut)
def update_teacher_planning_draft(
    planning_draft_id: uuid.UUID,
    body: PlanningDraftCreate,
    user: CurrentUser,
    db: DbSession,
) -> PlanningDraftOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = update_planning_draft(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            planning_draft_id=planning_draft_id,
            planning_scope=body.planning_scope,
            school_year_id=body.school_year_id,
            grading_period_id=body.grading_period_id,
            class_id=body.class_id,
            subject_id=body.subject_id,
            subject_ids=body.subject_ids,
            pacing_item_ids=body.pacing_item_ids,
            standard_ids=body.standard_ids,
            title=body.plan_title or body.title,
            module_title=body.module_title,
            start_date=body.start_date,
            end_date=body.end_date,
            estimated_weeks=body.estimated_weeks,
            instructional_days_count=body.instructional_days_count,
            notes=body.notes,
            status=body.status,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    subject_map, pacing_item_map, standard_map, resource_map = _planning_draft_maps(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
    )
    return _planning_draft_out(
        row,
        subject_ids=subject_map.get(row.id, [row.subject_id] if row.subject_id is not None else []),
        pacing_item_ids=pacing_item_map.get(row.id, []),
        standard_ids=standard_map.get(row.id, []),
        resource_ids=resource_map.get(row.id, []),
    )


@router.post("/planning-drafts/{planning_draft_id}/resources", response_model=PlanningDraftOut)
def create_teacher_planning_draft_resource(
    planning_draft_id: uuid.UUID,
    body: PlanningDraftResourceCreate,
    user: CurrentUser,
    db: DbSession,
) -> PlanningDraftOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = attach_planning_draft_resource(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            planning_draft_id=planning_draft_id,
            resource_library_item_id=body.resource_library_item_id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    subject_map, pacing_item_map, standard_map, resource_map = _planning_draft_maps(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
    )
    draft = next(
        item
        for item in list_planning_drafts(db, tenant_id=tenant_id, user_id=user.id)
        if item.id == row.planning_input_draft_id
    )
    return _planning_draft_out(
        draft,
        subject_ids=subject_map.get(draft.id, [draft.subject_id] if draft.subject_id is not None else []),
        pacing_item_ids=pacing_item_map.get(draft.id, []),
        standard_ids=standard_map.get(draft.id, []),
        resource_ids=resource_map.get(draft.id, []),
    )


@router.get(
    "/planning-drafts/{planning_draft_id}/context-preview",
    response_model=PlanningDraftContextPreviewOut,
)
def read_planning_draft_context_preview(
    planning_draft_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> PlanningDraftContextPreviewOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        preview = get_planning_draft_context_preview(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            planning_draft_id=planning_draft_id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    counts = list_resource_link_counts(db, tenant_id=tenant_id)
    subject_ids = [subject.id for subject in preview.subjects]
    pacing_item_ids = [pacing_item.id for pacing_item in preview.pacing_items]
    standard_ids = [standard.id for standard in preview.standards]
    resource_ids = [resource.id for resource in preview.resources]
    class_subject_ids = (
        [row.subject_id for row in preview.draft.teacher_class.class_subjects]
        if preview.draft.teacher_class is not None
        else []
    )

    return PlanningDraftContextPreviewOut(
        draft=_planning_draft_out(
            preview.draft,
            subject_ids=subject_ids,
            pacing_item_ids=pacing_item_ids,
            standard_ids=standard_ids,
            resource_ids=resource_ids,
        ),
        school_year=_school_year_out(preview.draft.school_year) if preview.draft.school_year is not None else None,
        grading_period=_grading_period_out(preview.draft.grading_period)
        if preview.draft.grading_period is not None
        else None,
        class_context=_class_out(preview.draft.teacher_class, subject_ids=class_subject_ids)
        if preview.draft.teacher_class is not None
        else None,
        subjects=[_subject_out(row) for row in preview.subjects],
        pacing_items=[
            _pacing_item_out(
                row,
                standard_ids=[
                    item.standard_id
                    for item in row.standard_links
                ],
                resource_ids=[
                    item.resource_library_item_id
                    for item in row.resource_links
                ],
            )
            for row in preview.pacing_items
        ],
        pacing_groups=[
            {
                "group_key": group.group_key,
                "label": group.label,
                "pacing_items": [
                    _pacing_item_out(
                        row,
                        standard_ids=[item.standard_id for item in row.standard_links],
                        resource_ids=[item.resource_library_item_id for item in row.resource_links],
                    )
                    for row in group.pacing_items
                ],
            }
            for group in preview.pacing_groups
        ],
        standards=[_standard_out(row) for row in preview.standards],
        resources=[
            _resource_out(
                row,
                linked_pacing_items_count=counts.pacing_items.get(row.id, 0),
                linked_planning_drafts_count=counts.planning_drafts.get(row.id, 0),
            )
            for row in preview.resources
        ],
        teacher_notes=preview.draft.notes,
        duration_summary={
            "start_date": preview.duration_summary.start_date,
            "end_date": preview.duration_summary.end_date,
            "estimated_weeks": preview.duration_summary.estimated_weeks,
            "instructional_days_count": preview.duration_summary.instructional_days_count,
            "summary": preview.duration_summary.summary,
        },
        readiness=PlanningDraftReadinessOut(
            is_ready=preview.readiness.is_ready,
            missing_items=preview.readiness.missing_items,
            warnings=preview.readiness.warnings,
        ),
    )


@router.patch("/planning-drafts/{planning_draft_id}/status", response_model=PlanningDraftOut)
def patch_teacher_planning_draft_status(
    planning_draft_id: uuid.UUID,
    body: PlanningDraftStatusUpdate,
    user: CurrentUser,
    db: DbSession,
) -> PlanningDraftOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = update_planning_draft_status(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            planning_draft_id=planning_draft_id,
            status=body.status,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    subject_map, pacing_item_map, standard_map, resource_map = _planning_draft_maps(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
    )
    return _planning_draft_out(
        row,
        subject_ids=subject_map.get(row.id, [row.subject_id] if row.subject_id is not None else []),
        pacing_item_ids=pacing_item_map.get(row.id, []),
        standard_ids=standard_map.get(row.id, []),
        resource_ids=resource_map.get(row.id, []),
    )


@router.post(
    "/planning-drafts/{planning_draft_id}/generation-preview",
    response_model=PlanningDraftGenerationPreviewOut,
)
def read_teacher_planning_generation_preview(
    planning_draft_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> PlanningDraftGenerationPreviewOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        preview = get_planning_draft_context_preview(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            planning_draft_id=planning_draft_id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return PlanningDraftGenerationPreviewOut(
        message="Generation will be available in a later phase.",
        draft_id=planning_draft_id,
        ready=preview.readiness.is_ready,
    )


@router.post(
    "/planning-drafts/{planning_draft_id}/workflows/weekly-plan",
    response_model=TeacherAssistWorkflowOut,
    status_code=202,
)
def start_teacher_weekly_plan_workflow(
    planning_draft_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> TeacherAssistWorkflowOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        workflow = create_weekly_plan_workflow(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            planning_draft_id=planning_draft_id,
        )
        db.commit()
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _workflow_out(workflow)


@router.get("/workflows", response_model=list[TeacherAssistWorkflowOut])
def read_teacher_assist_workflows(user: CurrentUser, db: DbSession) -> list[TeacherAssistWorkflowOut]:
    tenant_id = _teacher_assist_tenant_id(db, user)
    return [
        _workflow_out(row)
        for row in list_teacher_assist_workflows(db, tenant_id=tenant_id, user_id=user.id)
    ]


@router.get("/workflows/{workflow_id}", response_model=TeacherAssistWorkflowDetailOut)
def read_teacher_assist_workflow(
    workflow_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> TeacherAssistWorkflowDetailOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = get_teacher_assist_workflow_or_404(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            workflow_id=workflow_id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return _workflow_detail_out(row)


@router.patch("/workflows/{workflow_id}/cancel", response_model=TeacherAssistWorkflowOut)
def cancel_teacher_assist_workflow_route(
    workflow_id: uuid.UUID,
    _body: TeacherAssistWorkflowCancelUpdate,
    user: CurrentUser,
    db: DbSession,
) -> TeacherAssistWorkflowOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = cancel_teacher_assist_workflow(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            workflow_id=workflow_id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _workflow_out(row)


@router.get("/weekly-plans", response_model=list[WeeklyPlanOut])
def read_teacher_assist_weekly_plans(user: CurrentUser, db: DbSession) -> list[WeeklyPlanOut]:
    tenant_id = _teacher_assist_tenant_id(db, user)
    rows = list_weekly_plans(db, tenant_id=tenant_id, user_id=user.id)
    latest_usage_by_plan_id = _latest_usage_event_by_plan_id(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        plans=rows,
    )
    return [
        _weekly_plan_out(
            row,
            latest_usage_event=latest_usage_by_plan_id.get(row.id),
        )
        for row in rows
    ]


@router.get("/weekly-plans/{weekly_plan_id}", response_model=WeeklyPlanOut)
def read_teacher_assist_weekly_plan(
    weekly_plan_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> WeeklyPlanOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = get_visible_weekly_plan_or_404(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            weekly_plan_id=weekly_plan_id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    latest_usage_by_plan_id = _latest_usage_event_by_plan_id(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        plans=[row],
    )
    return _weekly_plan_out(
        row,
        latest_usage_event=latest_usage_by_plan_id.get(row.id),
    )


@router.post("/weekly-plans/{weekly_plan_id}/assignments", response_model=AssignmentOut, status_code=201)
def create_teacher_assignment_from_weekly_plan(
    weekly_plan_id: uuid.UUID,
    body: WeeklyPlanAssignmentCreate,
    user: CurrentUser,
    db: DbSession,
) -> AssignmentOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = create_assignment_from_weekly_plan(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            weekly_plan_id=weekly_plan_id,
            title=body.title,
            description=body.description,
            assignment_type=body.assignment_type,
            due_date=body.due_date,
            instructions=body.instructions,
            rubric_json=body.rubric_json,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    standards_map = _assignment_standard_map(
        list_assignment_standards(db, tenant_id=tenant_id, user_id=user.id, assignment_ids=[row.id])
    )
    resources_map = _assignment_resource_map(
        list_assignment_resources(db, tenant_id=tenant_id, user_id=user.id, assignment_ids=[row.id])
    )
    return _assignment_out(
        row,
        standard_ids=standards_map.get(row.id, []),
        resource_ids=resources_map.get(row.id, []),
    )


@router.put("/weekly-plans/{weekly_plan_id}", response_model=WeeklyPlanOut)
def update_teacher_assist_weekly_plan(
    weekly_plan_id: uuid.UUID,
    body: WeeklyPlanUpdate,
    user: CurrentUser,
    db: DbSession,
) -> WeeklyPlanOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = update_weekly_plan(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            weekly_plan_id=weekly_plan_id,
            title=body.title,
            status=body.status,
            content_json=body.content_json,
            change_reason=body.change_reason,
        )
        db.commit()
        db.refresh(row)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    latest_usage_by_plan_id = _latest_usage_event_by_plan_id(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        plans=[row],
    )
    return _weekly_plan_out(
        row,
        latest_usage_event=latest_usage_by_plan_id.get(row.id),
    )


@router.post("/weekly-plans/{weekly_plan_id}/copy", response_model=WeeklyPlanOut, status_code=201)
def copy_teacher_assist_weekly_plan(
    weekly_plan_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
    body: WeeklyPlanCopyCreate | None = None,
) -> WeeklyPlanOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = copy_weekly_plan(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            weekly_plan_id=weekly_plan_id,
            target_school_year_id=body.target_school_year_id if body else None,
            target_grading_period_id=body.target_grading_period_id if body else None,
            target_class_id=body.target_class_id if body else None,
            title_override=body.title_override if body else None,
            copy_mode=body.copy_mode if body else "personal_copy",
        )
        db.commit()
        db.refresh(row)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    latest_usage_by_plan_id = _latest_usage_event_by_plan_id(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        plans=[row],
    )
    return _weekly_plan_out(
        row,
        latest_usage_event=latest_usage_by_plan_id.get(row.id),
    )


@router.patch("/weekly-plans/{weekly_plan_id}/sharing", response_model=WeeklyPlanOut)
def patch_teacher_assist_weekly_plan_sharing(
    weekly_plan_id: uuid.UUID,
    body: WeeklyPlanSharingUpdate,
    user: CurrentUser,
    db: DbSession,
) -> WeeklyPlanOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = update_weekly_plan_sharing(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            weekly_plan_id=weekly_plan_id,
            is_template=body.is_template,
            visibility_scope=body.visibility_scope,
            reuse_status=body.reuse_status,
        )
        db.commit()
        db.refresh(row)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    latest_usage_by_plan_id = _latest_usage_event_by_plan_id(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        plans=[row],
    )
    return _weekly_plan_out(
        row,
        latest_usage_event=latest_usage_by_plan_id.get(row.id),
    )


@router.post("/weekly-plans/{weekly_plan_id}/regenerate-section", response_model=WeeklyPlanOut)
def regenerate_teacher_assist_weekly_plan_section(
    weekly_plan_id: uuid.UUID,
    body: WeeklyPlanSectionRegenerationCreate,
    user: CurrentUser,
    db: DbSession,
    settings: Settings = Depends(settings_dep),
) -> WeeklyPlanOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = regenerate_weekly_plan_section(
            db,
            settings=settings,
            tenant_id=tenant_id,
            user_id=user.id,
            weekly_plan_id=weekly_plan_id,
            section_key=body.section_key,
            section_path=body.section_path,
            teacher_instruction=body.teacher_instruction,
            provider_mode=body.provider_mode,
            preserve_existing_context=body.preserve_existing_context,
        )
        db.commit()
        db.refresh(row)
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    latest_usage_by_plan_id = _latest_usage_event_by_plan_id(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        plans=[row],
    )
    return _weekly_plan_out(
        row,
        latest_usage_event=latest_usage_by_plan_id.get(row.id),
    )


@router.get("/instructional-plans/library", response_model=list[InstructionalPlanLibraryItemOut])
def read_instructional_plan_library(
    user: CurrentUser,
    db: DbSession,
    school_year_id: uuid.UUID | None = Query(default=None),
    subject_id: uuid.UUID | None = Query(default=None),
    planning_scope: str | None = Query(default=None),
    visibility_scope: str | None = Query(default=None),
    reuse_status: str | None = Query(default=None),
    is_template: bool | None = Query(default=None),
    q: str | None = Query(default=None),
) -> list[InstructionalPlanLibraryItemOut]:
    tenant_id = _teacher_assist_tenant_id(db, user)
    rows = list_instructional_plan_library(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        school_year_id=school_year_id,
        subject_id=subject_id,
        planning_scope=planning_scope,
        visibility_scope=visibility_scope,
        reuse_status=reuse_status,
        is_template=is_template,
        q=q,
    )
    return [_instructional_plan_library_item_out(row, current_user_id=user.id) for row in rows]


@router.get("/curriculum-rollover/candidates", response_model=CurriculumRolloverCandidatesOut)
def read_teacher_assist_curriculum_rollover_candidates(
    user: CurrentUser,
    db: DbSession,
    source_school_year_id: uuid.UUID,
    target_school_year_id: uuid.UUID,
    subject_id: uuid.UUID | None = Query(default=None),
    planning_scope: str | None = Query(default=None),
    reuse_status: str | None = Query(default=None),
) -> CurriculumRolloverCandidatesOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    rows, existing_by_source_id = curriculum_rollover_candidates(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        source_school_year_id=source_school_year_id,
        target_school_year_id=target_school_year_id,
        subject_id=subject_id,
        planning_scope=planning_scope,
        reuse_status=reuse_status,
    )
    summary_counts_by_planning_scope: dict[str, int] = {}
    subjects_represented: set[str] = set()
    grading_periods_represented: set[str] = set()
    items: list[CurriculumRolloverCandidateOut] = []
    for row in rows:
        item = _instructional_plan_library_item_out(row, current_user_id=user.id)
        summary_counts_by_planning_scope[row.planning_scope] = (
            summary_counts_by_planning_scope.get(row.planning_scope, 0) + 1
        )
        subjects_represented.update(item.subject_names)
        if item.grading_period_title:
            grading_periods_represented.add(item.grading_period_title)
        existing_target = existing_by_source_id.get(row.id)
        items.append(
            CurriculumRolloverCandidateOut(
                **item.model_dump(),
                already_copied_to_target=existing_target is not None,
                existing_target_plan_id=existing_target.id if existing_target is not None else None,
            )
        )
    return CurriculumRolloverCandidatesOut(
        items=items,
        summary_counts_by_planning_scope=summary_counts_by_planning_scope,
        subjects_represented=sorted(subjects_represented),
        grading_periods_represented=sorted(grading_periods_represented),
    )


@router.post("/curriculum-rollover/copy", response_model=CurriculumRolloverCopyOut)
def create_teacher_assist_curriculum_rollover_copy(
    body: CurriculumRolloverCopyCreate,
    user: CurrentUser,
    db: DbSession,
) -> CurriculumRolloverCopyOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        copied_rows, warnings = curriculum_rollover_copy(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            source_school_year_id=body.source_school_year_id,
            target_school_year_id=body.target_school_year_id,
            plan_ids=body.plan_ids,
            preserve_titles=body.preserve_titles,
            title_suffix=body.title_suffix,
            target_grading_period_mapping=body.target_grading_period_mapping,
        )
        db.commit()
        for row in copied_rows:
            db.refresh(row)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    latest_usage_by_plan_id = _latest_usage_event_by_plan_id(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        plans=copied_rows,
    )
    return CurriculumRolloverCopyOut(
        copied_plans=[
            _weekly_plan_out(
                row,
                latest_usage_event=latest_usage_by_plan_id.get(row.id),
            )
            for row in copied_rows
        ],
        warnings=warnings,
    )


@router.get("/weekly-plans/{weekly_plan_id}/versions", response_model=list[WeeklyPlanVersionOut])
def read_teacher_assist_weekly_plan_versions(
    weekly_plan_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> list[WeeklyPlanVersionOut]:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        rows = list_weekly_plan_versions(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            weekly_plan_id=weekly_plan_id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return [_weekly_plan_version_out(row) for row in rows]


@router.get("/weekly-plans/{weekly_plan_id}/versions/{version_id}", response_model=WeeklyPlanVersionOut)
def read_teacher_assist_weekly_plan_version(
    weekly_plan_id: uuid.UUID,
    version_id: uuid.UUID,
    user: CurrentUser,
    db: DbSession,
) -> WeeklyPlanVersionOut:
    tenant_id = _teacher_assist_tenant_id(db, user)
    try:
        row = get_weekly_plan_version_or_404(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            weekly_plan_id=weekly_plan_id,
            version_id=version_id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return _weekly_plan_version_out(row)
