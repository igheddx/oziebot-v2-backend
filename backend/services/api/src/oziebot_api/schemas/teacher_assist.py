from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal
import uuid

from pydantic import BaseModel, Field


GradeLevelLiteral = Literal["Pre-K", "K", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"]
GradingPeriodTypeLiteral = Literal["nine_weeks", "six_weeks", "semester", "trimester", "custom"]
StandardTypeLiteral = Literal["TEKS", "CUSTOM"]
ResourceTypeLiteral = Literal["pdf", "pptx", "image", "worksheet", "spreadsheet", "link", "doc", "other"]
AssignmentTypeLiteral = Literal[
    "writing",
    "reading_response",
    "short_answer",
    "quiz",
    "exit_ticket",
    "project",
    "homework",
    "other",
]
AssignmentStatusLiteral = Literal[
    "draft",
    "ready",
    "assigned",
    "collected",
    "review_in_progress",
    "reviewed",
    "archived",
]
AssignmentPrintPacketStatusLiteral = Literal["generated", "archived"]
AssignmentPrintTemplateTypeLiteral = Literal[
    "blank_writing_page",
    "lined_writing_page",
    "short_answer_page",
]
AssignmentPrintOutputFormatLiteral = Literal["html"]
AssignmentStudentWorkUploadStatusLiteral = Literal["uploaded", "archived"]
AssignmentStudentWorkProcessingStatusLiteral = Literal[
    "pending_review",
    "ready_for_processing",
    "processing_deferred",
    "archived",
]
AssignmentGradingReviewStatusLiteral = Literal[
    "draft",
    "ai_suggested",
    "teacher_reviewing",
    "teacher_confirmed",
    "returned_for_revision",
    "archived",
]
AssignmentGradingReviewSourceLiteral = Literal["manual", "ai_placeholder"]
AssignmentGradeRecordStatusLiteral = Literal["active", "superseded", "reversed"]
AssignmentGradebookCommitTypeLiteral = Literal["initial_commit", "correction", "reversal"]
AssignmentGradebookCommitStatusLiteral = Literal["active", "superseded", "reversed"]
AssignmentGradebookAuditEventTypeLiteral = Literal[
    "commit_created",
    "commit_corrected",
    "commit_reversed",
    "commit_superseded",
    "export_generated",
]
MasteryMatrixStatusLiteral = Literal["draft", "active", "archived"]
MasteryLevelLiteral = Literal["not_assessed", "beginning", "developing", "mastery", "advanced"]
MasteryEvaluationStatusLiteral = Literal["draft", "active", "reversed"]
MasteryCommitTypeLiteral = Literal["initial_commit", "correction", "reversal"]
MasteryCommitStatusLiteral = Literal["active", "superseded", "reversed"]
MasteryEvidenceSourceTypeLiteral = Literal[
    "assignment",
    "grading_review",
    "gradebook_commit",
    "manual_observation",
]
MasteryConfidenceLevelLiteral = Literal["low", "medium", "high"]
ReteachOperationalStatusLiteral = Literal[
    "healthy",
    "monitor",
    "reteach_recommended",
    "critical_attention",
    "unassessed",
]
AssignmentEffectivenessStatusLiteral = Literal[
    "effective",
    "mixed_results",
    "reteach_likely",
    "insufficient_data",
]
StudentMasteryTrendLiteral = Literal["improving", "stable", "declining", "insufficient_data"]
StandardMasteryTrendLiteral = Literal["improving", "stable", "declining", "insufficient_data"]
ReteachPlanStatusLiteral = Literal["draft", "ai_draft", "teacher_review", "archived"]
ReteachPlanVersionSourceLiteral = Literal["initial", "ai_draft", "teacher_edit"]
NewsletterStatusLiteral = Literal["draft", "review", "approved", "archived"]
NewsletterVersionSourceLiteral = Literal["initial", "ai_draft", "ai_section_regen", "teacher_edit"]
NewsletterRegeneratableSectionLiteral = Literal["overview", "upcoming_learning", "teacher_message", "reminders"]
NewsletterExportFormatLiteral = Literal["html", "pdf", "docx"]
TeacherAssistExtractionArtifactTypeLiteral = Literal["resource", "student_work"]
TeacherAssistExtractionJobStatusLiteral = Literal[
    "queued",
    "running",
    "completed",
    "failed",
    "cancelled",
    "skipped",
]
ExtractionReviewStatusLiteral = Literal[
    "pending_review",
    "teacher_reviewing",
    "teacher_approved",
    "teacher_rejected",
    "reviewed",
    "issue_flagged",
    "needs_retry",
    "archived",
]
ExtractionConfidenceLevelLiteral = Literal["low", "medium", "high", "unknown"]
PlanningDraftStatusLiteral = Literal["draft", "ready"]
PlanningScopeLiteral = Literal["weekly", "multi_week", "module", "unit", "grading_period"]
PlanVisibilityScopeLiteral = Literal["private", "shared", "grade_team", "school", "district"]
PlanReuseStatusLiteral = Literal["active", "archived", "reusable"]
TeacherAssistWorkflowTypeLiteral = Literal[
    "weekly_plan_generation",
    "daily_deck_generation",
    "assessment_generation",
    "newsletter_generation",
    "grading_assist",
    "artifact_export",
]
TeacherAssistWorkflowStatusLiteral = Literal["queued", "running", "completed", "failed", "cancelled"]
TeacherAssistWorkflowStepStatusLiteral = Literal["queued", "running", "completed", "failed", "skipped"]
WeeklyPlanStatusLiteral = Literal["in_progress", "completed"]
TeacherAssistExportArtifactTypeLiteral = Literal[
    "lesson_slides",
    "guided_notes",
    "multiple_choice_quiz",
    "exit_ticket",
    "short_answer_quiz",
]
TeacherAssistExportArtifactStatusLiteral = Literal[
    "queued",
    "generating",
    "ready",
    "failed",
    "archived",
]
TeacherAssistExportFormatLiteral = Literal["pptx", "json", "printable_html"]
ActionWorkspaceSeverityLiteral = Literal["critical", "warning", "review", "ready", "info"]
ActionWorkspaceSectionKeyLiteral = Literal[
    "extractions",
    "grading",
    "gradebook",
    "workflows_exports",
    "planning_assignments",
]


class TeacherAssistActionWorkspaceNavigationOut(BaseModel):
    label: str
    href: str


class TeacherAssistActionWorkspaceItemOut(BaseModel):
    action_key: str
    action_type: str
    severity: ActionWorkspaceSeverityLiteral
    title: str
    description: str
    tenant_id: uuid.UUID
    school_year_id: uuid.UUID | None = None
    grading_period_id: uuid.UUID | None = None
    class_id: uuid.UUID | None = None
    assignment_id: uuid.UUID | None = None
    student_work_id: uuid.UUID | None = None
    grading_review_id: uuid.UUID | None = None
    gradebook_record_id: uuid.UUID | None = None
    workflow_id: uuid.UUID | None = None
    export_artifact_id: uuid.UUID | None = None
    extraction_job_id: uuid.UUID | None = None
    extracted_text_id: uuid.UUID | None = None
    navigation: TeacherAssistActionWorkspaceNavigationOut
    created_at: datetime | None = None
    updated_at: datetime | None = None


class TeacherAssistActionWorkspaceSectionOut(BaseModel):
    section_key: ActionWorkspaceSectionKeyLiteral
    title: str
    count: int
    items: list[TeacherAssistActionWorkspaceItemOut] = Field(default_factory=list)


class TeacherAssistActionWorkspaceSummaryOut(BaseModel):
    total_open_actions: int
    critical_count: int
    warning_count: int
    review_count: int
    ready_count: int
    mastery_alert_count: int = 0


class TeacherAssistActionWorkspaceClassRollupOut(BaseModel):
    class_id: uuid.UUID
    class_name: str
    open_action_count: int
    extraction_count: int
    grading_count: int
    gradebook_count: int
    workflow_export_count: int
    planning_assignment_count: int


class TeacherAssistActionWorkspaceActivityOut(BaseModel):
    id: uuid.UUID
    event_category: str
    event_type: str
    entity_type: str
    entity_id: uuid.UUID
    summary_text: str
    class_id: uuid.UUID | None = None
    created_at: datetime


class TeacherAssistActionWorkspaceOut(BaseModel):
    summary: TeacherAssistActionWorkspaceSummaryOut
    sections: list[TeacherAssistActionWorkspaceSectionOut] = Field(default_factory=list)
    priority_items: list[TeacherAssistActionWorkspaceItemOut] = Field(default_factory=list)
    class_rollups: list[TeacherAssistActionWorkspaceClassRollupOut] = Field(default_factory=list)
    recent_activity: list[TeacherAssistActionWorkspaceActivityOut] = Field(default_factory=list)


class TeacherAssistTodaySummaryOut(BaseModel):
    total_open_actions: int = 0
    critical_count: int = 0
    warning_count: int = 0
    review_count: int = 0
    ready_count: int = 0
    mastery_alert_count: int = 0
    today_open_count: int = 0
    items_needing_review_count: int = 0
    grading_pending_count: int = 0
    extraction_pending_count: int = 0
    gradebook_pending_count: int = 0
    reteach_plans_pending_count: int = 0
    mastery_reteach_standard_count: int = 0


class TeacherAssistTodayPriorityItemOut(TeacherAssistActionWorkspaceItemOut):
    today_category: str | None = None


class TeacherAssistTodayWorkspaceOut(BaseModel):
    summary: TeacherAssistTodaySummaryOut
    priority_items: list[TeacherAssistTodayPriorityItemOut] = Field(default_factory=list)
    categories: dict[str, list[TeacherAssistActionWorkspaceItemOut]] = Field(default_factory=dict)
    workflow_progress_cards: list[dict[str, Any]] = Field(default_factory=list)
    onboarding_checklist: dict[str, Any] = Field(default_factory=dict)
    recent_activity: list[TeacherAssistActionWorkspaceActivityOut] = Field(default_factory=list)
    current_school_year: SchoolYearOut | None = None
    active_grading_period: GradingPeriodOut | None = None
    mastery_insights: TeacherAssistWorkspaceMasteryInsightsOut | None = None


class TeacherAssistOptionsOut(BaseModel):
    grading_period_types: list[str]
    standard_types: list[str]
    resource_types: list[str]
    assignment_types: list[str]
    assignment_statuses: list[str]
    assignment_print_packet_statuses: list[str]
    assignment_print_template_types: list[str]
    assignment_print_output_formats: list[str]
    assignment_student_work_upload_statuses: list[str]
    assignment_student_work_processing_statuses: list[str]
    assignment_grading_review_statuses: list[str]
    assignment_grading_review_sources: list[str]
    extraction_artifact_types: list[str]
    extraction_job_statuses: list[str]
    extraction_review_statuses: list[str]
    extraction_confidence_levels: list[str]
    export_artifact_types: list[str]
    export_artifact_statuses: list[str]
    export_formats: list[str]
    assignment_grade_record_statuses: list[str]
    assignment_gradebook_commit_types: list[str]
    assignment_gradebook_commit_statuses: list[str]
    assignment_gradebook_audit_event_types: list[str]
    mastery_matrix_statuses: list[str]
    mastery_levels: list[str]
    mastery_evaluation_statuses: list[str]
    mastery_commit_types: list[str]
    mastery_commit_statuses: list[str]
    mastery_evidence_source_types: list[str]
    mastery_confidence_levels: list[str]
    reteach_plan_statuses: list[str] = Field(default_factory=list)
    reteach_plan_version_sources: list[str] = Field(default_factory=list)
    newsletter_statuses: list[str] = Field(default_factory=list)
    newsletter_version_sources: list[str] = Field(default_factory=list)
    newsletter_regeneratable_sections: list[str] = Field(default_factory=list)
    newsletter_export_formats: list[str] = Field(default_factory=list)
    planning_draft_statuses: list[str]
    planning_scopes: list[str]
    supported_grade_levels: list[str]


class TeacherProfileOut(BaseModel):
    id: uuid.UUID | None = None
    preferred_grade_level: GradeLevelLiteral | None = None
    default_student_count: int | None = None
    preferred_grading_period_type: GradingPeriodTypeLiteral | None = None
    timezone: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class TeacherProfileUpsert(BaseModel):
    preferred_grade_level: GradeLevelLiteral | None = None
    default_student_count: int | None = Field(default=None, ge=1)
    preferred_grading_period_type: GradingPeriodTypeLiteral | None = None
    timezone: str | None = Field(default=None, max_length=128)


class SchoolYearCreate(BaseModel):
    title: str = Field(min_length=1, max_length=64)
    start_date: date
    end_date: date
    is_active: bool = False


class SchoolYearOut(BaseModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    title: str
    start_date: date
    end_date: date
    is_active: bool
    created_at: datetime
    updated_at: datetime


class GradingPeriodCreate(BaseModel):
    school_year_id: uuid.UUID
    title: str = Field(min_length=1, max_length=128)
    grading_period_type: GradingPeriodTypeLiteral
    start_date: date
    end_date: date
    sort_order: int = Field(default=0, ge=0)


class GradingPeriodOut(BaseModel):
    id: uuid.UUID
    school_year_id: uuid.UUID
    title: str
    grading_period_type: GradingPeriodTypeLiteral
    start_date: date
    end_date: date
    sort_order: int
    created_at: datetime
    updated_at: datetime


class SubjectCreate(BaseModel):
    code: str | None = Field(default=None, max_length=32)
    name: str = Field(min_length=1, max_length=128)


class SubjectOut(BaseModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    code: str | None = None
    name: str
    created_at: datetime
    updated_at: datetime


class ClassCreate(BaseModel):
    school_year_id: uuid.UUID
    name: str = Field(min_length=1, max_length=128)
    grade_level: GradeLevelLiteral
    student_count: int = Field(ge=1)


class ClassOut(BaseModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    school_year_id: uuid.UUID
    name: str
    grade_level: GradeLevelLiteral
    student_count: int
    subject_ids: list[uuid.UUID] = Field(default_factory=list)
    student_number_range_start: int = 1
    student_number_range_end: int
    created_at: datetime
    updated_at: datetime


class ClassSubjectCreate(BaseModel):
    class_id: uuid.UUID
    subject_id: uuid.UUID


class ClassSubjectOut(BaseModel):
    id: uuid.UUID
    class_id: uuid.UUID
    subject_id: uuid.UUID
    created_at: datetime


class StandardCreate(BaseModel):
    subject_id: uuid.UUID | None = None
    standard_type: StandardTypeLiteral
    code: str = Field(min_length=1, max_length=64)
    description: str = Field(min_length=1, max_length=4000)
    grade_level: GradeLevelLiteral | None = None
    school_year_id: uuid.UUID | None = None


class StandardOut(BaseModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    subject_id: uuid.UUID | None = None
    standard_type: StandardTypeLiteral
    code: str
    description: str
    grade_level: GradeLevelLiteral | None = None
    school_year_id: uuid.UUID | None = None
    created_at: datetime
    updated_at: datetime


class PacingGuideCreate(BaseModel):
    school_year_id: uuid.UUID
    title: str = Field(min_length=1, max_length=128)
    description: str | None = Field(default=None, max_length=4000)
    grade_level: GradeLevelLiteral | None = None
    subject_id: uuid.UUID | None = None
    is_shared: bool = False


class PacingGuideOut(BaseModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    school_year_id: uuid.UUID
    title: str
    description: str | None = None
    grade_level: GradeLevelLiteral | None = None
    subject_id: uuid.UUID | None = None
    is_shared: bool
    created_by_user_id: uuid.UUID
    item_count: int = 0
    created_at: datetime
    updated_at: datetime


class PacingItemCreate(BaseModel):
    grading_period_id: uuid.UUID | None = None
    subject_id: uuid.UUID | None = None
    week_number: int | None = Field(default=None, ge=1)
    day_number: int | None = Field(default=None, ge=1)
    instructional_date: date | None = None
    title: str = Field(min_length=1, max_length=160)
    instructional_focus: str | None = Field(default=None, max_length=4000)
    objectives: str | None = Field(default=None, max_length=4000)
    notes: str | None = Field(default=None, max_length=4000)
    sort_order: int | None = Field(default=None, ge=0)


class PacingItemOut(BaseModel):
    id: uuid.UUID
    pacing_guide_id: uuid.UUID
    grading_period_id: uuid.UUID | None = None
    subject_id: uuid.UUID | None = None
    week_number: int | None = None
    day_number: int | None = None
    instructional_date: date | None = None
    title: str
    instructional_focus: str | None = None
    objectives: str | None = None
    notes: str | None = None
    sort_order: int | None = None
    standard_ids: list[uuid.UUID] = Field(default_factory=list)
    resource_ids: list[uuid.UUID] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime


class PacingItemStandardCreate(BaseModel):
    standard_id: uuid.UUID


class PacingItemResourceCreate(BaseModel):
    resource_library_item_id: uuid.UUID


class ResourceLinkCreate(BaseModel):
    title: str = Field(min_length=1, max_length=160)
    description: str | None = Field(default=None, max_length=4000)
    external_url: str = Field(min_length=1, max_length=2048)


class TeacherAssistExtractionJobOut(BaseModel):
    id: uuid.UUID
    artifact_type: TeacherAssistExtractionArtifactTypeLiteral
    resource_library_item_id: uuid.UUID | None = None
    student_work_submission_id: uuid.UUID | None = None
    assignment_id: uuid.UUID | None = None
    school_year_id: uuid.UUID | None = None
    grading_period_id: uuid.UUID | None = None
    class_id: uuid.UUID | None = None
    subject_id: uuid.UUID | None = None
    student_number: int | None = None
    status: TeacherAssistExtractionJobStatusLiteral
    progress_percent: int = 0
    provider_name: str | None = None
    provider_model: str | None = None
    provider_version: str | None = None
    provider_mode: Literal["mock", "real"] | None = None
    page_count: int | None = None
    processing_duration_ms: int | None = None
    estimated_cost_cents: int | None = None
    error_code: str | None = None
    error_message: str | None = None
    error_metadata_json: dict[str, Any] | None = None
    retry_count: int = 0
    max_retries: int = 0
    parent_extraction_job_id: uuid.UUID | None = None
    retry_root_job_id: uuid.UUID | None = None
    attempt_number: int = 1
    leased_by_worker: str | None = None
    lease_expires_at: datetime | None = None
    heartbeat_at: datetime | None = None
    execution_log_json: list[dict[str, Any]] | None = None
    created_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    updated_at: datetime


class TeacherAssistExtractedTextRecordOut(BaseModel):
    id: uuid.UUID
    extraction_job_id: uuid.UUID
    artifact_type: TeacherAssistExtractionArtifactTypeLiteral
    resource_library_item_id: uuid.UUID | None = None
    student_work_submission_id: uuid.UUID | None = None
    assignment_id: uuid.UUID | None = None
    class_id: uuid.UUID | None = None
    subject_id: uuid.UUID | None = None
    student_number: int | None = None
    preview_text: str
    text_char_count: int
    pii_flagged: bool = False
    redaction_applied: bool = False
    review_status: ExtractionReviewStatusLiteral = "pending_review"
    provider_confidence_score: float | None = None
    confidence_level: ExtractionConfidenceLevelLiteral = "unknown"
    teacher_corrected_text: str | None = None
    approved_text: str | None = None
    reviewed_at: datetime | None = None
    reviewed_by_user_id: uuid.UUID | None = None
    source_extraction_job_id: uuid.UUID | None = None
    teacher_review_notes: str | None = None
    teacher_issue_reason: str | None = None
    metadata_json: dict[str, Any] | None = None
    created_at: datetime
    updated_at: datetime


class TeacherAssistExtractedTextDetailOut(TeacherAssistExtractedTextRecordOut):
    extracted_text: str


class TeacherAssistExtractedTextReviewStatusUpdate(BaseModel):
    review_status: ExtractionReviewStatusLiteral
    teacher_review_notes: str | None = Field(default=None, max_length=4000)
    teacher_issue_reason: str | None = Field(default=None, max_length=4000)


class TeacherAssistExtractedTextApprovedTextUpdate(BaseModel):
    approved_text: str | None = None
    teacher_corrected_text: str | None = None


class TeacherAssistExtractionSummaryOut(BaseModel):
    job: TeacherAssistExtractionJobOut
    extracted_text: TeacherAssistExtractedTextRecordOut | None = None
    retry_eligible: bool = False
    processing_duration_seconds: int | None = None


class TeacherAssistExtractedTextHistoryOut(BaseModel):
    current_record: TeacherAssistExtractedTextDetailOut
    current_job: TeacherAssistExtractionJobOut
    attempt_jobs: list[TeacherAssistExtractionJobOut] = Field(default_factory=list)
    attempt_records: list[TeacherAssistExtractedTextRecordOut] = Field(default_factory=list)
    activity_events: list["TeacherAssistActivityEventOut"] = Field(default_factory=list)


class TeacherAssistExtractedTextDetailAggregateOut(BaseModel):
    record: TeacherAssistExtractedTextDetailOut
    job: TeacherAssistExtractionJobOut
    lineage_jobs: list[TeacherAssistExtractionJobOut] = Field(default_factory=list)
    retry_eligible: bool = False
    cancel_eligible: bool = False
    processing_duration_seconds: int | None = None
    activity_events: list["TeacherAssistActivityEventOut"] = Field(default_factory=list)


class TeacherAssistExtractionRunOut(BaseModel):
    job: TeacherAssistExtractionJobOut
    extracted_text: TeacherAssistExtractedTextRecordOut | None = None


class TeacherAssistExtractionSourceArtifactOut(BaseModel):
    artifact_type: TeacherAssistExtractionArtifactTypeLiteral
    original_filename: str
    mime_type: str
    file_size: int
    resource_library_item_id: uuid.UUID | None = None
    student_work_submission_id: uuid.UUID | None = None
    assignment_id: uuid.UUID | None = None
    student_number: int | None = None


class TeacherAssistExtractionJobDetailOut(BaseModel):
    job: TeacherAssistExtractionJobOut
    extracted_text: TeacherAssistExtractedTextRecordOut | None = None
    lineage_jobs: list[TeacherAssistExtractionJobOut] = Field(default_factory=list)
    retry_eligible: bool = False
    cancel_eligible: bool = False
    processing_duration_seconds: int | None = None
    execution_timeline: list[dict[str, Any]] = Field(default_factory=list)
    source_artifact: TeacherAssistExtractionSourceArtifactOut
    activity_events: list["TeacherAssistActivityEventOut"] = Field(default_factory=list)


class TeacherAssistExtractionJobCancelUpdate(BaseModel):
    status: Literal["cancelled"]


class ResourceOut(BaseModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    uploaded_by_user_id: uuid.UUID
    title: str
    description: str | None = None
    resource_type: ResourceTypeLiteral
    storage_key: str | None = None
    original_filename: str | None = None
    mime_type: str | None = None
    file_size: int | None = None
    external_url: str | None = None
    uploaded_at: datetime
    linked_pacing_items_count: int = 0
    linked_planning_drafts_count: int = 0
    latest_extraction_job: TeacherAssistExtractionJobOut | None = None
    latest_extracted_text: TeacherAssistExtractedTextRecordOut | None = None
    created_at: datetime
    updated_at: datetime


class TeacherAssistFileDownloadOut(BaseModel):
    url: str
    expires_at: datetime


class AssignmentCreate(BaseModel):
    school_year_id: uuid.UUID
    grading_period_id: uuid.UUID | None = None
    class_id: uuid.UUID
    subject_id: uuid.UUID
    title: str = Field(min_length=1, max_length=160)
    description: str | None = Field(default=None, max_length=4000)
    assignment_type: AssignmentTypeLiteral = "other"
    due_date: date | None = None
    status: AssignmentStatusLiteral = "draft"
    instructions: str | None = Field(default=None, max_length=4000)
    rubric_json: dict[str, Any] | None = None
    source_plan_id: uuid.UUID | None = None
    source_context_json: dict[str, Any] | None = None
    standard_ids: list[uuid.UUID] = Field(default_factory=list)
    resource_ids: list[uuid.UUID] = Field(default_factory=list)


class AssignmentStatusUpdate(BaseModel):
    status: AssignmentStatusLiteral


class AssignmentStandardCreate(BaseModel):
    standard_id: uuid.UUID


class AssignmentResourceCreate(BaseModel):
    resource_library_item_id: uuid.UUID


class AssignmentOut(BaseModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    teacher_user_id: uuid.UUID
    school_year_id: uuid.UUID
    grading_period_id: uuid.UUID | None = None
    class_id: uuid.UUID
    subject_id: uuid.UUID
    title: str
    description: str | None = None
    assignment_type: AssignmentTypeLiteral
    due_date: date | None = None
    status: AssignmentStatusLiteral
    instructions: str | None = None
    rubric_json: dict[str, Any] | None = None
    source_plan_id: uuid.UUID | None = None
    source_context_json: dict[str, Any] | None = None
    standard_ids: list[uuid.UUID] = Field(default_factory=list)
    resource_ids: list[uuid.UUID] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime


class AssignmentPrintPacketCreate(BaseModel):
    pages_per_student: int = Field(default=1, ge=1)
    template_type: AssignmentPrintTemplateTypeLiteral = "blank_writing_page"
    output_format: AssignmentPrintOutputFormatLiteral = "html"


class AssignmentPrintPacketOut(BaseModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    teacher_user_id: uuid.UUID
    assignment_id: uuid.UUID
    class_id: uuid.UUID
    school_year_id: uuid.UUID
    grading_period_id: uuid.UUID | None = None
    subject_id: uuid.UUID
    packet_status: AssignmentPrintPacketStatusLiteral
    pages_per_student: int
    student_count: int
    template_type: AssignmentPrintTemplateTypeLiteral
    output_format: AssignmentPrintOutputFormatLiteral
    storage_key: str | None = None
    total_page_count: int
    created_at: datetime
    updated_at: datetime


class AssignmentPrintPageOut(BaseModel):
    id: uuid.UUID
    packet_id: uuid.UUID
    assignment_id: uuid.UUID
    student_number: int
    page_number: int
    qr_payload_json: dict[str, Any]
    qr_token: str
    qr_svg_data_uri: str
    created_at: datetime


class AssignmentStudentWorkOut(BaseModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    teacher_user_id: uuid.UUID
    assignment_id: uuid.UUID
    assignment_print_packet_id: uuid.UUID | None = None
    assignment_print_page_id: uuid.UUID | None = None
    school_year_id: uuid.UUID
    grading_period_id: uuid.UUID | None = None
    class_id: uuid.UUID
    subject_id: uuid.UUID
    student_number: int
    original_filename: str
    mime_type: str
    file_size: int
    storage_key: str
    upload_status: AssignmentStudentWorkUploadStatusLiteral
    processing_status: AssignmentStudentWorkProcessingStatusLiteral
    latest_extraction_job: TeacherAssistExtractionJobOut | None = None
    latest_extracted_text: TeacherAssistExtractedTextRecordOut | None = None
    created_at: datetime
    updated_at: datetime


class AssignmentStudentWorkStatusUpdate(BaseModel):
    processing_status: AssignmentStudentWorkProcessingStatusLiteral


class AssignmentStudentWorkPacketContextUpdate(BaseModel):
    assignment_print_packet_id: uuid.UUID | None = None
    assignment_print_page_id: uuid.UUID | None = None


class AssignmentGradingReviewItemInput(BaseModel):
    criterion_title: str = Field(min_length=1, max_length=160)
    score_suggestion: float | None = None
    max_score: float | None = Field(default=None, ge=0)
    feedback_summary: str | None = Field(default=None, max_length=4000)
    strengths: list[str] = Field(default_factory=list)
    improvement_areas: list[str] = Field(default_factory=list)
    teacher_notes: str | None = Field(default=None, max_length=4000)
    sort_order: int = Field(default=0, ge=0)


class AssignmentGradingReviewCreate(BaseModel):
    student_number: int = Field(ge=1)
    score_suggestion: float | None = None
    max_score: float | None = Field(default=None, ge=0)
    feedback_summary: str | None = Field(default=None, max_length=4000)
    strengths: list[str] = Field(default_factory=list)
    improvement_areas: list[str] = Field(default_factory=list)
    teacher_notes: str | None = Field(default=None, max_length=4000)
    items: list[AssignmentGradingReviewItemInput] = Field(default_factory=list)


class AssignmentGradingReviewUpdate(BaseModel):
    status: AssignmentGradingReviewStatusLiteral = "draft"
    score_suggestion: float | None = None
    max_score: float | None = Field(default=None, ge=0)
    feedback_summary: str | None = Field(default=None, max_length=4000)
    strengths: list[str] = Field(default_factory=list)
    improvement_areas: list[str] = Field(default_factory=list)
    teacher_notes: str | None = Field(default=None, max_length=4000)
    teacher_confirmed_score: float | None = None
    teacher_confirmed_feedback: str | None = Field(default=None, max_length=4000)
    items: list[AssignmentGradingReviewItemInput] = Field(default_factory=list)


class AssignmentGradingReviewStatusUpdate(BaseModel):
    status: AssignmentGradingReviewStatusLiteral


class AssignmentGradingReviewAISuggestionCreate(BaseModel):
    provider_mode: Literal["mock", "real"] = "mock"
    teacher_instructions: str | None = Field(default=None, max_length=1000)


class AssignmentGradingReviewAISuggestionOut(BaseModel):
    review: AssignmentGradingReviewOut
    confidence_level: Literal["low", "medium", "high"]
    teacher_review_required: bool = True
    rubric_notes: str | None = None
    text_source: str | None = None
    message: str


class AssignmentGradingReviewItemOut(BaseModel):
    id: uuid.UUID
    grading_review_id: uuid.UUID
    criterion_title: str
    score_suggestion: float | None = None
    max_score: float | None = None
    feedback_summary: str | None = None
    strengths: list[str] = Field(default_factory=list)
    improvement_areas: list[str] = Field(default_factory=list)
    teacher_notes: str | None = None
    sort_order: int
    created_at: datetime
    updated_at: datetime


class AssignmentGradingReviewOut(BaseModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    teacher_user_id: uuid.UUID
    assignment_id: uuid.UUID
    student_work_submission_id: uuid.UUID
    student_number: int
    school_year_id: uuid.UUID
    grading_period_id: uuid.UUID | None = None
    class_id: uuid.UUID
    subject_id: uuid.UUID
    status: AssignmentGradingReviewStatusLiteral
    review_source: AssignmentGradingReviewSourceLiteral
    provider_name: str | None = None
    provider_model: str | None = None
    prompt_version: str | None = None
    ai_usage_event_id: uuid.UUID | None = None
    score_suggestion: float | None = None
    max_score: float | None = None
    feedback_summary: str | None = None
    strengths: list[str] = Field(default_factory=list)
    improvement_areas: list[str] = Field(default_factory=list)
    teacher_notes: str | None = None
    teacher_confirmed_score: float | None = None
    teacher_confirmed_feedback: str | None = None
    items: list[AssignmentGradingReviewItemOut] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime


class AssignmentGradebookCommitCreate(BaseModel):
    teacher_confirmation_note: str | None = Field(default=None, max_length=1000)


class AssignmentGradeRecordCorrectionCreate(BaseModel):
    committed_score: float | None = None
    max_score: float | None = None
    committed_feedback: str | None = Field(default=None, max_length=4000)
    reason: str = Field(min_length=3, max_length=1000)


class AssignmentGradeRecordReversalCreate(BaseModel):
    reason: str = Field(min_length=3, max_length=1000)


class AssignmentGradebookCommitOut(BaseModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    teacher_user_id: uuid.UUID
    grade_record_id: uuid.UUID
    assignment_id: uuid.UUID
    student_work_submission_id: uuid.UUID
    grading_review_id: uuid.UUID
    student_number: int
    school_year_id: uuid.UUID
    grading_period_id: uuid.UUID | None = None
    class_id: uuid.UUID
    subject_id: uuid.UUID
    commit_type: AssignmentGradebookCommitTypeLiteral
    commit_status: AssignmentGradebookCommitStatusLiteral
    committed_score: float | None = None
    max_score: float | None = None
    committed_feedback: str | None = None
    teacher_confirmation_checkpoint_at: datetime
    reason: str | None = None
    supersedes_commit_id: uuid.UUID | None = None
    reversed_by_commit_id: uuid.UUID | None = None
    audit_metadata_json: dict[str, Any] | None = None
    created_at: datetime


class AssignmentGradeRecordOut(BaseModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    teacher_user_id: uuid.UUID
    assignment_id: uuid.UUID
    student_work_submission_id: uuid.UUID
    grading_review_id: uuid.UUID
    student_number: int
    school_year_id: uuid.UUID
    grading_period_id: uuid.UUID | None = None
    class_id: uuid.UUID
    subject_id: uuid.UUID
    record_status: AssignmentGradeRecordStatusLiteral
    current_commit_id: uuid.UUID | None = None
    committed_score: float | None = None
    max_score: float | None = None
    committed_feedback: str | None = None
    created_at: datetime
    updated_at: datetime


class AssignmentGradeRecordDetailOut(BaseModel):
    record: AssignmentGradeRecordOut
    commits: list[AssignmentGradebookCommitOut] = Field(default_factory=list)
    audit_events: list["AssignmentGradebookAuditEventOut"] = Field(default_factory=list)


class AssignmentGradebookAuditEventOut(BaseModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    teacher_user_id: uuid.UUID
    grade_record_id: uuid.UUID | None = None
    gradebook_commit_id: uuid.UUID | None = None
    assignment_id: uuid.UUID
    student_number: int
    event_type: AssignmentGradebookAuditEventTypeLiteral
    summary_text: str
    details_json: dict[str, Any] | None = None
    created_at: datetime


class AssignmentGradebookCommitResultOut(BaseModel):
    grade_record: AssignmentGradeRecordOut
    commit: AssignmentGradebookCommitOut
    message: str


class AssignmentGradebookExportViewOut(BaseModel):
    assignment_id: uuid.UUID
    assignment_title: str
    assignment_type: str
    class_id: uuid.UUID
    subject_id: uuid.UUID
    school_year_id: uuid.UUID
    grading_period_id: uuid.UUID | None = None
    generated_at: datetime
    record_count: int
    active_record_count: int
    records: list[dict[str, Any]] = Field(default_factory=list)
    commits: list[dict[str, Any]] = Field(default_factory=list)


class MasteryMatrixStandardOut(BaseModel):
    id: uuid.UUID
    standard_id: uuid.UUID
    display_order: int
    target_mastery_level: MasteryLevelLiteral
    assessment_count: int
    standard_code: str | None = None
    standard_description: str | None = None


class MasteryMatrixOut(BaseModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    owner_user_id: uuid.UUID
    school_year_id: uuid.UUID
    grading_period_id: uuid.UUID | None = None
    class_id: uuid.UUID
    subject_id: uuid.UUID
    title: str
    status: MasteryMatrixStatusLiteral
    created_at: datetime
    updated_at: datetime
    standards: list[MasteryMatrixStandardOut] = Field(default_factory=list)


class MasteryMatrixCreate(BaseModel):
    school_year_id: uuid.UUID
    grading_period_id: uuid.UUID | None = None
    class_id: uuid.UUID
    subject_id: uuid.UUID
    title: str = Field(min_length=1, max_length=255)
    status: MasteryMatrixStatusLiteral = "active"
    standard_ids: list[uuid.UUID] = Field(min_length=1)
    target_mastery_level: MasteryLevelLiteral = "mastery"


class MasteryMatrixUpdate(BaseModel):
    title: str | None = Field(default=None, min_length=1, max_length=255)
    status: MasteryMatrixStatusLiteral | None = None
    standard_ids: list[uuid.UUID] | None = None
    target_mastery_level: MasteryLevelLiteral | None = None


class MasteryEvaluationOut(BaseModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    owner_user_id: uuid.UUID
    mastery_matrix_id: uuid.UUID
    student_number: int
    standard_id: uuid.UUID
    evaluation_status: MasteryEvaluationStatusLiteral
    mastery_level: MasteryLevelLiteral
    confidence_level: MasteryConfidenceLevelLiteral | None = None
    evidence_source_type: MasteryEvidenceSourceTypeLiteral | None = None
    evidence_source_id: uuid.UUID | None = None
    teacher_notes: str | None = None
    confirmed_by_user_id: uuid.UUID | None = None
    confirmed_at: datetime | None = None
    current_commit_id: uuid.UUID | None = None
    created_at: datetime
    updated_at: datetime


class MasteryEvaluationCreate(BaseModel):
    mastery_matrix_id: uuid.UUID
    student_number: int = Field(ge=1)
    standard_id: uuid.UUID
    mastery_level: MasteryLevelLiteral
    confidence_level: MasteryConfidenceLevelLiteral | None = None
    evidence_source_type: MasteryEvidenceSourceTypeLiteral | None = None
    evidence_source_id: uuid.UUID | None = None
    teacher_notes: str | None = Field(default=None, max_length=4000)


class MasteryEvaluationUpdate(BaseModel):
    mastery_level: MasteryLevelLiteral | None = None
    confidence_level: MasteryConfidenceLevelLiteral | None = None
    evidence_source_type: MasteryEvidenceSourceTypeLiteral | None = None
    evidence_source_id: uuid.UUID | None = None
    teacher_notes: str | None = Field(default=None, max_length=4000)
    clear_evidence: bool = False


class MasteryCommitOut(BaseModel):
    id: uuid.UUID
    mastery_evaluation_id: uuid.UUID
    mastery_matrix_id: uuid.UUID
    student_number: int
    standard_id: uuid.UUID
    commit_type: MasteryCommitTypeLiteral
    commit_status: MasteryCommitStatusLiteral
    previous_mastery_level: MasteryLevelLiteral | None = None
    new_mastery_level: MasteryLevelLiteral
    confidence_level: MasteryConfidenceLevelLiteral | None = None
    evidence_source_type: MasteryEvidenceSourceTypeLiteral | None = None
    evidence_source_id: uuid.UUID | None = None
    teacher_notes: str | None = None
    commit_reason: str | None = None
    supersedes_commit_id: uuid.UUID | None = None
    reversed_by_commit_id: uuid.UUID | None = None
    reversed_at: datetime | None = None
    reversed_by_user_id: uuid.UUID | None = None
    created_at: datetime


class MasteryEvaluationCommitCreate(BaseModel):
    commit_reason: str | None = Field(default=None, max_length=4000)


class MasteryEvaluationCorrectionCreate(BaseModel):
    mastery_level: MasteryLevelLiteral
    confidence_level: MasteryConfidenceLevelLiteral | None = None
    teacher_notes: str | None = Field(default=None, max_length=4000)
    commit_reason: str = Field(min_length=1, max_length=4000)


class MasteryEvaluationReversalCreate(BaseModel):
    commit_reason: str = Field(min_length=1, max_length=4000)


class MasteryCommitResultOut(BaseModel):
    evaluation: MasteryEvaluationOut
    commit: MasteryCommitOut
    message: str


class MasteryEvaluationDetailOut(BaseModel):
    evaluation: MasteryEvaluationOut
    commits: list[MasteryCommitOut] = Field(default_factory=list)


class MasteryMatrixSummaryOut(BaseModel):
    mastery_matrix_id: uuid.UUID
    title: str
    status: MasteryMatrixStatusLiteral
    class_id: uuid.UUID
    subject_id: uuid.UUID
    school_year_id: uuid.UUID
    grading_period_id: uuid.UUID | None = None
    tracked_standard_count: int
    active_evaluation_count: int
    draft_evaluation_count: int
    reversed_evaluation_count: int
    student_count: int
    unassessed_standard_count: int
    reteach_candidate_count: int
    mastery_distribution: dict[str, int]


class MasteryMatrixStandardsSummaryOut(BaseModel):
    mastery_matrix_id: uuid.UUID
    standards: list[dict[str, Any]] = Field(default_factory=list)


class MasteryMatrixStudentsSummaryOut(BaseModel):
    mastery_matrix_id: uuid.UUID
    students: list[dict[str, Any]] = Field(default_factory=list)


class MasteryMatrixReteachSummaryOut(BaseModel):
    mastery_matrix_id: uuid.UUID
    reteach_candidate_count: int
    unassessed_standard_count: int
    reteach_items: list[dict[str, Any]] = Field(default_factory=list)
    unassessed_standards: list[dict[str, Any]] = Field(default_factory=list)


class MasteryMatrixHeatmapOut(BaseModel):
    mastery_matrix_id: uuid.UUID
    title: str
    class_id: uuid.UUID
    subject_id: uuid.UUID
    school_year_id: uuid.UUID
    grading_period_id: uuid.UUID | None = None
    standards: list[dict[str, Any]] = Field(default_factory=list)
    student_numbers: list[int] = Field(default_factory=list)
    rows: list[dict[str, Any]] = Field(default_factory=list)
    mastery_distribution: dict[str, int] = Field(default_factory=dict)
    active_evaluation_count: int = 0
    student_count: int = 0


class MasteryMatrixReteachInsightsOut(BaseModel):
    mastery_matrix_id: uuid.UUID
    title: str
    class_id: uuid.UUID
    subject_id: uuid.UUID
    school_year_id: uuid.UUID
    grading_period_id: uuid.UUID | None = None
    standard_insights: list[dict[str, Any]] = Field(default_factory=list)
    status_counts: dict[str, int] = Field(default_factory=dict)
    panels: dict[str, list[dict[str, Any]]] = Field(default_factory=dict)


class StudentMasterySummaryOut(BaseModel):
    mastery_matrix_id: uuid.UUID
    student_number: int
    trend: StudentMasteryTrendLiteral
    active_evaluation_count: int = 0
    average_mastery_rank: float | None = None
    recent_assessment_count: int = 0
    recent_assignment_count: int = 0
    mastery_states: list[dict[str, Any]] = Field(default_factory=list)
    standards_needing_attention: list[dict[str, Any]] = Field(default_factory=list)
    latest_assignment_evidence: list[dict[str, Any]] = Field(default_factory=list)
    latest_grading_review_references: list[dict[str, Any]] = Field(default_factory=list)
    latest_gradebook_commit_references: list[dict[str, Any]] = Field(default_factory=list)


class AssignmentEffectivenessOut(BaseModel):
    assignment_id: uuid.UUID
    assignment_title: str
    class_id: uuid.UUID
    subject_id: uuid.UUID
    school_year_id: uuid.UUID
    grading_period_id: uuid.UUID | None = None
    linked_standards: list[dict[str, Any]] = Field(default_factory=list)
    mastery_distribution: dict[str, int] = Field(default_factory=dict)
    developing_or_beginning_count: int = 0
    average_mastery_rank: float | None = None
    mastery_percentage: float = 0.0
    total_committed_evaluations: int = 0
    grading_review_count: int = 0
    gradebook_commit_count: int = 0
    effectiveness_status: AssignmentEffectivenessStatusLiteral


class MasteryDashboardOut(BaseModel):
    filters: dict[str, uuid.UUID | None] = Field(default_factory=dict)
    matrix_count: int = 0
    active_evaluation_count: int = 0
    student_count: int = 0
    mastery_distribution: dict[str, int] = Field(default_factory=dict)
    matrix_snapshots: list[dict[str, Any]] = Field(default_factory=list)
    standards_needing_attention: list[dict[str, Any]] = Field(default_factory=list)
    reteach_recommended_standards: list[dict[str, Any]] = Field(default_factory=list)
    low_mastery_alerts: list[dict[str, Any]] = Field(default_factory=list)
    improving_standards: list[dict[str, Any]] = Field(default_factory=list)
    declining_standards: list[dict[str, Any]] = Field(default_factory=list)
    unassessed_standards: list[dict[str, Any]] = Field(default_factory=list)


class TeacherAssistWorkspaceMasteryInsightsOut(BaseModel):
    matrix_count: int = 0
    active_evaluation_count: int = 0
    reteach_recommended_count: int = 0
    low_mastery_alert_count: int = 0
    unassessed_standard_count: int = 0
    improving_standard_count: int = 0
    declining_standard_count: int = 0
    reteach_recommended_standards: list[dict[str, Any]] = Field(default_factory=list)
    standards_needing_attention: list[dict[str, Any]] = Field(default_factory=list)
    low_mastery_alerts: list[dict[str, Any]] = Field(default_factory=list)
    improving_standards: list[dict[str, Any]] = Field(default_factory=list)
    declining_standards: list[dict[str, Any]] = Field(default_factory=list)
    unassessed_standards: list[dict[str, Any]] = Field(default_factory=list)
    class_snapshots: list[dict[str, Any]] = Field(default_factory=list)


class ReteachPlanCreate(BaseModel):
    mastery_matrix_id: uuid.UUID
    standard_id: uuid.UUID
    title: str | None = Field(default=None, max_length=255)


class ReteachPlanUpdate(BaseModel):
    title: str | None = Field(default=None, max_length=255)
    status: ReteachPlanStatusLiteral | None = None


class ReteachPlanVersionCreate(BaseModel):
    content_json: dict[str, Any]
    change_reason: str | None = Field(default=None, max_length=4000)


class ReteachPlanAIDraftCreate(BaseModel):
    provider_mode: Literal["mock", "real"] = "mock"
    teacher_instructions: str | None = Field(default=None, max_length=4000)


class ReteachPlanVersionOut(BaseModel):
    id: uuid.UUID
    reteach_plan_id: uuid.UUID
    version_number: int
    version_source: ReteachPlanVersionSourceLiteral
    content_json: dict[str, Any]
    prompt_context_json: dict[str, Any] | None = None
    provider_name: str | None = None
    provider_model: str | None = None
    prompt_version: str | None = None
    ai_usage_event_id: uuid.UUID | None = None
    created_by_user_id: uuid.UUID
    change_reason: str | None = None
    created_at: datetime


class ReteachPlanOut(BaseModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    owner_user_id: uuid.UUID
    mastery_matrix_id: uuid.UUID
    standard_id: uuid.UUID
    school_year_id: uuid.UUID
    grading_period_id: uuid.UUID | None = None
    class_id: uuid.UUID
    subject_id: uuid.UUID
    title: str
    status: ReteachPlanStatusLiteral
    current_version_id: uuid.UUID | None = None
    latest_ai_usage_event_id: uuid.UUID | None = None
    created_at: datetime
    updated_at: datetime
    standard_code: str | None = None
    standard_description: str | None = None


class ReteachPlanAIDraftOut(BaseModel):
    plan: ReteachPlanOut
    version: ReteachPlanVersionOut
    teacher_review_required: bool = True
    provider_mode: str
    prompt_version: str
    message: str


class NewsletterCreate(BaseModel):
    school_year_id: uuid.UUID
    grading_period_id: uuid.UUID | None = None
    class_id: uuid.UUID
    subject_id: uuid.UUID
    title: str | None = Field(default=None, max_length=255)
    teacher_notes: str | None = Field(default=None, max_length=4000)
    week_start_date: date | None = None
    week_end_date: date | None = None


class NewsletterUpdate(BaseModel):
    title: str | None = Field(default=None, max_length=255)
    status: NewsletterStatusLiteral | None = None
    teacher_notes: str | None = Field(default=None, max_length=4000)
    week_start_date: date | None = None
    week_end_date: date | None = None


class NewsletterVersionCreate(BaseModel):
    content_json: dict[str, Any]
    change_reason: str | None = Field(default=None, max_length=4000)


class NewsletterAIDraftCreate(BaseModel):
    provider_mode: Literal["mock", "real"] = "mock"
    teacher_instructions: str | None = Field(default=None, max_length=4000)


class NewsletterSectionRegenerateCreate(BaseModel):
    section: NewsletterRegeneratableSectionLiteral
    provider_mode: Literal["mock", "real"] = "mock"
    teacher_instructions: str | None = Field(default=None, max_length=4000)


class NewsletterExportCreate(BaseModel):
    export_format: NewsletterExportFormatLiteral


class NewsletterVersionOut(BaseModel):
    id: uuid.UUID
    newsletter_id: uuid.UUID
    version_number: int
    version_source: NewsletterVersionSourceLiteral
    content_json: dict[str, Any]
    prompt_context_json: dict[str, Any] | None = None
    provider_name: str | None = None
    provider_model: str | None = None
    prompt_version: str | None = None
    ai_usage_event_id: uuid.UUID | None = None
    created_by_user_id: uuid.UUID
    change_reason: str | None = None
    created_at: datetime


class NewsletterOut(BaseModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    owner_user_id: uuid.UUID
    school_year_id: uuid.UUID
    grading_period_id: uuid.UUID | None = None
    class_id: uuid.UUID
    subject_id: uuid.UUID
    title: str
    status: NewsletterStatusLiteral
    week_start_date: date | None = None
    week_end_date: date | None = None
    teacher_notes: str | None = None
    current_version_id: uuid.UUID | None = None
    latest_ai_usage_event_id: uuid.UUID | None = None
    created_at: datetime
    updated_at: datetime
    subject_name: str | None = None
    class_name: str | None = None


class NewsletterAIDraftOut(BaseModel):
    newsletter: NewsletterOut
    version: NewsletterVersionOut
    teacher_review_required: bool = True
    provider_mode: str
    prompt_version: str
    message: str


class NewsletterSectionRegenerateOut(BaseModel):
    newsletter: NewsletterOut
    version: NewsletterVersionOut
    teacher_review_required: bool = True
    provider_mode: str
    prompt_version: str
    section: str
    message: str


class NewsletterExportOut(BaseModel):
    id: uuid.UUID
    newsletter_id: uuid.UUID
    newsletter_version_id: uuid.UUID | None = None
    export_format: NewsletterExportFormatLiteral
    file_size_bytes: int
    created_at: datetime
    download_filename: str


class NewsletterExportDownloadOut(BaseModel):
    export_id: uuid.UUID
    newsletter_id: uuid.UUID
    export_format: str
    mime_type: str
    download_filename: str
    download_url: str


class PlanningDraftCreate(BaseModel):
    planning_scope: PlanningScopeLiteral = "weekly"
    school_year_id: uuid.UUID | None = None
    grading_period_id: uuid.UUID | None = None
    class_id: uuid.UUID | None = None
    subject_id: uuid.UUID | None = None
    subject_ids: list[uuid.UUID] = Field(default_factory=list)
    pacing_item_ids: list[uuid.UUID] = Field(default_factory=list)
    standard_ids: list[uuid.UUID] = Field(default_factory=list)
    title: str | None = Field(default=None, max_length=160)
    plan_title: str | None = Field(default=None, max_length=160)
    module_title: str | None = Field(default=None, max_length=160)
    start_date: date | None = None
    end_date: date | None = None
    estimated_weeks: int | None = Field(default=None, ge=1)
    instructional_days_count: int | None = Field(default=None, ge=1)
    notes: str | None = Field(default=None, max_length=4000)
    status: PlanningDraftStatusLiteral = "draft"


class PlanningDraftOut(BaseModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    user_id: uuid.UUID
    planning_scope: PlanningScopeLiteral = "weekly"
    school_year_id: uuid.UUID | None = None
    grading_period_id: uuid.UUID | None = None
    class_id: uuid.UUID | None = None
    subject_id: uuid.UUID | None = None
    subject_ids: list[uuid.UUID] = Field(default_factory=list)
    pacing_item_ids: list[uuid.UUID] = Field(default_factory=list)
    standard_ids: list[uuid.UUID] = Field(default_factory=list)
    title: str | None = None
    plan_title: str | None = None
    module_title: str | None = None
    start_date: date | None = None
    end_date: date | None = None
    estimated_weeks: int | None = None
    instructional_days_count: int | None = None
    notes: str | None = None
    status: PlanningDraftStatusLiteral
    resource_ids: list[uuid.UUID] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime


class PlanningDraftResourceCreate(BaseModel):
    resource_library_item_id: uuid.UUID


class PlanningDraftStatusUpdate(BaseModel):
    status: PlanningDraftStatusLiteral


class PlanningDraftReadinessOut(BaseModel):
    is_ready: bool
    missing_items: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class PlanningDurationSummaryOut(BaseModel):
    start_date: date | None = None
    end_date: date | None = None
    estimated_weeks: int | None = None
    instructional_days_count: int | None = None
    summary: str


class PlanningPacingGroupOut(BaseModel):
    group_key: str
    label: str
    pacing_items: list[PacingItemOut] = Field(default_factory=list)


class PlanningDraftContextPreviewOut(BaseModel):
    draft: PlanningDraftOut
    school_year: SchoolYearOut | None = None
    grading_period: GradingPeriodOut | None = None
    class_context: ClassOut | None = Field(default=None, alias="class")
    subjects: list[SubjectOut] = Field(default_factory=list)
    pacing_items: list[PacingItemOut] = Field(default_factory=list)
    pacing_groups: list[PlanningPacingGroupOut] = Field(default_factory=list)
    standards: list[StandardOut] = Field(default_factory=list)
    resources: list[ResourceOut] = Field(default_factory=list)
    teacher_notes: str | None = None
    duration_summary: PlanningDurationSummaryOut
    readiness: PlanningDraftReadinessOut

    model_config = {"populate_by_name": True}


class PlanningDraftGenerationPreviewOut(BaseModel):
    message: str
    draft_id: uuid.UUID
    ready: bool


class TeacherAssistWorkflowStepOut(BaseModel):
    id: uuid.UUID
    workflow_id: uuid.UUID
    step_name: str
    status: TeacherAssistWorkflowStepStatusLiteral
    metadata_json: dict[str, object] | None = None
    error_message: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    created_at: datetime


class TeacherAssistWorkflowOut(BaseModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    user_id: uuid.UUID
    planning_input_draft_id: uuid.UUID | None = None
    workflow_type: TeacherAssistWorkflowTypeLiteral
    status: TeacherAssistWorkflowStatusLiteral
    input_snapshot_json: dict[str, object]
    output_ref_type: str | None = None
    output_ref_id: uuid.UUID | None = None
    error_message: str | None = None
    last_error_code: str | None = None
    progress_percent: int
    leased_by_worker: str | None = None
    lease_expires_at: datetime | None = None
    heartbeat_at: datetime | None = None
    retry_count: int = 0
    max_retries: int = 0
    timeout_at: datetime | None = None
    provider_name: str | None = None
    provider_model: str | None = None
    prompt_version: str | None = None
    input_tokens_total: int = 0
    output_tokens_total: int = 0
    estimated_cost_cents_total: int = 0
    execution_log_json: list[dict[str, Any]] = Field(default_factory=list)
    created_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    updated_at: datetime


class TeacherAssistWorkflowDetailOut(TeacherAssistWorkflowOut):
    steps: list[TeacherAssistWorkflowStepOut] = Field(default_factory=list)
    usage_events: list["TeacherAssistAIUsageEventOut"] = Field(default_factory=list)


class TeacherAssistAIUsageEventOut(BaseModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    user_id: uuid.UUID
    workflow_id: uuid.UUID | None = None
    provider: str
    model: str | None = None
    feature: str
    input_tokens: int | None = None
    output_tokens: int | None = None
    estimated_cost_cents: int | None = None
    metadata_json: dict[str, Any] | None = None
    created_at: datetime


class WeeklyPlanVersionOut(BaseModel):
    id: uuid.UUID
    weekly_plan_id: uuid.UUID
    version_number: int
    content_json: dict[str, Any]
    source_context_json: dict[str, Any]
    created_by_user_id: uuid.UUID
    created_at: datetime
    change_reason: str | None = None


class WeeklyPlanOut(BaseModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    user_id: uuid.UUID
    owner_user_id: uuid.UUID
    planning_input_draft_id: uuid.UUID
    workflow_id: uuid.UUID | None = None
    planning_scope: PlanningScopeLiteral = "weekly"
    title: str
    plan_title: str
    module_title: str | None = None
    start_date: date | None = None
    end_date: date | None = None
    estimated_weeks: int | None = None
    instructional_days_count: int | None = None
    source_plan_id: uuid.UUID | None = None
    derived_from_plan_id: uuid.UUID | None = None
    is_template: bool = False
    visibility_scope: PlanVisibilityScopeLiteral = "private"
    reuse_status: PlanReuseStatusLiteral = "active"
    school_year_origin_id: uuid.UUID | None = None
    status: WeeklyPlanStatusLiteral
    content_json: dict[str, Any]
    source_context_json: dict[str, Any]
    current_version_number: int = 1
    latest_usage_event: TeacherAssistAIUsageEventOut | None = None
    created_at: datetime
    updated_at: datetime


class TeacherAssistActivityEventOut(BaseModel):
    id: uuid.UUID
    event_category: str
    event_type: str
    entity_type: str
    entity_id: uuid.UUID
    timestamp: datetime
    summary_text: str
    workflow_id: uuid.UUID | None = None
    school_year_id: uuid.UUID | None = None
    grading_period_id: uuid.UUID | None = None
    class_id: uuid.UUID | None = None
    subject_id: uuid.UUID | None = None
    details_json: dict[str, Any] | None = None
    created_at: datetime


class TeacherAssistWorkspacePlanSummaryOut(BaseModel):
    id: uuid.UUID
    title: str
    planning_scope: PlanningScopeLiteral = "weekly"
    status: WeeklyPlanStatusLiteral
    workflow_id: uuid.UUID | None = None
    class_id: uuid.UUID | None = None
    school_year_id: uuid.UUID | None = None
    review_required: bool = False
    quality_flags: list[str] = Field(default_factory=list)
    missing_context_warnings: list[str] = Field(default_factory=list)
    updated_at: datetime


class TeacherAssistWorkspaceAssignmentSummaryOut(BaseModel):
    id: uuid.UUID
    class_id: uuid.UUID
    subject_id: uuid.UUID
    title: str
    status: AssignmentStatusLiteral
    assignment_type: AssignmentTypeLiteral
    due_date: date | None = None
    updated_at: datetime


class TeacherAssistWorkspacePacketSummaryOut(BaseModel):
    id: uuid.UUID
    assignment_id: uuid.UUID
    class_id: uuid.UUID
    packet_status: AssignmentPrintPacketStatusLiteral
    pages_per_student: int
    student_count: int
    template_type: AssignmentPrintTemplateTypeLiteral
    created_at: datetime
    updated_at: datetime


class TeacherAssistWorkspaceSubmissionSummaryOut(BaseModel):
    id: uuid.UUID
    assignment_id: uuid.UUID
    class_id: uuid.UUID
    student_number: int
    original_filename: str
    upload_status: AssignmentStudentWorkUploadStatusLiteral
    processing_status: AssignmentStudentWorkProcessingStatusLiteral
    latest_extraction_status: TeacherAssistExtractionJobStatusLiteral | None = None
    extraction_ready_for_teacher_review: bool = False
    created_at: datetime
    updated_at: datetime


class TeacherAssistWorkspaceGradingReviewSummaryOut(BaseModel):
    id: uuid.UUID
    assignment_id: uuid.UUID
    student_work_submission_id: uuid.UUID
    class_id: uuid.UUID
    student_number: int
    status: AssignmentGradingReviewStatusLiteral
    teacher_confirmed_score: float | None = None
    updated_at: datetime


class TeacherAssistWorkspaceWorkflowSummaryOut(BaseModel):
    id: uuid.UUID
    workflow_type: TeacherAssistWorkflowTypeLiteral
    status: TeacherAssistWorkflowStatusLiteral
    class_id: uuid.UUID | None = None
    school_year_id: uuid.UUID | None = None
    grading_period_id: uuid.UUID | None = None
    progress_percent: int
    retry_count: int = 0
    max_retries: int = 0
    provider_name: str | None = None
    provider_model: str | None = None
    last_error_code: str | None = None
    heartbeat_at: datetime | None = None
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None = None
    error_message: str | None = None


class TeacherAssistWorkspaceNeedsAttentionOut(BaseModel):
    type: str
    severity: Literal["info", "warning", "critical"]
    title: str
    message: str
    entity_type: str
    entity_id: uuid.UUID
    class_id: uuid.UUID | None = None
    created_at: datetime


class TeacherAssistWorkspaceReviewRequiredItemOut(BaseModel):
    entity_type: str
    entity_id: uuid.UUID
    class_id: uuid.UUID | None = None
    title: str
    status: str
    review_reason: str
    updated_at: datetime


class TeacherAssistWorkspaceTodaySummaryOut(BaseModel):
    active_grading_period_title: str | None = None
    active_workflows_count: int = 0
    plans_needing_review_count: int = 0
    grading_reviews_pending_confirmation_count: int = 0
    recent_uploads_count: int = 0
    workflow_failures_count: int = 0
    extraction_failures_count: int = 0
    student_work_ready_for_extraction_count: int = 0
    extracted_artifacts_ready_for_teacher_review_count: int = 0
    low_confidence_extractions_count: int = 0
    rejected_extractions_count: int = 0
    retry_required_extractions_count: int = 0
    awaiting_teacher_review_count: int = 0
    stale_extraction_jobs_count: int = 0
    recently_approved_extractions_count: int = 0


class TeacherAssistWorkspaceStatsOut(BaseModel):
    active_plans_count: int = 0
    plans_in_review_count: int = 0
    pending_grading_reviews_count: int = 0
    recent_upload_count: int = 0
    workflow_failure_count: int = 0
    assignments_in_review_count: int = 0
    extraction_failure_count: int = 0
    student_work_ready_for_extraction_count: int = 0
    extracted_artifacts_ready_for_teacher_review_count: int = 0
    low_confidence_extractions_count: int = 0
    rejected_extractions_count: int = 0
    retry_required_extractions_count: int = 0
    awaiting_teacher_review_count: int = 0
    stale_extraction_jobs_count: int = 0
    recently_approved_extractions_count: int = 0


class TeacherAssistClassWorkspaceOut(BaseModel):
    class_context: ClassOut = Field(alias="class")
    active_plans: list[TeacherAssistWorkspacePlanSummaryOut] = Field(default_factory=list)
    assignments: list[TeacherAssistWorkspaceAssignmentSummaryOut] = Field(default_factory=list)
    pending_grading_reviews: list[TeacherAssistWorkspaceGradingReviewSummaryOut] = Field(default_factory=list)
    recent_submissions: list[TeacherAssistWorkspaceSubmissionSummaryOut] = Field(default_factory=list)
    workflow_summaries: list[TeacherAssistWorkspaceWorkflowSummaryOut] = Field(default_factory=list)
    packet_summaries: list[TeacherAssistWorkspacePacketSummaryOut] = Field(default_factory=list)
    needs_attention_count: int = 0

    model_config = {"populate_by_name": True}


class TeacherAssistWorkspaceOut(BaseModel):
    current_school_year: SchoolYearOut | None = None
    active_grading_period: GradingPeriodOut | None = None
    today_summary: TeacherAssistWorkspaceTodaySummaryOut
    class_workspaces: list[TeacherAssistClassWorkspaceOut] = Field(default_factory=list)
    needs_attention: list[TeacherAssistWorkspaceNeedsAttentionOut] = Field(default_factory=list)
    recent_activity: list[TeacherAssistActivityEventOut] = Field(default_factory=list)
    active_workflows: list[TeacherAssistWorkspaceWorkflowSummaryOut] = Field(default_factory=list)
    review_required_items: list[TeacherAssistWorkspaceReviewRequiredItemOut] = Field(default_factory=list)
    workspace_stats: TeacherAssistWorkspaceStatsOut
    mastery_insights: TeacherAssistWorkspaceMasteryInsightsOut | None = None


GradingPrepTextSourceLiteral = Literal["approved_text", "teacher_corrected_text", "extracted_text"]


class TeacherAssistStudentWorkGradingPrepContextOut(BaseModel):
    student_work_submission_id: uuid.UUID
    assignment_id: uuid.UUID
    student_number: int
    ready_for_grading_prep: bool
    blocked_reason: str | None = None
    review_status: ExtractionReviewStatusLiteral | None = None
    text_source: GradingPrepTextSourceLiteral | None = None
    approved_text: str | None = None
    text_char_count: int | None = None
    extracted_text_record_id: uuid.UUID | None = None
    extraction_job_id: uuid.UUID | None = None
    ai_grading_enabled: bool = False
    message: str


class TeacherAssistAssignmentGradingPrepSubmissionOut(BaseModel):
    student_work_submission_id: uuid.UUID
    student_number: int
    ready_for_grading_prep: bool
    blocked_reason: str | None = None
    review_status: ExtractionReviewStatusLiteral | None = None
    text_source: GradingPrepTextSourceLiteral | None = None
    text_char_count: int | None = None
    extracted_text_record_id: uuid.UUID | None = None
    extraction_job_id: uuid.UUID | None = None


class TeacherAssistAssignmentGradingPrepSummaryOut(BaseModel):
    assignment_id: uuid.UUID
    assignment_title: str
    total_submissions: int
    ready_for_grading_prep_count: int
    blocked_count: int
    submissions: list[TeacherAssistAssignmentGradingPrepSubmissionOut] = Field(default_factory=list)
    ai_grading_enabled: bool = False
    message: str


class TeacherAssistExportArtifactCreate(BaseModel):
    artifact_type: TeacherAssistExportArtifactTypeLiteral
    export_format: TeacherAssistExportFormatLiteral | None = None
    provider_mode: Literal["mock", "real"] = "mock"


class TeacherAssistExportArtifactOut(BaseModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    user_id: uuid.UUID
    source_plan_id: uuid.UUID
    source_assignment_id: uuid.UUID | None = None
    workflow_id: uuid.UUID | None = None
    artifact_type: TeacherAssistExportArtifactTypeLiteral
    artifact_status: TeacherAssistExportArtifactStatusLiteral
    title: str
    export_format: TeacherAssistExportFormatLiteral
    storage_key: str | None = None
    preview_json: dict[str, Any]
    metadata_json: dict[str, Any] | None = None
    provider_name: str | None = None
    provider_model: str | None = None
    prompt_version: str | None = None
    created_at: datetime
    updated_at: datetime


class TeacherAssistExportArtifactDetailOut(BaseModel):
    artifact: TeacherAssistExportArtifactOut
    workflow_status: TeacherAssistWorkflowStatusLiteral | None = None
    workflow_progress_percent: int | None = None
    workflow_error_message: str | None = None
    download_url: str | None = None


class TeacherAssistExportDownloadOut(BaseModel):
    download_url: str
    filename: str
    mime_type: str
    expires_at: datetime


class TeacherAssistWorkflowCancelUpdate(BaseModel):
    status: Literal["cancelled"]


class WeeklyPlanUpdate(BaseModel):
    title: str | None = Field(default=None, min_length=1, max_length=160)
    status: WeeklyPlanStatusLiteral | None = None
    content_json: dict[str, Any] | None = None
    change_reason: str | None = Field(default=None, max_length=500)


class WeeklyPlanSharingUpdate(BaseModel):
    is_template: bool | None = None
    visibility_scope: PlanVisibilityScopeLiteral | None = None
    reuse_status: PlanReuseStatusLiteral | None = None


class WeeklyPlanCopyCreate(BaseModel):
    target_school_year_id: uuid.UUID | None = None
    target_grading_period_id: uuid.UUID | None = None
    target_class_id: uuid.UUID | None = None
    title_override: str | None = Field(default=None, min_length=1, max_length=160)
    copy_mode: Literal["personal_copy", "rollover_copy", "template_copy"] = "personal_copy"


class WeeklyPlanAssignmentCreate(BaseModel):
    title: str | None = Field(default=None, min_length=1, max_length=160)
    description: str | None = Field(default=None, max_length=4000)
    assignment_type: AssignmentTypeLiteral = "other"
    due_date: date | None = None
    instructions: str | None = Field(default=None, max_length=4000)
    rubric_json: dict[str, Any] | None = None


class WeeklyPlanSectionRegenerationCreate(BaseModel):
    section_key: Literal[
        "overview",
        "instructional_arc",
        "weekly_segments",
        "daily_breakdown",
        "vocabulary",
        "materials_needed",
        "differentiation",
        "assessment_checkpoints",
        "standards_progression",
        "review_notes",
    ]
    section_path: str | None = Field(default=None, max_length=160)
    teacher_instruction: str | None = Field(default=None, max_length=1000)
    provider_mode: Literal["mock", "real"] | None = None
    preserve_existing_context: bool = True


class InstructionalPlanLibraryItemOut(BaseModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    user_id: uuid.UUID
    owner_user_id: uuid.UUID
    owner_name: str | None = None
    is_owner: bool = False
    planning_input_draft_id: uuid.UUID
    workflow_id: uuid.UUID | None = None
    planning_scope: PlanningScopeLiteral = "weekly"
    title: str
    plan_title: str
    module_title: str | None = None
    start_date: date | None = None
    end_date: date | None = None
    estimated_weeks: int | None = None
    instructional_days_count: int | None = None
    source_plan_id: uuid.UUID | None = None
    derived_from_plan_id: uuid.UUID | None = None
    is_template: bool = False
    visibility_scope: PlanVisibilityScopeLiteral = "private"
    reuse_status: PlanReuseStatusLiteral = "active"
    school_year_origin_id: uuid.UUID | None = None
    source_school_year_id: uuid.UUID | None = None
    source_school_year_title: str | None = None
    subject_ids: list[uuid.UUID] = Field(default_factory=list)
    subject_names: list[str] = Field(default_factory=list)
    class_id: uuid.UUID | None = None
    class_name: str | None = None
    grading_period_id: uuid.UUID | None = None
    grading_period_title: str | None = None
    status: WeeklyPlanStatusLiteral
    created_at: datetime
    updated_at: datetime


class CurriculumRolloverCandidateOut(InstructionalPlanLibraryItemOut):
    already_copied_to_target: bool = False
    existing_target_plan_id: uuid.UUID | None = None


class CurriculumRolloverCandidatesOut(BaseModel):
    items: list[CurriculumRolloverCandidateOut] = Field(default_factory=list)
    summary_counts_by_planning_scope: dict[str, int] = Field(default_factory=dict)
    subjects_represented: list[str] = Field(default_factory=list)
    grading_periods_represented: list[str] = Field(default_factory=list)


class CurriculumRolloverCopyCreate(BaseModel):
    source_school_year_id: uuid.UUID
    target_school_year_id: uuid.UUID
    plan_ids: list[uuid.UUID] = Field(default_factory=list)
    copy_mode: Literal["rollover_copy"] = "rollover_copy"
    preserve_titles: bool = True
    title_suffix: str | None = Field(default=None, max_length=64)
    target_grading_period_mapping: dict[str, uuid.UUID] | None = None


class CurriculumRolloverCopyOut(BaseModel):
    copied_plans: list[WeeklyPlanOut] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
