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
]
TeacherAssistWorkflowStatusLiteral = Literal["queued", "running", "completed", "failed", "cancelled"]
TeacherAssistWorkflowStepStatusLiteral = Literal["queued", "running", "completed", "failed", "skipped"]
WeeklyPlanStatusLiteral = Literal["in_progress", "completed"]


class TeacherAssistOptionsOut(BaseModel):
    grading_period_types: list[str]
    standard_types: list[str]
    resource_types: list[str]
    assignment_types: list[str]
    assignment_statuses: list[str]
    assignment_print_packet_statuses: list[str]
    assignment_print_template_types: list[str]
    assignment_print_output_formats: list[str]
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
    created_at: datetime
    updated_at: datetime


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
