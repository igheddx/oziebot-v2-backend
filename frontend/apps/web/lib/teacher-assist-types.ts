export type TeacherProfile = {
  id: string | null;
  preferred_grade_level: string | null;
  default_student_count: number | null;
  preferred_grading_period_type: string | null;
  timezone: string | null;
  created_at: string | null;
  updated_at: string | null;
};

export type TeacherAssistOptions = {
  grading_period_types: string[];
  standard_types: string[];
  resource_types: string[];
  assignment_types: string[];
  assignment_statuses: string[];
  assignment_print_packet_statuses: string[];
  assignment_print_template_types: string[];
  assignment_print_output_formats: string[];
  assignment_student_work_upload_statuses: string[];
  assignment_student_work_processing_statuses: string[];
  assignment_grading_review_statuses: string[];
  assignment_grading_review_sources: string[];
  extraction_review_statuses?: string[];
  extraction_confidence_levels?: string[];
  planning_draft_statuses: string[];
  planning_scopes: string[];
  supported_grade_levels: string[];
};

export type SchoolYear = {
  id: string;
  tenant_id: string;
  title: string;
  start_date: string;
  end_date: string;
  is_active: boolean;
  created_at: string;
  updated_at: string;
};

export type GradingPeriod = {
  id: string;
  school_year_id: string;
  title: string;
  grading_period_type: string;
  start_date: string;
  end_date: string;
  sort_order: number;
  created_at: string;
  updated_at: string;
};

export type Subject = {
  id: string;
  tenant_id: string;
  code: string | null;
  name: string;
  created_at: string;
  updated_at: string;
};

export type TeacherClass = {
  id: string;
  tenant_id: string;
  school_year_id: string;
  name: string;
  grade_level: string;
  student_count: number;
  subject_ids: string[];
  student_number_range_start: number;
  student_number_range_end: number;
  created_at: string;
  updated_at: string;
};

export type Standard = {
  id: string;
  tenant_id: string;
  subject_id: string | null;
  standard_type: string;
  code: string;
  description: string;
  grade_level: string | null;
  school_year_id: string | null;
  created_at: string;
  updated_at: string;
};

export type PacingGuide = {
  id: string;
  tenant_id: string;
  school_year_id: string;
  title: string;
  description: string | null;
  grade_level: string | null;
  subject_id: string | null;
  is_shared: boolean;
  created_by_user_id: string;
  item_count: number;
  created_at: string;
  updated_at: string;
};

export type PacingItem = {
  id: string;
  pacing_guide_id: string;
  grading_period_id: string | null;
  subject_id: string | null;
  week_number: number | null;
  day_number: number | null;
  instructional_date: string | null;
  title: string;
  instructional_focus: string | null;
  objectives: string | null;
  notes: string | null;
  sort_order: number | null;
  standard_ids: string[];
  resource_ids: string[];
  created_at: string;
  updated_at: string;
};

export type ResourceLibraryItem = {
  id: string;
  tenant_id: string;
  uploaded_by_user_id: string;
  title: string;
  description: string | null;
  resource_type: string;
  storage_key: string | null;
  original_filename: string | null;
  mime_type: string | null;
  file_size: number | null;
  external_url: string | null;
  uploaded_at: string;
  linked_pacing_items_count: number;
  linked_planning_drafts_count: number;
  latest_extraction_job: TeacherAssistExtractionJob | null;
  latest_extracted_text: TeacherAssistExtractedTextRecord | null;
  created_at: string;
  updated_at: string;
};

export type TeacherAssistFileDownload = {
  url: string;
  expires_at: string;
};

export type Assignment = {
  id: string;
  tenant_id: string;
  teacher_user_id: string;
  school_year_id: string;
  grading_period_id: string | null;
  class_id: string;
  subject_id: string;
  title: string;
  description: string | null;
  assignment_type:
    | "writing"
    | "reading_response"
    | "short_answer"
    | "quiz"
    | "exit_ticket"
    | "project"
    | "homework"
    | "other";
  due_date: string | null;
  status:
    | "draft"
    | "ready"
    | "assigned"
    | "collected"
    | "review_in_progress"
    | "reviewed"
    | "archived";
  instructions: string | null;
  rubric_json: Record<string, unknown> | null;
  source_plan_id: string | null;
  source_context_json: Record<string, unknown> | null;
  standard_ids: string[];
  resource_ids: string[];
  created_at: string;
  updated_at: string;
};

export type AssignmentInput = {
  school_year_id: string;
  grading_period_id?: string | null;
  class_id: string;
  subject_id: string;
  title: string;
  description?: string | null;
  assignment_type?: Assignment["assignment_type"];
  due_date?: string | null;
  status?: Assignment["status"];
  instructions?: string | null;
  rubric_json?: Record<string, unknown> | null;
  source_plan_id?: string | null;
  source_context_json?: Record<string, unknown> | null;
  standard_ids?: string[];
  resource_ids?: string[];
};

export type AssignmentPrintPacket = {
  id: string;
  tenant_id: string;
  teacher_user_id: string;
  assignment_id: string;
  class_id: string;
  school_year_id: string;
  grading_period_id: string | null;
  subject_id: string;
  packet_status: "generated" | "archived";
  pages_per_student: number;
  student_count: number;
  template_type: "blank_writing_page" | "lined_writing_page" | "short_answer_page";
  output_format: "html";
  storage_key: string | null;
  total_page_count: number;
  created_at: string;
  updated_at: string;
};

export type AssignmentPrintPacketInput = {
  pages_per_student?: number;
  template_type?: AssignmentPrintPacket["template_type"];
  output_format?: AssignmentPrintPacket["output_format"];
};

export type AssignmentPrintPage = {
  id: string;
  packet_id: string;
  assignment_id: string;
  student_number: number;
  page_number: number;
  qr_payload_json: Record<string, unknown>;
  qr_token: string;
  qr_svg_data_uri: string;
  created_at: string;
};

export type AssignmentStudentWorkSubmission = {
  id: string;
  tenant_id: string;
  teacher_user_id: string;
  assignment_id: string;
  assignment_print_packet_id: string | null;
  assignment_print_page_id: string | null;
  school_year_id: string;
  grading_period_id: string | null;
  class_id: string;
  subject_id: string;
  student_number: number;
  original_filename: string;
  mime_type: string;
  file_size: number;
  storage_key: string;
  upload_status: "uploaded" | "archived";
  processing_status: "pending_review" | "ready_for_processing" | "processing_deferred" | "archived";
  latest_extraction_job: TeacherAssistExtractionJob | null;
  latest_extracted_text: TeacherAssistExtractedTextRecord | null;
  created_at: string;
  updated_at: string;
};

export type TeacherAssistExtractionJob = {
  id: string;
  artifact_type: "resource" | "student_work";
  resource_library_item_id: string | null;
  student_work_submission_id: string | null;
  assignment_id: string | null;
  school_year_id: string | null;
  grading_period_id: string | null;
  class_id: string | null;
  subject_id: string | null;
  student_number: number | null;
  status: "queued" | "running" | "completed" | "failed" | "cancelled" | "skipped";
  progress_percent: number;
  provider_name: string | null;
  error_code: string | null;
  error_message: string | null;
  error_metadata_json: Record<string, unknown> | null;
  retry_count: number;
  max_retries: number;
  parent_extraction_job_id: string | null;
  retry_root_job_id: string | null;
  attempt_number: number;
  leased_by_worker: string | null;
  lease_expires_at: string | null;
  heartbeat_at: string | null;
  execution_log_json: Array<Record<string, unknown>> | null;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
  updated_at: string;
};

export type ExtractionReviewStatus =
  | "pending_review"
  | "teacher_reviewing"
  | "teacher_approved"
  | "teacher_rejected"
  | "reviewed"
  | "issue_flagged"
  | "needs_retry"
  | "archived";

export type ExtractionConfidenceLevel = "low" | "medium" | "high" | "unknown";

export type TeacherAssistExtractedTextRecord = {
  id: string;
  extraction_job_id: string;
  artifact_type: "resource" | "student_work";
  resource_library_item_id: string | null;
  student_work_submission_id: string | null;
  assignment_id: string | null;
  class_id: string | null;
  subject_id: string | null;
  student_number: number | null;
  preview_text: string;
  text_char_count: number;
  pii_flagged: boolean;
  redaction_applied: boolean;
  review_status: ExtractionReviewStatus;
  provider_confidence_score: number | null;
  confidence_level: ExtractionConfidenceLevel;
  teacher_corrected_text: string | null;
  approved_text: string | null;
  reviewed_at: string | null;
  reviewed_by_user_id: string | null;
  source_extraction_job_id: string | null;
  teacher_review_notes: string | null;
  teacher_issue_reason: string | null;
  metadata_json: Record<string, unknown> | null;
  created_at: string;
  updated_at: string;
};

export type TeacherAssistExtractedTextDetail = TeacherAssistExtractedTextRecord & {
  extracted_text: string;
};

export type TeacherAssistExtractionSummary = {
  job: TeacherAssistExtractionJob;
  extracted_text: TeacherAssistExtractedTextRecord | null;
  retry_eligible: boolean;
  processing_duration_seconds: number | null;
};

export type TeacherAssistExtractedTextDetailAggregate = {
  record: TeacherAssistExtractedTextDetail;
  job: TeacherAssistExtractionJob;
  lineage_jobs: TeacherAssistExtractionJob[];
  retry_eligible: boolean;
  cancel_eligible: boolean;
  processing_duration_seconds: number | null;
  activity_events: TeacherAssistActivityEvent[];
};

export type TeacherAssistExtractionJobDetail = {
  job: TeacherAssistExtractionJob;
  extracted_text: TeacherAssistExtractedTextRecord | null;
  lineage_jobs: TeacherAssistExtractionJob[];
  retry_eligible: boolean;
  cancel_eligible: boolean;
  processing_duration_seconds: number | null;
  execution_timeline: Array<Record<string, unknown>>;
  source_artifact: {
    artifact_type: "resource" | "student_work";
    original_filename: string;
    mime_type: string;
    file_size: number;
    resource_library_item_id: string | null;
    student_work_submission_id: string | null;
    assignment_id: string | null;
    student_number: number | null;
  };
  activity_events: TeacherAssistActivityEvent[];
};

export type TeacherAssistExtractedTextHistory = {
  current_record: TeacherAssistExtractedTextDetail;
  current_job: TeacherAssistExtractionJob;
  attempt_jobs: TeacherAssistExtractionJob[];
  attempt_records: TeacherAssistExtractedTextRecord[];
  activity_events: TeacherAssistActivityEvent[];
};

export type TeacherAssistExtractionRun = {
  job: TeacherAssistExtractionJob;
  extracted_text: TeacherAssistExtractedTextRecord | null;
};

export type AssignmentGradingReviewItem = {
  id: string;
  grading_review_id: string;
  criterion_title: string;
  score_suggestion: number | null;
  max_score: number | null;
  feedback_summary: string | null;
  strengths: string[];
  improvement_areas: string[];
  teacher_notes: string | null;
  sort_order: number;
  created_at: string;
  updated_at: string;
};

export type AssignmentGradingReview = {
  id: string;
  tenant_id: string;
  teacher_user_id: string;
  assignment_id: string;
  student_work_submission_id: string;
  student_number: number;
  school_year_id: string;
  grading_period_id: string | null;
  class_id: string;
  subject_id: string;
  status:
    | "draft"
    | "ai_suggested"
    | "teacher_reviewing"
    | "teacher_confirmed"
    | "returned_for_revision"
    | "archived";
  review_source: "manual" | "ai_placeholder";
  provider_name: string | null;
  provider_model: string | null;
  prompt_version: string | null;
  ai_usage_event_id: string | null;
  score_suggestion: number | null;
  max_score: number | null;
  feedback_summary: string | null;
  strengths: string[];
  improvement_areas: string[];
  teacher_notes: string | null;
  teacher_confirmed_score: number | null;
  teacher_confirmed_feedback: string | null;
  items: AssignmentGradingReviewItem[];
  created_at: string;
  updated_at: string;
};

export type AssignmentGradingReviewCreateInput = {
  student_number: number;
  score_suggestion?: number | null;
  max_score?: number | null;
  feedback_summary?: string | null;
  strengths?: string[];
  improvement_areas?: string[];
  teacher_notes?: string | null;
  items?: Array<{
    criterion_title: string;
    score_suggestion?: number | null;
    max_score?: number | null;
    feedback_summary?: string | null;
    strengths?: string[];
    improvement_areas?: string[];
    teacher_notes?: string | null;
    sort_order?: number;
  }>;
};

export type AssignmentGradingReviewUpdateInput = {
  status: AssignmentGradingReview["status"];
  score_suggestion?: number | null;
  max_score?: number | null;
  feedback_summary?: string | null;
  strengths?: string[];
  improvement_areas?: string[];
  teacher_notes?: string | null;
  teacher_confirmed_score?: number | null;
  teacher_confirmed_feedback?: string | null;
  items?: AssignmentGradingReviewCreateInput["items"];
};

export type PlanningDraft = {
  id: string;
  tenant_id: string;
  user_id: string;
  planning_scope: "weekly" | "multi_week" | "module" | "unit" | "grading_period";
  school_year_id: string | null;
  grading_period_id: string | null;
  class_id: string | null;
  subject_id: string | null;
  subject_ids: string[];
  pacing_item_ids: string[];
  standard_ids: string[];
  title: string | null;
  plan_title: string | null;
  module_title: string | null;
  start_date: string | null;
  end_date: string | null;
  estimated_weeks: number | null;
  instructional_days_count: number | null;
  notes: string | null;
  status: string;
  resource_ids: string[];
  created_at: string;
  updated_at: string;
};

export type PlanningDraftReadiness = {
  is_ready: boolean;
  missing_items: string[];
  warnings: string[];
};

export type PlanningDraftContextPreview = {
  draft: PlanningDraft;
  school_year: SchoolYear | null;
  grading_period: GradingPeriod | null;
  class: TeacherClass | null;
  subjects: Subject[];
  pacing_items: PacingItem[];
  pacing_groups: Array<{
    group_key: string;
    label: string;
    pacing_items: PacingItem[];
  }>;
  standards: Standard[];
  resources: ResourceLibraryItem[];
  teacher_notes: string | null;
  duration_summary: {
    start_date: string | null;
    end_date: string | null;
    estimated_weeks: number | null;
    instructional_days_count: number | null;
    summary: string;
  };
  readiness: PlanningDraftReadiness;
};

export type TeacherAssistWorkflowStep = {
  id: string;
  workflow_id: string;
  step_name: string;
  status: "queued" | "running" | "completed" | "failed" | "skipped";
  metadata_json: Record<string, unknown> | null;
  error_message: string | null;
  started_at: string | null;
  completed_at: string | null;
  created_at: string;
};

export type TeacherAssistWorkflow = {
  id: string;
  tenant_id: string;
  user_id: string;
  planning_input_draft_id: string | null;
  workflow_type:
    | "weekly_plan_generation"
    | "daily_deck_generation"
    | "assessment_generation"
    | "newsletter_generation"
    | "grading_assist";
  status: "queued" | "running" | "completed" | "failed" | "cancelled";
  input_snapshot_json: Record<string, unknown>;
  output_ref_type: string | null;
  output_ref_id: string | null;
  error_message: string | null;
  last_error_code: string | null;
  progress_percent: number;
  leased_by_worker: string | null;
  lease_expires_at: string | null;
  heartbeat_at: string | null;
  retry_count: number;
  max_retries: number;
  timeout_at: string | null;
  provider_name: string | null;
  provider_model: string | null;
  prompt_version: string | null;
  input_tokens_total: number;
  output_tokens_total: number;
  estimated_cost_cents_total: number;
  execution_log_json: Array<Record<string, unknown>>;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
  updated_at: string;
};

export type TeacherAssistAIUsageEvent = {
  id: string;
  tenant_id: string;
  user_id: string;
  workflow_id: string | null;
  provider: string;
  model: string | null;
  feature: string;
  input_tokens: number | null;
  output_tokens: number | null;
  estimated_cost_cents: number | null;
  metadata_json: Record<string, unknown> | null;
  created_at: string;
};

export type TeacherAssistWorkflowDetail = TeacherAssistWorkflow & {
  steps: TeacherAssistWorkflowStep[];
  usage_events: TeacherAssistAIUsageEvent[];
};

export type TeacherAssistActivityEvent = {
  id: string;
  event_category: string;
  event_type: string;
  entity_type: string;
  entity_id: string;
  timestamp: string;
  summary_text: string;
  workflow_id: string | null;
  school_year_id: string | null;
  grading_period_id: string | null;
  class_id: string | null;
  subject_id: string | null;
  details_json: Record<string, unknown> | null;
  created_at: string;
};

export type TeacherAssistWorkspacePlanSummary = {
  id: string;
  title: string;
  planning_scope: "weekly" | "multi_week" | "module" | "unit" | "grading_period";
  status: "in_progress" | "completed";
  workflow_id: string | null;
  class_id: string | null;
  school_year_id: string | null;
  review_required: boolean;
  quality_flags: string[];
  missing_context_warnings: string[];
  updated_at: string;
};

export type TeacherAssistWorkspaceAssignmentSummary = {
  id: string;
  class_id: string;
  subject_id: string;
  title: string;
  status: Assignment["status"];
  assignment_type: Assignment["assignment_type"];
  due_date: string | null;
  updated_at: string;
};

export type TeacherAssistWorkspacePacketSummary = {
  id: string;
  assignment_id: string;
  class_id: string;
  packet_status: AssignmentPrintPacket["packet_status"];
  pages_per_student: number;
  student_count: number;
  template_type: AssignmentPrintPacket["template_type"];
  created_at: string;
  updated_at: string;
};

export type TeacherAssistWorkspaceSubmissionSummary = {
  id: string;
  assignment_id: string;
  class_id: string;
  student_number: number;
  original_filename: string;
  upload_status: AssignmentStudentWorkSubmission["upload_status"];
  processing_status: AssignmentStudentWorkSubmission["processing_status"];
  latest_extraction_status: TeacherAssistExtractionJob["status"] | null;
  extraction_ready_for_teacher_review: boolean;
  created_at: string;
  updated_at: string;
};

export type TeacherAssistWorkspaceGradingReviewSummary = {
  id: string;
  assignment_id: string;
  student_work_submission_id: string;
  class_id: string;
  student_number: number;
  status: AssignmentGradingReview["status"];
  teacher_confirmed_score: number | null;
  updated_at: string;
};

export type TeacherAssistWorkspaceWorkflowSummary = {
  id: string;
  workflow_type: TeacherAssistWorkflow["workflow_type"];
  status: TeacherAssistWorkflow["status"];
  class_id: string | null;
  school_year_id: string | null;
  grading_period_id: string | null;
  progress_percent: number;
  retry_count: number;
  max_retries: number;
  provider_name: string | null;
  provider_model: string | null;
  last_error_code: string | null;
  heartbeat_at: string | null;
  created_at: string;
  updated_at: string;
  completed_at: string | null;
  error_message: string | null;
};

export type TeacherAssistWorkspaceNeedsAttention = {
  type: string;
  severity: "info" | "warning" | "critical";
  title: string;
  message: string;
  entity_type: string;
  entity_id: string;
  class_id: string | null;
  created_at: string;
};

export type TeacherAssistWorkspaceReviewRequiredItem = {
  entity_type: string;
  entity_id: string;
  class_id: string | null;
  title: string;
  status: string;
  review_reason: string;
  updated_at: string;
};

export type TeacherAssistWorkspaceTodaySummary = {
  active_grading_period_title: string | null;
  active_workflows_count: number;
  plans_needing_review_count: number;
  grading_reviews_pending_confirmation_count: number;
  recent_uploads_count: number;
  workflow_failures_count: number;
  extraction_failures_count: number;
  student_work_ready_for_extraction_count: number;
  extracted_artifacts_ready_for_teacher_review_count: number;
  low_confidence_extractions_count: number;
  rejected_extractions_count: number;
  retry_required_extractions_count: number;
  awaiting_teacher_review_count: number;
  stale_extraction_jobs_count: number;
  recently_approved_extractions_count: number;
};

export type TeacherAssistWorkspaceStats = {
  active_plans_count: number;
  plans_in_review_count: number;
  pending_grading_reviews_count: number;
  recent_upload_count: number;
  workflow_failure_count: number;
  assignments_in_review_count: number;
  extraction_failure_count: number;
  student_work_ready_for_extraction_count: number;
  extracted_artifacts_ready_for_teacher_review_count: number;
  low_confidence_extractions_count: number;
  rejected_extractions_count: number;
  retry_required_extractions_count: number;
  awaiting_teacher_review_count: number;
  stale_extraction_jobs_count: number;
  recently_approved_extractions_count: number;
};

export type TeacherAssistClassWorkspace = {
  class: TeacherClass;
  active_plans: TeacherAssistWorkspacePlanSummary[];
  assignments: TeacherAssistWorkspaceAssignmentSummary[];
  pending_grading_reviews: TeacherAssistWorkspaceGradingReviewSummary[];
  recent_submissions: TeacherAssistWorkspaceSubmissionSummary[];
  workflow_summaries: TeacherAssistWorkspaceWorkflowSummary[];
  packet_summaries: TeacherAssistWorkspacePacketSummary[];
  needs_attention_count: number;
};

export type TeacherAssistWorkspace = {
  current_school_year: SchoolYear | null;
  active_grading_period: GradingPeriod | null;
  today_summary: TeacherAssistWorkspaceTodaySummary;
  class_workspaces: TeacherAssistClassWorkspace[];
  needs_attention: TeacherAssistWorkspaceNeedsAttention[];
  recent_activity: TeacherAssistActivityEvent[];
  active_workflows: TeacherAssistWorkspaceWorkflowSummary[];
  review_required_items: TeacherAssistWorkspaceReviewRequiredItem[];
  workspace_stats: TeacherAssistWorkspaceStats;
};

export type WeeklyPlanContentStandard = {
  id?: string;
  code?: string;
  description?: string;
};

export type WeeklyPlanContentDay = {
  day?: number;
  day_label?: string;
  focus?: string;
  teacher_actions?: string[];
  student_activities?: string[];
  checks_for_understanding?: string[];
  materials_needed?: string[];
};

export type WeeklyPlanContentSubject = {
  subject_id?: string;
  subject_name?: string;
  standards?: WeeklyPlanContentStandard[];
  objectives?: string[];
  vocabulary?: string[];
  daily_breakdown?: WeeklyPlanContentDay[];
  differentiation?: {
    support?: string[];
    extension?: string[];
    visual_supports?: string[];
  };
  suggested_artifacts?: string[];
};

export type WeeklyPlanContent = {
  metadata?: {
    is_mock?: boolean;
    generator?: string;
    provider_mode?: string;
    provider_model?: string | null;
    prompt_version?: string;
    version?: number;
    generated_at?: string;
    planning_draft_id?: string;
    workflow_id?: string | null;
    copied_from_plan_id?: string;
    copied_at?: string;
  };
  planning_scope?: string;
  plan_title?: string;
  module_title?: string | null;
  duration?: {
    start_date?: string | null;
    end_date?: string | null;
    estimated_weeks?: number | null;
    instructional_days_count?: number | null;
    summary?: string;
  };
  overview?: string;
  instructional_arc?: string[];
  weekly_objectives?: string[];
  subjects?: WeeklyPlanContentSubject[];
  weekly_segments?: Array<{
    segment_index?: number;
    segment_label?: string;
    focus?: string;
    objectives?: string[];
    subjects?: Array<{
      subject_id?: string;
      subject_name?: string;
      objectives?: string[];
      daily_breakdown?: WeeklyPlanContentDay[];
    }>;
    daily_breakdown?: WeeklyPlanContentDay[];
    assessment_checkpoints?: string[];
  }>;
  standards_progression?: Array<{ code?: string; description?: string; phase?: string }>;
  vocabulary?: string[];
  materials_needed?: string[];
  differentiation?: {
    support?: string[];
    extension?: string[];
    intervention?: string[];
  };
  assessment_checkpoints?: string[];
  daily_breakdown?: WeeklyPlanContentDay[];
  resources_used?: Array<{ id?: string; title?: string; resource_type?: string }>;
  teacher_notes_used?: string | null;
  review_notes?: string;
  review_required?: boolean;
  quality_flags?: string[];
  missing_context_warnings?: string[];
  standards_alignment_summary?: string;
  teacher_review_checklist?: string[];
};

export type WeeklyPlan = {
  id: string;
  tenant_id: string;
  user_id: string;
  owner_user_id: string;
  planning_input_draft_id: string;
  workflow_id: string | null;
  planning_scope: "weekly" | "multi_week" | "module" | "unit" | "grading_period";
  title: string;
  plan_title: string;
  module_title: string | null;
  start_date: string | null;
  end_date: string | null;
  estimated_weeks: number | null;
  instructional_days_count: number | null;
  source_plan_id: string | null;
  derived_from_plan_id: string | null;
  is_template: boolean;
  visibility_scope: "private" | "shared" | "grade_team" | "school" | "district";
  reuse_status: "active" | "archived" | "reusable";
  school_year_origin_id: string | null;
  status: "in_progress" | "completed";
  content_json: WeeklyPlanContent;
  source_context_json: Record<string, unknown>;
  current_version_number: number;
  latest_usage_event: TeacherAssistAIUsageEvent | null;
  created_at: string;
  updated_at: string;
};

export type WeeklyPlanVersion = {
  id: string;
  weekly_plan_id: string;
  version_number: number;
  content_json: WeeklyPlanContent;
  source_context_json: Record<string, unknown>;
  created_by_user_id: string;
  created_at: string;
  change_reason: string | null;
};

export type WeeklyPlanUpdateInput = {
  title?: string;
  status?: "in_progress" | "completed";
  content_json?: WeeklyPlanContent;
  change_reason?: string;
};

export type WeeklyPlanSectionKey =
  | "overview"
  | "instructional_arc"
  | "weekly_segments"
  | "daily_breakdown"
  | "vocabulary"
  | "materials_needed"
  | "differentiation"
  | "assessment_checkpoints"
  | "standards_progression"
  | "review_notes";

export type WeeklyPlanSharingUpdateInput = {
  is_template?: boolean;
  visibility_scope?: "private" | "shared" | "grade_team" | "school" | "district";
  reuse_status?: "active" | "archived" | "reusable";
};

export type WeeklyPlanCopyInput = {
  target_school_year_id?: string;
  target_grading_period_id?: string;
  target_class_id?: string;
  title_override?: string;
  copy_mode?: "personal_copy" | "rollover_copy" | "template_copy";
};

export type WeeklyPlanSectionRegenerationInput = {
  section_key: WeeklyPlanSectionKey;
  section_path?: string | null;
  teacher_instruction?: string | null;
  provider_mode?: "mock" | "real" | null;
  preserve_existing_context?: boolean;
};

export type InstructionalPlanLibraryItem = {
  id: string;
  tenant_id: string;
  user_id: string;
  owner_user_id: string;
  owner_name: string | null;
  is_owner: boolean;
  planning_input_draft_id: string;
  workflow_id: string | null;
  planning_scope: "weekly" | "multi_week" | "module" | "unit" | "grading_period";
  title: string;
  plan_title: string;
  module_title: string | null;
  start_date: string | null;
  end_date: string | null;
  estimated_weeks: number | null;
  instructional_days_count: number | null;
  source_plan_id: string | null;
  derived_from_plan_id: string | null;
  is_template: boolean;
  visibility_scope: "private" | "shared" | "grade_team" | "school" | "district";
  reuse_status: "active" | "archived" | "reusable";
  school_year_origin_id: string | null;
  source_school_year_id: string | null;
  source_school_year_title: string | null;
  subject_ids: string[];
  subject_names: string[];
  class_id: string | null;
  class_name: string | null;
  grading_period_id: string | null;
  grading_period_title: string | null;
  status: "in_progress" | "completed";
  created_at: string;
  updated_at: string;
};

export type CurriculumRolloverCandidate = InstructionalPlanLibraryItem & {
  already_copied_to_target: boolean;
  existing_target_plan_id: string | null;
};

export type CurriculumRolloverCandidates = {
  items: CurriculumRolloverCandidate[];
  summary_counts_by_planning_scope: Record<string, number>;
  subjects_represented: string[];
  grading_periods_represented: string[];
};

export type CurriculumRolloverCopyInput = {
  source_school_year_id: string;
  target_school_year_id: string;
  plan_ids: string[];
  copy_mode?: "rollover_copy";
  preserve_titles?: boolean;
  title_suffix?: string;
  target_grading_period_mapping?: Record<string, string>;
};

export type CurriculumRolloverCopyResult = {
  copied_plans: WeeklyPlan[];
  warnings: string[];
};
